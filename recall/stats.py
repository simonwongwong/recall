"""Aggregate statistics over the derived index.

All queries hit `index.db` (read-only). Reactions are excluded from counts unless
explicitly asked for — a reaction is its own iMessage row but it's noise for any
"how active is this conversation" question.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from . import db as dbmod


def _q(conn: sqlite3.Connection, sql: str, params: tuple | list = ()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _chat_filter(chat_ids: list[int] | None, col: str = "m.chat_rowid") -> tuple[str, list]:
    """Return (' AND col IN (?,?,…)', [params]) — or empty strings if no filter."""
    if not chat_ids:
        return "", []
    return f" AND {col} IN ({','.join('?' * len(chat_ids))})", list(chat_ids)


def overview(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> dict[str, Any]:
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(is_from_me) AS from_me,
            SUM(has_attachments) AS with_attachments,
            MIN(date_unix) AS first_unix,
            MAX(date_unix) AS last_unix
        FROM messages WHERE is_reaction = 0{cf}
        """,
        cp,
    ).fetchone()
    reactions = conn.execute(
        f"SELECT COUNT(*) AS n FROM messages WHERE is_reaction = 1{cf}", cp
    ).fetchone()["n"]
    if chat_ids:
        chats = {"n": len(chat_ids), "g": None}
        handles = None
    else:
        chats = conn.execute("SELECT COUNT(*) AS n, SUM(is_group) AS g FROM chats").fetchone()
        handles = conn.execute("SELECT COUNT(*) AS n FROM handles").fetchone()["n"]
    return {
        "total_messages": row["total"] or 0,
        "from_me": row["from_me"] or 0,
        "received": (row["total"] or 0) - (row["from_me"] or 0),
        "with_attachments": row["with_attachments"] or 0,
        "reactions": reactions or 0,
        "first_unix": row["first_unix"],
        "last_unix": row["last_unix"],
        "total_chats": chats["n"] or 0,
        "group_chats": chats["g"],
        "one_on_one_chats": (chats["n"] - chats["g"]) if chats["g"] is not None else None,
        "total_handles": handles,
    }


def by_month(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> list[dict]:
    """Year-month timeline (one bucket per calendar month across the archive)."""
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    return _q(conn, f"""
        SELECT strftime('%Y-%m', date_unix, 'unixepoch') AS bucket,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        GROUP BY bucket
        ORDER BY bucket
    """, cp)


def by_year(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> list[dict]:
    """One row per calendar year."""
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    return _q(conn, f"""
        SELECT strftime('%Y', date_unix, 'unixepoch') AS bucket,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        GROUP BY bucket
        ORDER BY bucket
    """, cp)


def by_month_of_year(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> list[dict]:
    """Seasonality — sum across all years for each calendar month (Jan-Dec)."""
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    rows = _q(conn, f"""
        SELECT CAST(strftime('%m', date_unix, 'unixepoch') AS INTEGER) AS m,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        GROUP BY m ORDER BY m
    """, cp)
    by_m = {r["m"]: r for r in rows}
    labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return [
        {"m": i, "label": labels[i-1], "n": (by_m.get(i) or {}).get("n", 0), "me": (by_m.get(i) or {}).get("me", 0)}
        for i in range(1, 13)
    ]


def by_hour(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> list[dict]:
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    rows = _q(conn, f"""
        SELECT CAST(strftime('%H', date_unix, 'unixepoch', 'localtime') AS INTEGER) AS h,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        GROUP BY h
        ORDER BY h
    """, cp)
    by_h = {r["h"]: r for r in rows}
    return [
        {"h": h, "n": (by_h.get(h) or {}).get("n", 0), "me": (by_h.get(h) or {}).get("me", 0) or 0}
        for h in range(24)
    ]


def by_weekday(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> list[dict]:
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    rows = _q(conn, f"""
        SELECT CAST(strftime('%w', date_unix, 'unixepoch', 'localtime') AS INTEGER) AS d,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        GROUP BY d ORDER BY d
    """, cp)
    by_d = {r["d"]: r for r in rows}
    labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    return [
        {"d": d, "label": labels[d], "n": (by_d.get(d) or {}).get("n", 0), "me": (by_d.get(d) or {}).get("me", 0) or 0}
        for d in range(7)
    ]


def top_chats(conn: sqlite3.Connection, limit: int = 15, offset: int = 0) -> dict:
    """Top conversations, with 1:1 chats merged across handles by resolved name.

    Mirrors `search.list_people`: a "person" with phone + email shows as one row
    with combined counts. Group chats stay distinct (one entry each).
    """
    rows = _q(conn, """
        SELECT c.rowid AS chat_id,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS name,
               c.is_group,
               COUNT(m.rowid) AS n,
               SUM(m.is_from_me) AS me,
               MIN(m.date_unix) AS first_unix,
               MAX(m.date_unix) AS last_unix
        FROM messages m
        JOIN chats c ON c.rowid = m.chat_rowid
        WHERE m.is_reaction = 0
        GROUP BY c.rowid
    """)
    # Merge 1:1 by name; groups pass through.
    merged: dict[str, dict] = {}
    for r in rows:
        # Bucket key: groups keep their own row by chat_id; 1:1s merge by name.
        key = f"g:{r['chat_id']}" if r["is_group"] else f"p:{r['name']}"
        cur = merged.get(key)
        if not cur:
            merged[key] = {
                "chat_id": r["chat_id"],
                "chat_ids": [r["chat_id"]],
                "name": r["name"],
                "is_group": r["is_group"],
                "n": r["n"],
                "me": r["me"] or 0,
                "first_unix": r["first_unix"],
                "last_unix": r["last_unix"],
            }
        else:
            cur["chat_ids"].append(r["chat_id"])
            cur["n"] += r["n"]
            cur["me"] += r["me"] or 0
            if r["first_unix"] and (cur["first_unix"] is None or r["first_unix"] < cur["first_unix"]):
                cur["first_unix"] = r["first_unix"]
            if r["last_unix"] and (cur["last_unix"] is None or r["last_unix"] > cur["last_unix"]):
                cur["last_unix"] = r["last_unix"]
    items = sorted(merged.values(), key=lambda r: -r["n"])
    return {"items": items[offset:offset + limit], "total": len(items)}


def busiest_days(conn: sqlite3.Connection, limit: int = 10, offset: int = 0,
                 mode: str = "all", chat_ids: list[int] | None = None) -> dict:
    """`mode` is 'all' (default), 'me' (sent), or 'them' (received)."""
    where = "is_reaction = 0 AND date_unix IS NOT NULL"
    if mode == "me":
        where += " AND is_from_me = 1"
    elif mode == "them":
        where += " AND is_from_me = 0"
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    rows = _q(conn, f"""
        SELECT date(date_unix, 'unixepoch', 'localtime') AS day,
               COUNT(*) AS n,
               SUM(is_from_me) AS me,
               COUNT(*) - SUM(is_from_me) AS them
        FROM messages
        WHERE {where}{cf}
        GROUP BY day ORDER BY n DESC LIMIT ? OFFSET ?
    """, [*cp, limit, offset])
    total = conn.execute(
        f"SELECT COUNT(DISTINCT date(date_unix, 'unixepoch', 'localtime')) AS n "
        f"FROM messages WHERE {where}{cf}", cp
    ).fetchone()["n"]
    return {"items": rows, "total": total, "mode": mode}


def lopsided(conn: sqlite3.Connection, limit: int = 10, offset: int = 0,
             min_msgs: int = 100) -> dict:
    """Per-1:1 send/receive ratios. 1:1 chats are merged by resolved name first."""
    rows = _q(conn, """
        SELECT c.rowid AS chat_id,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS name,
               COUNT(*) AS n,
               SUM(m.is_from_me) AS me
        FROM messages m
        JOIN chats c ON c.rowid = m.chat_rowid
        WHERE m.is_reaction = 0 AND c.is_group = 0
        GROUP BY c.rowid
    """)
    # Merge by resolved name across handles (e.g. phone + email).
    merged: dict[str, dict] = {}
    for r in rows:
        cur = merged.setdefault(r["name"], {"chat_id": r["chat_id"], "name": r["name"], "n": 0, "me": 0})
        cur["n"] += r["n"]
        cur["me"] += r["me"] or 0
    items = [r for r in merged.values() if r["n"] >= min_msgs]
    for r in items:
        r["me_pct"] = round(100.0 * r["me"] / r["n"], 1) if r["n"] else 0
    you_sorted = sorted(items, key=lambda r: -r["me_pct"])
    they_sorted = sorted(items, key=lambda r: r["me_pct"])
    return {
        "you_send_more": you_sorted[offset:offset + limit],
        "they_send_more": they_sorted[offset:offset + limit],
        "total": len(items),
    }


def longest_messages(conn: sqlite3.Connection, limit: int = 5, offset: int = 0,
                     chat_ids: list[int] | None = None, mode: str = "all") -> dict:
    """`mode` is 'all' | 'me' (sent) | 'them' (received)."""
    cf, cp = _chat_filter(chat_ids, "m.chat_rowid")
    extra = ""
    if mode == "me":
        extra = " AND m.is_from_me = 1"
    elif mode == "them":
        extra = " AND m.is_from_me = 0"
    rows = _q(conn, f"""
        SELECT m.rowid, length(m.text) AS len, m.text, m.date_unix,
               m.is_from_me,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        WHERE m.is_reaction = 0 AND m.text IS NOT NULL AND m.text != ''{cf}{extra}
        ORDER BY len DESC LIMIT ? OFFSET ?
    """, [*cp, limit, offset])
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM messages m WHERE is_reaction = 0 "
        f"AND text IS NOT NULL AND text != ''{cf}{extra}", cp
    ).fetchone()["n"]
    return {"items": rows, "total": total, "mode": mode}


def first_messages(conn: sqlite3.Connection, limit: int = 5,
                   chat_ids: list[int] | None = None) -> list[dict]:
    """Earliest messages overall — fun nostalgia."""
    cf, cp = _chat_filter(chat_ids, "m.chat_rowid")
    return _q(conn, f"""
        SELECT m.rowid, m.text, m.date_unix, m.is_from_me,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
               h.handle
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        WHERE m.is_reaction = 0 AND m.text != '' AND m.date_unix IS NOT NULL{cf}
        ORDER BY m.date_unix ASC LIMIT ?
    """, [*cp, limit])


def streaks(conn: sqlite3.Connection, chat_ids: list[int] | None = None) -> dict:
    """Single-day messaging streaks — longest consecutive run of days with any message."""
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    days = _q(conn, f"""
        SELECT DISTINCT date(date_unix, 'unixepoch', 'localtime') AS day
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL{cf}
        ORDER BY day
    """, cp)
    if not days:
        return {"longest": 0, "longest_start": None, "longest_end": None}

    from datetime import date as _date, timedelta

    longest = cur = 1
    cur_start = longest_start = days[0]["day"]
    longest_end = days[0]["day"]
    prev = _date.fromisoformat(days[0]["day"])
    for d in days[1:]:
        cd = _date.fromisoformat(d["day"])
        if cd - prev == timedelta(days=1):
            cur += 1
        else:
            cur = 1
            cur_start = d["day"]
        if cur > longest:
            longest = cur
            longest_start = cur_start
            longest_end = d["day"]
        prev = cd
    return {"longest": longest, "longest_start": longest_start, "longest_end": longest_end}


def all_stats(chat_ids: list[int] | None = None) -> dict[str, Any]:
    """Compute every stats section in parallel.

    Sections are independent SQLite aggregations; the slow one (`emoji_stats`)
    is regex-bound and used to dominate the sequential sum. SQLite connections
    aren't thread-safe, so each task opens its own; index.db is in WAL mode
    so concurrent read-only connections coexist fine. Wall-clock is now
    bounded by the slowest single section (~2–3s emojis on Termux/aarch64),
    not the sum of all of them.
    """
    from concurrent.futures import ThreadPoolExecutor

    def with_conn(fn, *args, **kwargs):
        c = dbmod.open_index_db(read_only=True)
        try:
            return fn(c, *args, **kwargs)
        finally:
            c.close()

    tasks: dict[str, Any] = {
        "overview":         lambda: with_conn(overview, chat_ids),
        "by_month":         lambda: with_conn(by_month, chat_ids),
        "by_year":          lambda: with_conn(by_year, chat_ids),
        "by_month_of_year": lambda: with_conn(by_month_of_year, chat_ids),
        "by_hour":          lambda: with_conn(by_hour, chat_ids),
        "by_weekday":       lambda: with_conn(by_weekday, chat_ids),
        "busiest_days":     lambda: with_conn(busiest_days, limit=10, chat_ids=chat_ids),
        "longest_messages": lambda: with_conn(longest_messages, limit=3, chat_ids=chat_ids),
        "first_messages":   lambda: with_conn(first_messages, chat_ids=chat_ids),
        "streaks":          lambda: with_conn(streaks, chat_ids),
        "stickers":         lambda: sticker_stats(limit=12, chat_ids=chat_ids),
        "emojis":           lambda: emoji_stats(limit=24, chat_ids=chat_ids),
        "tapbacks":         lambda: tapback_stats(chat_ids),
    }
    # Cross-chat comparisons only make sense without a chat filter.
    if not chat_ids:
        tasks["top_chats"] = lambda: with_conn(top_chats, limit=15)
        tasks["lopsided"]  = lambda: with_conn(lopsided, limit=10)

    out: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(tasks))) as ex:
        futures = {key: ex.submit(fn) for key, fn in tasks.items()}
        for key, fut in futures.items():
            out[key] = fut.result()
    out["chat_ids"] = chat_ids or []
    return out


# Stdlib-only emoji range. Single codepoints, no grapheme-cluster joining —
# 👨‍👩‍👧 will count as 👨 + 👩 + 👧 separately, but the leaderboard still tells the story.
import re as _re
_EMOJI_RE = _re.compile(
    "["
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F600-\U0001F64F"   # emoticons
    "\U0001F680-\U0001F6FF"   # transport & map
    "\U0001F700-\U0001F77F"   # alchemical
    "\U0001F780-\U0001F7FF"   # geometric
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"   # supplemental symbols
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002600-\U000026FF"   # misc symbols
    "\U00002700-\U000027BF"   # dingbats
    "\U0001F1E6-\U0001F1FF"   # flags
    "]"
)


def sticker_stats(limit: int = 12, offset: int = 0,
                  chat_ids: list[int] | None = None, mode: str = "all") -> dict:
    """Most-used stickers, with sent/received split and a thumbnail attachment id.

    Stickers live in `attachment` rows with `is_sticker = 1` and a path under
    `~/Library/Messages/StickerCache/`. Apple's content-addressed cache means
    the same sticker UUID may have multiple file paths; we group by `transfer_name`
    (the UUID basename) so all uses across cache copies fold together.

    If `chat_ids` is set, restrict to messages in those chats (joined via
    `chat_message_join` since the sticker query reads from the source chat.db).

    In index-only mode (no chat.db reachable) returns an empty result —
    sticker rows live only in the source chat.db, not the index.
    """
    if dbmod.chat_db_path() is None:
        return {"items": [], "total": 0, "mode": mode}
    src = dbmod.open_chat_db()
    chat_join = ""
    chat_where = ""
    chat_params: list = []
    if chat_ids:
        chat_join = "JOIN chat_message_join cmj ON cmj.message_id = m.ROWID"
        chat_where = f"AND cmj.chat_id IN ({','.join('?' * len(chat_ids))})"
        chat_params = list(chat_ids)
    mode_where = ""
    if mode == "me":
        mode_where = " AND m.is_from_me = 1"
    elif mode == "them":
        mode_where = " AND m.is_from_me = 0"
    # Order by the same dimension we filtered on so the leaderboard makes sense.
    order_col = (
        "sent"     if mode == "me"   else
        "received" if mode == "them" else
        "uses"
    )
    rows = src.execute(
        f"""
        SELECT
            a.transfer_name AS sticker_id,
            COUNT(*)        AS uses,
            SUM(m.is_from_me) AS sent,
            COUNT(*) - SUM(m.is_from_me) AS received,
            MAX(a.ROWID)    AS sample_att_rowid,
            MAX(a.mime_type) AS mime_type,
            MIN(m.date) AS first_used_mac,
            MAX(m.date) AS last_used_mac
        FROM attachment a
        JOIN message_attachment_join maj ON maj.attachment_id = a.ROWID
        JOIN message m ON m.ROWID = maj.message_id
        {chat_join}
        WHERE a.is_sticker = 1 {chat_where}{mode_where}
        GROUP BY a.transfer_name
        ORDER BY {order_col} DESC
        LIMIT ? OFFSET ?
        """,
        [*chat_params, limit, offset],
    ).fetchall()
    total = src.execute(
        f"""
        SELECT COUNT(DISTINCT a.transfer_name) AS n
        FROM attachment a
        JOIN message_attachment_join maj ON maj.attachment_id = a.ROWID
        JOIN message m ON m.ROWID = maj.message_id
        {chat_join}
        WHERE a.is_sticker = 1 {chat_where}{mode_where}
        """,
        chat_params,
    ).fetchone()["n"]
    return {
        "items": [
            {
                "sticker_id": r["sticker_id"],
                "uses": r["uses"],
                "sent": r["sent"] or 0,
                "received": (r["uses"] or 0) - (r["sent"] or 0),
                "att_rowid": r["sample_att_rowid"],
                "mime_type": r["mime_type"],
                "first_used_unix": dbmod.mac_ts_to_unix(r["first_used_mac"]),
                "last_used_unix":  dbmod.mac_ts_to_unix(r["last_used_mac"]),
            }
            for r in rows
        ],
        "total": total,
        "mode": mode,
    }


def emoji_stats(limit: int = 30, offset: int = 0,
                chat_ids: list[int] | None = None, mode: str = "all") -> dict:
    """Most-used inline emoji. `mode` ranks the leaderboard by all uses ('all'),
    only ones you sent ('me'), or only ones you received ('them')."""
    conn = dbmod.open_index_db(read_only=True)
    cf, cp = _chat_filter(chat_ids, "chat_rowid")
    rows = conn.execute(
        f"""
        SELECT text, is_from_me FROM messages
        WHERE is_reaction = 0 AND text IS NOT NULL AND text != ''{cf}
        """,
        cp,
    )
    counts_total: dict[str, int] = {}
    counts_sent: dict[str, int] = {}
    for r in rows:
        text = r["text"]
        if not text or not any(ord(c) > 0x2600 for c in text):
            continue
        for ch in _EMOJI_RE.findall(text):
            counts_total[ch] = counts_total.get(ch, 0) + 1
            if r["is_from_me"]:
                counts_sent[ch] = counts_sent.get(ch, 0) + 1

    def _rank_key(item):
        emoji, total = item
        if mode == "me":   return -counts_sent.get(emoji, 0)
        if mode == "them": return -(total - counts_sent.get(emoji, 0))
        return -total

    items = sorted(counts_total.items(), key=_rank_key)
    # Drop entries that are zero in the chosen mode (e.g. emoji you've never sent).
    if mode == "me":
        items = [it for it in items if counts_sent.get(it[0], 0) > 0]
    elif mode == "them":
        items = [it for it in items if (it[1] - counts_sent.get(it[0], 0)) > 0]

    return {
        "items": [
            {
                "emoji": e,
                "codepoint": f"U+{ord(e):04X}",
                "uses": n,
                "sent": counts_sent.get(e, 0),
                "received": n - counts_sent.get(e, 0),
            }
            for e, n in items[offset:offset + limit]
        ],
        "total": len(items),
        "mode": mode,
    }


# Tapback reactions: associated_message_type 2000-2005 = added; 3000-3005 = removed.
_TAPBACKS = {
    2000: ("❤️", "Loved"),    2001: ("👍", "Liked"),     2002: ("👎", "Disliked"),
    2003: ("😂", "Laughed"),  2004: ("‼️", "Emphasized"), 2005: ("❓", "Questioned"),
}


def tapback_stats(chat_ids: list[int] | None = None) -> dict:
    """Counts of each tapback reaction type.

    Three buckets per reaction type:
      - sent     — tapbacks I added (to anyone's message)
      - received — tapbacks others added TO MY messages (the strict definition)
      - by_others — tapbacks between other people in group chats (not really mine)

    Naive `is_from_me=0` would lump received + by_others together, inflating
    "received" in group chats. We resolve `associated_message_guid` back to the
    original message and check its `is_from_me` to separate them. The original
    GUID has a `p:N/` or `bp:` prefix we have to strip for the join.
    """
    conn = dbmod.open_index_db(read_only=True)
    cf, cp = _chat_filter(chat_ids, "t.chat_rowid")
    rows = conn.execute(
        f"""
        SELECT
            t.associated_type AS type,
            t.is_from_me      AS tb_from_me,
            m.is_from_me      AS orig_from_me,
            COUNT(*)          AS n
        FROM messages t
        LEFT JOIN messages m ON m.guid = SUBSTR(
            t.associated_guid,
            CASE WHEN INSTR(t.associated_guid, '/') > 0
                 THEN INSTR(t.associated_guid, '/') + 1
                 ELSE 4 END   -- past the 'bp:' prefix
        )
        WHERE t.is_reaction = 1 AND t.associated_type BETWEEN 2000 AND 2005{cf}
        GROUP BY t.associated_type, t.is_from_me, m.is_from_me
        """,
        cp,
    ).fetchall()

    by_type: dict[int, dict] = {}
    for r in rows:
        d = by_type.setdefault(r["type"], {"sent": 0, "received": 0, "by_others": 0, "unknown": 0})
        n = r["n"]
        if r["tb_from_me"]:
            d["sent"] += n
        elif r["orig_from_me"] == 1:
            d["received"] += n
        elif r["orig_from_me"] == 0:
            d["by_others"] += n
        else:
            d["unknown"] += n        # original message not found (orphaned reaction)

    out = []
    for t, (emoji, label) in _TAPBACKS.items():
        d = by_type.get(t, {"sent": 0, "received": 0, "by_others": 0, "unknown": 0})
        total = d["sent"] + d["received"] + d["by_others"] + d["unknown"]
        out.append({
            "type": t, "emoji": emoji, "label": label,
            "uses": total,
            "sent": d["sent"],
            "received": d["received"],
            "by_others": d["by_others"],
            "unknown": d["unknown"],
        })
    out.sort(key=lambda x: -x["uses"])
    return {"items": out}
