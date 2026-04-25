"""Aggregate statistics over the derived index.

All queries hit `index.db` (read-only). Reactions are excluded from counts unless
explicitly asked for — a reaction is its own iMessage row but it's noise for any
"how active is this conversation" question.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from . import db as dbmod


def _q(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def overview(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(is_from_me) AS from_me,
            SUM(has_attachments) AS with_attachments,
            MIN(date_unix) AS first_unix,
            MAX(date_unix) AS last_unix
        FROM messages WHERE is_reaction = 0
        """
    ).fetchone()
    reactions = conn.execute("SELECT COUNT(*) AS n FROM messages WHERE is_reaction = 1").fetchone()["n"]
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
        "group_chats": chats["g"] or 0,
        "one_on_one_chats": (chats["n"] or 0) - (chats["g"] or 0),
        "total_handles": handles or 0,
    }


def by_month(conn: sqlite3.Connection) -> list[dict]:
    """Year-month timeline (one bucket per calendar month across the archive)."""
    return _q(conn, """
        SELECT strftime('%Y-%m', date_unix, 'unixepoch') AS bucket,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        GROUP BY bucket
        ORDER BY bucket
    """)


def by_year(conn: sqlite3.Connection) -> list[dict]:
    """One row per calendar year."""
    return _q(conn, """
        SELECT strftime('%Y', date_unix, 'unixepoch') AS bucket,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        GROUP BY bucket
        ORDER BY bucket
    """)


def by_month_of_year(conn: sqlite3.Connection) -> list[dict]:
    """Seasonality — sum across all years for each calendar month (Jan-Dec)."""
    rows = _q(conn, """
        SELECT CAST(strftime('%m', date_unix, 'unixepoch') AS INTEGER) AS m,
               COUNT(*) AS n,
               SUM(is_from_me) AS me
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        GROUP BY m ORDER BY m
    """)
    by_m = {r["m"]: r for r in rows}
    labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return [
        {"m": i, "label": labels[i-1], "n": (by_m.get(i) or {}).get("n", 0), "me": (by_m.get(i) or {}).get("me", 0)}
        for i in range(1, 13)
    ]


def by_hour(conn: sqlite3.Connection) -> list[dict]:
    rows = _q(conn, """
        SELECT CAST(strftime('%H', date_unix, 'unixepoch', 'localtime') AS INTEGER) AS h,
               COUNT(*) AS n
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        GROUP BY h
        ORDER BY h
    """)
    by_h = {r["h"]: r["n"] for r in rows}
    return [{"h": h, "n": by_h.get(h, 0)} for h in range(24)]


def by_weekday(conn: sqlite3.Connection) -> list[dict]:
    # SQLite: strftime('%w') -> 0=Sunday … 6=Saturday
    rows = _q(conn, """
        SELECT CAST(strftime('%w', date_unix, 'unixepoch', 'localtime') AS INTEGER) AS d,
               COUNT(*) AS n
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        GROUP BY d ORDER BY d
    """)
    by_d = {r["d"]: r["n"] for r in rows}
    labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    return [{"d": d, "label": labels[d], "n": by_d.get(d, 0)} for d in range(7)]


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
                 mode: str = "all") -> dict:
    """`mode` is 'all' (default), 'me' (sent), or 'them' (received)."""
    where = "is_reaction = 0 AND date_unix IS NOT NULL"
    if mode == "me":
        where += " AND is_from_me = 1"
    elif mode == "them":
        where += " AND is_from_me = 0"
    rows = _q(conn, f"""
        SELECT date(date_unix, 'unixepoch', 'localtime') AS day,
               COUNT(*) AS n,
               SUM(is_from_me) AS me,
               COUNT(*) - SUM(is_from_me) AS them
        FROM messages
        WHERE {where}
        GROUP BY day ORDER BY n DESC LIMIT ? OFFSET ?
    """, (limit, offset))
    total = conn.execute(f"SELECT COUNT(DISTINCT date(date_unix, 'unixepoch', 'localtime')) AS n FROM messages WHERE {where}").fetchone()["n"]
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


def longest_messages(conn: sqlite3.Connection, limit: int = 5, offset: int = 0) -> dict:
    rows = _q(conn, """
        SELECT m.rowid, length(m.text) AS len, m.text, m.date_unix,
               m.is_from_me,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        WHERE m.is_reaction = 0 AND m.text IS NOT NULL AND m.text != ''
        ORDER BY len DESC LIMIT ? OFFSET ?
    """, (limit, offset))
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM messages WHERE is_reaction = 0 AND text IS NOT NULL AND text != ''"
    ).fetchone()["n"]
    return {"items": rows, "total": total}


def first_messages(conn: sqlite3.Connection, limit: int = 5) -> list[dict]:
    """Earliest messages overall — fun nostalgia."""
    return _q(conn, """
        SELECT m.rowid, m.text, m.date_unix, m.is_from_me,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
               h.handle
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        WHERE m.is_reaction = 0 AND m.text != '' AND m.date_unix IS NOT NULL
        ORDER BY m.date_unix ASC LIMIT ?
    """, (limit,))


def streaks(conn: sqlite3.Connection) -> dict:
    """Single-day messaging streaks — longest consecutive run of days with any message."""
    days = _q(conn, """
        SELECT DISTINCT date(date_unix, 'unixepoch', 'localtime') AS day
        FROM messages
        WHERE is_reaction = 0 AND date_unix IS NOT NULL
        ORDER BY day
    """)
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


def all_stats() -> dict[str, Any]:
    conn = dbmod.open_index_db(read_only=True)
    return {
        "overview": overview(conn),
        "by_month": by_month(conn),
        "by_year": by_year(conn),
        "by_month_of_year": by_month_of_year(conn),
        "by_hour": by_hour(conn),
        "by_weekday": by_weekday(conn),
        "top_chats": top_chats(conn, limit=15),
        "busiest_days": busiest_days(conn, limit=10),
        "lopsided": lopsided(conn, limit=10),
        "longest_messages": longest_messages(conn, limit=3),
        "first_messages": first_messages(conn),
        "streaks": streaks(conn),
    }
