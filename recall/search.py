"""Query API over the derived index."""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable

from . import db as dbmod


@dataclass
class SearchHit:
    rowid: int
    guid: str | None
    chat_rowid: int | None
    chat_name: str | None        # group display_name OR derived from members
    chat_identifier: str | None
    is_group: bool
    handle: str | None
    sender_name: str | None      # resolved contact name for the sender's handle
    is_from_me: bool
    date_unix: float | None
    date_iso: str | None
    text: str
    snippet: str
    has_attachments: bool
    is_reaction: bool
    service: str | None
    attachments: list[dict[str, Any]] | None = None  # populated by conversation_window

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SearchResult:
    hits: list[SearchHit]
    total: int
    query: dict[str, Any]
    elapsed_ms: float
    facets: dict[str, Any] | None = None


# Strip characters that have meaning in FTS5 query syntax so user input is treated as
# literal terms. We support quoted phrases by detecting double quotes ourselves.
_FTS_RESERVED = re.compile(r'[():*"^]')


def _normalize_query(q: str) -> str:
    q = q.strip()
    if not q:
        return ""
    # Pull out quoted phrases verbatim.
    parts: list[str] = []
    for chunk in re.findall(r'"[^"]+"|\S+', q):
        if chunk.startswith('"') and chunk.endswith('"') and len(chunk) >= 2:
            inner = _FTS_RESERVED.sub(" ", chunk[1:-1]).strip()
            if inner:
                parts.append(f'"{inner}"')
        else:
            cleaned = _FTS_RESERVED.sub(" ", chunk).strip()
            if cleaned:
                # Prefix-match the last token-ish thing for as-you-type feel.
                parts.append(f"{cleaned}*")
    return " ".join(parts)


def _to_unix(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    # ISO-8601 date or datetime.
    s = str(value)
    try:
        if len(s) == 10:  # YYYY-MM-DD
            dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError as e:
        raise ValueError(f"could not parse date {value!r}: {e}") from e


def search(
    query: str | None = None,
    *,
    handle: str | None = None,
    handles: list[str] | None = None,   # multi-handle OR filter
    contact: str | None = None,         # match by resolved contact name (LIKE)
    chat_id: int | None = None,
    chat_identifier: str | None = None,
    since: str | float | None = None,
    until: str | float | None = None,
    is_from_me: bool | None = None,
    has_attachments: bool | None = None,
    is_group: bool | None = None,
    include_reactions: bool = False,    # tapbacks are noise by default
    with_facets: bool = False,          # also compute per-chat counts under same filters
    limit: int = 50,
    offset: int = 0,
    order: str = "relevance",  # "relevance" | "newest" | "oldest"
) -> SearchResult:
    """Run a search against the derived index. All filters are AND-ed together."""
    import time

    started = time.time()
    conn = dbmod.open_index_db(read_only=True)

    where: list[str] = []
    params: list[Any] = []
    joins: list[str] = []
    select_extras = ""
    order_sql = "m.date_unix DESC"

    fts_query = _normalize_query(query) if query else ""
    if fts_query:
        joins.append("JOIN messages_fts f ON f.rowid = m.rowid")
        where.append("messages_fts MATCH ?")
        params.append(fts_query)
        select_extras = ", snippet(messages_fts, 0, '<<', '>>', '…', 16) AS snippet, bm25(messages_fts) AS rank"
        if order == "relevance":
            order_sql = "rank ASC"
    else:
        select_extras = ", '' AS snippet"

    if order == "newest":
        order_sql = "m.date_unix DESC"
    elif order == "oldest":
        order_sql = "m.date_unix ASC"

    if handle:
        where.append("h.handle = ?")
        params.append(handle)
    if handles:
        # Virtual "__me__" handle = "messages from me" — combined OR-wise with
        # any real handles in the list so multi-select still feels like "any of".
        include_me = "__me__" in handles
        real = [h for h in handles if h != "__me__"]
        clauses = []
        if real:
            placeholders = ",".join("?" * len(real))
            clauses.append(f"h.handle IN ({placeholders})")
            params.extend(real)
        if include_me:
            clauses.append("m.is_from_me = 1")
        if clauses:
            where.append("(" + " OR ".join(clauses) + ")")
    if contact:
        where.append("(cn_sender.name LIKE ? OR c.resolved_name LIKE ?)")
        params.extend([f"%{contact}%", f"%{contact}%"])
    if chat_id is not None:
        where.append("m.chat_rowid = ?")
        params.append(chat_id)
    if chat_identifier:
        where.append("c.chat_identifier = ?")
        params.append(chat_identifier)

    since_u = _to_unix(since)
    until_u = _to_unix(until)
    if since_u is not None:
        where.append("m.date_unix >= ?")
        params.append(since_u)
    if until_u is not None:
        where.append("m.date_unix < ?")
        params.append(until_u)

    if is_from_me is not None:
        where.append("m.is_from_me = ?")
        params.append(1 if is_from_me else 0)
    if has_attachments is not None:
        where.append("m.has_attachments = ?")
        params.append(1 if has_attachments else 0)
    if is_group is not None:
        where.append("c.is_group = ?")
        params.append(1 if is_group else 0)
    if not include_reactions:
        where.append("m.is_reaction = 0")

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    join_sql = " ".join(joins)

    base_joins = """
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        LEFT JOIN contact_names cn_sender ON cn_sender.handle = h.handle
    """

    sql = f"""
        SELECT
            m.rowid, m.guid, m.chat_rowid, m.handle_rowid, m.is_from_me,
            m.date_unix, m.text, m.has_attachments, m.is_reaction, m.service,
            COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
            c.chat_identifier AS chat_identifier,
            c.is_group AS is_group,
            h.handle AS handle,
            cn_sender.name AS sender_name
            {select_extras}
        FROM messages m
        {join_sql}
        {base_joins}
        {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, [*params, limit, offset]).fetchall()

    count_sql = f"""
        SELECT COUNT(*) AS n
        FROM messages m
        {join_sql}
        {base_joins}
        {where_sql}
    """
    total = conn.execute(count_sql, params).fetchone()["n"]

    facets = None
    if with_facets:
        # Per-chat counts under the same filter set. The frontend rolls these
        # up into person bundles so it can refine sidebar counts in real time.
        facet_sql = f"""
            SELECT m.chat_rowid AS chat_id, COUNT(*) AS n
            FROM messages m
            {join_sql}
            {base_joins}
            {where_sql}
            GROUP BY m.chat_rowid
        """
        by_chat = {r["chat_id"]: r["n"] for r in conn.execute(facet_sql, params).fetchall() if r["chat_id"] is not None}
        facets = {"by_chat": by_chat}

    hits = [_row_to_hit(r) for r in rows]
    elapsed_ms = (time.time() - started) * 1000
    return SearchResult(
        hits=hits,
        total=total,
        facets=facets,
        query={
            "q": query,
            "handle": handle,
            "handles": handles,
            "contact": contact,
            "chat_id": chat_id,
            "chat_identifier": chat_identifier,
            "since": since,
            "until": until,
            "is_from_me": is_from_me,
            "has_attachments": has_attachments,
            "is_group": is_group,
            "include_reactions": include_reactions,
            "limit": limit,
            "offset": offset,
            "order": order,
        },
        elapsed_ms=round(elapsed_ms, 2),
    )


def _row_to_hit(r: sqlite3.Row) -> SearchHit:
    date_unix = r["date_unix"]
    iso = (
        datetime.fromtimestamp(date_unix, tz=timezone.utc).isoformat()
        if date_unix is not None
        else None
    )
    text = r["text"] or ""
    snippet = r["snippet"] if "snippet" in r.keys() else ""
    if not snippet:
        snippet = text[:200]
    keys = r.keys()
    return SearchHit(
        rowid=r["rowid"],
        guid=r["guid"] if "guid" in keys else None,
        chat_rowid=r["chat_rowid"],
        chat_name=r["chat_name"],
        chat_identifier=r["chat_identifier"],
        is_group=bool(r["is_group"]) if r["is_group"] is not None else False,
        handle=r["handle"],
        sender_name=r["sender_name"] if "sender_name" in keys else None,
        is_from_me=bool(r["is_from_me"]),
        date_unix=date_unix,
        date_iso=iso,
        text=text,
        snippet=snippet,
        has_attachments=bool(r["has_attachments"]),
        is_reaction=bool(r["is_reaction"]) if "is_reaction" in keys else False,
        service=r["service"] if "service" in keys else None,
    )


def attachments_for(message_rowids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """Bulk-fetch attachment metadata for a list of message rowids.

    Reads from the source `chat.db` (the index doesn't store attachment rows).
    Returns {message_rowid: [attachments…]}. Each attachment carries `on_disk`
    so the UI can show a "pull from iCloud" prompt without first 404-ing.
    """
    if not message_rowids:
        return {}
    import os as _os

    src = dbmod.open_chat_db()
    placeholders = ",".join("?" * len(message_rowids))
    rows = src.execute(
        f"""
        SELECT
            maj.message_id AS message_rowid,
            a.ROWID        AS att_rowid,
            a.filename     AS filename,
            a.mime_type    AS mime_type,
            a.transfer_name AS transfer_name,
            a.total_bytes  AS total_bytes,
            a.is_sticker   AS is_sticker,
            a.uti          AS uti
        FROM message_attachment_join maj
        JOIN attachment a ON a.ROWID = maj.attachment_id
        WHERE maj.message_id IN ({placeholders})
        ORDER BY maj.message_id, a.ROWID
        """,
        message_rowids,
    ).fetchall()
    out: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        path = _os.path.expanduser(r["filename"] or "")
        on_disk = bool(path) and _os.path.isfile(path)
        out.setdefault(r["message_rowid"], []).append({
            "att_rowid": r["att_rowid"],
            "filename": r["filename"],
            "mime_type": r["mime_type"],
            "transfer_name": r["transfer_name"],
            "total_bytes": r["total_bytes"],
            "is_sticker": bool(r["is_sticker"]),
            "uti": r["uti"],
            "on_disk": on_disk,
        })
    return out


def attachment_path(att_rowid: int) -> tuple[str, str | None, bool] | None:
    """Look up the on-disk path + mime_type + is_sticker for an attachment ROWID."""
    src = dbmod.open_chat_db()
    row = src.execute(
        "SELECT filename, mime_type, is_sticker FROM attachment WHERE ROWID = ?",
        (att_rowid,),
    ).fetchone()
    if not row or not row["filename"]:
        return None
    return row["filename"], row["mime_type"], bool(row["is_sticker"])


def conversation_window(rowid: int, *, before: int = 5, after: int = 5) -> list[SearchHit]:
    """Return messages around a given message in its chat for context."""
    conn = dbmod.open_index_db(read_only=True)
    row = conn.execute(
        "SELECT chat_rowid, date_unix FROM messages WHERE rowid = ?", (rowid,)
    ).fetchone()
    if row is None or row["chat_rowid"] is None:
        return []
    chat_rowid, date_unix = row["chat_rowid"], row["date_unix"]

    before_rows = conn.execute(
        """
        SELECT m.*,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
               c.chat_identifier, c.is_group,
               h.handle AS handle,
               cn.name AS sender_name,
               '' AS snippet
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        WHERE m.chat_rowid = ? AND m.date_unix < ?
        ORDER BY m.date_unix DESC LIMIT ?
        """,
        (chat_rowid, date_unix, before),
    ).fetchall()
    after_rows = conn.execute(
        """
        SELECT m.*,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
               c.chat_identifier, c.is_group,
               h.handle AS handle,
               cn.name AS sender_name,
               '' AS snippet
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        WHERE m.chat_rowid = ? AND m.date_unix >= ? AND m.rowid != ?
        ORDER BY m.date_unix ASC LIMIT ?
        """,
        (chat_rowid, date_unix, rowid, after),
    ).fetchall()

    target = conn.execute(
        """
        SELECT m.*,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS chat_name,
               c.chat_identifier, c.is_group,
               h.handle AS handle,
               cn.name AS sender_name,
               '' AS snippet
        FROM messages m
        LEFT JOIN chats c ON c.rowid = m.chat_rowid
        LEFT JOIN handles h ON h.rowid = m.handle_rowid
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        WHERE m.rowid = ?
        """,
        (rowid,),
    ).fetchone()

    rows: Iterable[sqlite3.Row] = [
        *reversed(list(before_rows)),
        target,
        *after_rows,
    ]
    hits = [_row_to_hit(r) for r in rows if r is not None]
    # Bulk-load attachments for any hit that has them.
    attach_ids = [h.rowid for h in hits if h.has_attachments]
    if attach_ids:
        att_map = attachments_for(attach_ids)
        for h in hits:
            if h.has_attachments:
                h.attachments = att_map.get(h.rowid, [])
    return hits


def list_chats(limit: int = 200) -> list[dict[str, Any]]:
    conn = dbmod.open_index_db(read_only=True)
    rows = conn.execute(
        """
        SELECT c.rowid, c.guid, c.chat_identifier, c.display_name, c.is_group,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS name,
               COUNT(m.rowid) AS message_count,
               MAX(m.date_unix) AS last_message_unix
        FROM chats c
        LEFT JOIN messages m ON m.chat_rowid = c.rowid AND m.is_reaction = 0
        GROUP BY c.rowid
        ORDER BY last_message_unix DESC NULLS LAST
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def list_people(limit: int = 500) -> list[dict[str, Any]]:
    """Like list_chats, but merges 1:1 chats that resolve to the same contact name.

    A "person" bundle has one or more underlying `chat_rowid`s (e.g. one for the
    contact's phone number, one for their email). Group chats stay as one entry
    each. Returned shape is uniform so the frontend can treat people and groups
    the same way.
    """
    conn = dbmod.open_index_db(read_only=True)

    # 1:1 chats: group by resolved name (fall back to chat_identifier — that
    # keeps still-unknown numbers from collapsing together as a giant 'None'
    # bucket).
    rows = conn.execute(
        """
        SELECT
            COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS name,
            group_concat(c.rowid) AS chat_ids,
            group_concat(c.chat_identifier, '||') AS identifiers,
            COUNT(*) AS chat_count,
            SUM(mc.n) AS message_count,
            MAX(mc.last_unix) AS last_message_unix
        FROM chats c
        LEFT JOIN (
            SELECT chat_rowid, COUNT(*) AS n, MAX(date_unix) AS last_unix
            FROM messages WHERE is_reaction = 0 GROUP BY chat_rowid
        ) mc ON mc.chat_rowid = c.rowid
        WHERE c.is_group = 0
        GROUP BY name
        """
    ).fetchall()

    people = []
    for r in rows:
        ids = [int(x) for x in (r["chat_ids"] or "").split(",") if x]
        idents = [x for x in (r["identifiers"] or "").split("||") if x]
        people.append({
            "name": r["name"],
            "is_group": False,
            "chat_ids": ids,
            "identifiers": idents,
            "merged_count": r["chat_count"],
            "message_count": r["message_count"] or 0,
            "last_message_unix": r["last_message_unix"],
        })

    # Group chats: one entry each.
    groups = conn.execute(
        """
        SELECT c.rowid,
               COALESCE(c.resolved_name, c.display_name, c.chat_identifier) AS name,
               c.chat_identifier,
               COUNT(m.rowid) AS message_count,
               MAX(m.date_unix) AS last_message_unix
        FROM chats c
        LEFT JOIN messages m ON m.chat_rowid = c.rowid AND m.is_reaction = 0
        WHERE c.is_group = 1
        GROUP BY c.rowid
        """
    ).fetchall()
    for g in groups:
        people.append({
            "name": g["name"],
            "is_group": True,
            "chat_ids": [g["rowid"]],
            "identifiers": [g["chat_identifier"]],
            "merged_count": 1,
            "message_count": g["message_count"] or 0,
            "last_message_unix": g["last_message_unix"],
        })

    people.sort(key=lambda p: -(p["message_count"] or 0))
    return people[:limit]


def chat_members(chat_ids: list[int]) -> list[dict[str, Any]]:
    """For one or more chat rowids, return the participating handles + their
    contact-resolved name + how many messages they sent in those chats.

    "You" (is_from_me) is added as a synthetic entry keyed by `me`.
    """
    if not chat_ids:
        return []
    conn = dbmod.open_index_db(read_only=True)
    placeholders = ",".join("?" * len(chat_ids))
    rows = conn.execute(
        f"""
        SELECT h.handle AS handle,
               cn.name  AS contact_name,
               COUNT(m.rowid) AS message_count
        FROM messages m
        JOIN handles h        ON h.rowid = m.handle_rowid
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        WHERE m.chat_rowid IN ({placeholders})
          AND m.is_reaction = 0
          AND m.is_from_me = 0
        GROUP BY h.handle
        ORDER BY message_count DESC
        """,
        chat_ids,
    ).fetchall()
    me_count = conn.execute(
        f"SELECT COUNT(*) AS n FROM messages WHERE chat_rowid IN ({placeholders}) "
        f"AND is_reaction = 0 AND is_from_me = 1",
        chat_ids,
    ).fetchone()["n"]

    out = []
    if me_count:
        out.append({"handle": "__me__", "contact_name": "You", "message_count": me_count, "is_me": True})
    for r in rows:
        out.append({
            "handle": r["handle"],
            "contact_name": r["contact_name"] or r["handle"],
            "message_count": r["message_count"],
            "is_me": False,
        })
    return out


def find_handles(needle: str, limit: int = 30) -> list[dict[str, Any]]:
    """Lookup-by-contact-info for the People sidebar when no chats are picked.

    Matches handle string OR resolved contact name (case-insensitive substring).
    """
    needle = (needle or "").strip()
    if not needle:
        return []
    conn = dbmod.open_index_db(read_only=True)
    like = f"%{needle.lower()}%"
    rows = conn.execute(
        """
        SELECT h.handle AS handle,
               cn.name  AS contact_name,
               COUNT(m.rowid) AS message_count
        FROM handles h
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        LEFT JOIN messages m ON m.handle_rowid = h.rowid AND m.is_reaction = 0
        WHERE LOWER(h.handle) LIKE ? OR LOWER(COALESCE(cn.name, '')) LIKE ?
        GROUP BY h.handle
        ORDER BY message_count DESC
        LIMIT ?
        """,
        (like, like, limit),
    ).fetchall()
    return [
        {
            "handle": r["handle"],
            "contact_name": r["contact_name"] or r["handle"],
            "message_count": r["message_count"],
            "is_me": False,
        }
        for r in rows
    ]


def list_handles(limit: int = 500) -> list[dict[str, Any]]:
    conn = dbmod.open_index_db(read_only=True)
    rows = conn.execute(
        """
        SELECT h.rowid, h.handle, h.service, h.country,
               cn.name AS contact_name,
               COUNT(m.rowid) AS message_count,
               MAX(m.date_unix) AS last_message_unix
        FROM handles h
        LEFT JOIN messages m ON m.handle_rowid = h.rowid
        LEFT JOIN contact_names cn ON cn.handle = h.handle
        GROUP BY h.rowid
        ORDER BY message_count DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
