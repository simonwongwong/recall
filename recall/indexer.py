"""Build a derived search index from `chat.db`.

The output (`index.db`) has a flat `messages` table with the fields we care about
and an FTS5 virtual table over the message body. Rebuilding is incremental — we
remember the highest source ROWID we've seen and only pull newer rows.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable

from . import db as dbmod
from .typedstream import message_text

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS handles (
    rowid INTEGER PRIMARY KEY,
    handle TEXT NOT NULL,
    service TEXT,
    country TEXT
);
CREATE INDEX IF NOT EXISTS idx_handles_handle ON handles(handle);

CREATE TABLE IF NOT EXISTS chats (
    rowid INTEGER PRIMARY KEY,
    guid TEXT,
    chat_identifier TEXT,
    display_name TEXT,
    service_name TEXT,
    style INTEGER,
    is_group INTEGER NOT NULL DEFAULT 0,
    resolved_name TEXT  -- display_name if set, else derived from contact members
);

CREATE TABLE IF NOT EXISTS chat_handles (
    chat_rowid INTEGER NOT NULL,
    handle_rowid INTEGER NOT NULL,
    PRIMARY KEY (chat_rowid, handle_rowid)
);

CREATE TABLE IF NOT EXISTS messages (
    rowid INTEGER PRIMARY KEY,
    guid TEXT,
    chat_rowid INTEGER,
    handle_rowid INTEGER,
    is_from_me INTEGER NOT NULL DEFAULT 0,
    date_unix REAL,
    text TEXT,
    has_attachments INTEGER NOT NULL DEFAULT 0,
    is_reply INTEGER NOT NULL DEFAULT 0,
    is_reaction INTEGER NOT NULL DEFAULT 0,
    associated_type INTEGER,
    associated_guid TEXT,
    service TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_rowid, date_unix);
CREATE INDEX IF NOT EXISTS idx_messages_handle ON messages(handle_rowid, date_unix);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date_unix);
CREATE INDEX IF NOT EXISTS idx_messages_reaction ON messages(is_reaction);

-- Standalone FTS5 (not external-content). Slightly more storage but simpler updates.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    text,
    tokenize='unicode61 remove_diacritics 2'
);

-- Resolved contact names per handle string. Rebuilt fully on every contact sync.
CREATE TABLE IF NOT EXISTS contact_names (
    handle TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
"""


def init_index(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _last_indexed_rowid(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key='last_message_rowid'").fetchone()
    return int(row["value"]) if row else 0


def _set_last_indexed_rowid(conn: sqlite3.Connection, rowid: int) -> None:
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('last_message_rowid', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(rowid),),
    )


def _sync_handles(src: sqlite3.Connection, dst: sqlite3.Connection) -> None:
    rows = src.execute("SELECT ROWID as rowid, id, service, country FROM handle").fetchall()
    dst.executemany(
        "INSERT OR REPLACE INTO handles(rowid, handle, service, country) VALUES (?,?,?,?)",
        [(r["rowid"], r["id"], r["service"], r["country"]) for r in rows],
    )


def _sync_chats(src: sqlite3.Connection, dst: sqlite3.Connection) -> None:
    rows = src.execute(
        "SELECT ROWID as rowid, guid, chat_identifier, display_name, service_name, style "
        "FROM chat"
    ).fetchall()
    # style 43 = group, 45 = 1:1 (per Apple's enum); treat anything but 45 as group-ish.
    dst.executemany(
        "INSERT OR REPLACE INTO chats(rowid, guid, chat_identifier, display_name, "
        "service_name, style, is_group) VALUES (?,?,?,?,?,?,?)",
        [
            (
                r["rowid"],
                r["guid"],
                r["chat_identifier"],
                r["display_name"],
                r["service_name"],
                r["style"],
                1 if (r["style"] is not None and r["style"] != 45) else 0,
            )
            for r in rows
        ],
    )

    dst.execute("DELETE FROM chat_handles")
    join_rows = src.execute(
        "SELECT chat_id, handle_id FROM chat_handle_join"
    ).fetchall()
    dst.executemany(
        "INSERT OR IGNORE INTO chat_handles(chat_rowid, handle_rowid) VALUES (?,?)",
        [(r["chat_id"], r["handle_id"]) for r in join_rows],
    )


def _iter_message_rows(src: sqlite3.Connection, since_rowid: int) -> Iterable[sqlite3.Row]:
    # LEFT JOIN chat_message_join because some messages aren't yet in a chat.
    sql = """
        SELECT
            m.ROWID            AS rowid,
            m.guid             AS guid,
            m.text             AS text,
            m.attributedBody   AS attributed_body,
            m.handle_id        AS handle_rowid,
            m.is_from_me       AS is_from_me,
            m.date             AS date,
            m.cache_has_attachments AS has_attachments,
            m.associated_message_guid AS assoc_guid,
            m.associated_message_type AS assoc_type,
            m.thread_originator_guid AS thread_guid,
            m.service          AS service,
            cmj.chat_id        AS chat_rowid
        FROM message m
        LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        WHERE m.ROWID > ?
        ORDER BY m.ROWID
    """
    return src.execute(sql, (since_rowid,))


def index_messages(*, batch: int = 5000, verbose: bool = True) -> dict:
    """Sync `chat.db` → `index.db`. Returns stats."""
    started = time.time()
    src = dbmod.open_chat_db()
    dst = dbmod.open_index_db()
    init_index(dst)

    if verbose:
        print("→ syncing handles and chats…", file=sys.stderr)
    with dst:
        _sync_handles(src, dst)
        _sync_chats(src, dst)

    last = _last_indexed_rowid(dst)
    if verbose:
        print(f"→ indexing messages from ROWID > {last}…", file=sys.stderr)

    inserted = 0
    skipped = 0
    rows: list[tuple] = []
    fts_rows: list[tuple] = []
    max_rowid = last

    for r in _iter_message_rows(src, last):
        rowid = r["rowid"]
        max_rowid = max(max_rowid, rowid)
        text = message_text(r["text"], r["attributed_body"])
        if text is None:
            skipped += 1
            text = ""  # keep the row so reply context still lines up
        date_unix = dbmod.mac_ts_to_unix(r["date"])
        is_reply = 1 if r["thread_guid"] else 0
        # iMessage tapbacks: 2000-2005 add a reaction, 3000-3005 remove one.
        # Sticker overlays use 1000s. We treat anything >= 1000 as "noise".
        assoc_type = r["assoc_type"]
        is_reaction = 1 if (assoc_type and assoc_type >= 1000) else 0
        rows.append(
            (
                rowid,
                r["guid"],
                r["chat_rowid"],
                r["handle_rowid"],
                int(r["is_from_me"] or 0),
                date_unix,
                text,
                int(r["has_attachments"] or 0),
                is_reply,
                is_reaction,
                assoc_type,
                r["assoc_guid"],
                r["service"],
            )
        )
        fts_rows.append((rowid, text))

        if len(rows) >= batch:
            inserted += _flush(dst, rows, fts_rows)
            rows.clear()
            fts_rows.clear()
            if verbose:
                print(f"  …{inserted} rows ({rowid})", file=sys.stderr)

    if rows:
        inserted += _flush(dst, rows, fts_rows)

    with dst:
        _set_last_indexed_rowid(dst, max_rowid)

    # Refresh stats so the query planner picks the right indexes for FTS+JOIN paths.
    dst.execute("ANALYZE")
    dst.commit()

    elapsed = time.time() - started
    stats = {
        "inserted": inserted,
        "skipped_empty": skipped,
        "max_rowid": max_rowid,
        "elapsed_sec": round(elapsed, 2),
    }
    if verbose:
        print(f"✔ done: {stats}", file=sys.stderr)
    return stats


def _flush(
    dst: sqlite3.Connection,
    rows: list[tuple],
    fts_rows: list[tuple],
) -> int:
    with dst:
        dst.executemany(
            "INSERT OR REPLACE INTO messages "
            "(rowid, guid, chat_rowid, handle_rowid, is_from_me, date_unix, text, "
            " has_attachments, is_reply, is_reaction, associated_type, associated_guid, "
            " service) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        # Sync FTS: delete-then-insert so re-runs don't duplicate.
        dst.executemany("DELETE FROM messages_fts WHERE rowid = ?", [(r[0],) for r in fts_rows])
        dst.executemany("INSERT INTO messages_fts(rowid, text) VALUES (?,?)", fts_rows)
    return len(rows)


def reset_index(path: Path | None = None) -> None:
    """Delete the derived index — next run will rebuild from scratch.

    Respects `$RECALL_INDEX_DB` so demo mode doesn't accidentally wipe
    `data/index.db`.
    """
    import os as _os
    if path is None:
        env = _os.environ.get("RECALL_INDEX_DB")
        path = Path(env).expanduser().resolve() if env else dbmod.INDEX_DB
    if path.exists():
        path.unlink()


def sync_contacts(verbose: bool = True) -> dict:
    """Reload macOS Contacts → `contact_names` table, then refresh chat names.

    For each handle we store both the raw form and a normalized-phone form so the
    SQL JOIN can match either. Then we precompute `chats.resolved_name` so the
    search query stays cheap.
    """
    from . import contacts as contacts_mod

    pairs = contacts_mod.load_contacts()  # already keyed by E.164/email
    dst = dbmod.open_index_db()
    init_index(dst)
    with dst:
        dst.execute("DELETE FROM contact_names")
        dst.executemany(
            "INSERT OR REPLACE INTO contact_names(handle, name) VALUES (?,?)",
            pairs.items(),
        )
        # Also add an entry for any iMessage handle whose normalized form maps to
        # a contact, keyed by the original handle string. This avoids needing
        # phone-normalization in the query path.
        for hrow in dst.execute("SELECT rowid, handle FROM handles").fetchall():
            raw = hrow["handle"] or ""
            if not raw or raw in pairs:
                continue
            for key in contacts_mod.handle_lookup_keys(raw):
                if key in pairs:
                    dst.execute(
                        "INSERT OR REPLACE INTO contact_names(handle, name) VALUES (?,?)",
                        (raw, pairs[key]),
                    )
                    break

        _refresh_chat_resolved_names(dst)

    stats = {"contacts_loaded": len(pairs)}
    if verbose:
        print(f"✔ contacts synced: {stats}", file=sys.stderr)
    return stats


def _refresh_chat_resolved_names(dst: sqlite3.Connection) -> None:
    """Populate `chats.resolved_name` with a human-readable label for each chat."""
    # 1:1 → contact name for the sole other handle, else chat_identifier.
    # Group with display_name → keep it.
    # Group without display_name → comma-join of member contact first names (max 4).
    rows = dst.execute(
        """
        SELECT c.rowid, c.chat_identifier, c.display_name, c.is_group
        FROM chats c
        """
    ).fetchall()

    for c in rows:
        if c["display_name"]:
            name = c["display_name"]
        else:
            members = dst.execute(
                """
                SELECT COALESCE(cn.name, h.handle) AS label
                FROM chat_handles ch
                JOIN handles h ON h.rowid = ch.handle_rowid
                LEFT JOIN contact_names cn ON cn.handle = h.handle
                WHERE ch.chat_rowid = ?
                ORDER BY (cn.name IS NULL), label
                """,
                (c["rowid"],),
            ).fetchall()
            labels = [m["label"] for m in members]
            if not labels:
                name = c["chat_identifier"] or "?"
            elif c["is_group"]:
                # First names only, capped — keeps the UI readable.
                firsts = [lbl.split(" ", 1)[0] for lbl in labels[:4]]
                more = len(labels) - len(firsts)
                name = ", ".join(firsts) + (f" +{more}" if more > 0 else "")
            else:
                name = labels[0]
        dst.execute(
            "UPDATE chats SET resolved_name = ? WHERE rowid = ?", (name, c["rowid"])
        )
