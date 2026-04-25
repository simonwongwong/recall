"""SQLite connection helpers."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
LIVE_CHAT_DB = Path.home() / "Library/Messages/chat.db"
SNAPSHOT_CHAT_DB = DATA_DIR / "chat.db"  # legacy snapshot path; used as fallback
INDEX_DB = DATA_DIR / "index.db"

# Mac absolute time epoch offset: seconds between 1970-01-01 and 2001-01-01 UTC.
MAC_EPOCH_OFFSET = 978307200


def open_chat_db(path: Path | None = None) -> sqlite3.Connection:
    """Open the iMessage database read-only.

    Resolution order:
      1. Explicit `path` argument
      2. `$RECALL_CHAT_DB` env var (used for demo mode)
      3. Live `~/Library/Messages/chat.db` (the default)
      4. Snapshot `data/chat.db` (legacy fallback)

    Opened with `mode=ro` (no `immutable`) so SQLite handles concurrent writes
    from Messages.app cleanly via WAL.
    """
    if path is None:
        env = os.environ.get("RECALL_CHAT_DB")
        if env:
            path = Path(env).expanduser().resolve()
        elif LIVE_CHAT_DB.exists() and os.access(LIVE_CHAT_DB, os.R_OK):
            path = LIVE_CHAT_DB
        elif SNAPSHOT_CHAT_DB.exists():
            path = SNAPSHOT_CHAT_DB
        else:
            raise FileNotFoundError(
                f"chat.db not found at {LIVE_CHAT_DB} or {SNAPSHOT_CHAT_DB}. "
                f"Grant Full Disk Access, set $RECALL_CHAT_DB, or copy chat.db to data/."
            )
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def open_index_db(path: Path | None = None, *, read_only: bool = False) -> sqlite3.Connection:
    """Open (or create) the derived search index. `$RECALL_INDEX_DB` overrides default."""
    if path is None:
        env = os.environ.get("RECALL_INDEX_DB")
        path = Path(env).expanduser().resolve() if env else INDEX_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    if read_only:
        if not path.exists():
            raise FileNotFoundError(f"index not built — run `python3 -m recall.cli index` first.")
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def mac_ts_to_unix(ns: int | None) -> float | None:
    """Convert Mac absolute time (ns since 2001) to Unix seconds.

    Older messages used seconds, newer ones nanoseconds. Detect by magnitude.
    """
    if ns is None or ns == 0:
        return None
    if ns > 1_000_000_000_000:  # nanoseconds
        return ns / 1e9 + MAC_EPOCH_OFFSET
    return ns + MAC_EPOCH_OFFSET
