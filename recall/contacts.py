"""Read macOS Contacts.app to map phone/email handles → real names.

Contacts live in one or more SQLite databases under
`~/Library/Application Support/AddressBook/Sources/<UUID>/AddressBook-v22.abcddb`
(one per source: local "On My Mac", iCloud, Google, Exchange, …). We scan all of
them and merge.

Phone numbers are normalized to E.164-ish (digits only, leading `+`) so they line
up with iMessage's `handle.id` format. We also keep a "national" digits-only
variant so 10-digit US contacts still match `+1XXXXXXXXXX` handles.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

ADDRESSBOOK_ROOT = Path.home() / "Library/Application Support/AddressBook/Sources"


def find_address_books(root: Path = ADDRESSBOOK_ROOT) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.glob("*/AddressBook-v22.abcddb"))


_NON_DIGIT = re.compile(r"\D+")


def normalize_phone(raw: str | None) -> str | None:
    """Return an E.164-ish key (`+15555550123`). None if the input has no digits."""
    if not raw:
        return None
    digits = _NON_DIGIT.sub("", raw)
    if not digits:
        return None
    # If the user stored "(555) 555-0123", assume US (+1).
    if len(digits) == 10:
        digits = "1" + digits
    return "+" + digits


def _full_name(first: str | None, last: str | None, nickname: str | None, org: str | None) -> str:
    parts = [p for p in (first, last) if p]
    name = " ".join(parts).strip()
    if not name:
        name = (nickname or org or "").strip()
    return name


def _read_source(db_path: Path) -> Iterable[tuple[str, str]]:
    """Yield (handle_key, name) pairs from one AddressBook source."""
    uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        people = conn.execute(
            "SELECT Z_PK, ZFIRSTNAME, ZLASTNAME, ZNICKNAME, ZORGANIZATION FROM ZABCDRECORD"
        ).fetchall()
        names: dict[int, str] = {}
        for r in people:
            n = _full_name(r["ZFIRSTNAME"], r["ZLASTNAME"], r["ZNICKNAME"], r["ZORGANIZATION"])
            if n:
                names[r["Z_PK"]] = n

        for r in conn.execute("SELECT ZOWNER, ZFULLNUMBER FROM ZABCDPHONENUMBER"):
            name = names.get(r["ZOWNER"])
            phone = normalize_phone(r["ZFULLNUMBER"])
            if name and phone:
                yield phone, name

        for r in conn.execute("SELECT ZOWNER, ZADDRESS FROM ZABCDEMAILADDRESS"):
            name = names.get(r["ZOWNER"])
            email = (r["ZADDRESS"] or "").strip().lower()
            if name and email:
                yield email, name
    finally:
        conn.close()


def load_contacts() -> dict[str, str]:
    """Merge all address books → {handle_key: name}. Last-write wins (iCloud usually)."""
    merged: dict[str, str] = {}
    for db in find_address_books():
        try:
            for key, name in _read_source(db):
                merged[key] = name
        except sqlite3.DatabaseError as e:
            print(f"warning: skipping {db}: {e}", file=sys.stderr)
    return merged


def handle_lookup_keys(handle: str) -> list[str]:
    """Return the keys we should try when looking up an iMessage handle in contacts."""
    handle = (handle or "").strip()
    if not handle:
        return []
    if "@" in handle:
        return [handle.lower()]
    # Phone — normalize to E.164. Also try the raw form for non-NANP exotica.
    norm = normalize_phone(handle)
    keys = []
    if norm:
        keys.append(norm)
    if handle not in keys:
        keys.append(handle)
    return keys


def resolve_handle(handle: str, contacts: dict[str, str]) -> str | None:
    for key in handle_lookup_keys(handle):
        if key in contacts:
            return contacts[key]
    return None
