"""Decoder for the NSKeyedArchiver `attributedBody` blob.

On macOS Ventura+ the `message.text` column is often NULL — the body lives in
`attributedBody` as an NSAttributedString serialized via Apple's typedstream
(`streamtyped`) format.

This is a pragmatic extractor, not a full typedstream parser. It locates the
`NSString` class marker, walks past the typedstream metadata, then reads the
length-prefixed UTF-8 payload. Works for >99% of normal messages. For attachments,
URL previews, or rich content we may return only the visible text (which is what
we want for search).
"""
from __future__ import annotations


def decode_attributed_body(blob: bytes | memoryview | None) -> str | None:
    """Extract the visible text from an `attributedBody` blob.

    Returns None if the blob is empty, malformed, or contains no string payload.
    """
    if not blob:
        return None
    if isinstance(blob, memoryview):
        blob = bytes(blob)

    # The body always starts with the typedstream signature `streamtyped`.
    if b"streamtyped" not in blob[:32]:
        return None

    # The first NSString in the archive is the message text.
    marker = b"NSString"
    idx = blob.find(marker)
    if idx == -1:
        return None

    # After NSString comes class version/metadata, then a `+` (0x2B) which signals
    # "C string follows", then a length-prefixed UTF-8 payload.
    plus = blob.find(b"+", idx + len(marker))
    if plus == -1 or plus + 1 >= len(blob):
        return None

    p = plus + 1
    first = blob[p]
    if first == 0x81:  # uint16 length follows
        if p + 3 > len(blob):
            return None
        length = int.from_bytes(blob[p + 1 : p + 3], "little")
        start = p + 3
    elif first == 0x82:  # uint32 length follows
        if p + 5 > len(blob):
            return None
        length = int.from_bytes(blob[p + 1 : p + 5], "little")
        start = p + 5
    elif first == 0x83:  # uint64 length follows (rare)
        if p + 9 > len(blob):
            return None
        length = int.from_bytes(blob[p + 1 : p + 9], "little")
        start = p + 9
    else:  # single-byte length
        length = first
        start = p + 1

    if length <= 0 or start + length > len(blob):
        return None

    try:
        return blob[start : start + length].decode("utf-8")
    except UnicodeDecodeError:
        # Fall back to lossy decode rather than dropping the message entirely.
        return blob[start : start + length].decode("utf-8", errors="replace")


def message_text(text_col: str | None, attributed_body: bytes | None) -> str | None:
    """Return the best available text for a message row."""
    if text_col:
        return text_col
    return decode_attributed_body(attributed_body)
