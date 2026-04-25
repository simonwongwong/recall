"""Minimal JSON HTTP server for the frontend.

Stdlib only — no FastAPI dependency. CORS is wide open since this only listens
on localhost.

Endpoints:
  GET /health
  GET /search?q=…&handle=…&since=…&until=…&chat_id=…&is_from_me=…&has_attachments=…&is_group=…&limit=…&offset=…&order=…
  GET /messages/<rowid>/context?before=5&after=5
  GET /chats
  GET /handles
  POST /reindex
"""
from __future__ import annotations

import json
import mimetypes
import os
import platform
import subprocess
import sys
import tempfile
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import db as dbmod, indexer, search, stats as stats_mod

WEB_ROOT = dbmod.PROJECT_ROOT / "web"


def _capabilities() -> dict:
    """What modes/operations does this instance support?

    `full` mode (Mac with chat.db live) supports reindex, attachments,
    Open-in-Messages. `index-only` mode (server reading a synced index.db)
    supports search + stats only.
    """
    chat_path = dbmod.chat_db_path()
    index_path = dbmod.index_db_path()
    index_built = None
    if index_path.exists():
        index_built = datetime.fromtimestamp(
            index_path.stat().st_mtime, tz=timezone.utc
        ).isoformat()
    attachments_dir = Path.home() / "Library/Messages/Attachments"
    return {
        "mode": "full" if chat_path else "index-only",
        "chat_db": str(chat_path) if chat_path else None,
        "attachments": chat_path is not None and attachments_dir.exists(),
        "open_chat": platform.system() == "Darwin",
        "reindex": chat_path is not None,
        "index_db": str(index_path) if index_path.exists() else None,
        "index_built_at": index_built,
    }


def _bool(v: str | None) -> bool | None:
    if v is None:
        return None
    return v.lower() in ("1", "true", "yes", "y")


def _int(v: str | None) -> int | None:
    return int(v) if v not in (None, "") else None


def _csv_ints(v: str | None) -> list[int] | None:
    if not v:
        return None
    out = [int(x) for x in v.split(",") if x.strip().lstrip("-").isdigit()]
    return out or None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quieter log
        sys.stderr.write(f"[{self.log_date_time_string()}] {fmt % args}\n")

    def _send_json(self, status: int, body: object) -> None:
        payload = json.dumps(body, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self):  # noqa: N802
        self._send_json(204, {})

    def do_GET(self):  # noqa: N802
        try:
            self._dispatch_get()
        except Exception as e:
            traceback.print_exc()
            self._send_json(500, {"error": str(e)})

    def do_POST(self):  # noqa: N802
        try:
            url = urlparse(self.path)
            caps = _capabilities()
            if url.path == "/reindex":
                if not caps["reindex"]:
                    self._send_json(503, {"error": "reindex unavailable in index-only mode"})
                    return
                stats = indexer.index_messages(verbose=False)
                stats.update(indexer.sync_contacts(verbose=False))
                self._send_json(200, stats)
                return
            if url.path == "/contacts/sync":
                if not caps["reindex"]:
                    self._send_json(503, {"error": "contacts sync unavailable in index-only mode"})
                    return
                self._send_json(200, indexer.sync_contacts(verbose=False))
                return
            if url.path == "/open-chat":
                if not caps["open_chat"]:
                    self._send_json(503, {"error": "open-in-Messages only available on the Mac"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
                self._open_chat(body)
                return
            self._send_json(404, {"error": "not found"})
        except Exception as e:
            traceback.print_exc()
            self._send_json(500, {"error": str(e)})

    def _dispatch_get(self) -> None:
        url = urlparse(self.path)
        qs = {k: v[0] for k, v in parse_qs(url.query).items()}

        if url.path == "/health":
            self._send_json(200, {"ok": True})
            return

        if url.path == "/capabilities":
            self._send_json(200, _capabilities())
            return

        if url.path == "/search":
            handles_csv = qs.get("handles") or ""
            handles_list = [h for h in handles_csv.split(",") if h] or None
            result = search.search(
                qs.get("q") or None,
                handle=qs.get("handle") or None,
                handles=handles_list,
                contact=qs.get("contact") or None,
                chat_id=_int(qs.get("chat_id")),
                chat_identifier=qs.get("chat_identifier") or None,
                since=qs.get("since") or None,
                until=qs.get("until") or None,
                is_from_me=_bool(qs.get("is_from_me")),
                has_attachments=_bool(qs.get("has_attachments")),
                is_group=_bool(qs.get("is_group")),
                include_reactions=bool(_bool(qs.get("include_reactions"))),
                with_facets=bool(_bool(qs.get("with_facets"))),
                limit=_int(qs.get("limit")) or 50,
                offset=_int(qs.get("offset")) or 0,
                order=qs.get("order") or "relevance",
            )
            body = {
                "hits": [asdict(h) for h in result.hits],
                "total": result.total,
                "facets": result.facets,
                "query": result.query,
                "elapsed_ms": result.elapsed_ms,
            }
            self._send_json(200, body)
            return

        if url.path.startswith("/messages/") and url.path.endswith("/context"):
            try:
                rowid = int(url.path.split("/")[2])
            except (IndexError, ValueError):
                self._send_json(400, {"error": "invalid rowid"})
                return
            window = search.conversation_window(
                rowid,
                before=_int(qs.get("before")) or 5,
                after=_int(qs.get("after")) or 5,
            )
            self._send_json(200, {"messages": [asdict(h) for h in window]})
            return

        if url.path == "/chats":
            self._send_json(200, {"chats": search.list_chats(_int(qs.get("limit")) or 200)})
            return

        if url.path == "/people":
            self._send_json(200, {"people": search.list_people(_int(qs.get("limit")) or 2000)})
            return

        if url.path == "/chat-members":
            ids = [int(x) for x in (qs.get("chat_ids") or "").split(",") if x.strip().isdigit()]
            self._send_json(200, {"members": search.chat_members(ids)})
            return

        if url.path == "/handle-search":
            self._send_json(200, {"handles": search.find_handles(qs.get("q") or "", _int(qs.get("limit")) or 30)})
            return

        if url.path == "/handles":
            self._send_json(200, {"handles": search.list_handles(_int(qs.get("limit")) or 500)})
            return

        if url.path == "/stats":
            chat_ids = _csv_ints(qs.get("chat_ids"))
            self._send_json(200, stats_mod.all_stats(chat_ids))
            return

        if url.path.startswith("/stats/"):
            section = url.path[len("/stats/"):]
            limit = _int(qs.get("limit")) or 20
            offset = _int(qs.get("offset")) or 0
            chat_ids = _csv_ints(qs.get("chat_ids"))
            conn = dbmod.open_index_db(read_only=True)
            # top-chats and lopsided are cross-chat — filter ignored.
            if section == "top-chats":
                self._send_json(200, stats_mod.top_chats(conn, limit, offset)); return
            if section == "busiest-days":
                self._send_json(200, stats_mod.busiest_days(conn, limit, offset,
                    qs.get("mode") or "all", chat_ids)); return
            if section == "lopsided":
                self._send_json(200, stats_mod.lopsided(conn, limit, offset)); return
            if section == "longest":
                self._send_json(200, stats_mod.longest_messages(conn, limit, offset, chat_ids,
                    qs.get("mode") or "all")); return
            if section == "stickers":
                self._send_json(200, stats_mod.sticker_stats(limit, offset, chat_ids,
                    qs.get("mode") or "all")); return
            if section == "emojis":
                self._send_json(200, stats_mod.emoji_stats(limit, offset, chat_ids,
                    qs.get("mode") or "all")); return
            if section == "tapbacks":
                self._send_json(200, stats_mod.tapback_stats(chat_ids)); return
            self._send_json(404, {"error": "unknown stats section"}); return

        if url.path.startswith("/attachment/"):
            try:
                att_rowid = int(url.path.split("/")[2])
            except (IndexError, ValueError):
                self._send_json(400, {"error": "invalid att_rowid"})
                return
            self._serve_attachment(att_rowid)
            return

        # Static frontend.
        if self._serve_static(url.path):
            return

        self._send_json(404, {"error": "not found"})

    def _open_chat(self, body: dict) -> None:
        """Bring Messages.app forward to a specific chat.

        Strategy: use the `imessage:` URL scheme via the macOS `open` command.
        For 1:1 chats this navigates to the conversation. For groups (where the
        identifier is `chat<GUID>`) the URL scheme may just bring Messages.app
        forward without selecting the right thread — still useful for the user.
        """
        chat_id = (body.get("chat_identifier") or "").strip()
        if not chat_id:
            self._send_json(400, {"error": "missing chat_identifier"})
            return
        url = f"imessage:{chat_id}"
        try:
            subprocess.run(["open", url], capture_output=True, timeout=5)
        except Exception as e:
            self._send_json(500, {"error": str(e)})
            return
        self._send_json(200, {"ok": True, "url": url})

    def _serve_attachment(self, att_rowid: int) -> None:
        """Stream an attachment file. HEIC is transcoded — to PNG when the source
        likely has transparency (stickers always; explicitly typed HEIF too), to
        JPEG otherwise (smaller for photos)."""
        info = search.attachment_path(att_rowid)
        if info is None:
            self._send_json(404, {"error": "no such attachment"})
            return
        raw_path, mime, is_sticker = info
        path = os.path.expanduser(raw_path or "")
        if not path or not os.path.isfile(path):
            self._send_json(404, {"error": "file not on disk", "path": raw_path})
            return

        # Browsers don't render HEIC natively → transcode.
        mime_lc = (mime or "").lower()
        is_heic = mime_lc in ("image/heic", "image/heic-sequence", "image/heif")
        if not is_heic and path.lower().endswith((".heic", ".heif")):
            is_heic = True

        if is_heic:
            # Stickers (and HEIF in general) carry alpha → use PNG to preserve it.
            # Photos default to JPEG to keep size down.
            use_png = is_sticker or mime_lc == "image/heif"
            target_fmt, ctype, suffix = (
                ("png",  "image/png",  ".png")  if use_png else
                ("jpeg", "image/jpeg", ".jpg")
            )
            try:
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                    tmp_path = tmp.name
                r = subprocess.run(
                    ["sips", "-s", "format", target_fmt, path, "--out", tmp_path],
                    capture_output=True, timeout=30,
                )
                if r.returncode != 0:
                    raise RuntimeError(r.stderr.decode("utf-8", errors="replace"))
                with open(tmp_path, "rb") as f:
                    data = f.read()
                os.unlink(tmp_path)
            except Exception as e:
                self._send_json(500, {"error": "heic transcode failed", "detail": str(e)})
                return
        else:
            with open(path, "rb") as f:
                data = f.read()
            ctype = mime or mimetypes.guess_type(path)[0] or "application/octet-stream"

        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, max-age=3600")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def _serve_static(self, path: str) -> bool:
        """Serve files from web/. Returns True if a response was sent."""
        if path in ("", "/"):
            path = "/index.html"
        # Reject path traversal.
        rel = path.lstrip("/")
        target = (WEB_ROOT / rel).resolve()
        try:
            target.relative_to(WEB_ROOT.resolve())
        except ValueError:
            return False
        if not target.is_file():
            return False
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)
        return True


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"recall listening on http://{host}:{port}  (UI: /  · API: /search, /chats, …)", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down…", file=sys.stderr)
        server.server_close()
