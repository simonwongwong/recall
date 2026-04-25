"""Command-line entry point: `python3 -m recall.cli <command>`."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone

from . import api, indexer, search


def _fmt_date(unix: float | None) -> str:
    if unix is None:
        return "?"
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _print_hit(h: search.SearchHit) -> None:
    who = "me" if h.is_from_me else (h.sender_name or h.handle or "?")
    chat = h.chat_name or h.chat_identifier or "?"
    attach = " 📎" if h.has_attachments else ""
    react = " 👍" if h.is_reaction else ""
    print(f"[{_fmt_date(h.date_unix)}] {chat} · {who}{attach}{react}")
    print(f"  {h.snippet or h.text}")
    print(f"  rowid={h.rowid}")
    print()


def cmd_index(args: argparse.Namespace) -> int:
    if args.reset:
        indexer.reset_index()
    stats = indexer.index_messages(verbose=not args.json)
    if not args.skip_contacts:
        contact_stats = indexer.sync_contacts(verbose=not args.json)
        stats.update(contact_stats)
    if args.json:
        print(json.dumps(stats))
    return 0


def cmd_contacts(args: argparse.Namespace) -> int:
    stats = indexer.sync_contacts(verbose=not args.json)
    if args.json:
        print(json.dumps(stats))
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    result = search.search(
        args.query,
        handle=args.handle,
        contact=args.contact,
        chat_id=args.chat_id,
        chat_identifier=args.chat,
        since=args.since,
        until=args.until,
        is_from_me=args.from_me,
        has_attachments=args.attachments,
        is_group=args.group,
        include_reactions=args.reactions,
        limit=args.limit,
        offset=args.offset,
        order=args.order,
    )
    if args.json:
        print(
            json.dumps(
                {
                    "hits": [asdict(h) for h in result.hits],
                    "total": result.total,
                    "elapsed_ms": result.elapsed_ms,
                },
                default=str,
            )
        )
        return 0

    print(f"{result.total} hits in {result.elapsed_ms:.1f} ms\n", file=sys.stderr)
    for h in result.hits:
        _print_hit(h)
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    window = search.conversation_window(args.rowid, before=args.before, after=args.after)
    if args.json:
        print(json.dumps([asdict(h) for h in window], default=str))
        return 0
    for h in window:
        _print_hit(h)
    return 0


def cmd_chats(args: argparse.Namespace) -> int:
    rows = search.list_chats(args.limit)
    if args.json:
        print(json.dumps(rows, default=str))
        return 0
    for r in rows:
        last = _fmt_date(r["last_message_unix"])
        kind = "group" if r["is_group"] else "1:1"
        name = r.get("name") or r["display_name"] or r["chat_identifier"] or "?"
        print(f"#{r['rowid']:<6} [{kind:>5}] {r['message_count']:>6} msgs · last {last} · {name}")
    return 0


def cmd_handles(args: argparse.Namespace) -> int:
    rows = search.list_handles(args.limit)
    if args.json:
        print(json.dumps(rows, default=str))
        return 0
    for r in rows:
        last = _fmt_date(r["last_message_unix"])
        name = r.get("contact_name") or r["handle"]
        print(f"#{r['rowid']:<6} {r['message_count']:>6} msgs · last {last} · {name}  ({r['handle']})")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    api.serve(host=args.host, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="recall", description="Search your iMessages.")
    p.add_argument("--json", action="store_true", help="emit JSON instead of human output")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("index", help="(re)build the search index from chat.db")
    pi.add_argument("--reset", action="store_true", help="rebuild from scratch")
    pi.add_argument("--skip-contacts", action="store_true", help="don't refresh contacts")
    pi.set_defaults(func=cmd_index)

    pco = sub.add_parser("contacts", help="reload macOS Contacts → handle name map")
    pco.set_defaults(func=cmd_contacts)

    ps = sub.add_parser("search", help="search the index")
    ps.add_argument("query", nargs="?", default=None, help="text query (FTS5)")
    ps.add_argument("--handle", help='exact handle, e.g. "+15555550123" or "name@example.com"')
    ps.add_argument("--contact", help='match contact name (substring), e.g. "stephen"')
    ps.add_argument(
        "--reactions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="include tapback reactions in results (off by default)",
    )
    ps.add_argument("--chat-id", type=int, dest="chat_id")
    ps.add_argument("--chat", dest="chat", help="chat_identifier (e.g. group GUID or handle)")
    ps.add_argument("--since", help="ISO date or datetime, inclusive lower bound")
    ps.add_argument("--until", help="ISO date or datetime, exclusive upper bound")
    ps.add_argument(
        "--from-me",
        dest="from_me",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="only my messages (--from-me) or only theirs (--no-from-me)",
    )
    ps.add_argument(
        "--attachments",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="only messages with attachments",
    )
    ps.add_argument(
        "--group",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="only group chats / only 1:1",
    )
    ps.add_argument("--limit", type=int, default=50)
    ps.add_argument("--offset", type=int, default=0)
    ps.add_argument(
        "--order", choices=["relevance", "newest", "oldest"], default="relevance"
    )
    ps.set_defaults(func=cmd_search)

    pc = sub.add_parser("context", help="show messages around a result")
    pc.add_argument("rowid", type=int)
    pc.add_argument("--before", type=int, default=5)
    pc.add_argument("--after", type=int, default=5)
    pc.set_defaults(func=cmd_context)

    pch = sub.add_parser("chats", help="list known chats")
    pch.add_argument("--limit", type=int, default=200)
    pch.set_defaults(func=cmd_chats)

    ph = sub.add_parser("handles", help="list known handles (people)")
    ph.add_argument("--limit", type=int, default=500)
    ph.set_defaults(func=cmd_handles)

    psv = sub.add_parser("serve", help="run the JSON API for the frontend")
    psv.add_argument("--host", default="127.0.0.1")
    psv.add_argument("--port", type=int, default=8765)
    psv.set_defaults(func=cmd_serve)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
