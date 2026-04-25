"""Generate a synthetic `demo/chat.db` matching the iMessage schema.

Run once:    python3 demo/build_demo.py
Then serve:  RECALL_CHAT_DB=demo/chat.db RECALL_INDEX_DB=demo/index.db \
             python3 -m recall.cli serve --port 8766

Everything here is fictional — names are made up, phone numbers use the
NANPA-reserved 555-01XX fictional range, emails use example.com (RFC 2606).
"""
from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parent / "chat.db"
MAC_EPOCH = datetime(2001, 1, 1, tzinfo=timezone.utc)

# Fixed seed so the output is deterministic — re-running gives the same demo.
random.seed(42)

# -----------------------------------------------------------------------------
# People
# -----------------------------------------------------------------------------
CONTACTS = [
    ("Aang",             "+15555550101"),  # 0
    ("Sokka",            "+15555550102"),  # 1
    ("Zuko",             "zuko@example.com"),  # 2
    ("Suki",             "+15555550104"),  # 3
    ("Iroh",             "iroh@example.com"),  # 4
    ("Toph",             "+15555550106"),  # 5
    ("Azula",            "+15555550107"),  # 6
    ("Katara",           "+15555550108"),  # 7
    ("King Bumi",        "+15555550109"),  # 8
    ("Cabbage Merchant", "cabbage.corp@example.com"),  # 9
]

# Group chats: (display_name, [member_indices into CONTACTS])
GROUPS = [
    ("Team Avatar",            [0, 1, 5, 7]),  # Aang, Sokka, Toph, Katara
    ("Order of the White Lotus", [4, 8]),       # Iroh, Bumi
    ("Fire Nation Family",     [2, 6]),         # Zuko, Azula
]

# -----------------------------------------------------------------------------
# Conversation seeds — each one is a thematic exchange that gets stretched
# across days. Mix of incoming/outgoing, occasional attachments + reactions.
# -----------------------------------------------------------------------------

def _msg(text, direction="in", attach=False, react=None):
    return {"text": text, "direction": direction, "attach": attach, "react": react}

CONVERSATIONS_1TO1 = {
    0: [  # Aang — sweet, easily distracted, loves Appa
        _msg("katara says i need to practice my waterbending forms more 😩"),
        _msg("she's right tho", "out"),
        _msg("don't take her side. you're supposed to be on team appa"),
        _msg("team appa is a team of two", "out"),
        _msg("EXACTLY. small but mighty"),
        _msg("anyway, want to go penguin sledding sunday?"),
        _msg("always. should i bring snacks?", "out"),
        _msg("appa likes the apples"),
        _msg("the moon peaches were SO good last time", attach=True),
        _msg("how do you eat that many in one sitting", "out"),
        _msg("i am the avatar. it is my sacred duty", react="❤️"),
        _msg("twinkle toes!! toph just made up that name and i love it"),
        _msg("don't tell her i love it", "out"),
        _msg("too late she can feel my excitement through the floor"),
    ],
    1: [  # Sokka — jokes, plans, MEAT, boomerang
        _msg("BOOMERANG!! you DO always come back!", attach=True),
        _msg("where were you, sokka", "out"),
        _msg("stuck in a tree. it was a whole thing"),
        _msg("anyway plan B is on for tonight"),
        _msg("what was plan A", "out"),
        _msg("we do not speak of plan A"),
        _msg("plan B has snacks. plan A had... fewer snacks."),
        _msg("the lesson here: always plan for snacks", "out"),
        _msg("THIS is why you're my favorite non-bender besides me"),
        _msg("dinner tonight? i need meat. NEED. MEAT.", "out"),
        _msg("i was literally going to text you the same thing"),
        _msg("we are kindred spirits"),
    ],
    2: [  # Zuko — brooding, honor, bad at small talk
        _msg("is uncle there"),
        _msg("he's making tea", "out"),
        _msg("of course he is"),
        _msg("want me to tell him you came by?", "out"),
        _msg("...don't bother"),
        _msg("you ok zuko", "out"),
        _msg("I'M FINE. I'M ALWAYS FINE."),
        _msg("right.", "out"),
        _msg("sorry. that was loud."),
        _msg("a lot going on", "out"),
        _msg("i need to restore my honor and also i don't know what to wear to dinner"),
        _msg("priorities", "out"),
        _msg("EXACTLY"),
    ],
    3: [  # Suki — kind, capable
        _msg("kyoshi warriors training tomorrow at dawn"),
        _msg("i'll be there. i promise.", "out"),
        _msg("bring your fans this time. don't 'forget' them again"),
        _msg("i forgot them ONE TIME", "out"),
        _msg("twice. but who's counting"),
        _msg("you, apparently", "out", react="❤️"),
        _msg("how was the trip with sokka? did he bring 'plans'"),
        _msg("he had a binder. an actual binder.", "out"),
        _msg("of course he did 😂"),
    ],
    4: [  # Iroh — tea, proverbs, warmth
        _msg("have you tried the new jasmine? it is most calming"),
        _msg("i'm more of a black tea person honestly", "out"),
        _msg("ah. then we shall steep accordingly."),
        _msg("there is always something to learn from a good cup of tea"),
        _msg("noted, uncle", "out"),
        _msg("come over tonight. i made dumplings."),
        _msg("on my way", "out"),
        _msg("bring zuko if you see him. he forgets to eat."),
        _msg("on it", "out"),
    ],
    5: [  # Toph — sass, blind jokes, ribs Aang
        _msg("twinkle toes forgot to feed appa AGAIN"),
        _msg("HE DIDN'T", "out"),
        _msg("i FELT the bison vibrations. they are SAD vibrations."),
        _msg("...okay maybe he did", "out"),
        _msg("rookie mistake"),
        _msg("anyway, the swamp tomorrow?"),
        _msg("what's in the swamp", "out"),
        _msg("you'll see. or i won't. either way."),
        _msg("lol", "out"),
        _msg("oh i didn't see that text. literally."),
        _msg("toph 😭", "out"),
        _msg("i'll be using that joke until i'm 80"),
    ],
    6: [  # Azula — chilling, weirdly polite
        _msg("Almost isn't good enough."),
        _msg("uh sorry wrong number", "out"),
        _msg("Wrong number, you say."),
        _msg("yeah this is sokka's friend", "out"),
        _msg("...I see."),
        _msg("are we good", "out"),
        _msg("We are absolutely not good."),
        _msg("That said: tell Zuko mother's coming for dinner."),
        _msg("uh ok", "out"),
        _msg("Thank you. ☺️"),
        _msg("the smiley is somehow scarier than the threat", "out"),
    ],
    7: [  # Katara — caring, intense, makes stew
        _msg("did you eat today?"),
        _msg("yes katara i ate", "out"),
        _msg("what did you eat"),
        _msg("...moon peaches", "out"),
        _msg("that is a snack not a meal"),
        _msg("moon peaches are LIFE", "out"),
        _msg("i am making stew. you will eat the stew."),
        _msg("yes ma'am 🫡", "out"),
        _msg("good"),
        _msg("i've been working on a new waterbending form, look", attach=True),
        _msg("oh wow that's incredible", "out"),
        _msg("aang's face when i did it 😭"),
        _msg("i can imagine", "out"),
        _msg("he tried to copy it and slipped on his own ice"),
    ],
    8: [  # Bumi — chaos king
        _msg("flopsie says hi"),
        _msg("who is flopsie", "out"),
        _msg("the cabbages will know"),
        _msg("bumi what does that mean", "out"),
        _msg("BWAAAHAHAHAHAHA"),
        _msg("i don't know what i expected", "out"),
        _msg("neither did the cabbages"),
        _msg("you should visit omashu. we play games. some are even safe."),
        _msg("'some'", "out"),
        _msg("most. okay, several. okay, one."),
    ],
    9: [  # Cabbage Merchant — eternal suffering
        _msg("MY CABBAGES"),
        _msg("what happened this time", "out"),
        _msg("THE AVATAR HAPPENED"),
        _msg("oh no", "out"),
        _msg("that is the SEVENTEENTH cart this MONTH"),
        _msg("i am so sorry. i'll talk to him.", "out"),
        _msg("look at this devastation", attach=True),
        _msg("oh god", "out"),
        _msg("i am opening a corporation. CABBAGE CORP. you have been warned."),
        _msg("ipo when", "out"),
        _msg("Q3"),
    ],
}

CONVERSATIONS_GROUP = {
    0: [  # Team Avatar — Aang, Sokka, Toph, Katara
        ("Sokka",  _msg("team meeting at the campfire. agenda: dinner")),
        ("Toph",   _msg("the agenda is always dinner")),
        (None,     _msg("can we add 'aang stops avoiding earthbending' to the agenda", "out")),
        ("Sokka",  _msg("two items?? AMBITIOUS")),
        ("Toph",   _msg("i second the motion. third it. fourth it.")),
        ("Aang",   _msg("you guys are bullies 😢")),
        ("Katara", _msg("aang we love you. earthbend.")),
        ("Aang",   _msg("ok ok ok")),
        ("Katara", _msg("i'm bringing stew")),
        ("Sokka",  _msg("EXCELLENT. is there meat in the stew")),
        ("Katara", _msg("no")),
        ("Sokka",  _msg("then is there meat NEAR the stew")),
        ("Katara", _msg("sokka.")),
        ("Toph",   _msg("ten silver pieces sokka brings his own meat")),
        (None,     _msg("i'll take that bet", "out")),
        ("Sokka",  _msg("i'm literally already at the butcher")),
    ],
    1: [  # Order of the White Lotus — Iroh, Bumi
        ("Iroh",  _msg("the tea is ready, my old friend")),
        ("Bumi",  _msg("BWAHA. tea. i bring cabbages.")),
        ("Iroh",  _msg("...please do not bring cabbages.")),
        ("Bumi",  _msg("TOO LATE")),
        (None,    _msg("uncle, are you safe", "out")),
        ("Iroh",  _msg("i am fine. king bumi has brought... an unusual amount of cabbages.")),
        ("Iroh",  _msg("the merchant will be most distressed")),
        ("Bumi",  _msg("the merchant is ALWAYS distressed. it is his constant.")),
        ("Iroh",  _msg("a fair point.")),
    ],
    2: [  # Fire Nation Family — Zuko, Azula
        ("Azula", _msg("Father wants to see you.")),
        ("Zuko",  _msg("what for")),
        ("Azula", _msg("Oh, you'll find out.")),
        (None,    _msg("uh, this is still sokka's friend", "out")),
        ("Azula", _msg("How are you in this chat.")),
        (None,    _msg("i don't know honestly", "out")),
        ("Zuko",  _msg("sokka why")),
        ("Azula", _msg("Anyway. Dinner is at seven. Wear something that is not red.")),
        ("Zuko",  _msg("i only own red")),
        ("Azula", _msg("I am aware.")),
    ],
}


# -----------------------------------------------------------------------------
# DB construction
# -----------------------------------------------------------------------------

def _to_mac_ns(dt: datetime) -> int:
    return int((dt - MAC_EPOCH).total_seconds() * 1_000_000_000)


def build():
    if OUT.exists():
        OUT.unlink()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(OUT)
    db.executescript("""
        CREATE TABLE handle (
            ROWID INTEGER PRIMARY KEY,
            id TEXT NOT NULL,
            country TEXT,
            service TEXT,
            uncanonicalized_id TEXT,
            person_centric_id TEXT
        );
        CREATE TABLE chat (
            ROWID INTEGER PRIMARY KEY,
            guid TEXT NOT NULL,
            chat_identifier TEXT,
            display_name TEXT,
            service_name TEXT,
            style INTEGER,
            state INTEGER,
            account_id TEXT,
            properties BLOB,
            chat_identifier_canonical TEXT,
            last_addressed_handle TEXT
        );
        CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER, message_date INTEGER);
        CREATE TABLE attachment (
            ROWID INTEGER PRIMARY KEY,
            guid TEXT,
            created_date INTEGER,
            filename TEXT,
            uti TEXT,
            mime_type TEXT,
            transfer_name TEXT,
            total_bytes INTEGER,
            is_sticker INTEGER DEFAULT 0
        );
        CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
        CREATE TABLE message (
            ROWID INTEGER PRIMARY KEY,
            guid TEXT NOT NULL,
            text TEXT,
            attributedBody BLOB,
            handle_id INTEGER,
            is_from_me INTEGER DEFAULT 0,
            date INTEGER,
            cache_has_attachments INTEGER DEFAULT 0,
            associated_message_guid TEXT,
            associated_message_type INTEGER,
            thread_originator_guid TEXT,
            service TEXT
        );
    """)

    # Insert handles. We need a separate handle row per (id, service). We'll
    # use just iMessage for everyone.
    handle_rowids: dict[int, int] = {}  # contact index → handle ROWID
    for i, (name, ident) in enumerate(CONTACTS, start=1):
        db.execute(
            "INSERT INTO handle (ROWID, id, service, country) VALUES (?,?,?,?)",
            (i, ident, "iMessage", "us"),
        )
        handle_rowids[i - 1] = i

    # 1:1 chats — one per contact, chat_identifier = the contact's id.
    chat_rowids: dict[str, int] = {}  # key → chat ROWID
    next_chat_id = 1
    for i, (name, ident) in enumerate(CONTACTS):
        db.execute(
            "INSERT INTO chat (ROWID, guid, chat_identifier, display_name, service_name, style) "
            "VALUES (?,?,?,?,?,?)",
            (next_chat_id, f"iMessage;-;{ident}", ident, "", "iMessage", 45),
        )
        db.execute(
            "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?,?)",
            (next_chat_id, handle_rowids[i]),
        )
        chat_rowids[f"1:1:{i}"] = next_chat_id
        next_chat_id += 1

    # Group chats.
    group_chat_ids: dict[int, int] = {}
    for gi, (display, members) in enumerate(GROUPS):
        guid = f"chat{1000000 + gi:016d}"
        db.execute(
            "INSERT INTO chat (ROWID, guid, chat_identifier, display_name, service_name, style) "
            "VALUES (?,?,?,?,?,?)",
            (next_chat_id, f"iMessage;+;{guid}", guid, display, "iMessage", 43),
        )
        for m in members:
            db.execute(
                "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?,?)",
                (next_chat_id, handle_rowids[m]),
            )
        group_chat_ids[gi] = next_chat_id
        next_chat_id += 1

    # Messages — distribute conversations across the past 360 days, with each
    # conversation's lines spread over a few days at realistic times.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    next_msg_id = 1
    next_att_id = 1
    all_message_rows: list[tuple] = []
    chat_msg_rows: list[tuple] = []
    msg_att_rows: list[tuple] = []

    def add_message(text, *, chat_id, handle_id, is_from_me, when, has_attach=False, react=None, react_target=None):
        nonlocal next_msg_id, next_att_id
        rowid = next_msg_id
        next_msg_id += 1
        guid = f"DEMO-{rowid:08d}"
        date_ns = _to_mac_ns(when)
        all_message_rows.append((
            rowid, guid, text, None,                    # rowid, guid, text, attributedBody
            handle_id, 1 if is_from_me else 0, date_ns,
            1 if has_attach else 0,                     # cache_has_attachments
            react_target,                                # associated_message_guid (links reactions)
            2000 if react else None,                     # associated_message_type (Loved=2000)
            None,                                        # thread_originator_guid
            "iMessage",
        ))
        chat_msg_rows.append((chat_id, rowid, date_ns))
        if has_attach:
            ext = random.choice(["jpg", "png", "heic"])
            mime = {"jpg": "image/jpeg", "png": "image/png", "heic": "image/heic"}[ext]
            att_id = next_att_id
            next_att_id += 1
            # Path that won't exist on disk — exercises the "iCloud-pull" UI nicely.
            fname = f"~/Library/Messages/Attachments/de/mo/{guid}.{ext}"
            db.execute(
                "INSERT INTO attachment (ROWID, guid, filename, mime_type, transfer_name, total_bytes, is_sticker) "
                "VALUES (?,?,?,?,?,?,?)",
                (att_id, guid, fname, mime, f"IMG_{rowid:04d}.{ext}",
                 random.randint(120_000, 4_000_000), 0),
            )
            msg_att_rows.append((rowid, att_id))
        return rowid, guid

    # 1:1 conversations
    for ci, msgs in CONVERSATIONS_1TO1.items():
        chat_id = chat_rowids[f"1:1:{ci}"]
        handle_id = handle_rowids[ci]
        # Anchor each conversation at a random day in the past year.
        start = now - timedelta(days=random.randint(2, 340), hours=random.randint(0, 6))
        cur = start
        last_text_guid = None
        for m in msgs:
            cur = cur + timedelta(minutes=random.randint(1, 90))
            rowid, guid = add_message(
                m["text"], chat_id=chat_id, handle_id=handle_id,
                is_from_me=(m["direction"] == "out"), when=cur,
                has_attach=m["attach"],
            )
            if m["react"] and last_text_guid:
                # Add a reaction message from the OTHER side targeting last_text_guid.
                rdir_from_me = (m["direction"] != "out")
                add_message(
                    m["react"], chat_id=chat_id, handle_id=handle_id,
                    is_from_me=rdir_from_me, when=cur + timedelta(seconds=30),
                    react=m["react"], react_target=last_text_guid,
                )
            last_text_guid = guid

    # Group conversations
    for gi, msgs in CONVERSATIONS_GROUP.items():
        chat_id = group_chat_ids[gi]
        members = GROUPS[gi][1]
        start = now - timedelta(days=random.randint(2, 200), hours=random.randint(0, 6))
        cur = start
        for sender_name, m in msgs:
            cur = cur + timedelta(minutes=random.randint(1, 60))
            if m["direction"] == "out":
                handle_id = None  # NULL handle for is_from_me=1, matches real iMessage
            else:
                # Map sender name → contact index
                idx = next((i for i, (n, _) in enumerate(CONTACTS) if n.split()[0] == sender_name), members[0])
                handle_id = handle_rowids[idx]
            add_message(
                m["text"], chat_id=chat_id, handle_id=handle_id,
                is_from_me=(m["direction"] == "out"), when=cur,
                has_attach=m["attach"],
            )

    # Bulk insert
    db.executemany(
        "INSERT INTO message (ROWID, guid, text, attributedBody, handle_id, is_from_me, "
        "date, cache_has_attachments, associated_message_guid, associated_message_type, "
        "thread_originator_guid, service) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        all_message_rows,
    )
    db.executemany(
        "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (?,?,?)",
        chat_msg_rows,
    )
    db.executemany(
        "INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (?,?)",
        msg_att_rows,
    )
    db.commit()
    db.close()
    print(f"✔ wrote {OUT}")
    print(f"  {len(CONTACTS)} contacts, {len(GROUPS)} groups, {len(all_message_rows)} messages")


def index_and_seed_contacts():
    """Build the demo index right after the chat.db, and inject contact names
    directly (we don't read the real Contacts.app for demo data)."""
    import os, sys
    os.environ["RECALL_CHAT_DB"] = str(OUT)
    os.environ["RECALL_INDEX_DB"] = str(OUT.parent / "index.db")
    # Fresh import after env vars are set so module-level paths don't matter.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from recall import db as dbmod, indexer

    demo_index = OUT.parent / "index.db"
    indexer.reset_index(demo_index)
    indexer.index_messages(verbose=False)

    # Seed contact_names: every fictional contact maps to its display name.
    pairs = {ident: name for name, ident in CONTACTS}
    dst = dbmod.open_index_db()
    indexer.init_index(dst)
    with dst:
        dst.execute("DELETE FROM contact_names")
        dst.executemany(
            "INSERT OR REPLACE INTO contact_names(handle, name) VALUES (?,?)",
            pairs.items(),
        )
        indexer._refresh_chat_resolved_names(dst)
    print(f"✔ indexed + seeded contacts → {demo_index}")


if __name__ == "__main__":
    build()
    index_and_seed_contacts()
