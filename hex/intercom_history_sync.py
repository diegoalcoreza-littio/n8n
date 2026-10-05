# Intercom conversations -> warehouse: HISTORY job (everything before this year).
#
# One-off backfill of all conversations created before HISTORY_UNTIL, walking backwards
# in created_at windows. Each run fetches up to MAX_CONVERSATIONS_PER_RUN and the next
# run continues from the oldest conversation already written, so schedule it hourly and
# turn the schedule off once it prints DONE. ~500k conversations at ~10/s is roughly 14h
# of fetching, so about 50 runs.
#
# Writes to the same intercom.conversations table as intercom_daily_sync.py, tagged
# sync_source = 'history' so the two watermarks don't interfere. A conversation created
# before this year but updated during it gets fetched by both jobs; the merge keeps one.
#
# Hex layout:
#   1. SQL cell -> `history_watermark_df` (skip on the very first run):
#        select min(created_at_unix) from intercom.conversations where sync_source = 'history'
#   2. This Python cell -> `conversations_df`
#   3. Writeback `conversations_df` -> intercom.conversations_history_stg (overwrite)
#   4. SQL merge cell (see README.md), using the _history_stg table
#
# Secret: INTERCOM_TOKEN (Hex exposes secrets as Python variables). The cell raises
# an error if it can't find it.

import re
import time
import html as html_lib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

SYNC_SOURCE = "history"

# Where the daily job starts; this job covers everything created before it.
HISTORY_UNTIL = "2026-01-01"
# Safely before the workspace existed. Empty windows cost one API call each.
HISTORY_FROM = "2015-01-01"

# ~10 conversations/s -> 10000 is about 17 minutes per run.
MAX_CONVERSATIONS_PER_RUN = 10000
WINDOW_DAYS = 30

# ---------------------------------------------------------------------------------
# Shared with intercom_daily_sync.py - keep both copies identical.
# ---------------------------------------------------------------------------------
# US workspaces: api.intercom.io. EU: api.eu.intercom.io. Australia: api.au.intercom.io.
# A token from one region gets 401 Unauthorized on the others.
API = "https://api.intercom.io"
INTERCOM_VERSION = "2.16"
WORKERS = 8
FETCH_CHUNK = 250
# Each run stops fetching after this long and saves what it has; the next run resumes.
TIME_BUDGET_MINUTES = 40
# Redshift VARCHAR tops out at 65535 bytes; keep headroom for multi-byte characters.
MAX_TEXT_CHARS = 20000

COLUMNS = [
    "id", "created_at", "created_at_unix", "updated_at", "updated_at_unix", "state", "open",
    "priority", "title", "admin_assignee_id", "team_assignee_id", "contact_ids", "tags",
    "canal", "tipo_origen", "autor_tipo", "autor_email", "asunto", "primer_mensaje",
    "texto_completo", "rating", "rating_comentario", "first_response_at", "closed_at",
    "tiempo_primera_respuesta_s", "reaperturas", "n_partes", "sync_source", "synced_at",
]

_session = requests.Session()


def _find_token():
    """The Intercom token: a Hex secret / variable named INTERCOM_TOKEN (any case), or an
    environment variable of the same name. Fails loudly instead of silently doing nothing."""
    import os
    candidates = [v for k, v in list(globals().items()) if k.upper() == "INTERCOM_TOKEN"]
    candidates.append(os.environ.get("INTERCOM_TOKEN"))
    for value in candidates:
        if isinstance(value, str) and value.strip():
            # Tokens copied from an n8n header credential often include the scheme; we add it.
            return re.sub(r"^\s*Bearer\s+", "", value.strip().strip("'\""), flags=re.I)
    raise RuntimeError(
        "No Intercom token found. In Hex: Settings > Secrets > add a secret named "
        "INTERCOM_TOKEN, then rerun this cell."
    )


def _request(method, path, **kwargs):
    """Call Intercom, backing off on 429 and transient 5xx."""
    headers = {
        "Authorization": f"Bearer {_TOKEN}",
        "Intercom-Version": INTERCOM_VERSION,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    for attempt in range(6):
        r = _session.request(method, API + path, headers=headers, timeout=60, **kwargs)
        if r.status_code == 429:
            reset = r.headers.get("X-RateLimit-Reset")
            wait = max(1, int(reset) - int(time.time())) if reset else 2 ** attempt
            time.sleep(min(wait, 60))
            continue
        if r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        if r.status_code >= 400:
            hint = ""
            if r.status_code == 401:
                hint = (" -> Intercom rejected the token. Check it is the app's Access Token, "
                        "without quotes or 'Bearer ', from the same region as API.")
            elif r.status_code == 403:
                hint = " -> The token works but the app lacks permission to read conversations."
            raise requests.HTTPError(
                f"{r.status_code} from {method} {path}: {r.text[:500]}{hint}", response=r
            )
        return r.json()
    r.raise_for_status()


def search(query):
    """Return [(id, created_at, updated_at)] for every conversation matching query."""
    body = {"query": query, "pagination": {"per_page": 150}}
    found, pages = [], 0
    while True:
        page = _request("POST", "/conversations/search", json=body)
        pages += 1
        for c in page.get("conversations", []):
            found.append((str(c["id"]), int(c.get("created_at") or 0), int(c.get("updated_at") or 0)))
        if pages % 10 == 0:
            print(f"    ...{len(found)} listed so far", flush=True)
        nxt = (page.get("pages") or {}).get("next") or {}
        if not nxt.get("starting_after"):
            return found
        body["pagination"]["starting_after"] = nxt["starting_after"]


_TAG_RE = re.compile(r"<[^>]+>")
_BREAK_RE = re.compile(r"<br\s*/?>|</p>", re.I)
_WS_RE = re.compile(r"\s+")


def plain(text):
    if not text:
        return ""
    t = _BREAK_RE.sub(" ", str(text))
    t = html_lib.unescape(_TAG_RE.sub("", t))
    return _WS_RE.sub(" ", t).strip()


def ts(v):
    if not v:
        return None
    return datetime.fromtimestamp(int(v), tz=timezone.utc).replace(tzinfo=None)


def _opt_int(v):
    return None if v is None else int(v)


def _opt_str(v):
    return None if v is None else str(v)


def build_row(c, sync_source):
    """Same columns as the n8n "Armar filas" node, plus the *_unix and sync columns."""
    src = c.get("source") or {}
    author = src.get("author") or {}
    stats = c.get("statistics") or {}
    rating = c.get("conversation_rating") or {}
    tags = (c.get("tags") or {}).get("tags") or []
    contacts = (c.get("contacts") or {}).get("contacts") or []
    parts = (c.get("conversation_parts") or {}).get("conversation_parts") or []

    first = plain(src.get("body"))
    chunks = [f"{author.get('type') or 'origen'}: {first}"] if first else []
    for p in parts:
        b = plain(p.get("body"))
        if b:
            chunks.append(f"{(p.get('author') or {}).get('type') or 'desconocido'}: {b}")

    return {
        "id": str(c.get("id") or ""),
        "created_at": ts(c.get("created_at")),
        "created_at_unix": int(c.get("created_at") or 0),
        "updated_at": ts(c.get("updated_at")),
        "updated_at_unix": int(c.get("updated_at") or 0),
        "state": c.get("state") or "",
        "open": bool(c.get("open")),
        "priority": c.get("priority") or "",
        "title": c.get("title") or "",
        "admin_assignee_id": _opt_str(c.get("admin_assignee_id")),
        "team_assignee_id": _opt_str(c.get("team_assignee_id")),
        "contact_ids": " ".join(str(x.get("id")) for x in contacts),
        "tags": " | ".join(t.get("name", "") for t in tags),
        "canal": src.get("delivered_as") or "",
        "tipo_origen": src.get("type") or "",
        "autor_tipo": author.get("type") or "",
        "autor_email": author.get("email") or "",
        "asunto": plain(src.get("subject")),
        "primer_mensaje": first[:MAX_TEXT_CHARS],
        "texto_completo": " || ".join(chunks)[:MAX_TEXT_CHARS],
        "rating": _opt_int(rating.get("rating")),
        "rating_comentario": plain(rating.get("remark")),
        "first_response_at": ts(stats.get("first_admin_reply_at")),
        "closed_at": ts(stats.get("last_close_at")),
        "tiempo_primera_respuesta_s": _opt_int(stats.get("time_to_admin_reply")),
        "reaperturas": _opt_int(stats.get("count_reopens")),
        "n_partes": _opt_int(stats.get("count_conversation_parts")),
        "sync_source": sync_source,
        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def fetch_rows(items, sync_source, deadline):
    """Fetch full threads for [(id, sort_key)] in the given order, in chunks.

    Stops after the deadline, but only between two different sort_key seconds: a group
    of conversations sharing a second is always fetched whole, and the first chunk is
    always fetched. So the result is a leading slice that ends on a second boundary,
    every run makes progress, and the next run's watermark can't skip anything."""
    def one(conv_id):
        c = _request("GET", f"/conversations/{conv_id}", params={"display_as": "plaintext"})
        return build_row(c, sync_source)

    rows, t0, i = [], time.time(), 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while i < len(items):
            j = min(len(items), i + FETCH_CHUNK)
            if rows and time.time() > deadline:
                # Finish only the second we're in the middle of, then stop.
                j = i
                while j < len(items) and items[j][1] == items[i - 1][1]:
                    j += 1
                if j == i:
                    print(f"    time limit reached, stopping at {len(rows)}/{len(items)}", flush=True)
                    break
            rows += list(pool.map(one, [cid for cid, _ in items[i:j]]))
            i = j
            rate = len(rows) / max(1e-6, time.time() - t0)
            print(f"    fetched {len(rows)}/{len(items)} ({rate:.1f}/s)", flush=True)
    # Explicit columns so an empty run still gives the writeback a valid schema.
    return pd.DataFrame(rows, columns=COLUMNS)


def take_whole_seconds(items, cap):
    """First `cap` of [(id, sort_key)], extended so the last second isn't split."""
    if len(items) <= cap:
        return items
    j = cap
    while j < len(items) and items[j][1] == items[cap - 1][1]:
        j += 1
    return items[:j]


def _sql_value(name):
    """First cell of a dataframe from a Hex SQL cell, or None if missing/empty."""
    try:
        v = globals()[name].iloc[0, 0]
    except (KeyError, IndexError):
        return None
    return None if v is None or pd.isna(v) else int(v)


def _day(s):
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def _fmt(unix):
    return f"{datetime.fromtimestamp(unix, tz=timezone.utc):%Y-%m-%d %H:%M} UTC"


# ---------------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------------
_TOKEN = _find_token()
deadline = time.time() + TIME_BUDGET_MINUTES * 60
floor = _day(HISTORY_FROM)
# Everything created strictly before `upper` is still to do.
upper = _sql_value("history_watermark_df") or _day(HISTORY_UNTIL)
start_upper = upper

print(f"Searching conversations created before {_fmt(upper)}, going back in {WINDOW_DAYS}-day windows...", flush=True)
found = []
while upper > floor and len(found) < MAX_CONVERSATIONS_PER_RUN:
    lower = max(floor, upper - WINDOW_DAYS * 86400)
    found += search({
        "operator": "AND",
        "value": [
            {"field": "created_at", "operator": ">", "value": lower - 1},
            {"field": "created_at", "operator": "<", "value": upper},
        ],
    })
    upper = lower

# Newest-created first; next run resumes with created_at < the oldest written, so the
# cap and the time limit both only cut between whole seconds.
items = list({cid: (cid, created) for cid, created, _ in found}.values())
items.sort(key=lambda x: (-x[1], x[0]))
batch = take_whole_seconds(items, MAX_CONVERSATIONS_PER_RUN)

print(f"Found {len(items)}. Fetching full threads for {len(batch)}...", flush=True)
conversations_df = fetch_rows(batch, SYNC_SOURCE, deadline)
done = upper <= floor and len(conversations_df) == len(items)
reached = _fmt(int(conversations_df["created_at_unix"].min())) if len(conversations_df) else _fmt(start_upper)
print(
    f"created before {_fmt(start_upper)} -> back to {reached} | "
    f"fetched={len(conversations_df)}" + (" | DONE, turn off the schedule" if done else "")
)
