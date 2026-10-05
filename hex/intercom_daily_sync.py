# Intercom conversations -> warehouse: DAILY job (this year onward).
#
# Port of the n8n workflow "Diego - Intercom a Google Sheet (continuo)" (AVuxY4kIW0NyLJix),
# same columns as the Sheet. Picks up every conversation whose updated_at is later than
# the newest one this job already wrote, so new, answered and reopened conversations all
# get refreshed. First run starts at BACKFILL_SINCE.
#
# Older conversations come from the separate job intercom_history_sync.py. Both write to
# intercom.conversations; sync_source tells their rows apart so each job keeps its own
# watermark.
#
# Hex layout:
#   1. SQL cell -> `watermark_df` (skip on the very first run):
#        select max(updated_at_unix) from intercom.conversations where sync_source = 'daily'
#   2. This Python cell -> `conversations_df`
#   3. Writeback `conversations_df` -> intercom.conversations_stg (overwrite)
#   4. SQL merge cell (see README.md)
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

SYNC_SOURCE = "daily"

# First run only: start of the window. Later runs continue from the watermark.
BACKFILL_SINCE = "2026-01-01"

# Cap per run so the initial load of the year fits in a Hex run (~10 conversations/s,
# 5000 is about 8-9 minutes). Until `remaining=` hits 0, run the project again by hand
# (or schedule it hourly) - after that, one run a day keeps up easily.
MAX_CONVERSATIONS_PER_RUN = 5000

# Leave out the last few minutes: Intercom's search index can lag slightly behind, and a
# conversation updated just before the run but not yet searchable would otherwise be
# behind the watermark next time. The next run picks these minutes up.
SETTLE_SECONDS = 600

# Search in windows of this many days, oldest first, and stop once a run has enough.
WINDOW_DAYS = 7

# ---------------------------------------------------------------------------------
# Shared with intercom_history_sync.py - keep both copies identical.
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

# Quick token check before the long run: fails in 1 second with a clear message.
me = _request("GET", "/me")
print(f"Token OK - Intercom app: {(me.get('app') or {}).get('name', '?')}", flush=True)

# Everything updated at or before `since` is already in the table. Runs only ever stop
# between whole seconds, so a strict "> since" can't miss anything.
wm = _sql_value("watermark_df")
since = wm if wm is not None else _day(BACKFILL_SINCE) - 1
until = int(time.time()) - SETTLE_SECONDS

found, lo = [], since
while lo < until and len(found) < MAX_CONVERSATIONS_PER_RUN:
    hi = min(until, lo + WINDOW_DAYS * 86400)
    print(f"Searching conversations updated {_fmt(lo)} -> {_fmt(hi)}...", flush=True)
    window = search({
        "operator": "AND",
        "value": [
            {"field": "updated_at", "operator": ">", "value": lo},
            {"field": "updated_at", "operator": "<", "value": hi + 1},
        ],
    })
    print(f"    {len(window)} in this window", flush=True)
    found += window
    lo = hi

# Oldest-updated first, so a capped run leaves a clean watermark for the next one.
items = list({cid: (cid, upd) for cid, _, upd in found}.values())
items.sort(key=lambda x: (x[1], x[0]))
batch = take_whole_seconds(items, MAX_CONVERSATIONS_PER_RUN)

print(f"Fetching full threads for {len(batch)}...", flush=True)
conversations_df = fetch_rows(batch, SYNC_SOURCE, deadline)
caught_up = lo >= until and len(conversations_df) == len(items)
reached = _fmt(int(conversations_df["updated_at_unix"].max())) if len(conversations_df) else _fmt(since)
print(
    f"DONE THIS RUN: fetched={len(conversations_df)} | up to {reached} | "
    + ("caught up to today" if caught_up else "more remaining - run again")
)
