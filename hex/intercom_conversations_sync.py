# Intercom conversations -> warehouse, for a scheduled Hex project.
#
# Port of the n8n workflow "Diego - Intercom a Google Sheet (continuo)" (AVuxY4kIW0NyLJix).
# Same columns as the Sheet, but incremental instead of a one-off backfill:
#   - n8n walked GET /conversations newest -> oldest and kept a cursor in static data.
#   - Here we use POST /conversations/search on updated_at > watermark, where the
#     watermark is max(updated_at_unix) already in the target table. Re-opened and
#     newly answered conversations get picked up again, and no cursor state is needed.
#
# Hex layout (one Hex cell per "# %%" block):
#   1. SQL cell  -> dataframe `watermark_df` (see hex/README.md)
#   2. This Python cell -> dataframe `conversations_df`
#   3. Writeback cell: `conversations_df` -> staging table (overwrite)
#   4. SQL cell: merge staging into the main table (see hex/README.md)
#
# Secrets: create a Hex secret named INTERCOM_TOKEN (same token as the n8n
# "intercom conversations" credential). Hex exposes secrets as Python variables.

# %%
import re
import time
import html as html_lib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

API = "https://api.intercom.io"
INTERCOM_VERSION = "2.16"

# First run only (empty target table): how far back to start. The n8n export took the
# ~31k most recent conversations; pick the date that matches what you want in history.
BACKFILL_SINCE = "2026-01-01"

# Cap per run so a big backfill splits across several scheduled runs instead of hitting
# the Hex run timeout. Conversations are processed oldest-updated first, so the next run
# resumes from the new watermark. ~10 conversations/s -> 5000 is roughly 8-9 minutes.
MAX_CONVERSATIONS_PER_RUN = 5000

# Re-read a small window before the watermark: updated_at has 1s resolution, so rows
# sharing the boundary second could otherwise be skipped. The merge dedupes by id.
OVERLAP_SECONDS = 300

WORKERS = 8
# Redshift VARCHAR tops out at 65535 bytes; keep headroom for multi-byte characters.
MAX_TEXT_CHARS = 20000


def _headers():
    return {
        "Authorization": f"Bearer {INTERCOM_TOKEN}",  # noqa: F821 - Hex secret
        "Intercom-Version": INTERCOM_VERSION,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


_session = requests.Session()


def _request(method, path, **kwargs):
    """Call Intercom, backing off on 429 and transient 5xx."""
    for attempt in range(6):
        r = _session.request(method, API + path, headers=_headers(), timeout=60, **kwargs)
        if r.status_code == 429:
            reset = r.headers.get("X-RateLimit-Reset")
            wait = max(1, int(reset) - int(time.time())) if reset else 2 ** attempt
            time.sleep(min(wait, 60))
            continue
        if r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


# %% Watermark
def _watermark():
    try:
        v = watermark_df.iloc[0, 0]  # noqa: F821 - from the SQL cell
        if v is not None and not pd.isna(v):
            return int(v) - OVERLAP_SECONDS
    except (NameError, IndexError):
        pass
    since = datetime.strptime(BACKFILL_SINCE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(since.timestamp())


# %% Find conversations updated since the watermark
def search_updated_since(since_unix):
    """Return [(id, updated_at)] for every conversation updated after since_unix."""
    body = {
        "query": {"field": "updated_at", "operator": ">", "value": since_unix},
        "pagination": {"per_page": 150},
    }
    found = []
    while True:
        page = _request("POST", "/conversations/search", json=body)
        for c in page.get("conversations", []):
            found.append((str(c["id"]), int(c.get("updated_at") or 0)))
        nxt = (page.get("pages") or {}).get("next") or {}
        if not nxt.get("starting_after"):
            return found
        body["pagination"]["starting_after"] = nxt["starting_after"]


# %% Row building - same columns as the n8n "Armar filas" node
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


def build_row(c):
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
        "updated_at": ts(c.get("updated_at")),
        "updated_at_unix": int(c.get("updated_at") or 0),
        "state": c.get("state") or "",
        "open": bool(c.get("open")),
        "priority": c.get("priority") or "",
        "title": c.get("title") or "",
        "admin_assignee_id": None if c.get("admin_assignee_id") is None else str(c["admin_assignee_id"]),
        "team_assignee_id": None if c.get("team_assignee_id") is None else str(c["team_assignee_id"]),
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
        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def fetch_row(conv_id):
    return build_row(_request("GET", f"/conversations/{conv_id}", params={"display_as": "plaintext"}))


# %% Run
if "INTERCOM_TOKEN" in globals():  # Hex injects the secret as a global
    since = _watermark()
    candidates = search_updated_since(since)
    # Oldest-updated first, so a capped run leaves a clean watermark for the next one.
    candidates.sort(key=lambda x: x[1])
    ids = list(dict.fromkeys(cid for cid, _ in candidates))[:MAX_CONVERSATIONS_PER_RUN]

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        rows = list(pool.map(fetch_row, ids))

    conversations_df = pd.DataFrame(rows)
    print(
        f"since={datetime.fromtimestamp(since, tz=timezone.utc):%Y-%m-%d %H:%M} UTC | "
        f"updated={len(candidates)} | fetched={len(conversations_df)} | "
        f"remaining={max(0, len(set(c for c, _ in candidates)) - len(ids))}"
    )
