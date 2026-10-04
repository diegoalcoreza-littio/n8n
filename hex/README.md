# Intercom conversations in Hex

Hex version of the n8n workflow **"Diego - Intercom a Google Sheet (continuo)"** (`AVuxY4kIW0NyLJix`).
It writes to a warehouse table instead of a Google Sheet, so the downstream tagging bot can read it with SQL.

## How it differs from n8n

| | n8n (current) | Hex (this) |
|---|---|---|
| Source | `GET /conversations`, newest first | `POST /conversations/search` on `updated_at > watermark` |
| Progress state | cursor in workflow static data | `max(updated_at_unix)` in the target table |
| Scope | one-off: the ~31k most recent, then stops | backfill from `BACKFILL_SINCE`, then keeps syncing |
| Updated conversations | missed after the first pass | re-fetched and upserted |
| Destination | Google Sheet (10M cell and 50k char/cell limits) | warehouse table |

Columns match the Sheet, plus `updated_at_unix` (watermark) and `synced_at`. `open` is a boolean instead of `si`/`no`.

## Setting it up

1. **Secret:** add a Hex secret `INTERCOM_TOKEN` with the same token as the n8n credential "intercom conversations".
2. **Cell 1 (SQL)**, output `watermark_df`. On the very first run the table won't exist yet: skip this cell, and the Python falls back to `BACKFILL_SINCE`.
   ```sql
   select max(updated_at_unix) as wm from intercom.conversations
   ```
3. **Cell 2 (Python):** paste `intercom_conversations_sync.py`. Output: `conversations_df`.
4. **Cell 3 (Writeback):** write `conversations_df` to `intercom.conversations_stg` with **overwrite**.
5. **Cell 4 (SQL), merge:**
   ```sql
   begin;
   delete from intercom.conversations
   using intercom.conversations_stg s
   where intercom.conversations.id = s.id;
   insert into intercom.conversations select * from intercom.conversations_stg;
   commit;
   ```
   For the first run, create the main table from staging instead: `create table intercom.conversations as select * from intercom.conversations_stg;`
6. **Schedule:** hourly. While the backfill is running, each run handles `MAX_CONVERSATIONS_PER_RUN` (5000, about 8-9 min) and the next run continues from there. The print at the end shows `remaining=`; once that reaches 0 the backfill is done.

## Things to check

- `MAX_TEXT_CHARS` (20,000) keeps `texto_completo` under Redshift's VARCHAR limit. If the destination is BigQuery or Snowflake you can raise it a lot.
- The table holds **customer emails and conversation text**. Restrict access to the schema accordingly.
- Once the Hex sync is confirmed, deactivate the n8n workflow so the two don't both call the API.
