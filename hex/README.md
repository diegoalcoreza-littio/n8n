# Intercom conversations in Hex

Hex replacement for the n8n workflow **"Diego - Intercom a Google Sheet (continuo)"** (`AVuxY4kIW0NyLJix`).
Two Hex projects write to one warehouse table, `intercom.conversations`:

| Job | File | Covers | Schedule |
|---|---|---|---|
| Daily | `intercom_daily_sync.py` | conversations **updated** since 2026-01-01, then whatever changed since the last run | daily (hourly until the first load finishes) |
| History | `intercom_history_sync.py` | conversations **created** before 2026-01-01, walking back to the beginning | hourly until it prints `DONE`, then off |

Columns match the Sheet, plus `created_at_unix`, `updated_at_unix`, `sync_source` (`daily`/`history`) and `synced_at`.
Each job uses only its own rows (`sync_source`) to decide where to continue, so they can run at the same time.

## Setup (both projects)

Token: a Hex secret named `INTERCOM_TOKEN`. If you can't create secrets, put `INTERCOM_TOKEN = "..."` in its **own Python cell above the script**, so pasting a new version of the script doesn't wipe it. Never commit the token to this repo.

| Cell | Daily project | History project |
|---|---|---|
| 1. SQL → df | `watermark_df`:<br>`select max(updated_at_unix) from intercom.conversations where sync_source = 'daily'` | `history_watermark_df`:<br>`select min(created_at_unix) from intercom.conversations where sync_source = 'history'` |
| 2. Python | `intercom_daily_sync.py` | `intercom_history_sync.py` |
| 3. Writeback (overwrite) | `conversations_df` → `intercom.conversations_stg` | `conversations_df` → `intercom.conversations_history_stg` |
| 4. SQL merge | see below with `conversations_stg` | see below with `conversations_history_stg` |

Merge (cell 4):
```sql
begin;
delete from intercom.conversations
using intercom.conversations_stg s
where intercom.conversations.id = s.id;
insert into intercom.conversations select * from intercom.conversations_stg;
commit;
```

**Very first run** (the table doesn't exist yet): run the **daily** project first, skip cell 1, and replace cell 4 with
`create table intercom.conversations as select * from intercom.conversations_stg;`
After that the table exists, and both projects work with all four cells.

## Getting through the first load

- Every run stops by itself after **40 minutes** (`TIME_BUDGET_MINUTES`) or at its cap, keeps what it fetched, and the next run continues. It only ever stops between whole seconds, so nothing is skipped, even after a bulk update of thousands of conversations.
- Progress is printed every 10 search pages and every 250 conversations fetched, with the rate per second.
- **Daily:** up to 5,000 per run. Run it hourly until it prints `caught up to today`, then switch to daily. It leaves out the last 10 minutes (Intercom's search index lags slightly) and picks them up next run.
- **History:** up to 10,000 per run. About 500k conversations means roughly 50 hourly runs, so about 2 days. Turn the schedule off when it prints `DONE`.

## Notes

- A conversation created before 2026 but updated during it gets fetched by both jobs. The merge keeps the newest copy.
- `MAX_TEXT_CHARS` (20,000) keeps text under Redshift's VARCHAR limit. You can raise it a lot on BigQuery or Snowflake.
- The table holds **customer emails and conversation text**. Restrict access to the schema accordingly.
- Once this is running, deactivate the n8n workflow.
