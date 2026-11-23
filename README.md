# SkyPoints Member Pipeline — Incubyte Data Engineer Assessment

Loads the daily member flat file and the partner JSON redemption feed into Snowflake,
splits members into one table per country, and validates everything on the way in.

## The idea in one picture

```
 MEMBER_YYYYMMDD.txt  ──►  RAW_MEMBER_FEED  ──►  STG_MEMBER  ──►  TABLE_INDIA / TABLE_USA / TABLE_AUSTRALIA / ...
 (pipe file)               all text, never     typed, Age,        one table per country, latest record wins
                           fails to load       Stale_Member,           │
                                               DQ verdict              ▼ rejects parked in DQ_REJECT_MEMBER

 REDEMPTION_*.json    ──►  RAW_REDEMPTION_FEED (VARIANT) ──► FACT_REDEMPTION (1 row per txn) ──► joined to members on member_id
```

Three layers, each with one job: **land it safely → clean & check it → publish it.**

## Files (one per deliverable)

| # | Deliverable | File |
|---|---|---|
| 1 | DDL: raw, staging, country tables (+ DQ + redemption tables) | `sql/01_ddl.sql` |
| 2 | Staging load with **Age** and **Stale_Member** | `sql/02_load_staging.sql` |
| 3 | Split into country tables, **latest record wins** | `sql/03_split_by_country.sql` |
| 4 | Flatten the JSON + how to join to members | `sql/04_redemptions_json.sql` |
| 5 | Validations | rules in `02` and `04`, batch checks in `sql/05_validations.sql` |
| 6 | Live demo | `python3 run_demo.py` (local, no Snowflake needed) |

`skypoints/pipeline.py` is a Python version of the same logic, so every rule can be run and tested on a laptop.
`tests/` has 29 unit tests: `python3 -m unittest discover -s tests -t . -v`

## How each deliverable works (plain English)

**1. Tables.** RAW is all text, so a bad value can never make a file fail to load. STG has real types (DATE, NUMBER)
plus the derived columns and a data-quality verdict. Each country table has the same columns (built from one template with
`CREATE TABLE ... LIKE`). Adding a country is one row in `ref_country` and one `CREATE TABLE`, no code change.

**2. Age and Stale_Member.**
- *Age* = whole years. `DATEDIFF('year')` alone is wrong (it only counts year boundaries), so I subtract 1 if the birthday hasn't happened yet.
- *Stale_Member* = `DATEDIFF('day', last_flight, today) > 90`. The run date is a parameter, so replays give repeatable results.
- The sample DOB `03051985` reaches staging as `3051985` (leading zero lost). I keep DOB as text and `LPAD` it back to 8 digits before parsing.

**3. Latest record wins.** Per member, pick one winner: newest file date, and inside one file the last row (`ROW_NUMBER ... QUALIFY`).
Then for each country, `MERGE` the winners in and `DELETE` the member from every other country table, so a member who moved
India→USA exists in exactly one table. Two guards protect against re-delivered old files: the MERGE only updates when the incoming
file date is newer-or-equal, and rows older than anything already stored are dropped up front.
(The unit tests found that second guard was missing in my first draft. That is why it's there.)

**4. JSON.** Load each document into a `VARIANT` column, then `LATERAL FLATTEN` the `redemptions` array so each transaction becomes a row.
`txn_id` is the key and `MERGE` keeps the newest version, because the sample shows `RX10092` going `PENDING` to `COMPLETED`.
To join: `fact_redemption.member_id = v_member_all.member_id` (a `UNION ALL` view over the country tables), as a `LEFT JOIN` so unknown members
show up as orphans instead of vanishing.

**5. Validations.** Two kinds:
- **ERROR → row is rejected** (saved in `dq_reject_member`, never silently dropped): missing Member ID / Name / Enrollment Date, impossible dates (`20241399`),
  future enrollment, ID longer than 18, unknown country.
- **WARNING → row loads, issue logged:** short member ID, flight before enrollment, bad DOB, invalid tier/active flag (these optional values are set to NULL), unknown redemption status.
- **Batch checks** (`05`): RAW vs STG row counts, duplicate keys, member in two country tables (must be 0), orphan redemptions, a suspiciously common DOB.

## Problems I found in the sample data

| What I saw | Why it matters | What I did |
|---|---|---|
| Member IDs `22345`, `2256` vs 6 digits for the rest | Truncated or padded IDs can create false duplicates or non-matches | `SHORT_MEMBER_ID` warning |
| Countries `PHIL`, `AU` mixed with `USA`, `IND`, `CAN` | Routing would miss members | `ref_country` lookup maps aliases to one clean code; unknown codes are rejected |
| DOB `3051985` in staging | Leading zero lost, so the date parses wrongly | Keep as text, `LPAD` to 8 |
| Every member has DOB `03051985` | Looks like a default value | Batch check for a suspiciously common DOB |
| Spec marks **Member Name** as the key, not Member ID | Names aren't unique; looks like the flags are swapped | I treat **Member_Id** as the key |
| Spec has 11 columns incl. Post Code; sample has 10 | Layout drift | Post_Code is nullable and loads when present |
| Staging has `County`, `FLAG`; the spec says `Country`, `Is_Active` | Naming drift | Standard names in my tables |
| Partner `"SkyPoints"` on a redemption | Own program listed as an external partner | `PARTNER_IS_OWN_PROGRAM` warning |
| The 3 Excel files: USA dates like `6152022` | Integer lost its leading zero (MMDDYYYY) | Pad to 8, then parse |
| AUS: text `'NULL'` and date `2021-13-13` | `'NULL'` is missing; month 13 is impossible | `'NULL'` becomes empty; the bad mandatory date is rejected (Jonnathan) |
| IND has an extra `Individual or Corporate` column; the others lack State/Country/DOB | Each country has its own layout | One mapping per country into the standard layout |
| IDs `1,2,3` appear in all three Excel files | Different people share an ID | Prefixed with country (`IND-1`) |

## Billions of rows a day

- **Incremental only.** Every step filters on `batch_id` (today's files). History is never reread.
- **Land with `COPY INTO`** (it skips files it already loaded). Many mid-size files (~100–250 MB) load in parallel; JSON as one document per line.
- **`CLUSTER BY (member_id)`** on country tables and `txn_date` on redemptions, so MERGE and joins read only the micro-partitions they need.
- **Set-based SQL** everywhere (no row loops). The only loop is over the handful of countries.
- **Idempotent.** Re-running a file gives the same result (MERGE + latest-wins), so retries are safe.
- **Orchestration** (not built): Snowflake Tasks or Airflow, with `ERROR`-severity rows in `dq_results` stopping the run.
- Raw tables can be kept short-term (e.g. Time Travel or a retention task) to control storage.

## Assumptions (easy to change)

- DOB is `DDMMYYYY` (per the spec); `03051985` is 3 May 1985. Either reading gives the same age in the sample.
- "Latest" = newest file date, then last row in the file. The file date comes from the 8-digit date in the file name.
- A member with **no** flight date is treated as stale.
- Country codes are standardised to ISO alpha-3 (`PHIL` becomes `PHL`).
- File name pattern isn't specified in the PDF; I assumed `MEMBER_YYYYMMDD.txt` and `REDEMPTION_YYYYMMDD.json`.

## Not tested against a real Snowflake account

I had no Snowflake access, so the **SQL files are untested**. I wrote them in Snowflake syntax and kept them
logically identical to `pipeline.py`, which **is** tested (29 tests pass). Before the interview, run the SQL once in a
trial account and fix any syntax slips.

## How I used AI

I used Claude to read the brief and the three Excel files, draft the SQL and Python, and write tests. I reviewed every rule myself.
The tests then caught a real bug (replayed old files could re-insert a member who had moved) that I had not spotted on my own, so
I fixed it in both the SQL and Python. The commit history shows how it grew: DDL → staging → split → JSON → validations → tests.

## Run it

```bash
python3 run_demo.py                                  # day 1, day 2 (a member moves), JSON, Excel files
python3 -m unittest discover -s tests -t . -v        # 29 tests
```
