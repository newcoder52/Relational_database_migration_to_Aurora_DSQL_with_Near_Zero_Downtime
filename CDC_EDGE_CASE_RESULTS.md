# CDC Edge-Case & Data-Type Stress Test Results

_Empirical results from stress-testing what breaks CDC in the DMS → S3 → Glue → Aurora DSQL
pipeline. Complements `ENGINEERING_RECORD.md` §4 (DDL limitations). All tests run hands-off on
a CDC-working table (`DMS_SAMPLE.CDC_EDGE`, cols: int PK, varchar(200), numeric(19,4),
double, integer, boolean, timestamptz), verified source-vs-target._

Also validated two wide sample tables (a 71-col table and a 27-col table)
end-to-end for **full load + validation** — see §4.

> **Status of this document (updated 2026-10-04).** The empirical results in §1–§2 were
> captured in September 2026 on the **Python-shell CDC engine**, before the 2026-10-02 →
> 2026-10-04 changes (copy-not-move `processed/`, the NULL-marker rule, per-column validation,
> `bytea`/BINARY GUARD, multi-column-PK skip, case-insensitive folder discovery, and the
> automatic Spark CDC fallback). **The original PASS/BREAK results are kept below as a record**;
> claims the later code changed are marked inline with **[OUTDATED …]** or an updated **Status**,
> and new behaviour is added as its own entry. Nothing has been re-run on real AWS or on the
> Spark CDC engine since — see the "Re-test needed" list in §5.
>
> **Scope.** Every CDC result below is for **single-column-PK tables**. Multi-column-PK tables
> are **not applied** by the main CDC job since 2026-10-03 (a separate job is required, or
> cutover waits — see §2.6 and `ENGINEERING_RECORD.md` 2026-10-03). For how to operate and
> recover from these cases, see `RUNBOOK.md` →
> [Known issues](RUNBOOK.md#known-issues-temporary) and
> [Troubleshooting → CDC](RUNBOOK.md#cdc).

---

## 1. PASS — CDC handles these correctly (byte-exact / correct final state)

| Category | Cases tested | Result |
|---|---|---|
| **NULLs** | all-null row (every nullable col NULL) | ✅ exact |
| **Booleans** | 0 and 1 (Oracle NUMBER(1) → DSQL boolean) | ✅ False / True preserved |
| **Numeric(19,4)** | max `999999999999999.9999`, negative `-123456.7890`, tiny `0.0001` | ✅ all exact |
| **Integer** | min `-2147483648`, max `2147483647` | ✅ exact |
| **Double** | negative small `-1e-9` | ✅ exact |
| **Text — CSV hard cases** | embedded commas, double-quotes, single-quotes, newlines, tabs+CR, unicode/emoji (`café résumé 日本語 🚀 ñ`), max-length (200), empty string, `backslash \ pct % semicolon;`, and a combined "CSV-killer" (`CSV,killer⏎"with",everything⇥together 日本`) | ✅ **9/10 byte-exact** — RFC4180/CSV escaping is solid (the 10th case, leading/trailing spaces, is covered in §2.2) |
| **Rapid PK churn** | 20 sequential UPDATEs to the same row | ✅ correct final state (int_val=20) |
| **Delete + reinsert same PK** | INSERT → DELETE → INSERT on same key | ✅ correct final ('reinserted') |
| **Large single transaction** | 500 rows in one commit | ✅ all 500 applied |
| **Wide tables** | 71-col + 27-col tables, full load | ✅ 3000=3000 (**count-only validator — see note**) |
| **UUID PK** | single-column uuid primary key | ✅ range-validatable (**see note**) |

> **[OUTDATED — what "NULL" and "exact" meant here]** These rows were captured when the
> jobs still turned `NULL`, `N/A`, `NA`, `NONE`, `(NULL)` and `\N` (any case) into NULL in every
> column. **Since 2026-10-03 that is no longer true** (see §2.5): a value is NULL **only** if the
> CSV field is empty **or** exactly equals the S3 endpoint's `CsvNullValue` (DMS default: the
> literal text `NULL`). `NA`, `N/A`, `NONE`, `(NULL)`, `\N` and lowercase `null` are now stored as
> text. An **empty string** does **not** round-trip as an empty string: Oracle stores `''` as
> NULL and the pipeline maps an empty CSV field to NULL, so it lands as NULL. The "all types
> correct" / UUID `match` results came from the **count-only** validator in use at the time; the
> validator now checks **every column** by its DSQL type (default `aggregate`, 2026-10-04, §2.7),
> and tables without a single rangeable PK (composite / fractional / no PK) are compared as **one
> whole-table range** rather than skipped. The wide-table and UUID results have **not** been
> re-run under the new validator.

**Takeaway:** the core CDC path (I/U/D, batching, CSV parsing/escaping, ordering, PK-keyed
apply) is robust — including the notoriously fragile CSV text cases and same-PK I/D/I churn.
**This covers single-column-PK tables only.** Multi-column-PK tables are not applied by this CDC
job (§2.6), and keyless tables behave differently (UPDATEs skipped; an insert+delete within one
file survives).

---

## 2. BREAK — edge cases that corrupt or lose data (mark these)

### 2.1 TIMESTAMP WITH TIME ZONE, non-UTC offset → WRONG INSTANT (silent) ✅ FIXED (2026-09-23)
- **Test:** 5 tz-aware timestamps with offsets `+05:30, -08:00, +00:00, +14:00, -05:00`.
- **Was:** all landed at the WRONG UTC instant — the offset was **stripped** and the wall-clock
  kept & reinterpreted as UTC (e.g. `12:30:45 +05:30` (= `07:00 UTC`) stored as `12:30 UTC`).
  Silent (no error).
- **Root cause (both paths fixed):**
  - **CDC** (`glue_cdc_continuous.py._normalize_timestamp_str`): stripped the offset. Now parses
    the offset and **converts to the true UTC instant** (emits `…+00:00`). New regex
    `_TS_PARSE_OFFSET` captures the datetime + offset; offset-less values behave as before.
  - **Full load** (`job2_load.py.normalize_timestamp`): stripped the offset via
    `pre_clean_timestamp`. Now `pre_clean_timestamp` KEEPS the offset, and `normalize_timestamp`
    parses **offset-aware first** (`to_timestamp(..., "…XXX")`) then falls back to offset-less.
    Added `spark.sql.session.timeZone=UTC` so `to_timestamp`/`date_format` emit the true UTC
    wall-clock. Same fix mirrored in `job3_validate.py`.
- **Verified FIXED (CDC re-test):**
  - `12:30:45.123456 +05:30` → `2026-06-15 07:00:45.123456 UTC` ✓ (matches source true UTC)
  - `00:00:00 -08:00` → `2026-01-01 08:00:00 UTC` ✓
  - `06:00:00 +14:00` → `2026-03-13 16:00:00 UTC` ✓ (correctly rolls back a day)

### 2.2 Leading/trailing spaces in VARCHAR → STRIPPED (silent) ✅ FIXED (CDC 2026-09-23; full load + validation 2026-10-03)
- **Test:** `'   leading/trailing spaces   '` (29 chars).
- **Was:** target = `'leading/trailing spaces'` (23 chars) — leading AND trailing spaces
  removed (silent).
- **Root causes (two):**
  1. **CDC apply** — `_coerce_null()` in `glue_cdc_continuous.py` returned the *stripped* value
     (the trim was meant only to detect null sentinels). **Fixed 2026-09-23:** it now returns the
     ORIGINAL untrimmed value for text; typed columns (numeric/int/uuid/boolean/timestamp/date/
     float/json/bytea) are trimmed only because they re-parse/cast, and a whitespace-only typed
     value becomes NULL.
  2. **Full load + validation** — Spark's CSV reader defaults `ignoreLeadingWhiteSpace` /
     `ignoreTrailingWhiteSpace` to `true`; those were set to `false` in `CSV_READ_OPTIONS` on
     2026-09-23. **But the loader's and validator's NULL-sentinel step still ran
     `.otherwise(trim(col))` on every column until the 2026-10-03 NULL-marker change** (see §2.5).
     So **text loaded by a full load before 2026-10-03 could still be trimmed**, and because the
     validator trimmed too, validation could not catch it. Only the CDC path was re-tested on
     2026-09-23. See `USAGE_GUIDE.md` §4b for how to find and fix rows loaded by an earlier
     version.
- **Verified FIXED (CDC re-test, 2026-09-23):** `'   spaces preserved now   '`, `'  pad both  '`,
  `'trailing only   '` all land **byte-exact** (source == target).
- **[NOT re-tested]** The 2026-10-03 full-load / validation fix has not been re-run on real
  Spark/DSQL.

### 2.3 Smallest-normal double (~2.2e-308) → underflow to 0.0 ⚠️ LOW
- **Test:** `2.2250738585072014E-308` (the smallest *normal* double; the heading in earlier
  revisions wrongly called it "subnormal").
- **Result:** landed as `0.0`.
- **Impact:** minor — only affects values in the ~1e-308 range (rare in real data).
- **Root cause (UNCONFIRMED):** likely a precision loss in the double string round-trip
  (source/CSV/parse). The CDC job passes float text through unchanged to `::double precision`, so
  the loss is more likely in Oracle or DMS than in the Glue code — not verified. Check the DMS CSV
  in S3 to see whether the value is already `0` there.
- **Not detectable by validation:** the validator compares float sums with a relative tolerance
  (1e-9 for double), and it compares against the S3 CSV, so it cannot flag a ~1e-308 → 0 change or
  a DMS-side loss.

### Not a pipeline issue (recorded for completeness)
- `-1.7976931348623157E308` (double max-negative) was **rejected at the SOURCE insert** by
  `oracledb` (`ORA-01426 numeric overflow`) — never reached CDC. Oracle BINARY_DOUBLE input
  limitation, not a pipeline break.

### 2.5 NULL look-alike text (NA, N/A, NONE, (NULL), \N, null) → silently NULL ✅ FIXED (2026-10-03)
- **Was (up to and incl. the Sep run above):** the load, validation and CDC jobs turned `NULL`,
  `N/A`, `NA`, `NONE`, `(NULL)` and `\N` (any case, padding ignored) into NULL in **every** column.
  A real text value such as the product code `NA` was silently lost, or (into a NOT NULL column)
  rejected — which blocks the table in CDC.
- **Now:** a value is NULL **only** if the CSV field is empty **or** exactly equals the S3
  endpoint's `CsvNullValue` (DMS default: the literal text `NULL`). `resolve_task` reads the
  marker from the endpoint and `create_glue_jobs` passes `--csv_null_value` (`__EMPTY__` when the
  marker is the empty string) to the load, load-big, validate and CDC jobs. All three jobs apply
  the identical rule.
- **Edge case that remains:** a real source text value equal to the marker (e.g. the word `NULL`
  with the default marker) is still stored as NULL. Set `CsvNullValue` to something that can't
  appear in the data before full load if this matters.
- **Typed columns:** a typed column (number, date, uuid, boolean, bytea) holding `NA`-like text
  now **fails its cast** instead of becoming NULL silently; in CDC that **blocks the table**.
  Recover by fixing the data/mapping, then
  `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'` — **never
  delete the row** (applied files stay in the folder and would all be replayed).
- **Data loaded by older versions is NOT corrected automatically** — see `USAGE_GUIDE.md` §4b.
- **[NOT re-tested on real AWS]** verified in the cross-job plumbing simulation only
  (`ENGINEERING_RECORD.md` 2026-10-03).

### 2.6 Multi-column-PK tables → NOT applied by the main CDC job ✅ CHANGED (2026-10-03)
- **Was:** a table whose primary key has more than one column was treated as keyless — every
  UPDATE was skipped, DELETEs matched on every column, and a `_cdc_file` column was added.
- **Now:** the main CDC job raises `MultiColumnKeyTable` and **never processes** these tables; it
  lists them at startup and leaves their `cdc_control` rows for a **separate composite-key CDC
  job**. That job must write `cdc_control.cdc_file_status` the same way (see
  `RUNBOOK.md` → [Rules for many tasks](RUNBOOK.md#rules-for-the-task-list)). The drain check still
  waits for those tables, so **cutover cannot finish until the separate job has caught up**.
- **Not in the repo:** the separate composite-key CDC job is not shipped here (known gap — see
  `RUNBOOK.md` → [Known issues](RUNBOOK.md#known-issues-temporary) and the checklist WP10).
- All CDC results in §1–§2 are for **single-column-PK** tables only.

### 2.7 Binary (RAW/BLOB → bytea) stored as ASCII of the hex text (silent) ✅ FIXED 2026-10-04 (simulator only)
- **Not in the test table** (`CDC_EDGE` has no binary column), so this was never exercised in the
  Sep run; recorded here because the data-type handling changed.
- **Was:** DMS writes RAW/BLOB as hex text; the load and CDC cast it as `'<hex>'::bytea`, which
  stores the **ASCII characters of the hex text** (twice as long, wrong bytes) with no error.
- **Now:** the load, CDC and validation convert it to `'\x' + lowercase hex` (real bytes). A value
  that isn't valid hex stops the table with a `BINARY GUARD` block (recover as in §2.5). In the
  schemas tested so far every RAW column maps to `uuid` (no `bytea` columns), so this is for future
  schemas.
- **[NOT tested on real DMS/DSQL]** verified against stand-ins only.

### 2.8 processed/ files and folder letter-case (behaviour the doc previously didn't cover)
- **processed/ copy-not-move (2026-10-02):** applied CDC files are now **copied** to
  `<schema>/<table>/processed/`, never moved — no CDC file is deleted by the pipeline. Files are
  skipped by the high-water mark (`cdc_status.last_done_file`), not by where they sit. This does
  not change any §1/§2 result; it is noted so the earlier "move" assumption isn't carried forward.
- **Case-insensitive folder discovery (2026-10-03):** discovery now matches the DMS folder for a
  table **case-insensitively** and honours the endpoint `BucketFolder`. **Known latent bug:** a
  table that is *empty at full load* and whose DMS folder differs only in letter-case can be
  permanently mis-matched (the CDC job writes `processed/_manifest.json` into the guessed-case
  folder, which blocks the later case re-find), and the drain check then reports it caught up —
  cutover can succeed with that table never applied. This does **not** affect single-column-PK
  tables that have data at full load (the folder is found). See
  `RUNBOOK.md` → [Known issues](RUNBOOK.md#known-issues-temporary) /
  [Troubleshooting → CDC](RUNBOOK.md#cdc) and the checklist (F02).

---

## 3. Summary table

| Edge case | Verdict | Severity | Status |
|---|---|---|---|
| NULLs, booleans, numeric/int boundaries | PASS | — | current (but see §2.5 for the NULL rule) |
| Text: commas/quotes/newlines/tabs/unicode/emoji/max-len/backslash | PASS (byte-exact) | — | current |
| Rapid PK churn, delete+reinsert, 500-row txn | PASS | — | current (single-column PK) |
| **timestamptz non-UTC offset** | BREAK — wrong instant, silent | HIGH | **FIXED 2026-09-23** (§2.1) |
| **varchar leading/trailing spaces** | BREAK — stripped, silent | MEDIUM | **FIXED** — CDC 2026-09-23; full load/validation 2026-10-03 (§2.2) |
| **NULL look-alike text (NA/N/A/NONE/(NULL)/\N/null)** | BREAK — silently NULL | HIGH | **FIXED 2026-10-03** (§2.5) |
| **binary RAW/BLOB → bytea** | BREAK — ASCII-of-hex, silent | MEDIUM | **FIXED 2026-10-04, simulator only** (§2.7) |
| multi-column-PK table CDC | not applied by main job | — | **CHANGED 2026-10-03**; separate job required, not in repo (§2.6) |
| smallest-normal double (~2.2e-308) | BREAK — underflow to 0 | LOW | Open; root cause unconfirmed (§2.3) |
| new-schema LogMiner gap | BREAK — zero CDC captured, silent | HIGH | Open (DBA-side; §4) |

---

## 4. New-schema CDC blocker (environmental, DMS-side — important)

The two wide sample tables (above) were placed in a
**newly-created schema**. Full load + validation **PASSED** (3000=3000 each, all wide
types — uuid/boolean/double/numeric/timestamptz — correct; UUID PK range-validates). **But CDC
captured ZERO changes** for these tables.

- **Confirmed isolated:** an identical insert into an existing-schema table
  (`DMS_SAMPLE.CDC_EDGE`, and `SPORT_TEAM`) was captured immediately. A newly-created table in
  an **existing** schema also CDCs fine. Only the **new schema** fails.
- **Tried (none fixed it):** table-level `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` (DB-level was
  already min+pk+all=YES); stop+resume; delete+recreate the task fresh; grant the DMS source
  user full DML on the tables + restart.
- **Root cause (Oracle-side):** a schema created **after** the Oracle LogMiner mining
  dictionary can't be resolved from redo — DMS reads redo (LogMiner boundaries advance) but
  finds "No Event / No records" for the new schema's objects and silently drops them.
- **The pipeline cannot detect this.** With no CDC files, the drain check treats the table as
  caught up, so **cutover can succeed with zero changes applied**. Job 3 validates only the full
  load against S3, and the CDC job's own sampled validation is off by default.
- **Not covered by the fleet Preflight.** The `preflight_tasks` Lambda (the fleet "Preflight"
  state) only validates task ARNs, suffixes, table lists and the DSQL schema count — it does
  **not** look at Oracle/LogMiner. "Preflight" here is **not** a safeguard for this case.
- **Fix (DBA-side, outside the pipeline):** before CDC starts, confirm every source schema
  existed before the LogMiner mining dictionary was built, or rebuild it
  (`DBMS_LOGMNR_D.BUILD` / redo-log dictionary). This belongs in the RUNBOOK prerequisites — see
  `RUNBOOK.md` → [Prerequisites checklist](RUNBOOK.md#prerequisites-checklist).
- **Operator first step before blaming LogMiner:** check the DMS task's table statistics and the
  S3 folder `<BucketFolder>/<schema>/<table>/` for timestamped CDC files. If DMS shows changes but
  the files are under a different-case folder, it is a path/case issue (§2.8), not LogMiner.

---

## 5. Open items and re-tests needed

The two silent-corruption BREAKs that this doc was written to flag are **already fixed in the
code** (timestamptz offset, §2.1; varchar whitespace, §2.2), as are the NULL rule (§2.5) and
binary/bytea (§2.7). What remains:

**Still open**
1. **new-schema LogMiner gap (HIGH, DBA/runbook):** document the LogMiner-dictionary rebuild as a
   mandatory prerequisite; optionally add a "CDC file seen per table" check before cutover
   (the pipeline cannot detect this today — §4).
2. **smallest-normal double → 0.0 (LOW):** confirm where the loss happens (check the DMS CSV in
   S3) or accept as a known boundary; validation can't flag it (§2.3).
3. **composite-key CDC job (not in repo):** the separate multi-column-PK job that §2.6 relies on
   is not shipped here; until it exists, multi-column-PK tables are not migrated and cutover
   waits (checklist WP10).

**Re-test needed on real AWS / Spark CDC** (none of the Oct 2–4 changes were re-run against these
edge cases):
- the NULL-marker rule (§2.5) end-to-end;
- whitespace preservation in the **full load** and validator (§2.2, fixed 2026-10-03);
- binary/`bytea` on a real `bytea` column (§2.7);
- the per-column validator (§2.7 of ENGINEERING_RECORD / 2026-10-04) on the wide and UUID-PK
  tables;
- the whole edge-case suite on the **Spark CDC engine** (the automatic fallback runs the same
  `glue_cdc_continuous.py` under Spark — never exercised here).

> **Tested on:** September 2026, Python-shell CDC engine, a DSQL test cluster in `us-east-1`
> (exact commit not recorded). The 2026-09-23 timestamptz/whitespace CDC re-test is the newest
> empirical run in this document.
