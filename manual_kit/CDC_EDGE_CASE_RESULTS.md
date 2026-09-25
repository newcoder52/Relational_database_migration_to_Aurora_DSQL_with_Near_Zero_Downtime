# CDC Edge-Case & Data-Type Stress Test Results

_Empirical results from stress-testing what breaks CDC in the DMS → S3 → Glue → Aurora DSQL
pipeline. Complements `ENGINEERING_RECORD.md` §4 (DDL limitations). All tests run hands-off on
a CDC-working table (`DMS_SAMPLE.CDC_EDGE`, cols: int PK, varchar(200), numeric(19,4),
double, integer, boolean, timestamptz), verified source-vs-target._

Also validated two wide customer tables (a 71-col table and a 27-col table)
end-to-end for **full load + validation** — see §4.

---

## 1. PASS — CDC handles these correctly (byte-exact / correct final state)

| Category | Cases tested | Result |
|---|---|---|
| **NULLs** | all-null row (every nullable col NULL) | ✅ exact |
| **Booleans** | 0 and 1 (Oracle NUMBER(1) → DSQL boolean) | ✅ False / True preserved |
| **Numeric(19,4)** | max `999999999999999.9999`, negative `-123456.7890`, tiny `0.0001` | ✅ all exact |
| **Integer** | min `-2147483648`, max `2147483647` | ✅ exact |
| **Double** | negative small `-1e-9` | ✅ exact |
| **Text — CSV hard cases** | embedded commas, double-quotes, single-quotes, newlines, tabs+CR, unicode/emoji (`café résumé 日本語 🚀 ñ`), max-length (200), empty string, `backslash \ pct % semicolon;`, and a combined "CSV-killer" (`CSV,killer⏎"with",everything⇥together 日本`) | ✅ **9/10 byte-exact** — RFC4180/CSV escaping is solid |
| **Rapid PK churn** | 20 sequential UPDATEs to the same row | ✅ correct final state (int_val=20) |
| **Delete + reinsert same PK** | INSERT → DELETE → INSERT on same key | ✅ correct final ('reinserted') |
| **Large single transaction** | 500 rows in one commit | ✅ all 500 applied |
| **Wide tables** | 71-col + 27-col tables, full load | ✅ 3000=3000, all types correct |
| **UUID PK** | single-column uuid primary key | ✅ range-validatable (validation `match`, unlike composite/fractional PKs) |

**Takeaway:** the core CDC path (I/U/D, batching, CSV parsing/escaping, ordering, PK-keyed
apply) is robust — including the notoriously fragile CSV text cases and same-PK I/D/I churn.

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

### 2.2 Leading/trailing spaces in VARCHAR → STRIPPED (silent) ✅ FIXED (2026-09-23)
- **Test:** `'   leading/trailing spaces   '` (29 chars).
- **Was:** target = `'leading/trailing spaces'` (23 chars) — leading AND trailing spaces
  removed (silent).
- **Root causes (two, both fixed):**
  1. **CDC apply** — `_coerce_null()` in `glue_cdc_continuous.py` returned the *stripped* value
     (the trim was meant only to detect null sentinels). Fixed: it now returns the ORIGINAL
     untrimmed value for text; `convert_value()` strips only for typed categories
     (numeric/int/uuid/boolean/timestamp) which re-parse/cast.
  2. **Full load** — Spark's CSV reader defaults `ignoreLeadingWhiteSpace` /
     `ignoreTrailingWhiteSpace` to `true`. Fixed: set BOTH to `false` in `CSV_READ_OPTIONS`
     in `job2_load.py` AND `job3_validate.py` (so validation compares like-for-like).
- **Verified FIXED:** CDC re-test — `'   spaces preserved now   '`, `'  pad both  '`,
  `'trailing only   '` all land **byte-exact** (source == target).

### 2.3 Subnormal (denormalized) double → underflow to 0.0 ⚠️ LOW
- **Test:** `2.2250738585072014E-308` (smallest normal double, boundary of subnormal range).
- **Result:** landed as `0.0`.
- **Impact:** minor — only affects values in the ~1e-308 subnormal range (rare in real data).
- **Root cause:** precision loss in the double string round-trip (source/CSV/parse).

### Not a pipeline issue (recorded for completeness)
- `-1.7976931348623157E308` (double max-negative) was **rejected at the SOURCE insert** by
  `oracledb` (`ORA-01426 numeric overflow`) — never reached CDC. Oracle BINARY_DOUBLE input
  limitation, not a pipeline break.

---

## 3. Summary table

| Edge case | Verdict | Severity |
|---|---|---|
| NULLs, booleans, numeric/int boundaries | PASS | — |
| Text: commas/quotes/newlines/tabs/unicode/emoji/empty/max-len/backslash | PASS (byte-exact) | — |
| Rapid PK churn, delete+reinsert, 500-row txn | PASS | — |
| **timestamptz non-UTC offset** | **BREAK — wrong instant, silent** | **HIGH** |
| **varchar leading/trailing spaces** | **BREAK — stripped, silent** | MEDIUM |
| subnormal double | BREAK — underflow to 0 | LOW |

---

## 4. New-schema CDC blocker (environmental, DMS-side — important)

The two wide customer tables (above) were placed in a
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
- **Fix (DBA-side, outside the pipeline):** rebuild the LogMiner/redo dictionary
  (`DBMS_LOGMNR_D.BUILD` / redo-log dictionary), or ensure the schema exists before the CDC
  mining dictionary is built.
- **Pre-migration check to add:** "was the schema present before CDC/dictionary setup?" — a new
  schema needs a dictionary rebuild before CDC will capture it. (Add to the preflight
  assessment.)

---

## 5. Recommended fixes (priority order)
1. **timestamptz offset (HIGH):** convert by the source offset to a true UTC instant instead of
   stripping it — silent time-shift is the most dangerous finding.
2. **varchar whitespace (MEDIUM):** set `ignoreLeadingWhiteSpace=false` +
   `ignoreTrailingWhiteSpace=false` on the CSV reads.
3. **new-schema CDC (DBA/runbook):** document the LogMiner-dictionary rebuild requirement; add
   the preflight check.
4. subnormal double (LOW): accept as a known boundary, or use a higher-precision path if needed.
