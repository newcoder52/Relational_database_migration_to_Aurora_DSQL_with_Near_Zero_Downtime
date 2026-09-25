# Engineering Record — DMS → S3 → Glue → Aurora DSQL Pipeline

_Comprehensive record of the pipeline architecture, every bug found and fixed, and the
empirically-tested DDL support/limitations. Companion to `RUNBOOK.md` (deploy steps) and
`USAGE_GUIDE.md` (end-to-end operation)._

Last validated: 2026-09-22 (a test account in us-east-1,
a DSQL cluster).

---

## 1. What this pipeline does

Migrates data from a source database (Oracle in test) to Aurora DSQL, using **DMS → S3
(CSV) → AWS Glue → DSQL**. DMS never writes to DSQL directly; it lands CSVs in S3 and Glue
jobs load them into DSQL. One **Step Functions state machine per DMS task** orchestrates the
flow. Supports **full load** (bulk) + **CDC** (ongoing change capture) + **cutover**.

```
Source DB ──DMS(full-load-and-cdc)──▶ S3 (CSV)  ──AWS Glue──▶ Aurora DSQL
                                       │                        ▲
              full load: LOAD*.csv ────┤   Job1 discovery       │
              CDC:  <ts>.csv (Op col) ─┘   Job2 load ───────────┤
                                           Job3 validate ───────┤
                                           CDC job (continuous) ─┘
```

### Components
- **Glue scripts** (`manual_kit/scripts/`): `job1_discovery.py`, `job2_load.py`,
  `job3_validate.py`, `glue_cdc_continuous.py`. Staged to `s3://<bucket>/scripts/`.
- **Lambdas** (`manual_kit/lambdas/`): `resolve_task`, `driver_discovery`, `create_glue_jobs`,
  `plan_split`, `drain_check`, `stop_cdc_run`, `drop_tags`.
- **State machines** (`manual_kit/stepfunctions/`): `startup.asl.json` (full-load→validate→CDC),
  `cutover.asl.json` (drain + finalize). One instance per DMS task.
- **Control tables** in DSQL schema `cdc_control`: `cdc_status`, `cdc_file_status`,
  `cdc_chunk_log`, `cdc_apply_exceptions`, `cdc_skipped_ops`, `cdc_validation_failures`.

---

## 2. Key S3 / DMS facts (empirically confirmed)

### DMS S3 object layout (default, no `CdcPath`)
- **Full load**: `<BucketFolder>/<schema>/<table>/LOAD00000001.csv` (hex counter
  `LOAD00000001`..`LOAD0000000F`..). First column is `dms_timestamp` (NO `Op` column).
- **CDC / cached changes**: `<BucketFolder>/<schema>/<table>/<YYYYMMDD-HHMMSSmmm>.csv`,
  first column is **`Op`** (`I`/`U`/`D`), then `dms_timestamp`, then table columns.
- **Full load and CDC share the SAME per-table directory** — distinguished ONLY by filename
  (`LOAD*` vs timestamp) and layout (`Op` column presence). There is NO separate `cdc/` folder
  unless the endpoint has a `BucketFolder`.
- **Cached changes** (changes captured during full load, applied at
  `STOPPED_AFTER_CACHED_EVENTS`) are written as a **normal CDC file** (timestamp name + `Op`
  column). There is no distinct "cached-changes" file type.

### Casing
- DMS writes the S3 path AND column headers using the **source's native case (UPPERCASE for
  Oracle)** UNLESS the table-mapping has transforms: `rename schema`, `convert-lowercase
  table`, `convert-lowercase column`. The pipeline REQUIRES these transforms (all config /
  scripts assume lowercase paths + columns).

### Endpoint settings that matter (all now auto-derived by `resolve_task`)
- `BucketFolder` → the CDC/full-load path root (`cdc_root`). Empty → flat `<schema>/<table>/`.
- `AddColumnName=true` → CSVs carry a **header row** (the pipeline relies on this).
- `TimestampColumnName` (default `dms_timestamp`) → the CDC watermark column.
- `DatePartitionEnabled` → MUST be `false` (date partitioning breaks the flat per-table path
  the whole pipeline assumes; `resolve_task` now **fails fast** if it is true).
- `CdcPath` / `PreserveTransactions` → MUST be unset (mutually exclusive with `AddColumnName`;
  `resolve_task` fails fast if present).

---

## 3. Bugs found and fixed (chronological, with root cause + fix + verification)

### BUG 1 — 3-table full-load column corruption
- **Symptom**: `nfl_data`, `nfl_stadium_data`, `sport_location` loaded with shifted columns.
- **Root cause**: the loader's `recursiveFileLookup=true` read stale `processed/` CDC files
  (16-col, leading `Op`) alongside 15-col `LOAD*` files → column shift. NOT a parser bug.
- **Fix**: defense-in-depth — `job2 _is_full_load_key()` (per-file, requires `LOAD*.csv`,
  excludes `processed/`+`failed/`), `pathGlobFilter="LOAD*.csv"` on job2+job3, and a
  first-column `Op` backstop (`assert_not_cdc_layout`) in job1/job2/job3 that hard-fails if a
  CDC-layout file is ever read as full-load.
- **Verified**: full load row-exact on all tables after the guards.

### BUG 2 — CDC job `UnknownServiceError: Unknown service 'dsql'`
- **Symptom**: every CDC job run FAILED at the first DSQL connect (`_get_cached_dsql_token`).
- **Root cause**: the CDC (pythonshell) job needs a modern boto3 (≥1.34, which knows the
  `dsql` service) delivered as S3 wheels on `--extra-py-files`. The boto3 wheels had been moved
  OUT of `drivers/` (to keep them off the Spark jobs), so `driver_discovery` (which scans one
  folder) no longer returned them; the startup SM passed that drivers-only list as
  `--extra-py-files`, **overriding** the boto3-inclusive default `create_glue_jobs` set. Net:
  CDC ran with no modern boto3 → bundled old boto3 has no `dsql` client → crash.
- **Fix** (per-job driver folders): created three S3 folders —
  - `driver-fullload/`, `driver-validation/` = DSQL drivers ONLY (Spark jobs; boto3 via
    `--additional-python-modules`),
  - `driver-cdc/` = DSQL drivers **+ modern boto3/botocore** (pythonshell CDC job).
  The startup SM now calls `driver-discovery` 3× (once per folder) and routes each list to its
  job type; `create_glue_jobs` takes an optional `cdcExtraPyFiles` for the CDC role's stored
  default. Why split: boto3 wheels on a Spark job's `--extra-py-files` break botocore's
  data-dir resolution ("DataNotFoundError: endpoints"), so Spark jobs must NOT get them.
- **Verified**: CDC job loads `driver-cdc/boto3-1.42.97`, `dsql` client works, CDC applies.

### BUG 3 — `cdc_root` mismatch (CDC job looked in the wrong S3 folder)
- **Symptom**: after BUG 2, CDC job ran but applied nothing (0 rows), never saw the CDC files.
- **Root cause**: the SM hardcoded `cdc_root="cdc"`, so the job listed
  `cdc/<schema>/<table>/` (empty), while the no-`BucketFolder` endpoints write to the flat
  `<schema>/<table>/`.
- **Fix** (auto-derive, generic): `resolve_task` returns `cdcRoot` = the endpoint's
  `BucketFolder` (or the `.` sentinel when none, since Glue can't pass an empty arg). The SM
  captures it in the `ResolveTask` ResultSelector and passes `$.resolved.cdcRoot` to
  `CreateGlueJobs`, `PlanSplit`, and cutover `DrainCheck`. No hardcoded value anywhere.
  `drain_check.py` was also fixed to normalize the `.` sentinel (was `cdc_root.strip('/')`,
  which turned `.` into `./…`).
- **Also**: `resolve_task` was redeployed (the live lambda was stale and didn't return
  `cdcRoot`); it now also returns the full `s3Settings` block + `timestampColumnName` +
  `addColumnName` + `datePartitionEnabled`, and fails fast on `DatePartitionEnabled=true` or
  `CdcPath`/`PreserveTransactions`.
- **Verified**: CDC applied 62→72 with the auto-derived `cdc_root='.'`.

### BUG 4 — `IGNORE_COLUMNS` half-wiring (timestamp column)
- **Root cause**: `IGNORE_COLUMNS = {'op', 'dms_timestamp'}` was a hardcoded literal while
  `DMS_TIMESTAMP_COLUMN` was made endpoint-derivable — a renamed timestamp column would leak
  into the target insert set (and the stale literal would be ignored).
- **Fix**: `IGNORE_COLUMNS` is now derived from `OP_COLUMN + DMS_TIMESTAMP_COLUMN`, rebuilt via
  `_rebuild_ignore_columns()` whenever the endpoint override changes the timestamp column.
  Scope: target-apply only (which columns get inserted into DSQL).
- **Verified**: unit test — renamed timestamp column correctly excluded from target insert.

### BUG 5 — DDL-event detection never fired (`consume_ddl_event`)
- **Root cause**: the DDL watcher stores `_ddl_state` keyed **UPPERCASE** (from
  `{c['dms_table'].upper()}`), but `consume_ddl_event(ctx['dms_table'])` looked it up with the
  **lowercase** manifest name → `.get()` always returned `None` → `ddl_event` was structurally
  always `False`. (Secondary: the watcher also can't detect a DDL that predates the job start,
  since it seeds to the current `Ddls` count.)
- **Fix**: `consume_ddl_event` uppercases the key before lookup.
- **Note**: the more robust fix (BUG 6) makes rename detection NOT depend on this flaky signal.

### BUG 6 — RENAME COLUMN degraded to ADD-and-orphan → table BLOCKED
- **Symptom (found only via a clean end-to-end test)**: a `RENAME COLUMN` on the source caused
  the CDC job to **ADD the new column and keep the old one orphaned**, after which the
  missing-column safety guard blocked the table permanently.
- **Root cause**: the rename policy only fired for a `single_swap` (exactly 1 col gone + 1
  new) AND required `ddl_event` (broken, BUG 5) or a manual `rename_hint`. A realistic DDL that
  **combines RENAME + ADD** yields 2 new + 1 gone → `single_swap=False` → fell to ADD-both →
  old column orphaned → the guard ("a target column is absent from the CDC header → would
  silently NULL on UPDATE") blocked the table.
- **Fix** (positional rename detection): under `AddColumnName=true`, DMS preserves **column
  order**. A RENAME keeps the column's ordinal **position** (only the name changes); an ADD
  appends at the end. So each missing column is paired with the new column occupying its old
  ordinal position → that's the RENAME; remaining new columns are genuine ADDs. Priority:
  explicit `rename_hint` > positional match (default, gated by `SINGLE_SWAP_IS_RENAME=True`).
  New knob `--single_swap_is_rename` (default true) reverts to conservative ADD-and-keep if set
  false. Emits real `ALTER TABLE ... RENAME COLUMN` (Postgres RENAME preserves data).
- **Verified**: clean hands-off end-to-end — RENAME + ADD combined converged; old column gone,
  new column present, data intact; no blocks.

### Process lesson (important)
Two clean-slate pitfalls caused **false results** during testing and MUST be respected:
1. **Stale `_load_status.json`**: the loader skips tables marked `done` in this file. A "clean"
   re-run that only drops the target but leaves `_load_status.json` will load **0 rows**.
   Clean slate MUST purge `config/_task/<suffix>/_load_status.json` (and
   `_validation_report.json`).
2. **`processed/` files**: the CDC job moves applied files to `processed/` and skips them. A
   mid-test target reset leaves already-processed files in `processed/`, so their rows are NOT
   re-applied to the fresh target. Only a **single clean pass with no mid-run resets** yields a
   trustworthy result.

---

## 4. DDL support matrix (empirically tested, hands-off)

Tested on a dedicated `ddl_test` table via a single-table task, CDC running continuously, each
case as **DML → DDL → DML** (a trailing DML is required — see T12).

### ✅ Supported (converged + verified correct)
| Case | DDL config | Result |
|------|-----------|--------|
| T1 | Single `ADD COLUMN` | ADD, converged |
| T2 | Multiple `ADD` in one DDL (composite, 3 cols) | all added |
| T3 | Back-to-back `ADD`s (separate DML→DDL→DML cycles) | both added |
| T4 | Single `RENAME COLUMN` | renamed (positional) |
| T5 | Multiple **simultaneous** `RENAME`s (2 in one DDL) | both renamed via positional pairing |
| T6 | `RENAME` + `ADD` combined in one cycle | both correct |
| T11 | Two DDLs back-to-back, **no DML between**, one trailing DML | both surfaced (DDLs collapse into the trailing DML's header) |

### ⚠️ Limitations (where it breaks — mark these)
| Case | DDL config | Behavior | Root cause |
|------|-----------|----------|------------|
| **T8** | `DROP COLUMN` | **BLOCKS the table**; needs operator remediation (drop the column on the DSQL target + clear `cdc_status`), then converges | The missing-column guard can't distinguish a genuine DROP from a column omitted from a change row (which would silently NULL data), so it blocks. |
| **T10** | `CHANGE COLUMN DATA TYPE` | **SILENT DATA CORRUPTION** — no error, no block, row "converges" but the value is wrong (e.g. source `123.4567` → target `123`) | `handle_schema_changes` is purely **name-based**. A type change keeps the same column name → invisible → target keeps its original type → the value is coerced/truncated on apply. Most dangerous failure mode. |
| **T12** | DDL with **no trailing DML** | Not surfaced until the next DML row for that table | Inherent to DMS `AddColumnName=true`: a schema change only appears on the next data row. Not a script bug — a hard requirement that **every DDL be followed by DML** to propagate. |

### Root cause (design)
`handle_schema_changes` is a **name-based header-diff** engine. It is strong at ADD/RENAME in
any combination (positional pairing resolves renames even bundled with adds), **blind** to type
changes (same name = no diff → silent corruption), and **conservatively blocks** on drops.

### Recommended follow-up fixes (not yet implemented)
1. **CHANGE DATA TYPE (highest priority)**: detect value-vs-target-type incompatibility, or
   reconcile column types against an endpoint/type hint, instead of silently coercing.
2. **DROP COLUMN**: add a `drop_hints` config (mirroring `rename_hints`) or an auto-drop policy
   so a legitimate drop reconciles instead of blocking.

---

## 5. Current deployed state (as of last validation)

- **Staged CDC script** `s3://oragluedsql/scripts/glue_cdc_continuous.py` contains all fixes
  (positional rename, `SINGLE_SWAP_IS_RENAME`, `_rebuild_ignore_columns`, uppercase
  `consume_ddl_event`, endpoint-derived `DMS_TIMESTAMP_COLUMN`).
- **Driver folders**: `driver-fullload/` (5), `driver-validation/` (5), `driver-cdc/` (10, incl
  boto3/botocore).
- **Lambdas redeployed**: `resolve-task` (returns `cdcRoot`, `s3Settings`, `timestampColumnName`
  + fail-fast guards), `create-glue-jobs` (`cdcExtraPyFiles`, `--timestamp_column`),
  `drain-check` (`.` sentinel normalization).
- **State machines**: `startup.asl.json` (3× driver-discovery, per-job routing,
  `$.resolved.cdcRoot`), `cutover.asl.json` (`$.resolved.cdcRoot`). Both valid JSON.

### End-to-end validation results
- Full load + validate: 23/23 tables row-exact in the original full run; multi-table
  (2 separate tasks) row-exact in the clean re-test.
- CDC: converged with I/U/D + cached changes; ADD/RENAME (single, multiple, combined)
  propagate correctly hands-off.
- DDL limitations mapped (Section 4).
