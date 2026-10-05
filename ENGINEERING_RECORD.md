# Engineering Record — DMS → S3 → Glue → Aurora DSQL Pipeline

_Comprehensive record of the pipeline architecture, every bug found and fixed, and the
empirically-tested DDL support/limitations. Companion to `RUNBOOK.md` (deploy steps) and
`USAGE_GUIDE.md` (end-to-end operation)._

Last validated: 2026-10-04 (state-machine / script simulations against the real Lambda and Glue
code; see the dated entries in §6). The last **real-AWS** end-to-end run was earlier (a test
account in `us-east-1`, a DSQL cluster) and is captured in §5 as a historical snapshot — the
post-2026-10-01 features (shared workflows, fleet launcher, runtime Spark CDC fallback,
every-column validation, binary guard, NULL-marker rule, 7-day CDC timeout) have **not** yet been
run together on real AWS.

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
- **Glue scripts** (`scripts/`): `job1_discovery.py`, `job2_load.py`,
  `job3_validate.py`, `glue_cdc_continuous.py`. Staged to `s3://<bucket>/scripts/`.
- **Lambdas** (`lambdas/`, nine `.py` files): `resolve_task`, `driver_discovery`,
  `create_glue_jobs`, `plan_split`, `drain_check`, `stop_cdc_run`, `drop_tags` (the seven core
  functions deployed by RUNBOOK Step 2), `preflight_tasks` (the fleet launcher's own deployed
  function, with its own IAM role `iam/preflight-tasks-role.*`), and `prepare_cdc_wheels` (not a
  deployed function — it ships inside the `driver-discovery` zip and is run automatically by the
  startup workflow).
- **State machines** (`stepfunctions/`): `startup.asl.json` (full-load→validate→CDC),
  `cutover.asl.json` (drain + finalize), shared by every DMS task (started with `{"taskArn"}`);
  optional `fleet-startup` / `fleet-cutover` launchers start them for a list of tasks.
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
- **Symptom**: three tables (`table_a`, `table_b`, `table_c`) loaded with shifted columns.
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
   Clean slate MUST purge the per-group status under `config/_task/<suffix>/_orchestrator/`
   (`group-*/_load_status.json`, `group-*/_file_status/`, `group-*/_validation_report.json`).
   The loader and validator run once per table group with that group's own config prefix, so
   their status files live there, not at the task's top-level prefix.
2. **Applied CDC files**: the CDC job skips applied files by its high-water mark
   (`cdc_status.last_done_file`) and copies them to `processed/` (originals are kept since
   2026-10-02). A mid-test target reset therefore does NOT re-apply them to the fresh target
   unless the table's `cdc_status` row and S3 prefix are purged too. Only a **single clean pass with no mid-run resets** yields a
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
| **T8** | `DROP COLUMN` | **BLOCKS the table**; needs operator remediation (drop the column on the DSQL target, then set `cdc_status.status='active'`), then converges | The missing-column guard can't distinguish a genuine DROP from a column omitted from a change row (which would silently NULL data), so it blocks. |
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

## 5. Deployed state at the last real-AWS validation (historical snapshot)

> **This section is a snapshot of the earlier real-AWS run, kept for history.** It predates the
> post-2026-10-01 features. For what the pipeline does **now**, read §6 (the dated change log) and
> the current `RUNBOOK.md` / `USAGE_GUIDE.md`. The current feature set adds: shared startup/cutover
> state machines, the fleet launcher, runtime Spark CDC fallback, every-column validation, the
> `bytea` BINARY GUARD, the NULL-marker rule, case-insensitive folder discovery, and the hard
> 7-day (10080-minute) CDC Glue run timeout. The counts and figures below are from that one test
> run and are **not** kept current.

- **Staged CDC script** `s3://<test-bucket>/scripts/glue_cdc_continuous.py` contained all fixes
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

---

## 6. 2026-10 field run (locked-down VPC, firewall, no internet) and the shared workflows

A customer run in a VPC with a VPC-only DSQL endpoint and no internet access exposed the issues
below. Each is fixed in the repo.

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | CDC `... .whl installation failed` after ~20 min, `pypi.org` timeouts, no control tables | Glue Python shell pip-installs `--extra-py-files` one wheel at a time before the script starts; a wheel whose dependency isn't installed yet (boto3 → botocore, python-dateutil → six) makes pip ask PyPI, which the firewall blocks. Spark jobs are not affected (wheels go straight on the Python path) | `prepare_cdc_wheels.py` (now in `lambdas/`, run automatically by the startup workflow; see 2026-10-02 below): validates the `driver-cdc/` set for Python 3.9 (pure-Python, one version per package, Requires-Python, dependency ranges), then removes Requires-Dist and updates RECORD. Proven with `pip install --no-index` per wheel in the worst order, and in a real Glue run |
| 2 | `boto3-1.43.x` / `scramp-1.4.17` fail to install in the CDC job | wheels downloaded for Python 3.10; Glue Python shell only runs 3.6/3.9 (boto3/botocore 1.43+ and scramp 1.4.7+ need 3.10; on 3.9 botocore needs urllib3 <1.27) | RUNBOOK Step 3b downloads `driver-cdc/` for Python 3.9 with version caps |
| 3 | CDC waited forever (`full load not done`) | an old CDC script read only the top-level `_load_status.json` | fixed earlier (merges `_orchestrator/group-*/`); RUNBOOK troubleshooting points to it |
| 4 | Load job `KeyError: 'table'` after a table was empty at load time | that result had no `table` field | added |
| 5 | Jobs created outside the VPC when a workflow's connection value was left unfilled | `<<GLUE_CONNECTION>>` (or `""`) was treated as "no connection" | `create_glue_jobs` falls back to the Lambda's `GLUE_CONNECTIONS` setting; the shared workflows take it from `pipeline.json` |
| 6 | Startup waited ~24 h then failed when DMS was already past its full load | the completion gate only matches `STOPPED_AFTER_CACHED_EVENTS` | the shared startup checks the task before starting DMS and fails in seconds |
| 7 | — | `create_glue_jobs` cut names at 255 characters, so `-load` and `-load-big` could collide | raises instead |
| 8 | Optional CDC engine | some environments prefer the CDC job to load drivers exactly like the full-load jobs | `cdc_engine: "spark"` (`glue-templates/cdc-spark.json`, Glue 4.0, 2 × G.1X); `StartCdcJob` no longer passes a wheel list (the job's own setup is used), which a Spark job requires. Proven end to end in a real Glue run (I/U/D applied) |

### Shared startup/cutover state machines

One startup and one cutover state machine now serve every DMS task, started with
`{"taskArn": "..."}`:

- `config/pipeline.json` holds the values that are the same for every task (project, region,
  DSQL target, Glue role, Glue connection, CDC engine, control schema).
- The DMS task's **name** is the task suffix: config folder `config/_task/<name>/`, Glue jobs
  `<project>-<name>-<role>`. DMS names are letters/digits/hyphens (valid for S3, Glue and Step
  Functions run names) and unique per region.
- `resolve_task` (new `mode` = `startup` | `cutover`) runs **before** DMS is started: reads the
  settings, derives the per-task values, and checks `full-load-and-cdc`,
  `StopTaskCachedChangesApplied=true`, `AddColumnName=true`, target bucket = pipeline bucket, and
  that the task isn't already past its full load. Failures stop at `ResolveFailed` with the reason.
- Safeguards: `config/_task/<name>/_task.json` records the owning task ARN (a reused name of a
  deleted task is refused, since its old status files would let CDC skip tables the new task never
  loaded); `config/_task_index/<task id>.json` records the suffix by ARN (a renamed task keeps its
  folder and jobs, so cutover still finds them).
- Backward compatible: without `mode`, `resolve_task` returns exactly the old output, and the old
  per-task state machines run unchanged against the new Lambdas.
- Verified with a state-machine simulator driving the real `resolve_task`, `create_glue_jobs` and
  `stop_cdc_run` code: both engines, two concurrent tasks, cutover of one leaving the other
  running, a failed load group, every `ResolveFailed` case, and every JSONPath reference in both
  definitions resolved. **Not yet run on real AWS.**

Lesson: test the pipeline in a VPC **without** internet access. With internet, pip silently
fills dependency gaps and items 1 and 2 stay hidden.


### 2026-10-02 — processed/ copies, S3 retries, per-table manifest, cutover LOAD-file fix

- **Symptom:** one table's `processed/` folder dropped from 1,000+ files to ~60 while the
  table's CDC was blocked on a rejected NULL row.
- **Change:** applied files are now **copied** to `processed/`, never moved: no CDC file is ever
  deleted by the pipeline. Each copy is verified by size and retried (boto3 adaptive retries +
  6 jittered attempts); a copy that still fails is non-fatal (original kept) and is retried every
  cycle. `processed/_manifest.json` per table reports counts only (in folder, pending apply,
  applied, copied, pending copy, ledger total, `all_done`).
- **Consequence:** unblock a table with `UPDATE cdc_control.cdc_status SET status='active'`, never
  by deleting its row (that would replay every file still in the folder).
- **Cutover fix:** `drain_check` picked the alphabetically last file as "latest", which with no
  DMS BucketFolder was always a full-load `LOAD*.csv` (never in the CDC ledger), so cutover could
  never finish (`CdcDrainTimedOut`). It now skips `LOAD*` files, like the CDC job.
- **Verified:** the copy/retry/manifest code against a simulated S3 (throttling, failed and short
  copies, pagination; 17 checks), and the real `process_table` with simulated S3 + DSQL through
  apply, re-run, a throttled copy, a NULL-row block and an unblock (11 checks: no file deleted,
  no file applied twice).

### 2026-10-02 — CDC drivers prepared automatically; CDC start confirmed

- **Problem:** every new deployment needed the `driver-cdc/` wheels prepared by hand
  (`prepare_cdc_wheels.py`) before the Python-shell CDC job could install them behind a firewall.
  A wrong wheel only surfaced after the full load, as a CDC install failure ~20 min in, and the
  startup run still reported `Succeeded` because it didn't wait for the CDC run.
- **Change:** `driver_discovery` (with `prepare_for: pythonshell`, set by the shared startup from
  `cdc_engine`) validates the set for Python 3.9 with the same code as the tool, plus checks that
  still apply to already-stripped wheels (all 10 packages, urllib3 1.26.x, matching
  boto3/botocore). It then writes stripped, verified copies to
  `driver-cdc-prepared/<fingerprint>/` (`MANIFEST.txt`, then `_READY.json` last) and returns that
  list; later tasks reuse it. The driver steps now run **before DMS starts** (`DriversFailed`).
  After `StartCdcJob` the workflow waits for `config/_task/<task>/_cdc_started/<execution
  name>.json`, which the CDC script writes on reaching its poll loop (`CdcRunFailed`,
  `CdcRunEnded`, `CdcStartNotConfirmed`).
- **Deploy impact:** the driver-discovery Lambda needs 1 GB / 300 s; the Step Functions role gets
  `s3:ListBucket` on `config/_task/*`. No manual wheel step remains.
- **Verified (simulation):** the Lambda against the real PyPI wheel sets (23 checks: the prepared
  wheels are byte-identical to the tool's output that installed offline in Glue; scramp 1.4.17,
  urllib3 2.x, missing six, two scramps, boto3/botocore mismatch and a `.zip` all stopped with a
  clear message and nothing written; reuse; rebuild after a deleted file; Spark and old per-task
  calls unchanged). Workflow simulation: 40 checks, including every new failure path. CDC
  script: 38 checks, including the start marker.

### 2026-10-03 — Overlapping runs: one change applied once, whatever starts the jobs

- **Two CDC runs on one table** (a second Glue job for the same tables, an old per-task workflow
  next to the shared one, a hand-made copy, two DMS tasks sharing a table). Before: nothing
  stopped them. A chunk whose commit DSQL rejected (OC000) was retried as-is, so both runs applied
  every change (simulation: each change applied twice) and the checkpoint jumped back and forth.
  The final rows came out right in every interleaving simulated, but the work was doubled and
  intermediate states were stale. **Now:** every step that moves a table's checkpoint (each
  data chunk, the file-done step, the idle step) first checks, inside the same transaction, that
  `cdc_status` is still where this run left it (`check_position_cur`). Because both runs update the
  same `cdc_status` row, DSQL aborts the later committer; its retry sees the moved checkpoint and
  the run leaves the table for that cycle (`yielded`, with a warning naming the likely causes). A
  retry after a connection dropped *after* COMMIT now recognizes its own committed step instead of
  applying it again. A table set to `blocked` mid-run stops at the next step.
  Verified: the real `process_table`/`apply_file`/`apply_file_nonpk` against a simulated DSQL with
  optimistic concurrency, a second run interleaved before every commit of the first (keyed: every
  change committed exactly once and in order, 16 interleavings; keyless: final contents identical
  to a single run, 10 interleavings), plus the dropped-connection-after-commit and block-mid-run
  cases (12 checks, both script copies). The same test fails 7 of 12 checks on the previous script.
- **Two startup (or cutover) runs for one task:** resolve-task now refuses the later of two
  RUNNING executions of the same state machine for the same `taskArn` (tie-break so exactly one
  stops). Needs `states:ListExecutions`/`DescribeExecution`; without them it only warns.
- **Many CDC jobs starting at once:** `ensure_control_tables` retries concurrent-DDL conflicts
  (OC000/OC001/catalog 23505) up to 10 times; real errors are not retried.
- **Many tasks preparing CDC drivers at once:** the preparation is deterministic (same
  fingerprint, byte-identical wheels), so two simultaneous preparations write identical files.
- **Not guarded:** a load job started by hand while the workflow runs the same group (keyless
  tables can get duplicates; validation reports them).
- **Found, not changed:** for a keyless table the apply runs all of a file's DELETEs before its
  INSERTs, so a row inserted and deleted within the same file survives. Keyless tables only.
- **Docs:** RUNBOOK Step 1's fill-in command now replaces `<<PROJECT>>` (used by the Step
  Functions and Lambda policies).

### 2026-10-03 — Multi-column primary keys left to a separate CDC job

- **Before:** a table whose primary key has more than one column had no single apply key, so the
  CDC job treated it as keyless: every UPDATE was skipped (logged in `cdc_skipped_ops`), DELETEs
  matched on every column (and did nothing if DMS sent only the key columns), and a `_cdc_file`
  column was added to the target. The old comment promising a fail-closed "Tier 3" for this
  case had no code behind it.
- **Now:** `build_table_context` raises `MultiColumnKeyTable` for those tables (even if a logical
  key is declared); `main()` lists them at startup and never processes them, so their
  `cdc_control` rows stay free for the separate multi-column-key CDC job. A task whose tables all
  have multi-column keys starts, writes its start marker and stays idle. The drain check is
  unchanged and still waits for them, so cutover can't finish before that job has caught up.
- **Verified:** the real `build_table_context` and `main()` with a mix of single-key, keyless and
  multi-column-key tables (10 checks); the copy-fix (38) and two-run (12) suites still pass.

### 2026-10-03 — Only the DMS null marker is NULL; text is stored as written

- **Before:** the load, validation and CDC jobs turned `NULL`, `N/A`, `NA`, `NONE`, `(NULL)` and `\N`
  (any case, spaces ignored) into NULL in every column, so real text values such as a product
  code `NA` were silently lost, or rejected by a NOT NULL column (which blocks the table in CDC).
  The full load also trimmed spaces from all text, while CDC kept them.
- **Now:** a value is NULL only if it is empty or exactly equals the endpoint's `CsvNullValue`
  (DMS default `NULL`, per the DMS S3Settings reference). resolve-task reads it from the
  endpoint (shared mode), the startup workflow passes it to create-glue-jobs, which sets
  `--csv_null_value` on the load, load-big, validate and CDC jobs (`__EMPTY__` when the marker is
  the empty string, because Glue can't pass an empty argument). Without it (older workflows) the
  scripts use `NULL`. Text is kept exactly, spaces included, in all three jobs; typed columns are
  trimmed and whitespace-only becomes NULL. A typed column holding `NA` now fails its cast
  instead of silently becoming NULL.
- **Verified:** the real `convert_value` (both CDC copies) and the real Spark `null_marker_expr`
  (load and validation) on the same text and numeric values with three markers (`NULL`, empty,
  custom): all three jobs agree (12 checks); workflow plumbing including an empty marker and the
  legacy path (46 workflow checks); all other suites unchanged.
- **Not changed:** data already loaded by earlier versions (USAGE_GUIDE §4b explains how to find
  and fix it).

### 2026-10-03 — Table-list names in any case; DMS BucketFolder honoured by discovery

- **Before:** discovery built each table's path as `<bucket>/<schema>/<table>/` from the table list
  exactly as typed, ignoring the endpoint's BucketFolder, and never checked the folder existed. A
  case mismatch (table list `SRC_SCHEMA`, DMS folder `src_schema`, or the reverse) or a BucketFolder
  made discovery see no files: the table was marked empty, the load succeeded with 0 rows, CDC
  watched an empty folder and the drain check treated it as caught up, so cutover succeeded with
  an empty table and no error anywhere.
- **Now:** discovery receives the BucketFolder (`--cdc_root`, from the endpoint via
  create-glue-jobs) and lists the folders DMS actually wrote, matching schema and table
  case-insensitively (an exact match wins; two case variants with no exact match fail that table).
  The folder names it finds go into the index (`dms_schema`, `dms_table`, `dms_s3_path`), which the
  load, validation, plan-split, CDC job and drain check all read. If no table in the task has a
  folder, discovery fails before writing the index and lists the folders it found
  (`--allow_all_empty true` overrides). A table with no folder yet is loaded as empty with a
  warning and its folder name guessed in its siblings' case; the CDC job and drain check look for
  it in any case if the guessed folder never appears (every 5 minutes at most in the CDC job).
- **Verified:** the real discovery script run end to end with simulated S3, DSQL and Spark, then
  its index fed to the real CDC listing and drain check: uppercase, lowercase and mixed-case
  table lists against uppercase folders; a lowercase BucketFolder layout; exact-match preference;
  the ambiguous case; an empty table; the all-missing failure; a table later created in another
  case (18 checks). All other suites unchanged (47 workflow checks).

### 2026-10-04 — Validation checks every column; binary columns stored as real bytes

- **Validation gaps (before):** content was count-only by default (the optional checksum covered
  text columns only), a missing `_load_status.json` validated 0 tables and passed, tables whose key
  couldn't be range-split were skipped and the job still passed, and an empty source passed without
  looking at the target. Also found while testing: the validator converted values before renaming
  DMS's column names to the DSQL names (the load does the reverse), so with differently-cased
  names its uuid/boolean/timestamp conversions were skipped. Harmless while only counts were
  compared, it would have produced false mismatches once content was compared. Fixed.
- **Validation (now):** per key range, row count plus per-column summaries chosen from the column's
  real DSQL type (read from information_schema): text/char/binary = non-null count, total length,
  min, max, sum of a per-value md5 hash; uuid = count, min, max, hash; boolean = true count;
  integer/numeric = exact sum after the load's rounding; float = sum with tolerance;
  timestamp/date = sum of instants in microseconds; json/other = count. The md5 hash is probed once
  per run and dropped (with a log line) if DSQL rejects it. Missing status file, a table not marked
  `done`, or zero tables checked all fail. A table without a range-splittable key is compared as
  one whole-table range. An empty source requires an empty target. `--checksum_mode off` gives
  counts only.
- **Binary (before):** DMS writes RAW/BLOB as hex text; the load and CDC cast it as `'<hex>'::bytea`,
  which stores the ASCII of the hex text. **Now:** both convert to `'\x' + lowercase hex` and stop the
  table (`BINARY GUARD`) on a non-hex value. In the schemas tested so far every RAW column maps to
  `uuid` (no `bytea` columns), so this is for future schemas.
- **Verified:** the real validation script run with a Spark stand-in and a DSQL stand-in following
  PostgreSQL semantics, against a table loaded by the real conversion code: all 15 column types
  match when correct, and each of 15 single-column corruptions (NULLed text, same-length text
  change, trimmed spaces, different uuid, wrong cents, integer, boolean, timestamp ±1h and ±1µs,
  date, double, both binary bugs, char, NULLed note) fails naming exactly that column; missing row,
  empty source, whole-table compare, missing status file, not-done table, checksum off, no-md5 DSQL
  (34 checks). All other suites unchanged.

### 2026-10-04 — CDC drivers fail → the CDC job is re-created as Spark automatically

- **Why:** the Python-shell CDC job depends on Glue pip-installing its wheels; behind a firewall any
  wheel problem ends the run (often after ~20 min). The Spark engine loads drivers like the
  full-load jobs (sys.path + `--additional-python-modules`, no PyPI) and was already built and
  tested; only switching to it was manual.
- **Before DMS starts** (`driver_discovery`, `spark_fallback`): if the wheels fail the Python-shell
  checks, the three wheels a Spark CDC job takes from `driver-cdc/` are checked (one each of boto3,
  botocore, s3transfer; boto3/botocore matching; botocore has `dsql`). Good → returns
  `engine: "spark"` + reason and the CDC job is built as Spark. Bad → `DriversFailed` saying neither
  engine can run.
- **When the CDC run starts** (`IsCdcRunAlive` → `CdcDriverFallback` → `IsCdcFallbackSwitched` →
  `UseSparkCdcJob` → `StartCdcJob`): a FAILED Python-shell run's Glue error is classified by
  `create_glue_jobs.driver_error_reason` (pip/PyPI, CalledProcessError, .whl install/missing, wrong
  Python, driver ModuleNotFound/ImportError, `Unknown service: 'dsql'`; the CDC script starts no
  subprocesses, so CalledProcessError can only be Glue's installer). Driver error → only the CDC
  job is deleted and re-created with the same name as Spark (refused if another run is active →
  `CdcFallbackFailed`), started with the same run arguments and confirmed again. Once only; other
  errors → `CdcRunFailed` unchanged.
- **Spark CDC drivers picked by name:** `--extra-py-files` = pg8000, scramp, asn1crypto (+
  python_dateutil, six if present) from `driver-fullload/`, or all from `driver-validation/` when
  driver-fullload lacks one or has two versions (folders never mixed; any other wheel, e.g. a stray
  boto3 that would break botocore under Spark, is left out). boto3/botocore/s3transfer still via
  `--additional-python-modules` from `driver-cdc/` (the two Spark folders hold no boto3 by design).
- **Sticky:** both paths write `config/_task/<task>/_cdc_engine.json`; `resolve_task` reads it and
  sets `cdcEngine: "spark"` for later startups (warning says how to undo). Setting
  `cdc_spark_fallback` (default true) turns both paths off.
- **Verified:** workflow simulator with the real Lambdas, 59 checks (12 new): driver-check fallback
  on/off, pip failure → Spark and success, driver error on both engines → one switch then
  CdcRunFailed, DSQL error → no switch, fallback off, active run → CdcFallbackFailed, sticky
  engine on the next startup, every JSONPath reached. Driver Lambda with the real wheel sets,
  36 checks (11 new). Error classifier: 12 real driver messages caught, 13 non-driver messages
  (DSQL 54000, connection, broken pipe, permissions, script errors) not. Not yet run on real AWS.

### 2026-10-04 — Fleet launcher: start/cut over a list of tasks from one trigger (commit `41eaa88`)

- **Why:** running Step 5/6 by hand for every DMS task doesn't scale. The fleet launcher starts the
  shared `startup` (or `cutover`) for a whole list of tasks from one manual trigger.
- **What was added:** `lambdas/preflight_tasks.py` (its own deployed function + IAM role
  `iam/preflight-tasks-role.*`), two state machines `stepfunctions/fleet-startup.asl.json` and
  `fleet-cutover.asl.json`, the fleet IAM roles (`iam/fleet-startup-role.*`,
  `iam/fleet-cutover-role.*`), and `config/fleet_tasks.example.csv`.
- **How it works:** manual trigger, input `{"bucket","inputPrefix"}`. It reads
  `config/pipeline.json` (the same file every task uses — it does **not** write it) and
  `config/<inputPrefix>/fleet_tasks.csv` (columns `task_arn`, optional `task_suffix`, optional
  `adopt_existing_folder`). The task suffix defaults to the DMS task name. The `Preflight` state
  (`preflight_tasks`, reusing `resolve_task`'s rules) validates each task's ARN/readiness, the
  table lists, and that the DSQL database has ≤ 9 of the operator's own schemas (10-schema cap,
  `cdc_control` is the 10th); it is **fail-closed**. It then starts one per-task execution per task
  (5 at a time), **asynchronously**, and verifies each reached RUNNING. `FleetStarted` means every
  task got past its own input checks — **not** that the migrations succeeded.
- **Scope note (preflight):** the fleet `Preflight` does **not** inspect Oracle/LogMiner, so it
  does not catch the new-schema CDC-capture gap (`CDC_EDGE_CASE_RESULTS.md` §4).
- **New requirement not yet built:** a single parameters CSV holding every "export" value (bucket,
  account, region, project, DSQL endpoint/user/db, subnet, SG, Glue connection, …) alongside
  `fleet_tasks.csv`, so nobody retypes an export block. On `main` the fleet reads these from
  `config/pipeline.json`; the two-CSV input is a planned enhancement (see
  `RUNBOOK.md` → [planned parameters CSV](RUNBOOK.md#values-what-setup-needs-vs-what-running-tasks-needs)).
- **Status:** exercised in a state-machine simulator; **not yet run live on AWS** — run one small
  live fleet first. Full operation in `docs/FLEET_LAUNCHER.md` and
  `RUNBOOK.md` → [Running many tasks with the fleet](RUNBOOK.md#step-5--run-tasks-with-the-fleet).

### 2026-10-04 — Cleanup batch: cutover success, firewall timeouts, Step 1 order, Word docs, old kit

- **Cutover reported success when Glue jobs weren't deleted.** `DeleteGlueJobs` caught every error
  into `CutoverSucceeded`, and the delete Lambda only logged per-job errors. Now the Lambda returns
  them under `failed` and the workflow ends at `GlueJobsNotDeleted` naming each job (the data is
  already cut over at that point). A job already gone still counts as deleted, so starting the
  cutover again finishes it.
- **CDC behind a firewall.** The DMS (DDL watcher) and CloudWatch (one keyless-table metric) clients
  used boto3's defaults (60 s timeouts, several retries), so with no route each call hung for
  minutes. They now use 5 s connect / 15 s read / 2 attempts; an unreachable service is logged
  once and (CloudWatch) turned off for the run. S3 and DSQL clients unchanged.
- **RUNBOOK Step 1** created the IAM roles before filling in the placeholders. It now writes filled
  copies (`iam/*.filled.json`, portable `sed`) first and creates the roles from them.
- **Word docs** (`docs/USAGE_GUIDE.docx`, `docs/CONSIDERATIONS_AND_LIMITATIONS.docx`) brought up
  to date: shared workflows started with the task ARN, the real S3 layout, automatic driver
  preparation and Spark fallback, validation of every column, multi-column-key tables, binary and
  NULL-marker handling, the 10-schema limit, and the safe unblock (set `status='active'`, never
  delete the `cdc_status` row, which re-applies every file).
- **`cdc_firewall_fix/` retired.** Its `repo_changes/` held old copies of `create_glue_jobs.py` and
  `startup.asl.json` that would undo later fixes, and its `prepare_cdc_wheels.py` was an older copy
  of `lambdas/prepare_cdc_wheels.py`. The switch tool moved to `tools/switch_cdc_engine.py` and now
  picks Spark drivers by name like the pipeline, uses the prepared `driver-cdc-prepared/` set for
  Python shell (the raw set would need PyPI), and records the choice in `_cdc_engine.json`.
- **Verified:** workflow simulator 65 checks (4 new cutover cases); CDC optional-API test 12 checks
  on both script copies; switch tool 10 checks; all other suites unchanged.

---

## 7. Known issues still open (not yet fixed in this release)

These are real bugs in the code at this HEAD; they are documented, with workarounds, in
`RUNBOOK.md` → [Known issues](RUNBOOK.md#known-issues-temporary). Listed here so the record is
honest about what is **not** fixed:

1. **CDC Glue run 7-day timeout.** Both `glue-templates/cdc.json` and `cdc-spark.json` set
   `timeout_minutes = 10080` — the Glue maximum (7 days). The CDC job is a continuous poller;
   after 7 days Glue stops the run and nothing restarts or monitors it (DMS keeps writing change
   files, so no data is lost — but changes stop being applied until the job is restarted by hand).
   `10080` is deliberately the Glue max; a self-restarting watchdog is a recommended follow-up.
2. **Cutover is not idempotent once DMS is stopped.** The first step stops the DMS task; a re-run
   on an already-stopped task is rejected by the DMS API, so the remaining drain/stop-CDC/drop/
   delete steps must be finished by hand.
3. **A real DMS start failure is hidden for 24 h.** The start error is swallowed into the poll
   loop and surfaces as `DmsTimedOut` a day later.
4. **`config/pipeline.example.json` fails the placeholder scan.** Its `description` contains
   `<bucket>`/`<...>`, which `resolve_task` rejects (`<`/`>` in any value) → `ResolveFailed`.
   Generate `pipeline.json` from the RUNBOOK Step 3c script instead of copying the example.
5. **Case-folder latent bug.** A table empty at full load whose DMS folder differs only in letter
   case can be mis-matched permanently and reported caught up at cutover
   (`CDC_EDGE_CASE_RESULTS.md` §2.8). Does not affect tables that have data at full load.
6. **Composite-key CDC job not in the repo.** Multi-column-PK tables are skipped by the main CDC
   job (2026-10-03 entry); the separate job they require is not shipped here, so cutover waits for
   them indefinitely until it exists.
