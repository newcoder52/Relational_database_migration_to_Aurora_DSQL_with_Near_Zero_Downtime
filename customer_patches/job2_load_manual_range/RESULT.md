# RESULT.md - MANUAL-RANGE load patch

## What this delivers

A standalone, deployed-file patch that lets an operator load one big table with several
independent Glue `load-big` runs, each loading an OPERATOR-CHOSEN inclusive slice of the
table's `LOAD*.csv` part-files (`--load_file_start` / `--load_file_end`), then a merge tool
(`merge_ranges.py`) to reconcile and hand back to the normal pipeline. No Step Functions or
Lambda change; works against the DEPLOYED `job2_load.py`.

Files under `~/Downloads/review3d/manual-shard/`:
`apply_manual_shard_patch.py`, `job2_load_manual_shard.py`, `job2_load_manual_shard.diff`,
`merge_ranges.py`, `test_manual_shard_patch.py`, `README.md`, this `RESULT.md`, `READY_V1`,
`DONE`.

## Verified by EXECUTION (test suite: 51 passed, 0 failed, no AWS/Spark)

* Range resolution: number, bare name, full `s3://` key; start-only; end-only; single-file;
  bad number / bad name / bad key / non-parseable -> fail closed; gapped numbering; inclusive.
* BYTE-IDENTICAL when both args absent: every wrapper runs the ORIGINAL function (asserted by
  calling the extracted wrappers with mode OFF and comparing to the originals / sentinels).
* ON mode returns only the resolved slice and uses a range-private file-status path.
* Per-range status isolation: two ranges resolve to DIFFERENT `_file_status/...range-*.json`
  keys (no shared-object read-modify-write).
* Overlap guard: identical range allowed (resume); partial overlap refused; disjoint allowed;
  simultaneous disjoint starts both succeed; overlapping concurrent start caught (by the guard
  or, in the narrow race, left for the merge tool - test asserts one of the two).
* Resume reads this range's own done/started; no-PK mid-file crash FAILS CLOSED; single-PK
  mid-file crash is allowed through to the loader's probe redo.
* Per-range result JSON totals rows/files correctly.
* SSL EOF classification: plaintext message, `ssl.SSLEOFError` instance, and `OSError` errno 8
  all classified as dropped connection; unrelated errors not over-matched.
* Apply script: refuses unrecognised input, refuses already-patched input, patched file
  compiles, and `apply(base)` reproduces a compiling file.
* Merge tool: complete / missing / duplicate / failed reconciliation; dry-run writes nothing;
  `--write` creates the normal doc; the merged doc is ACCEPTED by the REAL `load_file_status`
  extracted from the patched file (4 files done, rows summed); `--plan` yields disjoint,
  complete, contiguous ranges and handles N > file count.
* Mutation checks: disabling the overlap test, the start>end check, or the no-PK `if started`
  check each makes a corresponding test fail (the guards are load-bearing).

Also verified by execution: the patched file and the apply script both `compile()`; the diff
is additive (one list-literal line is reflowed to append two arg names; its original tokens are
preserved).

## Verified by READING only (NOT executed here - no AWS, no Spark, no deployed env)

* That the wrappers actually intercept the loader at run time inside Glue: the install rebinds
  module-level names (`s3_list_table_files`, `_file_status_path`, `clear_file_status`,
  `assert_empty_or_register`, `validate_whole_table_after_ranges`, `is_broken_pipe_error`,
  `load_one_table_chunked`) AFTER all defs and BEFORE the main loop. The loader calls these by
  module name, so the rebind takes effect - confirmed by reading the call sites, not by running
  Spark/DSQL.
* The loader's own per-file resume semantics (probe redo for single-PK, mark_started/done,
  per-file error text, FAILED-run-on-any-failed-file) are UNCHANGED and relied upon; verified by
  reading the PER-FILE RESUME branch, not re-executed against DSQL.
* `--config_prefix` and the other per-run args are honoured by the existing
  `_apply_v6_arg_overrides()` parser; verified by reading the parser and the optional list.
* S3 `If-None-Match` / `If-Match` conditional put behaviour on real S3; the FakeS3 models it,
  but real botocore/endpoint support for these headers on `put_object` was NOT exercised live.

## Remaining race (honest)

The overlap guard reads the ranges ledger, checks for overlap, then writes its own entry with a
conditional put and a re-read. If two DIFFERENT overlapping ranges start within the same brief
window and BOTH read an absent/stale ledger before either writes, both may proceed. This is a
narrow window (seconds) and only matters if the operator chose OVERLAPPING ranges; disjoint
ranges (the intended use, and what `--plan` emits) never overlap by construction. The guard
reduces but does not fully eliminate the window with S3's conditional writes. `merge_ranges.py`
is the definitive backstop: it refuses to `--write` the normal doc if ANY file is covered by
more than one range, so an overlap can never silently reach the pipeline as duplicated rows.
Recommendation: use disjoint ranges (from `--plan`), and always run the merge dry-run before
`--write`.

## G.4X capacity math (operator lowered the load-big job to G.4X; estimates, not measured)

The operator runs each range on G.4X via per-run overrides (shared job definition unchanged):
`--worker-type G.4X --number-of-workers 20` (or `10`) on `aws glue start-job-run`. G.4X = 4 DPU
per worker, ~64 GB / 16 vCPU per node; the driver is also G.4X (~64 GB), roughly HALF the memory
of G.8X (~128 GB). The load is DRIVER-BOUND (executors only read S3; each concurrent file worker
pulls a whole ~250 MB Spark partition to the DRIVER via toLocalIterator), so DRIVER MEMORY, not
worker count, caps `--max_files_in_parallel`.

DRIVER MEMORY -> max_files_in_parallel (per run). The loader budgets PER_WORKER_MEM_BUDGET_MB =
1500 MB per concurrent file. Usable driver heap on a 64 GB G.4X driver is well under 64 GB after
the JVM/Python/Spark overhead; budget ~30-45 GB usable (ESTIMATE). Then:
  max_files_in_parallel ~= usable_driver_MB / 1500
  * 30 GB usable  -> ~30000/1500 = ~20 files
  * 45 GB usable  -> ~45000/1500 = ~30 files
RECOMMENDATION at G.4X: set `--max_files_in_parallel 20` per run (the README examples use 20).
This is an ESTIMATE from the loader's own 1500 MB/file budget and the ~half-of-G.8X driver RAM;
it is NOT measured. If a run OOMs the driver, lower to 12-16; if the driver log shows plenty of
free memory, you may raise toward 30. The loader prints a loud driver-free-vs-needed advisory at
start and does NOT auto-throttle, so watch that line on the first run and adjust.

DPU per run: G.4X x (20 workers + 1 driver) = 21 nodes x 4 DPU = ~84 DPU/run.
At 10 workers + 1 driver = 11 nodes x 4 DPU = ~44 DPU/run.

Per-run subnet IPs: ~1 ENI/IP per node => ~21 IPs/run at 20 workers, ~11 IPs/run at 10 workers
(budget a few extra for ENI churn).

Per-run DSQL connections (driver-threads): peak writers = min(files_in_flight, max_files_in_parallel)
x writers_per_file, capped by `--max_write_concurrency`. With max_files_in_parallel 20 and
writers_per_file 8 that is up to ~160 writers/run (<= your `--max_write_concurrency`). Keep the
sum across runs under the conn_budget of 900 (and well under the ~10,000 cluster cap, leaving
room for CDC + validation).

Budget table (per run x N concurrent runs; "20w" = --number-of-workers 20, "10w" = 10):

| N  | workers/run | DPU/run | total DPU | IPs/run | total IPs | DSQL/run (mfip20,wpf8) | total DSQL |
|----|-------------|---------|-----------|---------|-----------|------------------------|------------|
| 4  | 20w         | ~84     | ~336      | ~21     | ~84       | ~160                   | ~640       |
| 4  | 10w         | ~44     | ~176      | ~11     | ~44       | ~160                   | ~640       |
| 6  | 20w         | ~84     | ~504      | ~21     | ~126      | ~160                   | ~960 (>900!)|
| 6  | 10w         | ~44     | ~264      | ~11     | ~66       | ~120 (lower wpf/mfip)  | ~720       |
| 10 | 20w         | ~84     | ~840      | ~21     | ~210      | ~160                   | ~1600      |
| 10 | 10w         | ~44     | ~440      | ~11     | ~110      | ~90 (lower wpf/mfip)   | ~900       |

Reading the table:
* N=4: comfortable at 20w (336 DPU, 84 IPs, ~640 DSQL < 900). This meets the ~2 h goal.
* N=6: total DSQL at wpf8 would be ~960 > the 900 conn_budget. Lower `--writers_per_file` to ~6
  (or `--max_write_concurrency`) so N x per-run writers <= 900. DPU/IPs are fine.
* N=10: DPU (840 at 20w) and IPs (210) are the ceilings; drop to 10 workers/run (440 DPU, 110
  IPs) and lower writers so total DSQL stays <= 900 (~90/run). More runs than DPU/IP allow only
  queue - they do NOT go faster. For this table 4 runs is the sweet spot; 6-10 only help if DPU,
  IPs, `MaxConcurrentRuns`, and the DSQL budget are all raised.

All DPU/IP/connection figures are standard-figure ESTIMATES; confirm against your account quotas
and the load-big job's `MaxConcurrentRuns` (must be >= N or runs queue). Per-run `--worker-type` /
`--number-of-workers` on `start-job-run` leave the shared job definition untouched (confirm your
Glue version accepts them with `aws glue get-job ... --query Job.GlueVersion`).

## Target table: dropped and recreated EMPTY before the first range run

Range mode never empties anything, so the operator presents an empty target by DROPping and
recreating the table with the SAME DDL including the PRIMARY KEY and identity/default (discovery
and the loader need the table and its single-column PK; the PK also makes a mid-file crash
resumable without duplicates), then confirming 0 rows before starting. README section 2a has the
exact statements. If the table already has CDC running, the drop MUST be paired with the CDC
reset below or CDC will skip already-applied change files.

## CDC reset after a drop + manual reload (verified by READING the CDC code)

This table (`<task-suffix>`) already has its bg CDC job and DMS running, so DSQL
`cdc_control` holds a CDC high-water mark. Dropping + reloading WITHOUT resetting CDC causes the
CDC job to SKIP every change file whose name sorts at/under `cdc_status.last_done_file` -> silent
data loss. The safe, code-consistent reset (README section 8, sourced from
~/Downloads/review3d/cdc-reset/CDC_RESET.md) is, IN ORDER:

1. STOP ONLY this table's bg CDC run (its own job name; not batch-stop everything). DMS may keep
   running.
2. DROP + recreate the target EMPTY (same DDL + PK), confirm 0 rows.
3. In DSQL, KEEP the `cdc_status` ROW (never DELETE it) and `UPDATE ... SET status='active',
   last_done_file=NULL, in_progress_file=NULL, last_offset=0, full_load_rows=NULL,
   inserts_applied=0, deletes_applied=0, ...` for this table, then `DELETE FROM
   cdc_control.cdc_file_status WHERE table_name='<schema>.<table>'` (plus the smaller
   ledgers for hygiene). The `last_done_file=NULL` is what makes the next poll treat the table as
   fresh and replay ALL change files idempotently through the keyed upsert.
4. RELOAD the ranges onto the EMPTY table (range mode writes only, so the load's G1
   no-blank-after-CDC guard is not tripped). Do NOT force `--allow_manual_destructive`.
5. MERGE (writes the normal `_file_status/<table>.json`).
6. RESTART the bg CDC run with the SAME fork `--config_prefix` (`_orchestrator/bg-<slug>/`).

Verified by READING (CDC_RESET.md cites `glue_cdc_continuous.py` and `job2_load.py` line numbers):
the skip decision is `cdc_status.last_done_file`; the never-DELETE-the-row rule; the single-PK
upsert idempotency that makes full replay safe; G1's two-signal detector; G8/G9 default-warn.
NOT verified by execution here (no AWS/DSQL): the exact stored `table_name` string, the smaller
ledger tables' column names, and the live post-replay count.

NO DSQL CLIENT FROM CLOUDSHELL (operator decision, stated plainly): the operator does NOT have a
direct DSQL client in CloudShell, and the CDC skip state lives ONLY in DSQL `cdc_control`; no
shipped Lambda/job runs ad-hoc SQL. To run the step-3 statements the operator needs EITHER a
one-off Glue Python Shell job in the SAME VPC/subnets/role as the CDC job (reusing its IAM-token +
pg8000 DSQL connection) OR a VPC host with psql/pg8000 + an IAM auth token. DECISION FOR THE USER:
if neither is available, CDC progress cannot be reset from CloudShell, and the lower-risk path is a
fresh DMS full reload (new task/folder) per the main runbook instead of the in-place reset.

## NULL handling: copied unchanged from the earlier run

The patch adds NO null logic and changes NO null default. Every range run must carry the SAME
`--null_values` / `--null_rules` / `--csv_null_value` the earlier normal load-big run used, so the
data loads EXACTLY as before. The operator reads them with
`aws glue get-job-run --region <region> --job-name ... --run-id ... --query JobRun.Arguments
--output json` (README section 1) and pastes them verbatim into every range run (README section 4,
bold "PASTE ALL ARGUMENTS" note). Omit any key the earlier run did not set.

## Re-vet findings and fixes (manual-shard-vet/FINDINGS.md + BLOCKERS.md)

Verdict: SAFE WITH THESE FIXES. All addressed:

* B1 (BLOCKER) - apply anchor `_apply_v6_arg_overrides()` occurs twice in the real mirror. FIXED
  before READY_V1 (anchor is newline-delimited `"\n_apply_v6_arg_overrides()\n"`, count==1,
  replace-once). Confirmed: apply succeeds on the real base and output matches the prebuilt.
* F1 (MEDIUM) - `merge_ranges.list_table_files` over-matched: its regex counted
  `processed/LOAD*.csv` CDC copies as table files, so a stray `processed/LOAD99999999.csv` made a
  CORRECT complete load report MISSING and refuse `--write`. FIXED: merge now mirrors the loader's
  `_is_full_load_key` exactly - rejects any key with a `processed`/`failed` path segment and
  requires a decimal `^load0*[0-9]+\.csv$` basename. Tests added: the vetter's exact repro (2 real
  done + 1 `processed/LOAD99999999.csv` -> reconcile CLEAN and `--write` succeeds), a listing
  exclusion test, and a MUTATION check (removing the processed/failed exclusion miscounts the
  stray file).
* F2 (LOW) - `--region <region>` is now on EVERY `aws` command in the README and on every
  `merge_ranges.py` example (`--region <region>`).
* F3 (LOW) - the README section-4 start example now includes placeholders for
  `--null_values` / `--null_rules` / `--csv_null_value` and the DSQL overlay, with a bold "PASTE
  ALL ARGUMENTS from step 1" note so an operator never silently falls back to deployed constants.
* F4 (INFO) - mixed-width LOAD names would sort lexicographically; DMS zero-pads to fixed width so
  numeric == lexicographic for the real export (vet T1/T2). No change needed; noted here.
* F5 (INFO) - README now tells operators NOT to set `--auto_reblank_on_resume false` for range
  runs (it would reload done files on a rerun; the single-PK probe still prevents duplicates).

Test totals after fixes: builder suite 55 passed / 0 failed; vetter suite 19 passed / 0 failed.

## Decisions for the user

1. Use 4 disjoint ranges from `--plan 4` for `<table>` (matches the ~2 h goal and fits a
   typical DPU/IP budget). Only go to 10 if you have raised DPU + IP + `MaxConcurrentRuns`.
2. Keep `--max_write_concurrency` at your normal value for 4 runs; LOWER it per run (e.g. 100-150)
   if you push toward 10 concurrent runs.
3. Confirm the load-big job's `MaxConcurrentRuns` >= your run count before starting.
4. The table MUST be empty before the first run (range mode does not blank). Verify in DSQL.
5. `<table>` has a single-column PK: a mid-file crash resumes safely. For any NO-single-
   PK table, a mid-file crash fails closed and needs a full re-blank + restart of all ranges.
6. Always run the merge DRY-RUN, confirm CLEAN and that your manual SOURCE/DMS/DSQL counts
   agree, THEN `--write`, THEN start one normal (no-range) run to continue the pipeline.
