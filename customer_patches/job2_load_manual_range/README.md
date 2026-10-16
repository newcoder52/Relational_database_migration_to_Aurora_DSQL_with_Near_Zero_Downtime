# MANUAL-RANGE load patch for job2_load.py

> Range-mode load patch for job2_load.py: operator-chosen `--load_file_start`/`--load_file_end`;
> load only, no validation/CDC. Offline-tested only; not yet run on real AWS.

Load ONE big table with SEVERAL independent Glue `load-big` runs, each loading an
OPERATOR-CHOSEN inclusive slice of that table's `LOAD*.csv` part-files, then reconcile with
`merge_ranges.py` so the normal pipeline (validation, DMS resume, CDC) continues. Target:
`<schema>.<table>` (406 files, ~110M rows, ~106 GB) in ~4 runs, ~2 h.

Each run is its own Glue driver, so writer throughput scales with the number of runs
instead of fighting inside one driver's thread pool.

## What the patch adds (and what it does NOT change)

Two OPTIONAL Glue args on the load job: `--load_file_start` and `--load_file_end`
(INCLUSIVE). Each value is one of:

* a part-file NUMBER from the DMS name (`1` -> `LOAD00000001.csv`, `100` -> `LOAD00000100.csv`);
* a bare file name (`LOAD00000042.csv`);
* a full `s3://` key.

Both resolve against the real sorted listing of the table folder. Only start => start..last;
only end => first..end; neither => the script is BYTE-IDENTICAL to today (proven by the test
suite running the real functions both ways).

Every other arg the load job already takes is UNCHANGED and still honoured per run
(`--config_prefix`, `--max_files_in_parallel`, `--writers_per_file`,
`--max_write_concurrency`, `--null_values`, `--null_rules`, DSQL connection overlay, etc.).
No default is changed.

Safety: each range keeps its OWN progress doc (`_file_status/<table>.range-<first>-<last>.json`),
so concurrent runs never read-modify-write the same S3 object; range mode does NOT run the
whole-table empty check/reblank and does NOT clear shared state; an overlap ledger refuses a
run whose files overlap a DIFFERENT recorded range; a no-PK mid-file crash FAILS CLOSED (never
reblanks the whole table); the whole-table end validation is skipped (you verify counts in the
console); a failed file FAILS the run. The dropped-connection classifier now also treats
`EOF occurred in violation of protocol` (the SSL EOF <customer> saw on 2/406 files) as a retryable
dropped connection.

---

## 0. One-time: apply the patch and upload the patched script

Find the load-big job's script location (single line):

```
aws glue get-job --region <region> --job-name dms-dsql-<task-suffix>-load-big --query "Job.Command.ScriptLocation" --output text
```

That prints an `s3://.../job2_load.py`. Download it, back it up, apply, and re-upload. Set
SCRIPT to the value printed above, then paste this heredoc block:

```
SCRIPT=s3://REPLACE_ME/scripts/job2_load.py
aws s3 cp --region <region> "$SCRIPT" ./job2_load.deployed.py
aws s3 cp --region <region> "$SCRIPT" "${SCRIPT}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
python3 apply_manual_shard_patch.py job2_load.deployed.py job2_load_manual_shard.py
python3 -c "compile(open('job2_load_manual_shard.py').read(),'x','exec'); print('compiles OK')"
aws s3 cp --region <region> job2_load_manual_shard.py "$SCRIPT"
```

The apply step REFUSES to run if the file is not the expected `job2_load.py` or is already
patched. Uploading the patched file over the same key leaves the job byte-identical for any
run that does NOT pass the new args.

---

## 1. Read a normal run's Arguments (copy the other params, including NULL handling)

List recent runs, pick a normal (successful) one, and print its Arguments so you can copy
every other parameter a normal run needs (config_prefix, concurrency knobs, and the NULL
handling) VERBATIM. This patch adds NO null logic and changes NO null default: each range run
must carry the SAME NULL args the earlier normal load-big run used, so the data loads exactly
as before.

```
aws glue get-job-runs --region <region> --job-name dms-dsql-<task-suffix>-load-big --max-items 5 --query "JobRuns[].{Id:Id,State:JobRunState,Started:StartedOn}" --output table
```

```
aws glue get-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --run-id <job-run-id> --query "JobRun.Arguments" --output json
```

From that output copy, VERBATIM, for every range run:

* `--config_prefix` (points at THIS table's group, e.g.
  `s3://BKT/<schema>/config/_orchestrator/group-1/`) - use the SAME value for every range run;
* the NULL handling, if present: `--null_values`, `--null_rules`, `--csv_null_value`
  (copy each exactly as shown; if a key is absent in the normal run, do NOT add it);
* any concurrency knobs the normal run used: `--max_files_in_parallel`, `--writers_per_file`,
  `--max_write_concurrency`, and the DSQL overlay (`--dsql_endpoint`, `--dsql_user`,
  `--dsql_database`, `--region`) if present.

The ONLY args you add on top are `--load_file_start` / `--load_file_end` (and, per section 2b,
`--worker-type` / `--number-of-workers` as start-job-run flags). Everything else is copied so
NULL handling and behaviour are identical to the earlier run.

---

## 2a. Drop and recreate the DSQL target table EMPTY (before the first range run)

Range mode NEVER empties anything. You must present an EMPTY target before the first range
starts. The operator's chosen path is to DROP and recreate the table:

* Recreate with the SAME DDL as the original, INCLUDING the primary key and identity/default
  (discovery and the loader need the table and its single-column PK; the PK is also what makes
  a mid-file crash resumable without duplicates).
* Confirm 0 rows before starting any range run.

```
-- in your DSQL client (see section 6 note: the operator has NO DSQL client from CloudShell):
DROP TABLE IF EXISTS <schema>.<table>;
-- CREATE TABLE <schema>.<table> ( ... same columns ... , PRIMARY KEY (...) );
SELECT count(*) FROM <schema>.<table>;   -- MUST be 0 before you start
```

If this table already has CDC running (<task-suffix> does), do NOT just drop + reload:
you must also reset CDC progress or CDC will skip every change file it already applied and you
will LOSE updates. Follow section 8 (CDC reset) in order.

## 2b. Lower the load-big job to G.4X per run WITHOUT editing the shared job

`aws glue start-job-run` accepts `--worker-type` and `--number-of-workers` to override the job
definition's capacity for THIS run only, so the shared `load-big` job definition is not edited
and other callers are unaffected. Use these on every range run:

```
--worker-type G.4X --number-of-workers 20
```

Confirm your account/Glue version accepts per-run overrides (it is supported on
`start-job-run` for Glue 3.0+/4.0 jobs that use WorkerType/NumberOfWorkers):

```
aws glue get-job --region <region> --job-name dms-dsql-<task-suffix>-load-big --query "Job.{WorkerType:WorkerType,Workers:NumberOfWorkers,Glue:GlueVersion}" --output json
```

See RESULT.md "G.4X capacity math" for the recommended `--max_files_in_parallel` per run at
G.4X (driver memory is the limit) and the DPU/IP/connection budget for N=4, 6, 10.

## 3. Plan the ranges (optional, read-only)

Let the merge tool suggest N byte-balanced ranges from the real listing. `--dms-prefix` is
the table's DMS folder (same path the load uses for this table):

```
python3 merge_ranges.py --region <region> --bucket BKT --config-prefix <schema>/config/_orchestrator/group-1/ --table <schema>.<table> --dms-prefix s3://BKT/<schema>/<table> --plan 4
```

It prints lines like `run 0: --load_file_start LOAD00000001.csv --load_file_end LOAD00000104.csv`.
Numbers work too (`--load_file_start 1 --load_file_end 104`).

---

## 4. Start the N range runs (each its own driver)

Start one run per range. Keep the other args identical to a normal run (copied in step 1).

**PASTE ALL ARGUMENTS printed by step 1** into the `--arguments` JSON of every range run, then
ADD only `--load_file_start` / `--load_file_end`. In particular carry over the NULL handling
(`--null_values`, `--null_rules`, `--csv_null_value`) and the DSQL overlay
(`--dsql_endpoint`, `--dsql_user`, `--dsql_database`, `--region`) EXACTLY as the earlier run had
them - omit any key the earlier run did not set (do not invent one). The example below shows the
placeholders; replace the REPLACE_* values with the step-1 values (and delete any key the normal
run did not use):

```
aws glue start-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --worker-type G.4X --number-of-workers 20 --arguments '{"--config_prefix":"s3://BKT/<schema>/config/_orchestrator/group-1/","--load_file_start":"1","--load_file_end":"104","--max_files_in_parallel":"20","--writers_per_file":"8","--max_write_concurrency":"240","--null_values":"REPLACE_FROM_STEP1_OR_DELETE","--null_rules":"REPLACE_FROM_STEP1_OR_DELETE","--csv_null_value":"REPLACE_FROM_STEP1_OR_DELETE","--dsql_endpoint":"REPLACE_FROM_STEP1_OR_DELETE","--dsql_user":"REPLACE_FROM_STEP1_OR_DELETE","--dsql_database":"REPLACE_FROM_STEP1_OR_DELETE","--region":"<region>"}'
```

Repeat for the other ranges, changing ONLY `--load_file_start` / `--load_file_end` (keep every
other argument identical to the first range run):

```
aws glue start-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --worker-type G.4X --number-of-workers 20 --arguments '{"--config_prefix":"s3://BKT/<schema>/config/_orchestrator/group-1/","--load_file_start":"105","--load_file_end":"208","--max_files_in_parallel":"20","--writers_per_file":"8","--max_write_concurrency":"240"}'
```

```
aws glue start-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --worker-type G.4X --number-of-workers 20 --arguments '{"--config_prefix":"s3://BKT/<schema>/config/_orchestrator/group-1/","--load_file_start":"209","--load_file_end":"312","--max_files_in_parallel":"20","--writers_per_file":"8","--max_write_concurrency":"240"}'
```

```
aws glue start-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --worker-type G.4X --number-of-workers 20 --arguments '{"--config_prefix":"s3://BKT/<schema>/config/_orchestrator/group-1/","--load_file_start":"313","--load_file_end":"406","--max_files_in_parallel":"20","--writers_per_file":"8","--max_write_concurrency":"240"}'
```

(The 2nd-4th examples omit the NULL/overlay keys only to stay short - in practice paste the SAME
full argument set into every range run; only start/end change. `--max_files_in_parallel` is 20
here, not 30: at G.4X the driver memory limits concurrency - see RESULT.md capacity math.)

Each run prints a loud line at start: resolved first/last file, file count, bytes, and the
current estimated row count. If a start/end does not resolve, start is after end, the slice
is empty, or its files overlap a DIFFERENT recorded range, the run FAILS CLOSED with a clear
message and loads nothing.

Do NOT set `--auto_reblank_on_resume false` for range runs: it would make a range RERUN reload
its already-done files instead of skipping them (slower; for the single-PK target the per-chunk
probe still prevents duplicates, but there is no reason to turn resume off). Leave it at the
default.

IMPORTANT CONTRACT: the table must be EMPTY (or hold only rows from a previous attempt of
THESE SAME ranges) before the FIRST run starts. Range mode does not blank the table.

---

## 5. Monitor

```
aws glue get-job-runs --region <region> --job-name dms-dsql-<task-suffix>-load-big --max-items 10 --query "JobRuns[].{Id:Id,State:JobRunState,Err:ErrorMessage}" --output table
```

Look in each run's CloudWatch log for the per-file `-> N rows` lines and the final
`MANUAL RANGE first..last: files X, done Y, rows Z` summary.

## 6. Rerun a failed range

A run that had any failed file ends FAILED. Rerun it with the EXACT SAME start/end (same args)
to resume: it skips its done files and redoes only the unfinished ones (probe-guarded, no
duplicates, for a single-column-PK table). Rerunning the same range is NOT an overlap (it is a
resume) and is allowed.

```
aws glue start-job-run --region <region> --job-name dms-dsql-<task-suffix>-load-big --worker-type G.4X --number-of-workers 20 --arguments '{"--config_prefix":"s3://BKT/<schema>/config/_orchestrator/group-1/","--load_file_start":"209","--load_file_end":"312","--max_files_in_parallel":"30","--writers_per_file":"8","--max_write_concurrency":"240"}'
```

If a NO-single-column-PK table crashed mid-file, the rerun FAILS CLOSED (it will not reblank).
Recovery: blank the whole table, delete this table's `_file_status/*.range-*`,
`_range_result/*`, and `*.ranges.json` objects, and restart ALL ranges from scratch.

## 7. Merge and continue the pipeline

When all ranges show done, reconcile (dry-run default):

```
python3 merge_ranges.py --region <region> --bucket BKT --config-prefix <schema>/config/_orchestrator/group-1/ --table <schema>.<table> --dms-prefix s3://BKT/<schema>/<table>
```

It prints per-range and total rows and lists any missing / duplicate / failed / overlapping
files. Only when it says CLEAN, write the merged normal doc (it backs up any existing one):

```
python3 merge_ranges.py --region <region> --bucket BKT --config-prefix <schema>/config/_orchestrator/group-1/ --table <schema>.<table> --dms-prefix s3://BKT/<schema>/<table> --write
```

Then start a NORMAL load run for this table/group (NO range args). It will see every file
already done and print `all N file(s) already done on a prior run - nothing to load`, and the
pipeline continues (validation, DMS resume, CDC start) exactly as usual.

---

## 8. If this table already has CDC running: reset CDC progress after the reload

`<task-suffix>` already has its bg CDC job running and DMS running, so DSQL `cdc_control`
holds a CDC high-water mark for this table (`cdc_status.last_done_file` plus `cdc_file_status`
rows). If you drop + reload the table and do NOTHING to CDC, the CDC job will SKIP every change
file whose name sorts at/under `last_done_file` -> the updates/deletes since the full-load
snapshot are never re-applied -> SILENT DATA LOSS. You MUST reset this one table's CDC progress.

VERIFIED BY READING the CDC code (see RESULT.md for citations): the skip decision is driven
ENTIRELY by DSQL `cdc_status.last_done_file`; set it to NULL (keep the row - never DELETE the
`cdc_status` row) and the next poll treats the table as fresh and replays every change file
idempotently through the keyed upsert. The S3 `processed/` copies do NOT cause skipping and are
left in place. Do the steps IN ORDER:

1. STOP ONLY this table's bg CDC run (not batch-stop everything):

```
RUN=$(aws glue get-job-runs --region <region> --job-name "dms-dsql-<task-suffix>-bg-<schema>-<table>-<job-run-id>-cdc" --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text)
[ -n "$RUN" ] && aws glue batch-stop-job-run --region <region> --job-name "dms-dsql-<task-suffix>-bg-<schema>-<table>-<job-run-id>-cdc" --job-run-ids "$RUN"
```

Confirm it is no longer RUNNING before continuing. DMS may keep running.

2. DROP + recreate the target EMPTY (section 2a) and confirm 0 rows.

3. RESET this ONE table's CDC progress in DSQL (keep the `cdc_status` ROW; verify the exact
   `table_name` string with the SELECT first). These are DSQL statements, not CLI:

```
SELECT table_name, status, last_done_file, in_progress_file, last_offset, full_load_rows FROM cdc_control.cdc_status WHERE table_name='<schema>.<table>';
UPDATE cdc_control.cdc_status SET status='active', last_done_file=NULL, in_progress_file=NULL, last_offset=0, watermark_ts=NULL, rows_applied=0, full_load_rows=NULL, inserts_applied=0, deletes_applied=0, allow_mass_delete=false, error=NULL WHERE table_name='<schema>.<table>';
DELETE FROM cdc_control.cdc_file_status WHERE table_name='<schema>.<table>';
DELETE FROM cdc_control.cdc_chunk_log WHERE table_name='<schema>.<table>';
DELETE FROM cdc_control.cdc_apply_exceptions WHERE table_name='<schema>.<table>';
DELETE FROM cdc_control.cdc_skipped_ops WHERE table_name='<schema>.<table>';
DELETE FROM cdc_control.cdc_validation_failures WHERE table_name='<schema>.<table>';
```

The `cdc_file_status` DELETE is the one that MATTERS for replay; the others are hygiene (verify
their column name with `\d cdc_control.<table>` first - the smaller ledgers may differ).

4. RELOAD the ranges (sections 3-6) onto the freshly EMPTY table. Range mode only WRITES to the
   empty table, so the load's G1 "no-blank-after-CDC" guard is NOT tripped (G1 gates a blank, not
   a write to an empty table). Do NOT pass `--allow_manual_destructive`.

5. MERGE (section 7) so the normal `_file_status/<table>.json` exists.

6. RESTART the bg CDC run with the SAME fork `--config_prefix` it originally used (the
   `_orchestrator/bg-<slug>/` prefix, NOT the task prefix):

```
aws glue start-job-run --region <region> --job-name "dms-dsql-<task-suffix>-bg-<schema>-<table>-<job-run-id>-cdc" --arguments '{"--config_prefix":"s3://BKT/config/_task/<task-suffix>/_orchestrator/bg-<schema>-<table>-<job-run-id>/","--startup_execution":"manual-reset-REPLACE_TIMESTAMP"}' --query JobRunId --output text
```

With `last_done_file IS NULL` the next poll makes `pending` = ALL change files and replays them
in order; confirm the live count climbs and `cdc_status.status` returns to idle.

IMPORTANT - NO DSQL CLIENT FROM CLOUDSHELL: the operator does NOT have a direct DSQL client in
CloudShell. The CDC skip state lives ONLY in DSQL `cdc_control`, and no shipped Lambda or job runs
ad-hoc SQL. To run the step-3 statements you need EITHER a one-off Glue Python Shell job in the
SAME VPC/subnets/role as the CDC job (reuses its IAM-token + pg8000 DSQL connection) OR a host in
the VPC with `psql`/pg8000 and an IAM auth token. If neither is available you CANNOT reset CDC
progress, and the lower-risk alternative is a fresh DMS full reload (new task/folder) per the main
runbook's reload section - see RESULT.md "CDC reset" for the decision.

## 9. Manual row-count checklist (you verify in the console)

Range mode skips the automatic whole-table count. Verify manually:

1. SOURCE (SQL Server): `SELECT COUNT(*) FROM <schema>.<table>;`
2. DMS: FullLoadRows for the table in the task's table statistics (console or
   `aws dms describe-table-statistics`).
3. DSQL (target): `SELECT count(*) FROM <schema>.<table>;`
   (for >150M rows this may hit the 300s per-txn limit; count in key ranges if so.)
4. The merge tool's printed TOTAL rows (sum of per-range rows) should equal 2 and 3.

All four should agree. If any disagree, do NOT run `--write`; investigate first.
