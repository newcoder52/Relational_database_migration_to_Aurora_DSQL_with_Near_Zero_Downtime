# Relational Database Migration to Amazon Aurora DSQL — with Near-Zero Downtime

A schema-agnostic pipeline that migrates relational data (validated against Oracle sources)
into **Amazon Aurora DSQL** with a **full load → validate → continuous CDC → cutover** flow, so
the application keeps running against the source until you are ready to switch over.

**AWS DMS** extracts to Amazon S3 as CSV, **AWS Glue** loads, validates and continuously applies
the changes into Aurora DSQL, and **AWS Step Functions** runs each migration task end to end.

```
Source DB ──DMS (full load + CDC)──▶ S3 (CSV) ──AWS Glue──▶ Amazon Aurora DSQL
                                      │   Job 1  discover schema + PK/type metadata
              full load: LOAD*.csv ───┤   Job 2  load (Spark, per-file parallel)
              CDC:  <ts>.csv (Op col)─┘   Job 3  validate (counts + every column's content)
                                          CDC job  continuous apply (long-running)
```

![Full-load + CDC migration architecture](docs/architecture.png)

> **To deploy and run the pipeline, follow [`RUNBOOK.md`](RUNBOOK.md).** This page explains what
> the pipeline is and how it behaves; the RUNBOOK has every command, in order.

---

## Table of contents

- [Why this exists](#why-this-exists)
- [How it works](#how-it-works)
- [The Glue jobs and Lambdas](#the-glue-jobs-and-lambdas)
- [What a startup run does](#what-a-startup-run-does)
- [If a run fails](#if-a-run-fails)
- [Cutover](#cutover)
- [Monitoring](#monitoring)
- [Schema changes during CDC](#schema-changes-during-cdc)
- [Known limitations](#known-limitations)
- [Repository layout](#repository-layout)
- [Security](#security)
- [License](#license)

---

## Why this exists

Aurora DSQL is a distributed SQL database with a different write model than a traditional RDBMS
(per-transaction row and size limits, a ~1-hour connection cap, optimistic concurrency, no
`TRUNCATE`). DMS cannot write to DSQL directly, so this pipeline lands DMS output in S3 and uses
Glue to load and continuously apply it to DSQL within those limits: extract once, validate, and
keep the target in sync until cutover.

**Near-zero downtime:** the source stays live through full load and CDC. You stop writes to the
source only at cutover, once CDC has caught up and the target matches the source.

## How it works

- **The fleet is how you start and cut over tasks.** You launch the `fleet-startup` (or
  `fleet-cutover`) state machine once, by hand, with
  `{"bucket": "<pipeline bucket>", "inputPrefix": "<folder of fleet_tasks.csv>"}`. It reads
  `fleet_tasks.csv` from `s3://<bucket>/<inputPrefix>/`, checks every task first, and then starts
  the per-task **startup** (or **cutover**) state machine for each one. **One DMS task is one row
  in `fleet_tasks.csv`**, so starting a single task is a one-row list and starting many is the
  same list with more rows. Nothing runs on a schedule.
- **Two shared per-task Step Functions state machines — `startup` and `cutover` — do the actual
  work for every DMS task.** The fleet starts each one with just the DMS task's ARN
  (`{"taskArn": "arn:aws:dms:..."}`); the per-task startup and cutover state machines described
  below are exactly what the fleet runs per task.
- Settings shared by all tasks (project prefix, region, DSQL endpoint/user/database, Glue role,
  Glue network connection, CDC engine, control schema) live in one file,
  `s3://<bucket>/config/pipeline.json`. The fleet reads it (never writes it), and an edit applies
  to runs started after it.
- **Quick start — one `params.csv` + `tools/setup.sh`.** Copy `config/params.example.csv`, fill in
  `account_id`, `region`, `project`, `dsql_endpoint` (plus any optional keys), upload it as
  `s3://<bucket>/config/params.csv`, and run `tools/setup.sh s3://<bucket>/config/params.csv`
  (idempotent; add `--dry-run` to preview, `--with-drivers` to stage the Glue driver wheels). Setup
  builds all 6 roles, 8 Lambdas, 4 state machines and `config/pipeline.json` from that one file. The
  fleet reads the same `params.csv` and safely (re)publishes `config/pipeline.json` from it at
  startup. Then run tasks with the fleet. (params.csv is offline-tested; real-AWS test pending —
  run one small live fleet first. The hand-edited export block still works too.) See
  [`RUNBOOK.md` Step 3c](RUNBOOK.md#step-3c--pipeline-settings).
- The DMS task's **name** becomes its config folder, `s3://<bucket>/config/_task/<task name>/`,
  and the middle of its Glue job names, `<project>-<task name>-<role>` (for example
  `<project>-<task name>-load`). Each task runs and cuts over independently.
- DMS writes CSVs to `s3://<bucket>/[<BucketFolder>/]<schema>/<table>/` — the full load as
  `LOAD*.csv`, changes as `<timestamp>.csv` with a leading `Op` column. Folder names are used in
  the case DMS writes them.
- The S3 layout (`BucketFolder`, the timestamp column, the header row, the NULL marker) is read
  from the DMS endpoint at run time, not typed in.

See [`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md) for the fleet's inputs, preflight checks,
skip rules, results and limits, and [`RUNBOOK.md`](RUNBOOK.md) for the commands.

## The Glue jobs and Lambdas

Each task gets its own **five Glue jobs**, created by the startup run and deleted at cutover:

| Job | File | What it does |
|-----|------|--------------|
| **Job 1 — Discovery** | `scripts/job1_discovery.py` | Reads the DSQL target schema (authoritative for the column set) and builds per-table type and primary-key metadata. Finds each table's DMS folder whatever its letter case; fails loudly if none of the task's tables has a folder. |
| **Job 2 — Load** (`load`, `load-big`) | `scripts/job2_load.py` | Full load (Spark reads, pg8000 writes from the driver). Big tables load **per file in parallel** (250 MB files, up to 30 at once), with per-file S3 resume and a per-file rows-read = rows-committed check. |
| **Job 3 — Validate** | `scripts/job3_validate.py` | Per key range: row counts plus a content check of **every column**, chosen by its real DSQL type — integer/numeric: exact sum (after the load's rounding); real/double: sum with a small float tolerance; text/binary: non-null count, length, min, max; boolean: true count; timestamp/date: sum of instants; json: non-null count. A per-value hash is added when DSQL supports `md5()`. Names the column that differs; fails instead of skipping. |
| **CDC — Continuous** | `scripts/glue_cdc_continuous.py` | Long-running job that applies inserts, updates and deletes to DSQL, multi-table, with crash-proof resume through the DSQL `cdc_control` tables. Runs as a **Python shell** job by default; as a **Spark** job if `cdc_engine` is `spark`, or automatically if the Python-shell drivers fail (see below). |

The state machines call **eight Lambdas** (seven for the per-task workflows, plus `preflight_tasks`
for the fleet): `resolve_task`, `driver_discovery`, `plan_split`, `create_glue_jobs`,
`stop_cdc_run`, `drain_check`, `drop_tags`, and `preflight_tasks`. All ship in one zip;
`drain_check` and `drop_tags` connect to DSQL (they bundle `pg8000`), and `driver_discovery`
imports `prepare_cdc_wheels.py` from the same zip. The RUNBOOK's
[Step 2](RUNBOOK.md#step-2--create-the-lambda-functions) covers packaging.

## What a startup run does

The fleet starts one per-task **startup** execution per `fleet_tasks.csv` row, with the same input
it would use by hand (`{"taskArn": "..."}`). The `preflight_tasks` Lambda runs every per-task
check first and refuses to start anything if any row fails. Each per-task startup then runs, in
the order the code runs them:

1. **Check the task** (seconds). Reads `config/pipeline.json`, works out the folder and job
   names, and checks the DMS task **before starting it**: type `full-load-and-cdc`,
   `StopTaskCachedChangesApplied=true` (and `StopTaskCachedChangesNotApplied` not true),
   `AddColumnName=true`, an S3 target endpoint writing to the pipeline bucket, the same region as
   `pipeline.json`, a name of letters/digits/hyphens, and a task not already past its full load.
   A problem ends the run at **`ResolveFailed`**; DMS is untouched. A second startup for the same
   task while one is running also stops here, as does a folder owned by a different task ARN.
   (The fleet's preflight runs these same checks across the whole list before any task starts.)
2. **Check the driver files** (seconds; about a minute the first time). Checks the three
   `driver-*` folders and, for a Python-shell CDC job, prepares the `driver-cdc/` wheels into
   `driver-cdc-prepared/<fingerprint>/` so Glue can install them with no internet. A bad or
   missing wheel ends the run at **`DriversFailed`**, still before DMS starts — unless the Spark
   fallback can take over (step 7): if the Python-3.9 driver-cdc check fails but boto3, botocore
   and s3transfer are usable, this task's CDC job is built as Spark instead.
3. **Start DMS** and wait for the full load to finish (`STOPPED_AFTER_CACHED_EVENTS`; polled every
   30 s for up to 24 h, else **`DmsTimedOut`**; a DMS failure is **`DmsFailed`**).
4. **Create this task's five Glue jobs** from `glue-templates/`.
5. **Discover, then load and validate each table group** (up to six groups at once). If any group
   fails, the run stops at **`GroupsFailed`** and **DMS stays paused**, so CDC never starts on top
   of an incomplete load.
6. **Resume DMS into CDC and start the CDC job**, then wait up to 45 min until the CDC job
   confirms it reached its poll loop (**`CdcStartNotConfirmed`** if it never does;
   **`CdcRunFailed`** / **`CdcRunEnded`** if the run fails or stops first).
7. **Spark fallback, once.** If a Python-shell CDC run fails because Glue couldn't install or
   import its drivers (pip/PyPI timeouts, a missing or wrong-Python wheel, `No module named
   'pg8000'`, `Unknown service: 'dsql'`), the CDC job is re-created **with the same name** as
   Spark and started again. Any other error (DSQL, permissions, data) is not retried; a second
   failure ends at **`CdcFallbackFailed`**. The switch is recorded in
   `config/_task/<task name>/_cdc_engine.json`, so later startups of that task build Spark
   straight away; delete that file to go back to Python shell. Set `cdc_spark_fallback: false` in
   `pipeline.json` to disable it, or switch a job by hand with `tools/switch_cdc_engine.py`.

When the run succeeds, the full load is in DSQL and validated, and CDC is applying changes. The
fleet's `FleetStarted` result only means every task's startup was started and still running after
30 s (past its own input checks) — watch each per-task execution for the outcome.

Per-task input keys (set per row in `fleet_tasks.csv`, rarely needed): `task_suffix` uses a
different folder and job name than the DMS task's name, and `adopt_existing_folder=true` reuses a
folder from a run made before the shared state machines existed. The fleet commands are in the
RUNBOOK's [Step 4](RUNBOOK.md#step-4--create-the-state-machines) (create the state machines)
and [Step 5b](RUNBOOK.md#5b--upload-each-tasks-table-list) (stage each task's table list).

## If a run fails

The fleet starts tasks and then returns; each per-task startup runs on its own. **Where a task's
own run stopped decides what to do** — because once DMS has been resumed into CDC it is past its
full load, and starting that task's startup again just ends at `ResolveFailed`. Re-launching the
fleet with the same `fleet_tasks.csv` only starts the tasks that still need it: tasks whose
per-task execution is already running are skipped (`already_running`), and startup tasks already
past their full load are skipped (`past_full_load`).

| The task's run stopped… | What is already done | Then |
|---|---|---|
| **Before DMS started** — `MissingTaskArn`, `ResolveFailed`, `DriversFailed` | nothing; DMS untouched | Fix the cause in the error (or the fleet's `PreflightFailed` row) and launch the fleet again — the fixed task starts, the rest are skipped. |
| **During the full load** — `DmsFailed`, `DmsTimedOut` | DMS was starting/running | Fix it in the DMS console. The startup can only start a task that hasn't finished its full load; a task already past it needs a clean-slate reload with a new DMS task. |
| **Load/validate** — `GroupsFailed`, or `PipelineFailed` before `ResumeDmsToCdc` | full load is in S3; DMS is paused | Fix the failed group (its Glue log has the cause) and launch the fleet again — finished tables are skipped, and already-running tasks are skipped too. |
| **After DMS was resumed into CDC** — `CdcRunFailed`, `CdcRunEnded`, `CdcFallbackFailed`, `CdcStartNotConfirmed`, or `PipelineFailed` at `StartCdcJob` | full load done; **DMS is capturing changes to S3** | **Do not re-launch that task.** Nothing is lost while CDC is down. Fix the cause and start the CDC job by hand with its `--config_prefix` argument (RUNBOOK). |

The exact command for each state is in [`RUNBOOK.md`](RUNBOOK.md).

## Cutover

Cutover is driven by the **`fleet-cutover`** state machine, the same way as startup: one launch
with `{"bucket": "...", "inputPrefix": "..."}` reads `fleet_tasks.csv`, preflight-checks every
task, and starts the per-task **cutover** for each. **Cut over only tasks whose CDC has caught up**
(every table idle in `cdc_control.cdc_status`) — the fleet checks inputs, not readiness, and
**cutover is irreversible per task**.

For each task, in order:

1. **Stop writes to the source** for this task's tables, and let DMS deliver the last changes
   (its CDC latencies near zero). The per-task cutover's **first step stops DMS**, so any source
   change made after that is never migrated — getting this order wrong loses data silently. Do
   this for every table of every task in the list before you launch the fleet.
2. The per-task cutover stops the DMS task, waits until each table's latest CDC file is applied
   (up to ~12 h, else **`CdcDrainTimedOut`**), stops this task's CDC run, drops the internal
   `_cdc_file` tracking column, and deletes the task's five Glue jobs. It finds the task by its
   ARN, so a renamed task still cuts over its original folder and jobs. Other tasks are unaffected.
3. Repoint the application at Aurora DSQL.

End states (per task): `CutoverSucceeded`; `GlueJobsNotDeleted` (data is cut over, a Glue job
delete failed — delete it by hand); `CdcDrainTimedOut`, or `CutoverFailed` at a later step (DMS is
stopped); `ResolveFailed` or `CutoverFailed` while DMS is still running (nothing changed). **Once a
task's DMS is stopped, do not cut it over again** — its first step would fail on the already-stopped
task; finish that task by hand instead (see [`RUNBOOK.md`](RUNBOOK.md)). Re-launching the fleet is
safe: a task whose cutover is already running is skipped (`already_running`).

## Monitoring

**CDC control tables** (in the control schema, `cdc_control` by default):

```sql
-- per-table status (idle = caught up, blocked = needs attention)
SELECT table_name, status, error FROM cdc_control.cdc_status;

-- per-file apply ledger
SELECT cdc_file, status, rows_applied, all_rows_committed
FROM cdc_control.cdc_file_status WHERE table_name = 'target_schema.table' ORDER BY 1;

-- updates skipped for tables without a primary key
SELECT * FROM cdc_control.cdc_skipped_ops WHERE table_name = 'target_schema.table';
```

**Validation report:** one per table group, at
`s3://<bucket>/config/_task/<task name>/_orchestrator/group-<n>/_validation_report.json`
(`match`, `mismatch` or `error` per table, naming the differing column — never `skipped`).

**Glue logs** (CloudWatch): the Spark jobs (discovery, load, validate, and CDC when it runs as
Spark) log to `/aws-glue/jobs/output` and `/aws-glue/jobs/error`; a Python-shell CDC job logs to
`/aws-glue/python-jobs/output` and `/aws-glue/python-jobs/error`. After a Spark fallback the CDC
job's logs move to the Spark log groups.

To unblock a table after you have fixed the cause:
`UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';` — never
delete the row (applied files stay in the folder and would all be replayed).

## Schema changes during CDC

Golden rule: **every DDL must be followed by DML** (DMS only surfaces a schema change on the next
data row). The full matrix is in `ENGINEERING_RECORD.md` §4.

| Source change | Behavior | Action |
|---|---|---|
| `ADD COLUMN` (one or many) | Added to the target automatically | none |
| `RENAME COLUMN` | Renamed automatically (needs the DMS API reachable from the CDC job; without it the column is added as new) | none |
| `DROP COLUMN` | **Table blocks** (a drop can't be told apart from an omitted column) | drop it on the target, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'` (never delete the row) |
| `CHANGE DATA TYPE` | **Not detected** by the header-diff reconciliation | `ALTER` the target type and re-apply the affected rows |

## Known limitations

Details are in `ENGINEERING_RECORD.md` and `CDC_EDGE_CASE_RESULTS.md`.

- **NULL marker:** only an empty field or the DMS endpoint's `CsvNullValue` (default `NULL`) is
  stored as NULL. Every other text value, including `NA`, `NONE` and `N/A`, is stored as written
  (USAGE_GUIDE §4b). Don't change `CsvNullValue` partway through a migration.
- **Multi-column primary keys:** the main CDC job leaves these tables alone and lists them at
  startup; run a separate CDC job for them. Cutover treats such a table as caught up only when its
  newest S3 CDC file has a `cdc_control.cdc_file_status` row with `table_name` = `<dsql_schema>.<table>`
  (lowercase), `cdc_file` = the file's S3 key (or a value ending in its file name) and
  `status='done'` (or `all_rows_committed=true`); otherwise cutover waits the full ~12 h.
- **Tables without a primary key:** inserts and deletes are applied; **updates are skipped and
  logged** to `cdc_control.cdc_skipped_ops`, unless you declare a stable logical key.
- **DROP COLUMN** during CDC blocks the table until you fix it; **CHANGE DATA TYPE** isn't
  detected.
- **Validation** compares per-range summaries, not individual rows: two values swapped between
  rows of the same range can cancel out. A table whose key can't be split is compared as one
  whole-table range.
- **DSQL allows at most 10 schemas per database** (not adjustable), and the CDC job adds one
  (`cdc_control`), so keep ≤ 9 of your own; the fleet preflight enforces it across the whole list.
- **CDC Glue runs stop after 7 days** (the 10080-minute Glue maximum). A long migration's CDC run
  ends on its own — restart it by hand (RUNBOOK), or cut over before 7 days.
- **A DMS task can be loaded only once:** the startup refuses a task already past its full load, so
  a clean-slate reload needs a **new** DMS task (new name → new folder).
- CDC needs the source schema in the Oracle LogMiner dictionary before capture starts.
- Applied CDC files are **copied** to `<table>/processed/` (the originals are never deleted, so S3
  use grows). Without DMS/CloudWatch reachable from the CDC job, those optional calls time out
  after a few seconds and rename detection is off; changes still apply.

## Repository layout

```
scripts/                    The 4 Glue job scripts (job1_discovery, job2_load,
                            job3_validate, glue_cdc_continuous)
lambdas/                    The 7 per-task orchestration Lambdas (resolve_task, driver_discovery,
                            plan_split, create_glue_jobs, stop_cdc_run, drain_check,
                            drop_tags), prepare_cdc_wheels.py (used by driver_discovery,
                            same zip), and preflight_tasks.py (the fleet's preflight)
stepfunctions/              startup + cutover per-task state machines, plus the
                            fleet-startup / fleet-cutover launchers that drive them
glue-templates/             The 6 Glue job templates (discovery, load, load-big, validate,
                            cdc, cdc-spark)
iam/                        Role trust + policy documents (per-task roles, plus the fleet roles)
config/                     pipeline.example.json, params.example.csv, fleet_tasks.example.csv
tools/                      setup.sh (one-command setup from params.csv),
                            switch_cdc_engine.py (switch a CDC job Python shell <-> Spark by hand)
RUNBOOK.md                  Step-by-step deploy and operate guide
USAGE_GUIDE.md              Day-to-day operation, monitoring, manual runs
ENGINEERING_RECORD.md       Architecture, every bug found and fixed, DDL support matrix
CDC_EDGE_CASE_RESULTS.md    CDC edge cases and data-type limitations
docs/
  FLEET_LAUNCHER.md                     How the fleet works (inputs, preflight, skips, results)
  NO_PK_CDC_UPDATE_TRACKING.md          Design note: CDC for tables without a primary key
  USAGE_GUIDE.docx                      Word version of the usage guide
  CONSIDERATIONS_AND_LIMITATIONS.docx   Considerations and limitations
  PERFORMANCE_GUIDE.docx                Sizing and performance
  architecture.png                      Architecture diagram (full load + CDC)
  FullLoad_CDC_oracle-DSQL.drawio       Editable draw.io source for the diagram
```

## Security

No credentials or account-specific values are committed. You supply your own bucket, cluster
endpoint, schema and role ARNs through `config/pipeline.json` (or `config/params.csv`, which builds
it) and the RUNBOOK's fill-in steps. The
IAM policies in `iam/` are least-privilege templates scoped to your project prefix and bucket.
DSQL authentication uses short-lived IAM tokens generated at run time — no stored passwords.

## License

Licensed under the [MIT-0 License](LICENSE).
