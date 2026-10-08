# End-to-End Usage Guide — DMS → S3 → Glue → Aurora DSQL

_How to operate the pipeline for a migration task: prerequisites, running full load +
validation + CDC with the fleet, handling schema changes (DDL), cutover, monitoring, and
troubleshooting._

Companion docs: `RUNBOOK.md` (one-time deploy of IAM/lambdas/state machines/scripts) and
`ENGINEERING_RECORD.md` (architecture, fixes, DDL limitations). **Read the DDL limitations in
the Engineering Record before relying on schema-change replication.** This guide describes the
code **as it is today, including bugs that aren't fixed yet** — those are called out inline and
collected in `RUNBOOK.md`.

---

## 0. Mental model (read once)

- **The fleet is how you start and cut over tasks.** You launch the `fleet-startup` (or
  `fleet-cutover`) state machine once, by hand, with
  `{"bucket": "<pipeline bucket>"}`. The fleet reads
  `fleet_tasks.csv` from the fixed folder `s3://<bucket>/config/`, checks every task, and starts the
  per-task **startup** (or **cutover**) for each row. **One DMS task = one row in `fleet_tasks.csv`** — a
  single task is a one-row list, many tasks are more rows.
- **Two shared per-task state machines (startup, cutover) do the work for every DMS task**, each
  started by the fleet with `{"taskArn": "..."}`. Shared settings live in
  `s3://<bucket>/config/pipeline.json`; each task's config prefix is
  `s3://<bucket>/config/_task/<task name>/` (the DMS task's name). The fleet reads `pipeline.json`;
  it never writes it.
- DMS writes CSVs to `s3://<bucket>/<schema>/<table>/` (full load = `LOAD*.csv`, CDC =
  `<timestamp>.csv` with a leading `Op` column). Glue loads them into DSQL.
- Flow per task: **full load → (Glue) discover → load → validate → resume DMS to CDC →
  (Glue) CDC job applies ongoing changes → cutover** when ready.
- The endpoint's settings (bucket folder, timestamp column, headers) are **auto-derived** by
  `resolve_task` — you do not hardcode them.

---

## 1. Prerequisites (per task)

1. **One-time deploy done** (per `RUNBOOK.md`): IAM roles (setup creates the three
   `<project>-{glue,lambda,sfn}-exec-role` roles, or — with `manage_iam=false` — **uses the roles
   your IAM team already made**, named by `glue_role_arn`/`lambda_role_arn`/`sfn_role_arn`; see
   [RUNBOOK §4](RUNBOOK.md#using-roles-your-iam-team-already-created-manage_iamfalse)), the Lambda
   functions
   ([RUNBOOK Step 2](RUNBOOK.md#4-set-up) — **eight** functions:
   seven for the per-task workflows plus `preflight-tasks` for the fleet; `lambdas/` holds **nine**
   `.py` files because `prepare_cdc_wheels.py` ships inside the driver-discovery zip rather than as
   its own function), scripts staged to
   `s3://<bucket>/scripts/`, glue-templates staged, and the **three driver folders** populated:
   - `driver-fullload/`, `driver-validation/` — DSQL driver wheels only (pg8000, scramp,
     asn1crypto, python-dateutil, six).
   - `driver-cdc/` — the same DSQL drivers **plus** modern `boto3`/`botocore` wheels, downloaded
     **for Python 3.9** ([RUNBOOK Step 3b](RUNBOOK.md#4-set-up)). The startup
     workflow checks them and prepares install-safe copies in `driver-cdc-prepared/` before DMS
     starts, so the Python shell CDC job installs them without internet.
   - `config/pipeline.json` written ([RUNBOOK Step 3c](RUNBOOK.md#4-set-up)) and
     the two shared per-task state machines plus the two fleet state machines created
     ([RUNBOOK Step 4](RUNBOOK.md#4-set-up)). The quickest way to do all
     of this is one `params.csv`: copy `config/params.example.csv`, fill it in, upload it as
     `s3://<bucket>/config/params.csv`, and run `tools/setup.sh s3://<bucket>/config/params.csv`
     (idempotent; `--dry-run` previews, `--with-drivers` stages the driver wheels). The fleet reads
     the same `params.csv` and safely (re)publishes `config/pipeline.json` from it at startup.
2. **DMS S3 target endpoint** configured with (these are validated automatically):
   - `AddColumnName = true` (CSVs have header rows),
   - `DatePartitionEnabled = false`,
   - no `CdcPath` / `PreserveTransactions`,
   - `BucketFolder` empty, or any folder: every job reads it from the endpoint.
3. **DMS task** created as `full-load-and-cdc` with `StopTaskCachedChangesApplied=true`, and a
   table mapping that **lowercases** columns (schema and table names may stay in any case; this
   example also renames the schema and lowercases tables):
   ```json
   {"rules":[
     {"rule-type":"selection","rule-id":"1","rule-name":"1",
      "object-locator":{"schema-name":"SRC_SCHEMA","table-name":"%"},"rule-action":"include"},
     {"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"rename",
      "rule-target":"schema","object-locator":{"schema-name":"SRC_SCHEMA"},"value":"target_schema"},
     {"rule-type":"transformation","rule-id":"3","rule-name":"3","rule-action":"convert-lowercase",
      "rule-target":"table","object-locator":{"schema-name":"SRC_SCHEMA","table-name":"%"}},
     {"rule-type":"transformation","rule-id":"4","rule-name":"4","rule-action":"convert-lowercase",
      "rule-target":"column","object-locator":{"schema-name":"SRC_SCHEMA","table-name":"%","column-name":"%"}}
   ]}
   ```
4. **Target tables exist in DSQL** (created from your clean DDLs) in the lowercased target
   schema, with a single-column PK where possible (multi-column-PK tables are applied by their own
   per-table composite (`ck`) fork CDC job; range-validation needs an integer PK).
5. **Table list** — nothing to stage. The pipeline builds each task's table list automatically
   from the DMS task after its full load (from `describe_table_statistics` plus the task's table
   mappings), writing `s3://<bucket>/config/_task/<task name>/table_manifest.csv` for you. To load
   fewer tables, narrow the DMS task's selection rules.

---

## 2. Run the pipeline (the fleet)

Starting a migration — one task or many — always goes through the **fleet-startup** state machine.
Put one row per DMS task in `fleet_tasks.csv` (columns `task_arn`, optional `task_suffix`, optional
`adopt_existing_folder`), upload it to the bucket, and launch the fleet once with the bucket and
the folder that holds it ([RUNBOOK Step 4](RUNBOOK.md#4-set-up)):

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn-of-$PROJECT-fleet-startup> \
  --name fleet-startup-$(date +%Y%m%d%H%M) \
  --input '{"bucket":"<pipeline bucket>"}'
```

The fleet's `preflight-tasks` Lambda checks **every** task first (reusing `resolve_task`'s own
rules); if any row fails, it stops at `PreflightFailed` and **nothing starts**. Otherwise it starts
one per-task **startup** execution per row (five at a time) with the same input you would pass by
hand (`{"taskArn": "..."}`, plus `taskSuffix`/`adoptExistingFolder` only if the row sets them), and
confirms each one got past its own input checks. `FleetStarted` means every task's startup was
started (or skipped as already started); it does **not** mean the migrations finished — watch each
per-task execution. See [`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md) for inputs, preflight checks,
skip rules, results and limits.

Each per-task startup then performs, in order (matching the `startup` state machine):
1. **ResolveTask** → reads `config/pipeline.json`; derives the task's folder and job names from its
   name; derives `cdcRoot`, `timestampColumnName`, S3 settings from the endpoint; checks the DMS task
   **before starting it** (fails at `ResolveFailed` if `StopTaskCachedChangesApplied` isn't true,
   `AddColumnName` isn't true, the endpoint writes to another bucket, the task is already past its
   full load, `DatePartitionEnabled=true` or `CdcPath` set, or another startup for this task is
   already running). The fleet's preflight runs these same checks across the whole list first.
2. **DriverDiscovery** ×3 (fullload / validation / cdc folders) → runs **before DMS starts**, so a
   wrong or missing wheel fails in seconds at `DriversFailed` with **no DMS cost**. For a
   Python-shell CDC job this step also prepares `driver-cdc-prepared/`. With `cdc_spark_fallback`
   on, a problem that only affects Python shell (e.g. a wheel built for 3.10) builds this task's
   CDC job as Spark instead of stopping.
3. **StartDmsTask** → full load; waits for `STOPPED_AFTER_CACHED_EVENTS` (polled up to ~24 h).
4. **BuildTableList** → builds this task's table list from the DMS task (`describe_table_statistics`
   + the task's table-mapping transformations), writing `table_manifest.csv` and
   `table_list_source.json` under `config/_task/<task name>/`. Fails at `BuildTableListFailed`
   (before any Glue job) if a table didn't load cleanly, a transformation can't be reproduced for
   the S3 folder names, or more than 9 distinct DSQL schemas result.
5. **CreateGlueJobs** → creates this task's 5 Glue jobs (discovery/load/load-big/validate/cdc).
6. **RunDiscovery** (Job1) → writes `_manifest_index.json` + per-table column mappings.
7. **PlanSplit** + **GroupFanOut** → runs **Job2 load** then **Job3 validate** per group.
8. **ResumeDmsToCdc** → resumes DMS from cached-changes stop into ongoing CDC.
9. **StartCdcJob** → launches the continuous CDC job and confirms it started. A Python-shell run
   that fails on its drivers is switched to Spark automatically (see below).

After this, each task's full load is in DSQL, validated, and CDC is live.

**Automatic switch to Spark when the CDC drivers fail** (`cdc_spark_fallback` in
`config/pipeline.json`, default `true`). Two places:
- *Before DMS starts:* if `driver-cdc/` fails the Python-shell checks but has good boto3, botocore
  and s3transfer wheels, the CDC job is built as Spark.
- *When the CDC run starts:* if Glue's error shows a driver problem (pip/`pypi.org`,
  `CalledProcessError`, a `.whl` failing to install or missing, wrong Python, a driver module that
  won't import, `Unknown service: 'dsql'`), the CDC job is deleted and re-created with the same
  name as Spark, started with the same arguments and confirmed again. Once only; other errors are
  not touched.

The Spark CDC job picks its pg8000 wheels by name from `driver-fullload/` (else
`driver-validation/`), and boto3/botocore/s3transfer from `driver-cdc/`, the same as the
full-load jobs. Either way `config/_task/<task name>/_cdc_engine.json` records why, and later startups of that task
use Spark. Delete the file to go back to Python shell. CDC started by hand later (outside the
workflow) isn't switched automatically: use `tools/switch_cdc_engine.py`, which picks the drivers
the same way and records the choice in `_cdc_engine.json`.

---

## 3. Driving one step by hand (for testing / debugging)

The fleet always starts a task through its per-task **startup** state machine; you do not normally
start a per-task execution yourself. These are the individual steps the per-task startup runs, for
when you need to reproduce or debug one of them in isolation. Set once:
```bash
REGION=us-east-1; PROJECT="<project>"; SUF="<task name>"   # the DMS task's name
CFG="s3://<bucket>/config/_task/$SUF/"
ARN="<dms-task-arn>"
```

1. **Full load** → wait for cached-events stop:
   ```bash
   aws dms start-replication-task --region $REGION --replication-task-arn "$ARN" \
     --start-replication-task-type start-replication
   # poll ReplicationTasks[0].StopReason until *STOPPED_AFTER_CACHED_EVENTS*
   ```
2. **Resolve endpoint values** (needed for job args):
   ```bash
   aws lambda invoke --region $REGION --function-name $PROJECT-resolve-task \
     --payload "$(printf '{"taskArn":"%s","configPrefix":"%s"}' "$ARN" "$CFG" | base64)" rt.json
   # -> cdcRoot, timestampColumnName, s3Settings
   ```
3. **Driver lists** (per folder):
   ```bash
   for P in driver-fullload driver-cdc; do
     aws lambda invoke --region $REGION --function-name $PROJECT-driver-discovery \
       --payload "$(printf '{"bucket":"<bucket>","drivers_prefix":"%s"}' "$P" | base64)" drv_$P.json
   done
   ```
4. **Create jobs** (once) via `create-glue-jobs` with `extraPyFiles` = fullload list,
   `cdcExtraPyFiles` = cdc list, plus `cdc_root` + `timestampColumnName` from `resolve_task`.
5. **Discovery → Load → Validate**:
   ```bash
   aws glue start-job-run --job-name $PROJECT-$SUF-discovery --arguments "{\"--config_prefix\":\"$CFG\"}"
   aws glue start-job-run --job-name $PROJECT-$SUF-load     --arguments "{\"--config_prefix\":\"$CFG\",\"--extra-py-files\":\"<fullload-list>\"}"
   aws glue start-job-run --job-name $PROJECT-$SUF-validate --arguments "{\"--config_prefix\":\"$CFG\",\"--extra-py-files\":\"<fullload-list>\"}"
   ```
6. **Resume to CDC + start CDC job**:
   ```bash
   aws dms start-replication-task --region $REGION --replication-task-arn "$ARN" \
     --start-replication-task-type resume-processing
   aws glue start-job-run --job-name $PROJECT-$SUF-cdc --arguments "{
     \"--config_prefix\":\"$CFG\",\"--dms_task_arn\":\"$ARN\",
     \"--cdc_root\":\"<cdcRoot>\",\"--timestamp_column\":\"<timestampColumnName>\"}"
   ```
   No `--extra-py-files` here: `create-glue-jobs` saved the right driver setup on the job (Python
   shell: the `driver-cdc/` list; Spark: `driver-fullload/` plus boto3 via
   `--additional-python-modules`). Passing the `driver-cdc/` list to a Spark CDC job breaks it.
   Always pass `--config_prefix` as a run argument: cutover finds the CDC run by it.

> The CDC job is a **continuous poller** — it stays RUNNING and applies new files each cycle.
> Stop it with `aws glue batch-stop-job-run` when cutting over or pausing.
>
> **7-day limit (known issue).** The CDC Glue run has a hard **7-day (10080-minute)** timeout —
> the Glue maximum, set on both `cdc.json` and `cdc-spark.json`. Nothing restarts it
> automatically. A migration that stays in CDC for more than a week will have its CDC run end on
> its own (DMS keeps writing change files, so nothing is lost); start the CDC job again by hand to
> resume. Cut over within 7 days where you can. See `RUNBOOK.md`.

---

## 4. Handling schema changes (DDL) during CDC

**Golden rule: every DDL must be followed by DML.** DMS only surfaces a schema change on the
next data row after the DDL. Sequence any change as **DML → DDL → DML**.

| You do (source) | Pipeline behavior | Action needed |
|---|---|---|
| `ADD COLUMN` (single or many) | Auto-added to target | none |
| `RENAME COLUMN` (single, multiple, or combined with ADD) | Auto-renamed on target (positional detection) | none |
| `DROP COLUMN` | **Blocks the table when a change row later omits a column that still exists on the target** (the missing-column guard can't tell a genuine DROP from a column accidentally omitted from a change row, which would silently NULL data, so it stops) | remediate: `ALTER TABLE <target> DROP COLUMN <col>`, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'` (CDC resumes the blocked file at its saved offset; never delete the row) |
| `CHANGE DATA TYPE` | **Silent — value may be truncated/coerced to the old target type; no error** | avoid, or manually `ALTER` the target column type + re-apply affected rows |

- To force a rename deterministically (instead of relying on positional detection), add to the
  table's config `metadata.rename_hints`: `{"old_col":"new_col"}`.
- To disable the positional rename default (revert to conservative ADD-and-keep-old), pass
  `--single_swap_is_rename false` to the CDC job.

See `ENGINEERING_RECORD.md` §4 for the full tested matrix and root causes.

---

## 4b. How NULLs and text values are stored

DMS marks a real NULL in its CSV files with the S3 endpoint's `CsvNullValue` (DMS default: the text
`NULL`). The load, validation and CDC jobs use exactly that marker, read from the endpoint at
startup:

| Value in the DMS file | Stored in DSQL |
|---|---|
| the marker (`NULL` by default), or an empty field | NULL |
| `NA`, `N/A`, `NONE`, `(NULL)`, `\N`, `null`, ` NULL ` and any other text | exactly as written, spaces included |
| a number, date, uuid or boolean | trimmed and converted; whitespace-only becomes NULL |

So with the default marker, a source text value that is literally `NULL` can't be told apart from a
real NULL and is stored as NULL. If that matters, set `CsvNullValue` on the endpoint to something
that never appears in your data before the full load.

> **Upgrading from an earlier version:** earlier versions stored `NA`, `N/A`, `NONE`, `(NULL)`,
> `\N` and `null` as NULL in every column, and the full load trimmed spaces from text. Rows loaded
> or changed before the upgrade keep those values. To find affected rows, run this on the source
> for each text column (any count above 0 means those rows hold NULL in DSQL):
> ```sql
> SELECT COUNT(*) FROM <schema>.<table>
> WHERE UPPER(TRIM(<column>)) IN ('NA','N/A','NONE','(NULL)','\N') OR <column> = 'null';
> ```
> Reload an affected table (Section 8), or correct its rows from the source.

## 5. Monitoring

**Row counts (source vs target):**
```sql
-- source (Oracle): SELECT COUNT(*) FROM SRC_SCHEMA.TABLE;
-- target (DSQL):   SELECT COUNT(*) FROM target_schema.table;
```

**CDC control tables (DSQL schema `cdc_control`):** to open a DSQL session for these queries, see
[RUNBOOK → Connect to DSQL and check progress](RUNBOOK.md#connect-to-dsql-and-check-progress).
```sql
-- per-table status (idle = caught up, blocked = needs attention)
SELECT table_name, status, error FROM cdc_control.cdc_status;

-- per-file apply ledger (rows_applied, all_rows_committed)
SELECT cdc_file, status, rows_applied, all_rows_committed
FROM cdc_control.cdc_file_status WHERE table_name = 'target_schema.table' ORDER BY 1;

-- any apply exceptions
SELECT * FROM cdc_control.cdc_apply_exceptions WHERE table_name = 'target_schema.table';
```

**Full-load validation report:** `s3://<bucket>/config/_task/<task name>/_orchestrator/group-<n>/_validation_report.json` (one per table group).
Each table gets `match`, `mismatch` or `error`, never skipped. Besides row counts per key range,
every column is checked by its DSQL type; a `CONTENT_DIFF` entry names the column, the check and
both values. To switch the content check off for a deployment, add `"--checksum_mode": "off"` to
`default_arguments` in `glue-templates/validate.json`.

An **empty source table** (0 rows in the source, so DMS wrote no S3 folder) validates as `match`
when the DSQL target is also empty (`0 == 0`) — it is no longer an error. A **very large table**
is validated in per-key-range queries, each in its own short transaction under a statement timeout
below DSQL's 300s limit; a range that still hits the limit is **auto re-split** smaller and retried
(the log shows `validation re-split …`). Composite-PK tables are ranged on their first key column so they
are split the same way. If validation still times out, lower `validate_rows_per_range` in
`glue-templates/validate.json`.

**CDC validation (Tier-2)** is **on by default** (`cdc_validation=true`): each CDC job re-reads a
sample (`cdc_validation_sample`, default 20) of every committed file's rows by key and records
persistent mismatches in `cdc_control.cdc_validation_failures`. Cutover stops at
`CdcValidationFailed` if any row is unresolved (`resolved IS NOT TRUE` — a NULL counts as
unresolved); review each, then clear it with
`UPDATE cdc_control.cdc_validation_failures SET resolved=true WHERE table_name='<schema>.<table>'`
(never `DELETE`) and re-run cutover. Set `cdc_validation=false` in `params.csv` to disable.

**Binary columns (Oracle RAW, LONG RAW, BLOB → DSQL `bytea`).** DMS writes them to the CSV as
hexadecimal; the load and CDC store the real bytes. A value that isn't hexadecimal stops the
table (`BINARY GUARD`) instead of storing wrong bytes. RAW columns mapped to `uuid` (as in most
of these schemas) are unaffected. **A `bytea` column cannot be part of a PRIMARY KEY** — DSQL
rejects it (`0A000: datatype bytea is not supported in a key`), so an Oracle `RAW`/`BLOB` **key**
column must map to `uuid` (16-byte GUID keys) or `text` in the target DDL, not `bytea`.
_(Note: the `bytea`/BINARY GUARD path has been proven in simulation only — see
`CDC_EDGE_CASE_RESULTS.md` §2.7.)_

**Processed files and the per-table manifest:** after a CDC file is fully applied, the CDC job
**copies** it to `<schema>/<table>/processed/` (with retries, verified by size). The original is
**never deleted**: files are skipped by the high-water mark (`cdc_status.last_done_file`), not by
where they sit, so a failed or throttled S3 copy can't lose a file — it is retried next cycle.
Each table also has `<schema>/<table>/processed/_manifest.json` with counts only:

```json
{"table": "target_schema.table", "updated_at": "...", "status": "idle",
 "cdc_files_in_folder": 120, "pending_apply": 0, "applied_in_folder": 120,
 "copied_to_processed": 120, "pending_copy": 0, "processed_folder_files": 120,
 "applied_total_ledger": 120, "copy_errors_this_cycle": 0, "all_done": true}
```

`all_done: true` = nothing left to apply and nothing left to copy. `pending_copy` above 0 that
doesn't go down means copies keep failing (see `last_copy_error`). Busy tables update it every
cycle, idle tables at most every 5 minutes.

```bash
aws s3 cp s3://<bucket>/<schema>/<table>/processed/_manifest.json -
```

**Unblocking a table:** fix the cause, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'`. CDC resumes the blocked file at its saved offset on the next poll, without skipping or repeating rows.
**Never `DELETE` the table's `cdc_status` row:** applied files stay in the table folder, so a
missing row would replay all of them from the start.

**Glue job logs:** CloudWatch `/aws-glue/python-jobs/output` and `/aws-glue/python-jobs/error`
(CDC pythonshell), `/aws-glue/jobs/output` (Spark load/validate). Look for `🔄 SCHEMA CHANGE`,
`✅ RENAME`, `✅ ADD COLUMN`, `⛔ ... BLOCKED`.

**DMS per-table stats:** `aws dms describe-table-statistics --replication-task-arn <ARN>`
(`FullLoadRows`, `Inserts`, `Ddls`, `TableState`).

---

## 6. Cutover

Cutover runs through the **fleet-cutover** state machine, the same way as startup: one launch reads
`fleet_tasks.csv` and starts the per-task **cutover** for each row (one task is a one-row list).

**Before you start — stop writes to the source first.** Each task's cutover's **first** action is
to stop its DMS task, so any change written to the source **after** that is never captured — silent
data loss. For **every** table of **every** task in the list, put the application into maintenance
mode (or make the source read-only), let DMS deliver the last changes (CDCLatencySource/Target near
zero, and wait past any `CdcMaxBatchInterval`), confirm no table is `blocked` and that the CDC run
is RUNNING, and only then launch the fleet. **The fleet checks inputs, not readiness, and cutover
is irreversible per task** — only list tasks whose CDC has caught up.

When CDC has caught up (all tables `idle`, source≈target), launch the **fleet-cutover** state
machine ([RUNBOOK Step 4](RUNBOOK.md#4-set-up)):

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn-of-$PROJECT-fleet-cutover> \
  --name fleet-cutover-$(date +%Y%m%d%H%M) \
  --input '{"bucket":"<pipeline bucket>"}'
```

Preflight checks every task (each must have been started by the pipeline); then each per-task
cutover:
1. Stops the DMS task.
2. **Drain-checks** each table (latest CDC file applied) until quiesced.
3. Stops this task's CDC run, drops the `_cdc_file` tracking column, and deletes the task's Glue
   jobs. If a job can't be deleted it ends at `GlueJobsNotDeleted`, naming it (the data is already
   cut over; delete the job by hand, or simply **re-run the task's cutover** — it is idempotent).

**You CAN re-run a task's cutover, including after its DMS task has been stopped.** Cutover first
`DescribeBeforeStop`s the DMS task and SKIPS the stop when it is already stopped (and tolerates the
DMS `InvalidResourceStateFault`), and every later step is idempotent: a deleted Glue job is a
no-op, dropping an already-dropped `_cdc_file` column is a no-op, and the drain re-checks from the
current high-water mark. So if a cutover fails partway (`CdcDrainTimedOut`, a later `CutoverFailed`,
or `GlueJobsNotDeleted`), the supported recovery is to **re-run that task's cutover** (or re-launch
the fleet — a task whose cutover is already running is skipped as `already_running`). Only fall back
to finishing the steps by hand (stop the CDC run, drop the `_cdc_file` column on each table, delete
the task's Glue jobs) if a re-run cannot complete. See `RUNBOOK.md`.

Then repoint the application to DSQL. The fleet drives both start and cutover for one task or many;
see [`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md) for its inputs, skip rules and limits.

---

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Load run "SUCCEEDED" but **0 rows** | a stale per-group `_load_status.json` marks tables `done` → skipped | delete `s3://<bucket>/config/_task/<task name>/_orchestrator/` (recursive) and re-run |
| CDC job fails `UnknownServiceError: dsql` | CDC job didn't get modern boto3 | ensure `driver-cdc/` has boto3/botocore wheels and the job's `--extra-py-files` is the **cdc** list (not fullload). In the startup workflow this error switches the CDC job to Spark automatically |
| CDC log: `another CDC run is applying this table` (cycle summary lists the table as applied by another run) | two different CDC jobs or runs are applying the same table | each change is still applied once; find the extra job (old per-task workflow, hand-made copy, a second DMS task with the same table) and stop it |
| Startup stops at `ResolveFailed`: `Another startup run is already running` | a startup for this task is still running | wait for it or stop it, then launch the fleet again (the task is skipped while running) |
| CDC startup log: `NOT applied by this job (multi-column primary key)` | the table's primary key has more than one column | expected: the main CDC job skips it; the table's own per-table composite (`ck`) fork CDC job (`<project>-<task>-ck-<slug>-cdc`, started automatically for this task) applies it, and cutover waits until that fork has caught up |
| Startup stops at `DriversFailed` | a `driver-cdc/` wheel can't work on Python 3.9, or one is missing | the error names the wheel; fix `driver-cdc/` ([RUNBOOK Step 3b](RUNBOOK.md#4-set-up)) and launch the fleet again (DMS was not started) |
| Fleet stops at `PreflightFailed` | one or more rows failed a per-task check before anything started | the cause lists every problem by row; fix them and launch the fleet again (nothing was started) |
| CDC job fails installing a `.whl` (`CalledProcessError`, `pypi.org` timeouts), often after ~20 min | the run was given the raw `driver-cdc/` list (older per-task workflow, or a hand-made start with `--extra-py-files`) | start it without `--extra-py-files` so it uses the prepared list saved on the job |
| Startup ends at `CdcRunFailed`, `CdcRunEnded` or `CdcStartNotConfirmed` | the CDC run failed, stopped, or never reached its poll loop | full load is done and DMS is capturing changes: read the CDC log, fix, restart the CDC job with `--config_prefix` |
| Startup stops at `ResolveFailed` | a DMS task setting, `pipeline.json`, or folder-owner check failed before DMS started | read the execution's error message; RUNBOOK → Troubleshooting lists each case |
| CDC log: `The DMS API is not reachable` / `CloudWatch is not reachable` | no VPC endpoint or NAT route to that service | CDC keeps applying; only column-rename detection (DMS) or one metric (CloudWatch) is off |
| CDC job runs but applies 0 rows | `cdc_root` points at the wrong folder | confirm `resolve_task` `cdcRoot` matches where DMS writes; pass `--cdc_root` from `resolve_task` (use `.` for no bucketFolder) |
| Full-load columns shifted/corrupted | stale `processed/` or CDC files read as full-load | the guards prevent this; ensure a clean S3 (purge `processed/`,`failed/`, old CDC) before a fresh full load |
| Table `blocked` after a DROP COLUMN, or a rejected row (e.g. NULL into a NOT NULL column) | missing-column guard / DSQL rejected the row | fix the cause (drop the column on target / allow NULL or fix the source row), then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'` — never delete the row |
| Table `blocked`: "missing column(s) [X]" after a rename | should not happen post-fix; if using `--single_swap_is_rename false` | add `metadata.rename_hints` or re-enable the default |
| Target value wrong after a type change | header-diff blind to type change | `ALTER` target column type manually + re-apply affected rows (known limitation) |
| Re-run after a mid-test target reset misses rows | files already applied are skipped by the high-water mark (`cdc_status.last_done_file`) | for a true clean run, purge the whole per-table S3 prefix (incl `processed/`) + control tables + `config/_task/<task name>/_orchestrator/` |

---

## 8. Clean-slate checklist (for a fresh full reload)

> **A reload needs a DMS task that has not finished its full load.** The shared startup
> **refuses** a task that is already past its full load (`ResolveFailed` / `past_full_load`), so
> you **cannot** reload by re-launching the fleet against the same, already-run DMS task. There is
> no supported in-place reload. After purging the state below, you must **create a new DMS task**
> (or otherwise reset one so it hasn't completed its full load), add its ARN to `fleet_tasks.csv`,
> and launch the fleet for it. The full, ordered procedure — including the new-task step and
> archiving a reused folder name — is in `RUNBOOK.md`; use it for the DMS part.

Stop the task's CDC run (and the DMS task) first. Then purge **all** of these together, or you will
get stale-state artifacts (see Engineering Record §3). In particular, never delete a table's
`cdc_control` rows without also purging its S3 prefix: applied CDC files stay in the table
folder, so CDC would apply every one of them again on top of the fresh load.
```bash
# 1) S3 per-table prefixes INCLUDING processed/ and failed/
aws s3 rm s3://<bucket>/<schema>/<table>/ --recursive     # per table
# 2) config status files
aws s3 rm s3://<bucket>/config/_task/<task name>/_orchestrator/ --recursive   # per-group load status, _file_status/, validation reports (rebuilt next run)
# 3) DSQL: drop+recreate target tables, and clear control rows (ALL SIX control tables)
DELETE FROM cdc_control.cdc_status             WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_file_status        WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_chunk_log          WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_apply_exceptions   WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_validation_failures WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_skipped_ops        WHERE table_name='<schema.table>';
```
Then add a **new (not-yet-run) DMS task** to `fleet_tasks.csv` and launch the fleet from Section 2
(or Section 3 for a single-step manual run). Re-launching the fleet on the old task will stop at
`ResolveFailed`.
