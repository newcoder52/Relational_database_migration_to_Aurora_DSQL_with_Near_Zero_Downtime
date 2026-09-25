# Relational Database Migration to Amazon Aurora DSQL — with Near-Zero Downtime

A schema-agnostic pipeline that migrates relational data (validated against Oracle sources)
into **Amazon Aurora DSQL** with a **full load → validate → continuous CDC → cutover** flow,
so the application can keep running against the source until you're ready to switch over.

It uses **AWS DMS** to extract to S3 as CSV, **AWS Glue** to load/validate/apply changes into
Aurora DSQL, and **AWS Step Functions** to orchestrate one migration task end to end.

```
Source DB ──DMS (full load + CDC)──▶ S3 (CSV) ──AWS Glue──▶ Amazon Aurora DSQL
                                      │   Job 1  discover schema + PK/type metadata
              full load: LOAD*.csv ───┤   Job 2  load (Spark, per-file parallel)
              CDC:  <ts>.csv (Op col)─┘   Job 3  validate (count + optional checksum)
                                          CDC job  continuous apply (long-running)
```

---

## Table of contents

- [Why this exists](#why-this-exists)
- [How it works](#how-it-works)
- [The four Glue jobs](#the-four-glue-jobs)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Deploy (one-time)](#deploy-one-time)
- [Run a migration](#run-a-migration)
- [Monitoring](#monitoring)
- [Schema changes during CDC](#schema-changes-during-cdc)
- [Cutover](#cutover)
- [Clean-slate reload](#clean-slate-reload)
- [Known limitations](#known-limitations)
- [Security](#security)

---

## Why this exists

Aurora DSQL is a distributed SQL database with a different write model than a traditional
RDBMS (per-transaction row/size limits, ~1-hour connection cap, optimistic concurrency, no
`TRUNCATE`/`ctid`). DMS cannot write to DSQL directly, so this pipeline lands DMS output in
S3 and uses Glue to load and continuously apply changes into DSQL within those constraints —
extracting once, validating exactly, and keeping the target in sync via CDC until cutover.

**Near-zero downtime:** the source stays live through full load and CDC. You only stop writes
at the final cutover, once CDC has drained and the target matches the source.

## How it works

- **One DMS task = one Step Functions state machine = one config prefix**
  (`s3://<bucket>/config/_task/<TASK_SUFFIX>/`). Tasks run and cut over independently.
- DMS writes CSVs to `s3://<bucket>/<schema>/<table>/` — full load as `LOAD*.csv`, CDC as
  `<timestamp>.csv` with a leading `Op` column.
- The startup state machine runs: **start DMS → discover → load → validate → resume to CDC →
  start the continuous CDC job**. The cutover state machine drains CDC and finalizes.
- Endpoint settings (bucket folder, timestamp column, headers) are **auto-derived** at
  runtime — you don't hardcode them.

## The four Glue jobs

| Job | File | What it does |
|-----|------|--------------|
| **Job 1 — Discovery** | `manual_kit/scripts/job1_discovery.py` | Reads the DSQL target schema (authoritative for column set/order) and builds per-table type + primary-key metadata. Writes `_manifest_index.json` + per-table column-mapping JSONs. |
| **Job 2 — Load** | `manual_kit/scripts/job2_load.py` | Full-load apply (Spark, driver-side pg8000). Large tables load via **per-file parallelism** (250 MB files, up to 30 in parallel); per-file S3 resume; exact per-file no-loss gate. |
| **Job 3 — Validate** | `manual_kit/scripts/job3_validate.py` | Post-load validation: per-range **count compare** (default), with an **optional content checksum** tier (`aggregate`/`md5`) for text columns on rangeable single-column PKs. |
| **CDC — Continuous** | `manual_kit/scripts/glue_cdc_continuous.py` | Long-running Python-shell job that applies inserts/updates/deletes to DSQL, multi-table, with crash-proof resume via DSQL `cdc_control` tables and a no-missed/no-dup guarantee. |

## Repository layout

```
manual_kit/                 Operator deployment kit (deploy + run the pipeline by hand)
  scripts/                  The 4 Glue job scripts  (job1_discovery, job2_load,
                            job3_validate, glue_cdc_continuous)
  lambdas/                  Orchestration lambdas (resolve_task, plan_split, create_glue_jobs,
                            driver_discovery, drain_check, stop_cdc_run, drop_tags)
  stepfunctions/            startup + cutover state machines (ASL)
  glue-templates/           Glue job-definition templates
  iam/                      Role trust + policy documents
  RUNBOOK.md                Full step-by-step deploy + operate reference
  USAGE_GUIDE.md            End-to-end operational usage
  ENGINEERING_RECORD.md     Architecture, every bug found + fix, DDL support matrix
  CDC_EDGE_CASE_RESULTS.md  CDC edge-case + data-type limitations (with fixes)
docs/
  NO_PK_CDC_UPDATE_TRACKING.md   Design note: no-PK CDC update tracking
  architecture.png               Architecture diagram
```

> The complete, authoritative deploy/operate reference is **[`manual_kit/RUNBOOK.md`](manual_kit/RUNBOOK.md)**.
> The sections below summarize it so you can get oriented without leaving this page.

## Prerequisites

- **Aurora DSQL cluster** with the **target tables already created** in the (lowercased)
  target schema — the pipeline loads into existing tables, it never creates them. Use a
  single-column PK where possible (best for CDC apply + content validation).
- **DMS task** of type `full-load-and-cdc` with `StopTaskCachedChangesApplied=true`, and an
  **S3 target endpoint** with `AddColumnName=true`, `TimestampColumnName=dms_timestamp`,
  `Rfc4180=true`, `DatePartitionEnabled=false`, and no custom `CdcPath`.
- A DMS **table mapping that lowercases** schema/table/column names (so S3 paths + columns
  match the DSQL target).
- For **no-PK tables**: configure DMS to emit **insert/delete only** (updates are skipped and
  logged — see the no-PK design note).

## Deploy (one-time)

Detailed commands are in **[`manual_kit/RUNBOOK.md`](manual_kit/RUNBOOK.md)** (Steps 0–4). In brief:

0. **Stage to one S3 bucket** (fixed folder layout): the 4 scripts → `scripts/`, the 5 job
   templates → `glue-templates/`, and driver wheels into **three** per-job folders —
   `driver-fullload/`, `driver-validation/` (DSQL drivers only) and `driver-cdc/` (DSQL
   drivers **plus** modern boto3/botocore for the Python-shell CDC job). Stage each task's
   `table_manifest.csv` under `config/_task/<suffix>/`.
1. **Create the IAM roles** (Glue, Lambda, Step Functions) from `manual_kit/iam/`.
2. **Create the 7 Lambdas** from `manual_kit/lambdas/`.
3. **Create the per-task startup state machine** from `manual_kit/stepfunctions/startup.asl.json`.
4. **Create the per-task cutover state machine** from `manual_kit/stepfunctions/cutover.asl.json`.

## Run a migration

Start the task's startup state machine:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <startup-sm-arn-for-this-task> \
  --name run-$(date +%Y%m%d-%H%M%S)
```

It performs, in order:

1. **StartDmsTask** → full load; waits for `STOPPED_AFTER_CACHED_EVENTS`.
2. **ResolveTask** → derives `cdcRoot`, `timestampColumnName`, S3 settings from the endpoint.
3. **DriverDiscovery** ×3 (fullload / validation / cdc folders).
4. **CreateGlueJobs** → creates this task's Glue jobs (discovery/load/load-big/validate/cdc).
5. **RunDiscovery** (Job 1) → writes `_manifest_index.json` + per-table column mappings.
6. **PlanSplit + GroupFanOut** → runs **Job 2 load** then **Job 3 validate** per group.
7. **ResumeDmsToCdc** → resumes DMS from the cached-changes stop into ongoing CDC.
8. **StartCdcJob** → launches the continuous CDC job.

After this, full load is in DSQL, validated, and CDC is live. To resume after any failure,
just start the machine again — each stage skips completed work via its status files.

## Monitoring

**CDC control tables** (DSQL schema `cdc_control`):

```sql
-- per-table status (idle = caught up, blocked = needs attention)
SELECT table_name, status, error FROM cdc_control.cdc_status;

-- per-file apply ledger
SELECT cdc_file, status, rows_applied, all_rows_committed
FROM cdc_control.cdc_file_status WHERE table_name = 'target_schema.table' ORDER BY 1;

-- updates skipped for no-PK tables
SELECT * FROM cdc_control.cdc_skipped_ops WHERE table_name = 'target_schema.table';
```

**Validation report:** `s3://<bucket>/config/_task/<SUF>/_validation_report.json`
(`match` / `mismatch` / `skipped` per table). **Glue logs:** CloudWatch
`/aws-glue/python-jobs/*` (CDC) and `/aws-glue/jobs/output` (Spark load/validate).

## Schema changes during CDC

Golden rule: **every DDL must be followed by DML** (DMS only surfaces a schema change on the
next data row). Full matrix is in `manual_kit/ENGINEERING_RECORD.md` §4.

| Source change | Behavior | Action |
|---|---|---|
| `ADD COLUMN` (one or many) | Auto-added to target | none |
| `RENAME COLUMN` (single/multiple/with add) | Auto-renamed (positional detection) | none |
| `DROP COLUMN` | **Table blocks** (guard can't tell a drop from an omitted column) | drop on target + clear `cdc_status`, restart CDC |
| `CHANGE DATA TYPE` | **Silent** — not detected by name-based header diff | `ALTER` target type + re-apply affected rows |

## Cutover

When CDC has caught up (all tables `idle`, source ≈ target), run the **cutover state machine**.
It drain-checks each table until quiesced, stops the CDC job, and drops the `_cdc_file`
tracking column. Then stop the DMS task and repoint the application to Aurora DSQL. Other
tasks are unaffected.

## Clean-slate reload

For a fresh full reload, purge **all** prior state together or you'll get stale-state
artifacts (see `manual_kit/USAGE_GUIDE.md` §8):

```bash
aws s3 rm s3://<bucket>/<schema>/<table>/ --recursive          # per table (incl. processed/ + failed/)
aws s3 rm s3://<bucket>/config/_task/<SUF>/_load_status.json
aws s3 rm s3://<bucket>/config/_task/<SUF>/_validation_report.json
# DSQL: drop+recreate target tables, and clear the cdc_control rows for the table
```

## Known limitations

See `manual_kit/ENGINEERING_RECORD.md` and `manual_kit/CDC_EDGE_CASE_RESULTS.md` for detail.

- **DROP COLUMN** during CDC blocks the affected table (resumable after operator remediation).
- **In-place CHANGE DATA TYPE** is not detected by the header-diff schema reconciliation.
- Content-checksum validation covers single-column integer/UUID PK tables; composite /
  fractional-numeric PK tables are **count-validated** only.
- CDC requires the source schema to exist in the Oracle LogMiner dictionary before capture
  (a schema created after the dictionary build needs a DBA dictionary rebuild).
- **No-PK tables:** inserts and deletes are applied; **updates are skipped and logged** to
  `cdc_control.cdc_skipped_ops` (unless an operator declares a stable logical key).

## Security

No credentials or account-specific values are committed. Supply your own bucket, cluster
endpoint, schema, and role ARNs via the config / runtime arguments described in the RUNBOOK.
DSQL auth uses short-lived IAM tokens generated at runtime (no stored DB passwords).

## License

Licensed under the [MIT-0 License](LICENSE).
