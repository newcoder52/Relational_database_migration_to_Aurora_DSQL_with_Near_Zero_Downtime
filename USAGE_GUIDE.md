# End-to-End Usage Guide — DMS → S3 → Glue → Aurora DSQL

_How to operate the pipeline for a migration task: prerequisites, running full load +
validation + CDC, handling schema changes (DDL), cutover, monitoring, and troubleshooting._

Companion docs: `RUNBOOK.md` (one-time deploy of IAM/lambdas/state machines/scripts) and
`ENGINEERING_RECORD.md` (architecture, fixes, DDL limitations). **Read the DDL limitations in
the Engineering Record before relying on schema-change replication.**

---

## 0. Mental model (read once)

- **One DMS task = one Step Functions state machine = one config prefix**
  `s3://<bucket>/config/_task/<TASK_SUFFIX>/`.
- DMS writes CSVs to `s3://<bucket>/<schema>/<table>/` (full load = `LOAD*.csv`, CDC =
  `<timestamp>.csv` with a leading `Op` column). Glue loads them into DSQL.
- Flow per task: **full load → (Glue) discover → load → validate → resume DMS to CDC →
  (Glue) CDC job applies ongoing changes → cutover** when ready.
- The endpoint's settings (bucket folder, timestamp column, headers) are **auto-derived** by
  `resolve_task` — you do not hardcode them.

---

## 1. Prerequisites (per task)

1. **One-time deploy done** (per `RUNBOOK.md`): IAM roles, 7 lambdas, scripts staged to
   `s3://<bucket>/scripts/`, glue-templates staged, and the **three driver folders** populated:
   - `driver-fullload/`, `driver-validation/` — DSQL driver wheels only (pg8000, scramp,
     asn1crypto, python-dateutil, six).
   - `driver-cdc/` — the same DSQL drivers **plus** modern `boto3`/`botocore` wheels.
2. **DMS S3 target endpoint** configured with (these are validated automatically):
   - `AddColumnName = true` (CSVs have header rows),
   - `DatePartitionEnabled = false`,
   - no `CdcPath` / `PreserveTransactions`,
   - `BucketFolder` empty (or a folder you're OK with — it becomes `cdc_root`).
3. **DMS task** created as `full-load-and-cdc` with `StopTaskCachedChangesApplied=true`, and a
   table mapping that **lowercases** schema/table/columns:
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
   schema, with a single-column PK where possible (multi-column PK tables can't CDC-apply
   cleanly; range-validation needs an integer PK).
5. **Manifest** staged: `s3://<bucket>/config/_task/<TASK_SUFFIX>/table_manifest.csv`
   ```
   dms_schema,dms_table
   target_schema,table_a
   target_schema,table_b
   ```

---

## 2. Run the pipeline (the normal path — via the state machine)

The **startup state machine** does everything automatically. Substitute placeholders per
`RUNBOOK.md` Step 3, then:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <startup-sm-arn-for-this-task> \
  --name run-$(date +%Y%m%d-%H%M%S)
```

It performs, in order:
1. **StartDmsTask** → full load; waits for `STOPPED_AFTER_CACHED_EVENTS`.
2. **ResolveTask** → derives `cdcRoot`, `timestampColumnName`, S3 settings from the endpoint
   (fails fast if `DatePartitionEnabled=true` or `CdcPath` set).
3. **DriverDiscovery** ×3 (fullload / validation / cdc folders).
4. **CreateGlueJobs** → creates this task's 5 Glue jobs (discovery/load/load-big/validate/cdc).
5. **RunDiscovery** (Job1) → writes `_manifest_index.json` + per-table column mappings.
6. **PlanSplit** + **GroupFanOut** → runs **Job2 load** then **Job3 validate** per group.
7. **ResumeDmsToCdc** → resumes DMS from cached-changes stop into ongoing CDC.
8. **StartCdcJob** → launches the continuous CDC job.

After this, full load is in DSQL, validated, and CDC is live.

---

## 3. Run the pipeline manually (for testing / debugging a single step)

Useful when iterating. Set once:
```bash
REGION=us-east-1; PROJECT=<project>; SUF=<task_suffix>
CFG=s3://<bucket>/config/_task/$SUF/
ARN=<dms-task-arn>
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
     \"--extra-py-files\":\"<cdc-list>\",\"--cdc_root\":\"<cdcRoot>\",
     \"--timestamp_column\":\"<timestampColumnName>\"}"
   ```

> The CDC job is a **continuous poller** — it stays RUNNING and applies new files each cycle.
> Stop it with `aws glue batch-stop-job-run` when cutting over or pausing.

---

## 4. Handling schema changes (DDL) during CDC

**Golden rule: every DDL must be followed by DML.** DMS only surfaces a schema change on the
next data row after the DDL. Sequence any change as **DML → DDL → DML**.

| You do (source) | Pipeline behavior | Action needed |
|---|---|---|
| `ADD COLUMN` (single or many) | Auto-added to target | none |
| `RENAME COLUMN` (single, multiple, or combined with ADD) | Auto-renamed on target (positional detection) | none |
| `DROP COLUMN` | **Table BLOCKS** | remediate: `ALTER TABLE <target> DROP COLUMN <col>`, then `DELETE FROM cdc_control.cdc_status WHERE table_name='<schema.table>'` (with the CDC job stopped), restart CDC |
| `CHANGE DATA TYPE` | **Silent — value may be truncated/coerced to the old target type; no error** | avoid, or manually `ALTER` the target column type + re-apply affected rows |

- To force a rename deterministically (instead of relying on positional detection), add to the
  table's config `metadata.rename_hints`: `{"old_col":"new_col"}`.
- To disable the positional rename default (revert to conservative ADD-and-keep-old), pass
  `--single_swap_is_rename false` to the CDC job.

See `ENGINEERING_RECORD.md` §4 for the full tested matrix and root causes.

---

## 5. Monitoring

**Row counts (source vs target):**
```sql
-- source (Oracle): SELECT COUNT(*) FROM SRC_SCHEMA.TABLE;
-- target (DSQL):   SELECT COUNT(*) FROM target_schema.table;
```

**CDC control tables (DSQL schema `cdc_control`):**
```sql
-- per-table status (idle = caught up, blocked = needs attention)
SELECT table_name, status, error FROM cdc_control.cdc_status;

-- per-file apply ledger (rows_applied, all_rows_committed)
SELECT cdc_file, status, rows_applied, all_rows_committed
FROM cdc_control.cdc_file_status WHERE table_name = 'target_schema.table' ORDER BY 1;

-- any apply exceptions
SELECT * FROM cdc_control.cdc_apply_exceptions WHERE table_name = 'target_schema.table';
```

**Full-load validation report:** `s3://<bucket>/config/_task/<SUF>/_validation_report.json`
(`match` / `mismatch` / `skipped` per table; `skipped` = no single-column rangeable integer PK,
which is by-design — the count is still checked by Job2's `_load_status.json`).

**Glue job logs:** CloudWatch `/aws-glue/python-jobs/output` and `/aws-glue/python-jobs/error`
(CDC pythonshell), `/aws-glue/jobs/output` (Spark load/validate). Look for `🔄 SCHEMA CHANGE`,
`✅ RENAME`, `✅ ADD COLUMN`, `⛔ ... BLOCKED`.

**DMS per-table stats:** `aws dms describe-table-statistics --replication-task-arn <ARN>`
(`FullLoadRows`, `Inserts`, `Ddls`, `TableState`).

---

## 6. Cutover

When CDC has caught up (all tables `idle`, source≈target), run the **cutover state machine**
for the task (per `RUNBOOK.md` Step 4). It:
1. Resolves `cdc_root` (same auto-derive as startup).
2. **Drain-checks** each table (latest CDC file applied) until quiesced.
3. Finalizes and stops the CDC job.

Then stop the DMS task and repoint the application to DSQL.

---

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Load run "SUCCEEDED" but **0 rows** | stale `_load_status.json` marks tables `done` → skipped | delete `s3://<bucket>/config/_task/<SUF>/_load_status.json` and re-run load |
| CDC job fails `UnknownServiceError: dsql` | CDC job didn't get modern boto3 | ensure `driver-cdc/` has boto3/botocore wheels and the job's `--extra-py-files` is the **cdc** list (not fullload) |
| CDC job runs but applies 0 rows | `cdc_root` points at the wrong folder | confirm `resolve_task` `cdcRoot` matches where DMS writes; pass `--cdc_root` from `resolve_task` (use `.` for no bucketFolder) |
| Full-load columns shifted/corrupted | stale `processed/` or CDC files read as full-load | the guards prevent this; ensure a clean S3 (purge `processed/`,`failed/`, old CDC) before a fresh full load |
| Table `blocked` after a DROP COLUMN | missing-column guard | drop the column on target + clear `cdc_status` (CDC job stopped), restart |
| Table `blocked`: "missing column(s) [X]" after a rename | should not happen post-fix; if using `--single_swap_is_rename false` | add `metadata.rename_hints` or re-enable the default |
| Target value wrong after a type change | header-diff blind to type change | `ALTER` target column type manually + re-apply affected rows (known limitation) |
| Re-run after a mid-test target reset misses rows | files already in `processed/` are skipped | for a true clean run, purge the whole per-table S3 prefix (incl `processed/`) + control tables + `_load_status.json` |

---

## 8. Clean-slate checklist (for a fresh full reload)

Purge **all** of these, or you will get stale-state artifacts (see Engineering Record §3):
```bash
# 1) S3 per-table prefixes INCLUDING processed/ and failed/
aws s3 rm s3://<bucket>/<schema>/<table>/ --recursive     # per table
# 2) config status files
aws s3 rm s3://<bucket>/config/_task/<SUF>/_load_status.json
aws s3 rm s3://<bucket>/config/_task/<SUF>/_validation_report.json
# 3) DSQL: drop+recreate target tables, and clear control rows
DELETE FROM cdc_control.cdc_status        WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_file_status   WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_chunk_log     WHERE table_name='<schema.table>';
DELETE FROM cdc_control.cdc_apply_exceptions WHERE table_name='<schema.table>';
```
Then run the pipeline from Section 2 or 3.
