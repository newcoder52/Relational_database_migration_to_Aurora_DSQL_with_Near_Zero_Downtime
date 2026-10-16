# MRL Pipeline — Manual-Range-Load Step Functions (operator guide)

A NEW, SEPARATE Step Functions pair that automates the manual file-range full load delivered in
`customer_patches/job2_load_manual_range/`. It loads one or more large tables by splitting each
table's `LOAD*.csv` part-files into **contiguous, inclusive ranges** and loading each range with
its own Glue driver, then reconciling and verifying counts — all without a single manual
`aws glue start-job-run`.

**It does not change the existing pipeline.** The existing startup/cutover/fleet state machines,
Lambdas and Glue jobs are untouched. MRL adds its own two state machines, its own Lambdas, its
own dedicated per-task load Glue job and a count Glue job, and writes its state under
`s3://<bucket>/mrl/`. It reuses the existing `resolve_task` (build_table_list), `create_glue_jobs`
and `job1_discovery` for discovery, and the existing CDC Lambdas/jobs for the CDC tail.

Everything in this folder uses placeholders only (`<account-id>`, `<bucket>`, `<region>`,
`<project>`, `<task-suffix>`, `<schema>.<table>`); it is safe to push to a public repo.

**More docs:** step-by-step operation is in **[RUNBOOK.md](RUNBOOK.md)**; how MRL fits the broader
architecture (reused vs new, S3 keys, capacity, cutover) is in **[INTEGRATION.md](INTEGRATION.md)**.

### Which params file?

- **`config/params.csv`** — the EXISTING pipeline's file. MRL reads it **READ-ONLY** for shared
  settings (DSQL endpoint, region, roles, control schema, NULL defaults) via a byte-identical copy
  of the original parser. **Never add MRL-only keys here** — the original parser rejects unknown
  keys and would break the base pipeline.
- **`config/params_parallel_load.csv`** — the **MRL-only** optional file. Put MRL knobs here:
  `max_parallel_tasks` (how many tasks run at once, default 2), `max_concurrency`, `split_by`,
  `split_allow_hex`, `count_timeout_action`, `count_tolerance`, `write_merged_status`, `cdc`.
- MRL writes only its own **`config/pipeline_parallel_load.json`** and never `config/pipeline.json`.

---

## 1. Concept

```
PARENT (fleet)  mrl-fleet.asl.json
  reads s3://<bucket>/config/fleet_task_parallel_load.csv (+ optional params_parallel_load.csv)
  -> validates strictly -> Map(one child per row, MaxConcurrency=max_parallel_tasks)
       -> CHILD (task)  mrl-task.asl.json
            Preflight -> (start|adopt) DMS -> discovery (reused) -> PLAN (list+split)
            -> SAFETY GATE (empty or resume) -> LOAD (Map over (table,range)) -> RECONCILE
            -> COUNT (DSQL vs DMS FullLoadRows) -> mode tail
  -> aggregates {succeeded, failed, skipped}; fails if any child failed
```

Each **range** is one Glue run of the dedicated `<project>-<task-suffix>-mrl-load` job (a copy
of the `load-big` template pointed at the patched `scripts/job2_load_manual_range.py`), with
`--load_file_start` / `--load_file_end` set to EXACT file names. Ranges of one table are disjoint
by construction and each writes its own range-private progress doc, so concurrent ranges never
touch the same S3 object. RECONCILE proves every file is loaded exactly once; COUNT proves the
DSQL row count equals the DMS FullLoadRows.

The MRL **input** files live in the SAME `config/` folder as the existing pipeline's files, but
under DISTINCT names, and MRL **never** writes `config/params.csv`, `config/pipeline.json` or
`config/fleet_tasks.csv` (those belong to the existing pipeline). MRL reads its settings from
`config/params.csv` READ-ONLY (via a byte-identical copy of the original parser) and writes only
its own `config/pipeline_parallel_load.json`. Everything else MRL writes stays under `mrl/`.

---

## 2. S3 folder layout

```
s3://<bucket>/
  config/fleet_task_parallel_load.csv  # MRL parent input (you author) - in config/, MRL-named
  config/params_parallel_load.csv      # optional MRL params (parameter,value); MRL-named
  config/params.csv                    # existing pipeline's settings - MRL reads it READ-ONLY
  config/pipeline_parallel_load.json   # MRL's OWN settings file (the only config/ file MRL writes)
  mrl/state/<task-suffix>/
    state.json                         # phase + timestamps (mrl_status.py reads it)
    plan.json                          # frozen plan: sha256, counts, ranges, params
    reconcile.json                     # reconcile verdict
    count_result.json / probe_result.json
    tables/<schema>.<table>/config/    # ONE single-table config prefix per table
      _manifest_index.json             #   exactly ONE table (copied from discovery)
      <table>_column_mapping.json
      _file_status/<label>.range-*.json   # per-range progress (range-private)
      _range_result/<label>.range-*.json  # per-range result
      _file_status/<label>.json           # merged normal doc (RECONCILE, if enabled)
```

MRL never writes the existing pipeline's `config/params.csv` / `config/pipeline.json` /
`config/fleet_tasks.csv`; the existing pipeline reads `config/` by exact key names only (never by
listing the folder), so the two extra MRL-named files cannot disturb it (see `DESIGN.md`).

---

## 3. fleet_task_parallel_load.csv column reference

Header names must be exact. Unknown columns are rejected (fail closed). See
`config/fleet_task_parallel_load.example.csv`.

| column | required | values / notes |
|---|---|---|
| `task_arn` | yes | `arn:aws:dms:<region>:<account-id>:task:<id>`, listed once |
| `task_suffix` | no | folder / job-name override; blank = the task's recorded folder or DMS name |
| `cdc` | no | `true`/`false` (default false). `false` = full load only; `true` = resume DMS + start CDC after a passing count (Amendment 8) |
| `max_concurrency` | no | integer 1–40 (parallel range loads per table). Omit to use `config/params.csv` `map_max_concurrency` if explicit, else the built-in default **3**. A fleet row / execution input overrides params |
| `dms_start_mode` | no | `start` (default) or `adopt_stopped` |
| `load_worker_type` | no | Glue worker type for the load job (e.g. `G.4X`) |
| `load_workers` | no | integer ≥ 1 |
| `max_files_in_parallel` | no | integer ≥ 1 (per-run file fan-out; default 20 at G.4X) |
| `writers_per_file` | no | integer ≥ 1 (intra-file DSQL writers; default 8) |
| `tables` | no | include-list `schema.table;schema.table` (blank = all task tables) |
| `override` | no | `true`/`false` (default false). Bypass the non-empty safety gate (loud warning) |

`params_parallel_load.csv` (optional): `parameter,value` at `config/params_parallel_load.csv`
(a SEPARATE file from the existing `config/params.csv`, which MRL never writes). MRL-only keys:
`max_parallel_tasks` (default 2), `split_by`, `split_allow_hex`, `count_timeout_action`,
`count_tolerance`, `write_merged_status`, `cdc`, `max_concurrency` (all optional). Unknown keys are
rejected. Shared values (DSQL endpoint, region, roles, control schema, NULL defaults) come from
`config/params.csv` READ-ONLY (via the copied parser); MRL writes only its own
`config/pipeline_parallel_load.json`, never `config/pipeline.json`.

**Precedence:** per-row value > `config/params_parallel_load.csv` > `config/params.csv`.

---

## 4. Modes

- **full_load_only**: start (or adopt) the task, load the ranges, reconcile, verify counts,
  stop. DMS is NOT resumed and CDC is NOT started. Accepts a DMS stop of
  `STOPPED_AFTER_FULL_LOAD` or `FULL_LOAD_ONLY_FINISHED` with `FullLoadProgressPercent=100`.
- **full_load_and_cdc**: as above, then resume DMS (`resume-processing`) and start the main CDC
  job exactly as the existing startup workflow does, confirming the CDC start marker. Requires a
  `full-load-and-cdc` task with `StopTaskCachedChangesApplied=true`; waits for
  `STOPPED_AFTER_CACHED_EVENTS`.

**dms_start_mode:**
- `start` (default): the task must not have run a full load; MRL starts it and polls.
- `adopt_stopped`: the task is already stopped at 100%, 0 errored tables, all tables
  `Table completed`. MRL does NOT start DMS; it adopts the finished load. Any stop reason is
  accepted (including a hand `NORMAL` stop — the 2026-10-14 case), because the data is fully
  loaded regardless of how it was stopped.

---

## 5. NULL handling

Default (blank): exactly today's behaviour — the DMS endpoint `CsvNullValue` marker (usually the
text `NULL`) and an empty field are NULL, nothing else.

Two optional settings (in `config/params.csv`, applied by `create_glue_jobs` to every job):
- `null_values`: `|`-separated exact strings that become NULL in ALL columns, **replacing** the
  endpoint marker. Exact and case-sensitive.
- `null_rules`: `schema.table.column=none|V1|V2` entries separated by `;`, per column.
  `none` = keep the text as data. Precedence: rule > values > endpoint marker.

MRL validates these at **preflight** with the loader's own parser, so a malformed entry fails in
seconds naming the bad entry (not hours later in the load). An entry naming a table/column not in
the task is a warning.

**The case that hurts real data:** the text `NULL` (or `NA`, `n/a`) sitting in a NOT NULL
text/char column. If DMS wrote the literal word `NULL` and the endpoint marker is `NULL`, that
value becomes a real SQL NULL and a NOT NULL column rejects the row. Fix with
`null_rules=<schema>.<table>.<column>=none` to keep the text as data for that column.

**The danger of `null_values`:** setting it REPLACES the endpoint marker, so a real NULL that DMS
wrote as the word `NULL` would then load as the literal text `NULL` in a nullable text column
(and break a typed column). Prefer `null_rules` for a specific column over a blanket
`null_values`.

Optional read-only helper: `tools/gen_null_rules.py` (not shipped as automatic detection; a
decision for the user). MRL does NOT auto-detect NOT NULL columns.


## 5b. Settings source (every config/params.csv key)

MRL reads settings from the EXISTING `config/params.csv` (READ-ONLY) via a byte-identical copy of the original parser, so it accepts EVERY valid key and rejects unknown keys exactly as the original pipeline does - a valid key is never rejected just because MRL does not use it. The full `to_pipeline_settings()` output is written to `config/pipeline_parallel_load.json`. `map_max_concurrency` is the DEFAULT number of parallel range loads per table only when it is EXPLICITLY present in `config/params.csv`; otherwise the built-in default is 3 (NOT 6 - the original parser fills an absent key with 6, which MRL detects and ignores). In the original pipeline this key means 'groups at once'; here it means 'parallel range loads for one table'. A fleet-row `max_concurrency` column or an execution-input `max_concurrency` overrides it.

This table (generated from `tools/mrl_settings_classification.py`; a test fails if it drifts) classes each key USED / PASSED THROUGH (to the count or CDC jobs) / IGNORED HERE:

| params.csv key | class | where / reason |
|---|---|---|
| `account_id` | USED | identity; builds Glue job ARNs and the DSQL cluster resource |
| `big_table_bytes_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (cdc=true) |
| `big_table_row_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (CDC ownership only, cdc=true) |
| `cdc_drift_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_drift_check_minutes` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_drift_tolerance` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_engine` | PASSED THROUGH | reused CreateGlueJobs CDC engine selection (cdc=true) |
| `cdc_file_order_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_max_delete_fraction` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_max_delete_rows` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_nopk_overmatch_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_spark_fallback` | PASSED THROUGH | reused CDC Spark fallback (cdc=true) |
| `cdc_validation` | PASSED THROUGH | reused CDC jobs arg (cdc=true) |
| `cdc_validation_sample` | PASSED THROUGH | reused CDC jobs arg (cdc=true) |
| `conn_budget` | USED | capacity arithmetic + the count job's --conn_budget cap |
| `control_schema` | PASSED THROUGH | reused CDC jobs arg --control_schema (cdc=true only) |
| `cutover_count_check` | IGNORED HERE | original cutover count gate; not part of the MRL load |
| `discovery_num_workers` | PASSED THROUGH | reused CreateGlueJobs -> discovery job workers |
| `discovery_timeout_minutes` | PASSED THROUGH | reused CreateGlueJobs -> discovery timeout |
| `discovery_worker_type` | PASSED THROUGH | reused CreateGlueJobs -> discovery job WorkerType |
| `dsql_database` | USED | range-load + count job arg --dsql_database |
| `dsql_endpoint` | USED | DSQL endpoint for the range-load + count jobs (--dsql_endpoint) |
| `dsql_user` | USED | range-load + count job arg --dsql_user |
| `file_fanout_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (cdc=true) |
| `glue_connection` | USED | VPC Connections on every MRL Glue job |
| `glue_role_arn` | USED | Role on every MRL Glue job (range-load, count, reused discovery/CDC) |
| `glue_version` | USED | GlueVersion of the MRL Glue jobs |
| `guardrails_mode` | PASSED THROUGH | reused CDC/validate guardrail master (cdc=true) |
| `lambda_role_arn` | IGNORED HERE | setup-only (Lambda role; MRL runs under the existing roles) |
| `load_big_num_workers` | USED | DEFAULT NumberOfWorkers of the range-load job |
| `load_big_timeout_minutes` | USED | DEFAULT Timeout of the range-load job |
| `load_big_worker_type` | USED | DEFAULT WorkerType of the dedicated range-load job (load-big template sizing); a fleet-row load_worker_type overrides |
| `load_num_workers` | USED | range-load job NumberOfWorkers default |
| `load_timeout_minutes` | USED | range-load job Timeout default |
| `load_worker_type` | USED | range-load job WorkerType (per-task fleet-row overrides this) |
| `manage_iam` | IGNORED HERE | setup-only (IAM management mode) |
| `map_max_concurrency` | USED | the DEFAULT number of parallel range loads per table (Amendment 5; explicit key only, else built-in default 3); a fleet row / execution input overrides it |
| `max_big_cdc_forks` | PASSED THROUGH | reused PlanSplit CDC ownership cap (cdc=true) |
| `max_composite_forks` | PASSED THROUGH | reused PlanSplit CDC ownership cap (cdc=true) |
| `max_files_in_parallel` | USED | range-load job arg --max_files_in_parallel |
| `max_groups` | PASSED THROUGH | reused PlanSplit (CDC ownership only; MRL ignores load groups) |
| `max_parallel_tables` | IGNORED HERE | original job2 how-many-tables-at-once; MRL loads one table's ranges with its own max_concurrency (one table per task) |
| `max_writers_per_loader` | PASSED THROUGH | reused PlanSplit writer sizing (cdc=true) |
| `min_writers_per_loader` | PASSED THROUGH | reused PlanSplit writer sizing (cdc=true) |
| `null_rules` | USED | range-load + count job arg --null_rules (NULL handling) |
| `null_values` | USED | range-load + count job arg --null_values (NULL handling) |
| `per_worker_mem_budget_mb` | IGNORED HERE | original job2 driver-memory auto-throttle; MRL sizes the range-load job by worker type/count directly |
| `project` | USED | name prefix of the MRL Glue jobs and state machines |
| `region` | USED | AWS region for every DMS/Glue/S3 call |
| `security_group_id` | IGNORED HERE | setup-only (Glue VPC connection) |
| `sfn_role_arn` | IGNORED HERE | setup-only (Step Functions role) |
| `subnet_id` | IGNORED HERE | setup-only (Glue VPC connection); not an MRL runtime setting |
| `validate_count_check` | IGNORED HERE | the original validate's DMS-count gate; MRL's count job has its own count_timeout_action + tolerance |
| `validate_hash` | IGNORED HERE | per-value hash scope for the original validation; unused here |
| `validate_num_workers` | USED | NumberOfWorkers hint for the count Glue job |
| `validate_parallelism` | IGNORED HERE | validation throughput tuning; not used by the count job |
| `validate_rows_per_range` | IGNORED HERE | validation range sizing; MRL verifies by COUNT(*), not the per-range aggregate validation |
| `validate_target_seconds_per_range` | IGNORED HERE | validation range time sizing; unused here |
| `validate_timeout_minutes` | USED | Timeout for the count Glue job |
| `validate_worker_type` | USED | WorkerType hint for the count Glue job |
| `writers_per_file` | USED | range-load job arg --writers_per_file (and --max_write_concurrency) |

MRL-only knobs that are NOT in params.csv (max_parallel_tasks, count_timeout_action, count tolerance, dms_max_wait_hours, write_merged_status, and the per-task range-load worker overrides) live in fleet-row columns or the optional `config/params_parallel_load.csv`.

---

## 6. Deploy (CloudShell)

Your IAM team first attaches `iam/mrl-additions.json` to the existing roles (manage_iam=false:
MRL creates NO roles). Then, from `manual_range_pipeline/`:

```
tools/deploy_mrl.sh --dry-run --bucket <bucket> --project <project> --region <region> --account-id <account-id> --glue-role-arn <arn> --sfn-role-arn <arn> --lambda-role-arn <arn> --dsql-endpoint <host> --glue-connection <name>
```

Review the printed commands, then drop `--dry-run` to apply. The script uploads
`scripts/job2_load_manual_range.py` and `scripts/mrl_count.py` under their OWN keys (it never
overwrites the shared `scripts/job2_load.py`), builds the Lambda zip (bundling `pg8000`), creates
the count Glue job and the two state machines, and greps for any leftover `<<` placeholder.

---

## 7. Run, monitor, rerun

Upload `config/fleet_task_parallel_load.csv` (and optional `config/params_parallel_load.csv`), then start the fleet:

> **We load only `LOAD*.csv` full-load files.** DMS CDC change files (timestamp names like
> `20261002-123456789.csv`), `processed/` and `failed/` copies, and everything else in the table
> folder are NEVER picked, planned, loaded or counted — they are ignored and only reported in a
> plan info line. The plan fails closed if a file the deployed loader would load is not on the
> allow-list.

The `max_concurrency` and `cdc` execution inputs are OPTIONAL. Start the fleet:

```
# default (no override): max_concurrency from config/params.csv (or built-in 3); CDC off
aws stepfunctions start-execution --region <region> --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet --input '{"bucket":"<bucket>"}'
```

```
# override the parallel range count for EVERY task of this run (integer 1..40)
aws stepfunctions start-execution --region <region> --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet --input '{"bucket":"<bucket>","max_concurrency":4}'
```

```
# full_load_and_cdc: after the load + reconcile + a PASSING count, resume DMS and start CDC
aws stepfunctions start-execution --region <region> --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet --input '{"bucket":"<bucket>","cdc":true}'
```

The cutover for a CDC-enabled task is the ORIGINAL `<project>-cutover` / `<project>-fleet-cutover`
(MRL produces byte-for-byte the CDC jobs/tags/registry the original route produces); the Glue
7-day CDC-job limit still applies.

Monitor a task:

```
python3 tools/mrl_status.py --bucket <bucket> --task-suffix <task-suffix> --region <region>
```

**Rerun / resume.** Re-start the fleet with the same input: a task whose MRL child is already
RUNNING is skipped; a task that failed re-runs. Inside a child, a re-run resumes **only the
failed ranges** (done files are skipped via the per-range progress docs). A **listing change**
between runs fails closed (MRL never re-splits a table that already has loaded ranges).

---

## 8. Troubleshooting table

| Fail state | Meaning | What to do |
|---|---|---|
| `MrlPreflightFailed` | bad settings / DMS settings wrong for the mode / malformed NULL options / bucket mismatch / AddColumnName=false | read the cause; it names the exact fix (incl. the modify command) |
| `DmsStartFailed` | start-replication failed and the task is not running/finished | fix the DMS task (endpoint, mappings), re-run |
| `DmsFailed` | TablesErrored>0, status failed, or a stop reason wrong for the mode | fix the source/DMS issue, reload |
| `DmsPollBudgetExceeded` | DMS didn't reach the expected stop within `dms_max_wait_hours` | check DMS; raise the budget if the load is genuinely long |
| `MrlBuildTableListFailed` / `MrlDiscoveryFailed` | reused discovery failed | see the cause (unsupported transform, >9 schemas, a table not `Table completed`) |
| `MrlPlanFailed` | listing drift, two case-variant folders, mixed-width names, or no files with FullLoadRows>0 | the cause names the table + the difference; fix the export or delete the stale plan to re-split |
| `MrlSafetyGateFailed` | a target table is non-empty with no matching recorded plan (or missing) | DROP + recreate the table EMPTY (same DDL + PK), confirm 0 rows, re-run; or re-run with `override:true` if you accept possible duplicates |
| `MrlLoadFailed` | one or more range runs failed | re-run the same input to resume ONLY the failed ranges. A no-single-PK mid-file crash fails closed — blank the whole table, delete its range docs, restart all its ranges |
| `MrlReconcileFailed` | a file is missing / duplicated / failed / extra | see `reconcile.json` for the per-table lists; fix the failed ranges and re-run |
| `MrlCountFailed` | DSQL COUNT(*) ≠ DMS FullLoadRows, or a count couldn't complete (`count_timeout_action=fail`) | see `count_result.json` for both numbers per table; investigate before trusting the load |
| `MrlCdcStartNotConfirmed` | CDC job didn't report its poll loop in 45 min | full load is done + reconciled + counted and DMS is resumed; check the CDC job log and restart it with `--config_prefix` |

---

## 9. Capacity arithmetic (printed by preflight)

Per run: `max_files_in_parallel × writers_per_file` DSQL writers. Across runs:
`max_concurrency × max_files_in_parallel × writers_per_file` must stay under the DSQL connection
budget (`conn_budget`, default 900). The dedicated load job's `MaxConcurrentRuns` is set to
`max_concurrency + 1` (Glue defaults to 1 concurrent run per job — this is the critical setting).
G.4X driver memory limits `max_files_in_parallel` to ~20 (budget ~1500 MB per concurrent file).
Preflight prints the DPU (~`(load_workers+1)×4` per run) and subnet-IP (~`load_workers+1` per run)
estimates and WARNS on a tight budget; it fails only on a provably impossible single setting. For
a 406-file table, 4 ranges at G.4X×20 is the sweet spot (~2 h), matching the delivered patch's
RESULT.md math.

See `DESIGN.md` for the full concurrency/safety analysis and the reuse map, and `RESULT.md` for
what is verified by execution vs reading and the decisions made for you.
