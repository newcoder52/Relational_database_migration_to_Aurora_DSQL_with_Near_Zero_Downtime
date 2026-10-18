> **EXPERIMENTAL - not production-ready.** Offline-tested and live-tested in a test account through several rounds, each of which found and fixed defects; a full end-to-end run on a large multi-table dataset has not yet completed. Do not use on a production migration without your own dry run (`deploy_mrl.sh --dry-run`) and test run.

> _This folder was previously published as `manual_range_pipeline/`. It is **not** the same as `customer_patches/job2_load_manual_range/` (that one is the hand-driven CLI patch)._

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
| `max_concurrency` | no | integer 1–40 (parallel range loads per table). Precedence: execution input > this fleet column > `config/params_parallel_load.csv` > built-in default **3**. Not read from `config/params.csv` (Amendment 10) |
| `dms_start_mode` | no | `start` (default) or `adopt_stopped` |
| `load_worker_type` | no | Glue worker type for the range-load job (built-in default `G.4X`) |
| `load_workers` | no | integer ≥ 1 |
| `max_files_in_parallel` | no | integer ≥ 1 (per-run file fan-out; built-in default 30) |
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


## 5b. Settings source

MRL reads its identity/connection settings from the EXISTING `config/params.csv` (READ-ONLY, via a
byte-identical copy of the original parser) and writes only its own
`config/pipeline_parallel_load.json`; it never writes `config/pipeline.json`.

**Amendment 10:** MRL takes **sizing and parallelism ONLY from the fleet row / execution input /
built-in default** (one table per DMS task, so no "load big" sizing and no `map_max_concurrency`).
The keys MRL actually reads from `config/params.csv` are just these:

| params.csv key MRL uses | where |
|---|---|
| `account_id` | identity; builds Glue job ARNs and the DSQL cluster resource |
| `conn_budget` | capacity arithmetic + the count job's --conn_budget cap |
| `dsql_database` | range-load + count job arg --dsql_database |
| `dsql_endpoint` | DSQL endpoint for the range-load + count jobs (--dsql_endpoint) |
| `dsql_user` | range-load + count job arg --dsql_user |
| `glue_connection` | VPC Connections on every MRL Glue job |
| `glue_role_arn` | Role on every MRL Glue job (range-load, count, reused discovery/CDC) |
| `glue_version` | GlueVersion of the MRL Glue jobs |
| `null_rules` | range-load + count job arg --null_rules (NULL handling) |
| `null_values` | range-load + count job arg --null_values (NULL handling) |
| `project` | name prefix of the MRL Glue jobs and state machines |
| `region` | AWS region for every DMS/Glue/S3 call |

`control_schema` is also read, but only when `cdc=true` (passed to the reused CDC jobs). **Every
other valid `config/params.csv` key is accepted and ignored by MRL** (or passed untouched to the
reused CDC/plan_split jobs when `cdc=true`) - a valid key is never rejected, exactly like the
original pipeline. The full per-key classification table is in **[DESIGN.md](DESIGN.md)**
(generated from `tools/mrl_settings_classification.py`; a drift test keeps it in sync).

### Built-in defaults (used when the fleet row / execution input is blank)

These are the values that were effective before Amendment 10, so a blank fleet row changes nothing.
They live in ONE place, `lambdas/mrl_defaults.py`:

| knob | built-in default |
|---|---|
| `load_worker_type` | `G.4X` |
| `load_num_workers` | `10` |
| `load_timeout_minutes` | `2880` |
| `max_files_in_parallel` | `30` |
| `writers_per_file` | `8` |
| `max_concurrency` | `3` |
| `count_worker_type` | `G.8X` |
| `count_num_workers` | `10` |
| `count_timeout_minutes` | `2880` |
| `discovery_worker_type` | `G.2X` |
| `discovery_num_workers` | `5` |
| `discovery_timeout_minutes` | `480` |
| `conn_budget` | `900` |

---

## 6. Deploy (CloudShell)

The DEFAULT deployment needs NO IAM change — the existing roles already cover it (verify with `tools/mrl_iam_check.sh`; proven by `tests/test_existing_iam_covers_mrl.py`). Only the OPTIONAL fleet state machine (`--with-fleet-sm`) needs `iam/mrl-fleet-sm-optional.json` (manage_iam=false:
MRL creates NO roles). Then, from `parallel_range_load_pipeline/`:

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

The `max_concurrency` and `cdc` execution inputs are OPTIONAL. Start tasks with the default runner
(operator credentials, no state machine):

```
# default (no override): max_concurrency from the fleet row / built-in 3; CDC off
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region>
# override the parallel range count for EVERY task of this run (integer 1..40):
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region> --max-concurrency 4
# full_load_and_cdc: after the load + reconcile + a PASSING count, resume DMS and start CDC:
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region> --cdc true
```

**Optional — fleet state machine** (only if deployed with `deploy_mrl.sh --with-fleet-sm`, which
also needs `iam/mrl-fleet-sm-optional.json`): `aws stepfunctions start-execution --region <region>
--state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet --input
'{"bucket":"<bucket>"}'` (add `"max_concurrency":4` / `"cdc":true` as above).

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
