# MRL (Manual-Range-Load) — Operator Runbook

Step-by-step operation of the MRL pipeline. Commands are copy-pasteable in CloudShell / aws-cli;
fill the placeholders `<account-id>`, `<bucket>`, `<region>`, `<project>`, `<task-suffix>`,
`<schema>.<table>`.

> **Status:** the pipeline is fully **offline-tested** (unit + simulator + ASL audits). **Live
> testing is in progress** — see `LIVE_TEST_STEPS.md`. Do a `--dry-run` deploy and a single
> non-production task first.

---

## 1. When to use MRL vs the original fleet route

Use **MRL** for a table that is **too big for the usual full load** and needs its LOAD files split
across **parallel Glue runs** (e.g. hundreds of DMS LOAD part-files). MRL runs **one table per DMS
task**, plans the files into N contiguous ranges, and loads the ranges in parallel, then reconciles
and count-verifies.

Use the **original startup/fleet route** for everything else (normal full loads, multi-table tasks
handled by the existing grouping). MRL reuses the original discovery and CDC machinery verbatim and
replaces only the load planning/execution — so a table can move back to the original route at any
time (see `INTEGRATION.md`).

Never run **both** routes on one DMS task at once (the preflight route-conflict guard fails closed;
see §9 and the Do-NOT list).

---

## 2. Prerequisites

- The base pipeline is deployed in the account (its roles exist: `<project>-lambda-exec-role`,
  `<project>-sfn-exec-role`, `<project>-glue-exec-role`; `config/params.csv` present).
- Your DMS task(s) are `full-load` or `full-load-and-cdc`, with the S3 target endpoint
  `AddColumnName=true` and a `CsvNullValue` set.
- You can run aws-cli with permission to deploy. The DEFAULT deployment needs NO IAM change (run `tools/mrl_iam_check.sh` to confirm); only `--with-fleet-sm` needs `iam/mrl-fleet-sm-optional.json`.
- `pg8000` is bundled into the Lambda zip by the deploy script (verified at deploy time).

---

## 3. Deploy

The MRL Lambdas/state machines/Glue jobs run under the EXISTING customer roles; attach the extra
least-privilege policy first.

```
# 1. IAM: NONE for the default deployment. Run tools/mrl_iam_check.sh first (step 3a.4). Only
#    the OPTIONAL --with-fleet-sm needs iam/mrl-fleet-sm-optional.json.

# 2. DRY RUN first — prints every action, changes nothing:
bash tools/deploy_mrl.sh --dry-run \
  --bucket <bucket> --account-id <account-id> --region <region> --project <project> \
  --glue-role-arn arn:aws:iam::<account-id>:role/<project>-glue-exec-role \
  --sfn-role-arn  arn:aws:iam::<account-id>:role/<project>-sfn-exec-role \
  --lambda-role-arn arn:aws:iam::<account-id>:role/<project>-lambda-exec-role \
  --glue-connection <glue-connection> \
  --dsql-endpoint <dsql-endpoint> --dsql-user <dsql-user> --dsql-database <dsql-database>

# 3. Real deploy — drop --dry-run once the plan looks right:
bash tools/deploy_mrl.sh \
  --bucket <bucket> --account-id <account-id> --region <region> --project <project> \
  --glue-role-arn arn:aws:iam::<account-id>:role/<project>-glue-exec-role \
  --sfn-role-arn  arn:aws:iam::<account-id>:role/<project>-sfn-exec-role \
  --lambda-role-arn arn:aws:iam::<account-id>:role/<project>-lambda-exec-role \
  --glue-connection <glue-connection> \
  --dsql-endpoint <dsql-endpoint> --dsql-user <dsql-user> --dsql-database <dsql-database>
```

The deploy creates the MRL Lambdas (incl. `<project>-mrl-settings` and `<project>-mrl-cdc-handoff`),
the `<project>-mrl-task` + `<project>-mrl-fleet` state machines, and the `<project>-mrl-load` /
`<project>-mrl-count` Glue jobs. It fails closed if any ASL placeholder is unsubstituted or a Lambda
module is missing. The Lambda zip needs NO internet (no MRL Lambda imports pg8000; the Glue scripts
get pg8000 from the Glue runtime / VPC connection). By default the OPTIONAL fleet state machine is
NOT created (`--with-fleet-sm` to create it); start tasks with `tools/mrl_fleet_run.py`.

---

## 3a. Deploy from AWS CloudShell (step by step)

Public-safe placeholders only. One command per line; the expected output follows each.

**1. Variables.**
```
aws sts get-caller-identity --query Account --output text        # -> your 12-digit account id
export PROJECT=<project>        # the EXISTING pipeline's project prefix
export REGION=<region>
export BUCKET=<bucket>
```
Expected: the account id prints; the three vars are set.

**2. Get the code into CloudShell.**
Option A (GitHub reachable):
```
git clone https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime
cd Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime/parallel_range_load_pipeline
```
Option B (no GitHub from CloudShell): download the folder zip on a laptop, CloudShell **Actions ▸
Upload file**, then `unzip <file>.zip && cd parallel_range_load_pipeline`.
Then, either way, record what you have so you can compare:
```
git rev-parse HEAD                                               # -> the commit under test
sha256sum tools/deploy_mrl.sh lambdas/mrl_preflight.py scripts/job2_load_manual_range.py
```

**3. Discover the existing resources** (roles, DSQL, Glue connection) from what is already deployed:
```
aws lambda get-function-configuration --region $REGION --function-name $PROJECT-resolve-task --query Role --output text
aws stepfunctions describe-state-machine --region $REGION --state-machine-arn arn:aws:states:$REGION:$(aws sts get-caller-identity --query Account --output text):stateMachine:$PROJECT-startup --query roleArn --output text
aws glue get-job --region $REGION --job-name $(aws glue list-jobs --region $REGION --query "JobNames[?contains(@,'$PROJECT-') && contains(@,'-discovery')] | [0]" --output text) --query 'Job.Role' --output text
```
Expected: the `<project>-lambda-exec-role` / `-sfn-exec-role` / `-glue-exec-role` ARNs.

**4. IAM check — must say NO IAM CHANGE NEEDED:**
```
bash tools/mrl_iam_check.sh --project $PROJECT --region $REGION --account-id $(aws sts get-caller-identity --query Account --output text) --bucket $BUCKET
```
Expected last line: `VERDICT: NO IAM CHANGE NEEDED`. If instead it lists DENIED actions, STOP and
send your IAM team the denied lines (only the OPTIONAL fleet state machine ever needs
`iam/mrl-fleet-sm-optional.json`; the default deployment needs none).

**5. Deploy — dry run first, read it, then the real run.** Pass the role ARNs from step 3 (the
deploy copies the EXISTING `<project>-resolve-task` Runtime and VpcConfig for the new Lambdas,
falling back to the current defaults with a printed notice):
```
bash tools/deploy_mrl.sh --dry-run --bucket $BUCKET --project $PROJECT --region $REGION \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --glue-role-arn <glue-role-arn> --sfn-role-arn <sfn-role-arn> --lambda-role-arn <lambda-role-arn> \
  --glue-connection <glue-connection> --dsql-endpoint <dsql-endpoint> --dsql-user <dsql-user> --dsql-database <dsql-database>
# read the WILL CREATE / WILL UPDATE table and the exact aws commands, then drop --dry-run:
bash tools/deploy_mrl.sh --bucket $BUCKET --project $PROJECT --region $REGION \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --glue-role-arn <glue-role-arn> --sfn-role-arn <sfn-role-arn> --lambda-role-arn <lambda-role-arn> \
  --glue-connection <glue-connection> --dsql-endpoint <dsql-endpoint> --dsql-user <dsql-user> --dsql-database <dsql-database>
```
The deploy only needs permission to: **lambda create/update, glue create/update job,
stepfunctions create/update, s3 put**. (Optionally `states:ValidateStateMachineDefinition` lets the deploy pre-validate each state machine definition read-only before creating it; it is NOT required - the deploy warns and continues if the permission or CLI op is unavailable.) It calls **no `aws iam` command** and aborts (changing
nothing) if any target name exists but is not MRL-owned. It ends with
`EXISTING PIPELINE RESOURCES TOUCHED: 0`.

**6. Verify the deployment (changes nothing):**
```
aws lambda list-functions --region $REGION --query "Functions[?starts_with(FunctionName,'$PROJECT-mrl-')].FunctionName"
aws glue get-job --region $REGION --job-name $PROJECT-mrl-count --query 'Job.Name'
aws stepfunctions describe-state-machine --region $REGION --state-machine-arn arn:aws:states:$REGION:$(aws sts get-caller-identity --query Account --output text):stateMachine:$PROJECT-mrl-task --query name
aws s3 ls s3://$BUCKET/scripts/job2_load_manual_range.py
aws s3 ls s3://$BUCKET/scripts/job_mrl_count.py
aws s3 ls s3://$BUCKET/glue-templates/ | grep mrl-
python3 tools/mrl_fleet_run.py --bucket $BUCKET --project $PROJECT --region $REGION --dry-run   # smoke test: starts nothing
```

**7. First real run.** Upload the fleet file + optional MRL params, then start tasks (no state
machine needed):
```
aws s3 cp config/fleet_task_parallel_load.example.csv s3://$BUCKET/config/fleet_task_parallel_load.csv
aws s3 cp config/params_parallel_load.example.csv     s3://$BUCKET/config/params_parallel_load.csv   # optional
python3 tools/mrl_fleet_run.py --bucket $BUCKET --project $PROJECT --region $REGION
```
Watch with `python3 tools/mrl_status.py --bucket $BUCKET --region $REGION --suffix <task-suffix>`.
Stop a task from the Step Functions console (its `<project>-mrl-task` execution). **Rollback**
(original pipeline untouched): delete the `<project>-mrl-*` Lambdas, the `<project>-mrl-task` state
machine, `<project>-mrl-count` and `<project>-<suffix>-mrl-load` Glue jobs, and the S3 keys
`mrl/`, `glue-templates/mrl-*.json`, `scripts/job2_load_manual_range.py`,
`scripts/job_mrl_count.py`, `config/pipeline_parallel_load.json` — no original resource is touched.

**8. What this deployment changes.**

| created / written | names |
|---|---|
| Lambdas | `<project>-mrl-settings/-preflight/-plan/-reconcile/-cdc-handoff/-gate-eval/-ensure-job/-state/-fleet-preflight` (+ `-mrl-fleet` SM only with `--with-fleet-sm`) |
| Glue jobs | `<project>-mrl-count` (+ `<project>-<suffix>-mrl-load` per task at run time) |
| State machine | `<project>-mrl-task` |
| S3 keys | `scripts/job2_load_manual_range.py`, `scripts/job_mrl_count.py`, `glue-templates/mrl-load.json`, `glue-templates/mrl-count.json`, `config/pipeline_parallel_load.json`, `config/fleet_task_parallel_load.csv`, `config/params_parallel_load.csv`, `mrl/**` |
| **IAM** | **none** (the existing roles already cover the default deployment) |

---

## 4. The TWO params files

| file | owner | MRL behavior |
|---|---|---|
| `config/params.csv` | the **existing** pipeline | **READ-ONLY** for MRL. MRL reads its shared settings here via a byte-identical copy of the original parser. **Never add MRL-only keys here** — the original parser rejects unknown keys and would break the base pipeline. |
| `config/params_parallel_load.csv` | **MRL** | Optional `parameter,value`. MRL-only keys below; unknown keys are rejected (fail closed). |

MRL writes only its own `config/pipeline_parallel_load.json` (the published/pinned settings). MRL
**never** writes `config/params.csv`, `config/pipeline.json` or `config/fleet_tasks.csv`.

**MRL-only keys in `config/params_parallel_load.csv`** (all optional):

| key | meaning |
|---|---|
| `max_parallel_tasks` | how many **TASKS** (fleet rows) run at once (default **2**). E.g. 10 tasks with the default run **2 at a time**; the other 8 wait their turn. |
| `max_concurrency` | default parallel **range loads per table** (precedence layer; see §6). |
| `split_by` | `count` (default) or `bytes` — how ranges are balanced. |
| `split_allow_hex` | `true`/`false` — allow hex LOAD names (default per build). |
| `count_timeout_action` | `fail` (default) or `warn` — when a count cannot complete. |
| `count_tolerance` | integer ≥ 0 — allowed |DSQL − expected| row difference. |
| `write_merged_status` | `true`/`false` — write the merged normal `_file_status/<label>.json`. |
| `cdc` | `true`/`false` default for the run (a fleet row / execution input overrides). |

---

## 5. The fleet file `config/fleet_task_parallel_load.csv`

One row per DMS task. Header names must be exact; unknown columns are rejected. **Row cap: 200**
(more rows fail closed before anything starts). Columns:

`task_arn, task_suffix, cdc, max_concurrency, dms_start_mode, load_worker_type, load_workers,
max_files_in_parallel, writers_per_file, tables, override`

Worked example — **3 tasks**:

```
task_arn,task_suffix,cdc,max_concurrency,dms_start_mode,load_worker_type,load_workers,max_files_in_parallel,writers_per_file,tables,override
arn:aws:dms:<region>:<account-id>:task:<task-suffix>,<task-suffix>,false,3,start,G.4X,10,20,8,,false
arn:aws:dms:<region>:<account-id>:task:<task-suffix2>,,true,6,adopt_stopped,G.4X,10,20,6,<schema>.<table>,false
arn:aws:dms:<region>:<account-id>:task:<task-suffix3>,<task-suffix3>,false,,start,,,,,<schema>.<table2>,false
```

- Row 1: full-load only, 3 parallel range loads, MRL starts the task.
- Row 2: cdc=true, 6 parallel range loads, adopts an already-stopped task, one table only.
- Row 3: `max_concurrency` blank → falls through to params / the built-in default **3**.

Upload it to `s3://<bucket>/config/fleet_task_parallel_load.csv`.

---

## 6. `max_concurrency` precedence and the default 3

`max_concurrency` = the number of **parallel range loads for one table** (not a worker count, not
`max_parallel_tasks`). Precedence, highest first:

1. execution-input `max_concurrency` (on `start-execution`)
2. fleet-row `max_concurrency` column
3. `config/params_parallel_load.csv` `max_concurrency`
4. `config/params.csv` `map_max_concurrency` (only when **explicitly** present)
5. built-in default **3**

Worked split examples (ranges are contiguous, inclusive, balanced within 1):

- **406 files / 3** → `136, 135, 135`
- **5 files / 3** → `2, 2, 1`
- **406 files / 4** → `102, 102, 101, 101`

---

## 7. cdc true / false

- **`cdc=false`** (default): start/adopt the task → load ranges → reconcile → verify counts →
  **stop**. DMS is **not** resumed and CDC is **not** started.
- **`cdc=true`**: all of the above, then (only after a **passing** count) resume DMS
  (`resume-processing`) and start the CDC jobs exactly as the original startup does, confirming the
  start marker. Before CDC starts, MRL writes the full-load handoff at **every** prefix a CDC job
  will read (the task prefix AND each composite/big fork prefix), so a forked table is not skipped.

---

## 8. Start, monitor, re-run

### Start (default — no state machine)

Start tasks with `tools/mrl_fleet_run.py` using your OPERATOR credentials (this is the default; the
fleet state machine is NOT deployed unless you passed `--with-fleet-sm`):

```
# Minimal (uses params / built-in default 3; CDC off):
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region>

# With a max_concurrency override for every task this run:
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region> --max-concurrency 4

# With CDC on for every task this run:
python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region> --cdc true

# Preview only (starts nothing): add --dry-run. Start up to the limit and exit: --no-wait.
```

### Optional: fleet state machine (`--with-fleet-sm`)

ONLY if you deployed the optional fleet state machine (`deploy_mrl.sh --with-fleet-sm`, which also
needs `iam/mrl-fleet-sm-optional.json`), you may instead start it directly:

```
aws stepfunctions start-execution --region <region> \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet \
  --input '{"bucket":"<bucket>"}'                               # or add "max_concurrency":4 / "cdc":true
```

### Monitor

```
python3 tools/mrl_status.py --bucket <bucket> --region <region> --suffix <task-suffix>
```

Also watch the `mrl_fleet_run.py` live table (task suffix / execution / status) and the Step
Functions console (`<project>-mrl-task` executions), plus the Glue console for the
`<project>-<suffix>-mrl-load` / `<project>-mrl-count` runs. (With `--with-fleet-sm`, the parent
`<project>-mrl-fleet` execution is also visible.)

Expected phases per task: **Preflight → Discovery → Plan → SafetyGate → LoadRange (parallel) →
Reconcile → Count → (cdc=true: ResumeDms → StartCdc + fork CDC)**. Timings depend on file count,
range concurrency and worker size; the load phase dominates.

### Re-run / resume after a failure

Re-run the **same** execution input. MRL is resume-safe:

- Finished ranges (a per-range `done` status doc exists) are **skipped**; only the failed ranges
  re-run.
- The plan is **frozen** per `(suffix, max_concurrency)`: re-running with a **different**
  `max_concurrency` after any range has loaded **fails closed** (changing the split could lose or
  double-count rows). To re-plan, let the target be clean/empty again (or a fresh suffix).

---

## 9. Fail-closed messages — what they mean and what to do

Every MRL failure is **fail closed** (never a silent success). Common classes:

| message class | meaning | operator action |
|---|---|---|
| **settings** (`config/params.csv ... problem(s)` / `would change pipeline_parallel_load.json ... running`) | params.csv missing/invalid, or a params change while a run is live | fix `config/params.csv`; do not edit params mid-run |
| **route conflict** (`ORIGINAL ... RUNNING` / `ORIGINAL-route load is in progress` / `cannot verify ... R22-1`) | the original startup/cutover is running for this task, an original load is mid-flight, or the guard could not list executions (AccessDenied) | finish/stop the original route first; if denied, add the `states:ListExecutions/DescribeExecution` grants; `override=true` only if you accept the risk |
| **safety gate** (`SafetyGateFailed` — target not empty, no matching plan) | the DSQL target is non-empty and this is not a matching resume | confirm the table should be (re)loaded; use `override=true` to bypass the non-empty gate (loud warning) |
| **plan ≠ loader set** (`plan ... does not equal the loader's set` / stray / mixed-width / mixed-case hex) | the planned file set diverged from what the loader would load | investigate the DMS export (stray file, mixed width, mixed-case hex); re-run after fixing |
| **range load failed** (`MrlLoadFailed`) | one or more range Glue runs failed | read the Glue run logs; re-run the same input (finished ranges are skipped) |
| **count mismatch** (`MrlCountFailed` — `COUNT MISMATCH`) | DSQL row count ≠ expected (beyond tolerance) | investigate the data; never accept a mismatch |
| **count cannot complete** (`COUNT COULD NOT COMPLETE`, `count_timeout_action=fail`) | the count timed out | re-run; raise count timeouts, or set `count_timeout_action=warn` to downgrade |
| **count no basis** (`NO COMPARISON BASIS`, `count_no_basis_action=fail`) | a loaded table has no DMS/discovery row count to check against | confirm the load another way; `count_no_basis_action=warn` to downgrade |
| **cdc handoff** (`MrlCdcHandoffError` — forked table has no clean reconcile count) | a forked table was not clean when CDC was about to start | fix the failed ranges / reconcile; re-run |
| **Glue throttle** (`ConcurrentRunsExceededException`) | too many concurrent Glue runs | the state machine retries automatically; if persistent, lower concurrency or raise the Glue quota |
| **target-generated primary key** (`TARGET-GENERATED PRIMARY KEY refused` / `FAIL CLOSED (R6)`) | a primary-key column is NOT supplied by the source (not in the mapping's loaded columns), so the target DEFAULT (e.g. `gen_random_uuid()`) would generate it | make the source/mapping supply every PK column, or use a key the source provides; this is refused because a re-run would silently duplicate |

**Target-generated primary keys are refused (R6).** The parallel range pipeline REQUIRES every
primary-key column to come from the source file. If a PK column is not loaded (it is absent from the
DMS CSV, so the target's column DEFAULT — e.g. `gen_random_uuid()` or an identity sequence — would
mint the key), the pipeline FAILS CLOSED before any range is launched: because that key is not tied
to the source row, a re-run (manual, the automatic range re-run, or the commit-probe resume) would
insert the same source rows again with NEW keys and the PRIMARY KEY could not catch the duplicates.
Non-key columns that are missing from the file and have a default are allowed (they get the default,
as intended) but are listed in a warning so you can confirm it.

---

## 10. CDC cutover (after `cdc=true`)

MRL starts the main + fork CDC jobs exactly as the original startup does; the cutover is then the
**ORIGINAL** cutover — MRL does **not** add its own. Run the original cutover state machine for the
task as usual. It finds every CDC job (main + composite `ck-<slug>` + big `bg-<slug>`) by the
existing **tags / registry / `--config_prefix`**, stops them, and completes the handover. Because
MRL wrote the full-load status at every fork prefix (§7), each fork CDC job actually applied its
changes before cutover.

---

## 11. Cleanup

- Delete the MRL state for a finished task: `aws s3 rm s3://<bucket>/mrl/state/<task-suffix>/
  --recursive` (MRL-owned keys only — never touch `config/_task/*`).
- Remove the MRL Glue jobs / state machines / Lambdas only if you are retiring MRL in the account.

---

## 12. Do NOT

- **Never** start the original startup AND MRL on the **same** DMS task at once (the guard fails
  closed; both routes writing one table would corrupt it).
- **Never** put MRL-only keys in `config/params.csv` — the original parser rejects unknown keys and
  would break the base pipeline. Put them in `config/params_parallel_load.csv`.
- **Never** hand-edit anything under `mrl/` state (plans, status, reconcile) — it is the source of
  truth for resume and the safety gates.
- **Never** drop/reload a table whose CDC has already run without the **CDC reset** first — see the
  note below. A reload without the reset makes CDC skip already-applied change files (silent loss).
- **Never run the ORIGINAL `<project>-startup` (or `<project>-cutover`) on a DMS task MRL has
  already loaded**, unless you intend to continue it. MRL writes the shared owner marker
  `config/_task/<suffix>/_task.json`; the original startup, pointed at the SAME task+suffix, sees a
  matching `taskArn` and **PROCEEDS (reuses the folder)** — re-running its full load INTO the
  already-MRL-loaded tables (overwrite / duplication). MRL guards the reverse direction, but the
  unmodified original route has no guard against MRL.

### Continuing to validation / CDC after a `cdc=false` MRL run

After a `cdc=false` MRL run the tables are fully loaded and reconciled/counted. To continue to CDC,
re-run the SAME task through **MRL with `cdc=true`** (it resumes DMS and starts CDC exactly as the
original route would), NOT the original `<project>-startup` — the original startup would re-run its
full load into the already-loaded tables (verified against `../lambdas/resolve_task.py`: a matching
owner marker makes `_check_owner` return reuse, so startup proceeds with its own discovery + load).
If you must use the original route instead, treat the MRL load as the original's full-load: cut the
task over with the original `<project>-cutover` (it finds the CDC jobs by tag), not by re-running
startup.

### Live-test lessons (operational prerequisites)

- DMS names LOAD files in **hex** (LOAD0000000A after LOAD00000009). MRL accepts hex by default; `split_allow_hex` in `config/params_parallel_load.csv` is an opt-OUT (leave it unset). A table with 10+ files is the normal case.


- Each DMS task must use a replication instance whose **test connection to BOTH endpoints
  succeeds** before the run.
- The DSQL target tables must **already exist** (created durably — verify from a FRESH connection)
  and be **EMPTY** before the run (the non-empty safety gate fails closed otherwise).
- Reusing a **deleted task's NAME with a new ARN** is refused by the original folder-owner guard
  (`config/_task/<suffix>/_task.json` records the old ARN). Use a different `task_suffix`, or
  archive `config/_task/<suffix>/` first (e.g. `aws s3 mv … config/_archive/<suffix>-<date>/`).
- The count/probe Glue job runs **python-shell behind the VPC**: `driver-cdc/` must hold the DSQL
  driver wheels (the deploy checks this); MRL feeds the job the Requires-Dist-stripped set so Glue
  never contacts PyPI (a 20-minute hang otherwise).
- `driver-cdc/` must also contain **boto3 + botocore + s3transfer** wheels (dsql-aware, e.g.
  1.42.x): the Spark `mrl-load` job gets a DSQL-capable boto3 via `--additional-python-modules`
  from them (Glue 4.0's bundled botocore has no `dsql` service → `Unknown service: 'dsql'`). The
  pg8000 stack stays on `--extra-py-files`; boto3 must NOT go there (it breaks botocore's data-dir
  lookup under Spark). The deploy aborts if any of boto3/botocore/s3transfer is missing.

### CDC reset note

If a table already has CDC progress and you must reload it, reset its CDC state FIRST (do **not**
reload blind):

- `UPDATE` the table's `cdc_control` row so `cdc_status.last_done_file = NULL` (set it to NULL —
  **never `DELETE` the control row**).
- Delete that table's `cdc_file_status` rows (the per-file progress), so CDC re-applies from the
  start.

MRL does not do this automatically; its preflight **fails closed** when it detects prior CDC
progress unless you pass `override=true` (accepting that you performed the reset).

---

## 13. Coexistence with the original pipeline in the same bucket

MRL and the original pipeline share ONE bucket, account and project name. MRL only adds NEW-named
resources and writes under `mrl/` (plus the allow-listed cdc=true handoff inside the shared
`config/_task/<suffix>/`). `tests/test_coexistence_no_collision.py` proves there is no collision.

**Created (all NEW names):**

| resource | name |
|---|---|
| Lambdas | `<project>-mrl-*` (9 functions) |
| Glue jobs | `<project>-mrl-count`, `<project>-<suffix>-mrl-load` |
| State machine | `<project>-mrl-task` (+ optional `<project>-mrl-fleet`) |

**Written in the shared bucket:**

| key / prefix | who writes | shared or MRL-only |
|---|---|---|
| `mrl/**` | MRL | MRL-only |
| `config/pipeline_parallel_load.json` | MRL (mrl-settings) | MRL-only (never `config/pipeline.json`) |
| `config/fleet_task_parallel_load.csv`, `config/params_parallel_load.csv` | operator / MRL | MRL-only names |
| `scripts/job2_load_manual_range.py`, `scripts/job_mrl_count.py` | deploy | MRL-only names |
| `glue-templates/mrl-load.json`, `glue-templates/mrl-count.json` | deploy | MRL-only names |
| `config/_task/<suffix>/…` (manifests, column maps) | the REUSED original discovery | shared (same as the original route) |
| `config/_task/<suffix>/_load_status.json` + each `…/_orchestrator/ck-<slug>/` and `…/group-<k>/_load_status.json` | MRL reconcile / cdc-handoff, **cdc=true only** | shared (the handoff the reused CDC jobs gate on) |

With `cdc=false`, MRL's own code writes NOTHING inside `config/_task/` (only the reused discovery
does, exactly as the original route would). MRL **never** deletes any key outside `mrl/`.

**What the original pipeline does that can affect MRL objects:**
- Nothing in the base repo deletes unknown S3 keys (`setup.sh` only `cp`s its own file names and
  `rm`s its own `driver-*/ *.whl` wheels), so re-running the original setup cannot remove MRL
  objects.
- The original **cutover** (create_glue_jobs mode=delete, exact-tag selection) finds a task's jobs
  by the `dsql_pipeline_project` + `dsql_pipeline_task=<suffix>` tags. The `<project>-<suffix>-mrl-load`
  job carries those tags, so cutover WAITS for its runs to stop and then DELETES it — desired
  cleanup after cutover. `<project>-mrl-count` has NO task tag, so cutover never deletes it.
- `resolve_task` (original startup) refuses a second RUNNING execution for a task and, finding
  `config/_task/<suffix>/` state, behaves as tested: **(A)** MRL discovery/load artifacts with NO
  owner marker → the original startup **REFUSES** (fail closed), it does not overwrite; **(B)** with
  the matching `_task.json` owner marker (which the reused discovery writes) → it **REUSES** the
  folder (does not refuse, does not overwrite the marker) — which is exactly why the route-conflict
  guard forbids running both routes on one task at once; **(C)** a different owning task → refuse.

**Three cases an operator must know:**
1. **Same task started on both routes** — MRL preflight's route-conflict guard REFUSES (fail
   closed) if the original startup/cutover is running for the task or an original load is in
   progress; never run both on one task.
2. **Original cutover after an MRL `cdc=true` run** — works unchanged. Cutover finds the main CDC
   job plus each `ck-<slug>`/`bg-<slug>` fork CDC job by tag/registry/`--config_prefix`, stops them
   and completes the handover (MRL wrote the full-load handoff at every fork prefix first, so each
   fork actually applied its changes).
3. **Removing MRL** — see the §3a rollback list; the original pipeline is untouched.

---

## 14. DSQL transient errors in the parallel range load (AMENDMENT 2)

Running the parallel range load against Aurora DSQL, you WILL occasionally hit a network blip, a
"server unavailable" (XX000), or a concurrency conflict (40001/OC000/OC001) — especially with many
writers. The pipeline absorbs these at two layers so a blip does not fail a 100M-row table.

### What is retried IN-PROCESS (inside the Glue load job, per chunk)

Every chunk is one DSQL transaction. The attempt counters **reset after every successful chunk
commit** (they are per-chunk, not per-file or per-run), so a long load with the occasional blip
never exhausts a budget. Per chunk:

| Failure class | Detect | Retries | Backoff | On exhaustion |
|---|---|---|---|---|
| Network / broken pipe / class-08 / SSL EOF / reset | `is_broken_pipe_error` | **10** | exponential **full-jitter**, base 1 s, cap 60 s (≈5 min total) + reconnect with a **fresh IAM token** each time | loud failure, range re-runnable |
| DSQL "server unavailable" (XX000) | `is_transient_server_error` | **10** | exponential full-jitter, base 0.5 s, cap 60 s | loud failure, range re-runnable |
| Concurrency conflict (40001 / OC000 / OC001) | `is_occ_conflict` | **8** | exponential full-jitter, 50 ms → 5 s cap | loud failure, range re-runnable |
| Initial DSQL connect | `connect_dsql` | **8** | exponential full-jitter, fresh token each time | the writer's guard surfaces it |

**Ambiguous commit (the connection drops AT COMMIT):** DSQL may or may not have committed. The loader
simply re-inserts; because the target has a PRIMARY KEY, if the prior commit had landed the re-insert
hits a unique violation (23505) on a RETRY and is treated as **already committed** (counted, not
duplicated); if it had not landed, the re-insert succeeds (no loss). A 23505 on the **first** attempt
(no prior retry) is NOT swallowed — it means a genuine **duplicate PK in the source**, and the job
**fails loud** rather than skip rows. This is proven offline for every injected failure class by
`tests/test_dsql_fault_injection.py` (including a 2,000-run randomized soak).

All reconnect backoffs use **full jitter** so a wide fan-out (e.g. 288 writers) never retries in
lockstep.

### What is retried BY STEP FUNCTIONS (the whole range re-run)

If the Glue run for a range still FAILS after the in-process retries (e.g. a DSQL outage longer than
~5 min), the state machine automatically **re-runs the same range** up to **3 times** (waiting
2 / 4 / 8 min, `States.TaskFailed` Retry on `RunRange`). Re-running the same range is a safe
**resume**: the loader checkpoints per file and per chunk, the commit-probe skips already-committed
chunks, done files are skipped, and the overlap-guard treats an identical range as a resume (not a
conflict). This is IN ADDITION to the existing `Glue.ConcurrentRunsExceeded` / `Glue.Throttling` /
`States.ServiceException` / `States.Timeout` retries, which keep their own separate attempt counts.
The read-only `CountVerify` and `SafetyProbe` Glue runs also get a bounded 2× `States.TaskFailed`
re-run (safe — they only read).

### What is NOT retried (deterministic errors)

A **deterministic** failure re-fails at the same file after a quick resume, so the 3 automatic
re-runs are wasted effort but bounded (done files are skipped each time). These are NOT transient:
NOT NULL violation, type-cast failure, the no-single-PK resume refusal, the overlap guard, and bad
arguments. After the re-runs are exhausted, the execution ends at **`MrlLoadFailed`** and the result
surfaces the **ORIGINAL** error text (not just "retries exhausted"), so you can see the real cause.
Worst-case wasted time on a deterministic failure ≈ the 3 backoff waits (2 + 4 + 8 = **14 min**) plus
a quick resume per attempt — bounded and small relative to a multi-hour load.

### Reading the per-range retry summary

Each range's result document (`_range_result/<label>.range-<first>-<last>.json`) carries a
`retry_summary`: counts of `network_retries`, `server_unavailable_retries`, `occ_retries`,
`reconnects`, `ambiguous_commit_recoveries`, and the `longest_backoff_s`. The Glue log also prints a
one-line `ⓘ retry summary …` at the end of each worker, so you can see how rough the ride was. All
zeros = a clean load.

### Resuming manually if the automatic retries are exhausted

Re-run the **same execution input** (§8 "Re-run / resume after a failure"). It resumes: done files
are skipped, incomplete ranges reload, the commit-probe prevents duplicates, and the final count
verifies the whole table. There is nothing to clean up first.

### Killing a range — behaviour change

Because a failed Glue run is now auto-retried, **killing a single range's Glue run no longer stops
that range** — the state machine just re-runs it (up to 3×). To genuinely stop a table, **stop the
Step Functions EXECUTION** (not the individual Glue run). An operator kill that recurs past the 3
re-runs ends the execution at `MrlLoadFailed`.

### Sizing advice (start small, step down on throttling)

DSQL documents a **per-cluster connection limit of 10,000**; preflight now FAILS CLOSED if the
planned writers (`max_concurrency × max_files_in_parallel × writers_per_file`) exceed it (pass
`override=true` to proceed anyway, accepting the throttling risk). The soft `conn_budget` still only
warns. **Start conservative: `max_files_in_parallel=10` and `writers_per_file=8`** per run, and step
DOWN if you see repeated `server unavailable` / connection throttling in the retry summaries. A tight
`conn_budget` warning is your cue to lower concurrency before the cluster throttles you.
