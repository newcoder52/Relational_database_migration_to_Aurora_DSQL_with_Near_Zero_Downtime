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
- You can run aws-cli with permission to deploy (or hand `iam/mrl-additions.json` to your IAM team).
- `pg8000` is bundled into the Lambda zip by the deploy script (verified at deploy time).

---

## 3. Deploy

The MRL Lambdas/state machines/Glue jobs run under the EXISTING customer roles; attach the extra
least-privilege policy first.

```
# 1. Attach the IAM additions (give iam/mrl-additions.json to your IAM team, or apply the three
#    inline policies to the matching roles). Fill <account-id>/<region>/<project>/<bucket>.

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
`<project>-mrl-count` Glue jobs. It fails closed if any ASL placeholder is unsubstituted or the zip
is missing a module or `pg8000`.

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

### Start

```
# Minimal (uses params / built-in default 3; CDC off):
aws stepfunctions start-execution --region <region> \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet \
  --input '{"bucket":"<bucket>"}'

# With a max_concurrency override for every task this run:
aws stepfunctions start-execution --region <region> \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet \
  --input '{"bucket":"<bucket>","max_concurrency":4}'

# With CDC on for every task this run:
aws stepfunctions start-execution --region <region> \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet \
  --input '{"bucket":"<bucket>","cdc":true}'
```

### Monitor

```
python3 tools/mrl_status.py --bucket <bucket> --region <region> --suffix <task-suffix>
```

Also watch the Step Functions console (`<project>-mrl-fleet` → the child `<project>-mrl-task`
executions) and the Glue console for the `<project>-mrl-load` / `<project>-mrl-count` runs.

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

### CDC reset note

If a table already has CDC progress and you must reload it, reset its CDC state FIRST (do **not**
reload blind):

- `UPDATE` the table's `cdc_control` row so `cdc_status.last_done_file = NULL` (set it to NULL —
  **never `DELETE` the control row**).
- Delete that table's `cdc_file_status` rows (the per-file progress), so CDC re-applies from the
  start.

MRL does not do this automatically; its preflight **fails closed** when it detects prior CDC
progress unless you pass `override=true` (accepting that you performed the reset).
