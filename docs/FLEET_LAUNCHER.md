# Fleet launcher: how the fleet starts and cuts over tasks

The fleet is the way an operator starts and cuts over migration tasks. Two state machines sit on
top of the per-task workflows:

- `<project>-fleet-startup` starts `<project>-startup` for every task in a list.
- `<project>-fleet-cutover` starts `<project>-cutover` for every task in a list.

**One DMS task is one row in `fleet_tasks.csv`.** Starting a single task is a one-row list;
starting many is the same list with more rows. There is no separate "start one task by hand" path —
you always launch the fleet.

Each fleet is started once, by hand. It checks every task first, starts one per-task execution per
task (5 at a time), confirms each one got past its own input checks, and finishes in minutes. Each
task's migration then runs on its own, exactly as if the per-task workflow had been started with
`{"taskArn": "..."}`. Nothing in the per-task workflows changes.

Deploying the fleet (and everything it needs) is part of the one-time setup in
[`RUNBOOK.md`](../RUNBOOK.md): the `preflight-tasks` Lambda in
[Step 2](../RUNBOOK.md#step-2--create-the-lambda-functions), the shared settings in
[Step 3c](../RUNBOOK.md#step-3c--pipeline-settings), and the fleet state machines and roles in
[Step 4](../RUNBOOK.md#step-4--create-the-state-machines). This page is the reference for how
the fleet behaves once it exists.

## Inputs

**Settings:** the same `s3://<bucket>/config/pipeline.json` every task already uses
([RUNBOOK Step 3c](../RUNBOOK.md#step-3c--pipeline-settings)). The fleet never writes it directly.
If you keep a `pipeline.json` next to the task list too, it must be identical, or preflight stops
(the per-task workflows would not use it).

**Settings from `params.csv` (optional):** if `s3://<bucket>/<inputPrefix>/params.csv` exists next
to the task list, preflight validates it (shared parser `lambdas/params_csv.py`), builds the
`config/pipeline.json` it describes, and — **startup only, and only when no `startup`/`cutover`/fleet
execution is running (other than this fleet run)** — backs up the old `config/pipeline.json` to a
dated `config/pipeline.json.<UTC>` key and publishes the new one, then reads it back to verify. If
the candidate equals the live file nothing is written; if a cutover, or anything is running, or
listing executions fails, it stops at `PreflightFailed` and writes nothing (fail closed). A
`params.csv` plus a second `<inputPrefix>/pipeline.json` is ambiguous and fails. No `params.csv` →
exactly today's behaviour. The output reports `paramsPublished` / `backupKey` / `paramsReason`. See
the [safe-publish rule](../RUNBOOK.md#step-3c--pipeline-settings). (params.csv is offline-tested;
real-AWS test pending.)

**Task list:** a CSV in the bucket, e.g. `s3://<bucket>/config/fleet_tasks.csv`
([example](../config/fleet_tasks.example.csv)):

| Column | |
|---|---|
| `task_arn` | required. The DMS task ARN |
| `task_suffix` | optional. Leave blank to use the folder the pipeline would pick anyway: the one recorded for this task (a renamed DMS task keeps its first folder), else the DMS task name. Set it only for a task you also start by hand with a `taskSuffix` |
| `adopt_existing_folder` | optional, startup only. `true` for a task whose folder holds files from an earlier run but no owner record |

Before a startup fleet, stage each task's table list at
`config/_task/<task name>/table_manifest.csv`
([RUNBOOK Step 5b](../RUNBOOK.md#5b--upload-each-tasks-table-list)).

**Start input** (both fleets):

```json
{ "bucket": "<pipeline bucket>", "inputPrefix": "config" }
```

`inputPrefix` is the folder holding the task list; add `"tasksFile": "wave2.csv"` to use another
file name. Missing `bucket` or `inputPrefix` ends at `MissingFleetInput`.

## What preflight checks (nothing starts if any task fails)

The `preflight-tasks` Lambda reuses the per-task workflow's own checks (`resolve_task`, same zip):

- `params.csv` (if present): parsed and validated; its `project` must match the fleet's per-task
  state machine; on a startup with nothing running it publishes `config/pipeline.json` (see the
  [safe-publish rule](../RUNBOOK.md#step-3c--pipeline-settings)) before the settings check below;
- the settings file (all of `resolve_task`'s settings checks);
- every row: a DMS task ARN in the pipeline's region, listed once, that exists; a legal folder
  name short enough for the Glue job names; no two rows on the same folder; a folder not owned by
  another task (`_task.json`);
- startup: the same pre-start checks on each DMS task and S3 endpoint as `resolve_task`
  (`full-load-and-cdc`, `StopTaskCachedChangesApplied=true`, `AddColumnName=true`, the pipeline
  bucket, not past its full load), the table list staged and not empty, a leftover folder needs
  `adopt_existing_folder`, and at most 9 distinct DSQL schemas across the fleet's table lists;
- cutover: each task was started by the pipeline (owner record present).

A failed preflight ends at `PreflightFailed`; its cause lists every problem by row. Because
preflight runs the per-task rules across the whole list before anything starts, a task that would
fail its own `ResolveFailed` check is caught here first.

**Schema limit:** DSQL allows 10 schemas per database and `cdc_control` uses one. Preflight counts
the fleet's own schemas; schemas already in the database from other tasks count too but are not
visible to it (it prints a warning with the count). Check with
`SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg\_%' AND schema_name <> 'information_schema';`.

## Skip rules — starting again after a partial failure

Launch the fleet again with the same input. Tasks whose per-task execution is already running are
skipped (`already_running`), and so are startup tasks the pipeline started earlier that are past
their full load (`past_full_load`). Only the rest are started. Even without the skip (for example
if the Lambda can't list executions), the per-task workflow refuses a second run of a running task.

## Results — what the outcome means

- `FleetStarted`: every task was started, or skipped as already started, and each started one
  was still running 30 s later, i.e. past its own input checks. It does **not** mean the
  migrations succeeded: watch each `<project>-startup` / `<project>-cutover` execution.
- `FleetStartIncomplete`: at least one task did not start. The execution output's
  `results.tasks` lists each task with `status` (`started`, `skipped`, `not_started`) and, for
  `not_started`, the error or the child execution's status. The others were started.
- `PreflightFailed`: a task failed a check; **nothing was started**. The cause lists every problem
  by row.
- `MissingFleetInput`: the start input was missing `bucket` or `inputPrefix`.
- `FleetFailed`: the fan-out itself failed unexpectedly.

## Limits

- 5 tasks are started at a time (`MaxConcurrency` in `FanOut`), so each wave of 5 creates its
  Glue jobs together; raise it with care (Glue's job-creation API is rate limited).
- Each fleet execution handles up to a few hundred tasks comfortably (the Map's results stay
  well under Step Functions' 256 KB state limit up to roughly 400 tasks). Split bigger lists.
- Cutover is irreversible per task: the cutover fleet checks inputs, not readiness. Cut over
  only tasks whose CDC has caught up.
