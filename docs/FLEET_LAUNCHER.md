# Fleet launcher: start or cut over many DMS tasks with one trigger

Two optional state machines on top of the per-task workflows:

- `<project>-fleet-startup` starts `<project>-startup` for every task in a list.
- `<project>-fleet-cutover` starts `<project>-cutover` for every task in a list.

Each is started once, by hand. It checks every task first, starts one per-task execution per
task (5 at a time), confirms each one got past its own input checks, and finishes in minutes.
Each task's migration then runs on its own, exactly as if you had started it by hand with
`{"taskArn": "..."}`. Nothing in the per-task workflows changes.

## Inputs

**Settings:** the same `s3://<bucket>/config/pipeline.json` every task already uses (RUNBOOK
Step 3c). The fleet never writes it. If you keep a `pipeline.json` next to the task list too, it
must be identical, or preflight stops (the per-task workflows would not use it).

**Task list:** a CSV in the bucket, e.g. `s3://<bucket>/config/fleet_tasks.csv`
([example](../config/fleet_tasks.example.csv)):

| Column | |
|---|---|
| `task_arn` | required. The DMS task ARN |
| `task_suffix` | optional. Leave blank to use the folder the pipeline would pick anyway: the one recorded for this task (a renamed DMS task keeps its first folder), else the DMS task name. Set it only for a task you also start by hand with a `taskSuffix` |
| `adopt_existing_folder` | optional, startup only. `true` for a task whose folder holds files from an earlier run but no owner record |

Before a startup fleet, stage each task's table list at
`config/_task/<task name>/table_manifest.csv` (RUNBOOK Step 5b).

**Start input** (both fleets):

```json
{ "bucket": "<pipeline bucket>", "inputPrefix": "config" }
```

`inputPrefix` is the folder holding the task list; add `"tasksFile": "wave2.csv"` to use another
file name. Missing `bucket` or `inputPrefix` ends at `MissingFleetInput`.

## What preflight checks (nothing starts if any task fails)

The `preflight-tasks` Lambda reuses the per-task workflow's own checks (`resolve_task`, same zip):

- the settings file (all of `resolve_task`'s settings checks);
- every row: a DMS task ARN in the pipeline's region, listed once, that exists; a legal folder
  name short enough for the Glue job names; no two rows on the same folder; a folder not owned by
  another task (`_task.json`);
- startup: the same pre-start checks on each DMS task and S3 endpoint as `resolve_task`
  (`full-load-and-cdc`, `StopTaskCachedChangesApplied=true`, `AddColumnName=true`, the pipeline
  bucket, not past its full load), the table list staged and not empty, a leftover folder needs
  `adopt_existing_folder`, and at most 9 distinct DSQL schemas across the fleet's table lists;
- cutover: each task was started by the pipeline (owner record present).

A failed preflight ends at `PreflightFailed`; its cause lists every problem by row.

**Schema limit:** DSQL allows 10 schemas per database and `cdc_control` uses one. Preflight counts
the fleet's own schemas; schemas already in the database from other tasks count too but are not
visible to it (it prints a warning with the count). Check with
`SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg\_%' AND schema_name <> 'information_schema';`.

## Starting again after a partial failure

Start the fleet again with the same input. Tasks whose per-task execution is already running are
skipped (`already_running`), and so are startup tasks the pipeline started earlier that are past
their full load (`past_full_load`). Only the rest are started. Even without the skip (for example
if the Lambda can't list executions), the per-task workflow refuses a second run of a running task.

## What the result means

- `FleetStarted`: every task was started, or skipped as already started, and each started one
  was still running 30 s later, i.e. past its own input checks. It does **not** mean the
  migrations succeeded: watch each `<project>-startup` / `<project>-cutover` execution.
- `FleetStartIncomplete`: at least one task did not start. The execution output's
  `results.tasks` lists each task with `status` (`started`, `skipped`, `not_started`) and, for
  `not_started`, the error or the child execution's status. The others were started.

## Deploy (once)

Use the variables from the RUNBOOK's "Fill in your values" block (`$PROJECT`, `$REGION`,
`$ACCOUNT_ID`, `$BUCKET`).

1. **Lambda.** `preflight_tasks.py` is in `lambdas/`, so it is already in `fn.zip` (it needs
   `resolve_task.py` from the same zip). Create the role and function:

   ```bash
   export AWS_PAGER=""
   for f in iam/preflight-tasks-role.policy.json iam/fleet-startup-role.policy.json iam/fleet-cutover-role.policy.json; do
     sed -e "s|<<REGION>>|$REGION|g" -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" \
         -e "s|<<BUCKET>>|$BUCKET|g" -e "s|<<PROJECT>>|$PROJECT|g" "$f" > "${f%.json}.filled.json"
   done
   sed -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" iam/fleet-startup-role.trust.json > /tmp/fleet-trust.json

   aws iam create-role --role-name $PROJECT-preflight-tasks-role \
     --assume-role-policy-document file://iam/preflight-tasks-role.trust.json
   aws iam put-role-policy --role-name $PROJECT-preflight-tasks-role --policy-name preflight \
     --policy-document file://iam/preflight-tasks-role.policy.filled.json
   sleep 10
   aws lambda create-function --function-name $PROJECT-preflight-tasks --runtime python3.12 \
     --handler preflight_tasks.handler --zip-file fileb://fn.zip --timeout 300 --memory-size 256 \
     --role arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-preflight-tasks-role --no-cli-pager
   ```

2. **Fleet roles** (Step Functions):

   ```bash
   for w in startup cutover; do
     aws iam create-role --role-name $PROJECT-fleet-$w-role \
       --assume-role-policy-document file:///tmp/fleet-trust.json
     aws iam put-role-policy --role-name $PROJECT-fleet-$w-role --policy-name fleet \
       --policy-document file://iam/fleet-$w-role.policy.filled.json
   done
   ```

3. **State machines:**

   ```bash
   SM=arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine
   for w in startup cutover; do
     sed -e "s|<<PROJECT>>|$PROJECT|g" \
         -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT-preflight-tasks|g" \
         -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM:$PROJECT-startup|g" \
         -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM:$PROJECT-cutover|g" \
         stepfunctions/fleet-$w.asl.json > fleet-$w.filled.asl.json
     grep -c '<<' fleet-$w.filled.asl.json    # must print 0
     aws stepfunctions create-state-machine --name $PROJECT-fleet-$w \
       --definition file://fleet-$w.filled.asl.json \
       --role-arn arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-fleet-$w-role --no-cli-pager
   done
   ```

4. **Run:**

   ```bash
   aws s3 cp fleet_tasks.csv s3://$BUCKET/config/fleet_tasks.csv
   aws stepfunctions start-execution --state-machine-arn $SM:$PROJECT-fleet-startup \
     --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}" --no-cli-pager
   ```

## Limits

- 5 tasks are started at a time (`MaxConcurrency` in `FanOut`), so each wave of 5 creates its
  Glue jobs together; raise it with care (Glue's job-creation API is rate limited).
- Each fleet execution handles up to a few hundred tasks comfortably (the Map's results stay
  well under Step Functions' 256 KB state limit up to roughly 400 tasks). Split bigger lists.
- Cutover is irreversible per task: the cutover fleet checks inputs, not readiness. Cut over
  only tasks whose CDC has caught up.
