# MRL LIVE TEST STEPS (real AWS, test account — DO NOT RUN as part of the offline build)

A step-by-step plan for a later REAL-AWS test of the MRL pipeline in the TEST account, with the
test prefix `t14` (a small multi-file table first, then a big table). Includes expected values,
cleanup, and the failure injections that cannot be done offline. Run only in the test account,
never against the customer's production tasks. Single-line commands / heredocs for CloudShell. Fill
`<account-id>`, `<bucket>`, `<region>`, `<project>`, `<task-suffix>`, `<schema>.<table>`.

## 0. Preconditions
- The base pipeline is deployed in the test account (`config/pipeline.json` present, the
  resolve-task / driver-discovery / create-glue-jobs Lambdas + the Glue templates uploaded).
- No new IAM is attached for the default deployment (the existing `<project>-lambda-exec-role`,
  `<project>-sfn-exec-role`, `<project>-glue-exec-role`.
- `tools/deploy_mrl.sh` has been run (NOT dry-run) so the MRL Lambdas, the `-mrl-count` Glue job
  and the `-mrl-task` / `-mrl-fleet` state machines exist.
- A DMS task `t14` exists writing to the pipeline bucket with `AddColumnName=true`, a small
  multi-file table and (later) a big table, with cdc=false (default) for the first pass.

## 1. Small multi-file table first (full_load_only, start)
1. Confirm the target table is EMPTY (same DDL + single-column PK) in DSQL:
   `SELECT count(*) FROM <schema>.<table>;` — expect 0.
2. Upload the fleet file (single line):
   `printf 'task_arn,max_concurrency,dms_start_mode\narn:aws:dms:<region>:<account-id>:task:t14,3,start\n' > ft.csv && aws s3 cp --region <region> ft.csv s3://<bucket>/config/fleet_task_parallel_load.csv`
3. Start the task(s) with the default runner (no state machine):
   `python3 tools/mrl_fleet_run.py --bucket <bucket> --project <project> --region <region>`
   (Optional, only if deployed with `--with-fleet-sm`: `aws stepfunctions start-execution --region <region> --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:<project>-mrl-fleet --input '{"bucket":"<bucket>"}'`)
4. Watch the child in the console, and:
   `python3 tools/mrl_status.py --bucket <bucket> --task-suffix t14 --region <region>`
   Expected: phase progresses preflight_done → loading → counted → complete; plan shows
   `N files, concurrency 3 -> <sizes>`; reconcile `allClean=True`; count verdict `ok`
   (DSQL == DMS FullLoadRows for the table).
5. Confirm in DSQL the row count equals the DMS FullLoadRows for the table (console or
   `aws dms describe-table-statistics --region <region> --replication-task-arn ...`).

Expected final state: `TaskSucceeded` (full_load_only). The merged normal `_file_status` doc
exists under the table's MRL config prefix.

## 2. Big table (406-file-class), N=4 ranges
1. Target EMPTY again (DROP + recreate with the SAME DDL + PK; confirm 0 rows).
2. the execution input {"bucket":...,"max_concurrency":4} (or a fleet-row `max_concurrency,4`) and `load_worker_type,G.4X`, `load_workers,20`,
   `max_files_in_parallel,20`, `writers_per_file,8` in the fleet row.
3. Start the fleet; expect the split `406 files, concurrency 4 -> 102,102,101,101` and ~2 h to
   finish (G.4X capacity math). Watch the per-range status in `mrl_status.py`.
4. Verify the count equals DMS FullLoadRows; reconcile clean.

## 3. full_load_and_cdc pass (a cdc-capable task)
1. Use a `full-load-and-cdc` task with `StopTaskCachedChangesApplied=true`, start the
   fleet with `{"bucket":...,"cdc":true}` (or a fleet-row `cdc,true`), `dms_start_mode,start`.
2. Expect: after load + reconcile + count, DMS is resumed (`resume-processing`) and the main CDC
   job starts and confirms its start marker under `config/_task/t14/_cdc_started/<exec>.json`.
   Final state `TaskSucceededCdc`. Confirm the CDC job run is RUNNING and the live count climbs.

## 4. adopt_stopped (the 2026-10-14 case)
1. Hand-stop a finished task (100%, 0 errored, all tables `Table completed`) with any stop
   reason (e.g. a manual `NORMAL` stop).
2. Fleet row `dms_start_mode,adopt_stopped`. Expect MRL does NOT call start-replication and
   proceeds straight to discovery/plan/load. Confirm no second DMS start in the task's history.

## 5. Failure injections that CANNOT be done offline (do in the test account only)
- **A range Glue run fails** (e.g. revoke the DSQL grant for 1 minute during one range, or
  stop one range's Glue run): expect the task to end `MrlLoadFailed` listing the (table, range);
  the sibling ranges finish; a re-run of the SAME fleet input resumes ONLY the failed range.
- **Count mismatch**: delete a handful of rows from the target before COUNT runs: expect
  `MrlCountFailed` naming the table with both numbers (DSQL vs DMS). Never a success on a
  mismatch.
- **Count cannot complete**: on a very large table, set `count_timeout_action=fail` (default)
  and a tiny `COUNT_TABLE_BUDGET_SECONDS` build of the count job: expect `MrlCountFailed`;
  then `count_timeout_action=warn` downgrades to a pass with a warning.
- **Non-empty target without a matching plan**: pre-load a few rows, then start a FRESH run:
  expect `MrlSafetyGateFailed` with the drop-and-recreate fix; `override:true` bypasses with a
  loud warning.
- **Throttled Glue start**: start many ranges with a low per-account Glue concurrent-run quota:
  expect `Glue.ConcurrentRunsExceededException` to be retried (not fail the task).
- **PrivateLink/VPC DSQL**: verify the count job connects via the PrivateLink candidate when the
  public name is unreachable (the shared failover helper).

## 6. Cleanup (test account)
- Stop any running MRL child / fleet execution.
- Delete the dedicated load job and the count job:
  `aws glue delete-job --region <region> --job-name <project>-t14-mrl-load`
  `aws glue delete-job --region <region> --job-name <project>-mrl-count`
- Remove MRL state: `aws s3 rm s3://<bucket>/mrl/ --recursive`
- (Optional) delete the MRL state machines + Lambdas if the test is done:
  `aws stepfunctions delete-state-machine ...` and `aws lambda delete-function ...` for the
  `<project>-mrl-*` names. The BASE pipeline resources are left untouched.
- Do NOT touch any non-`t14` task or any base-pipeline resource.

## 7. Expected values cheat-sheet
- split: `n=5,c=3 -> 2,2,1`; `n=406,c=4 -> 102,102,101,101`.
- phases: preflight_done, loading, counted, complete (full_load_only); + cdc_started
  (full_load_and_cdc).
- reconcile clean: `allClean=True`, merged normal doc written (if `write_merged_status`).
- count: DSQL COUNT(*) == DMS FullLoadRows (tolerance 0 by default).
