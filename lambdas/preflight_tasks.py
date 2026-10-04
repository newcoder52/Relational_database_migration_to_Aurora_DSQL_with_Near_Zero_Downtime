"""
preflight-tasks Lambda (fleet-startup and fleet-cutover workflows).

The fleet workflows are started ONCE, by hand, to launch the per-task <project>-startup (or
<project>-cutover) workflow for many DMS tasks at once. This Lambda is their first step: it reads
the task list and checks EVERY task before a single one is launched. Any problem raises
PreflightError and the fleet stops at PreflightFailed: nothing is started (fail-closed).

It reuses resolve_task's own functions (same Lambda zip), so it can never pass a task that the
per-task workflow would refuse.

INPUTS
  - s3://<bucket>/config/pipeline.json   the shared settings EVERY task's workflow reads. Checked
                                         with resolve_task's full settings check. Never written.
                                         If <inputPrefix>/pipeline.json also exists it must be
                                         identical, otherwise preflight fails (the per-task
                                         workflows would not use it).
  - s3://<bucket>/<inputPrefix>/<tasksFile, default fleet_tasks.csv>   the task list. Header row:
        task_arn               REQUIRED  arn:aws:dms:<region>:<acct>:task:<id>
        task_suffix            optional  folder/job-name override. Blank = what the pipeline
                                         would use anyway: the folder recorded for this task ARN
                                         (a renamed task keeps its original folder), else the
                                         DMS task name.
        adopt_existing_folder  optional  true/false (startup only; see resolve_task)

CHECKS, every row (all problems collected, then one error):
  both modes  task_arn is a DMS task ARN in the pipeline's region, listed once; the task exists;
              the folder name is legal and short enough for the Glue job names; no two rows use
              the same folder; the folder is not owned by a different task (_task.json).
  startup     resolve_task's pre-start checks on the task and its S3 endpoint
              (full-load-and-cdc, StopTaskCachedChangesApplied, AddColumnName, bucket, not past
              full load, ...); table_manifest.csv is staged; a folder with files from an earlier
              run but no owner record needs adopt_existing_folder; the number of distinct DSQL
              schemas in the fleet's table lists is at most 9 (DSQL's 10-schema limit minus
              cdc_control).
  cutover     the task was started by the pipeline (owner record present).

SKIPPED, NOT STARTED AGAIN (so re-triggering the fleet after a partial failure only starts the
missing tasks):
  already_running    the per-task workflow ("stateMachineArn") already has a RUNNING execution
                     for this task (needs states:ListExecutions/DescribeExecution).
  past_full_load     startup only: the pipeline started this task before (owner record) and DMS
                     has finished its full load, i.e. its startup already ran through.

OUTPUT (consumed by the fleet Map):
  { "ok": true, "count": N, "toStart": M, "skipped": K,
    "tasks": [ { "taskArn", "taskSuffix", "suffixSource", "skip": null | "already_running" |
                 "past_full_load", "runningExecution", "runName", "input": {child start input} }, ... ],
    "distinctSchemas": [...], "warnings": [...] }
  "input" holds taskArn, plus taskSuffix only when the CSV sets one, plus adoptExistingFolder only
  when true, so the per-task workflow picks the folder exactly as it would when started by hand.

Needs: s3:GetObject + s3:ListBucket on the bucket; dms:DescribeReplicationTasks and
dms:DescribeEndpoints; states:ListExecutions + states:DescribeExecution on the per-task workflow
(optional: without them every task is started and the per-task duplicate-run guard still refuses
second runs).
"""

import csv
import io
import json
import os
import time

import boto3

import resolve_task as rt     # same zip: reuse the per-task workflow's exact rules

REGION = os.environ.get("AWS_REGION", "us-east-1")
SETTINGS_KEY = rt.SETTINGS_KEY_DEFAULT          # config/pipeline.json
MAX_DISTINCT_SCHEMAS = 9                        # DSQL: 10 schemas per database, minus cdc_control
_TRUE = {"true", "1", "yes", "y"}


class PreflightError(Exception):
    pass


def _get_text(s3, bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return None
        raise


def _manifest_schemas(text):
    """Distinct DSQL schemas a table list loads into (dms_schema lowercased, as discovery does)."""
    out = set()
    for row in csv.DictReader(io.StringIO(text or "")):
        s = str(row.get("dms_schema") or "").strip().lower()
        if s:
            out.add(s)
    return out


def _running_tasks(state_machine_arn, warnings):
    """{taskArn: executionArn} for RUNNING executions of the per-task workflow, or {} (with a
    warning) if they can't be listed."""
    if not state_machine_arn or "<<" in state_machine_arn:
        return {}
    found = {}
    try:
        sfn = boto3.client("stepfunctions", region_name=REGION)
        token, scanned = None, 0
        while True:
            kw = {"stateMachineArn": state_machine_arn, "statusFilter": "RUNNING", "maxResults": 100}
            if token:
                kw["nextToken"] = token
            page = sfn.list_executions(**kw)
            for ex in page.get("executions", []) or []:
                scanned += 1
                d = sfn.describe_execution(executionArn=ex["executionArn"])
                try:
                    arn = str((json.loads(d.get("input") or "{}") or {}).get("taskArn") or "").strip()
                except ValueError:
                    arn = ""
                if arn:
                    found[arn] = ex["executionArn"]
            token = page.get("nextToken")
            if not token or scanned >= 2000:
                break
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "") or type(e).__name__)
        warnings.append(f"could not list running executions of {state_machine_arn} ({code}); every "
                        f"task will be started, and the per-task workflow refuses a second run of "
                        f"a task that is already running.")
        return {}
    return found


def handler(event, context):
    fleet_input = event.get("fleetInput") or {}
    bucket = event.get("bucket") or fleet_input.get("bucket")
    mode = str(event.get("mode") or "startup").strip().lower()
    if mode not in ("startup", "cutover"):
        raise PreflightError(f"mode must be 'startup' or 'cutover' (got {mode!r}).")
    input_prefix = str(event.get("inputPrefix") or fleet_input.get("inputPrefix") or "").strip().strip("/")
    tasks_file = str(fleet_input.get("tasksFile") or event.get("tasksFile") or "fleet_tasks.csv").strip().lstrip("/")
    base = (input_prefix + "/") if input_prefix else ""
    tasks_key = base + tasks_file

    s3 = boto3.client("s3", region_name=REGION)
    dms = boto3.client("dms", region_name=REGION)
    errors, warnings = [], []

    # --- shared settings: the file every per-task workflow reads, with its full check ---
    try:
        cfg = rt._load_settings(s3, bucket, SETTINGS_KEY, warnings)
    except rt.SettingsError as e:
        raise PreflightError(f"Shared settings problem, nothing was started: {e}")
    local_key = base + "pipeline.json"
    if local_key != SETTINGS_KEY:
        local = _get_text(s3, bucket, local_key)
        if local is not None:
            try:
                same = json.loads(local) == json.loads(_get_text(s3, bucket, SETTINGS_KEY))
            except ValueError:
                same = False
            if not same:
                raise PreflightError(
                    f"s3://{bucket}/{local_key} differs from s3://{bucket}/{SETTINGS_KEY}. Every "
                    f"task's workflow reads {SETTINGS_KEY}, so the fleet won't run with a different "
                    f"copy next to the task list. Either make {SETTINGS_KEY} the settings you want "
                    f"(keep a dated copy first: aws s3 cp s3://{bucket}/{SETTINGS_KEY} "
                    f"s3://{bucket}/{SETTINGS_KEY}.$(date +%Y%m%d%H%M)), or remove {local_key}. "
                    f"Nothing was started.")
    project = cfg["project"]

    # --- task list ---
    tasks_raw = _get_text(s3, bucket, tasks_key)
    if tasks_raw is None:
        raise PreflightError(f"Task list s3://{bucket}/{tasks_key} not found. Create a CSV with a "
                             f"header row and at least a 'task_arn' column.")
    reader = csv.DictReader(io.StringIO(tasks_raw))
    if "task_arn" not in (reader.fieldnames or []):
        raise PreflightError(f"s3://{bucket}/{tasks_key} has no 'task_arn' column "
                             f"(header row: {reader.fieldnames}).")
    rows = [r for r in reader if any(str(v or "").strip() for v in r.values())]
    if not rows:
        raise PreflightError(f"s3://{bucket}/{tasks_key} has a header but no task rows.")

    running = _running_tasks(event.get("stateMachineArn"), warnings)
    out_tasks, suffix_seen, arn_seen, all_schemas = [], {}, {}, set()
    stamp = time.strftime("%Y%m%d%H%M", time.gmtime())

    for i, row in enumerate(rows, start=1):
        where = f"row {i}"
        arn = str(row.get("task_arn") or "").strip()
        try:
            ids = rt._parse_task_arn(arn)
        except rt.TaskCheckError as e:
            errors.append(f"{where}: {e}"); continue
        if ids["region"] != cfg["region"]:
            errors.append(f"{where}: task is in {ids['region']}, but pipeline.json region is "
                          f"{cfg['region']!r}."); continue
        if arn in arn_seen:
            errors.append(f"{where}: task {arn} is already listed in row {arn_seen[arn]}."); continue
        arn_seen[arn] = i

        try:
            found = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": [arn]}],
                WithoutSettings=(mode != "startup")).get("ReplicationTasks", [])
        except Exception as e:
            errors.append(f"{where}: describe_replication_tasks failed for {arn}: {e}"); continue
        if len(found) != 1:
            errors.append(f"{where}: DMS task not found (check the ARN): {arn}"); continue
        task = found[0]

        override = str(row.get("task_suffix") or "").strip()
        adopt = str(row.get("adopt_existing_folder") or "").strip().lower() in _TRUE
        child_input = {"taskArn": arn}
        if override:
            child_input["taskSuffix"] = override
        if adopt and mode == "startup":
            child_input["adoptExistingFolder"] = True
        try:
            suffix, source, _rec, _ = rt._pick_suffix(s3, bucket, child_input, task, ids)
            rt._validate_suffix(suffix, project)
        except (rt.TaskCheckError, rt.FolderOwnerError) as e:
            errors.append(f"{where}: {e}"); continue
        if suffix in suffix_seen:
            errors.append(f"{where}: folder {suffix!r} is also used by row {suffix_seen[suffix]} "
                          f"(two tasks can't share a folder and job names; set a different "
                          f"'task_suffix')."); continue
        suffix_seen[suffix] = i

        prefix = f"config/_task/{suffix}/"
        marker = rt._get_json(s3, bucket, prefix + "_task.json")
        if marker and marker.get("taskArn") != arn:
            errors.append(f"{where}: s3://{bucket}/{prefix} belongs to DMS task "
                          f"{marker.get('taskArn')!r}, not this one (a deleted task's name reused?). "
                          f"Archive that folder or set a different 'task_suffix'."); continue

        skip = "already_running" if arn in running else None
        if mode == "startup":
            if not skip:
                try:
                    contract = rt._endpoint_contract(dms, task, arn)
                    rt._check_startup_task(task, contract, bucket, warnings)
                except Exception as e:
                    if marker and "already finished its full load" in str(e):
                        skip = "past_full_load"
                        warnings.append(f"{where}: {suffix} was started earlier and DMS is past its "
                                        f"full load; not started again.")
                    else:
                        errors.append(f"{where}: {e}"); continue
                if not marker and not adopt:
                    leftover = rt._has_run_artifacts(s3, bucket, prefix)
                    if leftover:
                        errors.append(f"{where}: s3://{bucket}/{prefix} already holds files from an "
                                      f"earlier run (e.g. {leftover}) but no owner record. If they "
                                      f"belong to this task, set adopt_existing_folder=true; "
                                      f"otherwise archive the folder."); continue
            manifest = _get_text(s3, bucket, prefix + "table_manifest.csv")
            if manifest is None:
                errors.append(f"{where}: no table list at s3://{bucket}/{prefix}table_manifest.csv "
                              f"(RUNBOOK Step 5b)."); continue
            schemas = _manifest_schemas(manifest)
            if not schemas:
                errors.append(f"{where}: s3://{bucket}/{prefix}table_manifest.csv lists no tables."); continue
            all_schemas |= schemas
        elif not marker:
            errors.append(f"{where}: no owner record s3://{bucket}/{prefix}_task.json, so this task "
                          f"was never started by the pipeline; there is nothing to cut over."); continue

        out_tasks.append({
            "taskArn": arn, "taskSuffix": suffix, "suffixSource": source,
            "skip": skip, "runningExecution": running.get(arn),
            "runName": f"{suffix[:60]}-{stamp}",
            "input": child_input,
        })

    if mode == "startup" and len(all_schemas) > MAX_DISTINCT_SCHEMAS:
        errors.append(
            f"The fleet's table lists load into {len(all_schemas)} distinct DSQL schemas "
            f"({sorted(all_schemas)}); DSQL allows 10 per database and cdc_control takes one, so at "
            f"most {MAX_DISTINCT_SCHEMAS}. Split the fleet or use fewer schemas.")
    if mode == "startup" and all_schemas:
        warnings.append(f"This fleet uses {len(all_schemas)} DSQL schema(s) {sorted(all_schemas)}. "
                        f"Schemas already in the DSQL database from other tasks also count toward "
                        f"its limit of 10 (preflight can't see them).")

    if errors:
        raise PreflightError(f"Fleet preflight found {len(errors)} problem(s); nothing was "
                             f"started:\n  - " + "\n  - ".join(errors))

    to_start = sum(1 for t in out_tasks if not t["skip"])
    for w in warnings:
        print(f"(warn) {w}")
    print(f"preflight ({mode}): {len(out_tasks)} task(s) OK, {to_start} to start, "
          f"{len(out_tasks) - to_start} skipped; schemas {sorted(all_schemas)}.")
    return {"ok": True, "count": len(out_tasks), "toStart": to_start,
            "skipped": len(out_tasks) - to_start, "tasks": out_tasks,
            "distinctSchemas": sorted(all_schemas), "warnings": warnings}
