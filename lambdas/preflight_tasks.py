"""
preflight-tasks Lambda (fleet-startup and fleet-cutover workflows).

The fleet workflows are started ONCE, by hand, to launch the per-task <project>-startup (or
<project>-cutover) workflow for many DMS tasks at once. This Lambda is their first step: it reads
the task list and checks EVERY task before a single one is launched. Any problem raises
PreflightError and the fleet stops at PreflightFailed: nothing is started (fail-closed).

It reuses resolve_task's own functions (same Lambda zip), so it can never pass a task that the
per-task workflow would refuse.

INPUTS  (all operator files live in the one fixed folder s3://<bucket>/config/)
  - s3://<bucket>/config/pipeline.json   the shared settings EVERY task's workflow reads. Checked
                                         with resolve_task's full settings check. Written only by
                                         the params.csv safe-publish below (same folder).
  - s3://<bucket>/config/params.csv      optional. If present, parsed and (when safe) published to
                                         config/pipeline.json. params.csv and pipeline.json share
                                         config/ by design, so there is no "second pipeline.json"
                                         ambiguity.
  - s3://<bucket>/config/<tasksFile, default fleet_tasks.csv>   the task list. Header row:
        task_arn               REQUIRED  arn:aws:dms:<region>:<acct>:task:<id>
        task_suffix            optional  folder/job-name override. Blank = what the pipeline
                                         would use anyway: the folder recorded for this task ARN
                                         (a renamed task keeps its original folder), else the
                                         DMS task name.
        adopt_existing_folder  optional  true/false (startup only; see resolve_task)
        override               optional  true/false (blank = false). true adds {"override":
                                         true} to this task's child input so a validation failure
                                         is accepted (startup) / a validation gate or startup
                                         override is accepted (cutover). The fleet start input's
                                         top-level {"override": true} turns it on for EVERY task.
        override_reason        optional  free-text note recorded with the override (per-row;
                                         the fleet-level "overrideReason" is the fallback).
                                         An unknown extra column is ignored (never rejected).

CHECKS, every row (all problems collected, then one error):
  both modes  task_arn is a DMS task ARN in the pipeline's region, listed once; the task exists;
              the folder name is legal and short enough for the Glue job names; no two rows use
              the same folder; the folder is not owned by a different task (_task.json).
  startup     resolve_task's pre-start checks on the task and its S3 endpoint
              (full-load-and-cdc, StopTaskCachedChangesApplied, AddColumnName, bucket, not past
              full load, ...); a folder with files from an earlier run but no owner record needs
              adopt_existing_folder. The table list is built automatically after full load (the
              per-task workflow's BuildTableList step), so nothing is uploaded; preflight only
              ESTIMATES the distinct DSQL schemas from each task's selection rules and flags the
              fleet-wide <=9 cap (the hard check runs per task in BuildTableList, before any Glue
              job). A wildcard-schema task makes the estimate partial -> warning, not failure.
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
import params_csv as pc       # same zip: the one params.csv parser / pipeline.json builder

REGION = os.environ.get("AWS_REGION", "us-east-1")
SETTINGS_KEY = rt.SETTINGS_KEY_DEFAULT          # config/pipeline.json
# Operator files (fleet_tasks.csv, params.csv) and the generated pipeline.json all live in this
# one fixed folder. inputPrefix was removed by design, so there is a single place to look.
BASE_PREFIX = "config/"
# The <=9 distinct-DSQL-schema cap lives in resolve_task (rt._MAX_DISTINCT_SCHEMAS): preflight's
# pre-start estimate and each task's authoritative BuildTableList check share the one constant.
_TRUE = {"true", "1", "yes", "y"}
_DEFAULT_PARAMS_FILE = "params.csv"


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


# ---------------------------------------------------------------------------------------------
# params.csv -> config/pipeline.json  (contract: a single parameters CSV, published once)
# ---------------------------------------------------------------------------------------------
def _project_from_state_machine_arn(state_machine_arn):
    """The project of the per-task state machine the fleet runs, from its ARN. The fleet passes
    stateMachineArn = arn:aws:states:<region>:<acct>:stateMachine:<project>-startup (or
    -cutover). Returns (project, name) or (None, None) if it can't be parsed."""
    name = str(state_machine_arn or "").rsplit(":", 1)[-1]
    for suffix in ("-startup", "-cutover"):
        if name.endswith(suffix):
            return name[: -len(suffix)], name
    return None, None


def _sibling_state_machine_arns(state_machine_arn, project):
    """ARNs of the four workflows whose RUNNING executions block a settings change, derived from
    the per-task state machine ARN by swapping its name: <project>-startup, -cutover,
    -fleet-startup, -fleet-cutover."""
    head, _, _name = str(state_machine_arn or "").rpartition(":")
    if not head:
        return {}
    return {n: f"{head}:{n}" for n in (f"{project}-startup", f"{project}-cutover",
                                       f"{project}-fleet-startup", f"{project}-fleet-cutover")}


def _running_executions(sfn, state_machine_arn, exclude_execution_id):
    """Names of RUNNING executions of one state machine, excluding exclude_execution_id (this
    fleet run). Raises on any API error so the caller can fail closed."""
    names, token, scanned = [], None, 0
    while True:
        kw = {"stateMachineArn": state_machine_arn, "statusFilter": "RUNNING", "maxResults": 100}
        if token:
            kw["nextToken"] = token
        page = sfn.list_executions(**kw)
        for ex in page.get("executions", []) or []:
            if exclude_execution_id and ex.get("executionArn") == exclude_execution_id:
                continue
            names.append(ex.get("name") or ex.get("executionArn"))
            scanned += 1
        token = page.get("nextToken")
        if not token or scanned >= 2000:
            break
    return names


def _any_pipeline_running(sfn, sfn_arns, this_execution_id):
    """{name: [running execution names]} for every sibling workflow with a RUNNING execution
    (excluding this fleet run). Raises if any listing fails (fail closed)."""
    busy = {}
    for name, arn in sfn_arns.items():
        running = _running_executions(sfn, arn, this_execution_id)
        if running:
            busy[name] = running
    return busy


def _handle_params_csv(s3, event, fleet_input, bucket, base, mode, warnings):
    """If params.csv exists next to the task list, turn it into config/pipeline.json per the
    contract and (when safe) publish it. Returns a dict merged into the preflight output
    (paramsPublished / backupKey / paramsReason), or None when there is no params.csv (then the
    behaviour is exactly as before). Raises PreflightError on any problem (fail closed)."""
    params_file = str(fleet_input.get("paramsFile") or event.get("paramsFile")
                      or _DEFAULT_PARAMS_FILE).strip().lstrip("/")
    params_key = base + params_file
    text = _get_text(s3, bucket, params_key)
    if text is None:
        return None     # D6: no params.csv -> behave exactly as today

    # D1: parse and validate; build the candidate pipeline.json.
    parsed = pc.parse(text)
    if parsed["errors"]:
        raise PreflightError(
            f"s3://{bucket}/{params_key} has {len(parsed['errors'])} problem(s); nothing was "
            f"started:\n  - " + "\n  - ".join(parsed["errors"]))
    for w in parsed["warnings"]:
        warnings.append(f"{params_key}: {w}")
    try:
        candidate = pc.to_pipeline_settings(parsed["params"])
    except pc.ParamsError as e:
        raise PreflightError(f"s3://{bucket}/{params_key}: {e}")

    # D1: the params project must match the project of the per-task workflow the fleet runs.
    sm_project, sm_name = _project_from_state_machine_arn(event.get("stateMachineArn"))
    if sm_project is None:
        raise PreflightError(
            f"could not read the per-task workflow's project from stateMachineArn "
            f"{event.get('stateMachineArn')!r}; expected a name ending in '-startup' or "
            f"'-cutover'. Nothing was started.")
    if candidate["project"] != sm_project:
        raise PreflightError(
            f"s3://{bucket}/{params_key} sets project={candidate['project']!r}, but this fleet "
            f"runs the {sm_name!r} workflow (project {sm_project!r}). They must match. Nothing "
            f"was started.")

    # params.csv and the generated pipeline.json share config/ by design (inputPrefix removed),
    # so there is no "second pipeline.json next to the task list" ambiguity to check any more.

    # D3: compare the candidate with the live canonical pipeline.json.
    live_text = _get_text(s3, bucket, SETTINGS_KEY)
    live = None
    if live_text is not None:
        try:
            live = json.loads(live_text)
        except ValueError as e:
            raise PreflightError(f"s3://{bucket}/{SETTINGS_KEY} is not valid JSON ({e}); fix or "
                                 f"remove it. Nothing was started.")
    if live == candidate:
        return {"paramsPublished": False, "backupKey": None,
                "paramsReason": f"{SETTINGS_KEY} already matches {params_key}; nothing written."}

    # D4: candidate differs from (or there is no) live pipeline.json.
    if mode == "cutover":
        raise PreflightError(
            f"s3://{bucket}/{params_key} would change s3://{bucket}/{SETTINGS_KEY}, but settings "
            f"are never changed at cutover. Publish the new settings with a startup fleet (or by "
            f"hand) first, then cut over. Nothing was started.")

    # startup: refuse to change settings while any task might be running.
    sm_arns = _sibling_state_machine_arns(event.get("stateMachineArn"), sm_project)
    if not sm_arns:
        raise PreflightError(
            f"could not derive the pipeline's state machine ARNs from "
            f"{event.get('stateMachineArn')!r}; cannot safely change settings. Nothing was "
            f"started.")
    this_exec = event.get("fleetExecutionId")
    try:
        sfn = boto3.client("stepfunctions", region_name=REGION)
        busy = _any_pipeline_running(sfn, sm_arns, this_exec)
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
                   or type(e).__name__)
        raise PreflightError(
            f"could not list running executions to safely change s3://{bucket}/{SETTINGS_KEY} "
            f"({code}). The preflight role needs states:ListExecutions on {sorted(sm_arns)}. "
            f"Refusing to change settings while this can't be checked. Nothing was started.")
    if busy:
        detail = "; ".join(f"{sm}: {', '.join(execs)}" for sm, execs in sorted(busy.items()))
        raise PreflightError(
            f"s3://{bucket}/{params_key} would change s3://{bucket}/{SETTINGS_KEY}, but these "
            f"runs are in progress and read it: {detail}. Settings are not changed while any "
            f"startup/cutover (or another fleet) is running. Wait for them to finish, then start "
            f"the fleet again. Nothing was started.")

    # None running: back up the live file (if any), publish, read back and verify.
    backup_key = None
    if live_text is not None:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        backup_key = f"{SETTINGS_KEY}.{stamp}"
        s3.put_object(Bucket=bucket, Key=backup_key,
                      Body=live_text.encode("utf-8"), ContentType="application/json")
    body = (json.dumps(candidate, indent=2) + "\n").encode("utf-8")
    s3.put_object(Bucket=bucket, Key=SETTINGS_KEY, Body=body, ContentType="application/json")
    readback = _get_text(s3, bucket, SETTINGS_KEY)
    try:
        ok = readback is not None and json.loads(readback) == candidate
    except ValueError:
        ok = False
    if not ok:
        raise PreflightError(
            f"wrote s3://{bucket}/{SETTINGS_KEY} from {params_key} but reading it back did not "
            f"match. Check the bucket and try again. Nothing was started.")
    reason = (f"published {params_key} to {SETTINGS_KEY}"
              + (f" (backed up live settings to {backup_key})" if backup_key
                 else " (no previous settings to back up)"))
    print(f"(info) {reason}")
    return {"paramsPublished": True, "backupKey": backup_key, "paramsReason": reason}


def handler(event, context):
    fleet_input = event.get("fleetInput") or {}
    bucket = event.get("bucket") or fleet_input.get("bucket")
    mode = str(event.get("mode") or "startup").strip().lower()
    if mode not in ("startup", "cutover"):
        raise PreflightError(f"mode must be 'startup' or 'cutover' (got {mode!r}).")
    # Fleet-wide runtime override: top-level {"override": true} in the fleet start input turns
    # override on for EVERY task's child execution (a per-task CSV column can also turn it on for
    # one task). Normalized the same way the per-task workflow does.
    fleet_override, fleet_reason = rt._normalize_override(fleet_input)
    s3 = boto3.client("s3", region_name=REGION)
    dms = boto3.client("dms", region_name=REGION)
    errors, warnings = [], []

    tasks_file = str(fleet_input.get("tasksFile") or event.get("tasksFile") or "fleet_tasks.csv").strip().lstrip("/")
    # Operator files live in ONE fixed folder: s3://<bucket>/config/ (config/params.csv,
    # config/fleet_tasks.csv, config/pipeline.json). inputPrefix was removed. If an old caller
    # still passes one, ignore it with a single warning (don't fail) and read from config/.
    stray_prefix = str(event.get("inputPrefix") or fleet_input.get("inputPrefix") or "").strip()
    if stray_prefix:
        warnings.append(f"inputPrefix={stray_prefix!r} was passed but is no longer used; operator "
                        f"files are always read from config/. Ignoring it.")
    base = BASE_PREFIX
    tasks_key = base + tasks_file

    # --- params.csv (optional): build and, when safe, publish config/pipeline.json from it ---
    # Done BEFORE the settings are loaded, so the checks below run against the published file.
    # Returns None when there is no params.csv (then everything behaves exactly as before).
    params_result = _handle_params_csv(s3, event, fleet_input, bucket, base, mode, warnings)

    # --- shared settings: the file every per-task workflow reads, with its full check ---
    try:
        cfg = rt._load_settings(s3, bucket, SETTINGS_KEY, warnings)
    except rt.SettingsError as e:
        raise PreflightError(f"Shared settings problem, nothing was started: {e}")
    # No local-vs-canonical pipeline.json guard needed: with inputPrefix removed the only
    # pipeline.json location is config/pipeline.json (== SETTINGS_KEY), the file every per-task
    # workflow reads and the one params.csv publishes to.
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
    schema_estimate_unknown = False   # a wildcard-schema task made the pre-start estimate partial
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
        # Runtime override: a per-task CSV "override" column (blank = false) OR the fleet-wide
        # top-level {"override": true} turns override on for this task's child execution. The
        # child workflow re-normalizes it (resolve_task._normalize_override), so passing the
        # boolean true here matches a by-hand start. An optional "overrideReason" (per-row column
        # or fleet-level) is passed through unchanged. Blank/absent -> nothing added, so the
        # default (no override) child input is byte-identical to before.
        row_override, row_reason = rt._normalize_override(
            {"override": row.get("override"), "overrideReason": row.get("override_reason")})
        if fleet_override or row_override:
            child_input["override"] = True
            _reason = row_reason or fleet_reason
            if _reason:
                child_input["overrideReason"] = _reason
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
            # The table list is built automatically after full load (state BuildTableList in the
            # per-task startup workflow), so there is nothing to upload and preflight no longer
            # requires a table_manifest.csv. Estimate the distinct DSQL schemas from the task's
            # TableMappings selection rules so the fleet-wide <=9 cap can still be flagged before
            # launch; the authoritative check runs in BuildTableList (before any Glue job).
            try:
                task_rules = rt._parse_table_mappings(task, arn)
                est_schemas, est_wildcard = rt._estimate_selection_schemas(task_rules)
            except rt.TableListError as e:
                errors.append(f"{where}: {e}"); continue
            all_schemas |= est_schemas
            if est_wildcard:
                schema_estimate_unknown = True
                warnings.append(f"{where}: the task's selection rules use a wildcard schema, so "
                                f"the distinct-DSQL-schema count can't be known before full load; "
                                f"the <=9 cap is enforced in BuildTableList.")
        elif not marker:
            errors.append(f"{where}: no owner record s3://{bucket}/{prefix}_task.json, so this task "
                          f"was never started by the pipeline; there is nothing to cut over."); continue

        out_tasks.append({
            "taskArn": arn, "taskSuffix": suffix, "suffixSource": source,
            "skip": skip, "runningExecution": running.get(arn),
            "runName": f"{suffix[:60]}-{stamp}",
            "input": child_input,
        })

    # Pre-start estimate only: the authoritative distinct-DSQL-schema count comes from the real
    # table statistics in each task's BuildTableList step (which hard-fails before any Glue job).
    # Here we can only count the EXPLICIT (non-wildcard) schema names from selection rules, so we
    # fail only when even that lower bound already exceeds the cap (certainly too many); when a
    # wildcard schema made the count unknown we warn and let BuildTableList enforce it.
    if mode == "startup" and len(all_schemas) > rt._MAX_DISTINCT_SCHEMAS:
        errors.append(
            f"The fleet's tasks name {len(all_schemas)} distinct DSQL schemas in their selection "
            f"rules ({sorted(all_schemas)}); DSQL allows 10 per database and cdc_control takes one, "
            f"so at most {rt._MAX_DISTINCT_SCHEMAS}. Use fewer schemas (change the tasks' selection "
            f"rules) or split the fleet.")
    if mode == "startup" and (all_schemas or schema_estimate_unknown):
        extra = (" plus one or more wildcard-schema tasks whose schemas can't be counted until "
                 "full load" if schema_estimate_unknown else "")
        warnings.append(f"This fleet names {len(all_schemas)} explicit DSQL schema(s) "
                        f"{sorted(all_schemas)}{extra}; the <=9 cap is enforced per task in "
                        f"BuildTableList after full load. Schemas already in the DSQL database "
                        f"from other tasks also count toward its limit of 10 (preflight can't see "
                        f"them).")

    if errors:
        raise PreflightError(f"Fleet preflight found {len(errors)} problem(s); nothing was "
                             f"started:\n  - " + "\n  - ".join(errors))

    to_start = sum(1 for t in out_tasks if not t["skip"])
    for w in warnings:
        print(f"(warn) {w}")
    print(f"preflight ({mode}): {len(out_tasks)} task(s) OK, {to_start} to start, "
          f"{len(out_tasks) - to_start} skipped; schemas {sorted(all_schemas)}.")
    out = {"ok": True, "count": len(out_tasks), "toStart": to_start,
           "skipped": len(out_tasks) - to_start, "tasks": out_tasks,
           "distinctSchemas": sorted(all_schemas), "warnings": warnings,
           "paramsPublished": False, "backupKey": None, "paramsReason": None}
    if params_result:
        out.update(params_result)
    return out
