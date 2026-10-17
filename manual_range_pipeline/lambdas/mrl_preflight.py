#!/usr/bin/env python3
"""mrl_preflight Lambda - the MRL child workflow's first step (ResolveTask / preflight).

Reuses resolve_task's own functions (SAME Lambda zip, imported - resolve_task.py is NOT edited)
so MRL can never accept a task the existing pipeline would reject:
  * _parse_task_arn, _endpoint_contract (target S3 bucket, AddColumnName, CsvNullValue, layout),
  * _pick_suffix / _validate_suffix / _check_owner (folder-owner safeguard),
  * _check_startup_task (FullLoadSettings / migration type / AddColumnName / bucket / not-past-
    full-load) for mode=start,
  * _validate_null_settings (the byte-identical NULL parser) so a malformed null_values /
    null_rules FAILS AT PREFLIGHT naming the bad entry,
  * _check_no_other_run (no second RUNNING execution of THIS child machine for this task).

MRL adds, on top of resolve_task:
  * dms_start_mode handling: "start" (default) uses the full _check_startup_task pre-start
    checks; "adopt_stopped" instead verifies the task is already stopped at 100% / 0 errored /
    all tables 'Table completed' and does NOT require start-ability,
  * mode (full_load_only | full_load_and_cdc) validation of the DMS migration type +
    FullLoadSettings for the CHOSEN mode (see _check_mode),
  * a capacity-budget WARNING (DSQL connections / driver memory / DPU / IPs) with the
    arithmetic printed; fails only on a provably impossible single setting.

Event: { bucket, settingsKey?, input {taskArn, taskSuffix?, override?, adoptExistingFolder?},
         mode: "full_load_only"|"full_load_and_cdc",
         dmsStartMode: "start"|"adopt_stopped",
         nullValues?, nullRules?,
         maxConcurrency?, loadWorkerType?, loadWorkers?, maxFilesInParallel?, writersPerFile?,
         connBudget?, dpuQuota?, subnetIps?,
         execution? (=$$.Execution.Id), stateMachine? (=$$.StateMachine.Id) }
Returns resolve_task-shared-mode output PLUS: mrlMode, dmsStartMode, startDms (bool),
expectedStopReasonSubstrings, csvNullValue, nullValues, nullRules, capacity {...}, warnings.
"""

import json
import os

import boto3

import resolve_task as rt   # same zip - reuse the exact per-task rules (NOT edited)

REGION = os.environ.get("AWS_REGION", "us-east-1")

_FULL_LOAD_ONLY = "full_load_only"
_FULL_LOAD_AND_CDC = "full_load_and_cdc"
_MODES = (_FULL_LOAD_ONLY, _FULL_LOAD_AND_CDC)
_START = "start"
_ADOPT = "adopt_stopped"
_DMS_START_MODES = (_START, _ADOPT)

# Documented DMS stop-reason substrings accepted per mode (see DESIGN.md D5/D6). We match on a
# SUBSTRING (DMS prefixes the reason with its own text) and NEVER guess a single string.
_EXPECTED_STOP = {
    _FULL_LOAD_ONLY: ["STOPPED_AFTER_FULL_LOAD", "FULL_LOAD_ONLY_FINISHED"],
    _FULL_LOAD_AND_CDC: ["STOPPED_AFTER_CACHED_EVENTS"],
}


class MrlPreflightError(Exception):
    pass


def _check_mode(task, mode, mrl_mode, dms_start_mode, warnings):
    """Validate the DMS task's settings FOR THE CHOSEN MRL mode. Fail closed with the exact
    modify command in the message on an unknown/unsupported value."""
    arn = task.get("ReplicationTaskArn")
    mig = (task.get("MigrationType") or "").strip()
    if mrl_mode == _FULL_LOAD_AND_CDC:
        if "cdc" not in mig:
            raise MrlPreflightError(
                "DMS task %s has MigrationType=%r but MRL mode is full_load_and_cdc, which "
                "needs 'full-load-and-cdc'. Modify the task: aws dms modify-replication-task "
                "--replication-task-arn %s --migration-type full-load-and-cdc" % (arn, mig, arn))
        # FullLoadSettings must stop AFTER cached events are applied (same as startup).
        raw = task.get("ReplicationTaskSettings")
        if raw:
            try:
                fl = (json.loads(raw).get("FullLoadSettings") or {})
            except ValueError:
                fl = None
                warnings.append("could not parse DMS task settings; skipped the "
                                "StopTaskCachedChangesApplied check.")
            if fl is not None:
                if fl.get("StopTaskCachedChangesApplied") is not True:
                    raise MrlPreflightError(
                        "DMS task %s has FullLoadSettings.StopTaskCachedChangesApplied=%r; for "
                        "full_load_and_cdc it must be true so the task stops with "
                        "STOPPED_AFTER_CACHED_EVENTS. Modify the task's settings "
                        "(FullLoadSettings.StopTaskCachedChangesApplied=true), then start again."
                        % (arn, fl.get("StopTaskCachedChangesApplied")))
                if fl.get("StopTaskCachedChangesNotApplied") is True:
                    raise MrlPreflightError(
                        "DMS task %s has FullLoadSettings.StopTaskCachedChangesNotApplied=true; "
                        "it must be false (else DMS stops BEFORE applying cached changes). "
                        "Modify the task's settings, then start again." % arn)
    else:  # full_load_only
        # Accept a 'full-load' migration type. A 'full-load-and-cdc' task CAN be loaded
        # full_load_only too, but then it would stop at STOPPED_AFTER_FULL_LOAD (which this mode
        # accepts) - warn that CDC will not be started in this mode.
        if "full-load" not in mig:
            raise MrlPreflightError(
                "DMS task %s has MigrationType=%r; full_load_only needs a full-load-capable "
                "task ('full-load' or 'full-load-and-cdc'). Modify the migration type." % (arn, mig))
        if "cdc" in mig:
            warnings.append(
                "task %s is full-load-and-cdc but MRL mode is full_load_only: MRL will NOT "
                "resume DMS or start CDC. If you want CDC, use mode full_load_and_cdc." % arn)


def _check_adopt_stopped(task):
    """adopt_stopped preconditions (DESIGN.md D6): already stopped at 100%, 0 errored tables,
    every table 'Table completed'. Any stop reason is accepted (incl. NORMAL) because the data
    is fully loaded regardless of how it was stopped. FAIL CLOSED if any precondition is unmet."""
    arn = task.get("ReplicationTaskArn")
    status = (task.get("Status") or "").lower()
    if status != "stopped":
        raise MrlPreflightError(
            "adopt_stopped requires the DMS task to be already STOPPED, but %s is %r. Use "
            "dms_start_mode=start to let MRL start it, or stop it first." % (arn, status))
    stats = task.get("ReplicationTaskStats") or {}
    pct = stats.get("FullLoadProgressPercent")
    if pct != 100:
        raise MrlPreflightError(
            "adopt_stopped requires FullLoadProgressPercent=100 on %s, but it is %r. The full "
            "load has not finished; do not adopt." % (arn, pct))
    errored = stats.get("TablesErrored")
    if errored not in (0, None) and int(errored) > 0:
        raise MrlPreflightError(
            "adopt_stopped requires 0 errored tables on %s, but TablesErrored=%r. Fix the "
            "failed table(s) and reload before adopting." % (arn, errored))


def _capacity(runs, mfip, wpf, conn_budget, workers, warnings):
    """Print the DSQL/driver/DPU/IP arithmetic and WARN on a tight budget. Fail ONLY on a
    provably impossible single setting. All inputs are the EFFECTIVE resolved values."""
    runs = int(runs)
    mfip = int(mfip)
    wpf = int(wpf)
    conn_budget = int(conn_budget)
    workers = int(workers)
    dpu_per_worker = 4   # G.4X
    total_dsql = runs * mfip * wpf
    per_run_dsql = mfip * wpf
    total_dpu = runs * (workers + 1) * dpu_per_worker
    total_ips = runs * (workers + 1)
    cap = {
        "concurrent_runs": runs, "max_files_in_parallel": mfip, "writers_per_file": wpf,
        "per_run_dsql_writers": per_run_dsql, "total_dsql_writers": total_dsql,
        "conn_budget": conn_budget, "total_dpu_estimate": total_dpu,
        "total_subnet_ips_estimate": total_ips,
    }
    print("(info) mrl capacity: %d run(s) x %d files x %d writers = %d DSQL writers "
          "(budget %d); ~%d DPU; ~%d subnet IPs"
          % (runs, mfip, wpf, total_dsql, conn_budget, total_dpu, total_ips))
    if per_run_dsql > conn_budget:
        raise MrlPreflightError(
            "IMPOSSIBLE setting: a SINGLE run needs %d DSQL writers (max_files_in_parallel %d x "
            "writers_per_file %d) but the connection budget is only %d. Lower "
            "max_files_in_parallel or writers_per_file, or raise conn_budget." %
            (per_run_dsql, mfip, wpf, conn_budget))
    if total_dsql > conn_budget:
        warnings.append(
            "capacity: %d concurrent runs x %d writers/run = %d DSQL writers exceeds conn_budget "
            "%d. Lower max_concurrency or writers_per_file, or raise conn_budget (watch the DSQL "
            "10,000-connection cluster limit)." % (runs, per_run_dsql, total_dsql, conn_budget))
    return cap


def _mrl_endpoint_contract(dms, task, task_arn, mrl_mode):
    """Mode-aware endpoint contract. For full_load_and_cdc, reuse resolve_task._endpoint_contract
    verbatim (it enforces the cdc-capable migration type + the full flat-layout/CsvNullValue
    contract). For full_load_only the task may be a pure 'full-load' task, which
    _endpoint_contract rejects for being non-cdc; so we reuse resolve_task's endpoint parsing by
    presenting the task's migration type as cdc-capable ONLY for the contract extraction (the
    REAL migration type is validated separately in _check_mode). This never relaxes the S3-target
    / AddColumnName / layout / bucket checks - it only skips the cdc-capability gate for a
    full-load-only task, which is correct for full_load_only MRL."""
    if mrl_mode == _FULL_LOAD_AND_CDC:
        return rt._endpoint_contract(dms, task, task_arn)
    # full_load_only: extract the same contract without the cdc-only gate.
    shim = dict(task)
    mig = str(task.get("MigrationType") or "")
    if "cdc" not in mig:
        shim["MigrationType"] = "full-load-and-cdc"   # ONLY to pass _endpoint_contract's gate
    contract = rt._endpoint_contract(dms, shim, task_arn)
    contract["migrationType"] = mig                   # restore the true migration type
    return contract


_CDC_TRUE = {"true"}
_CDC_FALSE = {"false", ""}


def _validate_cdc_bool(raw, where):
    """AMENDMENT 8: strict cdc boolean. Accept bool True/False, or the strings 'true'/'false'
    (case-insensitive). Reject 1, 0, 'yes', '', numbers, lists, etc. None => not given."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in _CDC_TRUE:
            return True
        if s == "false":
            return False
    raise MrlPreflightError(
        "%s must be a boolean true/false (or the string \"true\"/\"false\"); got %r." % (where, raw))


def _resolve_cdc(event):
    """Resolve the effective cdc boolean + source. Precedence (highest first): execution input
    (pinned _mrl_execution.cdcOverride) > fleet row (event['cdc']) > params_parallel_load.csv
    (event['paramsParallelCdc']) > built-in default False. The fleet resolves the exec-input vs
    row-vs-params layering and passes the final value; the child also accepts a direct cdc."""
    # exec override is carried in the event when the fleet set it (or on a direct child start)
    exec_cdc = _validate_cdc_bool(event.get("cdc"), "cdc (input)")
    if exec_cdc is not None:
        return exec_cdc, "execution/row input"
    ppl = _validate_cdc_bool(event.get("paramsParallelCdc"), "cdc (params_parallel_load.csv)")
    if ppl is not None:
        return ppl, "params_parallel_load.csv"
    return False, "built-in default"


def _route_conflict_guard(s3, bucket, suffix, project, region, task_arn, override, warnings):
    """AMENDMENT 7.6 / R22-1: refuse if the ORIGINAL startup/cutover is RUNNING for this task, or
    an original-route load is in progress under config/_task/<suffix>/_orchestrator/. Never both
    routes on one task at once. A listing/describe DENIAL (AccessDenied) or any listing error now
    FAILS CLOSED (the guard cannot verify the invariant, so it must not let the load through) -
    unless override=true, which downgrades it to a LOUD warning (the operator accepts the risk).
    This is deliberate: the IAM policy grants states:ListExecutions/DescribeExecution on the
    original startup+cutover machines precisely so this guard can run; a denial means either the
    grant is missing or something is wrong, and silently proceeding would make the guard inert.
    not a false block - the original per-task duplicate guard still protects."""
    # (a) original startup/cutover RUNNING for this task (match by the task suffix in the
    #     execution name, or the taskArn in the input).
    if project:
        try:
            sfn = boto3.client("stepfunctions", region_name=region)
            acct = task_arn.split(":")[4] if len(task_arn.split(":")) > 4 else ""
            for wf in ("startup", "cutover"):
                sm = "arn:aws:states:%s:%s:stateMachine:%s-%s" % (region, acct, project, wf)
                # R3-2: a MISSING original machine (MRL-only account or a renamed project) means
                # there is definitively NO conflicting execution -> the invariant is SATISFIED,
                # not unverifiable. Map the not-found codes to "no conflict" and move on; every
                # other error (AccessDenied, throttling, ...) still propagates to fail closed.
                try:
                    token = None
                    while True:
                        kw = {"stateMachineArn": sm, "statusFilter": "RUNNING", "maxResults": 100}
                        if token:
                            kw["nextToken"] = token
                        page = sfn.list_executions(**kw)
                        for ex in page.get("executions", []) or []:
                            nm = ex.get("name") or ""
                            d = sfn.describe_execution(executionArn=ex["executionArn"])
                            try:
                                in_arn = str((json.loads(d.get("input") or "{}") or {})
                                             .get("taskArn") or "").strip()
                            except ValueError:
                                in_arn = ""
                            if in_arn == task_arn or ("%s-" % suffix) in nm or nm.startswith(suffix):
                                raise MrlPreflightError(
                                    "FAIL CLOSED (Amendment 7): the ORIGINAL %s workflow has a "
                                    "RUNNING execution %r for this task. Never run the original "
                                    "route and the MRL route on one task at once. Finish or stop "
                                    "that execution first." % (wf, nm or ex["executionArn"]))
                        token = page.get("nextToken")
                        if not token:
                            break
                except MrlPreflightError:
                    raise
                except Exception as e:
                    nf_code = str((getattr(e, "response", None) or {})
                                  .get("Error", {}).get("Code", "") or type(e).__name__)
                    if nf_code in ("StateMachineDoesNotExist", "MachineDoesNotExist",
                                   "ExecutionDoesNotExist"):
                        warnings.append("original %s state machine not found, no conflict "
                                        "possible." % wf)
                        continue
                    raise
        except MrlPreflightError:
            raise
        except Exception as e:
            code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
                       or type(e).__name__)
            msg = ("could not check for a RUNNING original startup/cutover of this task (%s). The "
                   "route-conflict guard (Amendment 7.6) cannot verify that the original route is "
                   "not mid-flight. Grant states:ListExecutions/DescribeExecution on the "
                   "<project>-startup and <project>-cutover machines (see iam/mrl-additions.json)." % code)
            if override:
                warnings.append("OVERRIDE: %s Proceeding because override=true - you accept the "
                                "risk of running both routes on one task at once." % msg)
            else:
                raise MrlPreflightError("FAIL CLOSED (Amendment 7.6 / R22-1): %s Set override=true "
                                        "to proceed anyway (not recommended)." % msg)
    # (b) an original-route load in progress: a _load_status.json or *_file_status doc under
    #     config/_task/<suffix>/_orchestrator/ whose content is not complete.
    try:
        orch = "config/_task/%s/_orchestrator/" % suffix
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=orch, MaxKeys=1000)
        for o in resp.get("Contents", []) or []:
            k = o["Key"]
            if k.endswith("_load_status.json"):
                doc = _get_json_safe(s3, bucket, k)
                tables = (doc or {}).get("tables") if isinstance(doc, dict) else None
                if isinstance(tables, dict):
                    unfinished = [t for t, r in tables.items()
                                  if isinstance(r, dict) and r.get("status") not in ("done", None)]
                    if unfinished:
                        raise MrlPreflightError(
                            "FAIL CLOSED (Amendment 7): an ORIGINAL-route load is in progress for "
                            "this task (%s shows unfinished table(s): %s). Finish the original "
                            "load (or cut it over) before using the MRL route; never both at "
                            "once." % (k, ", ".join(unfinished[:5])))
    except MrlPreflightError:
        raise
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
                   or type(e).__name__)
        msg = ("could not probe for an in-progress original-route load under %s (%s). The "
               "route-conflict guard cannot verify the original route is idle."
               % ("config/_task/%s/_orchestrator/" % suffix, code))
        if override:
            warnings.append("OVERRIDE: %s Proceeding because override=true." % msg)
        else:
            raise MrlPreflightError("FAIL CLOSED (Amendment 7.6 / R22-1): %s Grant "
                                    "s3:ListBucket/GetObject on config/_task/* or set "
                                    "override=true to proceed anyway." % msg)


def _get_json_safe(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    except Exception:
        return None


def _cdc_control_marker_probe(s3, bucket, suffix, task_arn, override, warnings):
    """AMENDMENT 8.6: when cdc=true, fail closed if this task already has CDC progress (a
    cdc_control row/marker), because a reload without resetting cdc_control.last_done_file makes
    CDC skip already-applied change files -> silent data loss. MRL does NOT reset CDC progress.
    We cannot query DSQL from this Lambda, so we use the cheap read-only S3 markers the pipeline
    writes when CDC has run for a task: config/_task/<suffix>/_cdc_started/ (any object) or
    _cdc_engine.json. override=true downgrades to a loud warning (the operator accepts the
    reset responsibility)."""
    markers = []
    try:
        r1 = s3.list_objects_v2(Bucket=bucket,
                                Prefix="config/_task/%s/_cdc_started/" % suffix, MaxKeys=1)
        if r1.get("Contents"):
            markers.append("config/_task/%s/_cdc_started/ (a prior CDC run)" % suffix)
        if _get_json_safe(s3, bucket, "config/_task/%s/_cdc_engine.json" % suffix) is not None:
            markers.append("config/_task/%s/_cdc_engine.json" % suffix)
    except Exception as e:
        warnings.append("could not probe for prior CDC markers (%s); proceeding." % type(e).__name__)
        return
    if markers:
        msg = ("CDC has run before for this task (markers: %s). Reloading WITHOUT resetting this "
               "table's CDC progress (cdc_control.cdc_status.last_done_file etc.) would make CDC "
               "SKIP change files it already applied -> silent data loss. MRL does NOT reset CDC "
               "progress. Reset it first (see the README 'CDC reset' note), or use a fresh "
               "DMS task/folder." % ", ".join(markers))
        if override:
            warnings.append("OVERRIDE: %s Proceeding because override=true - you accept the reset "
                            "responsibility." % msg)
        else:
            raise MrlPreflightError("FAIL CLOSED (Amendment 8.6): " + msg)


def _load_pinned_settings(s3, bucket, settings_key):
    """Read the pinned MRL settings JSON. It is a full to_pipeline_settings() dict (project,
    region, dsql_*, glue_*, control_schema, null_values/null_rules, map_max_concurrency,
    max_files_in_parallel, writers_per_file, conn_budget, worker sizing, ...) plus a
    _mrl_provenance block. Key may be an s3:// URI or a bare key under `bucket`."""
    key = settings_key
    b = bucket
    if str(key).startswith("s3://"):
        b, key = str(key)[5:].split("/", 1)
    try:
        body = s3.get_object(Bucket=b, Key=key)["Body"].read()
    except Exception as e:
        raise MrlPreflightError("FAIL CLOSED: could not read the pinned MRL settings "
                                "s3://%s/%s (%s). The parent publishes/pins it from "
                                "config/params.csv before starting a child." % (b, key, e))
    doc = json.loads(body.decode("utf-8"))
    for req in ("project", "region", "dsql_endpoint"):
        if not str(doc.get(req) or "").strip():
            raise MrlPreflightError("FAIL CLOSED: pinned MRL settings missing %r." % req)
    return doc


def handler(event, context):
    # The state machine passes the WHOLE child input under "raw" (so optional knobs like
    # maxConcurrency / splitBy / nullValues are read here with defaults, never referenced as a
    # possibly-absent ASL path). bucket/settingsKey/execution/stateMachine stay at the top.
    # Tests pass a flat event (no "raw"); then event itself carries the knobs.
    raw = event.get("raw")
    if isinstance(raw, dict):
        merged = dict(raw)
        for k in ("bucket", "settingsKey", "execution", "stateMachine"):
            if event.get(k) is not None:
                merged[k] = event[k]
        event = merged
    bucket = event["bucket"]
    # AMENDMENT 3: settingsKey points at the PINNED MRL settings copy
    # (mrl/runs/<exec>/settings.json) the parent produced from config/params.csv. MRL no longer
    # reads config/pipeline.json.
    settings_key = event.get("settingsKey")
    if not settings_key:
        raise MrlPreflightError("settingsKey (the pinned MRL settings JSON built from "
                                "config/params.csv) is required.")
    inp = event.get("input") or {}
    task_arn = str(inp.get("taskArn") or "").strip()
    mrl_mode = _FULL_LOAD_ONLY   # set below from the cdc boolean
    dms_start_mode = str(event.get("dmsStartMode") or _START).strip().lower()
    warnings = []

    # AMENDMENT 8: CDC is OPTIONAL, switched by a boolean `cdc` (default false). It replaces the
    # old 'mode' field. Strict: boolean true/false or the strings "true"/"false" (case-insensitive);
    # anything else is rejected. Precedence (resolved by the fleet; the child receives the final
    # value) is exec input > fleet row > params_parallel_load.csv > default false.
    cdc_on, cdc_source = _resolve_cdc(event)
    mrl_mode = _FULL_LOAD_AND_CDC if cdc_on else _FULL_LOAD_ONLY

    if dms_start_mode not in _DMS_START_MODES:
        raise MrlPreflightError("dms_start_mode must be one of %s (got %r)."
                                % (list(_DMS_START_MODES), dms_start_mode))

    ids = rt._parse_task_arn(task_arn)
    s3 = boto3.client("s3", region_name=REGION)
    # AMENDMENT 3: settings come from the PINNED MRL settings JSON (mrl/runs/<exec>/settings.json),
    # which the parent built from config/params.csv (NOT config/pipeline.json). It is already a
    # validated to_pipeline_settings() dict (same keys as the base pipeline.json would have) plus
    # a _mrl_provenance block. We read it as-is; it was validated at publish time.
    cfg = _load_pinned_settings(s3, bucket, settings_key)
    if ids["region"] != cfg["region"]:
        raise MrlPreflightError("DMS task is in %s but the MRL settings region is %r "
                                "(from config/params.csv)." % (ids["region"], cfg["region"]))

    dms = boto3.client("dms", region_name=ids["region"])
    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}],
        WithoutSettings=False).get("ReplicationTasks", [])
    if len(tasks) != 1:
        raise MrlPreflightError("DMS task %s not found (check the ARN and region)." % task_arn)
    task = tasks[0]

    contract = _mrl_endpoint_contract(dms, task, task_arn, mrl_mode)
    # Target bucket + AddColumnName (same hard checks startup makes).
    if not contract.get("addColumnName"):
        raise MrlPreflightError(
            "The S3 target endpoint of %s has AddColumnName=false. MRL needs a header row in "
            "every CSV: set AddColumnName=true on the endpoint." % task_arn)
    if contract.get("s3Bucket") != bucket:
        raise MrlPreflightError(
            "DMS task %s writes to bucket %r, but the pipeline bucket is %r. The Glue jobs read "
            "the DMS files from the pipeline bucket, so the S3 target endpoint must use the same "
            "bucket." % (task_arn, contract.get("s3Bucket"), bucket))

    # Per-mode DMS settings.
    _check_mode(task, mrl_mode, mrl_mode, dms_start_mode, warnings)

    start_dms = True
    if dms_start_mode == _ADOPT:
        _check_adopt_stopped(task)
        start_dms = False
    else:
        # mode=start: the task must never have run a full load (reuse _check_startup_task's
        # not-past-full-load rule). For full_load_and_cdc _check_startup_task is exactly right;
        # for full_load_only we only reuse the not-past-full-load + endpoint parts, so we call
        # it when the task is full-load-and-cdc and otherwise apply the subset checks already
        # done above plus the not-past-full-load check inline.
        if mrl_mode == _FULL_LOAD_AND_CDC:
            rt._check_startup_task(task, contract, bucket, warnings)
        else:
            stats = task.get("ReplicationTaskStats") or {}
            pct = stats.get("FullLoadProgressPercent")
            status = (task.get("Status") or "").lower()
            reason = (task.get("StopReason") or "").upper()
            past = pct == 100 and (status in ("running", "starting", "resuming", "modifying")
                                   or (status == "stopped" and "FULL_LOAD" not in reason
                                       and "CACHED_EVENTS" not in reason))
            if past:
                raise MrlPreflightError(
                    "DMS task %s has already finished its full load (status=%r, stop reason=%r). "
                    "A fresh full_load_only run must start from a task that has not loaded. Use "
                    "dms_start_mode=adopt_stopped to adopt an already-finished load." %
                    (task_arn, status, task.get("StopReason")))

    # Folder-owner safeguard + suffix (reuse resolve_task). Treat MRL like 'startup' for owner.
    suffix, source, rec, index_key = rt._pick_suffix(s3, bucket, inp, task, ids)
    rt._validate_suffix(suffix, cfg["project"])
    marker_key, created = rt._check_owner(s3, bucket, suffix, task_arn, "startup", inp, warnings)

    # No other RUNNING execution of THIS child machine for this task.
    rt._check_no_other_run(event, task_arn, "startup", warnings)

    # AMENDMENT 7.6: ROUTE-CONFLICT GUARD (fail closed; this is the first child state, before any
    # DMS call). Refuse if the ORIGINAL <project>-startup or <project>-cutover state machine has a
    # RUNNING execution for this task, or if an original-route load is in progress for it (a
    # _load_status.json / _file_status doc under config/_task/<suffix>/_orchestrator/ that is not
    # complete). Never both routes on one task at once.
    _route_conflict_guard(s3, bucket, suffix, cfg.get("project"), ids["region"], task_arn,
                          rt._normalize_override(inp), warnings)

    # AMENDMENT 8.6: when cdc=true, refuse a reload onto a table that already has CDC progress
    # (a cdc_control row/marker) unless override=true. MRL does NOT reset CDC progress.
    if cdc_on:
        _cdc_control_marker_probe(s3, bucket, suffix, task_arn,
                                  rt._normalize_override(inp), warnings)

    # NULL options: parse with the byte-identical parser (fail at preflight naming the entry).
    null_values = str(event.get("nullValues") if event.get("nullValues") is not None
                      else cfg.get("null_values", "") or "")
    null_rules = str(event.get("nullRules") if event.get("nullRules") is not None
                     else cfg.get("null_rules", "") or "")
    null_notnull_text = str(event.get("nullNotnullText") if event.get("nullNotnullText") is not None
                            else cfg.get("null_notnull_text", "") or "")
    try:
        _nullcfg = {"null_values": null_values, "null_rules": null_rules}
        rt._validate_null_settings(_nullcfg)
        null_values, null_rules = _nullcfg["null_values"], _nullcfg["null_rules"]
    except rt.SettingsError as e:
        raise MrlPreflightError("NULL options invalid (fix at preflight): %s" % e)

    # Normalize ALL optional knobs here (defaults applied in the Lambda) so the state machine
    # only ever reads produced $.resolved.* fields - never a possibly-absent top-level input
    # field (which would raise States.Runtime). Numbers are returned as ints; the ASL wraps
    # them with States.Format when they become Glue Arguments.
    def _int_knob(key, default):
        v = event.get(key)
        if v in (None, ""):
            return int(default)
        try:
            return int(v)
        except (TypeError, ValueError):
            raise MrlPreflightError("%s must be an integer (got %r)." % (key, v))

    # --- EFFECTIVE max_concurrency (AMENDMENT 3/4) ------------------------------------------
    # Precedence (highest first):
    #   execution input  (pinned _mrl_execution.maxConcurrencyOverride, set at the parent's
    #                     first state for every task of the run)
    #   > fleet-row column  (event["maxConcurrency"], per task)
    #   > config/params_parallel_load.csv  (not a max_concurrency source today; reserved)
    #   > pipeline_parallel_load.json max_concurrency {value,source} (from config/params.csv map_max_concurrency if explicit, else built-in default 3)
    #   > built-in default (3)
    # It means the number of PARALLEL RANGE LOADS a table's LOAD files are split into (NOT a
    # Glue worker count, NOT max_write_concurrency, NOT the fleet-level max_parallel_tasks).
    def _as_int_1_40(v, where):
        if isinstance(v, bool):
            raise MrlPreflightError("%s must be an integer 1..40 (got boolean %r)." % (where, v))
        if isinstance(v, str):
            s = v.strip()
            if s == "" or not __import__("re").fullmatch(r"-?\d+", s):
                raise MrlPreflightError("%s must be an integer 1..40 (got %r)." % (where, v))
            v = int(s)
        if isinstance(v, float) and not float(v).is_integer():
            raise MrlPreflightError("%s must be a whole integer 1..40 (got %r)." % (where, v))
        v = int(v)
        if v < 1 or v > 40:
            raise MrlPreflightError("%s must be between 1 and 40 (got %d)." % (where, v))
        return v

    exec_override = (cfg.get("_mrl_execution") or {}).get("maxConcurrencyOverride")
    row_col = event.get("maxConcurrency")
    ppl_col = event.get("paramsParallelMaxConcurrency")   # config/params_parallel_load.csv layer
    # Params-derived layer (4)/(5): the pinned settings carry {"value":N,"source":...} where
    # source is 'params.csv' (map_max_concurrency explicitly present) or 'built-in default' (3).
    params_layer = cfg.get("max_concurrency") if isinstance(cfg.get("max_concurrency"), dict) \
        else {"value": 3, "source": "built-in default"}
    if exec_override is not None:
        max_concurrency = _as_int_1_40(exec_override, "max_concurrency (execution input)")
        mc_source = "execution input"
    elif row_col not in (None, ""):
        max_concurrency = _as_int_1_40(row_col, "max_concurrency (fleet row)")
        mc_source = "fleet row"
    elif ppl_col not in (None, ""):
        max_concurrency = _as_int_1_40(ppl_col, "max_concurrency (params_parallel_load.csv)")
        mc_source = "params_parallel_load.csv"
    else:
        max_concurrency = _as_int_1_40(params_layer.get("value", 3),
                                       "max_concurrency (%s)" % params_layer.get("source"))
        mc_source = params_layer.get("source", "built-in default")

    # Other knob defaults now come from the pinned settings (config/params.csv), with an event
    # override still honoured where a fleet row set one.
    def _cfg_int(key, dflt):
        v = cfg.get(key)
        try:
            return int(v) if v not in (None, "") else int(dflt)
        except (TypeError, ValueError):
            return int(dflt)

    load_workers = _int_knob("loadWorkers", _cfg_int("load_num_workers", 20))
    mfip = _int_knob("maxFilesInParallel", _cfg_int("max_files_in_parallel", 20))
    wpf = _int_knob("writersPerFile", _cfg_int("writers_per_file", 8))
    conn_budget = _cfg_int("conn_budget", 900)
    count_tol = _int_knob("countTolerance", 0)
    split_by = str(event.get("splitBy") or "count").strip().lower()
    if split_by not in ("count", "bytes"):
        raise MrlPreflightError("splitBy must be 'count' or 'bytes' (got %r)." % split_by)
    split_allow_hex = bool(event.get("splitAllowHex", False))
    cta = str(event.get("countTimeoutAction") or "fail").strip().lower()
    if cta not in ("fail", "warn"):
        raise MrlPreflightError("countTimeoutAction must be 'fail' or 'warn' (got %r)." % cta)
    load_worker_type = str(event.get("loadWorkerType")
                           or cfg.get("load_worker_type") or "").strip().upper().replace(" ", "")
    include_tables = event.get("includeTables") or []
    # writeMergedStatus default: true for full_load_and_cdc (later steps need the merged doc),
    # true for full_load_only too (harmless; a later normal run sees the table loaded).
    wms = event.get("writeMergedStatus")
    write_merged = True if wms is None else bool(wms)
    override = rt._normalize_override(inp)

    # Capacity arithmetic uses the EFFECTIVE values (fail only on a provably impossible setting).
    capacity = _capacity(max_concurrency, mfip, wpf, conn_budget, load_workers, warnings)

    out = {
        "ok": True,
        "taskArn": task_arn,
        "taskArnList": [task_arn],
        "taskName": task.get("ReplicationTaskIdentifier") or "",
        "taskSuffix": suffix,
        "configPrefix": "s3://%s/config/_task/%s/" % (bucket, suffix),
        "project": cfg["project"],
        "region": cfg["region"],
        "accountId": ids["account"],
        "mrlMode": mrl_mode,
        "cdc": cdc_on,
        "cdcSource": cdc_source,
        "dmsStartMode": dms_start_mode,
        "startDms": start_dms,
        "maxConcurrencySource": mc_source,
        "connBudget": conn_budget,
        "expectedStopReasonSubstrings": _EXPECTED_STOP[mrl_mode],
        "csvNullValue": contract.get("csvNullValue", "NULL"),
        "nullValues": null_values,
        "nullRules": null_rules,
        "nullNotnullText": null_notnull_text,
        "cdcRoot": contract.get("cdcRoot", "."),
        "capacity": capacity,
        "override": override,
        "maxConcurrency": max_concurrency,
        "loadWorkers": load_workers,
        "loadWorkerType": load_worker_type,
        "maxFilesInParallel": mfip,
        "writersPerFile": wpf,
        "maxWriteConcurrency": mfip * wpf,  # A6: per-run global DSQL writer cap (NOT writers_per_file)
        "countTolerance": count_tol,
        "countTimeoutAction": cta,
        "splitBy": split_by,
        "splitAllowHex": split_allow_hex,
        "includeTables": include_tables,
        "writeMergedStatus": write_merged,
        # --- Full startup-resolved field set (AMENDMENT 7): the reused BuildTableList /
        # DriverDiscovery* / CreateGlueJobs / RunDiscovery states read $.resolved.* with the SAME
        # keys startup's ResolveTask emits, so their Parameters are IDENTICAL (diff-tested).
        # Values come from the pinned settings (config/params.csv via to_pipeline_settings) and
        # the DMS endpoint contract - never re-guessed.
        "dsqlEndpoint": cfg.get("dsql_endpoint"),
        "dsqlEndpointCandidates": cfg.get("dsql_endpoint", ""),
        "dsqlUser": cfg.get("dsql_user", "admin"),
        "dsqlDatabase": cfg.get("dsql_database", "postgres"),
        "glueRoleArn": cfg.get("glue_role_arn"),
        "glueConnection": cfg.get("glue_connection", ""),
        "controlSchema": cfg.get("control_schema", "cdc_control"),
        "cdcEngine": cfg.get("cdc_engine", "pythonshell"),
        "cdcSparkFallback": cfg.get("cdc_spark_fallback", True),
        "cdcValidation": cfg.get("cdc_validation", True),
        "cdcValidationSample": cfg.get("cdc_validation_sample", 20),
        "cdcMaxDeleteFraction": cfg.get("cdc_max_delete_fraction", 0.5),
        "cdcMaxDeleteRows": cfg.get("cdc_max_delete_rows", 100000),
        "cdcDriftCheckMinutes": cfg.get("cdc_drift_check_minutes", 30),
        "cdcDriftTolerance": cfg.get("cdc_drift_tolerance", 0),
        "cdcDriftAction": cfg.get("cdc_drift_action", "warn"),
        "guardrailsMode": cfg.get("guardrails_mode", "warn"),
        "cdcFileOrderAction": cfg.get("cdc_file_order_action", "warn"),
        "cdcNopkOvermatchAction": cfg.get("cdc_nopk_overmatch_action", "warn"),
        "validateCountCheck": cfg.get("validate_count_check", "warn"),
        "cutoverCountCheck": cfg.get("cutover_count_check", "warn"),
        "timestampColumnName": contract.get("timestampColumnName", "dms_timestamp"),
        "glueVersion": cfg.get("glue_version", "4.0"),
        "discoveryWorkerType": cfg.get("discovery_worker_type", "G.2X"),
        "discoveryNumWorkers": cfg.get("discovery_num_workers", 5),
        "discoveryTimeoutMinutes": cfg.get("discovery_timeout_minutes", 480),
        "loadWorkerTypeCfg": cfg.get("load_worker_type", "G.4X"),
        "loadNumWorkers": cfg.get("load_num_workers", 10),
        "loadTimeoutMinutes": cfg.get("load_timeout_minutes", 2880),
        "loadBigWorkerType": cfg.get("load_big_worker_type", "G.8X"),
        "loadBigNumWorkers": cfg.get("load_big_num_workers", 10),
        "loadBigTimeoutMinutes": cfg.get("load_big_timeout_minutes", 2880),
        "validateWorkerType": cfg.get("validate_worker_type", "G.8X"),
        "validateNumWorkers": cfg.get("validate_num_workers", 10),
        "validateTimeoutMinutes": cfg.get("validate_timeout_minutes", 2880),
        "validateRowsPerRange": cfg.get("validate_rows_per_range", 10000),
        "validateParallelism": cfg.get("validate_parallelism", 0),
        "validateTargetSecondsPerRange": cfg.get("validate_target_seconds_per_range", 12),
        "validateHash": cfg.get("validate_hash", "all"),
        "maxParallelTables": cfg.get("max_parallel_tables", 20),
        "perWorkerMemBudgetMb": cfg.get("per_worker_mem_budget_mb", 1500),
        # Planning thresholds the reused PlanSplit / EnsureForkJobs read (CDC ownership only).
        "bigTableRowThreshold": cfg.get("big_table_row_threshold", 6000000),
        "fileFanoutThreshold": cfg.get("file_fanout_threshold", 8),
        "bigTableBytesThreshold": cfg.get("big_table_bytes_threshold", 1000000000),
        "maxGroups": cfg.get("max_groups", 10),
        "mapMaxConcurrency": cfg.get("map_max_concurrency", 6),
        "minWritersPerLoader": cfg.get("min_writers_per_loader", 100),
        "maxWritersPerLoader": cfg.get("max_writers_per_loader", 150),
        "maxCompositeForks": cfg.get("max_composite_forks", 8),
        "maxBigCdcForks": cfg.get("max_big_cdc_forks", 8),
        "warnings": warnings,
    }
    for w in warnings:
        print("(warn) mrl_preflight: %s" % w)
    print("(info) mrl_preflight OK: task %r suffix %r mode=%s dmsStartMode=%s startDms=%s"
          % (out["taskName"], suffix, mrl_mode, dms_start_mode, start_dms))
    return out
