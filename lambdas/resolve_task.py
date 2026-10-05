"""
resolve-task Lambda.

TWO MODES
  * Shared mode (event has "mode": "startup" | "cutover") -- used by the SHARED startup and
    cutover state machines, which serve every DMS task and are started with only
    {"taskArn": "..."}. In this mode the Lambda also:
      - reads the pipeline-wide settings file s3://<bucket>/config/pipeline.json
        (project, region, DSQL target, Glue role, Glue connection, CDC engine, ...);
      - derives the per-task values from the DMS task itself: task name -> taskSuffix ->
        config folder s3://<bucket>/config/_task/<taskSuffix>/ and the Glue job names
        <project>-<taskSuffix>-<role>;
      - startup only: checks the DMS task BEFORE DMS is started (full-load-and-cdc,
        StopTaskCachedChangesApplied=true, AddColumnName=true, target bucket = pipeline
        bucket, task not already past its full load), so a misconfigured task fails in
        seconds instead of after the full load;
      - refuses a second RUNNING execution of the same state machine for the same task
        (needs "execution" and "stateMachine" in the event; the shared workflows pass them);
      - safeguards: config/_task/<taskSuffix>/_task.json records which task ARN owns the
        folder (a reused name of a deleted task is refused), and
        config/_task_index/<task-id>.json records the suffix by ARN (a task renamed after
        startup still resolves to its original folder and Glue jobs).
    See handler_shared() for the exact event and return shape.
  * Legacy mode (no "mode" key) -- the per-task state machines created before the shared
    design. Input/output below are UNCHANGED, so already-deployed workflows keep working.

LEGACY MODE (per-task startup SM, after the DMS completion gate).

Derives WHERE this DMS task's CSV data lives in S3, from the task's own S3 TARGET endpoint
settings (bucket + BucketFolder) — no custom CdcPath assumed (default DMS layout: full-load
LOAD*.csv and CDC timestamp files share the per-table directory
<BucketFolder>/<schema>/<table>/). The per-task SM uses this to point discovery/load at the
right prefix, and the pipeline SERIALIZES CDC resume AFTER loads+validate (so the loader
never reads CDC files as full-load rows under the default layout).

Input event: { "taskArn": "arn:...:task:...", "configPrefix": "s3://bucket/config/_task/<t>/" }
Returns: {
  "s3Bucket": "<target bucket>",
  "bucketFolder": "<BucketFolder or ''>",
  "dmsS3Base": "s3://<bucket>/<bucketFolder>/",     # tables live under <dmsS3Base><schema>/<table>/
  "cdcRoot": "<BucketFolder or '.'>",               # CDC/full-load share the per-table dir;
                                                    # '.' sentinel == no BucketFolder (flat root)
  "migrationType": "full-load-and-cdc",
  "s3Settings": {                                   # full CSV/S3 format contract from the endpoint
    "bucketName", "bucketFolder", "datePartitionEnabled", "addColumnName",
    "timestampColumnName", "csvDelimiter", "csvRowDelimiter", "compressionType",
    "dataFormat", "rfc4180", "serviceAccessRoleArn"
  },
  "datePartitionEnabled": <bool>,                   # hoisted copies for easy SM passthrough
  "addColumnName": <bool>,                          #   (True => CSVs carry a header row)
  "timestampColumnName": "<col>"                    #   (the CDC watermark column)
}
Fails fast if the task's target endpoint is not S3, or the task is not cdc-capable.

WHY cdcRoot: the CDC job / plan-split / drain-check locate a table's CDC files under
<cdc_root>/<schema>/<table>/. Under the default DMS layout (no CdcPath — required because this
pipeline uses AddColumnName=true, which is incompatible with CdcPath), CDC + cached-change
files share the SAME per-table directory as the LOAD*.csv full-load files, i.e. under the
endpoint's BucketFolder. So cdc_root MUST equal the endpoint's BucketFolder (empty when there
is none). We derive it here from the endpoint instead of hardcoding "cdc" in the SM templates,
so a no-BucketFolder endpoint resolves to the flat root and a BucketFolder=cdc endpoint resolves
to "cdc" — automatically, never a config guess. Glue getResolvedOptions cannot pass an empty
arg value, so the empty case is emitted as the '.' sentinel that derive_table_prefixes()
normalizes back to "no subfolder".
"""

import csv
import io
import json
import os
import re
from datetime import datetime, timezone

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")

# DSQL allows 10 schemas per database and cdc_control takes one, so at most 9 task schemas.
# Single source of truth: preflight_tasks imports this so its pre-start estimate and the
# authoritative BuildTableList check use the same cap.
_MAX_DISTINCT_SCHEMAS = 9


def handler(event, context):
    if event.get("mode") in ("startup", "cutover"):
        return handler_shared(event, context)
    if event.get("mode") == "build_table_list":
        return handler_build_table_list(event, context)
    task_arn = event["taskArn"]
    dms = boto3.client("dms", region_name=REGION)

    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}],
        WithoutSettings=True).get("ReplicationTasks", [])
    if len(tasks) != 1:
        raise Exception(f"Could not resolve exactly one task for {task_arn}.")
    task = tasks[0]
    out = _endpoint_contract(dms, task, task_arn)
    out.pop("csvNullValue", None)   # legacy output shape unchanged
    return out


def _endpoint_contract(dms, task, task_arn):
    """The task's S3 target contract (legacy return shape). Fails fast on layouts the
    pipeline can't handle."""
    mig = task.get("MigrationType", "")
    if "cdc" not in mig:
        raise Exception(f"Task {task_arn} MigrationType={mig!r} is not cdc-capable "
                        f"(need full-load-and-cdc).")
    target_ep_arn = task.get("TargetEndpointArn")
    if not target_ep_arn:
        raise Exception(f"Task {task_arn} has no TargetEndpointArn.")

    eps = dms.describe_endpoints(
        Filters=[{"Name": "endpoint-arn", "Values": [target_ep_arn]}]).get("Endpoints", [])
    if len(eps) != 1:
        raise Exception(f"Could not resolve the target endpoint {target_ep_arn}.")
    ep = eps[0]
    if (ep.get("EngineName") or "").lower() != "s3":
        raise Exception(f"Task {task_arn} target endpoint engine={ep.get('EngineName')!r} "
                        f"is not S3. This pipeline requires an S3 target endpoint.")
    s3s = ep.get("S3Settings") or {}
    bucket = s3s.get("BucketName")
    if not bucket:
        raise Exception(f"S3 target endpoint for {task_arn} has no BucketName.")
    bucket_folder = (s3s.get("BucketFolder") or "").strip("/")
    base = f"s3://{bucket}/" + (f"{bucket_folder}/" if bucket_folder else "")
    # cdc_root MUST match the endpoint's BucketFolder (CDC/full-load share the per-table dir
    # under the default no-CdcPath layout). Emit the '.' sentinel when there is no
    # BucketFolder, because Glue getResolvedOptions can't pass an empty-string arg value;
    # derive_table_prefixes() in the CDC job normalizes '.'/'/'/'' back to "no subfolder".
    cdc_root = bucket_folder if bucket_folder else "."

    # ── Validate the endpoint uses the DMS DEFAULT flat per-table layout ──────────────────
    # Per the DMS S3-target docs, with the default layout DMS writes BOTH full-load
    # (LOAD########.csv) and CDC (timestamp YYYYMMDD-HHMMSSmmm.csv) files into the SAME
    # per-table directory  <BucketFolder>/<schema>/<table>/ , distinguished only by filename.
    # The whole pipeline (loader LOAD*.csv glob, CDC timestamp listing, drain-check) relies on
    # that single shared base. Two endpoint settings break it:
    #   • DatePartitionEnabled=true -> CDC files get date-partitioned subfolders, so they are
    #     NOT at <base>/<schema>/<table>/ and the CDC job would never find them.
    #   • CdcPath / PreserveTransactions -> CDC written to a separate transaction dir. (These
    #     are also mutually exclusive with AddColumnName, which this pipeline requires, so a
    #     correctly-built endpoint can't have them — but we check defensively.)
    # Fail fast with a clear message rather than silently derive wrong paths.
    if bool(s3s.get("DatePartitionEnabled")):
        raise Exception(
            f"Target endpoint for {task_arn} has DatePartitionEnabled=true. This pipeline "
            f"requires the DMS DEFAULT flat per-table layout (full-load + CDC share "
            f"<BucketFolder>/<schema>/<table>/). Recreate the S3 target endpoint with "
            f"DatePartitionEnabled=false (and AddColumnName=true).")
    if s3s.get("CdcPath") or bool(s3s.get("PreserveTransactions")):
        raise Exception(
            f"Target endpoint for {task_arn} sets CdcPath/PreserveTransactions. This pipeline "
            f"requires the DMS DEFAULT layout with AddColumnName=true (incompatible with "
            f"CdcPath/PreserveTransactions). Recreate the endpoint without them.")

    # ── Pull the full CSV/S3 format contract from the endpoint ────────────────────────────
    # The Glue jobs (load/validate/CDC) must parse the CSVs exactly as DMS wrote them. Rather
    # than hardcode delimiters / header presence / the timestamp column in the scripts, we
    # surface the endpoint's actual S3Settings so the pipeline is endpoint-driven and can't
    # silently drift if the endpoint is reconfigured. Values mirror DMS's own defaults when a
    # setting is omitted from the endpoint (DMS applies the same defaults at run time).
    def _b(v, default):
        return bool(v) if v is not None else default

    date_partition_enabled = _b(s3s.get("DatePartitionEnabled"), False)
    add_column_name = _b(s3s.get("AddColumnName"), False)  # True => CSVs carry a header row
    timestamp_column = s3s.get("TimestampColumnName") or "dms_timestamp"
    csv_delimiter = s3s.get("CsvDelimiter") or ","
    csv_row_delimiter = s3s.get("CsvRowDelimiter") or "\\n"
    compression_type = (s3s.get("CompressionType") or "NONE").upper()
    data_format = (s3s.get("DataFormat") or "csv").lower()
    rfc4180 = _b(s3s.get("Rfc4180"), True)
    service_access_role = s3s.get("ServiceAccessRoleArn") or ""

    # How DMS marks a real NULL in the CSVs (DMS default "NULL"). Shared mode only: the legacy
    # handler drops it so its output stays unchanged for the older per-task workflows.
    _nv = s3s.get("CsvNullValue")
    csv_null_value = "NULL" if _nv is None else str(_nv)
    return {
        "csvNullValue": csv_null_value,
        "s3Bucket": bucket,
        "bucketFolder": bucket_folder,
        "dmsS3Base": base,
        "cdcRoot": cdc_root,
        "migrationType": mig,
        # Full S3 format contract pulled from the target endpoint (endpoint-driven, not guessed)
        "s3Settings": {
            "bucketName": bucket,
            "bucketFolder": bucket_folder,
            "datePartitionEnabled": date_partition_enabled,
            "addColumnName": add_column_name,
            "timestampColumnName": timestamp_column,
            "csvDelimiter": csv_delimiter,
            "csvRowDelimiter": csv_row_delimiter,
            "compressionType": compression_type,
            "dataFormat": data_format,
            "rfc4180": rfc4180,
            "serviceAccessRoleArn": service_access_role,
        },
        # Hoisted convenience copies (so the SM can pass a single value without a nested path)
        "datePartitionEnabled": date_partition_enabled,
        "addColumnName": add_column_name,
        "timestampColumnName": timestamp_column,
    }


# =============================================================================================
# SHARED MODE (one startup + one cutover state machine for every DMS task)
# =============================================================================================
#
# Event (built by the shared state machines):
#   {"mode": "startup" | "cutover",
#    "bucket": "<pipeline bucket>",                 # filled once when the SM is deployed
#    "settingsKey": "config/pipeline.json",         # optional, this is the default
#    "input": {                                     # the execution input, passed through as-is
#        "taskArn": "arn:aws:dms:<region>:<account>:task:<id>",       # required
#        "taskSuffix": "<name>",                    # optional override of the task name
#        "adoptExistingFolder": true}}              # optional, startup only (see _check_owner)
#
# Returns the legacy endpoint contract PLUS:
#   taskArn, taskArnList, taskName, taskSuffix, suffixSource, configPrefix, project, region,
#   dsqlEndpoint, dsqlUser, dsqlDatabase, glueRoleArn, glueConnection, cdcEngine,
#   cdcSparkFallback, controlSchema, jobNames{discovery,load,load-big,validate,cdc}, cdcJobName,
#   warnings[]
# cdcEngine is "spark" when config/_task/<task>/_cdc_engine.json says an earlier run switched
# this task's CDC job to Spark (written by create-glue-jobs), whatever pipeline.json says.

SETTINGS_KEY_DEFAULT = "config/pipeline.json"
SETTINGS_REQUIRED = ("project", "region", "dsql_endpoint", "glue_role_arn")
SETTINGS_DEFAULTS = {
    "dsql_user": "admin",
    "dsql_database": "postgres",
    "glue_connection": "",
    "cdc_engine": "pythonshell",
    # If the Python-shell CDC job's drivers fail (driver check before DMS starts, or Glue's
    # install when the CDC run starts), re-create the CDC job as Spark and carry on.
    "cdc_spark_fallback": True,
    "control_schema": "cdc_control",
}
SETTINGS_KNOWN = set(SETTINGS_REQUIRED) | set(SETTINGS_DEFAULTS) | {"description", "settings_version"}
GLUE_ROLES = ("discovery", "load", "load-big", "validate", "cdc")

_ARN_RE = re.compile(r"^arn:(aws[a-z-]*):dms:([a-z0-9-]+):(\d{12}):task:([A-Za-z0-9]+)$")
# DMS task identifiers are letters, digits and hyphens; the same rule keeps the name valid as
# an S3 folder, a Glue job name and a Step Functions run name. No leading/trailing hyphen.
_NAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$")
_GLUE_NAME_MAX = 255
_RUN_ARTIFACTS = ("_manifest_index.json", "_load_status.json", "/_orchestrator/")


class SettingsError(Exception):
    pass


class TaskCheckError(Exception):
    pass


class FolderOwnerError(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_json(s3, bucket, key):
    """JSON document at s3://bucket/key, or None if the key does not exist."""
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404") or type(e).__name__ == "NoSuchKey":
            return None
        raise
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError as e:
        raise SettingsError(f"s3://{bucket}/{key} is not valid JSON: {e}")


def _put_json(s3, bucket, key, doc):
    s3.put_object(Bucket=bucket, Key=key, Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                  ContentType="application/json")


def _load_settings(s3, bucket, key, warnings):
    doc = _get_json(s3, bucket, key)
    if doc is None:
        raise SettingsError(
            f"Pipeline settings file s3://{bucket}/{key} does not exist. Create it once "
            f"(RUNBOOK Step 3c); every task's startup and cutover read it.")
    if not isinstance(doc, dict):
        raise SettingsError(f"s3://{bucket}/{key} must be a JSON object.")
    cfg = dict(SETTINGS_DEFAULTS)
    cfg.update({k: v for k, v in doc.items() if v is not None})
    missing = [k for k in SETTINGS_REQUIRED if not str(cfg.get(k) or "").strip()]
    if missing:
        raise SettingsError(f"s3://{bucket}/{key} is missing required key(s): {missing}.")
    for k, v in cfg.items():
        vals = v if isinstance(v, list) else [v]
        if any(isinstance(x, str) and ("<" in x or ">" in x) for x in vals):
            raise SettingsError(f"s3://{bucket}/{key}: '{k}' still holds a placeholder ({v!r}). "
                                f"Replace it with the real value.")
    unknown = sorted(set(doc) - SETTINGS_KNOWN)
    if unknown:
        warnings.append(f"pipeline.json has unknown key(s) {unknown} (ignored; check spelling).")
    return _validate_settings(cfg, warnings)


def _validate_settings(cfg, warnings):
    """Validate and normalise an already-assembled settings dict (defaults already applied,
    required keys already present). Shared by _load_settings (reading config/pipeline.json)
    and params_csv.to_pipeline_settings (building the same dict from params.csv), so a CSV can
    never produce settings the per-task workflow would later reject. Mutates and returns cfg.

    Behaviour is unchanged from when this block lived inline in _load_settings: every message
    and every normalisation is identical, so existing pipeline.json files resolve exactly as
    before."""
    if not _NAME_RE.match(str(cfg["project"])):
        raise SettingsError(f"pipeline.json 'project' must be letters, digits and hyphens "
                            f"(got {cfg['project']!r}).")
    if not re.match(r"^arn:aws[a-z-]*:iam::\d{12}:role/.+", str(cfg["glue_role_arn"])):
        raise SettingsError(f"pipeline.json 'glue_role_arn' is not an IAM role ARN "
                            f"(got {cfg['glue_role_arn']!r}).")
    eng = str(cfg["cdc_engine"]).strip().lower()
    if eng in ("glueetl", "pyspark"):
        eng = "spark"
    if eng not in ("pythonshell", "spark"):
        raise SettingsError(f"pipeline.json 'cdc_engine' must be 'pythonshell' or 'spark' "
                            f"(got {cfg['cdc_engine']!r}).")
    cfg["cdc_engine"] = eng
    fb = cfg["cdc_spark_fallback"]
    if isinstance(fb, str) and fb.strip().lower() in ("true", "false"):
        fb = fb.strip().lower() == "true"
    if not isinstance(fb, bool):
        raise SettingsError(f"pipeline.json 'cdc_spark_fallback' must be true or false "
                            f"(got {cfg['cdc_spark_fallback']!r}).")
    cfg["cdc_spark_fallback"] = fb
    conn = cfg.get("glue_connection") or ""
    if isinstance(conn, list):
        conn = ",".join(str(c).strip() for c in conn if str(c).strip())
    cfg["glue_connection"] = str(conn).strip()
    if not cfg["glue_connection"]:
        warnings.append("pipeline.json has no glue_connection: Glue jobs run outside your VPC "
                        "(fine only if Glue can reach DSQL without one).")
    if ".dsql" not in str(cfg["dsql_endpoint"]):
        warnings.append(f"dsql_endpoint {cfg['dsql_endpoint']!r} does not look like an Aurora "
                        f"DSQL endpoint (expected <cluster>.dsql[-xxxx].<region>.on.aws).")
    return cfg


def _parse_task_arn(task_arn):
    m = _ARN_RE.match(task_arn or "")
    if not m:
        raise TaskCheckError(
            f"taskArn {task_arn!r} is not a DMS task ARN "
            f"(expected arn:aws:dms:<region>:<account>:task:<id>).")
    return {"partition": m.group(1), "region": m.group(2), "account": m.group(3),
            "resourceId": m.group(4)}


def _check_startup_task(task, contract, bucket, warnings):
    """Checks that would otherwise only fail hours later (or never finish)."""
    arn = task.get("ReplicationTaskArn")
    mig = task.get("MigrationType", "")
    if mig != "full-load-and-cdc":
        raise TaskCheckError(f"DMS task {arn} has MigrationType={mig!r}; the startup workflow "
                             f"needs 'full-load-and-cdc'.")
    raw = task.get("ReplicationTaskSettings")
    if raw:
        try:
            fl = (json.loads(raw).get("FullLoadSettings") or {})
        except ValueError:
            fl = None
            warnings.append("could not parse the DMS task settings; skipped the "
                            "StopTaskCachedChangesApplied check.")
        if fl is not None:
            if fl.get("StopTaskCachedChangesApplied") is not True:
                raise TaskCheckError(
                    f"DMS task {arn} has FullLoadSettings.StopTaskCachedChangesApplied="
                    f"{fl.get('StopTaskCachedChangesApplied')!r}; it must be true. The workflow "
                    f"waits for DMS to stop with STOPPED_AFTER_CACHED_EVENTS, which only "
                    f"happens with this setting. Modify the task, then start again.")
            if fl.get("StopTaskCachedChangesNotApplied") is True:
                raise TaskCheckError(
                    f"DMS task {arn} has FullLoadSettings.StopTaskCachedChangesNotApplied=true; "
                    f"it must be false (DMS would stop before applying cached changes).")
    else:
        warnings.append("DMS returned no task settings; skipped the "
                        "StopTaskCachedChangesApplied check.")
    if not contract.get("addColumnName"):
        raise TaskCheckError(
            f"The S3 target endpoint of {arn} has AddColumnName=false. The pipeline needs a "
            f"header row in every CSV: set AddColumnName=true on the endpoint.")
    if contract.get("s3Bucket") != bucket:
        raise TaskCheckError(
            f"DMS task {arn} writes to bucket {contract.get('s3Bucket')!r}, but the pipeline "
            f"bucket is {bucket!r}. The Glue jobs read the DMS files from the pipeline bucket, "
            f"so the DMS S3 target endpoint must use the same bucket.")
    stats = task.get("ReplicationTaskStats") or {}
    pct = stats.get("FullLoadProgressPercent")
    status = (task.get("Status") or "").lower()
    reason = (task.get("StopReason") or "").upper()
    past_full_load = pct == 100 and (
        status in ("running", "starting", "resuming", "modifying")
        or (status == "stopped" and "CACHED_EVENTS" not in reason and "FULL_LOAD" not in reason))
    if past_full_load:
        raise TaskCheckError(
            f"DMS task {arn} has already finished its full load and moved on (status={status!r}, "
            f"stop reason={task.get('StopReason')!r}). The startup workflow would wait ~24 h "
            f"for STOPPED_AFTER_CACHED_EVENTS and then fail. If CDC is already running, nothing "
            f"to do; to reload from scratch see USAGE_GUIDE (clean-slate reload).")
    tm = task.get("TableMappings")
    if tm:
        try:
            rules = json.loads(tm).get("rules") or []
            lower_cols = any((r.get("rule-action") == "convert-lowercase"
                              and r.get("rule-target") == "column") for r in rules)
            if not lower_cols:
                warnings.append("the DMS table mapping has no convert-lowercase rule for "
                                "columns; the pipeline expects lowercase schema/table/column "
                                "names.")
        except ValueError:
            pass


def _pick_suffix(s3, bucket, inp, task, ids):
    """(suffix, source, index_record). Order: input override > recorded-by-ARN > DMS name."""
    index_key = f"config/_task_index/{ids['resourceId']}.json"
    rec = _get_json(s3, bucket, index_key)
    override = str(inp.get("taskSuffix") or "").strip()
    if override:
        if rec and rec.get("taskSuffix") != override:
            raise FolderOwnerError(
                f"taskSuffix {override!r} was given, but this DMS task was started earlier "
                f"with taskSuffix {rec.get('taskSuffix')!r} (s3://{bucket}/{index_key}). "
                f"Use the recorded one, or remove the record if that run was abandoned.")
        return override, "input", rec, index_key
    if rec and rec.get("taskSuffix"):
        return rec["taskSuffix"], "recorded", rec, index_key
    return task.get("ReplicationTaskIdentifier") or "", "dms-task-name", rec, index_key


def _validate_suffix(suffix, project):
    if not _NAME_RE.match(suffix or ""):
        raise TaskCheckError(
            f"Task name/suffix {suffix!r} must be letters, digits and hyphens (no leading or "
            f"trailing hyphen). Rename the DMS task or pass a valid 'taskSuffix'.")
    longest = f"{project}-{suffix}-load-big"
    if len(longest) > _GLUE_NAME_MAX:
        raise TaskCheckError(
            f"Glue job names would be {len(longest)} characters (max {_GLUE_NAME_MAX}): "
            f"shorten the DMS task name or pass a shorter 'taskSuffix'.")


def _has_run_artifacts(s3, bucket, prefix):
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []) or []:
            k = o.get("Key", "")
            if any(a in k for a in _RUN_ARTIFACTS):
                return k
    return None


def _check_owner(s3, bucket, suffix, task_arn, mode, inp, warnings):
    """Safeguard: the task folder may only be used by the DMS task that created it."""
    prefix = f"config/_task/{suffix}/"
    marker_key = prefix + "_task.json"
    marker = _get_json(s3, bucket, marker_key)
    if marker:
        if marker.get("taskArn") != task_arn:
            raise FolderOwnerError(
                f"s3://{bucket}/{prefix} belongs to DMS task {marker.get('taskArn')!r} "
                f"(see {marker_key}), not {task_arn!r}. A deleted task's name was probably "
                f"reused: its old status files would let CDC skip tables this task never "
                f"loaded. Archive the folder first, e.g. aws s3 mv s3://{bucket}/{prefix} "
                f"s3://{bucket}/config/_archive/{suffix}-<date>/ --recursive, or pass a "
                f"different 'taskSuffix'.")
        return marker_key, False
    if mode != "startup":
        warnings.append(f"{marker_key} not found (task started before the shared workflow); "
                        f"continuing.")
        return marker_key, False
    leftover = _has_run_artifacts(s3, bucket, prefix)
    if leftover and not inp.get("adoptExistingFolder"):
        raise FolderOwnerError(
            f"s3://{bucket}/{prefix} already holds files from an earlier run (e.g. {leftover}) "
            f"but has no owner record. If they belong to THIS task (a re-run), start again with "
            f"\"adoptExistingFolder\": true; otherwise archive the folder first.")
    _put_json(s3, bucket, marker_key, {"taskArn": task_arn, "taskSuffix": suffix,
                                       "createdAt": _now()})
    return marker_key, True



def _check_no_other_run(event, task_arn, mode, warnings):
    """Refuse a second RUNNING execution of this state machine for the same DMS task. Two
    startup runs for one task would run the full load twice into the same tables; two cutover
    runs would race each other. Needs the workflow to pass its own ids ("execution" =
    $$.Execution.Id, "stateMachine" = $$.StateMachine.Id); older workflows don't, and are
    skipped. Tie-break: only the run that started LATER refuses, so two runs started in the
    same second never both fail. Missing permission -> warning, not failure (upgrade-safe)."""
    me, sm = event.get("execution"), event.get("stateMachine")
    if not me or not sm:
        return
    try:
        sfn = boto3.client("stepfunctions", region_name=REGION)
        mine = sfn.describe_execution(executionArn=me)
        my_start = mine.get("startDate")
        token, scanned = None, 0
        while True:
            kw = {"stateMachineArn": sm, "statusFilter": "RUNNING", "maxResults": 100}
            if token:
                kw["nextToken"] = token
            page = sfn.list_executions(**kw)
            for ex in page.get("executions", []) or []:
                arn = ex.get("executionArn")
                if not arn or arn == me:
                    continue
                scanned += 1
                other = sfn.describe_execution(executionArn=arn)
                try:
                    other_task = str((json.loads(other.get("input") or "{}") or {}).get("taskArn") or "").strip()
                except ValueError:
                    other_task = ""
                if other_task != task_arn:
                    continue
                o_start = other.get("startDate")
                earlier = (my_start is None or o_start is None or o_start < my_start
                           or (o_start == my_start and arn < me))
                if earlier:
                    raise TaskCheckError(
                        f"Another {mode} run is already running for this DMS task: "
                        f"{ex.get('name') or arn} (started {o_start}). Two {mode} runs for one "
                        f"task would {'load the same tables twice' if mode == 'startup' else 'race each other'}. "
                        f"Wait for it to finish, or stop it, then start again.")
            token = page.get("nextToken")
            if not token or scanned >= 1000:
                break
    except TaskCheckError:
        raise
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "") or type(e).__name__)
        warnings.append(f"could not check for another running {mode} of this task ({code}); add "
                        f"states:ListExecutions and states:DescribeExecution to the Lambda role "
                        f"(RUNBOOK Step 1). Don't start two runs for one task at once.")

def handler_shared(event, context):
    mode = event["mode"]
    bucket = event["bucket"]
    settings_key = event.get("settingsKey") or SETTINGS_KEY_DEFAULT
    inp = event.get("input") or {}
    task_arn = str(inp.get("taskArn") or "").strip()
    warnings = []

    ids = _parse_task_arn(task_arn)
    s3 = boto3.client("s3", region_name=REGION)
    cfg = _load_settings(s3, bucket, settings_key, warnings)
    if ids["region"] != cfg["region"]:
        raise SettingsError(f"DMS task is in {ids['region']}, but pipeline.json region is "
                            f"{cfg['region']!r}.")

    dms = boto3.client("dms", region_name=ids["region"])
    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}],
        WithoutSettings=(mode != "startup")).get("ReplicationTasks", [])
    if len(tasks) != 1:
        raise TaskCheckError(f"DMS task {task_arn} not found (check the ARN and region).")
    task = tasks[0]
    contract = _endpoint_contract(dms, task, task_arn)
    if mode == "startup":
        _check_startup_task(task, contract, bucket, warnings)
    _check_no_other_run(event, task_arn, mode, warnings)

    suffix, source, rec, index_key = _pick_suffix(s3, bucket, inp, task, ids)
    _validate_suffix(suffix, cfg["project"])
    marker_key, created = _check_owner(s3, bucket, suffix, task_arn, mode, inp, warnings)
    if mode == "startup" and not rec:
        _put_json(s3, bucket, index_key, {"taskArn": task_arn, "taskSuffix": suffix,
                                          "taskNameAtStart": task.get("ReplicationTaskIdentifier"),
                                          "recordedAt": _now()})
    name = task.get("ReplicationTaskIdentifier") or ""
    if source == "recorded" and rec.get("taskNameAtStart") and name != rec["taskNameAtStart"]:
        warnings.append(f"DMS task was renamed to {name!r}; keeping its original folder and "
                        f"Glue jobs ({suffix!r}).")

    jobs = {role: f"{cfg['project']}-{suffix}-{role}" for role in GLUE_ROLES}
    # An earlier run of this task switched its CDC job to Spark because the Python-shell
    # drivers failed (create-glue-jobs wrote this file): keep Spark, don't fail the same way again.
    cdc_engine = cfg["cdc_engine"]
    engine_key = f"config/_task/{suffix}/_cdc_engine.json"
    engine_doc = _get_json(s3, bucket, engine_key) if cdc_engine == "pythonshell" else None
    if isinstance(engine_doc, dict) and engine_doc.get("engine") == "spark":
        cdc_engine = "spark"
        warnings.append(f"This task's CDC job runs on Spark: an earlier run switched it on "
                        f"{engine_doc.get('at')} because {str(engine_doc.get('reason'))[:400]}. "
                        f"Delete s3://{bucket}/{engine_key} to use Python shell again.")
    out = dict(contract)
    out.update({
        "taskArn": task_arn,
        "taskArnList": [task_arn],
        "taskName": name,
        "taskSuffix": suffix,
        "suffixSource": source,
        "configPrefix": f"s3://{bucket}/config/_task/{suffix}/",
        "ownerRecord": f"s3://{bucket}/{marker_key}",
        "project": cfg["project"],
        "region": cfg["region"],
        "dsqlEndpoint": cfg["dsql_endpoint"],
        "dsqlUser": cfg["dsql_user"],
        "dsqlDatabase": cfg["dsql_database"],
        "glueRoleArn": cfg["glue_role_arn"],
        "glueConnection": cfg["glue_connection"],
        "cdcEngine": cdc_engine,
        "cdcSparkFallback": cfg["cdc_spark_fallback"],
        "controlSchema": cfg["control_schema"],
        "jobNames": jobs,
        "cdcJobName": jobs["cdc"],
        "warnings": warnings,
    })
    for w in warnings:
        print(f"(warn) {w}")
    print(f"(info) {mode}: task {name!r} -> suffix {suffix!r} ({source}); config "
          f"{out['configPrefix']}; CDC engine {cdc_engine}"
          f"{'' if cdc_engine == 'spark' or not cfg['cdc_spark_fallback'] else ' (Spark fallback on)'}")
    return out


# =============================================================================================
# BUILD-TABLE-LIST MODE (startup workflow, state BuildTableList)
# =============================================================================================
#
# The operator supplies only the DMS task ARN (fleet_tasks.csv). The pipeline builds the task's
# table list ITSELF from the DMS task, so there is nothing to upload. This mode runs AFTER DMS
# has finished its full load (the startup SM reaches BuildTableList only when DMS has stopped at
# STOPPED_AFTER_CACHED_EVENTS) and BEFORE any Glue job is created (BuildTableList -> CreateGlueJobs).
#
# It:
#   1. lists every table DMS loaded, via describe_table_statistics (paginated on Marker),
#      including empty-at-full-load tables (FullLoadRows == 0, which the pipeline already handles);
#   2. fails if any table is in an error/suspended state (naming them);
#   3. turns each SOURCE schema/table (what the stats report) into the S3 FOLDER names (what the
#      task's TableMappings transformation rules produce) — the names discovery expects;
#   4. hard-checks the <=9 distinct DSQL-schema cap (before any Glue job exists);
#   5. writes config/_task/<suffix>/table_manifest.csv (same 2-column format discovery reads) and
#      table_list_source.json, overwriting any pre-existing manifest (one log line; no override).
#
# Event (built by the SM from the carried `resolved` object):
#   {"mode": "build_table_list",
#    "taskArn": "...", "bucket": "<pipeline bucket>", "configPrefix": "s3://.../config/_task/<s>/",
#    "region": "<region>", "taskSuffix": "<suffix>"}
# Returns: {"ok": true, "count": N, "distinctSchemas": [...], "manifestKey": "...",
#           "sourceKey": "...", "replacedExisting": bool, "warnings": [...]}

# DMS table-load states we treat as healthy (full load done, or legitimately empty). Compared
# case-insensitively. Anything else -> the table failed to load and we fail the build naming it.
_HEALTHY_TABLE_STATES = {"table completed", "table loaded", "fully loaded"}
# Transformation rule-actions that CAN change a schema/table name and that we can reproduce
# exactly. Any OTHER transformation on a schema/table target -> fail (we can't predict the folder).
_NAME_TRANSFORMS = {"rename", "convert-lowercase", "convert-uppercase", "add-prefix",
                    "add-suffix", "remove-prefix", "remove-suffix"}


class TableListError(Exception):
    pass


def _is_error_table_state(state):
    """True if a DMS TableState means the table did NOT load cleanly (so the build must fail)."""
    s = str(state or "").strip().lower()
    if not s:
        return True
    if s in _HEALTHY_TABLE_STATES:
        return False
    # Explicit error wording, and a fail-closed default for anything else unexpected.
    return True


def _wildcard_to_regex(pat):
    """A DMS object-locator pattern -> compiled regex. DMS uses '%' (any run) and '_' (one char)
    like SQL LIKE; '*' is accepted as a synonym for '%'. Matching is case-SENSITIVE on the exact
    source name DMS reports (DMS matches the source object name)."""
    out = ["^"]
    for ch in str(pat or ""):
        if ch in ("%", "*"):
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    return re.compile("".join(out))


def _locator_matches(locator, schema, table, is_table_rule):
    """Does a transformation rule's object-locator match this (schema, table)? For a schema-target
    rule only the schema is matched; for a table-target rule both schema and table must match."""
    loc = locator or {}
    sp = loc.get("schema-name")
    if sp is not None and not _wildcard_to_regex(sp).match(schema):
        return False
    if is_table_rule:
        tp = loc.get("table-name")
        if tp is not None and not _wildcard_to_regex(tp).match(table or ""):
            return False
    return True


def _apply_one_transform(action, value, name):
    """Apply a single supported name transformation to one name component."""
    if action == "rename":
        return str(value if value is not None else name)
    if action == "convert-lowercase":
        return name.lower()
    if action == "convert-uppercase":
        return name.upper()
    if action == "add-prefix":
        return f"{value}{name}"
    if action == "add-suffix":
        return f"{name}{value}"
    if action == "remove-prefix":
        return name[len(value):] if value and name.startswith(value) else name
    if action == "remove-suffix":
        return name[: -len(value)] if value and name.endswith(value) else name
    return name   # unreachable: callers only pass _NAME_TRANSFORMS actions


def _parse_table_mappings(task, task_arn):
    """Parse the task's TableMappings JSON -> list of rules. Raise TableListError if absent/bad."""
    tm = task.get("TableMappings")
    if not tm:
        raise TableListError(
            f"DMS task {task_arn} has no TableMappings; cannot derive the table list. The task "
            f"must have at least one selection rule.")
    try:
        doc = json.loads(tm) if isinstance(tm, str) else tm
    except ValueError as e:
        raise TableListError(f"DMS task {task_arn} TableMappings is not valid JSON: {e}")
    rules = (doc or {}).get("rules")
    if not isinstance(rules, list):
        raise TableListError(f"DMS task {task_arn} TableMappings has no 'rules' array.")
    return rules


def _transform_names(rules, src_schema, src_table, task_arn):
    """Apply the task's transformation rules (in rule order) to one SOURCE (schema, table) and
    return the resulting (schema, table) FOLDER names. Fails on any name-affecting transformation
    we don't support (so a folder we can't predict never silently produces a wrong manifest)."""
    schema, table = src_schema, src_table
    for r in rules:
        if (r.get("rule-type") or "").lower() != "transformation":
            continue
        target = (r.get("rule-target") or "").lower()
        action = (r.get("rule-action") or "").lower()
        if target not in ("schema", "table"):
            continue   # column / table-tablespace / etc. don't affect the folder names
        if action not in _NAME_TRANSFORMS:
            raise TableListError(
                f"DMS task {task_arn} TableMappings uses transformation rule-action {action!r} on "
                f"{target!r} (rule-id {r.get('rule-id')!r}), which this pipeline can't reproduce "
                f"when deriving S3 folder names. Supported: {sorted(_NAME_TRANSFORMS)}. Remove the "
                f"rule or change the task so folder names are predictable.")
        is_table_rule = target == "table"
        if not _locator_matches(r.get("object-locator"), schema, table, is_table_rule):
            continue
        value = r.get("value")
        if is_table_rule:
            table = _apply_one_transform(action, value, table)
        else:
            schema = _apply_one_transform(action, value, schema)
    return schema, table


def _describe_table_statistics(dms, task_arn):
    """Every table DMS reports for this task, paginated on Marker. Returns the raw stat dicts."""
    stats, marker = [], None
    while True:
        kw = {"ReplicationTaskArn": task_arn, "MaxRecords": 500}
        if marker:
            kw["Marker"] = marker
        resp = dms.describe_table_statistics(**kw)
        stats.extend(resp.get("TableStatistics", []) or [])
        marker = resp.get("Marker")
        if not marker:
            break
    return stats


def _estimate_selection_schemas(rules):
    """Best-effort distinct DSQL schemas the task's SELECTION rules load into, for preflight's
    pre-start cap estimate (BuildTableList does the authoritative check from real stats).

    Returns (schemas:set, wildcard:bool). `schemas` is the explicit (non-wildcard) source schema
    names from 'include' selection rules, with the task's schema-level name transformations
    applied, lowercased as discovery does. `wildcard` is True if any include selection rule's
    schema is a wildcard (so the real count can't be known before full load)."""
    schemas, wildcard = set(), False
    for r in rules:
        if (r.get("rule-type") or "").lower() != "selection":
            continue
        if (r.get("rule-action") or "include").lower() != "include":
            continue
        sp = ((r.get("object-locator") or {}).get("schema-name"))
        s = str(sp or "").strip()
        if (not s) or ("%" in s) or ("*" in s) or ("_" in s):
            wildcard = True
            continue
        # Apply schema-target transforms to the explicit name (table left as a wildcard match).
        try:
            tschema, _ = _transform_names(rules, s, "", "")
        except TableListError:
            # An unsupported transform is a hard error at BuildTableList; for the estimate just
            # treat this schema as unknown rather than failing preflight here.
            wildcard = True
            continue
        if tschema:
            schemas.add(tschema.lower())
    return schemas, wildcard


def handler_build_table_list(event, context):
    task_arn = str(event.get("taskArn") or "").strip()
    bucket = event.get("bucket")
    region = event.get("region") or REGION
    warnings = []
    if not task_arn:
        raise TableListError("build_table_list: no taskArn in the event.")
    if not bucket:
        raise TableListError("build_table_list: no bucket in the event.")

    # configPrefix is s3://<bucket>/config/_task/<suffix>/ ; derive the key prefix under bucket.
    config_prefix = str(event.get("configPrefix") or "").strip()
    if config_prefix.startswith("s3://"):
        _b, key_prefix = config_prefix[len("s3://"):].split("/", 1)
    elif config_prefix:
        key_prefix = config_prefix
    else:
        suffix = str(event.get("taskSuffix") or "").strip()
        if not suffix:
            raise TableListError("build_table_list: neither configPrefix nor taskSuffix given.")
        key_prefix = f"config/_task/{suffix}/"
    if not key_prefix.endswith("/"):
        key_prefix += "/"

    dms = boto3.client("dms", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}],
        WithoutSettings=False).get("ReplicationTasks", [])
    if len(tasks) != 1:
        raise TableListError(f"DMS task {task_arn} not found (check the ARN and region).")
    task = tasks[0]
    rules = _parse_table_mappings(task, task_arn)

    stats = _describe_table_statistics(dms, task_arn)
    if not stats:
        raise TableListError(
            f"DMS task {task_arn} reports no table statistics. The full load must have run (and "
            f"matched at least one table) before the table list can be built. Check the task's "
            f"selection rules.")

    errored, rows, seen = [], [], set()
    for st in stats:
        src_schema = str(st.get("SchemaName") or "").strip()
        src_table = str(st.get("TableName") or "").strip()
        state = st.get("TableState")
        if not src_schema or not src_table:
            continue   # DMS sometimes reports aggregate/control rows with no name; skip them
        if _is_error_table_state(state):
            errored.append(f"{src_schema}.{src_table} (state={state!r})")
            continue
        folder_schema, folder_table = _transform_names(rules, src_schema, src_table, task_arn)
        if not folder_schema or not folder_table:
            errored.append(f"{src_schema}.{src_table} (empty name after TableMappings)")
            continue
        dedup = (folder_schema, folder_table)
        if dedup in seen:
            continue
        seen.add(dedup)
        rows.append((folder_schema, folder_table))

    if errored:
        raise TableListError(
            f"DMS task {task_arn} has {len(errored)} table(s) that did not load cleanly; the "
            f"table list was not built and no Glue jobs were created:\n  - "
            + "\n  - ".join(sorted(errored))
            + "\nFix the source/DMS problem (or exclude the table in the task's selection rules) "
              "and start the task again.")
    if not rows:
        raise TableListError(
            f"DMS task {task_arn} loaded no tables with a usable schema/table name. Check the "
            f"task's selection rules.")

    distinct_schemas = sorted({s.lower() for s, _ in rows})
    if len(distinct_schemas) > _MAX_DISTINCT_SCHEMAS:
        raise TableListError(
            f"DMS task {task_arn} loads into {len(distinct_schemas)} distinct DSQL schemas "
            f"({distinct_schemas}); DSQL allows 10 per database and cdc_control takes one, so at "
            f"most {_MAX_DISTINCT_SCHEMAS}. Use fewer schemas (change the task's selection rules). "
            f"No Glue jobs were created.")

    manifest_key = key_prefix + "table_manifest.csv"
    source_key = key_prefix + "table_list_source.json"

    replaced = _get_json(s3, bucket, source_key) is not None or _object_exists(s3, bucket, manifest_key)
    if replaced:
        print(f"(info) replaced existing table_manifest.csv with the DMS-derived list "
              f"({len(rows)} tables)")

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["dms_schema", "dms_table"])
    for schema, table in rows:
        w.writerow([schema, table])
    s3.put_object(Bucket=bucket, Key=manifest_key,
                  Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    _put_json(s3, bucket, source_key, {
        "source": "dms", "count": len(rows), "taskArn": task_arn, "generatedAt": _now()})

    print(f"(info) build_table_list: task {task_arn} -> {len(rows)} table(s) in "
          f"{len(distinct_schemas)} schema(s) {distinct_schemas}; wrote s3://{bucket}/{manifest_key}")
    return {"ok": True, "count": len(rows), "distinctSchemas": distinct_schemas,
            "manifestKey": manifest_key, "sourceKey": source_key,
            "replacedExisting": bool(replaced), "warnings": warnings}


def _object_exists(s3, bucket, key):
    """True if s3://bucket/key exists (cheap existence check via get_object)."""
    try:
        s3.get_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404") or type(e).__name__ == "NoSuchKey":
            return False
        raise
