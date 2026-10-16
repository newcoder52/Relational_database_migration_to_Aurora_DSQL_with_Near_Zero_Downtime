#!/usr/bin/env python3
"""mrl_fleet_preflight Lambda - first step of the MRL PARENT (fleet) state machine.

Reads the MRL fleet's input files, which live in the SAME config/ folder as the EXISTING
pipeline's files but under DISTINCT names (AMENDMENT 1), so the MRL pipeline never writes or
touches the existing pipeline's config/params.csv, config/pipeline.json or config/fleet_tasks.csv:
  * config/fleet_task_parallel_load.csv  (required)  - one row per DMS task to MRL-load.
  * config/params_parallel_load.csv      (optional)  - MRL fleet settings (parameter,value).

The existing pipeline reads config/ by EXACT key names only (never by listing the folder - see
DESIGN.md "Amendment 1 safety"), so the two extra files cannot disturb it. This Lambda reads
config/pipeline.json READ-ONLY for shared values and NEVER writes any config/* file.

Precedence (AMENDMENT 1): fleet row > params_parallel_load.csv > config/pipeline.json.

Validates the fleet CSV STRICTLY (fail closed, all problems collected):
  header present + exact known columns (unknown columns rejected); UTF-8/BOM tolerated; CRLF
  tolerated; blank lines ignored; quoting handled by csv; no duplicate task ARNs; a row cap;
  per-column types/ranges; mode and dms_start_mode enums; numeric columns are whole numbers in
  range; task_arn is a DMS task ARN. Builds one child input per row. Skips a task whose MRL child
  execution is already RUNNING (re-trigger safe), using a deterministic child execution name.

NO dependency on the existing preflight_tasks. This is the MRL fleet's own, separate validator.

Columns (header names exact):
  task_arn (req), task_suffix (opt), mode (opt; full_load_only|full_load_and_cdc),
  max_concurrency (opt int>=1), dms_start_mode (opt; start|adopt_stopped),
  load_worker_type (opt), load_workers (opt int>=1), max_files_in_parallel (opt int>=1),
  writers_per_file (opt int>=1), tables (opt; schema.table;schema.table include-list),
  override (opt true/false).

params_parallel_load.csv keys (optional; parameter,value): max_parallel_tasks (default 2),
and MRL-only shared defaults an operator may override without touching config/params.csv:
  split_by (count|bytes), split_allow_hex (true/false), count_timeout_action (fail|warn),
  count_tolerance (int), write_merged_status (true/false). All optional; blank = the built-in
  default. These are passed to each child so the child preflight applies the precedence.

Event: { bucket, stateMachineArn (the MRL CHILD machine ARN), fleetExecutionId }
Returns: { ok, count, toStart, skipped, maxParallelTasks, tasks:[{taskArn, runName, skip,
           runningExecution, input{...child preflight event fields...}}], warnings }
"""

import csv
import io
import json
import os
import re
import time

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
# AMENDMENT 1: the two MRL INPUT files live in config/ under MRL-specific names. Everything else
# MRL writes stays under mrl/ (mrl/state/<suffix>/...). These names are FIXED (not operator-
# overridable) so there is exactly one place to look, and they never collide with the existing
# pipeline's config/fleet_tasks.csv / config/params.csv / config/pipeline.json.
FLEET_TASKS_KEY = "config/fleet_task_parallel_load.csv"
FLEET_PARAMS_KEY = "config/params_parallel_load.csv"
PIPELINE_JSON_KEY = "config/pipeline.json"      # READ-ONLY (never written by MRL)
_MAX_ROWS = 200    # fleet row cap (user decision 2026-10-16; was 1000)

# MRL-only shared params an operator may set in params_parallel_load.csv (NOT in the existing
# config/params.csv, which rejects unknown keys). All optional.
_PARAM_KEYS = {"max_parallel_tasks", "split_by", "split_allow_hex", "count_timeout_action",
               "count_tolerance", "write_merged_status", "cdc", "max_concurrency"}

_COLUMNS = {
    "task_arn", "task_suffix", "cdc", "max_concurrency", "dms_start_mode",
    "load_worker_type", "load_workers", "max_files_in_parallel", "writers_per_file",
    "tables", "override",
}
_REQUIRED = {"task_arn"}
_DMS_START = {"start", "adopt_stopped"}
_TRUE = {"true", "1", "yes", "y", "on"}
_FALSE = {"false", "0", "no", "n", "off", ""}
_ARN_RE = re.compile(r"^arn:aws[a-z-]*:dms:[a-z0-9-]+:\d{12}:task:[A-Za-z0-9]+$")
_WORKER_TYPES = {"G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X",
                 "R.1X", "R.2X", "R.4X", "R.8X"}


class MrlFleetError(Exception):
    pass


def _get_text(s3, bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return None
        raise


def _bool(val, where, errors):
    s = str(val or "").strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    errors.append("%s: expected true/false (got %r)." % (where, val))
    return False


def _int(val, where, lo, errors):
    s = str(val or "").strip()
    if s == "":
        return None
    if not re.fullmatch(r"\d+", s):
        errors.append("%s: expected a whole number (got %r)." % (where, val))
        return None
    n = int(s)
    if n < lo:
        errors.append("%s: must be >= %d (got %d)." % (where, lo, n))
        return None
    return n


def _parse_params(text, errors, warnings):
    """Parse the OPTIONAL config/params_parallel_load.csv ('parameter,value'). Only the MRL-only
    keys in _PARAM_KEYS are allowed (unknown keys are rejected, mirroring the strictness of the
    existing params.csv, but this is a SEPARATE file so it never collides). Returns {key: value}.
    Shared pipeline values (dsql endpoint, region, roles, NULL defaults) come from
    config/pipeline.json via the child preflight; they are NOT settable here."""
    out = {}
    if text is None:
        return out
    rdr = csv.reader(io.StringIO(text))
    header_seen = False
    for row in rdr:
        if not row or not any(str(c).strip() for c in row):
            continue
        if str(row[0]).strip().startswith("#"):
            continue
        if not header_seen:
            if [c.strip().lower() for c in row[:2]] != ["parameter", "value"]:
                errors.append("%s first non-comment line must be 'parameter,value' (got %r)."
                              % (FLEET_PARAMS_KEY, row))
            header_seen = True
            continue
        key = str(row[0]).strip()
        val = str(row[1]).strip() if len(row) > 1 else ""
        if key not in _PARAM_KEYS:
            errors.append("%s: unknown key %r (allowed: %s)."
                          % (FLEET_PARAMS_KEY, key, sorted(_PARAM_KEYS)))
            continue
        out[key] = val
    return out


def _running_children(sfn_arn, warnings):
    """{taskArn: executionArn} for RUNNING executions of the MRL child machine."""
    if not sfn_arn or "<<" in sfn_arn:
        return {}
    found = {}
    try:
        sfn = boto3.client("stepfunctions", region_name=REGION)
        token, scanned = None, 0
        while True:
            kw = {"stateMachineArn": sfn_arn, "statusFilter": "RUNNING", "maxResults": 100}
            if token:
                kw["nextToken"] = token
            page = sfn.list_executions(**kw)
            for ex in page.get("executions", []) or []:
                scanned += 1
                d = sfn.describe_execution(executionArn=ex["executionArn"])
                try:
                    arn = str((json.loads(d.get("input") or "{}") or {})
                              .get("input", {}).get("taskArn")
                              or (json.loads(d.get("input") or "{}") or {}).get("taskArn")
                              or "").strip()
                except ValueError:
                    arn = ""
                if arn:
                    found[arn] = ex["executionArn"]
            token = page.get("nextToken")
            if not token or scanned >= 2000:
                break
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
                   or type(e).__name__)
        warnings.append("could not list running MRL child executions (%s); every task will be "
                        "started and the child duplicate-run guard still refuses a second run." % code)
        return {}
    return found


def handler(event, context):
    bucket = event["bucket"]
    child_sm = event.get("stateMachineArn")
    # The PublishSettings state (mrl_settings) passes the PINNED settings key + the validated
    # execution-input overrides. The fleet forwards them to every child unchanged.
    pinned_settings_key = event.get("pinnedSettingsKey")
    exec_max_concurrency = event.get("execMaxConcurrency")   # already validated 1..40 or None
    exec_cdc = event.get("execCdc")                          # already validated bool or None
    errors, warnings = [], []

    s3 = boto3.client("s3", region_name=REGION)
    dms = boto3.client("dms", region_name=REGION)

    # OPTIONAL MRL params file in config/ (NEVER the existing config/params.csv).
    params_text = _get_text(s3, bucket, FLEET_PARAMS_KEY)
    params = _parse_params(params_text, errors, warnings)
    param_cdc = params.get("cdc")        # config/params_parallel_load.csv layer
    max_parallel = 2
    if "max_parallel_tasks" in params:
        mp = _int(params["max_parallel_tasks"],
                  "%s max_parallel_tasks" % FLEET_PARAMS_KEY, 1, errors)
        if mp is not None:
            max_parallel = mp
    # MRL-only shared defaults (precedence layer between the fleet row and pipeline.json). Each
    # is OPTIONAL; a blank/absent key leaves the child's own default in place.
    param_split_by = params.get("split_by")
    param_split_allow_hex = params.get("split_allow_hex")
    param_count_action = params.get("count_timeout_action")
    param_count_tol = params.get("count_tolerance")
    param_write_merged = params.get("write_merged_status")
    if param_split_by is not None and param_split_by.strip().lower() not in ("count", "bytes"):
        errors.append("%s split_by must be 'count' or 'bytes' (got %r)."
                      % (FLEET_PARAMS_KEY, param_split_by))
    if param_count_action is not None and param_count_action.strip().lower() not in ("fail", "warn"):
        errors.append("%s count_timeout_action must be 'fail' or 'warn' (got %r)."
                      % (FLEET_PARAMS_KEY, param_count_action))
    if param_count_tol is not None and param_count_tol.strip() != "":
        _ct = _int(param_count_tol, "%s count_tolerance" % FLEET_PARAMS_KEY, 0, errors)

    tasks_text = _get_text(s3, bucket, FLEET_TASKS_KEY)
    if tasks_text is None:
        raise MrlFleetError("FAIL CLOSED: s3://%s/%s not found. Create the MRL fleet task list "
                            "(header + at least a task_arn column)." % (bucket, FLEET_TASKS_KEY))

    rdr = csv.DictReader(io.StringIO(tasks_text))
    header = [h.strip() for h in (rdr.fieldnames or [])]
    if "task_arn" not in header:
        raise MrlFleetError("FAIL CLOSED: %s has no 'task_arn' column (header=%r)." %
                            (FLEET_TASKS_KEY, header))
    unknown_cols = [h for h in header if h and h not in _COLUMNS]
    if unknown_cols:
        raise MrlFleetError("FAIL CLOSED: %s has unknown column(s) %s. Allowed: %s." %
                            (FLEET_TASKS_KEY, unknown_cols, sorted(_COLUMNS)))

    rows = [r for r in rdr if any(str(v or "").strip() for v in r.values())]
    if not rows:
        raise MrlFleetError("FAIL CLOSED: %s has a header but no task rows." % FLEET_TASKS_KEY)
    if len(rows) > _MAX_ROWS:
        raise MrlFleetError("FAIL CLOSED: %s has %d rows (max %d); failing before anything "
                            "starts (no child execution, no DMS call)." %
                            (FLEET_TASKS_KEY, len(rows), _MAX_ROWS))

    running = _running_children(child_sm, warnings)
    out_tasks, arn_seen, suffix_seen = [], {}, {}

    for i, row in enumerate(rows, start=1):
        where = "row %d" % i
        arn = str(row.get("task_arn") or "").strip()
        if not _ARN_RE.match(arn):
            errors.append("%s: task_arn %r is not a DMS task ARN." % (where, arn))
            continue
        if arn in arn_seen:
            errors.append("%s: task_arn %s already listed in row %d." % (where, arn, arn_seen[arn]))
            continue
        arn_seen[arn] = i
        try:
            found = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": [arn]}],
                WithoutSettings=True).get("ReplicationTasks", [])
        except Exception as e:
            errors.append("%s: describe_replication_tasks failed: %s" % (where, e))
            continue
        if len(found) != 1:
            errors.append("%s: DMS task not found: %s" % (where, arn))
            continue

        # AMENDMENT 8: resolve cdc per row (precedence: exec input > fleet row > params file >
        # default false). The exec/params layers were captured at the top; the row layer here.
        row_cdc_raw = str(row.get("cdc") or "").strip()
        row_cdc = None
        if row_cdc_raw:
            lc = row_cdc_raw.lower()
            if lc == "true":
                row_cdc = True
            elif lc == "false":
                row_cdc = False
            else:
                errors.append("%s: cdc %r must be true/false." % (where, row_cdc_raw))
        if exec_cdc is not None:
            eff_cdc = bool(exec_cdc)
        elif row_cdc is not None:
            eff_cdc = row_cdc
        elif param_cdc is not None:
            eff_cdc = str(param_cdc).strip().lower() == "true"
        else:
            eff_cdc = False
        dsm = str(row.get("dms_start_mode") or "start").strip().lower()
        if dsm not in _DMS_START:
            errors.append("%s: dms_start_mode %r must be one of %s." % (where, dsm, sorted(_DMS_START)))
        suffix = str(row.get("task_suffix") or "").strip()
        if suffix and suffix in suffix_seen:
            errors.append("%s: task_suffix %r also used by row %d." % (where, suffix, suffix_seen[suffix]))
        elif suffix:
            suffix_seen[suffix] = i
        wt = str(row.get("load_worker_type") or "").strip().upper().replace(" ", "")
        if wt and wt not in _WORKER_TYPES:
            errors.append("%s: load_worker_type %r must be one of %s." %
                          (where, row.get("load_worker_type"), sorted(_WORKER_TYPES)))
        max_conc = _int(row.get("max_concurrency"), "%s max_concurrency" % where, 1, errors)
        workers = _int(row.get("load_workers"), "%s load_workers" % where, 1, errors)
        mfip = _int(row.get("max_files_in_parallel"), "%s max_files_in_parallel" % where, 1, errors)
        wpf = _int(row.get("writers_per_file"), "%s writers_per_file" % where, 1, errors)
        override = _bool(row.get("override"), "%s override" % where, errors) \
            if str(row.get("override") or "").strip() else False
        tables_raw = str(row.get("tables") or "").strip()
        include_tables = [t.strip() for t in tables_raw.split(";") if t.strip()] if tables_raw else []
        for t in include_tables:
            if t.count(".") != 1 or t.startswith(".") or t.endswith("."):
                errors.append("%s: tables entry %r must be schema.table." % (where, t))

        # A7: DETERMINISTIC child execution name from task identity ONLY (no wall clock), so a
        # re-trigger of the same task maps to the same execution name. Step Functions then rejects
        # a concurrent duplicate with ExecutionAlreadyExists (the StartChild state treats that as
        # a SKIP, not a failure). Sanitised to the SFN name charset, <=80 chars.
        task_id = arn.rsplit(":", 1)[-1]
        run_base = (suffix or task_id)
        run_name = ("mrl-" + re.sub(r"[^A-Za-z0-9_-]", "-", run_base))[:80]

        skip = "already_running" if arn in running else None

        # The child input is the full event the CHILD state machine passes to mrl_preflight
        # (and carries through to the load). Per-row values override params; we set a key only
        # when the row sets it, so absent keys fall back in the child.
        child_input = {"taskArn": arn}
        if suffix:
            child_input["taskSuffix"] = suffix
        if override:
            child_input["override"] = True
        child = {
            "input": child_input,
            "cdc": eff_cdc,                       # resolved boolean (Amendment 8)
            "dmsStartMode": dsm,
            "settingsKey": pinned_settings_key,   # the PINNED settings this run started with
        }
        # Execution-input max_concurrency (validated at PublishSettings) overrides every row.
        if exec_max_concurrency is not None:
            child["maxConcurrency"] = exec_max_concurrency
        elif max_conc is not None:
            child["maxConcurrency"] = max_conc
        if wt:
            child["loadWorkerType"] = wt
        if workers is not None:
            child["loadWorkers"] = workers
        if mfip is not None:
            child["maxFilesInParallel"] = mfip
        if wpf is not None:
            child["writersPerFile"] = wpf
        if include_tables:
            child["includeTables"] = include_tables
        # params_parallel_load.csv layer (below the fleet row, above pipeline.json): set only
        # when the row did not set it, so the precedence fleet row > params file holds. The child
        # preflight applies the final fallback to pipeline.json / its built-in defaults.
        if param_split_by is not None:
            child["splitBy"] = param_split_by.strip().lower()
        if param_split_allow_hex is not None:
            child["splitAllowHex"] = str(param_split_allow_hex).strip().lower() in _TRUE
        if param_count_action is not None:
            child["countTimeoutAction"] = param_count_action.strip().lower()
        if param_count_tol is not None and param_count_tol.strip() != "":
            child["countTolerance"] = int(param_count_tol.strip())
        if param_write_merged is not None and param_write_merged.strip() != "":
            child["writeMergedStatus"] = str(param_write_merged).strip().lower() in _TRUE
        # params_parallel_load.csv max_concurrency = the child's precedence layer (3), used only
        # when neither the exec input nor the fleet row set max_concurrency.
        _ppl_mc = params.get("max_concurrency")
        if _ppl_mc is not None and str(_ppl_mc).strip() != "" and exec_max_concurrency is None \
                and max_conc is None:
            child["paramsParallelMaxConcurrency"] = str(_ppl_mc).strip()

        out_tasks.append({
            "taskArn": arn, "taskSuffix": suffix, "runName": run_name,
            "skip": skip, "runningExecution": running.get(arn), "input": child,
        })

    if errors:
        raise MrlFleetError("FAIL CLOSED: MRL fleet preflight found %d problem(s); nothing was "
                            "started:\n  - %s" % (len(errors), "\n  - ".join(errors)))

    to_start = sum(1 for t in out_tasks if not t["skip"])
    for w in warnings:
        print("(warn) mrl_fleet_preflight: %s" % w)
    print("(info) mrl_fleet_preflight: %d task(s), %d to start, %d skipped, max_parallel=%d"
          % (len(out_tasks), to_start, len(out_tasks) - to_start, max_parallel))
    return {"ok": True, "count": len(out_tasks), "toStart": to_start,
            "skipped": len(out_tasks) - to_start, "maxParallelTasks": max_parallel,
            "tasks": out_tasks, "warnings": warnings}
