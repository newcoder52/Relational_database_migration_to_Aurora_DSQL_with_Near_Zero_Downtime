#!/usr/bin/env python3
"""mrl_settings Lambda - the MRL parent's FIRST step: build the MRL settings file from the
EXISTING config/params.csv (READ-ONLY) and publish/pin it (AMENDMENT 3).

It NEVER writes config/params.csv, config/pipeline.json or config/fleet_tasks.csv, and it no
longer reads config/pipeline.json at all. Settings come ONLY from config/params.csv, parsed with
a BYTE-IDENTICAL copy of the existing parser (mrl_params_csv.parse / to_pipeline_settings; a sync
test fails if the copy drifts), so the validation, defaults and ranges — including rejecting
unknown keys — are exactly the existing ones.

PUBLISH RULE (config/pipeline_parallel_load.json):
  1. read config/params.csv; if missing or invalid, FAIL at once with the exact validation
     message (fail closed).
  2. build settings = to_pipeline_settings(parse(params.csv)) and add provenance: the params.csv
     sha256 + ETag, generated_at, and a tool version.
  3. compare (settings minus the volatile generated_at) with the live
     config/pipeline_parallel_load.json: identical => write NOTHING.
  4. different => if ANY execution of the two MRL state machines (fleet or task) is RUNNING, FAIL
     CLOSED naming it (and if listing is denied, FAIL CLOSED). Else write a dated backup under
     mrl/backups/ (NOT under config/), put the new file, read it back and verify.
  5. PIN: copy the (now-live) settings to mrl/runs/<fleetExecutionName>/settings.json. Children
     read the PINNED copy, so a params.csv edit mid-run cannot change a running load.

Event: { bucket, paramsKey? (default config/params.csv),
         settingsKey? (default config/pipeline_parallel_load.json),
         fleetExecutionName, fleetExecutionArn, fleetStateMachineArn, taskStateMachineArn }
Returns: { ok, settingsKey, pinnedSettingsKey, published (bool), backupKey|None,
           paramsSha256, settings, warnings }
"""

import hashlib
import json
import os
import re
from datetime import datetime, timezone

import boto3

import mrl_params_csv as pc   # BYTE-IDENTICAL copy of params_csv (sync-tested); reuses resolve_task

REGION = os.environ.get("AWS_REGION", "us-east-1")
TOOL_VERSION = "mrl-1"

_PARAMS_KEY_DEFAULT = "config/params.csv"
_SETTINGS_KEY_DEFAULT = "config/pipeline_parallel_load.json"
# The three EXISTING-pipeline files MRL must NEVER write.
_FORBIDDEN_WRITE = ("config/params.csv", "config/pipeline.json", "config/fleet_tasks.csv")


class MrlSettingsError(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get(s3, bucket, key):
    """(text, etag) or (None, None) if absent."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8"), obj.get("ETag")
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return None, None
        raise


def _guard_forbidden(key):
    k = str(key).lstrip("/")
    if k in _FORBIDDEN_WRITE:
        raise MrlSettingsError(
            "REFUSING to write %s: MRL must never write the existing pipeline's params.csv / "
            "pipeline.json / fleet_tasks.csv." % k)


def _put(s3, bucket, key, body_bytes):
    _guard_forbidden(key)
    s3.put_object(Bucket=bucket, Key=key, Body=body_bytes, ContentType="application/json")


def _any_mrl_running(sfn, arns, exclude_arn, warnings):
    """Return a list of 'name (sm)' for RUNNING executions of the two MRL machines (excluding
    this fleet execution). If listing is DENIED/unavailable, FAIL CLOSED (raise)."""
    running = []
    for sm in arns:
        if not sm or "<<" in sm:
            continue
        token = None
        while True:
            kw = {"stateMachineArn": sm, "statusFilter": "RUNNING", "maxResults": 100}
            if token:
                kw["nextToken"] = token
            page = sfn.list_executions(**kw)
            for ex in page.get("executions", []) or []:
                if ex.get("executionArn") == exclude_arn:
                    continue
                running.append("%s (%s)" % (ex.get("name") or ex.get("executionArn"),
                                            sm.rsplit(":", 1)[-1]))
            token = page.get("nextToken")
            if not token:
                break
    return running


def params_key_explicit(text, key):
    """AMENDMENT 5: did the operator EXPLICITLY write `key` with a NON-BLANK value in
    config/params.csv? The copied parser starts from OPTIONAL_DEFAULTS, so after parsing an
    ABSENT map_max_concurrency looks exactly like an explicit 6 - we cannot tell them apart from
    the parsed dict. So re-read the RAW text with the copy's OWN row-splitting helper
    (pc._split_row) under the same rules (skip blank + '#' lines, skip the 'parameter,value'
    header, trim values). Returns True only when the key appears with a non-blank value. Does NOT
    edit or re-validate the copied parser."""
    rows = (text or "").splitlines()
    header_seen = False
    for line in rows:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        fields = pc._split_row(line)
        k = str(fields[0]).strip() if fields else ""
        if not header_seen:
            header_seen = True
            if k.lower() == "parameter":
                continue  # the header row
            # no header (malformed) - the copied parse() will report it; fall through anyway
        v = str(fields[1]).strip() if len(fields) > 1 else ""
        if k == key and v != "":
            return True
    return False


def _resolved_params_max_concurrency(text, settings):
    """AMENDMENT 5 layers (4)+(5): the params-derived max_concurrency {value, source}. Source
    'params.csv' and the validated value ONLY when map_max_concurrency is EXPLICITLY present with
    a non-blank value; otherwise the BUILT-IN LAST-RESORT default 3, source 'built-in default'.
    Never infers by comparing with 6."""
    if params_key_explicit(text, "map_max_concurrency"):
        return {"value": int(settings.get("map_max_concurrency")), "source": "params.csv"}
    return {"value": 3, "source": "built-in default"}


def _validate_max_concurrency(raw):
    """AMENDMENT 4: strictly validate an execution-input max_concurrency BEFORE any DMS call.
    Accept an int or a numeric STRING; require an integer 1..40 (same bounds as params.csv
    map_max_concurrency). Reject 0, negatives, floats (3.5), booleans, empty, non-numeric text,
    and anything above the hard cap. Returns the int, or None when not provided. Raises
    MrlSettingsError with a clear message on an invalid value."""
    if raw is None:
        return None
    # bool is an int subclass - reject it explicitly (True/False is not a concurrency).
    if isinstance(raw, bool):
        raise MrlSettingsError("max_concurrency must be an integer 1..40 (got boolean %r)." % raw)
    if isinstance(raw, float) and not float(raw).is_integer():
        raise MrlSettingsError("max_concurrency must be a whole integer 1..40 (got float %r)." % raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s == "" or not re.fullmatch(r"-?\d+", s):
            raise MrlSettingsError(
                "max_concurrency must be an integer 1..40 (got %r)." % raw)
        val = int(s)
    elif isinstance(raw, (int, float)):
        val = int(raw)
    else:
        raise MrlSettingsError("max_concurrency must be an integer 1..40 (got %r)." % (raw,))
    if val < 1 or val > 40:
        raise MrlSettingsError("max_concurrency must be between 1 and 40 (got %d)." % val)
    return val


def _validate_cdc_bool(raw):
    """AMENDMENT 8: strict execution-input cdc. bool True/False, or the strings 'true'/'false'
    (case-insensitive). None => not given. Everything else (1, 0, 'yes', '', numbers, lists)
    is rejected BEFORE any DMS call."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
    raise MrlSettingsError(
        "cdc must be a boolean true/false (or the string \"true\"/\"false\"); got %r." % (raw,))


def handler(event, context):
    bucket = event["bucket"]
    # The fleet passes its WHOLE start input under rawInput so the OPTIONAL max_concurrency / cdc
    # are read here (never referenced as a possibly-absent ASL path).
    raw_input = event.get("rawInput") or {}
    params_key = event.get("paramsKey") or _PARAMS_KEY_DEFAULT
    settings_key = event.get("settingsKey") or _SETTINGS_KEY_DEFAULT
    exec_name = str(event.get("fleetExecutionName") or "unknown")
    exec_arn = event.get("fleetExecutionArn")
    warnings = []

    # AMENDMENT 4: validate the OPTIONAL execution-input max_concurrency STRICTLY, in this FIRST
    # state, BEFORE any DMS call. It overrides params.csv / the fleet-row column for every task of
    # this run. It is a PER-EXECUTION value: recorded only in the pinned settings + plan.json,
    # NEVER written to config/pipeline_parallel_load.json or any shared file.
    exec_max_concurrency = _validate_max_concurrency(raw_input.get("max_concurrency"))
    exec_cdc = _validate_cdc_bool(raw_input.get("cdc"))

    s3 = boto3.client("s3", region_name=REGION)

    # 1. read config/params.csv (READ-ONLY). Missing/invalid => fail at once with the exact msg.
    text, params_etag = _get(s3, bucket, params_key)
    if text is None:
        raise MrlSettingsError(
            "FAIL CLOSED: s3://%s/%s not found. MRL reads its settings from the existing "
            "config/params.csv (read-only)." % (bucket, params_key))
    parsed = pc.parse(text)
    if parsed["errors"]:
        raise MrlSettingsError(
            "FAIL CLOSED: config/params.csv has %d problem(s):\n  - %s"
            % (len(parsed["errors"]), "\n  - ".join(parsed["errors"])))
    for w in parsed["warnings"]:
        warnings.append("params.csv: %s" % w)
    try:
        settings = pc.to_pipeline_settings(parsed["params"])
    except pc.ParamsError as e:
        raise MrlSettingsError("FAIL CLOSED: config/params.csv: %s" % e)

    # 2. provenance.
    params_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    settings_doc = dict(settings)
    settings_doc["_mrl_provenance"] = {
        "source": "config/params.csv", "params_sha256": params_sha,
        "params_etag": params_etag, "generated_at": _now(), "tool_version": TOOL_VERSION,
    }
    # AMENDMENT 5: record the EXPLICIT-key marker and the params-derived max_concurrency
    # {value, source} so every later step applies the same rule (absent map_max_concurrency =>
    # built-in default 3, NOT the parser's filled-in 6). These are part of the identical-file
    # comparison. The per-EXECUTION override (if any) is layered on top in the pinned copy only.
    settings_doc["_explicit_keys"] = sorted(
        k for k in ("map_max_concurrency", "max_files_in_parallel", "writers_per_file",
                    "conn_budget")
        if params_key_explicit(text, k))
    settings_doc["max_concurrency"] = _resolved_params_max_concurrency(text, settings)

    # 3. compare with the live settings file (ignoring the volatile generated_at).
    live_text, _live_etag = _get(s3, bucket, settings_key)
    live = None
    if live_text is not None:
        try:
            live = json.loads(live_text)
        except ValueError as e:
            raise MrlSettingsError(
                "FAIL CLOSED: s3://%s/%s is not valid JSON (%s); fix or remove it."
                % (bucket, settings_key, e))

    def _comparable(d):
        d = dict(d or {})
        prov = dict(d.get("_mrl_provenance") or {})
        prov.pop("generated_at", None)
        d["_mrl_provenance"] = prov
        return d

    published = False
    backup_key = None
    if live is not None and _comparable(live) == _comparable(settings_doc):
        warnings.append("pipeline_parallel_load.json already matches config/params.csv; "
                        "nothing written.")
    else:
        # 4. different => refuse to change settings while any MRL run is in progress.
        sfn = boto3.client("stepfunctions", region_name=REGION)
        arns = [event.get("fleetStateMachineArn"), event.get("taskStateMachineArn")]
        try:
            busy = _any_mrl_running(sfn, arns, exec_arn, warnings)
        except Exception as e:
            code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
                       or type(e).__name__)
            raise MrlSettingsError(
                "FAIL CLOSED: could not list running MRL executions to safely change "
                "pipeline_parallel_load.json (%s). Grant states:ListExecutions on the two MRL "
                "state machines. Refusing to change settings while this can't be checked." % code)
        if busy:
            raise MrlSettingsError(
                "FAIL CLOSED: config/params.csv would change pipeline_parallel_load.json, but "
                "these MRL runs are in progress and read it: %s. Settings are not changed while "
                "any MRL fleet/task is running. Wait for them to finish, then start again."
                % ", ".join(busy))
        # dated backup under mrl/backups/ (NOT under config/), then put + read-back verify.
        if live_text is not None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup_key = "mrl/backups/pipeline_parallel_load.%s.json" % stamp
            _put(s3, bucket, backup_key, live_text.encode("utf-8"))
        body = (json.dumps(settings_doc, indent=2) + "\n").encode("utf-8")
        _put(s3, bucket, settings_key, body)
        readback, _ = _get(s3, bucket, settings_key)
        ok = readback is not None and _comparable(json.loads(readback)) == _comparable(settings_doc)
        if not ok:
            raise MrlSettingsError(
                "FAIL CLOSED: wrote s3://%s/%s but reading it back did not match." %
                (bucket, settings_key))
        published = True

    # 5. PIN the live settings for THIS fleet execution (children read the pinned copy).
    live_text2, _ = _get(s3, bucket, settings_key)
    if live_text2 is None:
        # Only possible if nothing was ever published AND we somehow skipped the write; publish now.
        body = (json.dumps(settings_doc, indent=2) + "\n").encode("utf-8")
        _put(s3, bucket, settings_key, body)
        live_text2 = body.decode("utf-8")
        published = True
    pinned_key = "mrl/runs/%s/settings.json" % _safe(exec_name)
    # The pinned copy records the PER-EXECUTION max_concurrency override (if any) under
    # _mrl_execution - this never touches the shared config/pipeline_parallel_load.json.
    pinned_doc = json.loads(live_text2)
    pinned_doc["_mrl_execution"] = {
        "fleetExecutionName": exec_name,
        "maxConcurrencyOverride": exec_max_concurrency,
        "cdcOverride": exec_cdc,
        "pinnedAt": _now(),
    }
    _put(s3, bucket, pinned_key, (json.dumps(pinned_doc, indent=2) + "\n").encode("utf-8"))

    for w in warnings:
        print("(warn) mrl_settings: %s" % w)
    print("(info) mrl_settings: published=%s settings=s3://%s/%s pinned=s3://%s/%s params_sha=%s "
          "exec_max_concurrency=%s"
          % (published, bucket, settings_key, bucket, pinned_key, params_sha[:12],
             exec_max_concurrency))
    return {"ok": True, "settingsKey": "s3://%s/%s" % (bucket, settings_key),
            "pinnedSettingsKey": "s3://%s/%s" % (bucket, pinned_key),
            "published": published, "backupKey": backup_key, "paramsSha256": params_sha,
            "execMaxConcurrency": exec_max_concurrency, "execCdc": exec_cdc, "settings": settings,
            "warnings": warnings}


def _safe(name):
    import re
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(name or "unknown"))
