#!/usr/bin/env python3
"""SHARED, fail-closed transforms for the stopAfterLoad patch.

Used by BOTH the deployed-environment apply script (patching Lambda zips + state-machine
definitions in place) and the plain-file / test paths, so the deployed change and the repo diff
are provably the SAME change (the tests assert patch_*(orig) == the shipped patched file).

Design:
  * The two Lambda .py transforms are ANCHORED string edits (add-only; nothing is removed).
    Each anchor must appear EXACTLY once or the transform raises PatchError (fail closed on an
    unrecognised or ambiguous file). The result is compiled before it is returned. Re-running on
    an already-patched source is a no-op (idempotent).
  * The startup ASL transform is a STRUCTURAL edit of the parsed JSON object, anchored on STATE
    NAMES and structure (never on ARNs / <<PLACEHOLDERS>>, which are already filled in a deployed
    definition). It fails closed if the shape is not the recognised startup workflow. Idempotent.
  * The fleet ASL is UNCHANGED; verify_fleet_startup_asl only checks the shape and returns it.
"""
import json


class PatchError(Exception):
    """A file/definition is not the recognised shape, an anchor is missing/ambiguous, or the
    patched result does not compile. The patch makes NO change in that case (fail closed)."""


# =============================================================================================
# Lambda .py transforms (anchored, add-only, idempotent, compile-checked)
# =============================================================================================
def _apply_anchored(src, filename, ops, already_marker):
    """ops: list of (anchor, payload, where) where where in {"before","after"}. Insert payload
    before/after the anchor's single occurrence. already_marker: if present in src, the file is
    already patched -> return src unchanged. Compile the result before returning."""
    if already_marker in src:
        compile(src, filename, "exec")
        return src
    new = src
    for anchor, payload, where in ops:
        n = new.count(anchor)
        if n != 1:
            raise PatchError(f"{filename}: anchor not found exactly once (found {n}): "
                             f"{anchor[:70]!r}")
        if where == "before":
            new = new.replace(anchor, payload + anchor)
        elif where == "after":
            new = new.replace(anchor, anchor + payload)
        else:
            raise PatchError(f"{filename}: bad op placement {where!r}")
    compile(new, filename, "exec")
    return new


# ---- resolve_task.py ----
_RT_DISPATCH_ANCHOR = (
    '    if event.get("mode") == "write_override_record":\n'
    '        return handler_write_override_record(event, context)\n'
)
_RT_DISPATCH_ADD = (
    '    if event.get("mode") == "write_load_only_record":\n'
    '        return handler_write_load_only_record(event, context)\n'
)

_RT_NORMALIZE_ANCHOR = (
    "    if isinstance(raw, (int, float)) and not isinstance(raw, bool):\n"
    "        return raw == 1\n"
    "    return str(raw).strip().lower() in _OVERRIDE_TRUE\n"
)
_RT_NORMALIZE_ADD = (
    '\n\n'
    '# \u2500\u2500 STOP-AFTER-LOAD (startup/fleet run input "stopAfterLoad": true) '
    '\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
    '# A RUN input, spelled exactly like the top-level "override" / "tasksFile" keys, never a\n'
    '# pipeline.json/params.csv setting, so the default (absent / null / false) run is BYTE-IDENTICAL\n'
    '# to before. It makes the startup workflow STOP AFTER THE FULL LOAD: every group loads (GroupLoad\n'
    '# / GroupLoadBig, all retries + resume semantics unchanged), but the path to GroupValidate is\n'
    '# skipped, no validation Glue job runs, DMS is NOT resumed and CDC is NOT started. It applies to\n'
    '# EVERY task of a fleet run (preflight passes it through to each child, like override).\n'
    '#\n'
    '# STRICT \u2014 unlike _normalize_override (which is lenient: "yes"/1/"on" are all truthy), this field\n'
    '# accepts ONLY a real boolean true/false or the strings "true"/"false" (any case). It mirrors the\n'
    '# string-casing of _normalize_override but REJECTS anything else (e.g. "yes", 1, "") so a typo can\n'
    '# never silently turn load-only on or off. Rejection raises TaskCheckError, which the fleet\n'
    '# PREFLIGHT turns into a PreflightError (nothing is started) and the per-task ResolveTask turns\n'
    '# into a failed execution before any Glue job \u2014 both name the field.\n'
    '_STOP_AFTER_LOAD_FIELD = "stopAfterLoad"\n'
    '_STOP_AFTER_LOAD_TRUE = {"true"}\n'
    '_STOP_AFTER_LOAD_FALSE = {"false"}\n'
    '\n\n'
    'def _normalize_stop_after_load(inp):\n'
    '    """bool: read the run input\'s "stopAfterLoad". Absent / null -> False (default run is\n'
    '    byte-identical). A real boolean -> that boolean. A string "true"/"false" (any case,\n'
    '    surrounding spaces ignored) -> that boolean. ANY OTHER value (e.g. "yes", 1, 1.0, "",\n'
    '    "on", [], {}) -> TaskCheckError naming the field. Strict on purpose: a load-only run must\n'
    '    never be turned on or off by a typo."""\n'
    '    raw = (inp or {}).get(_STOP_AFTER_LOAD_FIELD, None)\n'
    '    if raw is None:\n'
    '        return False\n'
    '    if isinstance(raw, bool):\n'
    '        return raw\n'
    '    if isinstance(raw, str):\n'
    '        s = raw.strip().lower()\n'
    '        if s in _STOP_AFTER_LOAD_TRUE:\n'
    '            return True\n'
    '        if s in _STOP_AFTER_LOAD_FALSE:\n'
    '            return False\n'
    '    raise TaskCheckError(\n'
    '        f"{_STOP_AFTER_LOAD_FIELD!r} must be a boolean true/false (or the string '
    '\\"true\\"/\\"false\\", "\n'
    '        f"any case); got {raw!r}. Remove the field for a normal run, or set it to exactly "\n'
    '        f"true/false. {_STOP_AFTER_LOAD_FIELD} stops the startup workflow after the full load "\n'
    '        f"(no validation, no DMS resume, no CDC).")\n'
)

_RT_HANDLER_ANCHOR = (
    '\n\nSETTINGS_REQUIRED = ("project", "region", "dsql_endpoint", "glue_role_arn")\n'
)
_RT_HANDLER_ADD = (
    '\n\n'
    '# \u2500\u2500 STOP-AFTER-LOAD marker '
    '\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
    '# A small operator-visibility marker written at the end of a load-only startup run (one that\n'
    '# finished the full load with every group loaded OK and stopped WITHOUT validation / DMS resume\n'
    '# / CDC). It mirrors the override-record mechanism (same _put_json helper, same config/_task/\n'
    '# <suffix>/ home) so an operator can see, from S3 alone, that a task stopped after load and what\n'
    '# to do next. Best-effort: the ASL RecordLoadOnly state catches a write failure and still ends\n'
    '# in TaskLoadOnlySucceeded, because the load itself succeeded \u2014 the marker is informational.\n'
    '_LOAD_ONLY_MARKER = "_load_only.json"\n'
    '\n\n'
    'def _load_only_marker_key(suffix):\n'
    '    return f"config/_task/{suffix}/{_LOAD_ONLY_MARKER}"\n'
    '\n\n'
    'def handler_write_load_only_record(event, context):\n'
    '    """Write config/_task/<suffix>/_load_only.json recording that this task\'s startup stopped\n'
    '    after the full load (stopAfterLoad=true). Event: { mode:"write_load_only_record", bucket,\n'
    '    taskSuffix, execution (ARN), executionName }. Returns {markerKey, when, who}. Never raises\n'
    '    for a normal write; a genuine S3 error propagates so the ASL RecordLoadOnly Catch can log it\n'
    '    (the run still succeeds)."""\n'
    '    bucket = event["bucket"]\n'
    '    suffix = event["taskSuffix"]\n'
    '    exec_arn = str(event.get("execution") or "").strip()\n'
    '    exec_name = str(event.get("executionName") or "").strip() or _execution_name(event)\n'
    '    when = _now()\n'
    '    s3 = boto3.client("s3", region_name=REGION)\n'
    '    marker_key = _load_only_marker_key(suffix)\n'
    '    doc = {\n'
    '        "stopAfterLoad": True,\n'
    '        "when": when,\n'
    '        "executionArn": exec_arn,\n'
    '        "executionName": exec_name,\n'
    '        "taskSuffix": suffix,\n'
    '        "note": ("Startup stopped after the full load: validation and CDC were NOT run and DMS "\n'
    '                 "was NOT resumed (left STOPPED_AFTER_CACHED_EVENTS). To continue, start the "\n'
    '                 "normal startup for this task WITHOUT stopAfterLoad; it skips already-loaded "\n'
    '                 "tables, validates, resumes DMS and starts CDC."),\n'
    '    }\n'
    '    _put_json(s3, bucket, marker_key, doc)\n'
    '    print(f"(info) load-only marker written: s3://{bucket}/{marker_key} "\n'
    '          f"(execution={exec_name})")\n'
    '    return {"markerKey": f"s3://{bucket}/{marker_key}", "when": when,\n'
    '            "who": exec_arn or exec_name}\n'
)

_RT_NORM_CALL_ANCHOR = '    override = _normalize_override(inp)\n'
_RT_NORM_CALL_ADD = (
    '    # stopAfterLoad: strict boolean (see _normalize_stop_after_load). Absent/null/false ->\n'
    '    # False, so the resolved dict (and the whole run) is byte-identical to before. An invalid\n'
    '    # value raises TaskCheckError here, failing ResolveTask before any Glue job is created (the\n'
    '    # fleet preflight rejects it even earlier, before any task starts).\n'
    '    stop_after_load = _normalize_stop_after_load(inp)\n'
)

_RT_OUT_ANCHOR = '        "override": override,\n'
_RT_OUT_ADD = '        "stopAfterLoad": stop_after_load,\n'


def patch_resolve_task_src(src):
    ops = [
        (_RT_DISPATCH_ANCHOR, _RT_DISPATCH_ADD, "after"),
        (_RT_NORMALIZE_ANCHOR, _RT_NORMALIZE_ADD, "after"),
        (_RT_HANDLER_ANCHOR, _RT_HANDLER_ADD, "before"),
        (_RT_NORM_CALL_ANCHOR, _RT_NORM_CALL_ADD, "after"),
        (_RT_OUT_ANCHOR, _RT_OUT_ADD, "after"),
    ]
    return _apply_anchored(src, "resolve_task.py", ops,
                           already_marker="def _normalize_stop_after_load(")


# ---- preflight_tasks.py ----
_PF_FLEET_ANCHOR = "    fleet_override = rt._normalize_override(fleet_input)\n"
_PF_FLEET_ADD = (
    '    # Fleet-wide stopAfterLoad: top-level {"stopAfterLoad": true} in the fleet start input makes\n'
    "    # EVERY task's child execution stop after the full load (no validation, no DMS resume, no\n"
    '    # CDC). STRICT: only boolean true/false or the strings "true"/"false" (any case); anything\n'
    '    # else (e.g. "yes", 1, "") is REJECTED HERE, in preflight, before any task is started, with a\n'
    "    # message naming the field. Normalized by resolve_task._normalize_stop_after_load, the same\n"
    "    # function the per-task ResolveTask uses, so the fleet and per-task rules can never diverge.\n"
    "    try:\n"
    "        fleet_stop_after_load = rt._normalize_stop_after_load(fleet_input)\n"
    "    except rt.TaskCheckError as e:\n"
    '        raise PreflightError(f"Fleet start input rejected, nothing was started: {e}")\n'
)

_PF_CHILD_ANCHOR = (
    '        row_override = rt._normalize_override({"override": row.get("override")})\n'
    "        if fleet_override or row_override:\n"
    '            child_input["override"] = True\n'
)
_PF_CHILD_ADD = (
    '        # stopAfterLoad: the fleet-wide flag OR an optional per-task CSV "stop_after_load" column\n'
    "        # turns load-only on for this task's child execution. RULE: TRUE WINS. The flag is a\n"
    "        # safety reduction (stop earlier, never resume DMS / start CDC), so it is ON when EITHER\n"
    "        # source asks for it; a fleet-level false can NOT turn off a per-task true, and a\n"
    "        # per-task blank/false can NOT turn off a fleet-level true. Only added when true, so the\n"
    "        # default (no load-only) child input is byte-identical to before. The per-task cell is\n"
    "        # validated with the SAME strict normalizer; an invalid cell is rejected here (naming the\n"
    "        # row and field) before any task starts.\n"
    "        row_stop_after_load = False\n"
    '        if "stop_after_load" in (reader.fieldnames or []):\n'
    '            _cell = row.get("stop_after_load")\n'
    '            if str(_cell or "").strip() != "":\n'
    "                try:\n"
    '                    row_stop_after_load = rt._normalize_stop_after_load({"stopAfterLoad": _cell})\n'
    "                except rt.TaskCheckError as e:\n"
    '                    errors.append(f"{where}: stop_after_load column: {e}"); continue\n'
    "        if fleet_stop_after_load or row_stop_after_load:\n"
    '            child_input["stopAfterLoad"] = True\n'
)


def patch_preflight_tasks_src(src):
    ops = [
        (_PF_FLEET_ANCHOR, _PF_FLEET_ADD, "after"),
        (_PF_CHILD_ANCHOR, _PF_CHILD_ADD, "after"),
    ]
    return _apply_anchored(src, "preflight_tasks.py", ops,
                           already_marker="fleet_stop_after_load = rt._normalize_stop_after_load(")


# =============================================================================================
# startup.asl.json structural transform (anchored on state names; fail closed; idempotent)
# =============================================================================================
def _require(cond, msg):
    if not cond:
        raise PatchError(msg)


def _iterator_states(sm):
    gfo = sm["States"].get("GroupFanOut")
    _require(isinstance(gfo, dict) and gfo.get("Type") == "Map",
             "startup ASL: GroupFanOut Map not found")
    it = gfo.get("Iterator") or gfo.get("ItemProcessor")
    _require(isinstance(it, dict) and "States" in it, "startup ASL: GroupFanOut has no Iterator")
    return gfo, it["States"]


def _resolve_task_lambda_ref(states):
    w = states.get("WriteStartupOverrideRecord")
    _require(isinstance(w, dict) and w.get("Parameters", {}).get("FunctionName"),
             "startup ASL: WriteStartupOverrideRecord FunctionName not found (needed to reuse the "
             "resolve-task Lambda reference for RecordLoadOnly)")
    return w["Parameters"]["FunctionName"]


def _bucket_ref(states):
    w = states.get("WriteStartupOverrideRecord")
    _require(isinstance(w, dict) and "bucket" in (w.get("Parameters", {}).get("Payload", {})),
             "startup ASL: WriteStartupOverrideRecord Payload.bucket not found (needed to reuse "
             "the bucket reference for RecordLoadOnly)")
    return w["Parameters"]["Payload"]["bucket"]


def patch_startup_asl(obj):
    """Return a NEW patched definition object. Fail closed if the recognised startup shape /
    anchor states are missing. Idempotent: a definition that already has the load-only states is
    returned unchanged (after a shape re-check)."""
    sm = json.loads(json.dumps(obj))  # deep copy
    states = sm.get("States")
    _require(isinstance(states, dict), "startup ASL: no States object")

    for req in ("EnsureForkJobs", "GroupFanOut", "EvaluateGroupResults", "AllGroupsSucceeded",
                "GroupsFailed", "WriteStartupOverrideRecord"):
        _require(req in states, f"startup ASL: expected state {req!r} not found "
                 f"(not the recognised startup workflow)")

    if "IsLoadOnly" in states and "TaskLoadOnlySucceeded" in states:
        _require(states["EnsureForkJobs"].get("Next") == "IsLoadOnly",
                 "startup ASL: half-patched (IsLoadOnly present but EnsureForkJobs not rerouted)")
        return sm

    gfo, itstates = _iterator_states(sm)
    for ld in ("GroupLoad", "GroupLoadBig"):
        _require(ld in itstates, f"startup ASL: iterator state {ld!r} not found")
        _require(itstates[ld].get("Next") == "GroupValidate",
                 f"startup ASL: {ld}.Next is {itstates[ld].get('Next')!r}, expected 'GroupValidate'")
    _require("GroupValidate" in itstates and "GroupOk" in itstates,
             "startup ASL: iterator GroupValidate/GroupOk not found")
    _require("AfterGroupLoad" not in itstates, "startup ASL: AfterGroupLoad already present")
    _require(states["EnsureForkJobs"].get("Next") == "GroupFanOut",
             f"startup ASL: EnsureForkJobs.Next is {states['EnsureForkJobs'].get('Next')!r}, "
             f"expected 'GroupFanOut'")
    _require(states["EvaluateGroupResults"].get("Next") == "AllGroupsSucceeded",
             "startup ASL: EvaluateGroupResults.Next is not 'AllGroupsSucceeded'")
    _require(isinstance(gfo.get("Parameters"), dict), "startup ASL: GroupFanOut has no Parameters")

    # EDIT 1: IsLoadOnly / MarkLoadOnly / MarkNotLoadOnly before GroupFanOut
    states["EnsureForkJobs"]["Next"] = "IsLoadOnly"
    states["IsLoadOnly"] = {
        "Type": "Choice",
        "Comment": ("stopAfterLoad: resolve the run's load-only flag ONCE into a guaranteed "
                    "$.loadOnly boolean, so every route into GroupFanOut produces it (ASL path "
                    "audit) and the Map can pass it to each group. The read of "
                    "$.resolved.stopAfterLoad is IsPresent-guarded: a NEW execution always has it; "
                    "an OLDER execution (no $.resolved.stopAfterLoad) falls through to "
                    "MarkNotLoadOnly -> loadOnly=false (byte-identical to today). stopAfterLoad only "
                    "ever stops EARLIER, never adds work, so defaulting a missing value to false is "
                    "safe."),
        "Choices": [{
            "And": [
                {"Variable": "$.resolved.stopAfterLoad", "IsPresent": True},
                {"Variable": "$.resolved.stopAfterLoad", "BooleanEquals": True},
            ],
            "Next": "MarkLoadOnly",
        }],
        "Default": "MarkNotLoadOnly",
    }
    states["MarkLoadOnly"] = {
        "Type": "Pass",
        "Comment": ("stopAfterLoad=true: carry loadOnly=true into the group fan-out. Each group "
                    "will LOAD exactly as today (GroupLoad / GroupLoadBig, same retries + resume) "
                    "but SKIP the path to GroupValidate."),
        "Result": True, "ResultPath": "$.loadOnly", "Next": "GroupFanOut",
    }
    states["MarkNotLoadOnly"] = {
        "Type": "Pass",
        "Comment": ("Normal run (stopAfterLoad absent/false): loadOnly=false. Produced on this "
                    "route too so $.loadOnly is present on every path into GroupFanOut (ASL path "
                    "audit)."),
        "Result": False, "ResultPath": "$.loadOnly", "Next": "GroupFanOut",
    }

    # EDIT 2: Map Parameters gets stopAfterLoad (= the run's $.loadOnly)
    gfo["Parameters"]["stopAfterLoad.$"] = "$.loadOnly"

    # EDIT 3: GroupLoad/GroupLoadBig -> AfterGroupLoad; AfterGroupLoad skips validate when load-only
    itstates["GroupLoad"]["Next"] = "AfterGroupLoad"
    itstates["GroupLoadBig"]["Next"] = "AfterGroupLoad"
    itstates["AfterGroupLoad"] = {
        "Type": "Choice",
        "Comment": ("stopAfterLoad: after a group's LOAD finishes, SKIP the validation Glue job "
                    "when the run is load-only. $.stopAfterLoad is injected per group by "
                    "GroupFanOut's Parameters (= the run's $.loadOnly); IsPresent-guarded so an "
                    "older in-flight execution falls through to GroupValidate (byte-identical). "
                    "When load-only, go straight to GroupOk: no validate job is ever started."),
        "Choices": [{
            "And": [
                {"Variable": "$.stopAfterLoad", "IsPresent": True},
                {"Variable": "$.stopAfterLoad", "BooleanEquals": True},
            ],
            "Next": "GroupOk",
        }],
        "Default": "GroupValidate",
    }

    # EDIT 4: EvaluateGroupResults -> LoadOnlyOrContinue; add terminal + marker states
    states["EvaluateGroupResults"]["Next"] = "LoadOnlyOrContinue"
    states["LoadOnlyOrContinue"] = {
        "Type": "Choice",
        "Comment": ("stopAfterLoad: when load-only, STOP AFTER THE LOAD instead of continuing to "
                    "the override/validation/CDC logic. All groups loaded OK -> "
                    "TaskLoadOnlySucceeded (DMS left STOPPED_AFTER_CACHED_EVENTS, no validation/CDC/"
                    "resume). ANY load failed -> GroupsFailed as today; override does NOT rescue a "
                    "failed load when stopAfterLoad is set (load-only never consults "
                    "$.resolved.override). $.loadOnly produced on every route (MarkLoadOnly / "
                    "MarkNotLoadOnly) and IsPresent-guarded so an older execution falls through to "
                    "AllGroupsSucceeded. No group can be validate_failed on a load-only run (the "
                    "validate job is never started)."),
        "Choices": [
            {
                "And": [
                    {"Variable": "$.loadOnly", "IsPresent": True},
                    {"Variable": "$.loadOnly", "BooleanEquals": True},
                    {"Variable": "$.groupCheck.anyLoadFailed", "BooleanEquals": True},
                ],
                "Next": "GroupsFailed",
            },
            {
                "And": [
                    {"Variable": "$.loadOnly", "IsPresent": True},
                    {"Variable": "$.loadOnly", "BooleanEquals": True},
                ],
                "Next": "RecordLoadOnly",
            },
        ],
        "Default": "AllGroupsSucceeded",
    }
    states["RecordLoadOnly"] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Comment": ("stopAfterLoad: write the operator-visibility marker "
                    "config/_task/<suffix>/_load_only.json (reuses resolve_task, mode "
                    "write_load_only_record). Best-effort: a write failure is caught "
                    "(RecordLoadOnlyFailedButLoaded) and the run STILL ends in "
                    "TaskLoadOnlySucceeded, because the load itself succeeded."),
        "Parameters": {
            "FunctionName": _resolve_task_lambda_ref(states),
            "Payload": {
                "mode": "write_load_only_record",
                "bucket": _bucket_ref(states),
                "taskSuffix.$": "$.resolved.taskSuffix",
                "execution.$": "$$.Execution.Id",
                "executionName.$": "$$.Execution.Name",
            },
        },
        "ResultSelector": {
            "markerKey.$": "$.Payload.markerKey",
            "when.$": "$.Payload.when",
            "who.$": "$.Payload.who",
        },
        "ResultPath": "$.loadOnlyRecord",
        "Retry": [{
            "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException",
                            "Lambda.SdkClientException", "Lambda.TooManyRequestsException"],
            "IntervalSeconds": 5, "MaxAttempts": 5, "BackoffRate": 2.0,
        }],
        "Catch": [{
            "ErrorEquals": ["States.ALL"],
            "Next": "RecordLoadOnlyFailedButLoaded",
            "ResultPath": "$.loadOnlyRecordError",
        }],
        "Next": "TaskLoadOnlySucceeded",
    }
    states["RecordLoadOnlyFailedButLoaded"] = {
        "Type": "Pass",
        "Comment": ("Writing the _load_only.json marker failed, but every group LOADED OK, so the "
                    "load-only run still succeeds (marker is operator-visibility only; error kept "
                    "in $.loadOnlyRecordError). Validation and CDC were still NOT run and DMS was "
                    "NOT resumed."),
        "Next": "TaskLoadOnlySucceeded",
    }
    states["TaskLoadOnlySucceeded"] = {
        "Type": "Succeed",
        "Comment": ("stopAfterLoad=true and every group loaded OK. The full load finished and all "
                    "groups loaded into DSQL, but VALIDATION and CDC were NOT run and DMS was NOT "
                    "resumed (left STOPPED_AFTER_CACHED_EVENTS). To continue, start the NORMAL "
                    "startup for this task WITHOUT stopAfterLoad: it is accepted (the task is at "
                    "STOPPED_AFTER_CACHED_EVENTS, not 'past full load'), skips already-loaded "
                    "tables, runs validation, resumes DMS and starts CDC. A marker "
                    "config/_task/<suffix>/_load_only.json is also written (RecordLoadOnly)."),
    }
    return sm


def verify_fleet_startup_asl(obj):
    """The fleet-startup definition is UNCHANGED by this patch: it already forwards the whole
    fleet input to Preflight as fleetInput ($), which carries stopAfterLoad to each child. Verify
    that recognised shape and return the object unchanged. Fail closed otherwise."""
    states = obj.get("States")
    _require(isinstance(states, dict), "fleet ASL: no States object")
    pf = states.get("Preflight")
    _require(isinstance(pf, dict), "fleet ASL: Preflight state not found")
    payload = pf.get("Parameters", {}).get("Payload", {})
    _require(payload.get("fleetInput.$") == "$",
             "fleet ASL: Preflight does not forward the whole fleet input as fleetInput.$=$ "
             "(stopAfterLoad would not reach preflight); refusing to proceed")
    return obj
