#!/usr/bin/env python3
"""Payload-contract audit (B19): every `$.Payload.<field>` a state machine reads in a
`ResultSelector` must be a field the invoked Lambda ACTUALLY returns in the mode that state
invokes it with.

Why this exists
---------------
`tests/test_asl_paths.py` only checks `$.path` *reachability* between states: it treats a
Task's `$.Payload` result as an OPEN (opaque) object, so ANY `$.Payload.<field>` read passes
its audit. That is exactly how B19 slipped through: `cutover.asl.json`'s `CutoverResolveTask`
`ResultSelector` read `$.Payload.compositeCdcJobName` and `$.Payload.hasCompositeTables`, but
`resolve_task.py` (cutover mode) never returns those keys -> `States.Runtime` at
`CutoverResolveTask`, before DMS is stopped. The reachability audit couldn't see it because it
never looks inside the Lambda.

This test closes that gap. For EVERY state (in every state machine, including fleet-startup and
fleet-cutover) that invokes a Lambda and has a `ResultSelector`, it:
  1. maps the state's `FunctionName` placeholder (e.g. ``<<RESOLVE_TASK_LAMBDA_ARN>>``) to the
     Lambda source file,
  2. reads the ``Payload.mode`` the state passes (``None`` when the Lambda takes no mode),
  3. statically extracts the set of keys that Lambda's handler can return in that mode (a scan
     of the handler's ``return {...}`` dict literals, ``out.update(...)`` calls and
     ``out[...] = v`` assignments -- see ``returned_keys``), and
  4. asserts every ``$.Payload.<field>`` in the state's ``ResultSelector`` is in that set.

Static key extraction is intentionally a SUPERSET of what a mode returns (it unions the keys of
every return/`.update`/subscript assignment in the mapped handler functions). That is the safe
direction for this check: a field read by the ASL must be *returnable*; if it is not even in the
superset, the ASL is reading a field the Lambda never produces (the B19 bug). It cannot produce a
false PASS for B19 because the removed fields appear in NO return dict of resolve_task.

Also included: a fake-invoke of resolve_task in CUTOVER mode (composite table + non-composite
table) whose output is run through the real cutover `CutoverResolveTask` ResultSelector -- it
must resolve with no missing `.$` path.

Static, offline (no AWS, no Spark, no network). Run: python3 tests/test_asl_payload_contract.py
"""
import ast
import importlib
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SF_DIR = os.path.join(REPO, "stepfunctions")
LAMBDAS = os.path.join(REPO, "lambdas")

_passed = 0
_failed = 0


def check(cond, msg):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"[PASS] {msg}")
    else:
        _failed += 1
        print(f"[FAIL] {msg}")


# ---------------------------------------------------------------------------------------------
# Placeholder (ASL FunctionName) -> Lambda source file.
# ---------------------------------------------------------------------------------------------
PLACEHOLDER_TO_FILE = {
    "<<RESOLVE_TASK_LAMBDA_ARN>>": "resolve_task.py",
    "<<DRIVER_DISCOVERY_LAMBDA_ARN>>": "driver_discovery.py",
    "<<PLAN_SPLIT_LAMBDA_ARN>>": "plan_split.py",
    "<<CREATE_GLUE_JOBS_LAMBDA_ARN>>": "create_glue_jobs.py",
    "<<STOP_CDC_RUN_LAMBDA_ARN>>": "stop_cdc_run.py",
    "<<DRAIN_CHECK_LAMBDA_ARN>>": "drain_check.py",
    "<<DROP_TAGS_LAMBDA_ARN>>": "drop_tags.py",
    "<<PREFLIGHT_TASKS_LAMBDA_ARN>>": "preflight_tasks.py",
}

# For each (lambda file, Payload.mode) the SET of handler/helper functions whose return-dict and
# `.update(...)` / `out[...] = v` keys TOGETHER form the keys that Lambda can return in that mode.
# `None` is the mode value when the state passes no Payload.mode (the Lambda's single/default
# handler path). Keeping this explicit (rather than guessing which function serves a mode) makes
# the contract a readable, reviewable spec and avoids over- or under-counting keys from unrelated
# branches.
MODE_RETURN_FUNCS = {
    # resolve_task: shared handler builds `out = dict(_endpoint_contract(...))` then
    # `out.update({...})`; _endpoint_contract supplies the S3-contract keys.
    ("resolve_task.py", "startup"): ["handler_shared", "_endpoint_contract"],
    ("resolve_task.py", "cutover"): ["handler_shared", "_endpoint_contract"],
    ("resolve_task.py", "build_table_list"): ["handler_build_table_list"],
    # driver_discovery: one handler; plain dict + `.update(engine=..., fallbackReason=...)`.
    ("driver_discovery.py", None): ["handler"],
    # plan_split: one handler, single terminal `return {...}`.
    ("plan_split.py", None): ["handler"],
    # create_glue_jobs: one handler, a return per mode (plus a shared `out[...]` block).
    ("create_glue_jobs.py", "list_fork_cdc"): ["handler"],
    ("create_glue_jobs.py", "delete"): ["handler"],
    ("create_glue_jobs.py", "create"): ["handler"],
    ("create_glue_jobs.py", "ensure_fork_jobs"): ["handler"],
    ("create_glue_jobs.py", "cdc_fallback"): ["handler"],
    # drain_check: validation_gate delegates to _validation_gate (ok/failures/byTable/tables);
    # the default (drain) path returns caughtUp/pending/checked from handler itself.
    ("drain_check.py", "validation_gate"): ["_validation_gate"],
    ("drain_check.py", None): ["handler"],
    # preflight_tasks: handler builds `out = {...}` then `out.update(params_result)`.
    ("preflight_tasks.py", "startup"): ["handler", "_handle_params_csv"],
    ("preflight_tasks.py", "cutover"): ["handler", "_handle_params_csv"],
    # stop_cdc_run / drop_tags: single return (not read via ResultSelector today, listed for
    # completeness so a future ResultSelector read is covered).
    ("stop_cdc_run.py", None): ["handler"],
    ("drop_tags.py", None): ["handler"],
}


# ---------------------------------------------------------------------------------------------
# Static key extraction.
# ---------------------------------------------------------------------------------------------
def _dict_literal_keys(node):
    """String keys of a dict literal AST node (ignores **spreads / non-constant keys)."""
    keys = set()
    if isinstance(node, ast.Dict):
        for k in node.keys:
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.add(k.value)
    return keys


def _func_return_keys(func_node):
    """Collect every key a function can contribute to a returned dict.

    Return-aware (precise, not 'every dict literal in the body'):
      * `return {...}` dict literals;
      * `return dict(...)` / `return dict(x, k=v)` -- literal-dict args, keyword args, and the
        keys of any Name arg that was itself built from a dict literal;
      * for each Name that is returned (`return out`), the keys of every dict literal assigned
        to it (`out = {...}`), every `out.update({...})` / `out.update(k=v)`, and every
        `out["k"] = v` subscript assignment;
      * the base of `out = dict(base, ...)` is followed too (so contract keys merged via
        `dict(contract)` are included).
    This is a SUPERSET across branches, the safe direction for a 'field must be returnable'
    contract, while staying scoped to values that actually flow to a return.
    """
    # 1) names that are returned somewhere in the function
    returned_names = set()
    for n in ast.walk(func_node):
        if isinstance(n, ast.Return):
            if isinstance(n.value, ast.Name):
                returned_names.add(n.value.id)
            if (isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                    and n.value.func.id == "dict" and n.value.args
                    and isinstance(n.value.args[0], ast.Name)):
                returned_names.add(n.value.args[0].id)

    # 2) also follow a Name that was assigned `x = dict(other, ...)` where x is returned
    #    (chase one extra hop: `out = dict(contract)` -> include `contract`'s dict keys)
    for n in ast.walk(func_node):
        if (isinstance(n, ast.Assign) and len(n.targets) == 1
                and isinstance(n.targets[0], ast.Name) and n.targets[0].id in returned_names
                and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                and n.value.func.id == "dict" and n.value.args
                and isinstance(n.value.args[0], ast.Name)):
            returned_names.add(n.value.args[0].id)

    keys = set()

    def _collect_dict_call(call):
        for a in call.args:
            keys.update(_dict_literal_keys(a))
        for kw in call.keywords:
            if kw.arg:
                keys.add(kw.arg)

    for n in ast.walk(func_node):
        # direct `return {...}` / `return dict(...)`
        if isinstance(n, ast.Return):
            if isinstance(n.value, ast.Dict):
                keys |= _dict_literal_keys(n.value)
            if (isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                    and n.value.func.id == "dict"):
                _collect_dict_call(n.value)
        # assignments to a returned Name: dict literals and `dict(...)` builders
        if isinstance(n, ast.Assign):
            for tgt in n.targets:
                if isinstance(tgt, ast.Name) and tgt.id in returned_names:
                    if isinstance(n.value, ast.Dict):
                        keys |= _dict_literal_keys(n.value)
                    if (isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                            and n.value.func.id == "dict"):
                        _collect_dict_call(n.value)
                # `out["k"] = v` on a returned Name (or any subscript-string assign)
                if (isinstance(tgt, ast.Subscript) and isinstance(tgt.slice, ast.Constant)
                        and isinstance(tgt.slice.value, str)):
                    base = tgt.value
                    if isinstance(base, ast.Name) and base.id in returned_names:
                        keys.add(tgt.slice.value)
        # `X.update({...})` / `X.update(k=v)` on a returned Name
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "update" and isinstance(n.func.value, ast.Name)
                and n.func.value.id in returned_names):
            for a in n.args:
                keys |= _dict_literal_keys(a)
            for kw in n.keywords:
                if kw.arg:
                    keys.add(kw.arg)
    return keys


_MODULE_FUNCS = {}


def _functions(lambda_file):
    if lambda_file in _MODULE_FUNCS:
        return _MODULE_FUNCS[lambda_file]
    with open(os.path.join(LAMBDAS, lambda_file)) as fh:
        tree = ast.parse(fh.read())
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    _MODULE_FUNCS[lambda_file] = funcs
    return funcs


def returned_keys(lambda_file, mode):
    """Static superset of the $.Payload keys <lambda_file> can return in <mode>."""
    fnames = MODE_RETURN_FUNCS.get((lambda_file, mode))
    if fnames is None:
        return None
    funcs = _functions(lambda_file)
    missing = [f for f in fnames if f not in funcs]
    if missing:
        raise AssertionError(f"{lambda_file}: expected function(s) {missing} not found "
                             f"(MODE_RETURN_FUNCS out of date)")
    keys = set()
    for f in fnames:
        keys |= _func_return_keys(funcs[f])
    return keys


# ---------------------------------------------------------------------------------------------
# Walk every state machine: for each Lambda-invoking state with a ResultSelector, verify the
# $.Payload.<field> reads against the invoked Lambda's returned keys for that mode.
# ---------------------------------------------------------------------------------------------
def _states_with_payload_resultselector(sm):
    """Yield (state_name, function_placeholder, mode, [payload_fields]) for every Task state
    (including nested Map iterator states) whose ResultSelector reads $.Payload.<field>."""
    out = []

    def walk(states, scope):
        for name, st in states.items():
            rs = st.get("ResultSelector")
            if isinstance(rs, dict):
                fields = sorted(
                    v.split("$.Payload.", 1)[1]
                    for v in rs.values()
                    if isinstance(v, str) and v.startswith("$.Payload.")
                )
                if fields:
                    params = st.get("Parameters") or {}
                    fn = params.get("FunctionName")
                    mode = (params.get("Payload") or {}).get("mode")
                    out.append((f"{scope}{name}", fn, mode, fields))
            if st.get("Type") == "Map":
                it = st.get("Iterator") or st.get("ItemProcessor")
                if it:
                    walk(it["States"], f"{scope}{name}.Iterator/")

    walk(sm["States"], "")
    return out


def test_payload_contract_all_state_machines():
    files = sorted(f for f in os.listdir(SF_DIR) if f.endswith(".asl.json"))
    check(len(files) >= 4, f"found {len(files)} state machines (>=4: startup, cutover, "
                           f"fleet-startup, fleet-cutover)")
    checked_states = 0
    for f in files:
        sm = json.load(open(os.path.join(SF_DIR, f)))
        for state, fn, mode, fields in _states_with_payload_resultselector(sm):
            lambda_file = PLACEHOLDER_TO_FILE.get(fn)
            check(lambda_file is not None,
                  f"{f}:{state} FunctionName {fn!r} maps to a known Lambda")
            if lambda_file is None:
                continue
            keys = returned_keys(lambda_file, mode)
            check(keys is not None,
                  f"{f}:{state} ({lambda_file} mode={mode}) has a declared return contract")
            if keys is None:
                continue
            missing = [fld for fld in fields if fld not in keys]
            check(not missing,
                  f"{f}:{state} ResultSelector: every $.Payload.<field> is returned by "
                  f"{lambda_file} (mode={mode}); missing={missing}")
            checked_states += 1
    check(checked_states >= 10,
          f"audited {checked_states} Lambda-ResultSelector state(s) across the fleet")


def test_b19_regression_cutover_resolve_fields_are_returned():
    """B19 pin: the cutover CutoverResolveTask ResultSelector must only read $.Payload fields
    that resolve_task returns in cutover mode. (This is the specific state that broke.)"""
    cut = json.load(open(os.path.join(SF_DIR, "cutover.asl.json")))
    rs = cut["States"]["CutoverResolveTask"]["ResultSelector"]
    read = sorted(v.split("$.Payload.", 1)[1] for v in rs.values()
                  if isinstance(v, str) and v.startswith("$.Payload."))
    keys = returned_keys("resolve_task.py", "cutover")
    missing = [f for f in read if f not in keys]
    check(not missing,
          f"B19: CutoverResolveTask reads only resolve_task cutover-mode fields; missing={missing}")
    # And specifically that the two obsolete fields are no longer read (they are found by tag at
    # cutover now, so resolve_task does not and should not return them).
    check("compositeCdcJobName" not in read,
          "B19: CutoverResolveTask no longer reads $.Payload.compositeCdcJobName")
    check("hasCompositeTables" not in read,
          "B19: CutoverResolveTask no longer reads $.Payload.hasCompositeTables")
    check("compositeCdcJobName" not in keys and "hasCompositeTables" not in keys,
          "B19: resolve_task (cutover mode) does not return the obsolete composite fields")


# ---------------------------------------------------------------------------------------------
# Fake-invoke resolve_task in CUTOVER mode (composite table + non-composite table), then run the
# real cutover ResultSelector against the output: every `.$` path must resolve.
# ---------------------------------------------------------------------------------------------
def _load_resolve_task():
    sys.path.insert(0, LAMBDAS)
    _b = types.ModuleType("boto3")
    _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    return rt


_ARN = "arn:aws:dms:us-east-1:111111111111:task:ABC"


class _FakeDms:
    """Minimal DMS stub: one cdc-capable task with an S3 target endpoint (default flat layout)."""

    def __init__(self):
        self._task = {
            "ReplicationTaskArn": _ARN,
            "ReplicationTaskIdentifier": "e2e-cutover",
            "MigrationType": "full-load-and-cdc",
            "TargetEndpointArn": "arn:aws:dms:us-east-1:111111111111:endpoint:S3TGT",
            "Status": "running",
        }

    def describe_replication_tasks(self, Filters=None, WithoutSettings=None):
        return {"ReplicationTasks": [self._task]}

    def describe_endpoints(self, Filters=None):
        return {"Endpoints": [{
            "EngineName": "s3",
            "S3Settings": {
                "BucketName": "sharedtest-bucket",
                "BucketFolder": "",
                "AddColumnName": True,
                "TimestampColumnName": "dms_timestamp",
                "DatePartitionEnabled": False,
                "Rfc4180": True,
            },
        }]}


class _Body:
    def __init__(self, b):
        self._b = b if isinstance(b, (bytes, bytearray)) else str(b).encode()

    def read(self):
        return self._b


class NoSuchKey(Exception):
    """Named 'NoSuchKey' so resolve_task._get_json treats it as a missing key (returns None)."""


class _FakeS3:
    """config/pipeline.json present; a composite marker object can be added via `extra` to model
    a task WITH composite tables -- cutover-mode resolve_task is composite-agnostic (forks are
    found by tag at cutover), so the output shape must be identical either way."""

    def __init__(self, extra=None):
        self._objs = {
            "config/pipeline.json": json.dumps({
                "project": "sharedtest",
                "region": "us-east-1",
                "dsql_endpoint": "efuaa.dsql.us-east-1.on.aws",
                "dsql_user": "admin",
                "dsql_database": "postgres",
                "glue_role_arn": "arn:aws:iam::111111111111:role/sharedtest-glue-exec-role",
                "glue_connection": "glue-noinet-conn",
                "control_schema": "cdc_control",
                "cdc_engine": "pythonshell",
            }).encode(),
        }
        self._objs.update(extra or {})

    def get_object(self, Bucket=None, Key=None):
        if Key in self._objs:
            return {"Body": _Body(self._objs[Key])}
        raise NoSuchKey(Key)

    def put_object(self, **k):
        self._objs[k.get("Key")] = (k.get("Body") or b"")


def _run_result_selector(selector, payload):
    """Apply an ASL ResultSelector to a Lambda result ({"Payload": payload}); raise KeyError
    naming the first `$.Payload.<field>` that is absent -- exactly the States.Runtime B19 hit."""
    frame = {"Payload": payload}
    out = {}
    for dst, src in selector.items():
        key = dst[:-2] if dst.endswith(".$") else dst
        if isinstance(src, str) and src.startswith("$.Payload."):
            field = src.split("$.Payload.", 1)[1]
            if field not in payload:
                raise KeyError(f"ResultSelector {dst}: $.Payload.{field} not in Lambda output")
            out[key] = payload[field]
        else:
            out[key] = frame
    return out


def _invoke_cutover_resolve(rt, s3):
    rt.boto3.client = lambda svc, region_name=None: (_FakeDms() if svc == "dms" else s3)
    # No "execution"/"stateMachine" -> _check_no_other_run short-circuits (no stepfunctions call),
    # keeping the fake-invoke fully offline. Cutover mode writes nothing and reads only pipeline.json.
    event = {"mode": "cutover", "bucket": "sharedtest-bucket",
             "settingsKey": "config/pipeline.json", "input": {"taskArn": _ARN}}
    return rt.handler_shared(event, None)


def test_cutover_resolve_output_satisfies_resultselector():
    rt = _load_resolve_task()
    cut = json.load(open(os.path.join(SF_DIR, "cutover.asl.json")))
    selector = cut["States"]["CutoverResolveTask"]["ResultSelector"]

    # (a) task WITH composite tables and (b) task WITHOUT -- cutover resolve is composite-agnostic
    # (forks found by tag), so the SAME resolve output shape must satisfy the ResultSelector in
    # both cases.
    cases = (
        ("with composite tables", {"config/_task/e2e-cutover/_jobs.json": json.dumps(
            {"jobs": [{"name": "sharedtest-e2e-cutover-ck-nd-cdc", "role": "ck-cdc"}]}).encode()}),
        ("without composite tables", {}),
    )
    for label, extra in cases:
        s3 = _FakeS3(extra)
        payload = _invoke_cutover_resolve(rt, s3)
        try:
            resolved = _run_result_selector(selector, payload)
            ok, err = True, ""
        except KeyError as e:
            resolved, ok, err = None, False, str(e)
        check(ok, f"cutover ResultSelector resolves against resolve_task output ({label})"
                  f"{'' if ok else ': ' + err}")
        if ok:
            check(resolved.get("cdcJobName") == "sharedtest-e2e-cutover-cdc",
                  f"cutover resolve yields the main cdcJobName ({label})")
            check("compositeCdcJobName" not in resolved and "hasCompositeTables" not in resolved,
                  f"cutover resolve carries no obsolete composite fields ({label})")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== asl-payload-contract: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
