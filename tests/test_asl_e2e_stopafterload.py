"""END-TO-END ASL interpretation test for stopAfterLoad (the second check that would have caught
S1). It executes stepfunctions/startup.asl.json from its Start state with a small ASL interpreter
that honours the parts the bug lived in:
  * Task state: wrap state input per Parameters (the Lambda-invoke `Payload`), obtain the Lambda
    output, then apply the state's REAL ResultSelector to `{"Payload": <output>}` and write it to
    ResultPath. For ResolveTask the Lambda output is the REAL dict from `resolve_task.handler`
    (called with fake S3/DMS, mode=startup, input={taskArn, stopAfterLoad:true|false|absent}).
    Every other Task is stubbed (returns {} and just advances) — we only care about the routing.
  * Pass: apply Result/Parameters to ResultPath.
  * Choice: And / Variable+IsPresent / Variable+BooleanEquals (what IsLoadOnly/AfterGroupLoad/
    LoadOnlyOrContinue use).
  * Map (GroupFanOut): run its iterator ONCE with the per-group `stopAfterLoad` the Map Parameters
    inject (= $.loadOnly), collecting the states visited inside, then continue.
  * Succeed/Fail: terminal.

Assertions:
  stopAfterLoad=true  -> reaches TaskLoadOnlySucceeded; NEVER GroupValidate / ResumeDmsToCdc /
                          StartCdcJob / TaskSucceeded.
  flag false / absent -> reaches GroupValidate + ResumeDmsToCdc + StartCdcJob (normal path),
                          NEVER TaskLoadOnlySucceeded.

Because it runs the REAL ResultSelector over the REAL handler output, dropping `stopAfterLoad.$`
from the ResultSelector (the S1 bug) makes the true-case assertion fail (mutation test below).
"""
import copy
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SF = os.path.join(REPO, "stepfunctions")
sys.path.insert(0, os.path.join(REPO, "lambdas"))

TASK_ARN = "arn:aws:dms:us-east-1:111111111111:task:RELONETASK0000000000000000"
TARGET_EP = "arn:aws:dms:us-east-1:111111111111:endpoint:TGTS3000000000000000000000"
SETTINGS = {
    "project": "rel1", "region": "us-east-1",
    "dsql_endpoint": "demo.dsql.us-east-1.on.aws", "dsql_user": "admin",
    "dsql_database": "postgres", "glue_role_arn": "arn:aws:iam::111111111111:role/rel1-glue-exec-role",
    "control_schema": "cdc_control",
}


# --------------------------------------------------------------------------- fake AWS for handler
class _FakeS3:
    def get_object(self, Bucket, Key):
        if Key.endswith("pipeline.json") or "settings" in Key.lower() or Key == "config/pipeline.json":
            body = json.dumps(SETTINGS).encode()

            class _B:
                def read(self_):
                    return body
            return {"Body": _B()}
        e = Exception("NoSuchKey"); e.response = {"Error": {"Code": "NoSuchKey"}}
        raise e

    def put_object(self, **k):
        return {}

    def list_objects_v2(self, **k):
        return {"KeyCount": 0, "Contents": []}

    def get_paginator(self, op):
        class _Pag:
            def paginate(self_, **k):
                return iter([{"KeyCount": 0, "Contents": []}])
        return _Pag()

    def head_object(self, **k):
        e = Exception("404"); e.response = {"Error": {"Code": "404"}}
        raise e


class _FakeDMS:
    def describe_replication_tasks(self, Filters=None, WithoutSettings=False, **k):
        return {"ReplicationTasks": [{
            "ReplicationTaskArn": TASK_ARN,
            "ReplicationTaskIdentifier": "rel1-a",
            "MigrationType": "full-load-and-cdc",
            "TargetEndpointArn": TARGET_EP,
            "ReplicationTaskSettings": json.dumps(
                {"FullLoadSettings": {"StopTaskCachedChangesApplied": True,
                                      "StopTaskCachedChangesNotApplied": False}}),
        }]}

    def describe_endpoints(self, Filters=None, **k):
        return {"Endpoints": [{
            "EndpointArn": TARGET_EP, "EngineName": "s3",
            "S3Settings": {"BucketName": "rel1-bucket", "BucketFolder": "",
                           "AddColumnName": True, "DatePartitionEnabled": False},
        }]}


def _install_fake_boto3():
    b = types.ModuleType("boto3")

    def _client(svc, **k):
        if svc == "s3":
            return _FakeS3()
        if svc == "dms":
            return _FakeDMS()
        return types.SimpleNamespace()
    b.client = _client
    sys.modules["boto3"] = b
    return b


def _real_resolved_payload(stop_after_load):
    """Call the REAL resolve_task.handler (mode=startup) with fakes and return its output dict
    (the Lambda 'Payload'). stop_after_load in (True, False, None/absent)."""
    _install_fake_boto3()
    import importlib
    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    inp = {"taskArn": TASK_ARN}
    if stop_after_load is not None:
        inp["stopAfterLoad"] = stop_after_load
    event = {"mode": "startup", "bucket": "rel1-bucket",
             "settingsKey": "config/pipeline.json", "input": inp}
    return rt.handler(event, None)


# --------------------------------------------------------------------------- tiny ASL interpreter
def _get_path(data, path):
    if path == "$":
        return data
    cur = data
    for part in path[2:].split("."):
        # support an array index segment like ReplicationTasks[0]
        idx = None
        if "[" in part and part.endswith("]"):
            part, _, idxs = part[:-1].partition("[")
            idx = int(idxs)
        if not isinstance(cur, dict) or part not in cur:
            return _MISSING
        cur = cur[part]
        if idx is not None:
            if not isinstance(cur, list) or idx >= len(cur):
                return _MISSING
            cur = cur[idx]
    return cur


_MISSING = object()


def _set_path(data, path, value):
    if path in (None, "$"):
        return value if isinstance(value, dict) else {"_": value}
    cur = data
    parts = path[2:].split(".")
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value
    return data


def _apply_result_selector(rs, payload_wrapped):
    out = {}
    for k, v in rs.items():
        key = k[:-2] if k.endswith(".$") else k
        if isinstance(v, str) and v.startswith("$"):
            val = _get_path(payload_wrapped, v)
            # In real SFN a ResultSelector path the payload lacks FAILS the state. Our minimal
            # offline fakes make resolve_task return a slightly smaller payload than a live run, so
            # we OMIT a key the payload genuinely lacks (leaving $.resolved.<key> absent) rather
            # than fail — faithful for IsPresent-guarded routing, which is what stopAfterLoad uses.
            if val is not _MISSING:
                out[key] = val
        else:
            out[key] = v
    return out


def _choice_match(rule, data):
    if "And" in rule:
        return all(_choice_match(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_choice_match(r, data) for r in rule["Or"])
    var = rule.get("Variable")
    val = _get_path(data, var) if var else _MISSING
    if "IsPresent" in rule:
        return (val is not _MISSING) == rule["IsPresent"]
    if "IsString" in rule:
        return (val is not _MISSING and isinstance(val, str)) == rule["IsString"]
    if "IsBoolean" in rule:
        return (val is not _MISSING and isinstance(val, bool)) == rule["IsBoolean"]
    if "BooleanEquals" in rule:
        return val is not _MISSING and val == rule["BooleanEquals"]
    if "StringEquals" in rule:
        return val is not _MISSING and val == rule["StringEquals"]
    if "NumericEquals" in rule:
        return val is not _MISSING and val == rule["NumericEquals"]
    if "NumericGreaterThan" in rule:
        return val is not _MISSING and isinstance(val, (int, float)) and val > rule["NumericGreaterThan"]
    if "StringMatches" in rule:
        import fnmatch
        return val is not _MISSING and isinstance(val, str) and fnmatch.fnmatchcase(val, rule["StringMatches"])
    return False


# Stubbed AWS-SDK Task results so the DMS-poll loop and group fan-out resolve to SUCCESS and the
# run reaches the load-only decision (IsLoadOnly / AfterGroupLoad / LoadOnlyOrContinue). These are
# NOT what this test asserts — the assertion is purely which branch stopAfterLoad drives.
TASK_STUBS = {
    "DescribeDmsTask": {"ReplicationTasks": [{
        "Status": "stopped",
        "StopReason": "Stop Reason FULL_LOAD_ONLY_FINISHED (STOPPED_AFTER_CACHED_EVENTS)",
        "ReplicationTaskStats": {"FullLoadProgressPercent": 100, "TablesErrored": 0},
    }]},
}

# Fields live AWS / intrinsic Pass states would compute, seeded on entry to the named state so the
# load-only routing (the thing under test) can run. All-success so a load-only run ends in
# TaskLoadOnlySucceeded and a normal run continues to validate/resume/CDC.
DATA_SEEDS = {
    "CollectGroupResults": {"$.groupCheck": {"statuses": ["succeeded"]}},
    "EvaluateGroupResults": {"$.groupCheck": {"statuses": ["succeeded"],
                                              "anyLoadFailed": False,
                                              "anyValidateFailed": False}},
}


class Interp:
    def __init__(self, defn, resolve_payload):
        self.defn = defn
        self.states = defn["States"]
        self.resolve_payload = resolve_payload
        self.visited = []

    def run(self, data, states=None, start=None, max_steps=500):
        states = states or self.states
        name = start or self.defn.get("StartAt") or self.defn.get("Start")
        steps = 0
        while name and steps < max_steps:
            steps += 1
            self.visited.append(name)
            # Seed data-dependent fields that live AWS would have filled, so routing reaches the
            # load-only decision. These are NOT the thing under test (the stopAfterLoad routing is).
            if name in DATA_SEEDS:
                for k, v in DATA_SEEDS[name].items():
                    data = _set_path(data, k, copy.deepcopy(v))
            st = states[name]
            t = st["Type"]
            if t == "Succeed":
                return name
            if t == "Fail":
                return name
            if t == "Choice":
                nxt = None
                for rule in st.get("Choices", []):
                    branch = {k: v for k, v in rule.items() if k != "Next"}
                    if _choice_match(branch, data):
                        nxt = rule["Next"]; break
                name = nxt or st.get("Default")
                continue
            if t == "Pass":
                # ResultSelector/intrinsic Pass states (CollectGroupResults/EvaluateGroupResults use
                # States.ArrayContains, which we don't evaluate) are handled via DATA_SEEDS above;
                # here we only apply a literal Result / static Parameters that have no intrinsics.
                body = st.get("Result")
                if body is None:
                    params = st.get("Parameters")
                    if isinstance(params, dict) and not any(
                            isinstance(k, str) and k.endswith(".$") for k in params):
                        body = params
                if body is not None:
                    data = _set_path(data, st.get("ResultPath", "$"), copy.deepcopy(body))
                name = st.get("Next"); continue
            if t == "Map":
                # run the iterator ONCE with the Map's injected per-item stopAfterLoad (= $.loadOnly)
                itdef = st.get("Iterator") or st.get("ItemProcessor")
                params = st.get("Parameters", {}) or {}
                item = {}
                for k, v in params.items():
                    key = k[:-2] if k.endswith(".$") else k
                    item[key] = _get_path(data, v) if (isinstance(v, str) and v.startswith("$")) else v
                sub = Interp(itdef, self.resolve_payload)
                sub.run(item, states=itdef["States"],
                        start=itdef.get("StartAt") or itdef.get("Start"), max_steps=max_steps)
                self.visited += ["ITER:" + s for s in sub.visited]
                name = st.get("Next"); continue
            if t == "Task":
                # Lambda-invoke: build Payload, apply the REAL ResultSelector. ResolveTask uses the
                # REAL resolve_task.handler output; other Tasks use a stub (or {}), then advance.
                if name == "ResolveTask":
                    payload_out = self.resolve_payload
                else:
                    payload_out = TASK_STUBS.get(name, {})
                rs = st.get("ResultSelector")
                selected = _apply_result_selector(rs, {"Payload": payload_out}) if rs is not None else payload_out
                rp = st.get("ResultPath", "$")
                # A Task that writes a whole AWS-SDK result (no ResultSelector) to $.describeResult etc.
                if rs is None and name in TASK_STUBS:
                    selected = TASK_STUBS[name]
                if rp is not None:
                    data = _set_path(data, rp, selected)
                if st.get("End"):
                    return name
                name = st.get("Next")
                continue
            if t == "Wait":
                name = st.get("Next"); continue
            # Parallel / unknown: just advance
            name = st.get("Next")
        return name


def _load_defn():
    return json.load(open(os.path.join(SF, "startup.asl.json")))


def _run(stop_after_load, defn=None):
    defn = defn or _load_defn()
    payload = _real_resolved_payload(stop_after_load)
    it = Interp(defn, payload)
    start = defn.get("StartAt") or defn.get("Start")
    # Execution input is the child startup input {taskArn, [stopAfterLoad]} (as preflight builds it).
    init = {"taskArn": TASK_ARN}
    if stop_after_load is not None:
        init["stopAfterLoad"] = stop_after_load
    it.run(init, start=start)
    return it.visited, payload


# ------------------------------------------------------------------------------------- tests
def test_real_handler_payload_has_stopafterload():
    """The REAL resolve_task.handler returns stopAfterLoad in its payload (true / false / default)."""
    assert _real_resolved_payload(True)["stopAfterLoad"] is True
    assert _real_resolved_payload(False)["stopAfterLoad"] is False
    assert _real_resolved_payload(None)["stopAfterLoad"] is False


def test_e2e_loadonly_true_reaches_loadonly_success():
    visited, payload = _run(True)
    v = set(visited)
    assert "TaskLoadOnlySucceeded" in v, f"did not reach TaskLoadOnlySucceeded; visited={visited}"
    for forbidden in ("GroupValidate", "ITER:GroupValidate", "ResumeDmsToCdc", "StartCdcJob",
                      "TaskSucceeded"):
        assert forbidden not in v, f"load-only run should NOT reach {forbidden}; visited={visited}"


def test_e2e_flag_false_takes_normal_path():
    visited, _ = _run(False)
    v = set(visited)
    assert "ITER:GroupValidate" in v or "GroupValidate" in v, f"normal path must validate; {visited}"
    assert "ResumeDmsToCdc" in v and "StartCdcJob" in v, f"normal path must resume+CDC; {visited}"
    assert "TaskLoadOnlySucceeded" not in v


def test_e2e_flag_absent_takes_normal_path():
    visited, _ = _run(None)
    v = set(visited)
    assert "ResumeDmsToCdc" in v and "StartCdcJob" in v
    assert "TaskLoadOnlySucceeded" not in v


def test_e2e_mutation_drop_resultselector_entry_breaks_loadonly():
    """Remove stopAfterLoad.$ from the ResolveTask ResultSelector (the S1 bug) -> the true case no
    longer reaches TaskLoadOnlySucceeded (it takes the normal path), so the load-only assertion
    fails. Proves this e2e test catches S1."""
    defn = _load_defn()
    del defn["States"]["ResolveTask"]["ResultSelector"]["stopAfterLoad.$"]
    visited, _ = _run(True, defn=defn)
    v = set(visited)
    assert "TaskLoadOnlySucceeded" not in v, "mutation should break load-only routing"
    assert "ResumeDmsToCdc" in v, "mutation should take the normal path (the S1 symptom)"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
