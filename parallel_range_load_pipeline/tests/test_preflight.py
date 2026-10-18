#!/usr/bin/env python3
"""Preflight cases for mrl_preflight: DMS settings valid/invalid per mode, adopt_stopped
preconditions, and the capacity impossible-setting guard. Drives the REAL handler with fakes."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_preflight as pf  # noqa: E402
import resolve_task as rt   # noqa: E402
from mrl_fakes import FakeS3, FakeDms  # noqa: E402

BUCKET = "bkt"
ARN = "arn:aws:dms:us-east-1:123456789012:task:ABC123"


def _pipeline():
    return {"project": "demo", "region": "us-east-1",
            "dsql_endpoint": "c.dsql.us-east-1.on.aws",
            "glue_role_arn": "arn:aws:iam::123456789012:role/demo-glue-exec-role"}


def _task(mode, status="stopped", pct=100, errored=0, stop="STOPPED_AFTER_CACHED_EVENTS",
          add_col=True, cached_applied=True):
    settings = json.dumps({"FullLoadSettings": {
        "StopTaskCachedChangesApplied": cached_applied,
        "StopTaskCachedChangesNotApplied": not cached_applied if not cached_applied else False}})
    return {"ReplicationTaskArn": ARN, "ReplicationTaskIdentifier": "ABC123",
            "MigrationType": "full-load-and-cdc" if mode == "full_load_and_cdc" else "full-load",
            "Status": status, "StopReason": "Stop Reason %s" % stop,
            "TargetEndpointArn": "arn:ep", "ReplicationTaskSettings": settings,
            "TableMappings": json.dumps({"rules": [{"rule-type": "selection", "rule-id": "1",
                                                    "rule-action": "include",
                                                    "object-locator": {"schema-name": "%",
                                                                       "table-name": "%"}}]}),
            "ReplicationTaskStats": {"FullLoadProgressPercent": pct, "TablesErrored": errored},
            "_add_col": add_col}


def _pinned_settings(bucket, max_concurrency_source="built-in default", mc_value=3,
                     exec_override=None, extra=None):
    """Build a pinned MRL settings dict like mrl_settings would write (a to_pipeline_settings
    dict + the _mrl markers). Uses the copied parser so it matches production."""
    import mrl_params_csv as pc
    params_text = ("parameter,value\naccount_id,123456789012\nregion,us-east-1\nproject,demo\n"
                   "dsql_endpoint,c.dsql.us-east-1.on.aws\n"
                   "glue_role_arn,arn:aws:iam::123456789012:role/demo-glue-exec-role\n")
    cfg = pc.to_pipeline_settings(pc.parse(params_text)["params"])
    cfg["max_concurrency"] = {"value": mc_value, "source": max_concurrency_source}
    cfg["_explicit_keys"] = []
    cfg["_mrl_execution"] = {"maxConcurrencyOverride": exec_override}
    if extra:
        cfg.update(extra)
    return cfg


def _run(mode, dms_start_mode, task, bucket=BUCKET, add_col=True, pinned=None,
         sfn_mode="empty", seed_unfinished=False, seed_done=False, **knobs):
    s3 = FakeS3()
    # MRL reads the PINNED settings (NOT config/pipeline.json). Seed it under mrl/runs/.
    pinned = pinned if pinned is not None else _pinned_settings(bucket)
    s3.put_object(Bucket=BUCKET, Key="mrl/runs/e1/settings.json",
                  Body=json.dumps(pinned).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task/ABC123/_task.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task_index/ABC123.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    # M8: seed an ORIGINAL-route load doc under config/_task/<suffix>/_orchestrator/ (the guard
    # reads these). unfinished -> a table whose status is not 'done'; done -> all done.
    if seed_unfinished or seed_done:
        st = "done" if seed_done else "loading"
        s3.put_object(Bucket=BUCKET,
                      Key="config/_task/ABC123/_orchestrator/group-0/_load_status.json",
                      Body=json.dumps({"tables": {"s.t": {"status": st, "rows": 10}}}).encode())
    endpoint = {"EngineName": "s3", "S3Settings": {"BucketName": bucket, "BucketFolder": "",
                "AddColumnName": add_col, "CsvNullValue": "NULL"}}
    dms = FakeDms(ARN, [task], endpoint=endpoint, table_mappings=task["TableMappings"])

    class _Denied(Exception):
        def __init__(self):
            super().__init__("AccessDenied")
            self.response = {"Error": {"Code": "AccessDenied"}}

    def fake_client(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            return dms
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    if sfn_mode == "deny":
                        raise _Denied()
                    if sfn_mode == "running":
                        return {"executions": [{"name": "ABC123-run1",
                                "executionArn": "arn:aws:states:us-east-1:123456789012:execution:"
                                "demo-startup:ABC123-run1"}]}
                    return {"executions": []}

                def describe_execution(self, **k):
                    return {"startDate": 0, "input": json.dumps({"taskArn": ARN})}
            return _S()
        if service == "dsql":
            class _D:
                def get_vpc_endpoint_service_name(self, **k):
                    raise Exception("no")
            return _D()
        raise Exception("no fake %r" % service)
    for mod in (pf, rt):
        mod.boto3 = type("b", (), {"client": staticmethod(fake_client)})
    event = {"bucket": BUCKET, "input": {"taskArn": ARN},
             "cdc": (mode == "full_load_and_cdc"),
             "dmsStartMode": dms_start_mode,
             "settingsKey": "s3://%s/mrl/runs/e1/settings.json" % BUCKET,
             "execution": "arn:aws:states:us-east-1:123456789012:execution:demo-mrl-task:e1",
             "stateMachine": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"}
    event.update(knobs)
    return pf.handler(event, None)


def test_cdc_mode_happy():
    out = _run("full_load_and_cdc", "start", _task("full_load_and_cdc"))
    assert out["ok"] and out["startDms"] is True
    assert "STOPPED_AFTER_CACHED_EVENTS" in out["expectedStopReasonSubstrings"]


def test_full_load_only_happy():
    out = _run("full_load_only", "start",
               _task("full_load_only", stop="STOPPED_AFTER_FULL_LOAD"))
    assert out["ok"] and out["mrlMode"] == "full_load_only"
    assert "FULL_LOAD_ONLY_FINISHED" in out["expectedStopReasonSubstrings"]


def test_cdc_mode_rejects_cached_not_applied():
    try:
        _run("full_load_and_cdc", "start", _task("full_load_and_cdc", cached_applied=False))
        assert False
    except pf.MrlPreflightError as e:
        assert "StopTaskCachedChangesApplied" in str(e)


def test_add_column_name_required():
    try:
        _run("full_load_only", "start", _task("full_load_only"), add_col=False)
        assert False
    except pf.MrlPreflightError as e:
        assert "AddColumnName" in str(e)


def test_bucket_must_match():
    try:
        _run("full_load_only", "start", _task("full_load_only"), bucket="other-bucket")
        assert False
    except pf.MrlPreflightError as e:
        assert "bucket" in str(e)


def test_adopt_stopped_ok_with_normal_reason():
    # adopt_stopped accepts any stop reason (incl. NORMAL) when 100% + 0 errored + stopped.
    out = _run("full_load_only", "adopt_stopped",
               _task("full_load_only", stop="NORMAL"))
    assert out["startDms"] is False


def test_adopt_stopped_rejects_not_100():
    try:
        _run("full_load_only", "adopt_stopped", _task("full_load_only", pct=50))
        assert False
    except pf.MrlPreflightError as e:
        assert "FullLoadProgressPercent" in str(e)


def test_adopt_stopped_rejects_errored():
    try:
        _run("full_load_and_cdc", "adopt_stopped", _task("full_load_and_cdc", errored=2))
        assert False
    except pf.MrlPreflightError as e:
        assert "errored" in str(e).lower()


def test_capacity_impossible_setting_fails():
    try:
        _run("full_load_only", "start", _task("full_load_only"),
             maxFilesInParallel=2000, writersPerFile=2000)
        assert False
    except pf.MrlPreflightError as e:
        assert "IMPOSSIBLE" in str(e)


# ---- R22-1: route-conflict guard must FAIL CLOSED on AccessDenied (was swallowed) ----------
def test_r22_1_route_guard_fails_closed_on_access_denied():
    """The guard lists RUNNING original startup/cutover executions. If that call is denied it
    cannot verify the invariant; it MUST fail closed (not swallow and proceed)."""
    try:
        _run("full_load_only", "start", _task("full_load_only",
             stop="STOPPED_AFTER_FULL_LOAD"), sfn_mode="deny")
        assert False, "AccessDenied on ListExecutions must fail closed"
    except pf.MrlPreflightError as e:
        assert "FAIL CLOSED" in str(e) and ("R22-1" in str(e) or "7.6" in str(e))


def test_r22_1_route_guard_access_denied_downgraded_by_override():
    """override=true downgrades the denial to a loud warning (operator accepts the risk)."""
    out = _run("full_load_only", "start", _task("full_load_only", stop="STOPPED_AFTER_FULL_LOAD"),
               sfn_mode="deny", input={"taskArn": ARN, "override": True})
    assert out["ok"] is True


def test_m8_route_guard_fails_on_running_original_execution():
    """A RUNNING original startup execution for THIS task -> fail closed before any DMS resume."""
    try:
        _run("full_load_only", "start", _task("full_load_only",
             stop="STOPPED_AFTER_FULL_LOAD"), sfn_mode="running")
        assert False, "a RUNNING original execution must fail closed"
    except pf.MrlPreflightError as e:
        assert "RUNNING" in str(e) and "startup" in str(e)


def test_m8_route_guard_fails_on_unfinished_original_load():
    """An unfinished original-route _load_status.json under _orchestrator/ -> fail closed."""
    try:
        _run("full_load_only", "start", _task("full_load_only",
             stop="STOPPED_AFTER_FULL_LOAD"), seed_unfinished=True)
        assert False, "an in-progress original-route load must fail closed"
    except pf.MrlPreflightError as e:
        assert "ORIGINAL-route load is in progress" in str(e)


def test_m8_route_guard_allows_when_original_load_done():
    """A fully-done original load doc does NOT block (the table is finished on the other route)."""
    out = _run("full_load_only", "start", _task("full_load_only", stop="STOPPED_AFTER_FULL_LOAD"),
               seed_done=True)
    assert out["ok"] is True


# ---- R3-2: a MISSING original state machine means no conflict (must NOT hard-fail) ----------
class _R3Err(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _R3Sfn:
    """Per-machine behavior keyed by the workflow name in the stateMachineArn."""
    def __init__(self, by_wf):
        self.by_wf = by_wf     # {"startup": mode, "cutover": mode}

    def _mode(self, arn):
        return self.by_wf["cutover" if arn.endswith("-cutover") else "startup"]

    def list_executions(self, stateMachineArn=None, **kw):
        m = self._mode(stateMachineArn)
        if m == "not_found":
            raise _R3Err("StateMachineDoesNotExist")
        if m == "denied":
            raise _R3Err("AccessDenied")
        if m == "running":
            return {"executions": [{"name": "ABC-run",
                    "executionArn": "arn:aws:states:us-east-1:111122223333:execution:"
                    "proj-%s:ABC-run" % ("cutover" if stateMachineArn.endswith("-cutover")
                                         else "startup")}]}
        return {"executions": []}

    def describe_execution(self, **kw):
        return {"input": json.dumps({"taskArn": "arn:aws:dms:us-east-1:111122223333:task:ABC"})}


class _R3S3:
    def list_objects_v2(self, **kw):
        return {"Contents": []}

    def get_object(self, **kw):
        raise _R3Err("NoSuchKey")


def _guard(by_wf, override=False):
    import mrl_preflight as mp
    mp.boto3 = type("x", (), {"client": staticmethod(lambda svc, **k: _R3Sfn(by_wf))})
    warnings = []
    mp._route_conflict_guard(_R3S3(), "b", "ABC", "proj", "us-east-1",
                             "arn:aws:dms:us-east-1:111122223333:task:ABC", override, warnings)
    return warnings


def test_r3_2_not_found_both_machines_passes():
    w = _guard({"startup": "not_found", "cutover": "not_found"})
    assert any("startup state machine not found" in x for x in w)
    assert any("cutover state machine not found" in x for x in w)


def test_r3_2_access_denied_still_fails_closed():
    try:
        _guard({"startup": "denied", "cutover": "empty"})
        assert False, "AccessDenied must still fail closed"
    except pf.MrlPreflightError as e:
        assert "FAIL CLOSED" in str(e)


def test_r3_2_running_execution_still_fails_closed():
    try:
        _guard({"startup": "running", "cutover": "empty"})
        assert False, "a RUNNING original execution must fail closed"
    except pf.MrlPreflightError as e:
        assert "RUNNING" in str(e)


def test_r3_2_mixed_startup_not_found_cutover_running_fails_closed():
    try:
        _guard({"startup": "not_found", "cutover": "running"})
        assert False, "a RUNNING cutover must fail closed even if startup is missing"
    except pf.MrlPreflightError as e:
        assert "cutover" in str(e) and "RUNNING" in str(e)


def test_r3_2_mutation_removing_not_found_branch_breaks_not_found():
    """Mutation: drop the not-found mapping so StateMachineDoesNotExist propagates and the guard
    fails closed; the not-found-passes test would then fail. Prove it on a mutated copy."""
    import ast
    import mrl_preflight as mp
    src = open(mp.__file__).read()
    tree = ast.parse(src)
    seg = None
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef) and n.name == "_route_conflict_guard":
            seg = ast.get_source_segment(src, n)
    assert seg is not None
    mutated = seg.replace(
        'if nf_code in ("StateMachineDoesNotExist", "MachineDoesNotExist",',
        'if nf_code in ("__none__", "MachineDoesNotExist",')
    assert mutated != seg
    ns = {"boto3": type("x", (), {"client": staticmethod(lambda svc, **k: _R3Sfn(
              {"startup": "not_found", "cutover": "not_found"}))}),
          "json": json, "MrlPreflightError": pf.MrlPreflightError,
          "_get_json_safe": pf._get_json_safe}
    exec(mutated, ns)
    try:
        ns["_route_conflict_guard"](_R3S3(), "b", "ABC", "proj", "us-east-1",
                                    "arn:aws:dms:us-east-1:111122223333:task:ABC", False, [])
        assert False, "mutation (no not-found branch) should make not-found fail closed"
    except pf.MrlPreflightError:
        pass
    # and the REAL guard passes not-found (the behavior this mutation guards)
    test_r3_2_not_found_both_machines_passes()


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d preflight test(s) passed" % len(fns))
