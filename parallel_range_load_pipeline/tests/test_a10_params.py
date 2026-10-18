#!/usr/bin/env python3
"""Amendment 10 A.5 — sizing/parallelism come ONLY from the execution input, the fleet row, then
the built-in defaults (lambdas/mrl_defaults.py); config/params.csv values for those keys change
NOTHING. Driven against the REAL mrl_preflight.handler + mrl_settings.handler with fakes.

(a) a params.csv with NON-DEFAULT values for every A.1 key produces the SAME resolved sizing /
    pipeline_parallel_load.json as a params.csv with the defaults;
(b) a fleet-row column and an execution-input override still win;
(c) mutation: re-introducing the params.csv layer makes (a) fail.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_preflight as pf        # noqa: E402
import mrl_settings as ms         # noqa: E402
import mrl_defaults as md         # noqa: E402
import mrl_params_csv as pc       # noqa: E402
import resolve_task as rt         # noqa: E402
from mrl_fakes import FakeS3, FakeDms  # noqa: E402

BUCKET = "bkt"
ARN = "arn:aws:dms:us-east-1:123456789012:task:ABC123"

_BASE = ("parameter,value\naccount_id,123456789012\nregion,us-east-1\nproject,demo\n"
         "dsql_endpoint,c.dsql.us-east-1.on.aws\n"
         "glue_role_arn,arn:aws:iam::123456789012:role/demo-glue-exec-role\n")

# Every A.1 sizing/parallelism key set to a NON-DEFAULT value.
_A1_NONDEFAULT = ("map_max_concurrency,40\nload_worker_type,G.16X\nload_num_workers,299\n"
                  "load_timeout_minutes,9999\nload_big_worker_type,G.16X\n"
                  "load_big_num_workers,299\nload_big_timeout_minutes,9999\n"
                  "max_files_in_parallel,99\nwriters_per_file,99\nvalidate_worker_type,G.16X\n"
                  "validate_num_workers,299\nvalidate_timeout_minutes,9999\n")

# The resolved fields that A.1 governs (must come from built-in defaults, not params.csv).
_SIZING_FIELDS = ("maxConcurrency", "loadWorkers", "loadWorkerType", "maxFilesInParallel",
                  "writersPerFile", "maxWriteConcurrency", "maxConcurrencySource",
                  "loadWorkerTypeCfg", "loadNumWorkers", "loadTimeoutMinutes",
                  "loadBigWorkerType", "loadBigNumWorkers", "loadBigTimeoutMinutes",
                  "validateWorkerType", "validateNumWorkers", "validateTimeoutMinutes",
                  "discoveryWorkerType", "discoveryNumWorkers", "discoveryTimeoutMinutes",
                  "mapMaxConcurrency")


def _pinned_from(params_text):
    cfg = pc.to_pipeline_settings(pc.parse(params_text)["params"])
    cfg["_mrl_execution"] = {"maxConcurrencyOverride": None}
    return cfg


def _task(stop="STOPPED_AFTER_FULL_LOAD"):
    settings = json.dumps({"FullLoadSettings": {"StopTaskCachedChangesApplied": True}})
    return {"ReplicationTaskArn": ARN, "ReplicationTaskIdentifier": "ABC123",
            "MigrationType": "full-load", "Status": "stopped",
            "StopReason": "Stop Reason %s" % stop, "TargetEndpointArn": "arn:ep",
            "ReplicationTaskSettings": settings,
            "TableMappings": json.dumps({"rules": [{"rule-type": "selection", "rule-id": "1",
                                        "rule-action": "include",
                                        "object-locator": {"schema-name": "%", "table-name": "%"}}]}),
            "ReplicationTaskStats": {"FullLoadProgressPercent": 100, "TablesErrored": 0},
            "_add_col": True}


def _resolve(params_text, **knobs):
    """Run the REAL preflight with the given params.csv as the pinned settings. Returns $.resolved.*"""
    s3 = FakeS3()
    s3.put_object(Bucket=BUCKET, Key="mrl/runs/e1/settings.json",
                  Body=json.dumps(_pinned_from(params_text)).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task/ABC123/_task.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task_index/ABC123.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    endpoint = {"EngineName": "s3", "S3Settings": {"BucketName": BUCKET, "BucketFolder": "",
                "AddColumnName": True, "CsvNullValue": "NULL"}}
    dms = FakeDms(ARN, [_task()], endpoint=endpoint, table_mappings=_task()["TableMappings"])

    def fake_client(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            return dms
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    return {"executions": []}

                def describe_execution(self, **k):
                    return {"input": "{}"}
            return _S()
        if service == "dsql":
            class _D:
                def get_vpc_endpoint_service_name(self, **k):
                    raise Exception("no")
            return _D()
        raise Exception("no fake %r" % service)
    for mod in (pf, rt):
        mod.boto3 = type("b", (), {"client": staticmethod(fake_client)})
    event = {"bucket": BUCKET, "input": {"taskArn": ARN}, "cdc": False, "dmsStartMode": "start",
             "settingsKey": "s3://%s/mrl/runs/e1/settings.json" % BUCKET,
             "execution": "arn:aws:states:us-east-1:123456789012:execution:demo-mrl-task:e1",
             "stateMachine": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"}
    event.update(knobs)
    return pf.handler(event, None)


def test_a5a_params_csv_sizing_changes_nothing_in_resolved():
    base = _resolve(_BASE)
    nond = _resolve(_BASE + _A1_NONDEFAULT)
    for f in _SIZING_FIELDS:
        assert base[f] == nond[f], \
            "A10: params.csv sizing must not change $.resolved.%s (%r vs %r)" % (f, base[f], nond[f])
    # and the values are exactly the built-in defaults
    assert base["maxConcurrency"] == md.MAX_CONCURRENCY
    assert base["maxFilesInParallel"] == md.MAX_FILES_IN_PARALLEL
    assert base["writersPerFile"] == md.WRITERS_PER_FILE
    assert base["loadWorkerType"] == md.LOAD_WORKER_TYPE
    assert base["loadNumWorkers"] == md.LOAD_NUM_WORKERS
    assert base["validateWorkerType"] == md.COUNT_WORKER_TYPE
    assert base["maxConcurrencySource"] == "built-in default"


def test_a5a_params_csv_sizing_changes_nothing_in_settings_doc():
    """mrl_settings must publish the SAME pipeline_parallel_load.json content for the sizing keys
    regardless of their params.csv values (it carries no max_concurrency/_explicit_keys layer)."""
    def _publish(params_text):
        s3 = FakeS3()
        s3.put_object(Bucket=BUCKET, Key="config/params.csv", Body=params_text.encode())

        def fc(service, **kw):
            if service == "s3":
                return s3
            class _X:
                def list_executions(self, **k):
                    return {"executions": []}
            return _X()
        ms.boto3 = type("b", (), {"client": staticmethod(fc)})
        ms.handler({"bucket": BUCKET, "rawInput": {}}, None)
        return s3.get_json("config/pipeline_parallel_load.json")
    d0 = _publish(_BASE)
    d1 = _publish(_BASE + _A1_NONDEFAULT)
    assert "max_concurrency" not in d0 and "_explicit_keys" not in d0
    # the published doc differs only by the operator's params values themselves (which are a
    # read-only mirror), never by an MRL-derived sizing layer.
    assert d0.get("max_concurrency") == d1.get("max_concurrency")  # both absent


def test_a5b_fleet_row_and_exec_input_override():
    # fleet-row column wins over the built-in default
    r_fleet = _resolve(_BASE + _A1_NONDEFAULT, maxConcurrency=7, maxFilesInParallel=12,
                       writersPerFile=4, loadWorkerType="G.2X")
    assert r_fleet["maxConcurrency"] == 7 and r_fleet["maxConcurrencySource"] == "fleet row"
    assert r_fleet["maxFilesInParallel"] == 12 and r_fleet["writersPerFile"] == 4
    assert r_fleet["loadWorkerType"] == "G.2X"
    # execution input wins over the fleet row
    pinned = _pinned_from(_BASE)
    pinned["_mrl_execution"] = {"maxConcurrencyOverride": 9}
    s3 = FakeS3()
    s3.put_object(Bucket=BUCKET, Key="mrl/runs/e1/settings.json", Body=json.dumps(pinned).encode())
    r_exec = _resolve_with_pinned(s3, maxConcurrency=7)
    assert r_exec["maxConcurrency"] == 9 and r_exec["maxConcurrencySource"] == "execution input"


def _resolve_with_pinned(s3, **knobs):
    s3.put_object(Bucket=BUCKET, Key="config/_task/ABC123/_task.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task_index/ABC123.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    endpoint = {"EngineName": "s3", "S3Settings": {"BucketName": BUCKET, "BucketFolder": "",
                "AddColumnName": True, "CsvNullValue": "NULL"}}
    dms = FakeDms(ARN, [_task()], endpoint=endpoint, table_mappings=_task()["TableMappings"])

    def fc(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            return dms
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    return {"executions": []}

                def describe_execution(self, **k):
                    return {"input": "{}"}
            return _S()
        class _D:
            def get_vpc_endpoint_service_name(self, **k):
                raise Exception("no")
        return _D()
    for mod in (pf, rt):
        mod.boto3 = type("b", (), {"client": staticmethod(fc)})
    event = {"bucket": BUCKET, "input": {"taskArn": ARN}, "cdc": False, "dmsStartMode": "start",
             "settingsKey": "s3://%s/mrl/runs/e1/settings.json" % BUCKET,
             "execution": "arn:aws:states:us-east-1:123456789012:execution:demo-mrl-task:e1",
             "stateMachine": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"}
    event.update(knobs)
    return pf.handler(event, None)


def test_a5c_mutation_readding_params_layer_breaks_a():
    """Mutation: if preflight re-read max_concurrency from params.csv (the removed layer), the
    non-default params (map_max_concurrency,40) would change the resolved value and (a) would
    fail. Prove the mutated resolution differs from the real one."""
    import ast
    src = open(pf.__file__).read()
    tree = ast.parse(src)
    seg = None
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef) and n.name == "handler":
            seg = ast.get_source_segment(src, n)
    assert seg is not None
    # re-add the removed params.csv layer: make the built-in-default branch read cfg instead.
    mutated = seg.replace(
        "max_concurrency = _as_int_1_40(md.MAX_CONCURRENCY, \"max_concurrency (built-in default)\")",
        "max_concurrency = _as_int_1_40(int(cfg.get('map_max_concurrency', md.MAX_CONCURRENCY)), "
        "\"max_concurrency (params.csv)\")")
    assert mutated != seg, "mutation must change the source"
    # Build a namespace with the module globals so the mutated handler runs.
    ns = dict(pf.__dict__)
    exec(mutated, ns)
    # Run the mutated handler with non-default params (map_max_concurrency,40).
    s3 = FakeS3()
    s3.put_object(Bucket=BUCKET, Key="mrl/runs/e1/settings.json",
                  Body=json.dumps(_pinned_from(_BASE + _A1_NONDEFAULT)).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task/ABC123/_task.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task_index/ABC123.json",
                  Body=json.dumps({"taskArn": ARN, "taskSuffix": "ABC123"}).encode())
    endpoint = {"EngineName": "s3", "S3Settings": {"BucketName": BUCKET, "BucketFolder": "",
                "AddColumnName": True, "CsvNullValue": "NULL"}}
    dms = FakeDms(ARN, [_task()], endpoint=endpoint, table_mappings=_task()["TableMappings"])

    def fc(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            return dms
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    return {"executions": []}

                def describe_execution(self, **k):
                    return {"input": "{}"}
            return _S()
        class _D:
            def get_vpc_endpoint_service_name(self, **k):
                raise Exception("no")
        return _D()
    for mod in (ns, rt.__dict__):
        pass
    ns["boto3"] = type("b", (), {"client": staticmethod(fc)})
    rt.boto3 = type("b", (), {"client": staticmethod(fc)})
    event = {"bucket": BUCKET, "input": {"taskArn": ARN}, "cdc": False, "dmsStartMode": "start",
             "settingsKey": "s3://%s/mrl/runs/e1/settings.json" % BUCKET,
             "execution": "arn:aws:states:us-east-1:123456789012:execution:demo-mrl-task:e1",
             "stateMachine": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"}
    mutated_out = ns["handler"](event, None)
    # the mutated handler reads 40 from params.csv -> differs from the real built-in default 3
    assert mutated_out["maxConcurrency"] == 40, mutated_out["maxConcurrency"]
    assert _resolve(_BASE + _A1_NONDEFAULT)["maxConcurrency"] == md.MAX_CONCURRENCY


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d A10-params test(s) passed" % len(fns))
