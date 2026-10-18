#!/usr/bin/env python3
"""Validate asl_sim itself: intrinsic + JSONPath + Choice unit checks, AND replay the EXISTING
stepfunctions/startup.asl.json on a simple happy-path scenario, asserting it reaches
TaskSucceeded. Replaying the real production machine catches interpreter bugs (brief section
11)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)

import asl_sim as sim  # noqa: E402


def test_intrinsics():
    d = {"a": {"b": 3}, "arr": [{"status": "ok"}, {"status": "failed"}]}
    ctx = {"Execution": {"Name": "exec1"}}
    assert sim.eval_intrinsic("States.Format('{}-{}', $.a.b, 'x')", d, ctx) == "3-x"
    assert sim.eval_intrinsic("States.MathAdd($.a.b, 1)", d, ctx) == 4
    assert sim.eval_intrinsic("States.ArrayContains($.arr[*].status, 'failed')", d, ctx) is True
    assert sim.eval_intrinsic("States.ArrayContains($.arr[*].status, 'nope')", d, ctx) is False


def test_jsonpath_projection_and_index():
    d = {"arr": [{"s": 1}, {"s": 2}], "t": [{"Status": "stopped"}]}
    assert sim._resolve_projection("$.arr[*].s", d, {}) == [1, 2]
    assert sim.resolve_path("$.t[0].Status", d, {}) == "stopped"


def test_choice_operators():
    d = {"x": "stopped", "n": 5, "b": True, "reason": "STOPPED_AFTER_FULL_LOAD stuff"}
    ctx = {}
    assert sim.eval_choice_rule({"Variable": "$.x", "StringEquals": "stopped"}, d, ctx)
    assert sim.eval_choice_rule({"Variable": "$.n", "NumericEquals": 5}, d, ctx)
    assert sim.eval_choice_rule({"Variable": "$.n", "NumericGreaterThan": 4}, d, ctx)
    assert sim.eval_choice_rule({"Variable": "$.b", "BooleanEquals": True}, d, ctx)
    assert sim.eval_choice_rule({"Variable": "$.reason", "StringMatches": "*STOPPED_AFTER_FULL_LOAD*"}, d, ctx)
    assert sim.eval_choice_rule({"Variable": "$.missing", "IsPresent": False}, d, ctx)
    assert not sim.eval_choice_rule({"Variable": "$.missing", "StringEquals": "x"}, d, ctx)
    assert sim.eval_choice_rule(
        {"And": [{"Variable": "$.x", "StringEquals": "stopped"},
                 {"Variable": "$.n", "NumericEquals": 5}]}, d, ctx)


def test_result_path_and_params():
    d = {"a": 1}
    out = sim.build_payload({"keep": "lit", "copy.$": "$.a"}, d, {})
    assert out == {"keep": "lit", "copy": 1}
    d2 = sim._set_path({"a": 1}, "$.b.c", {"z": 9})
    assert d2["b"]["c"] == {"z": 9}
    assert sim._set_path({"a": 1}, None, {"x": 1}) == {"a": 1}       # discard
    assert sim._set_path({"a": 1}, "$", {"x": 1}) == {"x": 1}        # replace


def test_startup_happy_path_reaches_success():
    """Replay the REAL startup.asl.json on a happy path with pluggable fakes. DMS reports
    STOPPED_AFTER_CACHED_EVENTS immediately; all Lambda/Glue tasks succeed; the CDC start
    marker is 'present' on the first check; no composite forks. Must reach TaskSucceeded."""
    sm = json.load(open(os.path.join(REPO, "stepfunctions", "startup.asl.json")))

    def lambda_invoke(resource, params, ctx):
        fn = params.get("FunctionName", "")
        pl = params.get("Payload", {})
        # resolve_task shared/build_table_list/write_override; driver-discovery; create-glue-jobs;
        # plan-split. Return just enough for the ResultSelectors to pick.
        if "RESOLVE_TASK" in fn or pl.get("mode") in ("startup", "build_table_list"):
            if pl.get("mode") == "build_table_list":
                return {"Payload": {"count": 1, "distinctSchemas": ["s"], "manifestKey": "k",
                                    "replacedExisting": False}}
            # shared startup resolve
            return {"Payload": {
                "taskArnList": ["arn:task"], "taskName": "t", "taskSuffix": "t",
                "configPrefix": "s3://b/config/_task/t/", "override": False,
                "stopAfterLoad": False, "project": "p",
                "region": "us-east-1", "dsqlEndpoint": "e", "dsqlEndpointCandidates": "e",
                "dsqlUser": "admin", "dsqlDatabase": "postgres", "glueRoleArn": "r",
                "glueConnection": "", "cdcEngine": "pythonshell", "controlSchema": "cdc_control",
                "cdcJobName": "p-t-cdc", "dmsS3Base": "s3://b/", "s3Bucket": "b", "cdcRoot": ".",
                "timestampColumnName": "ts", "warnings": [], "csvNullValue": "NULL",
                "cdcSparkFallback": True, "cdcValidation": True, "cdcValidationSample": 20,
                "cdcMaxDeleteFraction": 0.5, "cdcMaxDeleteRows": 100000,
                "cdcDriftCheckMinutes": 30, "cdcDriftTolerance": 0, "cdcDriftAction": "warn",
                "guardrailsMode": "warn", "cdcFileOrderAction": "warn",
                "cdcNopkOvermatchAction": "warn", "validateCountCheck": "warn",
                "cutoverCountCheck": "warn", "accountId": "123456789012",
                "maxCompositeForks": 8, "maxBigCdcForks": 8, "bigTableRowThreshold": 6000000,
                "fileFanoutThreshold": 8, "bigTableBytesThreshold": 1000000000, "maxGroups": 10,
                "mapMaxConcurrency": 6, "maxFilesInParallel": 30, "writersPerFile": 8,
                "connBudget": 900, "minWritersPerLoader": 100, "maxWritersPerLoader": 150,
                "glueVersion": "4.0", "discoveryWorkerType": "G.2X", "discoveryNumWorkers": 5,
                "discoveryTimeoutMinutes": 480, "loadWorkerType": "G.4X", "loadNumWorkers": 10,
                "loadTimeoutMinutes": 2880, "loadBigWorkerType": "G.8X", "loadBigNumWorkers": 10,
                "loadBigTimeoutMinutes": 2880, "validateWorkerType": "G.8X",
                "validateNumWorkers": 10, "validateTimeoutMinutes": 2880,
                "validateRowsPerRange": 10000, "validateParallelism": 0,
                "validateTargetSecondsPerRange": 12, "validateHash": "all",
                "maxParallelTables": 20, "nullValues": "", "nullRules": "",
                "nullNotnullText": "off",
                "perWorkerMemBudgetMb": 1500}}
        if "DRIVER_DISCOVERY" in fn:
            return {"Payload": {"extraPyFiles": "s3://b/d/pg8000.whl", "engine": "pythonshell",
                                "fallbackReason": ""}}
        if "CREATE_GLUE_JOBS" in fn:
            return {"Payload": {"jobs": {"discovery": "p-t-discovery", "load": "p-t-load",
                                         "load-big": "p-t-load-big", "validate": "p-t-validate",
                                         "cdc": "p-t-cdc"}, "cdcEngine": "pythonshell",
                                "created": [], "updated": []}}
        if "PLAN_SPLIT" in fn:
            # one small group, no forks
            return {"Payload": {"groups": [{"group_index": 0, "config_prefix": "s3://b/g0/",
                                            "kind": "small", "load_args": {
                                                "--max_files_in_parallel": "30",
                                                "--writers_per_file": "8",
                                                "--max_write_concurrency": "150"},
                                            "loadJobName": "p-t-load",
                                            "loadBigJobName": "p-t-load-big",
                                            "validateJobName": "p-t-validate"}],
                                "group_count": 1, "forks": [], "fork_count": 0}}
        return {"Payload": {}}

    def dms_describe(resource, params, ctx):
        return {"ReplicationTasks": [{"Status": "stopped",
                "StopReason": "Stop Reason STOPPED_AFTER_CACHED_EVENTS",
                "ReplicationTaskStats": {"FullLoadProgressPercent": 100, "TablesErrored": 0}}]}

    def dms_start(resource, params, ctx):
        return {"ReplicationTask": {"Status": "starting"}}

    def glue_start_sync(resource, params, ctx):
        return {"JobRunId": "jr1", "JobRunState": "SUCCEEDED"}

    def glue_start(resource, params, ctx):
        return {"JobRunId": "jr-cdc"}

    def glue_getrun(resource, params, ctx):
        return {"JobRun": {"JobRunState": "RUNNING"}}

    def s3_list(resource, params, ctx):
        return {"KeyCount": 1}      # CDC start marker present immediately

    handlers = {
        "arn:aws:states:::lambda:invoke": lambda_invoke,
        "arn:aws:states:::aws-sdk:databasemigration:describeReplicationTasks": dms_describe,
        "arn:aws:states:::aws-sdk:databasemigration:startReplicationTask": dms_start,
        "arn:aws:states:::glue:startJobRun.sync": glue_start_sync,
        "arn:aws:states:::glue:startJobRun": glue_start,
        "arn:aws:states:::aws-sdk:glue:getJobRun": glue_getrun,
        "arn:aws:states:::aws-sdk:s3:listObjectsV2": s3_list,
    }
    ctx = {"Execution": {"Id": "arn:aws:states:us-east-1:123456789012:execution:p-startup:e1",
                         "Name": "e1"},
           "StateMachine": {"Id": "arn:aws:states:us-east-1:123456789012:stateMachine:p-startup"},
           "Map": {"Item": {}}}
    interp = sim.Interpreter(sm, handlers, ctx)
    out = interp.run({"taskArn": "arn:aws:dms:us-east-1:123456789012:task:ABC"})
    assert out["status"] == "SUCCEEDED", (out, interp.trace[-8:])
    assert out["state"] == "TaskSucceeded", (out["state"], interp.trace[-8:])


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d asl_sim self-check(s) passed" % len(fns))
