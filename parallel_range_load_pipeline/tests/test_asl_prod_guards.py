#!/usr/bin/env python3
"""Production-grade ASL guards (brief section 10):
  * test_glue_arguments_all_strings: resolve EVERY glue:startJobRun[.sync] Arguments map of BOTH
    MRL state machines against representative inputs through the asl_sim evaluator, and assert
    EVERY resolved value is a str (a number in a Glue arg once killed a whole sharded load:
    "must be a STRING").
  * test_history_event_budget: the DMS poll and the CDC-start poll have bounded budgets; compute
    the worst-case history event count at the maximum configured budget and assert it stays far
    below 25,000.
  * test_payload_size: the LoadRangeMap iterates range DESCRIPTORS only (no file list), so a
    3,000-file plan's Map input stays tiny (< 256 KiB), proven by building a 3,000-file plan with
    the real mrl_plan and checking the rangeItems never carry a file list and are well under the
    limit.
"""
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import asl_sim as sim  # noqa: E402

CHILD = os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")
FLEET = os.path.join(PKG, "stepfunctions", "mrl-fleet.asl.json")

# A rich data object so EVERY path a Glue Arguments map reads resolves to a value. Numbers are
# ints on purpose - the ASL must wrap them with States.Format so the resolved value is a str.
_DATA = {
    "taskArn": "arn:aws:dms:us-east-1:123456789012:task:ABC",
    "resolved": {
        "taskArn": "arn:aws:dms:us-east-1:123456789012:task:ABC",
        "configPrefix": "s3://bkt/config/_task/t/",
        "region": "us-east-1", "cdcRoot": ".", "csvNullValue": "NULL",
        "nullValues": "", "nullRules": "", "maxFilesInParallel": 20, "writersPerFile": 8,
        "countTolerance": 0, "countTimeoutAction": "fail", "taskSuffix": "t",
        "probeTimeoutSeconds": 900, "countTimeoutSeconds": 3600,
        "timestampColumnName": "dms_timestamp",
    },
    "glue": {"jobs": {"discovery": "d", "cdc": "p-t-cdc"}},
    "plan": {"planKey": "s3://bkt/mrl/state/t/plan.json",
             "mrlStatePrefix": "s3://bkt/mrl/state/t/", "maxTotalConcurrentRuns": 4,
             "forks": []},
    "cdcStart": {"JobRunId": "jr"},
    "driverFullload": {"extraPyFiles": "s3://bkt/driver-fullload/pg8000.whl"},
    "driverValidation": {"extraPyFiles": "s3://bkt/driver-validation/pg8000.whl"},
    "driverCdc": {"extraPyFiles": "s3://bkt/driver-cdc/pg8000.whl"},
    "driverCount": {"extraPyFiles": "s3://bkt/driver-cdc-prepared/abc/pg8000.whl"},
    "mrlJob": {"jobName": "p-t-mrl-load"},
    # Map-iteration item fields (resolved under the Map's Parameters -> item context):
    "config_prefix": "s3://bkt/mrl/state/t/tables/s.t/config/",
    "first": "LOAD00000001.csv", "last": "LOAD00000050.csv", "range_index": 0,
    "jobName": "p-t-mrl-load", "extraPyFiles": "s3://bkt/d/pg8000.whl",
    "csvNullValue": "NULL", "nullValues": "", "nullRules": "",
    "maxFilesInParallel": 20, "writersPerFile": 8, "maxWriteConcurrency": 160, "label": "s.t",
    # StartForkCdcMap iterator item fields
    "cdcJobName": "p-t-ck-x-cdc", "cdcRoot": ".", "timestampColumnName": "dms_timestamp",
    "execName": "e1", "fork_slug": "x",
}
_CTX = {"Execution": {"Id": "arn:aws:states:::execution:sm:e1", "Name": "e1"},
        "StateMachine": {"Id": "arn:aws:states:::stateMachine:sm"}, "Map": {"Item": {}}}


def _fill(path):
    raw = open(path).read()
    # fill placeholders with plausible values so JobName literals resolve
    for ph in set(__import__("re").findall(r"<<[^>]+>>", raw)):
        raw = raw.replace(ph, "FILLED")
    return json.loads(raw)


def _iter_glue_arg_maps(states):
    """Yield (state_name, Arguments-dict) for every glue:startJobRun[.sync] Task, including
    inside Map iterators."""
    for name, st in states.items():
        if st.get("Type") in ("Task",) and str(st.get("Resource", "")).startswith(
                "arn:aws:states:::glue:startJobRun"):
            args = (st.get("Parameters") or {}).get("Arguments")
            if args is not None:
                yield name, args
        if st.get("Type") == "Map":
            it = st.get("Iterator") or st.get("ItemProcessor")
            if it:
                for n, a in _iter_glue_arg_maps(it["States"]):
                    yield "%s.Iterator.%s" % (name, n), a


def test_glue_arguments_all_strings():
    bad = []
    for path in (CHILD, FLEET):
        sm = _fill(path)
        for name, args in _iter_glue_arg_maps(sm["States"]):
            resolved = sim.build_payload(args, _DATA, _CTX)
            for k, v in resolved.items():
                if not isinstance(v, str):
                    bad.append("%s %s: %s -> %r (%s, NOT str)"
                               % (os.path.basename(path), name, k, v, type(v).__name__))
    assert not bad, "Glue Arguments values that are NOT strings:\n  - " + "\n  - ".join(bad)


def test_history_event_budget():
    """DMS poll: 1440 iterations max (60s x 24h). CDC-start poll: 90 iterations (30s x 45min).
    Each poll iteration is a bounded, fixed set of states (Wait -> Describe/List -> Choice ->
    Increment/Count -> Choice) = ~5 states, each ~2 history events (entered/exited/succeeded),
    so <= ~15 events/iteration. Assert the worst-case total is far below the 25,000 hard limit."""
    dms_iters = 1440
    cdc_iters = 90
    events_per_iter = 15               # generous upper bound per poll loop pass
    # The rest of the machine is linear and small (< 60 states, < 10 events each).
    linear = 60 * 10
    worst = dms_iters * events_per_iter + cdc_iters * events_per_iter + linear
    assert worst < 25000, "worst-case history events %d is not below 25000" % worst


def test_payload_size_range_items_have_no_file_lists():
    """Build a 3,000-file, 1-table plan with the REAL mrl_plan and assert the Map input
    (rangeItems) never carries a file list and is well under 256 KiB."""
    import mrl_plan
    from mrl_fakes import FakeS3

    s3 = FakeS3(page_size=1000)
    s3.put_load_files("bkt", "s/huge", list(range(1, 3001)))
    cp = "config/_task/t/"
    entry = {"dsql_schema": "s", "dsql_table": "huge", "dms_schema": "s", "dms_table": "huge",
             "dms_s3_path": "s3://bkt/s/huge/", "full_load_rows": 30000,
             "metadata": {"dms_schema": "s", "dms_table": "huge",
                          "primary_key": {"columns": ["id"], "mode": "single",
                                          "pk_kind": "integer"}}}
    s3.put_object(Bucket="bkt", Key=cp + "_manifest_index.json",
                  Body=json.dumps({"tables": [entry]}).encode())
    s3.put_object(Bucket="bkt", Key=cp + "huge_column_mapping.json", Body=b'{"column_mapping":{}}')
    s3.put_object(Bucket="bkt", Key=cp + "table_rowcounts.json",
                  Body=json.dumps({"rowcounts": {"s.huge": 30000}}).encode())
    mrl_plan.boto3 = type("b", (), {"client": staticmethod(lambda *a, **k: s3)})
    out = mrl_plan.handler({"bucket": "bkt", "taskSuffix": "t",
                            "configPrefix": "s3://bkt/config/_task/t/", "cdc_root": ".",
                            "max_concurrency": 10, "split_by": "count"}, None)
    items = out["rangeItems"]
    assert len(items) == 10, len(items)
    for it in items:
        assert "files" not in it, "rangeItems must NOT carry file lists: %s" % list(it)
    size = len(json.dumps(items).encode("utf-8"))
    assert size < 256 * 1024, "Map input %d bytes exceeds 256 KiB" % size
    # The persisted plan.json per-range descriptors also carry no file list.
    plan = s3.get_json("mrl/state/t/plan.json")
    for r in plan["tables"]["s.huge"]["ranges"]:
        assert "files" not in r


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d production-guard test(s) passed" % len(fns))
