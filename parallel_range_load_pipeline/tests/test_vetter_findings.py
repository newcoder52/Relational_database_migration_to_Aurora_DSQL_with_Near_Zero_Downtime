#!/usr/bin/env python3
"""Executing tests for RE-VETTER A findings (vet/BLOCKERS_A.md / FINDINGS_A.md).

A1 zero-byte LOAD file: excluded from the plan set (matching the loader) but reported; plan ==
   loader set holds (no false 'missing').
A2 hex export: hex allowed by default (Amendment 6), so the plan matches the loose loader; a
   decimal-only plan over a hex folder FAILS CLOSED via the cross-check.
A3 partition sub-folder LOAD files (parallel full load): the loader loads them, MRL plans only
   direct children -> the cross-check FAILS CLOSED naming the sub-folder keys.
A4 stray 'load_notes.csv': loader loads it, plan rejects it -> cross-check FAILS CLOSED.
A5 re-run with a DIFFERENT max_concurrency once a range is loaded -> FAIL CLOSED; clean-empty ->
   rebuild.
A6 --max_write_concurrency is max_files_in_parallel x writers_per_file (NOT writers_per_file).
A7 child execution name is deterministic (no wall clock); the fleet ASL treats
   ExecutionAlreadyExists as a skip.
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

import mrl_split as ms          # noqa: E402
import mrl_plan as mp           # noqa: E402
from mrl_fakes import FakeS3    # noqa: E402

DMS = "s3://b/s/t/"


# ---- A1: zero-byte ------------------------------------------------------------------------
def test_a1_zero_byte_excluded_reported_and_crosscheck_ok():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [(1, 1000), (2, 0), (3, 500)])
    files, info = ms.list_table_files(s3, DMS)
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv", "LOAD00000003.csv"]
    assert info["zero_byte_count"] == 1
    # loader also skips size<=0, so the cross-check passes (no false missing/extra)
    ms.assert_plan_equals_loader_set(s3, DMS, files)


# ---- A2: hex default vs decimal-only fail-closed ------------------------------------------
def test_a2_hex_default_matches_loader():
    s3 = FakeS3()
    for n in ("LOAD00000001.csv", "LOAD0000000f.csv"):
        s3.put_raw("b", "s/t/%s" % n, size=100)
    files, _ = ms.list_table_files(s3, DMS)              # hex allowed by default
    assert len(files) == 2
    ms.assert_plan_equals_loader_set(s3, DMS, files)     # matches the loose loader
    # decimal-only plan over a hex folder -> the hex file is in S_loader not S_plan -> fail closed
    dec_files, _ = ms.list_table_files(s3, DMS, allow_hex=False)
    try:
        ms.assert_plan_equals_loader_set(s3, DMS, dec_files, allow_hex=False)
        assert False, "decimal-only plan over a hex folder must fail closed"
    except ms.MrlSplitError as e:
        assert "load0000000f.csv" in str(e).lower()


# ---- A3: partition sub-folder (parallel full load) fail-closed ----------------------------
def test_a3_partition_subfolder_fails_closed():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [1, 2])                  # root files
    s3.put_raw("b", "s/t/1/LOAD00000003.csv", size=100)    # parallel-load partition sub-folder
    files, _ = ms.list_table_files(s3, DMS)                # MRL lists only direct children
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv", "LOAD00000002.csv"]
    try:
        ms.assert_plan_equals_loader_set(s3, DMS, files)
        assert False, "a partition sub-folder LOAD file must fail closed (loader would load it)"
    except ms.MrlSplitError as e:
        assert "1/LOAD00000003.csv" in str(e) and "SUB-FOLDER" in str(e)


# ---- A4: stray load_notes.csv fail-closed -------------------------------------------------
def test_a4_stray_loose_name_fails_closed():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [1])
    s3.put_raw("b", "s/t/load_notes.csv", size=100)   # loader's loose rule loads it; plan rejects
    files, _ = ms.list_table_files(s3, DMS)
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv"]
    try:
        ms.assert_plan_equals_loader_set(s3, DMS, files)
        assert False, "a stray load_notes.csv must fail closed"
    except ms.MrlSplitError as e:
        assert "load_notes.csv" in str(e)


# ---- A5: re-run with a changed max_concurrency --------------------------------------------
def _plan_seed(s3, files_n, rows, single_pk=True):
    schema, table = "s", "t"
    s3.put_load_files("b", "%s/%s" % (schema, table), list(range(1, files_n + 1)))
    pk = {"columns": ["id"], "mode": "single", "pk_kind": "integer"} if single_pk \
        else {"columns": ["a", "b"], "mode": "composite"}
    entry = {"dsql_schema": schema, "dsql_table": table, "dms_schema": schema, "dms_table": table,
             "dms_s3_path": "s3://b/%s/%s/" % (schema, table), "full_load_rows": rows,
             "metadata": {"dms_schema": schema, "dms_table": table, "primary_key": pk,
                          "full_load_rows": rows, "dms_s3_path": "s3://b/%s/%s/" % (schema, table)}}
    s3.put_object(Bucket="b", Key="config/_task/t/_manifest_index.json",
                  Body=json.dumps({"tables": [entry]}).encode())
    s3.put_object(Bucket="b", Key="config/_task/t/t_column_mapping.json",
                  Body=b'{"column_mapping":{}}')
    s3.put_object(Bucket="b", Key="config/_task/t/table_rowcounts.json",
                  Body=json.dumps({"rowcounts": {"s.t": rows}}).encode())


def _event(c):
    return {"bucket": "b", "taskSuffix": "t", "configPrefix": "s3://b/config/_task/t/",
            "cdc_root": ".", "max_concurrency": c, "split_by": "count"}


def test_a5_rerun_changed_concurrency_fails_closed_once_loaded():
    s3 = FakeS3()
    _plan_seed(s3, 6, rows=60)
    mp.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    out = mp.handler(_event(3), None)                       # plan with c=3 -> 2,2,2
    # simulate a loaded range (a per-range status doc under the table config prefix)
    cp_key = out["tables"][0]["config_prefix"][5:].split("/", 1)[1]
    s3.put_object(Bucket="b", Key=cp_key + "_file_status/s.t.range-LOAD00000001.csv-LOAD00000002.csv.json",
                  Body=json.dumps({"files": {}}).encode())
    try:
        mp.handler(_event(4), None)                         # re-run with c=4 -> fail closed
        assert False, "changed max_concurrency after a loaded range must fail closed"
    except mp.MrlPlanError as e:
        assert "DIFFERENT max_concurrency" in str(e)


def test_a5_rerun_changed_concurrency_clean_empty_rebuilds():
    s3 = FakeS3()
    _plan_seed(s3, 6, rows=60)
    mp.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    mp.handler(_event(3), None)                             # plan with c=3
    # NO loaded range -> a re-run with c=2 rebuilds cleanly (no fail)
    out2 = mp.handler(_event(2), None)
    assert [r["size"] for r in out2["tables"][0]["ranges"]] == [3, 3]


# ---- A6: --max_write_concurrency is mfip*wpf ----------------------------------------------
def test_a6_max_write_concurrency_is_mfip_times_wpf():
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    rr = d["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]["Parameters"]["Arguments"]
    assert rr["--max_write_concurrency.$"] == "States.Format('{}', $.maxWriteConcurrency)"
    assert d["States"]["LoadRangeMap"]["Parameters"]["maxWriteConcurrency.$"] == \
        "$.resolved.maxWriteConcurrency"
    # preflight emits mfip*wpf
    import mrl_preflight
    assert "maxWriteConcurrency.$" in d["States"]["Preflight"]["ResultSelector"]


# ---- A7: deterministic child name + ExecutionAlreadyExists skip ---------------------------
def test_a7_deterministic_child_name():
    import mrl_fleet_preflight as fp
    src = open(fp.__file__).read()
    assert "time.strftime" not in src, "child run name must not use a wall clock (A7)"
    assert "mrl-" in src
    # fleet ASL treats ExecutionAlreadyExists as a skip
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-fleet.asl.json")))
    it = d["States"]["FanOut"]["Iterator"]["States"]
    catches = it["StartChild"]["Catch"]
    first = catches[0]
    assert "States.ExecutionAlreadyExists" in first["ErrorEquals"]
    assert first["Next"] == "ChildAlreadyRunning"
    assert it["ChildAlreadyRunning"]["Parameters"]["status"] == "skipped"


def test_a45_driver_extras_parity():
    """A45 (Amendment 9.1) + A13-A: the range-load job gets the FULLLOAD extras (same as the
    original load-big), the reused CDC jobs the CDC extras, and the PYTHON-SHELL count/probe job
    the PREPARED (Requires-Dist-stripped) driver-cdc set from DriverDiscoveryCount (NOT the
    Spark-style driverValidation set, which made Glue pip hang on PyPI in a no-internet VPC)."""
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    S = d["States"]
    assert S["EnsureMrlLoadJob"]["Parameters"]["Payload"]["extraPyFiles.$"] == \
        "$.driverFullload.extraPyFiles"
    assert S["LoadRangeMap"]["Parameters"]["extraPyFiles.$"] == "$.driverFullload.extraPyFiles"
    for st in ("SafetyProbe", "CountVerify"):
        assert S[st]["Parameters"]["Arguments"]["--extra-py-files.$"] == \
            "$.driverCount.extraPyFiles", "%s must use the prepared python-shell set (A13)" % st
    # DriverDiscoveryCount prepares it with prepare_for=pythonshell (no spark fallback), BEFORE
    # the probe, in the unconditional discovery chain (so cdc=false runs get it too).
    dc = S["DriverDiscoveryCount"]["Parameters"]["Payload"]
    assert dc["prepare_for"] == "pythonshell" and dc["drivers_prefix"] == "driver-cdc"
    assert dc["spark_fallback"] is False
    assert S["DriverDiscoveryCdc"]["Next"] == "DriverDiscoveryCount"
    assert S["DriverDiscoveryCount"]["Next"] == "CreateGlueJobs"
    assert S["CreateGlueJobs"]["Parameters"]["Payload"].get("cdcExtraPyFiles.$") == \
        "$.driverCdc.extraPyFiles"


def test_a13_probe_count_fail_fast_timeouts():
    """A13-A.2 (field fixed by A14): the .sync Glue probe/count states carry TimeoutSecondsPath
    (NOT the invalid TimeoutSeconds.$) and a States.Timeout Catch to a clear MrlProbeTimedOut /
    MrlCountTimedOut Fail, so a no-internet pip hang fails in minutes, not ~20 min."""
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    S = d["States"]
    for st, secs, fail in (("SafetyProbe", "$.resolved.probeTimeoutSeconds", "MrlProbeTimedOut"),
                           ("CountVerify", "$.resolved.countTimeoutSeconds", "MrlCountTimedOut")):
        assert S[st]["Resource"] == "arn:aws:states:::glue:startJobRun.sync"
        assert "TimeoutSeconds.$" not in S[st], "%s must NOT use the invalid TimeoutSeconds.$" % st
        assert S[st].get("TimeoutSecondsPath") == secs, "%s must set TimeoutSecondsPath" % st
        catch0 = S[st]["Catch"][0]
        assert catch0["ErrorEquals"] == ["States.Timeout"] and catch0["Next"] == fail, \
            "%s must Catch States.Timeout -> %s" % (st, fail)
        assert S[fail]["Type"] == "Fail" and S[fail]["Error"] == fail


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d vetter-finding test(s) passed" % len(fns))
