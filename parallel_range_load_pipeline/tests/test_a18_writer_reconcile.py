#!/usr/bin/env python3
"""Amendment 18.4 - drive the never-run-live chain WRITER -> RECONCILE -> CDC-HANDOFF offline.

We synthesize the EXACT docs the MRL loader writes for a finished range (the range-private
`_file_status/<label>.range-<first>-<last>.json` = {"files": {uri: {"state":"done","rows":N}}} and
the per-range `_range_result/...json` with failed/first/last - the shapes written by
scripts/job2_load_manual_range.py `_mr_write_result` + the file-status upsert), feed them to the
REAL mrl_reconcile.handler, and assert: allClean True; the merged `_file_status/<label>.json` is in
the ORIGINAL format the loader's load_file_status reads ({"files":{uri:{"state":"done","rows":N}}}).
Then run the REAL mrl_cdc_handoff against the plan (cdc=true) and assert the fork prefix handoff.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_reconcile as mr        # noqa: E402
import mrl_cdc_handoff as ch      # noqa: E402
from mrl_fakes import FakeS3      # noqa: E402

BUCKET = "b"; SUFFIX = "t"; LABEL = "s.t"
CP_KEY = "mrl/state/t/tables/s.t/config/"
DMS = "s3://b/s/t/"


def _uri(n): return "s3://b/s/t/LOAD%08d.csv" % n


def _seed_two_ranges(s3):
    """Two finished ranges covering LOAD1..4, in the EXACT doc shapes the MRL loader writes."""
    s3.put_load_files(BUCKET, "s/t", [1, 2, 3, 4])
    # range A = LOAD1..2 ; range B = LOAD3..4
    for (a, b, u1, u2, r1, r2) in [(1, 2, _uri(1), _uri(2), 10, 20),
                                   (3, 4, _uri(3), _uri(4), 30, 40)]:
        rk = "LOAD%08d.csv-LOAD%08d.csv" % (a, b)
        # range-private _file_status doc (loader's load_file_status format)
        s3.put_object(Bucket=BUCKET, Key="%s_file_status/%s.range-%s.json" % (CP_KEY, LABEL, rk),
                      Body=json.dumps({"files": {u1: {"state": "done", "rows": r1},
                                                 u2: {"state": "done", "rows": r2}}}).encode())
        # per-range result doc (_mr_write_result shape: failed/first/last + files detail)
        s3.put_object(Bucket=BUCKET, Key="%s_range_result/%s.range-%s.json" % (CP_KEY, LABEL, rk),
                      Body=json.dumps({"label": LABEL, "first": "LOAD%08d.csv" % a,
                                       "last": "LOAD%08d.csv" % b, "failed": False,
                                       "files_assigned": 2, "files_done": 2,
                                       "rows_written_by_this_range": r1 + r2,
                                       "files": [{"uri": u1, "state": "done", "rows": r1},
                                                 {"uri": u2, "state": "done", "rows": r2}]}).encode())
    s3.put_object(Bucket=BUCKET, Key="mrl/state/t/plan.json",
                  Body=json.dumps({"allow_hex": True, "tables": {LABEL: {
                      "num_files": 4, "config_prefix": "s3://b/%s" % CP_KEY,
                      "dms_s3_path": DMS, "loaded_empty": False}}}).encode())


def test_a18_writer_docs_reconcile_clean_and_merge_original_format():
    s3 = FakeS3()
    _seed_two_ranges(s3)
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    out = mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                      "planKey": "s3://b/mrl/state/t/plan.json",
                      "writeMergedStatus": True, "cdc": False,
                      "configPrefix": "s3://b/config/_task/t/"}, None)
    assert out["allClean"] is True, out
    t = out["tables"][0]
    assert t["label"] == LABEL and t["total_files"] == 4 and t["covered"] == 4
    assert t["total_rows"] == 100 and t["clean"] is True
    # merged _file_status/<label>.json in the ORIGINAL load_file_status format
    merged = s3.get_json(CP_KEY + "_file_status/s.t.json")
    assert set(merged["files"].keys()) == {_uri(1), _uri(2), _uri(3), _uri(4)}
    for u, rec in merged["files"].items():
        assert rec["state"] == "done" and isinstance(rec["rows"], int)
    assert merged["files"][_uri(1)]["rows"] == 10 and merged["files"][_uri(4)]["rows"] == 40


def test_a18_failed_range_result_makes_reconcile_fail_closed():
    s3 = FakeS3()
    _seed_two_ranges(s3)
    # mark range B's result failed (as _mr_write_result would on a failure)
    rk = "LOAD00000003.csv-LOAD00000004.csv"
    s3.put_object(Bucket=BUCKET, Key="%s_range_result/%s.range-%s.json" % (CP_KEY, LABEL, rk),
                  Body=json.dumps({"label": LABEL, "first": "LOAD00000003.csv",
                                   "last": "LOAD00000004.csv", "failed": True,
                                   "error": "boom"}).encode())
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    try:
        mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                    "planKey": "s3://b/mrl/state/t/plan.json", "writeMergedStatus": True,
                    "cdc": False, "configPrefix": "s3://b/config/_task/t/"}, None)
        assert False, "a failed range result must make reconcile fail closed"
    except mr.MrlReconcileError as e:
        assert "NOT clean" in str(e)


def test_a18_cdc_true_chain_reconcile_then_handoff():
    """cdc=true: reconcile writes the task _load_status.json; mrl_cdc_handoff then writes the fork
    prefix handoff - driven end to end on the writer docs."""
    s3 = FakeS3()
    _seed_two_ranges(s3)
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    rout = mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                       "planKey": "s3://b/mrl/state/t/plan.json", "writeMergedStatus": True,
                       "cdc": True, "configPrefix": "s3://b/config/_task/t/"}, None)
    assert rout["allClean"] is True
    task_doc = s3.get_json("config/_task/t/_load_status.json")
    assert task_doc["tables"][LABEL]["status"] == "done" and task_doc["tables"][LABEL]["rows"] == 100
    # fork handoff at a bg group prefix
    ch.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    forks = [{"kind": "bg", "fork_slug": "s-t", "fork_table": LABEL,
              "config_prefix": "s3://b/config/_task/t/_orchestrator/group-0/",
              "config_prefix_key": "config/_task/t/_orchestrator/group-0/"}]
    hout = ch.handler({"bucket": BUCKET, "taskSuffix": SUFFIX, "cdc": True,
                       "configPrefix": "s3://b/config/_task/t/",
                       "reconcileKey": rout["reconcileKey"], "forks": forks}, None)
    assert hout["forkCount"] == 1
    fork_doc = s3.get_json("config/_task/t/_orchestrator/group-0/_load_status.json")
    assert fork_doc["tables"][LABEL]["status"] == "done" and fork_doc["tables"][LABEL]["rows"] == 100


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print("[PASS]", fn.__name__)
    print("\n%d writer-reconcile test(s) passed" % len(fns))
