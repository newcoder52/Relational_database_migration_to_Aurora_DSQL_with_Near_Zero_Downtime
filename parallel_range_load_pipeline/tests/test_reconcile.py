#!/usr/bin/env python3
"""Reconcile cases for mrl_reconcile (complete / missing / duplicate / failed / extra), plus a
proof that the merged normal doc is accepted by the REAL load_file_status extracted from the
patched loader."""
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_reconcile as mr  # noqa: E402
import mrl_split as ms      # noqa: E402
from mrl_fakes import FakeS3  # noqa: E402

BUCKET = "bkt"
LABEL = "s.t"
CP_KEY = "mrl/state/t/tables/s.t/config/"
DMS = "s3://bkt/s/t/"


def _uri(n):
    return "s3://bkt/s/t/LOAD%08d.csv" % n


def _seed(s3, ranges, listing, failed_ranges=None):
    s3.put_load_files(BUCKET, "s/t", listing)
    for rk, filemap in ranges.items():
        first, last = rk.split("..")
        sk = "%s_file_status/%s.range-%s-%s.json" % (CP_KEY, LABEL.replace("/", "_"), first, last)
        s3.put_object(Bucket=BUCKET, Key=sk, Body=json.dumps(
            {"files": {u: {"state": "done", "rows": r} for u, r in filemap.items()}}).encode())
        rrk = "%s_range_result/%s.range-%s-%s.json" % (CP_KEY, LABEL.replace("/", "_"), first, last)
        s3.put_object(Bucket=BUCKET, Key=rrk, Body=json.dumps(
            {"first": first, "last": last, "failed": bool(failed_ranges and rk in failed_ranges)}).encode())


def _report(s3):
    return mr.reconcile_table(s3, BUCKET, CP_KEY, LABEL, DMS, allow_hex=False)


def test_complete_clean():
    s3 = FakeS3()
    _seed(s3, {"LOAD00000001.csv..LOAD00000002.csv": {_uri(1): 10, _uri(2): 20},
               "LOAD00000003.csv..LOAD00000004.csv": {_uri(3): 30, _uri(4): 40}}, [1, 2, 3, 4])
    r = _report(s3)
    assert r["clean"] and r["total_rows"] == 100 and r["covered"] == 4


def test_missing_file():
    s3 = FakeS3()
    _seed(s3, {"LOAD00000001.csv..LOAD00000003.csv": {_uri(1): 10, _uri(2): 20, _uri(3): 30}},
          [1, 2, 3, 4])
    r = _report(s3)
    assert not r["clean"] and r["missing"] == ["LOAD00000004.csv"]


def test_duplicate_file():
    s3 = FakeS3()
    _seed(s3, {"LOAD00000001.csv..LOAD00000002.csv": {_uri(1): 10, _uri(2): 20},
               "LOAD00000002.csv..LOAD00000003.csv": {_uri(2): 20, _uri(3): 30}}, [1, 2, 3])
    r = _report(s3)
    assert not r["clean"] and r["duplicates"]


def test_failed_range():
    s3 = FakeS3()
    _seed(s3, {"LOAD00000001.csv..LOAD00000002.csv": {_uri(1): 10, _uri(2): 20},
               "LOAD00000003.csv..LOAD00000004.csv": {_uri(3): 30, _uri(4): 40}}, [1, 2, 3, 4],
          failed_ranges={"LOAD00000003.csv..LOAD00000004.csv"})
    r = _report(s3)
    assert not r["clean"] and r["failed_ranges"]


def test_extra_file():
    s3 = FakeS3()
    # a range reports a file that is not in the current listing
    _seed(s3, {"LOAD00000001.csv..LOAD00000009.csv": {_uri(1): 10, _uri(9): 90}}, [1])
    r = _report(s3)
    assert not r["clean"] and ("LOAD00000009.csv" in r["extra"])


def test_merged_doc_accepted_by_real_loader():
    """Write the merged normal doc and prove the REAL load_file_status (extracted from the
    patched loader) reads it as all-done."""
    s3 = FakeS3()
    _seed(s3, {"LOAD00000001.csv..LOAD00000002.csv": {_uri(1): 10, _uri(2): 20},
               "LOAD00000003.csv..LOAD00000004.csv": {_uri(3): 30, _uri(4): 40}}, [1, 2, 3, 4])
    r = _report(s3)
    assert r["clean"]
    mr.write_merged_status(s3, BUCKET, CP_KEY, LABEL, r)

    patched = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
    src = open(patched).read()
    tree = ast.parse(src)
    lns = {"json": json, "AUTO_REBLANK_ON_RESUME": True,
           "CONFIG_PREFIX": "s3://%s/%s" % (BUCKET, CP_KEY)}

    def split_s3(path):
        b, k = str(path)[5:].split("/", 1)
        return b, k
    lns["split_s3"] = split_s3
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in (
                "load_file_status", "_load_file_status_doc", "_file_status_path"):
            exec(ast.get_source_segment(src, n), lns)
    done_map, started = lns["load_file_status"](s3, "s", "t")
    assert len(done_map) == 4 and sum(done_map.values()) == 100 and not started


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d reconcile test(s) passed" % len(fns))
