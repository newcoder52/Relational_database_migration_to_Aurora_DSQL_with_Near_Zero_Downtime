#!/usr/bin/env python3
"""mrl_plan tests: same listing -> same plan (reuse); changed listing -> FAIL CLOSED; n=0 with
FullLoadRows 0 -> loaded-empty; n=0 with FullLoadRows>0 -> FAIL CLOSED; the single-table config
prefix is created with exactly one table (copied, not re-derived)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_plan as mp  # noqa: E402
from mrl_fakes import FakeS3  # noqa: E402

BUCKET = "bkt"
CP = "s3://bkt/config/_task/t/"


def _seed(s3, label, files, rows, single_pk=True):
    schema, table = label.split(".")
    if files:
        s3.put_load_files(BUCKET, "%s/%s" % (schema, table), files)
    pk = {"columns": ["id"], "mode": "single", "pk_kind": "integer"} if single_pk \
        else {"columns": ["a", "b"], "mode": "composite"}
    entry = {"dsql_schema": schema, "dsql_table": table, "dms_schema": schema, "dms_table": table,
             "dms_s3_path": "s3://bkt/%s/%s/" % (schema, table), "full_load_rows": rows,
             "metadata": {"dms_schema": schema, "dms_table": table, "primary_key": pk,
                          "full_load_rows": rows}}
    s3.put_object(Bucket=BUCKET, Key="config/_task/t/_manifest_index.json",
                  Body=json.dumps({"tables": [entry]}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task/t/%s_column_mapping.json" % table,
                  Body=json.dumps({"column_mapping": {}, "target_columns": ["id"]}).encode())
    s3.put_object(Bucket=BUCKET, Key="config/_task/t/table_rowcounts.json",
                  Body=json.dumps({"rowcounts": {label.lower(): rows} if rows is not None else {}}).encode())


def _patch(s3):
    mp.boto3 = type("b", (), {"client": staticmethod(lambda *a, **k: s3)})


def _event(**kw):
    e = {"bucket": BUCKET, "taskSuffix": "t", "configPrefix": CP, "cdc_root": ".",
         "max_concurrency": 3, "split_by": "count"}
    e.update(kw)
    return e


def test_plan_and_single_table_config():
    s3 = FakeS3()
    _seed(s3, "s.t", list(range(1, 6)), rows=50)
    _patch(s3)
    out = mp.handler(_event(), None)
    assert out["maxTotalConcurrentRuns"] == 3
    t = out["tables"][0]
    assert [r["size"] for r in t["ranges"]] == [2, 2, 1]
    # single-table config prefix created with EXACTLY one table, copied from discovery
    idx = s3.get_json("mrl/state/t/tables/s.t/config/_manifest_index.json")
    assert len(idx["tables"]) == 1 and idx["tables"][0]["dsql_table"] == "t"
    assert s3.get_json("mrl/state/t/tables/s.t/config/t_column_mapping.json")["column_mapping"] == {}


def test_same_listing_same_plan():
    s3 = FakeS3()
    _seed(s3, "s.t", list(range(1, 6)), rows=50)
    _patch(s3)
    out1 = mp.handler(_event(), None)
    out2 = mp.handler(_event(), None)       # re-run, unchanged listing
    assert out1["planSha"] == out2["planSha"]
    assert out2["ok"]


def test_changed_listing_fails_closed():
    s3 = FakeS3()
    _seed(s3, "s.t", list(range(1, 6)), rows=50)
    _patch(s3)
    mp.handler(_event(), None)              # persist plan.json
    s3.put_load_files(BUCKET, "s/t", [6])   # add a file -> listing drift
    try:
        mp.handler(_event(), None)
        assert False, "should fail closed on listing drift"
    except mp.MrlPlanError as e:
        assert "changed since the plan was persisted" in str(e)


def test_n0_flr0_loaded_empty():
    s3 = FakeS3()
    _seed(s3, "s.empty", [], rows=0)
    _patch(s3)
    out = mp.handler(_event(), None)
    t = out["tables"][0]
    assert t["loaded_empty"] is True and t["num_files"] == 0
    assert out["maxTotalConcurrentRuns"] == 0


def test_n0_flr_positive_fails_closed():
    s3 = FakeS3()
    _seed(s3, "s.bad", [], rows=100)
    _patch(s3)
    try:
        mp.handler(_event(), None)
        assert False, "should fail closed: positive row count but no file"
    except mp.MrlPlanError as e:
        assert "NO LOAD" in str(e) and "not 0" in str(e)


def test_composite_table_has_single_pk_false():
    s3 = FakeS3()
    _seed(s3, "s.ck", list(range(1, 4)), rows=30, single_pk=False)
    _patch(s3)
    out = mp.handler(_event(), None)
    assert out["tables"][0]["has_single_pk"] is False


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d plan test(s) passed" % len(fns))
