#!/usr/bin/env python3
"""A35 (Amendment 8.4) CDC HANDOFF — the likely production bug. When cdc=true, MRL reconcile must
write the full-load status in the ORIGINAL format at the prefix the reused CDC jobs run with
(config/_task/<suffix>/_load_status.json), so the CDC job's REQUIRE_FULL_LOAD_DONE gate sees the
table done and the G9 drift baseline (_FULL_LOAD_ROWS) is seeded. Proven by running the REAL
extracted CDC-job functions (load_full_load_status / _read_load_status_doc) against what reconcile
wrote.
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

import mrl_reconcile as mr      # noqa: E402
from mrl_fakes import FakeS3    # noqa: E402

BUCKET = "b"
SUFFIX = "t"
LABEL = "s.t"
CP_KEY = "mrl/state/t/tables/s.t/config/"           # MRL per-table config prefix
TASK_CP = "config/_task/t/"                          # the CDC job's --config_prefix
DMS = "s3://b/s/t/"


def _uri(n):
    return "s3://b/s/t/LOAD%08d.csv" % n


def _seed_clean_load(s3):
    s3.put_load_files(BUCKET, "s/t", [1, 2])
    rk = "LOAD00000001.csv-LOAD00000002.csv"
    s3.put_object(Bucket=BUCKET, Key="%s_file_status/%s.range-%s.json" % (CP_KEY, LABEL, rk),
                  Body=json.dumps({"files": {_uri(1): {"state": "done", "rows": 10},
                                             _uri(2): {"state": "done", "rows": 20}}}).encode())
    s3.put_object(Bucket=BUCKET, Key="%s_range_result/%s.range-%s.json" % (CP_KEY, LABEL, rk),
                  Body=json.dumps({"first": "LOAD00000001.csv", "last": "LOAD00000002.csv",
                                   "failed": False}).encode())
    # plan.json
    s3.put_object(Bucket=BUCKET, Key="mrl/state/t/plan.json",
                  Body=json.dumps({"allow_hex": True, "tables": {LABEL: {
                      "num_files": 2, "config_prefix": "s3://b/%s" % CP_KEY,
                      "dms_s3_path": DMS, "loaded_empty": False}}}).encode())


def _extract_cdc_gate():
    """AST-extract the REAL CDC-job load-status reader from scripts/glue_cdc_continuous.py."""
    src = open(os.path.join(REPO, "scripts", "glue_cdc_continuous.py")).read()
    tree = ast.parse(src)
    from datetime import datetime as _datetime, timezone as _timezone
    ns = {"json": json, "_FULL_LOAD_ROWS": {}, "LOAD_STATUS_KEY": None, "datetime": _datetime, "timezone": _timezone}

    def split_s3(p):
        b, k = str(p)[5:].split("/", 1)
        return b, k
    ns["split_s3"] = split_s3
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in (
                "_read_load_status_doc", "load_full_load_status", "_load_status_err_code"):
            exec(ast.get_source_segment(src, n), ns)
    return ns


def test_a35_cdc_handoff_task_load_status_seen_done_and_baseline_seeded():
    s3 = FakeS3()
    _seed_clean_load(s3)
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    out = mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                      "planKey": "s3://b/mrl/state/t/plan.json",
                      "writeMergedStatus": True, "cdc": True,
                      "configPrefix": "s3://b/%s" % TASK_CP}, None)
    assert out["allClean"] is True
    # the task-level _load_status.json was written at the CDC job's prefix
    tdoc = s3.get_json(TASK_CP + "_load_status.json")
    assert tdoc["tables"][LABEL]["status"] == "done"
    assert tdoc["tables"][LABEL]["rows"] == 30

    # now run the REAL CDC-job load_full_load_status against the SAME fake S3 + prefix
    ns = _extract_cdc_gate()
    ns["s3"] = s3
    ns["BUCKET"] = BUCKET
    ns["CONFIG_PREFIX"] = "s3://b/%s" % TASK_CP
    status = ns["load_full_load_status"]()
    assert status.get(LABEL) == "done", "CDC gate must see the table done (REQUIRE_FULL_LOAD_DONE)"
    # G9 drift baseline seeded from the "rows" field
    assert ns["_FULL_LOAD_ROWS"].get(LABEL) == 30, "G9 baseline must be seeded from the load status"


def test_cdc_false_does_not_write_task_load_status_unless_merge():
    s3 = FakeS3()
    _seed_clean_load(s3)
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX, "planKey": "s3://b/mrl/state/t/plan.json",
                "writeMergedStatus": False, "cdc": False,
                "configPrefix": "s3://b/%s" % TASK_CP}, None)
    # cdc=false: no task-level _load_status.json written
    assert TASK_CP + "_load_status.json" not in s3.keys()


# ============================================================================================
# R23-B1: the FORK-prefix handoff. A table with >= 8 files (the 406-file flagship) becomes a bg
# fork; a composite-PK table becomes a ck fork. The fork CDC job runs with --config_prefix = the
# FORK prefix and gates on <that prefix>/_load_status.json. mrl_reconcile only writes the TASK
# prefix, so without mrl_cdc_handoff the fork CDC job sees the table "not done" and skips it
# forever. These tests run the REAL extracted CDC gate against each fork prefix.
# ============================================================================================
import mrl_cdc_handoff as ch      # noqa: E402

CK_FORK_CP = "config/_task/t/_orchestrator/ck-s-t-deadbeef/"      # composite fork prefix
BG_GROUP_CP = "config/_task/t/_orchestrator/group-0/"            # big-table group prefix


def _reconcile_clean(s3):
    """Run reconcile (cdc=true) on the clean single-table load; returns the reconcile output."""
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    return mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                       "planKey": "s3://b/mrl/state/t/plan.json",
                       "writeMergedStatus": True, "cdc": True,
                       "configPrefix": "s3://b/%s" % TASK_CP}, None)


def _run_handoff(s3, forks, reconcile_key, cdc=True):
    ch.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    return ch.handler({"bucket": BUCKET, "taskSuffix": SUFFIX, "cdc": cdc,
                       "configPrefix": "s3://b/%s" % TASK_CP,
                       "reconcileKey": reconcile_key, "forks": forks}, None)


def _gate_status_at(s3, config_prefix):
    ns = _extract_cdc_gate()
    ns["s3"] = s3
    ns["BUCKET"] = BUCKET
    ns["CONFIG_PREFIX"] = "s3://b/" + config_prefix
    status = ns["load_full_load_status"]()
    return status.get(LABEL), ns["_FULL_LOAD_ROWS"].get(LABEL)


def test_r23b1_ck_fork_prefix_seen_done_and_baseline_seeded():
    """A composite (ck) fork CDC job reads config/_task/<suffix>/_orchestrator/ck-<slug>/. After
    mrl_cdc_handoff, the REAL gate sees the table done and seeds the G9 baseline there."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    out = _reconcile_clean(s3)
    # BEFORE the fork handoff: the fork prefix has NO load status -> gate sees None (the bug).
    assert _gate_status_at(s3, CK_FORK_CP) == (None, None)
    forks = [{"kind": "ck", "fork_slug": "s-t-deadbeef", "fork_table": LABEL,
              "config_prefix": "s3://b/%s" % CK_FORK_CP,
              "config_prefix_key": CK_FORK_CP}]
    res = _run_handoff(s3, forks, out["reconcileKey"])
    assert res["forkCount"] == 1 and len(res["written"]) == 1
    # AFTER: the REAL gate at the ck fork prefix sees the table done with the right baseline.
    status, baseline = _gate_status_at(s3, CK_FORK_CP)
    assert status == "done", "ck fork CDC gate must see the table done"
    assert baseline == 30, "ck fork G9 baseline must be seeded (30 rows)"


def test_r23b1_bg_group_prefix_seen_done_and_baseline_seeded():
    """A big (bg) fork CDC job reads the big GROUP prefix .../_orchestrator/group-<k>/. The
    flagship 406-file table is a bg fork. After mrl_cdc_handoff the REAL gate sees it done."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    out = _reconcile_clean(s3)
    assert _gate_status_at(s3, BG_GROUP_CP) == (None, None)
    forks = [{"kind": "bg", "fork_slug": "s-t-deadbeef", "fork_table": LABEL,
              "config_prefix": "s3://b/%s" % BG_GROUP_CP,
              "config_prefix_key": BG_GROUP_CP}]
    res = _run_handoff(s3, forks, out["reconcileKey"])
    assert res["forkCount"] == 1
    status, baseline = _gate_status_at(s3, BG_GROUP_CP)
    assert status == "done", "bg/group CDC gate must see the table done (silent-loss fix)"
    assert baseline == 30, "bg/group G9 baseline must be seeded"


def test_r23b1_mutation_removing_fork_write_makes_fork_gate_fail():
    """Mutation guard: if the fork handoff write is removed, the fork CDC gate reverts to None
    (the exact silent-loss regression). Simulate by NOT running the handoff and asserting the
    gate is None at the fork prefix."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    out = _reconcile_clean(s3)                       # task prefix only (reconcile)
    # Do NOT run mrl_cdc_handoff (the mutation). Fork prefix must be unseeded.
    status, baseline = _gate_status_at(s3, CK_FORK_CP)
    assert status is None and baseline is None, \
        "without the fork handoff the fork CDC gate MUST see the table not-done (this is the bug " \
        "mrl_cdc_handoff fixes); if this ever becomes 'done' the fork write was done elsewhere"
    # And prove the task prefix alone is NOT enough for the fork job.
    assert _gate_status_at(s3, TASK_CP)[0] == "done"       # main job fine
    assert _gate_status_at(s3, CK_FORK_CP)[0] is None       # fork job broken without handoff


def test_r23b1_handoff_is_idempotent_on_rerun():
    """Re-running the handoff must not corrupt or duplicate; the fork gate stays done."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    out = _reconcile_clean(s3)
    forks = [{"kind": "ck", "fork_slug": "s-t-deadbeef", "fork_table": LABEL,
              "config_prefix": "s3://b/%s" % CK_FORK_CP, "config_prefix_key": CK_FORK_CP}]
    _run_handoff(s3, forks, out["reconcileKey"])
    doc1 = s3.get_json(CK_FORK_CP + "_load_status.json")
    _run_handoff(s3, forks, out["reconcileKey"])          # re-run
    doc2 = s3.get_json(CK_FORK_CP + "_load_status.json")
    assert doc1["tables"][LABEL]["rows"] == doc2["tables"][LABEL]["rows"] == 30
    assert list(doc2["tables"].keys()) == [LABEL], "no duplicate/extra table entries on re-run"
    assert _gate_status_at(s3, CK_FORK_CP) == ("done", 30)


def test_r23b1_handoff_writes_nothing_for_cdc_false():
    """cdc=false must never write a fork handoff (defence in depth; the state only runs on cdc)."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    # reconcile cdc=false (no task status either)
    mr.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    out = mr.handler({"bucket": BUCKET, "taskSuffix": SUFFIX,
                      "planKey": "s3://b/mrl/state/t/plan.json", "writeMergedStatus": True,
                      "cdc": False, "configPrefix": "s3://b/%s" % TASK_CP}, None)
    forks = [{"kind": "ck", "fork_slug": "s-t-deadbeef", "fork_table": LABEL,
              "config_prefix": "s3://b/%s" % CK_FORK_CP, "config_prefix_key": CK_FORK_CP}]
    res = _run_handoff(s3, forks, out["reconcileKey"], cdc=False)
    assert res["written"] == [] and res["forkCount"] == 0
    assert CK_FORK_CP + "_load_status.json" not in s3.keys()


def test_r23b1_handoff_fails_closed_when_forked_table_not_clean():
    """If a fork's owned table has no clean reconcile row count, writing 'done' would be a lie ->
    fail closed."""
    s3 = FakeS3()
    _seed_clean_load(s3)
    out = _reconcile_clean(s3)
    forks = [{"kind": "ck", "fork_slug": "zz", "fork_table": "s.OTHER",
              "config_prefix": "s3://b/config/_task/t/_orchestrator/ck-zz/",
              "config_prefix_key": "config/_task/t/_orchestrator/ck-zz/"}]
    try:
        _run_handoff(s3, forks, out["reconcileKey"])
        assert False, "a fork owning a table with no clean reconcile row count must fail closed"
    except ch.MrlCdcHandoffError as e:
        assert "s.OTHER" in str(e)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d CDC-handoff test(s) passed" % len(fns))
