#!/usr/bin/env python3
"""Amendment 19 Defect 2 - DMS names LOAD files in HEX (LOAD00000001..LOAD0000000A..). The planner
must accept hex by default (allow_hex opt-OUT), name-sorted order must equal numeric order for
uniform-width upper-case hex, and the loader's range resolver must pick EXACTLY the planned files.
Production-shape end-to-end: the live t14i-a (12 files), the customer's 406-file table, t14i-b (1).
"""
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_split as ms          # noqa: E402
import mrl_plan as mp           # noqa: E402
from mrl_fakes import FakeS3    # noqa: E402

LOAD_SCRIPT = os.path.join(PKG, "scripts", "job2_load_manual_range.py")


def _hexname(n, width=8):
    return "LOAD%0*X.csv" % (width, n)   # DMS hex, zero-padded, UPPER-case


# ---- name-sort == numeric order for uniform-width upper-case hex, incl. crossing points ----
def test_hex_name_sort_equals_numeric_order():
    for N in (16, 22, 256, 406, 500):
        names = [_hexname(n) for n in range(1, N + 1)]   # LOAD00000001..
        assert names == sorted(names), "name sort != numeric order at N=%d" % N
    # explicit crossing points
    for a, b in [(9, 10), (15, 16), (255, 256), (16, 17)]:
        assert _hexname(a) < _hexname(b), "%s !< %s" % (_hexname(a), _hexname(b))
    # 406 files: the customer's table goes up to 0x196 = 406
    assert _hexname(406) == "LOAD00000196.csv"


# ---- splitter property with REAL hex names (equals-loader set preserved) --------------------
def test_hex_split_property_equals_loader_set():
    import random
    rng = random.Random(19)
    for _ in range(400):
        n = rng.randint(1, 500)
        c = rng.randint(1, 40)
        ranges = ms.split_ranges(n, c)
        k = min(c, n)
        assert len(ranges) == k
        sizes = [b - a + 1 for a, b in ranges]
        assert sum(sizes) == n
        assert max(sizes) - min(sizes) <= 1
        # contiguous + ordered + union == all indices
        flat = []
        for a, b in ranges:
            assert a <= b
            flat += list(range(a, b + 1))
        assert flat == list(range(n))


# ---- the loader's range resolver picks EXACTLY the planned hex files ------------------------
def _loader_resolve():
    """Extract the loader's _mr_resolve_slice (+ helpers) and run it on hex names."""
    src = open(LOAD_SCRIPT).read()
    tree = ast.parse(src)
    import re as _re
    ns = {"re": _re}
    want = {"_mr_basename", "_mr_file_number", "_mr_resolve_one", "_mr_resolve_slice",
            "ManualRangeError"}
    # ManualRangeError is a class; grab it too
    for n in tree.body:
        if isinstance(n, ast.ClassDef) and n.name == "ManualRangeError":
            exec(ast.get_source_segment(src, n), ns)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in want:
            exec(ast.get_source_segment(src, n), ns)
    return ns


def test_loader_resolves_planned_hex_ranges_exactly():
    ns = _loader_resolve()
    N = 406
    files = [("s3://b/s/t/%s" % _hexname(n), 100) for n in range(1, N + 1)]
    # plan 406 / c=3 -> 136,135,135 contiguous ranges
    sizes = [b - a + 1 for a, b in ms.split_ranges(N, 3)]
    assert sizes == [136, 135, 135]
    all_sorted = [u for u, _ in sorted(files, key=lambda t: str(t[0]))]
    idx = 0
    for sz in sizes:
        first_name = ns["_mr_basename"](all_sorted[idx])
        last_name = ns["_mr_basename"](all_sorted[idx + sz - 1])
        f, l, assigned, _bytes = ns["_mr_resolve_slice"](first_name, last_name, files)
        # the loader's name-based resolution returns EXACTLY the planned contiguous slice
        assert [ns["_mr_basename"](u) for u in assigned] == \
            [ns["_mr_basename"](all_sorted[idx + j]) for j in range(sz)]
        assert f == first_name and l == last_name
        idx += sz
    assert idx == N
    # crossing point 9->A in a range bound
    f, l, assigned, _b = ns["_mr_resolve_slice"]("LOAD00000009.csv", "LOAD0000000A.csv", files)
    assert [ns["_mr_basename"](u) for u in assigned] == ["LOAD00000009.csv", "LOAD0000000A.csv"]


# ---- production-shape plan end-to-end (fake S3/DMS) via the real mrl_plan.handler -----------
def _seed_table(s3, n, sizes=None, schema="s", table="orders"):
    """Seed a DMS table folder with n hex-named LOAD files + the single-table config the plan
    copies. Returns the configPrefix the plan reads."""
    cp = "config/_task/suf/"
    dms_schema = schema.upper()
    dms_folder = "%s/%s" % (dms_schema, table)
    dms_path = "s3://b/%s/" % dms_folder
    entry = {"dsql_schema": schema, "dsql_table": table,
             "dms_schema": dms_schema, "dms_table": table,
             "dms_s3_path": dms_path,
             "config_s3_path": "s3://b/%s%s_config.json" % (cp, table),
             "metadata": {"dms_schema": dms_schema, "dms_table": table,
                          "dms_s3_path": dms_path,
                          "primary_key": {"columns": ["id"], "mode": "single",
                                          "pk_kind": "integer"}}}
    s3.put_object(Bucket="b", Key=cp + "_manifest_index.json",
                  Body=json.dumps({"tables": [entry]}).encode())
    s3.put_object(Bucket="b", Key=cp + "%s_column_mapping.json" % table,
                  Body=json.dumps({"column_mapping": {}}).encode())
    s3.put_object(Bucket="b", Key=cp + "table_rowcounts.json",
                  Body=json.dumps({"rowcounts": {"%s.%s" % (schema, table): n * 1000}}).encode())
    for i in range(1, n + 1):
        key = "%s/%s" % (dms_folder, _hexname(i))
        sz = (sizes[i - 1] if sizes else 134217728)
        s3.put_raw("b", key, size=sz)
    return "s3://b/%s" % cp, "%s.%s" % (schema, table)


class _FakeDms:
    def describe_table_statistics(self, **k): return {"TableStatistics": []}
    def describe_replication_tasks(self, **k):
        return {"ReplicationTasks": [{"TableMappings": json.dumps({"rules": []})}]}


def _run_plan(s3, config_prefix, max_concurrency):
    def fc(svc, **k):
        return s3 if svc == "s3" else _FakeDms()
    mp.boto3 = type("b", (), {"client": staticmethod(fc)})
    ms.boto3 = type("b", (), {"client": staticmethod(fc)}) if hasattr(ms, "boto3") else None
    return mp.handler({"bucket": "b", "taskSuffix": "suf", "configPrefix": config_prefix,
                       "cdc_root": ".", "max_concurrency": max_concurrency,
                       "split_by": "count", "split_allow_hex": True}, None)


def test_a19_plan_12_file_hex_table_c4():
    s3 = FakeS3()
    sizes = [134217720 + i for i in range(11)] + [47728141]   # 11 x ~134 MiB + 1 small (t14i-a)
    cp, label = _seed_table(s3, 12, sizes=sizes)
    out = _run_plan(s3, cp, 4)
    t = [t for t in out["tables"] if t["label"] == label][0]
    sizes_r = [r["size"] for r in t["ranges"]]
    assert sum(sizes_r) == 12 and len(sizes_r) == 4 and max(sizes_r) - min(sizes_r) <= 1
    # every planned file is a hex name and the plan==loader assertion passed (no raise)
    assert t["num_files"] == 12


def test_a19_plan_406_file_hex_c3():
    s3 = FakeS3()
    cp, label = _seed_table(s3, 406, sizes=[100] * 406, table="big")
    out = _run_plan(s3, cp, 3)
    t = [t for t in out["tables"] if t["label"] == label][0]
    assert [r["size"] for r in t["ranges"]] == [136, 135, 135]
    # the plan's first/last range bounds are hex names
    assert t["ranges"][0]["first"] == "LOAD00000001.csv"
    assert t["ranges"][-1]["last"] == "LOAD00000196.csv"   # 0x196 = 406


def test_a19_plan_single_file_c2():
    s3 = FakeS3()
    cp, label = _seed_table(s3, 1, sizes=[1568553], table="orders")
    out = _run_plan(s3, cp, 2)
    t = [t for t in out["tables"] if t["label"] == label][0]
    assert len(t["ranges"]) == 1 and t["ranges"][0]["size"] == 1


def test_a19_splitallowhex_defaults_true_in_preflight():
    import mrl_preflight
    src = open(mrl_preflight.__file__).read()
    assert 'event.get("splitAllowHex", True)' in src, "splitAllowHex must default True (opt-out)"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print("[PASS]", fn.__name__)
    print("\n%d A19 hex/range test(s) passed" % len(fns))
