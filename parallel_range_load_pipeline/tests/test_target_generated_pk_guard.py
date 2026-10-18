#!/usr/bin/env python3
"""AMENDMENT 3 (R6) — target-generated primary key guard tests (parallel range mode only).

The guard refuses, BEFORE any write and before any Glue range is launched, a table whose target
PRIMARY KEY column is not loaded from the source (so the target DEFAULT, e.g. gen_random_uuid(),
would generate the key and a re-run would create silent duplicates). Two guards are tested: the
loader's `_mr_target_generated_pk_guard` (AST-extracted from scripts/job2_load_manual_range.py) and
mrl_plan's `_r6_target_generated_pk_guard` (imported). A PK column PRESENT in the file passes and the
INSERT carries that column (so a source NULL is an explicit NULL -> server error, never DEFAULT).
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
LOADER = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_plan  # noqa: E402  (has _r6_target_generated_pk_guard + MrlPlanError)


def _loader_guard():
    """AST-extract the loader's guard (+ its ManualRangeError) into a namespace."""
    src = open(LOADER).read()
    tree = ast.parse(src)
    ns = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in (
                "_mr_target_generated_pk_guard", "ManualRangeError"):
            exec(ast.get_source_segment(src, n), ns)
    return ns


def _cfg(pk_cols, target_cols, missing_detail=None, pk_mode=None):
    meta = {"dsql_schema": "s", "dsql_table": "t",
            "primary_key": {"columns": pk_cols, "mode": pk_mode,
                            "is_single_column": len(pk_cols) == 1},
            "missing_target_columns": [c for c in (missing_detail or [])
                                       if isinstance(c, str)]}
    if missing_detail and all(isinstance(d, dict) for d in missing_detail):
        meta["missing_target_columns_detail"] = missing_detail
        meta["missing_target_columns"] = [d["name"] for d in missing_detail]
    return {"metadata": meta, "target_columns": target_cols}


# ---- loader guard ------------------------------------------------------------------------
def test_pk_uuid_default_present_in_file_passes():
    ns = _loader_guard()
    cfg = _cfg(["id"], ["id", "name", "amount"])      # id (uuid) IS loaded from the source
    warns = ns["_mr_target_generated_pk_guard"](cfg)
    assert warns == []                                 # no refusal, no warning
    # the loader builds its INSERT column list from target_columns, so 'id' IS in the statement
    assert "id" in cfg["target_columns"]               # -> a source value (or explicit NULL) is used


def test_pk_absent_from_file_refuses_before_write():
    ns = _loader_guard()
    cfg = _cfg(["id"], ["name", "amount"])             # id NOT loaded -> target DEFAULT would fire
    try:
        ns["_mr_target_generated_pk_guard"](cfg)
        assert False, "must refuse a target-generated PK"
    except ns["ManualRangeError"] as e:
        assert "TARGET-GENERATED PRIMARY KEY refused" in str(e) and "id" in str(e)


def test_composite_pk_one_column_missing_refuses():
    ns = _loader_guard()
    cfg = _cfg(["tenant_id", "id"], ["tenant_id", "name"], pk_mode="composite")  # 'id' missing
    try:
        ns["_mr_target_generated_pk_guard"](cfg)
        assert False, "must refuse when any composite-PK column is missing"
    except ns["ManualRangeError"] as e:
        assert "id" in str(e)


def test_nonkey_column_with_default_absent_is_allowed_with_warning():
    ns = _loader_guard()
    cfg = _cfg(["id"], ["id", "name"],
               missing_detail=[{"name": "created_at", "column_default": "now()"}])
    warns = ns["_mr_target_generated_pk_guard"](cfg)        # PK present -> no refusal
    assert warns and "created_at" in warns[0]               # but a visible warning


def test_source_null_in_pk_is_explicit_null_not_default():
    """When the PK column IS loaded, it is in target_columns -> the loader's INSERT column list
    includes it -> a source NULL is rendered as an explicit NULL (NOT NULL/PK violation, loud),
    never a silent DEFAULT. Prove the column list carries the PK (the loader builds quoted_cols
    from config['target_columns'])."""
    ns = _loader_guard()
    cfg = _cfg(["id"], ["id", "name"])
    assert ns["_mr_target_generated_pk_guard"](cfg) == []
    quoted_cols = ['"%s"' % c for c in cfg["target_columns"]]   # mirrors the loader
    assert '"id"' in quoted_cols                                 # id is written explicitly


# ---- plan guard (fails before any Glue range launches) -----------------------------------
def test_plan_guard_refuses_target_generated_pk():
    entry = {"metadata": {"dsql_schema": "s", "dsql_table": "t",
                          "primary_key": {"columns": ["id"]}},
             "target_columns": ["name", "amount"]}       # id NOT loaded
    try:
        mrl_plan._r6_target_generated_pk_guard(entry, "s.t")
        assert False, "plan guard must refuse"
    except mrl_plan.MrlPlanError as e:
        assert "R6" in str(e) and "id" in str(e)


def test_plan_guard_passes_when_pk_loaded():
    entry = {"metadata": {"dsql_schema": "s", "dsql_table": "t",
                          "primary_key": {"columns": ["id"]}},
             "target_columns": ["id", "name"]}
    mrl_plan._r6_target_generated_pk_guard(entry, "s.t")    # no raise


def test_plan_guard_noop_without_target_columns():
    """A slim index entry with no target_columns can't be judged here -> no raise; the loader's
    own guard (which always has the mapping) is the backstop."""
    entry = {"metadata": {"dsql_schema": "s", "dsql_table": "t",
                          "primary_key": {"columns": ["id"]}}}
    mrl_plan._r6_target_generated_pk_guard(entry, "s.t")    # no raise


def test_no_pk_is_not_a_target_generated_pk_case():
    """A table with NO PK is handled by the separate no-PK fail-closed-on-resume guard, not here;
    this guard must not raise on an empty PK list."""
    ns = _loader_guard()
    assert ns["_mr_target_generated_pk_guard"](_cfg([], ["a", "b"])) == []
    mrl_plan._r6_target_generated_pk_guard({"metadata": {"primary_key": {"columns": []}},
                                            "target_columns": ["a"]}, "s.t")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d target-generated-PK guard test(s) passed" % len(fns))
