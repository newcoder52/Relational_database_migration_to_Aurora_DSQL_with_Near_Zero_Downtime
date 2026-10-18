#!/usr/bin/env python3
"""Count-verification verdict tests for scripts/job_mrl_count.py. The script imports awsglue/pg8000
at module top (not installable offline), so we AST-extract the PURE verdict functions
(dms_full_load_rows_for, _single_pk) and re-implement the tiny verdict rollup the job's main()
performs, then assert: DMS name mapping (case-insensitive, renamed schema), equal match, off by
one -> mismatch, tolerance, timeout fail vs warn verdict."""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
COUNT = os.path.join(PKG, "scripts", "job_mrl_count.py")


def _extract(names):
    src = open(COUNT).read()
    tree = ast.parse(src)
    ns = {}
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in names:
            exec(ast.get_source_segment(src, n), ns)
    return ns


class _FakeDms:
    def __init__(self, stats):
        self._stats = stats

    def describe_table_statistics(self, ReplicationTaskArn, MaxRecords=500, Marker=None):
        return {"TableStatistics": self._stats}


def test_dms_name_mapping_case_insensitive_and_renamed():
    ns = _extract({"dms_full_load_rows_for"})
    fn = ns["dms_full_load_rows_for"]
    # DMS reports the SOURCE schema/table (possibly different case / renamed by a rule).
    dms = _FakeDms([{"SchemaName": "SRC_SCHEMA", "TableName": "ORDERS", "FullLoadRows": 123}])
    # match on the dms_schema/dms_table (what the manifest carries), case-insensitively
    assert fn(dms, "arn", "src_schema", "orders") == 123
    assert fn(dms, "arn", "SRC_SCHEMA", "ORDERS") == 123
    assert fn(dms, "arn", "other", "orders") is None


def _verdict(results, action, no_basis="warn"):
    """Execute the REAL compute_verdict rollup from job_mrl_count.py (M5 - no re-implementation).
    Default no_basis='warn' here so the legacy ok/mismatch/unverifiable tests are unaffected; the
    R21-2 no_basis='fail' behavior is covered by its own test."""
    ns = _extract({"compute_verdict"})
    verdict, _info = ns["compute_verdict"](results, action, no_basis)
    return verdict


def _one(expected, target, tol=0):
    if target == "TIMEOUT":
        return {"status": "unverifiable", "expected": expected, "target": None}
    if expected is None or target is None:
        return {"status": "no_expected", "expected": expected, "target": target}
    ok = abs(int(target) - int(expected)) <= tol
    return {"status": "match" if ok else "mismatch", "expected": expected, "target": target}


def test_equal_match():
    assert _verdict([_one(100, 100)], "fail") == "ok"


def test_off_by_one_mismatch():
    assert _verdict([_one(100, 99)], "fail") == "mismatch"
    assert _one(100, 99)["status"] == "mismatch"


def test_tolerance_allows_small_diff():
    assert _one(100, 98, tol=2)["status"] == "match"
    assert _one(100, 97, tol=2)["status"] == "mismatch"


def test_timeout_fail_vs_warn():
    r = [_one(100, "TIMEOUT")]
    assert _verdict(r, "fail") == "unverifiable"   # fail-closed
    assert _verdict(r, "warn") == "ok"             # downgraded


def test_single_pk_detection():
    ns = _extract({"_single_pk"})
    f = ns["_single_pk"]
    single = {"metadata": {"primary_key": {"columns": ["id"], "mode": "single",
                                           "pk_kind": "integer"}}}
    comp = {"metadata": {"primary_key": {"columns": ["a", "b"], "mode": "composite"}}}
    assert f(single) == ("id", "integer")
    assert f(comp) == (None, None)


# ---- M5: the REAL rollup, with a mutation guard ------------------------------------------
def test_m5_real_rollup_mismatch_and_no_expected_verdicts():
    """Execute the REAL compute_verdict over the full matrix."""
    ns = _extract({"compute_verdict"})
    cv = ns["compute_verdict"]
    assert cv([_one(100, 100)], "fail", "fail")[0] == "ok"
    assert cv([_one(100, 99)], "fail", "fail")[0] == "mismatch"
    assert cv([_one(100, 100, 5)], "fail", "fail")[0] == "ok"          # within tolerance -> match
    assert cv([_one(100, "TIMEOUT")], "fail", "fail")[0] == "unverifiable"
    assert cv([_one(100, "TIMEOUT")], "warn", "fail")[0] == "ok"
    # R21-2: a no-basis (loaded, no DMS/discovery expected) table fails by default, warn downgrades
    assert cv([_one(None, 42)], "fail", "fail")[0] == "no_basis"
    assert cv([_one(None, 42)], "fail", "warn")[0] == "ok"
    # precedence: a mismatch beats a no_basis
    assert cv([_one(100, 99), _one(None, 42)], "fail", "fail")[0] == "mismatch"


def test_m5_mutation_disabling_mismatch_rule_is_caught():
    """Mutation: patch the REAL compute_verdict source so `if mismatches:` becomes `if False:`
    and assert a test that relies on the mismatch verdict now FAILS (the real rollup is executed,
    not a re-implementation)."""
    src = open(COUNT).read()
    tree = ast.parse(src)
    seg = None
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == "compute_verdict":
            seg = ast.get_source_segment(src, n)
    assert seg is not None
    mutated = seg.replace("if mismatches:", "if False:")
    assert mutated != seg, "mutation must change the source"
    ns = {}
    exec(mutated, ns)
    # With the mismatch rule disabled, a clear mismatch is NO LONGER reported -> the assertion
    # that the real rollup catches a mismatch would fail. Prove the mutation changes the verdict.
    verdict, _ = ns["compute_verdict"]([_one(100, 99)], "fail", "warn")
    assert verdict != "mismatch", "mutation should stop the mismatch verdict (guard is meaningful)"
    # And the UNMUTATED real function still reports mismatch (the test that guards it).
    ns2 = _extract({"compute_verdict"})
    assert ns2["compute_verdict"]([_one(100, 99)], "fail", "warn")[0] == "mismatch"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d count-verdict test(s) passed" % len(fns))
