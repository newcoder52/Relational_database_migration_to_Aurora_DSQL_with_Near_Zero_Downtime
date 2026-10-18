#!/usr/bin/env python3
"""Resolver-consistency test (brief section 5, proof (c)).

Extracts the REAL manual-range resolver (_mr_resolve_slice / _mr_resolve_one / _mr_basename /
_mr_file_number / ManualRangeError) from the PATCHED loader
(scripts/job2_load_manual_range.py) by AST, then proves that for every range the MRL splitter
assigns, re-resolving that range's first/last names through the loader's OWN resolver yields
EXACTLY the files the splitter assigned. This is the guarantee that the ranges MRL hands to
each Glue load run select precisely the intended slice.
"""
import ast
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lambdas"))
import mrl_split as ms  # noqa: E402

PATCHED = os.path.join(HERE, "..", "scripts", "job2_load_manual_range.py")


def load_resolver_ns():
    """AST-extract the resolver functions/classes from the patched loader into a namespace."""
    src = open(PATCHED).read()
    tree = ast.parse(src)
    ns = {"re": __import__("re")}
    wanted = {"_mr_basename", "_mr_file_number", "_mr_resolve_one", "_mr_resolve_slice",
              "ManualRangeError"}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in wanted:
            exec(ast.get_source_segment(src, n), ns)
    missing = wanted - set(ns)
    assert not missing, "could not extract resolver function(s): %s" % missing
    return ns


def _mk_files(numbers, width=8):
    """[(uri, size)] for LOAD<n>.csv (size irrelevant to the resolver)."""
    return [("s3://bkt/dms/s/t/LOAD%0*d.csv" % (width, n), 1000) for n in numbers]


def _assert_consistency(ns, files_sorted, c):
    """Build MRL ranges and re-resolve each through the loader's resolver; assert equality."""
    reslice = ns["_mr_resolve_slice"]
    base = ns["_mr_basename"]
    ranges = ms.build_ranges(files_sorted, c)
    ms.prove_ranges(ranges, files_sorted)
    for r in ranges:
        # The loader resolves against the whole-table listing. It accepts number/name/uri; MRL
        # passes the EXACT basenames it assigned as first/last.
        first, last, assigned, _bytes = reslice(r["first"], r["last"], files_sorted)
        assert first == r["first"], (first, r["first"])
        assert last == r["last"], (last, r["last"])
        assert [base(u) for u in assigned] == [base(u) for u in r["files"]], (
            r["index"], [base(u) for u in assigned], [base(u) for u in r["files"]])


def test_resolver_consistency_examples():
    ns = load_resolver_ns()
    for numbers, c in [
        (range(1, 6), 3),
        (range(1, 8), 3),
        (range(1, 5), 4),
        (range(1, 4), 5),
        (range(1, 2), 10),
        (range(1, 407), 4),
        (range(1, 101), 7),
    ]:
        _assert_consistency(ns, _mk_files(list(numbers)), c)


def test_resolver_consistency_random():
    ns = load_resolver_ns()
    rng = random.Random(99)
    for _ in range(200):
        n = rng.randint(1, 500)
        c = rng.randint(1, 16)
        _assert_consistency(ns, _mk_files(list(range(1, n + 1))), c)


def test_resolver_number_name_uri_forms_equivalent():
    """The loader resolver accepts a number, a bare name, or a full s3:// key; MRL uses the
    bare name. Confirm all three forms resolve to the same slice for a sample range."""
    ns = load_resolver_ns()
    files = _mk_files(list(range(1, 11)))
    reslice = ns["_mr_resolve_slice"]
    by_name = reslice("LOAD00000003.csv", "LOAD00000007.csv", files)
    by_num = reslice(3, 7, files)
    by_uri = reslice("s3://bkt/dms/s/t/LOAD00000003.csv",
                     "s3://bkt/dms/s/t/LOAD00000007.csv", files)
    assert by_name[2] == by_num[2] == by_uri[2]
    assert len(by_name[2]) == 5


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d resolver-consistency test(s) passed" % len(fns))
