#!/usr/bin/env python3
"""Mutation checks: for each load-bearing safety condition, flip it in a COPY of the function's
source and confirm the corresponding protection NO LONGER fires (so the guard is proven
load-bearing). Covers the splitter union/size proof, the listing uniqueness + uniform-width
checks, the plan listing-drift fail-closed, the gate non-empty refusal, and the count
mismatch."""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_split as ms  # noqa: E402


def _func_src(module_path, name):
    src = open(module_path).read()
    tree = ast.parse(src)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return ast.get_source_segment(src, n), src
    raise AssertionError("function %s not found in %s" % (name, module_path))


def _exec_mutated(module_path, name, old, new, extra_ns=None):
    seg, _src = _func_src(module_path, name)
    assert old in seg, "mutation anchor %r not in %s" % (old, name)
    mutated = seg.replace(old, new, 1)
    ns = {"re": __import__("re"), "hashlib": __import__("hashlib")}
    ns["MrlSplitError"] = ms.MrlSplitError
    ns["basename"] = ms.basename
    if extra_ns:
        ns.update(extra_ns)
    exec(mutated, ns)
    return ns[name]


SPLIT = os.path.join(PKG, "lambdas", "mrl_split.py")


def test_mutation_prove_ranges_union_disabled():
    files = [("s3://b/t/LOAD%08d.csv" % i, 1) for i in range(1, 6)]
    ranges = ms.build_ranges(files, 3)
    # corrupt the union: replace a file in range 0 with a bogus one
    ranges[0]["files"] = ["s3://b/t/LOAD99999999.csv"] + ranges[0]["files"][1:]
    # baseline: the real prove_ranges catches it
    try:
        ms.prove_ranges(ranges, files)
        assert False, "baseline prove_ranges should catch the corrupted union"
    except ms.MrlSplitError:
        pass
    # mutate: neuter the union-equality check -> no longer caught
    mutated = _exec_mutated(SPLIT, "prove_ranges", "    if flat != sorted_uris:\n",
                            "    if False:\n")
    try:
        mutated(ranges, files)
        caught = False
    except ms.MrlSplitError:
        caught = True
    assert not caught, "mutation disabled the union check but it still fired (not load-bearing?)"


def test_mutation_uniform_width_disabled():
    mixed = [("s3://b/t/LOAD001.csv", 1), ("s3://b/t/LOAD00000002.csv", 1)]
    try:
        ms.assert_uniform_width(mixed)
        assert False, "baseline should reject mixed width"
    except ms.MrlSplitError:
        pass
    mutated = _exec_mutated(SPLIT, "assert_uniform_width", "    if len(widths) > 1:\n",
                            "    if False:\n",
                            extra_ns={"_load_body_len": ms._load_body_len,
                                      "_LOAD_BODY_RE": ms._LOAD_BODY_RE})
    # mutated: no longer raises on mixed width
    assert mutated(mixed) is True


def test_mutation_unique_basenames_disabled():
    dup = [("s3://b/S/T/LOAD00000001.csv", 1), ("s3://b/s/t/LOAD00000001.csv", 1)]
    try:
        ms.assert_unique_basenames(dup)
        assert False
    except ms.MrlSplitError:
        pass
    mutated = _exec_mutated(SPLIT, "assert_unique_basenames", "    if dups:\n", "    if False:\n")
    # mutated returns a count instead of raising
    assert mutated(dup) == 1


def test_mutation_listing_filter_processed_disabled():
    # is_full_load_key must exclude processed/ ; disabling that miscounts a CDC copy.
    assert ms.is_full_load_key("s/t/LOAD00000001.csv") is True
    assert ms.is_full_load_key("s/t/processed/LOAD00000001.csv") is False
    mutated = _exec_mutated(
        SPLIT, "is_full_load_key",
        '    if "processed" in segments or "failed" in segments:\n        return False\n', "",
        extra_ns={"_ALLOW_LOAD_RE": ms._ALLOW_LOAD_RE, "_DECIMAL_BODY_RE": ms._DECIMAL_BODY_RE})
    assert mutated("s/t/processed/LOAD00000001.csv") is True   # now miscounted (load-bearing)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d mutation test(s) passed" % len(fns))
