#!/usr/bin/env python3
"""Exhaustive + property tests for the pure splitter (mrl_split.split_ranges / build_ranges).

Covers n in 0..300 x c in 1..64 exhaustively (sizes, balance, contiguity, determinism), the
required examples from the brief section 5, plus random large n up to 20000. Runs under
pytest AND standalone (python3 test_split.py).
"""
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lambdas"))

import mrl_split as ms  # noqa: E402


def _check_ranges(ranges, n, c):
    """Assert the universal split invariants for split_ranges output."""
    k = min(c, n) if n > 0 else 0
    assert len(ranges) == k, (n, c, ranges)
    if n == 0:
        assert ranges == []
        return
    sizes = [e - s + 1 for s, e in ranges]
    assert sum(sizes) == n, (n, c, sizes)
    assert max(sizes) - min(sizes) <= 1, (n, c, sizes)       # balance
    # contiguity + inclusivity + ordering
    assert ranges[0][0] == 0
    assert ranges[-1][1] == n - 1
    for i in range(1, len(ranges)):
        assert ranges[i][0] == ranges[i - 1][1] + 1, (n, c, ranges)
    for s, e in ranges:
        assert s <= e
    # exact first (n mod k) ranges are the big ones
    base = n // k
    rem = n % k
    for i, sz in enumerate(sizes):
        assert sz == (base + 1 if i < rem else base), (n, c, i, sizes)


def test_exhaustive_small():
    for n in range(0, 301):
        for c in range(1, 65):
            r1 = ms.split_ranges(n, c)
            _check_ranges(r1, n, c)
            # determinism
            assert ms.split_ranges(n, c) == r1


def test_required_examples():
    assert ms.range_sizes(5, 3) == [2, 2, 1]
    assert ms.range_sizes(7, 3) == [3, 2, 2]
    assert ms.range_sizes(4, 4) == [1, 1, 1, 1]
    assert ms.range_sizes(3, 5) == [1, 1, 1]
    assert ms.range_sizes(1, 10) == [1]
    assert ms.range_sizes(406, 4) == [102, 102, 101, 101]
    assert ms.range_sizes(0, 10) == []


def test_random_large():
    rng = random.Random(1234)
    for _ in range(400):
        n = rng.randint(0, 20000)
        c = rng.randint(1, 64)
        _check_ranges(ms.split_ranges(n, c), n, c)


def test_c_one_single_range():
    for n in (1, 2, 50, 406, 5000):
        r = ms.split_ranges(n, 1)
        assert r == [(0, n - 1)]


def test_build_ranges_by_count():
    files = [("s3://b/t/LOAD%08d.csv" % i, 10 * i) for i in range(1, 6)]  # 5 files
    rs = ms.build_ranges(files, 3)
    assert [r["size"] for r in rs] == [2, 2, 1]
    assert rs[0]["first"] == "LOAD00000001.csv" and rs[0]["last"] == "LOAD00000002.csv"
    assert rs[-1]["first"] == "LOAD00000005.csv" and rs[-1]["last"] == "LOAD00000005.csv"
    ms.prove_ranges(rs, files)  # must not raise


def test_build_ranges_406_c4():
    files = [("s3://b/t/LOAD%08d.csv" % i, 1000) for i in range(1, 407)]
    rs = ms.build_ranges(files, 4)
    assert [r["size"] for r in rs] == [102, 102, 101, 101]
    assert rs[0]["first"] == "LOAD00000001.csv"
    assert rs[0]["last"] == "LOAD00000102.csv"
    assert rs[1]["first"] == "LOAD00000103.csv"
    assert rs[-1]["last"] == "LOAD00000406.csv"
    ms.prove_ranges(rs, files)


def test_prove_ranges_catches_corruption():
    files = [("s3://b/t/LOAD%08d.csv" % i, 1000) for i in range(1, 6)]
    rs = ms.build_ranges(files, 3)
    # Corrupt one range so the union diverges.
    rs[0]["files"] = rs[0]["files"][::-1] if len(rs[0]["files"]) > 1 else rs[0]["files"]
    rs[0]["files"] = ["s3://b/t/LOAD99999999.csv"] + rs[0]["files"][1:]
    try:
        ms.prove_ranges(rs, files)
        assert False, "prove_ranges should have failed on corrupted union"
    except ms.MrlSplitError:
        pass


def test_bytes_split_covers_all_disjoint():
    rng = random.Random(7)
    for _ in range(50):
        n = rng.randint(1, 200)
        files = [("s3://b/t/LOAD%08d.csv" % i, rng.randint(1, 10000)) for i in range(1, n + 1)]
        c = rng.randint(1, 8)
        rs = ms.build_ranges_by_bytes(files, c)
        covered = []
        for r in rs:
            covered += r["files"]
        assert covered == [u for u, _s in files]           # contiguous, in order, complete
        assert len(set(covered)) == len(covered)           # disjoint
        assert 1 <= len(rs) <= min(c, n)


def test_c_must_be_ge_1():
    for bad in (0, -1):
        try:
            ms.split_ranges(10, bad)
            assert False
        except ms.MrlSplitError:
            pass


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
        passed += 1
    print("\n%d splitter test(s) passed" % passed)
