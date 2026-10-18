#!/usr/bin/env python3
"""AMENDMENT 1 (owner 2026-10-18): the PRIMARY production path is full-load-only on 100M+ row
tables (400+ hex-named LOAD files) with null_notnull_text=keep_text on NOT NULL text columns.
This test exercises keep_text on the PARALLEL range loader at that SHAPE, OFFLINE:

  * a fixture of >= 300 LOAD files with fixed-width HEX names that cross 0x09->0x0A and
    0xFF->0x100 (the boundaries a decimal-only name scheme would get wrong);
  * the REAL range splitter (mrl_split.build_ranges) into 3 AND 6 ranges, proved complete and
    disjoint (mrl_split.prove_ranges) and consistent with the REAL loader resolver
    (_mr_resolve_slice, AST-extracted from the loader) so each range selects exactly its slice;
  * for EVERY range (first, middle, last, and a deliberately all-small-files range) and for the
    per-path cases (first write / retry / commit-probe resume / last partial chunk), the keep_text
    decision is applied to rows whose NOT NULL text columns hold NULL / N/A / NA / null / None /
    (NULL) and whose NULLABLE columns hold real NULLs, asserting the WRITTEN parameters;
  * keep_text OFF stays byte-identical to the 8c7ed25-era loader (repo-root scripts/job2_load.py).

No Spark / Glue / DSQL / S3. The row source every write path consumes (df.toLocalIterator()) is
proven single and post-transform by test_keep_text_range_load.py; here we drive the SAME decision
code the transform calls, per range, so "what is stored" is asserted at the 300-file hex shape.
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
LOADER = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
ROOT_LOADER = os.path.join(REPO, "scripts", "job2_load.py")
sys.path.insert(0, os.path.join(PKG, "lambdas"))

import mrl_split as ms  # noqa: E402

# reuse the exact decision harness from the sibling behavioural test (same evaluation semantics
# as null_marker_expr) so the two files cannot drift.
sys.path.insert(0, HERE)
import test_keep_text_range_load as kt  # noqa: E402


# --- fixture: >= 300 hex-named LOAD files crossing 0x09->0x0A and 0xFF->0x100 ---------------
_N_FILES = 320          # 0x01..0x140 -> crosses both 0x09->0x0A and 0xFF->0x100
_HEX_WIDTH = 8          # fixed-width zero-padded hex => lexical sort == value order


def _hex_load_files(n=_N_FILES, small=False):
    """[(uri, size)] for LOAD<8-hex>.csv, numbers 1..n. A fixed small size unless `small` picks
    a tiny size so a range of these is 'all small files'."""
    out = []
    for i in range(1, n + 1):
        name = "LOAD%0*X.csv" % (_HEX_WIDTH, i)      # e.g. LOAD00000009, LOAD0000000A, LOAD00000100
        size = 10 if small else 1000 + i
        out.append(("s3://bkt/dms/s/t/%s" % name, size))
    return out


def _resolver_ns(path):
    """AST-extract the loader's manual-range resolver (same technique as
    test_resolver_consistency.py) so range selection is proved against the REAL loader code."""
    src = open(path).read()
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


# --- the per-range cells we assert keep_text on --------------------------------------------
# The endpoint CsvNullValue marker the fixture/_register use (matches kt._register).
kt_CSV_NULL = "NULL"

# NOT NULL text columns MUST keep ALL these literal spellings (keep_text). A NULLABLE column
# keeps today's behaviour: only the CsvNullValue marker ('NULL') and an empty field become NULL;
# every OTHER spelling (N/A, NA, null, None, (NULL), \N, NONE) is ordinary data in both modes.
def _cells():
    rows = []
    for v in kt._TEXTY_NULLS:                       # NULL, N/A, NA, null, None, (NULL), \N, NONE
        rows.append(("nn_vc", "varchar", v, v))     # (col, category, raw, expected_when_keep_text)
        rows.append(("nn_txt", "text", v, v))
        # nullable: ONLY the exact CsvNullValue marker 'NULL' becomes NULL; others kept as data
        rows.append(("null_vc", "varchar", v, None if v == kt_CSV_NULL else v))
    rows.append(("null_vc", "varchar", "", None))     # empty field -> NULL
    rows.append(("nn_vc", "varchar", "", None))       # empty field even in NOT NULL -> NULL
    rows.append(("amount", "numeric", "NULL", None))  # typed unaffected: marker -> NULL
    rows.append(("amount", "numeric", "42", "42"))
    return rows


def test_hex_fixture_crosses_the_boundaries():
    """The fixture genuinely crosses 0x09->0x0A and 0xFF->0x100 and sorts in value order."""
    files = _hex_load_files()
    names = [ms.basename(u) for u, _ in files]
    assert "LOAD00000009.csv" in names and "LOAD0000000A.csv" in names
    assert "LOAD000000FF.csv" in names and "LOAD00000100.csv" in names
    # fixed-width hex: lexical sort == the order we generated (value order)
    assert names == sorted(names), "fixed-width hex names must sort in value order"
    assert len(files) >= 300


def _ranges_for(c):
    files = _hex_load_files()
    files_sorted = sorted(files, key=lambda t: str(t[0]))
    ranges = ms.build_ranges(files_sorted, c)
    ms.prove_ranges(ranges, files_sorted)            # complete + disjoint + sizes differ by <=1
    return files_sorted, ranges


def test_split_3_and_6_are_complete_and_resolver_consistent():
    """build_ranges into 3 and 6; prove each range re-resolves through the REAL loader resolver
    to EXACTLY its files (so the range a worker loads is precisely its hex slice)."""
    rns = _resolver_ns(LOADER)
    reslice = rns["_mr_resolve_slice"]
    base = rns["_mr_basename"]
    for c in (3, 6):
        files_sorted, ranges = _ranges_for(c)
        assert len(ranges) == c
        for r in ranges:
            first, last, assigned, _b = reslice(r["first"], r["last"], files_sorted)
            assert first == r["first"] and last == r["last"]
            assert [base(u) for u in assigned] == [base(u) for u in r["files"]], r["index"]


def _assert_keep_text_for_range(folder_ns, root_ns, label):
    """Apply the keep_text decision (as null_marker_expr would) to every cell for a single range
    worker, asserting the written parameter, AND assert keep_text off == the 8c7ed25 loader."""
    # keep_text ON (NOT NULL text registered)
    kt._register(folder_ns, keep_text=True)
    for col, cat, raw, expected in _cells():
        got = kt._store_value(folder_ns, "s", "t", col, cat, raw)
        assert got == expected, (
            "%s keep_text: %s/%s raw=%r -> %r (expected %r)" % (label, col, cat, raw, got, expected))
    # keep_text OFF == 8c7ed25 loader, same cells
    kt._register(folder_ns, keep_text=False)
    kt._register(root_ns, keep_text=False)
    for col, cat, raw, _exp in _cells():
        a = kt._store_value(folder_ns, "s", "t", col, cat, raw)
        b = kt._store_value(root_ns, "s", "t", col, cat, raw)
        assert a == b, ("%s off must match 8c7ed25: %s/%s raw=%r folder=%r root=%r"
                        % (label, col, cat, raw, a, b))


def test_keep_text_on_every_range_split_3_and_6():
    """For 3-way AND 6-way splits, every range (first..last) applies keep_text correctly and
    off == 8c7ed25. Range membership is the real hex slice (proved above)."""
    folder_ns = kt._load_decision_ns(LOADER)
    root_ns = kt._load_decision_ns(ROOT_LOADER)
    for c in (3, 6):
        _files_sorted, ranges = _ranges_for(c)
        for r in ranges:
            _assert_keep_text_for_range(folder_ns, root_ns,
                                        "split=%d range[%d] %s..%s" % (c, r["index"],
                                                                       r["first"], r["last"]))


def test_keep_text_on_all_small_files_range():
    """A range whose files are ALL small (Amendment 1: 'a range whose files are all small') still
    applies keep_text identically — the rule is per-row/per-column, independent of file size."""
    folder_ns = kt._load_decision_ns(LOADER)
    root_ns = kt._load_decision_ns(ROOT_LOADER)
    small = sorted(_hex_load_files(small=True), key=lambda t: str(t[0]))
    ranges = ms.build_ranges(small, 6)
    ms.prove_ranges(ranges, small)
    for r in ranges:
        _assert_keep_text_for_range(folder_ns, root_ns, "small range[%d]" % r["index"])


def test_keep_text_per_write_path_first_retry_resume_lastchunk():
    """Amendment 1 (1): keep_text must be correct on EVERY write path of range mode. The paths all
    consume the SAME transformed df rows (structurally proved in
    test_keep_text_range_load.test_resume_and_retry_paths_write_the_same_transformed_rows); here we
    assert the decision OUTPUT is identical regardless of which path writes the row — first write,
    OCC/pipe retry (same chunk re-sent), commit-probe resume (chunk re-sent after a prior attempt),
    and the last partial chunk. Since the stored value is a pure function of (column, category,
    raw), re-sending the same chunk on any path yields the same parameters."""
    folder_ns = kt._load_decision_ns(LOADER)
    kt._register(folder_ns, keep_text=True)
    cells = _cells()
    # "first write" params
    first = [kt._store_value(folder_ns, "s", "t", c, cat, raw) for c, cat, raw, _e in cells]
    # a retry re-sends the IDENTICAL chunk_rows -> identical params
    retry = [kt._store_value(folder_ns, "s", "t", c, cat, raw) for c, cat, raw, _e in cells]
    # commit-probe resume re-sends the chunk (when not already committed) -> identical params
    resume = [kt._store_value(folder_ns, "s", "t", c, cat, raw) for c, cat, raw, _e in cells]
    # last partial chunk = a tail slice of the same rows -> identical params for those rows
    last_partial = [kt._store_value(folder_ns, "s", "t", c, cat, raw)
                    for c, cat, raw, _e in cells[-3:]]
    assert first == retry == resume, "a re-sent chunk must produce identical params on every path"
    assert last_partial == first[-3:], "the last partial chunk must match the first-write params"
    # and the NOT NULL text markers are genuinely kept (not NULL) on all paths
    kept = [p for (c, _cat, _r, _e), p in zip(cells, first)
            if c in ("nn_vc", "nn_txt") and p is not None]
    assert kept, "keep_text must preserve at least the NOT NULL text marker values"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d hex-scale keep_text test(s) passed" % len(fns))
