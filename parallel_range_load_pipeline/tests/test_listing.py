#!/usr/bin/env python3
"""Listing tests for mrl_split.list_table_files / resolve_table_folder / assert_* against the
FakeS3 (pagination across 3+ pages, processed/failed exclusion, duplicate basenames across
case-variant folders, hex names, zero-byte files, non-LOAD files, width check)."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lambdas"))
sys.path.insert(0, HERE)

import mrl_split as ms  # noqa: E402
from mrl_fakes import FakeS3  # noqa: E402


def test_basic_listing_sorted_by_name():
    s3 = FakeS3()
    s3.put_load_files("b", "schema/table", [3, 1, 2])
    files, info = ms.list_table_files(s3, "s3://b/schema/table/")
    names = [ms.basename(u) for u, _s in files]
    assert names == ["LOAD00000001.csv", "LOAD00000002.csv", "LOAD00000003.csv"]
    assert info["num_files"] == 3


def test_pagination_across_three_pages():
    s3 = FakeS3(page_size=100)   # 406 files -> 5 pages
    s3.put_load_files("b", "s/t", list(range(1, 407)))
    files, info = ms.list_table_files(s3, "s3://b/s/t/")
    assert info["num_files"] == 406
    # strictly increasing by name
    names = [ms.basename(u) for u, _s in files]
    assert names == sorted(names)
    assert names[0] == "LOAD00000001.csv" and names[-1] == "LOAD00000406.csv"


def test_excludes_processed_failed_and_subfolders():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [1, 2])
    s3.put_raw("b", "s/t/processed/LOAD00000003.csv")
    s3.put_raw("b", "s/t/failed/LOAD00000004.csv")
    s3.put_raw("b", "s/t/partition=1/LOAD00000005.csv")   # any sub-folder excluded
    s3.put_raw("b", "s/t/20260921-172204370.csv")          # CDC timestamp file (not LOAD)
    files, info = ms.list_table_files(s3, "s3://b/s/t/")
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv", "LOAD00000002.csv"]


def test_zero_byte_excluded_but_reported():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [(1, 1000), (2, 0), (3, 500)])
    files, info = ms.list_table_files(s3, "s3://b/s/t/")
    # AMENDMENT 6/2: a zero-byte LOAD file is NOT loaded by the loader, so it is EXCLUDED from
    # the splittable set (num_files=2) but REPORTED (zero_byte_count=1).
    assert info["num_files"] == 2
    assert info["zero_byte_count"] == 1
    assert ms.basename(info["zero_byte_files"][0]) == "LOAD00000002.csv"
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv", "LOAD00000003.csv"]


def test_hex_allowed_by_default_decimal_only_on_request():
    s3 = FakeS3()
    # hex names with letters (DMS names can be hex per the loader comment)
    for name in ("LOAD0000000a.csv", "LOAD0000000b.csv"):
        s3.put_raw("b", "s/t/%s" % name, size=100)
    # AMENDMENT 6: hex is allowed BY DEFAULT.
    files_default, _ = ms.list_table_files(s3, "s3://b/s/t/")
    assert len(files_default) == 2
    # decimal-only restriction on request rejects hex bodies
    files_dec, _ = ms.list_table_files(s3, "s3://b/s/t/", allow_hex=False)
    assert files_dec == []


def test_non_load_files_ignored():
    s3 = FakeS3()
    s3.put_load_files("b", "s/t", [1])
    s3.put_raw("b", "s/t/README.txt")
    s3.put_raw("b", "s/t/manifest.json")
    s3.put_raw("b", "s/t/LOADING.csv")     # 'loading' is not load<digits>
    files, _ = ms.list_table_files(s3, "s3://b/s/t/")
    assert [ms.basename(u) for u, _s in files] == ["LOAD00000001.csv"]


def test_assert_unique_basenames_fails_on_dup():
    files = [("s3://b/S/T/LOAD00000001.csv", 1), ("s3://b/s/t/LOAD00000001.csv", 1)]
    try:
        ms.assert_unique_basenames(files)
        assert False
    except ms.MrlSplitError:
        pass
    # unique set is fine
    ok = [("s3://b/s/t/LOAD00000001.csv", 1), ("s3://b/s/t/LOAD00000002.csv", 1)]
    assert ms.assert_unique_basenames(ok) == 2


def test_assert_uniform_width_fails_on_mixed():
    mixed = [("s3://b/s/t/LOAD001.csv", 1), ("s3://b/s/t/LOAD00000002.csv", 1)]
    try:
        ms.assert_uniform_width(mixed)
        assert False
    except ms.MrlSplitError:
        pass
    same = [("s3://b/s/t/LOAD00000001.csv", 1), ("s3://b/s/t/LOAD00000002.csv", 1)]
    assert ms.assert_uniform_width(same) is True


def test_r21_1_assert_uniform_width_rejects_mixed_case_hex():
    """R21-1: same-width hex names in MIXED case sort by name differently than they resolve
    numerically (and differ only by case => same hex value => a duplicate). Fail closed naming
    the files."""
    mixed_case = [("s3://b/s/t/LOAD0000000A.csv", 1), ("s3://b/s/t/load0000000b.csv", 1)]
    try:
        ms.assert_uniform_width(mixed_case)
        assert False, "mixed upper/lower-case hex must fail closed"
    except ms.MrlSplitError as e:
        assert "UPPER" in str(e) and "lower" in str(e)
        assert "LOAD0000000A.csv" in str(e) and "load0000000b.csv" in str(e)
    # consistent case (all upper) is fine
    all_upper = [("s3://b/s/t/LOAD0000000A.csv", 1), ("s3://b/s/t/LOAD0000000B.csv", 1)]
    assert ms.assert_uniform_width(all_upper) is True
    # consistent case (all lower) is fine
    all_lower = [("s3://b/s/t/load0000000a.csv", 1), ("s3://b/s/t/load0000000b.csv", 1)]
    assert ms.assert_uniform_width(all_lower) is True
    # pure decimal (no hex letters) is unaffected by case logic
    decimal = [("s3://b/s/t/LOAD00000001.csv", 1), ("s3://b/s/t/LOAD00000002.csv", 1)]
    assert ms.assert_uniform_width(decimal) is True


def test_resolve_table_folder_case_insensitive():
    s3 = FakeS3()
    s3.put_load_files("b", "MySchema/MyTable", [1, 2])
    loc = ms.resolve_table_folder(s3, "b", ".", "myschema", "mytable")
    assert loc["found"] is True
    assert loc["schema"] == "MySchema" and loc["table"] == "MyTable"


def test_resolve_table_folder_two_case_variants_fail_closed():
    s3 = FakeS3()
    s3.put_load_files("b", "s/Table", [1])
    s3.put_load_files("b", "s/table", [2])   # two case variants both hold LOAD files
    try:
        ms.resolve_table_folder(s3, "b", ".", "s", "table")
        assert False, "should fail closed on two case-variant folders"
    except ms.MrlSplitError:
        pass


def test_resolve_table_folder_with_bucket_folder_root():
    s3 = FakeS3()
    s3.put_load_files("b", "cdc/s/t", [1, 2, 3])
    loc = ms.resolve_table_folder(s3, "b", "cdc", "s", "t")
    assert loc["found"] and loc["dms_s3_path"] == "s3://b/cdc/s/t/"
    files, info = ms.list_table_files(s3, loc["dms_s3_path"])
    assert info["num_files"] == 3


def test_listing_sha256_stable_and_order_independent_of_seed():
    files1 = [("s3://b/t/LOAD%08d.csv" % i, i) for i in (1, 2, 3)]
    files2 = [("s3://b/t/LOAD%08d.csv" % i, i * 99) for i in (3, 1, 2)]
    files2.sort(key=lambda t: t[0])
    assert ms.listing_sha256(files1) == ms.listing_sha256(files2)   # depends on names only


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d listing test(s) passed" % len(fns))
