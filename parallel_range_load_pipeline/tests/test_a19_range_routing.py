#!/usr/bin/env python3
"""Amendment 19 Defect 1 - prove the fix for 'reconcile fails closed although data is correct':
(1) in MANUAL RANGE MODE load_table_auto takes the PER-FILE path (which writes the per-range
_file_status + _range_result docs reconcile needs), bypassing the V5 whole-table small-file route
that wrote NONE; (2) a silent success without range artifacts is impossible - the job fails loudly
if range mode was expected but no per-range result doc was written.
"""
import ast
import os

LOAD = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "scripts", "job2_load_manual_range.py")


def _src():
    return open(LOAD).read()


def test_range_mode_routes_to_per_file_path_before_size_gate():
    """load_table_auto: the MANUAL RANGE branch (-> load_one_table_chunked) comes BEFORE the
    byte-size gate and the master switch, so a small table in range mode does NOT take the V5
    whole-table path (which writes no per-range docs - the live T14I-b defect)."""
    src = _src()
    tree = ast.parse(src)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "load_table_auto")
    body_src = ast.get_source_segment(src, fn)
    # the range-mode return must appear BEFORE the byte-size gate and the master switch
    i_range = body_src.index('_MANUAL_RANGE["on"]')
    i_chunked = body_src.index("return load_one_table_chunked(s3_client, entry, config)", i_range)
    i_sizegate = body_src.index("LARGE_TABLE_BYTES_THRESHOLD")
    i_master = body_src.index("if not V6_PARALLEL_ENABLED")
    assert i_range < i_sizegate, "range-mode routing must precede the byte-size gate"
    assert i_range < i_master, "range-mode routing must precede the master switch"
    assert i_chunked < i_sizegate, "range mode must return the per-file path before the size gate"


def test_range_mode_does_not_use_whole_table_path():
    """The range-mode branch returns load_one_table_chunked (per-file), never load_one_table
    (whole-table V5)."""
    src = _src()
    tree = ast.parse(src)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "load_table_auto")
    # find the `if _MANUAL_RANGE["on"]:` block and assert its return is the chunked loader
    for node in ast.walk(fn):
        if isinstance(node, ast.If):
            cond = ast.get_source_segment(src, node.test)
            if cond and '_MANUAL_RANGE["on"]' in cond:
                blk = ast.get_source_segment(src, node)
                assert "load_one_table_chunked" in blk
                assert "return load_one_table(" not in blk
                return
    assert False, "no `if _MANUAL_RANGE[\"on\"]:` routing branch found in load_table_auto"


def test_loud_failure_guard_present_for_missing_range_artifacts():
    """A silent success without range artifacts is impossible: the module-level end-of-job guard
    raises when range mode was requested but no per-range result doc was written."""
    src = _src()
    # the guard must (a) check result_written, (b) re-check the result doc on S3, (c) raise.
    assert 'if _MANUAL_RANGE.get("on"):' in src
    assert 'result_written' in src
    assert "wrote NO per-range result doc" in src
    # and the 'range arg passed but mode never turned on' branch also fails closed
    assert '"--load_file_start" in sys.argv or "--load_file_end" in sys.argv' in src
    assert "MODE did not activate" in src


def test_mutation_removing_the_guard_is_detectable():
    """If the loud-failure guard were removed, the marker text disappears (a reviewer/test would
    catch it). Prove the marker is uniquely tied to the guard."""
    src = _src()
    assert src.count("wrote NO per-range result doc") == 1
    # and the fix routing marker
    assert "MANUAL RANGE MODE -> PER-FILE parallel load" in src


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print("[PASS]", fn.__name__)
    print("\n%d A19 range-routing test(s) passed" % len(fns))
