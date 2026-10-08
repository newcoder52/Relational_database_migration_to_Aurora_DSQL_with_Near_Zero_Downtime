#!/usr/bin/env python3
"""Offline regression test for the composite CDC NULL-coercion fix (C1 / rf-04-01).

scripts/glue_cdc_composite.py previously coerced a *set* of fuzzy, trimmed, upper-cased
"sentinels" ({"NULL","N/A","NA","NONE","(NULL)","\\N"}) to SQL NULL. That silently turned
legitimate values like 'NA' (a region code), 'NONE', 'N/A', 'null', '(NULL)', ' NULL ' into NULL
on CDC apply — corrupting data columns and, when such a value was part of a composite KEY,
blocking the whole table with "CDC row with empty key". The fix makes composite coerce NULL the
SAME way as scripts/glue_cdc_continuous.py (the correct reference): a real NULL is ONLY an empty
field or an EXACT, case-sensitive match to CSV_NULL_VALUE (the DMS null marker, default "NULL").

This test extracts the SHIPPED _coerce_null + the CSV_NULL_VALUE default from BOTH scripts via AST
(no awsglue/boto3/pg8000/Spark needed) and asserts composite now matches continuous on the
exact values that used to be corrupted. It FAILS before the fix (composite returned None for
'NA'/'NONE'/…) and PASSES after. Also enforces that composite reads --csv_null_value.

Run directly:  python3 tests/test_composite_coerce_null.py   (REPO_DIR overridable).
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SCRIPTS = os.path.join(REPO, "scripts")

_passed = 0
_failed = 0


def check(cond, msg):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"[PASS] {msg}")
    else:
        _failed += 1
        print(f"[FAIL] {msg}")
        import os as _os
        if "PYTEST_CURRENT_TEST" in _os.environ:
            raise AssertionError(msg)


def _extract_coerce_null(path, csv_null_value="NULL"):
    """AST-extract the module's _coerce_null and exec it with a given CSV_NULL_VALUE global."""
    src = open(path).read()
    tree = ast.parse(src)
    seg = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_coerce_null":
            seg = ast.get_source_segment(src, node)
            break
    if seg is None:
        raise AssertionError(f"_coerce_null not found in {path}")
    ns = {"CSV_NULL_VALUE": csv_null_value}
    exec(compile(seg, f"<{os.path.basename(path)}:_coerce_null>", "exec"), ns)
    return ns["_coerce_null"]


def test_composite_coerce_null_matches_continuous():
    comp = _extract_coerce_null(os.path.join(SCRIPTS, "glue_cdc_composite.py"))
    cont = _extract_coerce_null(os.path.join(SCRIPTS, "glue_cdc_continuous.py"))

    # Real NULLs: empty field and the EXACT default marker "NULL".
    for nullish in ("", "NULL"):
        check(comp(nullish) is None, f"composite: {nullish!r} -> None (real NULL)")

    # The values the OLD composite corrupted: these are DATA and MUST be preserved verbatim.
    preserved = ["NA", "N/A", "NONE", "(NULL)", "\\N", "null", "Null", " NULL ", "na", "none",
                 "0", "false", "normal text"]
    for v in preserved:
        check(comp(v) == v,
              f"composite: {v!r} kept verbatim (was corrupted to NULL before the fix)")

    # Composite must agree with the continuous reference on every case.
    for v in ["", "NULL", "NA", "N/A", "NONE", "(NULL)", "\\N", "null", " NULL ", "x"]:
        check(comp(v) == cont(v),
              f"composite matches continuous for {v!r} (got {comp(v)!r} vs {cont(v)!r})")

    # A non-default DMS marker is honored exactly (case-sensitive), nothing else coerced.
    comp_mk = _extract_coerce_null(os.path.join(SCRIPTS, "glue_cdc_composite.py"),
                                   csv_null_value="\\N")
    check(comp_mk("\\N") is None and comp_mk("NULL") == "NULL",
          "composite honors a configured CSV_NULL_VALUE marker exactly (\\N), keeps 'NULL' as data")


def test_composite_reads_csv_null_value_arg():
    src = open(os.path.join(SCRIPTS, "glue_cdc_composite.py")).read()
    check('"csv_null_value"' in src,
          "composite's optional-arg list includes csv_null_value (so the DMS marker is applied)")
    check("CSV_NULL_VALUE" in src and "NULL_SENTINELS" not in src,
          "composite uses CSV_NULL_VALUE (not the removed NULL_SENTINELS sentinel set)")


def main():
    for fn in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== composite _coerce_null: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
