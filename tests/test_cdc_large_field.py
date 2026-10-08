#!/usr/bin/env python3
"""Regression: the CDC jobs must read a CDC file whose row has a very large CLOB/text field.

The bug
-------
`scripts/glue_cdc_continuous.py` and `scripts/glue_cdc_composite.py` read CDC CSV files with
`read_cdc_file()`, which uses the stdlib `csv.reader`. The stdlib default field limit is
131072 chars (128 KiB). Neither script raised `csv.field_size_limit`, so a CDC row carrying a
CLOB/text value over 128 KiB made `csv.reader` raise:

    _csv.Error: field larger than field limit (131072)

`process_table` treats that as a transient error and retries the same file every poll forever,
wedging the table with all later files pending (observed in the field: 884 pending files).

The fix
-------
Right after `import csv`, each CDC script now calls `csv.field_size_limit(2**31 - 1)`
(2**31-1 fits a C long on every platform, so no OverflowError, and it needs no `sys`).

What this test does
-------------------
For EACH script it extracts, from the REAL current source (by AST / line slice so the test
tracks whatever the file actually contains):
  * every module-level `csv.field_size_limit(...)` statement, and
  * the `read_cdc_file` function.
It then execs them in an offline namespace (real `csv`/`io`, a stubbed `s3` that returns a CSV
with a 500 KiB field) with the process-global limit first reset to the stdlib default. So:
  * BEFORE the fix: the script has no field_size_limit call, the limit stays at 131072, and
    read_cdc_file raises csv.Error on the 500 KiB field  -> check FAILS.
  * AFTER the fix: the script's own field_size_limit(2**31-1) runs, the 500 KiB field parses
    -> check PASSES.

Run directly:   python3 tests/test_cdc_large_field.py   (exit 1 if any check fails)
Under pytest:   a failed check() raises AssertionError (so check() must raise under pytest).
"""
import ast
import csv
import io
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)

SCRIPTS = [
    os.path.join(REPO, "scripts", "glue_cdc_continuous.py"),
    os.path.join(REPO, "scripts", "glue_cdc_composite.py"),
]

STDLIB_DEFAULT_LIMIT = 131072       # csv's default field size limit (128 KiB)
BIG_FIELD = "x" * (500 * 1024)      # 500 KiB single field -> well over the default

RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append(bool(cond))
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"   <- {str(extra)[:400]}" if (not cond and extra) else ""))
    # Under pytest, surface a failed check as a real failure.
    if not cond and "PYTEST_CURRENT_TEST" in os.environ:
        raise AssertionError(f"{name}" + (f": {extra}" if extra else ""))


def _is_field_size_limit_call(node):
    """True for a module-level expression statement that calls csv.field_size_limit(...)."""
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return False
    f = node.value.func
    return isinstance(f, ast.Attribute) and f.attr == "field_size_limit" and \
        isinstance(f.value, ast.Name) and f.value.id == "csv"


def _extract(script):
    """Return (list_of_field_size_limit_source_lines, read_cdc_file_source)."""
    with open(script, "r", encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src)
    limit_srcs, read_src = [], None
    for node in tree.body:
        if _is_field_size_limit_call(node):
            limit_srcs.append(ast.get_source_segment(src, node))
        elif isinstance(node, ast.FunctionDef) and node.name == "read_cdc_file":
            read_src = ast.get_source_segment(src, node)
    return limit_srcs, read_src


class _FakeBody:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _FakeS3:
    """Minimal stub: get_object returns a CSV whose single data row has a 500 KiB field."""
    def __init__(self, csv_text):
        self._bytes = csv_text.encode("utf-8")

    def get_object(self, Bucket=None, Key=None):
        return {"Body": _FakeBody(self._bytes)}


def _run_read(script):
    """Exec the script's csv-limit setup + read_cdc_file offline against a 500 KiB-field CSV.
    Returns (header, rows) or raises whatever read_cdc_file raises."""
    limit_srcs, read_src = _extract(script)
    assert read_src is not None, f"could not find def read_cdc_file in {script}"

    csv_text = "id,blob\n1," + BIG_FIELD + "\n"

    g = {
        "__builtins__": __builtins__,
        "csv": csv,
        "io": io,
        "s3": _FakeS3(csv_text),
        "BUCKET": "fake-bucket",
    }
    # The script's own field_size_limit statement(s), run exactly as the module would. Before
    # the fix there are none, so the default (reset below) stays in effect.
    for s in limit_srcs:
        exec(compile(s, script, "exec"), g)
    exec(compile(read_src, script, "exec"), g)
    return g["read_cdc_file"]("some/table/cdc-0001.csv")


def run():
    for script in SCRIPTS:
        label = os.path.basename(script)
        # Reset the process-global limit to the stdlib default so each script is judged ONLY
        # by whether its OWN source raises the limit (order-independent, no cross-talk).
        csv.field_size_limit(STDLIB_DEFAULT_LIMIT)

        raised = None
        header = rows = None
        try:
            header, rows = _run_read(script)
        except Exception as e:  # noqa: BLE001 - we classify below
            raised = e

        ok = raised is None
        check(f"{label}: read_cdc_file parses a 500 KiB CDC field without csv.Error",
              ok, raised if raised else "")
        if ok:
            check(f"{label}: parsed header + exactly one 500 KiB data row",
                  header == ["id", "blob"] and len(rows) == 1 and len(rows[0][1]) == len(BIG_FIELD),
                  f"header={header} nrows={None if rows is None else len(rows)}")

    passed = sum(1 for r in RESULTS if r)
    print(f"\n{passed}/{len(RESULTS)} checks passed")
    return all(RESULTS)


def test_cdc_large_field():
    """pytest entrypoint: a failed check() raises AssertionError inside run()."""
    assert run()


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
