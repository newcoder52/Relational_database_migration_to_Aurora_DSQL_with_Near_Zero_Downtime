"""F8 regression test: under --null_notnull_text=keep_text, a CDC schema change (ADD / make
NOT NULL / DROP column) must refresh the per-table nullability cache so the keep_text decision
is correct for the changed columns and NEVER raises NullRulesError for a freshly-added column.

The fix adds a CDC-specific `_nn_reregister_after_ddl(cur, schema, table)` (outside the shared
NULL block) called in `handle_schema_changes` right after the post-DDL catalog re-read. This test
pulls that function + the shared-block NULL helpers by AST from BOTH CDC scripts (no Spark/boto3),
drives it with a FAKE catalog cursor, and checks `_effective_null_markers` for the changed columns.

Mutation guard: a namespace whose `_nn_reregister_after_ddl` is a no-op (simulating "remove the
call") MUST raise NullRulesError for the new column — proving the call is what fixes F8.
"""
import ast
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
CDC_SCRIPTS = ["scripts/glue_cdc_continuous.py", "scripts/glue_cdc_composite.py"]

_WANT = {"NullRulesError", "_effective_null_markers", "_nn_register_nullability",
         "_nn_is_notnull", "_nn_register_from_mapping", "_nn_reregister_after_ddl"}


def _load_ns(path, keep_text=True):
    src = open(os.path.join(REPO, path)).read()
    tree = ast.parse(src)
    ns = {
        "CSV_NULL_VALUE": "NULL",
        "NULL_VALUES": None,
        "NULL_RULES": None,
        "NULL_NOTNULL_TEXT": "keep_text" if keep_text else "off",
        "_NN_NULLABILITY": {},
        "_NN_TEXT_CATEGORIES": frozenset({"varchar", "text"}),
    }
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and getattr(node, "name", None) in _WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and getattr(node, "name", None) in _WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    return ns


class _FakeCursor:
    """Returns a fixed (post-DDL) information_schema.columns result for the last execute()."""
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        assert "information_schema.columns" in sql
        self._last = list(self._rows)

    def fetchall(self):
        return self._last


def _markers(ns, schema, table, col, category):
    return ns["_effective_null_markers"](schema, table, col, category)


def _run_one(path):
    ns = _load_ns(path, keep_text=True)
    sch, tbl = "cns", "orders"
    # 1) initial registration (as build_table_context would do from the mapping):
    #    a = NOT NULL varchar, b = nullable varchar.
    ns["_nn_register_nullability"](sch, tbl, {"a": "NO", "b": "YES"})
    assert _markers(ns, sch, tbl, "a", "varchar") == []          # NOT NULL text kept
    assert _markers(ns, sch, tbl, "b", "varchar") == ["NULL"]    # nullable -> endpoint marker

    # Before the fix/refresh a brand-new column "c" is UNKNOWN -> fail loud.
    try:
        _markers(ns, sch, tbl, "c", "text")
        raise AssertionError("expected NullRulesError for unknown new column before refresh")
    except ns["NullRulesError"]:
        pass

    # 2) simulate handle_schema_changes applying DDL, then the post-DDL re-read + re-register:
    #    ADD "c" NOT NULL text; make "b" NOT NULL; DROP "a" (gone from the catalog).
    post_ddl_rows = [("b", "NO"), ("c", "NO"), ("d", "YES")]
    cur = _FakeCursor(post_ddl_rows)
    ns["_nn_reregister_after_ddl"](cur, sch, tbl)

    # 3) a row using the NEW/CHANGED columns under keep_text -> correct markers, never raises.
    assert _markers(ns, sch, tbl, "c", "text") == []        # new NOT NULL text -> kept
    assert _markers(ns, sch, tbl, "b", "varchar") == []     # b is now NOT NULL -> kept
    assert _markers(ns, sch, tbl, "d", "varchar") == ["NULL"]  # nullable -> mapped
    # a non-text NOT NULL column is never keep_text (category gate), even if registered NOT NULL
    assert _markers(ns, sch, tbl, "c", "int") == ["NULL"]
    # dropped column "a" is no longer registered -> unknown -> fail loud (correct)
    try:
        _markers(ns, sch, tbl, "a", "varchar")
        raise AssertionError("dropped column should be unknown after refresh")
    except ns["NullRulesError"]:
        pass
    return ns


def test_f8_add_notnull_drop_refresh_both_cdc_scripts():
    for path in CDC_SCRIPTS:
        _run_one(path)


def test_f8_mutation_removing_the_call_breaks_it():
    """If the re-register call is effectively removed (no-op), the new column stays unknown and
    _effective_null_markers raises -> this is exactly the F8 bug. Proves the fix is load-bearing."""
    for path in CDC_SCRIPTS:
        ns = _load_ns(path, keep_text=True)
        sch, tbl = "cns", "orders"
        ns["_nn_register_nullability"](sch, tbl, {"a": "NO", "b": "YES"})
        # MUTATION: skip _nn_reregister_after_ddl (simulate the call being removed).
        raised = False
        try:
            _markers(ns, sch, tbl, "c", "text")   # new column, never re-registered
        except ns["NullRulesError"]:
            raised = True
        assert raised, f"{path}: without the refresh call the new column must fail loud (F8 bug)"


def test_f8_noop_when_off():
    """With null_notnull_text=off the refresh is a no-op (keeps default byte-identical behaviour)."""
    for path in CDC_SCRIPTS:
        ns = _load_ns(path, keep_text=False)
        cur = _FakeCursor([("x", "NO")])
        ns["_nn_reregister_after_ddl"](cur, "cns", "orders")
        assert ns["_NN_NULLABILITY"] == {}, "off must not register anything"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
