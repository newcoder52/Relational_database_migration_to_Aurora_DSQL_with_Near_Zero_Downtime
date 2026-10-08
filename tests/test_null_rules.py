#!/usr/bin/env python3
"""Offline tests for CUSTOMER-CONTROLLED NULL HANDLING (two params.csv settings:
null_values and null_rules).

What the feature is
-------------------
  * null_values : a '|'-separated list of exact strings that become SQL NULL for EVERY column.
                  Blank = today's behaviour (only the DMS endpoint CsvNullValue, passed as
                  --csv_null_value, and an empty field are NULL). Set -> REPLACES that marker.
  * null_rules  : per-column overrides 'schema.table.column=none|V1|V2' joined by ';'. 'none'
                  means no text value becomes NULL in that column (the text 'NULL' is KEPT as
                  data); a list REPLACES the default for that column. Names match case-
                  insensitively against the DSQL lowercase schema/table/column.
  * Precedence for a column: a null_rules entry, THEN null_values, THEN the endpoint marker.
  * EMPTY fields are ALWAYS a real NULL, in every mode including 'none' (unchanged).
  * With both settings blank/absent every script is BYTE-IDENTICAL to today.

The parse/match logic lives in ONE shared block copied byte-identically into the four Glue
scripts (job2_load, job3_validate, glue_cdc_continuous, glue_cdc_composite); those scripts do
not share imports. This test:
  1. extracts the shared block + _coerce_null from each script by AST / markers (no awsglue /
     boto3 / pg8000 / Spark needed) and exercises the semantics;
  2. asserts the four copies of the shared block are BYTE-IDENTICAL;
  3. drives the loader/validate Spark `null_marker_expr` through a tiny Spark-column SIMULATOR
     and the CDC _coerce_null through the real function on the SAME inputs, and asserts the
     load, validate and CDC paths AGREE on which values become NULL;
  4. asserts a malformed null_rules FAILS preflight (resolve_task._validate_settings /
     params_csv.to_pipeline_settings), naming the bad entry.

Each check() raises under pytest. Run directly:  python3 tests/test_null_rules.py
REPO_DIR overridable.  Each test FAILS before the feature and PASSES after.
"""
import ast
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SCRIPTS = os.path.join(REPO, "scripts")
LAMBDAS = os.path.join(REPO, "lambdas")

SCRIPT_FILES = ["job2_load.py", "job3_validate.py",
                "glue_cdc_continuous.py", "glue_cdc_composite.py"]
LOADER_SCRIPTS = ["job2_load.py", "job3_validate.py"]        # Spark null_marker_expr
CDC_SCRIPTS = ["glue_cdc_continuous.py", "glue_cdc_composite.py"]  # Python _coerce_null

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
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise AssertionError(msg)


# ------------------------------------------------------------------------------------------
# Extract the shared block (as a byte string, between the markers) from a script.
# ------------------------------------------------------------------------------------------
_START = "# SHARED NULL-RULES BLOCK"
_END = "# END SHARED NULL-RULES BLOCK"


def _extract_block(path):
    s = open(path).read()
    i = s.index(_START)
    j = s.index(_END)
    j = s.index("\n", j) + 1
    bs = s.rfind("# =====", 0, i)
    return s[bs:j]


# ------------------------------------------------------------------------------------------
# Build an executable namespace from a script: the shared block functions + _coerce_null,
# exec'd with a chosen CSV_NULL_VALUE. No Spark / boto3 / awsglue needed (we only pull the
# pure-Python functions by AST).
# ------------------------------------------------------------------------------------------
_WANT = {"NullRulesError", "_parse_null_values", "_parse_null_rules",
         "_effective_null_markers", "_apply_null_settings", "_null_rules_unknown_warnings",
         "_coerce_null"}


def _load_ns(path, csv_null_value="NULL"):
    src = open(path).read()
    tree = ast.parse(src)
    ns = {"CSV_NULL_VALUE": csv_null_value, "NULL_VALUES": None, "NULL_RULES": None}
    for node in tree.body:
        name = getattr(node, "name", None)
        if isinstance(node, ast.ClassDef) and name in _WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    for node in tree.body:
        name = getattr(node, "name", None)
        if isinstance(node, ast.FunctionDef) and name in _WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    return ns


# ------------------------------------------------------------------------------------------
# Tiny Spark-column SIMULATOR so we can evaluate the loader/validate null_marker_expr against
# a concrete raw value, offline. Each "column expression" is a Python callable row->value;
# col/lit/when/trim mirror the subset null_marker_expr uses.
# ------------------------------------------------------------------------------------------
class _Expr:
    def __init__(self, fn):
        self.fn = fn

    def __eq__(self, other):
        o = other.fn if isinstance(other, _Expr) else (lambda r, v=other: v)
        return _Expr(lambda r: self.fn(r) == o(r))

    def __or__(self, other):
        o = other.fn if isinstance(other, _Expr) else (lambda r, v=other: v)
        return _Expr(lambda r: bool(self.fn(r)) or bool(o(r)))

    def isNull(self):
        return _Expr(lambda r: self.fn(r) is None)


def _col(name):
    return _Expr(lambda r, n=name: r.get(n))


def _lit(v):
    return _Expr(lambda r, v=v: v)


def _trim(e):
    return _Expr(lambda r, e=e: (e.fn(r).strip() if isinstance(e.fn(r), str) else e.fn(r)))


class _When:
    def __init__(self, pred, val):
        self._cases = [(pred, val)]

    def when(self, pred, val):
        self._cases.append((pred, val))
        return self

    def otherwise(self, val):
        self._otherwise = val
        return self

    def eval(self, row):
        for pred, val in self._cases:
            if bool(pred.fn(row)):
                return val.fn(row) if isinstance(val, _Expr) else val
        v = self._otherwise
        return v.fn(row) if isinstance(v, _Expr) else v


def _when(pred, val):
    return _When(pred, val)


def _extract_fn(path, fnname, extra_globals):
    """AST-extract one function from a script and exec it with injected globals (the shared
    block functions + the Spark simulator). Returns the callable."""
    src = open(path).read()
    tree = ast.parse(src)
    seg = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == fnname:
            seg = ast.get_source_segment(src, node)
            break
    assert seg is not None, f"{fnname} not found in {path}"
    ns = dict(extra_globals)
    exec(compile(seg, f"<{os.path.basename(path)}:{fnname}>", "exec"), ns)
    return ns[fnname], ns


def _load_null_marker_expr(path, csv_null_value="NULL", null_values=None, null_rules=None):
    """Build the loader/validate null_marker_expr with the shared block + Spark simulator wired
    in, with the two settings already applied. Returns (fn, ns)."""
    ns = _load_ns(path, csv_null_value)
    ns["_apply_null_settings"](null_values, null_rules)
    g = dict(ns)
    g.update({"col": _col, "lit": _lit, "when": _when, "trim": _trim})
    # _TYPED_CATEGORIES: pull the frozenset literal from the script so typed-column trimming is
    # exactly the shipped set.
    src = open(path).read()
    tcat = {}
    exec(compile(ast.parse(_assign_segment(src, "_TYPED_CATEGORIES")), "<tc>", "exec"), tcat)
    g["_TYPED_CATEGORIES"] = tcat["_TYPED_CATEGORIES"]
    fn, _ = _extract_fn(path, "null_marker_expr", g)
    return fn, g


def _assign_segment(src, name):
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.get_source_segment(src, node)
    raise AssertionError(f"{name} assignment not found")


def _spark_is_null(fn, raw, category="varchar", schema="cns", table="orders", column="status"):
    """Evaluate null_marker_expr for one raw value -> True if it resolves to SQL NULL."""
    expr = fn(column, category, schema, table)
    return expr.eval({column: raw}) is None


# =========================================================================================
# TESTS
# =========================================================================================
def test_four_copies_identical():
    blocks = {f: _extract_block(os.path.join(SCRIPTS, f)) for f in SCRIPT_FILES}
    first = blocks[SCRIPT_FILES[0]]
    for f in SCRIPT_FILES:
        check(blocks[f] == first,
              f"shared null-rules block in {f} is byte-identical to {SCRIPT_FILES[0]}")
    check(len(first) > 500 and "_effective_null_markers" in first,
          "the extracted block is the real shared block (non-trivial, has _effective_null_markers)")


def test_defaults_unchanged():
    """Both settings unset -> ONLY the endpoint marker (and empty field) is NULL; identical to
    today, in every script, for the values earlier versions wrongly coerced."""
    kept = ["NA", "N/A", "NONE", "(NULL)", "\\N", "null", "Null", " NULL ", "na", "0",
            "false", "normal text"]
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        coerce, eff = ns["_coerce_null"], ns["_effective_null_markers"]
        m = eff("cns", "orders", "status")
        check(m == ["NULL"], f"{f}: default effective markers == ['NULL'] (endpoint marker)")
        check(coerce("NULL", m) is None, f"{f}: default -> 'NULL' is NULL")
        check(coerce("", m) is None, f"{f}: default -> empty field is NULL")
        for v in kept:
            check(coerce(v, m) == v, f"{f}: default -> {v!r} kept as data")
        # one-arg _coerce_null (no markers) is EXACTLY today's behaviour (the AST regression path)
        check(coerce("NULL") is None and coerce("NA") == "NA" and coerce("") is None,
              f"{f}: _coerce_null(v) with no markers == today's behaviour")
    for f in LOADER_SCRIPTS:
        fn, _ = _load_null_marker_expr(os.path.join(SCRIPTS, f))
        check(_spark_is_null(fn, "NULL"), f"{f}: Spark default -> 'NULL' is NULL")
        check(_spark_is_null(fn, ""), f"{f}: Spark default -> empty is NULL")
        for v in ["NA", "none", "null", " NULL "]:
            check(not _spark_is_null(fn, v), f"{f}: Spark default -> {v!r} kept")


def test_null_values_two_values():
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        ns["_apply_null_settings"]("NULL|NA", None)
        eff, coerce = ns["_effective_null_markers"], ns["_coerce_null"]
        m = eff("cns", "orders", "region")
        check(m == ["NULL", "NA"], f"{f}: null_values 'NULL|NA' -> both markers for all columns")
        check(coerce("NULL", m) is None and coerce("NA", m) is None,
              f"{f}: null_values -> 'NULL' and 'NA' both NULL")
        check(coerce("NONE", m) == "NONE", f"{f}: null_values -> 'NONE' still data")
        check(coerce("", m) is None, f"{f}: null_values -> empty still NULL")
    for f in LOADER_SCRIPTS:
        fn, _ = _load_null_marker_expr(os.path.join(SCRIPTS, f), null_values="NULL|NA")
        check(_spark_is_null(fn, "NULL") and _spark_is_null(fn, "NA"),
              f"{f}: Spark null_values -> 'NULL' and 'NA' NULL")
        check(not _spark_is_null(fn, "NONE"), f"{f}: Spark null_values -> 'NONE' data")


def test_null_rules_none_keeps_null_text():
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        ns["_apply_null_settings"](None, "cns.orders.status=none")
        eff, coerce = ns["_effective_null_markers"], ns["_coerce_null"]
        m = eff("cns", "orders", "status")
        check(m == [], f"{f}: rule 'none' -> no text markers for that column")
        check(coerce("NULL", m) == "NULL", f"{f}: rule 'none' -> text 'NULL' KEPT as data")
        check(coerce("", m) is None, f"{f}: rule 'none' -> empty field STILL NULL (unchanged)")
        # a different column still uses the endpoint default
        check(eff("cns", "orders", "other") == ["NULL"],
              f"{f}: rule 'none' only affects its own column")
    for f in LOADER_SCRIPTS:
        fn, _ = _load_null_marker_expr(os.path.join(SCRIPTS, f), null_rules="cns.orders.status=none")
        check(not _spark_is_null(fn, "NULL", column="status"),
              f"{f}: Spark rule 'none' -> 'NULL' kept")
        check(_spark_is_null(fn, "", column="status"),
              f"{f}: Spark rule 'none' -> empty still NULL")


def test_per_column_list():
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        ns["_apply_null_settings"](None, "cns.orders.region=NULL|NA")
        eff, coerce = ns["_effective_null_markers"], ns["_coerce_null"]
        m = eff("cns", "orders", "region")
        check(m == ["NULL", "NA"], f"{f}: per-column list -> ['NULL','NA'] for that column")
        check(coerce("NA", m) is None, f"{f}: per-column list -> 'NA' NULL in region")
        check(eff("cns", "orders", "status") == ["NULL"],
              f"{f}: per-column list leaves other columns on the endpoint default")


def test_precedence():
    """rule > null_values > endpoint. A column with a rule uses the rule; a column without a
    rule uses null_values; neither -> endpoint marker."""
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        ns["_apply_null_settings"]("X", "cns.orders.status=none; cns.orders.region=Y")
        eff = ns["_effective_null_markers"]
        check(eff("cns", "orders", "status") == [], f"{f}: precedence rule 'none' beats null_values")
        check(eff("cns", "orders", "region") == ["Y"], f"{f}: precedence rule list beats null_values")
        check(eff("cns", "orders", "other") == ["X"], f"{f}: no rule -> null_values applies")
    # endpoint fallback when both unset is covered by test_defaults_unchanged.


def test_value_case_sensitivity():
    """Marker VALUE matching is exact + case-sensitive (names are case-insensitive)."""
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        ns["_apply_null_settings"](None, "cns.orders.status=NULL")
        eff, coerce = ns["_effective_null_markers"], ns["_coerce_null"]
        m = eff("cns", "orders", "status")
        check(coerce("NULL", m) is None, f"{f}: value match 'NULL' -> NULL")
        check(coerce("null", m) == "null", f"{f}: value match is case-sensitive ('null' kept)")
        check(coerce("NULL ", m) == "NULL ", f"{f}: value match is whole-value (' ' matters)")
        # NAME match is case-insensitive (DSQL lowercases schema/table/column)
        check(eff("CNS", "Orders", "STATUS") == eff("cns", "orders", "status"),
              f"{f}: name match is case-insensitive")


def test_empty_fields_unchanged():
    """An empty field is ALWAYS a real NULL — default, null_values, and a 'none' rule alike."""
    for f in CDC_SCRIPTS:
        ns = _load_ns(os.path.join(SCRIPTS, f))
        for nv, nr, label in [(None, None, "default"),
                              ("NA", None, "null_values"),
                              (None, "cns.orders.status=none", "rule none")]:
            ns["_apply_null_settings"](nv, nr)
            eff, coerce = ns["_effective_null_markers"], ns["_coerce_null"]
            m = eff("cns", "orders", "status")
            check(coerce("", m) is None, f"{f}: empty field is NULL under {label}")
            check(coerce(None, m) is None, f"{f}: Python None is NULL under {label}")


def test_load_validate_cdc_agree():
    """The loader (job2 Spark), validate (job3 Spark) and CDC (_coerce_null) paths must make the
    SAME NULL/keep decision for the same inputs and the same settings — or validation would
    false-flag a correctly-loaded row."""
    cases = [
        # (settings_null_values, settings_null_rules, category, column, raw, expect_null)
        (None, None, "varchar", "status", "NULL", True),
        (None, None, "varchar", "status", "NA", False),
        (None, None, "varchar", "status", "", True),
        ("NULL|NA", None, "varchar", "region", "NA", True),
        ("NULL|NA", None, "varchar", "region", "NONE", False),
        (None, "cns.orders.status=none", "varchar", "status", "NULL", False),
        (None, "cns.orders.status=none", "varchar", "status", "", True),
        (None, "cns.orders.region=NULL|NA", "varchar", "region", "NA", True),
        (None, "cns.orders.status=NULL", "varchar", "status", "null", False),  # case-sensitive
    ]
    # CDC decision via _coerce_null
    cdc_ns = {f: _load_ns(os.path.join(SCRIPTS, f)) for f in CDC_SCRIPTS}
    # loader/validate decision via the Spark simulator
    for nv, nr, cat, col, raw, want_null in cases:
        decisions = {}
        for f in CDC_SCRIPTS:
            ns = cdc_ns[f]
            ns["_apply_null_settings"](nv, nr)
            m = ns["_effective_null_markers"]("cns", "orders", col)
            decisions[f] = ns["_coerce_null"](raw, m) is None
        for f in LOADER_SCRIPTS:
            fn, _ = _load_null_marker_expr(os.path.join(SCRIPTS, f), null_values=nv, null_rules=nr)
            decisions[f] = _spark_is_null(fn, raw, category=cat, column=col)
        vals = set(decisions.values())
        check(vals == {want_null},
              f"load/validate/CDC agree raw={raw!r} nv={nv!r} nr={nr!r} -> "
              f"NULL={want_null} (got {decisions})")


def test_malformed_fails_preflight():
    """A malformed null_rules / null_values must FAIL preflight (never silently ignored),
    naming the bad entry. Checked through BOTH resolve_task._validate_settings (pipeline.json
    path) and params_csv.to_pipeline_settings (params.csv path)."""
    sys.path.insert(0, LAMBDAS)
    if "boto3" not in sys.modules:
        sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: None)
    import importlib
    rt = importlib.import_module("resolve_task")
    pc = importlib.import_module("params_csv")

    base = {"project": "proj", "region": "us-east-1",
            "dsql_endpoint": "abc.dsql.us-east-1.on.aws",
            "glue_role_arn": "arn:aws:iam::123456789012:role/proj-x-glue-exec-role"}

    def _rt_settings(nv="", nr=""):
        cfg = dict(rt.SETTINGS_DEFAULTS)
        cfg.update(base)
        cfg["null_values"] = nv
        cfg["null_rules"] = nr
        return rt._validate_settings(dict(cfg), [])

    # valid: both blank, a list, a none rule, a per-col list — all OK
    for nv, nr in [("", ""), ("NULL|NA", ""),
                   ("", "cns.orders.status=none"),
                   ("", "cns.orders.region=NULL|NA; cns.orders.status=none")]:
        try:
            _rt_settings(nv, nr)
            ok = True
        except rt.SettingsError:
            ok = False
        check(ok, f"valid null settings accepted: null_values={nv!r} null_rules={nr!r}")

    # malformed -> SettingsError naming the bad entry
    bad_rules = ["cns.orders.status",          # no '='
                 "cns.orders=none",            # not schema.table.column
                 "cns.orders.status=",         # empty VALUES
                 "a.b.c=X||Y",                 # empty marker in list
                 "a.b.c=NULL; a.b.c=NA"]       # duplicate column
    for nr in bad_rules:
        try:
            _rt_settings("", nr)
            raised = None
        except rt.SettingsError as e:
            raised = str(e)
        check(raised is not None and "null_rules" in raised and (nr.split(";")[0].strip() in raised
                                                                 or "null_rules" in raised),
              f"resolve_task rejects malformed null_rules {nr!r} (msg: {str(raised)[:80]})")

    # malformed null_values (empty token)
    try:
        _rt_settings("NULL||NA", "")
        raised = None
    except rt.SettingsError as e:
        raised = str(e)
    check(raised is not None and "null_values" in raised,
          "resolve_task rejects null_values with an empty '|' token")

    # Same via params_csv.to_pipeline_settings (ParamsError), which preflight_tasks calls.
    csv = ("parameter,value\naccount_id,123456789012\nregion,us-east-1\nproject,proj\n"
           "dsql_endpoint,abc.dsql.us-east-1.on.aws\nnull_rules,cns.orders=none\n")
    parsed = pc.parse(csv)
    try:
        pc.to_pipeline_settings(parsed["params"])
        raised = None
    except pc.ParamsError as e:
        raised = str(e)
    check(raised is not None and "null_rules" in raised,
          "params_csv.to_pipeline_settings (preflight path) rejects a malformed null_rules")


def main():
    for fn in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== null-rules: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
