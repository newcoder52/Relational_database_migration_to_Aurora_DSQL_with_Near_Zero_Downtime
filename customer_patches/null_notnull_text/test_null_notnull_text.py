#!/usr/bin/env python3
"""Offline, executing tests for the --null_notnull_text feature (automatic NOT NULL char/text
keep-text rule). No AWS / DB / Spark: the shared null block + null_marker_expr + _coerce_null are
AST-extracted from the FOUR Glue scripts (and MRL copy) exactly like tests/test_null_rules.py,
and the deployed-env apply script is exercised against a fake boto3.

Covers:
  * matrix (type category x nullability x setting) for the shared _effective_null_markers;
  * each of the four scripts' REAL call sites (null_marker_expr for loader/validate; _coerce_null
    with the markers from _effective_null_markers for CDC) via AST extraction;
  * byte-identical-when-off proof (the four + MRL shared blocks are byte-identical AFTER patching,
    and off == today for every value the pre-feature code kept/nulled);
  * load/validate/CDC PARITY (same NULL/keep decision) so validation never false-flags;
  * old-mapping fallback (_nn_register_from_mapping returns False -> caller must catalog-query);
  * apply-script fake-boto3 tests: dry-run no change, apply, second apply no-op, restore,
    read-back, wrong-shape job skipped;
  * 3 mutation checks that MUST fail: rule applied to nullable cols; to int cols; ignoring a
    null_rules entry.

Run:  python3 test_null_notnull_text.py       (or via pytest)
REPO_DIR overridable (defaults to null-auto/repo relative to this file's grandparent).
"""
import ast
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nn_transform as T   # noqa: E402

# Repo root: .../null-auto/repo  (this file lives in null-auto/customer_patches/null_notnull_text)
_DEFAULT_REPO = os.path.abspath(os.path.join(HERE, "..", "..", "repo"))
REPO = os.environ.get("REPO_DIR") or _DEFAULT_REPO
SCRIPTS = os.path.join(REPO, "scripts")
MRL_SCRIPT = os.path.join(REPO, "manual_range_pipeline", "scripts", "job2_load_manual_range.py")

CDC_SCRIPTS = ["glue_cdc_continuous.py", "glue_cdc_composite.py"]
LOADER_SCRIPTS = ["job2_load.py", "job3_validate.py"]
ALL_SCRIPT_FILES = LOADER_SCRIPTS + CDC_SCRIPTS

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
# PATCH the real scripts IN MEMORY with the shared transform, then AST-extract the pure-Python
# pieces. This proves the SHIPPED transform produces working functions (no second copy of logic).
# ------------------------------------------------------------------------------------------
def _patched(path, kind):
    return T.patch_script(kind, open(path).read())


_SHARED_WANT = {"NullRulesError", "_parse_null_values", "_parse_null_rules",
                "_effective_null_markers", "_apply_null_settings", "_apply_null_notnull_text",
                "_nn_register_nullability", "_nn_is_notnull", "_nn_register_from_mapping",
                "_null_rules_unknown_warnings", "_coerce_null"}


def _ns_from_src(src, csv_null_value="NULL"):
    tree = ast.parse(src)
    ns = {"CSV_NULL_VALUE": csv_null_value, "NULL_VALUES": None, "NULL_RULES": None,
          "NULL_NOTNULL_TEXT": "off", "_NN_NULLABILITY": {},
          "_NN_TEXT_CATEGORIES": frozenset({"varchar", "text"})}
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and getattr(node, "name", None) in _SHARED_WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and getattr(node, "name", None) in _SHARED_WANT:
            exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    # fresh per-ns mutable globals so tests don't bleed into each other
    ns["_NN_NULLABILITY"] = {}
    return ns


def _cdc_ns(fname):
    return _ns_from_src(_patched(os.path.join(SCRIPTS, fname), "cdc"))


# ---- Spark simulator (same subset tests/test_null_rules.py uses) --------------------------
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
        self._otherwise = None

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


def _assign_segment(src, name):
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.get_source_segment(src, node)
    raise AssertionError(f"{name} assignment not found")


def _extract_fn(src, fnname, extra_globals):
    tree = ast.parse(src)
    seg = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == fnname:
            seg = ast.get_source_segment(src, node)
            break
    assert seg is not None, f"{fnname} not found"
    ns = dict(extra_globals)
    exec(compile(seg, f"<{fnname}>", "exec"), ns)
    return ns[fnname]


def _loader_marker_fn(fname, csv_null_value="NULL"):
    """Build the REAL null_marker_expr from a patched loader/validate script with the Spark
    simulator + shared block wired in. Returns (fn, ns)."""
    kind = "validate" if fname == "job3_validate.py" else "loader"
    src = _patched(os.path.join(SCRIPTS, fname), kind)
    ns = _ns_from_src(src, csv_null_value)
    g = dict(ns)
    g.update({"col": _col, "lit": _lit, "when": _when, "trim": _trim})
    tcat = {}
    exec(compile(ast.parse(_assign_segment(src, "_TYPED_CATEGORIES")), "<tc>", "exec"), tcat)
    g["_TYPED_CATEGORIES"] = tcat["_TYPED_CATEGORIES"]
    fn = _extract_fn(src, "null_marker_expr", g)
    return fn, g


def _spark_is_null(fn, raw, category="varchar", schema="cns", table="orders", column="status"):
    return fn(column, category, schema, table).eval({column: raw}) is None


# =========================================================================================
# 1) FOUR + MRL copies byte-identical AFTER patching (the shared block cannot drift).
# =========================================================================================
def test_shared_block_identical_after_patch():
    def block(n):
        i = n.index("# SHARED NULL-RULES BLOCK")
        j = n.index("# END SHARED NULL-RULES BLOCK")
        j = n.index("\n", j) + 1
        return n[n.rfind("# =====", 0, i):j]
    srcs = {f: block(_patched(os.path.join(SCRIPTS, f),
                              "cdc" if f in CDC_SCRIPTS else
                              ("validate" if f == "job3_validate.py" else "loader")))
            for f in ALL_SCRIPT_FILES}
    srcs["job2_load_manual_range.py"] = block(_patched(MRL_SCRIPT, "loader"))
    first = srcs["job2_load.py"]
    for f, b in srcs.items():
        check(b == first, f"patched shared block in {f} byte-identical to job2_load.py")
    check("NULL_NOTNULL_TEXT" in first and "_nn_is_notnull" in first,
          "patched shared block carries the feature (NULL_NOTNULL_TEXT + _nn_is_notnull)")


# =========================================================================================
# 2) MATRIX over (category x nullability x setting) for the shared _effective_null_markers.
# =========================================================================================
def test_matrix_effective_markers():
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        eff, app, reg = ns["_effective_null_markers"], ns["_apply_null_settings"], ns["_nn_register_nullability"]
        reg("cns", "orders", {"status": "NO", "note": "YES", "id": "NO", "amt": "NO", "ts": "NO"})
        cats = {"status": "varchar", "note": "varchar", "id": "integer", "amt": "numeric",
                "ts": "timestamptz"}
        # setting OFF -> every column keeps the endpoint marker (today's behaviour)
        app(None, None, "off")
        for c, cat in cats.items():
            check(eff("cns", "orders", c, cat) == ["NULL"],
                  f"{f}: off/{c}({cat}) -> endpoint marker kept")
        # setting keep_text
        app(None, None, "keep_text")
        check(eff("cns", "orders", "status", "varchar") == [],
              f"{f}: keep_text NOT NULL varchar -> no marker (keep text)")
        check(eff("cns", "orders", "note", "varchar") == ["NULL"],
              f"{f}: keep_text NULLABLE varchar -> marker unchanged")
        for c in ("id", "amt", "ts"):
            check(eff("cns", "orders", c, cats[c]) == ["NULL"],
                  f"{f}: keep_text NOT NULL {cats[c]} (non-text) -> marker unchanged")
        # a text category is also protected
        reg("cns", "orders", {"body": "NO"})
        check(eff("cns", "orders", "body", "text") == [],
              f"{f}: keep_text NOT NULL text -> no marker")


# =========================================================================================
# 3) PRECEDENCE: explicit null_rules > automatic rule > null_values > endpoint.
# =========================================================================================
def test_precedence():
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        eff, app, reg = ns["_effective_null_markers"], ns["_apply_null_settings"], ns["_nn_register_nullability"]
        reg("cns", "orders", {"status": "NO", "note": "YES"})
        # explicit rule on a NOT NULL text col BEATS the automatic keep_text rule
        app("X", "cns.orders.status=NULL", "keep_text")
        check(eff("cns", "orders", "status", "varchar") == ["NULL"],
              f"{f}: explicit null_rules beats automatic keep_text")
        # a NOT NULL text col with NO rule under keep_text ignores null_values (returns [])
        app("X", None, "keep_text")
        check(eff("cns", "orders", "status", "varchar") == [],
              f"{f}: automatic rule beats null_values on NOT NULL text")
        # a nullable text col under keep_text falls through to null_values
        check(eff("cns", "orders", "note", "varchar") == ["X"],
              f"{f}: null_values applies to a nullable text col under keep_text")


# =========================================================================================
# 4) _coerce_null / empty-field behaviour under keep_text.
# =========================================================================================
def test_coerce_and_empty():
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        eff, app, reg, coerce = (ns["_effective_null_markers"], ns["_apply_null_settings"],
                                 ns["_nn_register_nullability"], ns["_coerce_null"])
        reg("cns", "orders", {"status": "NO"})
        app(None, None, "keep_text")
        m = eff("cns", "orders", "status", "varchar")
        check(coerce("NULL", m) == "NULL", f"{f}: keep_text -> literal 'NULL' kept as data")
        check(coerce("NA", m) == "NA", f"{f}: keep_text -> 'NA' kept")
        check(coerce("", m) is None, f"{f}: keep_text -> empty field STILL NULL")
        check(coerce(None, m) is None, f"{f}: keep_text -> None STILL NULL")


# =========================================================================================
# 5) LOADER / VALIDATE real call sites (null_marker_expr) agree with the rule.
# =========================================================================================
def test_loader_validate_callsites():
    for f in LOADER_SCRIPTS:
        fn, g = _loader_marker_fn(f)
        g["_apply_null_settings"](None, None, "keep_text")
        g["_nn_register_nullability"]("cns", "orders", {"status": "NO", "note": "YES", "id": "NO"})
        check(not _spark_is_null(fn, "NULL", "varchar", column="status"),
              f"{f}: Spark keep_text -> 'NULL' KEPT in NOT NULL varchar")
        check(_spark_is_null(fn, "", "varchar", column="status"),
              f"{f}: Spark keep_text -> empty STILL NULL in NOT NULL varchar")
        check(_spark_is_null(fn, "NULL", "varchar", column="note"),
              f"{f}: Spark keep_text -> 'NULL' still NULL in NULLABLE varchar")
        check(_spark_is_null(fn, "NULL", "integer", column="id"),
              f"{f}: Spark keep_text -> non-text NOT NULL unaffected")


# =========================================================================================
# 6) PARITY: loader, validate and CDC make the SAME NULL/keep decision (no false-flag).
# =========================================================================================
def test_load_validate_cdc_parity():
    # (nullability, category, raw, expect_null) under keep_text
    cases = [
        ({"status": "NO"}, "varchar", "status", "NULL", False),
        ({"status": "NO"}, "varchar", "status", "", True),
        ({"note": "YES"}, "varchar", "note", "NULL", True),
        ({"id": "NO"}, "integer", "id", "NULL", True),
        ({"body": "NO"}, "text", "body", "NULL", False),
    ]
    for nulls, cat, col, raw, want_null in cases:
        decisions = {}
        for f in CDC_SCRIPTS:
            ns = _cdc_ns(f)
            ns["_apply_null_settings"](None, None, "keep_text")
            ns["_nn_register_nullability"]("cns", "orders", nulls)
            m = ns["_effective_null_markers"]("cns", "orders", col, cat)
            decisions[f] = ns["_coerce_null"](raw, m) is None
        for f in LOADER_SCRIPTS:
            fn, g = _loader_marker_fn(f)
            g["_apply_null_settings"](None, None, "keep_text")
            g["_nn_register_nullability"]("cns", "orders", nulls)
            decisions[f] = _spark_is_null(fn, raw, cat, column=col)
        check(set(decisions.values()) == {want_null},
              f"parity col={col}({cat}) raw={raw!r} -> NULL={want_null} (got {decisions})")


# =========================================================================================
# 7) BYTE-IDENTICAL-WHEN-OFF: with the arg off/unset, decisions == the pre-feature semantics.
# =========================================================================================
def test_off_is_todays_behaviour():
    kept = ["NA", "N/A", "NONE", "(NULL)", "\\N", "null", "Null", " NULL ", "na", "normal"]
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        eff, app, reg, coerce = (ns["_effective_null_markers"], ns["_apply_null_settings"],
                                 ns["_nn_register_nullability"], ns["_coerce_null"])
        reg("cns", "orders", {"status": "NO"})         # registered, but...
        for setting in (None, "off", "__NULL_UNSET__"):
            app(None, None, setting)
            m = eff("cns", "orders", "status", "varchar")
            check(m == ["NULL"], f"{f}: off({setting!r}) NOT NULL text -> endpoint marker (today)")
            check(coerce("NULL", m) is None, f"{f}: off({setting!r}) -> 'NULL' is NULL (today)")
            check(coerce("", m) is None, f"{f}: off({setting!r}) -> empty is NULL (today)")
            for v in kept:
                check(coerce(v, m) == v, f"{f}: off({setting!r}) -> {v!r} kept (today)")
    # invalid value -> off (warn) == today
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        ns["_nn_register_nullability"]("cns", "orders", {"status": "NO"})
        ns["_apply_null_settings"](None, None, "bogus")
        check(ns["_effective_null_markers"]("cns", "orders", "status", "varchar") == ["NULL"],
              f"{f}: invalid null_notnull_text -> off (today)")


# =========================================================================================
# 8) OLD-MAPPING FALLBACK: _nn_register_from_mapping returns False when is_nullable absent
#    (so the caller does one catalog query); True when present; fail-loud if neither.
# =========================================================================================
def test_old_mapping_fallback():
    for f in CDC_SCRIPTS:
        ns = _cdc_ns(f)
        app, frm, eff = (ns["_apply_null_settings"], ns["_nn_register_from_mapping"],
                         ns["_effective_null_markers"])
        app(None, None, "keep_text")
        # NEW mapping: column_mapping entries carry is_nullable
        new_cfg = {"column_mapping": [
            {"action": "map", "target_column": "status", "is_nullable": "NO"},
            {"action": "map", "target_column": "note", "is_nullable": "YES"}]}
        check(frm("cns", "orders", new_cfg) is True, f"{f}: new mapping registers -> True")
        check(eff("cns", "orders", "status", "varchar") == [],
              f"{f}: new mapping -> NOT NULL text protected")
        # OLD mapping: no is_nullable key anywhere
        ns2 = _cdc_ns(f)
        ns2["_apply_null_settings"](None, None, "keep_text")
        old_cfg = {"column_mapping": [
            {"action": "map", "target_column": "status", "type_category": "varchar"}]}
        check(ns2["_nn_register_from_mapping"]("cns", "orders", old_cfg) is False,
              f"{f}: OLD mapping (no is_nullable) -> False (caller must catalog-query)")
        # and since nothing was registered, the rule FAILS LOUD (never silently skips)
        try:
            ns2["_effective_null_markers"]("cns", "orders", "status", "varchar")
            check(False, f"{f}: old mapping unregistered should fail loud")
        except ns2["NullRulesError"]:
            check(True, f"{f}: old mapping + no catalog registration -> fail loud (no silent skip)")
        # after the caller does its one catalog query and registers, it works
        ns2["_nn_register_nullability"]("cns", "orders", {"status": "NO"})
        check(ns2["_effective_null_markers"]("cns", "orders", "status", "varchar") == [],
              f"{f}: after catalog fallback registration -> protected")
    # off -> _nn_register_from_mapping is a no-op True (nothing to do)
    ns3 = _cdc_ns(CDC_SCRIPTS[0])
    ns3["_apply_null_settings"](None, None, "off")
    check(ns3["_nn_register_from_mapping"]("cns", "orders", {}) is True,
          "off: _nn_register_from_mapping is a no-op True")


# =========================================================================================
# 9) JOB1 discovery carries is_nullable into each mapped column_mapping entry.
# =========================================================================================
def test_job1_mapping_carries_is_nullable():
    src = T.patch_job1(open(os.path.join(SCRIPTS, "job1_discovery.py")).read())
    check('"is_nullable": target_col.get(\'is_nullable\')' in src,
          "job1_discovery: mapped column_mapping entry now carries is_nullable")
    compile(src, "job1_discovery.py", "exec")
    check(True, "job1_discovery still compiles after the edit")


# =========================================================================================
# 10) MUTATION CHECKS — each wrong behaviour MUST fail a test.
# =========================================================================================
def test_mutation_nullable_must_not_be_protected():
    ns = _cdc_ns("glue_cdc_continuous.py")
    ns["_apply_null_settings"](None, None, "keep_text")
    ns["_nn_register_nullability"]("cns", "orders", {"note": "YES"})
    # Correct: a NULLABLE text col is NOT protected (marker kept). If a mutation protected it,
    # this assertion would fail -> the mutation is caught.
    check(ns["_effective_null_markers"]("cns", "orders", "note", "varchar") == ["NULL"],
          "MUTATION GUARD: nullable text col must NOT be protected (marker kept)")


def test_mutation_int_must_not_be_protected():
    ns = _cdc_ns("glue_cdc_continuous.py")
    ns["_apply_null_settings"](None, None, "keep_text")
    ns["_nn_register_nullability"]("cns", "orders", {"id": "NO"})
    check(ns["_effective_null_markers"]("cns", "orders", "id", "integer") == ["NULL"],
          "MUTATION GUARD: NOT NULL int col must NOT be protected (not a text category)")


def test_mutation_null_rules_must_win():
    ns = _cdc_ns("glue_cdc_continuous.py")
    ns["_apply_null_settings"](None, "cns.orders.status=NULL", "keep_text")
    ns["_nn_register_nullability"]("cns", "orders", {"status": "NO"})
    check(ns["_effective_null_markers"]("cns", "orders", "status", "varchar") == ["NULL"],
          "MUTATION GUARD: an explicit null_rules entry MUST win over the automatic rule")


# =========================================================================================
# 11) APPLY-SCRIPT (deployed env) against a FAKE boto3: dry-run, apply, no-op, restore,
#     read-back, wrong-shape skip. Exercises enable_glue_job_arg.py with no AWS.
# =========================================================================================
def _load_enable_module():
    import importlib.util
    path = os.path.join(HERE, "enable_glue_job_arg.py")
    spec = importlib.util.spec_from_file_location("enable_glue_job_arg", path)
    mod = importlib.util.module_from_spec(spec)
    # Inject a fake boto3 BEFORE exec so the module import of boto3 resolves to our fake.
    sys.modules.setdefault("boto3", types.SimpleNamespace())
    spec.loader.exec_module(mod)
    return mod


class _FakeGlue:
    def __init__(self, jobs):
        self._jobs = jobs            # {name: job-dict}
        self.updates = []

    def get_paginator(self, _):
        outer = self

        class _P:
            def paginate(self_inner):
                return [{"JobNames": list(outer._jobs.keys())}]
        return _P()

    def list_jobs(self, **kw):
        return {"JobNames": list(self._jobs.keys())}

    def get_job(self, JobName):
        import copy
        return {"Job": copy.deepcopy(self._jobs[JobName])}

    def update_job(self, JobName, JobUpdate):
        self.updates.append(JobName)
        j = self._jobs[JobName]
        # emulate UpdateJob: replace the mutable definition, keep Name/CreatedOn
        for k in list(j.keys()):
            if k not in ("Name", "CreatedOn", "LastModifiedOn"):
                j.pop(k, None)
        j.update(JobUpdate)
        return {}


def _job(name, script, args=None):
    return {"Name": name, "CreatedOn": "t0", "Role": "r",
            "Command": {"Name": "glueetl", "ScriptLocation": "s3://b/scripts/" + script},
            "DefaultArguments": dict(args or {"--TempDir": "s3://b/tmp/"}),
            "MaxRetries": 0, "Timeout": 2880, "WorkerType": "G.1X", "NumberOfWorkers": 2,
            "GlueVersion": "4.0", "Connections": {"Connections": ["c"]}}


def _make_env(mod):
    jobs = {
        "dms-dsql-08-load": _job("dms-dsql-08-load", "job2_load.py"),
        "dms-dsql-08-validate": _job("dms-dsql-08-validate", "job3_validate.py"),
        "dms-dsql-08-cdc": _job("dms-dsql-08-cdc", "glue_cdc_continuous.py"),
        "dms-dsql-08-ck-a-cdc": _job("dms-dsql-08-ck-a-cdc", "glue_cdc_composite.py"),
        "dms-dsql-08-discovery": _job("dms-dsql-08-discovery", "job1_discovery.py"),  # NOT targeted
        "other-proj-load": _job("other-proj-load", "job2_load.py"),                   # wrong project
    }
    return _FakeGlue(jobs)


def test_apply_script_dry_run_changes_nothing():
    mod = _load_enable_module()
    glue = _make_env(mod)
    rc, changed, skipped = mod.run(glue, project="dms-dsql", arg_name="--null_notnull_text",
                                   arg_value="keep_text", apply=False)
    check(rc == 0, "apply-script dry-run returns 0")
    check(glue.updates == [], "apply-script dry-run calls NO update_job")
    # the 4 target jobs would change; discovery + other-project are skipped
    check(sorted(changed) == ["dms-dsql-08-cdc", "dms-dsql-08-ck-a-cdc",
                              "dms-dsql-08-load", "dms-dsql-08-validate"],
          "apply-script targets exactly the 4 script jobs (load/validate/cdc/ck)")
    check("dms-dsql-08-discovery" in skipped and "other-proj-load" in skipped,
          "apply-script skips discovery and other-project jobs")


def test_apply_script_apply_then_noop_then_restore():
    mod = _load_enable_module()
    glue = _make_env(mod)
    backups = {}
    rc, changed, _ = mod.run(glue, project="dms-dsql", arg_name="--null_notnull_text",
                             arg_value="keep_text", apply=True, backups=backups)
    check(rc == 0 and sorted(glue.updates) == ["dms-dsql-08-cdc", "dms-dsql-08-ck-a-cdc",
                                               "dms-dsql-08-load", "dms-dsql-08-validate"],
          "apply: update_job called once per target job (4)")
    for n in ("dms-dsql-08-load", "dms-dsql-08-cdc"):
        check(glue._jobs[n]["DefaultArguments"].get("--null_notnull_text") == "keep_text",
              f"apply: {n} now carries --null_notnull_text=keep_text (read-back)")
        # every OTHER field preserved
        check(glue._jobs[n]["WorkerType"] == "G.1X" and glue._jobs[n]["Connections"] ==
              {"Connections": ["c"]}, f"apply: {n} other fields preserved")
    # second apply = no-op (already set)
    glue.updates.clear()
    rc2, changed2, _ = mod.run(glue, project="dms-dsql", arg_name="--null_notnull_text",
                               arg_value="keep_text", apply=True)
    check(glue.updates == [] and changed2 == [], "second apply is a no-op (idempotent)")
    # restore from the backup taken during apply
    any_name = "dms-dsql-08-load"
    rc3 = mod.restore_job(glue, backups[any_name])
    check(rc3 == 0 and "--null_notnull_text" not in glue._jobs[any_name]["DefaultArguments"],
          "restore: the job is put back exactly (arg removed)")


def test_apply_script_wrong_shape_skipped():
    mod = _load_enable_module()
    glue = _make_env(mod)
    # a job whose script is not one of ours -> skipped even if the name matches the filter
    glue._jobs["dms-dsql-08-load-big"] = _job("dms-dsql-08-load-big", "some_other.py")
    rc, changed, skipped = mod.run(glue, project="dms-dsql", arg_name="--null_notnull_text",
                                   arg_value="keep_text", apply=False)
    check("dms-dsql-08-load-big" in skipped,
          "apply-script skips a filter-matching job whose script is not one of the 4")


def main():
    for name in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[name]()
    print(f"\n==== null_notnull_text: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
