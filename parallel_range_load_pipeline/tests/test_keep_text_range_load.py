#!/usr/bin/env python3
"""release-2 behavioural tests for --null_notnull_text=keep_text on the PARALLEL (range-mode)
loader (scripts/job2_load_manual_range.py).

Background: the keep_text rule was first written against the ORIGINAL loader
(scripts/job2_load.py). main's parallel loader is the A10-A19 redesign; the Sunday merge grafted
the rule onto it, and NO parallel-pipeline test exercised keep_text behaviour (reviewer finding
2b item 8 / 2c). These tests close that gap OFFLINE (no Spark / Glue / DSQL / S3):

  - The DECISION of which exact text values become SQL NULL for one column lives in the loader's
    own pure-Python _effective_null_markers() (+ the _nn_* nullability helpers). We AST-extract
    those functions from the REAL loader file (same technique as test_resolver_consistency.py and
    test_helper_sync.py) so we test exactly what the Glue job runs, never a copy.
  - null_marker_expr() builds a Spark when/otherwise column expression from that marker list. We
    reproduce ITS exact evaluation semantics in Python (NULL on Spark-null / empty field / any
    effective marker; typed columns trimmed with whitespace-only -> NULL) and drive it per row, so
    we prove the end-to-end "what gets stored" outcome without a Spark runtime.

Proven here:
  (1) a NOT NULL char/varchar/text column receiving the literal text NULL / N/A / NA / null /
      None / (NULL) is STORED AS THAT TEXT (never SQL NULL) when null_notnull_text=keep_text;
  (2) a real NULL (empty field, or the CsvNullValue marker) in a NULLABLE column stays NULL;
  (3) typed columns (numeric/date/uuid/...) are unaffected by keep_text;
  (4) with null_notnull_text=off the per-row INSERT parameters are byte-identical to the
      8c7ed25-era loader (repo-root scripts/job2_load.py) run on the SAME fixture rows;
  (5) the commit-probe RESUME path and the OCC/pipe RETRY path write the SAME transformed rows
      (they consume df.toLocalIterator() chunks), so the rule applies there too;
  (6) nullability comes from the Job 1 mapping is_nullable, with the information_schema catalog
      fallback for an OLDER mapping (the loader's documented contract).
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
LOADER = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
ROOT_LOADER = os.path.join(REPO, "scripts", "job2_load.py")

# categories null_marker_expr trims (must match the loader's _TYPED_CATEGORIES)
_TYPED = frozenset({"uuid", "boolean", "timestamptz", "date", "bigint", "integer", "smallint",
                    "numeric", "float", "double", "real", "json", "jsonb", "bytea"})

_DECISION_NAMES = {"_effective_null_markers", "_nn_register_nullability", "_nn_is_notnull",
                   "_nn_register_from_mapping", "_apply_null_notnull_text", "_parse_null_values",
                   "_parse_null_rules", "NullRulesError"}


def _load_decision_ns(path):
    """AST-extract the keep_text decision functions from a loader file into a fresh namespace
    with the module globals they read. The namespace IS the loader's own code; the test can set
    NULL_NOTNULL_TEXT / NULL_VALUES / NULL_RULES / CSV_NULL_VALUE just like the job does at
    startup via _apply_null_settings()."""
    src = open(path).read()
    tree = ast.parse(src)
    ns = {
        "NULL_VALUES": None,
        "NULL_RULES": None,
        "NULL_NOTNULL_TEXT": "off",
        "CSV_NULL_VALUE": "NULL",
        "_NN_NULLABILITY": {},
        "_NN_TEXT_CATEGORIES": frozenset({"varchar", "text"}),
    }
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in _DECISION_NAMES:
            exec(ast.get_source_segment(src, n), ns)
    missing = _DECISION_NAMES - set(ns)
    assert not missing, "could not extract decision function(s) from %s: %s" % (path, missing)
    return ns


def _store_value(ns, schema, table, column, category, raw):
    """Reproduce null_marker_expr(column, category, schema, table) evaluated on one value `raw`.
    Returns the value that WOULD be the INSERT parameter for this cell (None == SQL NULL).

    Mirrors the loader EXACTLY:
        is_null = (raw is None) or (raw == "")                # Spark-null or empty field
        for m in _effective_null_markers(schema, table, column, category):
            is_null |= (raw == m)                             # exact, case-sensitive, whole-value
        if category in _TYPED:                                # typed: trim; whitespace-only -> NULL
            t = raw.strip()
            return None if (is_null or t == "") else t
        return None if is_null else raw                       # text: kept verbatim (whitespace too)
    """
    is_null = (raw is None) or (raw == "")
    if not is_null:
        for m in ns["_effective_null_markers"](schema, table, column, category):
            if raw == m:
                is_null = True
                break
    if category in _TYPED:
        t = raw.strip() if isinstance(raw, str) else raw
        return None if (is_null or t == "") else t
    return None if is_null else raw


# fixture: a table "s.t" with a NOT NULL varchar col, a NOT NULL text col, a NULLABLE varchar
# col, and some typed cols. is_nullable mirrors information_schema.columns ('NO' == NOT NULL).
_COLMAP = {"nn_vc": "NO", "nn_txt": "NO", "null_vc": "YES",
           "amount": "NO", "created": "YES", "id": "NO"}
_CATEGORY = {"nn_vc": "varchar", "nn_txt": "text", "null_vc": "varchar",
             "amount": "numeric", "created": "date", "id": "uuid"}
# the literal-text-NULL spellings the rule must PRESERVE in a NOT NULL text column
_TEXTY_NULLS = ["NULL", "N/A", "NA", "null", "None", "(NULL)", "\\N", "NONE"]


def _register(ns, keep_text):
    ns["NULL_NOTNULL_TEXT"] = "keep_text" if keep_text else "off"
    ns["NULL_VALUES"] = None
    ns["NULL_RULES"] = None
    ns["CSV_NULL_VALUE"] = "NULL"
    ns["_NN_NULLABILITY"].clear()
    if keep_text:
        ns["_nn_register_nullability"]("s", "t", _COLMAP)


# ----------------------------------------------------------------------------- (1)
def test_notnull_text_column_keeps_literal_null_text_when_keep_text():
    ns = _load_decision_ns(LOADER)
    _register(ns, keep_text=True)
    for col in ("nn_vc", "nn_txt"):
        for val in _TEXTY_NULLS:
            got = _store_value(ns, "s", "t", col, _CATEGORY[col], val)
            assert got == val, (
                "keep_text: NOT NULL %s column %r should KEEP %r as text, got %r"
                % (_CATEGORY[col], col, val, got))


def test_notnull_text_without_keep_text_still_nulls_the_marker():
    """Sanity / contrast: with keep_text OFF, the endpoint marker 'NULL' DOES become SQL NULL
    even in a NOT NULL column (today's behaviour; DSQL then rejects it at insert) -- so the
    feature genuinely changes outcome and the test is not vacuous."""
    ns = _load_decision_ns(LOADER)
    _register(ns, keep_text=False)
    assert _store_value(ns, "s", "t", "nn_vc", "varchar", "NULL") is None
    # a non-marker spelling like 'N/A' is kept as data in BOTH modes (only CSV_NULL_VALUE nulls)
    assert _store_value(ns, "s", "t", "nn_vc", "varchar", "N/A") == "N/A"


# ----------------------------------------------------------------------------- (2)
def test_real_null_in_nullable_column_stays_null():
    ns = _load_decision_ns(LOADER)
    _register(ns, keep_text=True)
    # empty field -> NULL in every mode
    assert _store_value(ns, "s", "t", "null_vc", "varchar", "") is None
    # the CsvNullValue endpoint marker in a NULLABLE text column -> NULL (keep_text only
    # protects NOT NULL columns; a nullable column keeps today's behaviour)
    assert _store_value(ns, "s", "t", "null_vc", "varchar", "NULL") is None
    # None (Spark null) -> NULL
    assert _store_value(ns, "s", "t", "null_vc", "varchar", None) is None


def test_real_null_in_notnull_column_empty_field_still_null():
    """keep_text protects TEXT *markers*; a genuinely empty field is ALWAYS NULL even in a NOT
    NULL column (it will then fail the NOT NULL constraint at insert -- correct, that's real
    missing data, not the text 'NULL')."""
    ns = _load_decision_ns(LOADER)
    _register(ns, keep_text=True)
    assert _store_value(ns, "s", "t", "nn_vc", "varchar", "") is None
    assert _store_value(ns, "s", "t", "nn_txt", "text", None) is None


# ----------------------------------------------------------------------------- (3)
def test_typed_columns_unaffected_by_keep_text():
    ns = _load_decision_ns(LOADER)
    _register(ns, keep_text=True)
    # numeric/date/uuid NOT NULL columns: the marker 'NULL' still -> NULL; real values pass.
    assert _store_value(ns, "s", "t", "amount", "numeric", "NULL") is None
    assert _store_value(ns, "s", "t", "amount", "numeric", "123.45") == "123.45"
    assert _store_value(ns, "s", "t", "created", "date", "2024-01-02") == "2024-01-02"
    uuid = "0b2e2a1a-1111-2222-3333-444455556666"
    assert _store_value(ns, "s", "t", "id", "uuid", uuid) == uuid
    # a typed column is NEVER in _NN_TEXT_CATEGORIES, so _effective_null_markers returns the
    # same marker list as off -> keep_text cannot turn a typed 'NULL' into kept text.
    assert ns["_effective_null_markers"]("s", "t", "amount", "numeric") == ["NULL"]


# ----------------------------------------------------------------------------- (4)
def _fixture_rows():
    """(column, category, raw) cells spanning text markers, real nulls, and typed values."""
    rows = []
    for v in _TEXTY_NULLS + ["", "real data", "  spaced  ", None]:
        rows.append(("nn_vc", "varchar", v))
        rows.append(("nn_txt", "text", v))
        rows.append(("null_vc", "varchar", v))
    for v in ["NULL", "", "123", "  ", None]:
        rows.append(("amount", "numeric", v))
        rows.append(("created", "date", v))
    return rows


def test_off_is_byte_identical_to_8c7ed25_loader():
    """(4) With null_notnull_text=off the PARALLEL loader's per-cell INSERT value is identical to
    the repo-root 8c7ed25-era loader (scripts/job2_load.py) on the SAME rows. Run BOTH loaders'
    own extracted decision code and compare every produced value."""
    folder = _load_decision_ns(LOADER)
    root = _load_decision_ns(ROOT_LOADER)
    for ns in (folder, root):
        _register(ns, keep_text=False)
    for col, cat, raw in _fixture_rows():
        a = _store_value(folder, "s", "t", col, cat, raw)
        b = _store_value(root, "s", "t", col, cat, raw)
        assert a == b, ("off must match 8c7ed25 loader for %s/%s raw=%r: folder=%r root=%r"
                        % (col, cat, raw, a, b))


def test_off_marker_list_matches_8c7ed25_loader():
    """Lower-level form of (4): the effective marker LIST per column is identical with keep_text
    off, for text AND typed categories."""
    folder = _load_decision_ns(LOADER)
    root = _load_decision_ns(ROOT_LOADER)
    for ns in (folder, root):
        _register(ns, keep_text=False)
    for col, cat in (("nn_vc", "varchar"), ("nn_txt", "text"), ("null_vc", "varchar"),
                     ("amount", "numeric"), ("id", "uuid")):
        assert (folder["_effective_null_markers"]("s", "t", col, cat)
                == root["_effective_null_markers"]("s", "t", col, cat))


# ----------------------------------------------------------------------------- (5)
def test_resume_and_retry_paths_write_the_same_transformed_rows():
    """(5) STRUCTURAL proof that the commit-probe resume path and the OCC/pipe retry path apply
    keep_text: in load_one_table the ONLY row source is df.toLocalIterator() AFTER the single
    null_marker_expr transform, and every chunk (first attempt, probe_all_chunks resume, and the
    retry loop) comes from iter_chunks() over those already-transformed rows. Assert the loader's
    structure still has exactly one transform application feeding one row iterator, and that the
    probe/retry code reuses the chunk rows (no second, untransformed read)."""
    src = open(LOADER).read()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "load_one_table")
    body = ast.get_source_segment(src, fn)
    # exactly one application of null_marker_expr WITH schema+table (keep_text active)
    assert body.count("null_marker_expr(c, type_categories.get(c), dsql_schema, dsql_table)") == 1
    # nullability registered before the transform
    assert "_nn_register_for_table(dsql_schema, dsql_table, config)" in body
    # the single streaming row source feeding every chunk/probe/retry
    assert "row_iter = df.toLocalIterator()" in body
    assert body.count("df.toLocalIterator()") == 1
    # resume commit-probe and the retry loop operate on chunk_rows (the transformed rows), and
    # re-build the INSERT from those same rows -- never a fresh untransformed read.
    assert "probe_all_chunks" in body and "_chunk_already_committed" in body
    assert "_build_stmt_params(chunk_rows)" in body
    # there is NO second read_dms_csv inside the probe/retry region (would bypass the transform)
    assert body.count("read_dms_csv(") == 1


def test_null_marker_expr_is_inert_without_schema_table():
    """Guard against the bypass hazard the reviewer flagged (2c): null_marker_expr applies the
    keep_text/marker set ONLY when schema AND table are passed. The one call site passes them;
    confirm the function itself is inert (markers = just CSV_NULL_VALUE) when they are omitted,
    so any un-wired caller keeps today's behaviour instead of silently losing the rule."""
    src = open(LOADER).read()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "null_marker_expr")
    body = ast.get_source_segment(src, fn)
    assert "if schema is None or table is None:" in body
    # the inert branch uses ONLY CSV_NULL_VALUE; the effective-markers branch is the else.
    assert "_effective_null_markers(schema, table, c, category)" in body


# ----------------------------------------------------------------------------- (6)
def test_nullability_from_mapping_is_nullable():
    """(6) _nn_register_from_mapping reads is_nullable off the Job 1 column_mapping (action=map)."""
    ns = _load_decision_ns(LOADER)
    ns["NULL_NOTNULL_TEXT"] = "keep_text"
    ns["_NN_NULLABILITY"].clear()
    config = {"column_mapping": [
        {"action": "map", "target_column": "nn_vc", "is_nullable": "NO"},
        {"action": "map", "target_column": "null_vc", "is_nullable": "YES"},
        {"action": "skip", "target_column": "ignored"},
    ]}
    registered = ns["_nn_register_from_mapping"]("s", "t", config)
    assert registered is True, "a mapping carrying is_nullable must register"
    assert ns["_nn_is_notnull"]("s", "t", "nn_vc") is True
    assert ns["_nn_is_notnull"]("s", "t", "null_vc") is False


def test_older_mapping_without_is_nullable_signals_catalog_fallback():
    """(6) An OLDER Job 1 mapping without is_nullable makes _nn_register_from_mapping return
    False -> the loader's _nn_register_for_table runs ONE information_schema query (the catalog
    fallback). We assert the False signal AND that the loader wires the fallback query."""
    ns = _load_decision_ns(LOADER)
    ns["NULL_NOTNULL_TEXT"] = "keep_text"
    ns["_NN_NULLABILITY"].clear()
    old_config = {"column_mapping": [
        {"action": "map", "target_column": "nn_vc"},      # no is_nullable
        {"action": "map", "target_column": "null_vc"},
    ]}
    assert ns["_nn_register_from_mapping"]("s", "t", old_config) is False
    # the loader's table-level registrar performs the catalog fallback query
    src = open(LOADER).read()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_nn_register_for_table")
    body = ast.get_source_segment(src, fn)
    assert "information_schema.columns" in body
    assert "_nn_register_from_mapping(dsql_schema, dsql_table, config)" in body


def test_unregistered_table_fails_loud_never_guesses():
    """(6) If keep_text is on and a table was never registered, _nn_is_notnull raises (fail loud,
    never silently skip the rule) -- the loader's documented safety contract."""
    ns = _load_decision_ns(LOADER)
    ns["NULL_NOTNULL_TEXT"] = "keep_text"
    ns["_NN_NULLABILITY"].clear()
    try:
        ns["_nn_is_notnull"]("s", "never_registered", "c")
        assert False, "expected a loud failure for an unregistered table"
    except ns["NullRulesError"]:
        pass


# ----------------------------------------------------------------------------- (2d i)
def _run_ensure_job(null_notnull_text):
    """Drive the folder's own mrl_ensure_job.handler with fakes and return the created Glue job's
    DefaultArguments. null_notnull_text=None omits the key entirely (today's/8c7ed25 behaviour);
    a string ('off'/''/'keep_text') is passed as the Lambda sees it from the ASL/preflight."""
    import json
    import importlib
    sys.path.insert(0, os.path.join(PKG, "lambdas"))
    sys.path.insert(0, HERE)
    import mrl_ensure_job
    importlib.reload(mrl_ensure_job)
    from mrl_fakes import FakeS3, FakeGlue
    s3, glue = FakeS3(), FakeGlue()
    s3.put_object(Bucket="b", Key="glue-templates/mrl-load.json",
                  Body=json.dumps({"role": "mrl-load", "script": "job2_load_manual_range.py",
                                   "command_name": "glueetl", "glue_version": "4.0",
                                   "worker_type": "G.4X", "number_of_workers": 20,
                                   "timeout_minutes": 2880, "max_concurrent_runs": 1,
                                   "default_arguments": {"--TempDir": "s3://<<BUCKET>>/glue-temp/"}}).encode())
    mrl_ensure_job.boto3 = type("x", (), {"client": staticmethod(
        lambda svc, **k: {"s3": s3, "glue": glue}.get(svc, s3))})
    ev = {
        "bucket": "b", "project": "demo", "taskSuffix": "t", "region": "us-east-1",
        "accountId": "123456789012", "glue_templates_prefix": "glue-templates",
        "scripts_prefix": "scripts",
        "glue_role_arn": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
        "dsql_endpoint": "c.dsql.us-east-1.on.aws", "dsql_user": "admin",
        "dsql_database": "postgres", "csv_null_value": "NULL", "null_values": "",
        "null_rules": "", "maxTotalConcurrentRuns": 4, "loadWorkerType": "G.4X",
        "loadWorkers": 20,
        "cdcExtraPyFiles": "s3://b/driver-cdc/boto3-1.42.97-py3-none-any.whl,"
                           "s3://b/driver-cdc/botocore-1.42.97-py3-none-any.whl,"
                           "s3://b/driver-cdc/s3transfer-0.16.1-py3-none-any.whl"}
    if null_notnull_text is not None:
        ev["null_notnull_text"] = null_notnull_text
    out = mrl_ensure_job.handler(ev, None)
    return dict(glue.jobs[out["jobName"]]["DefaultArguments"])


def test_ensure_job_off_default_arguments_byte_identical_to_8c7ed25():
    """(2d i) With null_notnull_text ABSENT the load job's DefaultArguments carry NO
    --null_notnull_text key at all -> byte-identical to the 8c7ed25 job arguments. With it set to
    the inert values ('off' / '' -> __NULL_UNSET__) the ONLY difference from absent is that one
    key, and every other argument is unchanged. The loader treats off/''/__NULL_UNSET__ as off
    (proved by test_*_without_keep_text / the parser), so behaviour is unchanged on all of them.
    Also: Glue cannot pass an empty value, so no DefaultArguments value is ever ''."""
    absent = _run_ensure_job(None)
    assert "--null_notnull_text" not in absent, (
        "with the feature unused the load job must carry NO --null_notnull_text arg (byte-identical "
        "to 8c7ed25); got %r" % absent.get("--null_notnull_text"))

    for inert in ("off", ""):
        got = _run_ensure_job(inert)
        # exactly one added key vs absent, and it is --null_notnull_text
        added = set(got) - set(absent)
        removed = set(absent) - set(got)
        assert added == {"--null_notnull_text"} and not removed, (
            "off/'' must add ONLY --null_notnull_text; added=%s removed=%s" % (added, removed))
        # every other argument value is identical to the absent (8c7ed25) baseline
        for k, v in absent.items():
            assert got[k] == v, ("arg %s changed vs 8c7ed25 baseline: %r -> %r" % (k, v, got[k]))
        # delivered via DefaultArguments (not a per-run arg) with a non-empty sentinel value
        assert got["--null_notnull_text"] in ("off", "__NULL_UNSET__"), got["--null_notnull_text"]

    # no DefaultArguments value is ever an empty string (Glue would drop it) -- on/off/absent
    for args in (absent, _run_ensure_job("off"), _run_ensure_job("keep_text")):
        empties = {k: val for k, val in args.items() if val == ""}
        assert not empties, "empty-string DefaultArguments value(s): %s" % empties


def test_ensure_job_keep_text_rides_on_default_arguments():
    """(2d i) When the operator turns it on, --null_notnull_text=keep_text is present on the load
    job's DefaultArguments (the sentinel-aware mechanism), so Glue delivers it on every run."""
    got = _run_ensure_job("keep_text")
    assert got.get("--null_notnull_text") == "keep_text", got.get("--null_notnull_text")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d keep_text range-load test(s) passed" % len(fns))
