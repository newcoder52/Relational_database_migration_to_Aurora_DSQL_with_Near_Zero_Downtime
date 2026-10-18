#!/usr/bin/env python3
"""Tests for AMENDMENTS 1-9 that are not already covered by the focused test files.

Covers: the forbidden mrl/fleet/ + bare fleet_tasks.csv reference grep (Amendment 1.5); the
params_csv byte-identical copy sync (Amendment 3.1); the settings publish/pin + provenance +
no-write-of-the-three-config-files (Amendment 3.2 / 9.3, asserted on the fake S3 write log); the
max_concurrency precedence + built-in default 3 and the explicit-key detection (Amendments 4/5);
the required split examples incl. 406/3 and 406/6 (Amendment 5.4 a-g); one-table-per-task passes
with an info line (Amendment 3.5); the cdc boolean strictness (Amendment 8.1).
"""
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_split as ms          # noqa: E402
import mrl_settings as msettings  # noqa: E402
import mrl_params_csv as mpc     # noqa: E402
import params_csv as orig_pc     # noqa: E402  (the existing pipeline's parser)
from mrl_fakes import FakeS3     # noqa: E402


# ---- AMENDMENT 1.5: no stray mrl/fleet/ or bare fleet_tasks.csv references ------------------
def test_no_forbidden_references():
    bad = []
    this_file = os.path.abspath(__file__)
    for path in glob.glob(os.path.join(PKG, "**", "*"), recursive=True):
        if not os.path.isfile(path) or path.endswith((".pyc",)):
            continue
        if os.path.abspath(path) == this_file:
            continue  # this test legitimately contains the forbidden literals it searches for
        rel = os.path.relpath(path, PKG)
        try:
            text = open(path, encoding="utf-8").read()
        except (UnicodeDecodeError, IsADirectoryError):
            continue
        for i, line in enumerate(text.splitlines(), 1):
            low = line.lower()
            # allowed ONLY as an explicit mention of the EXISTING pipeline's files / the
            # never-write guard (which names all three existing files together).
            allowed = any(w in low for w in (
                "existing", "never writes", "never write", "original", "config/fleet_tasks.csv",
                "next to fleet_tasks.csv", "pipeline.json / fleet_tasks.csv", "forbidden"))
            if "mrl/fleet" in line and not allowed:
                bad.append("%s:%d mrl/fleet/ reference" % (rel, i))
            if "fleet_tasks" in line and not allowed:
                bad.append("%s:%d bare fleet_tasks reference: %s" % (rel, i, line.strip()[:70]))
    assert not bad, "forbidden references:\n  - " + "\n  - ".join(bad)


# ---- AMENDMENT 3.1: the copied params parser is byte-identical for parse/to_pipeline_settings
def test_params_csv_copy_sync():
    import inspect
    for fn in ("parse", "to_pipeline_settings", "_split_row", "role_name_from_arn"):
        assert inspect.getsource(getattr(mpc, fn)) == inspect.getsource(getattr(orig_pc, fn)), \
            "mrl_params_csv.%s has DRIFTED from params_csv.%s" % (fn, fn)


# ---- AMENDMENT 3.2 / 9.3: settings publish/pin, provenance, NEVER write the three config files
def _settings_sim(params_rows, live_settings=None, max_concurrency=None, cdc=None,
                  running=False):
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key="config/params.csv",
                  Body=("\n".join(["parameter,value"] + params_rows) + "\n").encode())
    if live_settings is not None:
        s3.put_object(Bucket="b", Key="config/pipeline_parallel_load.json",
                      Body=json.dumps(live_settings).encode())
    writes = []
    orig_put = s3.put_object

    def logging_put(Bucket, Key, **kw):
        writes.append(Key)
        return orig_put(Bucket=Bucket, Key=Key, **kw)
    s3.put_object = logging_put

    class _Sfn:
        def list_executions(self, **k):
            return {"executions": ([{"executionArn": "arn:run1", "name": "run1"}]
                                   if running else [])}

        def describe_execution(self, **k):
            return {"input": "{}"}

    def fake_client(service, **kw):
        return {"s3": s3, "stepfunctions": _Sfn()}.get(service) or s3
    msettings.boto3 = type("b", (), {"client": staticmethod(fake_client)})
    raw_input = {"bucket": "b"}
    if max_concurrency is not None:
        raw_input["max_concurrency"] = max_concurrency
    if cdc is not None:
        raw_input["cdc"] = cdc
    out = msettings.handler({
        "bucket": "b", "rawInput": raw_input, "fleetExecutionName": "e1",
        "fleetExecutionArn": "arn:exec:e1",
        "fleetStateMachineArn": "arn:sm:fleet", "taskStateMachineArn": "arn:sm:task"}, None)
    return out, s3, writes


_BASE_PARAMS = ["account_id,123456789012", "region,us-east-1", "project,demo",
                "dsql_endpoint,c.dsql.us-east-1.on.aws",
                "glue_role_arn,arn:aws:iam::123456789012:role/demo-glue-exec-role"]


def test_settings_publish_and_never_writes_forbidden():
    out, s3, writes = _settings_sim(_BASE_PARAMS)
    assert out["published"] is True
    # the shared settings file + the pinned copy were written
    assert any(w == "config/pipeline_parallel_load.json" for w in writes)
    assert any(w.startswith("mrl/runs/e1/settings.json") for w in writes)
    # NEVER the three existing-pipeline files
    for forbidden in ("config/params.csv", "config/pipeline.json", "config/fleet_tasks.csv"):
        assert forbidden not in writes, "MRL wrote forbidden %s" % forbidden
    # provenance present
    doc = s3.get_json("config/pipeline_parallel_load.json")
    assert doc["_mrl_provenance"]["source"] == "config/params.csv"
    assert "params_sha256" in doc["_mrl_provenance"]


# ---- M7: the forbidden-write guard must RAISE for each forbidden key (negative test) --------
def test_m7_put_refuses_each_forbidden_key():
    """_put()/_guard_forbidden is pure defense-in-depth; prove it actually fires. A regression
    that pointed a write at config/params.csv / pipeline.json / fleet_tasks.csv must raise."""
    from mrl_fakes import FakeS3
    s3 = FakeS3()
    for forbidden in ("config/params.csv", "config/pipeline.json", "config/fleet_tasks.csv",
                      "/config/params.csv"):   # leading slash is stripped and still refused
        try:
            msettings._put(s3, "b", forbidden, b"{}")
            assert False, "writing forbidden %s must raise" % forbidden
        except msettings.MrlSettingsError as e:
            assert "REFUSING to write" in str(e)
        # and nothing was written
        assert forbidden.lstrip("/") not in s3.keys()
    # a NON-forbidden key still writes fine
    msettings._put(s3, "b", "config/pipeline_parallel_load.json", b"{}")
    assert "config/pipeline_parallel_load.json" in s3.keys()


def test_m7_mutation_disabling_guard_is_caught():
    """Mutation: if `if k in _FORBIDDEN_WRITE:` became `if False:`, _put would NOT raise on a
    forbidden key. Prove the negative test depends on the guard by executing a mutated copy."""
    import ast
    src = open(msettings.__file__).read()
    tree = ast.parse(src)
    seg = None
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == "_guard_forbidden":
            seg = ast.get_source_segment(src, n)
    assert seg is not None
    mutated = seg.replace("if k in _FORBIDDEN_WRITE:", "if False:")
    assert mutated != seg
    ns = {"_FORBIDDEN_WRITE": msettings._FORBIDDEN_WRITE,
          "MrlSettingsError": msettings.MrlSettingsError}
    exec(mutated, ns)
    # mutated guard does NOT raise -> the regression slips through (so the real test is meaningful)
    ns["_guard_forbidden"]("config/params.csv")   # must NOT raise under the mutation
    # the REAL guard DOES raise:
    try:
        msettings._guard_forbidden("config/params.csv")
        assert False
    except msettings.MrlSettingsError:
        pass


def test_settings_unchanged_no_write():
    out1, s3, _ = _settings_sim(_BASE_PARAMS)
    live = s3.get_json("config/pipeline_parallel_load.json")
    out2, s3b, writes2 = _settings_sim(_BASE_PARAMS, live_settings=live)
    assert out2["published"] is False
    assert "config/pipeline_parallel_load.json" not in writes2   # unchanged -> no write


def test_settings_change_while_running_fails_closed():
    out1, s3, _ = _settings_sim(_BASE_PARAMS)
    live = s3.get_json("config/pipeline_parallel_load.json")
    changed = [r for r in _BASE_PARAMS] + ["map_max_concurrency,8"]
    try:
        _settings_sim(changed, live_settings=live, running=True)
        assert False, "should fail closed while an MRL run is in progress"
    except msettings.MrlSettingsError as e:
        assert "in progress" in str(e)


# ---- AMENDMENT 10 A.2: params.csv no longer drives max_concurrency/sizing -----------------
def test_a10_params_csv_does_not_drive_max_concurrency():
    # absent -> the settings doc carries NO max_concurrency / _explicit_keys layer any more
    out, s3, _ = _settings_sim(_BASE_PARAMS)
    doc = s3.get_json("config/pipeline_parallel_load.json")
    assert "max_concurrency" not in doc, "A10: settings doc must not carry a max_concurrency layer"
    assert "_explicit_keys" not in doc, "A10: settings doc must not carry _explicit_keys"
    # even an explicit map_max_concurrency,6 in params.csv does NOT appear as a published layer
    out2, s3b, _ = _settings_sim(_BASE_PARAMS + ["map_max_concurrency,6"])
    doc2 = s3b.get_json("config/pipeline_parallel_load.json")
    assert "max_concurrency" not in doc2
    # the helpers that read the params.csv layer were removed (A.2)
    assert not hasattr(msettings, "params_key_explicit")
    assert not hasattr(msettings, "_resolved_params_max_concurrency")


def test_exec_max_concurrency_validation():
    for good in (4, "4", 1, 40):
        assert msettings._validate_max_concurrency(good) == int(good)
    assert msettings._validate_max_concurrency(None) is None
    for bad in (0, -1, 3.5, "3.5", "abc", "", True, 41, [1]):
        try:
            msettings._validate_max_concurrency(bad)
            assert False, "should reject %r" % (bad,)
        except msettings.MrlSettingsError:
            pass


def test_cdc_validation_strict():
    assert msettings._validate_cdc_bool(None) is None
    assert msettings._validate_cdc_bool(True) is True
    assert msettings._validate_cdc_bool("true") is True
    assert msettings._validate_cdc_bool("FALSE") is False
    for bad in (1, 0, "yes", "", "1", [True]):
        try:
            msettings._validate_cdc_bool(bad)
            assert False, "should reject cdc=%r" % (bad,)
        except msettings.MrlSettingsError:
            pass


# ---- AMENDMENT 5.4 split examples (the default 3 cases) -------------------------------------
def test_split_examples_default_3():
    assert ms.range_sizes(5, 3) == [2, 2, 1]
    assert ms.range_sizes(7, 3) == [3, 2, 2]
    assert ms.range_sizes(2, 3) == [1, 1]          # min(3,2)=2 ranges of 1
    assert ms.range_sizes(406, 3) == [136, 135, 135]
    assert ms.range_sizes(406, 6) == [68, 68, 68, 68, 67, 67]
    assert ms.range_sizes(406, 4) == [102, 102, 101, 101]


# ---- AMENDMENT 6: CDC files NEVER picked ---------------------------------------------------
def test_a6_planted_cdc_name_as_range_start_fails_closed():
    """A CDC timestamp name passed as a range start/end must FAIL CLOSED in the REAL loader
    resolver (extracted from the patched loader) - a CDC name never resolves to a LOAD file."""
    import ast
    patched = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
    src = open(patched).read()
    tree = ast.parse(src)
    ns = {"re": __import__("re")}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef,)) and n.name in (
                "_mr_basename", "_mr_file_number", "_mr_resolve_one", "_mr_resolve_slice"):
            exec(ast.get_source_segment(src, n), ns)
        if isinstance(n, ast.ClassDef) and n.name == "ManualRangeError":
            exec(ast.get_source_segment(src, n), ns)
    files = [("s3://b/s/t/LOAD%08d.csv" % i, 100) for i in range(1, 6)]
    for cdc in ("20261002-123456789.csv", "20261002-1234567890.csv"):
        try:
            ns["_mr_resolve_slice"](cdc, "LOAD00000005.csv", files)
            assert False, "a CDC name as a range start must fail closed (got a slice)"
        except ns["ManualRangeError"]:
            pass


def test_cdc_denylist_and_allowlist_mutation():
    """Mutation: a CDC timestamp name must be rejected by is_full_load_key; flipping the deny
    classification would mis-classify it. The allow-list + CDC deny-list are load-bearing."""
    assert ms.is_full_load_key("s/t/LOAD00000001.csv") is True
    assert ms.is_full_load_key("s/t/20261002-123456789.csv") is False   # CDC name never allowed
    assert ms.is_cdc_key("s/t/20261002-123456789.csv") is True
    assert ms.is_cdc_key("s/t/processed/LOAD00000001.csv") is True
    assert ms.classify_basename("s/t/20261002-123456789.csv", 100) == "cdc"
    assert ms.classify_basename("s/t/LOAD00000001.csv", 100) == "allowed"
    assert ms.classify_basename("s/t/notes.csv", 100) == "other"


def test_readme_has_cdc_box_and_simulator_has_cdc_scenario():
    readme = open(os.path.join(PKG, "README.md")).read()
    assert "We load only `LOAD*.csv`" in readme and "never" in readme.lower(), \
        "README must carry the CDC-only box (Amendment 6 / B9)"
    sim = open(os.path.join(PKG, "tools", "mrl_simulate.py")).read()
    assert "19_cdc_files_ignored" in sim, "simulator must have the CDC-files-ignored scenario (B9)"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d amendment test(s) passed" % len(fns))
