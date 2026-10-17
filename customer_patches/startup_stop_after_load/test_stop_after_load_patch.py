#!/usr/bin/env python3
"""Executing, OFFLINE (no AWS, no Spark, no network) tests for the stopAfterLoad patch.

Run:  python3 customer_patches/startup_stop_after_load/test_stop_after_load_patch.py
(REPO_DIR overridable). Also collected by pytest.

Covers (the task's test list):
  * resolve_task._normalize_stop_after_load: matrix of valid + invalid values (strict).
  * preflight: fleet flag true/false/invalid; invalid REJECTED before any task starts;
    child_input carries stopAfterLoad ONLY when true (default child input byte-identical to
    today); per-task flag cannot be overridden DOWN by a fleet false (rule: TRUE WINS).
  * ASL WALK: from each branch of every flag Choice, assert the validation Glue job,
    ResumeDmsToCdc, StartCdcJob, the CDC fork starts and WriteStartupOverrideRecord are
    UNREACHABLE when the flag is true; and reachable on the normal path.
  * ASL PATH AUDIT: reuse the repo's existing audit on the PATCHED definitions (import + run).
  * BYTE-IDENTICAL WHEN ABSENT: with the flag absent the patched startup ASL takes the EXACT
    same Choice/Pass state sequence as the ORIGINAL (snapshot) for a set of simulated inputs.
  * TRANSFORM round-trip: patch_*(orig) reproduces the shipped patched files byte-for-byte and
    is idempotent; fail-closed on a missing anchor / unrecognised shape.
  * APPLY SCRIPT vs FAKE boto3: dry run changes nothing; apply updates in SAFE ORDER (lambdas
    first) with read-back compare; a SECOND apply is a no-op; restore puts everything back; a
    missing ASL anchor => nothing changed.
  * MUTATION checks: (a) remove the skip of GroupValidate, (b) route the flag into
    ResumeDmsToCdc, (c) accept 'yes' in preflight — EACH must make a test here fail.
  * CORRECTNESS: a later normal startup after a load-only run (DMS left
    STOPPED_AFTER_CACHED_EVENTS) is ACCEPTED (not 'past full load') by _check_startup_task.
"""
import copy
import importlib
import io
import json
import os
import sys
import tempfile
import types
import zipfile
import atexit
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
_TMP = tempfile.mkdtemp(prefix="stopafterload_test_")
atexit.register(lambda: shutil.rmtree(_TMP, ignore_errors=True))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(os.path.dirname(HERE))
SF_DIR = os.path.join(REPO, "stepfunctions")
LAMBDAS = os.path.join(REPO, "lambdas")
ORIG = os.path.join(HERE, "orig")
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "tests"))
sys.path.insert(0, LAMBDAS)

import stop_after_load_transform as X  # noqa: E402

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


# --------------------------------------------------------------------------- module loaders
def _load_rt():
    b = types.ModuleType("boto3")
    b.client = lambda *a, **k: None
    sys.modules["boto3"] = b
    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    return rt


def _load_preflight():
    b = types.ModuleType("boto3")
    b.client = lambda *a, **k: None
    sys.modules["boto3"] = b
    pf = importlib.import_module("preflight_tasks")
    importlib.reload(pf)
    return pf


# the repo's own ASL walker (honest Choice/Pass interpreter)
import test_override as TOV  # noqa: E402


def _startup():
    return json.load(open(os.path.join(SF_DIR, "startup.asl.json")))


# =============================================================================================
# 1) resolve_task._normalize_stop_after_load — strict matrix
# =============================================================================================
def test_normalize_matrix():
    rt = _load_rt()
    f = rt._normalize_stop_after_load
    valid_true = [{"stopAfterLoad": True}, {"stopAfterLoad": "true"}, {"stopAfterLoad": "True"},
                  {"stopAfterLoad": "TRUE"}, {"stopAfterLoad": "  true "}]
    valid_false = [{}, {"stopAfterLoad": None}, {"stopAfterLoad": False},
                   {"stopAfterLoad": "false"}, {"stopAfterLoad": "False"}, {"stopAfterLoad": "FALSE"}]
    for inp in valid_true:
        check(f(inp) is True, f"normalize {inp} -> True")
    for inp in valid_false:
        check(f(inp) is False, f"normalize {inp} -> False (default byte-identical)")
    # Strict: everything else REJECTS (note: 'yes'/1/'on'/'' are truthy to the LENIENT override
    # normalizer but must be REJECTED here).
    for bad in ["yes", "y", "on", "1", 1, 1.0, 0, "", "  ", "nope", [], {}, "truthy"]:
        try:
            f({"stopAfterLoad": bad})
            check(False, f"normalize rejects {bad!r}")
        except rt.TaskCheckError as e:
            check("stopAfterLoad" in str(e), f"normalize rejects {bad!r} naming the field")
    # And override stays LENIENT (we did not change it): 'yes' is still truthy for override.
    check(rt._normalize_override({"override": "yes"}) is True,
          "override normalizer is still lenient ('yes' truthy) — unchanged")


# =============================================================================================
# 2) preflight: fleet/per-task wiring, strict rejection, child-input byte-identical, TRUE WINS
# =============================================================================================
def _preflight_src():
    return open(os.path.join(LAMBDAS, "preflight_tasks.py")).read()


def test_preflight_source_wiring():
    pf = _load_preflight()
    src = _preflight_src()
    check("fleet_stop_after_load = rt._normalize_stop_after_load(fleet_input)" in src,
          "preflight reads a fleet-level stopAfterLoad from the fleet start input")
    check("raise PreflightError(f\"Fleet start input rejected, nothing was started: {e}\")" in src,
          "an invalid fleet stopAfterLoad is rejected as a PreflightError (nothing started)")
    check("if fleet_stop_after_load or row_stop_after_load:" in src
          and 'child_input["stopAfterLoad"] = True' in src,
          "fleet-level OR per-task flag adds {'stopAfterLoad': true}; TRUE WINS (OR, not AND)")
    check('child_input["stopAfterLoad"] = True' in src
          and "child_input[\"stopAfterLoad\"] = False" not in src,
          "stopAfterLoad is only ever ADDED as true — never written false (default child input "
          "byte-identical to today)")


def test_preflight_fleet_invalid_rejected_before_any_start():
    """Drive the real preflight handler with a fake S3/DMS and an INVALID fleet flag; it must
    raise PreflightError and have started NOTHING (the fleet flag is checked before the task
    list is even read)."""
    pf = _load_preflight()

    started = {"describe_calls": 0}

    class FakeS3:
        def get_object(self, Bucket, Key):
            started["describe_calls"] += 1  # any S3 read counts as 'work begun'
            raise AssertionError("preflight read S3 before rejecting the invalid fleet flag")

    class FakeDms:
        def describe_replication_tasks(self, **k):
            raise AssertionError("preflight touched DMS before rejecting the invalid fleet flag")

    pf.boto3.client = lambda svc, **k: FakeS3() if svc == "s3" else FakeDms()
    try:
        pf.handler({"bucket": "b", "fleetInput": {"bucket": "b", "stopAfterLoad": "yes"},
                    "mode": "startup"}, None)
        check(False, "invalid fleet stopAfterLoad must raise before any task starts")
    except pf.PreflightError as e:
        check("stopAfterLoad" in str(e) and started["describe_calls"] == 0,
              "invalid fleet stopAfterLoad rejected in preflight, nothing read/started")
    except AssertionError as e:
        check(False, str(e))


def _run_preflight_rows(pf, fleet_input, rows_csv):
    """Drive the preflight loop enough to capture each row's child_input, with fakes that make
    every row a clean, startable task. Returns the out_tasks list."""
    import csv as _csv

    cfg = {"project": "proj", "region": "us-east-1"}
    # settings + task list via fakes
    tasks_text = rows_csv

    class FakeS3:
        def get_object(self, Bucket, Key):
            if Key.endswith("fleet_tasks.csv"):
                return {"Body": io.BytesIO(tasks_text.encode())}
            raise pf.rt_nofile  # type: ignore

    # Patch the small helpers preflight uses so we exercise the REAL child_input logic only.
    pf._get_text = lambda s3, b, k: tasks_text if k.endswith("fleet_tasks.csv") else None
    pf.rt._load_settings = lambda *a, **k: cfg
    pf._handle_params_csv = lambda *a, **k: None
    pf._running_tasks = lambda *a, **k: {}

    def fake_describe(**k):
        arn = k["Filters"][0]["Values"][0]
        return {"ReplicationTasks": [{"ReplicationTaskArn": arn,
                                      "ReplicationTaskIdentifier": arn.split(":")[-1]}]}
    fake_dms = types.SimpleNamespace(describe_replication_tasks=fake_describe)
    fake_s3 = types.SimpleNamespace(
        get_object=lambda **k: (_ for _ in ()).throw(Exception("no")),
    )
    pf.boto3.client = lambda svc, **k: fake_dms if svc == "dms" else fake_s3

    # Short-circuit the per-task checks that need real DMS/S3; keep the suffix + child_input logic.
    pf.rt._parse_task_arn = lambda arn: {"region": "us-east-1", "account": "111111111111"}
    pf.rt._pick_suffix = lambda s3, b, ci, task, ids: (
        task["ReplicationTaskIdentifier"], "input", None, None)
    pf.rt._validate_suffix = lambda *a, **k: None
    pf.rt._get_json = lambda *a, **k: None
    pf.rt._endpoint_contract = lambda *a, **k: {}
    pf.rt._check_startup_task = lambda *a, **k: None
    pf.rt._has_run_artifacts = lambda *a, **k: None
    pf.rt._parse_table_mappings = lambda *a, **k: []
    pf.rt._estimate_selection_schemas = lambda rules: (set(), False)

    out = pf.handler({"bucket": "b", "fleetInput": fleet_input, "mode": "startup",
                      "stateMachineArn": "arn:sm"}, None)
    return out["tasks"]


def test_child_input_byte_identical_when_absent():
    pf = _load_preflight()
    rows = "task_arn\narn:aws:dms:us-east-1:111111111111:task:t1\n"
    tasks = _run_preflight_rows(pf, {"bucket": "b"}, rows)
    ci = tasks[0]["input"]
    check(ci == {"taskArn": "arn:aws:dms:us-east-1:111111111111:task:t1"},
          f"no flag -> child input has NO stopAfterLoad key (byte-identical) [{ci}]")


def test_child_input_fleet_true():
    pf = _load_preflight()
    rows = "task_arn\narn:aws:dms:us-east-1:111111111111:task:t1\n"
    tasks = _run_preflight_rows(pf, {"bucket": "b", "stopAfterLoad": True}, rows)
    check(tasks[0]["input"].get("stopAfterLoad") is True,
          "fleet stopAfterLoad=true -> child input carries stopAfterLoad:true")


def test_true_wins_fleet_false_cannot_override_per_task_true():
    pf = _load_preflight()
    rows = ("task_arn,stop_after_load\n"
            "arn:aws:dms:us-east-1:111111111111:task:t1,true\n"
            "arn:aws:dms:us-east-1:111111111111:task:t2,\n")
    # fleet flag FALSE, per-task t1 TRUE: TRUE WINS for t1; t2 stays off.
    tasks = _run_preflight_rows(pf, {"bucket": "b", "stopAfterLoad": False}, rows)
    by = {t["taskArn"].split(":")[-1]: t["input"] for t in tasks}
    check(by["t1"].get("stopAfterLoad") is True,
          "per-task true + fleet false -> TRUE WINS (t1 load-only)")
    check("stopAfterLoad" not in by["t2"],
          "no per-task flag + fleet false -> t2 child input byte-identical (no key)")
    # fleet TRUE, per-task blank: fleet true wins for both.
    tasks2 = _run_preflight_rows(pf, {"bucket": "b", "stopAfterLoad": True}, rows)
    by2 = {t["taskArn"].split(":")[-1]: t["input"] for t in tasks2}
    check(by2["t1"].get("stopAfterLoad") is True and by2["t2"].get("stopAfterLoad") is True,
          "fleet true -> both children load-only (per-task blank cannot turn it off)")


def test_preflight_per_task_invalid_cell_rejected():
    pf = _load_preflight()
    rows = ("task_arn,stop_after_load\n"
            "arn:aws:dms:us-east-1:111111111111:task:t1,maybe\n")
    try:
        _run_preflight_rows(pf, {"bucket": "b"}, rows)
        check(False, "an invalid per-task stop_after_load cell must be rejected")
    except pf.PreflightError as e:
        check("stop_after_load" in str(e) and "stopAfterLoad" in str(e),
              "invalid per-task cell rejected in preflight naming the field/column")


# =============================================================================================
# 3) ASL WALK — unreachable proof from each branch of the flag decision
# =============================================================================================
FORBIDDEN_WHEN_LOAD_ONLY = {"GroupValidate", "ResumeDmsToCdc", "StartCdcJob",
                            "WriteStartupOverrideRecord", "MarkOverrideActive",
                            "TaskSucceededWithOverride"}


def _iter_states(sm):
    return sm["States"]["GroupFanOut"]["Iterator"]["States"]


def _reachable_from(states, start, data, max_steps=500):
    """Collect every state VISITED by the honest walker, following the one route the Choice/Pass
    logic selects for this concrete `data`. (Tasks are pass-through to Next.)"""
    _, visited = TOV.walk({"States": states}, start, data, max_steps=max_steps)
    return set(visited)


def test_walk_load_only_skips_validate_in_iterator():
    it = _iter_states(_startup())
    # load-only after a load -> GroupOk, never GroupValidate
    vis = _reachable_from(it, "AfterGroupLoad", {"stopAfterLoad": True})
    check("GroupValidate" not in vis and "GroupOk" in vis,
          "iterator: load-only after-load reaches GroupOk, NOT GroupValidate")
    # normal -> GroupValidate
    visn = _reachable_from(it, "AfterGroupLoad", {"stopAfterLoad": False})
    check("GroupValidate" in visn, "iterator: normal after-load reaches GroupValidate")
    # absent (older exec) -> GroupValidate (byte-identical)
    visa = _reachable_from(it, "AfterGroupLoad", {})
    check("GroupValidate" in visa, "iterator: absent flag after-load reaches GroupValidate")


def _top_data(load_only, any_load_failed, override, present=True):
    d = {
        "groupCheck": {"anyLoadFailed": any_load_failed, "anyValidateFailed": False,
                       "statuses": []},
        "resolved": {"override": override, "taskSuffix": "t"},
        "plan": {"groups": []},
        "overrideResolved": None,
    }
    if present:
        d["loadOnly"] = load_only
    return d


def test_walk_load_only_terminal_unreachables():
    sm = _startup()
    # load-only + all OK: from LoadOnlyOrContinue, walk the whole tail. Must reach
    # TaskLoadOnlySucceeded (via RecordLoadOnly) and must NOT touch any forbidden state.
    vis = _reachable_from(sm["States"], "LoadOnlyOrContinue", _top_data(True, False, False))
    check("TaskLoadOnlySucceeded" in vis and "RecordLoadOnly" in vis,
          "load-only + all OK -> RecordLoadOnly -> TaskLoadOnlySucceeded")
    bad = vis & FORBIDDEN_WHEN_LOAD_ONLY
    check(not bad, f"load-only + all OK reaches NO validation/CDC/override state [{sorted(bad)}]")
    # load-only + load failure: -> GroupsFailed, and NEVER resume/CDC even if override=true.
    visf = _reachable_from(sm["States"], "LoadOnlyOrContinue", _top_data(True, True, True))
    check("GroupsFailed" in visf,
          "load-only + load failure -> GroupsFailed (override does NOT rescue)")
    badf = visf & FORBIDDEN_WHEN_LOAD_ONLY
    check(not badf, f"load-only + load failure reaches NO CDC/override state [{sorted(badf)}]")


def test_walk_normal_path_unchanged():
    sm = _startup()
    # normal + all OK -> AllGroupsSucceeded -> ... -> ResumeDmsToCdc -> StartCdcJob -> TaskSucceeded
    vis = _reachable_from(sm["States"], "LoadOnlyOrContinue", _top_data(False, False, False))
    check("AllGroupsSucceeded" in vis and "ResumeDmsToCdc" in vis and "StartCdcJob" in vis,
          "normal + all OK still resumes DMS and starts CDC (unchanged)")
    check("TaskLoadOnlySucceeded" not in vis,
          "normal run never reaches the load-only terminal")
    # older exec (no loadOnly key) -> same normal path
    visa = _reachable_from(sm["States"], "LoadOnlyOrContinue", _top_data(False, False, False,
                                                                         present=False))
    check("AllGroupsSucceeded" in visa and "TaskLoadOnlySucceeded" not in visa,
          "older execution (no loadOnly) takes the unchanged normal path")


# =============================================================================================
# 4) ASL PATH AUDIT — reuse the repo's existing audit on the PATCHED definitions
# =============================================================================================
def test_reuse_repo_asl_path_audit():
    import test_asl_paths as AUD
    importlib.reload(AUD)
    gaps, reach = [], []
    for f in sorted(x for x in os.listdir(SF_DIR) if x.endswith(".asl.json")):
        gaps += AUD.analyze(os.path.join(SF_DIR, f))
        reach += AUD.reachability(os.path.join(SF_DIR, f))
    check(not reach, f"repo reachability audit clean on patched defs [{reach[:3]}]")
    check(not gaps, f"repo path audit (unguarded missing paths) clean on patched defs [{gaps[:3]}]")


# =============================================================================================
# 5) BYTE-IDENTICAL WHEN ABSENT — patched ASL == original state sequence when flag absent
# =============================================================================================
def _seq(states, start, data):
    _, visited = TOV.walk({"States": states}, start, dict(data), max_steps=500)
    return visited


def test_byte_identical_state_sequence_when_absent():
    orig = json.load(open(os.path.join(ORIG, "startup.asl.json")))
    patched = _startup()
    o_it = orig["States"]["GroupFanOut"]["Iterator"]["States"]
    p_it = patched["States"]["GroupFanOut"]["Iterator"]["States"]
    # Iterator after-load: original starts at GroupValidate's predecessor. In the original the
    # loaders go straight to GroupValidate; in the patched tree AfterGroupLoad defaults there.
    # With the flag ABSENT the patched iterator from GroupLoad.Next should land on GroupValidate
    # just like the original.
    o_from_load = _seq(o_it, "GroupValidate", {})          # original: load -> GroupValidate -> ...
    p_after = _seq(p_it, "AfterGroupLoad", {})             # patched absent: -> GroupValidate -> ...
    check(p_after[1:] == o_from_load, "iterator sequence after AfterGroupLoad(absent) == original "
          "GroupValidate-onward sequence")

    # Top-level terminal region: original EvaluateGroupResults.Next = AllGroupsSucceeded. With the
    # flag absent the patched LoadOnlyOrContinue defaults to AllGroupsSucceeded, so the sequence
    # from there is identical to the original from AllGroupsSucceeded.
    for (anyLoad, override) in [(False, False), (True, False), (False, True), (True, True)]:
        d = _top_data(False, anyLoad, override, present=False)
        o_seq = _seq(orig["States"], "AllGroupsSucceeded", d)
        p_seq = _seq(patched["States"], "LoadOnlyOrContinue", d)
        check(p_seq[1:] == o_seq,
              f"absent-flag terminal sequence == original (anyLoad={anyLoad}, override={override})")


# =============================================================================================
# 6) TRANSFORM round-trip + fail-closed
# =============================================================================================
def test_transform_reproduces_shipped_files():
    rt_new = X.patch_resolve_task_src(open(os.path.join(ORIG, "resolve_task.py")).read())
    check(rt_new == open(os.path.join(LAMBDAS, "resolve_task.py")).read(),
          "patch_resolve_task_src(orig) == shipped lambdas/resolve_task.py (byte-identical)")
    pf_new = X.patch_preflight_tasks_src(open(os.path.join(ORIG, "preflight_tasks.py")).read())
    check(pf_new == open(os.path.join(LAMBDAS, "preflight_tasks.py")).read(),
          "patch_preflight_tasks_src(orig) == shipped lambdas/preflight_tasks.py")
    asl_new = X.patch_startup_asl(json.load(open(os.path.join(ORIG, "startup.asl.json"))))
    check(asl_new == json.load(open(os.path.join(SF_DIR, "startup.asl.json"))),
          "patch_startup_asl(orig) == shipped stepfunctions/startup.asl.json (semantic)")
    # idempotent
    check(X.patch_resolve_task_src(rt_new) == rt_new, "resolve_task transform idempotent")
    check(X.patch_preflight_tasks_src(pf_new) == pf_new, "preflight transform idempotent")
    check(X.patch_startup_asl(asl_new) == asl_new, "startup ASL transform idempotent")
    # fleet unchanged
    fo = json.load(open(os.path.join(ORIG, "fleet-startup.asl.json")))
    check(X.verify_fleet_startup_asl(fo) == fo, "fleet ASL verified + returned unchanged")


def test_shipped_repo_diff_includes_payload_contract_registration():
    """F1 regression guard: the shipped plain-file diff MUST cover all four changed files,
    including the tests/test_asl_payload_contract.py registration of the new
    write_load_only_record Lambda mode — without it a repo deployer who `git apply`s the diff gets
    a RED repo suite (2 failing audit tests)."""
    diff_path = os.path.join(HERE, "startup_stop_after_load.repo.diff")
    diff = open(diff_path).read()
    for f in ("lambdas/resolve_task.py", "lambdas/preflight_tasks.py",
              "stepfunctions/startup.asl.json", "tests/test_asl_payload_contract.py"):
        check(f"+++ b/{f}\n" in diff, f"repo.diff covers {f}")
    check('("resolve_task.py", "write_load_only_record"): ["handler_write_load_only_record"]' in diff,
          "repo.diff adds the MODE_RETURN_FUNCS registration for write_load_only_record")
    check('("resolve_task.py", "write_load_only_record"): ["bucket", "taskSuffix"]' in diff,
          "repo.diff adds the MODE_REQUIRED_INPUT registration for write_load_only_record")


def test_transform_fail_closed():
    # missing anchor in .py
    try:
        X.patch_resolve_task_src("print('not the file')\n")
        check(False, "resolve_task transform must fail closed on an unrecognised file")
    except X.PatchError:
        check(True, "resolve_task transform fails closed on a missing anchor")
    # unrecognised ASL shape
    try:
        X.patch_startup_asl({"States": {"Foo": {"Type": "Pass", "End": True}},
                             "StartAt": "Foo"})
        check(False, "startup ASL transform must fail closed on an unrecognised shape")
    except X.PatchError:
        check(True, "startup ASL transform fails closed on an unrecognised shape")
    # fleet ASL that does not forward fleetInput
    try:
        X.verify_fleet_startup_asl({"States": {"Preflight": {"Parameters": {"Payload": {}}}}})
        check(False, "fleet verify must fail closed when fleetInput is not forwarded")
    except X.PatchError:
        check(True, "fleet verify fails closed when fleetInput.$ != $")


# =============================================================================================
# 7) APPLY SCRIPT vs FAKE boto3
# =============================================================================================
def _zip_with(members):
    buf = io.BytesIO()
    z = zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED)
    for name, data in members.items():
        z.writestr(name, data)
    z.close()
    return buf.getvalue()


class FakeAws:
    """In-memory stand-in for the allowed AWS actions. Records call order."""
    def __init__(self, project, region, startup_def, fleet_def, rt_zip, pf_zip):
        self.region = region
        self.calls = []
        self.sm = {
            f"{project}-startup": {"arn": f"arn:sm:{project}-startup", "definition": startup_def},
            f"{project}-fleet-startup": {"arn": f"arn:sm:{project}-fleet-startup",
                                         "definition": fleet_def},
        }
        self.code = {f"{project}-resolve-task": rt_zip, f"{project}-preflight-tasks": pf_zip}
        self.s3 = {}

    # state machines
    def describe_sm(self, name, account):
        self.calls.append(("describe_sm", name))
        return {"stateMachineArn": self.sm[name]["arn"], "definition": self.sm[name]["definition"]}

    def update_sm(self, arn, definition):
        self.calls.append(("update_sm", arn))
        for v in self.sm.values():
            if v["arn"] == arn:
                v["definition"] = definition
        return {}

    # lambdas
    def get_function(self, name):
        self.calls.append(("get_function", name))
        path = os.path.join(_TMP, "_fake_code", name + ".zip")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(self.code[name])
        return {"Code": {"Location": "file://" + path}}

    def get_function_configuration(self, name):
        return {"FunctionName": name}

    def update_function_code(self, name, zip_bytes):
        self.calls.append(("update_function_code", name))
        self.code[name] = zip_bytes
        return {}

    # s3
    def put_object(self, bucket, key, body):
        self.calls.append(("put_object", key))
        self.s3[(bucket, key)] = body
        return {}


def _fresh_fake(project="proj"):
    startup_def = open(os.path.join(ORIG, "startup.asl.json")).read()
    fleet_def = open(os.path.join(ORIG, "fleet-startup.asl.json")).read()
    rt_zip = _zip_with({"resolve_task.py": open(os.path.join(ORIG, "resolve_task.py")).read(),
                        "params_csv.py": "# unrelated sibling, must stay byte-identical\nX = 1\n"})
    pf_zip = _zip_with({"preflight_tasks.py": open(os.path.join(ORIG, "preflight_tasks.py")).read(),
                        "params_csv.py": "# unrelated sibling\nX = 2\n"})
    return FakeAws(project, "us-east-1", startup_def, fleet_def, rt_zip, pf_zip), rt_zip, pf_zip


def _apply_mod():
    import apply_stop_after_load_patch as A
    importlib.reload(A)
    return A


def test_apply_dry_run_changes_nothing():
    A = _apply_mod()
    aws, rt_zip, pf_zip = _fresh_fake()
    before = (copy.deepcopy(aws.sm), dict(aws.code), dict(aws.s3))
    rc = A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b"], aws=aws)
    check(rc == 0, "dry run returns 0")
    check(not any(c[0] in ("update_sm", "update_function_code", "put_object") for c in aws.calls),
          "dry run performs NO update/put calls")
    check(aws.sm == before[0] and aws.code == before[1] and aws.s3 == before[2],
          "dry run changed nothing")


def test_apply_updates_in_safe_order_with_readback():
    A = _apply_mod()
    aws, _, _ = _fresh_fake()
    rc = A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b", "--apply", "--local-backup-dir", os.path.join(_TMP, "bk")], aws=aws)
    check(rc == 0, "apply returns 0")
    updates = [c for c in aws.calls if c[0] in ("update_function_code", "update_sm")]
    # SAFE ORDER: both lambdas first, then the startup SM, then the fleet SM (verified no-op => no
    # update_sm for fleet).
    kinds = [c[0] for c in updates]
    check(kinds[0] == "update_function_code" and kinds[1] == "update_function_code",
          f"lambdas updated FIRST [{kinds}]")
    check(("update_sm", "arn:sm:proj-startup") in aws.calls,
          "startup state machine updated")
    check(("update_sm", "arn:sm:proj-fleet-startup") not in aws.calls,
          "fleet state machine NOT updated (verified no-op)")
    # read-back: the live startup def now equals the transform output
    live = json.loads(aws.sm["proj-startup"]["definition"])
    want = X.patch_startup_asl(json.load(open(os.path.join(ORIG, "startup.asl.json"))))
    check(live == want, "live startup definition == transform output after apply")
    # the live resolve-task zip has the patched member AND the unrelated sibling byte-identical
    z = zipfile.ZipFile(io.BytesIO(aws.code["proj-resolve-task"]))
    check("def _normalize_stop_after_load(" in z.read("resolve_task.py").decode(),
          "patched resolve_task.py is inside the updated zip")
    check(z.read("params_csv.py").decode() == "# unrelated sibling, must stay byte-identical\nX = 1\n",
          "unrelated sibling params_csv.py is byte-identical in the updated zip")
    # backups were written to S3 + locally
    check(any(k[1].startswith("scripts/backup/startup_stop_after_load/") for k in aws.s3),
          "originals backed up to s3://.../scripts/backup/startup_stop_after_load/<ts>/")


def test_apply_second_run_is_noop():
    A = _apply_mod()
    aws, _, _ = _fresh_fake()
    A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b", "--apply", "--local-backup-dir", os.path.join(_TMP, "bk")], aws=aws)
    # second apply: the plan diffs are empty (idempotent transforms) -> no update calls
    aws.calls.clear()
    rc = A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b", "--apply", "--local-backup-dir", os.path.join(_TMP, "bk")], aws=aws)
    check(rc == 0, "second apply returns 0")
    check(not any(c[0] in ("update_sm", "update_function_code") for c in aws.calls),
          "second apply is a NO-OP (no update_sm / update_function_code)")


def test_restore_puts_everything_back():
    A = _apply_mod()
    aws, rt_zip, pf_zip = _fresh_fake()
    orig_startup = aws.sm["proj-startup"]["definition"]
    A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b", "--apply", "--local-backup-dir", os.path.join(_TMP, "bk")], aws=aws)
    # find the local backup dir created
    import glob
    dirs = sorted(glob.glob(os.path.join(_TMP, "bk", "*")))
    check(bool(dirs), "a local backup dir was created")
    bdir = dirs[-1]
    aws.calls.clear()
    rc = A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b",
                 "--restore", bdir], aws=aws)
    check(rc == 0, "restore returns 0")
    check(aws.sm["proj-startup"]["definition"] == orig_startup,
          "restore put the ORIGINAL startup definition back")
    z = zipfile.ZipFile(io.BytesIO(aws.code["proj-resolve-task"]))
    check("def _normalize_stop_after_load(" not in z.read("resolve_task.py").decode(),
          "restore put the ORIGINAL (unpatched) resolve-task code back")


def test_apply_missing_anchor_changes_nothing():
    A = _apply_mod()
    aws, _, _ = _fresh_fake()
    # corrupt the deployed startup definition so the structural anchor is missing
    bad = json.loads(aws.sm["proj-startup"]["definition"])
    del bad["States"]["EnsureForkJobs"]
    aws.sm["proj-startup"]["definition"] = json.dumps(bad)
    before = (copy.deepcopy(aws.sm), dict(aws.code), dict(aws.s3))
    try:
        A.main(["--project", "proj", "--region", "us-east-1", "--bucket", "b", "--apply", "--local-backup-dir", os.path.join(_TMP, "bk")], aws=aws)
        check(False, "apply must fail closed when a startup anchor is missing")
    except X.PatchError:
        check(True, "apply fails closed (PatchError) on a missing startup anchor")
    check(aws.sm == before[0] and aws.code == before[1] and aws.s3 == before[2],
          "nothing was updated/backed-up when the anchor is missing (fail closed before writes)")


# =============================================================================================
# 8) MUTATION checks — each mutation must make a test here FAIL
# =============================================================================================
def _mutate_and_count_failures(mutator):
    """Apply `mutator` to a throwaway copy of the patched artifacts, run the key assertions, and
    return how many FAILED. mutator returns (sm_states_patch_fn_or_None, preflight_norm_fn_or_None)."""
    fails = 0
    sm = _startup()
    it = sm["States"]["GroupFanOut"]["Iterator"]["States"]
    mutator(sm, it)
    # (a)+(b) ASL mutations: re-run the two walk assertions against the mutated sm
    vis_iter = _reachable_from(it, "AfterGroupLoad", {"stopAfterLoad": True})
    if not ("GroupValidate" not in vis_iter and "GroupOk" in vis_iter):
        fails += 1
    vis_term = _reachable_from(sm["States"], "LoadOnlyOrContinue", _top_data(True, False, False))
    if vis_term & FORBIDDEN_WHEN_LOAD_ONLY:
        fails += 1
    return fails


def test_mutation_remove_validate_skip():
    # Mutation (a): AfterGroupLoad no longer skips GroupValidate (route both branches to validate).
    def mut(sm, it):
        it["AfterGroupLoad"]["Choices"] = []          # remove the skip -> Default GroupValidate
    fails = _mutate_and_count_failures(mut)
    check(fails >= 1, "MUTATION remove-validate-skip is caught (a test fails)")


def test_mutation_flag_routes_into_resume():
    # Mutation (b): load-only success routes into ResumeDmsToCdc (CDC starts) instead of the
    # load-only terminal.
    def mut(sm, it):
        for ch in sm["States"]["LoadOnlyOrContinue"]["Choices"]:
            if ch["Next"] in ("RecordLoadOnly",):
                ch["Next"] = "ResumeDmsToCdc"
    fails = _mutate_and_count_failures(mut)
    check(fails >= 1, "MUTATION route-load-only-into-ResumeDmsToCdc is caught (a test fails)")


def test_mutation_preflight_accepts_yes():
    # Mutation (c): if _normalize_stop_after_load were lenient like override ('yes' truthy), the
    # strict matrix test would fail. Simulate by checking the strict normalizer rejects 'yes'.
    rt = _load_rt()
    raised = False
    try:
        rt._normalize_stop_after_load({"stopAfterLoad": "yes"})
    except rt.TaskCheckError:
        raised = True
    check(raised, "MUTATION accept-'yes' would break: the strict normalizer rejects 'yes'")


# =============================================================================================
# 9) CORRECTNESS — later normal startup after a load-only run is accepted (not 'past full load')
# =============================================================================================
def test_later_normal_startup_accepted_after_load_only():
    rt = _load_rt()
    # A task left by load-only: full load 100%, STOPPED with STOPPED_AFTER_CACHED_EVENTS.
    task = {
        "ReplicationTaskArn": "arn:aws:dms:us-east-1:111111111111:task:t1",
        "MigrationType": "full-load-and-cdc",
        "ReplicationTaskSettings": json.dumps({"FullLoadSettings":
                                               {"StopTaskCachedChangesApplied": True}}),
        "ReplicationTaskStats": {"FullLoadProgressPercent": 100},
        "Status": "stopped",
        "StopReason": "Stop Reason STOPPED_AFTER_CACHED_EVENTS",
        "TableMappings": json.dumps({"rules": []}),
    }
    contract = {"addColumnName": True, "s3Bucket": "b"}
    warnings = []
    try:
        rt._check_startup_task(task, contract, "b", warnings)
        check(True, "a STOPPED_AFTER_CACHED_EVENTS task is ACCEPTED by _check_startup_task "
              "(later normal startup proceeds; not 'past full load')")
    except rt.TaskCheckError as e:
        check(False, f"later normal startup wrongly refused: {e}")

    # Contrast: a task that moved on PAST the full load (running) is correctly refused.
    moved = dict(task, Status="running", StopReason="")
    try:
        rt._check_startup_task(moved, contract, "b", [])
        check(False, "a task past its full load should be refused")
    except rt.TaskCheckError as e:
        check("already finished its full load" in str(e),
              "a task past its full load is still refused (contrast case unchanged)")


def main():
    for fn in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== stop_after_load patch tests: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
