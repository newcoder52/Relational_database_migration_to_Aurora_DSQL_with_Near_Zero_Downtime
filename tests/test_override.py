#!/usr/bin/env python3
"""Runtime "override" feature tests (startup + cutover + fleet), fully offline (no AWS, no
Spark, no network). Run: python3 tests/test_override.py   (REPO_DIR overridable).

What this covers (the override spec):

STARTUP (stepfunctions/startup.asl.json + resolve_task):
  * override=false (absent/false) + a load OR validate failed group -> GroupsFailed (UNCHANGED).
  * override=true  + a validate_failed group                 -> TaskSucceededWithOverride,
                                                                with CDC started (ResumeDmsToCdc
                                                                -> StartCdcJob on the path).
  * override=true  + a LOAD failure (data_error OR infra)     -> ALSO continues: override is a
                                                                FULL BYPASS of load AND validation
                                                                failures (MarkOverrideActive ->
                                                                CDC -> TaskSucceededWithOverride).
                                                                job2_load records the failure kind
                                                                (data_error vs infra) in
                                                                _load_status.json so the override
                                                                record's warnings can name it.
  * resolve_task normalizes $.override once (bool / "true"/"1"/"yes", any case) into
    resolved.override; the overridden groups+tables, the validation report paths and the
    per-table load-failure WARNINGS appear in the TaskSucceededWithOverride override record; the
    record + stable startup-override marker are written to config/_task/<suffix>/_overrides/.
  * A re-run with override does NOT reload tables already marked "done" (job2_load skips them
    from _load_status.json regardless of override).

CUTOVER (stepfunctions/cutover.asl.json):
  * CdcValidationPreGate / CdcValidationFinalGate are BYPASSED (logged) with override=true and
    REFUSE (CdcValidationFailed) without it.
  * The startup-override marker NO LONGER forces override at cutover: startupOverrideUsed=true +
    override!=true -> LogStartupOverrideWarning (a Pass) -> the cutover PROCEEDS with a warning
    (resolve_task also appends the warning to $.resolved.warnings). The refusal state
    StartupOverrideRequiresOverride is removed.
  * The safety ordering (DMS stop -> drain -> stop CDC runs -> delete jobs) is never bypassed.

FLEET: preflight_tasks adds {"override": true} to each child input from a fleet-level
  {"override": true} OR a per-task CSV override column; blank/absent adds nothing.

The ASL routing is checked with a small, honest Choice/Pass walker over the REAL ASL JSON
(evaluates Variable/BooleanEquals/IsPresent/And/Or/Not against a seeded input; Task/Map/Wait
are pass-through to Next; Succeed/Fail are terminal), so a wrong Next/guard in the shipped
state machine fails the test.
"""
import importlib
import json
import os
import re
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SF_DIR = os.path.join(REPO, "stepfunctions")
LAMBDAS = os.path.join(REPO, "lambdas")
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


# =============================================================================================
# A tiny, honest ASL walker: enough to resolve the TERMINAL state for a seeded input across the
# Choice/Pass logic the override feature adds. It does NOT execute Tasks (pass-through to Next);
# it DOES evaluate Choice rules and apply Pass Parameters to ResultPath so a later Choice can
# read a value a Pass produced (e.g. $.overrideResolved.active).
# =============================================================================================
def _get_path(data, path):
    if not isinstance(path, str) or not path.startswith("$"):
        return False, None
    cur = data
    parts = [p for p in re.sub(r"\[[^\]]*\]", "", path[1:]).split(".") if p]
    for p in parts:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return False, None
    return True, cur


def _eval_rule(rule, data):
    if "And" in rule:
        return all(_eval_rule(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_eval_rule(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not _eval_rule(rule["Not"], data)
    var = rule.get("Variable")
    found, val = _get_path(data, var) if var else (False, None)
    if "IsPresent" in rule:
        return found == rule["IsPresent"]
    if "IsNull" in rule:
        return (found and val is None) == rule["IsNull"]
    if "BooleanEquals" in rule:
        return found and val is rule["BooleanEquals"]
    if "StringEquals" in rule:
        return found and val == rule["StringEquals"]
    if "NumericEquals" in rule:
        return found and val == rule["NumericEquals"]
    if "NumericGreaterThan" in rule:
        return found and isinstance(val, (int, float)) and val > rule["NumericGreaterThan"]
    if "NumericLessThan" in rule:
        return found and isinstance(val, (int, float)) and val < rule["NumericLessThan"]
    if "StringMatches" in rule:
        return found and isinstance(val, str)
    return False


def _set_path(data, path, value):
    parts = [p for p in path[1:].split(".") if p]
    cur = data
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value


def walk(sm, start, data, stop_at=None, max_steps=400):
    states = sm["States"]
    name = start
    visited = []
    for _ in range(max_steps):
        visited.append(name)
        if stop_at and name in stop_at:
            return name, visited
        st = states[name]
        t = st.get("Type")
        if t in ("Succeed", "Fail"):
            return name, visited
        if t == "Choice":
            nxt = None
            for rule in st.get("Choices", []):
                target = rule.get("Next")
                r = {k: v for k, v in rule.items() if k != "Next"}
                if _eval_rule(r, data):
                    nxt = target
                    break
            name = nxt or st.get("Default")
            if name is None:
                return "<no-match>", visited
            continue
        if t == "Pass":
            body = st.get("Parameters", st.get("Result"))
            rp = st.get("ResultPath", "$")
            if isinstance(body, dict) and rp and rp != "$":
                resolved = {}
                for k, v in body.items():
                    kk = k[:-2] if k.endswith(".$") else k
                    if k.endswith(".$") and isinstance(v, str) and v.startswith("$"):
                        _f, _val = _get_path(data, v)
                        resolved[kk] = _val
                    else:
                        resolved[kk] = v
                _set_path(data, rp, resolved)
            name = st.get("Next")
            if name is None:
                return "<pass-end>", visited
            continue
        if "Next" in st:
            name = st["Next"]
            continue
        if st.get("End"):
            return "<end>", visited
        return "<stuck:%s>" % name, visited
    return "<max-steps>", visited


def _startup():
    return json.load(open(os.path.join(SF_DIR, "startup.asl.json")))


def _cutover():
    return json.load(open(os.path.join(SF_DIR, "cutover.asl.json")))


# =============================================================================================
# STARTUP ROUTING
# =============================================================================================
def _startup_input(any_load_failed, any_validate_failed, override):
    return {
        "resolved": {"override": override, "taskSuffix": "t"},
        "groupCheck": {
            "anyLoadFailed": any_load_failed,
            "anyValidateFailed": any_validate_failed,
            "statuses": ["ok"],
        },
        "plan": {"groups": []},
        "overrideResolved": None,
    }


def test_startup_override_false_validate_failed_goes_groupsfailed():
    sm = _startup()
    data = _startup_input(False, True, False)
    term, _ = walk(sm, "AllGroupsSucceeded", data,
                   stop_at={"GroupsFailed", "ResumeDmsToCdc", "MarkOverrideActive",
                            "MarkOverrideInactive"})
    check(term == "GroupsFailed",
          f"startup override=false + validate_failed -> GroupsFailed (unchanged) [{term}]")


def test_startup_override_true_validate_failed_resumes_and_succeeds_with_override():
    sm = _startup()
    first, _ = walk(sm, "AllGroupsSucceeded", _startup_input(False, True, True),
                    stop_at={"MarkOverrideActive", "MarkOverrideInactive", "GroupsFailed"})
    check(first == "MarkOverrideActive",
          f"startup override=true + validate_failed -> MarkOverrideActive [{first}]")
    _term, visited = walk(sm, "MarkOverrideActive", _startup_input(False, True, True),
                          stop_at={"StartCdcJob"})
    check("ResumeDmsToCdc" in visited and _term == "StartCdcJob",
          f"override path resumes DMS then starts CDC [{_term}]")
    term3, _ = walk(sm, "OverrideTerminal", {"overrideResolved": {"active": True}},
                    stop_at={"TaskSucceeded", "TaskSucceededWithOverride",
                             "WriteStartupOverrideRecord"})
    check(term3 == "WriteStartupOverrideRecord",
          f"override-active terminal writes the record then ends WithOverride [{term3}]")
    end, _ = walk(sm, "WriteStartupOverrideRecord", {"overrideResolved": {"active": True}},
                  stop_at={"TaskSucceededWithOverride", "OverrideRecordFailed"})
    check(end == "TaskSucceededWithOverride",
          f"WriteStartupOverrideRecord -> TaskSucceededWithOverride [{end}]")


def test_startup_override_true_load_failed_continues():
    sm = _startup()
    # override is a FULL BYPASS now: a LOAD failure with override=true continues (MarkOverrideActive
    # -> ResumeDmsToCdc -> CDC -> TaskSucceededWithOverride), exactly like a validation failure.
    term, _ = walk(sm, "AllGroupsSucceeded", _startup_input(True, False, True),
                   stop_at={"GroupsFailed", "MarkOverrideActive", "MarkOverrideInactive"})
    check(term == "MarkOverrideActive",
          f"override=true + LOAD failed -> MarkOverrideActive (full bypass) [{term}]")
    # LOAD failure + validation failure together, override=true -> still a full bypass.
    term2, _ = walk(sm, "AllGroupsSucceeded", _startup_input(True, True, True),
                    stop_at={"GroupsFailed", "MarkOverrideActive", "MarkOverrideInactive"})
    check(term2 == "MarkOverrideActive",
          f"override=true + LOAD+validation failed -> MarkOverrideActive (full bypass) [{term2}]")
    # The override path resumes DMS then starts CDC (same as the validation-only override path).
    _term, visited = walk(sm, "MarkOverrideActive", _startup_input(True, False, True),
                          stop_at={"StartCdcJob"})
    check("ResumeDmsToCdc" in visited and _term == "StartCdcJob",
          f"override load-failure path resumes DMS then starts CDC [{_term}]")


def test_startup_no_override_load_or_validate_failed_stops_unchanged():
    sm = _startup()
    # No override: a LOAD failure stops at GroupsFailed (byte-identical to before).
    t_load, _ = walk(sm, "AllGroupsSucceeded", _startup_input(True, False, False),
                     stop_at={"GroupsFailed", "MarkOverrideActive", "MarkOverrideInactive"})
    check(t_load == "GroupsFailed",
          f"no override + LOAD failed -> GroupsFailed (unchanged) [{t_load}]")
    # No override: a VALIDATION failure stops at GroupsFailed (byte-identical to before).
    t_val, _ = walk(sm, "AllGroupsSucceeded", _startup_input(False, True, False),
                    stop_at={"GroupsFailed", "MarkOverrideActive", "MarkOverrideInactive"})
    check(t_val == "GroupsFailed",
          f"no override + validation failed -> GroupsFailed (unchanged) [{t_val}]")


def test_startup_no_failure_normal_terminal_unchanged():
    sm = _startup()
    first, _ = walk(sm, "AllGroupsSucceeded", _startup_input(False, False, False),
                    stop_at={"MarkOverrideActive", "MarkOverrideInactive", "GroupsFailed"})
    check(first == "MarkOverrideInactive",
          f"all groups ok -> MarkOverrideInactive -> normal path [{first}]")
    term, _ = walk(sm, "OverrideTerminal", {"overrideResolved": {"active": False}},
                   stop_at={"TaskSucceeded", "TaskSucceededWithOverride"})
    check(term == "TaskSucceeded",
          f"no-override terminal is the normal TaskSucceeded (byte-identical) [{term}]")


# =============================================================================================
# CUTOVER ROUTING
# =============================================================================================
def _cutover_data(override=False, startup_override_used=False, pre_ok=True, final_ok=True):
    return {
        "resolved": {"override": override,
                     "startupOverrideUsed": startup_override_used, "taskSuffix": "t"},
        "validationPre": {"ok": pre_ok, "failures": 0 if pre_ok else 3, "byTable": {}},
        "validationFinal": {"ok": final_ok, "failures": 0 if final_ok else 2, "byTable": {}},
    }


def test_cutover_startup_override_marker_proceeds_with_warning():
    sm = _cutover()
    # The refusal state is GONE: a startup override no longer forces cutover override.
    check("StartupOverrideRequiresOverride" not in sm["States"],
          "T5 UNDO: the StartupOverrideRequiresOverride refusal state is removed")
    # startup-override marker + NO cutover override -> PROCEED, via the warning Pass, to the
    # normal pre-DMS-stop validation gate (not a refusal).
    term, visited = walk(sm, "StartupOverrideGate",
                         _cutover_data(override=False, startup_override_used=True),
                         stop_at={"CdcValidationPreCheck"})
    check("LogStartupOverrideWarning" in visited and term == "CdcValidationPreCheck",
          f"T5 UNDO: startup-override marker + no cutover override -> warn then PROCEED "
          f"to CdcValidationPreCheck [{term}, visited={visited}]")
    warn_state = sm["States"]["LogStartupOverrideWarning"]
    check(warn_state["Type"] == "Pass" and warn_state["Next"] == "CdcValidationPreCheck",
          "T5 UNDO: LogStartupOverrideWarning is a Pass that continues to CdcValidationPreCheck")
    check("override" in warn_state["Parameters"]["warning"].lower()
          and "proceed" in warn_state["Parameters"]["warning"].lower(),
          "T5 UNDO: the warning says the cutover proceeds despite the startup override")
    # startup-override marker + cutover override=true -> straight to the gate (unchanged).
    term2, visited2 = walk(sm, "StartupOverrideGate",
                           _cutover_data(override=True, startup_override_used=True),
                           stop_at={"LogStartupOverrideWarning", "CdcValidationPreCheck"})
    check(term2 == "CdcValidationPreCheck" and "LogStartupOverrideWarning" not in visited2,
          f"T5 UNDO: startup-override marker + cutover override=true -> proceed, no warning "
          f"detour [{term2}]")
    # no startup override -> cutover proceeds (unchanged).
    term3, _ = walk(sm, "StartupOverrideGate",
                    _cutover_data(override=False, startup_override_used=False),
                    stop_at={"LogStartupOverrideWarning", "CdcValidationPreCheck"})
    check(term3 == "CdcValidationPreCheck",
          f"T5 UNDO: no startup override -> cutover proceeds (unchanged) [{term3}]")


def test_cutover_startup_override_warning_names_record_and_tables():
    """resolve_task (cutover mode) appends a warning naming the startup-override record and the
    overridden tables when a startup override is present but cutover has no override. The
    warning flows out in $.resolved.warnings (the cutover output)."""
    rt = _load_resolve_task()
    marker = {
        "startupOverride": True,
        "overriddenTables": ["sales.orders", "sales.customers"],
        "lastRecordKey": "config/_task/orders-cdc/_overrides/run-7.json",
    }
    w = rt._startup_override_cutover_warning("mybucket", "orders-cdc", marker)
    check("sales.orders" in w and "sales.customers" in w,
          f"cutover startup-override warning names the overridden tables [{w}]")
    check("config/_task/orders-cdc/_overrides/run-7.json" in w,
          "cutover startup-override warning names the startup-override record key")
    check("proceeding WITHOUT override" in w,
          "cutover startup-override warning says the cutover proceeds without override")
    # A marker with no table list / no record key still yields a usable warning (no crash).
    w2 = rt._startup_override_cutover_warning("mybucket", "t", {})
    check("(see record)" in w2 and "config/_task/t/_overrides/_startup_override.json" in w2,
          f"warning degrades gracefully with an empty marker [{w2}]")


def test_cutover_pre_validation_gate_bypass_and_refuse():
    sm = _cutover()
    term, _ = walk(sm, "CdcValidationPreGate", _cutover_data(override=False, pre_ok=False),
                   stop_at={"CdcValidationFailedPre", "DescribeBeforeStop",
                            "LogPreValidationOverride"})
    check(term == "CdcValidationFailedPre",
          f"pre-gate without override + failures -> CdcValidationFailedPre (refuse) [{term}]")
    term2, visited = walk(sm, "CdcValidationPreGate", _cutover_data(override=True, pre_ok=False),
                          stop_at={"CdcValidationFailedPre", "DescribeBeforeStop"})
    check(term2 == "DescribeBeforeStop" and "LogPreValidationOverride" in visited,
          f"pre-gate with override + failures -> logged bypass -> DescribeBeforeStop [{term2}]")


def test_cutover_final_validation_gate_bypass_and_refuse():
    sm = _cutover()
    term, _ = walk(sm, "CdcValidationFinalGate", _cutover_data(override=False, final_ok=False),
                   stop_at={"CdcValidationFailedFinal", "StopCdcRun",
                            "LogFinalValidationOverride"})
    check(term == "CdcValidationFailedFinal",
          f"final-gate without override + failures -> CdcValidationFailedFinal (refuse) [{term}]")
    term2, visited = walk(sm, "CdcValidationFinalGate", _cutover_data(override=True, final_ok=False),
                          stop_at={"CdcValidationFailedFinal", "StopCdcRun"})
    check(term2 == "StopCdcRun" and "LogFinalValidationOverride" in visited,
          f"final-gate with override + failures -> logged bypass -> StopCdcRun [{term2}]")


def test_cutover_safety_ordering_not_bypassed():
    sm = _cutover()
    S = sm["States"]
    check(S["StopCdcRun"]["Next"] == "ListForkCdcJobs", "StopCdcRun -> ListForkCdcJobs (unchanged)")
    check(S["StopForkCdcRuns"]["Next"] == "DropTags", "StopForkCdcRuns -> DropTags (unchanged)")
    check(S["DropTags"]["Next"] == "InitDeleteLoop", "DropTags -> delete loop (unchanged)")
    check(S["LogFinalValidationOverride"]["Next"] == "StopCdcRun",
          "final override bypass still stops the CDC run before deleting jobs")
    t_over, _ = walk(sm, "CutoverOverrideTerminal", _cutover_data(override=True),
                     stop_at={"WriteCutoverOverrideRecord", "CutoverSucceeded"})
    check(t_over == "WriteCutoverOverrideRecord",
          f"cutover override terminal writes the record [{t_over}]")
    t_norm, _ = walk(sm, "CutoverOverrideTerminal", _cutover_data(override=False),
                     stop_at={"WriteCutoverOverrideRecord", "CutoverSucceeded"})
    check(t_norm == "CutoverSucceeded",
          f"cutover without override ends in plain CutoverSucceeded [{t_norm}]")


# =============================================================================================
# resolve_task: normalization + override record + startup marker
# =============================================================================================
def _load_resolve_task():
    sys.path.insert(0, LAMBDAS)
    _b = types.ModuleType("boto3")
    _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    return rt


def test_normalize_override_accepts_bool_and_strings():
    rt = _load_resolve_task()
    cases = [
        ({"override": True}, True), ({"override": "true"}, True), ({"override": "TRUE"}, True),
        ({"override": "1"}, True), ({"override": "yes"}, True), ({"override": 1}, True),
        ({}, False), ({"override": False}, False), ({"override": "false"}, False),
        ({"override": "0"}, False), ({"override": "no"}, False), ({"override": 0}, False),
    ]
    ok = all(rt._normalize_override(inp) is exp for inp, exp in cases)
    check(ok, "override normalizes bool true + 'true'/'1'/'yes'/1 (any case); else false")
    check(rt._normalize_override({"override": True}) is True
          and rt._normalize_override({}) is False,
          "_normalize_override returns a plain bool (no reason tuple)")


class _FakeS3Rec:
    def __init__(self, preset=None):
        self.puts = {}
        # preset: {key: json-serializable} returned by get_object; absent keys raise NoSuchKey.
        self._preset = {k: json.dumps(v).encode("utf-8") for k, v in (preset or {}).items()}

    def put_object(self, Bucket=None, Key=None, Body=None, **k):
        self.puts[Key] = Body

    def get_object(self, Bucket=None, Key=None):
        if Key in self._preset:
            import io
            return {"Body": io.BytesIO(self._preset[Key])}
        raise Exception("NoSuchKey")


def test_write_override_record_writes_record_and_startup_marker():
    rt = _load_resolve_task()
    # Group 1 validate_failed; group 2 LOAD failed with one data_error table and one infra table.
    g2_status_key = "config/_task/orders-cdc/g2/_load_status.json"
    s3 = _FakeS3Rec(preset={
        g2_status_key: {"tables": {
            "s.d": {"status": "failed", "kind": "data_error",
                    "data_error": {"table": "s.d", "column": "id", "file": "LOAD001.csv",
                                   "reason": "invalid input syntax for type uuid"}},
            "s.e": {"status": "failed", "kind": "infra", "error": "ENI capacity exceeded"},
        }},
    })
    rt.boto3.client = lambda *a, **k: s3
    event = {
        "mode": "write_override_record", "bucket": "b", "taskSuffix": "orders-cdc",
        "workflow": "startup",
        "execution": "arn:aws:states:us-east-1:111:execution:p-startup:run-7",
        "executionName": "run-7",
        "planGroups": [
            {"group_index": 0, "config_prefix": "s3://b/config/_task/orders-cdc/g0", "tables": ["s.a"]},
            {"group_index": 1, "config_prefix": "s3://b/config/_task/orders-cdc/g1", "tables": ["s.b", "s.c"]},
            {"group_index": 2, "config_prefix": "s3://b/config/_task/orders-cdc/g2", "tables": ["s.d", "s.e"]},
        ],
        "groupStatuses": ["ok", "validate_failed", "failed"],
    }
    out = rt.handler_write_override_record(event, None)
    check(out["overriddenGroups"] == [1, 2],
          f"record lists BOTH the validate_failed and the load-failed group [{out['overriddenGroups']}]")
    check(out["overriddenTables"] == ["s.b", "s.c", "s.d", "s.e"],
          f"record lists every overridden group's tables [{out['overriddenTables']}]")
    check(out["validationReportPaths"] == ["s3://b/config/_task/orders-cdc/g1/_validation_report.json"],
          "record lists the validate_failed group's validation report path")
    # Full-bypass warnings: a per-table load-failure warning naming the kind + detail.
    wtext = " || ".join(out["warnings"])
    check(any("data_error" in w and "s.d" in w for w in out["warnings"]),
          f"override record warns about the data_error load failure, naming the table [{wtext}]")
    check(any("invalid input syntax for type uuid" in w for w in out["warnings"]),
          "the data_error warning carries the recorded reason")
    check(any("infra" in w and "s.e" in w for w in out["warnings"]),
          f"override record warns about the infra load failure, naming the table [{wtext}]")
    rec_key = "config/_task/orders-cdc/_overrides/run-7.json"
    marker_key = "config/_task/orders-cdc/_overrides/_startup_override.json"
    check(rec_key in s3.puts, "per-execution override record written under _overrides/<exec>.json")
    check(marker_key in s3.puts and out["markerWritten"] is True,
          "stable startup-override marker written (startup workflow)")
    rec = json.loads(s3.puts[rec_key])
    check(rec["who"] == event["execution"] and rec["workflow"] == "startup",
          "record carries who (execution ARN), when, workflow")
    check(rec.get("warnings") == out["warnings"] and len(rec["warnings"]) >= 3,
          "the override record persists the full-bypass warnings")
    marker = json.loads(s3.puts[marker_key])
    check("reason" not in rec and "reason" not in marker and "reason" not in out,
          "override record / marker / return carry NO reason field")


def test_write_override_record_cutover_no_startup_marker():
    rt = _load_resolve_task()
    s3 = _FakeS3Rec()
    rt.boto3.client = lambda *a, **k: s3
    out = rt.handler_write_override_record(
        {"mode": "write_override_record", "bucket": "b", "taskSuffix": "t",
         "workflow": "cutover", "execution": "arn:x:execution:p-cutover:c1",
         "executionName": "c1"}, None)
    marker_key = "config/_task/t/_overrides/_startup_override.json"
    check(out["markerWritten"] is False and marker_key not in s3.puts,
          "cutover override record does NOT write the startup-override marker")
    check("config/_task/t/_overrides/c1.json" in s3.puts,
          "cutover override still writes a per-execution record")


# =============================================================================================
# job2_load: a done table is NOT reloaded on an override re-run (unconditional skip)
# =============================================================================================
def test_job2_load_skips_done_tables_regardless_of_override():
    src = open(os.path.join(SCRIPTS, "job2_load.py")).read()
    check(re.search(r'done_before\s*=\s*\{[^}]*status.*==\s*["\']done["\']', src),
          "job2_load derives done_before from _load_status.json tables marked status==done")
    check("if label in done_before:" in src and "skipped_done.append(label)" in src,
          "a done table is skipped (skipped_done), not added to the load worklist")
    # The done-skip must not depend on the RUNTIME override flag: job2_load takes no --override
    # Glue argument and does not read any 'startup override' value, so an override re-run skips
    # done tables exactly as a normal re-run does (the skip is unconditional on override).
    check("--override" not in src and "startup_override" not in src.lower(),
          "job2_load has no --override arg / startup-override read -> the done-skip is "
          "independent of the runtime override (override re-run reloads nothing already done)")
    # And the skip block itself has no inline override condition.
    blk = src[src.index("if label in done_before:"): src.index("if label in done_before:") + 400]
    check("override" not in blk.lower(),
          "the done-skip block has no override branch (unconditional skip)")


def _load_classify_load_error():
    """AST-extract job2_load.classify_load_error and exec it offline (no pyspark/Glue)."""
    import ast
    src = open(os.path.join(SCRIPTS, "job2_load.py")).read()
    tree = ast.parse(src)
    seg = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "classify_load_error":
            seg = ast.get_source_segment(src, node)
            break
    if seg is None:
        raise AssertionError("classify_load_error not found in job2_load.py")
    ns = {"re": re}
    exec(compile(seg, "<job2:classify_load_error>", "exec"), ns)
    return ns["classify_load_error"]


def test_job2_classify_load_error_data_vs_infra():
    classify = _load_classify_load_error()
    # Every DATA error kind the spec lists classifies as data_error (recording only).
    data_cases = {
        "row-split": "ROW-SPLIT GUARD (D1 expected-count) [s.t]: Spark parsed 11 rows but ...",
        "bad uuid (guard)": "UUID GUARD [s.t]: column 'id' holds a non-uuid value 'xyz' ...",
        "bad cast (22P02)": 'ERROR: invalid input syntax for type uuid: "nope" (SQLSTATE 22P02)',
        "not null (23502)": "null value in column \"name\" violates not-null constraint (23502)",
        "too long (22001)": "ERROR: value too long for type character varying(10) (SQLSTATE 22001)",
        "length guard": "LENGTH GUARD [s.t]: column 'descr' value length 300 exceeds target varchar(255)",
        "dup pk (23505)": "duplicate key value violates unique constraint \"t_pkey\" (SQLSTATE 23505)",
        "malformed csv": "malformed CSV: embedded newline caused column misalignment in LOAD003.csv",
    }
    for name, err in data_cases.items():
        out = classify(err, "s.t")
        check(out.get("kind") == "data_error",
              f"classify: {name} -> data_error (got {out.get('kind')!r})")
        check(isinstance(out.get("data_error"), dict) and out["data_error"].get("reason"),
              f"classify: {name} data_error carries a reason")
    # column/file best-effort extraction.
    c = classify("UUID GUARD [s.t]: column 'id' is not a uuid; file LOAD001.csv", "s.t")
    check(c["data_error"].get("column") == "id", "classify extracts the column name")
    check(c["data_error"].get("file") == "LOAD001.csv", "classify extracts the CSV file name")
    # INFRA / unknown kinds -> infra.
    infra_cases = [
        "Could not connect to Aurora DSQL on any candidate hostname [...]",
        "ENI capacity exceeded; no IP addresses available",
        "worker crashed: OutOfMemoryError",
        "Glue job timed out after 2880 minutes",
        "some totally unrecognized failure with no signature",
    ]
    for err in infra_cases:
        out = classify(err, "s.t")
        check(out.get("kind") == "infra", f"classify: infra/unknown -> infra [{err[:40]}...]")
        check("data_error" not in out, "classify: infra rows carry no data_error block")


# =============================================================================================
# fleet preflight: fleet-level + per-task override -> child input
# =============================================================================================
def _load_preflight():
    sys.path.insert(0, LAMBDAS)
    _b = types.ModuleType("boto3")
    _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    pf = importlib.import_module("preflight_tasks")
    importlib.reload(pf)
    return pf


def test_preflight_fleet_and_per_task_override_source_wiring():
    pf = _load_preflight()
    src = open(os.path.join(LAMBDAS, "preflight_tasks.py")).read()
    check("fleet_override = rt._normalize_override(fleet_input)" in src,
          "preflight reads a fleet-level override from the fleet start input")
    check('"override": row.get("override")' in src,
          "preflight reads a per-task 'override' CSV column")
    check("if fleet_override or row_override:" in src and 'child_input["override"] = True' in src,
          "fleet-level OR per-task override adds {'override': true} to the child input")
    _camel = "override" + "Reason"
    _snake = "override" + "_reason"
    check(_camel not in src and _snake not in src,
          "preflight has no reason-field handling")
    on = pf.rt._normalize_override({"override": ""})
    check(on is False, "a blank CSV override cell is false (adds nothing)")


def test_fleet_tasks_example_has_override_column():
    import csv
    path = os.path.join(REPO, "config", "fleet_tasks.example.csv")
    with open(path) as fh:
        reader = csv.DictReader(fh)
        cols = reader.fieldnames
        rows = list(reader)
    check("override" in cols, f"fleet_tasks.example.csv header has an 'override' column [{cols}]")
    check(any(str(r.get("override") or "").strip().lower() == "true" for r in rows),
          "the example shows at least one row with override=true")
    check(any(not str(r.get("override") or "").strip() for r in rows),
          "the example shows blank override cells (blank = false)")


def main():
    for fn in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== override tests: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
