#!/usr/bin/env python3
"""Amendment 14.2 - a STRICT offline ASL schema validator that fails on what Step Functions would
reject (the A13 `"TimeoutSeconds.$"` bug was green in 180 tests because nothing validated the
definition). Covers every *.asl.json in stepfunctions/.

Checks per state Type: only allowed field names; a key ending in `.$` is valid ONLY inside a
payload template (Parameters / ResultSelector / ItemSelector / Credentials / Result / Arguments),
NEVER as a direct state field and NEVER inside Retry/Catch/Choice rules; mutually-exclusive pairs
(TimeoutSeconds vs TimeoutSecondsPath, HeartbeatSeconds vs *Path, Seconds vs SecondsPath, Timestamp
vs *Path, MaxConcurrency vs MaxConcurrencyPath, Error vs ErrorPath, Cause vs CausePath) not both
present; exactly one StartAt that exists; every Next/Default/Catch.Next/Choice.Next target exists;
every non-terminal state has Next or End; intrinsic functions used are in the allowed set.
"""
import glob
import json
import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
SF_DIR = os.path.join(PKG, "stepfunctions")

# Allowed TOP-LEVEL field names per state Type (superset of what we use; extra standard fields ok).
_COMMON = {"Type", "Comment", "InputPath", "OutputPath", "QueryLanguage", "Assign"}
_ALLOWED = {
    "Task": _COMMON | {"Resource", "Parameters", "Arguments", "Credentials", "ResultPath",
                       "ResultSelector", "Retry", "Catch", "TimeoutSeconds", "TimeoutSecondsPath",
                       "HeartbeatSeconds", "HeartbeatSecondsPath", "Next", "End", "Output"},
    "Choice": _COMMON | {"Choices", "Default"},
    "Pass": _COMMON | {"Result", "ResultPath", "Parameters", "Next", "End", "Output"},
    "Wait": _COMMON | {"Seconds", "SecondsPath", "Timestamp", "TimestampPath", "Next", "End"},
    "Map": _COMMON | {"ItemsPath", "Items", "ItemProcessor", "Iterator", "Parameters",
                      "ItemSelector", "ItemReader", "ResultWriter", "MaxConcurrency",
                      "MaxConcurrencyPath", "ResultPath", "ResultSelector", "Retry", "Catch",
                      "ToleratedFailurePercentage", "ToleratedFailureCount",
                      "ToleratedFailureCountPath", "ToleratedFailurePercentagePath",
                      "ItemBatcher", "Label", "Next", "End", "Output"},
    "Parallel": _COMMON | {"Branches", "Parameters", "ResultPath", "ResultSelector", "Retry",
                           "Catch", "Next", "End", "Output"},
    "Succeed": _COMMON,
    "Fail": _COMMON | {"Error", "ErrorPath", "Cause", "CausePath"},
}
_EXCLUSIVE = [("TimeoutSeconds", "TimeoutSecondsPath"), ("HeartbeatSeconds", "HeartbeatSecondsPath"),
              ("Seconds", "SecondsPath"), ("Timestamp", "TimestampPath"),
              ("MaxConcurrency", "MaxConcurrencyPath"), ("Error", "ErrorPath"),
              ("Cause", "CausePath")]
# intrinsic functions we allow (used anywhere a value template is evaluated)
_INTRINSICS = {"States.Format", "States.StringToJson", "States.JsonToString", "States.Array",
               "States.ArrayPartition", "States.ArrayContains", "States.ArrayRange",
               "States.ArrayGetItem", "States.ArrayLength", "States.ArrayUnique", "States.Base64Encode",
               "States.Base64Decode", "States.Hash", "States.JsonMerge", "States.MathRandom",
               "States.MathAdd", "States.StringSplit", "States.UUID"}
_INTRINSIC_RE = re.compile(r"States\.[A-Za-z]+")


def _asl_files():
    return sorted(glob.glob(os.path.join(SF_DIR, "*.asl.json")))


def _iter_all_states(d):
    """Yield (qualified_name, state_dict) for top-level + nested Map/Parallel states."""
    def rec(states, prefix=""):
        for name, st in states.items():
            yield prefix + name, st
            sub = st.get("Iterator") or st.get("ItemProcessor")
            if isinstance(sub, dict) and "States" in sub:
                yield from rec(sub["States"], prefix + name + "/")
            for br in st.get("Branches", []) or []:
                if isinstance(br, dict) and "States" in br:
                    yield from rec(br["States"], prefix + name + "/branch/")
    yield from rec(d["States"])


# payload-template keys (where `.$` and intrinsics ARE allowed)
_TEMPLATE_FIELDS = {"Parameters", "ResultSelector", "ItemSelector", "Credentials", "Result",
                    "Arguments", "Output", "Assign"}


def _check_dollar_and_intrinsics(path, obj, problems, in_template):
    """Recurse a value. `.$` keys only allowed when in_template. Intrinsics must be known."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith(".$"):
                if not in_template:
                    problems.append("%s: key %r uses `.$` outside a payload template" % (path, k))
                if isinstance(v, str):
                    for m in _INTRINSIC_RE.findall(v):
                        if m not in _INTRINSICS:
                            problems.append("%s: unknown intrinsic %s in %r" % (path, m, k))
            # recurse; entering a template field flips in_template on
            _check_dollar_and_intrinsics(path + "." + k, v,
                                         problems, in_template or k in _TEMPLATE_FIELDS)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _check_dollar_and_intrinsics("%s[%d]" % (path, i), v, problems, in_template)


def _validate(fp):
    d = json.load(open(fp))
    problems = []
    states = d.get("States", {})
    assert "StartAt" in d, "%s missing StartAt" % fp
    # StartAt must be a top-level state
    if d["StartAt"] not in states:
        problems.append("StartAt %r is not a state" % d["StartAt"])
    for qname, st in _iter_all_states(d):
        t = st.get("Type")
        if t not in _ALLOWED:
            problems.append("%s: unknown Type %r" % (qname, t))
            continue
        # 1. only allowed top-level fields
        extra = set(st) - _ALLOWED[t]
        # a direct state field ending in `.$` is ALWAYS illegal (this is the A13 bug)
        for k in st:
            if k.endswith(".$"):
                problems.append("%s: state-level field %r uses `.$` (not allowed; use the "
                                "*Path variant)" % (qname, k))
        extra = {e for e in extra if not e.endswith(".$")}
        if extra:
            problems.append("%s (%s): unsupported field(s) %s" % (qname, t, sorted(extra)))
        # 2. mutually-exclusive pairs
        for a, b in _EXCLUSIVE:
            if a in st and b in st:
                problems.append("%s: both %s and %s present (mutually exclusive)" % (qname, a, b))
        # 3. non-terminal states need Next or End (Choice uses Default; Succeed/Fail terminal)
        if t in ("Task", "Pass", "Wait", "Map", "Parallel"):
            if "Next" not in st and st.get("End") is not True:
                problems.append("%s (%s): has neither Next nor End:true" % (qname, t))
        # 4. `.$` must not appear inside Retry/Catch rule objects (not payload templates)
        for rc in ("Retry", "Catch"):
            for i, rule in enumerate(st.get(rc, []) or []):
                for k in rule:
                    if k.endswith(".$"):
                        problems.append("%s: %s[%d] key %r uses `.$` (not a payload template)"
                                        % (qname, rc, i, k))
        # 5. Choice rules: no `.$` keys; the Variable path is a plain string
        for i, ch in enumerate(st.get("Choices", []) or []):
            for k in ch:
                if k.endswith(".$"):
                    problems.append("%s: Choices[%d] key %r uses `.$`" % (qname, i, k))
        # 6. recurse payload templates for `.$`/intrinsic correctness
        for f in _TEMPLATE_FIELDS:
            if f in st:
                _check_dollar_and_intrinsics("%s.%s" % (qname, f), st[f], problems, True)
    # 7. every transition target exists among TOP-LEVEL states (Next/Default/Catch.Next), and
    #    nested Map/Parallel targets exist within their own sub-states.
    def targets_ok(states_map, scope):
        names = set(states_map)
        for nm, st in states_map.items():
            outs = []
            if "Next" in st:
                outs.append(st["Next"])
            if "Default" in st:
                outs.append(st["Default"])
            for ch in st.get("Choices", []) or []:
                if "Next" in ch:
                    outs.append(ch["Next"])
            for c in st.get("Catch", []) or []:
                if "Next" in c:
                    outs.append(c["Next"])
            for tgt in outs:
                if tgt not in names:
                    problems.append("%s%s: transition target %r is not a state in this scope"
                                    % (scope, nm, tgt))
            sub = st.get("Iterator") or st.get("ItemProcessor")
            if isinstance(sub, dict) and "States" in sub:
                assert sub.get("StartAt") in sub["States"], \
                    "%s%s iterator StartAt missing" % (scope, nm)
                targets_ok(sub["States"], scope + nm + "/")
    targets_ok(states, "")
    return problems


@pytest.mark.parametrize("fp", _asl_files())
def test_asl_schema_valid(fp):
    problems = _validate(fp)
    assert not problems, "ASL schema problems in %s:\n  - %s" % (
        os.path.basename(fp), "\n  - ".join(problems))


def test_timeoutsecondspath_used_not_dollar():
    d = json.load(open(os.path.join(SF_DIR, "mrl-task.asl.json")))
    S = d["States"]
    for st in ("SafetyProbe", "CountVerify"):
        assert "TimeoutSeconds.$" not in S[st]
        assert S[st]["TimeoutSecondsPath"].startswith("$.resolved.")
    # the referenced values are emitted as NUMBERS by the preflight ResultSelector passthrough
    rs = S["Preflight"]["ResultSelector"]
    assert "probeTimeoutSeconds.$" in rs and "countTimeoutSeconds.$" in rs


def test_mutation_timeoutseconds_dollar_is_rejected():
    """Re-introduce the A13 bug in a copy and assert the validator flags it."""
    d = json.load(open(os.path.join(SF_DIR, "mrl-task.asl.json")))
    import copy
    d2 = copy.deepcopy(d)
    sp = d2["States"]["SafetyProbe"]
    sp["TimeoutSeconds.$"] = sp.pop("TimeoutSecondsPath")
    tmp = os.path.join(HERE, "_mutant.asl.json")
    open(tmp, "w").write(json.dumps(d2))
    try:
        problems = _validate(tmp)
        assert any("`.$`" in p and "SafetyProbe" in p for p in problems), problems
    finally:
        os.remove(tmp)


if __name__ == "__main__":
    for fp in _asl_files():
        probs = _validate(fp)
        print("[%s] %s" % ("PASS" if not probs else "FAIL", os.path.basename(fp)))
        for p in probs:
            print("   -", p)
