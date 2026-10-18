"""DATA-FLOW contract test (the check that would have caught the live-smoke HIGH defect S1).

The existing payload-contract test (test_asl_payload_contract.py) checks ONE direction: every
`$.Payload.<field>` a ResultSelector READS is a field the invoked Lambda RETURNS. It could NOT
catch S1, where `resolve_task` returned `stopAfterLoad` and the ASL READ `$.resolved.stopAfterLoad`
but the ResolveTask ResultSelector did not PRODUCE it (so the key was silently absent and the
IsPresent-guarded Choice always took the default branch).

This test checks the MISSING direction, across ALL FOUR state machines:

  For every Task state that has a ResultSelector AND writes its result to `ResultPath: $.<alias>`,
  collect the set of keys the ResultSelector PRODUCES (the `"<key>.$"` / `"<key>"` entries). Then
  for every `$.<alias>.<field>` read ANYWHERE in that state machine (including reads inside an
  `IsPresent` guard — the guard is exactly what hid S1), assert `<field>` is produced by that
  ResultSelector. A read of a field the ResultSelector never emits is a latent `States.Runtime`
  (or, behind an IsPresent guard, a SILENT wrong-branch) — the S1 class of bug.

Mutation: removing `stopAfterLoad.$` from the startup ResolveTask ResultSelector makes this test
fail (asserted explicitly).
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SF = os.path.join(REPO, "stepfunctions")
MACHINES = ["startup.asl.json", "cutover.asl.json", "fleet-startup.asl.json", "fleet-cutover.asl.json"]


def _load(name):
    return json.load(open(os.path.join(SF, name)))


def _iter_states(node):
    """Yield every state dict in a definition, descending into Map Iterator/ItemProcessor."""
    if isinstance(node, dict):
        states = node.get("States")
        if isinstance(states, dict):
            for st in states.values():
                yield st
                yield from _iter_states(st)
        for k in ("Iterator", "ItemProcessor"):
            if isinstance(node.get(k), dict):
                yield from _iter_states(node[k])


def _result_selector_keys(rs):
    out = set()
    for k in rs:
        out.add(k[:-2] if k.endswith(".$") else k)
    return out


def _alias_producers(defn):
    """Map ResultPath-alias -> set of keys produced, for every Task state with a ResultSelector
    that writes to `$.<alias>`. (A $ or $.x.y ResultPath that isn't a single `$.alias` is skipped;
    those don't create a `$.alias.<field>` namespace we validate here.)"""
    producers = {}
    for st in _iter_states(defn):
        if not isinstance(st, dict):
            continue
        rs = st.get("ResultSelector")
        rp = st.get("ResultPath")
        if isinstance(rs, dict) and isinstance(rp, str):
            m = re.fullmatch(r"\$\.([A-Za-z_][A-Za-z0-9_]*)", rp)
            if m:
                producers.setdefault(m.group(1), set()).update(_result_selector_keys(rs))
    return producers


def _reads(defn_text, alias):
    return set(re.findall(r"\$\." + re.escape(alias) + r"\.([A-Za-z_][A-Za-z0-9_]*)", defn_text))


def _check_machine(name):
    defn = _load(name)
    text = json.dumps(defn)
    producers = _alias_producers(defn)
    problems = []
    for alias, produced in producers.items():
        for field in sorted(_reads(text, alias)):
            if field not in produced:
                problems.append(f"{name}: reads $.{alias}.{field} but the ResultSelector that "
                                f"writes $.{alias} does NOT produce {field!r}")
    return problems


def test_every_resolved_read_is_produced_by_its_result_selector():
    all_problems = []
    for name in MACHINES:
        all_problems += _check_machine(name)
    assert not all_problems, "data-flow contract violations:\n  - " + "\n  - ".join(all_problems)


def test_startup_resolved_stopafterload_specifically_produced():
    """Pin the exact S1 fix: startup ResolveTask ResultSelector produces stopAfterLoad and the ASL
    reads $.resolved.stopAfterLoad."""
    d = _load("startup.asl.json")
    rs = d["States"]["ResolveTask"]["ResultSelector"]
    assert "stopAfterLoad.$" in rs and rs["stopAfterLoad.$"] == "$.Payload.stopAfterLoad"
    assert "stopAfterLoad" in _reads(json.dumps(d), "resolved")


def test_mutation_removing_stopafterload_from_resultselector_fails():
    """If stopAfterLoad.$ is removed from the startup ResolveTask ResultSelector, the data-flow
    check MUST flag the $.resolved.stopAfterLoad reads (proves the test catches S1)."""
    d = _load("startup.asl.json")
    del d["States"]["ResolveTask"]["ResultSelector"]["stopAfterLoad.$"]
    text = json.dumps(d)
    producers = _alias_producers(d)
    bad = [f for f in _reads(text, "resolved") if f not in producers.get("resolved", set())]
    assert "stopAfterLoad" in bad, "mutation should make stopAfterLoad an unproduced read"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
