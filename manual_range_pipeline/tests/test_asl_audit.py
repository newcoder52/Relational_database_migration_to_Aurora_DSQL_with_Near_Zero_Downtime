#!/usr/bin/env python3
"""ASL audits for the MRL state machines (brief section 10):
  * static path audit (no unguarded read of a path a predecessor route may not produce),
  * reachability (every state reachable; every Next/Default/Catch/Choice target exists;
    every non-terminal state has an onward transition),
  * Choice exhaustiveness (every Choice has a Default OR is provably exhaustive - we require a
    Default, the safest option).

Reuses the REPO's audit engine (tests/test_asl_paths.py analyze_scope / reachability) so the
MRL machines are held to the SAME standard as the production ones, but points it at the MRL
stepfunctions/ and supplies the MRL top-level input roots.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))            # .../repo
MRL_SF = os.path.join(os.path.dirname(HERE), "stepfunctions")
sys.path.insert(0, os.path.join(REPO, "tests"))

import test_asl_paths as ap  # noqa: E402

# MRL child input roots (the whole input is passed to Preflight via raw.$=$, and the fleet /
# hand-start provide these top-level keys). Mark them OPEN so reads under them are allowed.
_MRL_TOP_ROOTS = {
    ("$", "input"), ("$", "mode"), ("$", "dmsStartMode"), ("$", "bucket"),
    ("$", "maxConcurrency"), ("$", "loadWorkerType"), ("$", "loadWorkers"),
    ("$", "maxFilesInParallel"), ("$", "writersPerFile"), ("$", "splitBy"),
    ("$", "splitAllowHex"), ("$", "includeTables"), ("$", "countTolerance"),
    ("$", "countTimeoutAction"), ("$", "writeMergedStatus"), ("$", "connBudget"),
    ("$", "nullValues"), ("$", "nullRules"), ("$", "nullNotnullText"),
}
_FLEET_TOP_ROOTS = {("$", "bucket"), ("$", "tasksFile"), ("$", "paramsFile")}


def _analyze(sm_path, top_roots):
    sm = json.load(open(sm_path))
    name = os.path.basename(sm_path)
    scopes = [(sm["States"], sm["StartAt"], set(top_roots), name)]

    def collect_maps(states, lbl):
        for sname, st in states.items():
            if st.get("Type") == "Map":
                it = st.get("Iterator") or st.get("ItemProcessor")
                if it:
                    roots = set()
                    for k in (st.get("Parameters") or {}):
                        kk = k[:-2] if k.endswith(".$") else k
                        roots.add(("$", kk))
                    scopes.append((it["States"], it["StartAt"], roots,
                                   "%s::%s.Iterator" % (lbl, sname)))
                    collect_maps(it["States"], "%s::%s.Iterator" % (lbl, sname))
    collect_maps(sm["States"], name)
    gaps = []
    for states, start, roots, lbl in scopes:
        gaps += ap.analyze_scope(states, start, roots, lbl)
    return gaps


def _choices_have_default(sm_path):
    sm = json.load(open(sm_path))
    problems = []

    def walk(states, lbl):
        for sname, st in states.items():
            if st.get("Type") == "Choice" and "Default" not in st:
                problems.append("[%s] Choice %s has no Default (MRL requires an explicit "
                                "Default on every Choice)." % (lbl, sname))
            if st.get("Type") == "Map":
                it = st.get("Iterator") or st.get("ItemProcessor")
                if it:
                    walk(it["States"], "%s::%s.Iterator" % (lbl, sname))
    walk(sm["States"], os.path.basename(sm_path))
    return problems


def _machines():
    return [os.path.join(MRL_SF, f) for f in sorted(os.listdir(MRL_SF))
            if f.endswith(".asl.json")]


def test_path_audit():
    gaps = []
    for p in _machines():
        roots = _FLEET_TOP_ROOTS if "fleet" in os.path.basename(p) else _MRL_TOP_ROOTS
        gaps += _analyze(p, roots)
    assert not gaps, "ASL path audit gaps:\n  - " + "\n  - ".join(gaps)


def test_reachability():
    probs = []
    for p in _machines():
        probs += ap.reachability(p)
    assert not probs, "ASL reachability problems:\n  - " + "\n  - ".join(probs)


def test_choices_have_default():
    probs = []
    for p in _machines():
        probs += _choices_have_default(p)
    assert not probs, "Choice-without-Default:\n  - " + "\n  - ".join(probs)


if __name__ == "__main__":
    for fn in (test_path_audit, test_reachability, test_choices_have_default):
        fn()
        print("[PASS]", fn.__name__)
    print("\nASL audits passed for:", [os.path.basename(p) for p in _machines()])
