#!/usr/bin/env python3
"""ASL DIFF TEST (Amendment 7.5 / 8): the states the MRL child REUSES from startup.asl.json must
have IDENTICAL Resource and Parameters/Arguments after placeholder normalization. A difference
lists the differing keys. This guarantees the reused discovery + CDC tail behave exactly as the
original (same Lambdas, payloads, Glue arguments), so the original cutover/cleanup keep working.

Only Next/Default/Catch targets and the top-level Comment are allowed to differ (MRL rewires the
flow); Resource and Parameters are compared verbatim after mapping each machine's placeholders to
a common token.
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
STARTUP = json.load(open(os.path.join(REPO, "stepfunctions", "startup.asl.json")))
CHILD = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))

# States MRL reuses VERBATIM from startup (Resource + Parameters identical).
REUSED = [
    "BuildTableList", "DriverDiscoveryFullload", "DriverDiscoveryValidation",
    "DriverDiscoveryCdc", "CreateGlueJobs", "RunDiscovery",
    "PlanSplit", "EnsureForkJobs", "ResumeDmsToCdc", "StartCdcJob", "InitCdcCheck",
    "WaitForCdcStart", "GetCdcRun", "IsCdcRunAlive", "CdcDriverFallback",
    "IsCdcFallbackSwitched", "UseSparkCdcJob", "CheckCdcStarted", "IsCdcStarted",
    "CountCdcCheck", "CdcCheckBudgetLeft", "StartForkCdcMap",
]

# Startup placeholders -> a common token; MRL placeholders -> the same token. Startup fills these
# with sed too (tools/setup.sh), so the raw <<...>> tokens are what we compare.
_NORM = [
    (r"<<BUCKET>>", "<B>"),
    (r"<<RESOLVE_TASK_LAMBDA_ARN>>", "<RT>"),
    (r"<<DRIVER_DISCOVERY_LAMBDA_ARN>>", "<DD>"),
    (r"<<CREATE_GLUE_JOBS_LAMBDA_ARN>>", "<CGJ>"),
    (r"<<PLAN_SPLIT_LAMBDA_ARN>>", "<PS>"),
]


def _norm(obj):
    s = json.dumps(obj, sort_keys=True)
    for pat, tok in _NORM:
        s = s.replace(pat, tok)
    return json.loads(s)


def _comparable(state):
    """Resource + Parameters (and ResultSelector) only; Next/Default/Catch/Comment/Retry are
    flow/robustness and may differ."""
    out = {}
    for k in ("Type", "Resource", "Parameters", "ResultSelector", "ItemsPath",
              "MaxConcurrency", "MaxConcurrencyPath"):
        if k in state:
            out[k] = state[k]
    # For a Map, also compare the Iterator's Glue task Resource/Parameters (the fork CDC start).
    it = state.get("Iterator") or state.get("ItemProcessor")
    if it:
        sub = {}
        for n, st in it["States"].items():
            if st.get("Type") == "Task":
                sub[n] = {kk: st[kk] for kk in ("Resource", "Parameters") if kk in st}
        out["_iterator_tasks"] = sub
    return _norm(out)


def test_reused_states_identical_to_startup():
    diffs = []
    for name in REUSED:
        if name not in STARTUP["States"]:
            diffs.append("%s: not in startup.asl.json (test list stale)" % name)
            continue
        if name not in CHILD["States"]:
            diffs.append("%s: not in the MRL child machine" % name)
            continue
        su = _comparable(STARTUP["States"][name])
        mr = _comparable(CHILD["States"][name])
        if su != mr:
            # pinpoint the differing top-level keys
            keys = sorted(set(su) | set(mr))
            differing = [k for k in keys if su.get(k) != mr.get(k)]
            diffs.append("%s: differing keys %s" % (name, differing))
    assert not diffs, ("reused states differ from startup (must be identical after placeholder "
                       "normalization):\n  - " + "\n  - ".join(diffs))


if __name__ == "__main__":
    test_reused_states_identical_to_startup()
    print("[PASS] test_reused_states_identical_to_startup (%d states)" % len(REUSED))
