#!/usr/bin/env python3
"""Static ASL path audit for every Step Functions state machine in stepfunctions/.

Finding M01 and its whole class: a Choice `Variable`, a Task/Pass `Parameters` `<name>.$`
JSONPath, or a Map `ItemsPath` that reads `$.<something>` must be PRODUCED on *every* route
that can reach that state, or (for a Choice comparison) be IsPresent/IsNull-guarded at that
comparison. An unguarded read of a path some predecessor route never produced is the Step
Functions `States.Runtime` footgun that broke cutover (M01: `$.resolved.hasCompositeTables`
read by HasCompositeToStop but never selected into `$.resolved` by CutoverResolveTask).

Static, offline (no AWS). Walks the real graph: Next, Default, Choices[].Next, Catch[].Next,
Map Iterator (nested, own input), End. Fixpoint-intersects "produced" sets over predecessors.

Precision that catches M01: a prefix produced by a state with an *enumerable* key set
(a ResultSelector, or a Pass Parameters/Result) is CLOSED — a child read must match one of its
keys. A prefix that is an opaque Task result at `$` or a state-machine/iterator input root is
OPEN — any child read under it is allowed. A read is OK iff its exact path is produced, OR its
nearest produced ancestor is OPEN.

Run: python3 tests/test_asl_paths.py   (REPO_DIR overridable). Exit 0 = clean.
"""
import json
import os
import re
import sys

REPO = os.environ.get("REPO_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SF_DIR = os.path.join(REPO, "stepfunctions")
CTX = "$$"


def jsonpaths_in(obj):
    out = []

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k.endswith(".$") and isinstance(v, str):
                    out.append(v)
                else:
                    walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(obj)
    return out


def root_of(path):
    if not isinstance(path, str) or not path.startswith("$"):
        return None
    if path.startswith(CTX):
        return None
    if path.strip() in ("$", "$$"):
        return ("$",)
    if not path.startswith("$."):
        m = re.search(r"\$\.[A-Za-z0-9_.\[\]'\"\-]+", path)
        if m:
            path = m.group(0)
        else:
            return None
    body = re.sub(r"\[[^\]]*\]", "", path[2:])
    parts = [p for p in body.split(".") if p]
    return tuple(["$"] + parts)


def prefixes(parts):
    for i in range(1, len(parts) + 1):
        yield tuple(parts[:i])


def choice_rule_vars(rule):
    read, guarded = set(), set()

    def walk(r):
        if not isinstance(r, dict):
            return
        var = r.get("Variable")
        if var:
            rt = root_of(var)
            if rt:
                read.add(rt)
                if "IsPresent" in r or "IsNull" in r:
                    guarded.add(rt)
        for key in ("And", "Or"):
            if key in r:
                for sub in r[key]:
                    walk(sub)
        if "Not" in r:
            walk(r["Not"])
    walk(rule)
    return read, guarded


def _successors(st):
    return [st["Next"]] if "Next" in st else []


def produced_by(states, sname):
    """Return (produced:set[tuple], open_prefixes:set[tuple]).
    produced = exact path-prefixes this state guarantees for successors.
    open = those prefixes whose child key set is NOT enumerable (opaque)."""
    st = states[sname]
    prod, opn = set(), set()
    t = st.get("Type")
    if t not in ("Task", "Pass", "Map", "Parallel"):
        return prod, opn
    rp = st.get("ResultPath", "$")
    if rp is None:
        return prod, opn  # result discarded
    if rp == "$":
        if t == "Pass":
            body = st.get("Parameters", st.get("Result", {}))
            if isinstance(body, dict):
                for k, v in body.items():
                    kk = k[:-2] if k.endswith(".$") else k
                    child = ("$", kk)
                    prod.add(child)
                    if k.endswith(".$") or isinstance(v, (dict, list)):
                        opn.add(child)
            # ("$",) itself stays open (input merges at root; unknown full key set)
            opn.add(("$",))
        else:
            # opaque Task/Map result replacing root: root stays open
            opn.add(("$",))
        return prod, opn
    rroot = root_of(rp)
    if not rroot:
        return prod, opn
    for p in prefixes(rroot):
        prod.add(p)
    body = None
    if t == "Pass":
        b = st.get("Parameters", st.get("Result", None))
        body = b if isinstance(b, dict) else None
    elif "ResultSelector" in st:
        body = st.get("ResultSelector")
    if body is not None:
        # CLOSED at rroot: we know its exact immediate children (the selector/Parameters keys).
        for k, v in body.items():
            kk = k[:-2] if k.endswith(".$") else k
            child = rroot + (kk,)
            prod.add(child)
            # If the child's value is a path-copy ($.<...>) or a nested object, its OWN children
            # are unknown -> mark the child OPEN so deeper reads under it are allowed.
            if k.endswith(".$") or isinstance(v, (dict, list)):
                opn.add(child)
        # rroot itself is closed (its immediate key set is known).
    else:
        # opaque Task/Map result object at rroot: its children are unknown -> OPEN
        opn.add(rroot)
    return prod, opn


def analyze_scope(states, start, input_roots, label):
    gaps = []
    edges = []
    prod_cache = {}
    for sname in states:
        prod_cache[sname] = produced_by(states, sname)

    for sname, st in states.items():
        base_prod, base_open = prod_cache[sname]
        t = st.get("Type")
        if t == "Choice":
            for rule in st.get("Choices", []):
                nxt = rule.get("Next")
                rd, _ = choice_rule_vars(rule)
                extra = set()
                for v in rd:
                    for p in prefixes(v):
                        extra.add(p)
                if nxt:
                    edges.append((sname, nxt, base_prod | extra, set(base_open)))
            if st.get("Default"):
                edges.append((sname, st["Default"], set(base_prod), set(base_open)))
        else:
            for nxt in _successors(st):
                edges.append((sname, nxt, set(base_prod), set(base_open)))
        for c in st.get("Catch", []) or []:
            nxt = c.get("Next")
            if nxt:
                cp = set(base_prod)
                co = set(base_open)
                crp = root_of(c.get("ResultPath", "$.error"))
                if crp:
                    for p in prefixes(crp):
                        cp.add(p)
                    co.add(crp)  # error object is opaque
                edges.append((sname, nxt, cp, co))

    preds = {s: [] for s in states}
    for src, dst, extra, extra_open in edges:
        if dst in preds:
            preds[dst].append((src, extra, extra_open))

    # available[s] = (produced:set, open:set) intersected over predecessors
    start_prod = set(input_roots) | {("$",)}
    start_open = set(input_roots) | {("$",)}  # inputs are opaque (unknown full key set)
    available = {s: None for s in states}
    available[start] = (start_prod, start_open)
    changed, it = True, 0
    while changed and it < 100000:
        changed = False
        it += 1
        for s in states:
            if s == start or not preds[s]:
                continue
            newp = newo = None
            for (src, extra, extra_open) in preds[s]:
                srcav = available[src]
                if srcav is None:
                    continue
                sp, so = srcav
                cp = sp | extra
                co = so | extra_open
                if newp is None:
                    newp, newo = set(cp), set(co)
                else:
                    newp &= cp
                    newo &= co
            if newp is None:
                continue
            newp |= ({("$",)} | set(input_roots))
            newo |= ({("$",)} | set(input_roots))
            cur = available[s]
            if cur is None or newp != cur[0] or newo != cur[1]:
                available[s] = (newp, newo)
                changed = True

    for sname, st in states.items():
        av = available[sname]
        if av is None:
            continue
        prod, opn = av
        t = st.get("Type")
        reads = []
        if t == "Choice":
            for rule in st.get("Choices", []):
                rd, guarded = choice_rule_vars(rule)
                for v in rd:
                    reads.append((v, v in guarded, f"Choice/{sname}"))
        params = st.get("Parameters")
        if params:
            for p in jsonpaths_in(params):
                rt = root_of(p)
                if rt:
                    reads.append((rt, False, f"Parameters/{sname}"))
        ip = st.get("ItemsPath")
        if ip:
            rt = root_of(ip)
            if rt:
                reads.append((rt, False, f"ItemsPath/{sname}"))
        for (path, guarded, where) in reads:
            if guarded or path == ("$",):
                continue
            if path in prod:
                continue
            # nearest produced ancestor; OK only if that ancestor is OPEN
            ok = False
            for i in range(len(path) - 1, 0, -1):
                anc = tuple(path[:i])
                if anc in prod or anc in opn:
                    ok = anc in opn
                    break
            else:
                ok = False
            if not ok:
                gaps.append(f"[{label}] {where}: reads {'.'.join(path)} "
                            f"but it is not produced on every route in and is not IsPresent-guarded")
    return gaps


def analyze(sm_path):
    sm = json.load(open(sm_path))
    name = os.path.basename(sm_path)
    scopes = []
    top_roots = {("$", "taskArn"), ("$", "bucket"), ("$", "taskSuffix"),
                 ("$", "adoptExistingFolder")}
    scopes.append((sm["States"], sm["StartAt"], top_roots, name))

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
                                   f"{lbl}::{sname}.Iterator"))
                    collect_maps(it["States"], f"{lbl}::{sname}.Iterator")
    collect_maps(sm["States"], name)

    gaps = []
    for states, start, roots, lbl in scopes:
        gaps += analyze_scope(states, start, roots, lbl)
    return gaps


def reachability(sm_path):
    """Return list of unreachable-state problems: every state (incl. nested Map iterator states)
    must be reachable from its scope's StartAt, and every Next/Default/Catch/Choice target must
    exist. Also every non-terminal state must have an onward transition."""
    sm = json.load(open(sm_path))
    name = os.path.basename(sm_path)
    problems = []

    def check_scope(states, start, lbl):
        # targets exist
        def targets(st):
            outs = []
            if "Next" in st:
                outs.append(st["Next"])
            if "Default" in st:
                outs.append(st["Default"])
            for c in st.get("Choices", []) or []:
                if "Next" in c:
                    outs.append(c["Next"])
            for c in st.get("Catch", []) or []:
                if "Next" in c:
                    outs.append(c["Next"])
            return outs
        for sname, st in states.items():
            for tgt in targets(st):
                if tgt not in states:
                    problems.append(f"[{lbl}] {sname} -> missing target '{tgt}'")
            if st.get("Type") not in ("Succeed", "Fail") and not st.get("End"):
                if not targets(st) and "Next" not in st:
                    # Choice with no Default and no matching path is allowed only if it has Choices
                    if st.get("Type") != "Choice":
                        problems.append(f"[{lbl}] {sname} has no onward transition and is not terminal")
        # reachable set
        seen = set()
        stack = [start]
        while stack:
            s = stack.pop()
            if s in seen or s not in states:
                continue
            seen.add(s)
            stack.extend(targets(states[s]))
        for sname in states:
            if sname not in seen:
                problems.append(f"[{lbl}] state '{sname}' is unreachable from {start}")

    check_scope(sm["States"], sm["StartAt"], name)

    def nested(states, lbl):
        for sname, st in states.items():
            if st.get("Type") == "Map":
                it = st.get("Iterator") or st.get("ItemProcessor")
                if it:
                    check_scope(it["States"], it["StartAt"], f"{lbl}::{sname}.Iterator")
                    nested(it["States"], f"{lbl}::{sname}.Iterator")
    nested(sm["States"], name)
    return problems


def m01_regression():
    """Explicit guard that the critical cutover composite wiring is correct and stays correct:
    - CutoverResolveTask.ResultSelector carries hasCompositeTables + compositeCdcJobName
    - HasCompositeToStop IsPresent-guards hasCompositeTables before the BooleanEquals."""
    problems = []
    cut = json.load(open(os.path.join(SF_DIR, "cutover.asl.json")))
    rs = cut["States"]["CutoverResolveTask"]["ResultSelector"]
    for want in ("hasCompositeTables.$", "compositeCdcJobName.$"):
        if want not in rs:
            problems.append(f"[M01] cutover CutoverResolveTask.ResultSelector missing '{want}'")
    hc = cut["States"]["HasCompositeToStop"]
    txt = json.dumps(hc)
    if '"IsPresent": true' not in txt or "hasCompositeTables" not in txt:
        problems.append("[M01] HasCompositeToStop no longer IsPresent-guards hasCompositeTables")
    return problems


def main():
    files = sorted(f for f in os.listdir(SF_DIR) if f.endswith(".asl.json"))
    all_gaps = []
    reach = []
    for f in files:
        all_gaps += analyze(os.path.join(SF_DIR, f))
        reach += reachability(os.path.join(SF_DIR, f))
    reg = m01_regression()
    failed = False
    if reach:
        failed = True
        print("ASL REACHABILITY: FAIL")
        for r in reach:
            print("  -", r)
    else:
        print(f"ASL REACHABILITY: PASS ({len(files)} state machines)")
    if reg:
        failed = True
        print("M01 REGRESSION: FAIL")
        for r in reg:
            print("  -", r)
    else:
        print("M01 REGRESSION: PASS (cutover composite fields wired + guarded)")
    if all_gaps:
        print("ASL PATH AUDIT: FAIL")
        for g in all_gaps:
            print("  -", g)
        print(f"\n{len(all_gaps)} unguarded missing-path gap(s).")
        return 1
    print(f"ASL PATH AUDIT: PASS ({len(files)} state machines, no unguarded missing paths)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
