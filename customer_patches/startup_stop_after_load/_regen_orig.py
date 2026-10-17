#!/usr/bin/env python3
"""Regenerate customer_patches/startup_stop_after_load/orig/ snapshots so they are the
'pre-stopAfterLoad but post-null_notnull_text' baseline = release-1's shipped files with ONLY the
stopAfterLoad transform removed. Verified by round-trip: patch_*(orig) == shipped.

Writes nothing outside customer_patches/startup_stop_after_load/. Run from the repo root.
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) \
    if False else os.environ.get("REPO_DIR", os.getcwd())
PATCH = os.path.join(REPO, "customer_patches", "startup_stop_after_load")
ORIG = os.path.join(PATCH, "orig")
sys.path.insert(0, PATCH)
import stop_after_load_transform as X   # noqa: E402

LAMBDAS = os.path.join(REPO, "lambdas")
SF = os.path.join(REPO, "stepfunctions")


def _invert_py(shipped, ops):
    """Remove each (anchor, add, where) insertion from shipped text (exact string surgery)."""
    out = shipped
    for anchor, add, where in ops:
        if where == "after":
            needle = anchor + add
        else:
            needle = add + anchor
        assert needle in out, f"inverse anchor+add not found for {anchor[:40]!r}"
        out = out.replace(needle, anchor, 1)
    return out


def gen_resolve_task():
    shipped = open(os.path.join(LAMBDAS, "resolve_task.py")).read()
    ops = [
        (X._RT_DISPATCH_ANCHOR, X._RT_DISPATCH_ADD, "after"),
        (X._RT_NORMALIZE_ANCHOR, X._RT_NORMALIZE_ADD, "after"),
        (X._RT_HANDLER_ANCHOR, X._RT_HANDLER_ADD, "before"),
        (X._RT_NORM_CALL_ANCHOR, X._RT_NORM_CALL_ADD, "after"),
        (X._RT_OUT_ANCHOR, X._RT_OUT_ADD, "after"),
    ]
    orig = _invert_py(shipped, ops)
    assert X.patch_resolve_task_src(orig) == shipped, "resolve_task round-trip failed"
    open(os.path.join(ORIG, "resolve_task.py"), "w").write(orig)


def gen_preflight():
    shipped = open(os.path.join(LAMBDAS, "preflight_tasks.py")).read()
    ops = [
        (X._PF_FLEET_ANCHOR, X._PF_FLEET_ADD, "after"),
        (X._PF_CHILD_ANCHOR, X._PF_CHILD_ADD, "after"),
    ]
    orig = _invert_py(shipped, ops)
    assert X.patch_preflight_tasks_src(orig) == shipped, "preflight round-trip failed"
    open(os.path.join(ORIG, "preflight_tasks.py"), "w").write(orig)


_ADDED_TOP_STATES = ["IsLoadOnly", "MarkLoadOnly", "MarkNotLoadOnly", "LoadOnlyOrContinue",
                     "RecordLoadOnly", "RecordLoadOnlyFailedButLoaded", "TaskLoadOnlySucceeded"]


def gen_startup_asl():
    shipped = json.load(open(os.path.join(SF, "startup.asl.json")))
    orig = json.loads(json.dumps(shipped))
    st = orig["States"]
    # EDIT 0: drop the ResolveTask ResultSelector stopAfterLoad entry
    st["ResolveTask"]["ResultSelector"].pop("stopAfterLoad.$", None)
    # EDIT 1: EnsureForkJobs.Next back to GroupFanOut; drop the 3 pre-fanout states
    st["EnsureForkJobs"]["Next"] = "GroupFanOut"
    # EDIT 2: Map Parameters drop stopAfterLoad.$
    gfo = st["GroupFanOut"]
    gfo.get("Parameters", {}).pop("stopAfterLoad.$", None)
    # EDIT 3: iterator GroupLoad/GroupLoadBig.Next back to GroupValidate; drop AfterGroupLoad
    it = (gfo.get("Iterator") or gfo.get("ItemProcessor"))["States"]
    for ld in ("GroupLoad", "GroupLoadBig"):
        it[ld]["Next"] = "GroupValidate"
    it.pop("AfterGroupLoad", None)
    # EDIT 4: EvaluateGroupResults.Next back to AllGroupsSucceeded; drop terminal/marker states
    st["EvaluateGroupResults"]["Next"] = "AllGroupsSucceeded"
    for s in _ADDED_TOP_STATES:
        st.pop(s, None)
    assert X.patch_startup_asl(orig) == shipped, "startup ASL round-trip failed"
    # write pretty (2-space) to match the repo style
    open(os.path.join(ORIG, "startup.asl.json"), "w").write(
        json.dumps(orig, indent=2) + "\n")
    # re-read + re-verify from the written file (exact bytes the test will use)
    orig2 = json.load(open(os.path.join(ORIG, "startup.asl.json")))
    assert X.patch_startup_asl(orig2) == shipped, "startup ASL round-trip (from file) failed"


def gen_fleet_startup():
    # fleet-startup is UNCHANGED by the patch; snapshot it from the shipped file so
    # verify_fleet_startup_asl(orig) == orig holds and SHA256SUMS old==new.
    shipped = json.load(open(os.path.join(SF, "fleet-startup.asl.json")))
    assert X.verify_fleet_startup_asl(shipped) == shipped, "fleet verify failed"
    open(os.path.join(ORIG, "fleet-startup.asl.json"), "w").write(
        json.dumps(shipped, indent=2) + "\n")


if __name__ == "__main__":
    gen_resolve_task()
    gen_preflight()
    gen_startup_asl()
    gen_fleet_startup()
    print("orig/ regenerated and round-trip-verified:")
    for f in ("resolve_task.py", "preflight_tasks.py", "startup.asl.json", "fleet-startup.asl.json"):
        print("  ", f, "OK")
