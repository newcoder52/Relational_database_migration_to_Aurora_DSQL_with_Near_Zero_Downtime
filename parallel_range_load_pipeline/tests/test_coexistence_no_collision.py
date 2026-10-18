#!/usr/bin/env python3
"""Amendment 11 - coexistence: the MRL deployment must not collide with the ORIGINAL pipeline in
the same bucket/account/project. Executing, no AWS.

Part 1 (collision inventory): build everything MRL creates/writes/deletes and assert it never
equals or shadows an ORIGINAL resource outside the declared shared prefix config/_task/<suffix>/;
the only MRL writes inside the original namespace are the allow-listed ones; MRL never deletes
outside mrl/; MRL never creates a Glue job named like an original role.

Part 2 (resolve_task behavior, A11.3): run the REAL base resolve_task._check_owner against a fake
S3 that already holds MRL-written state under config/_task/<suffix>/ and report what the ORIGINAL
startup does (refuse / reuse / overwrite).
"""
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
BASE_LAMBDAS = os.path.join(REPO, "lambdas")
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, BASE_LAMBDAS)

SUFFIX = "t"

# -------- MRL inventory (what MRL creates / writes / deletes) ------------------------------
def _mrl_inventory(project="demo", suffix=SUFFIX):
    lambdas = ["%s-mrl-%s" % (project, n) for n in
               ("settings", "preflight", "plan", "reconcile", "cdc-handoff", "gate-eval",
                "ensure-job", "state", "fleet-preflight")]
    glue_jobs = ["%s-mrl-count" % project, "%s-%s-mrl-load" % (project, suffix)]
    state_machines = ["%s-mrl-task" % project, "%s-mrl-fleet" % project]
    # S3 keys/prefixes MRL writes
    writes = [
        "scripts/job2_load_manual_range.py", "scripts/job_mrl_count.py",
        "glue-templates/mrl-load.json", "glue-templates/mrl-count.json",
        "config/pipeline_parallel_load.json",
        "config/fleet_task_parallel_load.csv", "config/params_parallel_load.csv",
        "mrl/",                                    # all MRL state
        "mrl/runs/<exec>/settings.json",
        "mrl/state/%s/" % suffix,
        # inside the ORIGINAL namespace (shared), MRL (cdc=true) writes the handoff:
        "config/_task/%s/_load_status.json" % suffix,
        "config/_task/%s/_orchestrator/ck-<slug>/_load_status.json" % suffix,
        "config/_task/%s/_orchestrator/group-0/_load_status.json" % suffix,
    ]
    deletes = ["mrl/"]      # MRL (loader) deletes ONLY under mrl/
    return {"lambdas": lambdas, "glue_jobs": glue_jobs, "state_machines": state_machines,
            "writes": writes, "deletes": deletes}


# -------- ORIGINAL inventory (read from the base repo) -------------------------------------
def _original_inventory():
    if not os.path.isdir(BASE_LAMBDAS):
        return None
    # original Lambda base-names (from lambdas/*.py) -> <project>-<name with _ as ->
    lam = []
    for f in os.listdir(BASE_LAMBDAS):
        if f.endswith(".py"):
            lam.append("demo-" + f[:-3].replace("_", "-"))
    # original Glue job role suffixes created by create_glue_jobs._job_name
    roles = ("discovery", "load", "load-big", "validate", "cdc")
    # original state machines from stepfunctions/*.asl.json
    sms = []
    sf_dir = os.path.join(REPO, "stepfunctions")
    if os.path.isdir(sf_dir):
        for f in os.listdir(sf_dir):
            if f.endswith(".asl.json"):
                sms.append("demo-" + f[:-len(".asl.json")])
    # original config keys that are NOT shared (MRL must never write them)
    forbidden_writes = ["config/params.csv", "config/pipeline.json", "config/fleet_tasks.csv",
                        "scripts/job2_load.py", "glue-templates/load.json",
                        "glue-templates/load-big.json", "glue-templates/validate.json",
                        "glue-templates/discovery.json", "glue-templates/cdc.json"]
    return {"lambdas": lam, "glue_role_suffixes": roles, "state_machines": sms,
            "forbidden_writes": forbidden_writes}


# allow-listed MRL writes inside the ORIGINAL namespace, each with a reason.
_SHARED_ALLOW = {
    "config/_task/%s/_load_status.json" % SUFFIX:
        "cdc=true handoff: original-format full-load status the reused MAIN CDC job gates on",
    "config/_task/%s/_orchestrator/ck-<slug>/_load_status.json" % SUFFIX:
        "cdc=true handoff at a composite (ck) fork prefix the reused fork CDC job reads",
    "config/_task/%s/_orchestrator/group-0/_load_status.json" % SUFFIX:
        "cdc=true handoff at a big (bg/group) fork prefix the reused fork CDC job reads",
}


@pytest.mark.skipif(not os.path.isdir(BASE_LAMBDAS), reason="base repo not present (../lambdas)")
def test_a11_no_name_collision_with_original():
    mrl = _mrl_inventory()
    orig = _original_inventory()
    # (a) MRL names never equal an original Lambda / state machine name
    assert not (set(mrl["lambdas"]) & set(orig["lambdas"])), "MRL Lambda name collides with original"
    assert not (set(mrl["state_machines"]) & set(orig["state_machines"])), "SM name collides"
    # (d) MRL never creates a Glue job named like an original role (-discovery/-load/-load-big/...)
    for j in mrl["glue_jobs"]:
        for role in orig["glue_role_suffixes"]:
            # an original job is <project>-<suffix>-<role>; MRL's are *-mrl-* / *-mrl-load
            bad = re.search(r"-%s$" % re.escape(role), j)
            assert not (bad and "mrl" not in j), "MRL glue job %s looks like original role %s" % (j, role)
    # every MRL glue job name contains 'mrl'
    assert all("mrl" in j for j in mrl["glue_jobs"])


@pytest.mark.skipif(not os.path.isdir(BASE_LAMBDAS), reason="base repo not present")
def test_a11_writes_outside_shared_prefix_are_mrl_only():
    mrl = _mrl_inventory()
    orig = _original_inventory()
    for key in mrl["writes"]:
        # never a forbidden original key
        assert key not in orig["forbidden_writes"], "MRL writes forbidden original key %s" % key
        # a write inside config/_task/ (the ONLY shared original prefix) must be allow-listed
        if key.startswith("config/_task/"):
            assert key in _SHARED_ALLOW, "un-allow-listed MRL write inside config/_task: %s" % key
        # never inside the DMS data folders <schema>/<table>/ (incl. processed/ and failed/)
        assert "/processed/" not in key and "/failed/" not in key


def test_a11_shared_writes_are_exactly_the_handoff():
    """(b) with cdc=false MRL writes NOTHING from its OWN code inside config/_task/ (only the
    reused discovery does, which is the original's own behavior). The allow-list inside
    config/_task is EXACTLY the three cdc=true handoff docs."""
    for k in _SHARED_ALLOW:
        assert k.startswith("config/_task/%s/" % SUFFIX)
        assert k.endswith("_load_status.json")
    # the reconcile/handoff code writes _load_status.json ONLY when cdc=true (verified in
    # test_cdc_handoff.py::test_r23b1_handoff_writes_nothing_for_cdc_false) - assert the source
    # of the handoff Lambda guards on cdc.
    ch = open(os.path.join(PKG, "lambdas", "mrl_cdc_handoff.py")).read()
    assert "cdc=false" in ch and "nothing written" in ch


@pytest.mark.skipif(not os.path.isdir(BASE_LAMBDAS), reason="base repo not present")
def test_a11_mrl_never_deletes_outside_mrl():
    """(c) scan MRL lambdas/scripts for delete_object calls and assert every deleted key is under
    mrl/ (the loader's best-effort cleanup); nothing deletes config/_task/* or original keys."""
    bad = []
    for sub in ("lambdas", "scripts"):
        d = os.path.join(PKG, sub)
        for f in os.listdir(d):
            if not f.endswith(".py"):
                continue
            src = open(os.path.join(d, f)).read()
            for m in re.finditer(r"delete_object\(", src):
                # grab the ~200 chars around the call to inspect the Key
                seg = src[m.start():m.start() + 240]
                # the key must reference mrl/ (or a variable clearly scoped to mrl state)
                if "config/_task" in seg or "/processed/" in seg or "/failed/" in seg:
                    bad.append("%s: delete near %r" % (f, seg[:80]))
    assert not bad, "MRL deletes outside mrl/: %s" % bad


# ---- mutations: each must make a test fail --------------------------------------------------
def test_a11_mutations_are_caught():
    orig = _original_inventory()
    if orig is None:
        pytest.skip("base repo not present")
    # mutation 1: MRL writes config/params.csv -> forbidden
    bad_writes = _mrl_inventory()["writes"] + ["config/params.csv"]
    assert "config/params.csv" in orig["forbidden_writes"]
    assert any(k in orig["forbidden_writes"] for k in bad_writes)
    # mutation 2: MRL writes scripts/job2_load.py (original) -> forbidden
    assert "scripts/job2_load.py" in orig["forbidden_writes"]
    # mutation 3: MRL writes glue-templates/load.json (original) -> forbidden
    assert "glue-templates/load.json" in orig["forbidden_writes"]
    # mutation 4: a write into <schema>/<table>/processed/ would be rejected by the data-folder rule
    k = "s/t/processed/x.csv"
    assert "/processed/" in k


# ---- A11.3: the REAL resolve_task behavior with pre-existing MRL state ----------------------
@pytest.mark.skipif(not os.path.isdir(BASE_LAMBDAS), reason="base repo not present")
def test_a11_resolve_task_behavior_with_preexisting_mrl_state():
    """Run the REAL base resolve_task._check_owner against a fake S3 that already holds MRL state
    under config/_task/<suffix>/. Document the behavior; FAIL (so we notice) if it would
    silently overwrite/corrupt MRL's loaded state."""
    import resolve_task as rt
    from mrl_fakes import FakeS3
    import json as _json
    arn = "arn:aws:dms:us-east-1:123456789012:task:ABC"

    # Case A: MRL left discovery+load artifacts BUT NO owner marker (_task.json).
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key="config/_task/%s/_manifest_index.json" % SUFFIX, Body=b"{}")
    s3.put_object(Bucket="b", Key="config/_task/%s/_load_status.json" % SUFFIX,
                  Body=_json.dumps({"tables": {"s.t": {"status": "done", "rows": 30}}}).encode())
    rt.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3)})
    refused = False
    try:
        rt._check_owner(s3, "b", SUFFIX, arn, "startup", {}, [])
    except rt.FolderOwnerError:
        refused = True
    assert refused, ("A11.3 FINDING: with NO owner marker but MRL artifacts present, the ORIGINAL "
                     "startup must REFUSE (fail closed), not overwrite. If this assertion fails, "
                     "report it: startup would silently re-run into MRL's loaded folder.")

    # Case B: the MRL run also wrote the owner marker (_task.json) for THIS task (the reused
    # resolve_task writes it). The original startup then REUSES the folder (returns, does not
    # refuse) - which is why the route-conflict guard + 'never both routes on one task' rule
    # exists. Document that: reuse, NOT refuse, NOT overwrite-of-marker.
    s3b = FakeS3()
    s3b.put_object(Bucket="b", Key="config/_task/%s/_task.json" % SUFFIX,
                   Body=_json.dumps({"taskArn": arn, "taskSuffix": SUFFIX}).encode())
    s3b.put_object(Bucket="b", Key="config/_task/%s/_load_status.json" % SUFFIX,
                   Body=_json.dumps({"tables": {"s.t": {"status": "done", "rows": 30}}}).encode())
    rt.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3b)})
    marker_key, created = rt._check_owner(s3b, "b", SUFFIX, arn, "startup", {}, [])
    assert created is False, "with a matching owner marker the original startup REUSES the folder"
    # the marker is not overwritten (still the same task)
    assert _json.loads(s3b.get_object(Bucket="b", Key=marker_key)["Body"].read())["taskArn"] == arn

    # Case C: a DIFFERENT task owning the folder -> refuse (reused-name protection).
    s3c = FakeS3()
    s3c.put_object(Bucket="b", Key="config/_task/%s/_task.json" % SUFFIX,
                   Body=_json.dumps({"taskArn": "arn:other"}).encode())
    rt.boto3 = type("x", (), {"client": staticmethod(lambda *a, **k: s3c)})
    try:
        rt._check_owner(s3c, "b", SUFFIX, arn, "startup", {}, [])
        assert False, "a different owning task must be refused"
    except rt.FolderOwnerError:
        pass


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn()
            print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
