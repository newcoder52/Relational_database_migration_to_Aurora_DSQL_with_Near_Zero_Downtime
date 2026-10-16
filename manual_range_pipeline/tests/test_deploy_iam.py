#!/usr/bin/env python3
"""Executing tests for RE-VETTER B deployment/IAM blockers (vet/FINDINGS_B.md B2-B8)."""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_fleet_preflight as fp  # noqa: E402

DEPLOY = os.path.join(PKG, "tools", "deploy_mrl.sh")
IAM = json.load(open(os.path.join(PKG, "iam", "mrl-additions.json")))
TASK_ASL = os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")
FLEET_ASL = os.path.join(PKG, "stepfunctions", "mrl-fleet.asl.json")


# ---- B2: the shipped example CSV loads under the preflight's columns ----------------------
def test_b2_example_csv_columns_valid():
    ex = open(os.path.join(PKG, "config", "fleet_task_parallel_load.example.csv")).read()
    header = [h.strip() for h in ex.splitlines()[0].split(",")]
    unknown = [h for h in header if h and h not in fp._COLUMNS]
    assert not unknown, "example CSV has columns the preflight rejects: %s" % unknown
    assert "cdc" in header and "mode" not in header, "example must use cdc, not mode (Amendment 8)"


# ---- B3: deploy fills EVERY ASL placeholder and creates mrl-settings ----------------------
def test_b3_deploy_fills_all_placeholders_and_creates_mrl_settings():
    deploy = open(DEPLOY).read()
    assert 'create_or_update_fn "${PROJECT}-mrl-settings"' in deploy, \
        "deploy must create the mrl-settings Lambda (the fleet's first state invokes it)"
    # every <<...>> placeholder in BOTH ASL files must have a sed substitution in deploy
    placeholders = set()
    for f in (TASK_ASL, FLEET_ASL):
        placeholders |= set(re.findall(r"<<[^>]+>>", open(f).read()))
    missing = [ph for ph in placeholders if ph not in deploy]
    assert not missing, "deploy_mrl.sh does not substitute ASL placeholder(s): %s" % missing


# ---- B4: SFN role can invoke mrl-settings -------------------------------------------------
def test_b4_sfn_can_invoke_mrl_settings():
    inv = [st for st in IAM["SfnRoleAdditions"]["Statement"]
           if st["Sid"] == "MrlInvokeMrlLambdas"][0]
    assert any("mrl-settings" in r for r in inv["Resource"]), \
        "SFN role must allow lambda:InvokeFunction on <project>-mrl-settings (B4)"


# ---- B5: Lambda role can read the config inputs + read/write pipeline_parallel_load.json ---
def _lambda_actions_on(resource_substr):
    acts = set()
    for st in IAM["LambdaRoleAdditions"]["Statement"]:
        for r in st.get("Resource", []):
            if resource_substr in r:
                acts |= set(st["Action"])
    return acts


def test_b5_lambda_can_read_config_inputs_and_rw_settings():
    assert "s3:GetObject" in _lambda_actions_on("config/params.csv")
    assert "s3:GetObject" in _lambda_actions_on("config/params_parallel_load.csv")
    assert "s3:GetObject" in _lambda_actions_on("config/fleet_task_parallel_load.csv")
    ppl = _lambda_actions_on("config/pipeline_parallel_load.json")
    assert "s3:GetObject" in ppl and "s3:PutObject" in ppl


# ---- B6: IAM never writes the forbidden files; scripts/templates read-only; no _task delete
def test_b6_iam_not_overbroad():
    # config/pipeline.json must NOT appear as a write resource anywhere
    for role in ("LambdaRoleAdditions", "SfnRoleAdditions", "GlueRoleAdditions"):
        for st in IAM[role]["Statement"]:
            writes = {a for a in st["Action"] if a in ("s3:PutObject", "s3:DeleteObject")}
            if not writes:
                continue
            for r in st.get("Resource", []):
                assert "config/pipeline.json" not in r, "%s writes forbidden config/pipeline.json" % role
                assert not r.endswith("config/params.csv"), "%s writes forbidden config/params.csv" % role
                assert "config/fleet_tasks.csv" not in r, "%s writes forbidden config/fleet_tasks.csv" % role
    # scripts/* and glue-templates/* are GetObject-only on the Lambda role
    assert _lambda_actions_on("scripts/*") == {"s3:GetObject"}
    assert _lambda_actions_on("glue-templates/*") == {"s3:GetObject"}
    # config/_task/* has no DeleteObject on the Lambda role (Amendment 7.6)
    assert "s3:DeleteObject" not in _lambda_actions_on("config/_task/*")


# ---- B7: the count job gets its VPC connection when one is configured ----------------------
def test_b7_count_job_gets_connection():
    deploy = open(DEPLOY).read()
    assert "CONN_JSON" in deploy and "\\\"Connections\\\":{\\\"Connections\\\"" in deploy, \
        "count job update must include a Connections block when GLUE_CONNECTION is set"
    assert "CONN_CREATE" in deploy and "--connections-list" in deploy, \
        "count job create must pass --connections-list when GLUE_CONNECTION is set"


# ---- B8: deploy verifies the zip (modules + reused parsers + pg8000) and copies are mandatory
def test_b8_deploy_verifies_zip():
    deploy = open(DEPLOY).read()
    assert "MRL Lambda zip is missing" in deploy, "deploy must verify the zip contents (B8)"
    assert "missing pg8000" in deploy, "deploy must assert pg8000 is in the zip (B8)"
    # the resolve_task/params_csv copy is mandatory (fail closed if absent)
    assert "REQUIRES the base" in deploy


# ---- R22-1: the Lambda role can list/describe the ORIGINAL startup+cutover executions --------
def test_r22_1_route_guard_iam_grants_present():
    """The route-conflict guard calls states:ListExecutions/DescribeExecution on <project>-startup
    and <project>-cutover; without these grants the guard's AccessDenied would be swallowed."""
    def _acts_on(sub):
        acts = set()
        for st in IAM["LambdaRoleAdditions"]["Statement"]:
            for r in st.get("Resource", []):
                if sub in r:
                    acts |= set(st["Action"])
        return acts
    for machine in ("stateMachine:<project>-startup", "stateMachine:<project>-cutover"):
        assert "states:ListExecutions" in _acts_on(machine), \
            "Lambda role must allow states:ListExecutions on %s (R22-1)" % machine
    for execs in ("execution:<project>-startup:*", "execution:<project>-cutover:*"):
        assert "states:DescribeExecution" in _acts_on(execs), \
            "Lambda role must allow states:DescribeExecution on %s (R22-1)" % execs


# ---- R23-B1: the new mrl-cdc-handoff Lambda is created by deploy and invokable by the SFN ----
def test_r23b1_cdc_handoff_lambda_created_and_invokable():
    deploy = open(DEPLOY).read()
    assert "mrl-cdc-handoff" in deploy, "deploy must create the mrl-cdc-handoff Lambda"
    assert "<<MRL_CDC_HANDOFF_LAMBDA_ARN>>" in deploy, \
        "fill_sm must substitute <<MRL_CDC_HANDOFF_LAMBDA_ARN>>"
    inv = [st for st in IAM["SfnRoleAdditions"]["Statement"]
           if st["Sid"] == "MrlInvokeMrlLambdas"][0]
    assert any("mrl-cdc-handoff" in r for r in inv["Resource"]), \
        "SFN role must allow lambda:InvokeFunction on <project>-mrl-cdc-handoff"
    # the child ASL wires CdcForkHandoff between EnsureForkJobs and ResumeDmsToCdc
    d = json.load(open(TASK_ASL))
    assert "CdcForkHandoff" in d["States"]
    assert d["States"]["EnsureForkJobs"]["Next"] == "CdcForkHandoff"
    assert d["States"]["CdcForkHandoff"]["Next"] == "ResumeDmsToCdc"


# ---- R23-L7: the Glue role must NOT DeleteObject on config/_task/* (original-route state) -----
def test_r23l7_glue_role_no_delete_on_task_config():
    for st in IAM["GlueRoleAdditions"]["Statement"]:
        if "s3:DeleteObject" in st.get("Action", []):
            for r in st.get("Resource", []):
                assert "config/_task/" not in r, \
                    "Glue role must not DeleteObject on config/_task/* (R23-L7); Delete only on mrl/*"
    # and config/_task/* is still readable/writable (Get/Put) for the load/count jobs
    gp = set()
    for st in IAM["GlueRoleAdditions"]["Statement"]:
        for r in st.get("Resource", []):
            if r.endswith("config/_task/*"):
                gp |= set(st["Action"])
    assert "s3:GetObject" in gp and "s3:PutObject" in gp and "s3:DeleteObject" not in gp


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d deploy/IAM test(s) passed" % len(fns))
