#!/usr/bin/env python3
"""Deploy-correctness tests (re-vetter B + Amendment 10/12). IAM coverage is proven separately by
tests/test_existing_iam_covers_mrl.py (ZERO new IAM for the default deployment); the old
iam/mrl-additions.json was split/removed by Amendment 10 (only iam/mrl-fleet-sm-optional.json
remains, for the OPTIONAL fleet state machine)."""
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
        "deploy must create the mrl-settings Lambda"
    placeholders = set()
    for f in (TASK_ASL, FLEET_ASL):
        placeholders |= set(re.findall(r"<<[^>]+>>", open(f).read()))
    missing = [ph for ph in placeholders if ph not in deploy]
    assert not missing, "deploy_mrl.sh does not substitute ASL placeholder(s): %s" % missing


# ---- B7: the count job gets its VPC connection (A12: valid --connections, not --connections-list)
def test_b7_count_job_gets_connection_valid_flag():
    deploy = open(DEPLOY).read()
    assert "CONN_JSON" in deploy and "Connections" in deploy, \
        "count job update must include a Connections block when GLUE_CONNECTION is set"
    # A12: create-job uses the VALID top-level --connections flag, NOT the invalid --connections-list
    assert "--connections-list" not in deploy, "the exit-252 --connections-list defect is back"
    assert "CONN_CREATE_ARGS" in deploy and "--connections " in deploy


# ---- B8: deploy verifies the zip modules; A10 C.5: no pg8000 pip step (no MRL Lambda needs it) --
def test_b8_deploy_verifies_zip_no_pg8000():
    deploy = open(DEPLOY).read()
    assert "MRL Lambda zip is missing module(s)" in deploy, "deploy must verify the zip modules (B8)"
    assert "REQUIRES the base" in deploy, "resolve_task/params_csv copy must be mandatory"
    # A10 C.5: the Lambda zip no longer pip-installs pg8000 (deployment needs no internet)
    assert "pip install" not in deploy, "deploy must not pip-install (no MRL Lambda imports pg8000)"


# ---- A10 B.3: the fleet state machine is OPTIONAL (default off); no iam/mrl-additions.json ---
def test_a10_fleet_sm_is_optional_default_off():
    deploy = open(DEPLOY).read()
    assert "--with-fleet-sm" in deploy, "deploy must gate the fleet SM behind --with-fleet-sm"
    assert 'WITH_FLEET_SM=0' in deploy, "the fleet SM must default OFF"
    # the mrl-task machine is always created; the fleet machine only under the flag
    assert 'create_or_update_sm "${PROJECT}-mrl-task"' in deploy
    assert re.search(r'WITH_FLEET_SM.*-eq 1', deploy), "fleet SM creation must be flag-gated"


def test_a10_no_mrl_additions_reference_and_no_iam_call():
    deploy = open(DEPLOY).read()
    assert "mrl-additions.json" not in deploy, "the split/removed additions file must not be referenced"
    assert not os.path.exists(os.path.join(PKG, "iam", "mrl-additions.json")), \
        "iam/mrl-additions.json must be removed (Amendment 10 split)"
    assert os.path.exists(os.path.join(PKG, "iam", "mrl-fleet-sm-optional.json"))
    # deploy_mrl.sh must NEVER call aws iam ... (it changes no IAM)
    assert not re.search(r"\baws\s+iam\b", deploy), "deploy must not call any aws iam command"


# ---- R23-B1: the mrl-cdc-handoff Lambda is created + wired (ASL), still verified ----------
def test_r23b1_cdc_handoff_lambda_created_and_wired():
    deploy = open(DEPLOY).read()
    assert "mrl-cdc-handoff" in deploy
    assert "<<MRL_CDC_HANDOFF_LAMBDA_ARN>>" in deploy
    d = json.load(open(TASK_ASL))
    assert "CdcForkHandoff" in d["States"]
    assert d["States"]["EnsureForkJobs"]["Next"] == "CdcForkHandoff"
    assert d["States"]["CdcForkHandoff"]["Next"] == "ResumeDmsToCdc"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d deploy test(s) passed" % len(fns))
