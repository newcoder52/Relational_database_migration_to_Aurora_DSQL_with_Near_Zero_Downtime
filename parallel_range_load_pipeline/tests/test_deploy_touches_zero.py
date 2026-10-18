#!/usr/bin/env python3
"""Amendment 11.2 - with a FAKE aws CLI on PATH, assert deploy_mrl.sh prints
'EXISTING PIPELINE RESOURCES TOUCHED: 0' and never aims a create/update at a non-MRL name, for a
dry run AND a full run; and that it ABORTS when a target name exists but is NOT MRL-owned.
"""
import os
import re
import stat
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
DEPLOY = os.path.join(PKG, "tools", "deploy_mrl.sh")

# a fake `aws` that: records argv to $AWS_CALL_LOG; for get-function/get-job/list-state-machines
# returns 'not found' (empty/err) UNLESS $FAKE_EXISTING names the resource; returns dummy JSON for
# get-caller-identity; and 'succeeds' (prints nothing) for create/update/cp.
_FAKE_AWS = r'''#!/usr/bin/env python3
import os, sys, json
argv = sys.argv[1:]
log = os.environ.get("AWS_CALL_LOG")
if log:
    with open(log, "a") as f:
        f.write(" ".join(argv) + "\n")
existing = set((os.environ.get("FAKE_EXISTING") or "").split(","))
existing.discard("")
owned_marker = os.environ.get("FAKE_OWNED", "")   # name -> mrl-owned if listed

def out(s): sys.stdout.write(s)

svc = argv[0] if argv else ""
op = argv[1] if len(argv) > 1 else ""

def argval(flag):
    return argv[argv.index(flag)+1] if flag in argv else None

if svc == "sts" and op == "get-caller-identity":
    out(json.dumps({"Account": "123456789012"})); sys.exit(0)
if svc == "lambda" and op == "get-function":
    name = argval("--function-name")
    sys.exit(0 if name in existing else 254)
if svc == "lambda" and op == "get-function-configuration":
    name = argval("--function-name")
    q = argval("--query") or ""
    # resolve-task present iff FAKE_RESOLVE_RUNTIME set (for the R4-1 runtime-copy test)
    rt = os.environ.get("FAKE_RESOLVE_RUNTIME", "")
    if name.endswith("-resolve-task"):
        if not rt:
            sys.exit(254)
        if q == "Runtime": out(rt); sys.exit(0)
        if q.startswith("Architectures"): out(os.environ.get("FAKE_RESOLVE_ARCH", "arm64")); sys.exit(0)
        if "SubnetIds" in q: out(os.environ.get("FAKE_RESOLVE_SUBNETS", "None")); sys.exit(0)
        if "SecurityGroupIds" in q: out(os.environ.get("FAKE_RESOLVE_SGS", "None")); sys.exit(0)
        out("dsql-mrl" if False else "resolve-task"); sys.exit(0)
    if name not in existing: sys.exit(254)
    if q == "Description" or q == "":
        out("dsql-mrl owned" if name in owned_marker.split(",") else "original pipeline"); sys.exit(0)
    out("None"); sys.exit(0)
if svc == "glue" and op == "get-job":
    name = argval("--job-name")
    sys.exit(0 if name in existing else 254)
if svc == "glue" and op == "get-tags":
    out("dsql_pipeline_mrl" if True else ""); sys.exit(0)
if svc == "stepfunctions" and op == "list-state-machines":
    out("None"); sys.exit(0)
if svc == "stepfunctions" and op == "list-tags-for-resource":
    out(""); sys.exit(0)
if svc == "stepfunctions" and op == "validate-state-machine-definition":
    mode = os.environ.get("FAKE_SM_VALIDATE", "pass")
    if mode == "unavailable":
        sys.stderr.write("Invalid choice: 'validate-state-machine-definition'"); sys.exit(2)
    if mode == "fail":
        out(json.dumps({"result": "FAIL", "diagnostics": [
            {"severity": "ERROR", "code": "SCHEMA_VALIDATION_FAILED",
             "message": "Field 'TimeoutSeconds.$' is not supported",
             "location": "/States/SafetyProbe"}]})); sys.exit(0)
    out(json.dumps({"result": "OK", "diagnostics": []})); sys.exit(0)
if svc == "s3" and op == "ls":
    # driver-cdc/ wheel listing for the A13-A.3 prerequisite check
    n = int(os.environ.get("FAKE_CDC_WHEELS", "3"))
    wheels = ["boto3-1.42.97-py3-none-any.whl", "botocore-1.42.97-py3-none-any.whl",
              "s3transfer-0.16.1-py3-none-any.whl", "pg8000-1.31.2-py3-none-any.whl",
              "scramp-1.4.5-py3-none-any.whl"]
    # FAKE_CDC_WHEELS=0 -> empty; otherwise emit the full DSQL set (boto3/botocore/s3transfer+pg8000)
    emit = [] if n == 0 else wheels
    out("".join("2026-01-01 00:00:00  12345 %s\n" % w for w in emit))
    sys.exit(0)
# create/update/cp/etc -> succeed silently
sys.exit(0)
'''


def _run_deploy(extra_env=None, dry_run=True):
    d = tempfile.mkdtemp()
    fake = os.path.join(d, "aws")
    with open(fake, "w") as f:
        f.write(_FAKE_AWS)
    os.chmod(fake, os.stat(fake).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    log = os.path.join(d, "calls.log")
    env = dict(os.environ)
    env["PATH"] = d + os.pathsep + env["PATH"]
    env["AWS_CALL_LOG"] = log
    if extra_env:
        env.update(extra_env)
    args = ["bash", DEPLOY, "--bucket", "b", "--account-id", "123456789012", "--region",
            "us-east-1", "--project", "demo",
            "--glue-role-arn", "arn:aws:iam::123456789012:role/demo-glue-exec-role",
            "--sfn-role-arn", "arn:aws:iam::123456789012:role/demo-sfn-exec-role",
            "--lambda-role-arn", "arn:aws:iam::123456789012:role/demo-lambda-exec-role",
            "--dsql-endpoint", "c.dsql.us-east-1.on.aws", "--dsql-user", "admin",
            "--dsql-database", "postgres"]
    if dry_run:
        args.append("--dry-run")
    r = subprocess.run(args, capture_output=True, text=True, env=env)
    calls = open(log).read().splitlines() if os.path.exists(log) else []
    return r, calls


def _non_mrl_mutations(calls):
    """create/update aws calls (from the fake log = real executed calls) that target a non-mrl name."""
    bad = []
    for c in calls:
        if re.search(r"(create-function|update-function-code|create-job|update-job|"
                     r"create-state-machine|update-state-machine)", c):
            # the targeted name is the --name/--function-name/--job-name value
            m = re.search(r"--(?:function-name|job-name|name) (\S+)", c)
            if m and "mrl" not in m.group(1):
                bad.append(c)
    return bad


def test_dry_run_touches_zero():
    r, calls = _run_deploy(dry_run=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "EXISTING PIPELINE RESOURCES TOUCHED: 0" in r.stdout
    # a dry run executes only READ calls (get-*/list-*/sts); no create/update/cp actually ran
    muts = [c for c in calls if re.search(r"(create-|update-|s3 cp)", c)]
    assert not muts, "dry-run must not execute mutating aws calls: %s" % muts[:3]


def test_full_run_touches_zero_and_no_non_mrl_target():
    r, calls = _run_deploy(dry_run=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "EXISTING PIPELINE RESOURCES TOUCHED: 0" in r.stdout
    bad = _non_mrl_mutations(calls)
    assert not bad, "full run targeted a non-MRL name: %s" % bad
    # it DID create the mrl resources (sanity)
    assert any("create-function" in c and "mrl" in c for c in calls)


def test_abort_when_name_exists_not_mrl_owned():
    # demo-mrl-settings 'exists' but is NOT mrl-owned (its description is not dsql-mrl)
    r, calls = _run_deploy(extra_env={"FAKE_EXISTING": "demo-mrl-settings", "FAKE_OWNED": ""},
                           dry_run=True)
    assert r.returncode == 1, "must abort when a target exists but is not MRL-owned"
    assert "ABORT" in (r.stdout + r.stderr)
    assert "demo-mrl-settings" in (r.stdout + r.stderr)


def test_update_when_name_exists_and_mrl_owned():
    r, calls = _run_deploy(extra_env={"FAKE_EXISTING": "demo-mrl-settings",
                                      "FAKE_OWNED": "demo-mrl-settings"}, dry_run=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "WILL UPDATE (mrl-owned)" in r.stdout
    assert "EXISTING PIPELINE RESOURCES TOUCHED: 0" in r.stdout


# ---- R4-1: deploy copies Runtime/Architectures from resolve-task; optional VPC ------------
def test_r4_1_copies_runtime_and_arch_from_resolve_task():
    r, calls = _run_deploy(extra_env={"FAKE_RESOLVE_RUNTIME": "python3.12",
                                      "FAKE_RESOLVE_ARCH": "arm64"}, dry_run=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "Runtime=python3.12 Architectures=arm64" in r.stdout
    assert any("--runtime python3.12" in c for c in _create_fn_lines(r.stdout)) or \
        "--runtime python3.12" in r.stdout
    assert "--architectures arm64" in r.stdout


def test_r4_1_runtime_fallback_notice_when_resolve_task_absent():
    r, _ = _run_deploy(dry_run=True)   # no FAKE_RESOLVE_RUNTIME -> resolve-task 'not found'
    assert "resolve-task not found; using default Runtime=python3.11" in r.stdout
    assert "--runtime python3.11" in r.stdout


def test_r4_1_optional_vpc_flag_copies_vpc_config():
    d = tempfile.mkdtemp()
    fake = os.path.join(d, "aws")
    with open(fake, "w") as f:
        f.write(_FAKE_AWS)
    os.chmod(fake, os.stat(fake).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    env = dict(os.environ)
    env["PATH"] = d + os.pathsep + env["PATH"]
    env["AWS_CALL_LOG"] = os.path.join(d, "calls.log")
    env.update({"FAKE_RESOLVE_RUNTIME": "python3.12", "FAKE_RESOLVE_SUBNETS": "subnet-a,subnet-b",
                "FAKE_RESOLVE_SGS": "sg-1"})
    args = ["bash", DEPLOY, "--dry-run", "--lambda-vpc-from-resolve-task", "--bucket", "b",
            "--account-id", "123456789012", "--region", "us-east-1", "--project", "demo",
            "--glue-role-arn", "arn:aws:iam::123456789012:role/g",
            "--sfn-role-arn", "arn:aws:iam::123456789012:role/s",
            "--lambda-role-arn", "arn:aws:iam::123456789012:role/l",
            "--dsql-endpoint", "c.dsql.us-east-1.on.aws", "--dsql-user", "admin",
            "--dsql-database", "postgres"]
    r = subprocess.run(args, capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "SubnetIds=subnet-a,subnet-b,SecurityGroupIds=sg-1" in r.stdout
    assert "--vpc-config" in r.stdout


def _create_fn_lines(stdout):
    return [l for l in stdout.splitlines() if "create-function" in l]


# ---- A13-A.3: deploy aborts when driver-cdc/ has no wheels --------------------------------
def test_a13_deploy_aborts_when_driver_cdc_wheels_missing():
    r, _ = _run_deploy(extra_env={"FAKE_CDC_WHEELS": "0"}, dry_run=False)
    assert r.returncode == 1, "deploy must abort when driver-cdc/ has no .whl files"
    assert "driver-cdc/ has no .whl" in (r.stdout + r.stderr)


def test_a13_deploy_reports_driver_cdc_wheels_present():
    r, _ = _run_deploy(extra_env={"FAKE_CDC_WHEELS": "5"}, dry_run=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "driver-cdc/ .whl files found: 5" in r.stdout
    assert "count/probe job wheel set: driver-cdc/" in r.stdout


# ---- A14.3: deploy pre-validates the state machine definition -----------------------------
def test_a14_sm_validate_fail_aborts_before_create():
    r, calls = _run_deploy(extra_env={"FAKE_SM_VALIDATE": "fail"}, dry_run=False)
    assert r.returncode == 1, "a FAIL validation must abort the deploy"
    assert "Step Functions rejected" in (r.stdout + r.stderr)
    assert "SCHEMA_VALIDATION_FAILED" in (r.stdout + r.stderr)
    # nothing created: no create/update-state-machine executed
    assert not any("state-machine" in c and ("create-" in c or "update-" in c) for c in calls)


def test_a14_sm_validate_ok_continues():
    r, calls = _run_deploy(extra_env={"FAKE_SM_VALIDATE": "pass"}, dry_run=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "validated demo-mrl-task definition: result OK" in r.stdout
    assert any("create-state-machine" in c for c in calls)


def test_a14_sm_validate_unavailable_warns_and_continues():
    r, calls = _run_deploy(extra_env={"FAKE_SM_VALIDATE": "unavailable"}, dry_run=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "could not pre-validate" in r.stdout
    assert any("create-state-machine" in c for c in calls)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d deploy-touches test(s) passed" % len(fns))
