#!/usr/bin/env python3
"""Offline tests for the "use the customer's existing IAM roles" work (manage_iam / the three
role-ARN params). No AWS, no network: a fake `aws` on PATH (tests/fake_aws/aws) logs every call
and returns canned responses steered by env vars.

Covered (mirrors the task's test list):
  P*  params_csv: new keys parsed; derived defaults; NOT in pipeline.json; glue_role_arn stays;
      role_name_from_arn (incl. a path); validation (bad ARN, other account, manage_iam bool).
  A   manage_iam=false makes ZERO IAM write calls (only get-role + simulate reads).
  B   Lambdas get --role <lambda_role_arn>, state machines --role-arn <sfn_role_arn>.
  C   the Lambda policy's iam:PassRole is filled with the Glue role name (existing-glue).
  D   a missing role fails BEFORE any non-IAM (lambda/glue/stepfunctions/s3-write) call.
  E   wrong trust fails with the exact trust-statement message, before any change.
  F   a role given with a PATH works (iam calls use the last segment; Lambdas use the full ARN).
  G   simulate denied -> WARNING + stop (no create); --skip-permission-check -> continue.
  H   simulate CALL denied -> WARNING naming the actions + stop.
  I   manage_iam=true with defaulted ARNs is byte-identical (DRYRUN AWS commands) to the
      pre-change setup.sh (git HEAD:tools/setup.sh).
  J   manage_iam=true with a custom glue_role_arn targets THAT role name (not the default).
  K   legacy objects are REPORTED (startup-*/cutover-* machines, untagged <project>-* Glue jobs)
      with a delete command, and never actually deleted.
  L   --dry-run works for both modes and shows the role each Lambda/state machine uses.

Run: python3 tests/test_existing_roles.py   (REPO_DIR overridable). Exit 0 = clean.
"""
import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
FAKE_AWS_DIR = os.path.join(HERE, "fake_aws")
SETUP = os.path.join(REPO, "tools", "setup.sh")
sys.path.insert(0, os.path.join(REPO, "lambdas"))
import params_csv as pc  # noqa: E402

_passed = 0
_failed = 0


def check(cond, msg):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"[PASS] {msg}")
    else:
        _failed += 1
        print(f"[FAIL] {msg}")


# --------------------------------------------------------------------------------------------
# Helpers to run setup.sh with the fake aws and read back the JSON call log.
# --------------------------------------------------------------------------------------------
BASE_PARAMS = {
    "account_id": "111122223333",
    "region": "us-east-1",
    "project": "dms-dsql",
    "dsql_endpoint": "abcd0efgh1ijkl2mnop3qrstuv.dsql.us-east-1.on.aws",
}


def write_params(path, extra):
    rows = ["parameter,value"]
    merged = dict(BASE_PARAMS)
    merged.update(extra)
    for k, v in merged.items():
        rows.append(f"{k},{v}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows) + "\n")


def run_setup(extra_params, args=(), env=None, dry_run=False):
    """Run tools/setup.sh with a fake aws. Returns (returncode, stdout+stderr, calls list)."""
    tmp = tempfile.mkdtemp(prefix="er_test_")
    params = os.path.join(tmp, "params.csv")
    write_params(params, extra_params)
    log = os.path.join(tmp, "calls.jsonl")
    e = dict(os.environ)
    e["PATH"] = FAKE_AWS_DIR + os.pathsep + e.get("PATH", "")
    e["FAKE_AWS_LOG"] = log
    e["AWS_PAGER"] = ""
    if env:
        e.update(env)
    cmd = ["bash", SETUP, params, "--bucket", "my-bucket", *args]
    if dry_run:
        cmd.append("--dry-run")
    p = subprocess.run(cmd, cwd=REPO, env=e, capture_output=True, text=True)
    calls = []
    if os.path.exists(log):
        for ln in open(log, encoding="utf-8"):
            ln = ln.strip()
            if ln:
                calls.append(json.loads(ln)["argv"])
    return p.returncode, (p.stdout + p.stderr), calls


IAM_WRITE_OPS = {"create-role", "put-role-policy", "update-assume-role-policy",
                 "attach-role-policy", "detach-role-policy", "delete-role",
                 "delete-role-policy", "create-policy", "put-role-permissions-boundary"}


def iam_writes(calls):
    return [c for c in calls if len(c) >= 2 and c[0] == "iam"
            and (c[1] in IAM_WRITE_OPS or c[1].startswith("delete-"))]


def non_iam_mutations(calls):
    """Any lambda/glue/stepfunctions create/update or s3 upload — i.e. a real resource change."""
    out = []
    for c in calls:
        if len(c) < 2:
            continue
        svc, op = c[0], c[1]
        if svc == "lambda" and op in ("create-function", "update-function-code",
                                      "update-function-configuration"):
            out.append(c)
        elif svc == "stepfunctions" and op in ("create-state-machine", "update-state-machine"):
            out.append(c)
        elif svc == "glue" and op in ("create-job", "update-job", "create-connection",
                                      "update-connection"):
            out.append(c)
        elif svc == "s3" and op == "cp" and len(c) > 2 and not c[2].startswith("s3://"):
            out.append(c)  # an upload (local -> s3)
    return out


def opt(call, name):
    if name in call:
        i = call.index(name)
        if i + 1 < len(call):
            return call[i + 1]
    return None


FALSE_PARAMS = {
    "manage_iam": "false",
    "glue_role_arn": "arn:aws:iam::111122223333:role/existing-glue",
    "lambda_role_arn": "arn:aws:iam::111122223333:role/existing-lambda",
    "sfn_role_arn": "arn:aws:iam::111122223333:role/existing-sfn",
}


# ============================================================================================
# P — params_csv.py unit checks (no setup.sh)
# ============================================================================================
def test_params():
    for k in ("lambda_role_arn", "sfn_role_arn", "manage_iam"):
        check(k in pc.ALLOWED, f"P: {k} is an allowed params key")
        check(k not in pc.PIPELINE_KEYS, f"P: {k} is NOT written to pipeline.json")
    check("glue_role_arn" in pc.PIPELINE_KEYS, "P: glue_role_arn STAYS in pipeline.json")

    r = pc.parse("parameter,value\naccount_id,111122223333\nregion,us-east-1\n"
                 "project,dms-dsql\ndsql_endpoint,abcd.dsql.us-east-1.on.aws\n")
    check(not r["errors"], "P: minimal CSV parses clean")
    p = r["params"]
    check(p["lambda_role_arn"] == "arn:aws:iam::111122223333:role/dms-dsql-lambda-exec-role",
          "P: lambda_role_arn derived default")
    check(p["sfn_role_arn"] == "arn:aws:iam::111122223333:role/dms-dsql-sfn-exec-role",
          "P: sfn_role_arn derived default")
    check(p["manage_iam"] == "true", "P: manage_iam default is true")
    settings = pc.to_pipeline_settings(p)
    check(not any(k in settings for k in ("lambda_role_arn", "sfn_role_arn", "manage_iam")),
          "P: new setup-only keys absent from pipeline settings")
    check(settings["glue_role_arn"].endswith("dms-dsql-glue-exec-role"),
          "P: glue_role_arn present in settings")

    check(pc.role_name_from_arn("arn:aws:iam::111122223333:role/existing-lambda") == "existing-lambda",
          "P: role_name_from_arn plain name")
    check(pc.role_name_from_arn("arn:aws:iam::111122223333:role/a/b/existing-glue") == "existing-glue",
          "P: role_name_from_arn PATH -> last segment")

    def errs(extra):
        text = ("parameter,value\naccount_id,111122223333\nregion,us-east-1\n"
                "project,dms-dsql\ndsql_endpoint,abcd.dsql.us-east-1.on.aws\n" + extra)
        return pc.parse(text)["errors"]

    check(any("IAM role ARN" in e for e in errs("lambda_role_arn,nope\n")),
          "P: bad role ARN rejected")
    check(any("same account" in e for e in
              errs("sfn_role_arn,arn:aws:iam::999988887777:role/x\n")),
          "P: role ARN in another account rejected")
    check(any("manage_iam must be true or false" in e for e in errs("manage_iam,maybe\n")),
          "P: manage_iam non-boolean rejected")
    check(not errs("lambda_role_arn,arn:aws:iam::111122223333:role/dept/team/x\n"),
          "P: role ARN with a path accepted")


# ============================================================================================
# manage_iam=false behaviour (A–H via setup.sh + fake aws)
# ============================================================================================
def test_false_happy_path():
    rc, out, calls = run_setup(FALSE_PARAMS)
    check(rc == 0, "A: manage_iam=false run succeeds")
    check(iam_writes(calls) == [], "A: ZERO IAM write calls in manage_iam=false")
    # only reads: get-role + simulate (+ maybe list-*).
    iam_ops = {c[1] for c in calls if c and c[0] == "iam"}
    check(iam_ops <= {"get-role", "simulate-principal-policy", "list-role-policies",
                      "list-attached-role-policies"},
          f"A: only IAM READ ops used (saw {sorted(iam_ops)})")

    lam = [c for c in calls if c[:2] == ["lambda", "create-function"]]
    check(len(lam) == 8, "B: all 8 Lambdas created")
    check(all(opt(c, "--role") == FALSE_PARAMS["lambda_role_arn"] for c in lam),
          "B: every Lambda uses --role <lambda_role_arn> (existing-lambda)")
    sm = [c for c in calls if c[:2] == ["stepfunctions", "create-state-machine"]]
    check(len(sm) == 4, "B: all 4 state machines created")
    check(all(opt(c, "--role-arn") == FALSE_PARAMS["sfn_role_arn"] for c in sm),
          "B: every state machine uses --role-arn <sfn_role_arn> (existing-sfn)")

    # C: PassRole filled with existing-glue in the written policy file.
    pol = os.path.join(REPO, "iam-out", "existing-lambda.policy.json")
    check(os.path.exists(pol), "C: iam-out/existing-lambda.policy.json written")
    doc = json.load(open(pol))
    passrole = [s for s in doc.get("Statement", []) if s.get("Sid") == "PassGlueRoleToCreatedJobs"]
    ok = passrole and passrole[0]["Resource"] == ["arn:aws:iam::111122223333:role/existing-glue"]
    check(bool(ok), "C: Lambda policy iam:PassRole names the Glue role existing-glue")
    rd = os.path.join(REPO, "iam-out", "README-IAM.txt")
    check(os.path.exists(rd), "C: README-IAM.txt written")
    rdtxt = open(rd).read() if os.path.exists(rd) else ""
    check("existing-glue" in rdtxt and "existing-lambda" in rdtxt and "existing-sfn" in rdtxt,
          "C: README-IAM.txt lists all three roles")


def test_false_missing_role():
    rc, out, calls = run_setup(FALSE_PARAMS, env={"FAKE_MISSING_ROLES": "existing-lambda"})
    check(rc != 0, "D: missing role fails (non-zero)")
    check("existing-lambda" in out and "do not exist" in out, "D: failure names the missing role")
    check(iam_writes(calls) == [], "D: no IAM writes on the missing-role path")
    check(non_iam_mutations(calls) == [],
          "D: NO lambda/glue/sfn/s3 change before the missing-role failure")


def test_false_wrong_trust():
    rc, out, calls = run_setup(FALSE_PARAMS, env={"FAKE_TRUST_EXISTING_SFN": "NONE"})
    check(rc != 0, "E: wrong trust fails (non-zero)")
    check("does not trust states.amazonaws.com" in out,
          "E: failure says the sfn role does not trust states.amazonaws.com")
    check('"Service": "states.amazonaws.com"' in out and "sts:AssumeRole" in out,
          "E: prints the exact trust statement to add")
    check(non_iam_mutations(calls) == [], "E: nothing created before the trust failure")


def test_false_path_role():
    extra = dict(FALSE_PARAMS)
    extra["glue_role_arn"] = "arn:aws:iam::111122223333:role/dept/team/existing-glue"
    extra["lambda_role_arn"] = "arn:aws:iam::111122223333:role/dept/existing-lambda"
    rc, out, calls = run_setup(extra)
    check(rc == 0, "F: role-with-a-path run succeeds")
    getroles = {opt(c, "--role-name") for c in calls if c[:2] == ["iam", "get-role"]}
    check({"existing-glue", "existing-lambda", "existing-sfn"} <= getroles,
          "F: iam get-role uses the LAST path segment as the role name")
    lam = [c for c in calls if c[:2] == ["lambda", "create-function"]]
    check(lam and all(opt(c, "--role") == extra["lambda_role_arn"] for c in lam),
          "F: Lambdas still use the FULL path ARN as --role")


def test_false_simulate_denied():
    rc, out, calls = run_setup(FALSE_PARAMS, env={"FAKE_SIM_DENY": "glue:CreateJob"})
    check(rc != 0, "G: simulate-denied stops (non-zero)")
    check("WARNING" in out and "stopping before creating anything" in out,
          "G: simulate-denied prints WARNING and stops")
    check([c for c in calls if c[:2] == ["lambda", "create-function"]] == [],
          "G: NO Lambda created when simulate denies")

    rc2, out2, calls2 = run_setup(FALSE_PARAMS, args=("--skip-permission-check",),
                                  env={"FAKE_SIM_DENY": "glue:CreateJob"})
    check(rc2 == 0, "G: --skip-permission-check continues (zero exit)")
    check("continuing despite" in out2, "G: --skip-permission-check prints the override note")
    check(len([c for c in calls2 if c[:2] == ["lambda", "create-function"]]) == 8,
          "G: with --skip-permission-check all 8 Lambdas are created")
    check(iam_writes(calls2) == [], "G: still ZERO IAM writes even when continuing")


def test_false_simulate_call_denied():
    rc, out, calls = run_setup(FALSE_PARAMS, env={"FAKE_SIM_CALL_DENY": "existing-glue"})
    check(rc != 0, "H: simulate CALL denied stops (non-zero)")
    check("could not run iam simulate-principal-policy" in out,
          "H: warns the simulate call itself was denied")
    check("Could not verify these actions" in out and "s3:GetObject" in out,
          "H: lists the actions it could not verify")


# ============================================================================================
# manage_iam=true (I, J)
# ============================================================================================
def _dryrun_aws_cmds(setup_path, params_path):
    e = dict(os.environ)
    e["AWS_PAGER"] = ""
    p = subprocess.run(["bash", setup_path, params_path, "--bucket", "my-bucket", "--dry-run"],
                       cwd=REPO, env=e, capture_output=True, text=True)
    lines = [ln for ln in p.stdout.splitlines() if ln.startswith("DRYRUN")]
    # normalise the per-run mktemp dir so only the AWS command content is compared.
    norm = []
    for ln in lines:
        ln = re.sub(r"(/var/folders/\S+?/T/tmp\.[A-Za-z0-9]+|/tmp/tmp\.[A-Za-z0-9]+|"
                    r"/tmp/setup\.[A-Za-z0-9]+)", "TMPDIR", ln)
        norm.append(ln)
    return norm


def test_true_byte_identical():
    tmp = tempfile.mkdtemp(prefix="er_bid_")
    params = os.path.join(tmp, "params.csv")
    # Fully-defaulted ARNs + a VPC connection to exercise the glue-vpc branch too.
    write_params(params, {"glue_connection": "dms-dsql-vpc",
                          "subnet_id": "subnet-0abc1234def567890",
                          "security_group_id": "sg-0abc1234def567890"})
    new_cmds = _dryrun_aws_cmds(SETUP, params)
    # original setup.sh from git HEAD
    orig = subprocess.run(["git", "show", "HEAD:tools/setup.sh"], cwd=REPO,
                          capture_output=True, text=True)
    if orig.returncode != 0:
        check(False, "I: could not read HEAD:tools/setup.sh for the byte-identical check")
        return
    orig_path = os.path.join(tmp, "orig_setup.sh")
    with open(orig_path, "w", encoding="utf-8") as f:
        f.write(orig.stdout)
    orig_cmds = _dryrun_aws_cmds(orig_path, params)
    check(new_cmds == orig_cmds,
          "I: manage_iam=true + defaults = BYTE-IDENTICAL DRYRUN AWS commands vs pre-change setup")
    if new_cmds != orig_cmds:
        import difflib
        for d in list(difflib.unified_diff(orig_cmds, new_cmds, "orig", "new"))[:20]:
            print("      " + d)


def test_true_custom_role_name():
    rc, out, calls = run_setup({"manage_iam": "true",
                                "glue_role_arn": "arn:aws:iam::111122223333:role/my-custom-glue"})
    check(rc == 0, "J: manage_iam=true custom glue role run succeeds")
    # fake get-role returns success -> update path; the custom name must be targeted.
    targeted = {opt(c, "--role-name") for c in calls
                if c and c[0] == "iam" and c[1] in ("create-role", "update-assume-role-policy")}
    check("my-custom-glue" in targeted, "J: the CUSTOM glue role name is created/updated")
    check("dms-dsql-glue-exec-role" not in targeted,
          "J: the DEFAULT glue role name is NOT touched when a custom ARN is given")


# ============================================================================================
# K — legacy objects reported, never deleted
# ============================================================================================
def test_legacy_objects():
    rc, out, calls = run_setup(
        FALSE_PARAMS,
        env={"FAKE_LEGACY_SMS": "dms-dsql-startup-task-full-cdc-01 dms-dsql-cutover-task-x",
             "FAKE_GLUE_JOBS": "dms-dsql-oldjob dms-dsql-load",
             "FAKE_GLUE_TAGGED": "dms-dsql-load"})
    check(rc == 0, "K: run with legacy objects still succeeds")
    check("dms-dsql-startup-task-full-cdc-01" in out and "delete-state-machine" in out,
          "K: legacy per-task state machines reported with delete command")
    check("dms-dsql-oldjob" in out and "delete-job" in out,
          "K: untagged legacy Glue job reported with delete command")
    check("dms-dsql-load" not in out.split("delete-job")[-1] if "delete-job" in out else True,
          "K: the TAGGED job (dms-dsql-load) is not listed for deletion")
    deletes = [c for c in calls if len(c) >= 2 and c[1] in ("delete-state-machine", "delete-job")]
    check(deletes == [], "K: NOTHING is actually deleted")


# ============================================================================================
# L — --dry-run both modes shows the role each Lambda / state machine uses
# ============================================================================================
def test_dry_run_both_modes():
    rc, out, calls = run_setup(FALSE_PARAMS, dry_run=True)
    check(rc == 0, "L: manage_iam=false --dry-run succeeds")
    check("--role arn:aws:iam::111122223333:role/existing-lambda" in out,
          "L(false): dry-run shows Lambdas will use existing-lambda")
    check("--role-arn arn:aws:iam::111122223333:role/existing-sfn" in out,
          "L(false): dry-run shows state machines will use existing-sfn")
    # a dry-run makes no real AWS calls (fake aws never invoked).
    check(calls == [], "L(false): dry-run invokes no aws calls at all")

    rc2, out2, calls2 = run_setup({"manage_iam": "true"}, dry_run=True)
    check(rc2 == 0, "L: manage_iam=true --dry-run succeeds")
    check("role/dms-dsql-lambda-exec-role" in out2 and "role/dms-dsql-sfn-exec-role" in out2,
          "L(true): dry-run shows the default lambda/sfn roles each resource uses")


def main():
    test_params()
    test_false_happy_path()
    test_false_missing_role()
    test_false_wrong_trust()
    test_false_path_role()
    test_false_simulate_denied()
    test_false_simulate_call_denied()
    test_true_byte_identical()
    test_true_custom_role_name()
    test_legacy_objects()
    test_dry_run_both_modes()
    # clean the runtime output dir this suite caused setup.sh to write.
    import shutil
    shutil.rmtree(os.path.join(REPO, "iam-out"), ignore_errors=True)
    for fn in os.listdir(os.path.join(REPO, "iam")):
        if fn.endswith(".filled.json"):
            os.remove(os.path.join(REPO, "iam", fn))
    print(f"\n==== existing-roles: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
