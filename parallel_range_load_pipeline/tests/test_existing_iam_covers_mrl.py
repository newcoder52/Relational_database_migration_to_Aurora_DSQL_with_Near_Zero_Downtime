#!/usr/bin/env python3
"""Amendment 10 B.1 — prove the EXISTING roles (iam/lambda.json, iam/stepfunctions.json,
iam/glue.json at ../iam) already cover every IAM action the DEFAULT MRL deployment needs (ZERO new
IAM), and report the OPTIONAL fleet-state-machine gap explicitly.

Two halves:
  1. SCANNER: walk lambdas/*.py + scripts/*.py for boto3 client service methods and map each to an
     IAM action via _METHOD_ACTION. Any boto3 method on a known service that is NOT in the map
     fails the test (so a new call cannot silently need an un-reviewed action). Cross-check that
     every scanned action appears in tools/mrl_required_actions.json (drift guard).
  2. EVALUATOR: a tiny IAM policy evaluator (Allow only, Action wildcard, Resource wildcard/ARN
     glob, Condition StringLike s3:prefix + iam:PassedToService honoured; any Deny => fail) over
     the three base policies with <<...>> placeholders substituted. Assert the DEFAULT required
     actions are ALLOWED on their role; assert the FLEET-SM extras are NOT allowed (the named gap).
"""
import fnmatch
import json
import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
BASE_IAM = os.path.join(REPO, "iam")
REQ = json.load(open(os.path.join(PKG, "tools", "mrl_required_actions.json")))

SUBS = {"<<PROJECT>>": "demo", "<<BUCKET>>": "demo-bucket", "<<REGION>>": "us-east-1",
        "<<ACCOUNT_ID>>": "123456789012", "<<DSQL_CLUSTER_ID>>": "clstrid",
        "<<GLUE_EXEC_ROLE_NAME>>": "demo-glue-exec-role"}

# boto3 client method -> IAM action, per service. A method NOT here (on a known service) fails.
_METHOD_ACTION = {
    "s3": {"get_object": "s3:GetObject", "put_object": "s3:PutObject",
           "list_objects_v2": "s3:ListBucket", "delete_object": "s3:DeleteObject",
           "head_object": "s3:GetObject", "get_paginator": None},
    "glue": {"create_job": "glue:CreateJob", "update_job": "glue:UpdateJob",
             "get_job": "glue:GetJob", "get_tags": "glue:GetTags",
             "tag_resource": "glue:TagResource", "start_job_run": "glue:StartJobRun",
             "get_job_run": "glue:GetJobRun", "batch_stop_job_run": "glue:BatchStopJobRun"},
    "dms": {"describe_replication_tasks": "dms:DescribeReplicationTasks",
            "describe_endpoints": "dms:DescribeEndpoints",
            "describe_table_statistics": "dms:DescribeTableStatistics",
            "start_replication_task": "dms:StartReplicationTask"},
    "stepfunctions": {"list_executions": "states:ListExecutions",
                      "describe_execution": "states:DescribeExecution",
                      "start_execution": "states:StartExecution"},
    "dsql": {"get_vpc_endpoint_service_name": "dsql:GetVpcEndpointServiceName",
             "generate_db_connect_admin_auth_token": "dsql:DbConnectAdmin"},
}
_KNOWN_SERVICES = set(_METHOD_ACTION)


def _subst(s):
    for a, b in SUBS.items():
        s = s.replace(a, b)
    return s


# -------- 1. scanner ----------------------------------------------------------------------
def _scan_boto3_methods():
    """Return {service: set(methods)} called in lambdas/*.py + scripts/*.py. Heuristic: find
    client-variable = boto3.client("svc") (or client("svc")) bindings, then var.method( calls."""
    svc_methods = {}
    files = ([os.path.join(PKG, "lambdas", f) for f in os.listdir(os.path.join(PKG, "lambdas"))
              if f.endswith(".py")] +
             [os.path.join(PKG, "scripts", f) for f in os.listdir(os.path.join(PKG, "scripts"))
              if f.endswith(".py")])
    client_re = re.compile(r'(\w+)\s*=\s*(?:boto3\.)?client\(\s*["\']([a-z0-9-]+)["\']')
    for fp in files:
        src = open(fp).read()
        var_svc = {}
        for m in client_re.finditer(src):
            var_svc[m.group(1)] = m.group(2)
        # also map make_boto_client("svc") factory results used inline like glue=make_boto_client("glue")
        for m in re.finditer(r'(\w+)\s*=\s*make_boto_client\(\s*["\']([a-z0-9-]+)["\']', src):
            var_svc[m.group(1)] = m.group(2)
        for var, svc in var_svc.items():
            for mm in re.finditer(r'\b%s\.([a-z_]+)\(' % re.escape(var), src):
                svc_methods.setdefault(svc, set()).add(mm.group(1))
    return svc_methods


def test_scanner_every_boto3_method_is_mapped():
    scanned = _scan_boto3_methods()
    unmapped = []
    for svc, methods in scanned.items():
        if svc not in _KNOWN_SERVICES:
            # a brand-new service client => must be reviewed
            unmapped.append("%s.* (unknown service)" % svc)
            continue
        for mth in methods:
            if mth not in _METHOD_ACTION[svc]:
                unmapped.append("%s.%s" % (svc, mth))
    assert not unmapped, ("boto3 method(s) with no IAM-action mapping (review + add to "
                          "_METHOD_ACTION and mrl_required_actions.json): %s" % sorted(unmapped))


def test_scanned_actions_are_declared_in_required_actions():
    """Every IAM action the scanner derives must be declared somewhere in mrl_required_actions.json
    (default or fleet_sm_optional) - so the committed list cannot drift below the real calls."""
    declared = set()
    for scope in REQ:
        if scope.startswith("_"):
            continue
        for role in REQ[scope]:
            for e in REQ[scope][role]:
                declared.add(e["action"])
    scanned_actions = set()
    for svc, methods in _scan_boto3_methods().items():
        for mth in methods:
            a = _METHOD_ACTION.get(svc, {}).get(mth)
            if a:
                scanned_actions.add(a)
    missing = sorted(a for a in scanned_actions if a not in declared)
    assert not missing, "actions used in code but not declared in mrl_required_actions.json: %s" % missing


# -------- 2. evaluator --------------------------------------------------------------------
def _load_policy(fname):
    doc = json.loads(_subst(open(os.path.join(BASE_IAM, fname)).read()))
    stmts = list(doc["Policy"]["Statement"])
    if "VpcPolicy" in doc:
        stmts += doc["VpcPolicy"]["Statement"]
    return stmts


def _action_matches(statement_actions, action):
    acts = statement_actions if isinstance(statement_actions, list) else [statement_actions]
    for a in acts:
        if a == "*" or a == action:
            return True
        if a.endswith(":*") and action.startswith(a[:-1]):
            return True
        if "*" in a and fnmatch.fnmatch(action, a):
            return True
    return False


def _resource_matches(statement_resources, resource):
    res = statement_resources if isinstance(statement_resources, list) else [statement_resources]
    for r in res:
        if r == "*" or r == resource:
            return True
        if fnmatch.fnmatch(resource, r):
            return True
    return False


def _cond_ok(stmt, req):
    """Honour the two conditions MRL relies on: s3:prefix StringLike and iam:PassedToService."""
    cond = stmt.get("Condition") or {}
    # s3 ListBucket prefix
    want_prefix = req.get("condition_prefix")
    if want_prefix:
        sl = (cond.get("StringLike") or {}).get("s3:prefix")
        if sl is None:
            return True  # an unconditioned ListBucket is broader -> still allowed
        pats = sl if isinstance(sl, list) else [sl]
        return any(fnmatch.fnmatch(want_prefix.rstrip("*") or want_prefix, p) or
                   p == want_prefix or fnmatch.fnmatch(want_prefix, p) for p in pats)
    # iam:PassRole PassedToService
    if req["action"] == "iam:PassRole":
        se = (cond.get("StringEquals") or {}).get("iam:PassedToService")
        return se in (None, "glue.amazonaws.com") or (isinstance(se, list) and
                                                       "glue.amazonaws.com" in se)
    return True


def _is_allowed(stmts, req):
    denied = False
    allowed = False
    for st in stmts:
        if not _action_matches(st.get("Action", []), req["action"]):
            continue
        if not _resource_matches(st.get("Resource", []), req["resource"]):
            continue
        if st.get("Effect") == "Deny":
            denied = True
        elif st.get("Effect") == "Allow" and _cond_ok(st, req):
            allowed = True
    return allowed and not denied


_ROLE_FILE = {"lambda": "lambda.json", "glue": "glue.json", "stepfunctions": "stepfunctions.json"}


@pytest.mark.skipif(not os.path.isdir(BASE_IAM),
                    reason="base repo iam/ not present (../iam)")
def test_existing_roles_cover_default_mrl_deployment():
    """ZERO new IAM: every DEFAULT required action is ALLOWED by the existing role policy."""
    gaps = []
    for role, reqs in REQ["default"].items():
        stmts = _load_policy(_ROLE_FILE[role])
        for req in reqs:
            r = dict(req)
            r["action"] = _subst(req["action"])
            r["resource"] = _subst(req["resource"])
            if not _is_allowed(stmts, r):
                gaps.append("%s role: %s on %s NOT allowed (%s)"
                            % (role, r["action"], r["resource"], req.get("why", "")))
    assert not gaps, ("DEFAULT MRL deployment is NOT covered by the existing roles "
                      "(would need new IAM):\n  - " + "\n  - ".join(gaps))


@pytest.mark.skipif(not os.path.isdir(BASE_IAM), reason="base repo iam/ not present")
def test_fleet_sm_gap_is_named_and_uncovered():
    """The OPTIONAL fleet state machine's child hand-off is the ONLY gap: assert it is NOT covered
    by the existing SFN role (so it is correctly called out as needing iam/mrl-fleet-sm-optional.json)."""
    stmts = _load_policy(_ROLE_FILE["stepfunctions"])
    uncovered = []
    for req in REQ["fleet_sm_optional"]["stepfunctions"]:
        r = dict(req)
        r["action"] = _subst(req["action"])
        r["resource"] = _subst(req["resource"])
        if not _is_allowed(stmts, r):
            uncovered.append("%s on %s" % (r["action"], r["resource"]))
    # the StartExecution-on-mrl-task + the StepFunctions-execution event rule must be uncovered
    assert any("states:StartExecution" in u and "mrl-task" in u for u in uncovered), \
        "expected StartExecution on <project>-mrl-task to be the uncovered fleet-SM gap"
    assert any("StepFunctionsGetEventsForStepFunctionsExecutionRule" in u for u in uncovered)


@pytest.mark.skipif(not os.path.isdir(BASE_IAM), reason="base repo iam/ not present")
def test_mutation_unmapped_call_breaks_coverage():
    """Mutation: a required action that the existing roles do NOT grant (e.g. iam:CreateRole on the
    Lambda role) must be reported as a gap by the evaluator (proves the test can fail)."""
    stmts = _load_policy(_ROLE_FILE["lambda"])
    bogus = {"action": "iam:CreateRole", "resource": "*", "why": "mutation"}
    assert not _is_allowed(stmts, bogus), "evaluator must not allow an ungranted action"


def test_iam_check_script_is_readonly_and_uses_required_actions():
    """tools/mrl_iam_check.sh is read-only (no mutating aws calls) and reads the SAME
    mrl_required_actions.json this test uses (one source of truth)."""
    sh = open(os.path.join(PKG, "tools", "mrl_iam_check.sh")).read()
    assert "mrl_required_actions.json" in sh, "iam-check must read the required-actions JSON"
    assert "simulate-principal-policy" in sh, "iam-check must use simulate-principal-policy"
    assert "NO IAM CHANGE NEEDED" in sh
    import re as _re
    mutating = _re.findall(r"aws (?:iam (?:create|put|attach|delete|update)|lambda (?:create|update|"
                           r"delete)|glue (?:create|update|delete)|stepfunctions (?:create|update|"
                           r"delete)|s3 (?:cp|rm|mb))", sh)
    assert not mutating, "iam-check must be read-only (found mutating aws call(s): %s)" % mutating


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn()
            print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
