#!/usr/bin/env python3
"""Amendment 18 - the glueetl mrl-load job must get a DSQL-aware boto3 via
--additional-python-modules (boto3/botocore/s3transfer wheels from the driver-cdc set), the SAME
mechanism the original create_glue_jobs uses; --extra-py-files stays the pg8000 stack only (boto3
there breaks botocore under Spark). Glue 4.0's bundled botocore has no 'dsql' service, which is why
the live mrl-load crashed with `UnknownServiceError: Unknown service: 'dsql'`.
"""
import ast
import json
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_ensure_job as ej          # noqa: E402
from mrl_fakes import FakeS3, FakeGlue  # noqa: E402

# the driver-cdc prepared set (as live: boto3-1.42.97 etc.) passed as cdcExtraPyFiles
_CDC = ",".join("s3://b/driver-cdc-prepared/abc/%s" % w for w in (
    "asn1crypto-1.5.1-py2.py3-none-any.whl", "boto3-1.42.97-py3-none-any.whl",
    "botocore-1.42.97-py3-none-any.whl", "jmespath-1.0.1-py3-none-any.whl",
    "pg8000-1.31.2-py3-none-any.whl", "python_dateutil-2.9.0-py2.py3-none-any.whl",
    "s3transfer-0.16.1-py3-none-any.whl", "scramp-1.4.5-py3-none-any.whl",
    "six-1.17.0-py2.py3-none-any.whl", "urllib3-1.26.20-py2.py3-none-any.whl"))
# the driver-fullload set (pg8000 stack only, NO boto3) passed as extraPyFiles
_FULLLOAD = ",".join("s3://b/driver-fullload/%s" % w for w in (
    "asn1crypto-1.5.1-py2.py3-none-any.whl", "pg8000-1.31.2-py3-none-any.whl",
    "python_dateutil-2.9.0-py2.py3-none-any.whl", "scramp-1.4.5-py3-none-any.whl",
    "six-1.17.0-py2.py3-none-any.whl"))


def _run_ensure(cdc=_CDC, extra=_FULLLOAD):
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key="glue-templates/mrl-load.json",
                  Body=json.dumps({"role": "mrl-load", "script": "job2_load_manual_range.py",
                                   "command_name": "glueetl", "worker_type": "G.4X",
                                   "number_of_workers": 10, "timeout_minutes": 2880,
                                   "glue_version": "4.0", "default_arguments": {}}).encode())
    glue = FakeGlue()
    ev = {"bucket": "b", "project": "demo", "taskSuffix": "t", "region": "us-east-1",
          "accountId": "123456789012", "glue_templates_prefix": "glue-templates",
          "scripts_prefix": "scripts",
          "glue_role_arn": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
          "dsql_endpoint": "c.dsql.us-east-1.on.aws", "dsql_user": "admin",
          "dsql_database": "postgres", "maxTotalConcurrentRuns": 4,
          "extraPyFiles": extra, "cdcExtraPyFiles": cdc}
    for mod in (ej,):
        mod.boto3 = type("b", (), {"client": staticmethod(
            lambda svc, **k: s3 if svc == "s3" else glue)})
    out = ej.handler(ev, None)
    created = glue.jobs[out["jobName"]]
    return out, created, glue


def test_a18_mrl_load_has_additional_python_modules_with_boto3():
    out, job, glue = _run_ensure()
    da = job.get("DefaultArguments", {})
    apm = da.get("--additional-python-modules", "")
    assert apm, "mrl-load must set --additional-python-modules (DSQL-aware boto3)"
    mods = [m.rsplit("/", 1)[-1] for m in apm.split(",")]
    assert any(m.startswith("boto3-") for m in mods), apm
    assert any(m.startswith("botocore-") for m in mods), apm
    assert any(m.startswith("s3transfer-") for m in mods), apm
    assert all(m.startswith("s3://") for m in apm.split(",")), "must be S3 wheel URIs (no PyPI)"
    # --extra-py-files is the pg8000 stack only - NO boto3/botocore there (breaks Spark botocore)
    epf = da.get("--extra-py-files", "")
    assert "boto3-" not in epf and "botocore-" not in epf, epf
    assert "pg8000-" in epf


def test_a18_fails_closed_when_no_boto3_wheel():
    try:
        _run_ensure(cdc=_FULLLOAD)   # driver-fullload has NO boto3 -> must fail closed
        assert False, "must fail closed when cdcExtraPyFiles has no boto3 wheel"
    except ej.MrlEnsureJobError as e:
        assert "boto3/botocore/s3transfer" in str(e) and "dsql" in str(e)


def test_a18_mutation_removing_additional_modules_fails():
    """Mutation: drop the --additional-python-modules block from mrl_ensure_job -> the job no
    longer delivers a dsql-aware boto3 (the live defect)."""
    src = open(ej.__file__).read()
    assert "--additional-python-modules" in src
    mutated = src.replace('args["--additional-python-modules"] = _mods',
                          'pass  # mutated: no additional-python-modules')
    assert mutated != src
    ns = {}
    exec(compile(mutated, ej.__file__, "exec"), ns)
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key="glue-templates/mrl-load.json",
                  Body=json.dumps({"role": "mrl-load", "script": "job2_load_manual_range.py",
                                   "command_name": "glueetl", "default_arguments": {}}).encode())
    glue = FakeGlue()
    ns["boto3"] = type("b", (), {"client": staticmethod(
        lambda svc, **k: s3 if svc == "s3" else glue)})
    out = ns["handler"]({"bucket": "b", "project": "demo", "taskSuffix": "t",
                         "region": "us-east-1", "glue_role_arn": "arn:r",
                         "dsql_endpoint": "c", "dsql_user": "admin", "dsql_database": "postgres",
                         "maxTotalConcurrentRuns": 4, "extraPyFiles": _FULLLOAD,
                         "cdcExtraPyFiles": _CDC}, None)
    da = glue.jobs[out["jobName"]].get("DefaultArguments", {})
    assert "--additional-python-modules" not in da   # mutation removed the fix


# ---- byte-identical sync of the copied helper -----------------------------------------------
def _extract_func(path, name):
    src = open(path).read()
    tree = ast.parse(src)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return ast.get_source_segment(src, n)
    return None


def test_spark_boto3_modules_byte_identical():
    orig = _extract_func(os.path.join(REPO, "lambdas", "create_glue_jobs.py"),
                         "_spark_boto3_modules")
    copy = _extract_func(os.path.join(PKG, "lambdas", "mrl_ensure_job.py"),
                         "_spark_boto3_modules")
    assert orig is not None and copy is not None
    assert copy == orig, "mrl_ensure_job._spark_boto3_modules drifted from create_glue_jobs"
    # the prefix tuple too
    o = open(os.path.join(REPO, "lambdas", "create_glue_jobs.py")).read()
    c = open(os.path.join(PKG, "lambdas", "mrl_ensure_job.py")).read()
    pref = '_SPARK_BOTO3_PREFIXES = ("boto3-", "botocore-", "s3transfer-")'
    assert pref in o and pref in c


# ---- parity vs the ORIGINAL _spark_boto3_modules -------------------------------------------
def test_a18_parity_with_original_module_delivery():
    orig_ns, mrl_ns = {}, {}
    exec(compile(_extract_func(os.path.join(REPO, "lambdas", "create_glue_jobs.py"),
                               "_spark_boto3_modules"), "o", "exec"),
         {"_SPARK_BOTO3_PREFIXES": ("boto3-", "botocore-", "s3transfer-")}, orig_ns)
    assert orig_ns["_spark_boto3_modules"](_CDC) == ej._spark_boto3_modules(_CDC)
    # and that equals exactly boto3/botocore/s3transfer, sorted
    got = ej._spark_boto3_modules(_CDC).split(",")
    assert [g.rsplit("/", 1)[-1].split("-")[0] for g in got] == sorted(["boto3", "botocore", "s3transfer"])


# ---- general runtime-capability rule: any script using dsql must get a dsql-aware boto3 -----
def test_a18_dsql_capability_rule():
    """Every MRL Glue script that creates a dsql client must have its job definition deliver a
    dsql-aware boto3: glueetl via --additional-python-modules; pythonshell via the prepared
    driver-cdc --extra-py-files set."""
    def uses_dsql(path):
        s = open(path).read()
        return ('boto3.client("dsql"' in s or "make_boto_client('dsql'" in s
                or 'make_boto_client("dsql"' in s or "client(\"dsql\"" in s
                or "generate_db_connect_admin_auth_token" in s)
    load = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
    count = os.path.join(PKG, "scripts", "job_mrl_count.py")
    assert uses_dsql(load) and uses_dsql(count)
    # glueetl load: mrl_ensure_job sets --additional-python-modules from cdcExtraPyFiles
    ejsrc = open(ej.__file__).read()
    assert "--additional-python-modules" in ejsrc and "_spark_boto3_modules" in ejsrc
    # ASL passes cdcExtraPyFiles (the driver-cdc prepared set, includes boto3)
    asl = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    assert asl["States"]["EnsureMrlLoadJob"]["Parameters"]["Payload"].get("cdcExtraPyFiles.$") \
        == "$.driverCount.extraPyFiles"
    # pythonshell count: SafetyProbe/CountVerify --extra-py-files = the prepared driver-cdc set
    for st in ("SafetyProbe", "CountVerify"):
        assert asl["States"][st]["Parameters"]["Arguments"]["--extra-py-files.$"] \
            == "$.driverCount.extraPyFiles"


# ---- botocore CreateJob/UpdateJob still valid with the new arg -----------------------------
def test_a18_botocore_createjob_accepts_additional_modules():
    bc = pytest.importorskip("botocore.session")
    from botocore.validate import validate_parameters
    sess = bc.get_session()
    sh = sess.get_service_model("glue").operation_model("CreateJob").input_shape
    out, job, glue = _run_ensure()
    da = job["DefaultArguments"]
    assert "--additional-python-modules" in da
    params = {"Name": "demo-t-mrl-load", "Role": "arn:aws:iam::123456789012:role/r",
              "Command": {"Name": "glueetl", "PythonVersion": "3",
                          "ScriptLocation": "s3://b/scripts/job2_load_manual_range.py"},
              "DefaultArguments": da, "GlueVersion": "4.0", "WorkerType": "G.4X",
              "NumberOfWorkers": 10, "Timeout": 2880}
    validate_parameters(params, sh)   # raises if invalid


# ---- deploy prerequisite: driver-cdc must contain boto3/botocore/s3transfer ----------------
def test_a18_deploy_checks_boto3_wheels():
    deploy = open(os.path.join(PKG, "tools", "deploy_mrl.sh")).read()
    assert "driver-cdc/" in deploy
    # the prerequisite check must require boto3+botocore+s3transfer (not only any .whl)
    for pkg in ("boto3", "botocore", "s3transfer"):
        assert pkg in deploy, "deploy prerequisite must check for the %s wheel (A18)" % pkg


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn(); print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
