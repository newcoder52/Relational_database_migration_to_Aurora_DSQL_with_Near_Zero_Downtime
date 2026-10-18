#!/usr/bin/env python3
"""Amendment 14.4 - because 'offline green, live broken' happened twice, validate EVERY create/
update call on the deploy path OFFLINE against the REAL botocore input shapes, including the ones
never run live: the count-job Glue update-job JSON, mrl_ensure_job's glueetl CreateJob/UpdateJob
arguments, the Lambda create-function args, and the Step Functions create/update + the new
validate-state-machine-definition op.
"""
import json
import os
import re
import sys

import pytest

try:
    import botocore.session
    from botocore.validate import validate_parameters
    _SESSION = botocore.session.get_session()
except Exception:  # pragma: no cover
    _SESSION = None

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
DEPLOY = os.path.join(PKG, "tools", "deploy_mrl.sh")


def _input_shape(service, op):
    m = _SESSION.get_service_model(service)
    return m.operation_model(op).input_shape


def _validate(service, op, params):
    """Raise ParamValidationError (via botocore) if params don't match the op's input shape."""
    validate_parameters(params, _input_shape(service, op))


# ---- the count-job Glue update-job JobUpdate (from deploy_mrl.sh, member-for-member) --------
def _count_job_update(conn=True):
    ju = {"Role": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
          "Command": {"Name": "pythonshell", "PythonVersion": "3.9",
                      "ScriptLocation": "s3://b/scripts/job_mrl_count.py"},
          "GlueVersion": "3.0", "MaxCapacity": 1.0, "Timeout": 2880,
          "ExecutionProperty": {"MaxConcurrentRuns": 10},
          "DefaultArguments": {"--enable-continuous-cloudwatch-log": "true"}}
    if conn:
        ju["Connections"] = {"Connections": ["gconn"]}
    return ju


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_count_job_update_and_create_shapes_valid():
    # update-job: JobName + JobUpdate
    _validate("glue", "UpdateJob", {"JobName": "demo-mrl-count", "JobUpdate": _count_job_update()})
    _validate("glue", "UpdateJob", {"JobName": "demo-mrl-count",
                                    "JobUpdate": _count_job_update(conn=False)})
    # create-job top-level (python-shell: MaxCapacity 1.0, NO WorkerType)
    cj = dict(_count_job_update()); cj.pop("Connections", None)
    cj["Name"] = "demo-mrl-count"
    cj["Connections"] = {"Connections": ["gconn"]}   # create-job Connections is a structure
    cj["Tags"] = {"dsql_pipeline_project": "demo", "dsql_pipeline_mrl": "count"}
    _validate("glue", "CreateJob", cj)
    # python-shell MUST NOT carry WorkerType (WorkerType is for glueetl; exclusive with MaxCapacity)
    assert "WorkerType" not in cj and "MaxCapacity" in cj


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_count_create_connections_is_struct():
    """create-job --connections shape is a STRUCTURE {"Connections":[...]} (NOT a bare list) -
    exactly what deploy_mrl.sh sends: --connections '{"Connections":["<name>"]}'."""
    sh = _input_shape("glue", "CreateJob")
    conns = sh.members["Connections"]
    assert conns.type_name == "structure" and "Connections" in conns.members
    assert conns.members["Connections"].type_name == "list"


# ---- mrl_ensure_job glueetl CreateJob / UpdateJob (the per-task mrl-load job, never run live) --
def _ensure_job_kwargs():
    return {
        "Name": "demo-t-mrl-load",
        "Role": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
        "Command": {"Name": "glueetl", "PythonVersion": "3",
                    "ScriptLocation": "s3://b/scripts/job2_load_manual_range.py"},
        "DefaultArguments": {"--job-language": "python", "--extra-py-files": "s3://b/d/pg8000.whl"},
        "Timeout": 2880, "GlueVersion": "4.0", "WorkerType": "G.4X", "NumberOfWorkers": 10,
        "ExecutionProperty": {"MaxConcurrentRuns": 5},
        "Tags": {"dsql_pipeline_project": "demo", "dsql_pipeline_task": "t"},
        "Connections": {"Connections": ["gconn"]},
    }


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_ensure_job_glueetl_create_and_update_shapes_valid():
    create = _ensure_job_kwargs()
    _validate("glue", "CreateJob", create)
    # update = job_kwargs minus Name/Tags, wrapped in JobUpdate (exactly what mrl_ensure_job sends)
    upd = {k: v for k, v in create.items() if k not in ("Name", "Tags")}
    _validate("glue", "UpdateJob", {"JobName": create["Name"], "JobUpdate": upd})
    # glueetl uses WorkerType+NumberOfWorkers and must NOT set MaxCapacity (exclusive)
    assert "WorkerType" in create and "NumberOfWorkers" in create and "MaxCapacity" not in create


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_ensure_job_kwargs_match_source():
    """Guard: the structure above mirrors lambdas/mrl_ensure_job.py (so a change there is caught)."""
    src = open(os.path.join(PKG, "lambdas", "mrl_ensure_job.py")).read()
    for member in ("Role", "Command", "DefaultArguments", "Timeout", "GlueVersion", "WorkerType",
                   "NumberOfWorkers", "ExecutionProperty", "Tags"):
        assert '"%s"' % member in src, "mrl_ensure_job no longer sets %s" % member
    assert '"Name": "glueetl"' in src
    assert "MaxCapacity" not in src, "glueetl job must not set MaxCapacity (exclusive with WorkerType)"


# ---- Lambda create-function args (R4-1 runtime/arch/vpc/description) -----------------------
@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_lambda_create_function_shape_valid():
    p = {"FunctionName": "demo-mrl-settings", "Runtime": "python3.12",
         "Role": "arn:aws:iam::123456789012:role/demo-lambda-exec-role",
         "Handler": "mrl_settings.handler", "Timeout": 900, "MemorySize": 512,
         "Code": {"ZipFile": b"x"}, "Architectures": ["arm64"],
         "Description": "dsql-mrl MRL Lambda (demo-mrl-settings)",
         "VpcConfig": {"SubnetIds": ["subnet-a"], "SecurityGroupIds": ["sg-1"]}}
    _validate("lambda", "CreateFunction", p)
    # Description marker is well under the 256-char limit
    assert len(p["Description"]) <= 256


# ---- Step Functions create/update + the new validator op ----------------------------------
@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_stepfunctions_create_update_and_validate_op():
    _validate("stepfunctions", "CreateStateMachine",
              {"name": "demo-mrl-task", "definition": "{}",
               "roleArn": "arn:aws:iam::123456789012:role/demo-sfn-exec-role",
               "type": "STANDARD", "tags": [{"key": "dsql_pipeline_mrl", "value": "demo-mrl-task"}]})
    _validate("stepfunctions", "UpdateStateMachine",
              {"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task",
               "definition": "{}", "roleArn": "arn:aws:iam::123456789012:role/demo-sfn-exec-role"})
    # the new pre-check op exists with a required 'definition' + optional 'type'
    m = _SESSION.get_service_model("stepfunctions")
    om = m.operation_model("ValidateStateMachineDefinition")
    assert "definition" in om.input_shape.required_members
    assert "type" in om.input_shape.members
    out = om.output_shape
    assert "result" in out.members and "diagnostics" in out.members


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_deploy_uses_validate_state_machine_definition():
    deploy = open(DEPLOY).read()
    assert "validate-state-machine-definition" in deploy
    assert "--definition" in deploy and "--type STANDARD" in deploy
    # it runs read-only (not via `run`) BEFORE create/update, and warns (not abort) if unavailable
    assert "could not pre-validate" in deploy
    assert re.search(r"validate_sm_definition .*\$name.*\$def|validate_sm_definition \"\$name\"", deploy)


# ---- A15.4: the Glue StartJobRun Arguments the ASL sends are a map of str -> NON-EMPTY str ----
@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_startjobrun_arguments_shape_and_nonempty():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_gan", os.path.join(PKG, "tests", "test_glue_args_nonempty.py"))
    gan = importlib.util.module_from_spec(spec); spec.loader.exec_module(gan)
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    data = gan._state_input(null_blank=True, cdc=True)
    for name, args in gan._glue_arg_states(d):
        resolved = {}
        for k, v in args.items():
            key = k[:-2] if k.endswith(".$") else k
            val = gan._eval_value(v, data, gan._CTX) if k.endswith(".$") else v
            resolved[key] = val
        # StartJobRun.Arguments is map[str,str] (StringList-ish): every value must be a non-empty str
        _validate("glue", "StartJobRun", {"JobName": "demo-t-mrl-load", "Arguments": resolved})
        for key, val in resolved.items():
            assert isinstance(val, str) and val != "", "%s %s empty/non-str (%r)" % (name, key, val)


# ---- A15.5: every MRL-essential job2 arg is present in DefaultArguments ∪ run Arguments -------
def test_job2_required_args_are_all_passed():
    """The RunRange run Arguments ∪ mrl_ensure_job DefaultArguments must include every arg the
    MRL range-load path needs; otherwise the job fails at launch (getResolvedOptions / the script's
    overlay). job2 uses an all-optional overlay, so we assert the MRL-essential set explicitly."""
    asl = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    run_args = set(k[2:-2] if k.endswith(".$") else k[2:]
                   for k in asl["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]
                   ["Parameters"]["Arguments"])
    ej = open(os.path.join(PKG, "lambdas", "mrl_ensure_job.py")).read()
    default_args = set(re.findall(r'"--([a-z0-9_-]+)"', ej))
    passed = run_args | default_args
    essential = {"config_prefix", "load_file_start", "load_file_end", "mrl_range_index",
                 "dsql_endpoint", "dsql_user", "dsql_database", "region", "s3_bucket",
                 "extra-py-files", "max_files_in_parallel", "writers_per_file"}
    missing = sorted(essential - passed)
    assert not missing, "MRL-essential job2 arg(s) never passed (run∪default): %s" % missing
    # the three null args must NOT be run args (they live only in DefaultArguments, sentinel-aware)
    for a in ("null_values", "null_rules", "csv_null_value"):
        assert a not in run_args, "%s must not be a RunRange run argument (A15)" % a
        assert a in default_args, "%s must be in mrl_ensure_job DefaultArguments" % a


def test_count_job_required_args_are_all_passed():
    """The count/probe CountVerify+SafetyProbe run args must cover job_mrl_count's required set."""
    asl = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    cv = set(k[2:-2] if k.endswith(".$") else k[2:]
             for k in asl["States"]["CountVerify"]["Parameters"]["Arguments"])
    for need in ("config_prefix", "dsql_endpoint", "dsql_user", "dsql_database", "region",
                 "mrl_mode", "mrl_result_prefix"):
        assert need in cv, "CountVerify must pass --%s" % need
    # the count job never parses null settings -> must not receive them as run args
    for a in ("null_values", "null_rules"):
        assert a not in cv, "count job must not receive --%s" % a


def test_a15_script_treats_blank_and_sentinel_null_as_unset():
    """A15.3 script-side guard: the REAL job2 null parsers treat '' and the sentinel __NULL_UNSET__
    as UNSET (same as the original job2_load.py). job_mrl_count does not parse null settings."""
    import ast as _ast
    src = open(os.path.join(PKG, "scripts", "job2_load_manual_range.py")).read()
    tree = _ast.parse(src)
    ns = {}
    for n in tree.body:
        if isinstance(n, _ast.FunctionDef) and n.name in ("_parse_null_values", "_parse_null_rules"):
            exec(_ast.get_source_segment(src, n), ns)
    # '' and __NULL_UNSET__ -> empty marker list / empty rules (unset)
    assert ns["_parse_null_values"]("") is None            # unset -> keep endpoint marker
    assert ns["_parse_null_values"]("__NULL_UNSET__") is None
    assert ns["_parse_null_rules"]("") is None
    assert ns["_parse_null_rules"]("__NULL_UNSET__") is None
    # a real value still parses
    assert ns["_parse_null_values"]("\\N|NULL") == ["\\N", "NULL"]
    # job_mrl_count never parses null settings (not in its required/optional args)
    csrc = open(os.path.join(PKG, "scripts", "job_mrl_count.py")).read()
    assert "null_values" not in csrc and "null_rules" not in csrc, \
        "the count job must not read null settings"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn()
            print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
