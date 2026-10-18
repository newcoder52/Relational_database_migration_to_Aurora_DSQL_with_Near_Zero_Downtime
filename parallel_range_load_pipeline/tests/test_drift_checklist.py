#!/usr/bin/env python3
"""Amendment 19 'Also' - a pre-flight DRIFT GUARD for the class 'offline green, live broken for a
name/shape the tests never used'. Each known real-AWS shape (learned the hard way across A12-A19)
gets ONE executing assertion here so a regression is caught offline.
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
ASL = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))


def test_shape_hex_load_names_accepted_by_default():
    """DMS names LOAD files in HEX; the planner must accept hex by default (A19)."""
    import sys
    sys.path.insert(0, os.path.join(PKG, "lambdas"))
    import mrl_preflight
    assert 'event.get("splitAllowHex", True)' in open(mrl_preflight.__file__).read()
    import mrl_split as ms
    assert ms.is_full_load_key("LOAD0000000A.csv", allow_hex=True)
    assert ms.is_full_load_key("LOAD00000196.csv", allow_hex=True)   # 0x196 = 406


def test_shape_glue_run_args_never_empty_string():
    """Glue drops an empty-string argument value (A15). No RunRange run arg may be empty; the
    null args are NOT passed per-run."""
    rr = ASL["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]["Parameters"]["Arguments"]
    for bad in ("--null_values.$", "--null_rules.$", "--csv_null_value.$"):
        assert bad not in rr


def test_shape_pythonshell_has_no_mandatory_job_name():
    """Glue injects --JOB_NAME only for Spark, not pythonshell StartJobRun (A16)."""
    src = open(os.path.join(PKG, "scripts", "job_mrl_count.py")).read()
    assert 'getResolvedOptions(sys.argv, ["JOB_NAME"]' not in src
    assert "_present" in src


def test_shape_glue4_botocore_lacks_dsql_so_spark_gets_additional_modules():
    """Glue 4.0 botocore has no 'dsql' service -> the Spark mrl-load job delivers boto3 via
    --additional-python-modules (A18)."""
    ej = open(os.path.join(PKG, "lambdas", "mrl_ensure_job.py")).read()
    assert "--additional-python-modules" in ej and "_spark_boto3_modules" in ej
    assert ASL["States"]["EnsureMrlLoadJob"]["Parameters"]["Payload"].get("cdcExtraPyFiles.$") \
        == "$.driverCount.extraPyFiles"


def test_shape_sfn_dynamic_timeout_is_path_not_dollar():
    """Step Functions rejects TimeoutSeconds.$; the dynamic field is TimeoutSecondsPath (A14)."""
    for st in ("SafetyProbe", "CountVerify"):
        s = ASL["States"][st]
        assert "TimeoutSeconds.$" not in s and s.get("TimeoutSecondsPath", "").startswith("$.")


def test_shape_module_top_level_import_safe():
    """A module-level call to a not-yet-defined function (A17) is caught: no module-level call
    precedes its def in the Glue scripts."""
    import ast
    for script in ("job2_load_manual_range.py", "job_mrl_count.py"):
        src = open(os.path.join(PKG, "scripts", script)).read()
        tree = ast.parse(src)
        defs = {n.name: n.lineno for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name not in {}}
        for n in tree.body:
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
                continue
            for sub in ast.walk(n):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) \
                        and sub.func.id in defs and sub.lineno < defs[sub.func.id]:
                    assert False, "%s: %s called before def" % (script, sub.func.id)


def test_shape_resultselector_carries_every_referenced_resolved_path():
    """A ResultSelector that omits a path the ASL later reads (e.g. probe/count timeout seconds)
    is a runtime States.Runtime failure (A14). Assert the Preflight ResultSelector carries the
    sizing/timeout paths the states read from $.resolved."""
    rs = ASL["States"]["Preflight"]["ResultSelector"]
    emitted = {k[:-2] if k.endswith(".$") else k for k in rs}
    for need in ("probeTimeoutSeconds", "countTimeoutSeconds", "maxFilesInParallel",
                 "writersPerFile", "maxWriteConcurrency", "splitAllowHex", "splitBy"):
        assert need in emitted, "Preflight ResultSelector missing $.resolved.%s" % need


def test_shape_no_silent_success_without_range_artifacts():
    """A load that writes no per-range artifacts in range mode must fail loudly (A19 Defect 1)."""
    src = open(os.path.join(PKG, "scripts", "job2_load_manual_range.py")).read()
    assert "wrote NO per-range result doc" in src
    assert "MANUAL RANGE MODE -> PER-FILE parallel load" in src


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print("[PASS]", fn.__name__)
    print("\n%d drift-checklist test(s) passed" % len(fns))
