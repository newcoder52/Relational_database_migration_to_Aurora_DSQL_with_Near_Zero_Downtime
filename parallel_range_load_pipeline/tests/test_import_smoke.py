#!/usr/bin/env python3
"""Amendment 17.2/17.4 - the missing safety net: EXECUTE each Glue script's MODULE TOP LEVEL with
stubbed awsglue/pyspark/boto3/botocore/pg8000 and the REAL argument shape the pipeline passes, and
assert it completes without NameError/AttributeError/ImportError/TypeError (the A17 live defect was
a module-level call to a function defined 4400 lines later). This is the blind spot the 213
AST-extract tests never covered.

job2_load_manual_range.py is glueetl (Spark): it builds a SparkContext/Job at module top level, so
we stop it at the first Spark side effect with a StopHere sentinel and assert StopHere (not an
unrelated error) ended the run. job_mrl_count.py is python-shell: its top level is just arg parsing
+ constants and completes fully.
"""
import ast
import importlib.util
import json
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
LOAD = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
COUNT = os.path.join(PKG, "scripts", "job_mrl_count.py")


class StopHere(Exception):
    """Raised by the first stubbed Spark/DSQL/S3 side effect so the smoke test stops cleanly
    AFTER the module top level is proven import-safe, BEFORE any real I/O."""


class _StopMock:
    """Chained attribute access returns another _StopMock; the first CALL raises StopHere. So a
    module-top-level Spark/Glue/boto3 expression (e.g. spark.conf.set(...), SparkContext()) runs
    until its first actual call and stops there - NOT on an AttributeError."""
    def __init__(self, *a, **k):
        if a or k:
            raise StopHere("stub instantiated")
    def __getattr__(self, name):
        return _StopMock.__new__(_StopMock)
    def __call__(self, *a, **k):
        raise StopHere("stub call")
    def __iter__(self):
        raise StopHere("stub iter")


def _fake_get_resolved_options_factory(argv_defaults):
    def fake(argv, options):
        # realistic: every requested option must be present (in argv or the job defaults); a
        # missing REQUIRED one raises SystemExit(2) like Glue's helper.
        present = {}
        for i, tok in enumerate(argv):
            if tok.startswith("--") and i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                present[tok[2:]] = argv[i + 1]
        present.update(argv_defaults)
        out = {}
        for o in options:
            if o in present:
                out[o] = present[o]
            else:
                raise SystemExit(2)
        return out
    return fake


def _install_stubs(argv, argv_defaults, make_spark_raise=True):
    """Put stub modules in sys.modules for the Glue runtime deps; return a cleanup fn."""
    saved_mods = {}
    for name in ("awsglue", "awsglue.utils", "awsglue.context", "awsglue.job",
                 "awsglue.dynamicframe", "awsglue.transforms", "pyspark", "pyspark.context",
                 "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types", "boto3",
                 "botocore", "botocore.config", "botocore.exceptions", "pg8000", "pg8000.native"):
        saved_mods[name] = sys.modules.get(name)
        m = types.ModuleType(name)
        sys.modules[name] = m
    # awsglue.utils.getResolvedOptions
    sys.modules["awsglue.utils"].getResolvedOptions = _fake_get_resolved_options_factory(argv_defaults)
    # the heavy classes used at module top level -> StopHere on first use
    for mod, attrs in (("awsglue.context", ["GlueContext"]), ("awsglue.job", ["Job"]),
                       ("pyspark.context", ["SparkContext"]), ("pyspark", ["SparkContext"]),
                       ("pyspark.sql", ["SparkSession", "DataFrame", "Row"]),
                       ("awsglue.dynamicframe", ["DynamicFrame"])):
        for a in attrs:
            setattr(sys.modules[mod], a, _StopMock if make_spark_raise else type("X", (), {}))
    # pyspark.sql.functions / types -> benign callables
    for mod in ("pyspark.sql.functions", "pyspark.sql.types", "awsglue.transforms"):
        sys.modules[mod].__getattr__ = lambda name: (lambda *a, **k: None)
    # boto3.client -> StopMock (first real AWS call stops); pg8000.connect -> StopHere
    sys.modules["boto3"].client = lambda *a, **k: _StopMock()
    sys.modules["boto3"].Session = _StopMock
    sys.modules["botocore.config"].Config = lambda *a, **k: None
    sys.modules["botocore.exceptions"].ClientError = type("ClientError", (Exception,), {})
    sys.modules["pg8000"].connect = lambda *a, **k: (_ for _ in ()).throw(StopHere("pg8000.connect"))

    saved_argv = sys.argv
    sys.argv = argv

    def cleanup():
        sys.argv = saved_argv
        for name, old in saved_mods.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old
    return cleanup


def _exec_module_toplevel(path, argv, argv_defaults):
    cleanup = _install_stubs(argv, argv_defaults)
    try:
        src = open(path).read()
        code = compile(src, path, "exec")
        g = {"__name__": "__mrl_smoke__", "__file__": path}
        try:
            exec(code, g)
            return "COMPLETED", g
        except StopHere as e:
            return ("STOPHERE", str(e))
    finally:
        cleanup()


# ---- build the real argv the pipeline passes ---------------------------------------------
def _gan():
    spec = importlib.util.spec_from_file_location(
        "_gan17", os.path.join(HERE, "test_glue_args_nonempty.py"))
    g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)
    return g


def _runrange_argv():
    gan = _gan()
    d = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")))
    rr = d["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]["Parameters"]["Arguments"]
    data = gan._state_input(null_blank=True, cdc=False)
    argv = ["job2_load_manual_range.py", "--JOB_NAME", "demo-t-mrl-load"]
    for k, v in rr.items():
        key = k[:-2] if k.endswith(".$") else k
        val = gan._eval_value(v, data, gan._CTX) if k.endswith(".$") else v
        argv += ["--%s" % key.lstrip("-"), str(val)]
    return argv


# mrl_ensure_job DefaultArguments the glueetl job carries (names only; values benign)
_LOAD_DEFAULTS = {"dsql_endpoint": "c.dsql.us-east-1.on.aws", "dsql_user": "admin",
                  "dsql_database": "postgres", "region": "us-east-1", "s3_bucket": "b",
                  "dms_bucket": "b", "null_values": "__NULL_UNSET__", "null_rules": "__NULL_UNSET__",
                  "csv_null_value": "NULL", "job-language": "python"}


def test_job2_load_module_toplevel_import_safe():
    """The glueetl load script's module top level must run with NO NameError; it reaches the first
    Spark side effect (SparkContext()) and stops with StopHere - proving import got that far."""
    status, info = _exec_module_toplevel(LOAD, _runrange_argv(), _LOAD_DEFAULTS)
    assert status == "STOPHERE", "expected the module to reach a Spark side effect, got %s %r" % (
        status, info)
    # it stopped on a Spark/Glue/boto3/pg8000 stub, NOT on a NameError (the A17 bug)
    assert "StopHere" in type(StopHere()).__name__   # sanity


def test_job_mrl_count_module_toplevel_import_safe():
    """The python-shell count script's top level is arg parsing + constants: completes fully."""
    argv = ["job_mrl_count.py", "--config_prefix", "s3://b/config/_task/t/",
            "--mrl_mode", "probe", "--mrl_result_prefix", "s3://b/mrl/state/t/",
            "--dsql_endpoint", "c.dsql.us-east-1.on.aws", "--dsql_user", "admin",
            "--dsql_database", "postgres", "--region", "us-east-1",
            "--dms_task_arn", "arn:aws:dms:us-east-1:123456789012:task:ABC"]
    status, info = _exec_module_toplevel(COUNT, argv, {})
    assert status == "COMPLETED", "count script top level failed: %r" % (info,)
    assert info["MRL_MODE"] == "probe" and info["CONFIG_PREFIX"].startswith("s3://")


# ---- static: every module-level call's def precedes it (catches the A17 defect class) -----
def _module_level_call_before_def(src):
    tree = ast.parse(src)
    defs = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name not in defs:
            defs[n.name] = n.lineno
    bad = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for sub in ast.walk(n):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id in defs:
                if sub.lineno < defs[sub.func.id]:
                    bad.append((sub.func.id, sub.lineno, defs[sub.func.id]))
    return bad


@pytest.mark.parametrize("path", [LOAD, COUNT])
def test_no_module_level_call_before_def(path):
    bad = _module_level_call_before_def(open(path).read())
    assert not bad, "%s: module-level call(s) before their def: %s" % (os.path.basename(path), bad)


def test_mutation_call_before_def_is_caught():
    """Move _apply_manual_range_overrides back BELOW its call -> the static check flags it."""
    src = open(LOAD).read()
    lines = src.split("\n")
    # find the def block and the call
    import re
    call_idx = next(i for i, l in enumerate(lines) if l.strip() == "_apply_manual_range_overrides()")
    tree = ast.parse(src)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
              and n.name == "_apply_manual_range_overrides")
    block = lines[fn.lineno - 1:fn.end_lineno]
    # build a mutated source: remove the def, re-append it at the END (after the call)
    mutated_lines = lines[:fn.lineno - 1] + lines[fn.end_lineno:] + [""] + block
    bad = _module_level_call_before_def("\n".join(mutated_lines))
    assert any(n == "_apply_manual_range_overrides" for n, *_ in bad), bad


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn()
            print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
