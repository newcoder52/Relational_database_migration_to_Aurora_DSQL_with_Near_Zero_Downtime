#!/usr/bin/env python3
"""Amendment 16.2 - EXECUTE the real argument parsing of every Glue script the MRL ASL launches,
with a faithful fake awsglue.utils.getResolvedOptions (argparse where every REQUESTED option is
required and a missing one raises SystemExit(2) - exactly like the real Glue helper), feeding the
EXACT Arguments the ASL states send (evaluated from the templates) plus the job's DefaultArguments.
Catches `the following arguments are required: --JOB_NAME` (the live T14E exit-2) offline.
"""
import argparse
import ast
import importlib.util
import json
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
SF = os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")
COUNT = os.path.join(PKG, "scripts", "job_mrl_count.py")
LOAD = os.path.join(PKG, "scripts", "job2_load_manual_range.py")


# ---- a faithful fake getResolvedOptions (real one: all requested are REQUIRED) -------------
def fake_get_resolved_options(argv, options):
    ap = argparse.ArgumentParser(prog=os.path.basename(argv[0]) if argv else "job")
    for o in options:
        ap.add_argument("--%s" % o, required=True)
    # argparse exits 2 on a missing required arg (matches Glue's getResolvedOptions behaviour)
    ns, _ = ap.parse_known_args(argv[1:])
    return {o: getattr(ns, o.replace("-", "_")) for o in options}


# ---- evaluate the ASL Arguments for a Glue state (reuse the A15 evaluator) -----------------
def _load_gan():
    spec = importlib.util.spec_from_file_location(
        "_gan16", os.path.join(HERE, "test_glue_args_nonempty.py"))
    gan = importlib.util.module_from_spec(spec); spec.loader.exec_module(gan)
    return gan


def _state_argv(state_name, default_args):
    """Build the argv a Glue run of `state_name` receives = DefaultArguments overlaid by the
    state's resolved run Arguments, as sys.argv tokens (prog + --k v ...)."""
    gan = _load_gan()
    d = json.load(open(SF))
    args = d["States"][state_name]["Parameters"]["Arguments"]
    data = gan._state_input(null_blank=True, cdc=False)
    resolved = dict(default_args)
    for k, v in args.items():
        key = k[:-2] if k.endswith(".$") else k
        val = gan._eval_value(v, data, gan._CTX) if k.endswith(".$") else v
        resolved[key.lstrip("-")] = str(val)
    argv = ["job_mrl_count.py"]
    for k, v in resolved.items():
        argv += ["--%s" % k, v]
    return argv


# the deployed count job's DefaultArguments (deploy_mrl.sh / mrl-count.json)
_COUNT_DEFAULTS = {"enable-continuous-cloudwatch-log": "true"}


def _count_argblock_src():
    src = open(COUNT).read()
    start = src.index("_OPT = [")
    end = src.index("\ndef ", start)
    return src[start:end]


def _run_count_argblock(argv):
    """Execute the REAL arg-parsing block of job_mrl_count.py (ONLY the _OPT..getResolvedOptions
    constants, not the awsglue/boto3/pg8000 imports) under the fake helper + argv."""
    block = _count_argblock_src()
    ns = {"sys": __import__("sys"), "os": os,
          "getResolvedOptions": fake_get_resolved_options}
    saved = sys.argv
    sys.argv = argv
    try:
        exec(compile(block, COUNT, "exec"), ns)
    finally:
        sys.argv = saved
    return ns


@pytest.mark.parametrize("state", ["SafetyProbe", "CountVerify"])
def test_count_script_parses_probe_and_count_args_exit0(state):
    argv = _state_argv(state, _COUNT_DEFAULTS)
    ns = _run_count_argblock(argv)          # must NOT raise SystemExit
    assert ns["CONFIG_PREFIX"].startswith("s3://")
    assert ns["MRL_MODE"] in ("probe", "count")
    assert ns["JOB_NAME"]                    # defaulted, not required


def test_mutation_requiring_job_name_fails():
    """Re-require JOB_NAME (the live defect) and assert the parse exits 2 for the probe args."""
    block = _count_argblock_src()
    mutated = block.replace("args = getResolvedOptions(sys.argv, _present)",
                            'args = getResolvedOptions(sys.argv, ["JOB_NAME"] + _present)')
    assert mutated != block
    argv = _state_argv("SafetyProbe", _COUNT_DEFAULTS)
    ns = {"sys": __import__("sys"), "os": os, "getResolvedOptions": fake_get_resolved_options}
    saved = sys.argv; sys.argv = argv
    try:
        with pytest.raises(SystemExit) as ei:
            exec(compile(mutated, COUNT, "exec"), ns)
        assert ei.value.code == 2
    finally:
        sys.argv = saved


# ---- mandatory-options per engine: glueetl={JOB_NAME}, pythonshell=empty -------------------
def _mandatory_getresolved_lists(path):
    """Find every getResolvedOptions(sys.argv, <list literal>) whose list is a constant (these are
    the MANDATORY options); return the union of such constant option names."""
    src = open(path).read()
    tree = ast.parse(src)
    mand = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "getResolvedOptions"
                and len(node.args) == 2 and isinstance(node.args[1], ast.List)):
            lit = []
            ok = True
            for e in node.args[1].elts:
                if isinstance(e, ast.Constant) and isinstance(e.value, str):
                    lit.append(e.value)
                else:
                    ok = False   # a non-constant (e.g. _present / concatenation) is NOT mandatory
                    break
            if ok:
                mand |= set(lit)
    return mand


def test_glueetl_mandatory_is_job_name_only():
    # job2_load_manual_range.py is glueetl (Spark): Glue injects --JOB_NAME, so requiring only it
    # is correct (same pattern as the original job2_load.py that runs live in production).
    assert "glueetl" in open(os.path.join(PKG, "glue-templates", "mrl-load.json")).read()
    assert _mandatory_getresolved_lists(LOAD) == {"JOB_NAME"}


def test_pythonshell_mandatory_is_empty():
    # job_mrl_count.py is python-shell: Glue does NOT inject --JOB_NAME, so NO option may be
    # mandatory via a constant getResolvedOptions list (A16 fix).
    assert "pythonshell" in open(os.path.join(PKG, "glue-templates", "mrl-count.json")).read()
    assert _mandatory_getresolved_lists(COUNT) == set(), \
        "python-shell count job must have NO mandatory getResolvedOptions (Glue injects none)"


if __name__ == "__main__":
    for state in ("SafetyProbe", "CountVerify"):
        test_count_script_parses_probe_and_count_args_exit0(state)
        print("[PASS] parse", state)
    test_mutation_requiring_job_name_fails(); print("[PASS] mutation")
    test_glueetl_mandatory_is_job_name_only(); print("[PASS] glueetl mandatory")
    test_pythonshell_mandatory_is_empty(); print("[PASS] pythonshell mandatory")
