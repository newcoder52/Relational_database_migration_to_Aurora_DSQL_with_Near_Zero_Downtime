#!/usr/bin/env python3
"""Amendment 15.3 - every Glue run argument the ASL sends must be a NON-EMPTY STRING. Glue drops
an empty-string argument value (argparse then errors `expected one argument`), and booleans/numbers
must be stringified. We evaluate each `glue:startJobRun[.sync]` state's Arguments templates
(resolving `.$` JSONPaths + States.* intrinsics) against realistic state inputs - including BLANK
null settings, the live failure case - and assert every value is a non-empty string.
"""
import glob
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
SF_DIR = os.path.join(PKG, "stepfunctions")


# ---- a tiny JSONPath + States.Format evaluator over a state-input dict / context ----------
def _get(path, data, ctx):
    if path.startswith("$$."):
        cur, parts = ctx, path[3:].split(".")
    elif path == "$":
        return data
    elif path.startswith("$."):
        cur, parts = data, path[2:].split(".")
    else:
        return path
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            raise KeyError("missing path %r (at %r)" % (path, p))
        cur = cur[p]
    return cur


_FMT_RE = re.compile(r"^States\.Format\('([^']*)',\s*(.+)\)$")


def _eval_value(v, data, ctx):
    """Evaluate an ASL Arguments value (a `.$` template value)."""
    m = _FMT_RE.match(v)
    if m:
        tmpl, argstr = m.group(1), m.group(2)
        args = [a.strip() for a in argstr.split(",")]
        vals = [_get(a, data, ctx) for a in args]
        out = tmpl
        for val in vals:
            out = out.replace("{}", str(val), 1)
        return out
    if v.startswith("$"):
        return _get(v, data, ctx)
    return v


def _glue_arg_states(d):
    out = []
    def walk(states, pfx=""):
        for name, st in states.items():
            if "glue:startJobRun" in st.get("Resource", ""):
                args = (st.get("Parameters", {}) or {}).get("Arguments", {})
                out.append((pfx + name, args))
            sub = st.get("Iterator") or st.get("ItemProcessor")
            if sub and "States" in sub:
                walk(sub["States"], pfx + name + "/")
    walk(d["States"])
    return out


# Realistic state inputs: resolved.* from mrl_preflight + Map-item fields. The null settings are
# BLANK (the live case); csvNullValue blank too; everything else at its default.
def _state_input(null_blank=True, cdc=False):
    resolved = {
        "configPrefix": "s3://b/config/_task/t/", "region": "us-east-1",
        "taskArn": "arn:aws:dms:us-east-1:123456789012:task:ABC", "cdcRoot": ".",
        "timestampColumnName": "dms_timestamp", "countTimeoutAction": "fail",
        "csvNullValue": "" if null_blank else "NULL",
        "nullValues": "" if null_blank else "\\N|NULL",
        "nullRules": "" if null_blank else "s.t.c=V1",
        "maxFilesInParallel": 30, "writersPerFile": 8, "maxWriteConcurrency": 240,
        "countTolerance": 0,
        "probeTimeoutSeconds": 900, "countTimeoutSeconds": 3600, "cdc": cdc,
    }
    return {
        "taskArn": resolved["taskArn"],
        "resolved": resolved,
        "plan": {"mrlStatePrefix": "s3://b/mrl/state/t/"},
        "mrlJob": {"jobName": "demo-t-mrl-load"},
        "driverCount": {"extraPyFiles": "s3://b/driver-cdc-prepared/abc/pg8000.whl"},
        "driverFullload": {"extraPyFiles": "s3://b/driver-fullload/pg8000.whl"},
        # Map-item (RunRange) fields:
        "config_prefix": "s3://b/mrl/state/t/tables/s.t/config/",
        "first": "LOAD00000001.csv", "last": "LOAD00000050.csv", "range_index": 0,
        "extraPyFiles": "s3://b/driver-fullload/pg8000.whl",
        "maxFilesInParallel": 30, "writersPerFile": 8, "maxWriteConcurrency": 240,
        # Map-item (StartForkCdcJob) fields:
        "cdcRoot": ".", "timestampColumnName": "dms_timestamp", "execName": "exec-1", "fork_slug": "s-t-dead",
    }


_CTX = {"Execution": {"Name": "exec-1", "Id": "arn:aws:states:us-east-1:123456789012:execution:demo-mrl-task:exec-1"}}


def _check_all(d, data):
    problems = []
    for name, args in _glue_arg_states(d):
        for k, v in args.items():
            key = k[:-2] if k.endswith(".$") else k
            try:
                val = _eval_value(v, data, _CTX) if k.endswith(".$") else v
            except KeyError as e:
                problems.append("%s %s: unresolved (%s)" % (name, key, e))
                continue
            if not isinstance(val, str):
                problems.append("%s %s: not a string (%r)" % (name, key, val))
            elif val == "":
                problems.append("%s %s: EMPTY string (Glue drops it -> 'expected one argument')"
                                % (name, key))
    return problems


def test_all_glue_run_args_nonempty_strings_blank_and_nonblank():
    for fp in glob.glob(os.path.join(SF_DIR, "*.asl.json")):
        d = json.load(open(fp))
        for null_blank in (True, False):
            for cdc in (False, True):
                probs = _check_all(d, _state_input(null_blank=null_blank, cdc=cdc))
                assert not probs, "%s (null_blank=%s cdc=%s):\n  - %s" % (
                    os.path.basename(fp), null_blank, cdc, "\n  - ".join(probs))


def test_runrange_does_not_pass_null_args():
    """A15.1: RunRange must NOT pass --null_values/--null_rules/--csv_null_value as run args (the
    sentinel-aware DefaultArguments handle them, like the original pipeline)."""
    d = json.load(open(os.path.join(SF_DIR, "mrl-task.asl.json")))
    args = d["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]["Parameters"]["Arguments"]
    for bad in ("--null_values.$", "--null_rules.$", "--csv_null_value.$",
                "--null_values", "--null_rules", "--csv_null_value"):
        assert bad not in args, "RunRange must not pass %s as a run argument (A15)" % bad


def test_mutation_blank_null_values_arg_is_caught():
    """Re-introduce `--null_values.$`: with a BLANK nullValues it resolves to '' -> the checker
    must flag it (this is exactly the live GlueArgumentError)."""
    d = json.load(open(os.path.join(SF_DIR, "mrl-task.asl.json")))
    import copy
    d2 = copy.deepcopy(d)
    rr = d2["States"]["LoadRangeMap"]["Iterator"]["States"]["RunRange"]["Parameters"]["Arguments"]
    rr["--null_values.$"] = "$.resolved.nullValues"
    probs = _check_all(d2, _state_input(null_blank=True))
    assert any("null_values" in p and "EMPTY" in p for p in probs), probs
    # with a NON-blank value the same arg would pass (proving the checker is value-sensitive)
    probs2 = _check_all(d2, _state_input(null_blank=False))
    assert not any("null_values" in p for p in probs2), probs2


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d glue-args test(s) passed" % len(fns))
