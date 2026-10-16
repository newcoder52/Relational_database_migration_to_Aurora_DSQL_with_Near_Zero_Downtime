#!/usr/bin/env python3
"""AMENDMENT 9.4 tests for the params-key classification + VALUES handling.

(a) every key of PIPELINE_KEYS + the setup-only keys + required keys has a classification row;
(b) a params.csv that sets EVERY known key passes parse()+to_pipeline_settings() with no error,
    and an unknown key is rejected exactly as the existing parser rejects it;
(c) the range-load job's Glue Arguments contain the USED values as STRINGS and no IGNORED key
    appears as an argument;
(f) the README table is generated from the SAME classification data (checked by substring).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(PKG, "tools"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import params_csv as pc                      # noqa: E402
import mrl_params_csv as mpc                 # noqa: E402
import mrl_settings_classification as cls     # noqa: E402


def test_every_key_classified():
    keys = set(pc.PIPELINE_KEYS) | set(pc.SETUP_ONLY) | set(pc.REQUIRED) | set(pc._DERIVED_DEFAULT)
    missing = sorted(keys - set(cls.CLASSIFICATION))
    extra = sorted(set(cls.CLASSIFICATION) - keys)
    assert not missing, "unclassified params.csv key(s): %s" % missing
    # 'extra' rows are allowed only if they are real keys (none expected)
    assert not extra, "classification has unknown key(s): %s" % extra
    for k, (c, detail) in cls.CLASSIFICATION.items():
        assert c in (cls.USED, cls.PASSED_THROUGH, cls.IGNORED_HERE), (k, c)
        assert detail.strip(), "empty reason for %s" % k


def test_full_params_csv_passes_and_unknown_rejected():
    rows = ["parameter,value", "account_id,123456789012", "region,us-east-1", "project,demo",
            "dsql_endpoint,c.dsql.us-east-1.on.aws"]
    # set every OPTIONAL key to its own default so the file is valid and exercises every key
    for k, v in pc.OPTIONAL_DEFAULTS.items():
        if v != "":
            rows.append("%s,%s" % (k, v))
    text = "\n".join(rows) + "\n"
    parsed = mpc.parse(text)
    assert not parsed["errors"], parsed["errors"]
    settings = mpc.to_pipeline_settings(parsed["params"])
    assert settings["project"] == "demo"
    # an UNKNOWN key is rejected exactly as the original parser rejects it
    bad = mpc.parse(text + "totally_unknown_key,1\n")
    assert any("unknown key" in e for e in bad["errors"]), bad["errors"]


def test_used_values_are_string_glue_args_and_ignored_absent():
    """(c) build the range-load job via mrl_ensure_job against fakes; every USED value that maps
    to a Glue argument is a str, and no IGNORED-here key name appears as an argument."""
    import json
    import mrl_ensure_job
    from mrl_fakes import FakeS3, FakeGlue
    s3, glue = FakeS3(), FakeGlue()
    s3.put_object(Bucket="b", Key="glue-templates/mrl-load.json",
                  Body=json.dumps({"role": "mrl-load", "script": "job2_load_manual_range.py",
                                   "command_name": "glueetl", "glue_version": "4.0",
                                   "worker_type": "G.4X", "number_of_workers": 20,
                                   "timeout_minutes": 2880, "max_concurrent_runs": 1,
                                   "default_arguments": {"--TempDir": "s3://<<BUCKET>>/glue-temp/"}}).encode())
    mrl_ensure_job.boto3 = type("x", (), {"client": staticmethod(
        lambda svc, **k: {"s3": s3, "glue": glue}.get(svc, s3))})
    out = mrl_ensure_job.handler({
        "bucket": "b", "project": "demo", "taskSuffix": "t", "region": "us-east-1",
        "accountId": "123456789012", "glue_templates_prefix": "glue-templates",
        "scripts_prefix": "scripts",
        "glue_role_arn": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
        "dsql_endpoint": "c.dsql.us-east-1.on.aws", "dsql_user": "admin",
        "dsql_database": "postgres", "csv_null_value": "NULL", "null_values": "",
        "null_rules": "", "maxTotalConcurrentRuns": 4, "loadWorkerType": "G.4X",
        "loadWorkers": 20}, None)
    job = glue.jobs[out["jobName"]]
    args = job["DefaultArguments"]
    # every argument value is a str
    for k, v in args.items():
        assert isinstance(v, str), "Glue arg %s is %r (not str)" % (k, type(v).__name__)
    # no IGNORED-here key appears as a --<key> argument
    ignored = [k for k, (c, _d) in cls.CLASSIFICATION.items() if c == cls.IGNORED_HERE]
    for k in ignored:
        assert ("--%s" % k) not in args, "IGNORED key %s leaked as a Glue arg" % k
    # USED DSQL/null args are present as strings
    for req in ("--dsql_endpoint", "--dsql_user", "--dsql_database", "--region"):
        assert req in args and isinstance(args[req], str)


def test_readme_table_matches_classification():
    """(f) the README settings-source table is generated from the classification data, so each
    key's row appears verbatim."""
    readme = open(os.path.join(PKG, "README.md")).read()
    table = cls.markdown_table()
    # The README must contain the generated table block (every row).
    missing = [ln for ln in table.splitlines() if ln.startswith("| `") and ln not in readme]
    assert not missing, ("README settings-source table is out of sync with the classification "
                         "data (%d missing rows, e.g. %s)" % (len(missing), missing[:2]))


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d params-classification test(s) passed" % len(fns))
