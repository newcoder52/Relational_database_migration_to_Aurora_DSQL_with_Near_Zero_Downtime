#!/usr/bin/env python3
"""Offline self-test of deployment_fingerprint.py against a FAKE boto3 (no AWS). Run: python3 selftest.py"""
import sys, types, zipfile, json, os, datetime, runpy, py_compile

base = os.path.dirname(os.path.abspath(__file__)) + "/"
py_compile.compile(base + "deployment_fingerprint.py", doraise=True)

BAD = "def main():\n    _apply_null_settings()\nmain_x = 1\n_apply_null_settings()\ndef _apply_null_settings():\n    return 1\n"
GOOD = ("import sys\ndef is_broken_pipe_error(e):\n    return 'eof occurred in violation of protocol' in str(e)\n"
        "POST_LOAD_COUNT_SOCKET_READ_TIMEOUT=300\nx='post_load_check'\n")
FILES = {
    "scripts/job2_load.py": GOOD.encode(),
    "scripts/glue_cdc_composite.py": BAD.encode(),
    "config/params.csv": b"parameter,value\naccount_id,123456789012\nnull_notnull_text,keep_text\n",
    "config/pipeline.json": b'{"account_id":"123456789012","db_password":"x"}',
    "config/_task/abc/_task.json": b"{}",
    "glue-templates/load.json": b'{"DefaultArguments":{"--post_load_check":"count_only"}}',
    "scripts/backup/job2_load.py.bak": b"old",
    "driver-cdc/pg8000-1.31.5.whl": b"w" * 10,
}
zp = base + "fake_lambda.zip"
with zipfile.ZipFile(zp, "w") as z:
    z.writestr("resolve_task.py", 'def f():\n    return "stopAfterLoad"\n')
    z.writestr("params_csv.py", "x = 1\n")


class Body:
    def __init__(s, b): s.b = b
    def read(s): return s.b


class S3:
    def list_objects_v2(s, Bucket, Prefix="", Delimiter=None, MaxKeys=1000, ContinuationToken=None):
        keys = sorted(FILES)
        if Delimiter:
            rest = [k[len(Prefix):] for k in keys if k.startswith(Prefix)]
            cps = sorted({Prefix + r.split("/")[0] + "/" for r in rest if "/" in r})
            cts = [{"Key": Prefix + r, "Size": len(FILES[Prefix + r]), "LastModified": datetime.datetime(2026, 10, 1)}
                   for r in rest if "/" not in r]
            return {"CommonPrefixes": [{"Prefix": p} for p in cps], "Contents": cts, "IsTruncated": False}
        cts = [{"Key": k, "Size": len(FILES[k]), "LastModified": datetime.datetime(2026, 10, 1)}
               for k in keys if k.startswith(Prefix)]
        return {"Contents": cts[:MaxKeys], "IsTruncated": False}

    def get_object(s, Bucket, Key): return {"Body": Body(FILES[Key])}


class Lam:
    def list_functions(s, **kw):
        return {"Functions": [
            {"FunctionName": "dms-dsql-resolve-task", "Runtime": "python3.12", "Handler": "resolve_task.handler",
             "CodeSha256": "AAA", "CodeSize": 1000, "LastModified": "2026-10-05",
             "Role": "arn:aws:iam::123456789012:role/tmp-x", "Timeout": 60, "MemorySize": 256},
            {"FunctionName": "dms-dsql-plan-split", "Runtime": "python3.12", "Handler": "plan_split.handler",
             "CodeSha256": "AAA", "CodeSize": 1000, "LastModified": "2026-10-05", "Role": "r"},
            {"FunctionName": "other-thing", "CodeSha256": "ZZZ"}]}

    def get_function(s, FunctionName): return {"Code": {"Location": "file://" + zp}}


class Sfn:
    def list_state_machines(s, **kw): return {"stateMachines": [{"name": "dms-dsql-startup", "stateMachineArn": "arn:x"}]}

    def describe_state_machine(s, stateMachineArn):
        d = {"StartAt": "ResolveTask", "States": {
            "ResolveTask": {"Type": "Task", "ResultSelector": {"taskArn.$": "$.a", "stopAfterLoad.$": "$.b"}},
            "IsLoadOnly": {"Type": "Choice"}}}
        return {"name": "dms-dsql-startup", "type": "STANDARD", "definition": json.dumps(d),
                "roleArn": "arn:aws:iam::123456789012:role/sfn"}


class Glue:
    def get_jobs(s, **kw):
        return {"Jobs": [{"Name": "dms-dsql-t1-load", "Command": {"Name": "glueetl", "ScriptLocation": "s3://b/scripts/job2_load.py"},
                          "GlueVersion": "4.0", "WorkerType": "G.4X", "NumberOfWorkers": 10, "Timeout": 2880,
                          "DefaultArguments": {"--post_load_check": "count_only", "--db_password": "SECRET",
                                               "--null_values": "\\N"}}]}


class Sts:
    def get_caller_identity(s): return {"Arn": "arn:aws:sts::123456789012:assumed-role/x/y"}


fake = types.ModuleType("boto3")
fake.client = lambda n, **k: {"s3": S3(), "lambda": Lam(), "stepfunctions": Sfn(), "glue": Glue(), "sts": Sts()}[n]
sys.modules["boto3"] = fake
ns = runpy.run_path(base + "deployment_fingerprint.py", run_name="not_main")
out = base + "test_out"
sys.argv = ["deployment_fingerprint.py", "--bucket", "example-bucket", "--project", "dms-dsql", "--region", "us-east-1", "--out", out, "--bundle"]
ns["main"]()

fails = []
r = json.load(open(out + "/report.json"))
flagged = [f["name"] for f in r["s3"]["files"] if f.get("CALL_BEFORE_DEF")]
if flagged != ["scripts/glue_cdc_composite.py"]: fails.append("call-before-def flag wrong: %r" % flagged)
if r["glue_jobs"][0]["default_arguments"].get("--db_password") != "<redacted>": fails.append("secret not redacted")
if "123456789012" in json.dumps(r): fails.append("account id not masked in report.json")
if "123456789012" in open(out + "/report.txt").read(): fails.append("account id not masked in report.txt")
names = [f["name"] for f in r["s3"]["files"]]
if any("/_task/" in n for n in names): fails.append("_task files should be skipped")
if any("backup" in n for n in names): fails.append("backup should be list-only")
if not r["s3"]["listed_only"]: fails.append("backup not listed")
if len(r["lambdas"]["packages"]) != 1: fails.append("lambda package dedupe failed")
if "stopAfterLoad.$" not in r["state_machines"][0]["ResolveTask_ResultSelector_keys"]: fails.append("selector keys")
if not os.path.exists(out + ".zip"): fails.append("bundle zip missing")
jf = [f for f in r["s3"]["files"] if f["name"] == "scripts/job2_load.py"][0]
if "post_load_check" not in jf["markers"] or "eof occurred in violation of protocol" not in jf["markers"]: fails.append("markers")
print("\nTEST RESULT:", "PASS" if not fails else "FAIL %r" % fails)
sys.exit(1 if fails else 0)
