#!/usr/bin/env python3
"""
deployment_fingerprint.py  --  READ-ONLY inventory of what is deployed for the DMS -> Aurora DSQL pipeline.

Run it in AWS CloudShell (or any shell with AWS credentials) in the account/region where the pipeline lives. It only READS:
  s3:ListBucket / s3:GetObject, lambda:ListFunctions / lambda:GetFunction,
  states:ListStateMachines / states:DescribeStateMachine, glue:GetJobs, sts:GetCallerIdentity.
It writes nothing to AWS. It writes one report to the local CloudShell disk:
  deployment_fingerprint_<timestamp>/report.txt   (short, paste-able)
  deployment_fingerprint_<timestamp>/report.json  (everything)
  deployment_fingerprint_<timestamp>.zip          (only with --bundle: copies of the deployed files)

What it records: sha256 of every deployed script / template / config file / Lambda source file /
state machine definition (so each can be matched to an exact commit of the public repo), a list of
feature markers found inside each file, whether any module calls a function before it is defined,
the Glue job settings (script, DefaultArguments, workers, timeout), and the shared Lambda package.

Usage (--bucket and --project are required; --region defaults to $AWS_REGION / $AWS_DEFAULT_REGION):
  python3 deployment_fingerprint.py --bucket <pipeline-bucket> --project <project-prefix> --region <region>
  python3 deployment_fingerprint.py ... --bundle     # also zip copies of the deployed files
  python3 deployment_fingerprint.py ... --extra-prefix glue-scripts/   # hash one more S3 prefix
Account ids (12 digits) are masked in report.txt / report.json. The --bundle zip is NOT masked.
"""
import argparse
import ast
import datetime
import hashlib
import io
import json
import os
import re
import sys
import urllib.request
import zipfile

try:
    import boto3
except Exception as e:  # pragma: no cover
    sys.exit("boto3 is required (CloudShell has it): %s" % e)

# Strings whose presence/absence tells which version/patch a file is.
MARKERS = [
    "post_load_check", "POST_LOAD_COUNT_RANGE_THRESHOLD_ROWS", "POST_LOAD_COUNT_SOCKET_READ_TIMEOUT",
    "null_notnull_text", "keep_text", "stopAfterLoad", "_load_only",
    "null_rules", "null_values", "_apply_null_settings",
    "eof occurred in violation of protocol", "is_broken_pipe_error",
    "COUNT_TABLE_BUDGET_SECONDS", "COUNT_TASK_BUDGET_SECONDS", "checksum_mode", "VALIDATE_SOCKET_READ_TIMEOUT",
    "COUNT_SPLIT_ROWS", "COUNT_SOCKET_READ_TIMEOUT",
    "load_file_start", "_apply_manual_range_overrides",
    "_dt_isoish", "last_done_file", "REQUIRE_FULL_LOAD_DONE",
    "IsLoadOnly", "TaskLoadOnlySucceeded", "StartAt",
]
SECRET_RE = re.compile(r"(passw|secret|token|credential|private)", re.I)
ACCT_RE = re.compile(r"\b\d{8}(\d{4})\b")
SKIP_SUBSTR = ("/_task/", "/_task_index/", "/_file_status/", "/processed/", "/failed/", "/mrl/runs/")
LIST_ONLY_SUBSTR = ("/backup", "backup/")
MAX_HASH_BYTES = 3 * 1024 * 1024
DEFAULT_HASH_PREFIXES = ["scripts/", "glue-templates/", "config/", "lambdas/", "stepfunctions/", "tools/", "iam/"]


def sha(b):
    return hashlib.sha256(b).hexdigest()


def mask(s):
    return ACCT_RE.sub(lambda m: "********" + m.group(1), s) if isinstance(s, str) else s


def mask_obj(o):
    if isinstance(o, str):
        return mask(o)
    if isinstance(o, list):
        return [mask_obj(x) for x in o]
    if isinstance(o, dict):
        return {k: ("<redacted>" if SECRET_RE.search(str(k)) else mask_obj(v)) for k, v in o.items()}
    return o


def markers_in(text):
    return {m: text.count(m) for m in MARKERS if m in text}


def order_issues(text):
    """Module-level code that CALLS a module-level function before that function's def (the
    NameError-at-import class of bug). Returns [] when fine or not Python."""
    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        return ["SYNTAX ERROR: %s" % e]
    defs = {n.name: n.lineno for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    out = []
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for node in ast.walk(stmt):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                d = defs.get(node.func.id)
                if d is not None and node.lineno < d:
                    out.append("%s() called at line %d, defined at line %d" % (node.func.id, node.lineno, d))
    return sorted(set(out))


def fetch(url):
    with urllib.request.urlopen(url, timeout=90) as r:
        return r.read()


def describe_blob(name, data, bundle, bundle_key):
    rec = {"name": name, "size": len(data), "sha256": sha(data)}
    text = None
    if name.endswith((".py", ".json", ".csv", ".txt", ".sh", ".md", ".asl")):
        try:
            text = data.decode("utf-8", "replace")
        except Exception:
            text = None
    if text is not None:
        mk = markers_in(text)
        if mk:
            rec["markers"] = mk
        if name.endswith(".py"):
            iss = order_issues(text)
            if iss:
                rec["CALL_BEFORE_DEF"] = iss
    if bundle is not None and bundle_key:
        bundle[bundle_key] = data
    return rec


def section(report, name, fn):
    try:
        report[name] = fn()
    except Exception as e:  # keep going; record the failure (usually AccessDenied)
        report[name] = {"ERROR": "%s: %s" % (type(e).__name__, str(e)[:300])}


def s3_keys(s3, bucket, prefix, recursive=True, cap=5000):
    out, tok = [], None
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
        if not recursive:
            kw["Delimiter"] = "/"
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        out += r.get("Contents", []) or []
        if r.get("IsTruncated") and len(out) < cap:
            tok = r.get("NextContinuationToken")
        else:
            break
    return out


def collect_s3(s3, bucket, extra_prefixes, bundle):
    res = {"bucket": bucket, "top_level": {}, "files": [], "listed_only": [], "skipped_large": []}
    r = s3.list_objects_v2(Bucket=bucket, Delimiter="/", MaxKeys=1000)
    prefixes = [p["Prefix"] for p in r.get("CommonPrefixes", []) or []]
    res["top_level"]["prefixes"] = prefixes
    res["top_level"]["files"] = [c["Key"] for c in r.get("Contents", []) or []]
    wanted = list(DEFAULT_HASH_PREFIXES) + list(extra_prefixes or [])
    for p in prefixes:
        if re.search(r"script|glue|template|lambda|asl|stepfn|workflow", p, re.I) and p not in wanted:
            wanted.append(p)
    for p in wanted:
        if p not in prefixes and not any(k.startswith(p) for k in res["top_level"]["files"]):
            # prefix may still exist even if the delimiter listing missed it; probe cheaply
            probe = s3.list_objects_v2(Bucket=bucket, Prefix=p, MaxKeys=1)
            if not probe.get("Contents"):
                continue
        objs = s3_keys(s3, bucket, p, recursive=(p != "config/"))
        for o in objs:
            k = o["Key"]
            if k.endswith("/") or any(s in k for s in SKIP_SUBSTR):
                continue
            if any(s in k for s in LIST_ONLY_SUBSTR):
                res["listed_only"].append({"key": k, "size": o.get("Size"),
                                           "last_modified": str(o.get("LastModified"))})
                continue
            if o.get("Size", 0) > MAX_HASH_BYTES:
                res["skipped_large"].append({"key": k, "size": o.get("Size")})
                continue
            body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
            rec = describe_blob(k, body, bundle, "s3/" + k)
            rec["last_modified"] = str(o.get("LastModified"))
            res["files"].append(rec)
    # driver folders: names + sizes only (wheel versions matter, content does not)
    drv = {}
    for p in prefixes:
        if p.startswith("driver"):
            drv[p] = [{"key": o["Key"], "size": o.get("Size")} for o in s3_keys(s3, bucket, p, cap=500)
                      if not o["Key"].endswith("/")]
    res["driver_folders"] = drv
    return res


def collect_lambdas(lam, project, bundle):
    names, tok = [], None
    while True:
        kw = {"MaxItems": 50}
        if tok:
            kw["Marker"] = tok
        r = lam.list_functions(**kw)
        names += [f for f in r.get("Functions", []) if project in f["FunctionName"]]
        tok = r.get("NextMarker")
        if not tok:
            break
    out = {"functions": [], "packages": {}}
    for f in names:
        out["functions"].append({
            "name": f["FunctionName"], "runtime": f.get("Runtime"), "handler": f.get("Handler"),
            "code_sha256": f.get("CodeSha256"), "code_size": f.get("CodeSize"),
            "last_modified": f.get("LastModified"), "timeout": f.get("Timeout"),
            "memory": f.get("MemorySize"), "layers": [l.get("Arn") for l in f.get("Layers", []) or []],
            "role": mask(f.get("Role", "")),
        })
        cs = f.get("CodeSha256")
        if cs in out["packages"] or (f.get("CodeSize") or 0) > 50 * 1024 * 1024:
            continue
        url = lam.get_function(FunctionName=f["FunctionName"])["Code"]["Location"]
        z = zipfile.ZipFile(io.BytesIO(fetch(url)))
        files = []
        for zi in z.infolist():
            if zi.is_dir() or zi.file_size > MAX_HASH_BYTES:
                continue
            if not zi.filename.endswith((".py", ".json", ".csv", ".txt")):
                continue
            data = z.read(zi.filename)
            files.append(describe_blob(zi.filename, data, bundle,
                                       "lambda/%s/%s" % (cs[:12] if cs else "pkg", zi.filename)))
        out["packages"][cs] = {"first_function": f["FunctionName"], "files": files}
    return out


def collect_sfn(sfn, project, bundle):
    out, tok = [], None
    sms = []
    while True:
        kw = {"maxResults": 100}
        if tok:
            kw["nextToken"] = tok
        r = sfn.list_state_machines(**kw)
        sms += [s for s in r.get("stateMachines", []) if project in s["name"]]
        tok = r.get("nextToken")
        if not tok:
            break
    for s in sms:
        d = sfn.describe_state_machine(stateMachineArn=s["stateMachineArn"])
        text = d.get("definition", "")
        rec = {"name": d.get("name"), "type": d.get("type"), "sha256_raw": sha(text.encode()),
               "markers": markers_in(text), "role": mask(d.get("roleArn", ""))}
        try:
            j = json.loads(text)
            rec["sha256_normalized"] = sha(json.dumps(j, sort_keys=True, separators=(",", ":")).encode())
            rec["state_count"] = len(j.get("States", {}))
            rs = (j.get("States", {}).get("ResolveTask", {}) or {}).get("ResultSelector", {}) or {}
            rec["ResolveTask_ResultSelector_keys"] = sorted(rs.keys())
        except Exception as e:
            rec["parse_error"] = str(e)[:200]
        out.append(rec)
        if bundle is not None:
            bundle["stepfunctions/%s.asl.json" % d.get("name")] = text.encode()
    return out


def collect_glue(glue, project, bundle):
    out, tok = [], None
    while True:
        kw = {"MaxResults": 200}
        if tok:
            kw["NextToken"] = tok
        r = glue.get_jobs(**kw)
        for j in r.get("Jobs", []):
            if project not in j["Name"]:
                continue
            rec = {
                "name": j["Name"], "script": j.get("Command", {}).get("ScriptLocation"),
                "command": j.get("Command", {}).get("Name"), "glue_version": j.get("GlueVersion"),
                "worker_type": j.get("WorkerType"), "workers": j.get("NumberOfWorkers"),
                "timeout_min": j.get("Timeout"), "max_concurrent": j.get("ExecutionProperty", {}).get("MaxConcurrentRuns"),
                "connections": (j.get("Connections") or {}).get("Connections"),
                "default_arguments": mask_obj(j.get("DefaultArguments", {})),
                "last_modified": str(j.get("LastModifiedOn")),
            }
            out.append(rec)
            if bundle is not None:
                bundle["glue/%s.json" % j["Name"]] = json.dumps(mask_obj(rec), indent=2, default=str).encode()
        tok = r.get("NextToken")
        if not tok:
            break
    return out


def render(report, args):
    L = []
    w = L.append
    w("Pipeline deployment fingerprint  (read-only)   generated %s" % report["generated_utc"])
    w("project=%s  region=%s  bucket=%s  identity=%s" % (args.project, args.region, args.bucket,
                                                         mask(str(report.get("identity")))))
    w("")
    s3 = report.get("s3", {})
    w("== S3 (%s) ==" % args.bucket)
    if "ERROR" in s3:
        w("  ERROR: " + s3["ERROR"])
    else:
        w("  top-level prefixes: " + ", ".join(s3["top_level"]["prefixes"]))
        w("  top-level files: " + ", ".join(s3["top_level"]["files"]))
        for f in sorted(s3["files"], key=lambda x: x["name"]):
            w("  %-58s %9d  %s  %s" % (f["name"], f["size"], f["sha256"], f.get("last_modified", "")[:19]))
            if f.get("markers"):
                w("      markers: " + ", ".join("%s(%d)" % kv for kv in sorted(f["markers"].items())))
            if f.get("CALL_BEFORE_DEF"):
                w("      !! CALL BEFORE DEF: " + "; ".join(f["CALL_BEFORE_DEF"]))
        if s3["listed_only"]:
            w("  backup objects (names only): " + ", ".join(x["key"] for x in s3["listed_only"][:40]))
        if s3["skipped_large"]:
            w("  skipped (too large to hash): " + ", ".join(x["key"] for x in s3["skipped_large"]))
        for p, items in s3["driver_folders"].items():
            w("  %s : %s" % (p, ", ".join("%s(%d)" % (i["key"].split("/")[-1], i["size"]) for i in items)))
    w("")
    lam = report.get("lambdas", {})
    w("== LAMBDAS ==")
    if "ERROR" in lam:
        w("  ERROR: " + lam["ERROR"])
    else:
        for f in lam["functions"]:
            w("  %-44s %-12s %s  mod=%s" % (f["name"], f["runtime"], f["code_sha256"], str(f["last_modified"])[:19]))
        for cs, pk in lam["packages"].items():
            w("  package %s (first seen on %s):" % (cs, pk["first_function"]))
            for f in sorted(pk["files"], key=lambda x: x["name"]):
                w("    %-40s %8d  %s" % (f["name"], f["size"], f["sha256"]))
                if f.get("markers"):
                    w("        markers: " + ", ".join("%s(%d)" % kv for kv in sorted(f["markers"].items())))
                if f.get("CALL_BEFORE_DEF"):
                    w("        !! CALL BEFORE DEF: " + "; ".join(f["CALL_BEFORE_DEF"]))
    w("")
    w("== STEP FUNCTIONS ==")
    sf = report.get("state_machines")
    if isinstance(sf, dict) and "ERROR" in sf:
        w("  ERROR: " + sf["ERROR"])
    else:
        for s in sf or []:
            w("  %-40s states=%s raw=%s" % (s["name"], s.get("state_count"), s["sha256_raw"]))
            w("      normalized=%s" % s.get("sha256_normalized"))
            if s.get("markers"):
                w("      markers: " + ", ".join("%s(%d)" % kv for kv in sorted(s["markers"].items())))
            if s.get("ResolveTask_ResultSelector_keys") is not None:
                w("      ResolveTask ResultSelector keys (%d): %s" % (len(s["ResolveTask_ResultSelector_keys"]),
                                                                       " ".join(s["ResolveTask_ResultSelector_keys"])))
    w("")
    w("== GLUE JOBS ==")
    gl = report.get("glue_jobs")
    if isinstance(gl, dict) and "ERROR" in gl:
        w("  ERROR: " + gl["ERROR"])
    else:
        for j in gl or []:
            w("  %s  [%s %s x%s timeout=%s ver=%s]" % (j["name"], j["command"], j["worker_type"], j["workers"],
                                                       j["timeout_min"], j["glue_version"]))
            w("      script: %s" % j["script"])
            w("      args: %s" % json.dumps(j["default_arguments"], sort_keys=True))
    w("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="READ-ONLY fingerprint of the deployed DMS->DSQL pipeline")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    ap.add_argument("--bucket", required=True, help="the pipeline S3 bucket")
    ap.add_argument("--project", required=True, help="the project prefix used in Lambda / state machine / Glue job names")
    ap.add_argument("--extra-prefix", action="append", default=[], help="additional S3 prefix to hash, e.g. glue-scripts/")
    ap.add_argument("--bundle", action="store_true", help="also zip copies of the deployed files (NOT masked)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    if not args.region:
        ap.error("--region is required when AWS_REGION / AWS_DEFAULT_REGION is not set")

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = args.out or ("deployment_fingerprint_%s" % ts)
    os.makedirs(out_dir, exist_ok=True)
    bundle = {} if args.bundle else None

    c = lambda n: boto3.client(n, region_name=args.region)
    report = {"generated_utc": ts, "region": args.region, "bucket": args.bucket, "project": args.project}
    section(report, "identity", lambda: c("sts").get_caller_identity().get("Arn"))
    section(report, "s3", lambda: collect_s3(c("s3"), args.bucket, args.extra_prefix, bundle))
    section(report, "lambdas", lambda: collect_lambdas(c("lambda"), args.project, bundle))
    section(report, "state_machines", lambda: collect_sfn(c("stepfunctions"), args.project, bundle))
    section(report, "glue_jobs", lambda: collect_glue(c("glue"), args.project, bundle))

    txt = render(report, args)
    with open(os.path.join(out_dir, "report.txt"), "w") as f:
        f.write(txt)
    with open(os.path.join(out_dir, "report.json"), "w") as f:
        json.dump(mask_obj(report), f, indent=1, default=str)
    if bundle is not None:
        zp = out_dir.rstrip("/") + ".zip"
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
            for k, v in sorted(bundle.items()):
                z.writestr(k, v)
            z.write(os.path.join(out_dir, "report.txt"), "report.txt")
        print("bundle: %s  (%d files, NOT masked)" % (zp, len(bundle)))
    print(txt)
    print("\nWrote %s/report.txt and report.json  (nothing was changed in AWS)" % out_dir)


if __name__ == "__main__":
    main()
