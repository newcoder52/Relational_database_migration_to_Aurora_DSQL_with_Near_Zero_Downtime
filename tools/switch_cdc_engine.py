#!/usr/bin/env python3
"""
switch_cdc_engine.py -- switch an existing CDC Glue job between Python shell and Spark, in place, by
hand. Same job name, same script, same connection, same arguments; only the job type and the way
drivers are delivered change. The startup workflow does this by itself when the CDC drivers fail
(cdc_spark_fallback); use this tool for a CDC job you run outside the workflow, or to switch back.

  spark        Glue 4.0 Spark (Python 3.10), 2 x G.1X. Drivers delivered like the full-load jobs and
               picked the same way as the pipeline does: --extra-py-files = pg8000, scramp,
               asn1crypto (+ python_dateutil, six) BY NAME from driver-fullload/, or all of them from
               driver-validation/ if driver-fullload/ lacks one (never mixed; any other wheel is left
               out); boto3/botocore/s3transfer from driver-cdc/ via --additional-python-modules.
  pythonshell  Glue Python shell (Python 3.9), 1 DPU. --extra-py-files = the newest complete
               prepared set in driver-cdc-prepared/ (written by the startup workflow's driver
               check; Glue pip-installs these without contacting PyPI). Run a startup once, or
               prepare by hand with lambdas/prepare_cdc_wheels.py, if there is none.

The task's choice is recorded the same way the workflow records it, in
config/_task/<task>/_cdc_engine.json (written when switching to Spark, removed when switching back),
so the next startup of the task builds the same kind of CDC job.

Glue cannot change a job's type in place, so the job is deleted and re-created under the same
name. The old definition is saved to a JSON file first and restored automatically if the
re-create fails. Refuses to run while the job has an active run.

USAGE (CloudShell; default is a dry run that changes nothing):
  python3 switch_cdc_engine.py --job <name> --region <region> --bucket <bucket> --to spark
  python3 switch_cdc_engine.py --job <name> --region <region> --bucket <bucket> --to spark --yes
  # roll back:
  python3 switch_cdc_engine.py --job <name> --region <region> --bucket <bucket> --to pythonshell --yes

Exit code 0 = done (or dry run OK), 1 = refused / failed.
"""
import argparse
import datetime
import json
import os
import sys

import boto3

ACTIVE = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
SPARK_BOTO3 = ("boto3-", "botocore-", "s3transfer-")
# CreateJob fields that can be copied from GetJob output.
COPY_FIELDS = ("Description", "LogUri", "Role", "ExecutionProperty", "Command", "DefaultArguments",
               "NonOverridableArguments", "Connections", "MaxRetries", "Timeout",
               "SecurityConfiguration", "NotificationProperty", "GlueVersion", "WorkerType",
               "NumberOfWorkers", "MaxCapacity", "ExecutionClass")


def list_wheels(s3, bucket, prefix):
    """Same selection as the driver_discovery Lambda: every .whl/.zip under the prefix."""
    prefix = prefix.strip("/") + "/"
    out, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.lower().endswith((".whl", ".zip")):
                out.append(f"s3://{bucket}/{k}")
        token = r.get("NextContinuationToken")
        if not token:
            break
    return sorted(out)


def _base(path):
    return path.rsplit("/", 1)[-1].lower()


def create_kwargs_from_job(job):
    kw = {"Name": job["Name"]}
    for f in COPY_FIELDS:
        if f in job and job[f] not in (None, {}, []):
            kw[f] = job[f]
    if "WorkerType" in kw:          # GetJob also reports MaxCapacity for Spark jobs; CreateJob
        kw.pop("MaxCapacity", None)  # rejects both together.
    return kw


SPARK_REQUIRED = ("pg8000", "scramp", "asn1crypto")
SPARK_OPTIONAL = ("python-dateutil", "six")


def _pkg(uri):
    import re
    f = uri.rsplit("/", 1)[-1]
    return re.sub(r"[-_.]+", "-", f.split("-", 1)[0]).lower() if f.lower().endswith(".whl") else None


def pick_spark_drivers(sources):
    """Same rule as create_glue_jobs: first folder with exactly one of each required wheel."""
    probs = []
    for label, uris in sources:
        by = {}
        for u in uris:
            by.setdefault(_pkg(u), []).append(u)
        bad = [f"no {n}" for n in SPARK_REQUIRED if not by.get(n)]
        bad += [f"{len(by[n])} {n} wheels" for n in SPARK_REQUIRED if len(by.get(n, [])) > 1]
        if bad:
            probs.append(f"{label}: {', '.join(bad)}")
            continue
        picked = [by[n][0] for n in SPARK_REQUIRED] + [by[n][0] for n in SPARK_OPTIONAL if len(by.get(n, [])) == 1]
        return sorted(picked), label, sorted(u.rsplit("/", 1)[-1] for u in uris if u not in picked), None
    return None, None, [], "; ".join(probs)


def newest_prepared_set(s3, bucket, prepared_prefix="driver-cdc-prepared", source_prefix="driver-cdc"):
    """Wheel list of the newest complete prepared set made from source_prefix, or None."""
    best, token = None, None
    pp = prepared_prefix.strip("/") + "/"
    while True:
        kw = {"Bucket": bucket, "Prefix": pp}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if not o["Key"].endswith("/_READY.json"):
                continue
            try:
                doc = json.loads(s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read())
            except Exception:
                continue
            if not str(doc.get("source", "")).rstrip("/").endswith("/" + source_prefix.strip("/")):
                continue
            if best is None or str(doc.get("prepared_at", "")) > str(best.get("prepared_at", "")):
                best = doc
        token = r.get("NextContinuationToken")
        if not token:
            break
    if not best or not best.get("wheels"):
        return None
    present = set()
    folder = best["wheels"][0].split("/", 3)[3].rsplit("/", 1)[0] + "/"
    for w in list_wheels(s3, bucket, folder):
        present.add(w)
    return best["wheels"] if all(w in present for w in best["wheels"]) else None


def engine_file_key(args):
    cp = str((args or {}).get("--config_prefix") or "")
    if not cp.startswith("s3://"):
        return None, None
    b, k = cp[5:].split("/", 1) if "/" in cp[5:] else (cp[5:], "")
    return b, k.rstrip("/") + "/_cdc_engine.json"


def build_target(job, to, fullload, cdc, validation=(), prepared=None):
    """Return CreateJob kwargs for the job converted to `to`, plus a list of problems."""
    problems = []
    kw = create_kwargs_from_job(job)
    args = dict(job.get("DefaultArguments") or {})
    cmd = {"Name": "glueetl" if to == "spark" else "pythonshell",
           "ScriptLocation": job["Command"]["ScriptLocation"]}
    for k in ("WorkerType", "NumberOfWorkers", "MaxCapacity"):
        kw.pop(k, None)
    if to == "spark":
        picked, src, ignored, why = pick_spark_drivers([("driver-fullload", fullload),
                                                        ("driver-validation", list(validation))])
        if not picked:
            problems.append("the Spark CDC job needs one wheel each of pg8000, scramp and asn1crypto "
                            "from driver-fullload/ or driver-validation/: " + why)
        else:
            print(f"Spark CDC drivers from {src}: {', '.join(_base(w) for w in picked)}"
                  + (f" (left out: {', '.join(ignored)})" if ignored else ""))
        mods = [w for w in cdc if _base(w).startswith(SPARK_BOTO3)]
        if len({_base(m).split("-")[0] for m in mods}) != 3:
            problems.append("driver-cdc/ must contain exactly the boto3, botocore and s3transfer "
                            "wheels used for --additional-python-modules (found: "
                            f"{[_base(m) for m in mods]})")
        args.pop("--JOB_NAME", None)                  # Glue sets it; docs: never set it
        args["--extra-py-files"] = ",".join(picked or [])
        args["--additional-python-modules"] = ",".join(sorted(mods))
        args.setdefault("--job-bookmark-option", "job-bookmark-disable")
        cmd["PythonVersion"] = "3"
        kw.update(GlueVersion="4.0", WorkerType="G.1X", NumberOfWorkers=2)
    else:
        if not prepared:
            problems.append("no complete prepared wheel set in driver-cdc-prepared/. A Python-shell job "
                            "pip-installs its wheels, so it needs the prepared copies: run a startup "
                            "once (its driver check writes them), or prepare them with "
                            "lambdas/prepare_cdc_wheels.py")
        elif not any(_base(w).startswith("pg8000-") for w in prepared) or not any(_base(w).startswith("boto3-") for w in prepared):
            problems.append("the prepared set has no pg8000 or no boto3 wheel")
        args.pop("--additional-python-modules", None)  # Python shell can't take S3 wheels there
        args.pop("--job-bookmark-option", None)
        args["--extra-py-files"] = ",".join(prepared or [])
        args["--JOB_NAME"] = job["Name"]               # Python shell does not inject it
        cmd["PythonVersion"] = "3.9"
        kw.update(GlueVersion="3.0", MaxCapacity=1.0)
    kw["Command"] = cmd
    kw["DefaultArguments"] = args
    kw["Timeout"] = min(int(job.get("Timeout") or 10080), 10080)
    kw["ExecutionProperty"] = {"MaxConcurrentRuns": 1}
    return kw, problems


def start_command(job_name, region, args):
    """Start command that passes --config_prefix / --dms_task_arn as RUN arguments. The cutover
    workflow's stop-cdc-run Lambda finds the CDC run by the --config_prefix in the run's own
    arguments (job defaults are not visible there), so a run started without it is not stopped
    at cutover."""
    run = {k: args[k] for k in ("--config_prefix", "--dms_task_arn") if args.get(k)}
    return (f"aws glue start-job-run --region {region} --job-name {job_name} "
            f"--arguments '{json.dumps(run)}'")


def main(argv=None, glue=None, s3=None):
    ap = argparse.ArgumentParser(description="Switch a CDC Glue job between Python shell and Spark.")
    ap.add_argument("--job", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--to", required=True, choices=["spark", "pythonshell"])
    ap.add_argument("--fullload-prefix", default="driver-fullload")
    ap.add_argument("--validation-prefix", default="driver-validation")
    ap.add_argument("--cdc-prefix", default="driver-cdc")
    ap.add_argument("--prepared-prefix", default="driver-cdc-prepared")
    ap.add_argument("--yes", action="store_true", help="apply (default: dry run)")
    ap.add_argument("--backup-dir", default=".", help="where to save the old definition")
    a = ap.parse_args(argv)
    glue = glue or boto3.client("glue", region_name=a.region)
    s3 = s3 or boto3.client("s3", region_name=a.region)

    job = glue.get_job(JobName=a.job)["Job"]
    cur = job["Command"]["Name"]
    want = "glueetl" if a.to == "spark" else "pythonshell"
    print(f"Job {a.job}: currently {cur}; target {want}")
    if cur == want:
        print("Already the target type; nothing to change.")
        print("Start it with:\n  " + start_command(a.job, a.region, job.get("DefaultArguments") or {}))
        return 0
    active = [r["Id"] for r in glue.get_job_runs(JobName=a.job, MaxResults=50).get("JobRuns", [])
              if r.get("JobRunState") in ACTIVE]
    if active:
        print(f"REFUSED: active run(s) {active}. Stop them first:\n"
              f"  aws glue batch-stop-job-run --region {a.region} --job-name {a.job} "
              f"--job-run-ids {' '.join(active)}")
        return 1

    fullload = list_wheels(s3, a.bucket, a.fullload_prefix)
    validation = list_wheels(s3, a.bucket, a.validation_prefix) if a.to == "spark" else []
    cdc = list_wheels(s3, a.bucket, a.cdc_prefix)
    prepared = newest_prepared_set(s3, a.bucket, a.prepared_prefix, a.cdc_prefix) if a.to == "pythonshell" else None
    new, problems = build_target(job, a.to, fullload, cdc, validation, prepared)
    if problems:
        print("REFUSED:")
        for p in problems:
            print("  - " + p)
        return 1

    show = {k: v for k, v in new.items() if k not in ("DefaultArguments",)}
    print("New definition:")
    print(json.dumps(show, indent=2, default=str))
    print("Arguments that change:")
    old_args = job.get("DefaultArguments") or {}
    for k in sorted(set(old_args) | set(new["DefaultArguments"])):
        o, n = old_args.get(k), new["DefaultArguments"].get(k)
        if o != n:
            fmt = lambda v: "(none)" if v is None else (v if len(v) < 160 else v[:157] + "...")
            print(f"  {k}\n     old: {fmt(o)}\n     new: {fmt(n)}")
    if not new.get("Connections"):
        print("WARNING: the job has no Glue connection; it will not run inside your VPC.")
    if not a.yes:
        print("\nDry run only. Re-run with --yes to apply.")
        return 0

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = os.path.join(a.backup_dir, f"{a.job}.{cur}.{stamp}.json")
    with open(backup, "w") as f:
        json.dump(create_kwargs_from_job(job), f, indent=2, default=str)
    print(f"\nSaved the current definition to {backup}")
    glue.delete_job(JobName=a.job)
    try:
        glue.create_job(**new)
    except Exception as e:
        print(f"FAILED to create the {want} job: {e}\nRestoring the original from {backup} ...")
        glue.create_job(**create_kwargs_from_job(job))
        print("Original job restored.")
        return 1
    chk = glue.get_job(JobName=a.job)["Job"]
    ok = (chk["Command"]["Name"] == want and
          (chk.get("Connections") or {}) == (new.get("Connections") or {}))
    print(("DONE" if ok else "CHECK FAILED") + f": {a.job} is now {chk['Command']['Name']}, "
          f"connections {(chk.get('Connections') or {}).get('Connections')}")
    eb, ek = engine_file_key(new["DefaultArguments"])
    if eb and ok:
        try:
            if a.to == "spark":
                s3.put_object(Bucket=eb, Key=ek, ContentType="application/json", Body=(json.dumps({
                    "engine": "spark", "reason": "switched by hand with tools/switch_cdc_engine.py",
                    "stage": "by hand", "job": a.job, "at": stamp,
                    "undo": f"delete s3://{eb}/{ek} to build the Python-shell CDC job again"}, indent=2) + "\n").encode())
                print(f"Recorded in s3://{eb}/{ek}: the next startup of this task builds the Spark CDC job.")
            else:
                s3.delete_object(Bucket=eb, Key=ek)
                print(f"Removed s3://{eb}/{ek} (if it existed): the next startup builds the Python-shell CDC job.")
        except Exception as e:
            print(f"WARNING: could not update s3://{eb}/{ek} ({e}); the next startup may build the other engine.")
    print("Start it with:\n  " + start_command(a.job, a.region, new["DefaultArguments"]))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
