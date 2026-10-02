#!/usr/bin/env python3
"""
switch_cdc_engine.py -- switch an existing CDC Glue job between Python shell and Spark, in place,
without redeploying the Lambdas or the state machine. Same job name, same script, same
connection, same arguments; only the job type and the way drivers are delivered change.

  spark        Glue 4.0 Spark (Python 3.10), 2 x G.1X. Drivers delivered exactly like the full-load
               jobs (already proven behind a firewall): --extra-py-files = driver-fullload/ wheels
               (Glue adds them to sys.path, no pip) and boto3/botocore/s3transfer from driver-cdc/
               via --additional-python-modules. About 2x the hourly cost of Python shell.
  pythonshell  Glue Python shell (Python 3.9), 1 DPU. --extra-py-files = driver-cdc/ wheels (Glue
               pip-installs them, so behind a firewall they must be prepared with
               plan_a_python_shell/prepare_cdc_wheels.py).

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


def build_target(job, to, fullload, cdc):
    """Return CreateJob kwargs for the job converted to `to`, plus a list of problems."""
    problems = []
    kw = create_kwargs_from_job(job)
    args = dict(job.get("DefaultArguments") or {})
    cmd = {"Name": "glueetl" if to == "spark" else "pythonshell",
           "ScriptLocation": job["Command"]["ScriptLocation"]}
    for k in ("WorkerType", "NumberOfWorkers", "MaxCapacity"):
        kw.pop(k, None)
    if to == "spark":
        if not any(_base(w).startswith("pg8000-") for w in fullload):
            problems.append("no pg8000 wheel in driver-fullload/ (the Spark CDC job loads pg8000 "
                            "from there, like the full-load jobs)")
        if any(_base(w).startswith(("boto3-", "botocore-")) for w in fullload):
            problems.append("driver-fullload/ contains boto3/botocore wheels; on a Spark job's "
                            "--extra-py-files they cause 'DataNotFoundError: endpoints'")
        mods = [w for w in cdc if _base(w).startswith(SPARK_BOTO3)]
        if len({_base(m).split("-")[0] for m in mods}) != 3:
            problems.append("driver-cdc/ must contain exactly the boto3, botocore and s3transfer "
                            "wheels used for --additional-python-modules (found: "
                            f"{[_base(m) for m in mods]})")
        args.pop("--JOB_NAME", None)                  # Glue sets it; docs: never set it
        args["--extra-py-files"] = ",".join(fullload)
        args["--additional-python-modules"] = ",".join(sorted(mods))
        args.setdefault("--job-bookmark-option", "job-bookmark-disable")
        cmd["PythonVersion"] = "3"
        kw.update(GlueVersion="4.0", WorkerType="G.1X", NumberOfWorkers=2)
    else:
        if not any(_base(w).startswith("pg8000-") for w in cdc):
            problems.append("no pg8000 wheel in driver-cdc/")
        if not any(_base(w).startswith("boto3-") for w in cdc):
            problems.append("no boto3 wheel in driver-cdc/")
        args.pop("--additional-python-modules", None)  # Python shell can't take S3 wheels there
        args.pop("--job-bookmark-option", None)
        args["--extra-py-files"] = ",".join(cdc)
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
    ap.add_argument("--cdc-prefix", default="driver-cdc")
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
    cdc = list_wheels(s3, a.bucket, a.cdc_prefix)
    new, problems = build_target(job, a.to, fullload, cdc)
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
    print("Start it with:\n  " + start_command(a.job, a.region, new["DefaultArguments"]))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
