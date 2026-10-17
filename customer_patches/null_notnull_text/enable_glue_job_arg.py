#!/usr/bin/env python3
"""Set (or remove) ONE Glue job argument on a DEPLOYED environment's load/validate/CDC jobs,
safely, with NO repo checkout and NO params.csv plumbing. Modelled on the customer
enable_post_load_check.py, generalised to an arbitrary `--arg-name --arg-value`.

Primary use: turn the NOT NULL char/text keep-text rule on in a running environment:

    python3 enable_glue_job_arg.py --project dms-dsql --region us-east-2 \
            --arg-name --null_notnull_text --arg-value keep_text          # DRY RUN (default)
    python3 enable_glue_job_arg.py --project dms-dsql --region us-east-2 \
            --arg-name --null_notnull_text --arg-value keep_text --apply   # do it
    ... --arg-name --null_notnull_text --remove --apply                    # take it off again
    python3 enable_glue_job_arg.py --restore ~/nnjobargs/<job>.<ts>.json   # put one job back

SAFETY (same contract as enable_post_load_check.py):
  * DRY RUN by default: prints what WOULD change, changes nothing. --apply to act.
  * Only touches Glue jobs named <project>-*  whose ScriptLocation ends in ONE of the four
    scripts job2_load.py / job3_validate.py / glue_cdc_continuous.py / glue_cdc_composite.py
    AND whose name matches the load/validate/cdc/ck-*/bg-* role filter. Never discovery, never
    an MRL/other job, never a job whose script is not one of the four (wrong-shape -> SKIP).
  * Backs up the FULL current job definition (local file + optional S3) BEFORE any change.
  * UpdateJob replaces the whole definition, so we copy EVERY field from GetJob and change only
    DefaultArguments; after the update we GetJob again and compare every field, printing the
    restore command loudly on any drift.
  * Running job runs are unaffected (they keep the args they started with).

This file is NOT for the public repo (it is an operator tool); it has no customer constants
baked in (project/region/bucket are CLI args), so it is hygiene-clean.
"""
import argparse
import copy
import json
import os
import re
import sys
import time

try:
    import boto3
except Exception:   # the offline tests inject a fake and never import the real one
    boto3 = None

# The four scripts whose jobs this tool may touch (suffix match on ScriptLocation).
SCRIPT_SUFFIXES = ("/job2_load.py", "/job3_validate.py",
                   "/glue_cdc_continuous.py", "/glue_cdc_composite.py")
# Job-name role filter: load | load-big | validate | cdc | ck-* | bg-* (the fork CDC jobs).
_ROLE_RE = re.compile(r"-(load|load-big|validate|cdc|ck-[A-Za-z0-9]+-cdc|bg-[A-Za-z0-9]+-cdc)$")
# Fields GetJob returns that UpdateJob must NOT receive back.
NEVER_SEND = {"Name", "CreatedOn", "LastModifiedOn", "AllocatedCapacity", "ProfileName"}
BACKUP_DIR = os.path.expanduser("~/nnjobargs/job_backups")


def _code(e):
    return str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
               or type(e).__name__)


def is_target_job(name, project, job):
    """True iff this job is one we may modify: name starts with <project>-, matches the role
    filter, is not an MRL job, and its script is one of the four."""
    if not name.startswith(project + "-"):
        return False
    if "-mrl-" in name:
        return False
    if not _ROLE_RE.search(name):
        return False
    script = (job.get("Command") or {}).get("ScriptLocation", "") or ""
    return any(script.endswith(s) for s in SCRIPT_SUFFIXES)


def build_update(job):
    """JobUpdate = every field of the current job except the read-only ones."""
    upd = {k: v for k, v in job.items() if k not in NEVER_SEND}
    if job.get("WorkerType"):
        upd.pop("MaxCapacity", None)   # a job cannot carry both
    upd["DefaultArguments"] = dict(job.get("DefaultArguments") or {})
    return upd


def compare_jobs(before, after, expect_args):
    diffs = []
    skip = set(NEVER_SEND) | {"DefaultArguments"}
    if before.get("WorkerType"):
        skip.add("MaxCapacity")
    for k, v in before.items():
        if k in skip:
            continue
        if after.get(k) != v:
            diffs.append("%s: before=%r after=%r" % (k, v, after.get(k)))
    if (after.get("DefaultArguments") or {}) != expect_args:
        diffs.append("DefaultArguments: expected %r got %r"
                     % (expect_args, after.get("DefaultArguments")))
    return diffs


def _list_job_names(glue):
    names = []
    try:
        pg = glue.get_paginator("get_jobs")
        for page in pg.paginate():
            for j in page.get("Jobs", []):
                names.append(j["Name"])
        if names:
            return names
    except Exception:
        pass
    # fallback: list_jobs (and the fake)
    tok = None
    while True:
        kw = {"MaxResults": 200}
        if tok:
            kw["NextToken"] = tok
        r = glue.list_jobs(**kw)
        names += r.get("JobNames", []) or []
        tok = r.get("NextToken")
        if not tok:
            break
    return names


def save_backup(name, job, ts, s3=None, bucket=None):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path = os.path.join(BACKUP_DIR, "%s.%s.json" % (name, ts))
    body = json.dumps(job, indent=2, default=str, sort_keys=True)
    with open(path, "w") as f:
        f.write(body)
    if s3 is not None and bucket:
        try:
            s3.put_object(Bucket=bucket, Key="scripts/backup/jobargs/%s.%s.json" % (name, ts),
                          Body=body.encode("utf-8"), ContentType="application/json")
        except Exception as e:
            print("  (warn) S3 backup failed for %s: %s" % (name, _code(e)))
    return path


def run(glue, project, arg_name, arg_value=None, remove=False, apply=False,
        only=None, s3=None, bucket=None, backups=None):
    """Core engine (testable with a fake glue client). Returns (rc, changed, skipped):
      changed = names that were (or, in dry run, would be) modified;
      skipped = names skipped (not a target job).
    `backups` (optional dict) is filled {name: backup_path} for the restore tests."""
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    changed, skipped, problems = [], [], 0
    print("== %s %s%s | project=%s | %s" %
          ("REMOVE" if remove else "SET", arg_name,
           "" if remove else "=%s" % arg_value, project,
           "APPLY" if apply else "DRY RUN (nothing is changed)"))
    for name in sorted(_list_job_names(glue)):
        if only and only not in name:
            continue
        job = glue.get_job(JobName=name)["Job"]
        if not is_target_job(name, project, job):
            skipped.append(name)
            script = (job.get("Command") or {}).get("ScriptLocation", "")
            print("SKIP   %-40s (%s)" % (name, script or "no script"))
            continue
        cur = dict(job.get("DefaultArguments") or {})
        want = dict(cur)
        if remove:
            want.pop(arg_name, None)
        else:
            want[arg_name] = str(arg_value)
        if want == cur:
            print("OK     %-40s already %s" % (name, ("without " if remove else "with ") + arg_name))
            continue
        changed.append(name)
        print("%s %-40s %s %s" % ("CHANGE" if apply else "WOULD ", name,
                                  "remove" if remove else "set", arg_name))
        if not apply:
            continue
        path = save_backup(name, job, ts, s3=s3, bucket=bucket)
        if backups is not None:
            backups[name] = path
        upd = build_update(job)
        upd["DefaultArguments"] = want
        try:
            glue.update_job(JobName=name, JobUpdate=upd)
        except Exception as e:
            problems += 1
            print("FAILED %s: update_job refused (%s) -- NOTHING CHANGED for this job"
                  % (name, _code(e)))
            continue
        after = glue.get_job(JobName=name)["Job"]
        diffs = compare_jobs(job, after, want)
        if diffs:
            problems += 1
            print("!!!!!! VERIFY FAILED for %s:" % name)
            for d in diffs:
                print("         " + d)
            print("!!!!!! restore:  python3 %s --restore %s" % (sys.argv[0], path))
        else:
            print("       verified: every field identical; backup %s" % path)
    print("== done. would-change/changed: %d  skipped: %d  problems: %d%s"
          % (len(changed), len(skipped), problems,
             "" if apply else "   (dry run; add --apply)"))
    return (1 if problems else 0), changed, skipped


def restore_job(glue, path):
    job = json.load(open(path))
    name = job["Name"]
    upd = build_update(job)
    glue.update_job(JobName=name, JobUpdate=upd)
    after = glue.get_job(JobName=name)["Job"]
    diffs = compare_jobs(job, after, dict(job.get("DefaultArguments") or {}))
    if diffs:
        print("RESTORE of %s did not match:" % name)
        for d in diffs:
            print("   " + d)
        return 1
    print("RESTORED %s from %s (verified)" % (name, path))
    return 0


def main(argv):
    ap = argparse.ArgumentParser(description="Set/remove one Glue job argument on the "
                                 "load/validate/cdc/ck/bg jobs of a project (safe).")
    ap.add_argument("--project")
    ap.add_argument("--region")
    ap.add_argument("--bucket", default=None, help="optional: also copy backups to this S3 bucket")
    ap.add_argument("--arg-name", help="e.g. --null_notnull_text")
    ap.add_argument("--arg-value", default=None, help="e.g. keep_text")
    ap.add_argument("--remove", action="store_true", help="remove the arg instead of setting it")
    ap.add_argument("--apply", action="store_true", help="actually change (default is dry run)")
    ap.add_argument("--only", default=None, help="only jobs whose name contains this substring")
    ap.add_argument("--restore", default=None, help="restore ONE job from a backup json")
    a = ap.parse_args(argv)
    if boto3 is None:
        print("boto3 is required to talk to AWS")
        return 2
    glue = boto3.client("glue", region_name=a.region)
    s3 = boto3.client("s3", region_name=a.region) if a.bucket else None
    if a.restore:
        return restore_job(glue, a.restore)
    if not (a.project and a.arg_name and (a.remove or a.arg_value is not None)):
        ap.error("need --project, --arg-name and (--arg-value or --remove)")
    rc, _, _ = run(glue, project=a.project, arg_name=a.arg_name, arg_value=a.arg_value,
                   remove=a.remove, apply=a.apply, only=a.only, s3=s3, bucket=a.bucket)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
