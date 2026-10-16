#!/usr/bin/env python3
"""mrl_ensure_job Lambda - create/ensure the ONE dedicated per-task MRL load Glue job
(<project>-<suffix>-mrl-load), a copy of the load-big template but with the SEPARATE script
scripts/job2_load_manual_range.py (the patched loader) and
ExecutionProperty.MaxConcurrentRuns >= the max total concurrent range runs (CRITICAL: Glue
defaults to 1 concurrent run per job).

Does NOT overwrite the shared scripts/job2_load.py: the job points at
<scripts_prefix>/job2_load_manual_range.py. Tags the job like the existing jobs
(dsql_pipeline_project / dsql_pipeline_task + an MRL marker) so cutover/cleanup recognise it.
Idempotent: CreateJob, else UpdateJob (never while a run is active for a type switch - this job
is always glueetl). Refuses to modify a job that exists with DIFFERENT task tags (G5-style).

Event: { bucket, project, taskSuffix, region, accountId?, glue_templates_prefix, scripts_prefix,
         glue_role_arn, glue_connections?, extraPyFiles?,
         dsql_endpoint, dsql_endpoint_candidates?, dsql_user, dsql_database, region,
         csv_null_value?, null_values?, null_rules?,
         maxTotalConcurrentRuns, loadWorkerType?, loadWorkers?, loadTimeoutMinutes?, glueVersion? }
Returns: { jobName, maxConcurrentRuns, created|updated, tags }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
_ALLOWED_WORKER_TYPES = {"G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X",
                         "R.1X", "R.2X", "R.4X", "R.8X"}
_MRL_SCRIPT = "job2_load_manual_range.py"


class MrlEnsureJobError(Exception):
    pass


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _get_tags(glue, region, account, name):
    try:
        arn = "arn:aws:glue:%s:%s:job/%s" % (region, account, name)
        return glue.get_tags(ResourceArn=arn).get("Tags", {}) or {}
    except Exception as e:
        if type(e).__name__ == "EntityNotFoundException":
            return {}
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("EntityNotFoundException", "EntityNotFound"):
            return {}
        raise


def handler(event, context):
    bucket = event["bucket"]
    project = event["project"]
    suffix = event["taskSuffix"]
    region = event.get("region", REGION)
    account = str(event.get("accountId") or "").strip()
    templates_prefix = event.get("glue_templates_prefix", "glue-templates").strip("/")
    scripts_prefix = event.get("scripts_prefix", "scripts").strip("/")
    glue_role_arn = event["glue_role_arn"]
    max_total = int(event.get("maxTotalConcurrentRuns", 1) or 1)

    glue = boto3.client("glue", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    name = "%s-%s-mrl-load" % (project, suffix)
    if len(name) > 255:
        raise MrlEnsureJobError("Glue job name %r is %d chars (max 255); use a shorter "
                                "project/task name." % (name, len(name)))

    # Base on the load-big template (biggest driver, right defaults), but override the script.
    # AMENDMENT 9.2: the range-load job uses its OWN dedicated template glue-templates/mrl-load.json
    # (never load-big.json), whose "script" is the patched loader. Both live under NEW names so a
    # setup run never overwrites an existing key.
    tmpl = _read_json(s3, bucket, "%s/mrl-load.json" % templates_prefix)
    script_key = tmpl.get("script", _MRL_SCRIPT)
    script_location = "s3://%s/%s/%s" % (bucket, scripts_prefix, script_key)

    args = {
        "--dsql_endpoint": event["dsql_endpoint"],
        "--dsql_user": event.get("dsql_user", "admin"),
        "--dsql_database": event.get("dsql_database", "postgres"),
        "--region": region,
        "--s3_bucket": bucket,
        "--dms_bucket": bucket,
        "--enable-continuous-cloudwatch-log": "true",
        "--job-language": "python",
    }
    if event.get("dsql_endpoint_candidates"):
        args["--dsql_endpoint_candidates"] = event["dsql_endpoint_candidates"]
    if event.get("extraPyFiles"):
        args["--extra-py-files"] = event["extraPyFiles"]
    # NULL handling: same sentinels create_glue_jobs uses (Glue can't pass empty values).
    if event.get("csv_null_value") is not None:
        nv = str(event["csv_null_value"])
        args["--csv_null_value"] = nv if nv != "" else "__EMPTY__"
    if event.get("null_values") is not None:
        v = str(event["null_values"])
        args["--null_values"] = v if v != "" else "__NULL_UNSET__"
    if event.get("null_rules") is not None:
        v = str(event["null_rules"])
        args["--null_rules"] = v if v != "" else "__NULL_UNSET__"
    # Merge template default_arguments (e.g. --TempDir) + placeholder substitution.
    args.update(tmpl.get("default_arguments", {}) or {})
    subs = {"<<BUCKET>>": bucket, "<<REGION>>": region, "<<ACCOUNT_ID>>": account,
            "<<PROJECT>>": project, "<<TASK_SUFFIX>>": suffix}
    for k, v in list(args.items()):
        if isinstance(v, str) and "<<" in v:
            for ph, rep in subs.items():
                if rep:
                    v = v.replace(ph, rep)
            args[k] = v
    leftover = {k: v for k, v in args.items()
                if isinstance(v, str) and "<<" in v and ">>" in v}
    if leftover:
        raise MrlEnsureJobError("Unresolved <<...>> placeholder(s) in MRL load job args: %s. "
                                "Re-stage the load-big template with real values." % leftover)

    worker_type = str(event.get("loadWorkerType") or tmpl.get("worker_type", "G.4X")).upper()\
        .replace(" ", "")
    if worker_type not in _ALLOWED_WORKER_TYPES:
        raise MrlEnsureJobError("Invalid worker_type %r (allowed %s)."
                                % (worker_type, sorted(_ALLOWED_WORKER_TYPES)))
    num_workers = int(event.get("loadWorkers") or tmpl.get("number_of_workers", 20))
    timeout_min = int(event.get("loadTimeoutMinutes") or tmpl.get("timeout_minutes", 2880))
    glue_version = str(event.get("glueVersion") or tmpl.get("glue_version", "4.0"))
    # MaxConcurrentRuns MUST cover every concurrent range run (+1 headroom for a resume while a
    # sibling is still finishing).
    max_concurrent = max(int(tmpl.get("max_concurrent_runs", 1)), max_total + 1)

    tags = {"dsql_pipeline_project": project, "dsql_pipeline_task": suffix,
            "dsql_pipeline_mrl": "load"}

    # G5-style tag safety: refuse a name owned by a different task / hand-made job.
    if account:
        existing = _get_tags(glue, region, account, name)
        if existing and (existing.get("dsql_pipeline_project") != project
                         or existing.get("dsql_pipeline_task") != suffix):
            raise MrlEnsureJobError(
                "Refusing to modify Glue job %r: it exists with tags %s that do not match this "
                "task (project=%r, task=%r)." % (name, existing, project, suffix))

    job_kwargs = {
        "Name": name,
        "Role": glue_role_arn,
        "Command": {"Name": "glueetl", "PythonVersion": "3", "ScriptLocation": script_location},
        "DefaultArguments": args,
        "Timeout": max(1, min(10080, timeout_min)),
        "GlueVersion": glue_version,
        "WorkerType": worker_type,
        "NumberOfWorkers": num_workers,
        "ExecutionProperty": {"MaxConcurrentRuns": max_concurrent},
        "Tags": dict(tags),
    }
    conns = event.get("glue_connections")
    if isinstance(conns, str):
        conns = [c.strip() for c in conns.split(",") if c.strip() and "<<" not in c]
    if conns:
        job_kwargs["Connections"] = {"Connections": conns}

    action = "created"
    try:
        glue.create_job(**job_kwargs)
    except Exception as e:
        n = type(e).__name__
        if n in ("AlreadyExistsException", "IdempotentParameterMismatchException") \
                or "already exists" in str(e).lower() or "already submitted" in str(e).lower():
            upd = {k: v for k, v in job_kwargs.items() if k not in ("Name", "Tags")}
            # Preserve an existing VPC connection if this run didn't supply one.
            if "Connections" not in upd:
                try:
                    ex = glue.get_job(JobName=name).get("Job", {})
                    exc = (ex.get("Connections") or {}).get("Connections")
                    if exc:
                        upd["Connections"] = {"Connections": exc}
                except Exception:
                    pass
            glue.update_job(JobName=name, JobUpdate=upd)
            action = "updated"
            if account:
                try:
                    glue.tag_resource(
                        ResourceArn="arn:aws:glue:%s:%s:job/%s" % (region, account, name),
                        TagsToAdd=dict(tags))
                except Exception as te:
                    print("(warn) could not tag %s: %s" % (name, te))
        else:
            raise

    print("(info) mrl_ensure_job: %s %s (MaxConcurrentRuns=%d, %s x%d)"
          % (action, name, max_concurrent, worker_type, num_workers))
    return {"jobName": name, "maxConcurrentRuns": max_concurrent, "action": action,
            "tags": tags}
