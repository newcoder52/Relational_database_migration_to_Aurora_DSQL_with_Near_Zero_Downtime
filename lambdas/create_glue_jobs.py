"""
create-glue-jobs Lambda (manual-kit; the Step Functions manages Glue jobs at runtime).

The S3 bucket (source of truth) holds Glue job-DEFINITION TEMPLATES under a glue-templates
prefix. This Lambda reads those templates and, for THIS task, creates the per-task Glue jobs
(<project>-<task>-load, -load-big, -validate, -cdc) with the task's own config prefix + the
discovered driver wheels. On cutover it DELETES them (per-task teardown; no accumulation).

mode="create":
  - For each role in {load, load-big, validate, cdc}: read glue-templates/<role>.json from S3,
    fill in Name, Role, ScriptLocation (from the template's script key under the scripts
    prefix), and DefaultArguments (--config_prefix, --extra-py-files, --dsql_*, etc.), then
    glue:CreateJob. If the job already exists (idempotent re-run/resume) -> glue:UpdateJob.
  - Returns the created job names so the state machine can StartJobRun them.

mode="delete":
  - glue:DeleteJob each per-task job name. Ignores "not found" (idempotent).

Template JSON shape (each glue-templates/<role>.json), with <<PLACEHOLDERS>> the Lambda fills:
  {
    "role": "load|load-big|validate|cdc",
    "script": "job2_load.py",                 # key under <scripts_prefix>/
    "command_name": "glueetl|pythonshell",
    "glue_version": "4.0",
    "worker_type": "G.4X",                     # glueetl only
    "number_of_workers": 10,                   # glueetl only
    "max_capacity": 1,                         # pythonshell only
    "timeout_minutes": 480,
    "default_arguments": { "--foo": "bar" }    # static extras; merged with computed args
  }

Input event: {
  "mode": "create" | "delete",
  "bucket", "glue_templates_prefix", "scripts_prefix",
  "project", "taskSuffix", "configPrefix", "extraPyFiles",
  "cdcExtraPyFiles",   # optional: driver-cdc wheel list (incl. boto3) for the cdc role's stored default
  "glue_role_arn", "region",
  "dsql_endpoint", "dsql_user", "dsql_database",
  "cdc_root", "control_schema", "dms_task_arn"   # dms_task_arn used by the cdc role only
}
Returns: { "jobs": { "load": "<name>", "load-big": "...", "validate": "...", "cdc": "..." },
           "created": [...], "updated": [...], "deleted": [...] }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
_ROLES = ["discovery", "load", "load-big", "validate", "cdc"]


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _job_name(project, task_suffix, role):
    return f"{project}-{task_suffix}-{role}"[:255]


def handler(event, context):
    mode = event.get("mode", "create")
    bucket = event["bucket"]
    project = event["project"]
    task_suffix = event["taskSuffix"]

    glue = boto3.client("glue", region_name=REGION)
    names = {role: _job_name(project, task_suffix, role) for role in _ROLES}

    if mode == "delete":
        deleted = []
        for role, name in names.items():
            try:
                glue.delete_job(JobName=name)
                deleted.append(name)
            except glue.exceptions.EntityNotFoundException:
                pass
            except Exception as e:
                print(f"(warn) delete {name}: {e}")
        return {"jobs": names, "deleted": deleted, "created": [], "updated": []}

    # mode == create
    templates_prefix = event["glue_templates_prefix"].strip("/")
    scripts_prefix = event["scripts_prefix"].strip("/")
    config_prefix = event["configPrefix"]
    extra_py_files = event.get("extraPyFiles", "")
    glue_role_arn = event["glue_role_arn"]
    region = event.get("region", REGION)
    dsql_endpoint = event["dsql_endpoint"]
    dsql_user = event.get("dsql_user", "admin")
    dsql_database = event.get("dsql_database", "postgres")
    cdc_root = event.get("cdc_root", "cdc")
    control_schema = event.get("control_schema", "cdc_control")
    # CDC needs the DMS task ARN so glue_cdc_continuous can scope its control tables /
    # metrics / stop logic to THIS task. Passed through from the startup SM (which has the
    # task ARN hardcoded). Absent for non-cdc roles.
    dms_task_arn = event.get("dms_task_arn", "")

    s3 = boto3.client("s3", region_name=REGION)
    created, updated = [], []

    for role in _ROLES:
        tmpl = _read_json(s3, bucket, f"{templates_prefix}/{role}.json")
        name = names[role]
        command_name = tmpl.get("command_name", "glueetl")
        script_key = tmpl["script"]
        script_location = f"s3://{bucket}/{scripts_prefix}/{script_key}"

        # Computed args every job gets; template default_arguments merged on top.
        # NOTE the two bucket args are the SAME bucket under different names because the
        # scripts request them under different flags: job1_discovery reads --dms_bucket (to
        # derive s3://<bucket>/<schema>/<table>/ full-load paths); glue_cdc_continuous reads
        # --s3_bucket. job2/job3 use neither (they derive paths from the manifest). Passing
        # both is harmless — each script only reads the flag it asks for; the other is an
        # ignored extra DefaultArgument.
        args = {
            "--config_prefix": config_prefix,
            "--dsql_endpoint": dsql_endpoint,
            "--dsql_user": dsql_user,
            "--dsql_database": dsql_database,
            "--region": region,
            "--s3_bucket": bucket,
            "--dms_bucket": bucket,
            "--enable-continuous-cloudwatch-log": "true",
            "--job-language": "python",
        }
        if extra_py_files:
            args["--extra-py-files"] = extra_py_files
        if role == "cdc":
            args["--cdc_root"] = cdc_root
            args["--control_schema"] = control_schema
            _ts_col = event.get("timestampColumnName")
            if _ts_col:
                # The DMS TimestampColumnName (CDC watermark), derived from the endpoint by
                # resolve_task. Omit to let the CDC script default to 'dms_timestamp'.
                args["--timestamp_column"] = _ts_col
            if dms_task_arn:
                args["--dms_task_arn"] = dms_task_arn
            # Python Shell jobs do NOT auto-inject --JOB_NAME the way Spark (glueetl) jobs do,
            # but the script calls getResolvedOptions(sys.argv, ['JOB_NAME']) -> must pass it.
            args["--JOB_NAME"] = name
            # CDC (pythonshell) needs a dsql-aware boto3 delivered as S3 WHEELS on
            # --extra-py-files (the in-script shim promotes them ahead of Glue's bundled,
            # too-old boto3). The glueetl jobs must NOT get boto3 wheels (they break botocore's
            # data-dir resolution under Spark -> "DataNotFoundError: endpoints"; they use
            # --additional-python-modules instead). Rather than couple to a hardcoded wheel
            # folder, the orchestrator stages driver wheels in PER-JOB folders and passes the
            # cdc-specific list (driver-cdc/, which includes boto3+botocore) as `cdcExtraPyFiles`.
            # Use it for the cdc role's stored default; the startup SM also passes the same
            # list as --extra-py-files at run time (authoritative). Falls back to the shared
            # extraPyFiles if cdcExtraPyFiles was not provided.
            _cdc_extra = event.get("cdcExtraPyFiles", "") or extra_py_files
            if _cdc_extra:
                args["--extra-py-files"] = _cdc_extra
        args.update(tmpl.get("default_arguments", {}) or {})

        command = {"Name": command_name, "PythonVersion": "3",
                   "ScriptLocation": script_location}
        job_kwargs = {
            "Name": name,
            "Role": glue_role_arn,
            "Command": command,
            "DefaultArguments": args,
            "Timeout": int(tmpl.get("timeout_minutes", 480)),
            "ExecutionProperty": {"MaxConcurrentRuns":
                                  int(tmpl.get("max_concurrent_runs", 10))},
        }
        if command_name == "pythonshell":
            # Python Shell jobs are NOT Spark — capacity is set via MaxCapacity. The runtime
            # is pinned via Command.PythonVersion="3.9" + GlueVersion (the bare "3" runtime is
            # retired -> InvalidInputException "Python Shell version no longer available").
            command["PythonVersion"] = "3.9"
            job_kwargs["GlueVersion"] = tmpl.get("glue_version", "3.0")
            job_kwargs["MaxCapacity"] = float(tmpl.get("max_capacity", 1))
        else:
            job_kwargs["GlueVersion"] = tmpl.get("glue_version", "4.0")
            job_kwargs["WorkerType"] = tmpl.get("worker_type", "G.4X")
            job_kwargs["NumberOfWorkers"] = int(tmpl.get("number_of_workers", 10))

        try:
            glue.create_job(**job_kwargs)
            created.append(name)
        except Exception as _ce:
            # Job already exists — either AlreadyExistsException, or
            # IdempotentParameterMismatchException ("already submitted with different
            # configuration") when a prior run/manual edit left it with a different config.
            # Both mean "exists" -> update the definition in place (idempotent re-run/resume).
            _n = type(_ce).__name__
            if _n in ("AlreadyExistsException", "IdempotentParameterMismatchException") \
               or "already exists" in str(_ce).lower() \
               or "already submitted" in str(_ce).lower():
                upd = {k: v for k, v in job_kwargs.items() if k != "Name"}
                glue.update_job(JobName=name, JobUpdate=upd)
                updated.append(name)
            else:
                raise

    return {"jobs": names, "created": created, "updated": updated, "deleted": []}
