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
  - glue:DeleteJob each per-task job name. "Not found" counts as deleted (idempotent); any other
    error is listed under "failed" ({job, error}) so the cutover workflow can stop on it.

mode="cdc_fallback"  (startup workflow, after a Python-shell CDC run FAILED right after start):
  - Reads the run's Glue error message ("error_message"). If it is a DRIVER problem (Glue's pip
    install of the --extra-py-files wheels failed, PyPI unreachable, a listed wheel missing, a
    wheel for the wrong Python, a driver module that can't be imported, a boto3 too old for
    DSQL), the CDC job is deleted and re-created with the SAME name as a Spark job (drivers go
    on sys.path, no pip), and s3://<bucket>/config/_task/<task>/_cdc_engine.json records the
    switch so later startups of this task keep using Spark. Any other error: nothing changes
    ({"switched": false}). Same payload as mode="create" plus "error_message", "failed_run_id".
  - Refused (raises) while another run of the CDC job is active.

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
  "validationExtraPyFiles",  # optional: driver-validation wheel list; second place a Spark CDC
                       # job's pg8000 stack is taken from (after extraPyFiles = driver-fullload)
  "glue_role_arn", "region",
  "dsql_endpoint", "dsql_user", "dsql_database",
  "cdc_root", "control_schema", "dms_task_arn",  # dms_task_arn used by the cdc role only
  "csv_null_value",    # optional: the DMS endpoint's CsvNullValue (how DMS writes a real NULL);
                       # set on load/load-big/validate/cdc as --csv_null_value ("" -> "__EMPTY__").
                       # Absent (older workflows): the scripts use the DMS default "NULL".
  "cdc_engine"         # optional: "pythonshell" (default, glue-templates/cdc.json) or "spark"
                       # (the shared startup workflow passes pipeline.json's cdc_engine)
                       # (glue-templates/cdc-spark.json). Also settable with the Lambda env var
                       # CDC_ENGINE. Same job name and same script either way.
  "cdc_fallback_reason" # optional: set when the driver check fell back to Spark before DMS
                       # started; the switch is recorded in the task's _cdc_engine.json.
}

CDC engines:
  pythonshell  Python 3.9, 1 DPU. Glue pip-installs --extra-py-files (driver-cdc/ list), so the
               wheels must have no Requires-Dist behind a firewall (driver-discovery prepares them automatically).
  spark        Glue 4.0 Spark (Python 3.10), 2 x G.1X. Same driver delivery as the full-load
               jobs: --extra-py-files = only the pg8000 stack (pg8000, scramp, asn1crypto, plus
               python_dateutil/six if present), picked BY NAME from driver-fullload/, or from
               driver-validation/ if driver-fullload/ doesn't hold exactly one of each (any other
               wheel in the folder is left out); added to sys.path, no pip. boto3 via
               --additional-python-modules from driver-cdc/ (those two folders hold no boto3 by
               design). Costs ~2x pythonshell per hour. The script does not use Spark; it just
               runs on the Spark driver.
  Switching engine on an existing job: Glue cannot change a job's type in place, so the job is
  deleted and re-created with the same name (refused while a run is active).
Returns: { "jobs": { "load": "<name>", "load-big": "...", "validate": "...", "cdc": "..." },
           "created": [...], "updated": [...], "deleted": [...], "failed": [...] (delete mode) }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
_ROLES = ["discovery", "load", "load-big", "validate", "cdc"]
_CDC_ENGINES = {"pythonshell": "cdc", "spark": "cdc-spark"}   # engine -> template file stem
# COMPOSITE CDC job: a second CDC job that applies ONLY multi-column-PK tables (the main cdc job
# skips them). Same engine/fallback rules as cdc; its own template stems + script.
_CDC_COMPOSITE_ROLE = "cdc-composite"
_CDC_COMPOSITE_ENGINES = {"pythonshell": "cdc-composite", "spark": "cdc-composite-spark"}
_CDC_ROLES = ("cdc", _CDC_COMPOSITE_ROLE)   # roles wired like the CDC job (args, engine, drivers)
_ACTIVE_RUN_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def _task_has_composite_tables(s3_client, bucket, config_prefix):
    """True if THIS task's manifest index has at least one multi-column-PK table (pk_mode ==
    'composite', written by job1_discovery). Used to decide whether to create/delete the
    composite CDC job: no composite tables -> no composite job. Best-effort: if the index can't
    be read (not written yet), returns False so we never create a composite job for a task that
    has none. config_prefix is an s3://.../ prefix; the index is <config_prefix>_manifest_index.json."""
    try:
        if not str(config_prefix).startswith("s3://"):
            return False
        _b, _, _k = config_prefix[len("s3://"):].partition("/")
        key = _k.rstrip("/") + "/_manifest_index.json" if _k and not _k.endswith("/") \
            else _k + "_manifest_index.json"
        obj = s3_client.get_object(Bucket=_b, Key=key)
        idx = json.loads(obj["Body"].read())
        for t in idx.get("tables", []):
            if t.get("pk_mode") == "composite" or len(t.get("pk_columns") or []) > 1:
                return True
        return False
    except Exception as e:
        print(f"(info) composite-table check: could not read index under {config_prefix} "
              f"({type(e).__name__}: {e}); assuming NO composite tables.")
        return False


# A Python-shell CDC run that fails with one of these never got as far as the script: Glue could
# not install or import the drivers. (The CDC script itself never starts a subprocess, so a
# CalledProcessError can only come from Glue's pip install of the --extra-py-files wheels.)
_DRIVER_ERROR_PATTERNS = [
    (r"pypi\.org|files\.pythonhosted\.org|\bpypi\b",
     "the driver install tried to reach PyPI"),
    (r"no matching distribution found|could not find a version that satisfies",
     "pip could not find a driver package"),
    (r"library file does ?n[o']?t exist",
     "a driver wheel saved on the job is missing from S3"),
    (r"\.whl\b[^\n]{0,300}?\b(install(ation)?\s+failed|failed|error|invalid|not found|does ?n[o']?t exist)"
     r"|\b(install(ation)?\s+failed|failed to install)\b[^\n]{0,300}?\.whl\b",
     "a driver wheel failed to install"),
    (r"\b(python\s+)?(module|library|libraries|package)s?\s+install(ation)?\s+failed"
     r"|installation of python (modules|libraries|packages) failed",
     "the driver install failed"),
    (r"calledprocesserror|\bpip3?\b[^\n]{0,300}?(returned non-zero|exit status)",
     "pip failed while installing the drivers"),
    (r"requires a different python|requires-python|is not a supported wheel on this platform",
     "a driver wheel doesn't fit the job's Python version"),
    (r"no module named '?(pg8000|scramp|asn1crypto|boto3|botocore|s3transfer|dateutil|six|urllib3|jmespath)\b",
     "a driver module could not be imported"),
    (r"cannot import name [^\n]{0,120}? from '?(pg8000|scramp|asn1crypto|boto3|botocore|s3transfer|dateutil|urllib3|jmespath)\b",
     "a driver module is broken or mismatched"),
    (r"unknown service:? *'?dsql",
     "the job's boto3 is too old for Aurora DSQL"),
]


def driver_error_reason(message):
    """Why a CDC run's Glue error message means the drivers failed, or "" if it doesn't."""
    import re
    msg = str(message or "")
    for pattern, why in _DRIVER_ERROR_PATTERNS:
        if re.search(pattern, msg, re.IGNORECASE):
            return why
    return ""


def _engine_file_key(config_prefix):
    cp = str(config_prefix or "")
    key = cp.split("/", 3)[3] if cp.startswith("s3://") and cp.count("/") >= 3 else cp.lstrip("/")
    return key.rstrip("/") + "/_cdc_engine.json"


def _cdc_engine(event):
    eng = (event.get("cdc_engine") or os.environ.get("CDC_ENGINE") or "pythonshell")
    eng = str(eng).strip().lower()
    if eng in ("glueetl", "pyspark"):
        eng = "spark"
    if eng not in _CDC_ENGINES:
        raise Exception(f"cdc_engine must be 'pythonshell' or 'spark' (got {eng!r})")
    return eng


def _active_runs(glue, name):
    runs = glue.get_job_runs(JobName=name, MaxResults=50).get("JobRuns", [])
    return [r["Id"] for r in runs if r.get("JobRunState") in _ACTIVE_RUN_STATES]


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _job_name(project, task_suffix, role):
    # Never truncate: cutting at 255 could give two roles (e.g. -load and -load-big) the same
    # name. resolve_task already refuses suffixes that are too long; this is the backstop.
    name = f"{project}-{task_suffix}-{role}"
    if len(name) > 255:
        raise Exception(f"Glue job name {name[:60]}... is {len(name)} characters (max 255). "
                        f"Use a shorter project or task name.")
    return name


# Wheels a Spark (glueetl) job needs to get a DSQL-aware boto3. Glue 4.0 bundles a boto3 that
# predates Aurora DSQL (-> "UnknownServiceError: Unknown service: 'dsql'"). The fix is to
# pip-install a modern boto3 via --additional-python-modules. It must NOT go on
# --extra-py-files: boto3 wheels there break botocore's data-dir lookup under Spark
# ("DataNotFoundError: endpoints"). We reuse the wheels already staged in driver-cdc/
# (passed in as cdcExtraPyFiles) so nothing is fetched from PyPI. jmespath / urllib3 /
# python-dateutil are left to Glue's bundled versions (they satisfy botocore); s3transfer
# is included because boto3 pins a matching s3transfer version.
_SPARK_BOTO3_PREFIXES = ("boto3-", "botocore-", "s3transfer-")


# The pg8000 stack a Spark CDC job puts on sys.path. Required: exactly one wheel of each in the
# folder used. Optional: added if the folder has exactly one (Glue 4.0 also bundles them).
_SPARK_CDC_REQUIRED = ("pg8000", "scramp", "asn1crypto")
_SPARK_CDC_OPTIONAL = ("python-dateutil", "six")


def _wheel_pkg(uri):
    """'s3://b/driver-fullload/python_dateutil-2.9.0-py2.py3-none-any.whl' -> 'python-dateutil'."""
    import re
    f = uri.rsplit("/", 1)[-1]
    if not f.lower().endswith(".whl"):
        return None
    return re.sub(r"[-_.]+", "-", f.split("-", 1)[0]).lower()


def _spark_cdc_drivers(sources):
    """Pick the Spark CDC job's pg8000 stack by name from the first folder that has exactly one
    wheel of each required package. sources = [(label, comma-separated S3 URIs), ...] in order
    of preference. Returns (csv, label, ignored_file_names). Wheels from two folders are never
    mixed, so pg8000 and scramp always come from the same tested set."""
    problems = []
    for label, csv_ in sources:
        uris = [w.strip() for w in (csv_ or "").split(",") if w.strip()]
        if not uris:
            problems.append(f"{label}: no wheels")
            continue
        by = {}
        for u in uris:
            by.setdefault(_wheel_pkg(u), []).append(u)
        bad = [f"no {n}" for n in _SPARK_CDC_REQUIRED if not by.get(n)]
        bad += [f"{len(by[n])} {n} wheels" for n in _SPARK_CDC_REQUIRED if len(by.get(n, [])) > 1]
        if bad:
            problems.append(f"{label}: {', '.join(bad)}")
            continue
        picked = [by[n][0] for n in _SPARK_CDC_REQUIRED]
        picked += [by[n][0] for n in _SPARK_CDC_OPTIONAL if len(by.get(n, [])) == 1]
        ignored = sorted(u.rsplit("/", 1)[-1] for u in uris if u not in picked)
        return ",".join(sorted(picked)), label, ignored
    raise Exception("The Spark CDC job needs one wheel each of " + ", ".join(_SPARK_CDC_REQUIRED) +
                    " from driver-fullload/ or driver-validation/, and neither folder has them: " +
                    "; ".join(problems) + ". Stage the 5 pg8000 wheels there (RUNBOOK Step 3b).")


def _spark_boto3_modules(wheel_csv):
    picks = []
    for w in (wheel_csv or "").split(","):
        w = w.strip()
        if w and w.rsplit("/", 1)[-1].lower().startswith(_SPARK_BOTO3_PREFIXES):
            picks.append(w)
    return ",".join(sorted(picks))


def _connections_for(tmpl, event):
    """Glue connection name(s) to attach so the job runs INSIDE your VPC (needed when DSQL,
    S3 or anything else is only reachable from the VPC). Resolution order:
      1. the template's "connections" (list or comma-separated string), if the key exists
      2. the event's "glue_connections"
      3. the Lambda env var GLUE_CONNECTIONS (comma-separated) -- lets you enable it on a
         live deployment without editing the state machine.
    Empty -> no connection (job runs on Glue's default network, the previous behaviour).
    Unfilled <<...>> placeholders are ignored, so step 3 still applies."""
    def _clean(raw):
        if isinstance(raw, str):
            raw = raw.split(",")
        return [c.strip() for c in (raw or []) if isinstance(c, str) and c.strip()
                and "<<" not in c]

    if tmpl.get("connections") is not None:
        return _clean(tmpl.get("connections"))
    # An unfilled "<<GLUE_CONNECTION>>" from a workflow (or an empty value) falls through to
    # the Lambda's GLUE_CONNECTIONS setting instead of silently creating jobs with no VPC.
    return _clean(event.get("glue_connections")) or _clean(os.environ.get("GLUE_CONNECTIONS", ""))


def handler(event, context):
    mode = event.get("mode", "create")
    bucket = event["bucket"]
    project = event["project"]
    task_suffix = event["taskSuffix"]

    glue = boto3.client("glue", region_name=REGION)
    names = {role: _job_name(project, task_suffix, role) for role in _ROLES}
    # The composite CDC job name is always resolvable (delete mode removes it idempotently even
    # if it was never created; _job_name enforces the 255-char limit). Whether it is CREATED is
    # decided below from the task's composite-table count.
    names[_CDC_COMPOSITE_ROLE] = _job_name(project, task_suffix, _CDC_COMPOSITE_ROLE)

    if mode == "delete":
        # A job that is already gone counts as deleted. Any other error is returned under
        # "failed" (the cutover workflow then ends at GlueJobsNotDeleted instead of succeeding).
        deleted, failed = [], []
        for role, name in names.items():
            try:
                glue.delete_job(JobName=name)
                deleted.append(name)
            except glue.exceptions.EntityNotFoundException:
                pass
            except Exception as e:
                print(f"(error) could not delete {name}: {e}")
                failed.append({"job": name, "error": f"{type(e).__name__}: {e}"[:500]})
        return {"jobs": names, "deleted": deleted, "failed": failed, "created": [], "updated": []}

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
    created, updated, replaced = [], [], []
    spark_cdc_drivers = None
    roles, fallback_reason = _ROLES, ""
    # ensure_composite result (only meaningful in that mode).
    has_composite = False
    if mode == "cdc_fallback":
        err_msg = str(event.get("error_message") or "")
        why = driver_error_reason(err_msg)
        if not why:
            print(f"(info) CDC run failed for a reason other than its drivers; job left as is: "
                  f"{err_msg[:500]}")
            return {"jobs": names, "switched": False, "reason": "", "cdcEngine": _cdc_engine(event),
                    "created": [], "updated": [], "replaced": [], "deleted": []}
        # Which CDC job fell back: the main cdc job (default) or the composite one. The startup
        # SM passes fallback_role when it is the composite job's Spark fallback.
        _fb_role = event.get("fallback_role", "cdc")
        if _fb_role not in _CDC_ROLES:
            _fb_role = "cdc"
        fallback_reason = (f"Python-shell CDC run {event.get('failed_run_id') or ''} failed: {why} "
                           f"({err_msg[:600]})")
        print(f"(info) {names[_fb_role]}: {fallback_reason}. Re-creating it as a Spark job.")
        roles, cdc_engine = [_fb_role], "spark"
    elif mode == "ensure_composite":
        # Called by the startup SM AFTER RunDiscovery, when THIS run's _manifest_index.json
        # (with pk_mode, written by job1_discovery) finally exists. Decide composite ownership
        # from that fresh index and create ONLY the composite CDC job if any table is composite.
        # (create mode runs BEFORE discovery and therefore can NOT see pk_mode, so composite
        # creation was moved here — see MERGE_NOTES.md.) No composite tables -> create nothing
        # and report hasCompositeTables=false so startup skips the composite start cleanly.
        cdc_engine = _cdc_engine(event)
        if cdc_engine == "spark" and event.get("cdc_fallback_reason"):
            fallback_reason = str(event["cdc_fallback_reason"])
        has_composite = _task_has_composite_tables(s3, bucket, config_prefix)
        if has_composite:
            roles = [_CDC_COMPOSITE_ROLE]
            print(f"(info) ensure_composite: task has composite-PK table(s); creating/updating "
                  f"composite CDC job {names[_CDC_COMPOSITE_ROLE]}.")
        else:
            roles = []
            print("(info) ensure_composite: task has no composite-PK tables; composite CDC job "
                  "NOT created.")
    else:
        # mode == create: the full per-task job set EXCEPT the composite CDC job. The composite
        # job cannot be decided here because discovery (which writes pk_mode) has not run yet;
        # it is created later by the ensure_composite call after RunDiscovery.
        cdc_engine = _cdc_engine(event)
        if cdc_engine == "spark" and event.get("cdc_fallback_reason"):
            fallback_reason = str(event["cdc_fallback_reason"])

    for role in roles:
        tmpl_stem = (_CDC_COMPOSITE_ENGINES[cdc_engine] if role == _CDC_COMPOSITE_ROLE
                     else _CDC_ENGINES[cdc_engine] if role == "cdc" else role)
        tmpl = _read_json(s3, bucket, f"{templates_prefix}/{tmpl_stem}.json")
        name = names[role]
        command_name = tmpl.get("command_name", "glueetl")
        if role in _CDC_ROLES and (command_name == "pythonshell") != (cdc_engine == "pythonshell"):
            raise Exception(f"{tmpl_stem}.json has command_name={command_name!r}, which does not "
                            f"match cdc_engine={cdc_engine!r}")
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
        if role == "discovery" and "cdc_root" in event:
            # The DMS endpoint's BucketFolder ("." = none), so discovery finds each table's
            # folder where DMS really writes it (same root the CDC job and drain check use).
            args["--cdc_root"] = cdc_root
        if role != "discovery" and event.get("csv_null_value") is not None:
            # Glue can't pass an empty argument value, so an empty marker travels as __EMPTY__.
            _nv = str(event["csv_null_value"])
            args["--csv_null_value"] = _nv if _nv != "" else "__EMPTY__"
        if role in _CDC_ROLES:
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
            # Spark jobs: Glue sets --JOB_NAME itself and its docs say never to set it.
            if command_name == "pythonshell":
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
            # Spark CDC engine: only the pg8000 stack on --extra-py-files, picked by name from
            # driver-fullload/ (else driver-validation/), so a stray wheel in the folder (e.g. a
            # boto3 that would break botocore under Spark) is left out; boto3 arrives through
            # --additional-python-modules below.
            if command_name == "pythonshell":
                _cdc_extra = event.get("cdcExtraPyFiles", "") or extra_py_files
                if _cdc_extra:
                    args["--extra-py-files"] = _cdc_extra
            else:
                _csv, _src, _ign = _spark_cdc_drivers(
                    [("driver-fullload", extra_py_files),
                     ("driver-validation", event.get("validationExtraPyFiles", ""))])
                args["--extra-py-files"] = _csv
                spark_cdc_drivers = {"from": _src, "wheels": _csv.split(","), "ignored": _ign}
                print(f"(info) {name}: Spark CDC drivers from {_src}: "
                      f"{', '.join(w.rsplit('/', 1)[-1] for w in _csv.split(','))}"
                      + (f" (left out: {', '.join(_ign)})" if _ign else ""))
        args.update(tmpl.get("default_arguments", {}) or {})

        # Spark jobs: deliver a DSQL-aware boto3 via --additional-python-modules (S3 wheels,
        # no PyPI). A template may set its own value to override this.
        if command_name != "pythonshell" and "--additional-python-modules" not in args:
            _mods = _spark_boto3_modules(event.get("cdcExtraPyFiles", ""))
            if not _mods:
                raise Exception(
                    f"No boto3/botocore/s3transfer wheels found in cdcExtraPyFiles for Spark job "
                    f"{name}. Stage them in s3://{bucket}/driver-cdc/ (Glue 4.0's bundled boto3 "
                    f"has no 'dsql' client), or set --additional-python-modules in {role}.json.")
            args["--additional-python-modules"] = _mods

        # Defensive placeholder substitution: a template's default_arguments may still
        # contain <<BUCKET>>/<<REGION>>/<<ACCOUNT_ID>> (e.g. --TempDir "s3://<<BUCKET>>/glue-temp/")
        # if the template was staged to S3 without the Step-3 sed pass. create_glue_jobs
        # merges default_arguments verbatim, so an un-substituted placeholder would reach
        # Glue literally (invalid TempDir -> load job fails). Substitute the known tokens
        # here so a created job can never carry a raw <<...>> placeholder.
        _subs = {
            "<<BUCKET>>": bucket,
            "<<REGION>>": region,
            "<<ACCOUNT_ID>>": (event.get("account_id") or ""),
            "<<PROJECT>>": project,
            "<<TASK_SUFFIX>>": task_suffix,
        }
        for _k, _v in list(args.items()):
            if isinstance(_v, str) and "<<" in _v:
                for _ph, _rep in _subs.items():
                    if _rep:
                        _v = _v.replace(_ph, _rep)
                args[_k] = _v
        # Guard: fail fast if any placeholder survived (better a clear error than a
        # silently-broken Glue job that fails minutes later on an invalid path).
        _leftover = {_k: _v for _k, _v in args.items()
                     if isinstance(_v, str) and "<<" in _v and ">>" in _v}
        if _leftover:
            raise Exception(
                f"Unresolved <<...>> placeholder(s) in Glue args for job {name}: "
                f"{_leftover}. Re-stage the {role}.json template with real values "
                f"(the Step-3 sed pass), or check the create-glue-jobs event payload.")

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

        _conns = _connections_for(tmpl, event)
        if _conns:
            job_kwargs["Connections"] = {"Connections": _conns}

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
                try:
                    _existing_job = glue.get_job(JobName=name).get("Job", {})
                except Exception as _ge:
                    _existing_job = {}
                    print(f"(warn) {name}: could not read existing job: {_ge}")
                # UpdateJob REPLACES the whole job definition: any field we omit is wiped.
                # If no connection is configured for this run, keep the one already on the job
                # (e.g. a VPC connection added in the Glue console) instead of silently
                # stripping it -- otherwise the job falls back to Glue's default network and
                # can no longer reach a VPC-only DSQL endpoint.
                if "Connections" not in upd:
                    _existing = _existing_job.get("Connections") or {}
                    if _existing.get("Connections"):
                        upd["Connections"] = {"Connections": _existing["Connections"]}
                        job_kwargs["Connections"] = upd["Connections"]
                        print(f"(info) {name}: keeping existing connection(s) "
                              f"{_existing['Connections']}")
                _old_type = (_existing_job.get("Command") or {}).get("Name")
                if _old_type and _old_type != command_name:
                    # Engine switch (pythonshell <-> glueetl): Glue can't change a job's type
                    # with UpdateJob, so delete and re-create under the same name. Never while
                    # a run is active (that would kill CDC mid-apply).
                    _act = _active_runs(glue, name)
                    if _act:
                        raise Exception(
                            f"{name} is a {_old_type} job and this run wants {command_name}; "
                            f"switching needs a delete + re-create, but run(s) {_act} are "
                            f"active. Stop them (aws glue batch-stop-job-run) and retry.")
                    print(f"(info) {name}: switching job type {_old_type} -> {command_name} "
                          f"(delete + re-create, same name)")
                    glue.delete_job(JobName=name)
                    glue.create_job(**job_kwargs)
                    replaced.append(name)
                else:
                    glue.update_job(JobName=name, JobUpdate=upd)
                    updated.append(name)
            else:
                raise

    out = {"jobs": names, "created": created, "updated": updated, "replaced": replaced,
           "deleted": [], "cdcEngine": cdc_engine}
    if mode == "ensure_composite":
        # The startup SM reads these into $.composite to decide whether to start the composite
        # CDC job (and with what job name). Computed from THIS run's discovery index.
        out["hasCompositeTables"] = has_composite
        out["compositeCdcJobName"] = names[_CDC_COMPOSITE_ROLE]
    if spark_cdc_drivers:
        out["sparkCdcDrivers"] = spark_cdc_drivers
    if fallback_reason and mode != "ensure_composite":
        # Record the switch so the next startup of this task builds the Spark job straight away
        # (resolve-task reads this file). Delete the file to go back to Python shell.
        from datetime import datetime, timezone
        key = _engine_file_key(config_prefix)
        doc = {"engine": "spark", "reason": fallback_reason,
               "stage": "after start" if mode == "cdc_fallback" else "driver check",
               "failedRunId": event.get("failed_run_id") or None,
               "errorMessage": str(event.get("error_message") or "")[:2000] or None,
               "job": names[_fb_role] if mode == "cdc_fallback" else names["cdc"],
               "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "undo": f"delete s3://{bucket}/{key} to build the Python-shell CDC job again"}
        s3.put_object(Bucket=bucket, Key=key, Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                      ContentType="application/json")
        out["engineFile"] = f"s3://{bucket}/{key}"
        print(f"(info) CDC engine for this task is now spark; recorded in s3://{bucket}/{key}")
    if mode == "cdc_fallback":
        out.update(switched=True, reason=fallback_reason)
    return out
