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
  "dsql_endpoint_candidates",   # optional: ordered PrivateLink/public failover list (CSV) from
                       # resolve_task; set on every job as --dsql_endpoint_candidates. Absent/
                       # empty -> scripts use --dsql_endpoint only (backward compatible).
  "cdc_root", "control_schema", "dms_task_arn",  # dms_task_arn used by the cdc role only
  "cdc_validation",        # optional: true|false (default true). Set on every CDC job as
                           # --cdc_validation; turns the Tier-2 deferred validation on/off.
  "cdc_validation_sample", # optional: int (default 20; 0 = all). Set on every CDC job as
                           # --cdc_validation_sample. Absent (older workflows): script uses ON/20.
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

# AWS-docs-verified Glue worker types (worker-types.html). Defensive allow-list so a hand-built
# event can't set an invalid WorkerType (resolve_task already validates the params.csv values).
_ALLOWED_WORKER_TYPES = {"G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X",
                         "R.1X", "R.2X", "R.4X", "R.8X"}

# Map a template STEM to the sizing-key PREFIX in the event. Composite fork load/validate reuse
# the same stems ("load"/"load-big"/"validate"), so forks inherit the same sizing automatically.
_SIZING_STEM_TO_PREFIX = {
    "discovery": "discovery", "load": "load", "load-big": "loadBig", "validate": "validate",
}


def _sizing_for(event, tmpl_stem):
    """Per-role Glue job sizing from the event's resolved settings. Returns a dict with any of
    worker_type / num_workers / timeout_minutes / glue_version that the event supplies for this
    role's STEM, or {} if none. The SM passes the resolved sizing fields (loadWorkerType, ...);
    absent (older workflows) -> {} -> the template defaults stand (full backward compatibility)."""
    pfx = _SIZING_STEM_TO_PREFIX.get(tmpl_stem)
    if not pfx:
        return {}
    out = {}
    _wt = event.get(f"{pfx}WorkerType")
    _nw = event.get(f"{pfx}NumWorkers")
    _tm = event.get(f"{pfx}TimeoutMinutes")
    _gv = event.get("glueVersion")
    if _wt:
        out["worker_type"] = _wt
    if _nw not in (None, ""):
        out["num_workers"] = _nw
    if _tm not in (None, ""):
        out["timeout_minutes"] = _tm
    if _gv:
        out["glue_version"] = _gv
    return out


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


def _best_effort_stop_runs(glue, name):
    """Issue batch_stop_job_run for every STARTING/RUNNING run of a job (B21). Best-effort: a
    run already stopping/gone, or a transient Glue error, is ignored — the next DeleteGlueJobs
    loop pass checks again. Returns the run ids a stop was requested for."""
    try:
        runs = glue.get_job_runs(JobName=name, MaxResults=50).get("JobRuns", [])
    except Exception:
        return []
    ids = [r["Id"] for r in runs if r.get("JobRunState") in ("STARTING", "RUNNING")]
    if ids:
        try:
            glue.batch_stop_job_run(JobName=name, JobRunIds=ids[:25])
            print(f"(info) delete: requested stop of {len(ids[:25])} active run(s) of {name}")
        except Exception as e:
            print(f"(warn) delete: could not batch-stop runs of {name}: "
                  f"{type(e).__name__}: {e}")
    return ids[:25]


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _remaining_ms(context):
    """Milliseconds left before this Lambda is killed, or None when unknown (context=None in
    unit tests, or an old runtime without the method). None => callers use their static budget."""
    try:
        fn = getattr(context, "get_remaining_time_in_millis", None)
        return int(fn()) if callable(fn) else None
    except Exception:
        return None


# Never block a Lambda right up to its own timeout (B21): leave at least this much time to do the
# actual delete + return a clean result. The ASL loops DeleteGlueJobs, so an unfinished stop is
# retried on the next pass rather than killing the Lambda mid-flight.
_WAIT_MARGIN_MS = 30_000


def _wait_runs_stopped(glue, name, attempts=30, delay=10, context=None):
    """Wait for a job's STARTING/RUNNING/STOPPING run to reach a stopped state (G3) BEFORE
    deleting it, but never wait past the Lambda's own timeout (B21). Returns True once no run is
    active (or the job is gone); returns False if runs are still active when the time budget runs
    out — the caller then leaves that job for the next DeleteGlueJobs pass instead of blocking to
    a Sandbox.Timedout. A composite CDC run slow to stop therefore no longer times the Lambda
    out; it just stays 'pending' for one more loop.

    Budget = min(attempts*delay, remaining Lambda time - margin). With context=None (unit tests)
    the static attempts*delay budget is used unchanged."""
    import time
    deadline = None
    rem = _remaining_ms(context)
    if rem is not None:
        deadline = time.monotonic() + max(0.0, (rem - _WAIT_MARGIN_MS) / 1000.0)
    for _i in range(attempts):
        try:
            act = _active_runs(glue, name)
        except glue.exceptions.EntityNotFoundException:
            return True
        except Exception:
            return True
        if not act:
            return True
        # Stop if we're out of static attempts or about to run into the Lambda timeout.
        if deadline is not None and (time.monotonic() + delay) >= deadline:
            return False
        time.sleep(delay)
    # Static budget exhausted: report whether anything is still active.
    try:
        return not _active_runs(glue, name)
    except Exception:
        return True


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _job_arn(region, account_id, name):
    return f"arn:aws:glue:{region}:{account_id}:job/{name}"


_STS_ACCOUNT_CACHE = {"id": None}


def _account_from_context(context):
    """Account id from the Lambda's OWN invoked_function_arn (field 4 of
    arn:aws:lambda:<region>:<account>:function:<name>). Always correct — the Lambda runs in the
    pipeline account that owns the Glue jobs — and needs no extra IAM. Returns "" if unavailable
    (e.g. a unit test passing context=None)."""
    arn = str(getattr(context, "invoked_function_arn", "") or "")
    _p = arn.split(":")
    return _p[4] if len(_p) > 4 else ""


def _account_from_sts():
    """Last-resort account id via sts:GetCallerIdentity (cached for the warm container). Only
    reached when neither the event nor the Lambda context carried it. Returns "" if the call
    fails rather than raising, so the caller can emit its own clear error."""
    if _STS_ACCOUNT_CACHE["id"]:
        return _STS_ACCOUNT_CACHE["id"]
    try:
        acct = str(boto3.client("sts", region_name=REGION).get_caller_identity().get("Account")
                   or "").strip()
    except Exception as e:
        print(f"(warn) sts:GetCallerIdentity could not resolve the account id: "
              f"{type(e).__name__}: {e}")
        acct = ""
    _STS_ACCOUNT_CACHE["id"] = acct
    return acct


def _account_id(event, context=None):
    """The account id that owns this task's Glue jobs, needed to build job ARNs for get_tags.

    Resolution order (B20 — never fail just because the ASL payload omitted it):
      1. event["account_id"] (what resolve_task emits as $.resolved.accountId),
      2. field 4 of event["dms_task_arn"] (the cutover input carries taskArn),
      3. field 4 of the Lambda's OWN context.invoked_function_arn (same account as the jobs),
      4. sts:GetCallerIdentity (cached).
    Returns "" only if every source is unavailable."""
    acct = str(event.get("account_id") or "").strip()
    if not acct:
        _p = str(event.get("dms_task_arn") or "").split(":")
        acct = _p[4] if len(_p) > 4 else ""
    if not acct:
        acct = _account_from_context(context)
    if not acct:
        acct = _account_from_sts()
    return acct


def _get_job_tags(glue, region, account_id, name):
    """The tag dict on a Glue job, or {} if the job doesn't exist. Raises on access/throttle
    errors so callers fail closed (never a silent partial view)."""
    try:
        return glue.get_tags(ResourceArn=_job_arn(region, account_id, name)).get("Tags", {}) or {}
    except Exception as e:
        if type(e).__name__ == "EntityNotFoundException":
            return {}
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("EntityNotFoundException", "EntityNotFound"):
            return {}
        raise


def _select_task_jobs_by_tag(glue, region, account_id, project, task_suffix, fork_slug=None):
    """EXACT-TAG job selection (NEVER by name prefix/substring — see G1). Returns the set of job
    names whose tags match dsql_pipeline_project==project AND dsql_pipeline_task==task_suffix
    (AND dsql_pipeline_fork==fork_slug when given). Pages list_jobs, reads each job's tags.
    Raises on any list/get-tags error so the caller fails closed (a silent partial view could
    leave a CDC run going after cutover)."""
    selected = set()
    token = None
    while True:
        kw = {"MaxResults": 200}
        if token:
            kw["NextToken"] = token
        resp = glue.list_jobs(**kw)
        for n in resp.get("JobNames", []) or []:
            tags = _get_job_tags(glue, region, account_id, n)
            if tags.get("dsql_pipeline_project") == project and \
               tags.get("dsql_pipeline_task") == task_suffix and \
               (fork_slug is None or tags.get("dsql_pipeline_fork") == fork_slug):
                selected.add(n)
        token = resp.get("NextToken")
        if not token:
            break
    return selected


# ---- per-task job REGISTRY (G2): the ONE source of truth, config/_task/<suffix>/_jobs.json -----
def _registry_key(config_prefix, task_suffix):
    """Key of the task registry _jobs.json. config_prefix is the TASK config prefix
    (s3://.../config/_task/<suffix>/) for create/ensure_fork_jobs; for a fork fallback the
    task-level prefix is derived from task_suffix."""
    cp = str(config_prefix or "")
    if cp.startswith("s3://") and cp.count("/") >= 3:
        key = cp.split("/", 3)[3]
    else:
        key = cp.lstrip("/")
    key = key.rstrip("/")
    # If config_prefix is a FORK prefix (.../_orchestrator/ck-<slug>), walk up to the task root.
    marker = "/_orchestrator/"
    if marker in ("/" + key + "/"):
        key = key.split("/_orchestrator/", 1)[0]
    elif not key:
        key = f"config/_task/{task_suffix}"
    return key.rstrip("/") + "/_jobs.json"


def _read_registry(s3, bucket, key):
    """(doc, etag). doc is {} with no 'jobs' when absent. etag None when absent."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8")), obj.get("ETag")
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return {}, None
        raise


def _write_registry(s3, bucket, key, doc, etag):
    """Conditional write: If-Match the ETag we read (If-None-Match '*' when creating), so a
    concurrent update can't be clobbered. Returns the new ETag. Raises PreconditionFailed on a
    race (the caller retries the read-modify-write)."""
    body = (json.dumps(doc, indent=2) + "\n").encode("utf-8")
    kw = {"Bucket": bucket, "Key": key, "Body": body, "ContentType": "application/json"}
    if etag:
        kw["IfMatch"] = etag
    else:
        kw["IfNoneMatch"] = "*"
    return s3.put_object(**kw).get("ETag")


def _update_registry(s3, bucket, key, mutate, attempts=6):
    """Read-modify-write the registry safely against concurrent updates (S3 conditional write
    with ETag, retried on PreconditionFailed/412). `mutate(doc)` edits the doc in place."""
    import time
    for i in range(attempts):
        doc, etag = _read_registry(s3, bucket, key)
        mutate(doc)
        try:
            _write_registry(s3, bucket, key, doc, etag)
            return doc
        except Exception as e:
            code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
            name = type(e).__name__
            if code in ("PreconditionFailed", "412", "ConditionalRequestConflict") \
               or name in ("PreconditionFailed",):
                if i < attempts - 1:
                    time.sleep(0.2 * (2 ** i))
                    continue
            raise
    raise Exception(f"could not update job registry s3://{bucket}/{key} after {attempts} tries "
                    f"(concurrent writers?).")


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
    s3 = boto3.client("s3", region_name=REGION)
    region = event.get("region", REGION)
    account_id = _account_id(event, context)
    names = {role: _job_name(project, task_suffix, role) for role in _ROLES}
    reg_key = _registry_key(event.get("configPrefix") or f"config/_task/{task_suffix}/", task_suffix)

    if mode == "list_fork_cdc":
        # Cutover support: return THIS task's FORK CDC job names (ck-* and bg-*), from the UNION of
        # the registry and the exact-tag selection (so stale jobs are caught too). Fail closed on
        # a Glue list/tag error. Never selects by name prefix (G1).
        if not account_id:
            raise Exception("list_fork_cdc: cannot resolve account id (need account_id or "
                            "dms_task_arn) to read job tags.")
        reg_doc, _ = _read_registry(s3, bucket, reg_key)
        reg_fork_cdc = {j["name"] for j in reg_doc.get("jobs", [])
                        if j.get("role") in ("ck-cdc", "bg-cdc")}
        tagged = _select_task_jobs_by_tag(glue, region, account_id, project, task_suffix)
        tagged_fork_cdc = set()
        for n in tagged:
            t = _get_job_tags(glue, region, account_id, n)
            if t.get("dsql_pipeline_fork") and n.endswith("-cdc"):
                tagged_fork_cdc.add(n)
        fork_cdc = sorted(reg_fork_cdc | tagged_fork_cdc)
        missing = sorted(reg_fork_cdc - tagged)   # in registry but Glue has no tagged job
        print(f"(info) list_fork_cdc: {len(fork_cdc)} fork CDC job(s): {fork_cdc}"
              + (f"; missing from Glue: {missing}" if missing else ""))
        return {"forkCdcJobNames": fork_cdc, "missingForkCdcJobNames": missing, "jobs": names}

    if mode == "delete":
        # Delete the task's jobs = UNION of the registry and the exact-tag selection (so stale
        # jobs — e.g. a fork whose table was removed — are cleaned up too). NEVER by name prefix.
        # Fail closed if Glue listing/tagging fails. Wait for any active run to stop first.
        if not account_id:
            raise Exception("delete: cannot resolve account id to read job tags.")
        reg_doc, _ = _read_registry(s3, bucket, reg_key)
        reg_names = {j["name"] for j in reg_doc.get("jobs", [])}
        tagged = _select_task_jobs_by_tag(glue, region, account_id, project, task_suffix)
        found = reg_names | tagged
        reported_missing = sorted(reg_names - tagged)
        deleted, failed, pending = [], [], []
        for name in sorted(found):
            try:
                # Wait for any active run to stop, bounded by the Lambda's own time budget (B21).
                # If a run is still active when the budget runs low (e.g. a composite CDC run
                # slow to react to its stop), issue a best-effort batch-stop and leave the job
                # for the next DeleteGlueJobs loop pass instead of blocking to a Lambda timeout.
                if not _wait_runs_stopped(glue, name, context=context):
                    _best_effort_stop_runs(glue, name)
                    pending.append(name)
                    continue
                glue.delete_job(JobName=name)
                deleted.append(name)
            except glue.exceptions.EntityNotFoundException:
                pass
            except Exception as e:
                print(f"(error) could not delete {name}: {e}")
                failed.append({"job": name, "error": f"{type(e).__name__}: {e}"[:500]})
        if pending:
            print(f"(info) delete: {len(pending)} job(s) still have a run stopping; left for "
                  f"the next cutover DeleteGlueJobs pass: {pending}")
        # Clear the registry ONLY when every job is gone (nothing pending/failed) so a re-looped
        # DeleteGlueJobs can still find the pending jobs by registry on the next pass.
        if not pending and not failed:
            try:
                _update_registry(s3, bucket, reg_key, lambda d: d.update(
                    {"jobs": [], "cdcOwners": {}, "deletedAt": _now_iso()}))
            except Exception as e:
                print(f"(warn) could not clear registry {reg_key}: {e}")
        return {"jobs": names, "deleted": deleted, "failed": failed, "pending": pending,
                "reportedMissing": reported_missing, "created": [], "updated": []}

    # ---- create / ensure_fork_jobs / cdc_fallback ------------------------------------------
    templates_prefix = event["glue_templates_prefix"].strip("/")
    scripts_prefix = event["scripts_prefix"].strip("/")
    config_prefix = event["configPrefix"]
    extra_py_files = event.get("extraPyFiles", "")
    glue_role_arn = event["glue_role_arn"]
    dsql_endpoint = event["dsql_endpoint"]
    # Ordered PrivateLink/public failover list (CSV) derived by resolve_task. Optional for
    # backward compatibility; the scripts fall back to --dsql_endpoint when it is absent/empty.
    dsql_endpoint_candidates = event.get("dsql_endpoint_candidates", "")
    dsql_user = event.get("dsql_user", "admin")
    dsql_database = event.get("dsql_database", "postgres")
    cdc_root = event.get("cdc_root", "cdc")
    control_schema = event.get("control_schema", "cdc_control")
    # CDC needs the DMS task ARN so glue_cdc_continuous can scope its control tables /
    # metrics / stop logic to THIS task. Passed through from the startup SM (which has the
    # task ARN hardcoded). Absent for non-cdc roles.
    dms_task_arn = event.get("dms_task_arn", "")

    created, updated, replaced = [], [], []
    job_records = []
    spark_cdc_drivers = None
    fallback_reason = ""
    cdc_engine = _cdc_engine(event)

    # Base tags on EVERY job so cutover/re-runs can find a task's jobs (and a fork's jobs).
    base_tags = {"dsql_pipeline_project": project, "dsql_pipeline_task": task_suffix}

    # A "job spec" fully describes one job to upsert: its name, which template stem to read, the
    # command engine, the config_prefix it is scoped to, whether it is a CDC job (gets cdc args/
    # drivers), and its tags. This replaces the old role-only loop so forks (own name + own
    # config_prefix + composite CDC template) and the shared set use ONE code path.
    job_specs = []

    if mode == "cdc_fallback":
        err_msg = str(event.get("error_message") or "")
        why = driver_error_reason(err_msg)
        if not why:
            print(f"(info) CDC run failed for a reason other than its drivers; job left as is: "
                  f"{err_msg[:500]}")
            return {"jobs": names, "switched": False, "reason": "", "cdcEngine": cdc_engine,
                    "created": [], "updated": [], "replaced": [], "deleted": []}
        fallback_reason = (f"Python-shell CDC run {event.get('failed_run_id') or ''} failed: {why} "
                           f"({err_msg[:600]})")
        cdc_engine = "spark"
        # The failing CDC job: the main cdc job, OR a fork CDC job (fork_cdc_job_name + fork_kind).
        fb_name = event.get("fork_cdc_job_name") or names["cdc"]
        fb_kind = event.get("fork_kind")   # "ck" | "bg" | None(main)
        fb_slug = event.get("fork_slug")
        if fb_kind == "ck":
            fb_stem = _CDC_COMPOSITE_ENGINES["spark"]      # composite script (ck fork)
            fb_role, fb_owner = "ck-cdc", f"ck-{fb_slug}"
        elif fb_kind == "bg":
            fb_stem = _CDC_ENGINES["spark"]                # MAIN script (bg fork is big single/no-PK)
            fb_role, fb_owner = "bg-cdc", f"bg-{fb_slug}"
        else:
            fb_stem = _CDC_ENGINES["spark"]
            fb_role, fb_owner = "cdc", "main"
        fb_cp = event.get("fork_config_prefix") or config_prefix
        fb_tags = dict(base_tags)
        if fb_slug:
            fb_tags["dsql_pipeline_fork"] = fb_slug
        print(f"(info) {fb_name}: {fallback_reason}. Re-creating it as a Spark job.")
        job_specs.append({"name": fb_name, "tmpl_stem": fb_stem, "config_prefix": fb_cp,
                          "is_cdc": True, "tags": fb_tags, "role": fb_role, "owner_slug": fb_owner,
                          "table": event.get("fork_table")})
    elif mode == "ensure_fork_jobs":
        # Called by the startup SM AFTER plan_split: create/update the per-fork jobs from THIS
        # run's plan ($.plan.forks). Idempotent. A CK fork (composite key) gets load/validate/cdc
        # (composite script); a BG fork (big single/no-PK table) gets a CDC job ONLY (its
        # load/validate stay in the normal "big" group). No forks -> nothing created.
        if cdc_engine == "spark" and event.get("cdc_fallback_reason"):
            fallback_reason = str(event["cdc_fallback_reason"])
        forks = event.get("forks") or []
        for f in forks:
            fcp = f["config_prefix"]
            slug = f["fork_slug"]
            kind = f.get("kind", "ck")   # "ck" | "bg"
            ftags = dict(base_tags, dsql_pipeline_fork=slug)
            if kind == "bg":
                # Big single/no-PK CDC fork: one CDC job on the MAIN cdc script (glue_cdc_continuous),
                # same engine/driver/fallback rules as the main CDC job.
                cdc_stem = _CDC_ENGINES[cdc_engine]
                job_specs.append({"name": f["cdcJobName"], "tmpl_stem": cdc_stem,
                                  "config_prefix": fcp, "is_cdc": True, "tags": ftags,
                                  "role": "bg-cdc", "owner_slug": f"bg-{slug}",
                                  "table": f.get("fork_table")})
            else:
                # Composite-key fork: load + validate (shared templates) + CDC (composite script).
                load_stem = "load-big" if f.get("loadRole") == "load-big" else "load"
                cdc_stem = _CDC_COMPOSITE_ENGINES[cdc_engine]
                job_specs.append({"name": f["loadJobName"], "tmpl_stem": load_stem,
                                  "config_prefix": fcp, "is_cdc": False, "tags": ftags,
                                  "role": "ck-load", "owner_slug": f"ck-{slug}",
                                  "table": f.get("fork_table")})
                job_specs.append({"name": f["validateJobName"], "tmpl_stem": "validate",
                                  "config_prefix": fcp, "is_cdc": False, "tags": ftags,
                                  "role": "ck-validate", "owner_slug": f"ck-{slug}",
                                  "table": f.get("fork_table")})
                job_specs.append({"name": f["cdcJobName"], "tmpl_stem": cdc_stem,
                                  "config_prefix": fcp, "is_cdc": True, "tags": ftags,
                                  "role": "ck-cdc", "owner_slug": f"ck-{slug}",
                                  "table": f.get("fork_table")})
        n_ck = sum(1 for f in forks if f.get("kind", "ck") == "ck")
        n_bg = sum(1 for f in forks if f.get("kind") == "bg")
        print(f"(info) ensure_fork_jobs: {n_ck} composite + {n_bg} big fork(s); "
              f"{len(job_specs)} fork job(s) to create/update.")
    else:
        # mode == create: the shared per-task job set (discovery/load/load-big/validate/cdc).
        # NO fork job here — forks are created by ensure_fork_jobs from THIS run's discovery.
        if cdc_engine == "spark" and event.get("cdc_fallback_reason"):
            fallback_reason = str(event["cdc_fallback_reason"])
        for role in _ROLES:
            stem = _CDC_ENGINES[cdc_engine] if role == "cdc" else role
            job_specs.append({"name": names[role], "tmpl_stem": stem,
                              "config_prefix": config_prefix, "is_cdc": role == "cdc",
                              "tags": dict(base_tags), "role": role,
                              "owner_slug": "main" if role == "cdc" else None})

    for spec in job_specs:
        tmpl_stem = spec["tmpl_stem"]
        name = spec["name"]
        is_cdc = spec["is_cdc"]
        job_config_prefix = spec["config_prefix"]
        job_tags = spec["tags"]
        tmpl = _read_json(s3, bucket, f"{templates_prefix}/{tmpl_stem}.json")
        command_name = tmpl.get("command_name", "glueetl")
        if is_cdc and (command_name == "pythonshell") != (cdc_engine == "pythonshell"):
            raise Exception(f"{tmpl_stem}.json has command_name={command_name!r}, which does not "
                            f"match cdc_engine={cdc_engine!r}")
        script_key = tmpl["script"]
        script_location = f"s3://{bucket}/{scripts_prefix}/{script_key}"
        role = tmpl_stem   # for messages/placeholder error text

        # Computed args every job gets; template default_arguments merged on top.
        # NOTE the two bucket args are the SAME bucket under different names because the
        # scripts request them under different flags: job1_discovery reads --dms_bucket (to
        # derive s3://<bucket>/<schema>/<table>/ full-load paths); glue_cdc_continuous reads
        # --s3_bucket. job2/job3 use neither (they derive paths from the manifest). Passing
        # both is harmless — each script only reads the flag it asks for; the other is an
        # ignored extra DefaultArgument.
        args = {
            "--config_prefix": job_config_prefix,
            "--dsql_endpoint": dsql_endpoint,
            "--dsql_user": dsql_user,
            "--dsql_database": dsql_database,
            "--region": region,
            "--s3_bucket": bucket,
            "--dms_bucket": bucket,
            "--enable-continuous-cloudwatch-log": "true",
            "--job-language": "python",
        }
        if dsql_endpoint_candidates:
            # Ordered failover list (PrivateLink + public). The scripts try each host and pin
            # the first that connects; absent/empty -> they use --dsql_endpoint only.
            args["--dsql_endpoint_candidates"] = dsql_endpoint_candidates
        if extra_py_files:
            args["--extra-py-files"] = extra_py_files
        if role == "discovery" and "cdc_root" in event:
            # The DMS endpoint's BucketFolder ("." = none), so discovery finds each table's
            # folder where DMS really writes it (same root the CDC job and drain check use).
            args["--cdc_root"] = cdc_root
        if role in ("load", "load-big", "validate", "cdc", "cdc-spark",
                    _CDC_COMPOSITE_ROLE, "cdc-composite-spark") and \
                event.get("csv_null_value") is not None:
            # Glue can't pass an empty argument value, so an empty marker travels as __EMPTY__.
            # (Everything except discovery takes --csv_null_value.)
            _nv = str(event["csv_null_value"])
            args["--csv_null_value"] = _nv if _nv != "" else "__EMPTY__"
        if role in ("load", "load-big"):
            # job2 driver-side parallelism tuning (how many tables load at once + the per-table
            # driver-memory budget the auto-throttle uses). Passed as RUN defaults; absent ->
            # job2_load uses its built-in defaults (MAX_PARALLEL_TABLES=20, 1500 MB/table).
            _mpt = event.get("maxParallelTables")
            if _mpt not in (None, ""):
                args["--max_parallel_tables"] = str(_mpt)
            _pwb = event.get("perWorkerMemBudgetMb")
            if _pwb not in (None, ""):
                args["--per_worker_mem_budget_mb"] = str(_pwb)
            # ease-guardrails: the load job's blank guards (G1/G2/G3/G4/G5) read guardrails_mode.
            # Absent (older workflows) -> job2_load defaults to warn (never fails a run for its
            # own bookkeeping; still refuses a genuinely destructive blank via G1/G4).
            _gm = event.get("guardrails_mode")
            if _gm not in (None, ""):
                args["--guardrails_mode"] = str(_gm)
        if role in ("validate", "ck-validate"):
            # B14: rows per validation key-range. The shared validate job AND every composite
            # ck-validate fork read it so a too-big range (which raised a client read timeout on
            # 8M/16.3M-row tables at the old 50k default) is sized down to a value that returns
            # within DSQL's limits; a range that still times out is auto re-split. Absent ->
            # job3_validate uses its built-in default (10000).
            _vrr = event.get("validateRowsPerRange")
            if _vrr not in (None, ""):
                args["--validate_rows_per_range"] = str(_vrr)
            # B18 throughput controls: parallelism (0 = job auto-sizes), the time-sizer target,
            # the per-value-hash scope, and the shared DSQL conn budget that hard-caps
            # parallelism. Each is OPTIONAL (older workflows omit them -> job3 built-in defaults).
            _vp = event.get("validateParallelism")
            if _vp not in (None, ""):
                args["--validate_parallelism"] = str(_vp)
            _vts = event.get("validateTargetSecondsPerRange")
            if _vts not in (None, ""):
                args["--validate_target_seconds_per_range"] = str(_vts)
            _vh = event.get("validateHash")
            if _vh not in (None, ""):
                args["--validate_hash"] = str(_vh)
            _cb = event.get("connBudget")
            if _cb not in (None, ""):
                args["--conn_budget"] = str(_cb)
            # G10: validate also cross-checks the DSQL count against DMS FullLoadRows. Pass the
            # DMS task ARN + the mismatch tolerance (reuses cdc_drift_tolerance). Absent -> the
            # G10 check is a no-op (validate falls back to the S3 source comparison).
            if dms_task_arn:
                args["--dms_task_arn"] = dms_task_arn
            _cmt = event.get("cdc_drift_tolerance")
            if _cmt is not None and str(_cmt).strip() != "":
                args["--count_mismatch_tolerance"] = str(_cmt)
            # ease-guardrails: G10 validate count check is WARN by default (a DSQL-vs-DMS
            # FullLoadRows mismatch logs a WARNING; validation still passes — DMS counts can
            # legitimately differ). validate_count_check=strict (or guardrails_mode=strict)
            # makes the mismatch FAIL. Absent -> job3_validate defaults to warn.
            _vcc = event.get("validate_count_check")
            if _vcc not in (None, ""):
                args["--validate_count_check"] = str(_vcc)
            _gm_v = event.get("guardrails_mode")
            if _gm_v not in (None, ""):
                args["--guardrails_mode"] = str(_gm_v)
            # (both B18 throughput controls and the G10 DMS cross-check are set on validate)
        if is_cdc:
            args["--cdc_root"] = cdc_root
            args["--control_schema"] = control_schema
            # CDC validation (Tier-2 deferred by-PK net-state check). ON by default; the two
            # args are passed to EVERY CDC job (main cdc, cdc-spark fallback, cdc-composite and
            # its spark template) so the engine switch never changes the validation behaviour.
            # Absent from the event (older workflows) -> the CDC script defaults to ON / 20.
            _cv = event.get("cdc_validation")
            if _cv is not None:
                # Glue argument values are strings; the CDC script's overlay parses true/false.
                args["--cdc_validation"] = "true" if (_cv is True or str(_cv).strip().lower()
                                                       in ("true", "1", "yes")) else "false"
            _cvs = event.get("cdc_validation_sample")
            if _cvs is not None:
                args["--cdc_validation_sample"] = str(_cvs)
            # SAFETY GUARDRAILS (G6 mass-delete, G9 drift) — passed to EVERY CDC job (main,
            # spark, composite, composite-spark) so the engine/key shape never changes the
            # guard behaviour. Absent from the event (older workflows) -> the CDC script's
            # built-in SAFE defaults apply (fraction 0.5, rows 100000, drift check 30 min,
            # tolerance 0, action warn). camelCase keys mirror resolve_task's payload.
            for _ek, _ak in (("cdc_max_delete_fraction", "--cdc_max_delete_fraction"),
                             ("cdc_max_delete_rows", "--cdc_max_delete_rows"),
                             ("cdc_drift_check_minutes", "--cdc_drift_check_minutes"),
                             ("cdc_drift_tolerance", "--cdc_drift_tolerance"),
                             ("cdc_drift_action", "--cdc_drift_action"),
                             # ease-guardrails: master mode + G8/G7 per-guard actions. Absent
                             # (older workflows) -> the CDC script's warn-by-default applies.
                             ("guardrails_mode", "--guardrails_mode"),
                             ("cdc_file_order_action", "--cdc_file_order_action"),
                             ("cdc_nopk_overmatch_action", "--cdc_nopk_overmatch_action")):
                _v = event.get(_ek)
                if _v is not None and str(_v).strip() != "":
                    args[_ak] = str(_v)
            # OWNERSHIP: this CDC job applies a table only if _jobs.json cdcOwners[table] matches.
            args["--cdc_owner_self"] = spec.get("owner_slug") or "main"
            args["--cdc_owners_key"] = reg_key
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

        # SIZING OVERRIDE (speed over cost): if the event carries per-role sizing (worker type,
        # count, timeout, Glue version), apply it to the Spark jobs so load/load-big/validate run
        # on bigger drivers for big tables. Keyed by the template STEM (discovery/load/load-big/
        # validate); a composite fork's load/load-big/validate reuse the same stem, so forks get
        # the same sizing automatically. CDC roles keep their template defaults. Values were
        # already allow-list/int validated in resolve_task; re-validate the type here defensively
        # so a hand-built event can't set an invalid WorkerType.
        _sz = _sizing_for(event, tmpl_stem)
        if command_name != "pythonshell" and _sz:
            if _sz.get("worker_type"):
                _wt = str(_sz["worker_type"]).strip().upper().replace(" ", "")
                if _wt not in _ALLOWED_WORKER_TYPES:
                    raise Exception(f"Invalid worker_type {_sz['worker_type']!r} for job {name} "
                                    f"(role {tmpl_stem}); allowed: {sorted(_ALLOWED_WORKER_TYPES)}.")
                job_kwargs["WorkerType"] = _wt
            if _sz.get("num_workers"):
                job_kwargs["NumberOfWorkers"] = int(_sz["num_workers"])
            if _sz.get("glue_version"):
                job_kwargs["GlueVersion"] = str(_sz["glue_version"]).strip()
        if _sz and _sz.get("timeout_minutes"):
            # Timeout applies to Spark AND pythonshell jobs; cap at Glue's 7-day max (10080).
            job_kwargs["Timeout"] = max(1, min(10080, int(_sz["timeout_minutes"])))

        _conns = _connections_for(tmpl, event)
        if _conns:
            job_kwargs["Connections"] = {"Connections": _conns}
        if job_tags:
            job_kwargs["Tags"] = dict(job_tags)   # task/fork tags for find-by-tag at cutover

        # G5 TAG-SAFETY: if a job with this exact name already exists, it MUST carry this task's
        # tags (and the fork tag when applicable). A hand-made job (no tags), or one owned by a
        # DIFFERENT task that shares the name, is REFUSED — never overwritten/updated/deleted.
        if account_id:
            try:
                glue.get_job(JobName=name)
                _exists = True
            except glue.exceptions.EntityNotFoundException:
                _exists = False
            except Exception:
                _exists = False
            if _exists:
                _ex_tags = _get_job_tags(glue, region, account_id, name)
                _mismatch = (_ex_tags.get("dsql_pipeline_project") != project or
                             _ex_tags.get("dsql_pipeline_task") != task_suffix or
                             _ex_tags.get("dsql_pipeline_fork") != job_tags.get("dsql_pipeline_fork"))
                if _mismatch:
                    raise Exception(
                        f"Refusing to modify Glue job {name!r}: it exists with tags {_ex_tags or '{}'} "
                        f"that do not match this task (project={project!r}, task={task_suffix!r}, "
                        f"fork={job_tags.get('dsql_pipeline_fork')!r}). A different task or a "
                        f"hand-made job owns this name. Rename/remove it, or use a different "
                        f"project/task name. No job was changed.")

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
                upd = {k: v for k, v in job_kwargs.items() if k not in ("Name", "Tags")}
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

        # Ensure task/fork tags on the job whether it was created, updated or replaced. CreateJob
        # applies Tags inline; UpdateJob cannot, so (re)apply them here via tag_resource (no-op
        # if already present). Best-effort — a tagging hiccup must not fail job creation.
        if job_tags:
            try:
                # Reuse the account id resolved at the top of handler() (event -> dms_task_arn ->
                # context ARN -> STS). This can no longer be empty just because the payload
                # omitted account_id/dms_task_arn (B20).
                acct = account_id
                if acct:
                    arn = f"arn:aws:glue:{region}:{acct}:job/{name}"
                    glue.tag_resource(ResourceArn=arn, TagsToAdd=dict(job_tags))
            except Exception as _te:
                print(f"(warn) could not tag {name}: {type(_te).__name__}: {_te}")

        # Collect this job's registry record (G2). cdcOwners is filled from CDC specs below.
        job_records.append({
            "name": name,
            "role": spec.get("role", tmpl_stem),
            "table": spec.get("table"),
            "ownerSlug": spec.get("owner_slug"),
            "engine": "spark" if command_name != "pythonshell" else "pythonshell",
            "configPrefix": job_config_prefix,
            "createdByExecution": event.get("startupExecution") or event.get("executionName"),
            "updatedAt": _now_iso(),
        })

    out = {"jobs": names, "created": created, "updated": updated, "replaced": replaced,
           "deleted": [], "cdcEngine": cdc_engine}

    # ---- REGISTRY (G2) + RECONCILE (G3) ----------------------------------------------------
    # Merge this run's job records into the ONE source of truth _jobs.json (read-modify-write
    # with ETag). cdcOwners maps each forked table -> its CDC owner slug (ck-/bg-); tables not
    # listed default to owner 'main'. create mode seeds the shared jobs (cdc owner 'main');
    # ensure_fork_jobs adds the fork jobs + their table owners.
    if mode in ("create", "ensure_fork_jobs", "cdc_fallback") and account_id:
        def _mutate(doc):
            doc.setdefault("project", project)
            doc.setdefault("taskSuffix", task_suffix)
            doc.setdefault("jobs", [])
            doc.setdefault("cdcOwners", {})
            by_name = {j["name"]: j for j in doc["jobs"]}
            for rec in job_records:
                existing = by_name.get(rec["name"], {})
                merged = dict(existing, **{k: v for k, v in rec.items() if v is not None})
                merged.setdefault("createdAt", existing.get("createdAt") or rec["updatedAt"])
                by_name[rec["name"]] = merged
                # Record the per-table CDC owner (only for CDC specs that carry a table).
                if rec.get("table") and rec.get("role") in ("ck-cdc", "bg-cdc"):
                    doc["cdcOwners"][rec["table"]] = rec["ownerSlug"]
            doc["jobs"] = sorted(by_name.values(), key=lambda j: j["name"])
            doc["updatedAt"] = _now_iso()
        try:
            _update_registry(s3, bucket, reg_key, _mutate)
            out["registryKey"] = reg_key
        except Exception as e:
            raise Exception(f"could not update job registry s3://{bucket}/{reg_key}: "
                            f"{type(e).__name__}: {e}")

    if mode == "ensure_fork_jobs" and account_id:
        # RECONCILE (G3): a tagged fork job in Glue that is NOT in THIS run's plan is stale (e.g.
        # its table was dropped from the DMS selection). Report it (staleJobs); DO NOT start it.
        # Startup does not delete it (cutover's delete-by-tag cleans it up).
        planned = {s["name"] for s in job_specs} | set(names.values())
        tagged = _select_task_jobs_by_tag(glue, region, account_id, project, task_suffix)
        stale = sorted(n for n in tagged if n not in planned
                       and _get_job_tags(glue, region, account_id, n).get("dsql_pipeline_fork"))
        if stale:
            print(f"(info) ensure_fork_jobs: {len(stale)} stale fork job(s) not in this run's "
                  f"plan (left in place, NOT started): {stale}")
        out["staleJobs"] = stale
        out["forkJobsCreated"] = created
        out["forkJobsUpdated"] = updated
    if spark_cdc_drivers:
        out["sparkCdcDrivers"] = spark_cdc_drivers
    if fallback_reason and mode == "cdc_fallback":
        # Record the switch so the next startup of this task builds the Spark job straight away
        # (resolve-task reads this file). For a FORK CDC job, the engine marker is keyed to the
        # fork's config_prefix so each fork tracks its own engine independently.
        from datetime import datetime, timezone
        _spec = job_specs[0] if job_specs else {}
        _cp = _spec.get("config_prefix", config_prefix)
        key = _engine_file_key(_cp)
        doc = {"engine": "spark", "reason": fallback_reason, "stage": "after start",
               "failedRunId": event.get("failed_run_id") or None,
               "errorMessage": str(event.get("error_message") or "")[:2000] or None,
               "job": _spec.get("name", names["cdc"]),
               "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "undo": f"delete s3://{bucket}/{key} to build the Python-shell CDC job again"}
        s3.put_object(Bucket=bucket, Key=key, Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                      ContentType="application/json")
        out["engineFile"] = f"s3://{bucket}/{key}"
        print(f"(info) CDC engine is now spark; recorded in s3://{bucket}/{key}")
    if mode == "cdc_fallback":
        out.update(switched=True, reason=fallback_reason)
    return out
