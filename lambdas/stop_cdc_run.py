"""
stop-cdc-run Lambda (per-task cutover).

The CDC job (<project>-cdc) is a SINGLE Glue job definition run once PER TASK (each run gets
that task's --config_prefix). Cutover for ONE task must stop ONLY that task's run — NOT
BatchStopJobRun (which would stop every task's CDC). This Lambda finds the RUNNING JobRun
whose --config_prefix argument matches this task's config prefix and stops just that run.

Input event: { "cdcJobName": "<project>-cdc", "configPrefix": "s3://.../config/_task/<t>/" }
Returns: { "stopped": [runId], "matched": N, "alreadyStopped": bool, "jobMissing": bool }

IDEMPOTENCY (B27): cutover deletes the CDC job(s) at the end. A SECOND cutover of an
already-cut-over task reaches StopCdcRun again, but the job is gone, so glue.get_job_runs
raises EntityNotFoundException ("Job not found"). That is NOT an error for cutover — a
deleted job is, by definition, not running — so we treat it as a no-op and return
alreadyStopped=True, jobMissing=True. Only EntityNotFoundException is swallowed; every other
Glue error (AccessDeniedException, ThrottlingException, …) still propagates so the state
machine fails closed. Used for BOTH the per-task CDC run and each fork CDC run, so both are
idempotent at the Lambda level regardless of the state-machine version.
"""

import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
_RUNNING = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def handler(event, context):
    job_name = event["cdcJobName"]
    config_prefix = event["configPrefix"]

    glue = boto3.client("glue", region_name=REGION)
    stopped = []
    matched = 0
    to_stop = []
    token = None
    while True:
        kw = {"JobName": job_name, "MaxResults": 200}
        if token:
            kw["NextToken"] = token
        try:
            resp = glue.get_job_runs(**kw)
        except glue.exceptions.EntityNotFoundException:
            # B27: the job was already deleted by a prior cutover. Nothing to stop — a
            # non-existent job has no running run. Return a clean no-op so a re-cutover
            # proceeds to CutoverSucceeded instead of failing. Real errors (AccessDenied,
            # throttling, …) are NOT caught here and still fail the state closed.
            print(f"(info) stop-cdc-run: job {job_name} not found (already deleted) — "
                  f"treating as already stopped.")
            return {"stopped": [], "matched": 0, "alreadyStopped": True, "jobMissing": True}
        for run in resp.get("JobRuns", []):
            args = run.get("Arguments", {}) or {}
            if args.get("--config_prefix") == config_prefix:
                matched += 1
                if run.get("JobRunState") in _RUNNING:
                    to_stop.append(run["Id"])
        token = resp.get("NextToken")
        if not token:
            break

    if to_stop:
        # batch_stop_job_run takes explicit run IDs -> stops ONLY this task's run(s). The job
        # exists (get_job_runs succeeded above), so EntityNotFound is not expected here; a run
        # that finished between the list and the stop is a harmless no-op on Glue's side.
        glue.batch_stop_job_run(JobName=job_name, JobRunIds=to_stop[:25])
        stopped = to_stop[:25]

    return {"stopped": stopped, "matched": matched, "alreadyStopped": len(stopped) == 0,
            "jobMissing": False}
