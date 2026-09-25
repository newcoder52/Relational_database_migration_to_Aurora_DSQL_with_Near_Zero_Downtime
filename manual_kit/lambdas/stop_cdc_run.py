"""
stop-cdc-run Lambda (per-task cutover).

The CDC job (<project>-cdc) is a SINGLE Glue job definition run once PER TASK (each run gets
that task's --config_prefix). Cutover for ONE task must stop ONLY that task's run — NOT
BatchStopJobRun (which would stop every task's CDC). This Lambda finds the RUNNING JobRun
whose --config_prefix argument matches this task's config prefix and stops just that run.

Input event: { "cdcJobName": "<project>-cdc", "configPrefix": "s3://.../config/_task/<t>/" }
Returns: { "stopped": [runId], "matched": N, "alreadyStopped": bool }
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
        resp = glue.get_job_runs(**kw)
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
        # batch_stop_job_run takes explicit run IDs -> stops ONLY this task's run(s).
        glue.batch_stop_job_run(JobName=job_name, JobRunIds=to_stop[:25])
        stopped = to_stop[:25]

    return {"stopped": stopped, "matched": matched, "alreadyStopped": len(stopped) == 0}
