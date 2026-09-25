"""
resolve-task Lambda (per-task startup SM, after the DMS completion gate).

Derives WHERE this DMS task's CSV data lives in S3, from the task's own S3 TARGET endpoint
settings (bucket + BucketFolder) — no custom CdcPath assumed (default DMS layout: full-load
LOAD*.csv and CDC timestamp files share the per-table directory
<BucketFolder>/<schema>/<table>/). The per-task SM uses this to point discovery/load at the
right prefix, and the pipeline SERIALIZES CDC resume AFTER loads+validate (so the loader
never reads CDC files as full-load rows under the default layout).

Input event: { "taskArn": "arn:...:task:...", "configPrefix": "s3://bucket/config/_task/<t>/" }
Returns: {
  "s3Bucket": "<target bucket>",
  "bucketFolder": "<BucketFolder or ''>",
  "dmsS3Base": "s3://<bucket>/<bucketFolder>/",     # tables live under <dmsS3Base><schema>/<table>/
  "cdcRoot": "<BucketFolder or '.'>",               # CDC/full-load share the per-table dir;
                                                    # '.' sentinel == no BucketFolder (flat root)
  "migrationType": "full-load-and-cdc",
  "s3Settings": {                                   # full CSV/S3 format contract from the endpoint
    "bucketName", "bucketFolder", "datePartitionEnabled", "addColumnName",
    "timestampColumnName", "csvDelimiter", "csvRowDelimiter", "compressionType",
    "dataFormat", "rfc4180", "serviceAccessRoleArn"
  },
  "datePartitionEnabled": <bool>,                   # hoisted copies for easy SM passthrough
  "addColumnName": <bool>,                          #   (True => CSVs carry a header row)
  "timestampColumnName": "<col>"                    #   (the CDC watermark column)
}
Fails fast if the task's target endpoint is not S3, or the task is not cdc-capable.

WHY cdcRoot: the CDC job / plan-split / drain-check locate a table's CDC files under
<cdc_root>/<schema>/<table>/. Under the default DMS layout (no CdcPath — required because this
pipeline uses AddColumnName=true, which is incompatible with CdcPath), CDC + cached-change
files share the SAME per-table directory as the LOAD*.csv full-load files, i.e. under the
endpoint's BucketFolder. So cdc_root MUST equal the endpoint's BucketFolder (empty when there
is none). We derive it here from the endpoint instead of hardcoding "cdc" in the SM templates,
so a no-BucketFolder endpoint resolves to the flat root and a BucketFolder=cdc endpoint resolves
to "cdc" — automatically, never a config guess. Glue getResolvedOptions cannot pass an empty
arg value, so the empty case is emitted as the '.' sentinel that derive_table_prefixes()
normalizes back to "no subfolder".
"""

import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


def handler(event, context):
    task_arn = event["taskArn"]
    dms = boto3.client("dms", region_name=REGION)

    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}],
        WithoutSettings=True).get("ReplicationTasks", [])
    if len(tasks) != 1:
        raise Exception(f"Could not resolve exactly one task for {task_arn}.")
    task = tasks[0]
    mig = task.get("MigrationType", "")
    if "cdc" not in mig:
        raise Exception(f"Task {task_arn} MigrationType={mig!r} is not cdc-capable "
                        f"(need full-load-and-cdc).")
    target_ep_arn = task.get("TargetEndpointArn")
    if not target_ep_arn:
        raise Exception(f"Task {task_arn} has no TargetEndpointArn.")

    eps = dms.describe_endpoints(
        Filters=[{"Name": "endpoint-arn", "Values": [target_ep_arn]}]).get("Endpoints", [])
    if len(eps) != 1:
        raise Exception(f"Could not resolve the target endpoint {target_ep_arn}.")
    ep = eps[0]
    if (ep.get("EngineName") or "").lower() != "s3":
        raise Exception(f"Task {task_arn} target endpoint engine={ep.get('EngineName')!r} "
                        f"is not S3. This pipeline requires an S3 target endpoint.")
    s3s = ep.get("S3Settings") or {}
    bucket = s3s.get("BucketName")
    if not bucket:
        raise Exception(f"S3 target endpoint for {task_arn} has no BucketName.")
    bucket_folder = (s3s.get("BucketFolder") or "").strip("/")
    base = f"s3://{bucket}/" + (f"{bucket_folder}/" if bucket_folder else "")
    # cdc_root MUST match the endpoint's BucketFolder (CDC/full-load share the per-table dir
    # under the default no-CdcPath layout). Emit the '.' sentinel when there is no
    # BucketFolder, because Glue getResolvedOptions can't pass an empty-string arg value;
    # derive_table_prefixes() in the CDC job normalizes '.'/'/'/'' back to "no subfolder".
    cdc_root = bucket_folder if bucket_folder else "."

    # ── Validate the endpoint uses the DMS DEFAULT flat per-table layout ──────────────────
    # Per the DMS S3-target docs, with the default layout DMS writes BOTH full-load
    # (LOAD########.csv) and CDC (timestamp YYYYMMDD-HHMMSSmmm.csv) files into the SAME
    # per-table directory  <BucketFolder>/<schema>/<table>/ , distinguished only by filename.
    # The whole pipeline (loader LOAD*.csv glob, CDC timestamp listing, drain-check) relies on
    # that single shared base. Two endpoint settings break it:
    #   • DatePartitionEnabled=true -> CDC files get date-partitioned subfolders, so they are
    #     NOT at <base>/<schema>/<table>/ and the CDC job would never find them.
    #   • CdcPath / PreserveTransactions -> CDC written to a separate transaction dir. (These
    #     are also mutually exclusive with AddColumnName, which this pipeline requires, so a
    #     correctly-built endpoint can't have them — but we check defensively.)
    # Fail fast with a clear message rather than silently derive wrong paths.
    if bool(s3s.get("DatePartitionEnabled")):
        raise Exception(
            f"Target endpoint for {task_arn} has DatePartitionEnabled=true. This pipeline "
            f"requires the DMS DEFAULT flat per-table layout (full-load + CDC share "
            f"<BucketFolder>/<schema>/<table>/). Recreate the S3 target endpoint with "
            f"DatePartitionEnabled=false (and AddColumnName=true).")
    if s3s.get("CdcPath") or bool(s3s.get("PreserveTransactions")):
        raise Exception(
            f"Target endpoint for {task_arn} sets CdcPath/PreserveTransactions. This pipeline "
            f"requires the DMS DEFAULT layout with AddColumnName=true (incompatible with "
            f"CdcPath/PreserveTransactions). Recreate the endpoint without them.")

    # ── Pull the full CSV/S3 format contract from the endpoint ────────────────────────────
    # The Glue jobs (load/validate/CDC) must parse the CSVs exactly as DMS wrote them. Rather
    # than hardcode delimiters / header presence / the timestamp column in the scripts, we
    # surface the endpoint's actual S3Settings so the pipeline is endpoint-driven and can't
    # silently drift if the endpoint is reconfigured. Values mirror DMS's own defaults when a
    # setting is omitted from the endpoint (DMS applies the same defaults at run time).
    def _b(v, default):
        return bool(v) if v is not None else default

    date_partition_enabled = _b(s3s.get("DatePartitionEnabled"), False)
    add_column_name = _b(s3s.get("AddColumnName"), False)  # True => CSVs carry a header row
    timestamp_column = s3s.get("TimestampColumnName") or "dms_timestamp"
    csv_delimiter = s3s.get("CsvDelimiter") or ","
    csv_row_delimiter = s3s.get("CsvRowDelimiter") or "\\n"
    compression_type = (s3s.get("CompressionType") or "NONE").upper()
    data_format = (s3s.get("DataFormat") or "csv").lower()
    rfc4180 = _b(s3s.get("Rfc4180"), True)
    service_access_role = s3s.get("ServiceAccessRoleArn") or ""

    return {
        "s3Bucket": bucket,
        "bucketFolder": bucket_folder,
        "dmsS3Base": base,
        "cdcRoot": cdc_root,
        "migrationType": mig,
        # Full S3 format contract pulled from the target endpoint (endpoint-driven, not guessed)
        "s3Settings": {
            "bucketName": bucket,
            "bucketFolder": bucket_folder,
            "datePartitionEnabled": date_partition_enabled,
            "addColumnName": add_column_name,
            "timestampColumnName": timestamp_column,
            "csvDelimiter": csv_delimiter,
            "csvRowDelimiter": csv_row_delimiter,
            "compressionType": compression_type,
            "dataFormat": data_format,
            "rfc4180": rfc4180,
            "serviceAccessRoleArn": service_access_role,
        },
        # Hoisted convenience copies (so the SM can pass a single value without a nested path)
        "datePartitionEnabled": date_partition_enabled,
        "addColumnName": add_column_name,
        "timestampColumnName": timestamp_column,
    }
