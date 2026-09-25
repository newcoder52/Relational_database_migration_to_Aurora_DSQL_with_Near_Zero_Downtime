"""
drain-check Lambda (Phase-2 cutover) — the "Glue caught up to the latest CSV" gate.

After the DMS CDC task is stopped (no new CDC files land), we must wait until the CDC Glue
jobs have APPLIED every CDC file that exists in S3 before stopping them + dropping the
_cdc_file tag. This Lambda answers "caught up?" for ALL in-scope tables:
  - For each table: find the LATEST CDC CSV key in S3 under <cdc_root>/<schema>/<table>/
    (excluding the processed/ + failed/ subfolders).
  - Compare against cdc_control.cdc_file_status in DSQL: the CDC job marks each file 'done'
    (with all_rows_committed=true) when fully applied. Caught up for a table when the latest
    S3 CDC file has a matching 'done' ledger row (or the table has no CDC files at all).
  - CAUGHT UP overall when EVERY in-scope table is caught up.

The state machine calls this every N seconds (Wait→invoke→Choice) until caughtUp==true or
the attempt budget is exhausted (a stuck CDC job must NOT auto-proceed to stop+drop).

Connects to DSQL with an IAM auth token + pg8000 (same as the Glue jobs). Requires pg8000
in the deployment package/layer and dsql:DbConnectAdmin on the Lambda role.

Input event: { "bucket", "config_prefix", "cdc_root", "dsql_endpoint", "dsql_user",
               "dsql_database", "control_schema" }
Returns: { "caughtUp": bool, "pending": [ {table, latest_s3_file, last_done_file} ],
           "checked": N }
"""

import json
import os
import ssl

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


def _split_s3_uri(uri):
    no = uri.replace("s3://", "")
    parts = no.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def _connect_dsql(endpoint, user, database):
    import pg8000
    dsql = boto3.client("dsql", region_name=REGION)
    token = dsql.generate_db_connect_admin_auth_token(endpoint, Region=REGION, ExpiresIn=3600)
    ctx = ssl.create_default_context()
    return pg8000.connect(host=endpoint, port=5432, database=database, user=user,
                          password=token, ssl_context=ctx)


def _latest_cdc_file(s3, bucket, cdc_root, dms_schema, dms_table):
    """Latest CDC CSV key under <cdc_root>/<schema>/<table>/ (skip processed/ + failed/).
    cdc_root may be a '.'/'/'-style sentinel meaning "no subfolder" (DMS S3 target with NO
    BucketFolder -> CDC files share the per-table root <schema>/<table>/). Normalize it the
    same way glue_cdc_continuous.derive_table_prefixes does, so an empty root yields a clean
    prefix with no leading './'."""
    root = (cdc_root or "").strip("/. ")
    prefix = f"{root}/{dms_schema}/{dms_table}/" if root else f"{dms_schema}/{dms_table}/"
    latest = None
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            k = o["Key"]
            rel = k[len(prefix):]
            if "/" in rel:            # skip processed/ + failed/ subfolders
                continue
            if k.lower().endswith(".csv"):
                # CDC files are timestamp-named -> lexical max == latest.
                if latest is None or k > latest:
                    latest = k
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return latest


def handler(event, context):
    # config_prefix may be a full "s3://bucket/key/" URI (per-task) or a bare key + bucket.
    raw_cp = event["config_prefix"]
    if raw_cp.startswith("s3://"):
        bucket, config_prefix = _split_s3_uri(raw_cp)
        config_prefix = config_prefix.strip("/")
    else:
        bucket = event["bucket"]
        config_prefix = raw_cp.strip("/")
    cdc_root = event.get("cdc_root", "cdc")
    control_schema = event.get("control_schema", "cdc_control")
    endpoint = event["dsql_endpoint"]
    user = event.get("dsql_user", "admin")
    database = event.get("dsql_database", "postgres")

    s3 = boto3.client("s3", region_name=REGION)
    index = json.loads(s3.get_object(
        Bucket=bucket, Key=f"{config_prefix}/_manifest_index.json"
    )["Body"].read().decode("utf-8"))
    entries = index.get("tables", [])

    conn = _connect_dsql(endpoint, user, database)
    pending = []
    checked = 0
    try:
        cur = conn.cursor()
        for e in entries:
            checked += 1
            dms_schema = e.get("dms_schema")
            dms_table = e.get("dms_table")
            label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
            latest = _latest_cdc_file(s3, bucket, cdc_root, dms_schema, dms_table)
            if latest is None:
                continue   # no CDC files for this table -> caught up (nothing to apply)
            # Is this file marked done (all_rows_committed) in the ledger?
            cur.execute(
                f'SELECT 1 FROM {control_schema}.cdc_file_status '
                f'WHERE table_name = %s AND cdc_file = %s '
                f'AND (status = %s OR all_rows_committed = %s) LIMIT 1',
                (label, latest, "done", True))
            row = cur.fetchone()
            if row is None:
                # Also accept: the CDC job records the full S3 key OR just the filename —
                # compare on the basename as a fallback.
                cur.execute(
                    f'SELECT 1 FROM {control_schema}.cdc_file_status '
                    f'WHERE table_name = %s AND cdc_file LIKE %s '
                    f'AND (status = %s OR all_rows_committed = %s) LIMIT 1',
                    (label, f"%{latest.split('/')[-1]}", "done", True))
                row = cur.fetchone()
            if row is None:
                pending.append({"table": label, "latest_s3_file": latest})
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"caughtUp": len(pending) == 0, "pending": pending[:50], "checked": checked}
