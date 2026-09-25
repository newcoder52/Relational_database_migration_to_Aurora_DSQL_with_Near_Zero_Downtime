"""
drop-tags Lambda (Phase-2 cutover) — remove the Glue-managed _cdc_file tag column.

Runs LAST in the cutover state machine, only AFTER the CDC DMS task is stopped, the CDC Glue
jobs have caught up (drain-check) AND been stopped. At that point the _cdc_file tag (used by
the keyless Tier-2 path for idempotent file reload) is no longer needed and should be removed
so the target is the clean cutover copy.

Blunt + safe: run `ALTER TABLE <schema>.<table> DROP COLUMN IF EXISTS "_cdc_file"` on EVERY
target table in the manifest. IF EXISTS makes it a no-op on tables that never had it (keyed
Tier-1 tables), so we can't miss a keyless table and can't error on the rest. Idempotent -> a
cutover re-run is safe.

Connects to DSQL with an IAM auth token + pg8000. Requires pg8000 in the package/layer and
dsql:DbConnectAdmin on the Lambda role.

Input event: { "bucket", "config_prefix", "dsql_endpoint", "dsql_user", "dsql_database",
               "drop_cdc_file_tag": true }
Returns: { "dropped": [labels], "skipped": bool, "errors": [ {table, error} ] }
"""

import json
import os
import ssl

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
TAG_COLUMN = "_cdc_file"


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


def handler(event, context):
    if not event.get("drop_cdc_file_tag", True):
        return {"dropped": [], "skipped": True, "errors": []}

    raw_cp = event["config_prefix"]
    if raw_cp.startswith("s3://"):
        bucket, config_prefix = _split_s3_uri(raw_cp)
        config_prefix = config_prefix.strip("/")
    else:
        bucket = event["bucket"]
        config_prefix = raw_cp.strip("/")
    endpoint = event["dsql_endpoint"]
    user = event.get("dsql_user", "admin")
    database = event.get("dsql_database", "postgres")

    s3 = boto3.client("s3", region_name=REGION)
    index = json.loads(s3.get_object(
        Bucket=bucket, Key=f"{config_prefix}/_manifest_index.json"
    )["Body"].read().decode("utf-8"))
    entries = index.get("tables", [])

    conn = _connect_dsql(endpoint, user, database)
    conn.autocommit = True
    dropped = []
    errors = []
    try:
        cur = conn.cursor()
        for e in entries:
            schema = e.get("dsql_schema")
            table = e.get("dsql_table")
            label = f"{schema}.{table}"
            try:
                cur.execute(f'ALTER TABLE {schema}.{table} '
                            f'DROP COLUMN IF EXISTS "{TAG_COLUMN}"')
                dropped.append(label)
            except Exception as ex:
                # A table that doesn't exist / other DDL error is logged, not fatal — the
                # cutover should still report what it could clean up.
                errors.append({"table": label, "error": str(ex)[:500]})
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"dropped": dropped, "skipped": False, "errors": errors}
