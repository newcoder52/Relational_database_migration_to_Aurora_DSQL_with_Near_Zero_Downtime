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

# Socket connect timeout (seconds) per candidate probe during endpoint failover.
DSQL_CANDIDATE_CONNECT_TIMEOUT = 10


# === DSQL ENDPOINT FAILOVER HELPER (shared; keep byte-identical across all copies) =========
# One operator-supplied endpoint, but connectivity differs by where this code runs: Glue in a
# private VPC reaches DSQL over a PrivateLink connection endpoint whose working hostname is
# <cluster-id>.<service-identifier>.<region>.on.aws, while the public console name
# <cluster-id>.dsql.<region>.on.aws times out on 5432 there. resolve_task derives the private
# candidate (via dsql.get_vpc_endpoint_service_name) and passes an ordered, comma-separated
# --dsql_endpoint_candidates list; we try each in order and the first that connects wins. The
# auth token MUST be minted for the host actually connected to, so the caller's make_conn(host)
# builds the token from its host argument. This block is duplicated verbatim per script because
# Glue copies each script to S3 as a single file; tests/test_helper_sync.py diffs the copies.
# ============================================================================================
def dsql_candidate_list(candidates_csv, given_endpoint):
    """Ordered, de-duped candidate hostnames from the --dsql_endpoint_candidates CSV, always
    ending with the operator-given endpoint as a backstop. Never raises: a blank/missing CSV
    degrades to just [given_endpoint] so a connection is still attempted."""
    out = []
    for raw in (candidates_csv or "").split(","):
        host = raw.strip()
        if host and host not in out:
            out.append(host)
    g = (given_endpoint or "").strip()
    if g and g not in out:
        out.append(g)
    return out


def dsql_connect_first(candidates, make_conn, log=None):
    """Try make_conn(host) for each candidate in order; return (conn, host) for the first that
    connects. make_conn must mint the auth token for the host it is given. On total failure,
    raise one RuntimeError naming every host tried and its error, plus a one-line network hint.
    log(msg), if given, is called once with the hostname that worked."""
    errors = []
    for host in candidates:
        try:
            conn = make_conn(host)
            if log:
                log(host)
            return conn, host
        except Exception as e:   # noqa: BLE001 - any connect failure -> try the next host
            errors.append((host, f"{type(e).__name__}: {e}"))
    tried = "; ".join(f"{h} -> {err}" for h, err in errors) or "(no candidates)"
    raise RuntimeError(
        "Could not connect to Aurora DSQL on any candidate hostname [" + tried + "]. "
        "Hint: Glue/Lambda needs a network route to DSQL: a DSQL VPC endpoint with private "
        "DNS, or internet/NAT.")
# === END DSQL ENDPOINT FAILOVER HELPER ======================================================


def _split_s3_uri(uri):
    no = uri.replace("s3://", "")
    parts = no.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def _connect_dsql(endpoint, user, database, candidates_csv=""):
    import pg8000
    dsql = boto3.client("dsql", region_name=REGION)
    ctx = ssl.create_default_context()

    def _make(host):
        token = dsql.generate_db_connect_admin_auth_token(host, Region=REGION, ExpiresIn=3600)
        return pg8000.connect(host=host, port=5432, database=database, user=user,
                              password=token, ssl_context=ctx,
                              timeout=DSQL_CANDIDATE_CONNECT_TIMEOUT)

    candidates = dsql_candidate_list(candidates_csv, endpoint)
    conn, host = dsql_connect_first(
        candidates, _make, log=lambda h: print(f"drop-tags: DSQL reachable on {h}"))
    return conn


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
    candidates_csv = event.get("dsql_endpoint_candidates", "")
    user = event.get("dsql_user", "admin")
    database = event.get("dsql_database", "postgres")

    s3 = boto3.client("s3", region_name=REGION)
    index = json.loads(s3.get_object(
        Bucket=bucket, Key=f"{config_prefix}/_manifest_index.json"
    )["Body"].read().decode("utf-8"))
    entries = index.get("tables", [])

    conn = _connect_dsql(endpoint, user, database, candidates_csv)
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
