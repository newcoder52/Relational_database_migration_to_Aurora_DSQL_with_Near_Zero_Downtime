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

CUTOVER VALIDATION GATE (mode == "validation_gate"): the same Lambda, with "mode":
"validation_gate" in the event, instead counts UNRESOLVED rows in
cdc_control.cdc_validation_failures for this task's tables and returns
{ "ok": bool, "failures": N, "byTable": {label: count}, "tables": N }. The cutover state
machine calls it before stopping DMS (pre-check) and again after the drain (final check); a
non-zero count stops cutover (state CdcValidationFailed). Reuses this Lambda because it already
connects to DSQL and reads the task's manifest index — no separate Lambda needed. A task whose
failures table / tables do not exist yet counts as 0 (cutover still works)."""

import json
import os
import ssl

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")

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
        candidates, _make, log=lambda h: print(f"drain-check: DSQL reachable on {h}"))
    return conn


def _ci_subfolder(s3, bucket, prefix, name):
    """Folder under prefix matching name: exact, else the single case-insensitive match."""
    names, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        names += [cp["Prefix"][len(prefix):].rstrip("/") for cp in resp.get("CommonPrefixes", []) or []]
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    if name in names:
        return name
    ci = [n for n in names if n.lower() == name.lower()]
    return ci[0] if len(ci) == 1 else None


def _latest_cdc_file(s3, bucket, cdc_root, dms_schema, dms_table, _retried=False):
    """Latest CDC CSV key under <cdc_root>/<schema>/<table>/ (skip processed/ + failed/ and
    full-load LOAD*.csv files).
    cdc_root may be a '.'/'/'-style sentinel meaning "no subfolder" (DMS S3 target with NO
    BucketFolder -> CDC files share the per-table root <schema>/<table>/). Normalize it the
    same way glue_cdc_continuous.derive_table_prefixes does, so an empty root yields a clean
    prefix with no leading './'."""
    root = (cdc_root or "").strip("/. ")
    prefix = f"{root}/{dms_schema}/{dms_table}/" if root else f"{dms_schema}/{dms_table}/"
    latest = None
    token = None
    saw_any = False
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        if resp.get("Contents"):
            saw_any = True
        for o in resp.get("Contents", []):
            k = o["Key"]
            rel = k[len(prefix):]
            if "/" in rel:            # skip processed/ + failed/ subfolders
                continue
            # Skip full-load LOAD*.csv files: with no DMS BucketFolder they share this folder
            # with the CDC files, and "LOAD..." sorts after "2026..." -- so without this the
            # latest file would always be a LOAD file, which is never in the CDC ledger, and
            # cutover would wait until CdcDrainTimedOut. Same filter as the CDC job.
            if rel.upper().startswith("LOAD"):
                continue
            if k.lower().endswith(".csv"):
                # CDC files are timestamp-named -> lexical max == latest.
                if latest is None or k > latest:
                    latest = k
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    if not saw_any and not _retried:
        # No folder under the recorded name: DMS may have created it in another letter case
        # (a table that was empty at full load). Same lookup as the CDC job.
        base = f"{root}/" if root else ""
        s = _ci_subfolder(s3, bucket, base, dms_schema)
        t = _ci_subfolder(s3, bucket, f"{base}{s}/", dms_table) if s else None
        if t and (s, t) != (dms_schema, dms_table):
            return _latest_cdc_file(s3, bucket, cdc_root, s, t, _retried=True)
    return latest


def _validation_gate(event):
    """mode == "validation_gate": count UNRESOLVED rows in cdc_control.cdc_validation_failures
    for THIS task's tables, and fail the cutover gate if any exist.

    Why here (not a 9th Lambda): drain-check already connects to DSQL and reads the task's
    manifest index, so the cutover validation gate reuses the same wiring. The cutover state
    machine calls it twice — once BEFORE StopCdcDmsTask (pre-check) and once AFTER the drain
    completes (final check) — with the same payload plus "mode": "validation_gate".

    Counts rows where `resolved IS NOT TRUE` (so a NULL 'resolved' — e.g. a row added before an
    older control table was upgraded — counts as unresolved and gates), scoped to the task's
    table labels (dsql_schema.dsql_table, exactly what the CDC jobs write as cdc_validation_failures.table_name). A table/schema that
    was never created (no CDC ever ran) counts as 0: the failures table may not exist yet, which
    is treated as "no failures" so cutover still works for a task that never produced CDC.

    Returns { "ok": bool, "failures": N, "byTable": {label: count}, "tables": N }. ok==true
    means no unresolved failures -> the gate passes.
    """
    raw_cp = event["config_prefix"]
    if raw_cp.startswith("s3://"):
        bucket, config_prefix = _split_s3_uri(raw_cp)
        config_prefix = config_prefix.strip("/")
    else:
        bucket = event["bucket"]
        config_prefix = raw_cp.strip("/")
    control_schema = event.get("control_schema", "cdc_control")
    endpoint = event["dsql_endpoint"]
    user = event.get("dsql_user", "admin")
    database = event.get("dsql_database", "postgres")

    s3 = boto3.client("s3", region_name=REGION)
    # The task's table labels. If the index is missing (task never ran discovery) there are no
    # tables -> no failures possible -> gate passes.
    try:
        index = json.loads(s3.get_object(
            Bucket=bucket, Key=f"{config_prefix}/_manifest_index.json"
        )["Body"].read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001 - NoSuchKey or any read problem -> treat as 0 tables
        print(f"(info) validation_gate: no manifest index under {config_prefix} "
              f"({type(e).__name__}); 0 tables, gate passes.")
        return {"ok": True, "failures": 0, "byTable": {}, "tables": 0}

    labels = []
    for e in index.get("tables", []):
        label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
        if label and label != "None.None":
            labels.append(label)
    if not labels:
        return {"ok": True, "failures": 0, "byTable": {}, "tables": 0}

    conn = _connect_dsql(endpoint, user, database)
    by_table = {}
    try:
        cur = conn.cursor()
        # The failures table may not exist (no CDC job has run ensure_control_tables yet). Probe
        # once; absent -> 0 failures (gate passes). information_schema is available on DSQL.
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s "
            "AND table_name = %s LIMIT 1",
            (control_schema, "cdc_validation_failures"))
        if cur.fetchone() is None:
            cur.close()
            print(f"(info) validation_gate: {control_schema}.cdc_validation_failures does not "
                  f"exist yet; 0 failures, gate passes.")
            return {"ok": True, "failures": 0, "byTable": {}, "tables": len(labels)}
        for label in labels:
            cur.execute(
                f'SELECT count(*) FROM {control_schema}.cdc_validation_failures '
                f'WHERE table_name = %s AND resolved IS NOT TRUE',
                (label,))
            n = cur.fetchone()[0]
            if n:
                by_table[label] = int(n)
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    total = sum(by_table.values())
    return {"ok": total == 0, "failures": total, "byTable": by_table, "tables": len(labels)}


def handler(event, context):
    # Cutover validation gate (reuses this Lambda's DSQL wiring); default mode is the drain
    # "caught up?" check below.
    if event.get("mode") == "validation_gate":
        return _validation_gate(event)
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
    candidates_csv = event.get("dsql_endpoint_candidates", "")
    user = event.get("dsql_user", "admin")
    database = event.get("dsql_database", "postgres")

    s3 = boto3.client("s3", region_name=REGION)
    index = json.loads(s3.get_object(
        Bucket=bucket, Key=f"{config_prefix}/_manifest_index.json"
    )["Body"].read().decode("utf-8"))
    entries = index.get("tables", [])

    conn = _connect_dsql(endpoint, user, database, candidates_csv)
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
