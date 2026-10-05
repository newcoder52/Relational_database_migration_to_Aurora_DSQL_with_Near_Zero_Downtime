"""
Job 3 (MULTI-TABLE): Post-Load Validation — S3 CSV (source of truth) vs Aurora DSQL target
==========================================================================================
A PySpark Glue job that VALIDATES a completed full load by comparing, per key-range and in
parallel, a compact FINGERPRINT of the source (the DMS CSV in S3, transformed exactly as
the loader would) against the same fingerprint computed on the DSQL target. It reports which
ranges match and which differ — WITHOUT pulling rows across the wire.

WHY THIS SHAPE
--------------
- Source is S3-only here, so "validation" = "did the loader correctly land what DMS wrote
  to S3" (it trusts DMS's source->S3 capture, which is AWS-managed). This job cannot see
  the live source DB and does not need to.
- Comparing rows one-by-one is O(rows) and hits DSQL scan limits. Instead we compute a
  small FINGERPRINT per key-range on each side and compare the fingerprints — orders of
  magnitude less data moves. This mirrors how DMS's own enhanced validation works
  (partitioned hash comparison), adapted to DSQL.
- Full-load validation has a QUIESCENT endpoint (the load finished), so a source-vs-target
  fingerprint is a clean comparison of two static things. (CDC validation is different and
  lives in the CDC job — this job is for the post-full-load gate, and can be re-run on a
  schedule as the CDC drift check.)

SPARK, LIKE THE LOADER (v15)
----------------------------
Reads the (potentially GB) source CSVs in parallel across executors and applies the SAME
transform the loader applied — so the source-side fingerprint reflects the transformed
values that SHOULD be in the target. This is a PySpark job (big-file parallel read), unlike
the CDC job (Python Shell, tiny deltas).

TWO-TIER (speed)
----------------
  Tier 1 (always): per-range COUNT compare — fast, catches missing/extra rows.
  Tier 2 (CHECKSUM_MODE="aggregate", the default): per-range, per-column summaries chosen
          from each column's DSQL type (see CHECKSUM_MODE below) — proves the TRANSFORM
          landed (a count matches even if every value is wrong). Two portable modes:
            "aggregate" (default): per-range column aggregates that need NO special DB
                        function — count(non-null) per col + sum(len) over a canonical
                        row string + min/max of the PK. Computable identically in Spark
                        and DSQL SQL. Safe on DSQL (no pgcrypto/extension dependency).
            "md5"     : md5(string_agg(row_string ORDER BY pk)) per range. Stronger, but
                        depends on md5()/string_agg being available on your DSQL cluster
                        — verify before using (DSQL has NO pgcrypto extension; md5() is a
                        core builtin and usually present, string_agg likewise).

DSQL CONSTRAINTS honored: index-usable range predicates on the PK, per-range queries sized
under the 5-min txn limit, bounded concurrency, IAM-token auth, 60-min connection recycle.

DEPLOY
------
  Job Type: Glue ETL (Spark), Glue 4.0+, additional module: pg8000
"""
import sys
import ssl
import json
import math
import time
import socket
import threading
from decimal import Decimal
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, lit, lower, trim, upper, concat, substring, coalesce,
    to_timestamp, date_format, regexp_replace,
)

import pg8000

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")  # correct TIMESTAMPTZ offset handling
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================================================================
# CONFIGURATION  (generic placeholders — the customer edits these for their env)
# =============================================================================
CONFIG_PREFIX = 's3://<YOUR_S3_BUCKET>/<SCHEMA>/config/'   # Job 1 output prefix
INDEX_S3_KEY = None                                        # else CONFIG_PREFIX+_manifest_index.json

DSQL_ENDPOINT = '<YOUR_CLUSTER>.dsql.<REGION>.on.aws'
# Ordered, comma-separated DSQL hostnames to try (resolve_task derives a PrivateLink candidate
# and passes --dsql_endpoint_candidates). connect_dsql() tries each in order, pins the first
# that connects into DSQL_ENDPOINT, and later connects reuse the pinned host. Empty -> just
# DSQL_ENDPOINT is used (full backward compatibility).
DSQL_ENDPOINT_CANDIDATES = ''
DSQL_DATABASE = 'postgres'
DSQL_USER = 'admin'
REGION = 'us-east-1'

# ---- CUSTOMER-TUNABLE KNOBS -------------------------------------------------
# Rows per validation range. Bigger = fewer, larger queries (watch the 300s DSQL txn
# limit); smaller = more parallelism, more round-trips. Ranges are index-usable on the PK.
# V2: lowered from 250000 to 50000 so a single per-range per-column aggregate stays well under
# DSQL's hard 300s transaction-age limit even on wide, large tables (the earlier real test hit
# 303s at 250k on a 16.3M-row table). Each range query runs in its own short autocommit txn and
# is additionally bounded by VALIDATE_STATEMENT_TIMEOUT_MS; a range that still times out is
# auto re-split smaller and retried (see validate_one_table).
VALIDATE_ROWS_PER_RANGE = 50000
# V2: per-statement timeout (ms) set on every DSQL validation connection, kept comfortably
# below DSQL's 300s transaction-age limit so a too-big range fails FAST and deterministically
# (so it can be re-split) instead of burning ~300s and erroring the table.
VALIDATE_STATEMENT_TIMEOUT_MS = 240000
# V2: how many times a single range may be re-split (halved / sub-bucketed) and retried after a
# transaction-age / statement-timeout error before giving up on that range.
VALIDATE_MAX_RESPLIT_DEPTH = 6
# V2: whole-table / composite-PK tables (no single rangeable column) are split into this many
# key buckets (by the FIRST pk column's value ranges when a key exists) so a large composite
# table is never validated in one unbounded transaction. Auto-grows with row count.
VALIDATE_COMPOSITE_BUCKET_ROWS = 50000
# Max concurrent per-range TARGET queries across all tables (DSQL connection budget).
MAX_QUERY_CONCURRENCY = 20
# Max tables validated concurrently on the driver.
MAX_PARALLEL_TABLES = 4
# Content check:
#   "aggregate" (DEFAULT): per key range, besides the row count, compare per-column summaries
#               chosen from each column's REAL DSQL type, so every column is checked:
#                 text/varchar/char/binary : non-null count, total length, min, max
#                 uuid                     : non-null count, min, max
#                 boolean                  : non-null count, number of true values
#                 integer/numeric          : non-null count, exact sum (after the same
#                                            rounding the load applies)
#                 real/double              : non-null count, sum (tiny float tolerance)
#                 timestamp/date           : non-null count, sum of the instants (microseconds)
#                 json/other               : non-null count
#               A wrong, missing, NULLed, truncated, rounded or shifted value changes one of
#               these, and the report names the column. (Values swapped between two rows of
#               the same range can cancel out; that is the one thing summaries can't see.)
#   "off"     : row counts only.
# Turn it off for a deployment by adding "--checksum_mode": "off" to default_arguments in
# glue-templates/validate.json. ("md5" is accepted and treated as "aggregate".)
CHECKSUM_MODE = "aggregate"
# Only validate tables the load marked done (reads _load_status.json). Off = validate all
# tables in the manifest.
REQUIRE_FULL_LOAD_DONE = True

CONN_RECYCLE_SECONDS = 50 * 60
GLUE_API_TIMEOUT = 5

# =============================================================================
# OPTIONAL GLUE ARG OVERLAY  (orchestrator wiring — mirrors v16's _apply_v6_arg_overrides)
# =============================================================================
# The orchestrator points each split-group's validate run at that group's OWN config prefix
# (its own _manifest_index.json + _load_status.json), and injects env-specific endpoints so
# the customer never hand-edits this file. Every arg is OPTIONAL: getResolvedOptions raises
# on a requested-but-absent arg, so we only request the ones actually present in sys.argv,
# and each hardcoded constant above stays the FALLBACK default. Job3 derives its index /
# status / report S3 paths at call time from CONFIG_PREFIX (see load_manifest /
# load_full_load_status / the report writer), so overriding the CONFIG_PREFIX global here is
# sufficient — there are no import-time derived path constants to recompute (unlike v16).
CSV_NULL_VALUE = "NULL"   # DMS null marker; see null_marker_expr


def _apply_job3_arg_overrides():
    global CSV_NULL_VALUE
    global CONFIG_PREFIX, INDEX_S3_KEY, DSQL_ENDPOINT, DSQL_USER, DSQL_DATABASE, REGION
    global DSQL_ENDPOINT_CANDIDATES   # kit: PrivateLink/public failover list
    global CHECKSUM_MODE, MAX_PARALLEL_TABLES, VALIDATE_ROWS_PER_RANGE, MAX_QUERY_CONCURRENCY
    global REQUIRE_FULL_LOAD_DONE
    optional = ["config_prefix", "index_s3_key", "dsql_endpoint", "dsql_user",
                "dsql_database", "region", "dsql_endpoint_candidates",
                "checksum_mode", "max_parallel_tables", "validate_rows_per_range",
                "max_query_concurrency", "require_full_load_done", "csv_null_value"]
    present = [a for a in optional if f"--{a}" in sys.argv]
    if not present:
        return
    ov = getResolvedOptions(sys.argv, present)
    if "config_prefix" in ov and str(ov["config_prefix"]).strip():
        _cp = str(ov["config_prefix"]).strip()
        if not _cp.endswith("/"):
            _cp += "/"
        CONFIG_PREFIX = _cp
        print(f"  ↪ CONFIG_PREFIX overridden -> {CONFIG_PREFIX}")
    if "index_s3_key" in ov and str(ov["index_s3_key"]).strip():
        INDEX_S3_KEY = str(ov["index_s3_key"]).strip()
    if "dsql_endpoint" in ov and str(ov["dsql_endpoint"]).strip():
        DSQL_ENDPOINT = str(ov["dsql_endpoint"]).strip()
    if "dsql_endpoint_candidates" in ov and str(ov["dsql_endpoint_candidates"]).strip():
        DSQL_ENDPOINT_CANDIDATES = str(ov["dsql_endpoint_candidates"]).strip()
        print(f"  ↪ DSQL_ENDPOINT_CANDIDATES -> {DSQL_ENDPOINT_CANDIDATES}")
    if "dsql_user" in ov and str(ov["dsql_user"]).strip():
        DSQL_USER = str(ov["dsql_user"]).strip()
    if "dsql_database" in ov and str(ov["dsql_database"]).strip():
        DSQL_DATABASE = str(ov["dsql_database"]).strip()
    if "region" in ov and str(ov["region"]).strip():
        REGION = str(ov["region"]).strip()
    if "checksum_mode" in ov:
        _cm = str(ov["checksum_mode"]).strip().lower()
        if _cm in ("off", "aggregate", "md5"):
            CHECKSUM_MODE = "aggregate" if _cm == "md5" else _cm
        else:
            print(f"  ⚠️ ignoring invalid checksum_mode={ov['checksum_mode']!r} (use off|aggregate|md5)")
    if "max_parallel_tables" in ov:
        try:
            MAX_PARALLEL_TABLES = max(1, int(ov["max_parallel_tables"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid max_parallel_tables={ov['max_parallel_tables']!r}")
    if "validate_rows_per_range" in ov:
        try:
            VALIDATE_ROWS_PER_RANGE = max(1, int(ov["validate_rows_per_range"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid validate_rows_per_range={ov['validate_rows_per_range']!r}")
    if "max_query_concurrency" in ov:
        try:
            MAX_QUERY_CONCURRENCY = max(1, int(ov["max_query_concurrency"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid max_query_concurrency={ov['max_query_concurrency']!r}")
    if "csv_null_value" in ov and ov["csv_null_value"] is not None:
        _nv = str(ov["csv_null_value"])
        CSV_NULL_VALUE = "" if _nv == "__EMPTY__" else _nv
        print(f"  ↪ CSV_NULL_VALUE (DMS null marker) -> {CSV_NULL_VALUE!r}")
    if "require_full_load_done" in ov:
        REQUIRE_FULL_LOAD_DONE = str(ov["require_full_load_done"]).strip().lower() in ("true", "1", "yes")


_apply_job3_arg_overrides()

# =============================================================================
# SHARED CONSTANTS + CSV READ (byte-identical to the loader so the transform matches)
# =============================================================================
CSV_READ_OPTIONS = {
    "header": None,                 # set per-table from config (dms_has_headers)
    "inferSchema": "false",
    "multiLine": "true",
    "recursiveFileLookup": "true",
    "quote": '"',
    "escape": '"',
    # PRESERVE leading/trailing whitespace (Spark defaults these to true = trims). Must match
    # job2's loader read so validation compares like-for-like (and doesn't false-flag a value
    # the loader preserved).
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "unescapedQuoteHandling": "STOP_AT_CLOSING_QUOTE",
    "mode": "PERMISSIVE",
    "sep": ",",
    "encoding": "UTF-8",
    # SILENT-CORRUPTION GUARD: validate only DMS FULL-LOAD files (LOAD*.csv). Otherwise
    # recursiveFileLookup descends into processed/ + failed/ and counts leftover CDC files
    # (16 cols, leading `Op`) as source rows, inflating the source count and false-flagging
    # a mismatch. CDC files are timestamp-named; LOAD*.csv excludes them. Must match the
    # loader's read set so source/target fingerprints compare like-for-like.
    "pathGlobFilter": "LOAD*.csv",
}

# How DMS marks a real NULL in its CSV files: the S3 target endpoint's CsvNullValue (DMS
# default "NULL"), passed by create-glue-jobs as --csv_null_value ("__EMPTY__" = the endpoint
# sets it to the empty string). Only that exact text, or an empty field, is stored as NULL.
# Every other value is data: 'NA', 'N/A', 'NONE', '(NULL)', '\\N', 'null' are kept as written.
# MUST stay identical in job2_load, job3_validate and glue_cdc_continuous (_coerce_null).
_TYPED_CATEGORIES = frozenset({
    'uuid', 'boolean', 'timestamptz', 'date', 'bigint', 'integer', 'smallint', 'numeric',
    'float', 'double', 'real', 'json', 'jsonb', 'bytea'})


def null_marker_expr(c, category):
    """Spark expression for column c: NULL for a real NULL (Spark null, empty field, or the
    exact DMS null marker), else the value. Text columns keep the value exactly (whitespace
    too); typed columns are trimmed, and whitespace-only becomes NULL (it can't be cast)."""
    raw = col(c)
    is_null = raw.isNull() | (raw == lit(""))
    if CSV_NULL_VALUE:
        is_null = is_null | (raw == lit(CSV_NULL_VALUE))
    if category in _TYPED_CATEGORIES:
        t = trim(raw)
        return when(is_null | (t == lit("")), lit(None)).otherwise(t)
    return when(is_null, lit(None)).otherwise(raw)

UUID_CANONICAL_RE = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
UUID_RAW_HEX_RE = r'^[0-9a-fA-F]{32}$'

TIMESTAMP_INPUT_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd", "dd-MMM-yy hh.mm.ss.SSSSSS a", "dd-MMM-yy hh.mm.ss a",
    "dd-MMM-yyyy HH:mm:ss", "dd-MMM-yy", "dd-MMM-yyyy", "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy",
]

_client_lock = threading.Lock()


def make_boto_client(service):
    with _client_lock:
        return boto3.client(service, region_name=REGION)


def split_s3(path):
    no_scheme = path.replace("s3://", "")
    parts = no_scheme.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def s3_prefix_has_objects(s3, bucket, prefix):
    """V1: True if ANY object exists under the DMS table prefix. DMS writes no S3 folder for a
    0-row source table, so an empty listing means 'no full-load files' (an empty source), which
    the validator treats as 0 source rows instead of letting Spark raise 'Path does not exist'.
    Best-effort: any listing error -> assume present so the normal read path still runs."""
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix.lstrip("/"), MaxKeys=1)
        return resp.get("KeyCount", 0) > 0 or bool(resp.get("Contents"))
    except Exception:
        return True


# =============================================================================
# DSQL CONNECTION
# =============================================================================
# Socket connect timeout (seconds) per candidate probe during endpoint failover: a wrong
# PrivateLink/public host should fail FAST so we move to the next candidate.
DSQL_CANDIDATE_CONNECT_TIMEOUT = 10
_dsql_endpoint_resolved = False
_dsql_resolve_lock = threading.Lock()

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


def _make_dsql_conn(host, autocommit=True):
    """Open an authenticated pg8000 connection to ONE host (token minted for that host)."""
    client = make_boto_client("dsql")
    tok = client.generate_db_connect_admin_auth_token(host, Region=REGION, ExpiresIn=3600)
    conn = pg8000.connect(host=host, port=5432, database=DSQL_DATABASE, user=DSQL_USER,
                          password=tok, ssl_context=ssl.create_default_context(),
                          timeout=DSQL_CANDIDATE_CONNECT_TIMEOUT)
    conn.autocommit = autocommit
    # V2: bound every statement well under DSQL's 300s transaction-age limit so a too-large
    # range query fails fast and deterministically (then gets re-split) instead of running
    # ~300s and raising 54000. Best-effort: ignore if the server rejects the GUC.
    try:
        _c = conn.cursor()
        _c.execute(f"SET statement_timeout = {int(VALIDATE_STATEMENT_TIMEOUT_MS)}")
        _c.close()
    except Exception:
        pass
    return conn


def connect_dsql(autocommit=True):
    """Open a short-lived DSQL connection for one range fingerprint query.

    Bounded retry with backoff: a fresh IAM token is minted on EVERY attempt, so a transient
    open failure (08006 unable-to-connect, TLS blip, throttle) retries instead of failing the
    whole table's validation. On exhaustion the last error is raised (the caller records the
    table as 'error').

    ENDPOINT FAILOVER: the first connect tries each candidate hostname (PrivateLink private
    name vs public console name) and PINS the first that reaches DSQL into DSQL_ENDPOINT, so
    later connects reuse it. If every retry fails, the pin is cleared so a later call
    re-probes the full candidate list."""
    global DSQL_ENDPOINT, _dsql_endpoint_resolved
    _last = None
    for _attempt in range(1, 5):   # up to 4 attempts
        try:
            with _dsql_resolve_lock:
                _need_resolve = not _dsql_endpoint_resolved
            if _need_resolve:
                candidates = dsql_candidate_list(DSQL_ENDPOINT_CANDIDATES, DSQL_ENDPOINT)
                conn, host = dsql_connect_first(
                    candidates, lambda h: _make_dsql_conn(h, autocommit),
                    log=lambda h: print(f"  ↪ DSQL reachable on {h} (pinned for this run)"))
                with _dsql_resolve_lock:
                    DSQL_ENDPOINT = host
                    _dsql_endpoint_resolved = True
                return conn
            return _make_dsql_conn(DSQL_ENDPOINT, autocommit)
        except Exception as e:
            _last = e
            if _attempt < 4:
                time.sleep(min(8.0, 0.5 * (2 ** (_attempt - 1))))   # 0.5,1,2s backoff
    with _dsql_resolve_lock:
        _dsql_endpoint_resolved = False   # pinned host stopped working -> re-probe next call
    raise _last


# =============================================================================
# UUID / RANGE helpers  (mirror the loader so bounds partition identically)
# =============================================================================
def normalize_uuid_hex(value):
    if value is None:
        return None
    s = str(value).strip().lower().replace("-", "")
    if len(s) != 32 or any(ch not in "0123456789abcdef" for ch in s):
        return None
    return s


def hex_to_int(h):
    return int(h, 16)


def int_to_hex(n):
    return format(n, "032x")


def hex_to_canonical_uuid(h):
    if h is None:
        return None
    s = str(h).strip().lower().replace("-", "")
    if len(s) != 32 or any(ch not in "0123456789abcdef" for ch in s):
        return None
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


def _sql_str_literal(s):
    return "'" + str(s).replace("'", "''") + "'"


def plan_ranges(min_id, max_id, total_rows, per):
    """Half-open [lo,hi) integer ranges covering [min_id, max_id+1). Same math as the loader."""
    if total_rows <= 0 or max_id < min_id:
        return []
    per = max(1, int(per))
    n = max(1, math.ceil(total_rows / per))
    key_span = (max_id - min_id) + 1
    n = max(1, min(n, key_span))
    out = []
    start = min_id
    end_excl = max_id + 1
    for i in range(1, n + 1):
        boundary = min_id + (i * key_span) // n
        if i == n:
            boundary = end_excl
        if boundary > start:
            out.append((start, boundary))
            start = boundary
    return out


def plan_ranges_hex(min_hex, max_hex, total_rows, per):
    lo_i, hi_i = hex_to_int(min_hex), hex_to_int(max_hex)
    out = []
    for lo, hi in plan_ranges(lo_i, hi_i, total_rows, per):
        out.append((int_to_hex(lo),
                    format(hi, "032x") if hi <= (16 ** 32 - 1) else format(hi, "x")))
    return out


# =============================================================================
# SPARK TRANSFORM  (identical normalization to the loader — so the source fingerprint
# reflects what SHOULD be stored in the target)
# =============================================================================
def pre_clean_timestamp(c):
    expr = col(c)
    expr = regexp_replace(expr, r'(\.\d{6})\d+', r'$1')
    return expr


def _strip_offset_expr(c):
    expr = regexp_replace(c, r'(\d{2}:\d{2}:\d{2}(\.\d+)?)[+-]\d{2}(:?\d{2})?$', r'$1')
    expr = regexp_replace(expr, r'(\s+[+-]\d{2}(:?\d{2})?|\s*Z|\s+UTC)\s*$', '')
    return expr


# Offset-aware formats (convert to true UTC instant; requires session TZ=UTC). Matches job2.
TIMESTAMP_TZ_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS XXX", "yyyy-MM-dd HH:mm:ss XXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS XXX", "yyyy-MM-dd HH:mm:ss.SSSSSSXXX", "yyyy-MM-dd HH:mm:ssXXX",
]


def normalize_timestamp(column_name, emit_pattern):
    cleaned = pre_clean_timestamp(column_name)   # offset preserved
    tz_attempts = [to_timestamp(cleaned, fmt) for fmt in TIMESTAMP_TZ_FORMATS]
    plain_attempts = [to_timestamp(_strip_offset_expr(cleaned), fmt) for fmt in TIMESTAMP_INPUT_FORMATS]
    parsed = coalesce(*(tz_attempts + plain_attempts))
    return when(parsed.isNotNull(), date_format(parsed, emit_pattern)).otherwise(lit(None))


def read_dms_csv(dms_s3_path, dms_has_headers):
    reader = spark.read
    for k, v in CSV_READ_OPTIONS.items():
        if k == "header":
            reader = reader.option("header", str(dms_has_headers).lower())
        elif v is not None:
            reader = reader.option(k, v)
    df = reader.csv(dms_s3_path)
    # CDC-CONTAMINATION BACKSTOP (version-independent). pathGlobFilter="LOAD*.csv" is a
    # best-effort prevention layer but its basename-vs-path semantics vary across Spark
    # versions, so a stale CDC file (processed/ or timestamp-named, 16-col leading 'Op') could
    # still be read and INFLATE the source count — silently masking a real mismatch or
    # false-flagging a good load. A DMS full-load CSV never leads with 'Op'; a CDC CSV always
    # does. If the first column is 'Op', a CDC file leaked in -> FAIL LOUD rather than validate
    # against a contaminated source count.
    if df.columns and str(df.columns[0]).strip().lower() == "op":
        raise Exception(
            f"CDC CONTAMINATION during validation: source read of {dms_s3_path} produced a "
            f"leading 'Op' column ({df.columns[:3]}...) — a CDC file was read as full-load "
            f"source. The source count would be inflated by CDC rows and the validation "
            f"result would be meaningless. Purge stale CDC files (processed/ / timestamp-named "
            f"CDC CSVs) from the table prefix (or verify pathGlobFilter='LOAD*.csv') and re-run.")
    return df


def apply_transform(df, target_columns, type_categories):
    """Apply the loader's per-column normalization so the source rows match what the target
    stores: real NULLs (null_marker_expr), uuid canonicalization, boolean mapping, ts/date ISO."""
    # (a) real NULLs -> NULL; every other value kept (same rule as the loader and CDC)
    for c in df.columns:
        df = df.withColumn(c, null_marker_expr(c, type_categories.get(c)))
    # (b) uuid: exactly-32-hex -> canonical (anchored, anti-truncation); canonical -> lower
    for c in [n for n in df.columns if type_categories.get(n) == 'uuid']:
        raw = col(c)
        reshaped = lower(concat(substring(raw, 1, 8), lit("-"), substring(raw, 9, 4), lit("-"),
                                substring(raw, 13, 4), lit("-"), substring(raw, 17, 4), lit("-"),
                                substring(raw, 21, 12)))
        df = df.withColumn(c, when(raw.isNull() | (raw == ""), lit(None))
                           .when(raw.rlike(UUID_RAW_HEX_RE), reshaped)
                           .when(raw.rlike(UUID_CANONICAL_RE), lower(raw))
                           .otherwise(raw))
    # (c) boolean -> true/false
    _bt, _bf = ["true", "t", "y", "yes"], ["false", "f", "n", "no"]
    for c in [n for n in df.columns if type_categories.get(n) == 'boolean']:
        numv = col(c).cast("double")
        txt = lower(trim(col(c)))
        df = df.withColumn(c, when(numv == 1, lit("true")).when(numv == 0, lit("false"))
                           .when(txt.isin(_bt), lit("true")).when(txt.isin(_bf), lit("false"))
                           .otherwise(lit(None)))
    # (c2) binary: DMS hex text -> '\x' + lowercase hex (same rule as the loader)
    for c in [n for n in df.columns if type_categories.get(n) == 'bytea']:
        hexpart = regexp_replace(col(c), r'^(\\x|0[xX])', '')
        df = df.withColumn(c, when(col(c).isNull(), lit(None))
                           .when(hexpart.rlike(r'^([0-9a-fA-F]{2})*$'), concat(lit('\\x'), lower(hexpart)))
                           .otherwise(col(c)))
    # (d) timestamptz / date
    for c in [n for n in df.columns if type_categories.get(n) == 'timestamptz']:
        df = df.withColumn(c, normalize_timestamp(c, "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    for c in [n for n in df.columns if type_categories.get(n) == 'date']:
        df = df.withColumn(c, normalize_timestamp(c, "yyyy-MM-dd"))
    return df


# =============================================================================
# MANIFEST + CONFIG
# =============================================================================
def load_manifest(s3):
    if INDEX_S3_KEY:
        bucket, key = split_s3(CONFIG_PREFIX)[0], INDEX_S3_KEY
    else:
        bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_manifest_index.json')
    doc = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8'))
    tables = doc.get('tables', [])
    if not tables:
        raise Exception("Master index has no tables — run Job 1 first.")
    return tables


def load_full_load_status(s3):
    """{table: status} from this group's _load_status.json, or None if the file doesn't exist.
    Any other read error is raised (a permissions or network problem must not look like
    "nothing to validate")."""
    bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_load_status.json')
    try:
        body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    doc = json.loads(body.decode('utf-8'))
    return {k: (v or {}).get('status') for k, v in (doc.get('tables', {}) or {}).items()}


def load_config(s3, entry):
    bucket, key = split_s3(entry['config_s3_path'])
    return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8'))


# =============================================================================
# PER-TABLE VALIDATION
# =============================================================================
def spark_pk_bounds(df, pk_src_col, pk_kind):
    """(min, max, count) of the PK on the transformed source, typed for the planner."""
    from pyspark.sql import functions as F
    if pk_kind == "uuid":
        pkc = F.lower(F.regexp_replace(F.col(pk_src_col), "-", ""))
        pkc = F.when(F.length(pkc) == 32, pkc)
    elif pk_kind == "text":
        pkc = F.col(pk_src_col).cast("string")
    else:
        pkc = F.col(pk_src_col).cast("long")
    row = df.select(F.min(pkc).alias("mn"), F.max(pkc).alias("mx"),
                    F.count(F.lit(1)).alias("n")).collect()[0]
    if row["n"] == 0 or row["mn"] is None:
        return (None, None, 0)
    if pk_kind == "integer":
        return (int(row["mn"]), int(row["mx"]), int(row["n"]))
    return (str(row["mn"]), str(row["mx"]), int(row["n"]))


def _range_predicate_sql(pk_col, pk_kind, lo, hi, is_top):
    """Index-usable SQL WHERE for a target range [lo,hi). Mirrors the loader's compare."""
    if pk_kind == "uuid":
        lo_lit = f"{_sql_str_literal(hex_to_canonical_uuid(lo))}::uuid"
        pred = f'"{pk_col}" >= {lo_lit}'
        if not is_top:
            hi_canon = hex_to_canonical_uuid(hi)
            if hi_canon:
                pred += f' AND "{pk_col}" < {_sql_str_literal(hi_canon)}::uuid'
        return pred
    if pk_kind == "text":
        pred = f'"{pk_col}" >= {_sql_str_literal(lo)}'
        if not is_top:
            pred += f' AND "{pk_col}" < {_sql_str_literal(hi)}'
        return pred
    pred = f'"{pk_col}" >= {int(lo)}'
    if not is_top:
        pred += f' AND "{pk_col}" < {int(hi)}'
    return pred


def read_target_column_types(conn, dsql_schema, dsql_table):
    """{column: (data_type, numeric_precision, numeric_scale, datetime_precision)} from DSQL."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name, data_type, numeric_precision, numeric_scale, datetime_precision "
            "FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (dsql_schema, dsql_table))
        return {r[0]: (r[1], r[2], r[3], r[4]) for r in cur.fetchall()}
    finally:
        cur.close()


def column_kind(data_type):
    """How a column is summarized, from its DSQL data_type."""
    dt = (data_type or "").lower()
    if dt in ("character varying", "text", "varchar"):
        return "text"
    if dt in ("character", "char", "bpchar"):
        return "char"
    if dt == "uuid":
        return "uuid"
    if dt == "bytea":
        return "bytea"
    if dt == "boolean":
        return "bool"
    if dt in ("smallint", "integer", "bigint"):
        return "int"
    if dt in ("numeric", "decimal"):
        return "numeric"
    if dt in ("real", "double precision"):
        return "float"
    if dt.startswith("timestamp"):
        return "ts"
    if dt == "date":
        return "date"
    return "count"


_TS_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
_HEX = "0123456789abcdef"
_HASH_DIGITS = 6          # first 6 hex digits of md5 (24 bits) per value
_HASH_STATE = {"ok": None}
_HASH_LOCK = threading.Lock()


def _hash_sql(text_expr):
    """DSQL: the first _HASH_DIGITS hex digits of md5(value) as an integer, using only
    md5/substr/strpos (no bit-string casts)."""
    h = f"md5({text_expr})"
    terms = [f"(strpos('{_HEX}', substr({h}, {i + 1}, 1)) - 1)::bigint * {16 ** (_HASH_DIGITS - 1 - i)}"
             for i in range(_HASH_DIGITS)]
    return "(" + " + ".join(terms) + ")"


def hash_supported():
    """Probe once per run whether DSQL has md5()/strpos(); without them the per-value hash
    check is dropped (the other checks still run) and the run log says so."""
    with _HASH_LOCK:
        if _HASH_STATE["ok"] is None:
            try:
                conn = connect_dsql(autocommit=True)
                try:
                    cur = conn.cursor()
                    cur.execute(f"SELECT {_hash_sql(chr(39) + 'abc' + chr(39))}")
                    got = int(cur.fetchone()[0])
                    cur.close()
                finally:
                    conn.close()
                # md5('abc') = 900150983cd24fb0d6963f7d28e17f72
                _HASH_STATE["ok"] = (got == int("900150983cd24fb0d6963f7d28e17f72"[:_HASH_DIGITS], 16))
                if not _HASH_STATE["ok"]:
                    print(f"  ⚠️ DSQL md5 check returned {got}; per-value hash check disabled")
            except Exception as e:
                _HASH_STATE["ok"] = False
                print(f"  ⚠️ DSQL has no usable md5()/strpos() ({type(e).__name__}: {e}); "
                      f"per-value hash check disabled, other checks still run")
        return _HASH_STATE["ok"]


def build_metrics(columns, target_types, with_hash=False):
    """Per-column summaries with the same meaning on both sides. Each metric is a dict:
         col, check, src (fn(F, column) -> Spark aggregate), sql (DSQL aggregate),
         cmp ('exact' | 'decimal' | 'float'), tol (allowed difference: per row for decimal,
         relative for float), empty (its value over zero rows)."""
    m = []
    for c in columns:
        dt, prec, scale, dtp = target_types[c]
        kind = column_kind(dt)
        q = '"' + c.replace('"', '""') + '"'
        tick = '`' + c.replace('`', '``') + '`'

        def add(check, src, sql, cmp="exact", tol=0, empty=0, _c=c, _k=kind, _dt=dt):
            m.append({"col": _c, "kind": _k, "type": _dt, "check": check, "src": src,
                      "sql": sql, "cmp": cmp, "tol": tol, "empty": empty})

        add("non-null count", lambda F, x: F.count(x), f"COUNT({q})")
        if kind in ("text", "char", "uuid", "bytea"):
            # Source value = what the load stored. Target ::text = the stored value as text
            # (lowercase canonical uuid; '\x' + lowercase hex for binary; char(n) without its
            # padding, so the source drops trailing spaces for char columns too). DSQL uses the
            # C collation (byte order), the same order Spark uses for min/max on strings.
            if kind == "char":
                s = lambda F, x: F.rtrim(x)
            else:
                s = lambda F, x: x
            t_ = q if kind == "text" else f"({q})::text"
            if kind != "uuid":
                add("total length", lambda F, x, s=s: F.coalesce(F.sum(F.length(s(F, x))), F.lit(0)),
                    f"COALESCE(SUM(length({t_})), 0)")
            add("min", lambda F, x, s=s: F.min(s(F, x)), f"MIN({t_})", empty=None)
            add("max", lambda F, x, s=s: F.max(s(F, x)), f"MAX({t_})", empty=None)
            if with_hash:
                # Sum of a per-value hash: catches a changed value even when its length and the
                # column's min/max stay the same. md5 is over the UTF-8 bytes on both sides.
                add("value hash sum",
                    lambda F, x, s=s: F.coalesce(F.sum(F.conv(F.substring(F.md5(s(F, x)), 1, _HASH_DIGITS),
                                                              16, 10).cast("decimal(38,0)")),
                                                 F.lit(0).cast("decimal(38,0)")),
                    f"COALESCE(SUM({_hash_sql(t_)}), 0)", "decimal", 0)
        elif kind == "bool":
            add("true count",
                lambda F, x: F.coalesce(F.sum(F.when(x == F.lit("true"), 1).otherwise(0)), F.lit(0)),
                f"COALESCE(SUM(CASE WHEN {q} THEN 1 ELSE 0 END), 0)")
        elif kind == "int":
            # The load casts '%s::numeric::bigint': numeric -> integer rounds half away from
            # zero, which is what Spark's round() does.
            add("sum", lambda F, x: F.coalesce(F.sum(F.round(x.cast("decimal(38,6)"), 0).cast("decimal(38,0)")),
                                              F.lit(0).cast("decimal(38,0)")),
                f"COALESCE(SUM({q}), 0)", "decimal", 0)
        elif kind == "numeric":
            # numeric(p,s) stores the value rounded to s places (half away from zero); Spark's
            # cast to decimal(38,s) rounds the same way. Unconstrained or very wide numerics are
            # summed at 10 places with a matching tolerance.
            exact = scale is not None and 0 <= int(scale) <= 18 and (prec is None or int(prec) <= 31)
            s_eff = int(scale) if exact else 10
            add("sum", lambda F, x, s_eff=s_eff: F.coalesce(F.sum(x.cast(f"decimal(38,{s_eff})")),
                                                           F.lit(0).cast(f"decimal(38,{s_eff})")),
                f"COALESCE(SUM({q}), 0)", "decimal", 0 if exact else 10 ** -s_eff)
        elif kind == "float":
            add("sum", lambda F, x: F.coalesce(F.sum(x.cast("double")), F.lit(0.0)),
                f"COALESCE(SUM(({q})::double precision), 0)", "float",
                1e-6 if (dt or "").lower() == "real" else 1e-9)
        elif kind in ("ts", "date"):
            fmt = _TS_FMT if kind == "ts" else "yyyy-MM-dd"
            p6 = 6 if (dtp is None or kind == "date") else max(0, min(6, int(dtp)))
            add("sum of instants (us)",
                lambda F, x, tick=tick, fmt=fmt: F.coalesce(F.sum(F.expr(
                    f"CAST(unix_micros(to_timestamp({tick}, '{fmt}')) AS DECIMAL(38,0))")),
                    F.lit(0).cast("decimal(38,0)")),
                f"COALESCE(SUM(EXTRACT(EPOCH FROM {q}) * 1000000), 0)", "decimal",
                0 if p6 >= 6 else 10 ** (6 - p6))
    return m


def _same(metric, s, t, rows):
    """Do the two summaries agree?"""
    if s is None or t is None:
        return s is None and t is None
    if metric["cmp"] == "exact":
        if isinstance(s, str) or isinstance(t, str):
            return str(s) == str(t)
        return Decimal(str(s)) == Decimal(str(t))
    if metric["cmp"] == "decimal":
        diff = abs(Decimal(str(s)) - Decimal(str(t)))
        return diff <= Decimal(str(metric["tol"])) * max(1, int(rows or 0))
    a, b = float(s), float(t)
    return abs(a - b) <= metric["tol"] * max(1.0, abs(a), abs(b))


def target_range_summary(conn, dsql_schema, dsql_table, pred, metrics):
    """(row count, [metric values]) for one range of the DSQL target (pred None = whole table)."""
    parts = ["count(*)"] + [m["sql"] for m in metrics]
    where = f" WHERE {pred}" if pred else ""
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT {", ".join(parts)} FROM {dsql_schema}.{dsql_table}{where}')
        r = cur.fetchone()
        return int(r[0]), list(r[1:])
    finally:
        cur.close()


def source_range_summaries(df, pk_src_col, pk_kind, ranges, metrics):
    """{range index: (row count, [metric values])} on the transformed SOURCE, in one pass.
    pk_kind None = the whole table as a single range."""
    from pyspark.sql import functions as F
    if pk_kind is None:
        bucket = F.lit(0)
    else:
        if pk_kind == "uuid":
            keyc = F.lower(F.regexp_replace(F.col(pk_src_col), "-", ""))
        else:
            keyc = F.col(pk_src_col).cast("string")
        bucket = F.lit(-1)
        for i, (lo, hi) in enumerate(ranges):
            is_top = (i == len(ranges) - 1)
            if pk_kind == "integer":
                keyi = F.col(pk_src_col).cast("long")
                cond = (keyi >= F.lit(int(lo))) if is_top else \
                    ((keyi >= F.lit(int(lo))) & (keyi < F.lit(int(hi))))
            else:
                cond = (keyc >= F.lit(str(lo))) if is_top else \
                    ((keyc >= F.lit(str(lo))) & (keyc < F.lit(str(hi))))
            bucket = F.when(cond, F.lit(i)).otherwise(bucket)
    dfb = df.withColumn("_rng", bucket)
    aggs = [F.count(F.lit(1)).alias("_cnt")]
    for j, m in enumerate(metrics):
        aggs.append(m["src"](F, F.col(m["col"])).alias(f"_m{j}"))
    out = {}
    for row in dfb.groupBy("_rng").agg(*aggs).collect():
        ri = row["_rng"]
        if ri is None or ri < 0:
            continue
        out[ri] = (int(row["_cnt"]), [row[f"_m{j}"] for j in range(len(metrics))])
    return out


def _target_query(dsql_schema, dsql_table, pred, metrics):
    conn = connect_dsql(autocommit=True)
    try:
        return target_range_summary(conn, dsql_schema, dsql_table, pred, metrics)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =============================================================================
# V2: keep every validation query under DSQL's 300s transaction-age limit
# =============================================================================
def is_txn_age_error(exc):
    """True if an exception is DSQL's transaction-age limit (SQLSTATE 54000) or a
    statement_timeout (57014) — the two ways a too-large validation query fails. Matches on
    SQLSTATE where pg8000 exposes it and on the message text otherwise."""
    s = ""
    try:
        a0 = exc.args[0] if getattr(exc, "args", None) else None
        if isinstance(a0, dict):
            code = a0.get("C") or a0.get("code")
            if code in ("54000", "57014"):
                return True
            s = str(a0.get("M") or a0.get("message") or a0)
        else:
            s = str(a0 if a0 is not None else exc)
    except Exception:
        s = str(exc)
    s = s.lower()
    return ("transaction age limit" in s or "54000" in s
            or "statement timeout" in s or "statement_timeout" in s
            or "canceling statement due to statement timeout" in s or "57014" in s)


def combine_metric(check, a, b):
    """Combine the per-range metric values of two adjacent sub-ranges into the value the single
    unsplit query would have produced. Additive checks (counts/lengths/sums/hashes) add; min
    takes the min; max takes the max. None means 'no rows contributed' for min/max."""
    if check == "min":
        if a is None:
            return b
        if b is None:
            return a
        return a if a <= b else b
    if check == "max":
        if a is None:
            return b
        if b is None:
            return a
        return a if a >= b else b
    # additive: non-null count, total length, true count, value hash sum, sum,
    # sum of instants (us)
    av = a if a is not None else 0
    bv = b if b is not None else 0
    try:
        return av + bv
    except TypeError:
        return Decimal(str(av)) + Decimal(str(bv))


def combine_summaries(res_a, res_b, metrics):
    """Combine two (count, [metric values]) sub-range results into one, re-aggregating each
    metric per combine_metric so a re-split range equals the unsplit range exactly."""
    ca, va = res_a
    cb, vb = res_b
    merged = []
    for j, m in enumerate(metrics):
        merged.append(combine_metric(m["check"], va[j], vb[j]))
    return (int(ca) + int(cb), merged)


def split_bounds(lo, hi, pk_kind):
    """Split a half-open key range [lo,hi) into [[lo,mid),[mid,hi)] with a midpoint in the same
    representation the range uses. Returns None when it cannot be split further (degenerate
    single-key range, or no bound to split on)."""
    if lo is None or hi is None:
        return None
    if pk_kind == "integer":
        lo_i, hi_i = int(lo), int(hi)
        if hi_i - lo_i <= 1:
            return None
        mid = lo_i + (hi_i - lo_i) // 2
        if mid <= lo_i or mid >= hi_i:
            return None
        return [(lo_i, mid), (mid, hi_i)]
    # uuid/text ranges are compared as hex integers of the (dash-stripped) key
    try:
        lo_i, hi_i = hex_to_int(str(lo)), hex_to_int(str(hi))
    except Exception:
        return None
    if hi_i - lo_i <= 1:
        return None
    mid = lo_i + (hi_i - lo_i) // 2
    if mid <= lo_i or mid >= hi_i:
        return None
    return [(int_to_hex(lo_i), int_to_hex(mid)), (int_to_hex(mid), int_to_hex(hi_i))]


def target_range_resplit(dsql_schema, dsql_table, pk_col, pk_kind, lo, hi, is_top,
                         metrics, depth=0):
    """Summarize ONE target range, re-splitting on a transaction-age / statement-timeout error.
    The combined result equals what a single unsplit query would return (combine_summaries),
    so the source side's fixed top-level ranges still line up. Each query runs in its own short
    autocommit transaction (connect_dsql) bounded by statement_timeout; a range that still
    times out is halved and retried up to VALIDATE_MAX_RESPLIT_DEPTH times. If it cannot be
    split further, the error propagates (table recorded as 'error' with the real cause)."""
    pred = None if (lo is None and hi is None and pk_col is None) \
        else _range_predicate_sql(pk_col, pk_kind, lo, hi, is_top)
    try:
        return _target_query(dsql_schema, dsql_table, pred, metrics)
    except Exception as e:
        if not is_txn_age_error(e) or depth >= VALIDATE_MAX_RESPLIT_DEPTH:
            raise
        halves = split_bounds(lo, hi, pk_kind) if pk_col is not None else None
        if not halves:
            raise
        (l1, h1), (l2, h2) = halves
        print(f"    ↪ validation re-split {dsql_schema}.{dsql_table} range [{lo},{hi}) at depth "
              f"{depth} after a transaction-age/timeout error; retrying two sub-ranges")
        r1 = target_range_resplit(dsql_schema, dsql_table, pk_col, pk_kind, l1, h1,
                                  False, metrics, depth + 1)
        # the upper sub-range keeps the original is_top (only the very top range is unbounded)
        r2 = target_range_resplit(dsql_schema, dsql_table, pk_col, pk_kind, l2, h2,
                                  is_top, metrics, depth + 1)
        return combine_summaries(r1, r2, metrics)


def _first_pk_rangeable(pk_meta):
    """For a COMPOSITE (multi-column) PK, decide whether its FIRST key column can be range-
    split, and return (first_column_name, pk_kind) where pk_kind is 'integer' | 'uuid' | 'text'
    — the kinds plan_ranges / spark_pk_bounds / _range_predicate_sql understand. Returns
    (None, None) if the first column is not a rangeable kind (timestamp/float/scaled numeric/
    bytea/bool) or metadata is missing. Value ranges on the first column partition every row
    into exactly one bucket identically on Spark and DSQL."""
    cols = pk_meta.get("columns") or []
    if len(cols) < 2:
        return (None, None)
    data_types = pk_meta.get("data_types") or []
    scales = pk_meta.get("numeric_scales") or []
    dt0 = (data_types[0] if data_types else None)
    scale0 = (scales[0] if scales else None)
    kind = column_kind(dt0)
    if kind == "int":
        return (cols[0], "integer")
    if kind == "numeric" and scale0 is not None and int(scale0) == 0:
        return (cols[0], "integer")
    if kind == "uuid":
        return (cols[0], "uuid")
    # NOTE: text/char first columns are intentionally NOT range-split here — job3 has no
    # byte-string range planner (and split_bounds only halves integer/uuid key spaces), so a
    # text first column would collapse to one unsplit range. Those stay whole-table, bounded by
    # statement_timeout. (The real large composite tables key on integer/uuid first columns.)
    return (None, None)


def validate_one_table(s3, entry):
    """Validate ONE table: plan ranges, summarize the source (Spark, one pass) and the target
    (DSQL, parallel), compare. Every table ends as match, mismatch or error — never skipped."""
    config = load_config(s3, entry)
    meta = config.get('metadata', {})
    dsql_schema = meta.get('dsql_schema') or entry['dsql_schema']
    dsql_table = meta.get('dsql_table') or entry['dsql_table']
    label = f"{dsql_schema}.{dsql_table}"
    dms_s3_path = meta.get('dms_s3_path')
    dms_has_headers = meta.get('dms_has_headers', False)
    type_categories = config.get('type_categories', {}) or {}
    target_columns = [c['name'] if isinstance(c, dict) else c
                      for c in config.get('target_columns', [])]
    pk_meta = meta.get('primary_key', {}) or {}
    pk_cols = pk_meta.get('columns') or []
    pk_kind = pk_meta.get('pk_kind')
    rangeable = len(pk_cols) == 1 and bool(pk_kind) and bool(pk_meta.get('span_recoverable'))
    pk_col = pk_cols[0] if rangeable else None
    notes = []
    if not rangeable:
        # V2: a MULTI-column (composite) PK can still be split into index-usable ranges on its
        # FIRST key column's value — every row falls in exactly one half-open first-column
        # range, so per-range counts and additive metric sums (and min/max) sum to the whole-
        # table result on BOTH the Spark source and the DSQL target. This keeps a large
        # composite table (e.g. 16M+ rows) off the single-unbounded-transaction path that
        # exceeds DSQL's 300s limit. Only a genuinely keyless table (no PK at all) stays
        # whole-table (there is no column to range on); statement_timeout still bounds it.
        fc_col, fc_kind = _first_pk_rangeable(pk_meta)
        if len(pk_cols) > 1 and fc_col:
            rangeable = True
            pk_col = fc_col
            pk_kind = fc_kind
            notes.append(f"composite PK {pk_cols}: ranged on the first key column "
                         f"{fc_col!r} ({fc_kind}) so each query stays under the DSQL 300s limit")
        else:
            notes.append("whole table compared as one range (no single-column key that can be "
                         "split into ranges)")
            pk_kind = None

    conn = connect_dsql(autocommit=True)
    try:
        target_types = read_target_column_types(conn, dsql_schema, dsql_table)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not target_types:
        return {"table": label, "status": "error", "reason": "target table not found in DSQL"}

    # ---- V1: empty-source table (no DMS S3 folder) ----
    # A 0-row source table produces NO DMS full-load folder, so reading it with Spark raises
    # "Path does not exist". Detect the missing folder up front: if discovery recorded this
    # table as empty-at-discovery (or 0 full-load rows), treat the source as 0 rows WITHOUT
    # reading S3 and just compare the DSQL target count — 0 target rows = PASS (empty source,
    # empty target), >0 = a real mismatch (rows in the target that are not in the empty source).
    # A table discovery did NOT mark empty but whose folder is missing is a genuine problem and
    # still surfaces as a clear error (not a silent pass).
    _empty_at_discovery = bool(meta.get("empty_at_discovery")) or (meta.get("full_load_rows") == 0)
    _src_bucket, _src_prefix = split_s3(dms_s3_path) if dms_s3_path else ("", "")
    _source_present = bool(dms_s3_path) and s3_prefix_has_objects(s3, _src_bucket, _src_prefix)
    if not _source_present:
        conn = connect_dsql(autocommit=True)
        try:
            tgt_cnt, _ = target_range_summary(conn, dsql_schema, dsql_table, None, [])
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if _empty_at_discovery:
            if int(tgt_cnt) == 0:
                return {"table": label, "status": "match", "ranges": 0,
                        "source_rows": 0, "target_rows": 0, "checksum_mode": CHECKSUM_MODE,
                        "columns_compared": 0, "columns_differing": [], "mismatches": [],
                        "mismatch_count": 0,
                        "notes": ["empty source table (no DMS full-load folder) and empty "
                                  "DSQL target: 0 == 0, PASS"]}
            return {"table": label, "status": "mismatch", "ranges": 1,
                    "source_rows": 0, "target_rows": int(tgt_cnt), "checksum_mode": CHECKSUM_MODE,
                    "columns_compared": 0, "columns_differing": [],
                    "mismatches": [{"range": ["whole table"], "type": "COUNT_DIFF",
                                    "source": 0, "target": int(tgt_cnt)}],
                    "mismatch_count": 1,
                    "notes": ["empty source table (no DMS full-load folder) but the DSQL target "
                              f"has {int(tgt_cnt)} row(s)"]}
        return {"table": label, "status": "error",
                "reason": f"no DMS full-load files under {dms_s3_path} and discovery did NOT "
                          f"mark this table empty-at-discovery; cannot validate (is the DMS "
                          f"folder/path correct?)"}

    # ---- SOURCE: read, rename DMS columns to their target names, THEN transform ----
    # Same order as the load: the per-type conversions are keyed by TARGET column name, so
    # renaming first is what makes them apply when DMS writes the names in another case.
    df = read_dms_csv(dms_s3_path, dms_has_headers)
    rename = {}
    for mp in config.get('column_mapping', []):
        if isinstance(mp, dict) and mp.get('action') == 'map':
            dmsn, tgtn = mp.get('dms_column_name'), mp.get('target_column')
            for dc in df.columns:
                if dc.lower().strip() == str(dmsn).lower().strip():
                    rename[dc] = tgtn
    for old, new in rename.items():
        if old != new and old in df.columns:
            df = df.withColumnRenamed(old, new)
    df_t = apply_transform(df, target_columns, type_categories)
    if rangeable and pk_col not in df_t.columns:
        # The chosen key column isn't in the DMS CSV. For a single-column rangeable PK that's a
        # real error (we cannot range or even identify rows); for a composite table ranged on
        # its first key column, fall back to whole-table (statement_timeout still bounds it).
        if len(pk_cols) == 1:
            return {"table": label, "status": "error",
                    "reason": f"key column {pk_col!r} not found in the DMS CSV"}
        notes.append(f"first key column {pk_col!r} not in the DMS CSV; fell back to whole table")
        rangeable = False
        pk_col = None
        pk_kind = None

    # Compared: every target column the source provides (the others get their DSQL DEFAULT).
    compared = [c for c in target_columns if c in df_t.columns and c in target_types]
    not_in_source = [c for c in target_columns if c not in df_t.columns]
    if not_in_source:
        notes.append(f"not in the DMS CSV (filled by DSQL defaults), not compared: {not_in_source}")
    metrics = build_metrics(compared, target_types, with_hash=hash_supported()) \
        if CHECKSUM_MODE != "off" else []

    if rangeable:
        mn, mx, total = spark_pk_bounds(df_t, pk_col, pk_kind)
    else:
        mn = mx = None
        total = df_t.count()

    if total == 0:
        ranges = [(None, None)]
        src = {}
        rangeable_now = False
    else:
        rangeable_now = rangeable
        if not rangeable:
            ranges = [(None, None)]
        elif pk_kind == "uuid":
            ranges = plan_ranges_hex(mn, mx, total, VALIDATE_ROWS_PER_RANGE)
        elif pk_kind == "integer":
            ranges = plan_ranges(mn, mx, total, VALIDATE_ROWS_PER_RANGE)
        else:
            ranges = [(mn, mx)]
        src = source_range_summaries(df_t, pk_col if rangeable else None,
                                     pk_kind if rangeable else None, ranges, metrics)

    # ---- TARGET: per-range summaries (DSQL, parallel) ----
    tgt = {}
    lock = threading.Lock()

    def _one(i_rg):
        i, (lo, hi) = i_rg
        is_top = (i == len(ranges) - 1)
        if not rangeable_now:
            res = _target_query(dsql_schema, dsql_table, None, metrics)
        else:
            # V2: run with re-split-on-timeout so a too-large range is halved and retried
            # instead of erroring the whole table at the DSQL 300s limit.
            res = target_range_resplit(dsql_schema, dsql_table, pk_col, pk_kind,
                                       lo, hi, is_top, metrics)
        with lock:
            tgt[i] = res

    with ThreadPoolExecutor(max_workers=max(1, MAX_QUERY_CONCURRENCY),
                            thread_name_prefix="vquery") as pool:
        for f in as_completed([pool.submit(_one, (i, rg)) for i, rg in enumerate(ranges)]):
            f.result()

    # ---- COMPARE ----
    mismatches, bad_cols = [], set()
    src_total = tgt_total = 0
    empty_vals = [mt["empty"] for mt in metrics]
    for i, (lo, hi) in enumerate(ranges):
        s_cnt, s_vals = src.get(i, (0, empty_vals))
        t_cnt, t_vals = tgt.get(i, (0, empty_vals))
        src_total += s_cnt
        tgt_total += t_cnt
        rng = ["whole table"] if lo is None else [str(lo), str(hi)]
        if s_cnt != t_cnt:
            mismatches.append({"range": rng, "type": "COUNT_DIFF", "source": s_cnt, "target": t_cnt})
            continue
        for j, mt in enumerate(metrics):
            if not _same(mt, s_vals[j], t_vals[j], s_cnt):
                bad_cols.add(mt["col"])
                mismatches.append({"range": rng, "type": "CONTENT_DIFF", "column": mt["col"],
                                   "column_type": mt["type"], "check": mt["check"],
                                   "source": str(s_vals[j])[:120], "target": str(t_vals[j])[:120]})

    status = "match" if (not mismatches and src_total == tgt_total) else "mismatch"
    return {
        "table": label, "status": status, "ranges": len(ranges),
        "source_rows": src_total, "target_rows": tgt_total,
        "checksum_mode": CHECKSUM_MODE, "columns_compared": len(compared) if metrics else 0,
        "columns_differing": sorted(bad_cols),
        "mismatches": mismatches[:50], "mismatch_count": len(mismatches), "notes": notes,
    }


# =============================================================================
# MAIN
# =============================================================================
print("=" * 70)
print("JOB 3 VALIDATION — S3 source vs Aurora DSQL target (per-range fingerprint)")
print(f"  rows/range={VALIDATE_ROWS_PER_RANGE} query_conc={MAX_QUERY_CONCURRENCY} "
      f"parallel_tables={MAX_PARALLEL_TABLES} checksum={CHECKSUM_MODE}")
print("=" * 70)

s3_main = make_boto_client('s3')
tables = load_manifest(s3_main)
results = []
_rlock = threading.Lock()
worklist = []
if REQUIRE_FULL_LOAD_DONE:
    load_status = load_full_load_status(s3_main)
    if load_status is None:
        raise Exception(
            f"No full-load status file at {CONFIG_PREFIX.rstrip('/')}/_load_status.json, so there is "
            f"no record that these {len(tables)} table(s) were loaded. Validation will not report "
            f"success without checking anything. Run the load for this group first, or start "
            f"validation with --require_full_load_done false to compare whatever is in DSQL now.")
    for entry in tables:
        label = f"{entry['dsql_schema']}.{entry['dsql_table']}"
        st = load_status.get(label)
        if st != "done":
            # A table in this group that the load didn't finish is a failure, not a skip.
            results.append({"table": label, "status": "error",
                            "reason": f"full load not marked done (status={st!r})"})
            print(f"  ✗ {label}: full load not marked done (status={st!r})")
            continue
        worklist.append(entry)
else:
    worklist = list(tables)

print(f"  Validating {len(worklist)} table(s)")


def _run(entry):
    s3w = make_boto_client('s3')
    label = f"{entry['dsql_schema']}.{entry['dsql_table']}"
    try:
        r = validate_one_table(s3w, entry)
    except Exception as e:
        r = {"table": label, "status": "error", "reason": str(e)}
    with _rlock:
        results.append(r)
    print(f"  • {r['table']}: {r['status']}"
          + (f" ({r.get('mismatch_count', 0)} difference(s), "
             f"src={r.get('source_rows')} tgt={r.get('target_rows')}, "
             f"columns compared={r.get('columns_compared', 0)}"
             + (f", differing={r['columns_differing']}" if r.get('columns_differing') else "")
             + ")"
             if r['status'] in ('match', 'mismatch') else
             f" — {r.get('reason', '')}"))
    return r


if MAX_PARALLEL_TABLES <= 1:
    for e in worklist:
        _run(e)
else:
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES, thread_name_prefix="vtable") as pool:
        futs = [pool.submit(_run, e) for e in worklist]
        for f in as_completed(futs):
            f.result()

# ---- SUMMARY + report ----
matched = [r for r in results if r["status"] == "match"]
mismatched = [r for r in results if r["status"] == "mismatch"]
errored = [r for r in results if r["status"] in ("error",)]
skipped = [r for r in results if r["status"] not in ("match", "mismatch", "error")]

print(f"\n{'='*70}")
print("JOB 3 VALIDATION SUMMARY")
print(f"{'='*70}")
print(f"  Validated : {len(results)}")
print(f"  MATCH     : {len(matched)}")
print(f"  MISMATCH  : {len(mismatched)}")
print(f"  ERROR     : {len(errored)}")
print(f"  SKIPPED   : {len(skipped)}")
for r in mismatched:
    print(f"    ✗ {r['table']}: {r['mismatch_count']} difference(s) "
          f"(src={r['source_rows']} tgt={r['target_rows']}; columns: {r.get('columns_differing') or '-'})")
    for m in r["mismatches"][:5]:
        print(f"        {m}")
for r in errored:
    print(f"    ! {r['table']}: {r.get('reason')}")

# Write a JSON report to S3 next to the config.
try:
    rep_bucket, rep_key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_validation_report.json')
    s3_main.put_object(Bucket=rep_bucket, Key=rep_key,
                       Body=json.dumps({"results": results}, indent=2).encode('utf-8'),
                       ContentType='application/json')
    print(f"\n  Report written: {CONFIG_PREFIX.rstrip('/')}/_validation_report.json")
except Exception as e:
    print(f"  ⚠️ could not write report (non-fatal): {e}")

# M12: Guard job.commit() with a connectivity check — without a Glue VPC Interface
# Endpoint this call HANGS for 10+ minutes on a no-internet Glue connection. (Same guard
# as Job 1 / Job 2.) The validation report is already written to S3 above, so skipping the
# commit when Glue is unreachable loses nothing.
commit_glue_reachable = False
try:
    _sock = socket.create_connection((f"glue.{REGION}.amazonaws.com", 443), timeout=GLUE_API_TIMEOUT)
    _sock.close()
    commit_glue_reachable = True
except (socket.timeout, socket.error, OSError):
    pass

if commit_glue_reachable:
    job.commit()
    print("  ✓ job.commit() succeeded")
else:
    print("  ⚠️ Skipping job.commit() — Glue API not reachable (validation report already written to S3).")

if not results:
    raise Exception("Validation checked no tables, which can't be reported as success.")
if mismatched or errored or skipped:
    raise Exception(f"Validation found {len(mismatched)} mismatched + {len(errored)} errored + {len(skipped)} unchecked "
                    f"table(s). See _validation_report.json.")
