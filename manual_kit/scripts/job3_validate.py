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
  Tier 2 (CHECKSUM_MODE != "off"): per-range content fingerprint — proves the TRANSFORM
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
import threading
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
DSQL_DATABASE = 'postgres'
DSQL_USER = 'admin'
REGION = 'us-east-1'

# ---- CUSTOMER-TUNABLE KNOBS -------------------------------------------------
# Rows per validation range. Bigger = fewer, larger queries (watch the 300s DSQL txn
# limit); smaller = more parallelism, more round-trips. Ranges are index-usable on the PK.
VALIDATE_ROWS_PER_RANGE = 250000
# Max concurrent per-range TARGET queries across all tables (DSQL connection budget).
MAX_QUERY_CONCURRENCY = 20
# Max tables validated concurrently on the driver.
MAX_PARALLEL_TABLES = 4
# Content check tier:
#   "off"       (DEFAULT): per-range COUNT compare only. ALWAYS reliable — counts match
#               regardless of how each engine renders a value. Catches missing/extra rows.
#   "aggregate": count + a per-column fingerprint (SUM(length)+COUNT) over the fingerprint
#               columns. IMPORTANT: a content checksum is only trustworthy for columns whose
#               SOURCE string form == TARGET ::text form. That holds for TEXT/VARCHAR columns
#               but NOT for typed columns (numeric/timestamp/uuid render differently as text
#               on each side), so v4 restricts the checksum to VARCHAR/TEXT columns only
#               (fp_text_cols below). Typed columns are still covered by the COUNT check.
#   "md5"     : same restriction; md5(string_agg(...)) — needs md5()+string_agg on DSQL.
# Start at "off" (count-only, zero false positives); enable "aggregate" for text-content
# assurance once you've confirmed it behaves on your data.
CHECKSUM_MODE = "off"
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
def _apply_job3_arg_overrides():
    global CONFIG_PREFIX, INDEX_S3_KEY, DSQL_ENDPOINT, DSQL_USER, DSQL_DATABASE, REGION
    global CHECKSUM_MODE, MAX_PARALLEL_TABLES, VALIDATE_ROWS_PER_RANGE, MAX_QUERY_CONCURRENCY
    global REQUIRE_FULL_LOAD_DONE
    optional = ["config_prefix", "index_s3_key", "dsql_endpoint", "dsql_user",
                "dsql_database", "region",
                "checksum_mode", "max_parallel_tables", "validate_rows_per_range",
                "max_query_concurrency", "require_full_load_done"]
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
    if "dsql_user" in ov and str(ov["dsql_user"]).strip():
        DSQL_USER = str(ov["dsql_user"]).strip()
    if "dsql_database" in ov and str(ov["dsql_database"]).strip():
        DSQL_DATABASE = str(ov["dsql_database"]).strip()
    if "region" in ov and str(ov["region"]).strip():
        REGION = str(ov["region"]).strip()
    if "checksum_mode" in ov:
        _cm = str(ov["checksum_mode"]).strip().lower()
        if _cm in ("off", "aggregate", "md5"):
            CHECKSUM_MODE = _cm
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

NULL_SENTINELS = {"NULL", "N/A", "NA", "NONE", "(NULL)", "\\N"}
_SENTINEL_UPPER = [s.upper() for s in NULL_SENTINELS]

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


# =============================================================================
# DSQL CONNECTION
# =============================================================================
def connect_dsql(autocommit=True):
    """Open a short-lived DSQL connection for one range fingerprint query.

    Bounded retry with backoff: a fresh IAM token is minted on EVERY attempt, so a transient
    open failure (08006 unable-to-connect, TLS blip, throttle) retries instead of failing the
    whole table's validation. On exhaustion the last error is raised (the caller records the
    table as 'error')."""
    ctx = ssl.create_default_context()
    _last = None
    for _attempt in range(1, 5):   # up to 4 attempts
        try:
            client = make_boto_client("dsql")
            tok = client.generate_db_connect_admin_auth_token(
                DSQL_ENDPOINT, Region=REGION, ExpiresIn=3600)
            conn = pg8000.connect(host=DSQL_ENDPOINT, port=5432, database=DSQL_DATABASE,
                                  user=DSQL_USER, password=tok, ssl_context=ctx)
            conn.autocommit = autocommit
            return conn
        except Exception as e:
            _last = e
            if _attempt < 4:
                time.sleep(min(8.0, 0.5 * (2 ** (_attempt - 1))))   # 0.5,1,2s backoff
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
    stores: null-sentinel coercion, uuid canonicalization, boolean mapping, ts/date ISO."""
    # (a) null sentinels + empty -> NULL
    for c in df.columns:
        trimmed = trim(col(c))
        df = df.withColumn(c, when(trimmed == "", lit(None))
                           .when(upper(trimmed).isin(_SENTINEL_UPPER), lit(None))
                           .otherwise(trimmed))
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
    bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_load_status.json')
    try:
        doc = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8'))
        return {k: (v or {}).get('status') for k, v in (doc.get('tables', {}) or {}).items()}
    except Exception:
        return {}


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


def target_range_fingerprint(conn, dsql_schema, dsql_table, pk_col, pk_kind,
                             lo, hi, is_top, fp_cols):
    """Compute (count, checksum) for one range on the DSQL target. checksum is None in
    count-only mode. 'aggregate' mode: sum of per-column text lengths + non-null counts —
    portable, no special function. 'md5' mode: md5 over an ordered concat (needs md5()+
    string_agg)."""
    pred = _range_predicate_sql(pk_col, pk_kind, lo, hi, is_top)
    cur = conn.cursor()
    try:
        if CHECKSUM_MODE == "off" or not fp_cols:
            cur.execute(f'SELECT count(*) FROM {dsql_schema}.{dsql_table} WHERE {pred}')
            return (int(cur.fetchone()[0]), None)
        if CHECKSUM_MODE == "md5":
            row_expr = " || '|' || ".join(f'COALESCE("{c}"::text, \'\')' for c in fp_cols)
            cur.execute(
                f'SELECT count(*), md5(COALESCE(string_agg(({row_expr}), \'\' '
                f'ORDER BY "{pk_col}"), \'\')) '
                f'FROM {dsql_schema}.{dsql_table} WHERE {pred}')
            r = cur.fetchone()
            return (int(r[0]), r[1])
        # aggregate (default, portable): count + sum(len of each col) + non-null counts
        parts = ["count(*)"]
        for c in fp_cols:
            parts.append(f'COALESCE(SUM(length(COALESCE("{c}"::text, \'\'))), 0)')
            parts.append(f'COUNT("{c}")')
        cur.execute(f'SELECT {", ".join(parts)} FROM {dsql_schema}.{dsql_table} WHERE {pred}')
        r = cur.fetchone()
        cnt = int(r[0])
        fp = "|".join(str(x) for x in r[1:])
        return (cnt, fp)
    finally:
        cur.close()


def source_range_fingerprints(df, pk_src_col, pk_kind, ranges, fp_src_cols):
    """Compute per-range (count, checksum) on the transformed SOURCE dataframe, in ONE pass
    using groupBy over a range-bucket column. Mirrors target_range_fingerprint's math so
    the two fingerprints are comparable."""
    from pyspark.sql import functions as F
    if pk_kind == "uuid":
        keyc = F.lower(F.regexp_replace(F.col(pk_src_col), "-", ""))
    elif pk_kind == "text":
        keyc = F.col(pk_src_col).cast("string")
    else:
        keyc = F.col(pk_src_col).cast("long").cast("string")

    # Assign each row to a range index via a CASE ladder on the half-open bounds.
    bucket = F.lit(-1)
    for i, (lo, hi) in enumerate(ranges):
        is_top = (i == len(ranges) - 1)
        if pk_kind == "integer":
            lo_c, hi_c = F.lit(int(lo)), F.lit(int(hi))
            keyi = F.col(pk_src_col).cast("long")
            cond = (keyi >= lo_c) if is_top else ((keyi >= lo_c) & (keyi < hi_c))
        else:
            lo_c, hi_c = F.lit(str(lo)), F.lit(str(hi))
            cond = (keyc >= lo_c) if is_top else ((keyc >= lo_c) & (keyc < hi_c))
        bucket = F.when(cond, F.lit(i)).otherwise(bucket)
    dfb = df.withColumn("_rng", bucket)

    aggs = [F.count(F.lit(1)).alias("cnt")]
    if CHECKSUM_MODE == "aggregate" and fp_src_cols:
        for c in fp_src_cols:
            aggs.append(F.coalesce(F.sum(F.length(F.coalesce(F.col(c).cast("string"), F.lit("")))),
                                   F.lit(0)).alias(f"len_{c}"))
            aggs.append(F.count(F.col(c)).alias(f"nn_{c}"))
    grouped = dfb.groupBy("_rng").agg(*aggs).collect()

    out = {}
    for row in grouped:
        ri = row["_rng"]
        if ri is None or ri < 0:
            continue
        cnt = int(row["cnt"])
        if CHECKSUM_MODE == "aggregate" and fp_src_cols:
            # Interleave "len|nonnull" per column, in fp_src_cols order — MUST match the
            # target side, which emits SUM(length), COUNT(col) per column in the same order.
            fp = "|".join(sum(([str(int(row[f"len_{c}"])), str(int(row[f"nn_{c}"]))]
                               for c in fp_src_cols), []))
        else:
            fp = None
        out[ri] = (cnt, fp)
    return out


def validate_one_table(s3, entry):
    """Validate ONE table: plan ranges, compute source fingerprints (Spark, one pass) and
    target fingerprints (DSQL, parallel), compare. Returns a result dict."""
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
    span_recoverable = bool(pk_meta.get('span_recoverable'))

    if len(pk_cols) != 1 or not pk_kind or not span_recoverable:
        return {"table": label, "status": "skipped",
                "reason": "no single-column rangeable PK (cannot range-validate)"}

    pk_col = pk_cols[0]
    # Resolve the DMS/source column name for the PK (pre-rename).
    pk_src = pk_col
    for m in config.get('column_mapping', []):
        if isinstance(m, dict) and m.get('action') == 'map' \
                and (m.get('target_column', '').lower() == pk_col.lower()):
            pk_src = m.get('dms_column_name')
            break

    # Fingerprint columns (Tier-2 content check only): restrict to VARCHAR/TEXT columns,
    # because a content checksum is only comparable when the SOURCE string form equals the
    # TARGET ::text form — true for text/varchar, NOT for typed columns (numeric/timestamp/
    # uuid render differently as text on each engine, which would false-positive). Typed
    # columns are still validated by the per-range COUNT check. Bounded to K for speed.
    K = 24
    fp_cols = [c for c in target_columns
               if type_categories.get(c, 'varchar') in ('varchar', 'text')][:K]

    # ---- SOURCE: read + transform + bounds + per-range fingerprints (Spark) ----
    df = read_dms_csv(dms_s3_path, dms_has_headers)
    # match df column for pk_src (case/space-insensitive)
    matched_pk = None
    for dc in df.columns:
        if dc.lower().strip() == str(pk_src).lower().strip():
            matched_pk = dc
            break
    if matched_pk is None:
        return {"table": label, "status": "error", "reason": f"PK source col {pk_src!r} not in CSV"}

    df_t = apply_transform(df, target_columns, type_categories)
    # rename df columns to target names where mapped, so fp col names line up with DSQL
    rename = {}
    for m in config.get('column_mapping', []):
        if isinstance(m, dict) and m.get('action') == 'map':
            dmsn, tgtn = m.get('dms_column_name'), m.get('target_column')
            for dc in df_t.columns:
                if dc.lower().strip() == str(dmsn).lower().strip():
                    rename[dc] = tgtn
    for old, new in rename.items():
        if old != new and old in df_t.columns:
            df_t = df_t.withColumnRenamed(old, new)
    pk_target_name = rename.get(matched_pk, pk_col)

    mn, mx, total = spark_pk_bounds(df_t, pk_target_name, pk_kind)
    if total == 0 or mn is None:
        return {"table": label, "status": "empty", "source_rows": 0}

    if pk_kind == "uuid":
        ranges = plan_ranges_hex(mn, mx, total, VALIDATE_ROWS_PER_RANGE)
    else:
        ranges = plan_ranges(mn, mx, total, VALIDATE_ROWS_PER_RANGE) if pk_kind == "integer" \
            else [(mn, mx)]   # text: single span (portable); refine later if needed

    # Both sides MUST fingerprint the SAME ordered column set, or the checksums won't line
    # up. Use only columns present in the transformed source df (df_t), in target-column
    # order — and pass this SAME list to the target side below.
    fp_src_cols = [c for c in fp_cols if c in df_t.columns]
    src_fps = source_range_fingerprints(df_t, pk_target_name, pk_kind, ranges, fp_src_cols)

    # ---- TARGET: per-range fingerprints (DSQL, parallel) ----
    tgt_fps = {}
    lock = threading.Lock()

    def _one(i_rg):
        i, (lo, hi) = i_rg
        is_top = (i == len(ranges) - 1)
        conn = connect_dsql(autocommit=True)
        try:
            # SAME column set + order as the source side (fp_src_cols) so fingerprints align.
            cnt, fp = target_range_fingerprint(conn, dsql_schema, dsql_table, pk_col, pk_kind,
                                                lo, hi, is_top, fp_src_cols)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        with lock:
            tgt_fps[i] = (cnt, fp)

    with ThreadPoolExecutor(max_workers=max(1, MAX_QUERY_CONCURRENCY),
                            thread_name_prefix="vquery") as pool:
        futs = [pool.submit(_one, (i, rg)) for i, rg in enumerate(ranges)]
        for f in as_completed(futs):
            f.result()

    # ---- COMPARE ----
    mismatches = []
    src_total = 0
    tgt_total = 0
    for i, (lo, hi) in enumerate(ranges):
        s_cnt, s_fp = src_fps.get(i, (0, None))
        t_cnt, t_fp = tgt_fps.get(i, (0, None))
        src_total += s_cnt
        tgt_total += t_cnt
        if s_cnt != t_cnt:
            mismatches.append({"range": [str(lo), str(hi)], "type": "COUNT_DIFF",
                               "source": s_cnt, "target": t_cnt})
        elif CHECKSUM_MODE != "off" and s_fp is not None and t_fp is not None and s_fp != t_fp:
            mismatches.append({"range": [str(lo), str(hi)], "type": "CHECKSUM_DIFF",
                               "source_fp": str(s_fp)[:120], "target_fp": str(t_fp)[:120]})

    status = "match" if (not mismatches and src_total == tgt_total) else "mismatch"
    return {
        "table": label, "status": status, "ranges": len(ranges),
        "source_rows": src_total, "target_rows": tgt_total,
        "checksum_mode": CHECKSUM_MODE, "mismatches": mismatches[:50],
        "mismatch_count": len(mismatches),
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
load_status = load_full_load_status(s3_main) if REQUIRE_FULL_LOAD_DONE else {}

worklist = []
for entry in tables:
    label = f"{entry['dsql_schema']}.{entry['dsql_table']}"
    if REQUIRE_FULL_LOAD_DONE and load_status.get(label) != "done":
        print(f"  ⏭  SKIP {label} (full load status={load_status.get(label)!r})")
        continue
    worklist.append(entry)

print(f"  Validating {len(worklist)} table(s)")

results = []
_rlock = threading.Lock()


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
          + (f" ({r.get('mismatch_count', 0)} mismatch range(s), "
             f"src={r.get('source_rows')} tgt={r.get('target_rows')})"
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
skipped = [r for r in results if r["status"] in ("skipped", "empty")]

print(f"\n{'='*70}")
print("JOB 3 VALIDATION SUMMARY")
print(f"{'='*70}")
print(f"  Validated : {len(results)}")
print(f"  MATCH     : {len(matched)}")
print(f"  MISMATCH  : {len(mismatched)}")
print(f"  ERROR     : {len(errored)}")
print(f"  SKIPPED   : {len(skipped)}")
for r in mismatched:
    print(f"    ✗ {r['table']}: {r['mismatch_count']} range(s) differ "
          f"(src={r['source_rows']} tgt={r['target_rows']})")
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

job.commit()

if mismatched or errored:
    raise Exception(f"Validation found {len(mismatched)} mismatched + {len(errored)} errored "
                    f"table(s). See _validation_report.json.")
