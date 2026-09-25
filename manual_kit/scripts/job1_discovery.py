"""
Job 1 v2 (MULTI-TABLE): Schema Discovery & Column Mapping Generator
===================================================================

v2 CHANGE (additive, backward-compatible): captures PRIMARY KEY metadata per table
(columns, type_categories, numeric_scales, and a pk_kind) so Job 2 v6 can choose its
parallel-load recovery strategy per table. A SINGLE-column PK is span-recoverable when
its pk_kind is one of:
  - 'integer' : integer/bigint/smallint, OR numeric/decimal with scale == 0
  - 'uuid'    : uuid  (ranged over dash-stripped lowercase hex)
  - 'text'    : varchar/text  (ranged over byte-ordered string)
  -> Job 2 does per-range recovery (bounded, kind-appropriate DELETE + reload).
Everything else (composite / no PK / float / scaled numeric / other):
  -> whole-table blank-and-reload (or chunk fan-out) on failure.
Everything else is IDENTICAL to job1_discovery_multi_table.py. The new
metadata.primary_key block is ADDITIVE — the old Job 2 (v5) ignores unknown JSON keys,
so v2 discovery output is safe to feed to either loader.

--------------------------------------------------------------------------------
Refactored from the single-table version to process up to 170 tables in one run,
driven by a customer-maintained manifest CSV in S3.

WHAT IT DOES (per table listed in the manifest):
1. Connects to Aurora DSQL -> reads information_schema.columns for the target table
   (TARGET SCHEMA IS AUTHORITATIVE for column set AND order via ordinal_position)
   AND reads information_schema PK constraint (v2) -> records pk columns + types.
2. Reads the DMS S3 output for that table (with headers) -> gets source column names
3. Builds a column mapping JSON (reverse-mapped: walk target columns, match each to a
   DMS CSV column by name; any DMS column NOT in the target is auto-skipped)
4. Saves the per-table mapping to S3 (.../onfig/<dsql_table>_column_mapping.json)

At the end it writes a MASTER INDEX JSON (.../onfig/_manifest_index.json) that lists
every table that was successfully discovered, with the S3 path to its per-table config.
Job 2 reads this index and loops over the tables one at a time in a single run.

NOTE: The single source of truth for Job 2 is the S3 JSON (per-table config + master
index). Job 1 does NOT touch the Glue Data Catalog.

DESIGN NOTES
------------
- Column mapping direction is TARGET -> DMS (reverse). The DSQL target schema decides
  which columns exist and in what order. DMS columns that have no target (e.g. the
  DMS-added `dms_timestamp`) are recorded as `action: skip` and never loaded.
- The DMS S3 path per table is DERIVED from the manifest as:
      s3://<DMS_BUCKET>/<dms_schema>/<dms_table>/
  matching DMS S3 endpoint settings BucketFolder="" and DatePartitionEnabled=false.
- CONTINUE-ON-FAILURE: a table that is missing in DSQL, missing in S3, or otherwise
  errors is logged, skipped, and EXCLUDED from the index. The run keeps going so one
  bad table does not block the other 169. A per-table + overall summary is printed.

MANIFEST CSV FORMAT (header row required)
-----------------------------------------
    dms_schema,dms_table

    - dms_schema  REQUIRED  e.g. SRC_SCHEMA (matches the S3 folder casing)
    - dms_table   REQUIRED  e.g. MY_TABLE    (matches the S3 folder casing)

    That's it — just the two columns. Derived automatically:
      - dsql_schema = dms_schema.lower()   (SRC_SCHEMA -> src_schema)
      - dsql_table  = dms_table.lower()
"""

import sys
import io
import csv
import json
import ssl
import socket
import boto3
import pg8000
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================================================================
# CONFIGURATION — infrastructure endpoints only, no per-table column lists
# =============================================================================
DSQL_ENDPOINT = "<YOUR_CLUSTER>.dsql.<REGION>.on.aws"  # overridden at runtime via --dsql_endpoint
REGION = "us-east-1"
DSQL_USER = "admin"

# S3 bucket that DMS writes CSVs into (BucketFolder="" so tables live at bucket root
# under <schema>/<table>/). Used to derive each table's DMS CSV path from the manifest.
DMS_BUCKET = "<YOUR_S3_BUCKET>"

# Manifest CSV: the customer-maintained authoritative list of tables to process.
MANIFEST_S3_PATH = "s3://<YOUR_S3_BUCKET>/<SCHEMA>/config/table_manifest.csv"

# Where per-table mapping JSONs and the master index are written.
CONFIG_PREFIX = "s3://<YOUR_S3_BUCKET>/<SCHEMA>/config/"
# Master index consumed by Job 2.
INDEX_S3_PATH = CONFIG_PREFIX + "_manifest_index.json"


# =============================================================================
# optional Glue-arg overlay (kit: multi-task deploy). Discovery is configured via the
# module-level CONSTANTS above; a few Glue args, IF present, override them so the
# orchestrator can point ONE script at each task's own config prefix / DSQL cluster /
# DMS bucket without editing the file. All args are optional — omitting one keeps its
# default. (getResolvedOptions raises on a required arg that is missing, so we only
# request the args actually present in sys.argv.)
#
# CRITICAL: --config_prefix re-derives INDEX_S3_PATH (consumed by Job 2 — the two MUST
# match) and MANIFEST_S3_PATH (Job 1 reads the manifest from the SAME prefix). The
# manifest basename `table_manifest.csv` is fixed under the prefix.
# =============================================================================
def _apply_job1_arg_overrides():
    global CONFIG_PREFIX, INDEX_S3_PATH, MANIFEST_S3_PATH
    global DSQL_ENDPOINT, DSQL_USER, REGION, DMS_BUCKET
    optional = ["config_prefix", "dsql_endpoint", "dsql_user", "region", "dms_bucket"]
    present = [a for a in optional if f"--{a}" in sys.argv]
    if not present:
        return
    ov = getResolvedOptions(sys.argv, present)
    if "config_prefix" in ov:
        _cp = str(ov["config_prefix"]).strip()
        if _cp:
            if not _cp.endswith("/"):
                _cp += "/"
            CONFIG_PREFIX = _cp
            INDEX_S3_PATH = CONFIG_PREFIX + "_manifest_index.json"
            MANIFEST_S3_PATH = CONFIG_PREFIX + "table_manifest.csv"
            print(f"  ↪ CONFIG_PREFIX overridden -> {CONFIG_PREFIX} "
                  f"(index={INDEX_S3_PATH}, manifest={MANIFEST_S3_PATH})")
    if "dsql_endpoint" in ov:
        _de = str(ov["dsql_endpoint"]).strip()
        if _de:
            DSQL_ENDPOINT = _de
            print(f"  ↪ DSQL_ENDPOINT overridden -> {DSQL_ENDPOINT}")
    if "dsql_user" in ov:
        _du = str(ov["dsql_user"]).strip()
        if _du:
            DSQL_USER = _du
            print(f"  ↪ DSQL_USER overridden -> {DSQL_USER}")
    if "region" in ov:
        _rg = str(ov["region"]).strip()
        if _rg:
            REGION = _rg
            print(f"  ↪ REGION overridden -> {REGION}")
    if "dms_bucket" in ov:
        _db = str(ov["dms_bucket"]).strip()
        if _db:
            DMS_BUCKET = _db
            print(f"  ↪ DMS_BUCKET overridden -> {DMS_BUCKET}")


_apply_job1_arg_overrides()

# Timeout for Glue API connectivity check (seconds)
GLUE_API_TIMEOUT = 5

# v2 (multi-kind PK): a SINGLE-column PK is span-recoverable (Job 2 range path) when its
# type maps to one of three PK KINDS, each with a matching range planner in Job 2:
#   'integer' -> integer/bigint/smallint, OR numeric/decimal WITH scale == 0 (integer-
#                valued). Ranged as an integer. Excludes float and scaled numerics
#                (scale>0 would collapse the fraction; float has no exact key math).
#   'uuid'    -> uuid. Ranged over the dash-stripped-lowercase-hex (128-bit) key space.
#   'text'    -> varchar/text. Ranged over a byte-ordered string key space.
# Composite PKs (and no PK) are NOT span-recoverable -> Job 2 uses whole-table blank-and-
# reload (or chunk fan-out). No ON CONFLICT, no schema change in any case.
_INTEGER_PK_CATEGORIES = {'bigint', 'integer', 'smallint'}   # always integer-ranged
_UUID_PK_CATEGORIES = {'uuid'}
_TEXT_PK_CATEGORIES = {'varchar', 'text'}
# 'numeric' is integer-ranged ONLY when numeric_scale == 0 (checked in the block below).
# Kept for reference; membership alone is NOT sufficient — scale must be 0.
_NUMERIC_PK_CATEGORY = 'numeric'


def classify_pk_kind(type_category, numeric_scale):
    """Map a single PK column's (type_category, numeric_scale) to a Job 2 range KIND, or
    None if it is NOT span-recoverable. numeric is 'integer' ONLY when scale == 0."""
    if type_category in _INTEGER_PK_CATEGORIES:
        return 'integer'
    if type_category == _NUMERIC_PK_CATEGORY:
        # scale 0 (or unspecified-as-0) => integer-valued numeric, safe to integer-range.
        # scale > 0 => fractional; NOT rangeable by the integer planner.
        try:
            if numeric_scale is not None and int(numeric_scale) == 0:
                return 'integer'
        except (TypeError, ValueError):
            return None
        return None
    if type_category in _UUID_PK_CATEGORIES:
        return 'uuid'
    if type_category in _TEXT_PK_CATEGORIES:
        return 'text'
    return None   # float, date, boolean, json, bytea, etc. -> not span-recoverable


# Map a DSQL data_type -> internal type_category used for casting in Job 2.
def categorize_type(dt):
    if dt == 'uuid':
        return 'uuid'
    elif dt == 'boolean':
        return 'boolean'
    elif 'timestamp' in dt:
        return 'timestamptz'
    elif dt == 'bigint':
        return 'bigint'
    elif dt in ('integer', 'int'):
        return 'integer'
    elif dt == 'smallint':
        return 'smallint'
    elif dt == 'numeric' or dt.startswith('numeric') or dt == 'decimal':
        return 'numeric'
    elif dt in ('double precision', 'real', 'float'):
        return 'float'
    elif dt == 'date':
        return 'date'
    elif dt == 'bytea':
        return 'bytea'
    elif dt in ('jsonb', 'json'):
        return 'json'
    elif dt == 'text':
        return 'text'
    else:
        return 'varchar'


# =============================================================================
# Helpers
# =============================================================================
def split_s3(path):
    """s3://bucket/key... -> (bucket, key)"""
    no_scheme = path.replace("s3://", "")
    parts = no_scheme.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def read_manifest(s3_client, manifest_path):
    """
    Read the two-column manifest CSV (dms_schema, dms_table) from S3 and return a
    list of normalized table specs. dsql_schema/dsql_table are derived (not in the
    manifest).
    """
    bucket, key = split_s3(manifest_path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    text = obj['Body'].read().decode('utf-8-sig')  # utf-8-sig strips BOM if present
    reader = csv.DictReader(io.StringIO(text))

    specs = []
    for raw in reader:
        row = {(k or "").strip().lower(): (v.strip() if isinstance(v, str) else v)
               for k, v in raw.items()}
        dms_schema = row.get("dms_schema", "")
        dms_table = row.get("dms_table", "")
        if not dms_schema or not dms_table:
            if any(row.values()):
                print(f"  ⚠️ Manifest row missing dms_schema/dms_table -> ignored: {row}")
            continue

        dsql_table = dms_table.lower()
        specs.append({
            "dms_schema": dms_schema,
            "dms_table": dms_table,
            # Target schema derived by lowercasing the source schema. Postgres/DSQL
            # folds unquoted identifiers to lowercase, so SRC_SCHEMA -> src_schema.
            "dsql_schema": dms_schema.lower(),
            "dsql_table": dsql_table,
        })
    return specs


def connect_dsql():
    """Open a fresh authenticated pg8000 connection to DSQL.

    Bounded retry with backoff: a fresh IAM token is minted on EVERY attempt (no cache here),
    so a transient open failure (08006 unable-to-connect, TLS blip, throttle) self-heals on
    the next attempt instead of failing the whole discovery run. On exhaustion the last error
    is raised."""
    import time as _time
    ctx = ssl.create_default_context()
    _last = None
    for _attempt in range(1, 5):   # up to 4 attempts
        try:
            client = boto3.client("dsql", region_name=REGION)
            token = client.generate_db_connect_admin_auth_token(
                DSQL_ENDPOINT, Region=REGION, ExpiresIn=3600
            )
            return pg8000.connect(
                host=DSQL_ENDPOINT, port=5432, database="postgres",
                user=DSQL_USER, password=token, ssl_context=ctx
            )
        except Exception as e:
            _last = e
            if _attempt < 4:
                _time.sleep(min(8.0, 0.5 * (2 ** (_attempt - 1))))   # 0.5,1,2s backoff
    raise _last


def read_target_schema(conn, dsql_schema, dsql_table):
    """Return ordered target columns from DSQL information_schema (authoritative)."""
    cursor = conn.cursor()
    # Parameterized: schema/table are matched as VALUES in information_schema (not SQL
    # identifiers), so pg8000 placeholders prevent injection from manifest-derived names.
    cursor.execute(
        """
        SELECT column_name, data_type, ordinal_position, character_maximum_length,
               is_nullable, column_default, numeric_scale, numeric_precision
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (dsql_schema, dsql_table),
    )
    target_columns = []
    for row in cursor.fetchall():
        (col_name, data_type, ordinal_pos, char_max_len, is_nullable,
         col_default, num_scale, num_precision) = row
        target_columns.append({
            "name": col_name,
            "data_type": data_type,
            "ordinal_position": ordinal_pos,
            "max_length": char_max_len,
            "is_nullable": is_nullable,          # 'YES' / 'NO'
            "column_default": col_default,       # None if no default
            # v2 (multi-kind PK): numeric_scale lets Job 2 range ONLY integer-valued
            # numerics (scale 0). A scaled numeric (scale>0) or NULL-scale float is NOT
            # span_recoverable — the range path would collapse/overflow a fractional key.
            "numeric_scale": num_scale,          # int for numeric/decimal; None otherwise
            "numeric_precision": num_precision,  # informational
        })
    cursor.close()
    return target_columns


def read_primary_key(conn, dsql_schema, dsql_table):
    """v2: Return the target table's PRIMARY KEY column names in key order (or [] if
    none). Reads information_schema.table_constraints + key_column_usage. Parameterized.
    Best-effort: any error -> treat as "no PK" (caller falls back to whole-table
    reload), never fails the table's discovery over PK detection."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.constraint_schema = kcu.constraint_schema
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (dsql_schema, dsql_table),
        )
        rows = cursor.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"    ⚠️ PK detection failed for {dsql_schema}.{dsql_table} "
              f"({type(e).__name__}: {e}); treating as NO PK (whole-table reload).")
        return []
    finally:
        cursor.close()


def build_primary_key_block(pk_columns, target_columns, type_categories):
    """v2: Build the metadata.primary_key block that Job 2 v6 branches on.

    span_recoverable is TRUE for a SINGLE-column PK whose kind is one of:
      - 'integer' : integer/bigint/smallint, OR numeric/decimal with scale == 0
      - 'uuid'    : uuid
      - 'text'    : varchar/text
    Each kind has a matching range planner in Job 2 (integer / hex / byte-string). Job 2
    recovers a failed range per-partition via a bounded, kind-appropriate DELETE + reload.
    Composite / no PK / float / scaled-numeric / other types => span_recoverable False =>
    Job 2 uses whole-table blank-and-reload (or chunk fan-out). No schema change, no ON
    CONFLICT in any case. pk_kind is recorded so Job 2 dispatches the correct planner.
    """
    # Resolve each PK column's declared type, type_category, and numeric_scale.
    by_name = {c['name']: c for c in target_columns}
    pk_data_types = []
    pk_type_categories = []
    pk_numeric_scales = []
    for c in pk_columns:
        col = by_name.get(c) or by_name.get(c.lower()) or {}
        pk_data_types.append(col.get('data_type'))
        pk_type_categories.append(type_categories.get(col.get('name', c)))
        pk_numeric_scales.append(col.get('numeric_scale'))

    is_single = len(pk_columns) == 1
    # pk_kind is meaningful ONLY for a single-column PK (the only shape we range).
    pk_kind = None
    if is_single:
        pk_kind = classify_pk_kind(pk_type_categories[0], pk_numeric_scales[0])
    span_recoverable = bool(is_single and pk_kind is not None)
    # Back-compat flag some earlier consumers/logs read; now means "integer-ranged".
    is_numeric = bool(span_recoverable and pk_kind == 'integer')

    return {
        "columns": pk_columns,                    # [] if no PK
        "column_count": len(pk_columns),
        "data_types": pk_data_types,              # declared DSQL types, key order
        "type_categories": pk_type_categories,    # internal categories, key order
        "numeric_scales": pk_numeric_scales,      # scale per PK col (None if n/a)
        "is_single_column": is_single,
        "is_numeric": is_numeric,
        # pk_kind: 'integer' | 'uuid' | 'text' | None. Job 2 v6 uses this to pick the
        # range planner + range-filter + keyset-DELETE form for the span path.
        "pk_kind": pk_kind,
        # The flag Job 2 v6 gates on: single-column PK of a rangeable kind => per-range
        # recovery; otherwise whole-table blank-and-reload / chunk fan-out.
        "span_recoverable": span_recoverable,
    }


def s3_prefix_has_objects(s3_client, bucket, prefix):
    """True if at least one object exists under prefix (i.e. DMS wrote files)."""
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def build_mapping_for_table(spec, target_columns, dms_columns, pk_columns):
    """Reverse-map target columns to DMS CSV columns; skip DMS cols not in target.
    v2: also embeds the metadata.primary_key block (from pk_columns)."""
    dsql_schema = spec['dsql_schema']
    dsql_table = spec['dsql_table']

    # Lookups
    dms_columns_lower = {c.lower().strip(): c for c in dms_columns}
    target_column_lookup = {c['name'].lower(): c for c in target_columns}

    type_categories = {c['name']: categorize_type(c['data_type']) for c in target_columns}

    column_mapping = []
    target_column_names = []
    missing_in_dms = []

    # Walk TARGET columns in ordinal order — target is authoritative for order.
    for target_col in target_columns:
        target_name = target_col['name']
        target_lower = target_name.lower()
        if target_lower in dms_columns_lower:
            dms_original_name = dms_columns_lower[target_lower]
            dms_pos = list(dms_columns).index(dms_original_name)
            column_mapping.append({
                "dms_position": dms_pos,
                "dms_column_name": dms_original_name,
                "action": "map",
                "target_column": target_name,
                "target_type": target_col['data_type'],
                "type_category": type_categories[target_name],
                "ordinal_position": target_col['ordinal_position'],
                "max_length": target_col['max_length'],
            })
            target_column_names.append(target_name)
        else:
            missing_in_dms.append(target_name)

    # Any DMS column with no matching target is auto-skipped (reverse-map skip).
    skipped_entries = []
    for dms_pos, dms_col_name in enumerate(dms_columns):
        if dms_col_name.lower().strip() not in target_column_lookup:
            skipped_entries.append({
                "dms_position": dms_pos,
                "dms_column_name": dms_col_name,
                "action": "skip",
                "reason": f"No matching column in {dsql_schema}.{dsql_table}",
            })

    column_mapping = column_mapping + skipped_entries
    unmatched_columns = [e['dms_column_name'] for e in skipped_entries]

    # v2: PK block. NOTE: a PK column that is MISSING from the DMS CSV (not in
    # target_column_names) cannot be used for span recovery — flag that so Job 2 falls
    # back to whole-table reload rather than DELETE-ing on a column it never loads.
    pk_block = build_primary_key_block(pk_columns, target_columns, type_categories)
    # Case-insensitive membership: target_column_names carries DSQL's stored case and
    # pk_columns carries the PK-constraint's case; Postgres/DSQL fold unquoted
    # identifiers to lowercase so they normally match, but compare lower() to be safe
    # (a case mismatch must not silently downgrade a numeric PK to whole-table reload).
    _loaded_lower = {c.lower() for c in target_column_names}
    pk_in_load = bool(pk_columns) and all(c.lower() in _loaded_lower for c in pk_columns)
    if pk_block["span_recoverable"] and not pk_in_load:
        pk_block["span_recoverable"] = False
        pk_block["span_recoverable_note"] = (
            "PK column not present in the loaded (mapped) columns — cannot DELETE-by-span; "
            "falling back to whole-table reload.")

    config = {
        "metadata": {
            "generated_by": "cns-schema-discovery-job-multitable-v2",
            "dsql_endpoint": DSQL_ENDPOINT,
            "dsql_schema": dsql_schema,
            "dsql_table": dsql_table,
            "dms_schema": spec['dms_schema'],
            "dms_table": spec['dms_table'],
            "dms_s3_path": f"s3://{DMS_BUCKET}/{spec['dms_schema']}/{spec['dms_table']}/",
            "dms_column_count": len(dms_columns),
            "dms_has_headers": True,
            "target_column_count": len(target_column_names),
            "mapped_column_count": len(target_column_names),
            "skipped_column_count": len(skipped_entries),
            "unmatched_dms_columns": unmatched_columns,
            "missing_target_columns": missing_in_dms,
            "column_order": "DSQL ordinal_position (target schema is authoritative)",
            # v2 ADDITIVE: primary-key metadata for Job 2 v6 recovery-mode selection.
            "primary_key": pk_block,
        },
        "target_columns": target_column_names,
        "type_categories": type_categories,
        "column_mapping": column_mapping,
        "skip_columns": unmatched_columns,
    }
    return config, unmatched_columns, missing_in_dms


# =============================================================================
# MAIN
# =============================================================================
print("=" * 70)
print("JOB 1 v2 (MULTI-TABLE): Schema Discovery & Column Mapping Generator")
print("=" * 70)

s3_client = boto3.client('s3', region_name=REGION)

# ---- Load manifest --------------------------------------------------------
print(f"\nLoading manifest: {MANIFEST_S3_PATH}")
specs = read_manifest(s3_client, MANIFEST_S3_PATH)
print(f"  Manifest tables: {len(specs)}")
if not specs:
    raise Exception("No tables in manifest — nothing to do.")

# ---- Open one DSQL connection reused for all tables ----------------------
dsql_conn = connect_dsql()


def read_schema_and_pk_resilient(dsql_schema, dsql_table):
    """
    Read the target schema AND primary key, reconnecting once if the long-lived DSQL
    connection has dropped mid-loop (network blip / idle timeout). Rebinds the
    module-level dsql_conn so subsequent tables reuse the healthy connection.
    Returns (target_columns, pk_columns).
    """
    global dsql_conn
    try:
        cols = read_target_schema(dsql_conn, dsql_schema, dsql_table)
        pk = read_primary_key(dsql_conn, dsql_schema, dsql_table)
        return cols, pk
    except Exception as e:
        # pg8000 raises InterfaceError/OperationalError on a dead socket; boto3
        # token expiry can also surface here. Reconnect once and retry.
        print(f"  ↻ DSQL connection error ({type(e).__name__}: {e}) — reconnecting once...")
        try:
            dsql_conn.close()
        except Exception:
            pass
        dsql_conn = connect_dsql()
        cols = read_target_schema(dsql_conn, dsql_schema, dsql_table)
        pk = read_primary_key(dsql_conn, dsql_schema, dsql_table)
        return cols, pk

index_entries = []
succeeded, skipped_missing_dsql, skipped_missing_s3, failed = [], [], [], []
skipped_duplicate = []
seen_dsql_tables = {}  # "schema.table" -> first manifest row index that claimed it

for i, spec in enumerate(specs, start=1):
    dms_schema = spec['dms_schema']
    dms_table = spec['dms_table']
    dsql_schema = spec['dsql_schema']
    dsql_table = spec['dsql_table']
    dms_s3_path = f"s3://{DMS_BUCKET}/{dms_schema}/{dms_table}/"

    print(f"\n[{i}/{len(specs)}] {dms_schema}.{dms_table} -> {dsql_schema}.{dsql_table}")

    # Fix #5: guard against two manifest rows targeting the same DSQL table, which
    # would overwrite the first config JSON and produce a misleading index entry.
    dsql_key = f"{dsql_schema}.{dsql_table}"
    if dsql_key in seen_dsql_tables:
        print(f"  ⚠️ SKIP: duplicate target {dsql_key} (already produced by row "
              f"{seen_dsql_tables[dsql_key]}). Fix the manifest.")
        skipped_duplicate.append(dsql_key)
        continue
    seen_dsql_tables[dsql_key] = i

    try:
        # 1) Target schema + PK from DSQL (authoritative; reconnects once if dropped)
        target_columns, pk_columns = read_schema_and_pk_resilient(dsql_schema, dsql_table)
        if not target_columns:
            print(f"  ⚠️ SKIP: no columns in DSQL {dsql_schema}.{dsql_table} (table missing?)")
            skipped_missing_dsql.append(f"{dsql_schema}.{dsql_table}")
            continue
        print(f"  Target columns: {len(target_columns)} (ordered by ordinal_position)")
        if pk_columns:
            print(f"  Primary key   : {pk_columns}")
        else:
            print("  Primary key   : (none) -> Job 2 will use whole-table reload")

        # 2) Confirm DMS wrote CSVs for this table.
        bucket, prefix = split_s3(dms_s3_path)
        _has_fullload = s3_prefix_has_objects(s3_client, bucket, prefix)
        _empty_at_discovery = not _has_fullload

        if _empty_at_discovery:
            # EMPTY-SOURCE-TABLE CASE (legit 0 rows at full-load time). DMS writes NO
            # full-load file, but the table EXISTS in source + target and may receive rows
            # during the CDC window. Historically Job1 dropped it here (continue) -> it was
            # absent from the manifest -> the CDC job never scanned it -> any CDC it later
            # got was silently missed. Instead we INCLUDE it: build the mapping from the
            # DSQL target schema (DMS uses AddColumnName=true with the same lowercased
            # column names for full-load AND cdc, so the target column names ARE the DMS/CDC
            # header names), mark it empty (full_load_rows=0), and continue to steps 4/5 so
            # it lands in the manifest. Full-load load is a no-op; CDC applies later inserts.
            print(f"  ℹ️ no full-load S3 objects under {dms_s3_path} — treating as "
                  f"EMPTY-AT-DISCOVERY (0 rows). Including in manifest for CDC anyway.")
            dms_columns = [c['name'] for c in target_columns]
        else:
            # 3) Read DMS CSV headers. recursiveFileLookup=true so a DMS PARALLEL FULL LOAD
            # layout (partitions-auto/list/ranges -> nested .../table/<partition>/LOAD*.csv)
            # is discovered too; Spark is non-recursive by default and would otherwise read
            # zero files -> zero columns. Matches Job 2's CSV_READ_OPTIONS + the recursive
            # s3 lister. Harmless for the flat single-/multi-file layout.
            # pathGlobFilter="LOAD*.csv": read ONLY DMS full-load files for the header. CDC
            # files (timestamp-named, leading `Op`) share this flat folder — AddColumnName=
            # true forbids CdcPath/DatePartition per the DMS S3-target docs — so without this
            # discovery could sample a CDC header and mis-map every column. Full-load files
            # are always LOAD*.csv; the glob excludes CDC by name at any depth.
            df_sample = (spark.read
                         .option("header", "true")
                         .option("inferSchema", "false")
                         .option("recursiveFileLookup", "true")
                         .option("pathGlobFilter", "LOAD*.csv")
                         .csv(dms_s3_path))
            dms_columns = df_sample.columns
            print(f"  DMS columns: {len(dms_columns)}")
            # Same CDC-contamination backstop as the loader: if the sampled header leads with
            # an 'Op' column, a CDC file slipped in — fail loudly rather than mis-map.
            if dms_columns and str(dms_columns[0]).strip().lower() == "op":
                raise Exception(
                    f"CDC CONTAMINATION at discovery for {dsql_schema}.{dsql_table}: sampled "
                    f"header leads with 'Op' (a CDC file was read). Full-load files are "
                    f"LOAD*.csv; remove/relocate CDC output under {dms_s3_path} and re-run.")

        # 4) Build reverse mapping (target-driven, skip DMS cols not in target)
        config, unmatched, missing_in_dms = build_mapping_for_table(
            spec, target_columns, dms_columns, pk_columns)
        pkb = config['metadata']['primary_key']
        print(f"  Mapped: {config['metadata']['mapped_column_count']}, "
              f"skipped: {config['metadata']['skipped_column_count']}, "
              f"pk_span_recoverable: {pkb['span_recoverable']} "
              f"(pk_kind={pkb.get('pk_kind')})")
        if unmatched:
            print(f"    ⚠️ DMS cols with no target (skipped): {unmatched}")
        if missing_in_dms:
            # These target columns have no source in the DMS CSV. Job 2's INSERT will
            # omit them, so DSQL applies the column DEFAULT (or NULL). If any such
            # column is NOT NULL with no DEFAULT, the INSERT will fail at runtime —
            # so fail-fast here at discovery time with a clear message.
            nn_lookup = {c['name']: c for c in target_columns}
            hard_blockers = []
            for mc in missing_in_dms:
                info = nn_lookup.get(mc, {})
                if info.get('is_nullable') == 'NO' and not info.get('column_default'):
                    hard_blockers.append(mc)
            print(f"    ⚠️⚠️ WARNING: {len(missing_in_dms)} target column(s) MISSING from DMS CSV "
                  f"-> will be DEFAULT/NULL in DSQL: {missing_in_dms}")
            if hard_blockers:
                raise Exception(
                    f"NOT NULL columns with no DEFAULT are missing from the DMS CSV: "
                    f"{hard_blockers}. Job 2 INSERT would fail for {dsql_schema}.{dsql_table}. "
                    f"Fix the DMS source/mapping or add a DEFAULT in DSQL."
                )

        # 5) Save per-table config JSON
        per_table_config_s3 = CONFIG_PREFIX + f"{dsql_table}_column_mapping.json"
        cfg_bucket, cfg_key = split_s3(per_table_config_s3)
        s3_client.put_object(
            Bucket=cfg_bucket, Key=cfg_key,
            Body=json.dumps(config, indent=2).encode('utf-8'),
            ContentType='application/json',
        )
        print(f"  ✓ Config saved: {per_table_config_s3}")

        index_entries.append({
            "dms_schema": dms_schema,
            "dms_table": dms_table,
            "dsql_schema": dsql_schema,
            "dsql_table": dsql_table,
            "config_s3_path": per_table_config_s3,
            "dms_s3_path": dms_s3_path,
            "target_column_count": config['metadata']['target_column_count'],
            "dms_column_count": config['metadata']['dms_column_count'],
            # v2 ADDITIVE: surface PK recovery hint in the index too (handy for Job 2
            # planning without opening every per-table config).
            "pk_columns": config['metadata']['primary_key']['columns'],
            "pk_span_recoverable": config['metadata']['primary_key']['span_recoverable'],
            "pk_kind": config['metadata']['primary_key'].get('pk_kind'),
            # EMPTY-AT-DISCOVERY: no full-load file (legit 0 source rows). full_load_rows=0
            # so plan-split sizes it as tiny; empty_at_discovery lets the full-load status be
            # marked 'done' immediately (nothing to load) so the CDC gate opens and any CDC
            # rows arriving later are applied instead of waiting on a load that never comes.
            "empty_at_discovery": _empty_at_discovery,
            "full_load_rows": 0 if _empty_at_discovery else None,
        })
        succeeded.append(f"{dsql_schema}.{dsql_table}")

    except Exception as e:
        # CONTINUE-ON-FAILURE: log and move on; this table is excluded from the index.
        print(f"  ✗ ERROR discovering {dms_schema}.{dms_table}: {e}")
        failed.append(f"{dms_schema}.{dms_table}: {e}")

# Close the reused DSQL connection. Guard it: if the last table's reconnect failed,
# dsql_conn may be dead and close() could raise — which would crash BEFORE the master
# index is written, losing all discovery progress. Never let cleanup block the index.
try:
    dsql_conn.close()
except Exception:
    pass

# ---- Write master index (consumed by Job 2) ------------------------------
index_doc = {
    "metadata": {
        "generated_by": "cns-schema-discovery-job-multitable-v2",
        "manifest_s3_path": MANIFEST_S3_PATH,
        "config_prefix": CONFIG_PREFIX,
        "dms_bucket": DMS_BUCKET,
        "region": REGION,
        "dsql_endpoint": DSQL_ENDPOINT,
        "total_in_manifest": len(specs),
        "succeeded": len(succeeded),
        "skipped_missing_dsql": len(skipped_missing_dsql),
        "skipped_missing_s3": len(skipped_missing_s3),
        "skipped_duplicate": len(skipped_duplicate),
        "failed": len(failed),
    },
    "tables": index_entries,
}
idx_bucket, idx_key = split_s3(INDEX_S3_PATH)
s3_client.put_object(
    Bucket=idx_bucket, Key=idx_key,
    Body=json.dumps(index_doc, indent=2).encode('utf-8'),
    ContentType='application/json',
)

# ---- Summary --------------------------------------------------------------
print(f"\n{'='*70}")
print("✅ JOB 1 v2 COMPLETE — DISCOVERY SUMMARY")
print(f"{'='*70}")
print(f"  Manifest tables           : {len(specs)}")
print(f"  Discovered OK             : {len(succeeded)}")
print(f"  Skipped (missing in DSQL) : {len(skipped_missing_dsql)} {skipped_missing_dsql or ''}")
print(f"  Skipped (missing in S3)   : {len(skipped_missing_s3)} {skipped_missing_s3 or ''}")
print(f"  Skipped (duplicate target): {len(skipped_duplicate)} {skipped_duplicate or ''}")
print(f"  Failed                    : {len(failed)}")
for f in failed:
    print(f"      - {f}")
print(f"  Master index              : {INDEX_S3_PATH}")
print(f"{'='*70}")
print("\n  Next step: Run Job 2 v6 (Data Load) — it reads the master index + PK metadata.")

# Guard job.commit() with a connectivity check — without a Glue VPC Interface
# Endpoint this call HANGS for 10+ minutes. (Same guard as Job 2.)
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
    print("  ⚠️ Skipping job.commit() — Glue API not reachable (configs are already written to S3).")
