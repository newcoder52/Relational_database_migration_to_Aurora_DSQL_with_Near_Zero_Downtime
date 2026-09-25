"""
Job 2 v16 (MULTI-TABLE): Data Load (Oracle DMS -> Aurora DSQL) — PySpark, driver-side pg8000.

Reads the master index (CONFIG_PREFIX + _manifest_index.json) written by Job 1 and loads
each table into Aurora DSQL in the same Glue job, driver-side via pg8000.

============================================================================================
V16 CHANGE (vs v15) — DEDICATED FILE-PARALLELISM KNOB for the orchestrator's 250MB/30-file
model. v15's per-file fan-out concurrency was min(PER_TABLE_WRITE_CONCURRENCY, num_files)
(default 10), and the stale INTRA_TABLE_WRITERS(=4) constant was unused in that path — so a
big table capped at ~10 parallel files even when the customer wanted 30. v16 adds an
EXPLICIT, independent MAX_FILES_IN_PARALLEL knob (default 30, arg --max_files_in_parallel)
that governs ONLY the per-file fan-out width, decoupled from the per-writer knob, so a
7.5 GB table split into 30x250MB files loads all 30 in parallel. The intra-job global writer
semaphore default (MAX_WRITE_CONCURRENCY=150) is ALREADY >= 30, so it does not throttle a
single table's 30-way fan-out (a per-table warn fires if an operator lowers it below the
fan-out). NOTHING else changes: all guards, resume, validation, casts, uuid/timestamp
normalization, and the no-loss/no-dup invariants are byte-for-byte identical to v15.

DRIVER SIZING (IMPORTANT): the load runs DRIVER-SIDE (pg8000). 30 concurrent 250MB file
reads each pull a whole Spark partition to the driver via toLocalIterator, so peak driver
working set is roughly 30 x (250MB parsed + chunk/pg8000 buffers) ≈ tens of GB. v16 does NOT
memory-clamp the fan-out (the operator/orchestrator sets it deliberately), so run the
big-table loaders on a LARGE driver (G.4X ~64GB floor, G.8X ~128GB comfortable) or lower
--max_files_in_parallel. An undersized driver will OOM rather than silently throttle — by
design, since the customer chose "go big, don't throttle." A soft driver-memory WARNING is
logged at fan-out time (see load_one_table_chunked) so an undersized driver is flagged.
============================================================================================

Per table:
  1. Read the table's column-mapping config from S3 (Job 1 output).
  2. Read the DMS CSV from S3 with Spark (multiLine-safe, header-based).
  3. Apply the mapping (rename/skip/type-cast) in Spark.
  4. Bulk-insert into DSQL.
  5. Validate, then record the outcome.

LOADING MODEL:
  - Small tables (CSV bytes < LARGE_TABLE_BYTES_THRESHOLD): single whole-table stream.
  - Large tables: PER-FILE parallel load — each CSV part-file is one worker (concurrency
    = min(MAX_FILES_IN_PARALLEL, num_files) in v16, globally capped by MAX_WRITE_CONCURRENCY).
    Parallelism is at the FILE level; there is no PK-range path. With DMS MaxFileSize=250MB
    and MAX_FILES_IN_PARALLEL=30, a table loads up to ~7.5 GB (30 files) concurrently.

INSERTS: LITERAL_INSERT_MODE inlines each value as a safely-escaped SQL literal (no bind
parameters), so a statement is not limited by the PostgreSQL 32767 bind-parameter cap and
wide tables use up to DSQL_MAX_ROWS_PER_TXN (3000) rows/txn instead of ~461. An ambiguous
XX000-at-commit is handled by a self-correcting re-insert (a PK 23505 on retry = already
committed -> counted, not duplicated).

RESUME (per file, S3-tracked): each file is marked started/done in a per-table S3 status
file. On a re-run, done files are skipped and only unfinished files reload:
  - PK table: the failed file reloads with a per-chunk commit-probe (already-committed
    chunks are skipped -> no duplicates).
  - No-PK table: whole-table reblank + full reload (no per-file discriminator in the target).
Table-level outcomes are also tracked (STATUS_S3_PATH); a table marked "done" is skipped on
re-run (delete the status file or set RESET_STATUS=True to force a full reload).

VALIDATION: no-loss is EXACT and per-file — every file worker asserts parsed-rows ==
committed-rows (rows_read == total_written), so a lost/short file fails loudly (always-on,
free). The whole-table post-load check then confirms with an EXACT COUNT(*) == loaded
(viable on DSQL: ~60s for 30M); only if COUNT(*) itself exceeds the txn limit does it fall
back to a pg_class.reltuples INFORMATIONAL estimate (too noisy — 3.6%-18% off at 30M — to
gate on). No-dup is guaranteed by the target PRIMARY KEY.
UUID-format and auth-token-leak scans run when they can complete and warn (not fail) if the
scan exceeds DSQL limits.

DSQL CONSTRAINTS: no TRUNCATE (blanking is batched DELETE), per-transaction limits (~3000
rows / ~5 min), 32767 bind-parameter cap per statement, no ctid. Targets must be created
(with inline PKs) and empty at the start of a fresh load; cleanup is customer-owned.

CONTINUE-ON-FAILURE: a failed table is logged and the loop continues; a per-table + overall
summary is printed and the job exits non-zero if any table failed.
"""

import sys
import time
import json
import ssl
import socket
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import pg8000
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, lit, concat, substring, lower, trim, upper,
    coalesce, to_timestamp, date_format, regexp_replace
)


# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Pin the Spark SQL session timezone to UTC so to_timestamp() on an offset-bearing value
# (…±HH:MM) and date_format() emit the true UTC instant (not the machine-local rendering).
# Required for correct TIMESTAMP WITH TIME ZONE handling — see normalize_timestamp.
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def utc_now_iso():
    """Timezone-aware UTC timestamp as ISO string with trailing 'Z'."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

# =============================================================================
# CONFIGURATION
# =============================================================================
DSQL_ENDPOINT = "REPLACE_ME.dsql.us-east-1.on.aws"  # overridden at runtime from config; placeholder default
REGION = "us-east-1"
DSQL_USER = "admin"
DSQL_DATABASE = "postgres"

# Config prefix in S3 (Job 1 writes the master index + per-table mapping JSONs here,
# and Job 2 reads them). SINGLE SOURCE OF TRUTH — must match Job 1's config_prefix.
CONFIG_PREFIX = "s3://<YOUR_S3_BUCKET>/<SCHEMA>/config/"  # overridden at runtime via --config_prefix

# Master index written by Job 1 — the list of tables to load.
INDEX_S3_PATH = CONFIG_PREFIX + "_manifest_index.json"

# Resume status file (JSON in S3).
STATUS_S3_PATH = CONFIG_PREFIX + "_load_status.json"
RESET_STATUS = False

# =============================================================================
# AUTO-REBLANK-ON-RESUME (opt-in)
# =============================================================================
# When True: a table THIS pipeline owns (in this manifest as not-done, or carrying an
# "in_progress"/"failed" marker in the S3 status file) is auto-reblanked (batched
# whole-table DELETE) and reloaded — or per-file-resumed — on a re-run instead of failing
# the empty-gate. No-dup/no-loss: the reblank DELETEs the whole target first, then the
# source-count gate (Spark CSV COUNT == committed) brings it to exactly the source count.
# Safety invariant: it only fires for a table the pipeline was pointed at; an untouched
# customer table still hits the empty-gate and is refused if non-empty. Set False to
# restore strict "refuse if non-empty".
AUTO_REBLANK_ON_RESUME = True  # per-file resume ON (skip done files; redo failed file — PK via commit-probe, no-PK via whole-table reblank)

GLUE_API_TIMEOUT = 5

# =============================================================================
# PARALLEL-LOAD KNOBS (all overridable via Glue args; see the arg-parsing block below)
# =============================================================================
# write_mode: "driver_threads" (default) | "executors" (opt-in, probe-gated).
V6_WRITE_MODE = "driver_threads"
# ROUTING GATE (cheap, automatic): a table takes the per-file parallel path only if the
# SUM of its DMS CSV object sizes in S3 is >= this many bytes. S3 object size comes free
# from list_objects_v2 (no data scanned), so small/normal tables are gated out at
# near-zero cost and take the single-stream whole-table path.
LARGE_TABLE_BYTES_THRESHOLD = 10 * 1024 * 1024  # 10 MiB (force per-file path for DMS_SAMPLE test)
# Rows per range slice. CLAMPED to [1, 100000]. A range is a parallelism/recovery unit,
# NOT a transaction (the chunker still commits <=3000 rows/txn under DSQL's 5-min cap).
TARGET_ROWS_PER_PARTITION = 100_000
PARTITION_ROWS_MIN = 1
PARTITION_ROWS_MAX = 100_000
# Max distinct text keys sampled to compute quantile range boundaries (plan_ranges_text).
# Affects boundary BALANCE only for high-cardinality text PKs, not correctness.
TEXT_SAMPLE_MAX = 10000
# TWO-LEVEL WRITE CONCURRENCY:
#   PER_TABLE_WRITE_CONCURRENCY = inner cap: max concurrent writers for ONE table.
#   MAX_WRITE_CONCURRENCY = GLOBAL cap across ALL parallel tables (shared
#     _DSQL_RANGE_WRITER_SEM). Keep >= MAX_PARALLEL_TABLES * PER_TABLE_WRITE_CONCURRENCY
#     to let every table use its full per-table concurrency. Both overridable via args.
PER_TABLE_WRITE_CONCURRENCY = 10
MAX_WRITE_CONCURRENCY = 150     # global cap: 15 tables x 10 per-table
# SINGLE-TABLE BOOST: with exactly ONE table to load there is no cross-table contention
# for the global semaphore, so the lone table's inner writers use this (higher) value.
# Only raised, never lowered. Set equal to PER_TABLE_WRITE_CONCURRENCY to disable.
SINGLE_TABLE_WRITE_CONCURRENCY = 10
# Master switch: if False, every table takes the single-stream whole-table path.
V6_PARALLEL_ENABLED = True

# PER-TABLE FORCE-SINGLE-STREAM OVERRIDE (default empty). Pin a specific parse-bound large
# table to the single-stream path WITHOUT disabling parallelism globally, via
# --force_v5_tables "schema.table,schema.table2". Matched case-insensitively.
FORCE_V5_TABLES = set()

# Per-chunk progress logging. OFF by default (the per-WORKER summary line always prints).
# When on, log one [CHUNK] line every VERBOSE_CHUNK_EVERY committed chunks per worker.
# Enable via --verbose_chunks true. Purely logging — no effect on the load.
VERBOSE_CHUNKS = False
VERBOSE_CHUNK_EVERY = 10

# ---- PER-FILE FAN-OUT (large tables) ----------------------------------------
# Large tables load via the per-file path: the table's S3 part-files are split across a
# few workers (each reads only its files via load_one_table(file_subset=...)). File
# subsets are DISJOINT so no row loads twice; partition is by S3 part-file, never by key
# value. Workers share the global _DSQL_RANGE_WRITER_SEM and the inner pool is memory-
# clamped (effective_inner_concurrency). On a memory-constrained driver (~1 GB part-files),
# lower MAX_WRITE_CONCURRENCY or set --chunk_fanout_enabled false for the single-stream path.
V6_CHUNK_FANOUT_ENABLED = True
# Max fan-out workers PER large table. The GLOBAL semaphore (MAX_WRITE_CONCURRENCY) is the
# real cap across all tables; this only bounds how many files ONE table splits into.
# NOTE (v16): INTRA_TABLE_WRITERS is LEGACY/UNUSED in the per-file path — the per-file
# fan-out width is now governed by MAX_FILES_IN_PARALLEL (below), not this constant. Kept
# only to avoid touching unrelated references; do not rely on it.
INTRA_TABLE_WRITERS = 4

# ---- V16: PER-TABLE FILE-PARALLELISM (the 250MB / 30-files-in-parallel model) ----------
# The per-file fan-out for ONE large table opens this many CSV part-files CONCURRENTLY
# (bounded by the actual number of files to load). Decoupled from PER_TABLE_WRITE_CONCURRENCY
# so the orchestrator can drive true file-level parallelism: with DMS MaxFileSize=250MB, a
# 7.5 GB table = 30 files -> all 30 load at once. Override via --max_files_in_parallel.
# The intra-job global writer cap (MAX_WRITE_CONCURRENCY) must be >= this or it throttles a
# single table's fan-out (see the raised default below). Overridable via Glue arg.
MAX_FILES_IN_PARALLEL = 30

# GLOBAL DSQL-writer semaphore. Caps TOTAL concurrent DSQL writers across ALL tables at
# MAX_WRITE_CONCURRENCY regardless of how many tables are in flight, so
# MAX_PARALLEL_TABLES x per-table-workers can never exceed the global budget. Created
# after the arg overlay (so MAX_WRITE_CONCURRENCY is final).
_DSQL_RANGE_WRITER_SEM = None

# -----------------------------------------------------------------------------
# EMPTY-TARGET SAFETY BARRIER (per Glue attempt).
# INVARIANT: the target table MUST be empty at the start of EVERY Glue attempt. A Glue
# retry starts a fresh JVM (this registry is empty again), so a partially-loaded table
# must re-pass the empty gate — no cross-attempt auto-resume without the opt-in feature.
# Enforcement is STRUCTURAL, not by call-ordering convention:
#   - assert_empty_or_register() is the ONE place the empty check happens; it records
#     the table in _EMPTY_VERIFIED for this attempt.
#   - blank_pk_range() REFUSES to run unless the table is already in _EMPTY_VERIFIED,
#     so a range DELETE is impossible against an unverified (possibly customer-owned) target.
_EMPTY_VERIFIED = set()          # {"schema.table"} proven empty in THIS attempt
_EMPTY_VERIFIED_LOCK = threading.Lock()

# labels ("schema.table", lowercased) of tables PREVIOUSLY ATTEMPTED by this pipeline
# (status "in_progress"/"failed", NOT "done"), eligible for auto-reblank-on-resume when
# AUTO_REBLANK_ON_RESUME is on. Populated ONCE by the main loop from the S3 status file
# before any table loads. A table with no prior marker is NOT in this set and is refused
# if non-empty, so an untouched customer table is never auto-deleted.
_RESUME_ELIGIBLE = set()

def _resume_ok_for(dsql_schema, dsql_table):
    """True if this table is eligible for auto-reblank-on-resume (previously attempted)."""
    return f"{dsql_schema}.{dsql_table}".lower() in _RESUME_ELIGIBLE


# =============================================================================
# Recovery-mode selection + PK range planner (pure functions, no Spark/DSQL).
# =============================================================================
def select_recovery_mode(config):
    """Given a per-table config dict (Job 1 v2 output), decide the recovery mode.

    Returns 'pk_span' only when Job 1 marked the table span_recoverable (single numeric
    PK present in the loaded columns). Otherwise 'whole_table' (blank-and-reload on any
    failure) — lossless, no ON CONFLICT, no schema change. Missing/old config (no
    primary_key block, e.g. produced by Job 1 v1) safely -> 'whole_table'.
    """
    pk = (config or {}).get("metadata", {}).get("primary_key") or {}
    return "pk_span" if pk.get("span_recoverable") is True else "whole_table"


def pk_column_of(config):
    """The single PK column name to range on (only meaningful when mode == 'pk_span')."""
    pk = (config or {}).get("metadata", {}).get("primary_key") or {}
    cols = pk.get("columns") or []
    return cols[0] if len(cols) == 1 else None


def clamp_partition_rows(target_rows_per_partition):
    """Clamp to [1, 100000]. target_rows_per_partition is the PARTITION size (a parallel
    writer's slice), NOT the DSQL transaction size (that stays <=3000 rows, enforced by
    the loader's chunker under the 5-min txn cap)."""
    try:
        v = int(target_rows_per_partition)
    except (TypeError, ValueError):
        v = PARTITION_ROWS_MAX
    return max(PARTITION_ROWS_MIN, min(PARTITION_ROWS_MAX, v))


def plan_ranges(min_id, max_id, total_rows, target_rows_per_partition,
                max_concurrency=None):
    """[retired range path] Half-open [lo,hi) integer-PK ranges, equi-width over the id
    space, ~target_rows_per_partition rows each; contiguous, no gaps/overlaps."""
    if total_rows <= 0:
        return []
    if max_id < min_id:
        raise ValueError(f"max_id ({max_id}) < min_id ({min_id})")

    per = clamp_partition_rows(target_rows_per_partition)

    import math
    n = max(1, math.ceil(total_rows / per))

    key_span = (max_id - min_id) + 1  # inclusive width of the id space
    # Never make more ranges than there are distinct integer keys (avoid zero-width).
    n = max(1, min(n, key_span))

    ranges = []
    start = min_id
    end_exclusive = max_id + 1
    for i in range(1, n + 1):
        boundary = min_id + (i * key_span) // n
        if i == n:
            boundary = end_exclusive
        lo = start
        hi = boundary
        if hi <= lo:
            continue
        ranges.append((lo, hi))
        start = hi

    if ranges and ranges[-1][1] != end_exclusive:
        lo, _ = ranges[-1]
        ranges[-1] = (lo, end_exclusive)
    return ranges


def pk_kind_of(config):
    """The PK range KIND from Job 1 v2: 'integer' | 'uuid' | 'text' | None.
    Old (v1) configs / non-span tables -> None (caller won't range them)."""
    pk = (config or {}).get("metadata", {}).get("primary_key") or {}
    return pk.get("pk_kind")


# =============================================================================
# PK planners (uuid/text) — thin wrappers over the integer plan_ranges(): map the string
# bounds to an integer key space, cut with the integer planner (contiguous, no
# gap/overlap, full coverage), then map boundaries back to the string form used by the
# range filter + keyset DELETE.
# =============================================================================
def normalize_uuid_hex(value):
    """Canonicalize any uuid representation to 32 LOWERCASE hex digits (no dashes).
    Accepts canonical 8-4-4-4-12 (any case), raw 32-hex (any case), surrounding space.
    Returns None if not exactly 32 hex digits after stripping dashes/space. This SINGLE
    normalization is used IDENTICALLY on the Spark side (raw CSV) and the DSQL side
    (stored canonical uuid), so range bounds compare consistently across both forms."""
    if value is None:
        return None
    s = str(value).strip().lower().replace("-", "")
    if len(s) != 32:
        return None
    for ch in s:
        if ch not in "0123456789abcdef":
            return None
    return s


def hex_to_int(h):
    """32-hex-digit string -> 128-bit int."""
    return int(h, 16)


def int_to_hex(n):
    """128-bit int -> 32-hex-digit lowercase string (zero-padded)."""
    if n < 0:
        raise ValueError("uuid hex int must be >= 0")
    return format(n, "032x")


def plan_ranges_hex(min_hex, max_hex, total_rows, target_rows_per_partition,
                    max_concurrency=None):
    """Half-open [lo_hex, hi_hex) ranges over the 32-hex uuid key space. min/max_hex are
    NORMALIZED 32-hex (from normalize_uuid_hex). The final hi is max_int+1 rendered as
    hex (may be 33 digits — a strict exclusive upper bound; stored values stay 32 digits,
    always < it). Comparison at load/DELETE is on the normalized hex value (int-equiv)."""
    lo_i = hex_to_int(min_hex)
    hi_i = hex_to_int(max_hex)
    int_ranges = plan_ranges(lo_i, hi_i, total_rows, target_rows_per_partition,
                             max_concurrency)
    out = []
    for lo, hi in int_ranges:
        out.append((int_to_hex(lo),
                    format(hi, "032x") if hi <= (16 ** 32 - 1) else format(hi, "x")))
    return out


def plan_ranges_text(min_str, max_str, total_rows, target_rows_per_partition,
                     max_concurrency=None, sample=None):
    """[retired range path] Half-open [lo,hi) text-PK ranges cut on real sorted sampled
    keys (NUL-free) for exact contiguous coverage; ~ceil(total_rows/target) ranges."""
    if total_rows <= 0:
        return []
    if not sample:
        # No sample provided (or empty): single whole-span range. The loader's is_top
        # makes it ceilingless, so it covers [min_str, +inf) i.e. every key.
        return [(min_str, max_str)]
    # Defensive: sample must be sorted-ascending, distinct, NUL-free. We do NOT re-sort
    # here (caller guarantees byte order == Spark/DSQL order); we only de-dup adjacent and
    # assert no NUL, so a contract violation fails loud rather than silently mis-partitioning.
    uniq = []
    for k in sample:
        if "\x00" in k:
            raise ValueError(f"plan_ranges_text: sampled key contains NUL byte: {k!r} "
                             f"(keys with embedded NUL are not supported / not valid PKs)")
        if not uniq or k != uniq[-1]:
            uniq.append(k)
    if len(uniq) == 1:
        return [(uniq[0], uniq[0])]     # single distinct key -> one (top) range

    import math
    per = clamp_partition_rows(target_rows_per_partition)
    n_ranges = max(1, math.ceil(total_rows / per))
    # Can't have more ranges than we have interior cut points + 1.
    n_ranges = max(1, min(n_ranges, len(uniq)))

    # Choose n_ranges-1 interior cut indices evenly across the sample (excluding index 0).
    # Boundary i (1..n_ranges-1) = uniq[round(i * (len-1) / n_ranges)]. Dedup + enforce
    # strictly increasing so no zero-width range.
    cuts = []
    m = len(uniq)
    for i in range(1, n_ranges):
        idx = (i * (m - 1)) // n_ranges
        if idx <= 0:
            idx = 1
        cut = uniq[idx]
        if cut != uniq[0] and (not cuts or cut > cuts[-1]):
            cuts.append(cut)

    # Build contiguous half-open ranges: [min, c1), [c1, c2), ..., [ck, max].
    bounds = [uniq[0]] + cuts + [uniq[-1]]
    ranges = []
    for i in range(len(bounds) - 1):
        lo, hi = bounds[i], bounds[i + 1]
        if hi <= lo:
            continue                    # skip any degenerate (shouldn't happen post-dedup)
        ranges.append((lo, hi))
    if not ranges:
        return [(uniq[0], uniq[-1])]
    # Ensure the last range's hi is exactly max_str (top range; loader omits its ceiling).
    lo_last, _ = ranges[-1]
    ranges[-1] = (lo_last, uniq[-1])
    return ranges


# =============================================================================
# optional Glue-arg overlay. Configures via module-level CONSTANTS; a few Glue args, IF
# present, override them. All args are optional — omitting them keeps the defaults.
# (getResolvedOptions raises on a required arg that's missing, so we only request args
# actually present in sys.argv.)
# =============================================================================
def _apply_v6_arg_overrides():
    global V6_WRITE_MODE, LARGE_TABLE_BYTES_THRESHOLD, TARGET_ROWS_PER_PARTITION
    global MAX_WRITE_CONCURRENCY, V6_PARALLEL_ENABLED, PER_TABLE_WRITE_CONCURRENCY
    global V6_CHUNK_FANOUT_ENABLED, INTRA_TABLE_WRITERS, FORCE_V5_TABLES
    global AUTO_REBLANK_ON_RESUME, VERBOSE_CHUNKS, VERBOSE_CHUNK_EVERY
    global MAX_FILES_IN_PARALLEL   # v16
    global CONFIG_PREFIX, INDEX_S3_PATH, STATUS_S3_PATH   # v16: per-group prefix override
    global DSQL_ENDPOINT, DSQL_USER, DSQL_DATABASE, REGION   # kit: connection overlay
    optional = ["write_mode", "large_table_bytes_threshold", "target_rows_per_partition",
                "max_write_concurrency", "v6_parallel_enabled",
                "max_files_in_parallel",   # v16
                "config_prefix",           # v16: orchestrator points each group at its own prefix
                "dsql_endpoint", "dsql_user", "dsql_database", "region",  # kit: connection overlay
                "chunk_fanout_enabled", "intra_table_writers", "force_v5_tables",
                "auto_reblank_on_resume", "per_table_write_concurrency",
                "verbose_chunks", "verbose_chunk_every"]
    present = [a for a in optional if f"--{a}" in sys.argv]
    if not present:
        return
    ov = getResolvedOptions(sys.argv, present)
    if "write_mode" in ov:
        wm = str(ov["write_mode"]).strip().lower()
        if wm in ("driver_threads", "executors"):
            V6_WRITE_MODE = wm
        else:
            print(f"  ⚠️ ignoring invalid write_mode={ov['write_mode']!r} "
                  f"(use driver_threads|executors); keeping {V6_WRITE_MODE}")
    if "large_table_bytes_threshold" in ov:
        try:
            LARGE_TABLE_BYTES_THRESHOLD = max(1, int(ov["large_table_bytes_threshold"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid large_table_bytes_threshold={ov['large_table_bytes_threshold']!r}")
    if "target_rows_per_partition" in ov:
        TARGET_ROWS_PER_PARTITION = clamp_partition_rows(ov["target_rows_per_partition"])
    if "max_write_concurrency" in ov:
        try:
            MAX_WRITE_CONCURRENCY = max(1, int(ov["max_write_concurrency"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid max_write_concurrency={ov['max_write_concurrency']!r}")
    if "per_table_write_concurrency" in ov:
        try:
            PER_TABLE_WRITE_CONCURRENCY = max(1, int(ov["per_table_write_concurrency"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid per_table_write_concurrency={ov['per_table_write_concurrency']!r}")
    if "max_files_in_parallel" in ov:   # v16
        try:
            MAX_FILES_IN_PARALLEL = max(1, int(ov["max_files_in_parallel"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid max_files_in_parallel={ov['max_files_in_parallel']!r}")
    if "config_prefix" in ov:   # v16: override the hardcoded CONFIG_PREFIX so the
        # orchestrator can give each split-group its OWN prefix (its own _manifest_index.json
        # + _load_status.json + _file_status/), letting many v16 jobs run disjoint groups
        # concurrently without clobbering one shared status file. MUST recompute the derived
        # paths here (they were computed from the default constant at import, before this
        # overlay runs). Normalize to a trailing slash.
        _cp = str(ov["config_prefix"]).strip()
        if _cp:
            if not _cp.endswith("/"):
                _cp += "/"
            CONFIG_PREFIX = _cp
            INDEX_S3_PATH = CONFIG_PREFIX + "_manifest_index.json"
            STATUS_S3_PATH = CONFIG_PREFIX + "_load_status.json"
            print(f"  ↪ CONFIG_PREFIX overridden -> {CONFIG_PREFIX} "
                  f"(index={INDEX_S3_PATH}, status={STATUS_S3_PATH})")
    # kit: connection overlay — let the orchestrator point the loader at this task's DSQL
    # cluster/user/db/region instead of the hardcoded constants (multi-task deploy).
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
    if "dsql_database" in ov:
        _dd = str(ov["dsql_database"]).strip()
        if _dd:
            DSQL_DATABASE = _dd
            print(f"  ↪ DSQL_DATABASE overridden -> {DSQL_DATABASE}")
    if "region" in ov:
        _rg = str(ov["region"]).strip()
        if _rg:
            REGION = _rg
            print(f"  ↪ REGION overridden -> {REGION}")
    if "v6_parallel_enabled" in ov:
        V6_PARALLEL_ENABLED = str(ov["v6_parallel_enabled"]).strip().lower() in ("true", "1", "yes")
    if "chunk_fanout_enabled" in ov:
        V6_CHUNK_FANOUT_ENABLED = str(ov["chunk_fanout_enabled"]).strip().lower() in ("true", "1", "yes")
    if "intra_table_writers" in ov:
        try:
            INTRA_TABLE_WRITERS = max(1, int(ov["intra_table_writers"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid intra_table_writers={ov['intra_table_writers']!r}")
    if "force_v5_tables" in ov:
        # Comma/semicolon/whitespace-separated "schema.table" list; case-insensitive.
        # (No `re` here — it's imported later in the module; normalize separators to ','.)
        raw = str(ov["force_v5_tables"])
        for _sep in (";", "\t", "\n", " "):
            raw = raw.replace(_sep, ",")
        FORCE_V5_TABLES = {t.strip().lower() for t in raw.split(",") if t.strip()}
    if "auto_reblank_on_resume" in ov:
        AUTO_REBLANK_ON_RESUME = str(ov["auto_reblank_on_resume"]).strip().lower() in ("true", "1", "yes")
    if "verbose_chunks" in ov:
        VERBOSE_CHUNKS = str(ov["verbose_chunks"]).strip().lower() in ("true", "1", "yes")
    if "verbose_chunk_every" in ov:
        try:
            VERBOSE_CHUNK_EVERY = max(1, int(ov["verbose_chunk_every"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid verbose_chunk_every={ov['verbose_chunk_every']!r}")


_apply_v6_arg_overrides()
# Global cap on TOTAL concurrent range writers across all tables (see comment above).
_DSQL_RANGE_WRITER_SEM = threading.Semaphore(max(1, MAX_WRITE_CONCURRENCY))
print(f"[v16] write_mode={V6_WRITE_MODE} parallel_enabled={V6_PARALLEL_ENABLED} "
      f"large_table_bytes_threshold={LARGE_TABLE_BYTES_THRESHOLD} "
      f"target_rows_per_partition={TARGET_ROWS_PER_PARTITION} "
      f"max_write_concurrency={MAX_WRITE_CONCURRENCY} "
      f"chunk_fanout_enabled={V6_CHUNK_FANOUT_ENABLED} "
      f"max_files_in_parallel={MAX_FILES_IN_PARALLEL} "
      f"intra_table_writers={INTRA_TABLE_WRITERS} "
      f"force_v5_tables={sorted(FORCE_V5_TABLES) if FORCE_V5_TABLES else '(none)'}")

# ---- Parallel table loading -------------------------------------------------
# How many tables to load CONCURRENTLY on the driver thread pool. 1 = sequential.
# Capped hard at PARALLEL_HARD_CAP so a config typo can't stampede the driver. The
# effective value is min(MAX_PARALLEL_TABLES, PARALLEL_HARD_CAP, tables-to-load), then
# clamped by driver free memory when AUTO_THROTTLE_WORKERS is on (each concurrent table
# buffers ~1 partition on the driver).
MAX_PARALLEL_TABLES = 20
PARALLEL_HARD_CAP = 20

# MEMORY-AWARE WORKER AUTO-THROTTLE:
# toLocalIterator() buffers up to the largest partition PER concurrent table, so effective
# parallelism is bounded by driver memory. Estimate a safe worker count from free memory /
# a per-worker budget and take the MIN with MAX_PARALLEL_TABLES. Set AUTO_THROTTLE_WORKERS
# =False to use MAX_PARALLEL_TABLES as-is. PER_WORKER_MEM_BUDGET_MB allows for one table's
# largest-partition buffer + overhead (1500 MB is reasonable for multiLine ~1 GB part-files).
AUTO_THROTTLE_WORKERS = True
PER_WORKER_MEM_BUDGET_MB = 1500
MIN_AUTO_WORKERS = 1

# boto3 CLIENT CREATION is NOT thread-safe (client USE is). Under parallel table loading,
# concurrent client creation can raise resolver/credential-provider races, so serialize
# *creation* behind this lock. Held only for the brief construction call, not the I/O.
_BOTO_CLIENT_LOCK = threading.Lock()


def make_boto_client(service):
    """Thread-safe boto3 client factory: only client CREATION is serialized."""
    with _BOTO_CLIENT_LOCK:
        return boto3.client(service, region_name=REGION)


def driver_free_mem_mb():
    """Best-effort available driver memory in MB. Prefers cgroup/container limits
    (Glue runs containerized) and falls back to /proc/meminfo, then None if unknown.
    Returns None on any error so the caller can skip throttling rather than guess."""
    # 1) cgroup v2 (memory.max + memory.current)
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            limit = int(raw)
            with open("/sys/fs/cgroup/memory.current") as f:
                used = int(f.read().strip())
            return max(0, (limit - used)) // (1024 * 1024)
    except Exception:
        pass
    # 2) cgroup v1
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
        with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f:
            used = int(f.read().strip())
        # reports a huge sentinel when unlimited; ignore absurd values.
        if limit < (1 << 62):
            return max(0, (limit - used)) // (1024 * 1024)
    except Exception:
        pass
    # 3) /proc/meminfo MemAvailable
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024  # kB -> MB
    except Exception:
        pass
    return None


def compute_effective_workers(requested, to_load):
    """Decide the worker count: min(requested, hard cap, tables-to-load), then, if
    AUTO_THROTTLE_WORKERS, further cap by driver free memory / per-worker budget.
    Never returns less than MIN_AUTO_WORKERS (>=1). Returns (workers, reason)."""
    base = max(1, min(requested, PARALLEL_HARD_CAP, to_load or 1))
    if not AUTO_THROTTLE_WORKERS or base == 1:
        return base, "no auto-throttle"
    free_mb = driver_free_mem_mb()
    if not free_mb or PER_WORKER_MEM_BUDGET_MB <= 0:
        return base, f"mem unknown ({free_mb}); using base={base}"
    mem_cap = max(MIN_AUTO_WORKERS, free_mb // PER_WORKER_MEM_BUDGET_MB)
    workers = max(MIN_AUTO_WORKERS, min(base, mem_cap))
    return workers, (f"free≈{free_mb}MB / {PER_WORKER_MEM_BUDGET_MB}MB/worker "
                     f"-> mem_cap={mem_cap}, base={base} -> {workers}")


def effective_inner_concurrency(requested):
    """V10: inner concurrency is set EXACTLY to the requested value (the per-table
    write-concurrency the customer configures), NOT memory-clamped. Rationale: the memory
    clamp was silently flooring this to 1 on memory-pressured drivers, so only ONE range
    writer ran (one session committing) regardless of the configured connections. The
    operator's per-table setting is now authoritative.

    ⚠️ TRADEOFF (deliberate): this REMOVES the driver-OOM guard that the clamp provided.
    Each concurrent inner writer holds a toLocalIterator partition buffer on the driver, so
    `requested` concurrent writers on a WIDE table or LARGE partitions can pressure driver
    memory. Keep `requested` (PER_TABLE_WRITE_CONCURRENCY) sane for the driver size, and use
    a bigger driver (G.2X/G.4X) if you push it high. Returns (n, reason)."""
    n = max(1, int(requested))
    return n, f"set to requested={n} (memory clamp DISABLED — per-table setting is authoritative; watch driver memory)"

# =============================================================================
# CSV READ OPTIONS  (correct for DMS default Rfc4180=true; see module docstring)
# =============================================================================
# These options make Spark correctly read DMS CSV where RFC-4180-quoted fields (the
# JSON REQUEST_BODY / RESPONSE_BODY) contain raw NEWLINES, embedded COMMAS, and
# embedded QUOTES. Without them, records misalign and JSON text lands in the wrong
# column (e.g. a JSON fragment ends up in the `id` uuid column -> DSQL "invalid
# input syntax for type uuid"). These match the DMS S3-target DEFAULT Rfc4180=true
# output (fields quoted with ", embedded " doubled). If the endpoint set
# Rfc4180=false the data is unquoted and NO options fix it — fix it upstream (see
# module docstring) and rely on the UUID guard to fail loudly meanwhile.
#
#   multiLine                    : a quoted field may span multiple physical lines.
#   quote                        : the field quote char ('"'), per Rfc4180=true.
#   escape                       : RFC-4180 escapes an embedded '"' by DOUBLING it
#                                  (""), i.e. the escape char is also '"'.
#   unescapedQuoteHandling       : STOP_AT_CLOSING_QUOTE tolerates a stray quote
#                                  inside a value without derailing the whole row.
#   mode                         : PERMISSIVE keeps rows even if a field is odd; the
#                                  Spark-side uuid guard (below) then hard-fails only
#                                  when a uuid column actually got garbage.
#
# TRADEOFF — multiLine makes CSV files NON-SPLITTABLE: each S3 part-file is read by ONE
# Spark task, so peak driver memory scales with the largest PART-FILE (not the whole
# table) and parallelism drops to (number of part-files). If a single part-file is huge,
# lower DMS's max file size or give the Glue worker more memory (e.g. G.2X).
#
# If your DMS S3 endpoint uses a different CSV dialect (e.g. backslash escaping or a
# different quote char), adjust here — this is the single place to tune the read.
#   sep / encoding : mirror the DMS endpoint (default CsvDelimiter ',', codePage 65001
#                    = UTF-8).
#   lineSep        : INTENTIONALLY UNSET. An explicit lineSep under multiLine=true breaks
#                    legitimately multi-line quoted fields (embedded newlines in stack
#                    traces). Auto-detect handles \n / \r\n / \r.
CSV_READ_OPTIONS = {
    "header": None,          # set per-table from config (dms_has_headers)
    "inferSchema": "false",
    "multiLine": "true",
    # DMS PARALLEL FULL LOAD writes CSVs into NESTED per-partition subfolders under the
    # table prefix (e.g. .../schema/table/<partition>/LOAD*.csv). Spark's DataFrameReader
    # is NON-recursive by default, so reader.csv(prefix) would MISS rows in subfolders.
    # recursiveFileLookup=true makes the whole-prefix read == the union of every LOAD*.csv
    # regardless of parallel-load layout, matching s3_list_table_files (already recursive)
    # so the Spark read and the chunk-fanout file list can never disagree. Safe for the
    # single-file / flat-multifile layouts too.
    "recursiveFileLookup": "true",
    "quote": '"',
    "escape": '"',
    # PRESERVE leading/trailing whitespace in field values. Spark's CSV reader defaults BOTH
    # of these to true, which SILENTLY TRIMS spaces from every field (e.g. a padded VARCHAR
    # '  x  ' loads as 'x'). DMS writes the exact bytes, so we must keep them to avoid silent
    # data corruption. (Matches the CDC path, which preserves original whitespace for text.)
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "unescapedQuoteHandling": "STOP_AT_CLOSING_QUOTE",
    "mode": "PERMISSIVE",
    "sep": ",",              # DMS CsvDelimiter default (endpoint: not overridden)
    "encoding": "UTF-8",     # DMS codePage 65001 (endpoint: confirmed)
    # SILENT-CORRUPTION GUARD (whole-prefix read): restrict the recursive read to DMS
    # FULL-LOAD files only. DMS names full-load files LOAD*.csv; CDC output is timestamp-
    # named (e.g. 20260921-172204370.csv) and lives under processed/ + failed/. Without
    # this, recursiveFileLookup=true descends into processed/ and reads leftover CDC files
    # (16 cols, leading `Op`) as full-load rows (15 cols) -> every column shifts. pathGlob
    # Filter matches the basename, so LOAD*.csv excludes CDC files wherever they sit. This
    # mirrors the s3_list_table_files LOAD*.csv guard used by the per-file fan-out path.
    "pathGlobFilter": "LOAD*.csv",
    # lineSep is INTENTIONALLY NOT SET. A text column can hold a legitimately multi-line
    # quoted value (e.g. a stack trace with an embedded newline, RFC-4180 quoted with
    # doubled inner quotes). An explicit lineSep="\n" under multiLine=true makes the
    # univocity parser treat that in-field newline as a RECORD boundary, splitting one row
    # into two and shifting every following column. Do NOT re-add lineSep unless you are
    # certain no text column ever contains a newline.
}

# hard-fail a table if any value routed to a uuid column is non-null and not a valid uuid
# shape. The guard accepts canonical 8-4-4-4-12 OR clean raw 32-hex and rejects anything
# else (e.g. JSON text from a misaligned CSV row). Checked in TWO layers without an extra
# Spark scan: (1) FAIL-BEFORE-WRITE on the in-memory pre-load sample, and (2) INLINE
# backstop per row in flatten_chunk(). Set False to disable (not recommended).
STRICT_UUID_GUARD = True

# ---- Multi-row batched INSERT + retry tuning --------------------------------
# INSERT_CHUNK_SIZE: rows per multi-row INSERT/txn (the target; compute_chunk_size takes
# the MIN of this, the param cap, the 3000-row DSQL txn cap, and the byte cap). Kept at or
# below the 3000-row DSQL per-txn cap; the byte cap still auto-shrinks this for wide/LOB
# rows so a txn never exceeds MAX_CHUNK_BYTES.
INSERT_CHUNK_SIZE = 3000   # literal mode has no param cap -> target the DSQL 3000-row/txn max (byte cap still auto-shrinks wide/LOB rows). Overridable via --insert_chunk_size.

# ---- READ REPARTITION (break the multiLine single-partition bottleneck) --------------
# multiLine=true makes each CSV file a SINGLE non-splittable Spark partition, so one big
# DMS file = one read task and every toLocalIterator serializes on that partition (only ONE
# session commits at a time). Repartitioning the just-read DataFrame to N =
# PER_TABLE_WRITE_CONCURRENCY (one read partition per concurrent range writer) spreads the
# parse/filter across executors. READ_REPARTITION_ENABLED=False falls back to file-count
# partitioning; READ_REPARTITION_MIN is a floor so a tiny setting still spreads a big file.
READ_REPARTITION_ENABLED = True
READ_REPARTITION_MIN = 8       # floor so tiny per-table concurrency still spreads a big file

# ---- STRICT ROW-SPLIT GUARD (D1 expected-count) --------------------------------------
# Defends against the ONE failure the internal count gate CANNOT catch: an unquoted /
# mis-quoted embedded newline in a source text field makes Spark's multiLine parser split
# ONE logical source row into TWO physical rows. Both spark_pk_bounds (planning) and the
# load read via the SAME parser, so both see the inflated count, committed==total, and the
# internal gate passes while the target has EXTRA phantom rows (a shifted fragment can even
# land a valid uuid in the PK slot, so a real PK does NOT reject it). D1 compares the
# Spark-parsed count against metadata.expected_source_rows (authoritative, e.g. DMS
# FullLoadRows) — the only check OUTSIDE the parser. No-op if expected_source_rows is
# absent. Set False to disable.
STRICT_ROWSPLIT_GUARD = True
# pg8000 / PostgreSQL wire protocol binds parameters with an int16 count, so a SINGLE
# statement can carry at most 32767 bound parameters. compute_chunk_size derives its
# per-INSERT row cap as (MAX_PARAMS_PER_STATEMENT // num_columns), so this MUST be <= 32767
# (e.g. a 12-col table at the 3000-row txn cap = 36000 params > 32767).
MAX_PARAMS_PER_STATEMENT = 32767
MIN_CHUNK_SIZE = 25
DSQL_MAX_ROWS_PER_TXN = 3000
COUNT_EXACT_MAX_ROWS = 100000

# large-table no-loss falls back to pg_class.reltuples (planner estimate) when an exact
# COUNT(*) exceeds DSQL limits at scale. DSQL auto-analyzes so reltuples is current without
# us running ANALYZE. No-dup is guaranteed by the PK constraint, not this.
#
# DSQL Auto Analyze refreshes stats probabilistically and can briefly LAG right after a
# rapid bulk load. We do NOT run ANALYZE ourselves (redundant + 0A000 in a txn). Instead we
# ADAPTIVELY poll pg_class.reltuples every RELTUPLES_POLL_INTERVAL_SECONDS and accept the
# value once TWO CONSECUTIVE reads MATCH (Auto Analyze has stabilized), up to
# RELTUPLES_MAX_POLLS attempts. Safer than a COUNT(*) that hits the 300s txn-age limit.
RELTUPLES_POLL_INTERVAL_SECONDS = 30
RELTUPLES_MAX_POLLS = 6   # safety cap: up to ~RELTUPLES_MAX_POLLS * interval seconds


def _files_pk_disjoint(file_ranges):
    """Return True IFF every file has a usable [pk_min, pk_max] and no two files' ranges
    OVERLAP — i.e. the PK is effectively file-ordered (e.g. RAW-hex->uuid), so each file
    owns a contiguous, non-overlapping key slice. Only then is a per-file
    COUNT(*) WHERE pk in [min,max] == file rows a valid exact check (an index-range scan
    over a disjoint slice). Random UUIDs (gen_random_uuid) interleave -> overlap -> False
    -> caller falls back to a whole-table COUNT(*). Comparison is on the stored string form
    (fixed-width lowercase hex uuids sort identically as text and as uuid)."""
    rs = [fr for fr in (file_ranges or [])
          if fr.get("pk_min") is not None and fr.get("pk_max") is not None]
    if not rs or len(rs) != len(file_ranges or []):
        return False   # some file had no PK min/max -> can't trust per-file ranges
    rs = sorted(rs, key=lambda fr: fr["pk_min"])
    for i in range(1, len(rs)):
        # sorted by min; overlap iff this file's min <= previous file's max
        if rs[i]["pk_min"] <= rs[i - 1]["pk_max"]:
            return False
    return True


def _read_reltuples_stable(cursor, dsql_schema, dsql_table):
    """Adaptively read pg_class.reltuples, polling every RELTUPLES_POLL_INTERVAL_SECONDS
    until TWO CONSECUTIVE reads MATCH (DSQL Auto Analyze has stabilized after the bulk load)
    or RELTUPLES_MAX_POLLS is reached. Returns the last estimate (int), or -1 if unavailable.
    Bounded so it can never hang. The caller's cursor is autocommit (each read its own txn)."""
    def _one():
        cursor.execute(
            "SELECT c.reltuples::bigint FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (dsql_schema, dsql_table))
        r = cursor.fetchone()
        return int(r[0]) if r and r[0] is not None else -1

    prev = _one()
    if prev < 0:
        return -1
    for i in range(1, RELTUPLES_MAX_POLLS + 1):
        time.sleep(RELTUPLES_POLL_INTERVAL_SECONDS)
        cur_est = _one()
        if cur_est == prev:
            print(f"    ✓ reltuples stabilized at {cur_est:,} "
                  f"(two consecutive reads matched after ~{i*RELTUPLES_POLL_INTERVAL_SECONDS}s "
                  f"of Auto-Analyze settling)", flush=True)
            return cur_est
        print(f"    … reltuples still moving: {prev:,} -> {cur_est:,} "
              f"(poll {i}/{RELTUPLES_MAX_POLLS}); waiting for it to settle", flush=True)
        prev = cur_est
    print(f"    ⚠️ reltuples did not fully stabilize after {RELTUPLES_MAX_POLLS} polls "
          f"(~{RELTUPLES_MAX_POLLS*RELTUPLES_POLL_INTERVAL_SECONDS}s); using last estimate "
          f"{prev:,}", flush=True)
    return prev

# LITERAL-INSERT MODE: the 32767 bind-parameter cap forces WIDE tables to tiny chunks
# (e.g. a 71-col table floors at 32767//71 = 461 rows/txn), throttling throughput.
# In literal mode we inline each value as a SAFELY-ESCAPED SQL literal (single quotes
# doubled, same ::type cast the bind path used) instead of a %s parameter, so a statement
# carries ZERO bind parameters. The per-INSERT row cap is then bound only by DSQL's real
# per-txn limits (DSQL_MAX_ROWS_PER_TXN=3000 rows and MAX_CHUNK_BYTES bytes), NOT the param
# cap. Escaping is injection-safe: every value is emitted as '<escaped>'::type or NULL.
LITERAL_INSERT_MODE = True

# ---- MEMORY-aware chunk sizing ----------------------------------------------
MAX_CHUNK_BYTES = 8 * 1024 * 1024        # ~8 MiB per multi-row INSERT (driver-memory sizing)
AVG_BYTES_PER_VALUE_FALLBACK = 64
ROW_SAMPLE_FOR_SIZING = 200

# V16 BUG-L1 FIX: HARD DSQL per-transaction modify limit is ~10 MiB.
#
# SIZING IS NOT SPECULATIVE (v16 re-vet): we do NOT pad the budget by a guessed "escaping
# inflation %" — that was hand-wavy in both directions (too low wastes throughput; too high
# risks a >10 MiB rejection). Instead:
#   1. CHUNK_BYTE_BUDGET is the HARD limit minus a small FIXED reserve for the parts of the
#      statement that estimate_avg_row_bytes does NOT measure: the `INSERT INTO ... VALUES`
#      prefix, the per-value `::type` casts + quotes + commas, and any quote-doubling. This
#      is a concrete byte figure (9.8 MiB), not raw_bytes x a_guessed_factor. It's the
#      TARGET the row-count estimate divides by — best-effort for THROUGHPUT only.
#   2. The GUARANTEE against exceeding 10 MiB does NOT rely on that estimate at all: after
#      the real literal statement is built, insert_one_chunk MEASURES len(stmt) and, if it
#      still exceeds DSQL_MAX_TXN_BYTES (a quote-dense row inflated past the estimate),
#      re-slices the chunk and rebuilds — the exact wire bytes, no assumption. So the
#      estimate only affects how OFTEN we re-slice, never whether a txn is oversized.
# Net: budget ~9.8 MiB (tight 2% buffer) keeps v16 chunk sizes >= v15 for every row size v15
# could commit (v15 used an 8 MiB divisor), so it is never slower — in fact larger chunks on
# wide/LOB rows; and the measured backstop makes the LOB correctness fix exact rather than
# probabilistic, so running this close to the limit is safe.
DSQL_MAX_TXN_BYTES = 10 * 1024 * 1024        # hard DSQL per-txn modify limit (rejection point)
# Buffer under the hard limit: a TIGHT 2% reserve (~205 KiB) — we run the system to the max.
# It covers the statement prefix + per-row casts/quotes/commas + quote-doubling slack the
# raw-byte row estimate omits. This is the SIZING target only; the measured backstop in
# insert_one_chunk is the actual correctness guarantee (it re-slices on the real bytes), so
# running this close to the 10 MiB limit is safe. NOTE: for very large rows the row count is
# ceilinged by physics, not this buffer (e.g. 1 MiB rows -> at most 9/txn since 10x1MiB hits
# the limit); the buffer only shifts the count at sizes where integer division lands astride
# a boundary (e.g. ~410 KB rows: 24/txn at 2% vs 23 at 5%).
STMT_OVERHEAD_RESERVE_BYTES = 205 * 1024                              # ~2% of 10 MiB
CHUNK_BYTE_BUDGET = DSQL_MAX_TXN_BYTES - STMT_OVERHEAD_RESERVE_BYTES   # ~9.8 MiB sizing target

# MINIMAL-HEADROOM target fractions for the MEASURED proportional shrinks (maximize perf).
# These are NOT speculative padding — they're the tiny margin that keeps a proportional
# re-size from immediately overshooting again (which would cost a re-slice or, for time, a
# rejected txn). Kept minimal:
#   BYTE_TARGET_FRACTION 0.99: the byte calc (measured bytes/row) is near-exact and linear,
#     and an overshoot only costs one more CHEAP in-memory re-slice (no DB round trip), so we
#     pack to 99% of the 10 MiB limit.
#   TIME_TARGET_FRACTION 0.95 / _AFTER_FAIL 0.90: applied ON TOP of the batch trigger (270s),
#     which is ITSELF 30s under the 300s hard limit — so this is a SECOND, thinner margin
#     absorbing commit-time noise. A time overshoot risks a rejected 300s txn (expensive
#     retry), so time keeps slightly more headroom than bytes; a chunk that already FAILED
#     gets a touch more (0.90). 0.95*270=256s, 0.90*270=243s — both well under 300.
BYTE_TARGET_FRACTION = 0.99
TIME_TARGET_FRACTION = 0.95
TIME_TARGET_FRACTION_AFTER_FAIL = 0.90

# V16 BUG-L2 FIX: DSQL caps a NON-INDEX column at 1 MiB and a whole row at 2 MiB (hard
# limits). A single LOB (text/json/bytea, or a varchar with no declared max) larger than
# this is NOT caught by varchar_max_len (which only covers type_category=='varchar' with a
# positive max_length), so it would reach INSERT and fail with a cryptic NON-RETRIABLE DSQL
# error that fails the whole table. LOB_MAX_COLUMN_BYTES lets us guard it PRE-WRITE (loud,
# actionable). Set slightly under 1 MiB for headroom; a value over it is a genuine data/
# schema problem the operator must resolve (widen model / drop the column / fix source).
LOB_MAX_COLUMN_BYTES = 1024 * 1024       # ~1 MiB DSQL non-index column limit
# Categories that are NOT length-bounded by varchar_max_len and can hold a large LOB.
_LOB_GUARD_CATEGORIES = frozenset({"text", "json", "jsonb", "bytea", "varchar"})

MAX_CHUNK_RETRIES = 3
CHUNK_RETRY_BACKOFF_SECONDS = 2
# Bounded retry for the INITIAL DSQL open (connect_dsql). Each failed attempt on a
# connection-class/transient error invalidates the cached IAM token + backs off so the next
# attempt mints a fresh token — heals a token/endpoint blip at open time instead of failing
# a whole writer/table. On exhaustion the last error is raised (caller's guard handles it).
CONNECT_MAX_RETRIES = 4
# Socket connect timeout (seconds) on EVERY pg8000.connect — without it a refused/half-open
# DSQL socket blocks the connect indefinitely (silent hang; the retry path never runs).

# ---- OCC (optimistic concurrency) retry on COMMIT ---------------------------
OCC_MAX_RETRIES = 5
OCC_BASE_BACKOFF_SECONDS = 0.05   # 50 ms
OCC_MAX_BACKOFF_SECONDS = 5.0     # 5 s cap
# DSQL concurrency-conflict SQLSTATEs (per the Aurora DSQL troubleshooting doc):
#   40001 - serialization_failure (standard); also the class DSQL returns for OC001
#   OC000 - "change conflicts with another transaction" (tuple contention)
#   OC001 - "schema has been updated by another transaction" (catalog rebase; retry from
#           the SAME session refreshes the catalog cache and typically succeeds)
# All are RETRIABLE: retry the whole transaction with backoff+jitter, same session.
OCC_SQLSTATES = {"40001", "OC000", "OC001"}

# ---- Transient DSQL server-unavailable retry (SQLSTATE XX000 "server unavailable") --
# DSQL is distributed; a node/shard can be briefly unavailable (failover, throttling,
# transient internal error) and return SQLSTATE XX000 "server unavailable". That is
# RETRIABLE after a reconnect + backoff — distinct from a deterministic data error (which
# XX000 could also be, hence we gate on the MESSAGE too). Concurrent writers make transient
# XX000 more likely, so without this branch a single blip would fail the whole worker.
SERVER_MAX_RETRIES = 6
SERVER_BASE_BACKOFF_SECONDS = 0.5
SERVER_MAX_BACKOFF_SECONDS = 30.0
# XX000 = internal_error (broad). Only treat as transient when the message matches one
# of these fragments, so we never retry a genuine internal bug indefinitely.
SERVER_TRANSIENT_FRAGMENTS = (
    "server unavailable", "server is not available", "server not available",
    "temporarily unavailable", "try again", "too many connections",
    "connection refused", "service unavailable", "not available",
)


def _is_oom_error(exc):
    """V10: True if this failure looks like a driver/JVM out-of-memory condition — used to
    surface an OOM clearly (attributed to inner write concurrency) instead of a cryptic
    stack trace, now that effective_inner_concurrency no longer memory-clamps."""
    msg = str(exc).lower()
    return any(f in msg for f in (
        "outofmemory", "out of memory", "java heap space", "gc overhead limit",
        "memoryerror", "cannot allocate memory", "container killed",
        "killed by yarn", "exceeding memory limits", "oom",
    ))


def is_txn_timeout(exc):
    """True if this looks like the DSQL 5-minute (300s) transaction-age limit."""
    msg = str(exc).lower()
    if "transaction age limit" in msg or "300s" in msg:
        return True
    if "54000" in msg and ("age" in msg or "duration" in msg or "timeout" in msg):
        return True
    return False

# TIME safeguard (DSQL 5-min / 300s transaction limit)
# Slow-batch shrink TRIGGER, under the DSQL 5-min (300s) txn-age hard limit. NOT a cutoff —
# the txn already committed; exceeding it just shrinks the NEXT chunk. So we run it close to
# 300 (20s buffer = 280) to keep chunks big / commits few / the full load fast (a slow full
# load lets CDC accumulate). A txn-age FAILURE is retriable (the chunk re-slices smaller), so
# a rare overshoot self-heals. SELF-CORRECTING: if txn-age failures pile up past
# DSQL_BATCH_TXN_AGE_FALLBACK_THRESHOLD across the run, 270 is too aggressive for this
# workload -> the effective trigger permanently drops to the SAFE 240 for the rest of the run.
DSQL_BATCH_MAX_SECONDS = 280          # aggressive trigger (20s buffer under 300s; post-commit signal, never aborts a running chunk)
DSQL_BATCH_MAX_SECONDS_SAFE = 240     # conservative fallback after repeated txn-age failures
DSQL_BATCH_TXN_AGE_FALLBACK_THRESHOLD = 10  # >this many txn-age failures in a run -> use SAFE

# Connection recycle, under DSQL's ~60-min hard connection duration. The recycle check runs
# BETWEEN chunks (top of insert_one_chunk / blank_pk_range batch), never mid-transaction, so
# a connection that PASSES the check can still run ONE more chunk before the next check. That
# chunk can last up to the 300s (5-min) txn-age limit. So the safe threshold is NOT ~57 min
# (57 + 5 = 62 > 60 -> DSQL force-closes mid-chunk); it must leave a FULL worst-case chunk +
# slop under 60: 50 min + 5-min chunk = 55 min, ~5 min slop for OCC/backoff retries. 50 is
# the CONSERVATIVE value (aligned with the CDC job) that keeps a comfortable margin under the
# ~60-min DSQL connection close to avoid connection-close errors on long single-table loads.
CONN_RECYCLE_SECONDS = 50 * 60    # 50 min (60 - one 5-min max chunk - ~5 min retry slop)

# Run-wide txn-age failure tracking for the self-correcting batch-time trigger (thread-safe:
# the loader runs many table/file writers concurrently). Aggressive 270s by default; after
# repeated real txn-age (300s) failures the trigger backs off to the SAFE 240s run-wide.
_txn_age_failures = 0
_txn_age_lock = threading.Lock()

def _record_txn_age_failure():
    """Count one real DSQL txn-age (300s) failure; backs the batch trigger off to SAFE."""
    global _txn_age_failures
    with _txn_age_lock:
        _txn_age_failures += 1

def effective_batch_max_seconds():
    """Slow-batch shrink trigger: aggressive 270s until repeated txn-age failures prove this
    workload needs the conservative 240s, then permanently 240s for the rest of the run."""
    with _txn_age_lock:
        over = _txn_age_failures > DSQL_BATCH_TXN_AGE_FALLBACK_THRESHOLD
    return DSQL_BATCH_MAX_SECONDS_SAFE if over else DSQL_BATCH_MAX_SECONDS


def occ_backoff_seconds(attempt):
    """Full-jitter exponential backoff for OCC retries. attempt is 1-based."""
    import random
    capped = min(OCC_MAX_BACKOFF_SECONDS, OCC_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    return random.uniform(0, capped)


def is_occ_conflict(exc):
    """True if this exception is a DSQL concurrency-conflict abort (retriable): SQLSTATE
    40001 (serialization_failure), OC000 (tuple change conflict), or OC001 (schema/catalog
    updated by another txn). Per the Aurora DSQL troubleshooting doc, all are retried from
    the SAME session with backoff+jitter (OC001 additionally refreshes the catalog cache)."""
    try:
        payload = exc.args[0]
        if isinstance(payload, dict) and payload.get("C") in OCC_SQLSTATES:
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    if ("40001" in msg or "oc000" in msg or "oc001" in msg
            or "serialization" in msg or "occ" in msg or "concurrency" in msg
            or "conflicts with another transaction" in msg
            or "schema has been updated by another transaction" in msg):
        return True
    return False


def is_unique_violation(exc):
    """True if this exception is a PostgreSQL/DSQL UNIQUE constraint violation, SQLSTATE
    23505 (unique_violation). V13 uses this to make a blind re-insert SELF-CORRECTING after
    an ambiguous XX000-at-commit: if the interrupted commit ACTUALLY landed, re-inserting the
    same chunk hits the PK unique index -> 23505 -> we treat the chunk as already committed
    (count it, do NOT duplicate). If it did NOT land, the re-insert simply succeeds. This
    replaces the commit-probe and preserves BOTH no-dup and no-loss WITHOUT a probe — relies
    on the table having a PRIMARY KEY (it does: single clean run into a PK'd target)."""
    try:
        payload = exc.args[0]
        if isinstance(payload, dict) and payload.get("C") == "23505":
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    if ("23505" in msg or "unique_violation" in msg
            or "duplicate key value violates unique constraint" in msg
            or "violates unique constraint" in msg):
        return True
    return False


def estimate_avg_row_bytes(sample_rows, target_columns):
    """Estimate average encoded (UTF-8) byte size of a row from a bounded sample."""
    if not sample_rows:
        return max(1, len(target_columns) * AVG_BYTES_PER_VALUE_FALLBACK)
    total = 0
    n = min(len(sample_rows), ROW_SAMPLE_FOR_SIZING)
    for r in sample_rows[:n]:
        d = r.asDict()
        row_bytes = 0
        for c in target_columns:
            v = d.get(c)
            if v is None:
                row_bytes += 1
            elif isinstance(v, (bytes, bytearray)):
                row_bytes += len(v)
            else:
                row_bytes += len(str(v).encode("utf-8"))
        total += row_bytes
    return max(1, total // n)


def compute_chunk_size(num_columns, avg_row_bytes=None):
    """Effective rows-per-INSERT.

    V14: in LITERAL_INSERT_MODE the statement carries no bind parameters, so the 32767
    param cap (by_params) does NOT apply — a wide table is then bound only by the DSQL
    per-txn row cap and the byte cap, allowing much larger chunks.

    V16 BUG-L1 FIX (LOB correctness): the BYTE cap enforces DSQL's HARD ~10 MiB-per-txn
    modify limit and therefore must be AUTHORITATIVE — it must be able to drive the chunk
    all the way down to 1 row for very large (LOB) rows. Previously the result was
    `max(min(caps), MIN_CHUNK_SIZE)`, so the MIN_CHUNK_SIZE=25 PERFORMANCE floor could
    OVERRIDE a smaller byte cap: e.g. ~1 MiB/row -> byte cap says 7 rows, floor forces 25 ->
    ~25 MiB txn -> EXCEEDS the 10 MiB limit -> DSQL rejects (54000). Fix: apply the
    MIN_CHUNK_SIZE floor ONLY to the row/param caps (a throughput floor), then take the MIN
    with the byte cap (floored at 1, never 25). So the byte cap can shrink the chunk below
    MIN_CHUNK_SIZE when — and only when — a hard DSQL byte limit requires it."""
    if num_columns <= 0:
        num_columns = 1
    # Row/param caps: these are throughput-oriented, so the MIN_CHUNK_SIZE floor applies.
    row_caps = [INSERT_CHUNK_SIZE, DSQL_MAX_ROWS_PER_TXN]
    if not LITERAL_INSERT_MODE:
        row_caps.append(MAX_PARAMS_PER_STATEMENT // num_columns)   # bind-param protocol cap
    result = max(min(row_caps), MIN_CHUNK_SIZE)
    # Byte cap — two-tier, so v16 is NEVER slower than v15 on any row size v15 could
    # actually commit, yet still safe on LOB rows where v15 was broken:
    #
    #   v15 was `max(min(caps), MIN_CHUNK_SIZE)` with an 8 MiB byte divisor, so it ALWAYS
    #   emitted at least MIN_CHUNK_SIZE(=25) rows. That is only a *problem* once 25 rows
    #   exceed DSQL's HARD 10 MiB txn limit (avg_row_bytes > 10 MiB/25 ~= 419 KB): there v15
    #   produced a rejected >10 MiB txn (the BUG-L1 correctness failure). BELOW that point
    #   25 rows is a perfectly valid txn and v15 shipped it — so v16 must too (parity), even
    #   if a plain CHUNK_BYTE_BUDGET//avg would shave it to 24.
    #
    # So: the MIN_CHUNK_SIZE floor stays authoritative UP TO the point where 25 rows would
    # break the HARD limit; only past that does the byte cap take over and shrink toward 1.
    #   - normal/wide rows: chunk = min(row caps, CHUNK_BYTE_BUDGET//avg) but never forced
    #     below MIN_CHUNK_SIZE while MIN_CHUNK_SIZE rows still fit under the HARD limit
    #     (identical to v15 for every row size v15 could commit).
    #   - true LOB rows (MIN_CHUNK_SIZE rows would exceed the hard limit): byte cap wins,
    #     using the safe CHUNK_BYTE_BUDGET (escaping headroom), down to 1 row.
    if avg_row_bytes:
        avg = max(avg_row_bytes, 1)
        soft_byte_cap = max(1, CHUNK_BYTE_BUDGET // avg)          # 8 MiB sizing (headroom)
        hard_floor_rows = max(1, DSQL_MAX_TXN_BYTES // avg)       # rows that fit the HARD 10 MiB
        # Never shrink below MIN_CHUNK_SIZE *unless* MIN_CHUNK_SIZE rows can't fit the hard
        # limit. i.e. the effective byte cap is the soft cap, but floored back up to
        # MIN_CHUNK_SIZE when 25 rows are still hard-limit-safe (that is exactly v15's floor).
        byte_cap = soft_byte_cap
        if byte_cap < MIN_CHUNK_SIZE and MIN_CHUNK_SIZE <= hard_floor_rows:
            byte_cap = MIN_CHUNK_SIZE          # v15 parity: 25 rows still fits <=10 MiB raw
        result = min(result, byte_cap)
    return max(1, result)


def is_broken_pipe_error(exc):
    """Heuristic: does this exception look like a dropped/broken DSQL connection?
    Also covers connection-failure SQLSTATEs (class 08) so a reconnect is attempted with a
    FRESH token — see _invalidate_dsql_token."""
    import errno
    if isinstance(exc, (BrokenPipeError, ConnectionError)):
        return True
    for cls_name in ("InterfaceError", "OperationalError"):
        if type(exc).__name__ == cls_name:
            return True
    # SQLSTATE class 08 = "connection exception" (08006 unable-to-connect, 08003 no-active-
    # connection, 08001/08004 unable-to-establish/rejected, 08007, 08P01). pg8000 surfaces
    # the SQLSTATE as args[0]["C"]. Treat any class-08 as a dropped connection so the retry
    # ladders reconnect (with a fresh token). This is the 08006 stall the CDC job hit.
    try:
        payload = exc.args[0]
        if isinstance(payload, dict):
            _code = payload.get("C") or ""
            if isinstance(_code, str) and _code.startswith("08"):
                return True
    except Exception:
        pass
    msg = str(exc).lower()
    fragments = (
        "broken pipe", "connection is closed", "connection reset",
        "server closed the connection", "eof detected",
        "connection already closed", "network is unreachable",
        "ssl connection has been closed", "ssl syscall",
        "could not receive data", "could not send data",
        "socket is closed", "socket closed", "unable to connect",
    )
    if any(f in msg for f in fragments):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET):
        return True
    return False


def is_transient_server_error(exc):
    """True if this looks like a TRANSIENT DSQL server-unavailable error worth
    retrying after a reconnect (as opposed to a deterministic data/logic error).

    (V6 fix — ported from reference_pipeline/glue/job2_load.py.) DSQL can briefly return
    SQLSTATE XX000 with message 'server unavailable' (node failover / throttling /
    transient internal error). XX000 = internal_error is broad, so we require BOTH the
    code AND a transient message fragment; a plain XX000 with an unrecognized message is
    NOT retried here (it falls through to non-retriable so we never loop forever on a
    genuine internal bug). We also match the transient fragments in the message alone,
    for drivers that don't surface the SQLSTATE dict."""
    code = None
    try:
        payload = exc.args[0]
        if isinstance(payload, dict):
            code = payload.get("C")
    except Exception:
        pass
    msg = str(exc).lower()
    has_fragment = any(f in msg for f in SERVER_TRANSIENT_FRAGMENTS)
    if code == "XX000" and has_fragment:
        return True
    # Some paths only carry the message (no dict). Match the strongest fragments there.
    if has_fragment and ("server unavailable" in msg or "service unavailable" in msg
                         or "temporarily unavailable" in msg or "too many connections" in msg):
        return True
    return False


def server_backoff_seconds(attempt):
    """Full-jitter exponential backoff for transient-server retries (1-based)."""
    import random
    capped = min(SERVER_MAX_BACKOFF_SECONDS,
                 SERVER_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    return random.uniform(0, capped)

# Tokens DMS / Oracle may emit to represent NULL — coerced to real NULL.
NULL_SENTINELS = {"NULL", "N/A", "NA", "NONE", "(NULL)", r"\N"}

# Candidate timestamp/date input formats emitted by Oracle/DMS.
TIMESTAMP_INPUT_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd",
    "dd-MMM-yy hh.mm.ss.SSSSSS a",   # Oracle default TIMESTAMP
    "dd-MMM-yy hh.mm.ss a",
    "dd-MMM-yyyy HH:mm:ss",
    "dd-MMM-yy",                      # Oracle default DATE
    "dd-MMM-yyyy",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy",
]
DATE_INPUT_FORMATS = TIMESTAMP_INPUT_FORMATS

sentinel_upper = [s.upper() for s in NULL_SENTINELS]

# Canonical + raw uuid shapes for the uuid guard.
UUID_CANONICAL_RE = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
UUID_RAW_HEX_RE = r'^[0-9a-fA-F]{32}$'

# Pre-compiled matchers for the inline (driver-side, per-row) uuid guard. Using
# Python `re` here (not Spark rlike) keeps the check inside the existing streaming
# pass — no extra Spark scan of the input.
import re as _re
_UUID_CANONICAL = _re.compile(UUID_CANONICAL_RE)
_UUID_RAW_HEX = _re.compile(UUID_RAW_HEX_RE)


def is_valid_uuid_value(v):
    """True if v is a valid uuid shape (canonical 8-4-4-4-12 OR raw 32-hex). None is
    treated as valid (nullable ids are allowed; NOT NULL is enforced by DSQL)."""
    if v is None:
        return True
    s = v if isinstance(v, str) else str(v)
    return bool(_UUID_CANONICAL.match(s) or _UUID_RAW_HEX.match(s))


# =============================================================================
# Helpers
# =============================================================================
def split_s3(path):
    no_scheme = path.replace("s3://", "")
    parts = no_scheme.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def load_status(s3_client):
    if RESET_STATUS:
        print("  RESET_STATUS=True -> ignoring any existing status; all tables will be attempted.")
        return {"tables": {}}
    bucket, key = split_s3(STATUS_S3_PATH)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj['Body'].read().decode('utf-8'))
        if "tables" not in doc or not isinstance(doc["tables"], dict):
            doc = {"tables": {}}
        return doc
    except Exception:
        return {"tables": {}}


def save_status(s3_client, status):
    """Persist the status doc to S3. Best-effort — never fatal to the load."""
    status["updated_at"] = utc_now_iso()
    bucket, key = split_s3(STATUS_S3_PATH)
    try:
        s3_client.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(status, indent=2).encode('utf-8'),
            ContentType='application/json',
        )
    except Exception as e:
        print(f"  ⚠️ Could not write status file (non-fatal): {e}")


# =============================================================================
# (PK-range path only)
# =============================================================================
# A large range table is loaded as N disjoint, idempotent PK ranges. To resume across
# Glue attempts WITHOUT redoing already-loaded ranges, we durably record each range that
# COMMITTED in a per-table S3 file. On a re-run we skip the recorded ranges and reload only
# the missing ones — each via its own idempotent blank_pk_range (DELETE just its [lo,hi)) +
# reload, so no dup / no loss.
# STABLE RANGE KEY: ranges are planned deterministically from (min_id,max_id,total_rows,
# TARGET_ROWS_PER_PARTITION), so the same inputs reproduce the same [lo,hi) boundaries and
# the key "<lo>|<hi>|<is_top>" is stable across attempts. If total_rows changed between
# attempts the recorded keys won't match the new plan, so nothing is skipped and the table
# reloads in full (correctness over speed when the plan shifts).
_RANGE_STATUS_LOCK = threading.Lock()

def _range_status_path(dsql_schema, dsql_table):
    safe = f"{dsql_schema}.{dsql_table}".replace("/", "_")
    return f"{CONFIG_PREFIX}_range_status/{safe}.json"

def _range_key(lo, hi, is_top):
    return f"{lo}|{hi}|{1 if is_top else 0}"

def load_completed_ranges(s3_client, dsql_schema, dsql_table):
    """Return {range_key: rows} of ranges that committed in a PRIOR attempt (empty dict if
    none / resume disabled / unreadable)."""
    if not AUTO_REBLANK_ON_RESUME:
        return {}
    bucket, key = split_s3(_range_status_path(dsql_schema, dsql_table))
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj['Body'].read().decode('utf-8'))
        r = doc.get("ranges", {})
        return r if isinstance(r, dict) else {}
    except Exception:
        return {}

def mark_range_done(s3_client, dsql_schema, dsql_table, lo, hi, is_top, rows):
    """Durably record that a range committed. Read-modify-write under a lock (concurrent
    range writers on the same table serialize here). Best-effort but IMPORTANT for resume;
    a failed write only costs re-loading that range next attempt (idempotent — no dup).

    ALWAYS writes (NOT gated on AUTO_REBLANK_ON_RESUME): recording a completed range is a
    harmless S3 write that deletes nothing. Decoupling it from the destructive auto-reblank
    flag means a run made with resume DISABLED still leaves a resumable checkpoint trail, so
    a LATER run can resume by range instead of reblanking. (Gating this on the flag was the
    bug that forced a whole-table reblank of a 7M-row partial table whose first load ran
    with the flag off — no checkpoints had been written.) The flag now controls only whether
    those checkpoints are USED to resume (load_completed_ranges) vs. a reblank."""
    path = _range_status_path(dsql_schema, dsql_table)
    bucket, key = split_s3(path)
    with _RANGE_STATUS_LOCK:
        try:
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                doc = json.loads(obj['Body'].read().decode('utf-8'))
                if "ranges" not in doc or not isinstance(doc["ranges"], dict):
                    doc = {"ranges": {}}
            except Exception:
                doc = {"ranges": {}}
            doc["ranges"][_range_key(lo, hi, is_top)] = {"rows": int(rows), "at": utc_now_iso()}
            doc["updated_at"] = utc_now_iso()
            s3_client.put_object(Bucket=bucket, Key=key,
                                 Body=json.dumps(doc).encode('utf-8'),
                                 ContentType='application/json')
        except Exception as e:
            print(f"  ⚠️ Could not record completed range (non-fatal, will reload next "
                  f"attempt — idempotent): {e}")

def clear_range_status(s3_client, dsql_schema, dsql_table):
    """Delete a table's range-status file (called after the table fully completes, so a
    future load of the same table starts clean). Best-effort. ALWAYS runs (not flag-gated)
    to match the now-unconditional mark_range_done — otherwise a fully-loaded table could
    leave a STALE checkpoint file behind that a later resume might act on."""
    bucket, key = split_s3(_range_status_path(dsql_schema, dsql_table))
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass


# =============================================================================
# PER-FILE STATUS (resume unit = one CSV part-file)
# =============================================================================
# Each file is loaded by exactly one worker; on completion the file's URI is durably
# recorded. On a later run we SKIP files already marked done and re-run ONLY the files that
# did not finish. Two lifecycle states per file:
#   - "started": a worker began this file this run (written before load) — used to
#     detect a mid-file crash so the reload runs with per-chunk commit-probe.
#   - "done":    the file fully committed (rows recorded) — skipped on resume.
# Read-modify-write under a lock; best-effort writes; recording is decoupled from the
# destructive reblank flag so a run with resume OFF still leaves a resumable trail.
# =============================================================================
def _file_status_path(dsql_schema, dsql_table):
    safe = f"{dsql_schema}.{dsql_table}".replace("/", "_")
    return f"{CONFIG_PREFIX}_file_status/{safe}.json"

def _load_file_status_doc(s3_client, dsql_schema, dsql_table):
    """Return the raw {files: {uri: {...}}} doc (empty skeleton if none/unreadable)."""
    bucket, key = split_s3(_file_status_path(dsql_schema, dsql_table))
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj['Body'].read().decode('utf-8'))
        if "files" not in doc or not isinstance(doc["files"], dict):
            return {"files": {}}
        return doc
    except Exception:
        return {"files": {}}

def load_file_status(s3_client, dsql_schema, dsql_table):
    """Return (done_map, started_set) from a PRIOR attempt (empty if resume disabled).
      done_map    : {file_uri: rows} for files that fully committed.
      started_set : {file_uri} for files that a prior run BEGAN but did not mark done
                    (i.e. crashed mid-file) — these need a probe-guarded reload."""
    if not AUTO_REBLANK_ON_RESUME:
        return {}, set()
    doc = _load_file_status_doc(s3_client, dsql_schema, dsql_table)
    done_map, started = {}, set()
    for uri, rec in doc.get("files", {}).items():
        if isinstance(rec, dict) and rec.get("state") == "done":
            done_map[uri] = int(rec.get("rows", 0))
        elif isinstance(rec, dict) and rec.get("state") == "started":
            started.add(uri)
    return done_map, started

def mark_file_started(s3_client, dsql_schema, dsql_table, file_uri):
    """Record (before load) that a worker began this file. If the run crashes mid-file,
    the next run sees state=started (not done) and reloads that file WITH the per-chunk
    commit-probe so already-committed chunks are skipped (no duplicates). Always writes
    (recording deletes nothing); best-effort."""
    _upsert_file_state(s3_client, dsql_schema, dsql_table, file_uri, "started", None)

def mark_file_done(s3_client, dsql_schema, dsql_table, file_uri, rows):
    """Durably record that a file fully committed. Read-modify-write under a lock
    (concurrent file workers on the same table serialize here). A failed write only costs
    re-loading that ONE file next attempt (idempotent via probe/reblank — no dup)."""
    _upsert_file_state(s3_client, dsql_schema, dsql_table, file_uri, "done", rows)

def _upsert_file_state(s3_client, dsql_schema, dsql_table, file_uri, state, rows):
    path = _file_status_path(dsql_schema, dsql_table)
    bucket, key = split_s3(path)
    with _RANGE_STATUS_LOCK:   # reuse the existing status lock (per-process serialization)
        try:
            doc = _load_file_status_doc(s3_client, dsql_schema, dsql_table)
            rec = {"state": state, "at": utc_now_iso()}
            if rows is not None:
                rec["rows"] = int(rows)
            doc["files"][file_uri] = rec
            doc["updated_at"] = utc_now_iso()
            s3_client.put_object(Bucket=bucket, Key=key,
                                 Body=json.dumps(doc).encode('utf-8'),
                                 ContentType='application/json')
        except Exception as e:
            print(f"  ⚠️ Could not record file state '{state}' for {file_uri} "
                  f"(non-fatal, will reload next attempt — idempotent): {e}")

def clear_file_status(s3_client, dsql_schema, dsql_table):
    """Delete a table's file-status file (after the table fully completes) so a future
    load starts clean. Best-effort; always runs (mirrors clear_range_status)."""
    bucket, key = split_s3(_file_status_path(dsql_schema, dsql_table))
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass


# ---- DSQL auth-token cache (reuse one bearer token across many connections) --------------
# ExpiresIn set well above a connection's max life (~59 min) so a cached token never expires
# mid-connection; refresh cadence keeps the token young enough that even a connection opened
# just before a refresh outlives it comfortably (see connect_dsql docstring for the math).
DSQL_TOKEN_EXPIRES_IN = 7200          # 2 h token lifetime (max allowed is 1 week)
DSQL_TOKEN_REFRESH_SECONDS = 30 * 60  # regenerate the cached token once it's older than 30 min
_dsql_token = None
_dsql_token_born = 0.0
_dsql_token_lock = threading.Lock()
_dsql_ssl_context = None
_dsql_ssl_lock = threading.Lock()

def _get_ssl_context():
    """One shared TLS context for all DSQL connections (thread-safe build once)."""
    global _dsql_ssl_context
    if _dsql_ssl_context is None:
        with _dsql_ssl_lock:
            if _dsql_ssl_context is None:
                _dsql_ssl_context = ssl.create_default_context()
    return _dsql_ssl_context

def _get_cached_dsql_token():
    """Return a cached DSQL IAM auth token, regenerating it only when older than
    DSQL_TOKEN_REFRESH_SECONDS. The token is a bearer credential (not connection-bound), so
    all connections opened in the window share it. Generation is a local SigV4 sign (no STS
    round-trip); caching just avoids repeating it hundreds of times under fan-out."""
    global _dsql_token, _dsql_token_born
    now = time.monotonic()
    with _dsql_token_lock:
        if _dsql_token is None or (now - _dsql_token_born) > DSQL_TOKEN_REFRESH_SECONDS:
            client = make_boto_client("dsql")   # thread-safe client creation
            _dsql_token = client.generate_db_connect_admin_auth_token(
                DSQL_ENDPOINT, Region=REGION, ExpiresIn=DSQL_TOKEN_EXPIRES_IN)
            _dsql_token_born = now
        return _dsql_token


def _invalidate_dsql_token():
    """Force the NEXT connect to mint a FRESH IAM auth token. Called whenever a connection
    fails/drops (SQLSTATE class 08, closed pipe, TLS drop). Without this, every reconnect
    reused the same cached token — so if the token/endpoint state was the problem, ALL
    reconnects (rebuild_conn + the pool) failed identically. Clearing the cache here lets
    each reconnect regenerate a token and actually self-heal (same fix as the CDC job)."""
    global _dsql_token, _dsql_token_born
    with _dsql_token_lock:
        _dsql_token = None
        _dsql_token_born = 0.0


class ConnPool:
    """A bounded, thread-safe pool of long-lived DSQL connections for ONE table's per-file
    fan-out. It exists to CUT CONNECTION CHURN: instead of a TLS handshake + IAM auth per
    part-file, a finished file worker returns its OPEN connection for the next file to reuse.

    PARALLELISM IS PRESERVED: the pool is sized to the worker count (inner_conc), so every
    concurrently-running worker gets its OWN connection — never two workers on one connection
    (pg8000 is not concurrency-safe). borrow() NEVER blocks: if the pool is empty it opens a
    fresh connection on demand (up to no hard cap — the caller's ThreadPoolExecutor already
    bounds concurrency to inner_conc, and the global _DSQL_RANGE_WRITER_SEM bounds it across
    tables). So the pool only ever holds <= inner_conc idle connections; reuse happens in
    TIME (slot picks up its next file) not by sharing across concurrent workers.

    Recycle-safe: give_back stores the connection's age; borrow() proactively recycles one
    that has passed CONN_RECYCLE_SECONDS (closes + reopens) so a reused connection can never
    outlive DSQL's ~60-min limit. A connection returned after a broken-pipe rebuild carries
    its fresh age, so this stays correct across rebuilds."""

    def __init__(self):
        self._idle = []            # list of (conn, born_monotonic)
        self._lock = threading.Lock()

    def borrow(self):
        """Return (conn, born_monotonic) — a reused idle connection (recycled if it aged out)
        or a freshly opened one. Never blocks."""
        while True:
            with self._lock:
                item = self._idle.pop() if self._idle else None
            if item is None:
                return connect_dsql(), time.monotonic()   # pool empty -> open on demand
            conn, born = item
            if time.monotonic() - born > CONN_RECYCLE_SECONDS:
                # Aged out: close and loop to get/open another (never hand back a stale conn).
                try:
                    conn.close()
                except Exception:
                    pass
                continue
            return conn, born

    def give_back(self, conn, born):
        """Return an OPEN connection to the pool for reuse (caller must have committed/rolled
        back any txn first — load_one_table's finally runs after the file is fully done)."""
        with self._lock:
            self._idle.append((conn, born))

    def close_all(self):
        """Close every idle connection (call once the table's fan-out is fully done)."""
        with self._lock:
            items, self._idle = self._idle, []
        for conn, _ in items:
            try:
                conn.close()
            except Exception:
                pass


def connect_dsql():
    """Open a fresh authenticated pg8000 connection to DSQL from the driver.

    The IAM auth token is a BEARER credential valid for its whole ExpiresIn window (not tied
    to one connection), and generation is a LOCAL SigV4 signing operation. So instead of
    re-signing on every connect (hundreds of times at 30-way fan-out across tables), we CACHE
    one token and reuse it for all connections until it nears refresh age. The token must
    outlive any connection it opens: a connection lives up to CONN_RECYCLE_SECONDS (~54 min)
    plus a final ~5-min chunk (~59 min), so ExpiresIn is set well above that (2 h) and the
    cache refreshes every DSQL_TOKEN_REFRESH_SECONDS (~30 min). Worst case a connection opened
    at token-age 30 min lives ~59 min => token used to ~89 min < 120-min expiry. Safe margin.

    REACTIVE SELF-HEAL AT OPEN: the connect is retried with bounded backoff; on a connection-
    class failure (08006 unable-to-connect, dropped/closed pipe, TLS drop) the cached token is
    INVALIDATED so each retry mints a FRESH token. A stale/expired cached token can otherwise
    make every reconnect fail identically (the 08006 stall). On exhaustion the last error is
    raised so the caller's existing per-chunk/per-table guard isolates it."""
    _last = None
    for _attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            return pg8000.connect(
                host=DSQL_ENDPOINT, port=5432, database=DSQL_DATABASE,
                user=DSQL_USER, password=_get_cached_dsql_token(),
                ssl_context=_get_ssl_context()
            )
        except Exception as e:
            _last = e
            if is_broken_pipe_error(e) or is_transient_server_error(e):
                _invalidate_dsql_token()   # next attempt gets a fresh token
            if _attempt < CONNECT_MAX_RETRIES:
                time.sleep(server_backoff_seconds(_attempt))
    raise _last


def pre_clean_timestamp(c):
    """Return an expr that truncates fractional seconds to 6 digits (Spark/DSQL max) but
    KEEPS any trailing timezone offset (so normalize_timestamp can CONVERT it to UTC). Only
    the >6 fractional-second digits are removed here — purely subtractive, never shifts."""
    expr = col(c)
    expr = regexp_replace(expr, r'(\.\d{6})\d+', r'$1')
    return expr


def _strip_offset_expr(c):
    """Expr that removes a trailing TZ offset/Z/UTC (for OFFSET-LESS parsing fallback only)."""
    expr = regexp_replace(c, r'(\d{2}:\d{2}:\d{2}(\.\d+)?)[+-]\d{2}(:?\d{2})?$', r'$1')
    expr = regexp_replace(expr, r'(\s+[+-]\d{2}(:?\d{2})?|\s*Z|\s+UTC)\s*$', '')
    return expr


# Offset-AWARE parse formats (value carries a trailing ' +HH:MM' / ' -HHMM' / 'Z'). Spark's
# to_timestamp with an offset token (XXX / Z) converts to the correct instant; with the
# SESSION TZ pinned to UTC (spark.sql.session.timeZone=UTC, set at job start) the emitted
# UTC wall-clock is the true instant. This FIXES the previous bug where the offset was
# stripped and the local wall-clock stored as if it were UTC (wrong instant).
TIMESTAMP_TZ_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS XXX",
    "yyyy-MM-dd HH:mm:ss XXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS XXX",
    "yyyy-MM-dd HH:mm:ss.SSSSSSXXX",
    "yyyy-MM-dd HH:mm:ssXXX",
]


def normalize_timestamp(column_name, formats, emit_pattern):
    cleaned = pre_clean_timestamp(column_name)   # 6-digit frac, offset PRESERVED
    # 1) try offset-AWARE parse first (converts to true UTC instant when an offset is present)
    tz_attempts = [to_timestamp(cleaned, fmt) for fmt in TIMESTAMP_TZ_FORMATS]
    # 2) then offset-LESS parse (bare local/UTC values with no offset token)
    stripped = _strip_offset_expr(cleaned)
    plain_attempts = [to_timestamp(stripped, fmt) for fmt in formats]
    parsed = coalesce(*(tz_attempts + plain_attempts))
    return when(parsed.isNotNull(), date_format(parsed, emit_pattern)).otherwise(lit(None))


def read_dms_csv(dms_s3_path, dms_has_headers, file_subset=None):
    """
    V3: read the DMS CSV with multiline + explicit quote/escape so quoted JSON
    fields containing raw newlines / commas / quotes are parsed as single fields.
    This is the fix for the misaligned-column uuid error.

    V6 CHUNK FAN-OUT: when file_subset (a non-empty list of full s3:// object paths) is
    given, read ONLY those explicit part-files instead of the whole prefix. Spark's
    DataFrameReader.csv() accepts a list of paths, so each fan-out worker reads a
    DISJOINT subset — the union of all subsets == the full prefix, exactly once. All
    reader OPTIONS are identical to the prefix read (same parsing/quoting), so per-file
    parsing is byte-for-byte the same as the whole-table read.
    """
    reader = spark.read
    for k, v in CSV_READ_OPTIONS.items():
        if k == "header":
            reader = reader.option("header", str(dms_has_headers).lower())
        elif v is not None:
            reader = reader.option(k, v)
    if file_subset:
        # Explicit list of part-files (chunk fan-out worker). Spark accepts *paths.
        return reader.csv(list(file_subset))
    # WHOLE-PREFIX read: restrict to full-load part-files ONLY. recursiveFileLookup=true
    # descends into subfolders (needed for DMS parallel-load nested partitions), but that
    # ALSO reaches the CDC processed/ + failed/ archives that share this table prefix. Those
    # CDC CSVs have a DIFFERENT layout (a leading 'Op' column -> 16 cols vs the 15-col LOAD
    # file), so unioning them shifts every column by one and silently corrupts the load
    # (the exact nfl_data/nfl_stadium/sport_location failure). DMS full-load files are always
    # named LOAD*.csv (LOAD00000001.csv, ... LOAD0000000F.csv — verified), and CDC files are
    # timestamp-named, so pathGlobFilter="LOAD*.csv" matches every full-load part and NEVER a
    # CDC file. (pathGlobFilter matches the FILENAME only, so it works alongside recursion.)
    reader = reader.option("pathGlobFilter", "LOAD*.csv")
    return reader.csv(dms_s3_path)


def assert_not_cdc_layout(df, dsql_schema, dsql_table):
    """Q1 ANTI-SILENT-CORRUPTION BACKSTOP. A DMS full-load CSV never has an 'Op' column;
    a CDC CSV ALWAYS leads with 'Op' (I/U/D). If a CDC file is ever read as full-load input
    (e.g. a stale processed/ archive under the table prefix, or a mis-scoped read), its extra
    leading 'Op' column shifts every value by one and silently corrupts the load while the
    row-count check still 'passes'. This is version-independent and targets that exact
    signature: if the parsed header contains an 'Op' column (case-insensitive) that the
    target does not expect, FAIL LOUD rather than load shifted data. Complements
    pathGlobFilter='LOAD*.csv' (which should already exclude CDC files) as defense-in-depth."""
    cols_lower = [c.lower() for c in df.columns]
    # PRECISE CDC signature: a DMS CDC CSV ALWAYS leads with the 'Op' column (I/U/D). Check
    # the FIRST column only — not "any column named op" — so a legitimate table with a
    # mid-schema column happening to be named 'op' is not a false positive.
    if cols_lower and cols_lower[0] == "op":
        raise Exception(
            f"CDC-LAYOUT GUARD [{dsql_schema}.{dsql_table}]: the full-load read produced a "
            f"leading 'Op' column ({df.columns[:3]}...) — that is a CDC file layout, NOT "
            f"full-load. A CDC CSV (I/U/D) must never be loaded as full-load data (its "
            f"leading Op column shifts every value by one and would silently corrupt the "
            f"table). Cause is usually a stale CDC file (processed/ archive or a "
            f"timestamp-named CDC CSV) under the table's S3 prefix being read by the "
            f"full-load. Purge stale CDC files from the table prefix (or verify "
            f"pathGlobFilter='LOAD*.csv' / _is_full_load_key) and re-run.")


def blank_whole_table(dsql_schema, dsql_table, pk_col=None, batch=2000, any_col=None):
    """V8 CROSS-ATTEMPT RESUME: blank an ENTIRE target table in DSQL-limit-safe batches
    (DSQL has no TRUNCATE; ~3000 rows / 5-min per txn). Deletes ALL rows by repeatedly
    removing a bounded window and committing each batch, so a large partially-loaded
    table can be cleared without blowing the per-transaction limits.

    Method: DELETE the rows whose key is in the next `batch`-sized ORDER BY window
    (DELETE ... WHERE col IN (SELECT col ... ORDER BY col LIMIT batch)). With a single-
    column PK we page by it (index-usable, unique). Without one we page by `any_col`
    (a non-key column also works: each pass deletes the rows sharing the <= batch key
    values selected, so a pass may delete slightly more than `batch` on duplicate values,
    but it stays bounded to that key set and the loop terminates when COUNT hits 0). DSQL
    has NO ctid, so we never use it. Returns total rows deleted.

    SAFETY: caller (assert_empty_or_register under AUTO_REBLANK_ON_RESUME) only invokes
    this for a table THIS pipeline previously attempted; it never runs against an
    untouched customer table. Idempotent: re-running finds fewer/zero rows. Retries
    OCC/XX000/pipe per batch (bounded)."""
    batch = max(MIN_CHUNK_SIZE, min(int(batch), DSQL_MAX_ROWS_PER_TXN))
    key = pk_col or any_col
    if not key:
        raise Exception(
            f"blank_whole_table [{dsql_schema}.{dsql_table}]: no column to page by "
            f"(need pk_col or any_col). Cannot safely batch-delete — refuse rather than "
            f"risk an unbounded DELETE exceeding the DSQL per-transaction limit.")
    col = f'"{key}"'
    conn = connect_dsql()
    conn.autocommit = False
    total = 0
    try:
        while True:
            occ_attempt = server_attempt = pipe_attempt = 0
            while True:
                try:
                    c = conn.cursor()
                    try:
                        c.execute(
                            f'DELETE FROM {dsql_schema}.{dsql_table} '
                            f'WHERE {col} IN (SELECT {col} FROM {dsql_schema}.{dsql_table} '
                            f'ORDER BY {col} LIMIT {int(batch)})')
                        deleted = c.rowcount or 0
                        conn.commit()
                    finally:
                        try:
                            c.close()
                        except Exception:
                            pass
                    break
                except Exception as e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    if is_occ_conflict(e) and occ_attempt < OCC_MAX_RETRIES:
                        occ_attempt += 1
                        time.sleep(occ_backoff_seconds(occ_attempt))
                        continue
                    if is_transient_server_error(e) and server_attempt < SERVER_MAX_RETRIES:
                        server_attempt += 1
                        time.sleep(server_backoff_seconds(server_attempt))
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = connect_dsql()
                        conn.autocommit = False
                        continue
                    if is_broken_pipe_error(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                        pipe_attempt += 1
                        time.sleep(CHUNK_RETRY_BACKOFF_SECONDS * pipe_attempt)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        _invalidate_dsql_token()   # drop => fresh token (heals 08006)
                        conn = connect_dsql()
                        conn.autocommit = False
                        continue
                    raise
            total += deleted
            if deleted == 0:
                break
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return total


def assert_empty_or_register(dsql_schema, dsql_table, resume_ok=False, pk_col=None,
                             any_col=None):
    """EMPTY-TARGET SAFETY BARRIER — the ONE place the whole-table empty check lives.

    Verifies the target table is empty in DSQL and records it as empty-verified for
    THIS Glue attempt (in _EMPTY_VERIFIED). Every load path (V5 whole-table, V6 range-
    parallel, any future chunk path) MUST pass through here exactly once per table
    before writing. Idempotent within an attempt: if already verified empty in this
    attempt, it returns immediately without another round-trip.

    Raises if the target is NOT empty — refusing to append/overwrite. On a Glue retry
    (fresh JVM) the registry is empty again, so a partially-loaded big table will fail
    here until it is re-blanked (customer-owned step). This is the enforcement of the
    'empty-at-start required on every attempt; no cross-attempt auto-resume' contract.
    """
    key = f"{dsql_schema}.{dsql_table}"
    with _EMPTY_VERIFIED_LOCK:
        if key in _EMPTY_VERIFIED:
            return
    guard_conn = connect_dsql()
    guard_cursor = guard_conn.cursor()
    try:
        guard_cursor.execute(
            f"SELECT EXISTS (SELECT 1 FROM {dsql_schema}.{dsql_table} LIMIT 1)")
        target_not_empty = bool(guard_cursor.fetchone()[0])
    except Exception as e:
        raise Exception(f"Failed to check emptiness for {dsql_schema}.{dsql_table}: {e}")
    finally:
        try:
            guard_cursor.close()
            guard_conn.close()
        except Exception:
            pass
    if target_not_empty:
        # if the caller confirmed this table was PREVIOUSLY ATTEMPTED by this pipeline
        # (in_progress/failed marker) AND AUTO_REBLANK_ON_RESUME is on, auto-clear the whole
        # table (batched, DSQL-safe) and proceed instead of failing. NO-DUP/NO-LOSS: the
        # full reblank removes every partial/prior row, then the normal load + source-count
        # gate brings the table to EXACTLY the source count. NEVER runs for a table without
        # a prior marker (resume_ok=False), so pre-existing customer data is never deleted.
        if resume_ok and AUTO_REBLANK_ON_RESUME:
            print(f"  ♻ RESUME [{dsql_schema}.{dsql_table}]: target non-empty AND "
                  f"previously-attempted by this pipeline -> auto-reblanking whole table "
                  f"(batched DELETE) then reloading from scratch (no-dup/no-loss via "
                  f"source-count gate). pk_col={pk_col!r}")
            removed = blank_whole_table(dsql_schema, dsql_table, pk_col=pk_col,
                                        any_col=any_col)
            print(f"  ♻ RESUME [{dsql_schema}.{dsql_table}]: cleared {removed:,} prior "
                  f"row(s); table now empty for a clean reload")
            # Re-verify empty after the reblank before registering (defensive).
            vconn = connect_dsql(); vcur = vconn.cursor()
            try:
                vcur.execute(
                    f"SELECT EXISTS (SELECT 1 FROM {dsql_schema}.{dsql_table} LIMIT 1)")
                still_not_empty = bool(vcur.fetchone()[0])
            finally:
                try:
                    vcur.close(); vconn.close()
                except Exception:
                    pass
            if still_not_empty:
                raise Exception(
                    f"RESUME reblank FAILED for {dsql_schema}.{dsql_table}: table still "
                    f"not empty after batched DELETE. Refusing to reload onto residual "
                    f"rows (would risk duplicates). Investigate / manually blank + re-run.")
            with _EMPTY_VERIFIED_LOCK:
                _EMPTY_VERIFIED.add(key)
            return
        raise Exception(
            f"Target {dsql_schema}.{dsql_table} is NOT empty. This pipeline expects "
            f"tables to be blanked before a bulk load — refusing to append duplicates. "
            f"Aurora DSQL does NOT support TRUNCATE: blank the table with "
            f"'DELETE FROM {dsql_schema}.{dsql_table};' (note DSQL's ~3,000 rows / "
            f"10 MiB / 5-min per-transaction limits — a large table needs batched "
            f"DELETEs), or DROP and recreate the table from its DDL. This is a manual, "
            f"customer-owned step — the job never deletes/drops data itself unless "
            f"AUTO_REBLANK_ON_RESUME is enabled AND this table was previously attempted "
            f"(V8 cross-attempt resume). Re-blank, then re-run — or enable "
            f"--auto_reblank_on_resume=true to auto-resume previously-attempted tables."
        )
    with _EMPTY_VERIFIED_LOCK:
        _EMPTY_VERIFIED.add(key)


def _sql_str_literal(s):
    """Single-quote-escaped SQL string literal for a text/hex bound (no driver params
    used in these dynamically-built range queries)."""
    return "'" + str(s).replace("'", "''") + "'"


def hex_to_canonical_uuid(h):
    """32-hex (no dashes, lowercase) -> canonical 8-4-4-4-12 uuid string. Used to build
    INDEX-USABLE bare-uuid bounds ('<canon>'::uuid) for the uuid-PK range DELETE/SELECT,
    so DSQL compares against the native uuid PK column (index-usable) instead of a
    functional expression lower(replace(id::text,'-','')) (which forces a full scan).
    Returns None if h is not exactly 32 hex digits (e.g. the plan_ranges_hex TOP sentinel
    max+1, which can be 33 hex digits and has NO uuid representation — the caller omits
    the upper bound for that range instead of casting it)."""
    if h is None:
        return None
    s = str(h).strip().lower().replace("-", "")
    if len(s) != 32 or any(ch not in "0123456789abcdef" for ch in s):
        return None
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


def blank_pk_range(conn, dsql_schema, dsql_table, pk_col, lo, hi, batch=2000,
                   pk_kind="integer", is_top=False):
    """[retired range path] Idempotently DELETE one PK range [lo,hi) in DSQL-safe
    batches (no TRUNCATE), so a re-run replaces only its own rows. Refuses unless the
    table was empty-verified this attempt."""
    key = f"{dsql_schema}.{dsql_table}"
    with _EMPTY_VERIFIED_LOCK:
        verified = key in _EMPTY_VERIFIED
    if not verified:
        raise Exception(
            f"REFUSING range DELETE on {key}: table was not empty-verified for this "
            f"attempt. blank_pk_range is a within-attempt resume mechanism only and "
            f"must never run against a target whose emptiness at attempt-start was not "
            f"proven (would risk deleting pre-existing customer data). This indicates a "
            f"code path reached a range write without going through "
            f"load_one_table_parallel's empty gate — a bug, not a data condition.")
    # DELETE BY PRIMARY KEY, NOT ctid. Aurora DSQL is a distributed,
    # serverless engine with no PostgreSQL heap storage, so it does NOT expose the
    # physical `ctid` system column. Instead we KEYSET-page the PK: each batch selects up
    # to `batch` PK values in the range (ORDER BY pk LIMIT batch — LIMIT in a SUBQUERY),
    # deletes exactly that window, then advances the cursor STRICTLY PAST the largest key
    # deleted. Bounded per transaction (<= batch rows), walks forward, terminates.
    #
    # MULTI-KIND (pk_kind): the PK comparison EXPRESSION and bound LITERALS differ by kind
    # and MUST mirror the Spark-side range filter so a row is in the SAME range on both:
    #   integer -> pk_expr = "pk"                         ; bounds are int literals
    #   uuid    -> pk_expr = "pk" (BARE, native uuid col) ; bounds are canonical uuid
    #              literals cast '<canon>'::uuid. INDEX-USABLE: comparing the native uuid
    #              PK column directly uses the PK index. Native uuid ordering ==
    #              dash-stripped-lowercase-hex order == byte order (zero-padded 32-hex sorts
    #              identically), so the same boundaries the Spark hex filter uses partition
    #              rows identically. The plan_ranges_hex TOP sentinel (max+1, possibly 33
    #              hex digits) has NO uuid representation, so that range OMITs its upper
    #              bound (hi_has_ceiling=False).
    #   text    -> pk_expr = "pk"                         ; bounds are quoted string lits
    #              (byte/C ordering assumed to match Spark code-point order; ASCII-safe.)
    # TOP-range ceiling omission is driven by the POSITIONAL is_top flag (which knows the
    # last range in the ordered list) — NOT by sniffing the hi bound. An interior text
    # boundary can legitimately end in '\x00', and sniffing would misread it as the top
    # range -> drop its ceiling -> overlap later ranges -> DUPLICATE loads.
    hi_has_ceiling = not is_top
    # agg_k: expression the window's MAX() aggregates over. DSQL/Postgres has NO
    # max(uuid) aggregate, so for uuid we aggregate the canonical TEXT form (which sorts
    # identically to the uuid byte order) and cast the result back to ::uuid via fmt_bound.
    agg_k = "_k"
    if pk_kind == "uuid":
        pk_expr = f'"{pk_col}"'                      # BARE native uuid column (indexable)
        agg_k = "_k::text"                           # no max(uuid) -> aggregate as text
        # lo is always a real 32-hex key bound -> a valid canonical uuid.
        lo_lit = f"{_sql_str_literal(hex_to_canonical_uuid(lo))}::uuid"
        if hi_has_ceiling:
            _hi_canon = hex_to_canonical_uuid(hi)
            if _hi_canon is None:
                # Defensive: an interior hi should always be a valid 32-hex -> canonical.
                # If it isn't (planner contract broken), fail loud rather than silently
                # drop the ceiling (which would overlap later ranges).
                raise Exception(
                    f"RANGE CLEAR [{dsql_schema}.{dsql_table}]: interior uuid range hi "
                    f"{hi!r} is not a valid 32-hex bound — cannot build an index-usable "
                    f"::uuid ceiling. This indicates a plan_ranges_hex contract violation.")
            hi_lit = f"{_sql_str_literal(_hi_canon)}::uuid"
        else:
            # TOP range: out-of-space sentinel (max+1) has no uuid form -> no ceiling; the
            # max stored uuid is the natural top (owned here exactly once).
            hi_lit = None
        # window_max comes back as TEXT (canonical uuid str); render it canonical::uuid.
        fmt_bound = lambda v: f"{_sql_str_literal(str(v))}::uuid"   # noqa: E731
    elif pk_kind == "text":
        pk_expr = f'"{pk_col}"'
        lo_lit = _sql_str_literal(lo)
        # TOP range (is_top): its exclusive upper bound is the max_str + '\x00' sentinel;
        # a raw NUL byte cannot appear in a Postgres text literal (breaks the wire protocol
        # -> 08P01) and no stored string exceeds max_str under byte order -> ceiling omitted
        # (hi_has_ceiling already False via is_top). Interior bounds are ordinary strings.
        # DEFENSIVE: an interior text bound must not contain a NUL (would corrupt the
        # literal); the positional is_top means only the genuine last range is ceilingless,
        # so any NUL in an interior hi signals a planner contract violation -> fail loud.
        if hi_has_ceiling:
            if isinstance(hi, str) and "\x00" in hi:
                raise Exception(
                    f"RANGE CLEAR [{dsql_schema}.{dsql_table}]: interior text range hi "
                    f"{hi!r} contains a NUL byte — cannot build a safe text literal. This "
                    f"indicates a plan_ranges_text contract violation (only the TOP range "
                    f"should carry the '\\x00' sentinel).")
            hi_lit = _sql_str_literal(hi)
        else:
            hi_lit = None
        fmt_bound = _sql_str_literal
    else:
        pk_expr = f'"{pk_col}"'
        lo_lit, hi_lit = str(int(lo)), str(int(hi))
        fmt_bound = lambda v: str(int(v))     # noqa: E731 (int literal)

    # keep each DELETE batch under the DSQL per-transaction ROW cap, and
    # time-guard + adaptively shrink it (mirrors insert_one_chunk) so a wide/dense range
    # can't blow the 5-min (300s) txn-age limit on the clear step.
    batch = max(MIN_CHUNK_SIZE, min(int(batch), DSQL_MAX_ROWS_PER_TXN))
    total = 0
    # cur_lo_lit is the CURRENT lower bound literal; cur_lo_incl says whether it's an
    # inclusive (>=) or exclusive (>) comparison. First window is inclusive of `lo`;
    # every subsequent window starts strictly AFTER the previous window_max (works for
    # all kinds without computing a type-specific successor).
    cur_lo_lit = lo_lit
    cur_lo_incl = True

    # a transient/pipe retry inside _clear_batch REBINDS `conn` to a freshly-opened
    # connection (via `nonlocal conn`). To avoid leaking that handle, blank_pk_range OWNS
    # the connection lifecycle: the whole paging loop runs under try/finally and closes
    # whatever `conn` ends up being.
    def _clear_batch(lo_lit_arg, lo_incl, size):
        """DELETE up to `size` rows with (pk >= lo OR pk > lo) AND pk < hi, by PK value.
        Returns (deleted_count, max_pk_deleted_literal_or_None). Retries OCC / XX000 /
        broken-pipe (reconnecting via a fresh cursor on `conn`)."""
        nonlocal conn
        occ_attempt = 0
        server_attempt = 0
        pipe_attempt = 0
        lo_cmp = ">=" if lo_incl else ">"
        while True:
            try:
                c = conn.cursor()
                try:
                    # Upper bound of this window: the (size)-th key from the low bound.
                    # For the TOP uuid range the exclusive ceiling is an out-of-space
                    # sentinel with no uuid literal -> omit the '< hi' clause entirely
                    # (the max stored key is the natural top, owned by this range).
                    _hi_clause = f' AND {pk_expr} < {hi_lit}' if hi_has_ceiling else ''
                    c.execute(
                        f'SELECT MAX({agg_k}) FROM ('
                        f'  SELECT {pk_expr} AS _k FROM {dsql_schema}.{dsql_table} '
                        f'  WHERE {pk_expr} {lo_cmp} {lo_lit_arg}{_hi_clause} '
                        f'  ORDER BY {pk_expr} LIMIT {int(size)}) AS _w'
                    )
                    row = c.fetchone()
                    window_max = row[0] if row else None
                    if window_max is None:
                        conn.commit()          # nothing left in the range
                        return (0, None)
                    wmax_lit = fmt_bound(window_max)
                    # Delete the inclusive window [lo, window_max] (<= size rows).
                    c.execute(
                        f'DELETE FROM {dsql_schema}.{dsql_table} '
                        f'WHERE {pk_expr} {lo_cmp} {lo_lit_arg} AND {pk_expr} <= {wmax_lit}'
                    )
                    deleted = c.rowcount or 0
                    conn.commit()
                    return (deleted, wmax_lit)
                finally:
                    try:
                        c.close()
                    except Exception:
                        pass
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if is_occ_conflict(e) and occ_attempt < OCC_MAX_RETRIES:
                    occ_attempt += 1
                    time.sleep(occ_backoff_seconds(occ_attempt))
                    continue
                if is_transient_server_error(e) and server_attempt < SERVER_MAX_RETRIES:
                    server_attempt += 1
                    wait = server_backoff_seconds(server_attempt)
                    print(f"    ↻ range-clear DSQL server unavailable (XX000); reconnect "
                          f"+ retry {server_attempt}/{SERVER_MAX_RETRIES} after {wait:.2f}s")
                    time.sleep(wait)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = connect_dsql()
                    conn.autocommit = False
                    continue
                if is_broken_pipe_error(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                    pipe_attempt += 1
                    time.sleep(CHUNK_RETRY_BACKOFF_SECONDS * pipe_attempt)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    _invalidate_dsql_token()   # drop => fresh token (heals 08006)
                    conn = connect_dsql()
                    conn.autocommit = False
                    continue
                raise

    try:
        while True:
            t0 = time.monotonic()
            deleted, wmax_lit = _clear_batch(cur_lo_lit, cur_lo_incl, batch)
            if wmax_lit is None:
                break                              # range fully cleared
            # `total` can UNDER-count under a commit-ambiguous retry — if a DELETE durably
            # applied but its commit() threw, the retry re-SELECTs, finds those rows gone,
            # and re-deletes 0, so the first attempt's rows aren't added to `total`. The
            # RANGE IS STILL FULLY CLEARED (what matters); `total` is only for the log line
            # below and must NOT be trusted as an exact count.
            total += deleted
            # Keyset advance: next window starts STRICTLY AFTER this window_max (kind-
            # agnostic — no successor computation; the '>' comparison handles it).
            cur_lo_lit = wmax_lit
            cur_lo_incl = False
            # Adaptive shrink if a batch got close to the txn-age limit.
            elapsed = time.monotonic() - t0
            _batch_trigger = effective_batch_max_seconds()
            if elapsed > _batch_trigger and batch > MIN_CHUNK_SIZE:
                # PROPORTIONAL (throughput-based, not blind halving): this DELETE batch took
                # `elapsed`s for `batch` rows, so size the next to ~90% of the trigger:
                # batch * (0.9*trigger/elapsed). Floored at MIN_CHUNK_SIZE, forced smaller.
                new_batch = int(batch * (TIME_TARGET_FRACTION * _batch_trigger) / max(elapsed, 0.001))
                new_batch = min(new_batch, batch - 1)
                new_batch = max(MIN_CHUNK_SIZE, new_batch)
                print(f"    ⏱ range-clear batch took {elapsed:.0f}s (> {_batch_trigger}s); "
                      f"shrinking delete batch {batch} -> {new_batch} (throughput-proportional)")
                batch = new_batch
    finally:
        # Close whatever connection we ended up holding (original OR reconnected).
        try:
            conn.close()
        except Exception:
            pass
    return total


def load_one_table(s3_client, entry, pk_range=None, file_subset=None, config=None,
                   probe_all_chunks=False, conn_pool=None):
    """Load a single table end-to-end. Returns a result dict.

    conn_pool (optional): a ConnPool the write path borrows its DSQL connection FROM and
    returns it TO (still open) instead of opening a fresh connection and closing it at the
    end. This is the connection-REUSE lever for the per-file fan-out: N files then share a
    small set of long-lived connections instead of paying a TLS handshake + IAM auth per
    file. No-loss/no-dup is unaffected — each file still commits independently; only the
    connection's ORIGIN (pool vs fresh) and DISPOSAL (returned vs closed) change. If None,
    behavior is exactly as before (open one fresh connection, close it at the end).

    V6: pk_range is None for the ORIGINAL V5 whole-table behavior (unchanged — used by
    every normal/small/non-span table). When pk_range=(pk_col, lo, hi) is given (the
    range-parallel path for a large span_recoverable table), this loads ONLY the rows
    with lo <= pk_col < hi:
      - the DataFrame is FILTERED to the range right after read,
      - the whole-table empty-target guard is REPLACED by an idempotent range-clear
        (DELETE WHERE pk_col >= lo AND pk_col < hi, batched) so a re-run/failed range
        cleanly replaces ONLY its own rows (no dupes, no ON CONFLICT, no schema change),
      - per-range POST-LOAD VALIDATION IS SKIPPED here; the orchestrator validates the
        whole table ONCE after all ranges complete (COUNT(*) == sum of range rows, etc).

    V6 CHUNK FAN-OUT: when file_subset (list of full s3:// part-file paths) is given
    (large NON-numeric-PK table, one worker), this loads ONLY those part-files:
      - the CSV read is restricted to that DISJOINT file subset (no row overlap across
        workers, so no duplicate rows and no ON CONFLICT / schema change needed),
      - the whole-table empty gate is NOT done here — the orchestrator
        (load_one_table_chunked) runs assert_empty_or_register ONCE before launching
        workers; no per-worker DELETE happens (recovery is whole-table reload),
      - per-worker POST-LOAD VALIDATION IS SKIPPED; the orchestrator validates the whole
        table ONCE after all workers finish.
    pk_range and file_subset are MUTUALLY EXCLUSIVE (numeric-PK range vs file fan-out).
    """
    if pk_range is not None and file_subset is not None:
        raise Exception("load_one_table: pk_range and file_subset are mutually exclusive.")

    # The per-table config JSON is IMMUTABLE, so accept an already-parsed `config` from the
    # caller (load_table_auto reads it once for routing; the orchestrators pass it to every
    # worker). Only GET+parse it from S3 when not supplied, avoiding N redundant identical
    # S3 reads (1 per range/worker) for a large table.
    if config is None:
        cfg_bucket, cfg_key = split_s3(entry['config_s3_path'])
        response = s3_client.get_object(Bucket=cfg_bucket, Key=cfg_key)
        config = json.loads(response['Body'].read().decode('utf-8'))

    dms_s3_path = config['metadata']['dms_s3_path']
    dsql_schema = config['metadata']['dsql_schema']
    dsql_table = config['metadata']['dsql_table']
    target_columns = config['target_columns']
    type_categories = config['type_categories']
    column_mapping = config['column_mapping']
    skip_columns = config.get('skip_columns', [])
    dms_has_headers = config['metadata'].get('dms_has_headers', False)
    expected_col_count = config['metadata']['dms_column_count']

    # per-column varchar max length (from Job 1's DSQL schema read). Used by the
    # length guard to catch a value longer than the target varchar(N) BEFORE the insert
    # (which would otherwise fail with a raw DSQL 22001 "value too long" at row 0).
    # Only varchar-category columns with a positive max_length are enforced; text/other
    # types have no limit. character_maximum_length counts CHARACTERS (matches DSQL).
    varchar_max_len = {}
    for _em in column_mapping:
        if _em.get('action') == 'map':
            _tc = _em.get('target_column')
            _ml = _em.get('max_length')
            if _tc and isinstance(_ml, int) and _ml > 0 and _em.get('type_category') == 'varchar':
                varchar_max_len[_tc] = _ml

    print(f"    Source: {dms_s3_path}")
    print(f"    Target: {dsql_schema}.{dsql_table} ({len(target_columns)} cols, "
          f"{len(skip_columns)} skipped)")

    # ---- header-consistency preflight ----
    # The confirmed DMS endpoint has addColumnName=true, so it WROTE a header row. If a
    # table's Job-1 config says dms_has_headers=False, the reader would treat the header row
    # as DATA and shift every column by one (silent corruption). Warn (not hard-fail: a
    # future endpoint could legitimately set addColumnName=false; the column-count check +
    # uuid guard below still catch real misalignment).
    if not dms_has_headers:
        print(f"    ⚠️ HEADER MISMATCH WARNING: config dms_has_headers=False, but the "
              f"DMS S3 endpoint writes headers (addColumnName=true). If the CSV really "
              f"has a header row it will be read as DATA and shift columns. Re-check "
              f"Job 1 discovery for {dsql_table} if the load fails validation.")

    # ---- Read DMS data (multiline-safe reader) ----
    # per-file fan-out: file_subset restricts the read to this worker's disjoint
    # part-files; None = whole prefix.
    df = read_dms_csv(dms_s3_path, dms_has_headers, file_subset=file_subset)
    assert_not_cdc_layout(df, dsql_schema, dsql_table)   # Q1 backstop: reject CDC-layout input
    if file_subset is not None:
        print(f"    ⑃ chunk-fanout worker: reading {len(file_subset)} part-file(s) "
              f"of {dsql_schema}.{dsql_table}")

    # repartition the just-read DataFrame so a single non-splittable multiLine file (one
    # Spark partition) is spread across executors. Without this, one big DMS file = one read
    # task = the parse/filter (and every range's toLocalIterator) serialize on one core, so
    # only ONE session commits at a time regardless of connections/inner_concurrency. N =
    # PER-TABLE parallel-connection setting (one read partition per concurrent range writer).
    # Keeps multiLine=true (embedded-newline parsing intact -> no row-split).
    if READ_REPARTITION_ENABLED:
        try:
            _nparts = max(READ_REPARTITION_MIN, int(PER_TABLE_WRITE_CONCURRENCY))
            _before = df.rdd.getNumPartitions()
            if _before < _nparts:
                df = df.repartition(_nparts)
                print(f"    ⇉ read-repartition {dsql_schema}.{dsql_table}: "
                      f"{_before} -> {_nparts} partitions "
                      f"(= per-table write concurrency {PER_TABLE_WRITE_CONCURRENCY}) "
                      f"to spread the multiLine single-file read across executors",
                      flush=True)
        except Exception as _rp_e:
            print(f"    ⚠️ read-repartition skipped ({_rp_e}); using file-count partitioning")

    # RANGE FILTER: when loading a single PK range, keep only rows in [lo, hi).
    # The pk column in the CSV is the mapped DMS column name; we filter on the DMS
    # column BEFORE renames/casts (the raw value is a numeric-PK string, so cast to
    # long for a correct numeric comparison — not a lexical string compare). Half-open
    # [lo, hi) matches plan_ranges so no row is in two ranges or none.
    if pk_range is not None:
        _pk_col, _lo, _hi, _pk_kind, _is_top = pk_range
        # Resolve the DMS/source column name for the target PK column (case-insensitive),
        # so the filter works against the df's current (pre-rename) column names.
        _pk_df_col = None
        for _em in column_mapping:
            if _em.get('action') == 'map' and \
               (_em.get('target_column', '').lower() == _pk_col.lower()):
                _pk_df_col = _em.get('dms_column_name')
                break
        if _pk_df_col is None:
            raise Exception(
                f"RANGE LOAD [{dsql_schema}.{dsql_table}]: PK column {_pk_col!r} not found "
                f"in the column mapping — cannot range-filter. This should not happen for a "
                f"span_recoverable table; check Job 1 v2 output.")
        # Match the df's actual column (case/space-insensitive).
        _matched = None
        for _c in df.columns:
            if _c.lower().strip() == str(_pk_df_col).lower().strip():
                _matched = _c
                break
        if _matched is None:
            raise Exception(
                f"RANGE LOAD [{dsql_schema}.{dsql_table}]: PK source column {_pk_df_col!r} "
                f"not present in CSV columns {df.columns}.")
        # KIND-appropriate range filter, half-open [lo, hi). The Spark-side expression MUST
        # mirror the DSQL-side blank_pk_range comparison so a row lands in the SAME range on
        # both sides:
        #   integer -> numeric long compare
        #   uuid    -> dash-stripped lowercase hex (identical normalization both sides)
        #   text    -> raw string, byte/code-point order (== DSQL 'C'/byte order for ASCII)
        # TOP-range ceiling is omitted based on the POSITIONAL _is_top flag (NOT by sniffing
        # the hi string) — an interior text bound can also end in '\x00', which sniffing
        # would misread as the top range and overlap later ranges (dupes). The top range's
        # hi is an out-of-space sentinel with no comparable literal, so we compare lo only.
        if _pk_kind == "uuid":
            # Normalized dash-stripped lowercase hex (identical to bounds + DSQL side).
            _pkc = lower(regexp_replace(col(_matched), "-", ""))
            if _is_top:
                df = df.filter(_pkc >= lit(str(_lo)))
            else:
                df = df.filter((_pkc >= lit(str(_lo))) & (_pkc < lit(str(_hi))))
        elif _pk_kind == "text":
            _pkc = col(_matched).cast("string")
            if _is_top:
                df = df.filter(_pkc >= lit(str(_lo)))
            else:
                df = df.filter((_pkc >= lit(str(_lo))) & (_pkc < lit(str(_hi))))
        else:
            _pkc = col(_matched).cast("long")
            df = df.filter((_pkc >= lit(int(_lo))) & (_pkc < lit(int(_hi))))
        print(f"    RANGE [{_lo}, {_hi}) kind={_pk_kind} is_top={_is_top} on {_pk_col} "
              f"(df col {_matched})")

    # CDC-CONTAMINATION BACKSTOP (version-independent). The full-load read must contain ONLY
    # DMS full-load files (LOAD*.csv). CDC files (timestamp-named, same flat folder because
    # AddColumnName=true forbids CdcPath/DatePartition per the DMS S3-target docs) carry a
    # leading "Op" column (I/U/D). If one leaks into the read, df.columns[0] == "Op" — a
    # signature a full-load config never has. Raise LOUDLY instead of silently shifting every
    # column. This backstops the LOAD*.csv name filter (pathGlobFilter + _is_full_load_key),
    # so safety never depends on Spark's version-specific pathGlobFilter path/basename quirk.
    if df.columns and str(df.columns[0]).strip().lower() == "op":
        raise Exception(
            f"CDC CONTAMINATION: the full-load read for {dsql_schema}.{dsql_table} sees a "
            f"leading 'Op' column — a CDC file (timestamp-named, with I/U/D op column) was "
            f"read as full-load data. Full-load files are LOAD*.csv; CDC files must not be in "
            f"the load set. This happens when a full load is re-run over a prefix still "
            f"holding earlier CDC output. Remove/relocate the CDC files (they belong to the "
            f"CDC job) and re-run. NOT a data-type issue."
        )

    if len(df.columns) != expected_col_count:
        raise Exception(
            f"COLUMN COUNT MISMATCH: CSV has {len(df.columns)}, config expects "
            f"{expected_col_count}. Re-run Job 1 for {dsql_table}."
        )

    # ---- Apply column mapping (name-based) ----
    select_cols = []
    for entry_map in column_mapping:
        if entry_map['action'] == 'map':
            dms_col_name = entry_map['dms_column_name']
            target_col_name = entry_map['target_column']
            matched_df_col = None
            for df_col in df.columns:
                if df_col.lower().strip() == dms_col_name.lower().strip():
                    matched_df_col = df_col
                    break
            if matched_df_col:
                if matched_df_col != target_col_name:
                    df = df.withColumnRenamed(matched_df_col, target_col_name)
                select_cols.append(target_col_name)

    missing_from_df = set(target_columns) - set(select_cols)
    if missing_from_df:
        raise Exception(
            f"Target columns not mapped from CSV: {missing_from_df}. Re-run Job 1 for {dsql_table}."
        )
    df = df.select(select_cols)

    # ---- Type conversions ----
    # (a) empty string AND NULL sentinels -> real NULL
    for c in df.columns:
        trimmed = trim(col(c))
        df = df.withColumn(
            c,
            when(trimmed == "", lit(None))
            .when(upper(trimmed).isin(sentinel_upper), lit(None))
            .otherwise(trimmed)
        )

    # (b) UUID: RAW hex -> UUID format
    uuid_cols = sorted([n for n in df.columns if type_categories.get(n) == 'uuid'])
    for col_name in uuid_cols:
        raw = col(col_name)
        # Only reshape input that is EXACTLY 32 hex chars. The reshape uses fixed-offset
        # substrings (positions 1..32); a MISALIGNED value that is 32-hex followed by
        # trailing junk would read only the first 32 chars and SILENTLY DROP the junk,
        # manufacturing a canonical-looking uuid from corrupted bytes. Anything not
        # exactly-32-hex (already-canonical, over-length, or non-hex) is passed through
        # UNCHANGED so the uuid guard sees the real value and rejects junk.
        is_exactly_raw_hex = raw.rlike(UUID_RAW_HEX_RE)      # ^[0-9a-fA-F]{32}$
        is_canonical = raw.rlike(UUID_CANONICAL_RE)          # ^8-4-4-4-12$
        reshaped = lower(concat(
            substring(raw, 1, 8), lit("-"),
            substring(raw, 9, 4), lit("-"),
            substring(raw, 13, 4), lit("-"),
            substring(raw, 17, 4), lit("-"),
            substring(raw, 21, 12)
        ))
        # lower-case BOTH forms so stored uuids are uniformly lowercase (the lowercase-only
        # post-load check would otherwise flag an uppercase canonical uuid as INVALID):
        #   - exactly-32-hex  -> reshape to canonical (already lower()).
        #   - already-canonical (any case) -> lower() it.
        #   - anything else (junk / 32-hex+trailing) -> pass through UNCHANGED so the
        #     uuid guard sees the real bytes and fails the table loudly.
        df = df.withColumn(
            col_name,
            when(raw.isNull() | (raw == ""), lit(None))
            .when(is_exactly_raw_hex, reshaped)
            .when(is_canonical, lower(raw))
            .otherwise(raw)
        )

    # (b2) The uuid shape guard is NOT a separate Spark scan here — that would read the
    # whole input twice and, with multiLine=true non-splittable reads, double the parse
    # cost. Instead the shape check runs INLINE in flatten_chunk() on the rows already
    # being streamed to the insert (see STRICT_UUID_GUARD usage below). The set of uuid
    # columns to check is captured here for that inline check.

    # (c) BOOLEAN: normalize every engine's boolean serialization -> true/false.
    #   - SQL Server BIT / MySQL TINYINT(1) -> numeric 1/0
    #   - Postgres BOOLEAN -> 't'/'f' (DMS) or 'true'/'false'
    #   - some sources -> 'Y'/'N', 'yes'/'no'
    # Match numerically first (covers 1/0, 1.0/0.0), then the textual forms
    # (case-insensitive). Unrecognized -> NULL (surfaced by the ::boolean cast if the
    # target is NOT NULL, otherwise stored NULL).
    _BOOL_TRUE = ["true", "t", "y", "yes"]
    _BOOL_FALSE = ["false", "f", "n", "no"]
    bool_cols = sorted([n for n in df.columns if type_categories.get(n) == 'boolean'])
    for col_name in bool_cols:
        numeric_val = col(col_name).cast("double")
        text_val = lower(trim(col(col_name)))
        df = df.withColumn(
            col_name,
            when(numeric_val == 1, lit("true"))
            .when(numeric_val == 0, lit("false"))
            .when(text_val.isin(_BOOL_TRUE), lit("true"))
            .when(text_val.isin(_BOOL_FALSE), lit("false"))
            .otherwise(lit(None))
        )

    # (d) TIMESTAMPTZ / DATE normalization to ISO
    ts_cols = sorted([n for n in df.columns if type_categories.get(n) == 'timestamptz'])
    for col_name in ts_cols:
        df = df.withColumn(
            col_name,
            normalize_timestamp(col_name, TIMESTAMP_INPUT_FORMATS, "yyyy-MM-dd HH:mm:ss.SSSSSS")
        )
    date_cols = sorted([n for n in df.columns if type_categories.get(n) == 'date'])
    for col_name in date_cols:
        df = df.withColumn(
            col_name,
            normalize_timestamp(col_name, DATE_INPUT_FORMATS, "yyyy-MM-dd")
        )

    # ---- Build INSERT SQL with casts ----
    df_cols = set(df.columns)
    missing = [c for c in target_columns if c not in df_cols]
    if missing:
        raise Exception(f"Target columns not in DataFrame (mapping gap): {missing}.")

    # NOTE (integer casts ROUND, they don't truncate): '%s::numeric::bigint' (and
    # ::integer/::smallint) uses PostgreSQL/DSQL numeric->integer rounding (round-half-to-
    # even), so a fractional source string for an integer-typed target (e.g. an Oracle
    # NUMBER with unexpected scale mapped to a DSQL integer) is SILENTLY ROUNDED, not
    # rejected. Intentional and low-probability; for strict rejection, change the target
    # column type or fail on non-integer input upstream in Job 1.
    # NOTE (bytea): '%s::bytea' expects DMS to have emitted the hex-escape ("\x..") form,
    # and the blanket trim() in step (a) also trims bytea-category columns — if a bytea
    # value legitimately has leading/trailing whitespace bytes, trim() would corrupt it.
    # bytea is uncommon in these migrations and this path is NOT verified against real
    # DMS bytea output; validate before relying on a bytea column.
    cast_map = {
        'uuid': '%s::uuid', 'boolean': '%s::boolean', 'timestamptz': '%s::timestamptz',
        'bigint': '%s::numeric::bigint', 'integer': '%s::numeric::integer',
        'smallint': '%s::numeric::smallint', 'numeric': '%s::numeric',
        'float': '%s::double precision', 'date': '%s::date',
        'json': '%s::jsonb', 'bytea': '%s::bytea',
    }
    placeholders = [cast_map.get(type_categories.get(c, 'varchar'), '%s') for c in target_columns]
    quoted_cols = [f'"{c}"' for c in target_columns]
    single_values = "(" + ", ".join(placeholders) + ")"
    insert_prefix = f'INSERT INTO {dsql_schema}.{dsql_table} ({", ".join(quoted_cols)}) VALUES '

    # LITERAL MODE: per-column cast suffix (":: type") applied to an inlined literal,
    # derived from the SAME cast_map (strip the leading '%s' placeholder to get '::uuid'
    # etc.; empty suffix for plain varchar). e.g. cast_map['uuid']='%s::uuid' -> '::uuid'.
    _lit_cast_suffix = [cast_map.get(type_categories.get(c, 'varchar'), '%s')[2:]
                        for c in target_columns]   # drop the '%s' prefix

    def _sql_literal(v, cast_suffix):
        """Render one value as an injection-SAFE SQL literal for inline (no-bind) INSERT.
        NULL for None; otherwise single-quote the string form with EVERY single quote
        DOUBLED (the complete escaping rule for standard-conforming string literals, which
        DSQL uses), then apply the same ::type cast the bind path used. This is exactly as
        safe as parameter binding: no value can break out of its quoted literal."""
        if v is None:
            return "NULL"
        s = v if isinstance(v, str) else str(v)
        return "'" + s.replace("'", "''") + "'" + cast_suffix

    def build_literal_values(chunk_rows):
        """Build the '(...),(...)' VALUES text with inlined safe literals for a chunk.
        Runs the SAME per-row uuid/length guards as flatten_chunk (via _guard_row)."""
        groups = []
        for r in chunk_rows:
            d = r.asDict()
            _guard_row(d)
            vals = [_sql_literal(d.get(c), _lit_cast_suffix[i])
                    for i, c in enumerate(target_columns)]
            groups.append("(" + ", ".join(vals) + ")")
        return ", ".join(groups)

    def build_multi_insert(n_rows):
        """INSERT statement with n_rows repeated VALUES groups (bind-param mode only)."""
        return insert_prefix + ", ".join([single_values] * n_rows)

    def _build_stmt_params(chunk_rows):
        """Return (sql, params) for a chunk. LITERAL_INSERT_MODE inlines safe literals and
        returns params=None (no binds -> escapes the 32767 param cap); otherwise builds the
        %s-placeholder statement + positional params. Both run the shared per-row guards."""
        if LITERAL_INSERT_MODE:
            values = build_literal_values(chunk_rows)   # runs _guard_row per row
            return insert_prefix + values, None
        return build_multi_insert(len(chunk_rows)), flatten_chunk(chunk_rows)

    # uuid guard scope: only the uuid-category target columns, resolved once.
    guard_uuid_cols = uuid_cols if STRICT_UUID_GUARD else []

    # V16 BUG-L2: LOB size-guard scope. Columns whose category can hold a large value
    # (text/json/jsonb/bytea, or a varchar WITHOUT a declared max) and are therefore NOT
    # covered by varchar_max_len. A value in one of these exceeding DSQL's ~1 MiB non-index
    # column limit must fail LOUD pre-write (not as a cryptic non-retriable INSERT error).
    # Resolved once (cheap): the intersection of target columns with a LOB category, minus
    # those already length-guarded by varchar_max_len.
    lob_guard_cols = [c for c in target_columns
                      if type_categories.get(c) in _LOB_GUARD_CATEGORIES
                      and c not in varchar_max_len]

    def _guard_row(d):
        """Per-row shape guards (uuid format + varchar length + LOB byte size), shared by
        the bind-param (flatten_chunk) and literal (build_literal_values) INSERT paths so
        BOTH enforce identical correctness checks. Raises loudly on a violation."""
        for gc in guard_uuid_cols:
            gv = d.get(gc)
            if not is_valid_uuid_value(gv):
                raise Exception(
                    f"UUID GUARD [{dsql_schema}.{dsql_table}]: column '{gc}' holds a "
                    f"non-uuid value {repr(gv)[:160]} — this indicates CSV column "
                    f"misalignment (embedded newlines/quotes/commas in a text field), "
                    f"NOT a type issue. Verify the DMS S3 endpoint wrote quoted CSV "
                    f"(Rfc4180=true, the default) or switch it to Parquet; then re-run. "
                    f"See CSV_READ_OPTIONS / module docstring.")
        for lc, limit in varchar_max_len.items():
            lv = d.get(lc)
            if lv is not None:
                lv_len = len(lv) if isinstance(lv, str) else len(str(lv))
                if lv_len > limit:
                    raise Exception(
                        f"LENGTH GUARD [{dsql_schema}.{dsql_table}]: column '{lc}' value "
                        f"length {lv_len} exceeds target varchar({limit}). This is a target "
                        f"schema width mismatch (source data is wider than the DSQL column) "
                        f"OR a CSV misalignment. Widen the DSQL column (e.g. ALTER ... TYPE "
                        f"varchar({max(limit*2, lv_len)}) or text) and re-run, or verify the "
                        f"mapping. Sample: {repr(lv)[:160]}")
        # V16 BUG-L2: LOB byte-size guard for text/json/bytea/unbounded-varchar columns.
        # DSQL caps a non-index column at ~1 MiB (bytes). A larger value would fail at INSERT
        # as a cryptic non-retriable error and kill the whole table; catch it here, loud and
        # actionable, naming the column and its size. Byte length (UTF-8) — DSQL's limit is
        # bytes, not characters.
        for lc in lob_guard_cols:
            lv = d.get(lc)
            if lv is None:
                continue
            if isinstance(lv, (bytes, bytearray)):
                lv_bytes = len(lv)
            else:
                # CHAR-COUNT PRE-FILTER (v16 perf re-vet): UTF-8 is 1-4 bytes/char, so
                # char length bounds byte length. Skip the full .encode() (an O(n) pass
                # that would DOUBLE-walk a large LOB the literal path re-escapes anyway) in
                # the two cases where the char count alone is decisive:
                #   len <= LIMIT//4  -> guaranteed <= LIMIT bytes  (safe, skip)
                #   len  > LIMIT     -> guaranteed  > LIMIT bytes  (over, no need to encode)
                # Only in the narrow ambiguous band [LIMIT//4, LIMIT] chars do we encode to
                # get the exact byte length. So a typical well-under-limit LOB pays only a
                # cheap len() per row, not a full re-encode.
                s = lv if isinstance(lv, str) else str(lv)
                s_len = len(s)
                if s_len <= LOB_MAX_COLUMN_BYTES // 4:
                    continue                       # guaranteed under the byte limit
                if s_len <= LOB_MAX_COLUMN_BYTES:
                    lv_bytes = len(s.encode("utf-8"))   # ambiguous band: measure exactly
                    if lv_bytes <= LOB_MAX_COLUMN_BYTES:
                        continue
                else:
                    lv_bytes = len(s.encode("utf-8"))   # guaranteed over: exact size for msg
            if lv_bytes > LOB_MAX_COLUMN_BYTES:
                raise Exception(
                    f"LOB SIZE GUARD [{dsql_schema}.{dsql_table}]: column '{lc}' value is "
                    f"{lv_bytes:,} bytes, exceeding the DSQL ~1 MiB "
                    f"({LOB_MAX_COLUMN_BYTES:,} B) non-index column limit. DSQL cannot store "
                    f"this value; the load would otherwise fail at INSERT with a cryptic "
                    f"error. Resolve the oversized LOB (split/compress the field, drop it "
                    f"from the load, or confirm the source data is valid). Sample: "
                    f"{repr(lv)[:120]}")

    def flatten_chunk(chunk_rows):
        """Flatten chunk rows into a single positional params list (row-major) for the
        BIND-PARAM insert path. Enforces the shared per-row guards inline."""
        flat = []
        for r in chunk_rows:
            d = r.asDict()
            _guard_row(d)
            flat.extend(d.get(c) for c in target_columns)
        return flat

    # ---- Connect + empty/clear guard ----
    if pk_range is not None:
        # RANGE PATH: instead of the whole-table empty check, idempotently CLEAR this range
        # (DELETE WHERE lo <= pk < hi), so a retried/failed range replaces only its own
        # rows. The whole-table "must be empty" contract is enforced ONCE by
        # load_one_table_parallel (via assert_empty_or_register) before launching ranges.
        # blank_pk_range ALSO self-guards: it refuses unless the table is empty-verified for
        # this attempt, so a range DELETE can never touch pre-existing customer data. A Glue
        # retry (fresh JVM) resets the registry -> re-requires empty.
        _pk_col, _lo, _hi, _pk_kind, _is_top = pk_range
        _gc = connect_dsql()
        _gc.autocommit = False   # blank_pk_range drives its own per-batch commits
        # blank_pk_range closes _gc (or its reconnected replacement) itself — do NOT close
        # it here (would close a possibly-stale handle and leak the live reconnected one).
        # The '♻ cleared N' count is best-effort (may undercount, see blank_pk_range).
        _removed = blank_pk_range(_gc, dsql_schema, dsql_table, _pk_col, _lo, _hi,
                                  pk_kind=_pk_kind, is_top=_is_top)
        if _removed:
            print(f"    ♻ cleared {_removed:,} prior rows in range [{_lo},{_hi}) "
                  f"(idempotent re-run) before reload")
    elif file_subset is not None:
        # CHUNK FAN-OUT worker: do NOT run the empty gate here and do NOT DELETE.
        # The orchestrator (load_one_table_chunked) enforces the whole-table empty contract
        # ONCE via assert_empty_or_register before launching workers; each worker only
        # APPENDS its own disjoint part-files. Recovery is whole-table reload (any worker
        # failure fails the table -> re-blank + re-run). Belt-and-suspenders: refuse to
        # proceed if the parent somehow didn't gate.
        _key = f"{dsql_schema}.{dsql_table}"
        with _EMPTY_VERIFIED_LOCK:
            _gated = _key in _EMPTY_VERIFIED
        if not _gated:
            raise Exception(
                f"REFUSING chunk-fanout worker on {_key}: table was not empty-verified "
                f"for this attempt. load_one_table_chunked must call "
                f"assert_empty_or_register ONCE before launching workers — this "
                f"indicates a bug, not a data condition.")
    else:
        # WHOLE-TABLE PATH: verify target empty via the single shared barrier and register
        # it as empty-verified for this attempt (refuse to append). Pass resume_ok + a
        # column to page the auto-reblank by (single-col PK if present, else the first
        # target column) so a previously-attempted table can be auto-cleared+reloaded when
        # AUTO_REBLANK_ON_RESUME is on.
        _v5_pk_meta = (config.get('metadata', {}).get('primary_key') or {})
        _v5_pk_cols = _v5_pk_meta.get('columns') or []
        _v5_pk = _v5_pk_cols[0] if len(_v5_pk_cols) == 1 else None
        _v5_any = target_columns[0] if target_columns else None
        assert_empty_or_register(dsql_schema, dsql_table,
                                 resume_ok=_resume_ok_for(dsql_schema, dsql_table),
                                 pk_col=_v5_pk, any_col=_v5_any)

    # ---- STREAM rows from Spark (bounded driver memory) ----
    row_iter = df.toLocalIterator()
    df = None

    sample = []
    for r in row_iter:
        sample.append(r)
        if len(sample) >= ROW_SAMPLE_FOR_SIZING:
            break

    if sample:
        missing_in_row = [c for c in target_columns if c not in sample[0].asDict()]
        if missing_in_row:
            raise Exception(f"Collected Row missing target columns: {missing_in_row}.")

        # uuid guard — FAIL-BEFORE-WRITE on the already-in-memory sample. CSV misalignment
        # is STRUCTURAL, so it corrupts (nearly) every row; checking the sample here (free)
        # aborts the table BEFORE any chunk is committed, so a misaligned load never leaves
        # the target partially populated. The inline check in flatten_chunk() still guards
        # every remaining row as a backstop.
        if STRICT_UUID_GUARD and uuid_cols:
            for r in sample:
                d = r.asDict()
                for gc in uuid_cols:
                    gv = d.get(gc)
                    if not is_valid_uuid_value(gv):
                        raise Exception(
                            f"UUID GUARD [{dsql_schema}.{dsql_table}]: column '{gc}' holds a "
                            f"non-uuid value {repr(gv)[:160]} in the pre-load sample — this "
                            f"indicates CSV column misalignment (embedded newlines/quotes/"
                            f"commas in a text field), NOT a type issue. No rows were "
                            f"written. Verify the DMS S3 endpoint wrote quoted CSV "
                            f"(Rfc4180=true, the default) or switch it to Parquet; then "
                            f"re-run. See CSV_READ_OPTIONS / module docstring."
                        )

        # LENGTH GUARD (report all, then fail once): scan the pre-load sample and collect
        # EVERY varchar column whose value exceeds its target varchar(N), then fail the
        # table ONCE with a consolidated list so all width mismatches surface in a single
        # run. Fix over-length manually (widen the DSQL column or correct the data).
        # Lengths here are post-trim (step (a) already trimmed), matching DSQL's trailing-
        # space handling and the inline guard. The per-row inline guard in flatten_chunk()
        # is the backstop for any over-length value that appears only AFTER the sample.
        if varchar_max_len:
            over_len = {}  # column -> {"max_len": int, "limit": int, "sample": str}
            for r in sample:
                d = r.asDict()
                for lc, limit in varchar_max_len.items():
                    lv = d.get(lc)
                    if lv is None:
                        continue
                    lv_len = len(lv) if isinstance(lv, str) else len(str(lv))
                    if lv_len > limit:
                        prev = over_len.get(lc)
                        if prev is None or lv_len > prev["max_len"]:
                            over_len[lc] = {"max_len": lv_len, "limit": limit,
                                            "sample": repr(lv)[:160]}
            if over_len:
                details = "; ".join(
                    f"'{c}' max_len={v['max_len']} > varchar({v['limit']}) "
                    f"[e.g. {v['sample']}]"
                    for c, v in sorted(over_len.items())
                )
                raise Exception(
                    f"LENGTH GUARD [{dsql_schema}.{dsql_table}]: {len(over_len)} column(s) "
                    f"have values longer than their target varchar(N) in the pre-load "
                    f"sample. No rows were written. Fix ALL of these together (widen the "
                    f"DSQL column, e.g. ALTER ... TYPE varchar(<bigger>) or text, OR "
                    f"correct the source data) then re-run: {details}"
                )

    avg_row_bytes = estimate_avg_row_bytes(sample, target_columns)
    chunk_state = {"size": compute_chunk_size(len(target_columns), avg_row_bytes)}

    rows_read = {"n": 0}

    # per-file range-count validation support: track this file's PK min/max as we stream
    # rows (only when a single-col PK exists). Cheap: one dict lookup + compare per row. The
    # orchestrator records these in the file-status tracker; post-load validation uses them
    # to do exact per-file COUNT(*) WHERE pk in [min,max] IFF files are PK-disjoint.
    pk_minmax = {"min": None, "max": None}
    _tk_pk_meta = (config.get('metadata', {}).get('primary_key') or {})
    _tk_pk_cols = _tk_pk_meta.get('columns') or []
    # string min/max equals VALUE order ONLY for fixed-width uuid keys (canonical uuid text
    # sorts identically to the uuid type). For integer PKs lexical order != numeric order
    # (e.g. "100000" < "99999"), and variable-width text can also differ — so tracking is
    # ENABLED ONLY for pk_kind == 'uuid'. Other kinds leave pk_min/max None ->
    # _files_pk_disjoint False -> whole-table COUNT(*).
    _track_pk = (_tk_pk_cols[0] if (len(_tk_pk_cols) == 1
                 and _tk_pk_meta.get('pk_kind') == 'uuid') else None)

    def _track(r):
        if _track_pk is None:
            return
        v = r.asDict().get(_track_pk)
        if v is None:
            return
        s = v if isinstance(v, str) else str(v)
        if pk_minmax["min"] is None or s < pk_minmax["min"]:
            pk_minmax["min"] = s
        if pk_minmax["max"] is None or s > pk_minmax["max"]:
            pk_minmax["max"] = s

    def iter_chunks():
        buf = []
        for r in sample:                      # already-pulled sample rows first
            rows_read["n"] += 1
            _track(r)
            buf.append(r)
            if len(buf) >= chunk_state["size"]:
                yield buf
                buf = []
        for r in row_iter:                    # then the rest of the stream
            rows_read["n"] += 1
            _track(r)
            buf.append(r)
            if len(buf) >= chunk_state["size"]:
                yield buf
                buf = []
        if buf:
            yield buf

    # ---- Insert rows: multi-row chunks; commit on rows|bytes|time; OCC + pipe retry ----
    print(f"    Chunk size: {chunk_state['size']} rows/txn "
          f"({len(target_columns)} cols, ~{avg_row_bytes}B/row) — streamed multi-row INSERT")

    # identify this load's worker thread + its DSQL connection and count chunks, so
    # [WORKER]/[CHUNK] logs attribute "which connection loaded how many chunks/rows" per
    # driver worker. _range_tag names the slice this worker owns (a PK range, a file subset,
    # or the whole table). Driver-side facts — the connection is a pg8000 object on the
    # driver, not a Glue executor.
    _wk_thread = threading.current_thread().name
    if pk_range is not None:
        _range_tag = f"range[{pk_range[1]},{pk_range[2]})"
    elif file_subset is not None:
        _range_tag = f"files[{len(file_subset)}]"
    else:
        _range_tag = "whole_table"
    _chunk_count = {"n": 0}
    _wk_start = time.time()

    total_written = 0
    # Borrow a long-lived connection from the per-table pool if one was provided (so files
    # reuse connections instead of a fresh TLS+IAM handshake each); else open a fresh one.
    # A borrowed connection is returned to the pool (still open) in the finally; a
    # self-opened one is closed there. _pooled_conn carries the age so recycle still works.
    if conn_pool is not None:
        conn, conn_started = conn_pool.borrow()
    else:
        conn = connect_dsql()
        conn_started = time.monotonic()
    conn.autocommit = False
    cursor = conn.cursor()
    # Short, stable-per-connection tag (pg8000 has no friendly id): thread + object id.
    _conn_tag = f"{_wk_thread}:{id(conn) & 0xffff:04x}"
    print(f"    ⇢ [WORKER] START {dsql_schema}.{dsql_table} {_range_tag} "
          f"thread={_wk_thread} conn={_conn_tag} chunk_size={chunk_state['size']}",
          flush=True)

    # RESUME COMMIT-PROBE: when re-running a file that was previously attempted but not
    # finished (probe_all_chunks=True), probe each chunk's first-row PK before inserting; if
    # it's already present (a prior attempt committed it) skip the INSERT -> no duplicate.
    # Only meaningful with a single-col PK; else the probe returns None and we insert (that
    # no-PK case never reaches here — the orchestrator whole-table-reblanks instead).
    # Independently, the insert_one_chunk except block also handles ambiguous XX000-at-commit
    # via a self-correcting re-insert (PK 23505 on retry = already committed).
    _pk_meta = (config.get('metadata', {}).get('primary_key') or {})
    _pk_cols = _pk_meta.get('columns') or []
    _probe_pk = _pk_cols[0] if len(_pk_cols) == 1 else None
    _probe_cast = cast_map.get(type_categories.get(_probe_pk, 'varchar'), '%s') if _probe_pk else '%s'

    def _chunk_already_committed(chunk_rows):
        """True if the chunk's FIRST row already exists in the target (a prior attempt's
        commit landed). DSQL commits are atomic per txn, so probing row[0] is sufficient.
        Returns None when it can't tell (no single PK / null pk / probe error) -> caller
        inserts (safe default)."""
        if _probe_pk is None or not chunk_rows:
            return None
        pkval = chunk_rows[0].asDict().get(_probe_pk)
        if pkval is None:
            return None
        pc = None
        try:
            pc = conn.cursor()
            pc.execute(f'SELECT 1 FROM {dsql_schema}.{dsql_table} '
                       f'WHERE "{_probe_pk}" = {_probe_cast} LIMIT 1', (pkval,))
            return pc.fetchone() is not None
        except Exception:
            return None
        finally:
            try:
                if pc:
                    pc.close()
            except Exception:
                pass

    def rebuild_conn(fresh_token=False):
        nonlocal conn, cursor, conn_started, _conn_tag
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
        # On a broken-pipe/connection-drop rebuild, mint a FRESH token: the cached one may be
        # the reason the connection failed (stale/expired/endpoint blip). A proactive age
        # recycle passes fresh_token=False (the cached token is fine — just a new socket).
        if fresh_token:
            _invalidate_dsql_token()
        conn = connect_dsql()
        conn.autocommit = False
        cursor = conn.cursor()
        conn_started = time.monotonic()
        # refresh the connection tag so [CHUNK]/[WORKER] logs reflect the NEW conn.
        _conn_tag = f"{_wk_thread}:{id(conn) & 0xffff:04x}"

    def insert_one_chunk(chunk_rows):
        """Insert one chunk in a single transaction. Handles OCC/timeout/pipe retry,
        per-chunk timing with adaptive shrink, and connection recycling."""
        nonlocal conn, cursor, total_written

        if time.monotonic() - conn_started > CONN_RECYCLE_SECONDS:
            print(f"    ♻ recycling DSQL connection (>{CONN_RECYCLE_SECONDS//60} min) "
                  f"[{_range_tag} thread={_wk_thread} conn={_conn_tag}]")
            rebuild_conn()

        stmt, params = _build_stmt_params(chunk_rows)
        pending_tail = []

        # MEASURED SIZE BACKSTOP (v16): the row-count estimate (CHUNK_BYTE_BUDGET // avg) is
        # best-effort — a quote-dense row can inflate the built literal past the estimate.
        # This is the EXACT guarantee against DSQL's hard ~10 MiB per-txn limit: measure the
        # real statement bytes and, if over, split the chunk in half and rebuild until it
        # fits (or a single row remains — a 1-row over-limit statement means a >10 MiB row,
        # a genuine data problem that will surface as a clear DSQL error, not silent loss).
        # Only literal mode inlines values into stmt; bind mode's payload is the params list,
        # which is bounded by the same row/byte caps and carries no escaping inflation.
        if params is None and len(chunk_rows) > 1:
            _measured = len(stmt.encode("utf-8"))
            while len(chunk_rows) > 1 and _measured > DSQL_MAX_TXN_BYTES:
                # PROPORTIONAL shrink (not blind halving): we have the EXACT measured byte
                # size for len(chunk_rows) rows, so the real bytes/row is _measured/N. The
                # largest row count that fits the budget is ~ BUDGET / (bytes/row). Aim at
                # BYTE_TARGET_FRACTION (0.99) of the budget — minimal headroom, since an
                # overshoot only costs one more CHEAP in-memory re-slice — so we land in ONE
                # step at ~99% of the limit instead of 9->4->2->1 halving. Clamp to
                # [1, N-1] so we always make progress; the while-loop re-measures and only
                # iterates again in the rare case the kept slice is denser than average.
                bytes_per_row = _measured / len(chunk_rows)
                target = int((DSQL_MAX_TXN_BYTES * BYTE_TARGET_FRACTION) / max(bytes_per_row, 1))
                new_n = max(1, min(target, len(chunk_rows) - 1))
                print(f"    ✂ measured statement {_measured:,} B > "
                      f"{DSQL_MAX_TXN_BYTES:,} B hard limit; shrinking chunk "
                      f"{len(chunk_rows)} -> {new_n} rows (proportional) and deferring the rest",
                      flush=True)
                pending_tail = chunk_rows[new_n:] + pending_tail
                chunk_rows = chunk_rows[:new_n]
                stmt, params = _build_stmt_params(chunk_rows)
                _measured = len(stmt.encode("utf-8"))

        pipe_attempt = 0
        occ_attempt = 0
        server_attempt = 0
        while True:
            t0 = time.monotonic()
            # RESUME NO-DUP: on a probe_all_chunks reload (previously-incomplete file),
            # skip any chunk whose rows already landed in a prior attempt (PK probe) -> no dup.
            if probe_all_chunks and _chunk_already_committed(chunk_rows) is True:
                total_written += len(chunk_rows)
                return pending_tail
            try:
                if params is None:
                    cursor.execute(stmt)          # literal mode: no bind parameters
                else:
                    cursor.execute(stmt, params)  # bind-param mode
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                # SELF-CORRECTING RE-INSERT: a UNIQUE violation (23505) on a RETRY means a
                # prior attempt's interrupted commit ACTUALLY landed this chunk (its PK rows
                # already exist). Treat it as success — count the chunk, do NOT re-insert (no
                # duplicate). Only on a retry (server/pipe attempt > 0): a 23505 on the FIRST
                # attempt means the SOURCE has duplicate PKs (or the target wasn't empty) — a
                # real data problem, so FAIL LOUDLY rather than silently swallow it.
                if is_unique_violation(e):
                    if server_attempt > 0 or pipe_attempt > 0:
                        print(f"    ✓ chunk @row {total_written} UNIQUE-violation on retry "
                              f"=> prior interrupted commit LANDED; treating as committed "
                              f"(no re-insert, no dup)")
                        total_written += len(chunk_rows)
                        return pending_tail
                    raise Exception(
                        f"Write failed at row {total_written} for {dsql_table} "
                        f"[UNIQUE violation 23505 on FIRST insert — the SOURCE has duplicate "
                        f"primary keys OR the target was not empty at start. This is a data "
                        f"problem, not a transient error; refusing to skip rows]: {e}")

                if is_occ_conflict(e):
                    if occ_attempt < OCC_MAX_RETRIES:
                        occ_attempt += 1
                        wait = occ_backoff_seconds(occ_attempt)
                        print(f"    ⟲ OCC 40001 @row {total_written}; retry "
                              f"{occ_attempt}/{OCC_MAX_RETRIES} after {wait:.3f}s")
                        time.sleep(wait)
                        continue
                    raise Exception(
                        f"Write failed at row {total_written} for {dsql_table} "
                        f"[OCC 40001 retries exhausted]: {e}"
                    )

                if is_txn_timeout(e) and len(chunk_rows) > MIN_CHUNK_SIZE:
                    _record_txn_age_failure()   # feeds the aggressive(270)->safe(240) trigger fallback
                    # PROPORTIONAL (throughput-based, not blind halving): the txn ran from t0
                    # until it hit the age limit, so `_failed_elapsed` is the REAL time these
                    # rows took. rows/sec ~= size/_failed_elapsed; size the retry to land at
                    # ~85% of the trigger (extra margin since this one already timed out):
                    # size * (0.85*trigger/_failed_elapsed). Floored at MIN_CHUNK_SIZE, and
                    # forced strictly smaller so we always make progress.
                    _failed_elapsed = max(time.monotonic() - t0, 0.001)
                    _trigger = effective_batch_max_seconds()
                    new_size = int(len(chunk_rows) * (TIME_TARGET_FRACTION_AFTER_FAIL * _trigger) / _failed_elapsed)
                    new_size = min(new_size, len(chunk_rows) - 1)
                    new_size = max(MIN_CHUNK_SIZE, new_size)
                    print(f"    ⏱ txn-age limit hit on {len(chunk_rows)}-row chunk after "
                          f"{_failed_elapsed:.0f}s; shrinking to {new_size} and re-slicing "
                          f"(throughput-proportional; chunk_state {chunk_state['size']} -> {new_size})")
                    chunk_state["size"] = new_size
                    pending_tail = chunk_rows[new_size:] + pending_tail
                    chunk_rows = chunk_rows[:new_size]
                    stmt, params = _build_stmt_params(chunk_rows)
                    pipe_attempt = 0
                    occ_attempt = 0
                    server_attempt = 0
                    continue

                # Transient DSQL server-unavailable (SQLSTATE XX000 "server unavailable").
                # Per AWS DSQL guidance the connection stays ACTIVE (do NOT reconnect); retry
                # the whole transaction with exponential backoff + jitter. The error can occur
                # AT COMMIT with the txn actually committed (ambiguous), so we simply RETRY
                # (re-insert). Because the target has a PRIMARY KEY the re-insert is
                # self-correcting:
                #   - if the interrupted commit had NOT landed -> re-insert succeeds (no loss)
                #   - if it HAD landed -> re-insert hits the PK unique index -> 23505, handled
                #     just below as "already committed" (count it, no duplicate).
                # This preserves BOTH no-dup and no-loss with no probe SELECT.
                if is_transient_server_error(e) and server_attempt < SERVER_MAX_RETRIES:
                    server_attempt += 1
                    wait = server_backoff_seconds(server_attempt)
                    print(f"    ↻ chunk @row {total_written} DSQL server unavailable "
                          f"(XX000); re-insert retry {server_attempt}/{SERVER_MAX_RETRIES} "
                          f"after {wait:.2f}s (PK 23505 on retry => already committed, no dup)")
                    time.sleep(wait)
                    # Connection remains active per DSQL guidance — do NOT rebuild_conn().
                    continue

                if is_broken_pipe_error(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                    pipe_attempt += 1
                    wait = CHUNK_RETRY_BACKOFF_SECONDS * pipe_attempt
                    print(f"    ↻ chunk @row {total_written} broken-pipe/conn error "
                          f"({type(e).__name__}: {e}); reconnect + retry "
                          f"{pipe_attempt}/{MAX_CHUNK_RETRIES} after {wait}s")
                    time.sleep(wait)
                    rebuild_conn(fresh_token=True)   # drop => mint a fresh token (heals 08006)
                    continue

                if is_transient_server_error(e):
                    kind = "DSQL server-unavailable (retries exhausted)"
                elif is_broken_pipe_error(e):
                    kind = "broken-pipe (retries exhausted)"
                else:
                    kind = "non-retriable"
                raise Exception(
                    f"Write failed at row {total_written} for {dsql_table} [{kind}]: {e}"
                )

            elapsed = time.monotonic() - t0
            total_written += len(chunk_rows)

            # NOTE: we NEVER abort a chunk at the trigger. This runs AFTER conn.commit()
            # succeeded — the chunk is fully committed and counted above. A chunk is allowed
            # to run all the way to DSQL's real 300s hard limit; it often finishes before
            # then. The trigger (270s) only adjusts the NEXT chunk's size (below); it is a
            # post-commit tuning signal, not a client-side timeout/cutoff.
            _batch_trigger = effective_batch_max_seconds()
            if elapsed > _batch_trigger and chunk_state["size"] > MIN_CHUNK_SIZE:
                # PROPORTIONAL (throughput-based, not blind halving): this chunk committed
                # but took `elapsed`s at `chunk_state['size']` rows. Rows/sec ~= size/elapsed,
                # so the size that lands at ~90% of the trigger is size * (0.9*trigger/elapsed).
                # Aim at 90% (not 100%) for margin against commit-time variance; floored at
                # MIN_CHUNK_SIZE. One calculation from measured throughput instead of a
                # crude halve, so we don't over-shrink a chunk that was only slightly slow.
                new_size = max(MIN_CHUNK_SIZE,
                               int(chunk_state["size"] * (TIME_TARGET_FRACTION * _batch_trigger) / max(elapsed, 0.001)))
                new_size = min(new_size, chunk_state["size"] - 1)   # guarantee progress
                new_size = max(MIN_CHUNK_SIZE, new_size)
                print(f"    ⏱ chunk took {elapsed:.0f}s (> {_batch_trigger}s); "
                      f"shrinking chunk size {chunk_state['size']} -> {new_size} "
                      f"(throughput-proportional)")
                chunk_state["size"] = new_size

            return pending_tail

    _load_clean = False   # set True only after the chunk loop finishes with no exception
    try:
        for chunk in iter_chunks():
            pending = insert_one_chunk(chunk)
            while pending:
                pending = insert_one_chunk(pending)
            # count chunks and (if --verbose_chunks) log throttled per-chunk progress,
            # 1 line every VERBOSE_CHUNK_EVERY chunks to avoid a CloudWatch firehose.
            _chunk_count["n"] += 1
            if VERBOSE_CHUNKS and (_chunk_count["n"] % VERBOSE_CHUNK_EVERY == 0):
                print(f"    · [CHUNK] {dsql_schema}.{dsql_table} {_range_tag} "
                      f"thread={_wk_thread} conn={_conn_tag} chunk#{_chunk_count['n']} "
                      f"committed_rows={total_written:,}", flush=True)
        _load_clean = True   # reached only if every chunk committed with no exception
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        # Return to the pool ONLY on a clean finish. A connection that failed mid-file may
        # carry an aborted/uncommitted transaction; do NOT hand a dirty connection to the
        # next file — close it and let the pool open a fresh one on the next borrow.
        if conn_pool is not None and _load_clean:
            conn_pool.give_back(conn, conn_started)
        else:
            try:
                conn.close()
            except Exception:
                pass
    # per-worker SUMMARY — the high-signal answer to "which connection loaded how
    # much per worker": thread + connection tag + slice + chunk count + rows + seconds.
    _wk_secs = max(0.001, time.time() - _wk_start)
    print(f"    ✓ Wrote {total_written:,} rows")
    print(f"    ⇠ [WORKER] DONE {dsql_schema}.{dsql_table} {_range_tag} "
          f"thread={_wk_thread} conn={_conn_tag} chunks={_chunk_count['n']} "
          f"rows={total_written:,} secs={_wk_secs:.1f} "
          f"rows_per_sec={total_written/_wk_secs:.0f}", flush=True)

    # ---- Three-way validation, gate 1 (EXACT): source CSV == written ----
    if rows_read["n"] != total_written:
        raise Exception(
            f"ROW COUNT (source vs written): CSV streamed {rows_read['n']} rows, "
            f"but committed {total_written} for {dsql_schema}.{dsql_table}."
        )
    # D1: on the WHOLE-TABLE path (this read covers the entire table), assert the parsed
    # row count matches the authoritative source count if provided (catches row-split
    # phantom rows the internal parsed-vs-parsed gate cannot). Range/fanout sub-loads read
    # only a slice, so D1 runs at the orchestrator (range path) not here.
    if pk_range is None and file_subset is None:
        assert_expected_source_rows(config, rows_read["n"], dsql_schema, dsql_table)

    # RANGE PATH: skip the WHOLE-TABLE post-load validation here — under concurrent
    # ranges a whole-table COUNT(*)/scan would race other ranges' commits. The
    # orchestrator runs the full validation ONCE after all ranges finish. Return this
    # range's committed row count so the orchestrator can sum + reconcile.
    if pk_range is not None:
        return {
            "table": f"{dsql_schema}.{dsql_table}",
            "rows": total_written,
            "range": [pk_range[1], pk_range[2]],
            "target_columns": len(target_columns),
            "skipped_columns": len(skip_columns),
        }

    # CHUNK FAN-OUT worker: same as the range path — skip whole-table validation
    # here (concurrent workers' commits would race a COUNT/scan); the orchestrator
    # (load_one_table_chunked) validates the assembled table ONCE. Return this worker's
    # committed row count so the orchestrator can sum + reconcile.
    if file_subset is not None:
        return {
            "table": f"{dsql_schema}.{dsql_table}",
            "rows": total_written,
            "files": len(file_subset),
            "pk_min": pk_minmax["min"],
            "pk_max": pk_minmax["max"],
            "target_columns": len(target_columns),
            "skipped_columns": len(skip_columns),
        }

    # ---- Post-load validation ----
    conn = connect_dsql()
    # autocommit so each read-only check stands alone: if one scan errors (e.g. XX000 on a
    # large table, caught+warned in _run_uuid_and_leak_checks) it can't poison a transaction
    # and break the remaining checks.
    conn.autocommit = True
    cursor = conn.cursor()
    errors = []
    try:
        actual_count = None
        if total_written <= COUNT_EXACT_MAX_ROWS:
            cursor.execute(f"SELECT COUNT(*) FROM {dsql_schema}.{dsql_table}")
            actual_count = cursor.fetchone()[0]
            if actual_count != total_written:
                errors.append(f"ROW COUNT: wrote {total_written}, target has {actual_count}")
        else:
            limit_n = total_written + 1
            cursor.execute(
                f"SELECT COUNT(*) FROM "
                f"(SELECT 1 FROM {dsql_schema}.{dsql_table} LIMIT {limit_n}) AS _bounded"
            )
            bounded = cursor.fetchone()[0]
            if bounded == total_written:
                print(f"    ✓ bounded target count == written ({total_written:,}) "
                      f"[>{COUNT_EXACT_MAX_ROWS:,} rows: skipped full COUNT(*)]")
            elif bounded >= total_written + 1:
                errors.append(
                    f"ROW COUNT: target has MORE than written {total_written} "
                    f"(bounded probe hit {limit_n}) — possible duplicate rows.")
            else:
                errors.append(
                    f"ROW COUNT: target has FEWER than written {total_written} "
                    f"(bounded probe found {bounded}).")
            try:
                cursor.execute(
                    "SELECT reltuples FROM pg_class WHERE relname = %s", (dsql_table,)
                )
                row = cursor.fetchone()
                est = float(row[0]) if row and row[0] is not None else -1.0
                if est is not None and est >= 0:
                    print(f"    (reltuples estimate for {dsql_table}: {est:.0f}; "
                          f"informational only)")
            except Exception as rel_err:
                print(f"    (reltuples lookup skipped: {rel_err})")

        # UUID-format + auth-token-leak scans (shared single source of truth).
        errors.extend(_run_uuid_and_leak_checks(
            cursor, dsql_schema, dsql_table, uuid_cols, target_columns))
    finally:
        cursor.close()
        conn.close()

    if errors:
        raise Exception("VALIDATION FAILED: " + "; ".join(errors))

    return {
        "table": f"{dsql_schema}.{dsql_table}",
        "rows": total_written,
        "target_columns": len(target_columns),
        "skipped_columns": len(skip_columns),
    }


# =============================================================================
# range-parallel orchestration (driver_threads substrate). Reuses load_one_table
# per range (pk_range set) for the proven guards/insert/retry, then validates the whole
# table ONCE. Only invoked for large + span_recoverable tables (see load_table_auto).
# =============================================================================
def s3_table_total_bytes(s3_client, dms_s3_path):
    """Sum the sizes of all DMS CSV objects under a table's S3 prefix (paginated).
    Cheap ROUTING GATE — metadata only, no data scanned."""
    bucket, prefix = split_s3(dms_s3_path)
    total = 0
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            total += int(o.get("Size", 0))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return total


def assert_expected_source_rows(config, parsed_rows, dsql_schema, dsql_table):
    """V10 STRICT ROW-SPLIT GUARD (D1): if the config supplies an authoritative expected
    source row count (metadata.expected_source_rows — e.g. DMS FullLoadRows), assert the
    Spark-PARSED row count equals it. A CSV row-split inflates the parsed count above the
    true source count while the internal count gate (parsed-vs-parsed) stays consistent, so
    THIS is the only check that detects the inflation — it compares against a truth OUTSIDE
    the parser. No-op if expected_source_rows is absent (backward compatible) or the guard
    is disabled. Raises loudly on any mismatch (inflation = fabricated rows; shortfall =
    dropped rows)."""
    if not STRICT_ROWSPLIT_GUARD:
        return
    exp = (config.get('metadata', {}) or {}).get('expected_source_rows')
    if exp is None:
        return
    try:
        exp = int(exp)
    except (TypeError, ValueError):
        return
    if parsed_rows != exp:
        _why = ("INFLATION -> a CSV row-split fabricated rows (an unquoted embedded newline "
                "in a text field split one source row into two)." if parsed_rows > exp
                else "SHORTFALL -> rows were dropped/unparsed.")
        raise Exception(
            f"ROW-SPLIT GUARD (D1 expected-count) [{dsql_schema}.{dsql_table}]: Spark parsed "
            f"{parsed_rows:,} rows but the authoritative source count "
            f"(metadata.expected_source_rows, e.g. DMS FullLoadRows) is {exp:,}. {_why} "
            f"Refusing to load a count that disagrees with the source. Fix the DMS S3 endpoint "
            f"to emit RFC-4180-quoted CSV (Rfc4180=true) or re-export, then re-run.")


def spark_pk_bounds(dms_s3_path, dms_has_headers, config, pk_col, pk_kind="integer"):
    """[retired range path] Spark MIN/MAX/COUNT (and text sample) of the PK for range
    planning."""
    from pyspark.sql import functions as F
    df = read_dms_csv(dms_s3_path, dms_has_headers)
    assert_not_cdc_layout(df, config.get('metadata', {}).get('dsql_schema', '?'),
                          config.get('metadata', {}).get('dsql_table', '?'))
    column_mapping = config['column_mapping']
    pk_src = None
    for _em in column_mapping:
        if _em.get('action') == 'map' and _em.get('target_column', '').lower() == pk_col.lower():
            pk_src = _em.get('dms_column_name')
            break
    if pk_src is None:
        return (None, None, 0, None)
    matched = None
    for c in df.columns:
        if c.lower().strip() == str(pk_src).lower().strip():
            matched = c
            break
    if matched is None:
        return (None, None, 0, None)

    if pk_kind == "uuid":
        # Normalize IDENTICALLY to the range filter: strip dashes + lowercase. min/max
        # over the 32-hex string is the true key-space min/max (zero-padded hex sorts
        # numerically). Rows that don't normalize to 32-hex won't be in [min,max] and are
        # caught by the source-count gate.
        pkc = F.lower(F.regexp_replace(F.col(matched), "-", ""))
        pkc = F.when(F.length(pkc) == 32, pkc)   # only well-formed 32-hex contribute bounds
    elif pk_kind == "text":
        pkc = F.col(matched).cast("string")
    else:
        pkc = F.col(matched).cast("long")

    row = df.select(
        F.min(pkc).alias("mn"), F.max(pkc).alias("mx"), F.count(F.lit(1)).alias("n")
    ).collect()[0]
    if row["n"] == 0 or row["mn"] is None:
        return (None, None, 0, None)
    if pk_kind == "integer":
        return (int(row["mn"]), int(row["mx"]), int(row["n"]), None)
    if pk_kind == "uuid":
        # uuid keeps synthetic hex-space planning (plan_ranges_hex); no key sample needed.
        return (str(row["mn"]), str(row["mx"]), int(row["n"]), None)
    # ---- text: also collect a SORTED, DISTINCT, byte-ordered key SAMPLE for the quantile
    # partitioner (plan_ranges_text cuts on REAL keys). Distinct keys ordered by byte value
    # (== DSQL C-collation / Spark UTF8 order), capped at TEXT_SAMPLE_MAX (evenly ordered
    # subset). Correctness (coverage/no-overlap) does NOT depend on sample completeness
    # because the per-range filter/DELETE compares the FULL key and ranges are contiguous
    # half-open on real cut points. Exclude any NUL-bearing key (a stored PK cannot contain
    # NUL in practice).
    distinct_df = (df.select(pkc.alias("k"))
                     .where(F.col("k").isNotNull())
                     .where(~F.col("k").contains("\u0000"))
                     .distinct()
                     .orderBy("k"))
    n_distinct = distinct_df.count()
    if n_distinct <= TEXT_SAMPLE_MAX:
        sample = [r["k"] for r in distinct_df.collect()]
    else:
        # Evenly pick TEXT_SAMPLE_MAX rows across the ordered distinct set using row_number.
        from pyspark.sql.window import Window
        w = Window.orderBy("k")
        ranked = distinct_df.withColumn("_rn", F.row_number().over(w) - 1)
        step = n_distinct / float(TEXT_SAMPLE_MAX)
        picks = {int(i * step) for i in range(TEXT_SAMPLE_MAX)}
        picks.add(0); picks.add(n_distinct - 1)           # always include min & max
        sample = [r["k"] for r in ranked.where(F.col("_rn").isin(list(picks)))
                  .orderBy("_rn").collect()]
    # Guarantee min/max endpoints present and sample sorted/distinct (belt-and-suspenders).
    if sample and sample[0] != str(row["mn"]):
        sample = [str(row["mn"])] + sample
    if sample and sample[-1] != str(row["mx"]):
        sample = sample + [str(row["mx"])]
    # de-dup adjacent while preserving order
    dedup = []
    for k in sample:
        if not dedup or k != dedup[-1]:
            dedup.append(k)
    return (str(row["mn"]), str(row["mx"]), int(row["n"]), dedup)


def _run_uuid_and_leak_checks(cursor, dsql_schema, dsql_table, uuid_cols, target_columns):
    """Shared post-load content checks (single source of truth — used by BOTH the V5
    whole-table path and validate_whole_table_after_ranges, so the uuid regex / leak
    patterns / error strings can't drift between the two). Appends any problems to a
    list and returns it: (a) uuid-format scan (case-insensitive) on each uuid column;
    (b) auth-token-leak scan on the first uuid/target column. `cursor` is an open cursor
    the caller owns."""
    errors = []
    # These are full-table WHERE scans. On large DSQL tables they can return XX000 "server
    # unavailable" (same limit that blocks COUNT(*)). They are also largely redundant: the
    # per-row UUID shape guard (_guard_row/flatten_chunk) validates every uuid BEFORE insert,
    # so a bad uuid can't be committed. Each scan is wrapped: if it can't complete (XX000 /
    # txn-age) we WARN and skip it; a COMPLETED scan that finds a problem still reports it.
    # UUID format check — lower() the stored value before matching so a canonical-uppercase
    # uuid is not falsely flagged (case-insensitive check).
    for uuid_col in uuid_cols:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {dsql_schema}.{dsql_table}
                WHERE "{uuid_col}" IS NOT NULL
                AND lower("{uuid_col}"::text) !~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            """)
            bad = cursor.fetchone()[0]
            if bad > 0:
                errors.append(f"INVALID UUID: {bad} rows in '{uuid_col}'")
        except Exception as _ue:
            if is_txn_timeout(_ue) or is_transient_server_error(_ue):
                print(f"    ⚠️ uuid-format scan on '{uuid_col}' could not complete on this "
                      f"large table ({type(_ue).__name__}) — WARNING; per-row load guard "
                      f"already validated uuids.")
            else:
                errors.append(f"UUID SCAN error on '{uuid_col}': {_ue}")
    # Auth token leak detection.
    leak_col = uuid_cols[0] if uuid_cols else (target_columns[0] if target_columns else None)
    if leak_col:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {dsql_schema}.{dsql_table}
                WHERE CAST("{leak_col}" AS text) LIKE '%X-Amz-Algorithm%'
                OR CAST("{leak_col}" AS text) LIKE '%Action=DbConnect%'
            """)
            leak = cursor.fetchone()[0]
            if leak > 0:
                errors.append(f"AUTH TOKEN LEAK: {leak} rows in '{leak_col}'")
        except Exception as _le:
            if is_txn_timeout(_le) or is_transient_server_error(_le):
                print(f"    ⚠️ auth-token-leak scan on '{leak_col}' could not complete on "
                      f"this large table ({type(_le).__name__}) — WARNING, not failing.")
            else:
                errors.append(f"LEAK SCAN error on '{leak_col}': {_le}")
    return errors


def _run_whole_table_count(cursor, dsql_schema, dsql_table, expected_total, errors):
    """Whole-table exact COUNT(*) == expected_total (viable on DSQL: ~60s/30M). Appends to
    errors on mismatch. If COUNT(*) itself exceeds the DSQL txn/scan limit (txn-age/XX000 on
    an extremely large table), fall back to pg_class.reltuples as an INFORMATIONAL log only
    (sampled estimate, too noisy to gate on) — the always-on per-file rows_read==written gate
    remains the authoritative no-loss guarantee."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {dsql_schema}.{dsql_table}")
        actual = cursor.fetchone()[0]
        if actual != expected_total:
            errors.append(f"ROW COUNT: loaded {expected_total}, target has {actual} "
                          f"(exact whole-table COUNT(*)).")
        else:
            print(f"    ✓ exact whole-table COUNT(*) == loaded ({expected_total:,})")
    except Exception as _ce:
        if is_txn_timeout(_ce) or is_transient_server_error(_ce):
            est = _read_reltuples_stable(cursor, dsql_schema, dsql_table)
            if est < 0:
                print(f"    ⚠️ COUNT(*) exceeded DSQL limits and reltuples unavailable — "
                      f"no-loss relies on the per-file rows_read==written gate (already "
                      f"passed); no-dup on the PK. ({type(_ce).__name__})")
            else:
                pct = 100.0 * abs(est - expected_total) / max(1, expected_total)
                print(f"    ⚠️ COUNT(*) exceeded DSQL limits; reltuples estimate {est:,} vs "
                      f"loaded {expected_total:,} (Δ {pct:.1f}%) — INFORMATIONAL only (sampled "
                      f"estimate, not a gate). No-loss = per-file gate; no-dup = PK.")
        else:
            errors.append(f"ROW COUNT check error: {_ce}")


def validate_whole_table_after_ranges(dsql_schema, dsql_table, uuid_cols,
                                      target_columns, expected_total, pk_col=None,
                                      file_ranges=None):
    """Whole-table validation run ONCE after all files load. Confirms no-loss and runs
    uuid/leak scans. Raises on mismatch.

    NO-LOSS: authoritative guarantee is the always-on per-file gate (rows_read==total_written)
    during load. This function CONFIRMS it against DSQL:
      - If file_ranges is provided AND the files are PK-DISJOINT (ordered key, e.g. RAW-hex->
        uuid), do an EXACT per-file COUNT(*) WHERE pk in [file_min, file_max] == file rows —
        an index-range scan per file, stronger (per-file) and parallel-safe (disjoint slices).
      - Else fall back to a single whole-table exact COUNT(*) == expected_total (viable on
        DSQL: ~60s/30M). Only if COUNT(*) itself exceeds the txn limit -> reltuples INFO.
    NO-DUP: guaranteed by the target PRIMARY KEY (23505 at insert); no distinct scan."""
    conn = connect_dsql()
    # autocommit so each read-only validation query stands alone: if one scan errors (e.g.
    # XX000 on a large table) it can't poison a transaction and break the remaining checks.
    # No ANALYZE here — DSQL auto-analyzes, so reltuples is current; ANALYZE would be
    # redundant and 0A000 in a txn.
    conn.autocommit = True
    cursor = conn.cursor()
    errors = []
    try:
        # NO-LOSS: prefer EXACT PER-FILE range counts when the PK is file-ordered (disjoint
        # per-file [min,max] slices, e.g. RAW-hex->uuid) — each file gets an index-range
        # COUNT(*) WHERE pk in [file_min,file_max] == its committed rows (verifies each file
        # individually). If files overlap (random uuid) or no PK / no ranges, fall back to a
        # single whole-table COUNT(*).
        if pk_col and file_ranges and _files_pk_disjoint(file_ranges):
            print(f"    ⑃ per-file range-count validation: {len(file_ranges)} files are "
                  f"PK-disjoint (ordered key) — verifying each file's rows via index-range "
                  f"COUNT(*) on '{pk_col}'")
            per_file_ok = True
            checked = 0
            for fr in file_ranges:
                lo, hi, exp_rows = fr["pk_min"], fr["pk_max"], fr["rows"]
                try:
                    cursor.execute(
                        f'SELECT COUNT(*) FROM {dsql_schema}.{dsql_table} '
                        f'WHERE "{pk_col}" >= %s AND "{pk_col}" <= %s', (lo, hi))
                    got = cursor.fetchone()[0]
                    checked += 1
                    if got != exp_rows:
                        errors.append(
                            f"PER-FILE ROW COUNT: file {fr['uri'].rsplit('/',1)[-1]} "
                            f"expected {exp_rows} rows in [{lo},{hi}] but target has {got}.")
                        per_file_ok = False
                except Exception as _pe:
                    # a single file's range scan failed to complete -> don't fail the load on
                    # it; fall back to the whole-table COUNT(*) below for the definitive check.
                    print(f"    ⚠️ per-file range count failed for "
                          f"{fr['uri'].rsplit('/',1)[-1]} ({type(_pe).__name__}); will use "
                          f"whole-table COUNT(*) instead.")
                    per_file_ok = None   # signal: fall through to whole-table
                    break
            if per_file_ok is True:
                print(f"    ✓ per-file range counts all match ({checked} files, "
                      f"{expected_total:,} rows total) — exact, no loss")
            # if per_file_ok is None (a scan failed), fall through to whole-table COUNT(*)
            _do_whole_count = (per_file_ok is None)
        else:
            _do_whole_count = True

        if _do_whole_count:
            _run_whole_table_count(cursor, dsql_schema, dsql_table, expected_total, errors)
        errors.extend(_run_uuid_and_leak_checks(
            cursor, dsql_schema, dsql_table, uuid_cols, target_columns))
    finally:
        cursor.close()
        conn.close()

    # NO-DUP: intentionally NO post-load COUNT(DISTINCT pk) scan. (1) REDUNDANT for a PK'd
    # table — DSQL's PRIMARY KEY unique index rejects any duplicate pk at INSERT (23505), so
    # duplicates cannot be committed. (2) NOT VIABLE on DSQL at scale — a COUNT(DISTINCT)
    # full scan returns XX000 "server unavailable" on multi-million-row tables. No-loss is
    # covered by the row-count check above; no-dup by the PK constraint during load. So the
    # pk_col path is a no-op; the parameter is retained for signature compatibility.
    _ = pk_col  # (retained for compatibility; no distinct scan performed)

    if errors:
        raise Exception("VALIDATION FAILED (whole-table, post-range): " + "; ".join(errors))


def load_one_table_parallel(s3_client, entry, config, pk_col, min_id, max_id, total_rows,
                            sample=None,
                            pk_kind="integer"):
    """[retired range path] Range-parallel load of one large single-PK table: plan
    ranges, load them concurrently, validate once. Superseded by the per-file path
    (load_one_table_chunked); retained for reference / non-file callers."""
    dsql_schema = config['metadata']['dsql_schema']
    dsql_table = config['metadata']['dsql_table']
    target_columns = config['target_columns']
    type_categories = config['type_categories']
    uuid_cols = sorted([n for n in target_columns if type_categories.get(n) == 'uuid'])

    # WHOLE-TABLE empty contract (once, before launching ranges) via the SINGLE shared
    # barrier: verifies the target is empty AND registers it as empty-verified for this
    # attempt, which UNLOCKS blank_pk_range for the ranges below (blank_pk_range refuses on
    # any table not registered here). Ranges DELETE-by-range as they go, but only against a
    # table proven empty at the start of THIS attempt. A Glue retry (fresh JVM) resets the
    # registry, so a partially-loaded table must be re-blanked. resume_ok + pk_col let a
    # previously-attempted range table auto-reblank+reload.
    #
    # RESUME: if resume is enabled AND a prior range-status file exists for this table, we do
    # NOT reblank the whole table (that would throw away completed ranges). Instead we
    # register it empty-verified WITHOUT clearing it and let each range's own idempotent
    # blank_pk_range+reload handle only the incomplete ranges. Only triggers when a
    # range-status file exists (written only by THIS pipeline), so pre-existing customer
    # data is never adopted.
    ws3_main = make_boto_client('s3')
    completed = load_completed_ranges(ws3_main, dsql_schema, dsql_table)
    range_resume = bool(completed) and AUTO_REBLANK_ON_RESUME and _resume_ok_for(dsql_schema, dsql_table)
    if range_resume:
        print(f"  ↺ PER-RANGE RESUME [{dsql_schema}.{dsql_table}]: found "
              f"{len(completed)} completed range(s) from a prior attempt "
              f"({sum(v.get('rows',0) for v in completed.values()):,} rows already loaded) "
              f"-> skipping those, reloading ONLY the incomplete ranges (no whole-table "
              f"reblank; each incomplete range idempotently re-clears its own [lo,hi)).")
        # Register empty-verified WITHOUT clearing (unlocks blank_pk_range for incomplete
        # ranges). We do NOT run assert_empty_or_register's reblank here.
        with _EMPTY_VERIFIED_LOCK:
            _EMPTY_VERIFIED.add(f"{dsql_schema}.{dsql_table}")
    else:
        assert_empty_or_register(dsql_schema, dsql_table,
                                 resume_ok=_resume_ok_for(dsql_schema, dsql_table),
                                 pk_col=pk_col,
                                 any_col=(target_columns[0] if target_columns else None))

    # Plan ranges with the KIND-appropriate planner (all return half-open [lo,hi)).
    if pk_kind == "uuid":
        ranges = plan_ranges_hex(min_id, max_id, total_rows, TARGET_ROWS_PER_PARTITION,
                                 max_concurrency=MAX_WRITE_CONCURRENCY)
    elif pk_kind == "text":
        ranges = plan_ranges_text(min_id, max_id, total_rows, TARGET_ROWS_PER_PARTITION,
                                  max_concurrency=MAX_WRITE_CONCURRENCY, sample=sample)
    else:
        ranges = plan_ranges(min_id, max_id, total_rows, TARGET_ROWS_PER_PARTITION,
                             max_concurrency=MAX_WRITE_CONCURRENCY)
    # Memory-aware inner concurrency: each concurrent range writer holds its own driver
    # stream, so clamp by driver free memory (the global semaphore is still the hard
    # cross-table cap). Does NOT change the range COUNT — only how many load at once.
    inner_conc, inner_reason = effective_inner_concurrency(PER_TABLE_WRITE_CONCURRENCY)
    print(f"  ↔ RANGE-PARALLEL {dsql_schema}.{dsql_table}: {total_rows:,} rows, "
          f"pk={pk_col} kind={pk_kind} [{min_id}..{max_id}] -> {len(ranges)} ranges, "
          f"inner_concurrency={inner_conc} (global cap={MAX_WRITE_CONCURRENCY}; "
          f"{inner_reason})")

    range_lock = threading.Lock()
    committed_total = {"n": 0}
    range_errors = []

    # The TOP range (last in the ascending list) is identified POSITIONALLY — NOT by
    # string-sniffing the hi bound. The top range's exclusive upper bound is an out-of-space
    # sentinel (text: max+'\x00'; uuid: 33-hex max+1) with no valid literal, so it omits its
    # ceiling. An INTERIOR text boundary can also legitimately end in '\x00', so
    # endswith('\x00') would WRONGLY mark it as the top range -> drop its ceiling -> overlap
    # later ranges -> DUPLICATE loads. Positional is_top (index == last) removes that
    # ambiguity for every kind.
    _top_idx = len(ranges) - 1

    def _load_range(idx_rg):
        idx, rg = idx_rg
        lo, hi = rg
        is_top = (idx == _top_idx)
        ws3 = make_boto_client('s3')
        # Global bound on TOTAL concurrent range writers across ALL tables so the outer
        # table pool x inner range pool can't overwhelm DSQL. Held for the whole range
        # load (its DSQL connection lifetime).
        with _DSQL_RANGE_WRITER_SEM:
            res = load_one_table(ws3, entry, pk_range=(pk_col, lo, hi, pk_kind, is_top),
                                 config=config)
        # RESUME: durably record this range as committed IMMEDIATELY after it
        # returns (its rows are committed in DSQL at this point), so a later attempt skips
        # it. Thread-safe (mark_range_done serializes its read-modify-write).
        mark_range_done(ws3, dsql_schema, dsql_table, lo, hi, is_top, res["rows"])
        with range_lock:
            committed_total["n"] += res["rows"]
        return (rg, res["rows"])

    # RESUME: seed the committed total with the rows from ALREADY-COMPLETED
    # ranges (loaded in a prior attempt, skipped this run). This keeps the source-count
    # gate honest: skipped_rows + this_attempt_rows must still equal total_rows, so a lost
    # range (neither skipped-with-rows nor reloaded) is still caught. Build the skip set
    # from the completed-ranges keys that MATCH a currently-planned range (a plan shift
    # invalidates old keys -> nothing skipped -> full reload).
    planned_keys = {}
    for _i, _rg in enumerate(ranges):
        _lo, _hi = _rg
        _is_top = (_i == _top_idx)
        planned_keys[_range_key(_lo, _hi, _is_top)] = (_i, _rg, _is_top)
    skip_keys = set(completed.keys()) & set(planned_keys.keys()) if range_resume else set()
    skipped_rows = sum(int(completed[k].get("rows", 0)) for k in skip_keys)
    if range_resume:
        committed_total["n"] += skipped_rows
        to_run = [(planned_keys[k][0], planned_keys[k][1]) for k in planned_keys
                  if k not in skip_keys]
        print(f"    ↺ resume: skipping {len(skip_keys)} completed range(s) "
              f"({skipped_rows:,} rows), reloading {len(to_run)} incomplete range(s)")
    else:
        to_run = list(enumerate(ranges))

    # Load ranges concurrently. Pool size = memory-clamped inner concurrency; the global
    # semaphore inside _load_range is still the hard cross-table cap.
    with ThreadPoolExecutor(max_workers=max(1, inner_conc),
                            thread_name_prefix="rngload") as pool:
        futs = {pool.submit(_load_range, (i, rg)): rg for i, rg in to_run}
        for fut in as_completed(futs):
            rg = futs[fut]
            try:
                _, n = fut.result()
                print(f"    ✓ range {rg} -> {n:,} rows")
            except Exception as e:
                range_errors.append((rg, str(e)))
                print(f"    ✗ range {rg} FAILED: {e}")

    if range_errors:
        # if any range failed due to driver OOM, surface it EXPLICITLY and attribute it to
        # the inner-concurrency setting: effective_inner_concurrency no longer memory-clamps,
        # so an over-high PER_TABLE_WRITE_CONCURRENCY on a small driver can OOM (each
        # concurrent range writer holds a toLocalIterator partition buffer). Fail LOUD with
        # actionable guidance rather than a cryptic JVM/executor stack trace.
        _oom = [ (rg, msg) for rg, msg in range_errors if _is_oom_error(msg) ]
        if _oom:
            raise Exception(
                f"DRIVER OUT-OF-MEMORY loading {dsql_schema}.{dsql_table}: "
                f"{len(_oom)}/{len(to_run)} range(s) failed with an OOM/memory error "
                f"(first: {_oom[0][1][:200]}). CAUSE: inner write concurrency "
                f"(PER_TABLE_WRITE_CONCURRENCY={PER_TABLE_WRITE_CONCURRENCY}) is too high for "
                f"this driver — {PER_TABLE_WRITE_CONCURRENCY} concurrent range writers each "
                f"buffer a toLocalIterator partition on the driver. FIX: lower "
                f"--per_table_write_concurrency, and/or use a larger driver (G.2X/G.4X), "
                f"and/or lower --target_rows_per_partition (smaller per-range buffers). "
                f"Completed ranges are checkpointed + will be skipped on re-run "
                f"(no dup / no loss).")
        raise Exception(
            f"{len(range_errors)}/{len(to_run)} loaded ranges FAILED for "
            f"{dsql_schema}.{dsql_table} (first: {range_errors[0][1]}). "
            f"Re-run: completed ranges are recorded + skipped, and each incomplete range "
            f"is idempotent (re-clears its own [lo,hi) then reloads) — no dup / no loss.")

    # SOURCE-COUNT GATE: the self-consistent check (DSQL COUNT == sum of range rows) CANNOT
    # detect a row that no range loaded — a PK that doesn't parse (casts to NULL) fails both
    # (pk>=lo) and (pk<hi), is matched by NO range, and never written; absent from both the
    # committed total and the DSQL count, so downstream validation would still pass. Compare
    # against the independent Spark CSV COUNT(*) (total_rows from spark_pk_bounds, which
    # counts EVERY row) and FAIL LOUDLY on a shortfall, so PK-unparseable/dropped rows can't
    # vanish silently. (total_rows is the same value used to plan the ranges.)
    if committed_total["n"] != total_rows:
        _cause = {
            "integer": f"a PK ({pk_col}) value that does not parse as an integer",
            "uuid": f"a PK ({pk_col}) value that does not normalize to 32-hex",
            "text": f"a NULL/unrepresentable PK ({pk_col}) value",
        }.get(pk_kind, f"an out-of-range PK ({pk_col}) value")
        raise Exception(
            f"ROW COUNT (source vs range-loaded) for {dsql_schema}.{dsql_table}: Spark "
            f"counted {total_rows:,} CSV rows but the ranges committed "
            f"{committed_total['n']:,}. The {total_rows - committed_total['n']:,} missing "
            f"row(s) were matched by NO range — almost certainly rows with {_cause} "
            f"(CSV misalignment or a PK value that passed Job 1's type check but not the "
            f"range filter). Refusing to report success on a partial load. Fix the "
            f"source/mapping (or route this table to the whole-table path) and re-run "
            f"after re-blanking.")

    # Whole-table validation ONCE (count == sum of ranges, uuid + token-leak scans).
    validate_whole_table_after_ranges(dsql_schema, dsql_table, uuid_cols,
                                      target_columns, committed_total["n"])
    # RESUME: table fully loaded + validated -> clear its range-status file so
    # a future (re)load of this table starts with a clean slate (no stale completed-range
    # keys). Best-effort; a leftover file is harmless (next load's plan keys would still
    # have to match to skip anything).
    clear_range_status(ws3_main, dsql_schema, dsql_table)
    return {
        "table": f"{dsql_schema}.{dsql_table}",
        "rows": committed_total["n"],
        "ranges": len(ranges),
        "target_columns": len(target_columns),
        "skipped_columns": len(config.get('skip_columns', [])),
    }


# =============================================================================
# CHUNK FAN-OUT orchestration (large NON-numeric-PK tables) — OFF by default.
# Splits the table's S3 part-files across a few workers; each worker loads its disjoint
# subset via load_one_table(file_subset=...) (all proven guards/insert/retry reused),
# under the SAME global _DSQL_RANGE_WRITER_SEM as the range path. Empty gate ONCE;
# recovery is WHOLE-TABLE reload (any worker failure fails the table). No PK needed;
# disjoint files => no duplicate rows, no ON CONFLICT, no schema change.
# =============================================================================
def _is_full_load_key(key):
    """True only for a DMS FULL-LOAD CSV object key — used to keep the loader from ever
    ingesting CDC output that shares the table prefix.

    Two independent exclusions (either one is sufficient):
      1. PATH: reject any key with a `processed/` or `failed/` path segment — those are the
         CDC file-lifecycle folders; full-load files never live there.
      2. NAME: require the basename to match the DMS full-load pattern LOAD*.csv
         (case-insensitive). DMS names full-load files LOAD00000001.csv, LOAD00000002.csv,
         ... (nested under partition subfolders for a parallel full load). CDC files are
         timestamp-named (e.g. 20260921-172204370.csv) and carry a leading `Op` column, so
         name-filtering to LOAD*.csv excludes them even if they sit at the table root.
    """
    kl = key.lower()
    segments = kl.split("/")
    if "processed" in segments or "failed" in segments:
        return False
    base = segments[-1]
    return base.startswith("load") and base.endswith(".csv")


def s3_list_table_files(s3_client, dms_s3_path):
    """List the DMS CSV part-file object paths (full s3:// URIs) under a table's prefix,
    with sizes. Skips zero-byte keys and any 'directory placeholder' keys (ending in
    '/'). Returns a list of (s3_uri, size_bytes), largest first. Metadata only."""
    bucket, prefix = split_s3(dms_s3_path)
    files = []
    token = None
    skipped_cdc = 0
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            key = o["Key"]
            size = int(o.get("Size", 0))
            if key.endswith("/") or size <= 0:
                continue   # directory placeholder / empty object
            if not _is_full_load_key(key):
                # SILENT-CORRUPTION GUARD: only DMS FULL-LOAD files (LOAD*.csv) are loaded
                # here. Exclude CDC output — CDC files live under processed/ + failed/ AND
                # carry a leading "Op" column (16 cols vs the full-load 15), so reading one
                # as full-load shifts every column. This happened in the field when a full
                # load was re-run over a prefix that still held earlier CDC files. CDC is
                # applied by the CDC job, never by the loader.
                skipped_cdc += 1
                continue
            files.append((f"s3://{bucket}/{key}", size))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    if skipped_cdc:
        print(f"    ⓘ s3_list_table_files: skipped {skipped_cdc} non-full-load object(s) "
              f"(CDC files under processed/ or failed/, or non-LOAD*.csv) under {dms_s3_path}")
    files.sort(key=lambda t: t[1], reverse=True)   # largest first (for balanced split)
    return files


def _split_files_round_robin(files, n_buckets):
    """Distribute (uri,size) files across n_buckets by size-descending round-robin —
    a cheap way to balance total bytes per worker (LPT-style). Returns a list of lists
    of s3 URIs; empty buckets are dropped so we never spawn idle workers."""
    n_buckets = max(1, int(n_buckets))
    buckets = [[] for _ in range(n_buckets)]
    loads = [0] * n_buckets
    for uri, size in files:   # already largest-first
        i = loads.index(min(loads))   # assign to the currently-lightest bucket
        buckets[i].append(uri)
        loads[i] += size
    return [b for b in buckets if b]


def load_one_table_chunked(s3_client, entry, config):
    """V11 PER-FILE parallel load of one large table (driver_threads substrate).

    The RESUME UNIT is a single CSV part-file. This REPLACES the PK-range path for all
    large tables: we partition purely by S3 part-file, load each file with its own worker
    in parallel, and checkpoint EACH FILE independently. On a later run we skip files
    already marked done and re-run ONLY the files that failed / never finished.

    Contract (preserves the absolute no-duplicate / no-loss requirement):
      1. Empty/register contract. On a FRESH load (no prior file-status) the whole table
         must be empty (assert_empty_or_register). On a RESUME (prior file-status exists)
         we DO NOT reblank — completed files stay, only incomplete files are re-run.
      2. List part-files (largest first). Each file = one independent unit.
      3. For every file:
           - already 'done'      -> SKIP (its rows are already in the target).
           - 'started' not done  -> mid-file crash last time. Reload with a per-chunk
                                     (v11 legacy note; v13 has no commit-probe) chunks that
                                     already landed are skipped -> NO DUPLICATES. Requires
                                     a single-col PK; if the table has none, we cannot
                                     safely resume a partial file -> WHOLE-TABLE reblank +
                                     restart (loud), the only no-dup option without a PK.
           - never attempted     -> fresh load of just that file.
         Each file is marked 'started' before load and 'done' (with rows) after commit.
      4. Workers run under the GLOBAL _DSQL_RANGE_WRITER_SEM so total concurrent DSQL
         writers across ALL parallel tables stay <= MAX_WRITE_CONCURRENCY.
      5. Validate the assembled table ONCE: COUNT(*) == (rows from resumed 'done' files) +
         (rows loaded this run). D1 source-count gate + uuid/leak scans preserved.
    """
    dsql_schema = config['metadata']['dsql_schema']
    dsql_table = config['metadata']['dsql_table']
    dms_s3_path = config['metadata']['dms_s3_path']
    target_columns = config['target_columns']
    type_categories = config['type_categories']
    uuid_cols = sorted([n for n in target_columns if type_categories.get(n) == 'uuid'])

    # Single-col PK? Determines whether an INCOMPLETE file can be probe-resumed (no dup)
    # or must trigger a whole-table reblank. (Same metadata the per-chunk probe uses.)
    _pk_meta = (config.get('metadata', {}).get('primary_key') or {})
    _pk_cols = _pk_meta.get('columns') or []
    has_single_pk = (len(_pk_cols) == 1)

    resume_ok = _resume_ok_for(dsql_schema, dsql_table)
    done_map, started_set = load_file_status(s3_client, dsql_schema, dsql_table)
    prior_done_rows = sum(done_map.values())
    resuming = bool(done_map or started_set)

    # (2) List part-files.
    files = s3_list_table_files(s3_client, dms_s3_path)
    if not files:
        raise Exception(
            f"PER-FILE LOAD [{dsql_schema}.{dsql_table}]: no CSV part-files found under "
            f"{dms_s3_path}. Nothing to load — verify the DMS export.")
    all_uris = [uri for (uri, _sz) in files]

    # (1) Empty contract. On a fresh load (nothing checkpointed) require empty. On resume
    # we keep the completed files' rows in place and only re-run the incomplete ones, so
    # we must NOT reblank. If resume is disabled entirely, always enforce empty.
    if resuming and resume_ok:
        print(f"  ↩ PER-FILE RESUME {dsql_schema}.{dsql_table}: "
              f"{len(done_map)} file(s) done ({prior_done_rows:,} rows), "
              f"{len(started_set)} started-but-incomplete, {len(all_uris)} total.")
        # Register the table verified for this attempt WITHOUT reblanking (completed files'
        # rows must stay). Unlocks the per-file workers' belt-and-suspenders gate. We do NOT
        # run assert_empty_or_register here — that would reblank and destroy done files.
        with _EMPTY_VERIFIED_LOCK:
            _EMPTY_VERIFIED.add(f"{dsql_schema}.{dsql_table}")
    else:
        # Fresh (or resume disabled): enforce the whole-table empty contract ONCE.
        assert_empty_or_register(dsql_schema, dsql_table,
                                 resume_ok=resume_ok,
                                 pk_col=(_pk_cols[0] if has_single_pk else None),
                                 any_col=(target_columns[0] if target_columns else None))
        done_map, started_set, prior_done_rows = {}, set(), 0

    # Decide, per file, what to do.
    to_load = []          # (uri, probe_all)  -> load these
    reblank_required = False
    for uri in all_uris:
        if uri in done_map:
            continue                                   # already fully loaded -> skip
        if uri in started_set:
            # Mid-file crash last run. Probe-resume iff we have a single-col PK.
            if has_single_pk:
                to_load.append((uri, True))            # reload w/ per-chunk probe (no dup)
            else:
                reblank_required = True                # no PK -> can't partial-resume
        else:
            to_load.append((uri, False))               # never attempted -> fresh load

    # NO-PK mid-file crash -> the only no-dup option is a full reblank + reload of ALL
    # files. Do it loudly, then reset checkpoints and load every file fresh.
    if reblank_required:
        print(f"  ⚠️ PER-FILE RESUME {dsql_schema}.{dsql_table}: a file crashed mid-load "
              f"AND the table has NO single-col PK -> cannot safely resume a partial file "
              f"without risking duplicates. Falling back to WHOLE-TABLE REBLANK + full "
              f"reload (the only no-dup path without a PK).", flush=True)
        blank_whole_table(dsql_schema, dsql_table,
                          pk_col=None,
                          any_col=(target_columns[0] if target_columns else None))
        clear_file_status(s3_client, dsql_schema, dsql_table)
        done_map, started_set, prior_done_rows = {}, set(), 0
        to_load = [(uri, False) for uri in all_uris]

    if not to_load:
        print(f"  ✓ PER-FILE {dsql_schema}.{dsql_table}: all {len(all_uris)} file(s) "
              f"already done on a prior run ({prior_done_rows:,} rows) — nothing to load.")
        validate_whole_table_after_ranges(dsql_schema, dsql_table, uuid_cols,
                                          target_columns, prior_done_rows,
                                          pk_col=(_pk_cols[0] if has_single_pk else None))
        clear_file_status(s3_client, dsql_schema, dsql_table)
        return {
            "table": f"{dsql_schema}.{dsql_table}",
            "rows": prior_done_rows,
            "file_workers": 0,
            "part_files": len(all_uris),
            "resumed_files": len(done_map),
            "target_columns": len(target_columns),
            "skipped_columns": len(config.get('skip_columns', [])),
        }

    # V16 PER-FILE fan-out width: open up to MAX_FILES_IN_PARALLEL (default 30) part-files
    # CONCURRENTLY, bounded by how many files actually need loading. This is DECOUPLED from
    # PER_TABLE_WRITE_CONCURRENCY (which historically capped this at 10) so the customer's
    # 250MB/30-files-in-parallel model actually loads 30 at once. The global
    # _DSQL_RANGE_WRITER_SEM (MAX_WRITE_CONCURRENCY) still caps total writers across ALL
    # tables; warn (don't silently throttle) if it would cap this single table's fan-out.
    _fanout_target = max(1, min(MAX_FILES_IN_PARALLEL, len(to_load)))
    if MAX_WRITE_CONCURRENCY < _fanout_target:
        print(f"  ⚠️ {dsql_schema}.{dsql_table}: MAX_WRITE_CONCURRENCY={MAX_WRITE_CONCURRENCY} "
              f"< requested file fan-out {_fanout_target} — the global writer cap will "
              f"THROTTLE this table's parallelism. Raise --max_write_concurrency to "
              f">= {_fanout_target} (or --max_files_in_parallel lower) to avoid it.")
    _requested_conc = _fanout_target
    inner_conc, inner_reason = effective_inner_concurrency(_requested_conc)
    # V16 DRIVER-MEMORY SOFT WARNING (does NOT throttle — the customer chose "go big, don't
    # throttle"). The load is DRIVER-SIDE: each concurrent file worker pulls a whole ~250MB
    # Spark partition to the driver via toLocalIterator, so peak driver working set grows
    # ~linearly with the fan-out. If free driver memory looks too small for
    # inner_conc x PER_WORKER_MEM_BUDGET_MB, WARN loudly so an operator can size up
    # (G.4X/G.8X) or lower --max_files_in_parallel — rather than silently clamping (which
    # would defeat the intended 30-way parallelism) or OOMing without explanation.
    try:
        _free_mb = driver_free_mem_mb()
        _need_mb = inner_conc * PER_WORKER_MEM_BUDGET_MB
        if _free_mb is not None and _free_mb < _need_mb:
            print(f"  ⚠️ {dsql_schema}.{dsql_table}: driver free≈{_free_mb}MB but a "
                  f"{inner_conc}-file fan-out may need ≈{_need_mb}MB "
                  f"({PER_WORKER_MEM_BUDGET_MB}MB/file). NOT throttling (by design) — but "
                  f"this driver may OOM. Use a bigger WorkerType (G.4X/G.8X) or lower "
                  f"--max_files_in_parallel. (250MB DMS files keep per-file memory bounded.)")
    except Exception:
        pass   # advisory only; never fail the load on the mem probe
    print(f"  ⑃ PER-FILE {dsql_schema}.{dsql_table}: {len(all_uris)} part-file(s); "
          f"{len(to_load)} to load ({len(done_map)} skipped as done), "
          f"file_parallelism={inner_conc} (max_files_in_parallel={MAX_FILES_IN_PARALLEL}, "
          f"global write cap={MAX_WRITE_CONCURRENCY}; {inner_reason})")

    lock = threading.Lock()
    committed_total = {"n": 0}
    worker_errors = []
    file_ranges = []   # per-file (pk_min, pk_max, rows) from THIS run, for range-count validation

    # CONNECTION REUSE: one pool for this table's fan-out. Workers borrow a long-lived
    # connection and return it (open) for the next file, so N files share ~inner_conc
    # connections instead of a TLS+IAM handshake per file. Sized implicitly to inner_conc
    # (borrow opens on demand only when idle is empty, and only inner_conc workers run at
    # once), so PARALLELISM IS UNCHANGED — every concurrent worker still gets its own conn.
    _conn_pool = ConnPool()

    def _load_file(uri, probe_all):
        ws3 = make_boto_client('s3')
        # resume: mark 'started' BEFORE loading (so a mid-file crash is detectable next
        # run), load the file (probe_all=True re-runs a previously-incomplete file with a
        # per-chunk PK commit-probe so already-committed chunks are skipped -> no dup), then
        # mark 'done' only after a clean commit of the whole file.
        mark_file_started(ws3, dsql_schema, dsql_table, uri)
        with _DSQL_RANGE_WRITER_SEM:
            res = load_one_table(ws3, entry, file_subset=[uri], config=config,
                                 probe_all_chunks=probe_all, conn_pool=_conn_pool)
        mark_file_done(ws3, dsql_schema, dsql_table, uri, res["rows"])
        with lock:
            committed_total["n"] += res["rows"]
            file_ranges.append({"uri": uri, "rows": res["rows"],
                                "pk_min": res.get("pk_min"), "pk_max": res.get("pk_max")})
        return (uri, res["rows"])

    try:
        with ThreadPoolExecutor(max_workers=max(1, inner_conc),
                                thread_name_prefix="fileload") as pool:
            futs = {pool.submit(_load_file, uri, pa): uri for (uri, pa) in to_load}
            for fut in as_completed(futs):
                uri = futs[fut]
                try:
                    _uri, n = fut.result()
                    print(f"    ✓ file {_uri.rsplit('/', 1)[-1]} -> {n:,} rows")
                except Exception as e:
                    worker_errors.append(str(e))
                    print(f"    ✗ file {uri.rsplit('/', 1)[-1]} FAILED: {e}")
    finally:
        _conn_pool.close_all()   # close all reused connections once the fan-out is done

    if worker_errors:
        # Per-file recovery: the failed file(s) are NOT marked done, so a re-run reloads
        # ONLY those files (probe-guarded for no dup). Completed files stay checkpointed.
        raise Exception(
            f"{len(worker_errors)}/{len(to_load)} file worker(s) FAILED for "
            f"{dsql_schema}.{dsql_table} (first: {worker_errors[0]}). "
            f"RE-RUN to resume: completed files are checkpointed and will be SKIPPED; "
            f"only the failed file(s) reload"
            f"{' with a per-chunk commit-probe (no duplicates)' if has_single_pk else ''}"
            f". No whole-table reblank.")

    total_rows = prior_done_rows + committed_total["n"]
    # (5) Validate the assembled table ONCE.
    validate_whole_table_after_ranges(dsql_schema, dsql_table, uuid_cols,
                                      target_columns, total_rows,
                                      pk_col=(_pk_cols[0] if has_single_pk else None),
                                      file_ranges=file_ranges)
    # Fully complete -> clear the per-file checkpoint so a future load starts clean.
    clear_file_status(s3_client, dsql_schema, dsql_table)
    return {
        "table": f"{dsql_schema}.{dsql_table}",
        "rows": total_rows,
        "file_workers": len(to_load),
        "part_files": len(all_uris),
        "resumed_files": len(done_map),
        "target_columns": len(target_columns),
        "skipped_columns": len(config.get('skip_columns', [])),
    }


def load_table_auto(s3_client, entry):
    """V6 dispatcher: automatically choose the whole-table (V5) path or the range-
    parallel path per table. ZERO required config — routes on the CSV byte-size gate +
    the span_recoverable flag from Job 1 v2. Falls back to the exact V5 path in every
    case where parallelism doesn't apply (small table, no numeric PK, disabled, old
    Job1 config, or executors mode not verified)."""
    # Load the per-table config once (used for routing; load_one_table re-reads it —
    # cheap S3 GET, kept simple to avoid changing load_one_table's signature further).
    cfg_bucket, cfg_key = split_s3(entry['config_s3_path'])
    config = json.loads(
        s3_client.get_object(Bucket=cfg_bucket, Key=cfg_key)['Body'].read().decode('utf-8'))
    dms_s3_path = config['metadata']['dms_s3_path']

    # Gate 0: master switch. If parallelism is off, take the whole-table path for all.
    # (Pass the already-parsed config to avoid a second identical S3 GET.)
    if not V6_PARALLEL_ENABLED:
        return load_one_table(s3_client, entry, config=config)   # unchanged V5 path

    # Gate 0b: per-table force-single-stream override. Pin a specific (parse-bound) large
    # table to the single-stream whole-table path without disabling parallelism for
    # everything else. Matched case-insensitively on "schema.table".
    _label = f"{entry['dsql_schema']}.{entry['dsql_table']}".lower()
    if _label in FORCE_V5_TABLES:
        print(f"    (routing: {_label} in force_v5_tables -> V5 whole-table path)")
        return load_one_table(s3_client, entry, config=config)

    # Gate 1 (cheap): CSV byte size. Small tables -> whole-table path (parallelism isn't
    # worth it below the threshold).
    total_bytes = s3_table_total_bytes(s3_client, dms_s3_path)
    if total_bytes < LARGE_TABLE_BYTES_THRESHOLD:
        print(f"    (routing: {total_bytes:,} B < {LARGE_TABLE_BYTES_THRESHOLD:,} B "
              f"gate -> V5 whole-table path)")
        return load_one_table(s3_client, entry, config=config)

    # LARGE table: all large tables load via the PER-FILE path (load_one_table_chunked).
    # Partitioning is by CSV part-file and the resume unit is a single file. A single-column
    # PK (when present) is used only for the per-chunk commit-probe that makes reloading an
    # incomplete file duplicate-free — not for range planning.
    if V6_WRITE_MODE == "executors":
        raise Exception(
            "write_mode=executors is NOT verified for this environment. Run "
            "dsql_executor_probe.py first (executor->DSQL reachability). Until then "
            "use write_mode=driver_threads (default). Refusing an unverified substrate.")
    print(f"    (routing: {total_bytes:,} B large -> PER-FILE parallel load "
          f"[PK-range retired in v11])")
    return load_one_table_chunked(s3_client, entry, config)


# =============================================================================
# MAIN — loop over the master index
# =============================================================================
print("=" * 70)
print("JOB 2 V15 (PER-FILE PARALLEL + PER-FILE RESUME, LITERAL-INSERT, RELTUPLES VALIDATION): Data Load (Driver-Only Mode) — up to N tables")
print("=" * 70)

s3_client = make_boto_client('s3')   # main-thread client (used before/after the pool)

print(f"\nLoading master index: {INDEX_S3_PATH}")
idx_bucket, idx_key = split_s3(INDEX_S3_PATH)
idx_obj = s3_client.get_object(Bucket=idx_bucket, Key=idx_key)
index_doc = json.loads(idx_obj['Body'].read().decode('utf-8'))
tables = index_doc.get('tables', [])
print(f"  Tables to load: {len(tables)}")
if not tables:
    raise Exception("Master index has no tables — run Job 1 first.")

status = load_status(s3_client)
done_before = {t for t, v in status["tables"].items() if v.get("status") == "done"}
print(f"  Status file: {STATUS_S3_PATH}")
print(f"  Already done (will skip): {len(done_before)}")

# Resume-eligibility (broadened below to include any not-done table in THIS manifest): a
# non-empty table the pipeline was pointed at but hasn't marked "done" is a partial load it
# owns, even if a prior run wrote no in_progress/failed marker.

succeeded = []
failed = []
skipped_done = []
total_rows_all = 0

# Build the worklist first (skip already-done tables up front, single-threaded, so
# the resume/skip decision is deterministic and not racy).
worklist = []
for i, entry in enumerate(tables, start=1):
    label = f"{entry['dsql_schema']}.{entry['dsql_table']}"
    if label in done_before:
        print(f"[{i}/{len(tables)}] ↩ SKIP {label} (already done in status file)")
        skipped_done.append(label)
    else:
        worklist.append((i, label, entry))

# RESUME ELIGIBILITY: every not-done table IN THIS MANIFEST is resume-eligible. This is
# safe — the pipeline was explicitly pointed at these tables, so a non-empty one is a
# partial load it owns (not an unrelated customer table). A table NOT in this manifest is
# never in _RESUME_ELIGIBLE, so it can never be auto-reblanked. Only meaningful when
# AUTO_REBLANK_ON_RESUME is on.
if AUTO_REBLANK_ON_RESUME:
    for _i, _label, _entry in worklist:
        _RESUME_ELIGIBLE.add(_label.lower())
    print(f"  Resume-eligible (in-manifest, not-done): {len(_RESUME_ELIGIBLE)} "
          f"[AUTO_REBLANK_ON_RESUME=ON -> a non-empty one auto-reblanks (whole-table) or "
          f"per-range-resumes (if range-status exists) instead of failing]")

# Effective concurrency: min(requested, hard cap, tables-to-load), then optionally
# throttled by available driver memory (toLocalIterator buffers ~1 partition/table).
workers, throttle_reason = compute_effective_workers(MAX_PARALLEL_TABLES, len(worklist))
print(f"\n  Parallel workers: {workers} "
      f"(MAX_PARALLEL_TABLES={MAX_PARALLEL_TABLES}, cap={PARALLEL_HARD_CAP}, "
      f"to-load={len(worklist)}, throttle: {throttle_reason})")

# SINGLE-TABLE WRITE-CONCURRENCY BOOST: with exactly one table to load there is no
# cross-table contention for the global write semaphore, so raise the effective
# PER_TABLE_WRITE_CONCURRENCY to SINGLE_TABLE_WRITE_CONCURRENCY (only if higher) and grow
# the global semaphore to match. effective_inner_concurrency picks up the boosted value at
# load time; the driver-memory clamp still applies.
if len(worklist) == 1 and SINGLE_TABLE_WRITE_CONCURRENCY > PER_TABLE_WRITE_CONCURRENCY:
    _prev = PER_TABLE_WRITE_CONCURRENCY
    PER_TABLE_WRITE_CONCURRENCY = SINGLE_TABLE_WRITE_CONCURRENCY
    if MAX_WRITE_CONCURRENCY < PER_TABLE_WRITE_CONCURRENCY:
        MAX_WRITE_CONCURRENCY = PER_TABLE_WRITE_CONCURRENCY
        _DSQL_RANGE_WRITER_SEM = threading.Semaphore(MAX_WRITE_CONCURRENCY)
    print(f"  ⑃ SINGLE-TABLE boost: only 1 table to load -> per-table write concurrency "
          f"{_prev} -> {PER_TABLE_WRITE_CONCURRENCY} (global cap={MAX_WRITE_CONCURRENCY}; "
          f"no cross-table contention; disjoint ranges don't OCC-conflict)")

# Guards for shared state mutated by worker threads:
#   _RESULTS_LOCK : protects succeeded/failed/total_rows_all AND the status dict +
#                   its S3 write (save_status rewrites the whole file, so concurrent
#                   writers must be serialized or they clobber each other).
# Each worker builds its log lines into a local buffer and prints them under the lock
# in one shot, so concurrent tables don't interleave mid-line in the Glue log.
_RESULTS_LOCK = threading.Lock()


def run_one_table(item):
    """Worker: load ONE table (own S3 client + own DSQL connections via
    load_one_table). Returns (label, result_or_None, error_or_None, log_lines).
    Never raises — errors are captured so one table's failure can't kill the pool
    (continue-on-failure, same guarantee as V4)."""
    i, label, entry = item
    log = [f"[{i}/{len(tables)}] Loading {label} ..."]
    # write an "in_progress" marker BEFORE loading so a mid-load crash leaves a durable
    # breadcrumb in the S3 status file. On a later re-run with AUTO_REBLANK_ON_RESUME, this
    # marker makes the table resume-eligible (auto-reblank + reload) instead of failing the
    # empty gate. Best-effort + serialized (save_status rewrites the whole file, so it holds
    # the results lock). The in_progress row is overwritten by the terminal status via
    # record_outcome.
    if AUTO_REBLANK_ON_RESUME:
        with _RESULTS_LOCK:
            status["tables"][label] = {"status": "in_progress", "at": utc_now_iso()}
            save_status(s3_client, status)
    # Boto3 clients are NOT thread-safe to SHARE and their CREATION is not thread-safe
    # either; give each worker its own, created via the serialized factory.
    worker_s3 = make_boto_client('s3')
    # PARALLELISM PROOF: print (NOT log.append) the per-table START/END with the driver
    # thread name + wall-clock epoch, flushed IMMEDIATELY, bypassing the per-worker `log`
    # buffer (flushed once at the end) — otherwise START/END would print together and prove
    # nothing. Overlapping windows on DISTINCT thread names == tables loading concurrently.
    # This is DRIVER-THREAD parallelism (the load runs on the driver), NOT Glue executor
    # parallelism.
    _th = threading.current_thread().name
    _t_start = time.time()
    print(f"[PARALLEL] START {label} thread={_th} epoch={_t_start:.3f}", flush=True)
    try:
        # EMPTY-AT-FULL-LOAD short-circuit. A table with 0 source rows at full-load time has
        # NO LOAD*.csv, so the normal per-file path would raise "no CSV part-files". But the
        # table is legit — it can still receive INSERT/UPDATE/DELETE during the CDC window —
        # so we must NOT fail it: mark the full load DONE (0 rows) so the CDC gate opens and
        # CDC applies any later rows. Discovery flags this as empty_at_discovery; we RE-CHECK
        # S3 here (files may have appeared between discovery and load — if so, fall through to
        # a normal load instead of skipping real data).
        if entry.get("empty_at_discovery") is True:
            if not s3_list_table_files(worker_s3, entry["dms_s3_path"]):
                log.append(f"  ○ EMPTY {label}: 0 full-load rows (no S3 files) — marking "
                           f"done(0); CDC will apply any rows that arrive later.")
                print(f"[PARALLEL] END   {label} thread={_th} epoch={time.time():.3f} "
                      f"dur={time.time()-_t_start:.1f}s EMPTY(done,0)", flush=True)
                return (label, {"rows": 0, "ranges": 0, "empty_at_full_load": True}, None, log)
            log.append(f"  ↪ {label}: was empty at discovery but S3 files now present — "
                       f"loading normally.")
        # auto-dispatch (whole-table path or range-parallel) per table.
        result = load_table_auto(worker_s3, entry)
        rngtxt = f", {result['ranges']} ranges" if result.get('ranges') else ""
        log.append(f"  ✓ DONE {label}: {result['rows']:,} rows{rngtxt}")
        print(f"[PARALLEL] END   {label} thread={_th} epoch={time.time():.3f} "
              f"dur={time.time()-_t_start:.1f}s OK", flush=True)
        return (label, result, None, log)
    except Exception as e:
        log.append(f"  ✗ FAILED {label}: {e}")
        print(f"[PARALLEL] END   {label} thread={_th} epoch={time.time():.3f} "
              f"dur={time.time()-_t_start:.1f}s FAILED", flush=True)
        return (label, None, str(e), log)


def record_outcome(label, result, error):
    """Serialize all shared-state mutation + the status S3 write under one lock."""
    global total_rows_all
    with _RESULTS_LOCK:
        if error is None:
            succeeded.append(result)
            total_rows_all += result['rows']
            status["tables"][label] = {"status": "done", "rows": result['rows'],
                                       "at": utc_now_iso()}
        else:
            failed.append({"table": label, "error": error})
            status["tables"][label] = {"status": "failed", "error": error,
                                       "at": utc_now_iso()}
        # save_status is inside the lock on purpose: it rewrites the whole JSON file,
        # so two threads writing at once would race and lose an update.
        save_status(s3_client, status)


_BENCH_T0 = time.time()
print(f"BENCH_START epoch={_BENCH_T0:.3f} workers={workers} "
      f"parallel={V6_PARALLEL_ENABLED} conc={MAX_WRITE_CONCURRENCY} "
      f"part={TARGET_ROWS_PER_PARTITION} fanout={V6_CHUNK_FANOUT_ENABLED} "
      f"gate={LARGE_TABLE_BYTES_THRESHOLD}")
# Glue/driver footprint for context. NOTE these are DIFFERENT dimensions:
#   driver_table_workers  = concurrent tables on the driver thread pool (the real
#                           parallelism of this driver-only loader)
#   per_table_writers     = inner range/chunk writers per table
#   global_write_cap      = hard cap on total DSQL writers across all tables (semaphore)
#   defaultParallelism/executors = Spark WORKER footprint (helps CSV read only; the LOAD
#                           runs on the driver, so more executors do NOT speed the writes)
try:
    _sc = spark.sparkContext
    print(f"[GLUE] driver_table_workers={workers} "
          f"per_table_writers={PER_TABLE_WRITE_CONCURRENCY} "
          f"global_write_cap={MAX_WRITE_CONCURRENCY} "
          f"defaultParallelism={_sc.defaultParallelism} "
          f"executors={_sc.getConf().get('spark.executor.instances', '?')}", flush=True)
except Exception as _e:
    print(f"[GLUE] footprint log skipped: {_e}", flush=True)

if workers == 1:
    # Sequential path — no thread-pool overhead when not parallelizing.
    for item in worklist:
        label, result, error, log = run_one_table(item)
        for line in log:
            print(line)
        record_outcome(label, result, error)
else:
    # Parallel path: submit all tables, cap in-flight at `workers`.
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tblload") as pool:
        future_to_label = {pool.submit(run_one_table, item): item[1] for item in worklist}
        for fut in as_completed(future_to_label):
            # run_one_table never raises, but guard anyway so the pool always drains.
            try:
                label, result, error, log = fut.result()
            except Exception as e:  # pragma: no cover — defensive
                label = future_to_label[fut]
                result, error, log = None, f"worker crashed: {e}", [
                    f"  ✗ FAILED {label}: worker crashed: {e}"]
            with _RESULTS_LOCK:
                for line in log:
                    print(line)
            record_outcome(label, result, error)

_BENCH_T1 = time.time()
_bench_secs = max(0.001, _BENCH_T1 - _BENCH_T0)
print(f"BENCH_END epoch={_BENCH_T1:.3f} load_secs={_bench_secs:.3f} "
      f"rows={total_rows_all} succeeded={len(succeeded)} failed={len(failed)} "
      f"rows_per_sec={total_rows_all/_bench_secs:.1f}")

# =============================================================================
# OVERALL SUMMARY
# =============================================================================
print(f"\n{'='*70}")
print("JOB 2 V15 COMPLETE — LOAD SUMMARY")
print(f"{'='*70}")
print(f"  Tables in index  : {len(tables)}")
print(f"  Skipped (done)   : {len(skipped_done)}")
print(f"  Attempted        : {len(succeeded) + len(failed)}")
print(f"  Succeeded        : {len(succeeded)}")
print(f"  Failed           : {len(failed)}")
print(f"  Total rows loaded: {total_rows_all:,}")
print("\n  Per-table (succeeded):")
for r in succeeded:
    print(f"    ✓ {r['table']:40s} {r['rows']:>12,} rows")
if failed:
    print("\n  Per-table (FAILED):")
    for fail_rec in failed:
        print(f"    ✗ {fail_rec['table']:40s} {fail_rec['error']}")
print(f"{'='*70}")

glue_reachable = False
try:
    sock = socket.create_connection((f"glue.{REGION}.amazonaws.com", 443), timeout=GLUE_API_TIMEOUT)
    sock.close()
    glue_reachable = True
except (socket.timeout, socket.error, OSError):
    pass

if glue_reachable:
    job.commit()
    print("  ✓ job.commit() succeeded")
else:
    print("  ⚠️ Skipping job.commit() — Glue API not reachable (data still loaded).")

if failed:
    raise Exception(
        f"{len(failed)} of {len(tables)} tables FAILED to load. "
        f"See per-table summary above. Successful tables are committed."
    )
