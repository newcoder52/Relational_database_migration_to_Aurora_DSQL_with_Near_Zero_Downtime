"""
Glue CDC Continuous Processor (v4 — MULTI-TABLE, DSQL-STATE, ZERO-LOSS)
=======================================================================
A long-running Python Shell Glue job that applies DMS CDC (change-data-capture)
CSV files from S3 to Aurora DSQL, for MANY tables in one job, with crash-proof
resume and a hard "no missed / no duplicated DML" guarantee.

WHAT CHANGED FROM v3 (v3 is left UNTOUCHED — this is a new file)
----------------------------------------------------------------
v3 was single-table, hardcoded to `refund_event`, applied rows one-by-one, committed
every 100 rows, and declared a file "success" if <10% of rows errored (silent data
loss). It also implemented UPDATE as DELETE-then-INSERT with a crash window that could
lose a row, and had no mid-file resume and no OCC/connection-expiry retry (the job runs
up to 48h but a DSQL connection dies at ~60min).

v4 fixes all of that:

 1. MULTI-TABLE. Tables are discovered from Job 1 v2's master index
    (CONFIG_PREFIX + "_manifest_index.json") and per-table config JSON (PK columns,
    pk_kind, type_categories, column_mapping). No hardcoded KNOWN_* column sets.

 2. PER-TABLE ISOLATION. Each table's CDC files are processed SERIALLY (per-PK order is
    mandatory for CDC), but tables are independent: one table hitting a genuinely bad row
    is marked `blocked` and SKIPPED, while every other table keeps flowing.

 3. STATE IN DSQL (control tables, inspired by the DMS control-table design
    awsdms_status / awsdms_apply_exceptions):
      - cdc_control.cdc_status          : per-table high-water file + in-progress file +
                                          row offset + watermark (max dms_timestamp) +
                                          status. Keyed by table -> CROSS-RUN RESUME
                                          survives; never dropped.
      - cdc_control.cdc_apply_exceptions: the exact failing statement + error for a
                                          blocked table (queryable audit).
    The checkpoint (file/offset/watermark) is written in the SAME TRANSACTION as the
    data chunk, so state can never disagree with data -> no missed row on a crash.

 4. HIGH-WATER FILE SKIP. cdc_status.last_done_file lets a resume skip every CSV file
    already fully applied (files sort by DMS timestamp filename) instead of re-scanning.

 5. ZERO-ERROR COMPLETION. A file is only marked done if EVERY row applied. Transient
    errors (OCC 40001/OC000/OC001, DSQL server-unavailable XX000, broken pipe, 60-min
    connection expiry) are RETRIED (borrowed verbatim from job2 v15). A genuinely bad row
    HALTS that table (records the exception, marks it blocked) — it is never skipped and
    the file is never moved to processed/.

 6. ATOMIC UPDATE. An UPDATE (op=U) applies DELETE + INSERT for the SAME pk inside ONE
    transaction/chunk commit, so a crash can never leave a row deleted-but-not-reinserted.

 7. IMMUTABLE-PK ASSUMPTION. v4 assumes a row's PK never changes (surrogate keys). On S3
    CDC there is no before-image, so a PK change cannot be detected here; it must be
    guaranteed at the source. Deletes are fully supported (the D record carries the PK).

PRESERVED FROM v3 (kept intentionally, adapted to be per-table)
---------------------------------------------------------------
  - The background DMS DDL watcher thread + schema-change handling (ADD / RENAME;
    non-destructive DROP = keep column, log). Now keyed per table.
  - hex(32) -> canonical uuid conversion (v3 hex_to_uuid), hardened with v15's anchored
    exactly-32 rule so "32-hex + trailing junk" is NOT silently truncated.
  - Header-based CSV column detection (DMS AddColumnName=true).
  - The 30s poll loop and the S3 processed/ + failed/ file moves (kept as a human-visible
    artifact; DSQL cdc_status is the authoritative resume position).

DMS S3 ENDPOINT SETTINGS REQUIRED
---------------------------------
  AddColumnName=true                 (CDC CSVs carry a header row)
  TimestampColumnName=dms_timestamp  (the watermark column)
  Rfc4180=true (default)             (quoted CSV so embedded commas/newlines don't split)

DEPLOY
------
  Job Type: Python Shell
  Python Version: 3.9
  Additional Modules: pg8000
  Timeout: up to 2880 min (48h)
"""
import sys

# =============================================================================
# BOTO3 PRIORITY SHIM  (MUST run before `import boto3` below)
# =============================================================================
# Glue Python Shell ships its OWN (older) boto3/botocore that may not know the `dsql`
# service. We deliver a modern boto3+botocore as S3 wheels via --extra-py-files (NO PyPI —
# firewall-safe). But --extra-py-files entries can land AFTER Glue's bundled site-packages
# on sys.path, so a plain `import boto3` could still pick up the old bundled one. This shim
# PROMOTES any sys.path entry that contains a boto3/botocore wheel to the FRONT, then drops
# any already-imported boto3/botocore modules, so the `import boto3` below binds to the
# modern wheel. No-op (harmless) if no such wheel was staged — the bundled boto3 is used.
def _prioritize_bundled_boto3_override():
    import os
    # Glue Python Shell pip-INSTALLS each --extra-py-files wheel into an "installation" dir
    # (e.g. /glue/lib/installation) rather than leaving the .whl on sys.path. The BUNDLED
    # (old) boto3 lives in an EARLIER sys.path entry (e.g. /glue/lib), so a plain
    # `import boto3` binds the bundled one. Fix: find the sys.path dir that actually contains
    # the freshly-installed modern boto3 (has boto3/ AND botocore/ subdirs) and PROMOTE it to
    # the front, ahead of any other entry that also has a boto3 — then drop cached modules so
    # the re-import binds to the promoted (modern) copy.
    def _has_boto3(d):
        try:
            return os.path.isdir(os.path.join(d, "boto3")) and \
                   os.path.isdir(os.path.join(d, "botocore"))
        except Exception:
            return False
    # Prefer an "installation"/glue-python-libs dir (where --extra-py-files lands); fall back
    # to any sys.path dir containing boto3+botocore that isn't the first such entry.
    candidates = [p for p in sys.path if p and _has_boto3(p)]
    if not candidates:
        return
    def _rank(p):
        b = p.lower()
        # highest priority: the extra-py-files install locations
        if "installation" in b or "glue-python-libs" in b:
            return 0
        return 1
    best = sorted(candidates, key=_rank)[0]
    # Only reorder if a non-preferred boto3 currently precedes 'best'.
    if _rank(best) == 0 or candidates[0] != best:
        while best in sys.path:
            sys.path.remove(best)
        sys.path.insert(0, best)
        for _m in [m for m in list(sys.modules)
                   if m == "boto3" or m == "botocore"
                   or m.startswith("boto3.") or m.startswith("botocore.")]:
            del sys.modules[_m]   # force fresh import from the promoted modern copy


_prioritize_bundled_boto3_override()

import json
import time
import ssl
import csv
import io
import re
import uuid
import threading
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from awsglue.utils import getResolvedOptions

import pg8000

# Glue Python Shell heavily buffers stdout, so poll-loop / apply progress can be invisible
# for many minutes, making a HEALTHY job look hung. Force line-buffering so every print is
# flushed as it happens (Py3.7+). One global switch instead of flush=True on every call site.
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# =============================================================================
# CONFIGURATION  (generic placeholders — the customer edits these for their env)
# =============================================================================
# S3 bucket that DMS writes CDC CSVs into, and where the Job 1 config/manifest live.
BUCKET = '<YOUR_S3_BUCKET>'

# Job 1 v2 output prefix (contains _manifest_index.json + per-table *_column_mapping.json
# + _load_status.json written by Job 2 / the full-load job).
# The master index lists every table + the S3 path to its per-table config.
CONFIG_PREFIX = 's3://<YOUR_S3_BUCKET>/<SCHEMA>/config/'
INDEX_S3_KEY = None   # optional explicit key override; else derived from CONFIG_PREFIX

# FULL-LOAD ELIGIBILITY GATE. CDC must NOT apply changes to a table whose full load isn't
# finished — applying a delta on top of a half-loaded table corrupts it. When True, v4
# reads _load_status.json (written by the full-load job) and processes a table ONLY if its
# status == "done". A table not-done / missing is SKIPPED this cycle with a clear
# "waiting for full load" log, and retried next poll (so CDC auto-starts a table the moment
# its full load completes). Set False only if you deliberately run CDC without the gate
# (e.g. full load handled entirely out-of-band and you accept the risk).
REQUIRE_FULL_LOAD_DONE = True
LOAD_STATUS_KEY = None   # optional explicit key; else CONFIG_PREFIX + "_load_status.json"

# Per-table CDC file layout. Each table's CDC CSVs live under:
#     s3://{BUCKET}/{CDC_ROOT}/{dms_schema}/{dms_table}/*.csv
# with per-table processed/ and failed/ subfolders. Adjust CDC_ROOT to match the DMS
# S3 target BucketFolder. If your layout differs, override derive_table_prefixes().
CDC_ROOT = 'cdc'

# Aurora DSQL target.
DSQL_ENDPOINT = '<YOUR_CLUSTER>.dsql.<REGION>.on.aws'
DSQL_DATABASE = 'postgres'
DSQL_USER = 'admin'
REGION = 'us-east-1'

# DMS replication task ARN (for the DDL watcher's describe_table_statistics).
DMS_TASK_ARN = 'arn:aws:dms:<REGION>:<ACCOUNT>:task:<TASK_ID>'

# DSQL schema that holds v4's control tables (created if not exists on startup).
CONTROL_SCHEMA = 'cdc_control'

# Timing
POLL_INTERVAL = 30              # seconds between S3 polls when idle
DDL_WATCH_INTERVAL = 10         # seconds between DMS DDL-count checks

# SINGLE-SWAP RENAME POLICY. When a CDC header shows exactly one column gone + one new
# ("single swap"), treat it as a RENAME COLUMN by default. This is the correct interpretation
# under DMS AddColumnName=true (every column is sent on every row, so a 1-for-1 header swap is
# a rename), and it AVOIDS the missing-column block that keeping the old column would cause.
# Set False to revert to conservative ADD-and-keep-old (a real rename then needs a config
# rename_hint to avoid blocking). Overridable via the --single_swap_is_rename job arg.
SINGLE_SWAP_IS_RENAME = True

# ---- WAKE: how the loop learns a new CDC file arrived --------------------------------
# The loop re-lists S3 every POLL_INTERVAL seconds and runs a list+sort+high-water apply
# cycle. Per-table serial DMS-timestamp order is owned by files.sort(); already-applied
# files are skipped via last_done_file / processed-move (idempotent no-op), and the
# MIN_FILE_AGE_SECONDS guard avoids reading a file DMS is still finalizing.

# CONTINUOUS OPERATION: CDC is an unbounded stream — DMS keeps writing new files
# indefinitely, and a table can be legitimately quiet for hours then resume. RUN_FOREVER
# keeps the fetch/apply loop polling until the job is stopped (Glue timeout, manual stop,
# or SIGTERM), which is what a steady-state CDC processor should do. Set RUN_FOREVER=False
# to fall back to the drain-and-exit behavior (stop after MAX_IDLE_HOURS with no work) —
# useful for a one-shot backfill drain, NOT for steady state.
RUN_FOREVER = True
MAX_IDLE_HOURS = 4              # only used when RUN_FOREVER=False

# IN-FLIGHT FILE GUARD: S3 ListObjectsV2 can return a CDC file's key while DMS is still
# writing it (or the instant it finalizes). Reading a partially-written CSV would apply
# fewer rows than the file will ultimately contain and then mark it done -> silent tail
# loss. Only process files whose LastModified is at least this many seconds in the past,
# so a file still being written is left for the next poll. DMS's write-then-rename usually
# avoids this, but the age margin makes it safe regardless.
MIN_FILE_AGE_SECONDS = 15

# (MAX_PARALLEL_TABLES is defined in the CUSTOMER-TUNABLE KNOBS block below.)

# Columns never written to the target (DMS control columns).
# The DMS operation column name.
OP_COLUMN = 'op'
# The DMS timestamp column name (TimestampColumnName). Used as the CDC watermark.
# Overridable from the S3 target endpoint (resolve_task -> --timestamp_column). When it is
# overridden, IGNORE_COLUMNS is rebuilt (see _rebuild_ignore_columns) so the ACTUAL timestamp
# column — whatever the endpoint calls it — is excluded from the insert column set. Otherwise
# a renamed timestamp column would leak into the target insert set (and the stale literal
# would be ignored instead).
DMS_TIMESTAMP_COLUMN = 'dms_timestamp'
# Control columns never written as table data. Derived from OP_COLUMN + DMS_TIMESTAMP_COLUMN
# (lower-cased) rather than hardcoded, so it tracks an endpoint-derived timestamp column name.
IGNORE_COLUMNS = {OP_COLUMN.lower(), DMS_TIMESTAMP_COLUMN.lower()}


def _rebuild_ignore_columns():
    """Recompute IGNORE_COLUMNS from the current OP_COLUMN + DMS_TIMESTAMP_COLUMN. Call after
    overriding DMS_TIMESTAMP_COLUMN so the (possibly renamed) timestamp column is still
    excluded from the data/insert column set."""
    global IGNORE_COLUMNS
    IGNORE_COLUMNS = {OP_COLUMN.lower(), DMS_TIMESTAMP_COLUMN.lower()}

# TIER-2 (keyless) TARGET-ONLY TAG COLUMN — Glue-managed, added to the DSQL target by
# apply_file_nonpk (never in the source / CDC header). It enables per-file idempotent reload:
#   _cdc_file : the CDC CSV file key that inserted this row. On (re)apply of a file we
#               DELETE WHERE _cdc_file=<file> then re-insert the file's rows -> re-applying a
#               file is idempotent (crash/replay safe) without a primary key.
# It is NOT a targeting key (target-only, not carried by the source) — it only tags which
# file inserted the row. It is excluded from the full-row CONTENT match used for DELETE.
# (A _dms_ts provenance column was considered and dropped: with updates SKIPPED there are no
# duplicates to resolve at read time, and ordering/watermark already live in cdc_status +
# cdc_file_status, so a per-row timestamp on the target added no functional value.)
NONPK_FILE_TAG_COLUMN = '_cdc_file'

# =============================================================================
# CUSTOMER-TUNABLE KNOBS  (safe to adjust per environment; defaults are conservative)
# =============================================================================
# CHUNK SIZE (net-ops per apply transaction). A "chunk" is a batch of collapsed net-ops
# (one per PK) applied in ONE transaction (DMS-style batch apply). Bigger = fewer commits
# = higher throughput / lower latency, but a bigger transaction is more likely to hit the
# DSQL 5-min (300s) txn-age limit or the per-txn row cap. v4 STARTS at CDC_CHUNK_SIZE and
# ADAPTIVELY HALVES toward CDC_MIN_CHUNK_SIZE whenever a chunk times out or runs slow, so
# the start value is an upper bound, not a fixed size — it self-tunes down under pressure.
#   - CDC_CHUNK_SIZE     : starting chunk size (upper bound). MUST be <= DSQL_MAX_ROWS_PER_TXN.
#   - CDC_MIN_CHUNK_SIZE : floor the adaptive shrink won't go below.
#
# ┌─ MASTER OVERRIDE ──────────────────────────────────────────────────────────────────┐
# │ CDC_FIXED_CHUNK_SIZE is the single knob a customer sets to TAKE FULL CONTROL of the  │
# │ chunk size. It overrides everything below.                                          │
# │   • 0 (default) -> AUTO mode: use the adaptive engine described above (start at      │
# │     CDC_CHUNK_SIZE, self-shrink toward CDC_MIN_CHUNK_SIZE on slow/timeout chunks)    │
# │     AND the automatic byte-budget guard for wide/outlier rows.                       │
# │   • any value > 0 -> FIXED mode: every chunk is EXACTLY this many net-ops. The       │
# │     adaptive row-cap shrink is DISABLED and honored as-is. It is still clamped to    │
# │     the hard DSQL per-txn ceiling (<= (DSQL_MAX_ROWS_PER_TXN-1)//2) so a fixed value │
# │     can never violate the 3,000-row limit, and the BYTE budget still applies as a    │
# │     safety net so a rare huge row can't blow the 10 MiB txn/message limit (that is a │
# │     hard DSQL failure, not a tuning choice — it only ever shrinks the RARE outlier   │
# │     chunk, never the fixed size for normal rows). Set this when you have measured    │
# │     your workload and want deterministic, non-adaptive batches.                      │
# └─────────────────────────────────────────────────────────────────────────────────────┘
CDC_FIXED_CHUNK_SIZE = 0              # 0 = auto/adaptive (default); >0 = force this exact chunk size
CDC_CHUNK_SIZE = 2500                 # AUTO-mode start (upper bound); <= DSQL_MAX_ROWS_PER_TXN
CDC_MIN_CHUNK_SIZE = 100              # AUTO-mode adaptive-shrink floor (won't go below this)
# AUTO-mode step-down: when a chunk times out / runs slow, the chunk budget is reduced by
# THIS MANY (a gentle fixed decrement — NOT halving), then retried, flooring at
# CDC_MIN_CHUNK_SIZE. e.g. from the top it steps ~1499 -> 999 -> 499 -> 100. Gentle steps
# avoid over-shrinking on a one-off slow chunk while still backing off under real pressure.
CDC_CHUNK_STEP_DOWN = 500

# ROW-MOD PACKING. DSQL's hard limit is 3,000 ROW-MODIFICATIONS per transaction — NOT
# net-ops. As of the UPSERT model (build_chunk_sql), EVERY net-op is now a SINGLE row
# modification: a non-delete op is one `INSERT ... ON CONFLICT DO UPDATE` (1 mod) and a
# delete is one DELETE (1 mod). This DOUBLES the old all-insert ceiling — an insert used to
# cost 2 (delete-then-insert), capping ~1499 net-ops/txn; at 1 mod each we now pack up to
# ~2999 net-ops/txn (3000 budget minus the 1 reserved checkpoint row). These constants are
# the per-op costs; keep them matching build_chunk_sql.
CDC_MODS_PER_DELETE = 1               # a DELETE net-op = 1 row modification
CDC_MODS_PER_INSERT = 1              # an INSERT/UPDATE net-op = 1 upsert (ON CONFLICT) = 1 mod

# ACROSS-TABLE CONCURRENCY. CDC is SERIAL WITHIN a table (per-PK order is mandatory);
# tables are independent. 1 = process tables sequentially each cycle (each still gets its
# own dedicated DSQL session). Raise to run N tables' sessions concurrently (more
# throughput, more connections). Keep at 1 for the first run; tune up once validated.
# (Defined here as the tunable; the working value is set in the GLOBALS section below.)
MAX_PARALLEL_TABLES = 1

# ---- TIER-2 CDC VALIDATION (deferred, sampled by-PK net-state check) ----------------
# OFF by default — it adds target reads AFTER a file commits, so it costs some time/IO.
# When on, after a file's apply fully commits, v4 samples up to VALIDATION_SAMPLE_PER_FILE
# of that file's net-ops and re-reads each row by PK from the target, comparing to the
# expected net-op image (INSERT -> row present + values match; DELETE -> row absent). A
# mismatch is RE-CHECKED after VALIDATION_RETRY_DELAY_SECONDS (absorbs any commit lag);
# only a persistent mismatch is recorded in cdc_control.cdc_validation_failures. This is a
# DISCREPANCY REPORT — it does NOT block the apply (the apply already succeeded and is
# authoritative). It runs on the table's own connection, AFTER the commit, never inside a
# chunk transaction, so it never touches the apply hot path.
VALIDATION_ENABLED = False               # opt-in; keeps apply latency flat by default
VALIDATION_SAMPLE_PER_FILE = 20          # net-ops sampled per file (0 = all — expensive)
VALIDATION_RETRY_DELAY_SECONDS = 5       # re-check a mismatch after this, before recording
VALIDATION_MAX_FAILURES_PER_TABLE = 100  # circuit breaker: stop validating a table past this

# =============================================================================
# DSQL TRANSACTION LIMITS + RETRY TUNING  (borrowed verbatim from job2 v15)
# =============================================================================
DSQL_MAX_ROWS_PER_TXN = 3000          # DSQL per-transaction row cap (hard DSQL limit)
# Slow-chunk shrink TRIGGER, under the DSQL 5-min (300s) txn-age hard limit. This is NOT a
# cutoff — the txn already committed; exceeding it just shrinks the NEXT chunk. So we run it
# aggressively close to 300 (20s buffer = 280) to keep chunks big / commits few / CDC latency
# low. A txn-age FAILURE is retriable (the chunk re-slices smaller), so the rare overshoot is
# self-healing. SELF-CORRECTING SAFETY: if txn-age failures pile up (> DSQL_BATCH_TXN_AGE_
# FALLBACK_THRESHOLD in a run), 270 is proving too aggressive for this workload, so the
# effective trigger drops to the conservative DSQL_BATCH_MAX_SECONDS_SAFE (240) for the rest
# of the run. Best of both: aggressive by default, backs off only if the data actually needs it.
DSQL_BATCH_MAX_SECONDS = 280              # aggressive trigger (20s buffer under 300s; post-commit signal, never aborts a running chunk)
DSQL_BATCH_MAX_SECONDS_SAFE = 240         # conservative fallback after repeated txn-age failures
DSQL_BATCH_TXN_AGE_FALLBACK_THRESHOLD = 10  # >this many txn-age failures in a run -> use SAFE
# Recycle check runs BETWEEN chunks (ensure_fresh_conn at top of each), never mid-txn, so a
# connection that passes can still run ONE more chunk (up to the 300s/5-min txn-age limit)
# before the next check. Safe threshold must leave a full worst-case chunk + slop under the
# ~60-min hard limit: 50 + 5 = 55 min (~5 min slop). NOT ~57 (57 + 5 = 62 > 60 -> DSQL
# force-closes mid-chunk). 50 is the conservative setting: a comfortable 5-min slop under the
# 60-min limit to avoid connection-close errors even if a final chunk runs long.
CONN_RECYCLE_SECONDS = 50 * 60        # 50 min, under DSQL's ~60-min connection duration

# BYTE BUDGET PER CHUNK — DSQL rejects a write txn whose modified data exceeds 10 MiB
# ("transaction size limit 10mb exceeded", 54000) and the wire protocol FATALs on a single
# message over 10 MiB ("invalid message length", 08P01, which DROPS the connection). v4
# INLINES values into the SQL text, so a chunk of wide rows (e.g. text columns up to the
# DSQL 1 MiB/column max) can blow past both limits long before the 3,000-row cap.
#
# SIZING PHILOSOPHY (matches job2 v16): run CLOSE to the hard limit with a TIGHT buffer and
# a MEASURED backstop — do NOT stack speculative padding. Previously this used an 8 MiB
# budget AND a x2 char-count safety multiplier => effectively chunking against only ~4 MiB,
# a double margin that needlessly shrank CDC chunks (=> more commits, higher apply latency,
# more CDC backlog). We replace that with:
#   1. CHUNK_BYTE_BUDGET = 10 MiB - a small fixed 2% reserve (~9.8 MiB) for the DELETE/INSERT
#      keywords, quoting, ::type casts, and the cdc_status checkpoint row — a concrete byte
#      figure, not raw x a_guessed_factor. This is the packing TARGET (throughput hint).
#   2. CDC_CHUNK_BYTE_SAFETY = 1: the cheap char-count estimate is used as-is (fast, no
#      per-op .encode() on the latency-sensitive CDC hot path). It is only an ESTIMATE now,
#      not a guarantee.
#   3. A MEASURED backstop at execute time (below): after build_chunk_sql produces the real
#      statements, we sum their actual UTF-8 byte length and, if over DSQL_MAX_TXN_BYTES,
#      halve the chunk and re-pack. THAT is the correctness guarantee against the 10 MiB
#      limit — so the estimate can be honest (x1) without risking a multi-byte outlier
#      slipping over. A single row that alone exceeds the limit surfaces as a clear error.
DSQL_MAX_TXN_BYTES = 10 * 1024 * 1024     # hard DSQL limit (txn data + wire message)
# Tight 2% reserve under the hard limit (~205 KiB), same as job2 v16. Measured backstop is
# the real guarantee, so we pack right up to ~9.8 MiB.
CDC_CHUNK_BYTE_RESERVE = 205 * 1024
CDC_CHUNK_BYTE_BUDGET = DSQL_MAX_TXN_BYTES - CDC_CHUNK_BYTE_RESERVE   # ~9.8 MiB packing target
# Char-count estimate multiplier. 1 = use the cheap len()-based estimate as-is (the measured
# backstop below enforces the real byte limit, so no speculative padding is needed here).
CDC_CHUNK_BYTE_SAFETY = 1

# MINIMAL-HEADROOM target fractions for the MEASURED proportional shrinks (maximize perf).
# Same rationale as job2 v16: BYTE 0.99 (near-exact calc, overshoot = one cheap re-slice);
# TIME 0.95 / _AFTER_FAIL 0.90 applied ON TOP of the batch trigger (already 30s under the
# 300s hard limit), a thin second margin for commit-time noise; time overshoot risks a
# rejected 300s txn so it keeps slightly more headroom than bytes.
BYTE_TARGET_FRACTION = 0.99
TIME_TARGET_FRACTION = 0.95
TIME_TARGET_FRACTION_AFTER_FAIL = 0.90

OCC_MAX_RETRIES = 5
OCC_BASE_BACKOFF_SECONDS = 0.05
OCC_MAX_BACKOFF_SECONDS = 5.0
OCC_SQLSTATES = {"40001", "OC000", "OC001"}

SERVER_MAX_RETRIES = 6
SERVER_BASE_BACKOFF_SECONDS = 0.5
SERVER_MAX_BACKOFF_SECONDS = 30.0
SERVER_TRANSIENT_FRAGMENTS = (
    "server unavailable", "server is not available", "server not available",
    "temporarily unavailable", "try again", "too many connections",
    "connection refused", "service unavailable", "not available",
)

MAX_CHUNK_RETRIES = 3
CHUNK_RETRY_BACKOFF_SECONDS = 2

# Initial per-table DSQL connect attempts (reactive self-heal). Each failed attempt on a
# connection-class/transient error invalidates the cached IAM token and backs off (reusing
# the server backoff curve) so the next attempt mints a fresh token — heals a token/endpoint
# blip within the same poll instead of stalling. Persistent outage still isolates the table.
CONNECT_MAX_RETRIES = 4

# Socket connect timeout (seconds) for EVERY pg8000.connect. Without this, a refused or
# half-open DSQL socket blocks the connect INDEFINITELY — the exact silent startup hang seen
# in the field (process stuck inside connect(), so the retry/token-invalidation path never
# even runs). With a timeout the blocked connect raises fast and the bounded retry heals it.

# =============================================================================
# OPTIONAL GLUE ARG OVERLAY  (orchestrator wiring — Python Shell; mirrors v16's pattern)
# =============================================================================
# The orchestrator injects env-specific endpoints + points each split-group's CDC run at
# that group's OWN config prefix (its own _manifest_index.json + _load_status.json), so the
# customer never hand-edits this file. Every arg is OPTIONAL: getResolvedOptions raises on a
# requested-but-absent arg, so we only request the ones actually present in sys.argv, and
# each hardcoded constant above stays the FALLBACK default. This MUST run BEFORE the GLOBALS
# section below, because the boto clients read REGION at creation. CONFIG_PREFIX override
# also re-derives nothing at import (load_
# manifest / load_full_load_status derive their keys from CONFIG_PREFIX at call time), so
# overriding the CONFIG_PREFIX global is sufficient.
def _apply_cdc_arg_overrides():
    global CONFIG_PREFIX, INDEX_S3_KEY, LOAD_STATUS_KEY, BUCKET, CDC_ROOT
    global DSQL_ENDPOINT, DSQL_DATABASE, DSQL_USER, REGION
    global DMS_TASK_ARN, CONTROL_SCHEMA
    global MAX_PARALLEL_TABLES, REQUIRE_FULL_LOAD_DONE, POLL_INTERVAL
    global DMS_TIMESTAMP_COLUMN, SINGLE_SWAP_IS_RENAME
    optional = ["config_prefix", "index_s3_key", "load_status_key", "s3_bucket", "cdc_root",
                "dsql_endpoint", "dsql_database", "dsql_user", "region",
                "dms_task_arn", "control_schema",
                "max_parallel_tables", "require_full_load_done", "poll_interval",
                "timestamp_column", "single_swap_is_rename"]
    present = [a for a in optional if f"--{a}" in sys.argv]
    if not present:
        return
    ov = getResolvedOptions(sys.argv, present)

    def _s(key):
        return str(ov[key]).strip() if key in ov and str(ov[key]).strip() else None

    _cp = _s("config_prefix")
    if _cp:
        if not _cp.endswith("/"):
            _cp += "/"
        CONFIG_PREFIX = _cp
        print(f"  ↪ CONFIG_PREFIX overridden -> {CONFIG_PREFIX}")
    if _s("index_s3_key"):
        INDEX_S3_KEY = _s("index_s3_key")
    if _s("load_status_key"):
        LOAD_STATUS_KEY = _s("load_status_key")
    if _s("s3_bucket"):
        BUCKET = _s("s3_bucket")
    if _s("cdc_root"):
        CDC_ROOT = _s("cdc_root").strip("/")
    if _s("timestamp_column"):
        # The DMS TimestampColumnName (CDC watermark column), derived from the S3 target
        # endpoint by resolve_task. Defaults to 'dms_timestamp' when the arg is absent.
        DMS_TIMESTAMP_COLUMN = _s("timestamp_column")
        _rebuild_ignore_columns()
        print(f"  ↪ DMS_TIMESTAMP_COLUMN overridden -> {DMS_TIMESTAMP_COLUMN} "
              f"(IGNORE_COLUMNS={sorted(IGNORE_COLUMNS)})")
    if _s("single_swap_is_rename"):
        SINGLE_SWAP_IS_RENAME = _s("single_swap_is_rename").strip().lower() in ("true", "1", "yes")
        print(f"  ↪ SINGLE_SWAP_IS_RENAME overridden -> {SINGLE_SWAP_IS_RENAME}")
    if _s("dsql_endpoint"):
        DSQL_ENDPOINT = _s("dsql_endpoint")
    if _s("dsql_database"):
        DSQL_DATABASE = _s("dsql_database")
    if _s("dsql_user"):
        DSQL_USER = _s("dsql_user")
    if _s("region"):
        REGION = _s("region")
    if _s("dms_task_arn"):
        DMS_TASK_ARN = _s("dms_task_arn")
    if _s("control_schema"):
        CONTROL_SCHEMA = _s("control_schema")
    if "max_parallel_tables" in ov:
        try:
            MAX_PARALLEL_TABLES = max(1, int(ov["max_parallel_tables"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid max_parallel_tables={ov['max_parallel_tables']!r}")
    if "poll_interval" in ov:
        try:
            POLL_INTERVAL = max(1, int(ov["poll_interval"]))
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid poll_interval={ov['poll_interval']!r}")
    if "require_full_load_done" in ov:
        REQUIRE_FULL_LOAD_DONE = str(ov["require_full_load_done"]).strip().lower() in ("true", "1", "yes")


_apply_cdc_arg_overrides()

# =============================================================================
# GLOBALS
# =============================================================================
s3 = boto3.client('s3', region_name=REGION)
dms = boto3.client('dms', region_name=REGION)
try:
    cloudwatch = boto3.client('cloudwatch', region_name=REGION)
except Exception:
    cloudwatch = None
_client_lock = threading.Lock()

# Run-wide txn-age failure tracking for the self-correcting batch-time trigger. The apply
# loop uses the AGGRESSIVE DSQL_BATCH_MAX_SECONDS (270s) by default; if genuine txn-age
# (300s) FAILURES accumulate past the threshold across the run, 270 is proving too close for
# this workload's chunk sizes, so effective_batch_max_seconds() permanently returns the SAFE
# 240s. Thread-safe because MAX_PARALLEL_TABLES can apply multiple tables concurrently.
_txn_age_failures = 0
_txn_age_lock = threading.Lock()

def _record_txn_age_failure():
    """Count one real DSQL txn-age (300s) failure; used to back the batch trigger off to SAFE."""
    global _txn_age_failures
    with _txn_age_lock:
        _txn_age_failures += 1

def effective_batch_max_seconds():
    """The slow-chunk shrink trigger: aggressive 270s until repeated txn-age failures prove
    this workload needs the conservative 240s, then permanently 240s for the rest of the run."""
    with _txn_age_lock:
        over = _txn_age_failures > DSQL_BATCH_TXN_AGE_FALLBACK_THRESHOLD
    return DSQL_BATCH_MAX_SECONDS_SAFE if over else DSQL_BATCH_MAX_SECONDS


def make_boto_client(service):
    """boto3 client creation is not thread-safe; serialize it."""
    with _client_lock:
        return boto3.client(service, region_name=REGION)


# Per-table DDL-watcher state, keyed by dms_table name (uppercased as DMS reports it).
# Each entry: {"count": int, "event": bool, "time": datetime|None}
_ddl_state = {}
_ddl_lock = threading.Lock()
_stop_event = threading.Event()

# Per-table cache of (last CDC header tuple, resolved DSQL schema dict). Lets
# handle_schema_changes SKIP the per-file information_schema catalog query when a file's
# header is identical to the last file's (the overwhelmingly common no-change case). The
# catalog is only re-read on the first file or when the header actually differs. Keyed by
# table label. Safe: a table is processed by a single serial worker, and a DDL that
# changes the target always changes the header too (so a change is never missed).
_schema_cache = {}


# =============================================================================
# UUID / VALUE CONVERSION  (v3 hex_to_uuid hardened with v15's anchored rule)
# =============================================================================
UUID_CANONICAL_RE = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
UUID_RAW_HEX_RE = r'^[0-9a-fA-F]{32}$'
_UUID_CANONICAL = re.compile(UUID_CANONICAL_RE)
_UUID_RAW_HEX = re.compile(UUID_RAW_HEX_RE)

NULL_SENTINELS = {"NULL", "N/A", "NA", "NONE", "(NULL)", "\\N"}
_SENTINEL_UPPER = {s.upper() for s in NULL_SENTINELS}

# Boolean serializations across engines (v15 mapping).
_BOOL_TRUE = {"true", "t", "y", "yes", "1"}
_BOOL_FALSE = {"false", "f", "n", "no", "0"}

# type_category -> ::cast suffix for inline literals (mirrors v15 cast_map, stripped '%s').
CAST_SUFFIX = {
    'uuid': '::uuid', 'boolean': '::boolean', 'timestamptz': '::timestamptz',
    'bigint': '::numeric::bigint', 'integer': '::numeric::integer',
    'smallint': '::numeric::smallint', 'numeric': '::numeric',
    'float': '::double precision', 'date': '::date',
    'json': '::jsonb', 'bytea': '::bytea',
}


def is_valid_uuid_value(v):
    """True if v is a valid uuid shape (canonical 8-4-4-4-12 OR raw 32-hex). None ok."""
    if v is None:
        return True
    s = v if isinstance(v, str) else str(v)
    return bool(_UUID_CANONICAL.match(s) or _UUID_RAW_HEX.match(s))


def hex_to_canonical_uuid(v):
    """32-hex (any case, no dashes) -> canonical lowercase 8-4-4-4-12. ANCHORED: only an
    EXACTLY-32-hex value is reshaped; anything else (already-canonical any case, junk, or
    32-hex+trailing bytes) is returned UNCHANGED so the uuid guard sees the real value and
    rejects it — never silently truncated into a fake uuid (v15's anti-truncation rule)."""
    if v is None:
        return None
    s = str(v)
    if _UUID_RAW_HEX.match(s):
        h = s.lower()
        return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if _UUID_CANONICAL.match(s):
        return s.lower()
    return s   # pass through unchanged -> guard will reject if it's a uuid column


def _coerce_null(v):
    """Map DMS/CSV null sentinels and empty/whitespace-only string to None.
    IMPORTANT: the trim is used ONLY to DETECT a null/sentinel — the RETURNED value keeps the
    ORIGINAL, untrimmed string so that significant leading/trailing whitespace in VARCHAR data
    is preserved (previously this returned the stripped value, silently trimming padded text).
    Typed categories (numeric/int/uuid/timestamp/boolean) re-parse in convert_value and are
    unaffected by surrounding spaces (the DSQL ::cast and the timestamp/uuid normalizers
    tolerate them)."""
    if v is None:
        return None
    s = v.strip() if isinstance(v, str) else str(v)
    if s == "" or s.upper() in _SENTINEL_UPPER:
        return None
    # not a null/sentinel -> return the ORIGINAL value (whitespace intact) for str inputs;
    # for non-str, return the str() form (no meaningful surrounding whitespace to preserve).
    return v if isinstance(v, str) else s


# Oracle/DMS timestamp input formats, translated from v15's Java (Spark) patterns to
# Python strptime. Ordered most-specific-first; the first that parses wins. Kept in lock-
# step with v15.TIMESTAMP_INPUT_FORMATS so the full load and CDC normalize a given source
# string to the SAME stored value (no target drift between the two jobs).
_TS_INPUT_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%d-%b-%y %I.%M.%S.%f %p",   # Oracle default TIMESTAMP (e.g. 21-SEP-22 03.19.23.163000 PM)
    "%d-%b-%y %I.%M.%S %p",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%y",                   # Oracle default DATE
    "%d-%b-%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
]

# Pre-clean regexes mirroring v15.pre_clean_timestamp: (1) truncate >6 fractional-second
# digits to exactly 6 (Postgres/DSQL ::timestamptz rejects 7-9 digits that Oracle emits);
# (2) strip a trailing numeric TZ offset like " +00:00"/"-0530"; (3) strip a trailing
# " Z"/" UTC". All timestamps are already UTC (DMS/DSQL system TZ is UTC), so dropping the
# zero offset does not shift the value.
_TS_FRAC_TRUNCATE = re.compile(r'(\.\d{6})\d+')
# Strip a trailing TZ offset ONLY when it follows a TIME (…HH:MM:SS[.fff]), optionally
# space-separated. Anchoring to the time is essential: a naive '[+-]\d\d$' would match the
# '-21' in a bare date 'YYYY-MM-DD' and corrupt it (caught in test). Group 1 keeps the
# time; the offset (space + [+-]HH[:?MM] | 'Z' | 'UTC') is dropped.
_TS_STRIP_OFFSET = re.compile(
    r'(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s*(?:[+-]\d{2}(?::?\d{2})?|Z|UTC)\s*$',
    re.IGNORECASE)

# Like _TS_STRIP_OFFSET but CAPTURES the full datetime part (group 1) AND the offset
# (group 2) so _normalize_timestamp_str can CONVERT to UTC instead of dropping the offset.
# Group 1 = everything up to and including HH:MM:SS[.fff]; group 2 = the offset token.
_TS_PARSE_OFFSET = re.compile(
    r'^(.*\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s*([+-]\d{2}(?::?\d{2})?|Z|UTC)\s*$',
    re.IGNORECASE)


def _normalize_timestamp_str(v):
    """Pre-clean + (best-effort) reformat an Oracle/DMS timestamp/date string to a form
    DSQL's ::timestamptz / ::date accepts. Returns a normalized string.

    OFFSET HANDLING (fixed 2026-09-23): a value with an explicit NON-ZERO timezone offset
    (e.g. '2026-06-15 12:30:45.123456000 +05:30') is CONVERTED to the equivalent UTC instant
    and emitted with an explicit '+00:00' (e.g. '2026-06-15 07:00:45.123456+00:00'). Previously
    the offset was STRIPPED and the wall-clock kept, which silently shifted every tz-aware
    value by its offset (a wrong instant). A '+00:00'/'Z'/'UTC' offset is already UTC, so it is
    simply normalized (no shift). Offset-less values are reformatted as before.

    Stages:
      1) truncate fractional seconds to 6 digits (DSQL rejects 7-9).
      2) if a trailing numeric offset is present -> parse (date time + offset), convert to UTC,
         emit 'YYYY-MM-DD HH:MM:SS[.ffffff]+00:00'.
      3) else -> try the Oracle/DMS input formats; emit ISO 'YYYY-MM-DD HH:MM:SS[.ffffff]'.
    On no match we return the pre-cleaned string so a good value still gets its ::cast shot and
    a genuinely malformed one fails LOUD (zero-silent-loss)."""
    from datetime import timedelta
    s = v.strip()
    s = _TS_FRAC_TRUNCATE.sub(r'\1', s)
    # Detect a trailing numeric offset (±HH[:?MM]) following a real time. Keep the datetime
    # part (group 1) and the offset (group 2) so we can CONVERT rather than drop.
    m = _TS_PARSE_OFFSET.search(s)
    if m:
        dt_part = m.group(1).strip()
        off = m.group(2)  # e.g. '+05:30', '-0800', '+00:00', 'Z', 'UTC'
        # parse offset -> minutes
        off_min = 0
        ou = off.upper()
        if ou not in ('Z', 'UTC'):
            sign = 1 if off[0] == '+' else -1
            digits = re.sub(r'[^\d]', '', off)   # HHMM or HH
            oh = int(digits[:2]) if len(digits) >= 2 else 0
            om = int(digits[2:4]) if len(digits) >= 4 else 0
            off_min = sign * (oh * 60 + om)
        # parse the datetime part with the ISO-ish formats
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(dt_part, fmt)
            except ValueError:
                continue
            # local = UTC + offset  ->  UTC = local - offset
            dt_utc = dt - timedelta(minutes=off_min)
            if dt_utc.microsecond:
                return dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f") + "+00:00"
            return dt_utc.strftime("%Y-%m-%d %H:%M:%S") + "+00:00"
        # offset present but datetime part unparseable -> fall through to plain strip
    # No offset (or unparseable-with-offset): reformat offset-less forms as before.
    s2 = _TS_STRIP_OFFSET.sub(r'\1', s).strip()
    for fmt in _TS_INPUT_FORMATS:
        try:
            dt = datetime.strptime(s2, fmt)
        except ValueError:
            continue
        if dt.microsecond:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return s2


def convert_value(raw, category):
    """Normalize one raw CSV string to the canonical STRING form for its type category,
    ready to be wrapped by a ::type cast. Returns None for null/sentinel. Mirrors v15's
    Spark normalization, reimplemented in plain Python (no Spark in a Python Shell job).

    Does NOT hard-fail here on a bad uuid — the per-row guard (guard_row) does that so the
    error is attributed clearly and halts the table (zero-error policy)."""
    v = _coerce_null(raw)
    if v is None:
        return None
    # Typed categories re-parse/cast and must not be affected by surrounding whitespace, so
    # they operate on a stripped copy. VARCHAR/text (the final pass-through) keeps the ORIGINAL
    # value so significant leading/trailing spaces are preserved.
    vs = v.strip() if isinstance(v, str) else v
    if category == 'uuid':
        return hex_to_canonical_uuid(vs)
    if category == 'boolean':
        low = vs.lower()
        # numeric 1/0 or 1.0/0.0 first, then textual forms.
        try:
            f = float(low)
            if f == 1:
                return "true"
            if f == 0:
                return "false"
        except (ValueError, TypeError):
            pass
        if low in _BOOL_TRUE:
            return "true"
        if low in _BOOL_FALSE:
            return "false"
        return None   # unrecognized -> NULL (::boolean of NULL is NULL)
    if category in ('timestamptz', 'date'):
        # Oracle/DMS timestamps carry 7-9 fractional digits and/or a trailing offset that
        # DSQL's ::timestamptz rejects; Oracle-native forms aren't ISO at all. Normalize
        # exactly as v15's full load does, so the SAME source value stores identically in
        # both jobs (no full-load-vs-CDC drift) and the cast never chokes on a good value.
        return _normalize_timestamp_str(vs)
    if category in ('integer', 'bigint', 'smallint', 'numeric', 'float', 'double', 'real',
                    'json', 'jsonb', 'bytea'):
        # numeric/structured casts: strip surrounding whitespace so the ::cast never chokes.
        return vs
    # varchar/text/char: pass the ORIGINAL value through (preserve leading/trailing spaces);
    # the ::type cast on the DSQL side does any conversion.
    return v


def sql_literal(v, cast_suffix):
    """Injection-safe inline SQL literal (v15 rule): None -> NULL; else single-quote the
    string with every quote DOUBLED, then append the ::type cast. Safe as parameter
    binding for standard-conforming string literals (which DSQL uses)."""
    if v is None:
        return "NULL"
    s = v if isinstance(v, str) else str(v)
    if "\x00" in s:
        # A NUL byte cannot appear in a Postgres text literal (breaks the wire protocol).
        raise ValueError("value contains a NUL byte (0x00); cannot build SQL literal")
    return "'" + s.replace("'", "''") + "'" + cast_suffix


# =============================================================================
# RETRY CLASSIFIERS  (borrowed verbatim from job2 v15)
# =============================================================================
def is_occ_conflict(exc):
    """DSQL concurrency-conflict abort (retriable): 40001 / OC000 / OC001."""
    try:
        payload = exc.args[0]
        if isinstance(payload, dict) and payload.get("C") in OCC_SQLSTATES:
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return ("40001" in msg or "oc000" in msg or "oc001" in msg
            or "serialization" in msg or "occ" in msg or "concurrency" in msg
            or "conflicts with another transaction" in msg
            or "schema has been updated by another transaction" in msg)


def is_schema_conflict(exc):
    """A DSQL OCC abort caused specifically by a CONCURRENT SCHEMA CHANGE (DDL committed on
    another connection while this data txn was open). This is a SUBSET of is_occ_conflict,
    but it must be handled differently: a plain OCC retry re-runs the SAME frozen SQL with
    the SAME (now-stale) column list and can never succeed. When we see this we must
    RE-RECONCILE the schema and rebuild the column set before retrying — not just sleep and
    retry. Matches the DSQL 'schema has been updated by another transaction' message and
    the OC001 schema-conflict SQLSTATE if the driver surfaces it structurally."""
    try:
        payload = exc.args[0]
        if isinstance(payload, dict) and payload.get("C") == "OC001":
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return ("schema has been updated by another transaction" in msg
            or ("schema" in msg and "another transaction" in msg))


def is_unique_violation(exc):
    """UNIQUE constraint violation (SQLSTATE 23505)."""
    try:
        payload = exc.args[0]
        if isinstance(payload, dict) and payload.get("C") == "23505":
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return ("23505" in msg or "unique_violation" in msg
            or "duplicate key value violates unique constraint" in msg
            or "violates unique constraint" in msg)


def is_txn_timeout(exc):
    """DSQL 5-minute (300s) transaction-age limit."""
    msg = str(exc).lower()
    if "transaction age limit" in msg or "300s" in msg:
        return True
    return "54000" in msg and ("age" in msg or "duration" in msg or "timeout" in msg)


def is_broken_pipe_error(exc):
    """Dropped/broken DSQL connection heuristic (also covers connection-failure SQLSTATEs
    so a reconnect is attempted with a FRESH token — see _invalidate_dsql_token)."""
    import errno
    if isinstance(exc, (BrokenPipeError, ConnectionError)):
        return True
    if type(exc).__name__ in ("InterfaceError", "OperationalError"):
        return True
    # SQLSTATE class 08 = "connection exception" (08006 unable-to-connect, 08003 no-active-
    # connection, 08001/08004 unable-to-establish/rejected, 08007, 08P01). pg8000 surfaces
    # the SQLSTATE as args[0]["C"]. This is the exact code (08006) that silently stalled the
    # apply: treat any class-08 as a dropped connection so the retry loops reconnect.
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
        "server closed the connection", "eof detected", "connection already closed",
        "network is unreachable", "ssl connection has been closed", "ssl syscall",
        "could not receive data", "could not send data", "socket is closed", "socket closed",
        "unable to connect",
    )
    if any(f in msg for f in fragments):
        return True
    return isinstance(exc, OSError) and getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET)


def is_transient_server_error(exc):
    """Transient DSQL server-unavailable (XX000 + a transient message fragment)."""
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
    return has_fragment and ("server unavailable" in msg or "service unavailable" in msg
                             or "temporarily unavailable" in msg or "too many connections" in msg)


def occ_backoff_seconds(attempt):
    import random
    capped = min(OCC_MAX_BACKOFF_SECONDS, OCC_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    return random.uniform(0, capped)


def server_backoff_seconds(attempt):
    import random
    capped = min(SERVER_MAX_BACKOFF_SECONDS, SERVER_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    return random.uniform(0, capped)


# =============================================================================
# DSQL CONNECTION  (v15 connect_dsql pattern)
# =============================================================================
# DSQL auth-token cache: reuse one bearer token across connections/recycles (see connect_dsql).
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
    """Cached DSQL IAM auth token; regenerate only when older than DSQL_TOKEN_REFRESH_SECONDS.
    Bearer credential (not connection-bound), so all connections in the window share it."""
    global _dsql_token, _dsql_token_born
    now = time.monotonic()
    with _dsql_token_lock:
        if _dsql_token is None or (now - _dsql_token_born) > DSQL_TOKEN_REFRESH_SECONDS:
            client = make_boto_client("dsql")
            _dsql_token = client.generate_db_connect_admin_auth_token(
                DSQL_ENDPOINT, Region=REGION, ExpiresIn=DSQL_TOKEN_EXPIRES_IN)
            _dsql_token_born = now
        return _dsql_token


def _invalidate_dsql_token():
    """Force the NEXT connect to mint a FRESH IAM auth token. Called whenever a connection
    fails/drops (SQLSTATE class 08, closed pipe, TLS drop). Without this, every reconnect
    reused the same cached token — so if the token/endpoint state was the problem, ALL
    reconnects failed identically (the 08006 stall that silently froze the CDC apply). By
    clearing the cache here, the reconnect paths (data-chunk loop, run_control_op, and the
    per-table initial connect) each regenerate a token and can actually self-heal."""
    global _dsql_token, _dsql_token_born
    with _dsql_token_lock:
        _dsql_token = None
        _dsql_token_born = 0.0


def connect_dsql(autocommit=False):
    # The IAM auth token is a BEARER credential valid for its whole ExpiresIn window (not
    # tied to one connection), and generation is a LOCAL SigV4 sign. CACHE one token and
    # reuse it across connections/recycles until it nears refresh age. ExpiresIn (2 h) is set
    # well above a connection's max life (~54-min recycle + ~5-min final chunk = ~59 min) so
    # a cached token never expires mid-connection; refresh every ~30 min keeps it young.
    conn = pg8000.connect(
        host=DSQL_ENDPOINT, port=5432, database=DSQL_DATABASE,
        user=DSQL_USER, password=_get_cached_dsql_token(), ssl_context=_get_ssl_context())
    conn.autocommit = autocommit
    return conn


def connect_dsql_with_retry(autocommit=False, what="connect"):
    """connect_dsql wrapped in the SAME bounded fresh-token retry used by process_table's
    initial connect. Use this for STARTUP connects (ensure_control_tables, etc.) so a
    refused/slow/blocked DSQL connect fails fast (via the socket timeout) and retries with a
    fresh token instead of hanging or failing the whole job on one blip. Raises the last
    error only after exhausting CONNECT_MAX_RETRIES."""
    _last = None
    for _attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            return connect_dsql(autocommit=autocommit)
        except Exception as e:
            _last = e
            if is_broken_pipe_error(e) or is_transient_server_error(e):
                _invalidate_dsql_token()
            if _attempt < CONNECT_MAX_RETRIES:
                _bo = server_backoff_seconds(_attempt)
                print(f"    ↻ {what}: DSQL connect attempt {_attempt}/{CONNECT_MAX_RETRIES} "
                      f"failed ({e}); fresh-token retry in {_bo:.1f}s", flush=True)
                time.sleep(_bo)
    raise _last


def ensure_fresh_conn(conn_holder, label):
    """Recycle the persistent per-table connection if it is within RECYCLE of DSQL's ~60-min
    hard connection limit. conn_holder = [conn, started_monotonic]; both are updated in
    place. Called BEFORE every use of the persistent session (resume read, each chunk,
    each status marker) so no use can ever hit an expired connection. Returns the live conn.

    A recycle here happens BETWEEN transactions (never mid-transaction), so it can't drop
    an in-flight chunk: apply_file only calls this at the top of a chunk, before BEGIN."""
    if time.monotonic() - conn_holder[1] > CONN_RECYCLE_SECONDS:
        try:
            conn_holder[0].close()
        except Exception:
            pass
        conn_holder[0] = connect_dsql(autocommit=False)
        conn_holder[1] = time.monotonic()
        print(f"    ♻ recycled DSQL connection for {label} (approaching 60-min limit)")
    return conn_holder[0]


def split_s3(path):
    no_scheme = path.replace("s3://", "")
    parts = no_scheme.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# DSQL CONTROL TABLES  (inspired by DMS awsdms_status / awsdms_apply_exceptions)
# =============================================================================
# cdc_status keeps the authoritative resume position per table:
#   last_done_file   : HIGH-WATER — every CDC file <= this is fully applied (skip on scan)
#   in_progress_file : file currently mid-apply (resume here)
#   last_offset      : row offset within in_progress_file already applied
#   watermark_ts     : max dms_timestamp committed (DMS SOURCE_TIMESTAMP_APPLIED)
#   status           : active | blocked | idle
#   rows_applied     : running total
# The checkpoint is written in the SAME TRANSACTION as the chunk's data -> atomic.
# Keyed by table_name -> cross-run resume survives; NEVER dropped.
def ensure_control_tables():
    conn = connect_dsql_with_retry(autocommit=True, what="ensure_control_tables")
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}')
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_status (
                table_name        varchar(512) PRIMARY KEY,
                status            varchar(32),
                last_done_file    varchar(1024),
                in_progress_file  varchar(1024),
                last_offset       bigint,
                watermark_ts      varchar(64),
                rows_applied      bigint,
                error             varchar(4000),
                status_time       timestamptz
            )
        """)
        # NOTE: the PK id is supplied by Python (uuid4), NOT a server-side
        # gen_random_uuid() DEFAULT. Aurora DSQL does not expose pg_proc and does not
        # guarantee gen_random_uuid() in DDL defaults, so we avoid depending on it.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_apply_exceptions (
                id            uuid PRIMARY KEY,
                table_name    varchar(512),
                error_time    timestamptz,
                cdc_file      varchar(1024),
                row_offset    bigint,
                statement     varchar(8000),
                error         varchar(8000)
            )
        """)
        # Tier-2 validation failures (deferred by-PK net-state check). Mirrors DMS's
        # awsdms_validation_failures: what row (KEY) mismatched and how (failure_type +
        # details). A validation failure is a DISCREPANCY REPORT, not an apply failure —
        # the apply already committed; this flags that the target row didn't match the
        # expected net-op image for investigation. id supplied by Python (uuid4).
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_validation_failures (
                id            uuid PRIMARY KEY,
                table_name    varchar(512),
                failure_time  timestamptz,
                cdc_file      varchar(1024),
                pk_value      varchar(1024),
                failure_type  varchar(64),
                details       varchar(8000)
            )
        """)
        # PER-FILE LEDGER — one durable row per (table_name, cdc_file) recording the file's
        # apply lifecycle. This is ADDITIVE observability/audit on top of cdc_status (which
        # holds only the single moving resume position per table); it never changes how rows
        # are applied. Two markers at two grains (both on purpose — see NO_PK_CDC_UPDATE_
        # TRACKING.md §Build-1):
        #   • all_rows_committed (Option B, GRANULAR TRUTH): set true in the SAME txn as the
        #     file's FINAL data chunk. Atomic with the data itself -> can never lie: if it's
        #     true, every row in the file is durably in the target.
        #   • status 'started'->'done' (Option A, LIFECYCLE): 'started' written before the
        #     chunk loop; 'done' written in the immediately-following high-water txn that
        #     advances cdc_status.last_done_file + moves the file to processed/.
        # The narrow crash window (final chunk committed, high-water not yet advanced) is now
        # OBSERVABLE + self-describing: all_rows_committed=true AND status='started' means
        # "data safe, high-water lagging; resume re-marks it" (idempotent, no loss). Composite
        # natural key (table_name, cdc_file); DSQL supports a multi-column PRIMARY KEY.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_file_status (
                table_name          varchar(512),
                cdc_file            varchar(1024),
                status              varchar(32),
                has_pk              boolean,
                file_min_ts         varchar(64),
                file_max_ts         varchar(64),
                rows_applied        bigint,
                chunks_committed    bigint,
                all_rows_committed  boolean,
                started_time        timestamptz,
                committed_time      timestamptz,
                done_time           timestamptz,
                PRIMARY KEY (table_name, cdc_file)
            )
        """)
        # PER-CHUNK AUDIT TRAIL — one durable row per COMMITTED chunk, written in the SAME
        # txn as that chunk's data + cdc_status checkpoint. cdc_status only ever shows the
        # current moving offset (no history); this gives the full history of every chunk that
        # ever committed for a file (start/end offset, rows, watermark, when). Grain = chunk
        # (the atomic apply unit). id is a Python uuid4 (DSQL does not guarantee a server-side
        # gen_random_uuid() DDL default — same reason as cdc_apply_exceptions). PK-agnostic:
        # works for PK tables now and no-PK tables once the key path lands.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_chunk_log (
                id              uuid PRIMARY KEY,
                table_name      varchar(512),
                cdc_file        varchar(1024),
                chunk_seq       bigint,
                start_offset    bigint,
                end_offset      bigint,
                rows            bigint,
                watermark_ts    varchar(64),
                committed_time  timestamptz
            )
        """)
        # TIER-2 (keyless) SKIPPED-OP LOG. A keyless table is INSERT+DELETE only (the customer
        # configures DMS to emit insert/delete-only for no-PK tables). If an UPDATE (op=U) still
        # arrives, v4 CANNOT target the prior row from an after-image-only S3 CSV record (no key,
        # no before-image — proven in NO_PK_CDC_UPDATE_TRACKING.md). Owner decision: SKIP the U
        # (do not apply, do NOT block the table — keep flowing) and log it here so post-migration
        # you can enumerate exactly which updates were skipped (and thus which target rows still
        # hold their pre-update value). after_image is the full skipped-row image as JSON. id =
        # Python uuid4 (DSQL has no guaranteed gen_random_uuid() DDL default). AUDIT/REPORT only,
        # not apply state. NOT the same as cdc_apply_exceptions (that marks a table BLOCKED; a
        # skip does not block).
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_skipped_ops (
                id              uuid PRIMARY KEY,
                table_name      varchar(512),
                cdc_file        varchar(1024),
                dms_timestamp   varchar(64),
                change_seq      varchar(64),
                op              varchar(8),
                reason          varchar(64),
                after_image     text,
                skipped_time    timestamptz
            )
        """)
        print(f"  ✓ control tables ready in {CONTROL_SCHEMA}")
    finally:
        cur.close()
        conn.close()


def load_cdc_status(cur, table_name):
    """Read one table's cdc_status row (or a default 'never seen' dict)."""
    cur.execute(
        f'SELECT status, last_done_file, in_progress_file, last_offset, '
        f'watermark_ts, rows_applied FROM {CONTROL_SCHEMA}.cdc_status '
        f'WHERE table_name = %s', (table_name,))
    row = cur.fetchone()
    if not row:
        return {"status": None, "last_done_file": None, "in_progress_file": None,
                "last_offset": 0, "watermark_ts": None, "rows_applied": 0}
    return {"status": row[0], "last_done_file": row[1], "in_progress_file": row[2],
            "last_offset": int(row[3] or 0), "watermark_ts": row[4],
            "rows_applied": int(row[5] or 0)}


def update_cdc_status(cur, table_name, **fields):
    """HOT-PATH checkpoint writer: a PURE UPDATE of an already-existing cdc_status row.

    Called INSIDE the data-chunk transaction so the checkpoint commits atomically with the
    chunk. The row is guaranteed to exist because process_table calls ensure_status_row()
    once, before the file loop, so the hot path never needs SELECT-exists, never branches,
    never risks a 23505 that would abort the chunk txn (DSQL has no savepoints). This is
    the reliability+performance choice: exactly ONE statement + ONE round-trip per chunk
    checkpoint, and it can only ever UPDATE.

    We do NOT read cur.rowcount (unreliable on pg8000/DSQL). If the row were somehow
    missing (only possible if an operator deleted it mid-run) the UPDATE is a harmless
    no-op for this chunk; ensure_status_row on the next process_table cycle re-creates it,
    and the same-txn data still commits — no loss, resume re-derives position from S3."""
    fields["status_time"] = utc_now_iso()
    cols = list(fields.keys())
    vals = [fields[c] for c in cols]
    set_clause = ", ".join(f'{c} = %s' for c in cols)
    cur.execute(
        f'UPDATE {CONTROL_SCHEMA}.cdc_status SET {set_clause} WHERE table_name = %s',
        vals + [table_name])


def ensure_status_row_cur(cur, table_name):
    """Create the cdc_status row for a table if absent — ONCE, BEFORE the hot loop, so the
    per-chunk checkpoint can be a pure UPDATE. Cursor-based: the caller (run_control_op)
    owns the connection, commit, retries, and rollback-on-error.

    SELECT-exists-then-INSERT within the caller's txn. For a single serial writer per table
    the exists-check makes an INSERT race essentially impossible; in the rare event a
    concurrent creator wins between the SELECT and the INSERT, the INSERT raises 23505,
    run_control_op rolls back and retries, and the retry's SELECT finds the row -> success.
    So a 23505 here is transient-by-construction (see run_control_op's unique-violation
    branch)."""
    cur.execute(
        f'SELECT 1 FROM {CONTROL_SCHEMA}.cdc_status WHERE table_name = %s',
        (table_name,))
    if cur.fetchone() is not None:
        return
    cur.execute(
        f'INSERT INTO {CONTROL_SCHEMA}.cdc_status (table_name, status, status_time) '
        f'VALUES (%s, %s, %s)', (table_name, "init", utc_now_iso()))


def upsert_cdc_status(cur, table_name, **fields):
    """OFF-HOT-PATH insert-or-update, used only between files (_commit_status) where the
    write is its OWN short transaction (not fused to a data chunk). Deterministic
    SELECT-exists-then-branch (never cur.rowcount, never a blind INSERT that could 23505).
    Kept separate from update_cdc_status so the hot path stays a single UPDATE."""
    fields["status_time"] = utc_now_iso()
    cols = list(fields.keys())
    vals = [fields[c] for c in cols]
    cur.execute(
        f'SELECT 1 FROM {CONTROL_SCHEMA}.cdc_status WHERE table_name = %s',
        (table_name,))
    exists = cur.fetchone() is not None
    if exists:
        set_clause = ", ".join(f'{c} = %s' for c in cols)
        cur.execute(
            f'UPDATE {CONTROL_SCHEMA}.cdc_status SET {set_clause} WHERE table_name = %s',
            vals + [table_name])
    else:
        all_cols = ["table_name"] + cols
        placeholders = ", ".join(["%s"] * len(all_cols))
        cur.execute(
            f'INSERT INTO {CONTROL_SCHEMA}.cdc_status ({", ".join(all_cols)}) '
            f'VALUES ({placeholders})', [table_name] + vals)


# =============================================================================
# PER-FILE LEDGER + PER-CHUNK AUDIT  (additive tracking; never alters the apply path)
# =============================================================================
def mark_file_started_cur(cur, table_name, cdc_file, has_pk, file_min_ts, file_max_ts):
    """LIFECYCLE marker 'started' for a (table, cdc_file). Cursor-based (caller owns the
    txn/commit/retry), run OFF the hot path BEFORE the chunk loop. Idempotent via
    SELECT-exists-then-branch (same rule as upsert_cdc_status — never cur.rowcount, never a
    blind INSERT that could 23505): a re-run (resume of a partially-applied file) UPDATES
    the existing row back to 'started' and refreshes the file ts bounds, preserving the
    original started_time. Never regresses a row that already reached committed/done."""
    cur.execute(
        f'SELECT status FROM {CONTROL_SCHEMA}.cdc_file_status '
        f'WHERE table_name = %s AND cdc_file = %s', (table_name, cdc_file))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            f'INSERT INTO {CONTROL_SCHEMA}.cdc_file_status '
            f'(table_name, cdc_file, status, has_pk, file_min_ts, file_max_ts, '
            f'rows_applied, chunks_committed, all_rows_committed, started_time) '
            f'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (table_name, cdc_file, "started", bool(has_pk), file_min_ts, file_max_ts,
             0, 0, False, utc_now_iso()))
    else:
        # Resume of an in-progress file: keep started_time + any all_rows_committed already
        # set (never regress the granular truth marker), just refresh the ts bounds.
        cur.execute(
            f'UPDATE {CONTROL_SCHEMA}.cdc_file_status SET has_pk = %s, file_min_ts = %s, '
            f'file_max_ts = %s WHERE table_name = %s AND cdc_file = %s',
            (bool(has_pk), file_min_ts, file_max_ts, table_name, cdc_file))


def insert_chunk_log_cur(cur, table_name, cdc_file, chunk_seq, start_offset, end_offset,
                         rows, watermark_ts):
    """PER-CHUNK audit row. Cursor-based, called INSIDE the chunk's data txn so it commits
    atomically with the chunk + the cdc_status checkpoint. A pure INSERT (id = Python
    uuid4) — one row-modification, accounted for in the _pack_chunk mod reserve. On a
    crash-replay of a committed chunk the whole txn re-runs; a duplicate audit row is
    harmless (audit trail, not correctness state)."""
    cur.execute(
        f'INSERT INTO {CONTROL_SCHEMA}.cdc_chunk_log '
        f'(id, table_name, cdc_file, chunk_seq, start_offset, end_offset, rows, '
        f'watermark_ts, committed_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        (str(uuid.uuid4()), table_name, cdc_file, int(chunk_seq), int(start_offset),
         int(end_offset), int(rows), watermark_ts, utc_now_iso()))


def mark_file_committed_cur(cur, table_name, cdc_file, rows_applied, chunks_committed,
                            watermark_ts):
    """GRANULAR TRUTH marker (Option B): set all_rows_committed=true in the SAME txn as the
    file's FINAL data chunk. Atomic with the last rows -> if true, the whole file is durably
    applied. Pure UPDATE of the row mark_file_started_cur created (row guaranteed to exist);
    counted in the _pack_chunk mod reserve for the final chunk. Does NOT set status='done'
    — that's the lifecycle marker written by the following high-water txn."""
    cur.execute(
        f'UPDATE {CONTROL_SCHEMA}.cdc_file_status SET all_rows_committed = %s, '
        f'rows_applied = %s, chunks_committed = %s, file_max_ts = COALESCE(%s, file_max_ts), '
        f'committed_time = %s WHERE table_name = %s AND cdc_file = %s',
        (True, int(rows_applied), int(chunks_committed), watermark_ts, utc_now_iso(),
         table_name, cdc_file))


def mark_file_done_cur(cur, table_name, cdc_file):
    """LIFECYCLE marker 'done' (Option A): set status='done' + done_time in the same short
    txn that advances cdc_status.last_done_file (high-water) after the file is fully applied
    and about to be moved to processed/. Cursor-based; the caller (_commit_status via
    run_control_op) owns the commit + retry set. Pure UPDATE; harmless no-op if the row is
    somehow absent (resume re-creates it via mark_file_started_cur)."""
    cur.execute(
        f'UPDATE {CONTROL_SCHEMA}.cdc_file_status SET status = %s, done_time = %s '
        f'WHERE table_name = %s AND cdc_file = %s',
        ("done", utc_now_iso(), table_name, cdc_file))


def insert_skipped_op_cur(cur, table_name, cdc_file, dms_timestamp, change_seq, op,
                          after_image_json, reason="skipped_update_nonpk_table"):
    """TIER-2 audit: record one SKIPPED op (a keyless-table UPDATE we did not apply).
    Cursor-based, written in the SAME txn as the file's apply (so the log can't disagree with
    what was applied). Pure INSERT (id = Python uuid4). A replay re-inserts a skip row —
    harmless (report, not correctness state); post-migration queries dedup by
    (table_name, cdc_file, after_image) if needed. Does NOT block the table."""
    cur.execute(
        f'INSERT INTO {CONTROL_SCHEMA}.cdc_skipped_ops '
        f'(id, table_name, cdc_file, dms_timestamp, change_seq, op, reason, after_image, '
        f'skipped_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        (str(uuid.uuid4()), table_name, cdc_file, dms_timestamp, change_seq, op, reason,
         (after_image_json or "")[:1_000_000], utc_now_iso()))


def emit_skipped_update_metric(label, count):
    """Best-effort CloudWatch metric for Tier-2 skipped UPDATEs on a keyless table. Never
    raises (metrics must not affect the apply). Mirrors the guarded cloudwatch usage elsewhere."""
    if not cloudwatch or count <= 0:
        return
    try:
        cloudwatch.put_metric_data(
            Namespace="GlueCDC/NonPK",
            MetricData=[{
                "MetricName": "SkippedUpdates",
                "Dimensions": [{"Name": "Table", "Value": label}],
                "Value": float(count),
                "Unit": "Count",
            }])
    except Exception as e:
        print(f"    ⚠️ CW metric emit failed (non-fatal) for {label}: {e}")


def record_exception(table_name, cdc_file, row_offset, statement, error):
    """Log a genuine apply failure to cdc_apply_exceptions + mark the table blocked.
    Own short transaction (autocommit) — this runs after the table's apply txn rolled
    back, so it must not depend on that connection."""
    try:
        conn = connect_dsql(autocommit=True)
        cur = conn.cursor()
        try:
            cur.execute(
                f'INSERT INTO {CONTROL_SCHEMA}.cdc_apply_exceptions '
                f'(id, table_name, error_time, cdc_file, row_offset, statement, error) '
                f'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (str(uuid.uuid4()), table_name, utc_now_iso(), cdc_file, int(row_offset),
                 str(statement)[:8000], str(error)[:8000]))
            # SELECT-exists-then-branch (not cur.rowcount) for the same reason as
            # upsert_cdc_status: rowcount after UPDATE is unreliable on pg8000/DSQL.
            cur.execute(
                f'SELECT 1 FROM {CONTROL_SCHEMA}.cdc_status WHERE table_name = %s',
                (table_name,))
            if cur.fetchone() is not None:
                cur.execute(
                    f'UPDATE {CONTROL_SCHEMA}.cdc_status SET status = %s, error = %s, '
                    f'status_time = %s WHERE table_name = %s',
                    ("blocked", str(error)[:4000], utc_now_iso(), table_name))
            else:
                cur.execute(
                    f'INSERT INTO {CONTROL_SCHEMA}.cdc_status '
                    f'(table_name, status, error, status_time) VALUES (%s,%s,%s,%s)',
                    (table_name, "blocked", str(error)[:4000], utc_now_iso()))
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(f"    ⚠️ could not record exception for {table_name} (non-fatal): {e}")


def _record_validation_failure(conn, table_name, cdc_file, pk_value, failure_type, details):
    """Insert one Tier-2 validation discrepancy into cdc_validation_failures. Uses the
    caller's (autocommit) connection. Best-effort — never fatal."""
    try:
        c = conn.cursor()
        try:
            c.execute(
                f'INSERT INTO {CONTROL_SCHEMA}.cdc_validation_failures '
                f'(id, table_name, failure_time, cdc_file, pk_value, failure_type, details) '
                f'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (str(uuid.uuid4()), table_name, utc_now_iso(), cdc_file,
                 str(pk_value)[:1024], failure_type, str(details)[:8000]))
        finally:
            c.close()
    except Exception as e:
        print(f"    ⚠️ could not record validation failure for {table_name} (non-fatal): {e}")


def _values_match(expected, actual, category):
    """Compare an expected net-op value (already convert_value-normalized to the string
    form v4 sends) against the value read back from the target, tolerantly per category.
    Both sides are coerced to comparable strings; None==None. This is a best-effort
    content check, not a byte-exact one (DSQL may normalize casing/precision)."""
    if expected is None:
        return actual is None
    if actual is None:
        return False
    e = str(expected).strip()
    a = str(actual).strip()
    if category == 'uuid':
        return e.lower().replace("-", "") == a.lower().replace("-", "")
    if category == 'boolean':
        norm = {"true": "t", "t": "t", "1": "t", "false": "f", "f": "f", "0": "f"}
        return norm.get(e.lower(), e.lower()) == norm.get(a.lower(), a.lower())
    if category in ('integer', 'bigint', 'smallint'):
        try:
            return int(float(e)) == int(float(a))
        except (ValueError, TypeError):
            return e == a
    if category in ('numeric', 'float'):
        try:
            return abs(float(e) - float(a)) < 1e-9
        except (ValueError, TypeError):
            return e == a
    return e == a


def validate_file_netops(ctx, cdc_key, netops, col_category):
    """TIER-2 (deferred, sampled) validation for ONE just-committed file. For a sample of
    the file's net-ops, re-read the target row by PK and compare to the expected net-op
    image (INSERT -> present + values match; DELETE -> absent). Retry a mismatch once after
    a short delay (absorbs any lag), then record persistent mismatches to
    cdc_validation_failures. Runs on its OWN autocommit connection AFTER the file committed
    — never inside the apply transaction, so it can't affect apply latency/correctness.

    KNOWN LIMITATION (why it's advisory, sampled, and off by default): this checks a
    single file's net-ops against the CURRENT target. If a later CDC file re-inserts a PK
    this file DELETEd (or updates a PK this file INSERTed), the deferred check can see the
    LATER state and report a false MISSING_DELETE / RECORD_DIFF. The retry-once absorbs the
    immediate lag window; genuine drift persists across the retry. Treat recorded failures
    as leads to investigate (query cdc_validation_failures + the source), not hard proof.

    Returns the number of persistent discrepancies found (0 = clean)."""
    if not VALIDATION_ENABLED or not netops:
        return 0
    label = ctx["label"]
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]
    # Validation re-reads each sampled net-op by its TARGETING key (apply_key = PK or logical
    # key), matching how collapse/apply keyed the row. Only reached for keyed (Tier-1) tables;
    # keyless tables never produce pk-keyed netops (the router diverts them earlier).
    pk_col = ctx["apply_key"]
    pk_suffix = CAST_SUFFIX.get(col_category.get(pk_col, 'varchar'), '')

    # Sample: first N net-ops (deterministic + cheap). 0 = all (expensive; opt-in).
    sample = netops if VALIDATION_SAMPLE_PER_FILE <= 0 else netops[:VALIDATION_SAMPLE_PER_FILE]

    def _check_one(conn, netop):
        """Return None if the target matches the expected net-op, else a (type, details)."""
        pk_lit = sql_literal(netop["pk"], pk_suffix)
        cur = conn.cursor()
        try:
            if netop["op"] == "DELETE":
                cur.execute(f'SELECT 1 FROM {dsql_schema}.{dsql_table} '
                            f'WHERE "{pk_col}" = {pk_lit} LIMIT 1')
                if cur.fetchone() is not None:
                    return ("MISSING_DELETE", "row for pk still present after delete")
                return None
            # INSERT: row must exist; compare the expected columns.
            cols = list(netop["values"].keys())
            if not cols:
                return None
            quoted = ", ".join(f'"{c}"' for c in cols)
            cur.execute(f'SELECT {quoted} FROM {dsql_schema}.{dsql_table} '
                        f'WHERE "{pk_col}" = {pk_lit} LIMIT 1')
            row = cur.fetchone()
            if row is None:
                return ("MISSING_TARGET", "expected row absent from target")
            mismatches = []
            for i, c in enumerate(cols):
                if not _values_match(netop["values"].get(c), row[i], col_category.get(c, 'varchar')):
                    mismatches.append({c: [str(netop["values"].get(c))[:80],
                                           str(row[i])[:80]]})
            if mismatches:
                return ("RECORD_DIFF", json.dumps(mismatches)[:8000])
            return None
        finally:
            cur.close()

    conn = connect_dsql(autocommit=True)
    failures = 0
    try:
        for netop in sample:
            if failures >= VALIDATION_MAX_FAILURES_PER_TABLE:
                print(f"    ⚠️ VALIDATION {label}: failure cap "
                      f"({VALIDATION_MAX_FAILURES_PER_TABLE}) reached — stopping this pass.")
                break
            res = _check_one(conn, netop)
            if res is None:
                continue
            # Retry once after a delay — absorbs a transient lag between commit and read.
            time.sleep(VALIDATION_RETRY_DELAY_SECONDS)
            res2 = _check_one(conn, netop)
            if res2 is None:
                continue   # cleared on retry -> was just lag, not a real discrepancy
            ftype, details = res2
            _record_validation_failure(conn, label, cdc_key, netop["pk"], ftype, details)
            failures += 1
        if failures:
            print(f"    ⚠️ VALIDATION {label} {cdc_key.split('/')[-1]}: "
                  f"{failures} discrepancy(ies) recorded in cdc_validation_failures "
                  f"(apply already committed; this is a report, not a block).")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return failures


# =============================================================================
# PER-TABLE SCHEMA + CONFIG  (from Job 1 v2 manifest)
# =============================================================================
def load_manifest():
    """Read Job 1 v2 master index -> list of table entries with config_s3_path."""
    if INDEX_S3_KEY:
        bucket, key = BUCKET, INDEX_S3_KEY
    else:
        bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_manifest_index.json')
    obj = s3.get_object(Bucket=bucket, Key=key)
    doc = json.loads(obj['Body'].read().decode('utf-8'))
    tables = doc.get('tables', [])
    if not tables:
        raise Exception("Master index has no tables — run Job 1 first.")
    return tables


def load_table_config(entry):
    """Read a table's per-table config JSON (column_mapping, type_categories, PK)."""
    bucket, key = split_s3(entry['config_s3_path'])
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def load_full_load_status():
    """Read _load_status.json (written by the full-load job) -> {label: status_string}.
    Returns {} if the file is absent/unreadable (treated as 'no table done yet' when the
    gate is on). Shape matches Job 2: {"tables": {"<schema.table>": {"status": "done", ...}}}.
    Re-read each poll cycle so a table becomes CDC-eligible as soon as its load finishes."""
    if LOAD_STATUS_KEY:
        bucket, key = BUCKET, LOAD_STATUS_KEY
    else:
        bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_load_status.json')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        # Distinguish "file legitimately not written yet" (expected before the first full
        # load completes -> quiet) from "present but we can't read it / access denied /
        # wrong key" (a MISCONFIG that would otherwise masquerade as 'waiting_full_load'
        # forever). Fail-closed either way (gate stays shut), but make a misconfig LOUD so
        # a stuck pipeline isn't mistaken for a slow full load.
        err_code = (e.response.get("Error", {}).get("Code") if hasattr(e, "response") else "") or ""
        if err_code in ("NoSuchKey", "404", "NoSuchBucket"):
            print(f"  ℹ️ _load_status.json not present yet at s3://{bucket}/{key} "
                  f"— treating all tables as 'full load not done' (expected pre-load).")
        else:
            print(f"  ⚠️ CANNOT READ _load_status.json at s3://{bucket}/{key}: {e}. "
                  f"CDC gate stays CLOSED (fail-closed). Check LOAD_STATUS_KEY / IAM "
                  f"s3:GetObject — this is a MISCONFIG, not a slow full load.")
        return {}
    try:
        doc = json.loads(obj['Body'].read().decode('utf-8'))
        tables = doc.get('tables', {}) if isinstance(doc, dict) else {}
        return {k: (v or {}).get('status') for k, v in tables.items()}
    except Exception as e:
        print(f"  ⚠️ _load_status.json at s3://{bucket}/{key} is present but UNPARSEABLE: "
              f"{e}. CDC gate stays CLOSED. Fix the file (must be JSON "
              f'{{"tables": {{"<schema.table>": {{"status": "done"}}}}}}).')
        return {}


def load_dsql_schema(cur, dsql_schema, dsql_table):
    """Current DSQL columns for a table: {col_name: {'type':..,'max_length':..}}."""
    cur.execute(
        """SELECT column_name, data_type, character_maximum_length
             FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position""",
        (dsql_schema, dsql_table))
    schema = {}
    for row in cur.fetchall():
        schema[row[0]] = {'type': row[1], 'max_length': row[2]}
    return schema


def derive_table_prefixes(entry):
    """CDC file prefixes for a table. Mirrors the DMS S3 layout <schema>/<table>/."""
    dms_schema = entry['dms_schema']
    dms_table = entry['dms_table']
    # CDC_ROOT may be empty (or a '.'/'/'-style sentinel, since Glue getResolvedOptions
    # cannot pass an empty-string arg value) when the DMS S3 target has NO bucketFolder and
    # full-load AND CDC files share the per-table root <schema>/<table>/. Normalize any of
    # those to "no cdc subfolder" and avoid a leading slash in the key.
    root = (CDC_ROOT or "").strip("/. ")
    base = f"{root}/{dms_schema}/{dms_table}/" if root else f"{dms_schema}/{dms_table}/"
    return {
        "cdc": base,
        "processed": base + "processed/",
        "failed": base + "failed/",
    }


def build_table_context(entry):
    """Assemble everything a table's CDC worker needs from Job 1 config."""
    config = load_table_config(entry)
    meta = config.get('metadata', {})
    dsql_schema = meta.get('dsql_schema') or entry['dsql_schema']
    dsql_table = meta.get('dsql_table') or entry['dsql_table']
    type_categories = config.get('type_categories', {}) or {}
    target_columns = [c['name'] if isinstance(c, dict) else c
                      for c in config.get('target_columns', [])]
    pk_meta = meta.get('primary_key', {}) or {}
    pk_cols = pk_meta.get('columns') or []
    pk_col = pk_cols[0] if len(pk_cols) == 1 else None

    # ── APPLY KEY (Tier-1 targeting key) ─────────────────────────────────────────────
    # The column CDC targets a row by. It is the real DB PRIMARY KEY when the table has a
    # single-column PK; otherwise, for a no-PK table, it is an OPERATOR-DECLARED (or Job1-
    # discovered) single-column LOGICAL KEY — a column that is unique + never updated
    # (declared in metadata.logical_key, kept SEPARATE from primary_key so v16 span-recover
    # isn't falsely triggered and so we know the key is target-side only). This is the
    # generic no-PK UPDATE solution: the after-image still carries an unchanged logical key,
    # so ON CONFLICT(logical_key) DO UPDATE applies I/U/D correctly (see NO_PK_CDC_UPDATE_
    # TRACKING.md §9). A UNIQUE index on the target (built by Job2 after the full load) makes
    # ON CONFLICT valid and validates the declaration. logical_key is honored ONLY when the
    # table has NO real PK (a real PK always wins — never override it).
    lk_meta = meta.get('logical_key', {}) or {}
    lk_cols = lk_meta.get('columns') or []
    logical_key_col = None
    if pk_col is None and len(lk_cols) == 1:
        # Single-column logical key only (multi-col logical keys are a later enhancement;
        # collapse/upsert currently key on one column, same as the single-col PK path).
        logical_key_col = lk_cols[0]
    apply_key = pk_col if pk_col is not None else logical_key_col
    if pk_col is not None:
        key_source = "pk"
    elif logical_key_col is not None:
        key_source = "logical_key"
    else:
        key_source = "none"
    # Tier hint from configuration alone (op-stream observation in collapse_net_ops can
    # still DOWNGRADE a keyless table to Tier 3 the moment it sees an UPDATE):
    #   Tier 1 = has an apply key (PK or logical) -> correct synchronous I/U/D.
    #   keyless -> provisional; classified at apply time (Tier 2 insert/delete-only, or
    #   Tier 3 fail-closed if an UPDATE is observed).
    tier_hint = 1 if apply_key is not None else None
    # varchar length guards from column_mapping (name -> max_length).
    varchar_max = {}
    for m in config.get('column_mapping', []):
        if isinstance(m, dict) and m.get('action') == 'map':
            tgt = m.get('target_column')
            ml = m.get('max_length')
            if tgt and type_categories.get(tgt) == 'varchar' and isinstance(ml, int) and ml > 0:
                varchar_max[tgt] = ml
    # Optional operator-declared rename map {old_col: new_col} in the Job 1 config's
    # metadata — lets a deterministic RENAME be applied even if the DMS DDL event is
    # missed. Absent for most tables.
    rename_hints = meta.get('rename_hints') or {}
    label = f"{dsql_schema}.{dsql_table}"
    return {
        "label": label,
        "dsql_schema": dsql_schema,
        "dsql_table": dsql_table,
        "dms_table": entry['dms_table'],
        "type_categories": type_categories,
        "target_columns": target_columns,
        "pk_col": pk_col,               # real DB PRIMARY KEY (single-col) or None
        "apply_key": apply_key,         # targeting key: PK, else declared logical key, else None
        "logical_key_col": logical_key_col,   # the logical key when key_source == 'logical_key'
        "key_source": key_source,       # 'pk' | 'logical_key' | 'none'
        "tier_hint": tier_hint,         # 1 if keyed; None if keyless (classified at apply)
        "varchar_max": varchar_max,
        "rename_hints": rename_hints,
        "prefixes": derive_table_prefixes(entry),
        "config": config,
    }


# =============================================================================
# DDL WATCHER (background thread)  — preserved from v3, now PER TABLE
# =============================================================================
def ddl_watcher(dms_tables):
    """Poll DMS table-statistics for DDL-count changes per table. dms_tables is a list of
    DMS table names (as DMS reports them, typically UPPERCASE)."""
    # seed counts
    for t in dms_tables:
        try:
            stats = dms.describe_table_statistics(
                ReplicationTaskArn=DMS_TASK_ARN,
                Filters=[{'Name': 'table-name', 'Values': [t]}])
            ddls = stats['TableStatistics'][0].get('Ddls', 0) if stats.get('TableStatistics') else 0
            with _ddl_lock:
                _ddl_state[t] = {"count": ddls, "event": False, "time": None}
        except Exception:
            with _ddl_lock:
                _ddl_state[t] = {"count": 0, "event": False, "time": None}
    while not _stop_event.is_set():
        for t in dms_tables:
            try:
                stats = dms.describe_table_statistics(
                    ReplicationTaskArn=DMS_TASK_ARN,
                    Filters=[{'Name': 'table-name', 'Values': [t]}])
                if stats.get('TableStatistics'):
                    cur_ddl = stats['TableStatistics'][0].get('Ddls', 0)
                    with _ddl_lock:
                        prev = _ddl_state.get(t, {"count": 0})
                        if cur_ddl > prev.get("count", 0):
                            _ddl_state[t] = {"count": cur_ddl, "event": True,
                                             "time": datetime.now(timezone.utc)}
                            print(f"  ⚡ DDL EVENT for {t} ({prev.get('count',0)} -> {cur_ddl})")
            except Exception:
                pass
        _stop_event.wait(DDL_WATCH_INTERVAL)


def consume_ddl_event(dms_table):
    """Read-and-reset the DDL event flag for a table (preserves v3 semantics).
    _ddl_state is keyed by the DMS table name as the watcher seeds it — UPPERCASE (dms_tables
    in main() is built as {c['dms_table'].upper()}), because DMS describe_table_statistics
    reports/filters table names in the source's native (uppercase) case. ctx['dms_table'] here
    is the lowercased manifest name, so we MUST uppercase before the lookup — otherwise the
    key never matches, consume always returns False, and the RENAME-COLUMN path can never fire
    (renames silently degrade to ADD + orphaned old column)."""
    key = (dms_table or "").upper()
    with _ddl_lock:
        st = _ddl_state.get(key)
        if st and st.get("event"):
            st["event"] = False
            return True
    return False


def infer_type_from_samples(col_name, sample_values):
    """Type inference for a NEW column (preserved from v3, name + data based)."""
    nl = col_name.lower()
    if nl.startswith('is_') or nl.startswith('has_'):
        return 'boolean'
    if nl.endswith('_date') or nl.startswith('date_'):
        return 'timestamptz'
    if nl.endswith('_id') and any(v and _UUID_RAW_HEX.match(v) for v in sample_values):
        return 'uuid'
    if nl == 'version' or nl.endswith('_count') or nl.endswith('_number'):
        return 'bigint'
    for v in sample_values:
        if v and v.strip():
            s = v.strip()
            if _UUID_RAW_HEX.match(s):
                return 'uuid'
            if s in ('0', '1'):
                return 'boolean'
            if re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', s):
                return 'timestamptz'
            if re.match(r'^-?\d+$', s) and len(s) <= 18:
                return 'integer' if len(s) <= 9 else 'bigint'
            if re.match(r'^-?\d+\.\d+$', s):
                return 'numeric'
            ln = len(s)
            return ('varchar(50)' if ln <= 25 else 'varchar(100)' if ln <= 50
                    else 'varchar(255)' if ln <= 100 else 'varchar(500)' if ln <= 255 else 'text')
    return 'text'


def handle_schema_changes(ctx, header, data_rows):
    """Compare CDC file header vs DSQL schema; ADD new columns, RENAME (DDL+1<->1),
    non-destructive DROP (log only). Preserved from v3, keyed per table. Refreshes and
    returns the up-to-date DSQL schema dict."""
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]
    label = ctx["label"]
    header_key = tuple(header)
    # FAST PATH: if this file's header matches the last processed file's header for this
    # table, the schema cannot have drifted -> reuse the cached DSQL schema and skip the
    # catalog query entirely (no information_schema round-trip per file).
    cached = _schema_cache.get(label)
    if cached and cached[0] == header_key:
        return cached[1]

    conn = connect_dsql(autocommit=True)
    cur = conn.cursor()
    try:
        schema = load_dsql_schema(cur, dsql_schema, dsql_table)
        dsql_cols_lower = {c.lower() for c in schema}
        file_cols = [c for c in header if c not in IGNORE_COLUMNS]
        file_cols_lower = {c.lower() for c in file_cols}
        new_cols = file_cols_lower - dsql_cols_lower
        missing_cols = dsql_cols_lower - file_cols_lower
        final_schema = schema   # updated after any DDL below
        if new_cols or missing_cols:
            ddl_happened = consume_ddl_event(ctx["dms_table"])
            print(f"    🔄 SCHEMA CHANGE {label}: new={sorted(new_cols)} "
                  f"missing={sorted(missing_cols)} ddl_event={ddl_happened}")
            renamed = False
            # RENAME vs DROP+ADD has no before-image on S3 CDC. But under the DMS
            # AddColumnName=true model DMS sends EVERY column on EVERY change row AND PRESERVES
            # COLUMN ORDER: a RENAME keeps the column's ordinal POSITION (only its name changes),
            # while an ADD appends new column(s) at the END. So we can disambiguate a RENAME even
            # when it is combined with ADDs in the same DDL batch, by POSITION:
            #   - Build the old target column order and the new CDC header order (both minus
            #     control cols).
            #   - For each MISSING column (in old but not new), the column now occupying its old
            #     ordinal position in the header is its RENAME target (if that position holds a
            #     NEW column). Pair them -> RENAME.
            #   - Any remaining NEW columns (typically appended at the end) are genuine ADDs.
            # Priority for confirming a positional pairing: explicit rename_hint > this positional
            # match (default, gated by SINGLE_SWAP_IS_RENAME). This fixes the block that occurred
            # when a rename was bundled with an add (2 new + 1 gone), which the old single_swap
            # (==1 and ==1) check could not handle -> it ADDed the renamed col and orphaned the old.
            rename_hints = {k.lower(): v.lower()
                            for k, v in (ctx.get("rename_hints") or {}).items()}
            # Ordered, control-cols-excluded views of old target schema and new header.
            old_order = [c for c in schema if c.lower() not in IGNORE_COLUMNS]
            new_order = [c for c in header if c.lower() not in IGNORE_COLUMNS]
            old_lower = [c.lower() for c in old_order]
            new_lower = [c.lower() for c in new_order]
            rename_pairs = {}   # old_lower -> new_lower  (to RENAME)
            for om in sorted(missing_cols):
                mapped = None
                # 1) explicit hint wins
                if om in rename_hints and rename_hints[om] in new_cols:
                    mapped = rename_hints[om]
                elif SINGLE_SWAP_IS_RENAME or ddl_happened:
                    # 2) positional: the column now at om's old ordinal position, if it's a NEW col
                    try:
                        pos = old_lower.index(om)
                    except ValueError:
                        pos = -1
                    if 0 <= pos < len(new_lower) and new_lower[pos] in new_cols:
                        mapped = new_lower[pos]
                if mapped:
                    rename_pairs[om] = mapped
            for old_name, new_name in rename_pairs.items():
                try:
                    cur.execute(f'ALTER TABLE {dsql_schema}.{dsql_table} '
                                f'RENAME COLUMN "{old_name}" TO "{new_name}"')
                    why = ("config rename_hint" if (old_name in rename_hints
                           and rename_hints[old_name] == new_name)
                           else "DMS DDL event (positional)" if ddl_happened
                           else "positional single-swap default (AddColumnName=true)")
                    print(f"    ✅ RENAME {old_name} -> {new_name} ({why})")
                    renamed = True
                except Exception as e:
                    print(f"    ⚠️ RENAME {old_name}->{new_name} failed ({e}); will ADD instead")
            # New columns that were NOT consumed by a rename pairing are genuine ADDs.
            add_cols = new_cols - set(rename_pairs.values())
            if add_cols:
                # ADD new columns (infer type from samples).
                header_lower = [c.lower() for c in header]
                for col_name in sorted(add_cols):
                    idx = header_lower.index(col_name) if col_name in header_lower else -1
                    samples = []
                    if idx >= 0:
                        for r in data_rows[:20]:
                            if idx < len(r):
                                samples.append(r[idx])
                    col_type = infer_type_from_samples(col_name, samples)
                    try:
                        cur.execute(f'ALTER TABLE {dsql_schema}.{dsql_table} '
                                    f'ADD COLUMN "{col_name}" {col_type}')
                        print(f"    ✅ ADD COLUMN \"{col_name}\" {col_type}")
                    except Exception as e:
                        if 'already exists' not in str(e).lower():
                            print(f"    ❌ ADD COLUMN {col_name} failed: {e}")
            # DROP: non-destructive — keep column, log only (v3 behavior). Exclude any missing
            # column that was consumed by a RENAME pairing above (it wasn't dropped, it moved).
            dropped_cols = set(missing_cols) - set(rename_pairs.keys())
            for col_name in sorted(dropped_cols):
                print(f"    ⚠️ column '{col_name}' no longer in source "
                      f"(kept in DSQL, will be NULL)")
            # Re-read the schema after any DDL so active_cols reflects reality.
            final_schema = load_dsql_schema(cur, dsql_schema, dsql_table)
        # Cache (header -> resolved schema) so an identical next header skips the catalog.
        _schema_cache[label] = (header_key, final_schema)
        return final_schema
    finally:
        cur.close()
        conn.close()


# =============================================================================
# CDC FILE IO
# =============================================================================
def list_cdc_files(ctx):
    """List a table's pending CDC CSVs (sorted by name == DMS timestamp order)."""
    prefix = ctx["prefixes"]["cdc"]
    files = []
    token = None
    # Files younger than MIN_FILE_AGE_SECONDS may still be mid-write by DMS -> defer them
    # to the next poll (see MIN_FILE_AGE_SECONDS). Compare S3 LastModified to now (UTC).
    now = datetime.now(timezone.utc)
    skipped_too_new = 0
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            key = o["Key"]
            if not (key.endswith(".csv") and "/processed/" not in key and "/failed/" not in key):
                continue
            # When the DMS S3 target has NO bucketFolder, full-load LOAD*.csv files share
            # this per-table directory with the timestamp-named CDC files. NEVER treat a
            # full-load file as CDC (it has no Op column) -> exclude LOAD*.csv.
            fname = key.rsplit("/", 1)[-1]
            if fname.upper().startswith("LOAD"):
                continue
            lm = o.get("LastModified")
            if lm is not None and (now - lm).total_seconds() < MIN_FILE_AGE_SECONDS:
                skipped_too_new += 1
                continue   # still possibly being written; pick it up next poll
            files.append(key)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    files.sort()
    if skipped_too_new:
        print(f"    ⏳ {ctx['label']}: deferring {skipped_too_new} file(s) < "
              f"{MIN_FILE_AGE_SECONDS}s old (possibly still being written)")
    return files


def read_cdc_file(key):
    """Read a CDC CSV -> (header_lowercased, data_rows). First row is the header."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    content = obj['Body'].read().decode('utf-8')
    rows = list(csv.reader(io.StringIO(content)))
    if not rows:
        return [], []
    header = [c.lower().strip() for c in rows[0]]
    return header, rows[1:]


def move_file(key, dest_prefix):
    filename = key.split('/')[-1]
    new_key = dest_prefix + filename
    s3.copy_object(Bucket=BUCKET, Key=new_key, CopySource={'Bucket': BUCKET, 'Key': key})
    s3.delete_object(Bucket=BUCKET, Key=key)


# =============================================================================
# CDC APPLY — one file, serial, chunked, same-txn checkpoint, zero-loss
# =============================================================================
class TableBlocked(Exception):
    """Raised when a genuinely un-appliable row halts a table (after logging it)."""


class _SchemaChangedMidFile(Exception):
    """Internal signal (NOT a block): a concurrent DDL changed the table's schema while a
    chunk txn was open, so the file's frozen column list is stale. apply_file unwinds; the
    table is NOT blocked. process_table logs it and returns normally so the NEXT poll cycle
    re-runs apply_file from the committed offset, re-reconciling the schema at the top."""


def dsql_type_to_category(data_type):
    """Map an information_schema.data_type string to v4's type_category. Used for columns
    that exist in the LIVE DSQL schema but not in the static Job 1 config (i.e. columns
    ADDED or RENAMED after discovery) — so DDL-created columns still get the correct
    ::type cast and conversion instead of being written as raw varchar."""
    if not data_type:
        return 'varchar'
    dt = data_type.lower()
    if dt == 'uuid':
        return 'uuid'
    if dt == 'boolean':
        return 'boolean'
    if 'timestamp' in dt:
        return 'timestamptz'
    if dt == 'date':
        return 'date'
    if dt in ('bigint', 'int8'):
        return 'bigint'
    if dt in ('integer', 'int', 'int4'):
        return 'integer'
    if dt in ('smallint', 'int2'):
        return 'smallint'
    if dt in ('numeric', 'decimal'):
        return 'numeric'
    if dt in ('double precision', 'real', 'float', 'float8', 'float4'):
        return 'float'
    if dt in ('json', 'jsonb'):
        return 'json'
    if dt == 'bytea':
        return 'bytea'
    return 'varchar'


def collapse_net_ops(rows, header, ctx, insert_cols, col_category):
    """Collapse raw CDC rows into ORDERED net operations, immutable-PK model.

    insert_cols  : the columns actually written this file = (LIVE DSQL schema) ∩ (CDC
                   header), resolved by apply_file AFTER any DDL. Driving the column set
                   off the LIVE schema (not the static Job 1 config) is what makes ADD /
                   RENAME columns get written and dropped columns get skipped — the config
                   drifts from the schema after DDL, the live schema does not.
    col_category : {col -> type_category} for every insert col (from config, else derived
                   from the live DSQL type for DDL-created columns).

    Returns netops = list of {op:'INSERT'|'DELETE', pk:<canonical>, values:{col:converted}}
    preserving first-appearance order of each pk (DMS commit order within the file).

    Immutable-PK collapse (last op wins per pk):
      I / U (any) ending non-delete -> INSERT with the LAST row's values
      ... ending in D               -> DELETE
      I ... D within the window     -> net nothing (dropped)."""
    op_idx = header.index(OP_COLUMN) if OP_COLUMN in header else 0
    # APPLY KEY = the column we target a row by: the real DB PK, or (for a no-PK table) a
    # declared/discovered single-column LOGICAL KEY. build_table_context resolved it.
    apply_key = ctx["apply_key"]

    # KEYED (TIER-1) COLLAPSE ONLY. Tier routing happens in process_table: a keyless table
    # (apply_key is None) is sent to apply_file_nonpk (Tier-2: insert/delete only, updates
    # skipped+logged) and NEVER reaches this function. This guard makes that invariant
    # explicit — reaching here keyless is an internal routing bug, not a data condition, so
    # block loudly rather than run the keyed logic (which requires a key column).
    if apply_key is None:
        raise TableBlocked(
            f"INTERNAL [{ctx['label']}]: collapse_net_ops (keyed/Tier-1) was called for a "
            f"keyless table — keyless tables must route to apply_file_nonpk. Routing bug.")

    pk_col = apply_key   # keep the local name `pk_col` for the existing keyed logic below
    pk_lower = pk_col.lower()
    pk_idx = header.index(pk_lower) if pk_lower in header else None
    if pk_idx is None:
        raise TableBlocked(f"CDC file for {ctx['label']} has no '{pk_col}' key column in "
                           f"header (key_source={ctx['key_source']})")

    pk_category = col_category.get(pk_col, ctx["type_categories"].get(pk_col, 'varchar'))
    header_lower = header
    # header index for each insert column (all are guaranteed present in the header:
    # apply_file computed insert_cols as schema ∩ header).
    col_idx = {c: header_lower.index(c.lower()) for c in insert_cols}

    ordered_pks = []
    net = {}
    raw_rows = 0        # data rows actually processed (excludes malformed <2-field lines)
    skipped_short = 0   # malformed lines skipped (fewer than 2 fields)
    for row in rows:
        if len(row) < 2:
            skipped_short += 1
            continue
        raw_rows += 1
        op = (row[op_idx].upper() if op_idx < len(row) else 'I')
        pk_raw = row[pk_idx] if pk_idx < len(row) else None
        pk_val = hex_to_canonical_uuid(pk_raw) if pk_category == 'uuid' else _coerce_null(pk_raw)
        if pk_val is None:
            raise TableBlocked(f"CDC row with empty PK '{pk_col}' in {ctx['label']} "
                               f"(op={op}) — cannot key the change")
        if pk_val not in net:
            ordered_pks.append(pk_val)
        if op == 'D':
            net[pk_val] = {"op": "DELETE", "pk": pk_val, "values": None}
        else:  # I or U -> the row's full current image (values for every insert col)
            values = {}
            for c, idx in col_idx.items():
                raw = row[idx] if idx < len(row) else None
                values[c] = convert_value(raw, col_category.get(c, 'varchar'))
            net[pk_val] = {"op": "INSERT", "pk": pk_val, "values": values}
    netops = [net[pk] for pk in ordered_pks]
    # Validation stats (all counters we already had while iterating — no extra work):
    #   raw_rows     : real data rows in the file (op-carrying lines)
    #   distinct_pks : == len(netops); each PK collapsed to one net-op
    #   collapsed    : raw_rows - distinct_pks (multiple ops on same PK folded to last-wins)
    #   n_insert/n_delete : net-op breakdown
    stats = {
        "raw_rows": raw_rows,
        "skipped_short": skipped_short,
        "distinct_pks": len(netops),
        "collapsed": raw_rows - len(netops),
        "n_insert": sum(1 for x in netops if x["op"] == "INSERT"),
        "n_delete": sum(1 for x in netops if x["op"] == "DELETE"),
    }
    return netops, stats


def guard_row(ctx, values, col_category):
    """Per-row shape guards (v15): uuid shape + varchar length. Raises TableBlocked.
    Uses the resolved col_category (config + DDL-derived) so ADD/RENAME columns are
    guarded correctly too."""
    for c, v in values.items():
        if col_category.get(c) == 'uuid' and not is_valid_uuid_value(v):
            raise TableBlocked(
                f"UUID GUARD [{ctx['label']}]: column '{c}' holds a non-uuid value "
                f"{repr(v)[:120]} — CSV misalignment or bad source data.")
    for c, limit in ctx["varchar_max"].items():
        v = values.get(c)
        if v is not None:
            n = len(v) if isinstance(v, str) else len(str(v))
            if n > limit:
                raise TableBlocked(
                    f"LENGTH GUARD [{ctx['label']}]: column '{c}' length {n} exceeds "
                    f"varchar({limit}). Widen the column or fix the mapping.")


def max_dms_ts(rows, header):
    """Max dms_timestamp string in a set of raw rows (the batch watermark)."""
    if DMS_TIMESTAMP_COLUMN not in header:
        return None
    ts_idx = header.index(DMS_TIMESTAMP_COLUMN)
    best = None
    for r in rows:
        if ts_idx < len(r):
            v = r[ts_idx]
            if v and (best is None or v > best):
                best = v
    return best


def min_dms_ts(rows, header):
    """Min dms_timestamp string in a set of raw rows (the file's earliest change time).
    Recorded in the cdc_file_status ledger alongside the max (file_max_ts) so a file's
    change-time span is visible. Lexical min is correct here for the same reason max is:
    dms_timestamp is a fixed-width normalized timestamp string, so string order == time
    order (matches how the whole pipeline sorts CDC files by name/timestamp)."""
    if DMS_TIMESTAMP_COLUMN not in header:
        return None
    ts_idx = header.index(DMS_TIMESTAMP_COLUMN)
    best = None
    for r in rows:
        if ts_idx < len(r):
            v = r[ts_idx]
            if v and (best is None or v < best):
                best = v
    return best


def apply_file(ctx, cdc_key, start_offset, conn_holder, prior_watermark=None):
    """Apply ONE CDC file to DSQL, SERIALLY, in chunks. Each chunk commits its data AND
    the cdc_status checkpoint (in_progress_file, last_offset, watermark_ts) in ONE
    transaction. Resumes from start_offset. Zero-loss: any genuinely bad row raises
    TableBlocked (after the caller logs it); transient errors are retried.

    conn_holder is [conn, started_monotonic] — the table's persistent session + its age,
    shared with process_table so ensure_fresh_conn can recycle it under the 60-min limit.
    prior_watermark is the last committed dms_timestamp (for the monotonicity check).
    Returns (total_applied, file_watermark)."""
    label = ctx["label"]
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]
    type_categories = ctx["type_categories"]
    # TARGETING KEY for the keyed (Tier-1) apply path: the real DB PK when present, else the
    # declared/discovered single-column LOGICAL KEY (ctx["apply_key"]). Everything below that
    # keys a row — ON CONFLICT(...), DELETE WHERE ...=, the pk_suffix cast, the _schema_
    # missing PK exemption — targets THIS column, so a logical-key table applies I/U/D exactly
    # like a PK table. For a KEYLESS table apply_key is None, but collapse_net_ops' 3-tier
    # router raises before any SQL is built here (Tier 2 accept-duplicates path / Tier 3), so
    # the keyed logic below is only ever reached when apply_key is non-None.
    pk_col = ctx["apply_key"]

    header, data_rows = read_cdc_file(cdc_key)
    if not header or not data_rows:
        return 0, None, 0   # empty file -> no rows, no watermark advance, no chunks

    # DDL / schema reconciliation BEFORE building the column list (v3 ordering). Returns
    # the LIVE DSQL schema {col: {type, max_length}} AFTER any ADD/RENAME this file caused.
    schema = handle_schema_changes(ctx, header, data_rows)

    # COLUMN SET IS DRIVEN BY THE LIVE DSQL SCHEMA, NOT the static Job 1 config. This is
    # the DDL-correctness fix: after an ADD or RENAME, the config's target_columns drifts
    # from the real table (a renamed/added column isn't in the config), so using the config
    # would SILENTLY DROP that column's data. We insert exactly the columns present in BOTH
    # the live DSQL schema AND this file's CDC header (minus DMS control columns):
    #   - a column added to DSQL + present in the header  -> written (picked up automatically)
    #   - a column in DSQL but NOT in the header          -> the source isn't sending it;
    #       writing NULL would clobber a real value on UPDATE -> FAIL LOUD (no-loss).
    #   - a column in the header but NOT in DSQL          -> handle_schema_changes ADDed it
    #       (so it's now in schema) or it's ignorable; not in schema -> not inserted.
    header_set = {c.lower() for c in header if c not in IGNORE_COLUMNS}
    schema_lower = {c.lower(): c for c in schema}   # live DSQL columns
    insert_cols = [schema_lower[lc] for lc in schema_lower if lc in header_set]
    if not insert_cols:
        raise TableBlocked(f"no columns to write for {label} (schema ∩ header is empty)")
    # A DSQL column missing from the header (excluding the PK, which is always keyed and
    # always present) means the source stopped sending it -> silent-NULL risk -> block.
    _schema_missing = [schema_lower[lc] for lc in schema_lower
                       if lc not in header_set and lc != (pk_col or "").lower()]
    if _schema_missing:
        raise TableBlocked(
            f"CDC file {cdc_key.split('/')[-1]} for {label} is missing column(s) "
            f"{_schema_missing} that exist in the DSQL target. DMS CDC with "
            f"AddColumnName=true should send every column on each change; a missing "
            f"column would silently NULL a real value on UPDATE. Fix the DMS mapping / "
            f"header, then clear cdc_status to resume.")

    # Resolve a type_category for every insert column + the PK: prefer the Job 1 config,
    # else derive from the LIVE DSQL type (so ADD/RENAME columns get the right ::cast).
    def _cat(col_name):
        c = type_categories.get(col_name)
        if c:
            return c
        return dsql_type_to_category((schema.get(col_name) or {}).get('type'))
    col_category = {c: _cat(c) for c in insert_cols}
    if pk_col:
        col_category[pk_col] = _cat(pk_col)

    # Collapse to ordered net-ops (immutable-PK, last-op-wins), driven by the live cols.
    netops, vstats = collapse_net_ops(data_rows, header, ctx, insert_cols, col_category)
    file_watermark = max_dms_ts(data_rows, header)

    # ── TIER-1 IN-APPLY VALIDATION (cheap, no DB/S3 round-trips — pure arithmetic on
    # counters we already computed). Proves every op in the file is accounted for and the
    # stream is ordered. This is the "validation in CDC" for an S3-only source: it verifies
    # we correctly applied what DMS wrote to S3 (it cannot see the live source DB).
    #   (a) op reconciliation: raw data rows == distinct PKs + collapsed (folded) rows.
    #       If this fails, the collapse dropped/duplicated a row -> apply bug -> block.
    #   (b) watermark monotonicity: this file's max dms_timestamp must be >= the last
    #       committed watermark, else files are being applied OUT OF ORDER -> block.
    # No source query, no target query, no re-read — so it adds no measurable latency.
    if vstats["raw_rows"] != vstats["distinct_pks"] + vstats["collapsed"]:
        raise TableBlocked(
            f"VALIDATION (op reconciliation) {label} file {cdc_key.split('/')[-1]}: "
            f"raw_rows={vstats['raw_rows']} != distinct_pks={vstats['distinct_pks']} + "
            f"collapsed={vstats['collapsed']}. A row was dropped/duplicated during collapse.")
    if file_watermark is not None and prior_watermark is not None \
            and file_watermark < prior_watermark:
        raise TableBlocked(
            f"VALIDATION (watermark monotonicity) {label} file {cdc_key.split('/')[-1]}: "
            f"file watermark {file_watermark} < last committed {prior_watermark} — files "
            f"applied out of order. Check S3 file ordering / DMS timestamp naming.")
    if vstats["skipped_short"]:
        print(f"    ⚠️ {label} {cdc_key.split('/')[-1]}: skipped "
              f"{vstats['skipped_short']} malformed (<2-field) line(s)")

    quoted_cols = ", ".join(f'"{c}"' for c in insert_cols)
    cast_suffix = {c: CAST_SUFFIX.get(col_category.get(c, 'varchar'), '') for c in insert_cols}
    pk_suffix = CAST_SUFFIX.get(col_category.get(pk_col, 'varchar'), '')
    # UPSERT optimization: non-PK columns to overwrite on conflict (updating the PK to
    # itself is disallowed/pointless). If a table is ALL PK columns (no data cols), a
    # conflict has nothing to update -> DO NOTHING is the correct idempotent action.
    _pk_lower = pk_col.lower()
    _update_cols = [c for c in insert_cols if c.lower() != _pk_lower]
    if _update_cols:
        _conflict_clause = (f' ON CONFLICT ("{pk_col}") DO UPDATE SET '
                            + ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in _update_cols))
    else:
        _conflict_clause = f' ON CONFLICT ("{pk_col}") DO NOTHING'

    def build_chunk_sql(chunk_ops, new_offset):
        """Build the full SQL for one chunk, then the cdc_status checkpoint — one txn.

        UPSERT MODEL (v4.x perf): a non-delete net-op (source I or U — DMS gives us the op,
        but the collapsed net image is the same 'row should exist with these values') is
        applied as a SINGLE `INSERT ... ON CONFLICT (pk) DO UPDATE`. This is:
          - 1 row-modification per row (not 2), so an all-upsert chunk packs ~2x more net-ops
            (up to the ~2999-net-op / 3000-mod budget) than the old DELETE+INSERT (~1499).
          - IDEMPOTENT on replay: re-applying a committed chunk after a crash just overwrites
            with identical values (no 23505, no whole-chunk abort — critical since DSQL has
            no savepoints and a chunk mixes many ops). This REPLACES the old delete-first
            'idempotent replace' with a cheaper, equally-safe native upsert.
          - CORRECT for updates too: EXCLUDED.<col> writes the new image for every data col,
            so a source UPDATE lands its full new row exactly as delete-then-insert did.
        D ops remain a plain DELETE (1 mod), unchanged.

        FULL-REPLACE EQUIVALENCE (why this MERGE is not lossy vs the old delete+insert): the
        old model physically deleted the row then re-inserted only insert_cols, so any DSQL
        column NOT in insert_cols was reset to its default. This upsert instead leaves such a
        column at its prior value (partial-column merge). Those two differ ONLY for a DSQL
        column that is absent from insert_cols — but apply_file's _schema_missing check
        HARD-BLOCKS exactly that case (any DSQL column except the PK missing from the CDC
        header fails loud), and IGNORE_COLUMNS (op + the DMS timestamp column, derived from
        the endpoint's TimestampColumnName) are DMS control columns not present in the target. So for a well-formed AddColumnName=true stream insert_cols
        == every real data column, and DO UPDATE SET overwrites all of them -> identical
        result to the old full-row replace, with no reset-to-default surprise."""
        stmts = []
        # DELETEs: only genuine D net-ops now (no more delete-half for upserts).
        delete_ops = [n for n in chunk_ops if n["op"] == "DELETE"]
        for netop in delete_ops:
            pk_lit = sql_literal(netop["pk"], pk_suffix)
            stmts.append((f'DELETE FROM {dsql_schema}.{dsql_table} '
                          f'WHERE "{pk_col}" = {pk_lit}', None))
        # UPSERTs for the non-delete ops: one multi-row INSERT ... ON CONFLICT DO UPDATE.
        insert_ops = [n for n in chunk_ops if n["op"] == "INSERT"]
        if insert_ops:
            groups = []
            for netop in insert_ops:
                guard_row(ctx, netop["values"], col_category)
                vals = [sql_literal(netop["values"].get(c), cast_suffix[c]) for c in insert_cols]
                groups.append("(" + ", ".join(vals) + ")")
            stmts.append((f'INSERT INTO {dsql_schema}.{dsql_table} ({quoted_cols}) '
                          f'VALUES ' + ", ".join(groups) + _conflict_clause, None))
        return stmts

    # BYTE-BUDGET SLICER — designed for the REAL shape of the data: the vast majority of
    # rows are small and only a few OUTLIERS carry big text/JSON. We must NOT pay an
    # O(rows*cols) measuring cost on every chunk to defend against rare outliers (that is
    # the slowness). Strategy:
    #   • Compute one CHEAP per-row size once (sum of value string lengths, len() only — no
    #     per-column .encode()), lazily and MEMOIZED, so each row is measured at most once
    #     across the whole file even if chunks are retried/re-sliced.
    #   • Take the full row_cap by DEFAULT. Only when the running cheap total crosses the
    #     budget do we stop early — so an all-small chunk does one add + one compare per row
    #     (trivial) and returns the full chunk_size. A big outlier is what triggers a
    #     smaller chunk, exactly and only when needed.
    #   • Use a UTF-8 safety multiplier on the cheap char count instead of encoding, so we
    #     stay conservative for multibyte data without the encode cost.
    _row_bytes_cache = {}
    _DELETE_OVERHEAD = len(f'DELETE FROM {dsql_schema}.{dsql_table} WHERE "{pk_col}" = ') + 24
    _UTF8_SAFETY = CDC_CHUNK_BYTE_SAFETY  # bytes/char multiplier on the cheap char count

    def _netop_cheap_bytes(j):
        """Cheap, memoized upper-bound on the SQL bytes net-op j contributes. Uses len()
        (char count) * UTF-8 safety, never .encode(), so it's fast even for big values."""
        b = _row_bytes_cache.get(j)
        if b is not None:
            return b
        netop = netops[j]
        chars = len(str(netop["pk"])) + 16
        if netop["op"] == "INSERT":
            vals = netop["values"]
            for c in insert_cols:
                v = vals.get(c)
                chars += (len(v) if isinstance(v, str) else (len(str(v)) if v is not None else 4)) + 4
        b = _DELETE_OVERHEAD + chars * _UTF8_SAFETY
        _row_bytes_cache[j] = b
        return b

    def _mods(netop):
        """Real DSQL row-modification cost of one net-op (op mix aware)."""
        return CDC_MODS_PER_INSERT if netop["op"] == "INSERT" else CDC_MODS_PER_DELETE

    def _pack_chunk(base_idx, mod_budget, netop_cap):
        """Pack net-ops from base_idx into ONE chunk, stopping at the FIRST limit that binds:
          (1) ROW-MOD budget  — sum of per-op costs + 3 reserved control rows (cdc_status
              checkpoint + cdc_chunk_log audit + final-chunk cdc_file_status marker) must
              stay within mod_budget (which itself stays <= DSQL_MAX_ROWS_PER_TXN). Under the UPSERT model
              every net-op (upsert or delete) costs 1 mod (see CDC_MODS_PER_*), so a chunk
              packs up to ~2999 net-ops regardless of op mix; the per-op cost is read from
              _mods() so this stays correct if the costs ever change.
          (2) BYTE budget     — accumulated inlined-SQL bytes must stay under
              CDC_CHUNK_BYTE_BUDGET (defends the 10 MiB txn/message limit; only the rare
              wide/outlier row makes this bind).
          (3) NET-OP cap      — an optional hard ceiling on the net-op COUNT, used by FIXED
              mode to honor the customer's exact chunk size (netop_cap); in AUTO mode this is
              just n (unbounded, so the mod budget governs).
        FAST PATH: for the common all-small-rows case this is one add + one compare per row.
        Always returns >= 1 op (forward progress even for a single wide/expensive row).
        Deterministic + memoized -> resume-safe, no repeated work across retries.
        Returns (chunk_ops_list, mods_used)."""
        end = min(base_idx + netop_cap, n)
        # Reserve 3 control-row modifications that share the chunk's txn: (1) the cdc_status
        # checkpoint UPDATE, (2) the cdc_chunk_log audit INSERT (every chunk), (3) the
        # cdc_file_status all_rows_committed UPDATE (only on the FINAL chunk, but reserved
        # always so the last chunk never overflows the 3,000 row-mod limit). Cost of the
        # constant reserve is 2 fewer data net-ops per ~2999-op chunk — negligible.
        used_mods = 3
        used_bytes = 0
        taken = 0
        for j in range(base_idx, end):
            m = _mods(netops[j])
            b = _netop_cheap_bytes(j)
            if taken > 0 and (used_mods + m > mod_budget
                              or used_bytes + b > CDC_CHUNK_BYTE_BUDGET):
                break
            used_mods += m
            used_bytes += b
            taken += 1
        return netops[base_idx: base_idx + taken], used_mods

    total_applied = 0
    chunks_committed = 0   # telemetry: number of committed chunks (== commits) for this file
    # We chunk the ORDERED netops. Because netops are keyed by distinct PK, order within a
    # chunk is irrelevant for correctness (disjoint PKs); order ACROSS chunks is preserved
    # by committing chunk N before N+1. start_offset lets us resume mid-file.
    #
    # DSQL 3,000-ROW-PER-TXN LIMIT is on ROW MODIFICATIONS, not net-ops. We PACK each chunk
    # up to a row-mod BUDGET using each op's real cost (upsert=1, delete=1 under the ON
    # CONFLICT model) + 1 checkpoint row — see _pack_chunk. Never exceeds DSQL_MAX_ROWS_PER_TXN.
    _MAX_MOD_BUDGET = DSQL_MAX_ROWS_PER_TXN
    idx = start_offset
    n = len(netops)

    # MASTER OVERRIDE: CDC_FIXED_CHUNK_SIZE > 0 forces a deterministic, non-adaptive NET-OP
    # count per chunk (customer takes control). 0 = AUTO: pack to the full row-mod budget and
    # step the budget DOWN gently only under pressure.
    _fixed_mode = CDC_FIXED_CHUNK_SIZE > 0
    if _fixed_mode:
        # FIXED: honor an exact net-op count. Cap the net-op count at (budget-1)//mods-per-op
        # so a fixed chunk still fits the 3000-mod budget after reserving 1 for the
        # checkpoint. With the upsert model every op is 1 mod, so this is (3000-1)//1 = 2999.
        # Byte budget still applies.
        _fixed_netop_cap = max(1, (DSQL_MAX_ROWS_PER_TXN - 1) // CDC_MODS_PER_INSERT)
        netop_cap = min(CDC_FIXED_CHUNK_SIZE, _fixed_netop_cap)
        if CDC_FIXED_CHUNK_SIZE > _fixed_netop_cap:
            print(f"    ℹ️ {label}: CDC_FIXED_CHUNK_SIZE={CDC_FIXED_CHUNK_SIZE} exceeds the "
                  f"safe per-txn net-op ceiling; clamped to {netop_cap} (protects the "
                  f"3,000 row-mod limit for an all-insert chunk).")
        mod_budget = _MAX_MOD_BUDGET
    else:
        # AUTO: pack to the full row-mod budget; net-op count is unbounded (mod budget +
        # byte budget govern). Under pressure, mod_budget steps DOWN by CDC_CHUNK_STEP_DOWN.
        netop_cap = n if n > 0 else 1
        mod_budget = _MAX_MOD_BUDGET
    # AUTO-mode floor for the stepped-down budget (never below CDC_MIN_CHUNK_SIZE worth of
    # mods; keep it a valid budget >= a single insert + checkpoint).
    _MIN_MOD_BUDGET = max(CDC_MODS_PER_INSERT + 1, min(CDC_MIN_CHUNK_SIZE, _MAX_MOD_BUDGET))

    # PER-FILE LEDGER — 'started' lifecycle marker (Option A), OFF the hot path, BEFORE the
    # first chunk. Its own short txn on the table's connection with the full control-op
    # retry set (never fused to a data chunk). Idempotent on resume: mark_file_started_cur
    # re-UPDATEs an existing row without regressing all_rows_committed/started_time. has_pk
    # reflects whether this table has a targeting key (single-col PK today; the logical-key
    # path will set this for no-PK tables later). file_min/max_ts bound the file's change
    # time span. Failure here is isolated by run_control_op and never blocks the apply.
    _file_min_ts = min_dms_ts(data_rows, header)
    # Ledger has_pk = "this file was applied via the keyed path" (a targeting key exists —
    # real PK or declared logical key). For a Tier-1 logical-key table pk_col-the-DB-PK is
    # None but apply_key is set, and the apply IS keyed, so record True. key_source (pk /
    # logical_key / none) in ctx distinguishes the two if finer detail is needed.
    _has_pk = ctx.get("apply_key") is not None
    def _mark_started(c):
        mark_file_started_cur(c, label, cdc_key, _has_pk, _file_min_ts, file_watermark)
        conn_holder[0].commit()
    run_control_op(conn_holder, label, _mark_started, "mark_file_started")

    while idx < n:
        # Refresh the session BEFORE this chunk if it's approaching the 60-min limit
        # (only ever between chunks, never mid-transaction).
        ensure_fresh_conn(conn_holder, label)
        # PACK by row-mod budget + byte budget (+ net-op cap in FIXED mode). Under the upsert
        # model every op is 1 mod, so a chunk packs up to ~2999 net-ops; wide rows hit the
        # byte budget; a txn never exceeds the 3,000 row-mod or 10 MiB limits.
        chunk_ops, _ = _pack_chunk(idx, mod_budget, netop_cap)
        # MEASURED SIZE BACKSTOP (matches job2 v16): _pack_chunk sizes against a CHEAP
        # char-count estimate (CDC_CHUNK_BYTE_SAFETY=1). A multi-byte-heavy outlier could
        # inflate the REAL built SQL past the hard 10 MiB txn/message limit. Measure the
        # actual bytes of the built statements and, if over, drop the tail of the chunk and
        # retry — the exact wire bytes, no speculation. Never shrinks below 1 net-op (a lone
        # net-op that alone exceeds the limit is a >10 MiB row -> surfaces as a clear DSQL
        # error rather than silent loss). Cheap in the common case: one .encode() over an
        # already-built ~<=10 MiB string, only when a chunk actually has >1 op.
        # Build once, measure, reuse. build_chunk_sql is called ONCE here and the built
        # statements are handed to the execute loop below (no double-build on the hot path).
        # On a shrink we rebuild for the smaller chunk; the LAST (fitting) build is what the
        # execute loop runs. new_offset == idx + len(chunk_ops) is the same argument both the
        # measure and execute paths used before, so the reused statements are identical.
        _built = build_chunk_sql(chunk_ops, idx + len(chunk_ops))
        while len(chunk_ops) > 1:
            _measured = sum(len(_s.encode("utf-8")) for _s, _ in _built)
            if _measured <= DSQL_MAX_TXN_BYTES:
                break
            # PROPORTIONAL shrink (not blind halving): we have the exact measured bytes for
            # len(chunk_ops) net-ops, so bytes/op = _measured/len. Aim at 97% of the budget
            # to land in ONE step; clamp to [1, len-1] for guaranteed progress. The loop
            # re-measures and only iterates again if the kept slice is denser than average.
            _bpo = _measured / len(chunk_ops)
            _target = int((DSQL_MAX_TXN_BYTES * BYTE_TARGET_FRACTION) / max(_bpo, 1))
            _new_n = max(1, min(_target, len(chunk_ops) - 1))
            print(f"    ✂ [{label}] measured chunk SQL {_measured:,} B > "
                  f"{DSQL_MAX_TXN_BYTES:,} B hard limit; shrinking {len(chunk_ops)} -> "
                  f"{_new_n} net-ops (proportional) and re-slicing", flush=True)
            chunk_ops = chunk_ops[:_new_n]
            _built = build_chunk_sql(chunk_ops, idx + len(chunk_ops))
        new_offset = idx + len(chunk_ops)
        _built_stmts = _built   # reused by the execute loop's FIRST attempt (avoids rebuild)
        conn = conn_holder[0]
        cur = conn.cursor()
        pipe_attempt = occ_attempt = server_attempt = 0
        while True:
            t0 = time.monotonic()
            try:
                # Use the statements already built by the backstop for the first attempt;
                # rebuild only after a re-pack (txn-timeout step-down) changed chunk_ops.
                if _built_stmts is None:
                    _built_stmts = build_chunk_sql(chunk_ops, new_offset)
                for stmt, _ in _built_stmts:
                    cur.execute(stmt)
                # SAME-TXN CHECKPOINT: advance in-progress file + offset + watermark.
                # Pure UPDATE (row pre-created by ensure_status_row) -> 1 stmt, no 23505.
                update_cdc_status(
                    cur, label,
                    status="active",
                    in_progress_file=cdc_key,
                    last_offset=new_offset,
                    watermark_ts=file_watermark,
                    rows_applied=(idx + len(chunk_ops)),
                )
                # SAME-TXN PER-CHUNK AUDIT: one durable cdc_chunk_log row per committed
                # chunk (Option-B granularity), atomic with this chunk's data + checkpoint.
                # chunk_seq is 1-based: chunks_committed counts chunks committed BEFORE this
                # one. Reserved in the _pack_chunk mod budget (control row #2).
                insert_chunk_log_cur(
                    cur, label, cdc_key,
                    chunk_seq=chunks_committed + 1,
                    start_offset=idx,
                    end_offset=new_offset,
                    rows=len(chunk_ops),
                    watermark_ts=file_watermark,
                )
                # SAME-TXN GRANULAR FILE-COMMITTED MARKER (Option B): if this chunk finishes
                # the file (new_offset == n, all net-ops applied), stamp cdc_file_status
                # all_rows_committed=true in THIS txn — atomic with the file's last rows, so
                # the truth marker can never lie. The lifecycle 'done' marker is written
                # separately by the following high-water txn (mark_file_done_cur). Reserved
                # in the _pack_chunk mod budget (control row #3, final chunk only).
                if new_offset >= n:
                    mark_file_committed_cur(
                        cur, label, cdc_key,
                        rows_applied=total_applied + len(chunk_ops),
                        chunks_committed=chunks_committed + 1,
                        watermark_ts=file_watermark,
                    )
                conn.commit()
                break
            except TableBlocked:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                # SCHEMA-CHANGE OCC (concurrent DDL committed on another connection): a
                # plain retry would re-issue the SAME frozen SQL with the SAME stale column
                # list and never succeed. The rolled-back chunk committed nothing, and the
                # checkpoint still points at the last GOOD offset (idx). Stop this file
                # cleanly and let the next process_table cycle re-run apply_file, which
                # re-reconciles the schema at the top and rebuilds the column set. Fully
                # deterministic: resume re-collapses the file and slices from the committed
                # offset — no missed/duplicate rows. Checked BEFORE is_occ_conflict because
                # the schema message also matches the generic OCC classifier.
                if is_schema_conflict(e):
                    raise _SchemaChangedMidFile(
                        f"{label}: schema changed under file "
                        f"{cdc_key.split('/')[-1]} at offset {idx}; will re-reconcile and "
                        f"resume next cycle.")
                if is_occ_conflict(e) and occ_attempt < OCC_MAX_RETRIES:
                    occ_attempt += 1
                    time.sleep(occ_backoff_seconds(occ_attempt))
                    continue
                # AUTO mode: a txn-timeout steps the row-mod budget DOWN by a gentle fixed
                # amount (CDC_CHUNK_STEP_DOWN — not halving) and re-packs a smaller chunk.
                # FIXED mode: the customer pinned the size, so we do NOT silently shrink it;
                # a timeout at the fixed size falls through to a loud TableBlocked (honor the
                # value or surface the failure — never override it quietly).
                if not _fixed_mode and is_txn_timeout(e) and mod_budget > _MIN_MOD_BUDGET:
                    _record_txn_age_failure()   # feeds the aggressive->safe trigger fallback
                    # PROPORTIONAL (throughput-based, not a fixed 500 step): the txn ran from
                    # t0 until it hit the age limit, so `_failed_elapsed` is the real time
                    # these mods took. mods/sec ~= chunk_mods/_failed_elapsed; target the mod
                    # budget that lands at ~85% of the trigger (extra margin since it already
                    # timed out): chunk_mods*(0.85*trigger/_failed_elapsed). Floored at
                    # _MIN_MOD_BUDGET, forced strictly smaller so we always make progress.
                    _failed_elapsed = max(time.monotonic() - t0, 0.001)
                    _cur_mods = 1 + sum(_mods(o) for o in chunk_ops)
                    _trigger = effective_batch_max_seconds()
                    _target = int(_cur_mods * (TIME_TARGET_FRACTION_AFTER_FAIL * _trigger) / _failed_elapsed)
                    mod_budget = max(_MIN_MOD_BUDGET, min(_target, mod_budget - 1))
                    chunk_ops, _ = _pack_chunk(idx, mod_budget, netop_cap)
                    new_offset = idx + len(chunk_ops)
                    _built_stmts = None   # chunk changed -> rebuild for the new smaller chunk
                    pipe_attempt = occ_attempt = server_attempt = 0
                    continue
                if is_transient_server_error(e) and server_attempt < SERVER_MAX_RETRIES:
                    server_attempt += 1
                    time.sleep(server_backoff_seconds(server_attempt))
                    continue
                if is_broken_pipe_error(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                    pipe_attempt += 1
                    time.sleep(CHUNK_RETRY_BACKOFF_SECONDS * pipe_attempt)
                    try:
                        conn_holder[0].close()
                    except Exception:
                        pass
                    _invalidate_dsql_token()   # reconnect with a FRESH token (heals 08006)
                    conn_holder[0] = connect_dsql(autocommit=False)
                    conn_holder[1] = time.monotonic()   # reset age on the shared holder
                    conn = conn_holder[0]
                    cur = conn.cursor()
                    continue
                # Genuine, non-retriable error -> block the table (caller logs + isolates).
                cur.close()
                raise TableBlocked(
                    f"apply failed for {label} at file {cdc_key.split('/')[-1]} "
                    f"offset {idx}: {e}")
        cur.close()
        elapsed = time.monotonic() - t0
        # Adaptive step-down on a slow chunk (AUTO only), by the gentle fixed CDC_CHUNK_STEP
        # _DOWN — NOT halving. Gated on the chunk having used most of its row-mod budget
        # (chunk_mods >= mod_budget - a small INSERT's worth): if the chunk was short because
        # the BYTE budget bound (a big outlier row), the slowness came from DATA VOLUME, not
        # too many rows — stepping the row-mod budget down wouldn't help and would needlessly
        # throttle the many normal chunks that follow. This stops one outlier from
        # permanently crashing throughput ("gets conservative and performance crashes").
        # NOTE: we NEVER abort a chunk at the trigger. This runs AFTER conn.commit()
        # succeeded (the chunk is committed). A chunk runs to DSQL's real 300s hard limit and
        # often finishes before then; the 270s trigger only adjusts the NEXT chunk's mod
        # budget (below) — a post-commit tuning signal, not a client-side timeout/cutoff.
        chunk_mods = 1 + sum(_mods(o) for o in chunk_ops)
        _trigger = effective_batch_max_seconds()
        if not _fixed_mode and elapsed > _trigger \
                and chunk_mods >= (mod_budget - CDC_MODS_PER_INSERT) \
                and mod_budget > _MIN_MOD_BUDGET:
            # PROPORTIONAL (throughput-based, not a fixed 500 step): this chunk committed
            # `chunk_mods` mods in `elapsed`s, so mods/sec ~= chunk_mods/elapsed. Target the
            # mod budget that lands at ~90% of the trigger: chunk_mods*(0.9*trigger/elapsed).
            # Floored at _MIN_MOD_BUDGET, forced strictly smaller for progress. This adapts to
            # HOW slow the chunk actually was instead of always stepping a flat 500.
            _target = int(chunk_mods * (TIME_TARGET_FRACTION * _trigger) / max(elapsed, 0.001))
            mod_budget = max(_MIN_MOD_BUDGET, min(_target, mod_budget - 1))
        total_applied += len(chunk_ops)
        chunks_committed += 1
        idx = new_offset

    # ZERO-NET-OP FILE: a file whose rows all cancelled out (e.g. insert-then-delete of the
    # same PK within the file) has n == 0, so the chunk loop never ran and the granular
    # all_rows_committed marker was never set inside a chunk txn. The file is still
    # legitimately fully applied (there was nothing to apply), and process_table will mark it
    # 'done'. Set all_rows_committed=true here (own short txn, control-op retry) so the
    # ledger's truth marker stays consistent with the lifecycle marker — never a 'done' row
    # that shows committed=false. Only needed when n == 0; a non-empty file's final chunk
    # already set this atomically with its data.
    if n == 0:
        def _mark_committed_empty(c):
            mark_file_committed_cur(c, label, cdc_key, rows_applied=0,
                                    chunks_committed=0, watermark_ts=file_watermark)
            conn_holder[0].commit()
        run_control_op(conn_holder, label, _mark_committed_empty, "mark_file_committed_empty")

    # TIER-2 (deferred, sampled) validation — runs ONLY after the whole file committed,
    # on its own connection, and is a no-op unless VALIDATION_ENABLED. It reports
    # discrepancies to cdc_validation_failures; it does NOT block the apply (already done).
    # Guarded so a validation error can never fail a successful apply.
    if VALIDATION_ENABLED:
        try:
            validate_file_netops(ctx, cdc_key, netops, col_category)
        except Exception as _ve:
            print(f"    ⚠️ validation pass errored (non-fatal): {_ve}")

    return total_applied, file_watermark, chunks_committed


# =============================================================================
# TIER-2 (KEYLESS) APPLY — insert/delete only; updates skipped+logged; no chunking
# =============================================================================
def collapse_net_ops_nonpk(rows, header, ctx, insert_cols, col_category):
    """Keyless collapse for Tier-2 tables (no PK, no logical key). There is NO targeting key,
    so a row's IDENTITY is its FULL-ROW CONTENT (valid under the operator's 'no duplicate
    rows' contract for these tables). Per the finalized Tier-2 model:
      • I (insert) -> an INSERT net-op carrying the row's full image.
      • D (delete) -> a DELETE net-op carrying the row's full image; apply_file_nonpk turns it
                      into `DELETE ... WHERE <every content col> = <value>` (exactly one row
                      under the no-dup contract).
      • U (update) -> SKIPPED (not applied). A keyless UPDATE cannot target the prior row from
                      an after-image-only S3 record. Collected in `skipped` for logging to
                      cdc_skipped_ops; the table is NOT blocked and keeps flowing.
    Unlike the keyed collapse there is NO per-key folding (no key to fold on) — each I and D
    is emitted in file order. Returns (netops, skipped, stats):
      netops  : [{op:'INSERT'|'DELETE', values:{col:converted}, dms_ts:<str|None>}]
      skipped : [{op:'U', values:{...}, dms_ts, change_seq}] — updates we did not apply
      stats   : counters for logging/validation."""
    op_idx = header.index(OP_COLUMN) if OP_COLUMN in header else 0
    ts_idx = header.index(DMS_TIMESTAMP_COLUMN) if DMS_TIMESTAMP_COLUMN in header else None
    col_idx = {c: header.index(c.lower()) for c in insert_cols}

    def _row_values(row):
        vals = {}
        for c, idx in col_idx.items():
            raw = row[idx] if idx < len(row) else None
            vals[c] = convert_value(raw, col_category.get(c, 'varchar'))
        return vals

    netops = []
    skipped = []
    raw_rows = 0
    skipped_short = 0
    n_insert = n_delete = n_skip = 0
    for row in rows:
        if len(row) < 2:
            skipped_short += 1
            continue
        raw_rows += 1
        op = (row[op_idx].upper() if op_idx < len(row) else 'I')
        dms_ts = row[ts_idx] if (ts_idx is not None and ts_idx < len(row)) else None
        if op == 'D':
            netops.append({"op": "DELETE", "values": _row_values(row), "dms_ts": dms_ts})
            n_delete += 1
        elif op == 'U':
            # KEYLESS UPDATE -> skip + log. Never applied, never blocks (Tier-2 model).
            skipped.append({"op": "U", "values": _row_values(row), "dms_ts": dms_ts})
            n_skip += 1
        else:  # I (or anything non-D/U treated as insert of the current image)
            netops.append({"op": "INSERT", "values": _row_values(row), "dms_ts": dms_ts})
            n_insert += 1
    stats = {
        "raw_rows": raw_rows,
        "skipped_short": skipped_short,
        "n_insert": n_insert,
        "n_delete": n_delete,
        "n_skipped_update": n_skip,
    }
    return netops, skipped, stats


def apply_file_nonpk(ctx, cdc_key, start_offset, conn_holder, prior_watermark=None):
    """TIER-2 keyless apply for ONE CDC file. INSERT + DELETE only (full-row content match);
    UPDATEs are skipped + logged (cdc_skipped_ops) and never block. Signature + return shape
    MATCH apply_file — (total_applied, file_watermark, chunks_committed) — so process_table's
    file loop, ledger markers, and checkpoint handling are identical for both tiers.

    IDEMPOTENT FILE RELOAD (no PK, so we can't upsert): every inserted row is tagged with the
    Glue-managed target-only column _cdc_file = this file's key. Re-applying a file first does
    `DELETE FROM target WHERE _cdc_file = <this file>` to purge any partial prior apply of THIS
    file, then re-inserts the file's rows. So a crash/replay of a file is exactly idempotent
    for the INSERTs it owns. (DELETEs from the file are content-matched and naturally
    idempotent — deleting an already-absent row is a no-op.)

    WITHIN-FILE CHUNKING (never across files). The file's net ops are packed into chunks by
    the DSQL per-txn budget — at most (3,000 - reserve) row-mods per chunk and under the byte
    budget — and each chunk commits in its own transaction, applied IN ORDER. A small file is
    ONE chunk (the whole file); only a large file splits. The file-scoped purge (the _cdc_file
    reload) runs ONCE, fused into the FIRST chunk's txn; the cdc_status checkpoint + ledger
    markers commit with the file's FINAL chunk. This keeps a large keyless file within the
    DSQL 3,000-row-mod / 10 MiB per-txn limits instead of relying on files being small.

    start_offset is accepted for signature-compatibility but Tier-2 always re-applies the
    whole file (the _cdc_file reload makes that idempotent), so mid-file offset resume is not
    used here; the checkpoint records offset = row count for observability."""
    label = ctx["label"]
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]
    type_categories = ctx["type_categories"]

    header, data_rows = read_cdc_file(cdc_key)
    if not header or not data_rows:
        return 0, None, 0

    schema = handle_schema_changes(ctx, header, data_rows)

    # Ensure the two Glue-managed tag columns exist on the target (idempotent). They are
    # target-only (never in the CDC header), so handle_schema_changes won't add them.
    _ensure_nonpk_tag_columns(conn_holder, label, dsql_schema, dsql_table, schema)

    # Column set = live DSQL schema ∩ CDC header, minus DMS control cols AND minus our own
    # tag columns (so a re-read of the target's tag columns is never treated as source data).
    header_set = {c.lower() for c in header if c not in IGNORE_COLUMNS}
    _tag_lower = {NONPK_FILE_TAG_COLUMN.lower()}
    schema_lower = {c.lower(): c for c in schema if c.lower() not in _tag_lower}
    insert_cols = [schema_lower[lc] for lc in schema_lower if lc in header_set]
    if not insert_cols:
        raise TableBlocked(f"no columns to write for {label} (schema ∩ header is empty)")
    # A DSQL data column missing from the header would silently NULL it — block (same rule as
    # the keyed path; the tag columns are excluded from this check since they're target-only).
    _schema_missing = [schema_lower[lc] for lc in schema_lower if lc not in header_set]
    if _schema_missing:
        raise TableBlocked(
            f"CDC file {cdc_key.split('/')[-1]} for {label} (keyless) is missing column(s) "
            f"{_schema_missing} present in the DSQL target. Every column must be sent on each "
            f"change; a missing column would corrupt the full-row content match. Fix the DMS "
            f"mapping/header, then clear cdc_status to resume.")

    def _cat(col_name):
        c = type_categories.get(col_name)
        if c:
            return c
        return dsql_type_to_category((schema.get(col_name) or {}).get('type'))
    col_category = {c: _cat(c) for c in insert_cols}

    netops, skipped, vstats = collapse_net_ops_nonpk(data_rows, header, ctx, insert_cols,
                                                     col_category)
    file_watermark = max_dms_ts(data_rows, header)

    # Watermark monotonicity (same guard as the keyed path): files must apply in order.
    if file_watermark is not None and prior_watermark is not None \
            and file_watermark < prior_watermark:
        raise TableBlocked(
            f"VALIDATION (watermark monotonicity) {label} file {cdc_key.split('/')[-1]}: "
            f"file watermark {file_watermark} < last committed {prior_watermark} — keyless "
            f"files applied out of order. Check S3 file ordering / DMS timestamp naming.")
    if vstats["skipped_short"]:
        print(f"    ⚠️ {label} {cdc_key.split('/')[-1]}: skipped "
              f"{vstats['skipped_short']} malformed (<2-field) line(s)")

    cast_suffix = {c: CAST_SUFFIX.get(col_category.get(c, 'varchar'), '') for c in insert_cols}
    file_lit = sql_literal(cdc_key, '')

    def _content_where(values):
        """Full-row content match predicate. Under the no-duplicate-rows contract this
        identifies exactly one target row. NULLs use `IS NULL` (=' NULL' never matches)."""
        parts = []
        for c in insert_cols:
            v = values.get(c)
            if v is None:
                parts.append(f'"{c}" IS NULL')
            else:
                parts.append(f'"{c}" = {sql_literal(v, cast_suffix[c])}')
        return " AND ".join(parts) if parts else "TRUE"

    # Precompute each op's SQL statement + its cheap byte estimate ONCE. A keyless op is
    # exactly 1 row-modification (a content DELETE or a single-row INSERT), so the DSQL
    # 3,000-row-mod cap governs the per-chunk COUNT and the ~10 MiB cap governs the BYTES.
    delete_ops = [nop for nop in netops if nop["op"] == "DELETE"]
    insert_ops = [nop for nop in netops if nop["op"] == "INSERT"]
    all_cols = insert_cols + [NONPK_FILE_TAG_COLUMN]
    quoted_all = ", ".join(f'"{c}"' for c in all_cols)
    _ins_prefix = f'INSERT INTO {dsql_schema}.{dsql_table} ({quoted_all}) VALUES '

    op_units = []   # each = (sql_text, est_bytes); DELETEs first then INSERTs (file order-safe)
    for nop in delete_ops:
        guard_row(ctx, nop["values"], col_category)
        s = f'DELETE FROM {dsql_schema}.{dsql_table} WHERE {_content_where(nop["values"])}'
        op_units.append((s, len(s)))
    for nop in insert_ops:
        guard_row(ctx, nop["values"], col_category)
        vals = [sql_literal(nop["values"].get(c), cast_suffix[c]) for c in insert_cols]
        vals.append(file_lit)                                       # _cdc_file (only tag col)
        row_sql = "(" + ", ".join(vals) + ")"
        # Each insert becomes its OWN single-row INSERT statement (1 row-mod). Simpler than
        # multi-row grouping and lets the chunk packer count/size ops uniformly; DSQL applies
        # a batch of single-row INSERTs in one txn exactly as it would a multi-row one.
        s = _ins_prefix + row_sql
        op_units.append((s, len(s)))

    n_rows = len(op_units)
    purge_sql = (f'DELETE FROM {dsql_schema}.{dsql_table} '
                 f'WHERE "{NONPK_FILE_TAG_COLUMN}" = {file_lit}')

    # WITHIN-FILE CHUNKING (never across files). Pack op_units into chunks by the DSQL
    # per-txn budget: at most (3000 - reserve) ops per chunk (reserve 3 control rows:
    # cdc_status checkpoint + cdc_chunk_log + final-chunk file-committed), and keep the chunk
    # under CDC_CHUNK_BYTE_BUDGET. A small file -> ONE chunk (the whole file). Only a large
    # file splits into multiple chunks, applied IN ORDER. The purge (file-scoped reload) runs
    # ONCE, fused into the FIRST chunk's txn. RESUME = whole-file restart (re-purge + replay);
    # every insert is tagged _cdc_file so re-applying the whole file is idempotent — so we
    # ignore start_offset for the keyless path (mid-file resume isn't needed; the reload makes
    # a full replay safe and correct).
    _reserve = 3
    _max_ops = max(1, DSQL_MAX_ROWS_PER_TXN - _reserve)
    # Byte budget for a keyless chunk. The estimate is a cheap char count (len()); to stay
    # safe against multi-byte data WITHOUT a per-op .encode() measured backstop, divide the
    # ~9.8 MiB packing target by the worst-case UTF-8 bytes/char (4). This is conservative
    # (most data is 1 byte/char), which only means a large file splits into a few more small
    # chunks — a negligible cost, and it cannot exceed the 10 MiB txn limit. Each op is also
    # executed as its OWN statement, so the 10 MiB per-message wire limit is never at risk.
    _nonpk_byte_budget = max(64 * 1024, CDC_CHUNK_BYTE_BUDGET // 4)
    chunks = []          # list of (list_of_sql, is_first)
    i = 0
    while i < n_rows:
        cur_sql = []
        cur_bytes = 0
        while i < n_rows and len(cur_sql) < _max_ops:
            s, b = op_units[i]
            if cur_sql and cur_bytes + b > _nonpk_byte_budget:
                break                       # byte budget binds -> close this chunk
            cur_sql.append(s)
            cur_bytes += b
            i += 1
        chunks.append(cur_sql)
    if not chunks:
        chunks = [[]]    # a file with only skipped-U (no I/D): still one chunk to purge+log

    ensure_fresh_conn(conn_holder, label)
    conn = conn_holder[0]
    cur = conn.cursor()
    total_committed = 0
    n_chunks = len(chunks)
    for ci, chunk_sql in enumerate(chunks):
        is_first = (ci == 0)
        is_last = (ci == n_chunks - 1)
        occ_attempt = server_attempt = pipe_attempt = 0
        while True:
            try:
                if is_first:
                    cur.execute(purge_sql)   # file-scoped reload — ONCE, in the first chunk
                for _s in chunk_sql:
                    cur.execute(_s)
                if is_last:
                    # SKIPPED UPDATE LOG — same txn as the file's final chunk, so the log is
                    # committed atomically with the completed apply.
                    for sk in skipped:
                        insert_skipped_op_cur(
                            cur, label, cdc_key, sk.get("dms_ts"), None, sk["op"],
                            json.dumps(sk["values"], default=str))
                # SAME-TXN CHECKPOINT: running row count applied so far (observability; resume
                # for keyless is whole-file restart, so offset is informational, not seeked).
                _applied_so_far = total_committed + len(chunk_sql)
                update_cdc_status(
                    cur, label, status="active", in_progress_file=cdc_key,
                    last_offset=_applied_so_far, watermark_ts=file_watermark,
                    rows_applied=_applied_so_far)
                insert_chunk_log_cur(
                    cur, label, cdc_key, chunk_seq=ci + 1, start_offset=total_committed,
                    end_offset=_applied_so_far, rows=len(chunk_sql),
                    watermark_ts=file_watermark)
                if is_last:
                    mark_file_committed_cur(
                        cur, label, cdc_key, rows_applied=n_rows,
                        chunks_committed=n_chunks, watermark_ts=file_watermark)
                conn.commit()
                break
            except TableBlocked:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if is_schema_conflict(e):
                    raise _SchemaChangedMidFile(
                        f"{label}: schema changed under keyless file "
                        f"{cdc_key.split('/')[-1]}; will re-reconcile and resume next cycle.")
                if is_occ_conflict(e) and occ_attempt < OCC_MAX_RETRIES:
                    occ_attempt += 1
                    time.sleep(occ_backoff_seconds(occ_attempt))
                    continue
                if is_transient_server_error(e) and server_attempt < SERVER_MAX_RETRIES:
                    server_attempt += 1
                    time.sleep(server_backoff_seconds(server_attempt))
                    continue
                if is_broken_pipe_error(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                    pipe_attempt += 1
                    time.sleep(CHUNK_RETRY_BACKOFF_SECONDS * pipe_attempt)
                    try:
                        conn_holder[0].close()
                    except Exception:
                        pass
                    _invalidate_dsql_token()   # reconnect with a FRESH token (heals 08006)
                    conn_holder[0] = connect_dsql(autocommit=False)
                    conn_holder[1] = time.monotonic()
                    conn = conn_holder[0]
                    cur = conn.cursor()
                    continue
                cur.close()
                raise TableBlocked(
                    f"keyless apply failed for {label} at file "
                    f"{cdc_key.split('/')[-1]} chunk {ci + 1}/{n_chunks}: {e}")
        total_committed += len(chunk_sql)
        # Recycle-check between chunks (never mid-txn), same as the keyed path.
        ensure_fresh_conn(conn_holder, label)
        conn = conn_holder[0]
    cur.close()

    if vstats["n_skipped_update"]:
        print(f"    ⏭  {label} {cdc_key.split('/')[-1]}: SKIPPED "
              f"{vstats['n_skipped_update']} UPDATE(s) (keyless insert/delete-only table; "
              f"logged in {CONTROL_SCHEMA}.cdc_skipped_ops).")
        emit_skipped_update_metric(label, vstats["n_skipped_update"])

    # chunks_committed = number of within-file chunks committed (1 for a small file).
    return n_rows, file_watermark, n_chunks


def _ensure_nonpk_tag_columns(conn_holder, label, dsql_schema, dsql_table, schema):
    """Add the Glue-managed target-only tag column (_cdc_file) to a keyless target if absent.
    Idempotent (ADD COLUMN ... ; 'already exists' tolerated). Runs on the table's own
    connection as its own short txn via run_control_op (full retry set). schema is the live
    DSQL schema dict (lowercased keys checked) so we skip the ALTER when it already exists —
    the common steady-state case, so this is a cheap no-op after the first file."""
    have = {c.lower() for c in schema}
    need = []
    if NONPK_FILE_TAG_COLUMN.lower() not in have:
        need.append((NONPK_FILE_TAG_COLUMN, 'varchar(1024)'))
    if not need:
        return
    def _do(c):
        for col, typ in need:
            try:
                c.execute(f'ALTER TABLE {dsql_schema}.{dsql_table} ADD COLUMN "{col}" {typ}')
            except Exception as e:
                if 'already exists' not in str(e).lower():
                    raise
        conn_holder[0].commit()
    run_control_op(conn_holder, label, _do, "ensure_nonpk_tag_columns")
    # Refresh the schema cache so subsequent files see the new columns without a re-query.
    _schema_cache.pop(label, None)


def run_control_op(conn_holder, label, fn, what):
    """Run a CONTROL-TABLE operation (resume-read, ensure_status_row, _commit_status) with
    the SAME resilience the data-chunk loop already has. These run on the persistent
    per-table connection and, per the DSQL docs, are actually the statements MOST likely to
    hit a transient error: the first interaction on a session that has been idle between
    poll cycles can get OC001 (stale catalog after a concurrent DDL), and multi-Region
    recovery can surface transient concurrency/connection errors on any statement.

    Retries the FULL error set (not just OCC):
      - OCC / OC001 (40001)              -> the session refreshes its catalog cache on
                                            retry; the op typically succeeds (DSQL docs).
      - transient server-unavailable      -> backoff + retry (server recovery windows).
      - broken pipe / closed connection    -> reconnect the shared holder, then retry.
      - txn-timeout (5-min age limit)      -> retry on a fresh, short transaction (control
                                            ops are tiny, so a timeout means a stall, not
                                            size; a clean retry clears it).

    fn takes the live cursor and does its own conn.commit(). On success returns True. If a
    class's retry budget is exhausted the LAST error is raised (caller's table-level guard
    turns it into an isolated 'error' for THIS table only — never crashes the cycle)."""
    occ = server = pipe = timeout = 0
    while True:
        ensure_fresh_conn(conn_holder, label)
        conn = conn_holder[0]
        cur = conn.cursor()
        try:
            fn(cur)
            return True
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            if is_occ_conflict(e) and occ < OCC_MAX_RETRIES:
                occ += 1
                time.sleep(occ_backoff_seconds(occ))
                continue
            if is_transient_server_error(e) and server < SERVER_MAX_RETRIES:
                server += 1
                time.sleep(server_backoff_seconds(server))
                continue
            if is_txn_timeout(e) and timeout < OCC_MAX_RETRIES:
                timeout += 1
                time.sleep(occ_backoff_seconds(timeout))
                continue
            if is_unique_violation(e) and occ < OCC_MAX_RETRIES:
                # Only reachable from ensure_status_row_cur when a concurrent creator won
                # the SELECT->INSERT race: the row now EXISTS, so a retry's SELECT short-
                # circuits to success. Bounded by the OCC budget.
                occ += 1
                time.sleep(occ_backoff_seconds(occ))
                continue
            if is_broken_pipe_error(e) and pipe < MAX_CHUNK_RETRIES:
                pipe += 1
                time.sleep(CHUNK_RETRY_BACKOFF_SECONDS * pipe)
                try:
                    conn_holder[0].close()
                except Exception:
                    pass
                _invalidate_dsql_token()   # reconnect with a FRESH token (heals 08006)
                conn_holder[0] = connect_dsql(autocommit=False)
                conn_holder[1] = time.monotonic()
                continue
            # Exhausted / non-retriable -> surface to the table-level guard (isolates
            # THIS table; other tables keep flowing).
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass


def process_table(ctx, load_status_map=None):
    """Process ALL pending CDC files for ONE table, serially, with resume. Never raises —
    a blocked table is recorded and isolated so other tables keep flowing. Returns a
    summary dict.

    load_status_map: {label: status} from _load_status.json (read once per cycle by the
    caller). When REQUIRE_FULL_LOAD_DONE is on, a table is processed ONLY if its full load
    is 'done'; otherwise it is SKIPPED this cycle (retried next poll) so CDC never applies
    a delta on top of a half-loaded table."""
    label = ctx["label"]

    # FULL-LOAD ELIGIBILITY GATE (cheap early-exit, before opening any DSQL connection):
    # skip a table whose full load isn't 'done' yet. This auto-sequences full-load -> CDC:
    # the moment the load job marks the table done, the next poll picks it up.
    if REQUIRE_FULL_LOAD_DONE:
        st = (load_status_map or {}).get(label)
        if st != "done":
            print(f"  ⏭  {label}: full load not done (load-status={st!r}) — "
                  f"waiting; will retry next poll.")
            return {"table": label, "status": "waiting_full_load", "files": 0, "rows": 0}

    # ONE SESSION PER TABLE: a single long-lived DSQL connection is created here and reused
    # for EVERYTHING this table does this cycle — the resume-position read, every chunk
    # apply + same-txn checkpoint, the per-file done marker, and the idle marker. This is
    # both the "each table has its own session to target" guarantee AND the biggest latency
    # win: it removes the ~6 throwaway connections (each an IAM-token gen + TLS handshake)
    # the previous version opened per cycle.
    #
    # 60-MIN CONNECTION LIMIT: DSQL force-closes a connection at ~60 min. The age is
    # tracked HERE (conn_holder carries "started") and enforced by ensure_fresh_conn()
    # BEFORE EVERY use — the resume read, each chunk (via apply_file), each _commit_status,
    # and the idle marker — so no use of the persistent session can ever hit an expired
    # connection, even during a long backlog catch-up. Recycle threshold is 54 min (leaves a
    # full 5-min max chunk + slop under 60). conn_holder = [conn, started_monotonic].
    #
    # The initial connect itself can fail transiently (IAM token gen, TLS, throttling, or
    # a multi-Region recovery window). Isolate that to THIS table too — never let it crash
    # the cycle. conn_holder stays [None, 0] on failure so the finally: close is safe.
    conn_holder = [None, 0.0]
    # REACTIVE SELF-HEAL: the initial connect is retried with bounded backoff, and on any
    # connection-class failure (08006 unable-to-connect, dropped/closed pipe, TLS drop) the
    # cached IAM token is INVALIDATED so each retry mints a FRESH token. Previously this was
    # a single attempt that reused the cached token — so once the token/endpoint state went
    # bad, every table failed 08006 identically and the apply silently stalled for hours.
    # Now a transient blip self-heals within this call; a persistent outage still isolates
    # THIS table (returns "error") and the NEXT poll retries with a fresh token.
    conn_holder = [None, 0.0]
    _last_err = None
    for _attempt in range(1, CONNECT_MAX_RETRIES + 1):
        try:
            conn_holder = [connect_dsql(autocommit=False), time.monotonic()]
            _last_err = None
            break
        except Exception as e:
            _last_err = e
            if is_broken_pipe_error(e) or is_transient_server_error(e):
                _invalidate_dsql_token()   # next connect gets a fresh token
            if _attempt < CONNECT_MAX_RETRIES:
                _bo = server_backoff_seconds(_attempt)
                print(f"    ↻ {label}: DSQL connect attempt {_attempt}/{CONNECT_MAX_RETRIES} "
                      f"failed ({e}); fresh-token retry in {_bo:.1f}s")
                time.sleep(_bo)
    if _last_err is not None:
        print(f"    ⚠️ {label}: could not open DSQL session after {CONNECT_MAX_RETRIES} "
              f"attempts (isolated, retry next poll): {_last_err}")
        return {"table": label, "status": "error", "files": 0, "rows": 0, "error": str(_last_err)}

    def _commit_status(_done_file=None, **fields):
        """Write a cdc_status marker on THE TABLE'S OWN connection as its own short txn
        (outside a chunk apply), with the full control-op retry set (OCC/OC001, transient
        server-unavailable, txn-timeout, broken-pipe reconnect).

        _done_file (optional): when set, the SAME short txn also stamps the cdc_file_status
        ledger row for that file status='done' + done_time (Option-A lifecycle marker). This
        is fused to the high-water advance (last_done_file) so 'done' and the high-water move
        commit together — the file is marked done exactly when it's retired from the pending
        scan. The granular all_rows_committed=true was already set atomically with the file's
        final data chunk (Option B), so a crash between the two leaves an observable, correct
        state (committed=true, status='started')."""
        def _do(c):
            upsert_cdc_status(c, label, **fields)
            if _done_file is not None:
                mark_file_done_cur(c, label, _done_file)
            conn_holder[0].commit()
        run_control_op(conn_holder, label, _do, "commit_status")

    applied_rows = 0
    files_done = 0
    chunks_done = 0   # telemetry: total committed chunks (commits) across this table's files
    try:
        # Read resume position on the SAME connection (no separate handshake). This is the
        # FIRST statement on a session that was idle between poll cycles -> per the DSQL
        # docs it is the single most likely place to hit OC001 (stale catalog after a
        # concurrent DDL). Route it through run_control_op so that (and transient server /
        # broken-pipe / txn-timeout) are retried instead of crashing the table.
        _state_box = {}
        def _read_state(c):
            _state_box["state"] = load_cdc_status(c, label)
            conn_holder[0].commit()   # end the read txn cleanly
        run_control_op(conn_holder, label, _read_state, "read_resume_state")
        state = _state_box["state"]

        if state["status"] == "blocked":
            print(f"  ⛔ {label} is BLOCKED (prior bad row) — skipping. Clear cdc_status to resume.")
            return {"table": label, "status": "blocked", "files": 0, "rows": 0}

        files = list_cdc_files(ctx)
        last_done = state["last_done_file"]
        in_progress = state["in_progress_file"]
        resume_offset = state["last_offset"] if in_progress else 0

        # HIGH-WATER SKIP: drop files already fully applied (<= last_done_file). Files sort
        # by DMS timestamp filename and complete strictly in order (serial), so this is safe.
        pending = [key for key in files if not (last_done and key <= last_done)]
        if not pending:
            return {"table": label, "status": "idle", "files": 0, "rows": 0}

        # Guarantee the cdc_status row EXISTS before any chunk runs, so the per-chunk
        # checkpoint (update_cdc_status) is a pure UPDATE on the hot path — no SELECT,
        # no INSERT, no 23505 risk fused to a data transaction. One-time, own txn, with the
        # full control-op retry set.
        def _ensure_row(c):
            ensure_status_row_cur(c, label)
            conn_holder[0].commit()
        run_control_op(conn_holder, label, _ensure_row, "ensure_status_row")

        print(f"  ▶ {label}: {len(pending)} pending file(s)"
              + (f" (resume @offset {resume_offset} in {in_progress.split('/')[-1]})"
                 if in_progress else ""))

        prior_wm = state["watermark_ts"]   # last committed watermark (for monotonicity check)
        # TIER ROUTING: a table with a targeting key (real PK or declared logical key) uses the
        # keyed apply_file (Tier 1, correct I/U/D). A keyless table uses apply_file_nonpk (Tier
        # 2: insert/delete only, updates skipped+logged, no chunking). Both share the same
        # (total_applied, file_watermark, chunks_committed) return shape, so everything below
        # (ledger markers, checkpoint, done/blocked handling, file moves) is identical.
        _apply_fn = apply_file if ctx["apply_key"] is not None else apply_file_nonpk
        for key in pending:
            start = resume_offset if (in_progress and key == in_progress) else 0
            try:
                n, file_wm, n_chunks = _apply_fn(ctx, key, start, conn_holder,
                                                 prior_watermark=prior_wm)
                applied_rows += n
                chunks_done += n_chunks
                if file_wm is not None:
                    prior_wm = file_wm   # advance for the next file's monotonicity check
            except _SchemaChangedMidFile as sc:
                # NOT a block. A concurrent DDL invalidated this file's frozen column list.
                # The failing chunk rolled back; the checkpoint still points at the last
                # committed offset. Return cleanly with what we applied so far — the next
                # poll cycle re-runs apply_file from that offset and re-reconciles schema.
                print(f"    🔄 {label}: {sc} (non-blocking; resuming next cycle)")
                return {"table": label, "status": "schema_changed",
                        "files": files_done, "rows": applied_rows, "chunks": chunks_done}
            except TableBlocked as tb:
                # Record the failing file + error and mark the table blocked. The table
                # stops; other tables keep flowing.
                #
                # IMPORTANT — do NOT move the file to failed/. The block is RESUMABLE:
                # cdc_status.in_progress_file + last_offset point at THIS file, at the
                # offset AFTER the last successfully committed chunk (the failing chunk
                # rolled back atomically with its checkpoint). Once an operator fixes the
                # cause (e.g. widen a varchar that tripped the length guard, or re-export a
                # corrupt file from DMS) and clears the block (set status != 'blocked'),
                # the next run RESUMES this same file from last_offset — re-applying only
                # the un-applied rows (idempotent). Moving it to failed/ would hide it from
                # list_cdc_files and the remaining rows would be silently skipped -> loss.
                # The file therefore stays in place; failed/ is reserved for a file an
                # operator explicitly abandons out of band.
                record_exception(label, key, start, "(apply)", str(tb))
                print(f"    ⛔ {label} BLOCKED at {key.split('/')[-1]} @offset {start}: {tb}")
                print(f"       RESUMABLE: fix the cause, then clear cdc_status.status "
                      f"('{label}') to resume from offset {start} (file left in place).")
                return {"table": label, "status": "blocked", "files": files_done,
                        "rows": applied_rows, "chunks": chunks_done, "error": str(tb)}
            # File fully applied -> advance the high-water + clear in-progress on THE
            # TABLE'S OWN connection (no new handshake), then move the S3 file to
            # processed/ (human artifact; DSQL is the source of truth). The same txn stamps
            # the cdc_file_status ledger row 'done' (Option-A lifecycle marker), fused to the
            # high-water advance.
            _commit_status(status="active", last_done_file=key,
                           in_progress_file=None, last_offset=0, _done_file=key)
            try:
                move_file(key, ctx["prefixes"]["processed"])
            except Exception as e:
                print(f"    ⚠️ move to processed/ failed (non-fatal, DSQL is source of truth): {e}")
            files_done += 1
            resume_offset = 0
            in_progress = None
        # All pending files done -> mark idle (same session).
        _commit_status(status="idle", in_progress_file=None, last_offset=0)
    except Exception as e:
        # TABLE-LEVEL SAFETY NET — process_table must NEVER raise, so one table's
        # exhausted/unclassified DSQL error (e.g. a control-op that retried through OCC /
        # transient-server / txn-timeout / pipe and still failed) is ISOLATED to this
        # table. Other tables keep flowing; this table is retried next poll cycle. We do
        # NOT mark it 'blocked' (that's reserved for a genuinely un-appliable row that
        # needs an operator) — 'error' is transient/self-healing. Best-effort log only;
        # the checkpoint already reflects the last committed offset, so resume is exact.
        print(f"    ⚠️ {label}: cycle error (isolated, will retry next poll): {e}")
        return {"table": label, "status": "error", "files": files_done,
                "rows": applied_rows, "chunks": chunks_done, "error": str(e)}
    finally:
        try:
            conn_holder[0].close()
        except Exception:
            pass

    return {"table": label, "status": "ok", "files": files_done,
            "rows": applied_rows, "chunks": chunks_done}


# =============================================================================
# WAKE — how the loop waits between apply cycles
# =============================================================================
def wait_for_wake():
    """Sleep POLL_INTERVAL, then return so the caller runs the next list+apply cycle.

    The loop re-lists S3 each cycle; per-table serial DMS-timestamp order is owned by
    files.sort(), and already-applied files are skipped via last_done_file / processed-move
    (idempotent no-op), so an extra cycle with nothing new is cheap.

    Returns True on a normal wake (always True today; reserved for future stop signals)."""
    time.sleep(POLL_INTERVAL)
    return True


# =============================================================================
# MAIN
# =============================================================================
def main():
    # flush=True on EVERY startup line: Python Shell buffers stdout, so without flushing the
    # banner (and any hang location) never appears — a stall inside ensure_control_tables()
    # then looks like a hang "before main()". Flushing makes the real stall point visible.
    print("=" * 70, flush=True)
    print("CDC CONTINUOUS PROCESSOR v4 (MULTI-TABLE, DSQL-STATE, ZERO-LOSS)", flush=True)
    print(f"  Wake: poll every {POLL_INTERVAL}s | DDL Watch: {DDL_WATCH_INTERVAL}s | "
          f"parallel_tables={MAX_PARALLEL_TABLES} | min_file_age={MIN_FILE_AGE_SECONDS}s", flush=True)
    print(f"  Manifest: {CONFIG_PREFIX}_manifest_index.json", flush=True)
    print(f"  Target:   {DSQL_ENDPOINT}  control_schema={CONTROL_SCHEMA}", flush=True)
    print("=" * 70, flush=True)

    print("  [startup] ensuring control tables (DSQL connect)…", flush=True)
    ensure_control_tables()
    print("  [startup] control tables ready.", flush=True)

    print("  [startup] loading manifest…", flush=True)
    entries = load_manifest()
    print(f"  [startup] manifest loaded: {len(entries)} entries; building contexts…", flush=True)
    contexts = []
    for _i, e in enumerate(entries):
        try:
            print(f"    [startup] context {_i+1}/{len(entries)}: {e.get('dsql_schema')}.{e.get('dsql_table')}", flush=True)
            contexts.append(build_table_context(e))
        except Exception as ex:
            print(f"  ⚠️ skipping {e.get('dsql_table')} — config load failed: {ex}", flush=True)
    print(f"  Tables discovered: {len(contexts)}", flush=True)
    if not contexts:
        raise Exception("No usable tables from the manifest.")

    # STARTUP KEY-CONSISTENCY DIAGNOSTIC. The full-load gate matches a table by its EXACT
    # label (dsql_schema.dsql_table) against the keys in _load_status.json. v4 builds that
    # label from the per-table config metadata (fallback to the manifest entry); the
    # full-load job writes it from the manifest entry. In the clean flow they're identical,
    # but if they ever DIVERGE (case/quoting/schema-prefix edit in one source), the gate
    # silently returns None and that table logs "waiting for full load" FOREVER. Surface
    # any such mismatch ONCE at startup so a key divergence is caught immediately instead of
    # masquerading as a slow load. Read-only, one S3 GET, never fatal.
    if REQUIRE_FULL_LOAD_DONE:
        print("  [startup] reading full-load gate status…", flush=True)
        try:
            _startup_status = load_full_load_status()
            _labels = {c["label"] for c in contexts}
            _status_keys = set(_startup_status.keys())
            _no_entry = sorted(_labels - _status_keys)
            _done = sorted(l for l in _labels if _startup_status.get(l) == "done")
            print(f"  Full-load gate: {len(_done)}/{len(_labels)} table(s) marked 'done' "
                  f"and CDC-eligible now.", flush=True)
            if _no_entry:
                print(f"  ⚠️ {len(_no_entry)} manifest table(s) have NO entry in "
                      f"_load_status.json — they will WAIT until their full load marks them "
                      f"'done'. If a table below is already fully loaded, this is a KEY "
                      f"MISMATCH (config metadata vs manifest label), NOT a slow load: "
                      f"{_no_entry[:20]}{' …' if len(_no_entry) > 20 else ''}", flush=True)
        except Exception as _diag_e:
            print(f"  (startup gate diagnostic skipped, non-fatal: {_diag_e})", flush=True)

    # Start the per-table DDL watcher (uppercased DMS table names, as DMS reports them).
    dms_tables = sorted({c["dms_table"].upper() for c in contexts})
    print(f"  [startup] starting DDL watcher for {len(dms_tables)} table(s)…", flush=True)
    watcher = threading.Thread(target=ddl_watcher, args=(dms_tables,), daemon=True,
                               name="DDLWatcher")
    watcher.start()
    print(f"  DDL watcher started for {len(dms_tables)} table(s)", flush=True)
    print("  [startup] entering poll loop.", flush=True)

    last_activity = time.time()
    poll = 0
    try:
        while True:
            poll += 1
            any_work = False
            results = []
            _cycle_t0 = time.monotonic()   # telemetry: wall time of this apply cycle

            # Read the full-load status ONCE per cycle (cheap S3 GET), shared by all tables
            # so a table becomes CDC-eligible as soon as its load is marked 'done'.
            load_status_map = load_full_load_status() if REQUIRE_FULL_LOAD_DONE else {}

            if MAX_PARALLEL_TABLES <= 1:
                for ctx in contexts:
                    try:
                        r = process_table(ctx, load_status_map)
                    except Exception as e:  # process_table shouldn't raise, but guard
                        r = {"table": ctx["label"], "status": "error", "error": str(e)}
                    results.append(r)
            else:
                with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES,
                                        thread_name_prefix="cdctbl") as pool:
                    futs = {pool.submit(process_table, ctx, load_status_map): ctx["label"]
                            for ctx in contexts}
                    for fut in as_completed(futs):
                        try:
                            results.append(fut.result())
                        except Exception as e:  # process_table shouldn't raise, but guard
                            results.append({"table": futs[fut], "status": "error", "error": str(e)})

            for r in results:
                if r.get("files", 0) > 0 or r.get("rows", 0) > 0:
                    any_work = True
                    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                          f"{r['table']}: {r['status']} — {r.get('files',0)} file(s), "
                          f"{r.get('rows',0):,} row(s), {r.get('chunks',0)} chunk(s)")

            # ── PER-CYCLE THROUGHPUT TELEMETRY (CloudWatch-visible; printed only when work
            # happened so idle cycles stay quiet). Pure aggregation over the result dicts +
            # one monotonic() delta — no DSQL round-trips, no measurable cost. Lets you SEE
            # actual rows/sec, chunks, and avg rows/commit instead of guessing.
            #   • rows/sec           : committed rows this cycle / wall seconds
            #   • rows/commit (avg)  : how densely chunks packed (op-mix packing efficiency)
            #   • commits            : total chunk transactions (fewer = higher throughput)
            _cycle_secs = max(1e-6, time.monotonic() - _cycle_t0)
            _tot_files = sum(r.get("files", 0) for r in results)
            _tot_rows = sum(r.get("rows", 0) for r in results)
            _tot_chunks = sum(r.get("chunks", 0) for r in results)
            _active_tables = sum(1 for r in results
                                 if r.get("files", 0) or r.get("rows", 0))
            if any_work:
                _rows_per_sec = _tot_rows / _cycle_secs
                _rows_per_commit = (_tot_rows / _tot_chunks) if _tot_chunks else 0
                print(f"  ⏱  cycle #{poll}: {_tot_rows:,} row(s) in {_tot_files} file(s) "
                      f"across {_active_tables} table(s) via {_tot_chunks} commit(s) in "
                      f"{_cycle_secs:.2f}s -> {_rows_per_sec:,.0f} rows/s, "
                      f"{_rows_per_commit:,.0f} rows/commit")

            blocked = [r["table"] for r in results if r.get("status") == "blocked"]
            if blocked:
                print(f"  ⛔ blocked tables (need operator attention): {blocked}")
            # 'error' = transient/isolated (control-op retries exhausted, connect failure,
            # etc.). Self-healing: retried next poll. Surface it so a PERSISTENT error is
            # visible, but it does NOT need operator action the way 'blocked' does.
            errored = [r["table"] for r in results if r.get("status") == "error"]
            if errored:
                print(f"  ⚠️ transient-error tables (auto-retry next poll): {errored}")

            if any_work:
                last_activity = time.time()
            else:
                idle_s = time.time() - last_activity
                if poll % 10 == 0:
                    mode = "continuous" if RUN_FOREVER else f"idle-stop@{MAX_IDLE_HOURS}h"
                    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                          f"idle {int(idle_s)}s ({mode})")
                # CONTINUOUS: never self-terminate on idle — DMS keeps producing files and
                # a quiet table may resume at any time. Only the drain-and-exit mode
                # (RUN_FOREVER=False) stops after MAX_IDLE_HOURS of no work.
                if not RUN_FOREVER and idle_s / 3600 > MAX_IDLE_HOURS:
                    print(f"\n⏹️  Idle {MAX_IDLE_HOURS}h and RUN_FOREVER=False — stopping.")
                    break

            # Wait for the next cycle: sleep POLL_INTERVAL, then re-list and apply. The
            # apply cycle above is idempotent, so an extra cycle with nothing new is cheap.
            wait_for_wake()
    except KeyboardInterrupt:
        print("\n⏹️  Manual stop.")
    finally:
        _stop_event.set()
        print("=" * 70)
        print("CDC v4 STOPPED")
        print("=" * 70)


# Glue Python Shell executes this module directly; run unconditionally.
main()
