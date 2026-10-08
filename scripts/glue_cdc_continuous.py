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
  - The 30s poll loop and the S3 processed/ copies of applied files (kept as a human-visible
    artifact; originals stay in place; DSQL cdc_status is the authoritative resume position).

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
import random
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
LOAD_STATUS_KEY = None   # optional explicit key; else merge CONFIG_PREFIX + "_load_status.json"
                         # and every CONFIG_PREFIX + "_orchestrator/group-*/_load_status.json"

# Per-table CDC file layout. Each table's CDC CSVs live under:
#     s3://{BUCKET}/{CDC_ROOT}/{dms_schema}/{dms_table}/*.csv
# with per-table processed/ and failed/ subfolders. Adjust CDC_ROOT to match the DMS
# S3 target BucketFolder. If your layout differs, override derive_table_prefixes().
CDC_ROOT = 'cdc'

# Aurora DSQL target.
DSQL_ENDPOINT = '<YOUR_CLUSTER>.dsql.<REGION>.on.aws'
# Ordered, comma-separated DSQL hostnames to try (resolve_task derives a PrivateLink candidate
# and passes --dsql_endpoint_candidates). connect_dsql() tries each in order, pins the first
# that connects into DSQL_ENDPOINT, and all later connects/reconnects reuse the pinned host.
# Empty -> just DSQL_ENDPOINT is used (full backward compatibility).
DSQL_ENDPOINT_CANDIDATES = ''
DSQL_DATABASE = 'postgres'
DSQL_USER = 'admin'
REGION = 'us-east-1'

# DMS replication task ARN (for the DDL watcher's describe_table_statistics).
DMS_TASK_ARN = 'arn:aws:dms:<REGION>:<ACCOUNT>:task:<TASK_ID>'

# DSQL schema that holds v4's control tables (created if not exists on startup).
CONTROL_SCHEMA = 'cdc_control'

# CDC ownership (fork design): the ONE persisted record config/_task/<suffix>/_cdc_owners.json
# maps each table label -> its CDC owner: "main" | "ck-<slug>" | "bg-<slug>". This job applies a
# table ONLY if its recorded owner == CDC_OWNER_SELF. For the main CDC job CDC_OWNER_SELF="main"
# (it skips tables owned by a ck/bg fork, in addition to the composite skip); a fork job sets its
# own identity. Absent record/owner -> fall back to "main" (back-compat: pre-fork tasks have no
# record and the single main job owns everything it can apply).
CDC_OWNER_SELF = 'main'
CDC_OWNERS_KEY = None          # S3 key of _cdc_owners.json; derived from CONFIG_PREFIX if unset

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
# files are skipped via last_done_file (the high-water mark; originals stay in place), and the
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
# ON by default (override with --cdc_validation false). It adds target reads AFTER a file
# commits, so it costs some time/IO. After a file's apply fully commits, v4 samples up to
# VALIDATION_SAMPLE_PER_FILE of that file's net-ops and re-reads each row by PK from the
# target, comparing to the expected net-op image (INSERT -> row present + values match;
# DELETE -> row absent). A mismatch is RE-CHECKED after VALIDATION_RETRY_DELAY_SECONDS
# (absorbs any commit lag); only a persistent mismatch is recorded in
# cdc_control.cdc_validation_failures (with resolved=false). The apply is NOT blocked (the
# apply already succeeded and is authoritative). It runs on the table's own connection,
# AFTER the commit, never inside a chunk transaction, so it never touches the apply hot path.
# CUTOVER reads cdc_validation_failures: any unresolved row (resolved IS NOT TRUE; NULL counts
# as unresolved) STOPS cutover.
VALIDATION_ENABLED = True                # ON by default; override with --cdc_validation false
VALIDATION_SAMPLE_PER_FILE = 20          # net-ops sampled per file (0 = all — expensive)
VALIDATION_RETRY_DELAY_SECONDS = 5       # re-check a mismatch after this, before recording
VALIDATION_MAX_FAILURES_PER_TABLE = 100  # circuit breaker: stop validating a table past this

# =============================================================================
# SAFETY GUARDRAILS (data-loss protection; see WHAT_IF.md / RUNBOOK "Safety guardrails")
# Defaults are SAFE. Each override is an explicit --cdc_* arg (set by create_glue_jobs from
# pipeline.json). G6 mass-delete guard + G9 drift detector read these.
# =============================================================================
CDC_MAX_DELETE_FRACTION = 0.5    # G6: a file deleting > this fraction AND > CDC_MAX_DELETE_ROWS
CDC_MAX_DELETE_ROWS = 100000     #     of a table's current rows blocks it; fraction >= 1 = off
CDC_DRIFT_CHECK_MINUTES = 30     # G9: minutes between live-count-vs-expected checks (0 = off)
CDC_DRIFT_TOLERANCE = 0.0        # G9: allowed |live − expected| row difference before firing
CDC_DRIFT_ACTION = "warn"        # G9: "warn" (log+metric+audit) or "block" (also set 'blocked')
_DRIFT_LAST_RUN = {}             # G9: per-table monotonic clock of the last drift check (throttle)
# G9 DRIFT BASELINE: per-table full-load row count harvested from each _load_status.json's
# "rows" field (same source Job 2 writes). The drift expectation is
# full_load_rows + inserts_applied − deletes_applied; without the full-load baseline the
# expectation omits every row the full load inserted and drift fires a FALSE +N alarm (G9).
# cdc_status.full_load_rows is seeded from this map once (idempotent) by process_table so the
# expectation is correct and persists. Merged newest-wins alongside the status map.
_FULL_LOAD_ROWS = {}             # {label: int full-load row count from _load_status.json}
# EASE-GUARDRAILS: master mode + per-guard WARN/block knobs. In "warn" (default) a soft guard
# NEVER blocks a table for its own bookkeeping — it logs a WARNING + a DsqlGuardWarn metric and
# keeps applying. "strict" restores the fail-closed (block) behaviour. The HARD G6 mass-delete
# guard blocks in BOTH modes (it only ever stops a destructive op). The per-guard actions below
# default to the master mode when left at "warn".
GUARDRAILS_MODE = "warn"            # "warn" | "strict"
CDC_FILE_ORDER_ACTION = "warn"      # G8 order/gap/new-LOAD-after-CDC: "warn" | "block"
CDC_NOPK_OVERMATCH_ACTION = "warn"  # G7 no-PK over-match precision: "warn" | "block"

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
# How DMS marks a real NULL in its CSV files: the S3 target endpoint's CsvNullValue (DMS
# default "NULL"), passed by create-glue-jobs as --csv_null_value ("__EMPTY__" = the endpoint
# sets it to the empty string). Only that exact text, or an empty field, is stored as NULL.
CSV_NULL_VALUE = "NULL"

# ===================================================================================== #
# SHARED NULL-RULES BLOCK — customer-controlled NULL handling (two params.csv settings).  #
# COPIED BYTE-IDENTICALLY into scripts/job2_load.py, scripts/job3_validate.py,            #
# scripts/glue_cdc_continuous.py and scripts/glue_cdc_composite.py. These four scripts do #
# NOT share imports (each is a self-contained Glue job script), so the parse/match logic  #
# lives here as ONE function set that is copied verbatim; tests assert the copies are     #
# byte-identical (tests/test_null_rules.py::test_four_copies_identical).                   #
#                                                                                         #
# TWO settings travel from params.csv -> pipeline.json -> resolve_task -> create_glue_jobs #
# as the Glue job args --null_values and --null_rules (set on load/load-big/validate/cdc/ #
# ck/bg, exactly like --csv_null_value):                                                  #
#                                                                                         #
#   null_values  (all columns): a '|'-separated list of EXACT strings that become SQL     #
#                NULL. Blank = today's behaviour: the DMS endpoint's CsvNullValue          #
#                (CSV_NULL_VALUE, passed as --csv_null_value). If set it REPLACES that     #
#                marker list for EVERY column.                                            #
#   null_rules   (per-column): 'schema.table.column=VALUES' entries separated by ';'.     #
#                VALUES is either 'none' (nothing in that column becomes NULL from a text  #
#                value, so the text 'NULL' is KEPT as data) or a '|'-separated list that   #
#                REPLACES the default for that one column. Names match case-INSENSITIVELY  #
#                against the DSQL lowercase schema/table/column.                          #
#                                                                                         #
# PRECEDENCE for a column: a null_rules entry for that column, THEN null_values, THEN the  #
# endpoint marker (CSV_NULL_VALUE). With both settings blank/absent the effective marker  #
# list is exactly [CSV_NULL_VALUE] (when non-empty) — i.e. byte-identical to today.       #
#                                                                                         #
# EMPTY-FIELD handling is UNCHANGED and lives in the callers, not here: an empty field is #
# always a real NULL in every mode (including 'none'), exactly as before. This block only #
# decides which NON-empty TEXT values are markers. The comparison the callers do with the #
# returned markers is the SAME comparison point as the canonical _coerce_null: exact,     #
# case-sensitive, whole-value (no trimming).                                              #
# ===================================================================================== #

# Parsed forms of the two settings. None = "unset" (the arg was absent or blank), which is
# distinct from an empty list. Set once at startup by _apply_null_settings().
NULL_VALUES = None   # None | list[str]  (replacement marker list for ALL columns)
NULL_RULES = None    # None | dict[(schema,table,col) -> list[str]]  ([] == 'none': no markers)


class NullRulesError(Exception):
    """A malformed null_rules / null_values setting. Raised so preflight (and each job at
    startup) FAILS LOUD naming the bad entry — wrong data, never silently ignored."""


def _parse_null_values(raw):
    """'a|b|c' -> ['a','b','c']. Blank/None -> None (unset: keep today's endpoint marker).
    A '|'-separated list; EMPTY tokens are not allowed (an empty field is always NULL already
    and is handled by the caller, so '' as a marker is meaningless and almost certainly a typo).
    Exact strings, no trimming of the token's own characters (only the surrounding arg is
    stripped by the caller)."""
    if raw is None:
        return None
    s = str(raw)
    if s == "" or s == "__NULL_UNSET__":
        return None
    parts = s.split("|")
    if any(p == "" for p in parts):
        raise NullRulesError(
            f"null_values {raw!r} has an empty marker between '|' separators; list exact, "
            f"non-empty strings (an empty field is already treated as NULL).")
    return parts


def _parse_null_rules(raw):
    """'schema.table.column=VALUES; ...' -> {(schema,table,column): [markers] | []}.
    Blank/None -> None (unset). Keys are lower-cased (matched against the DSQL lowercase
    schema/table/column). VALUES is 'none' (-> [], no text value becomes NULL) or a
    '|'-separated marker list (-> that list). Raises NullRulesError, naming the bad entry, on
    any malformed entry: missing '=', a left side that is not exactly schema.table.column, an
    empty VALUES, or an empty marker inside the list."""
    if raw is None:
        return None
    s = str(raw)
    if s == "" or s == "__NULL_UNSET__":
        return None
    rules = {}
    for entry in s.split(";"):
        e = entry.strip()
        if e == "":
            continue   # tolerate a trailing ';' or blank between entries
        if "=" not in e:
            raise NullRulesError(
                f"null_rules entry {entry!r} is malformed: expected "
                f"'schema.table.column=none|VALUE|VALUE'. No '=' found.")
        left, _, right = e.partition("=")
        left = left.strip()
        name_parts = left.split(".")
        if len(name_parts) != 3 or any(p.strip() == "" for p in name_parts):
            raise NullRulesError(
                f"null_rules entry {entry!r} is malformed: the left side {left!r} must be "
                f"exactly 'schema.table.column' (three dot-separated non-empty names).")
        key = tuple(p.strip().lower() for p in name_parts)
        right = right.strip()
        if right == "":
            raise NullRulesError(
                f"null_rules entry {entry!r} is malformed: VALUES after '=' is empty; use "
                f"'none' (keep text as data) or a '|'-separated marker list.")
        if right.lower() == "none":
            markers = []
        else:
            markers = right.split("|")
            if any(m == "" for m in markers):
                raise NullRulesError(
                    f"null_rules entry {entry!r} is malformed: an empty marker between '|' "
                    f"separators. List exact, non-empty strings, or 'none'.")
        if key in rules:
            raise NullRulesError(
                f"null_rules names {left!r} more than once; give each schema.table.column at "
                f"most one entry.")
        rules[key] = markers
    return rules


def _effective_null_markers(schema, table, column):
    """The ORDERED list of exact text values that mean SQL NULL for this one column, applying
    precedence: a null_rules entry for the column (which may be [] for 'none'), THEN null_values
    (replaces for all columns), THEN the endpoint marker CSV_NULL_VALUE. Empty-field handling is
    separate (always NULL) and NOT included here. Names are matched case-insensitively. With both
    settings unset this returns [CSV_NULL_VALUE] (if non-empty) or [] — i.e. today's behaviour."""
    if NULL_RULES is not None:
        key = (str(schema).lower(), str(table).lower(), str(column).lower())
        if key in NULL_RULES:
            return list(NULL_RULES[key])
    if NULL_VALUES is not None:
        return list(NULL_VALUES)
    return [CSV_NULL_VALUE] if CSV_NULL_VALUE else []


def _apply_null_settings(null_values_arg, null_rules_arg):
    """Parse the --null_values / --null_rules Glue args into the NULL_VALUES / NULL_RULES
    globals. The sentinel '__NULL_UNSET__' (and blank/absent) means "unset" (Glue cannot pass an
    empty arg value, so create_glue_jobs sends the sentinel for a blank setting). Raises
    NullRulesError on a malformed setting. Returns a short human summary for the startup log."""
    global NULL_VALUES, NULL_RULES
    NULL_VALUES = _parse_null_values(null_values_arg)
    NULL_RULES = _parse_null_rules(null_rules_arg)
    nv = "unset (endpoint marker)" if NULL_VALUES is None else f"{NULL_VALUES!r} (ALL columns)"
    if NULL_RULES is None:
        nr = "unset"
    else:
        nr = "; ".join(f"{'.'.join(k)}=" + ("none" if v == [] else "|".join(v))
                       for k, v in NULL_RULES.items()) or "(empty)"
    return f"null_values={nv}; null_rules={nr}; endpoint CSV_NULL_VALUE={CSV_NULL_VALUE!r}"


def _null_rules_unknown_warnings(schema, table, columns):
    """WARN (do not fail) for a null_rules entry that targets THIS table (schema+table match,
    case-insensitively) but names a COLUMN the table does not have — almost always a column
    typo. `columns` is this table's column names (any case). Entries for a different table are
    NOT reported here (they simply never match any column — harmless, still only a warning).
    Returns a list of warning strings. An unknown table/column is only a WARNING: the operator
    may drive several tasks from one pipeline.json, so a name not in THIS task is not an error."""
    if NULL_RULES is None:
        return []
    s_l, t_l = str(schema).lower(), str(table).lower()
    have = {str(c).lower() for c in (columns or [])}
    return [f"null_rules entry {'.'.join(k)} names column {k[2]!r} not found in "
            f"{s_l}.{t_l} (ignored; check the column name)"
            for k in NULL_RULES if k[0] == s_l and k[1] == t_l and k[2] not in have]
# ===================================================================================== #
# END SHARED NULL-RULES BLOCK                                                             #
# ===================================================================================== #
# ===================================================================================== #
# ===================================================================================== #



def _apply_cdc_arg_overrides():
    global CSV_NULL_VALUE
    global CONFIG_PREFIX, INDEX_S3_KEY, LOAD_STATUS_KEY, BUCKET, CDC_ROOT
    global DSQL_ENDPOINT, DSQL_DATABASE, DSQL_USER, REGION
    global DSQL_ENDPOINT_CANDIDATES   # kit: PrivateLink/public failover list
    global DMS_TASK_ARN, CONTROL_SCHEMA
    global MAX_PARALLEL_TABLES, REQUIRE_FULL_LOAD_DONE, POLL_INTERVAL
    global DMS_TIMESTAMP_COLUMN, SINGLE_SWAP_IS_RENAME
    global VALIDATION_ENABLED, VALIDATION_SAMPLE_PER_FILE
    global CDC_OWNER_SELF, CDC_OWNERS_KEY
    global CDC_MAX_DELETE_FRACTION, CDC_MAX_DELETE_ROWS
    global CDC_DRIFT_CHECK_MINUTES, CDC_DRIFT_TOLERANCE, CDC_DRIFT_ACTION
    global GUARDRAILS_MODE, CDC_FILE_ORDER_ACTION, CDC_NOPK_OVERMATCH_ACTION
    optional = ["config_prefix", "index_s3_key", "load_status_key", "s3_bucket", "cdc_root",
                "dsql_endpoint", "dsql_database", "dsql_user", "region",
                "dsql_endpoint_candidates",
                "dms_task_arn", "control_schema",
                "max_parallel_tables", "require_full_load_done", "poll_interval",
                "timestamp_column", "single_swap_is_rename", "csv_null_value",
                "null_values", "null_rules",
                "cdc_validation", "cdc_validation_sample",
                "cdc_max_delete_fraction", "cdc_max_delete_rows",
                "cdc_drift_check_minutes", "cdc_drift_tolerance", "cdc_drift_action",
                "guardrails_mode", "cdc_file_order_action", "cdc_nopk_overmatch_action",
                "cdc_owner_self", "cdc_owners_key"]
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
    if "csv_null_value" in ov and ov["csv_null_value"] is not None:
        _nv = str(ov["csv_null_value"])
        CSV_NULL_VALUE = "" if _nv == "__EMPTY__" else _nv
        print(f"  ↪ CSV_NULL_VALUE (DMS null marker) -> {CSV_NULL_VALUE!r}")
    # Customer NULL handling (null_values / null_rules). Parse AFTER CSV_NULL_VALUE is final
    # (precedence falls back to it). A malformed setting fails LOUD (NullRulesError). The
    # effective rules are logged for every CDC run; an entry naming a table/column not in this
    # task is only a WARNING (checked per-table as files are processed).
    _nvl = ov.get("null_values") if "null_values" in ov else None
    _nrl = ov.get("null_rules") if "null_rules" in ov else None
    if _nvl is not None or _nrl is not None:
        _summary = _apply_null_settings(_nvl, _nrl)
        print(f"  ↪ NULL rules: {_summary}")
    if _s("single_swap_is_rename"):
        SINGLE_SWAP_IS_RENAME = _s("single_swap_is_rename").strip().lower() in ("true", "1", "yes")
        print(f"  ↪ SINGLE_SWAP_IS_RENAME overridden -> {SINGLE_SWAP_IS_RENAME}")
    if _s("dsql_endpoint"):
        DSQL_ENDPOINT = _s("dsql_endpoint")
    if _s("dsql_endpoint_candidates"):
        DSQL_ENDPOINT_CANDIDATES = _s("dsql_endpoint_candidates")
        print(f"  ↪ DSQL_ENDPOINT_CANDIDATES -> {DSQL_ENDPOINT_CANDIDATES}")
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
    if _s("cdc_owner_self"):
        CDC_OWNER_SELF = _s("cdc_owner_self")
        print(f"  ↪ CDC_OWNER_SELF -> {CDC_OWNER_SELF}")
    if _s("cdc_owners_key"):
        CDC_OWNERS_KEY = _s("cdc_owners_key")
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
    if "cdc_validation" in ov and ov["cdc_validation"] is not None:
        # Missing arg leaves the ON-by-default constant untouched; an explicit value overrides.
        VALIDATION_ENABLED = str(ov["cdc_validation"]).strip().lower() in ("true", "1", "yes")
        print(f"  ↪ VALIDATION_ENABLED overridden -> {VALIDATION_ENABLED}")
    if "cdc_validation_sample" in ov and ov["cdc_validation_sample"] is not None:
        try:
            VALIDATION_SAMPLE_PER_FILE = max(0, int(ov["cdc_validation_sample"]))
            print(f"  ↪ VALIDATION_SAMPLE_PER_FILE overridden -> {VALIDATION_SAMPLE_PER_FILE}")
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid cdc_validation_sample={ov['cdc_validation_sample']!r}")
    # ── SAFETY GUARDRAIL overrides (G6 mass-delete, G9 drift). A bad value is IGNORED (keep the
    # SAFE default) with a warning — never silently turns a guard off through a parse error.
    if "cdc_max_delete_fraction" in ov and ov["cdc_max_delete_fraction"] is not None:
        try:
            CDC_MAX_DELETE_FRACTION = max(0.0, float(ov["cdc_max_delete_fraction"]))
            print(f"  ↪ CDC_MAX_DELETE_FRACTION overridden -> {CDC_MAX_DELETE_FRACTION}")
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid cdc_max_delete_fraction={ov['cdc_max_delete_fraction']!r}")
    if "cdc_max_delete_rows" in ov and ov["cdc_max_delete_rows"] is not None:
        try:
            CDC_MAX_DELETE_ROWS = max(0, int(ov["cdc_max_delete_rows"]))
            print(f"  ↪ CDC_MAX_DELETE_ROWS overridden -> {CDC_MAX_DELETE_ROWS}")
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid cdc_max_delete_rows={ov['cdc_max_delete_rows']!r}")
    if "cdc_drift_check_minutes" in ov and ov["cdc_drift_check_minutes"] is not None:
        try:
            CDC_DRIFT_CHECK_MINUTES = max(0, int(ov["cdc_drift_check_minutes"]))
            print(f"  ↪ CDC_DRIFT_CHECK_MINUTES overridden -> {CDC_DRIFT_CHECK_MINUTES}")
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid cdc_drift_check_minutes={ov['cdc_drift_check_minutes']!r}")
    if "cdc_drift_tolerance" in ov and ov["cdc_drift_tolerance"] is not None:
        try:
            CDC_DRIFT_TOLERANCE = max(0.0, float(ov["cdc_drift_tolerance"]))
            print(f"  ↪ CDC_DRIFT_TOLERANCE overridden -> {CDC_DRIFT_TOLERANCE}")
        except (TypeError, ValueError):
            print(f"  ⚠️ ignoring invalid cdc_drift_tolerance={ov['cdc_drift_tolerance']!r}")
    if "cdc_drift_action" in ov and ov["cdc_drift_action"] is not None:
        _da = str(ov["cdc_drift_action"]).strip().lower()
        if _da in ("warn", "block"):
            CDC_DRIFT_ACTION = _da
            print(f"  ↪ CDC_DRIFT_ACTION overridden -> {CDC_DRIFT_ACTION}")
        else:
            print(f"  ⚠️ ignoring invalid cdc_drift_action={ov['cdc_drift_action']!r} "
                  f"(must be warn|block)")
    # EASE-GUARDRAILS master mode. strict => fail-closed: the soft CDC guards (G7/G8) block.
    if "guardrails_mode" in ov and ov["guardrails_mode"] is not None:
        _gm = str(ov["guardrails_mode"]).strip().lower()
        if _gm in ("warn", "strict"):
            GUARDRAILS_MODE = _gm
            print(f"  ↪ GUARDRAILS_MODE -> {GUARDRAILS_MODE}")
            if _gm == "strict":
                # strict implies block for the soft CDC guards (an explicit per-guard override
                # below still wins).
                CDC_FILE_ORDER_ACTION = "block"
                CDC_NOPK_OVERMATCH_ACTION = "block"
        else:
            print(f"  ⚠️ ignoring invalid guardrails_mode={ov['guardrails_mode']!r} "
                  f"(must be warn|strict)")
    for _ek, _gl in (("cdc_file_order_action", "CDC_FILE_ORDER_ACTION"),
                     ("cdc_nopk_overmatch_action", "CDC_NOPK_OVERMATCH_ACTION")):
        if _ek in ov and ov[_ek] is not None and str(ov[_ek]).strip():
            _v = str(ov[_ek]).strip().lower()
            if _v in ("warn", "block"):
                globals()[_gl] = _v
                print(f"  ↪ {_gl} overridden -> {_v}")
            else:
                print(f"  ⚠️ ignoring invalid {_ek}={ov[_ek]!r} (must be warn|block)")


_apply_cdc_arg_overrides()

# =============================================================================
# GLOBALS
# =============================================================================
# S3 calls retry throttling/5xx with boto3's adaptive mode (client-side rate limiting), plus the
# explicit retry loop in _s3_retry for copies, lists and manifest writes.
try:
    from botocore.config import Config as _S3Config
    s3 = boto3.client('s3', region_name=REGION,
                      config=_S3Config(retries={"max_attempts": 10, "mode": "adaptive"}))
except Exception:
    s3 = boto3.client('s3', region_name=REGION)
# DMS (column-rename detection in the DDL watcher) and CloudWatch (one optional metric) are not
# needed to apply changes. Behind a firewall with no route to them, boto3's defaults (60 s
# timeouts, several retries) make each call hang for minutes, so these clients fail fast.
try:
    from botocore.config import Config as _OptConfig
    _OPTIONAL_API = _OptConfig(connect_timeout=5, read_timeout=15,
                               retries={"max_attempts": 2, "mode": "standard"})
    dms = boto3.client('dms', region_name=REGION, config=_OPTIONAL_API)
except Exception:
    dms = boto3.client('dms', region_name=REGION)
try:
    try:
        cloudwatch = boto3.client('cloudwatch', region_name=REGION, config=_OPTIONAL_API)
    except NameError:
        cloudwatch = boto3.client('cloudwatch', region_name=REGION)
except Exception:
    cloudwatch = None
_OPTIONAL_API_DOWN = {"cloudwatch": False, "dms_logged": False}


def _is_unreachable(e):
    """True for errors that mean 'no route to the service' (not a permission or API error)."""
    n = type(e).__name__
    return n in ("ConnectTimeoutError", "EndpointConnectionError", "ReadTimeoutError",
                 "ConnectionClosedError", "ConnectionError") or "Could not connect" in str(e)
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

# Categories that are re-parsed or cast (anything else is text and is stored exactly as DMS
# wrote it). A whitespace-only value in one of these is NULL: it can't be cast.
_TYPED_CATEGORIES = frozenset({
    'uuid', 'boolean', 'timestamptz', 'date', 'bigint', 'integer', 'smallint', 'numeric',
    'float', 'double', 'real', 'json', 'jsonb', 'bytea'})

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


# Binary (bytea) columns. DMS writes an Oracle RAW / BLOB value into its CSV as hexadecimal
# (e.g. CBDE965C...). Postgres reads '\x<hex>'::bytea as those bytes, but plain '<hex>'::bytea
# as the ASCII characters of the hex text (twice as many, wrong bytes). So a binary value is
# normalized to '\x' + lowercase hex; an optional '\x' or '0x' prefix is accepted. Anything that
# isn't whole-byte hex is left as written and stopped by guard_row (it can't be decoded safely).
_BYTEA_PREFIX = re.compile(r'^(?:\\x|0[xX])')
_HEX_BYTES = re.compile(r'^(?:[0-9a-fA-F]{2})*$')
_BYTEA_CANONICAL = re.compile(r'^\\x(?:[0-9a-f]{2})*$')


def to_bytea_hex(v):
    """'CBDE' / '0xCBDE' / '\\xcbde' -> '\\xcbde' (a backslash, x, then hex). Non-hex input is
    returned unchanged."""
    h = _BYTEA_PREFIX.sub('', v, count=1)
    return '\\x' + h.lower() if _HEX_BYTES.match(h) else v


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


def _coerce_null(v, markers=None):
    """None for a real NULL, else the value exactly as DMS wrote it.

    `markers` (optional) is the pre-resolved list of exact NULL-marker strings for THIS column,
    from the shared _effective_null_markers(schema, table, column) (customer null_values /
    null_rules). When it is None (the default, and the case the AST-extract regression test
    exercises) the behaviour is EXACTLY today's: the only text marker is CSV_NULL_VALUE. An empty
    field is ALWAYS a real NULL, in every mode (including a 'none' per-column rule). The marker
    comparison is the canonical one: exact, case-sensitive, whole-value (no trimming).

    A real NULL is an empty field or the endpoint's null marker (CSV_NULL_VALUE, DMS default
    "NULL"), compared exactly: no trimming, case-sensitive. Every other value is data and is
    kept as written, including 'NA', 'N/A', 'NONE', '(NULL)', '\\N', 'null' and ' NULL '.
    (Earlier versions turned all of those into NULL in every column, silently losing real text
    values.) Whitespace is never trimmed here; convert_value strips it for typed columns only."""
    if v is None:
        return None
    s = v if isinstance(v, str) else str(v)
    if s == "":
        return None
    if markers is None:
        if CSV_NULL_VALUE and s == CSV_NULL_VALUE:
            return None
    elif s in markers:
        return None
    return s


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


def convert_value(raw, category, markers=None):
    """Normalize one raw CSV string to the canonical STRING form for its type category,
    ready to be wrapped by a ::type cast. Returns None for null/sentinel. Mirrors v15's
    Spark normalization, reimplemented in plain Python (no Spark in a Python Shell job).

    Does NOT hard-fail here on a bad uuid — the per-row guard (guard_row) does that so the
    error is attributed clearly and halts the table (zero-error policy)."""
    v = _coerce_null(raw, markers)
    if v is None:
        return None
    # Typed categories re-parse/cast and must not be affected by surrounding whitespace, so
    # they operate on a stripped copy. VARCHAR/text (the final pass-through) keeps the ORIGINAL
    # value so significant leading/trailing spaces are preserved.
    vs = v.strip() if isinstance(v, str) else v
    if category in _TYPED_CATEGORIES and vs == "":
        return None   # whitespace-only in a typed column can't be cast -> NULL
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
    if category == 'bytea':
        return to_bytea_hex(vs)
    if category in ('integer', 'bigint', 'smallint', 'numeric', 'float', 'double', 'real',
                    'json', 'jsonb'):
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


# Socket connect timeout (seconds) per candidate probe during endpoint failover: a wrong
# PrivateLink/public host should fail FAST so we move on to the next candidate rather than
# blocking CDC startup on one unreachable name.
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


def _probe_connect_dsql(host):
    """Open a connection to ONE candidate host with a short socket timeout and a token minted
    for THAT host (not the global cache). Used only during first-connect endpoint resolution."""
    client = make_boto_client("dsql")
    token = client.generate_db_connect_admin_auth_token(
        host, Region=REGION, ExpiresIn=DSQL_TOKEN_EXPIRES_IN)
    return pg8000.connect(
        host=host, port=5432, database=DSQL_DATABASE, user=DSQL_USER, password=token,
        ssl_context=_get_ssl_context(), timeout=DSQL_CANDIDATE_CONNECT_TIMEOUT)


def _resolve_dsql_endpoint_once():
    """On the FIRST connect, try each candidate hostname and PIN the first that reaches DSQL
    into DSQL_ENDPOINT, so all later connects reuse it. The probe connection is closed (the
    caller opens its own with the normal cached-token path). No-op once resolved. If no
    candidate connects, raise the single clear error from dsql_connect_first."""
    global DSQL_ENDPOINT, _dsql_endpoint_resolved
    if _dsql_endpoint_resolved:
        return
    with _dsql_resolve_lock:
        if _dsql_endpoint_resolved:
            return
        candidates = dsql_candidate_list(DSQL_ENDPOINT_CANDIDATES, DSQL_ENDPOINT)
        if len(candidates) <= 1:
            _dsql_endpoint_resolved = True   # nothing to choose; use DSQL_ENDPOINT as-is
            return
        conn, host = dsql_connect_first(
            candidates, _probe_connect_dsql,
            log=lambda h: print(f"  ↪ DSQL reachable on {h} (pinned for this run)", flush=True))
        try:
            conn.close()
        except Exception:
            pass
        if host != DSQL_ENDPOINT:
            DSQL_ENDPOINT = host
            _invalidate_dsql_token()   # re-mint the cached token for the pinned host
        _dsql_endpoint_resolved = True


def _unpin_dsql_endpoint():
    """Clear the resolved-endpoint pin so the next connect re-probes the full candidate list
    (the pinned host stopped working)."""
    global _dsql_endpoint_resolved
    with _dsql_resolve_lock:
        _dsql_endpoint_resolved = False


def connect_dsql(autocommit=False):
    # The IAM auth token is a BEARER credential valid for its whole ExpiresIn window (not
    # tied to one connection), and generation is a LOCAL SigV4 sign. CACHE one token and
    # reuse it across connections/recycles until it nears refresh age. ExpiresIn (2 h) is set
    # well above a connection's max life (~54-min recycle + ~5-min final chunk = ~59 min) so
    # a cached token never expires mid-connection; refresh every ~30 min keeps it young.
    # ENDPOINT FAILOVER: the first connect pins the reachable candidate host into
    # DSQL_ENDPOINT (PrivateLink vs public); later connects reuse it.
    _resolve_dsql_endpoint_once()
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
    _unpin_dsql_endpoint()   # pinned host stopped working -> re-probe candidates next time
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
    """Create the control schema + tables if missing. Every statement is IF NOT EXISTS, so the
    whole step is safe to repeat. Many CDC jobs starting at the same moment run these DDLs
    concurrently, and DSQL can abort a concurrent DDL (OC000/OC001, or a duplicate-key error
    on its catalog); those are retried with backoff instead of failing the run at startup."""
    for _attempt in range(1, 11):
        try:
            _ensure_control_tables_once()
            return
        except Exception as e:
            retriable = (is_occ_conflict(e) or is_schema_conflict(e) or is_unique_violation(e)
                         or is_transient_server_error(e) or is_broken_pipe_error(e))
            if not retriable or _attempt == 10:
                raise
            _bo = min(30.0, 0.5 * (2 ** (_attempt - 1))) * (0.5 + random.random())
            print(f"  ↻ control tables: attempt {_attempt}/10 hit a concurrent change ({e}); "
                  f"retry in {_bo:.1f}s", flush=True)
            time.sleep(_bo)


def _ensure_resolved_column(cur):
    """Idempotently ensure cdc_control.cdc_validation_failures has the 'resolved' boolean column
    on an OLDER control table that predates it, WITHOUT ever using a DEFAULT on ALTER.

    DSQL rejects `ALTER TABLE ... ADD COLUMN ... DEFAULT ...` at PARSE time (SQLSTATE 0A000),
    even with IF NOT EXISTS and even when the column already exists — so we must not emit a
    DEFAULT. Strategy:
      1. Probe information_schema.columns; if 'resolved' already exists, do nothing.
      2. Otherwise ADD COLUMN resolved boolean (NO DEFAULT) -> existing rows get NULL.
      3. Backfill those NULLs to false in batches strictly under the DSQL ~3000-rows/txn cap.
    Readers treat NULL as unresolved (`resolved IS NOT TRUE`), so the gate is correct even
    between steps 2 and 3. `cur` is an autocommit cursor on CONTROL_SCHEMA."""
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s LIMIT 1",
        (CONTROL_SCHEMA, "cdc_validation_failures", "resolved"))
    if cur.fetchone() is not None:
        return  # already present (fresh CREATE TABLE path or already-upgraded)
    print(f"  ↻ upgrading {CONTROL_SCHEMA}.cdc_validation_failures: adding 'resolved' column "
          f"(no DEFAULT; DSQL forbids DEFAULT on ALTER) + backfilling NULL->false in batches",
          flush=True)
    cur.execute(f"ALTER TABLE {CONTROL_SCHEMA}.cdc_validation_failures ADD COLUMN resolved boolean")
    # Backfill existing rows (NULL -> false) in batches comfortably under the ~3000-rows/txn cap.
    batch = 2000
    while True:
        cur.execute(
            f"UPDATE {CONTROL_SCHEMA}.cdc_validation_failures SET resolved = false "
            f"WHERE id IN (SELECT id FROM {CONTROL_SCHEMA}.cdc_validation_failures "
            f"WHERE resolved IS NULL LIMIT {batch})")
        n = cur.rowcount or 0
        if n:
            print(f"    …backfilled {n} row(s) resolved=false", flush=True)
        if n < batch:
            break


def _ensure_cdc_status_guardrail_columns(cur):
    """G9/G6 upgrade path: idempotently ensure cdc_control.cdc_status has the drift counters
    (full_load_rows, inserts_applied, deletes_applied) and the G6 override flag
    (allow_mass_delete) on an OLDER control table that predates them, WITHOUT a DEFAULT on
    ALTER (DSQL rejects ADD COLUMN ... DEFAULT at parse time, SQLSTATE 0A000). For each column:
    probe information_schema.columns; only if missing, ADD COLUMN with NO DEFAULT. Existing
    rows get NULL; readers coalesce NULL to 0/false. `cur` is an autocommit cursor on
    CONTROL_SCHEMA. A fresh CREATE TABLE already declares all of them, so this is a no-op in the
    steady state."""
    for col, decl in (("full_load_rows", "bigint"),
                      ("inserts_applied", "bigint"),
                      ("deletes_applied", "bigint"),
                      ("allow_mass_delete", "boolean")):
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s LIMIT 1",
            (CONTROL_SCHEMA, "cdc_status", col))
        if cur.fetchone() is not None:
            continue
        print(f"  ↻ upgrading {CONTROL_SCHEMA}.cdc_status: adding '{col}' (no DEFAULT; DSQL "
              f"forbids DEFAULT on ALTER)", flush=True)
        # No DEFAULT, no backfill needed: these are counters/flags read with COALESCE(...,0/false).
        cur.execute(f"ALTER TABLE {CONTROL_SCHEMA}.cdc_status ADD COLUMN {col} {decl}")


def write_audit_log(cur, table_name, action, rows_before, rows_deleted, reason,
                    task=None, job=None, run_id=None, execution_id=None):
    """G5: write ONE cdc_control.audit_log row describing a destructive action, BEFORE it runs,
    so the operation is attributable even if the job then crashes mid-op. `cur` is a cursor the
    caller commits (the audit write should commit before/with the destructive op). id is a
    Python uuid4 — never a server-side DEFAULT. Never raises on a formatting issue: the values
    are coerced to safe types. Returns the id written."""
    _id = str(uuid.uuid4())
    def _int_or_none(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    cur.execute(
        f'INSERT INTO {CONTROL_SCHEMA}.audit_log (id, event_time, task, job, run_id, '
        f'execution_id, table_name, action, rows_before, rows_deleted, reason) '
        f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (_id, utc_now_iso(), (task or None), (job or None), (run_id or None),
         (execution_id or None), table_name, str(action)[:64],
         _int_or_none(rows_before), _int_or_none(rows_deleted), str(reason)[:8000]))
    return _id


def guard_action_blocks(action):
    """EASE-GUARDRAILS (PURE): map a per-guard action string to whether the guard should BLOCK
    the table (True) or merely WARN and keep applying (False). Any value other than the exact
    string 'block' is treated as warn (the SAFE default never fails a run). Used by the soft CDC
    guards G7 (cdc_nopk_overmatch_action) and G8 (cdc_file_order_action)."""
    return str(action).strip().lower() == "block"


def emit_guard_warn_metric(label, guard):
    """EASE-GUARDRAILS best-effort CloudWatch metric emitted when a SOFT guard (G7/G8) trips in
    WARN mode (table NOT blocked). Lets operators alarm on 'a guard would have blocked but we
    kept going' without failing the run. Same namespace as the drift metric (GlueCDC/NonPK), so
    no new IAM. Never raises — a metric must not affect the apply."""
    if not cloudwatch or _OPTIONAL_API_DOWN["cloudwatch"]:
        return
    try:
        cloudwatch.put_metric_data(
            Namespace="GlueCDC/NonPK",
            MetricData=[{
                "MetricName": "DsqlGuardWarn",
                "Dimensions": [{"Name": "Table", "Value": label},
                               {"Name": "Guard", "Value": str(guard)}],
                "Value": 1.0,
                "Unit": "Count",
            }])
    except Exception as e:
        if _is_unreachable(e):
            _OPTIONAL_API_DOWN["cloudwatch"] = True
            print(f"    ⚠️ CloudWatch is not reachable from this job ({type(e).__name__}); the "
                  f"DsqlGuardWarn metric is turned off for this run (the warning is still "
                  f"logged + written to {CONTROL_SCHEMA}.audit_log).")
        else:
            print(f"    ⚠️ CW guard-warn-metric emit failed (non-fatal) for {label}: {e}")


def emit_drift_metric(label, delta):
    """G9 best-effort CloudWatch metric for row drift (|live − expected|). Never raises — a
    metric must not affect the apply. Emits into the SAME namespace the pipeline already uses
    (GlueCDC/NonPK) so no new IAM is needed (the glue role's cloudwatch:PutMetricData is scoped
    to that namespace). Value is the absolute drift; the sign is in the log + audit_log row."""
    if not cloudwatch or _OPTIONAL_API_DOWN["cloudwatch"]:
        return
    try:
        cloudwatch.put_metric_data(
            Namespace="GlueCDC/NonPK",
            MetricData=[{
                "MetricName": "DsqlRowDrift",
                "Dimensions": [{"Name": "Table", "Value": label}],
                "Value": float(abs(int(delta))),
                "Unit": "Count",
            }])
    except Exception as e:
        if _is_unreachable(e):
            _OPTIONAL_API_DOWN["cloudwatch"] = True
            print(f"    ⚠️ CloudWatch is not reachable from this job ({type(e).__name__}); the "
                  f"DsqlRowDrift metric is turned off for this run (drift is still logged + "
                  f"written to {CONTROL_SCHEMA}.audit_log).")
        else:
            print(f"    ⚠️ CW drift-metric emit failed (non-fatal) for {label}: {e}")


def _read_count_and_allow(conn_holder, dsql_schema, dsql_table, label):
    """G6 helper: read (current_row_count, allow_mass_delete) for a table on its own session.
    Read-only; best-effort. On a read failure returns (None, False) so guard_mass_delete fails
    SAFE (a None count makes it block). allow_mass_delete comes from the table's cdc_status row
    (NULL -> False). Uses a fresh short txn on conn_holder[0] and leaves it committed."""
    conn = conn_holder[0]
    cur = conn.cursor()
    cnt = None
    allow = False
    try:
        cur.execute(f"SELECT count(*) FROM {dsql_schema}.{dsql_table}")
        row = cur.fetchone()
        cnt = int(row[0]) if row and row[0] is not None else 0
        cur.execute(
            f"SELECT allow_mass_delete FROM {CONTROL_SCHEMA}.cdc_status WHERE table_name = %s",
            (label,))
        r2 = cur.fetchone()
        allow = bool(r2[0]) if r2 and r2[0] is not None else False
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"    ⚠️ {label}: could not read count/allow_mass_delete for the mass-delete "
              f"guard ({e}); treating as unknown (guard fails SAFE).")
        return None, False
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return cnt, allow


def _audit_destructive(conn_holder, label, action, rows_before, rows_deleted, reason):
    """G5 helper: write a cdc_control.audit_log row for a destructive/blocked CDC action, on the
    table's own session, as its own short committed txn. Best-effort — audit must never crash
    the apply (the primary signal is the TableBlocked the caller raises). Fills task/job/run/
    execution from this run's args."""
    conn = conn_holder[0]
    cur = conn.cursor()
    try:
        write_audit_log(cur, label, action, rows_before, rows_deleted, reason,
                        task=CONFIG_PREFIX, job=_run_arg("JOB_NAME"),
                        run_id=_run_arg("JOB_RUN_ID"), execution_id=_start_token())
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"    ⚠️ {label}: audit_log write failed (non-fatal) for {action}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _nopk_delete_precision_check(conn_holder, label, dsql_schema, dsql_table,
                                 delete_ops, content_where_fn):
    """G7 helper: before applying no-PK content DELETEs, verify none over-matches. Group the D
    ops by their content predicate (identical rows collapse to one predicate requiring that many
    deletes), then for each distinct predicate COUNT the matching target rows and compare with
    guard_nopk_delete_bound. Returns (ok, reason). Read-only; on a read error fails SAFE (not ok)
    so a precision check that cannot be performed blocks rather than risks over-deletion. Caps
    the number of distinct predicates probed (bounded cost) — beyond the cap it trusts the mass-
    delete guard already run and returns ok (the per-op probe is a precision refinement, not the
    volume guard)."""
    # Group identical delete predicates -> required delete count per predicate.
    required = {}
    for nop in delete_ops:
        w = content_where_fn(nop["values"])
        required[w] = required.get(w, 0) + 1
    _PROBE_CAP = 2000
    if len(required) > _PROBE_CAP:
        return True, ""
    conn = conn_holder[0]
    cur = conn.cursor()
    try:
        for where, need in required.items():
            cur.execute(f"SELECT count(*) FROM {dsql_schema}.{dsql_table} WHERE {where}")
            row = cur.fetchone()
            match_count = int(row[0]) if row and row[0] is not None else 0
            ok, why = guard_nopk_delete_bound(match_count, need)
            if not ok:
                conn.commit()
                return False, why
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, (f"could not verify no-PK delete precision ({e}) — blocking to be safe")
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return True, ""


def _set_blocked_status(conn_holder, label, reason):
    """Set a table's cdc_status.status='blocked' with the reason, on its own session as its own
    short committed txn. Used by the G6/G7/G8/G9 guards when they block a table from
    process_table (outside an apply txn). upsert_cdc_status creates the row if missing. Mirrors
    how record_exception blocks a table; best-effort (the returned 'blocked' result + the raised
    signal are the primary stop)."""
    conn = conn_holder[0]
    cur = conn.cursor()
    try:
        upsert_cdc_status(cur, label, status="blocked", error=str(reason)[:4000])
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"    ⚠️ {label}: could not persist 'blocked' status (non-fatal; the table is "
              f"still skipped this cycle): {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _advance_cdc_counters(cur, label, n_insert, n_delete):
    """G9: fold a file's applied INSERT/DELETE net-op counts into the table's running
    expectation counters on cdc_status (inserts_applied, deletes_applied). COALESCE so a NULL
    (older row / fresh add-if-missing column) starts at 0. Called by process_table on its own
    session AFTER a file fully applies (its own short txn), so a crash mid-file never double-
    counts (the file re-applies from its offset and this runs only once the file is done)."""
    if not n_insert and not n_delete:
        return
    cur.execute(
        f'UPDATE {CONTROL_SCHEMA}.cdc_status '
        f'SET inserts_applied = COALESCE(inserts_applied, 0) + %s, '
        f'    deletes_applied = COALESCE(deletes_applied, 0) + %s '
        f'WHERE table_name = %s',
        (int(n_insert or 0), int(n_delete or 0), label))


def _seed_full_load_rows_cur(cur, label):
    """G9 DRIFT BASELINE: write cdc_status.full_load_rows = the table's full-load row count
    (from _load_status.json, harvested into _FULL_LOAD_ROWS) IF it is still unset (NULL).

    Without this the drift expectation (full_load_rows + inserts − deletes) omits every row the
    full load inserted, so the FIRST drift check fires a false '+<full_load_rows>' alarm (e.g.
    sport_type logged delta +4,063 over a ~4,063-row full load). Idempotent: only sets a NULL ->
    never clobbers a real value, so re-running is safe and it never double-counts. Writes only
    when we actually know the baseline (label present in _FULL_LOAD_ROWS). Runs in the caller's
    txn (the one-time ensure_status_row op)."""
    baseline = _FULL_LOAD_ROWS.get(label)
    if baseline is None:
        return
    cur.execute(
        f'UPDATE {CONTROL_SCHEMA}.cdc_status '
        f'SET full_load_rows = %s WHERE table_name = %s AND full_load_rows IS NULL',
        (int(baseline), label))


def _maybe_run_drift_check(conn_holder, label, dsql_schema, dsql_table):
    """G9 drift detector, run periodically (every CDC_DRIFT_CHECK_MINUTES) per table. Reads the
    live DSQL count and the tracked expectation (full_load_rows + inserts_applied −
    deletes_applied) and compares via guard_drift. Beyond CDC_DRIFT_TOLERANCE it logs ERROR,
    writes cdc_control.audit_log, emits the DsqlRowDrift metric, and — when CDC_DRIFT_ACTION ==
    'block' — sets the table 'blocked'. Returns the status string to surface ('blocked' or None).
    Throttled by a module-level per-table last-run clock (_DRIFT_LAST_RUN). Setting
    CDC_DRIFT_CHECK_MINUTES to 0 turns it off. Never raises (best-effort; read-only unless it
    blocks)."""
    if CDC_DRIFT_CHECK_MINUTES <= 0:
        return None
    now = time.monotonic()
    due_after = CDC_DRIFT_CHECK_MINUTES * 60.0
    last = _DRIFT_LAST_RUN.get(label, 0.0)
    if last and (now - last) < due_after:
        return None
    _DRIFT_LAST_RUN[label] = now
    conn = conn_holder[0]
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT count(*) FROM {dsql_schema}.{dsql_table}")
        row = cur.fetchone()
        live = int(row[0]) if row and row[0] is not None else 0
        cur.execute(
            f'SELECT COALESCE(full_load_rows,0), COALESCE(inserts_applied,0), '
            f'COALESCE(deletes_applied,0) FROM {CONTROL_SCHEMA}.cdc_status '
            f'WHERE table_name = %s', (label,))
        r2 = cur.fetchone()
        flr, ins, dels = (int(r2[0]), int(r2[1]), int(r2[2])) if r2 else (0, 0, 0)
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"    ⚠️ {label}: drift check read failed (non-fatal, retry next window): {e}")
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
    fired, delta, expected = guard_drift(live, flr, ins, dels, CDC_DRIFT_TOLERANCE)
    if not fired:
        return None
    msg = (f"ROW DRIFT [{label}]: live DSQL count {live:,} vs expected {expected:,} "
           f"(full_load_rows+inserts-deletes); delta {delta:+,} exceeds tolerance "
           f"{CDC_DRIFT_TOLERANCE}. "
           + ("Target is SHORT — possible data loss." if delta < 0
              else "Target has MORE rows than expected — possible duplicate/replay."))
    print(f"    ❌ {msg}", flush=True)
    emit_drift_metric(label, delta)
    _audit_destructive(conn_holder, label, "drift_detected",
                       rows_before=live, rows_deleted=0, reason=msg)
    if CDC_DRIFT_ACTION == "block":
        _set_blocked_status(conn_holder, label, msg)
        return "blocked"
    return None


def _ensure_control_tables_once():
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
                status_time       timestamptz,
                full_load_rows    bigint,
                inserts_applied   bigint,
                deletes_applied   bigint,
                allow_mass_delete boolean
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
        # 'resolved' is in the CREATE TABLE so a fresh control schema never needs an ALTER.
        # The cutover gate counts only UNRESOLVED rows; readers treat NULL as unresolved via
        # `resolved IS NOT TRUE` (never `resolved = false`), so a backfilled NULL still gates.
        # An operator clears an investigated failure with
        #   UPDATE {CONTROL_SCHEMA}.cdc_validation_failures SET resolved=true WHERE table_name=...
        # (never DELETE — the audit row is kept).
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_validation_failures (
                id            uuid PRIMARY KEY,
                table_name    varchar(512),
                failure_time  timestamptz,
                cdc_file      varchar(1024),
                pk_value      varchar(1024),
                failure_type  varchar(64),
                details       varchar(8000),
                resolved      boolean
            )
        """)
        # UPGRADE PATH for a control table created by an OLDER build WITHOUT 'resolved'. DSQL
        # rejects `ADD COLUMN ... DEFAULT` at PARSE time (0A000) even when the column already
        # exists (IF NOT EXISTS does NOT short-circuit the DEFAULT parse), so we must NEVER emit
        # a DEFAULT on ALTER. Instead: probe information_schema.columns; only if 'resolved' is
        # missing do we ADD COLUMN with NO DEFAULT, then backfill existing rows to false in
        # batches well under the DSQL ~3000-rows/txn cap. New inserts always set resolved
        # explicitly (see _record_validation_failure).
        _ensure_resolved_column(cur)
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
        #     advances cdc_status.last_done_file; the file is then copied to processed/.
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
        # G5 AUDIT LOG — one durable row written BEFORE every destructive action (whole-table
        # blank, range blank, CDC mass-delete, _cdc_file purge) so the actual operation that
        # emptied/shrank a table is always attributable, even if the job then crashes. id is a
        # Python uuid4 (DSQL has no guaranteed server-side gen_random_uuid() DDL default — same
        # reason as cdc_apply_exceptions). CREATE TABLE only; no DEFAULT-on-ALTER anywhere.
        # EASE-GUARDRAILS: audit_log + the cdc_status guardrail columns are GUARDRAIL bookkeeping,
        # not the resume ledger. If their create/upgrade fails on an older or locked schema, a
        # guardrail must not fail CDC startup — warn and continue. The guards that write them are
        # all best-effort (they catch their own write errors), so a missing audit_log/column just
        # means that bookkeeping is skipped, never that the apply stops.
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.audit_log (
                    id              uuid PRIMARY KEY,
                    event_time      timestamptz,
                    task            varchar(512),
                    job             varchar(256),
                    run_id          varchar(256),
                    execution_id    varchar(512),
                    table_name      varchar(512),
                    action          varchar(64),
                    rows_before     bigint,
                    rows_deleted    bigint,
                    reason          varchar(8000)
                )
            """)
            # G9 upgrade path: an OLDER cdc_status table predates the drift counters + the G6
            # allow_mass_delete flag. Add any missing column with NO DEFAULT (DSQL forbids
            # DEFAULT-on-ALTER, SQLSTATE 0A000) — new rows set them explicitly; readers treat
            # NULL as "unknown/0". Same add-if-missing pattern as _ensure_resolved_column (B17).
            _ensure_cdc_status_guardrail_columns(cur)
        except Exception as _ge:
            print(f"  ⚠️ guardrail control bookkeeping (audit_log / cdc_status counters) could "
                  f"not be created/upgraded ({_ge}); CDC continues without it (the guards that "
                  f"use it are best-effort). guardrails_mode does not change this.")
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
    and about to be copied to processed/. Cursor-based; the caller (_commit_status via
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
    if not cloudwatch or count <= 0 or _OPTIONAL_API_DOWN["cloudwatch"]:
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
        if _is_unreachable(e):
            _OPTIONAL_API_DOWN["cloudwatch"] = True
            print(f"    ⚠️ CloudWatch is not reachable from this job ({type(e).__name__}); the "
                  f"SkippedUpdates metric is turned off for this run (skipped updates are still "
                  f"recorded in cdc_skipped_ops).")
        else:
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
                f'(id, table_name, failure_time, cdc_file, pk_value, failure_type, details, resolved) '
                f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (str(uuid.uuid4()), table_name, utc_now_iso(), cdc_file,
                 str(pk_value)[:1024], failure_type, str(details)[:8000], False))
        finally:
            c.close()
    except Exception as e:
        print(f"    ⚠️ could not record validation failure for {table_name} (non-fatal): {e}")


def _canon_timestamp(x):
    """Canonicalize a timestamp/date value (either the convert_value string form or a DB-native
    datetime/date) to a single comparable string, so an expected normalized string and the
    datetime pg8000 reads back compare equal. Strategy: parse to a datetime, drop any tzinfo
    (the whole pipeline is UTC — convert_value emits naive-UTC '+00:00' only when an offset was
    present), and format with microseconds. Anything unparseable falls back to a trimmed string
    with a trailing '+00:00'/' UTC'/'Z' removed so '...05' and '...05+00:00' still match."""
    if x is None:
        return None
    if isinstance(x, datetime):
        dt = x.replace(tzinfo=None) if x.tzinfo is not None else x
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    s = str(x).strip()
    # Try the common normalized/ISO shapes (with and without an explicit +00:00).
    for fmt in ("%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            continue
    # Unparseable: strip a trailing UTC marker so the two sides still line up.
    return re.sub(r'\s*(?:\+00:00|Z|UTC)\s*$', '', s, flags=re.IGNORECASE)


def _canon_bytea(x):
    """Canonicalize a bytea value to lowercase '\\x<hex>'. Accepts the convert_value string form
    ('\\xcbde'), a raw/0x-prefixed hex string, or the Python bytes/memoryview pg8000 returns."""
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray, memoryview)):
        return '\\x' + bytes(x).hex()
    s = str(x).strip()
    h = _BYTEA_PREFIX.sub('', s, count=1)
    return '\\x' + h.lower() if _HEX_BYTES.match(h) else s


def _values_match(expected, actual, category):
    """Compare an expected net-op value (already convert_value-normalized to the string form v4
    sends to DSQL) against the value READ BACK from the target. The target read returns DB-native
    Python types (datetime, Decimal, bool, bytes, int/float), so each category coerces BOTH sides
    to one canonical form before comparing — otherwise a correct row reads back as a false
    mismatch (e.g. '\\xcbde' vs b'\\xcb\\xde', '2026-01-02 03:04:05' vs a datetime, '1.50' vs
    Decimal('1.5')). None==None; a NULL on one side only is a real mismatch.

    This mirrors the normalisation the APPLY uses (convert_value / sql_literal casts):
      • NULL      : empty/sentinel -> None on both sides (_coerce_null); None==None.
      • uuid      : case-insensitive, dashes ignored.
      • boolean   : t/f/true/false/1/0 folded.
      • bytea     : lowercase '\\x'+hex, DMS hex vs DB bytes reconciled.
      • numeric   : Decimal/float compared by value (scale-insensitive: 1.50 == 1.5).
      • int kinds : integer value.
      • timestamp : parsed to a tz-naive UTC datetime (offset dropped, pipeline is UTC).
      • json      : parsed and compared structurally (key order / whitespace ignored).
      • text/char : exact (significant leading/trailing spaces preserved — convert_value keeps
                    them for text, so trimming here would mask a real diff).
    """
    if expected is None:
        return actual is None
    if actual is None:
        return False
    if category == 'uuid':
        return str(expected).strip().lower().replace("-", "") == \
               str(actual).strip().lower().replace("-", "")
    if category == 'boolean':
        norm = {"true": "t", "t": "t", "1": "t", "false": "f", "f": "f", "0": "f"}
        def _b(v):
            if isinstance(v, bool):
                return "t" if v else "f"
            s = str(v).strip().lower()
            return norm.get(s, s)
        return _b(expected) == _b(actual)
    if category == 'bytea':
        return _canon_bytea(expected) == _canon_bytea(actual)
    if category in ('timestamptz', 'timestamp', 'date'):
        ce, ca = _canon_timestamp(expected), _canon_timestamp(actual)
        return ce == ca
    if category in ('json', 'jsonb'):
        try:
            return json.loads(str(expected)) == (actual if not isinstance(actual, (str, bytes, bytearray))
                                                 else json.loads(actual if isinstance(actual, str)
                                                                 else actual.decode('utf-8')))
        except (ValueError, TypeError):
            return str(expected).strip() == str(actual).strip()
    if category in ('integer', 'bigint', 'smallint'):
        try:
            return int(float(expected)) == int(float(actual))
        except (ValueError, TypeError):
            return str(expected).strip() == str(actual).strip()
    if category in ('numeric', 'float', 'double', 'real'):
        try:
            from decimal import Decimal
            return Decimal(str(expected)) == Decimal(str(actual))
        except Exception:  # noqa: BLE001 - any parse failure -> fall back to string compare
            try:
                return abs(float(expected) - float(actual)) < 1e-9
            except (ValueError, TypeError):
                return str(expected).strip() == str(actual).strip()
    # text/char/varchar and anything else: EXACT (preserve significant whitespace + case; the
    # apply stored the value verbatim via a ::text cast, so a trimmed/lowered compare here would
    # hide a real target drift).
    return str(expected) == str(actual)


def validate_file_netops(ctx, cdc_key, netops, col_category):
    """TIER-2 (deferred, sampled) validation for ONE just-committed file. For a sample of
    the file's net-ops, re-read the target row by PK and compare to the expected net-op
    image (INSERT -> present + values match; DELETE -> absent). Retry a mismatch once after
    a short delay (absorbs any lag), then record persistent mismatches to
    cdc_validation_failures (resolved=false). Runs on its OWN autocommit connection AFTER the
    file committed — never inside the apply transaction, so it can't affect apply latency.

    NO FALSE FAILURES ON LATER-FILE OVERWRITES: a deferred by-key check sees the CURRENT
    target, so if a LATER CDC file re-inserts a key this file DELETEd (or changes a key this
    file INSERTed) the raw check would wrongly report MISSING_DELETE / RECORD_DIFF. Two
    guards prevent recording such a false failure, so a validation failure always means real
    target drift (and therefore safely gates cutover):
      1. Per-table serial apply means no later file for THIS table commits during this pass
         (the apply loop is blocked here), so the retry delay cannot straddle a newer file.
      2. Before RECORDING a persistent mismatch we re-check the per-file ledger: if ANY file
         for this table with a filename LATER than this one is already committed/done, this
         file's key may have been superseded, so we SKIP it (do not record) rather than record
         a false failure. A genuine drift on a key no later file touched still persists and is
         recorded. (Supersession is keyed on the ledger, not per-key, so it is conservative:
         it can only ever SKIP, never invent, a failure.)

    Returns the number of persistent discrepancies recorded (0 = clean)."""
    if not VALIDATION_ENABLED or not netops:
        return 0
    label = ctx["label"]
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]
    # Validation re-reads each sampled net-op by its TARGETING key (apply_key = PK or logical
    # key), matching how collapse/apply keyed the row. Only reached for keyed (Tier-1) tables;
    # keyless tables never produce pk-keyed netops (the router diverts them earlier).
    pk_col = ctx["apply_key"]
    pk_suffix = CAST_SUFFIX.get(col_category.get(pk_col, 'varchar'), '')
    # This file's basename, used to detect LATER committed files for the same table.
    _this_base = cdc_key.split("/")[-1]

    def _superseded_by_later_file(conn):
        """True if a LATER CDC file for this table is already committed/done in the ledger —
        meaning the current target may reflect that later file, so a mismatch on this (older)
        file's keys must NOT be recorded. Best-effort: on any error, return False (do not
        suppress a real failure)."""
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    f'SELECT 1 FROM {CONTROL_SCHEMA}.cdc_file_status '
                    f'WHERE table_name = %s '
                    f'AND (status = %s OR all_rows_committed = %s) '
                    f'AND split_part(cdc_file, %s, -1) > %s LIMIT 1',
                    (label, "done", True, "/", _this_base))
                return cur.fetchone() is not None
            finally:
                cur.close()
        except Exception:
            return False

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
            # Still mismatched after the retry. Before recording it as a real failure (which
            # will block cutover), make sure a LATER committed file for this table hasn't
            # superseded this key — if so the current target reflects that later file, not a
            # genuine drift, so skip rather than record a false positive.
            if _superseded_by_later_file(conn):
                print(f"    ↪ VALIDATION {label} {cdc_key.split('/')[-1]}: key "
                      f"{str(netop['pk'])[:60]} superseded by a later committed file; "
                      f"not recorded.")
                continue
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


def _load_cdc_owners():
    """The ONE persisted CDC-ownership record (fork design): cdcOwners{label->owner} inside the
    task registry config/_task/<suffix>/_jobs.json. Returns the {label: owner} dict, or None when
    there is no registry (a pre-fork task: every table defaults to owner 'main').

    The registry lives at the TASK config prefix. A fork CDC job runs with its OWN (per-fork)
    config_prefix, so the SM passes the absolute task-level key as --cdc_owners_key; the main CDC
    job (task-level config_prefix) derives it from CONFIG_PREFIX when the arg is absent."""
    if CDC_OWNERS_KEY:
        bucket, key = (BUCKET, CDC_OWNERS_KEY) if not str(CDC_OWNERS_KEY).startswith("s3://") \
            else split_s3(CDC_OWNERS_KEY)
    else:
        bucket, key = split_s3(CONFIG_PREFIX.rstrip('/') + '/_jobs.json')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            print(f"  ℹ️ no job registry at s3://{bucket}/{key}; every table defaults to owner "
                  f"'main' (pre-fork task).", flush=True)
            return None
        raise
    doc = json.loads(obj['Body'].read().decode('utf-8'))
    owners = doc.get('cdcOwners') or {}
    print(f"  ↪ CDC owners loaded ({len(owners)} table(s)); this job applies owner="
          f"{CDC_OWNER_SELF!r}.", flush=True)
    return owners


def load_table_config(entry):
    """Read a table's per-table config JSON (column_mapping, type_categories, PK)."""
    bucket, key = split_s3(entry['config_s3_path'])
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def _read_load_status_doc(bucket, key):
    """Read ONE _load_status.json -> (last_modified, {label: status}). Raises on any error.

    SIDE EFFECT (G9): also harvests each table's full-load "rows" count into the module-level
    _FULL_LOAD_ROWS map so the drift baseline (full_load_rows) can be seeded. Caller merges
    files oldest-first, so a later file's row count overwrites an earlier one (newest wins),
    matching the status merge. A missing/non-numeric "rows" is skipped (leaves any prior value)."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    doc = json.loads(obj['Body'].read().decode('utf-8'))
    tables = doc.get('tables', {}) if isinstance(doc, dict) else {}
    for k, v in tables.items():
        rows = (v or {}).get('rows')
        if rows is not None:
            try:
                _FULL_LOAD_ROWS[k] = int(rows)
            except (TypeError, ValueError):
                pass
    return obj.get('LastModified'), {k: (v or {}).get('status') for k, v in tables.items()}


def _load_status_err_code(e):
    return (e.response.get("Error", {}).get("Code") if hasattr(e, "response") else "") or ""


def load_full_load_status():
    """Read the full-load status -> {label: status_string}. Re-read each poll cycle so a table
    becomes CDC-eligible as soon as its load finishes. Shape of each file matches Job 2:
    {"tables": {"<schema.table>": {"status": "done", ...}}}.

    WHERE THE STATUS LIVES: the startup state machine runs the loader once PER GROUP, each
    with --config_prefix = <CONFIG_PREFIX>_orchestrator/group-<k>/, so Job 2 writes one
    status file per group. This (single, task-level) CDC job therefore merges:
      - <CONFIG_PREFIX>_load_status.json                    (single-run / legacy layout)
      - <CONFIG_PREFIX>_orchestrator/group-*/_load_status.json   (orchestrated layout)
    Files are merged oldest-first by LastModified, so the newest file wins for a table that
    appears in more than one (e.g. a stale group left over from an earlier plan).
    LOAD_STATUS_KEY, if set, reads exactly that one key instead.

    Fail-closed: a missing/unreadable/unparseable file contributes nothing, so its tables
    stay 'not done' and CDC keeps waiting. Misconfigs (access denied, bad JSON) are LOUD so a
    stuck pipeline isn't mistaken for a slow full load."""
    if LOAD_STATUS_KEY:
        candidates = [(BUCKET, LOAD_STATUS_KEY)]
    else:
        bucket, base = split_s3(CONFIG_PREFIX.rstrip('/') + '/')
        candidates = [(bucket, base + '_load_status.json')]
        try:
            pag = s3.get_paginator('list_objects_v2')
            for page in pag.paginate(Bucket=bucket, Prefix=base + '_orchestrator/', Delimiter='/'):
                for cp in page.get('CommonPrefixes', []) or []:
                    p = cp.get('Prefix', '')
                    if p[len(base) + len('_orchestrator/'):].startswith('group-'):
                        candidates.append((bucket, p + '_load_status.json'))
        except Exception as e:
            print(f"  ⚠️ CANNOT LIST s3://{bucket}/{base}_orchestrator/ to find the per-group "
                  f"_load_status.json files: {e}. CDC gate stays CLOSED (fail-closed). Check "
                  f"IAM s3:ListBucket on the bucket — this is a MISCONFIG, not a slow full load.")
            return {}

    found = []
    for bucket, key in candidates:
        try:
            found.append(_read_load_status_doc(bucket, key))
        except Exception as e:
            code = _load_status_err_code(e)
            if code in ("NoSuchKey", "404", "NoSuchBucket"):
                continue   # that group hasn't written status yet (expected mid-load)
            if code:
                print(f"  ⚠️ CANNOT READ _load_status.json at s3://{bucket}/{key}: {e}. "
                      f"Its tables stay 'not done' (fail-closed). Check LOAD_STATUS_KEY / IAM "
                      f"s3:GetObject — this is a MISCONFIG, not a slow full load.")
            else:
                print(f"  ⚠️ _load_status.json at s3://{bucket}/{key} is present but UNPARSEABLE: "
                      f"{e}. Its tables stay 'not done'. Fix the file (must be JSON "
                      f'{{"tables": {{"<schema.table>": {{"status": "done"}}}}}}).')

    if not found:
        where = (f"s3://{candidates[0][0]}/{candidates[0][1]}" if LOAD_STATUS_KEY else
                 f"{CONFIG_PREFIX} (top level or _orchestrator/group-*/)")
        print(f"  ℹ️ no _load_status.json found yet under {where} — treating all tables as "
              f"'full load not done' (expected pre-load).")
        return {}

    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    found.sort(key=lambda f: f[0] or _epoch)
    merged = {}
    for _, statuses in found:
        merged.update(statuses)
    return merged


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


class MultiColumnKeyTable(Exception):
    """Raised by build_table_context for a table whose primary key has more than one column.
    This CDC job does not apply those tables: its keyed path targets one key column, and its
    keyless path would skip every UPDATE. They are left to a separate CDC job built for
    multi-column keys. main() lists them at startup and never touches them."""


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
    # MULTI-COLUMN PRIMARY KEY -> not this job's table. Checked before anything else (a
    # declared logical key does not override it): applying such a table here would key it on
    # one column or treat it as keyless and skip its UPDATEs. Nothing is written for it, so
    # its cdc_control rows stay free for the job that does apply it.
    if len(pk_cols) > 1:
        raise MultiColumnKeyTable(
            f"{dsql_schema}.{dsql_table} has a multi-column primary key "
            f"({', '.join(str(c) for c in pk_cols)})")
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
    # Tier hint from configuration alone:
    #   Tier 1 = has an apply key (single-column PK or logical key) -> I/U/D applied by key.
    #   keyless (no PK, no logical key) -> Tier 2: INSERT/DELETE applied, UPDATE skipped and
    #   logged in cdc_skipped_ops (the table is not blocked).
    #   Multi-column PK tables never reach here (MultiColumnKeyTable above).
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
    for _w in _null_rules_unknown_warnings(
            dsql_schema, dsql_table, set(type_categories) | set(target_columns)):
        print(f"  ⚠️ {label}: {_w}")
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
def _note_dms_unreachable(e):
    """Log once when the DMS API can't be reached (column renames then apply as add-column)."""
    if _is_unreachable(e) and not _OPTIONAL_API_DOWN["dms_logged"]:
        _OPTIONAL_API_DOWN["dms_logged"] = True
        print(f"  ⚠️ The DMS API is not reachable from this job ({type(e).__name__}). Changes are "
              f"still applied; only column-rename detection is off (a renamed column is added as "
              f"a new column). Add a DMS VPC endpoint or NAT route to enable it.", flush=True)


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
        except Exception as e:
            _note_dms_unreachable(e)
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
            except Exception as e:
                _note_dms_unreachable(e)
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
_REFIND_EVERY_SECONDS = 300   # how often to look for a missing table folder in another case


def _ci_folder(name, folders):
    """Exact match, else the single case-insensitive match, else None (also None if ambiguous)."""
    if name in folders:
        return name
    ci = [f for f in folders if f.lower() == name.lower()]
    return ci[0] if len(ci) == 1 else None


def _subfolders(prefix):
    names, token = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        resp = _s3_retry(lambda: s3.list_objects_v2(**kw), f"list {prefix or '/'}")
        names += [cp["Prefix"][len(prefix):].rstrip("/") for cp in resp.get("CommonPrefixes", []) or []]
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return [n for n in names if n]


def _refind_table_folder(ctx):
    """The table's folder doesn't exist under the name in the index. That happens when the table
    was empty at full load (discovery could only guess the letter case DMS would use) and DMS has
    since created it in another case. Look at most every _REFIND_EVERY_SECONDS; if a folder that
    differs only in case exists, switch this table to it. Returns True if it switched."""
    now = time.monotonic()
    if now - ctx.get("_refind_at", -1e12) < _REFIND_EVERY_SECONDS:
        return False
    ctx["_refind_at"] = now
    root = (CDC_ROOT or "").strip("/. ")
    base = f"{root}/" if root else ""
    parts = ctx["prefixes"]["cdc"][len(base):].rstrip("/").split("/")
    if len(parts) != 2:
        return False
    try:
        s = _ci_folder(parts[0], _subfolders(base))
        tb = _ci_folder(parts[1], _subfolders(f"{base}{s}/")) if s else None
    except Exception as e:
        print(f"    ⚠️ {ctx['label']}: could not look for its folder in another letter case: {e}")
        return False
    if not tb:
        return False
    new = f"{base}{s}/{tb}/"
    if new == ctx["prefixes"]["cdc"]:
        return False
    print(f"  ↪ {ctx['label']}: DMS folder is {new} (expected {ctx['prefixes']['cdc']}; "
          f"letter case differs). Using it.", flush=True)
    ctx["prefixes"] = {"cdc": new, "processed": new + "processed/", "failed": new + "failed/"}
    return True


def list_cdc_files(ctx, _retried=False):
    """List a table's pending CDC CSVs (sorted by name == DMS timestamp order)."""
    prefix = ctx["prefixes"]["cdc"]
    files = []
    token = None
    # Files younger than MIN_FILE_AGE_SECONDS may still be mid-write by DMS -> defer them
    # to the next poll (see MIN_FILE_AGE_SECONDS). Compare S3 LastModified to now (UTC).
    now = datetime.now(timezone.utc)
    skipped_too_new = 0
    saw_any = False   # anything at all under the folder (files or subfolders)
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        if resp.get("Contents") or resp.get("CommonPrefixes"):
            saw_any = True
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
    if not saw_any and not _retried and _refind_table_folder(ctx):
        return list_cdc_files(ctx, _retried=True)
    files.sort()
    if skipped_too_new:
        print(f"    ⏳ {ctx['label']}: deferring {skipped_too_new} file(s) < "
              f"{MIN_FILE_AGE_SECONDS}s old (possibly still being written)")
    return files


def list_load_files(ctx):
    """G8 helper: list a table's full-load LOAD*.csv files in its CDC folder (sorted). CDC
    normally EXCLUDES these (list_cdc_files skips LOAD*). The guardrail needs to SEE them: a new
    LOAD* appearing after CDC started is a DMS 'reload table' that must block the table rather
    than be silently ignored. Read-only; applies the same processed/failed exclusion as
    list_cdc_files. Never raises (returns [] on any list error)."""
    prefix = ctx["prefixes"]["cdc"]
    out = []
    token = None
    try:
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                key = o["Key"]
                if not (key.endswith(".csv") and "/processed/" not in key
                        and "/failed/" not in key):
                    continue
                fname = key.rsplit("/", 1)[-1]
                if fname.upper().startswith("LOAD"):
                    out.append(key)
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
    except Exception as e:
        print(f"    ⚠️ {ctx['label']}: could not list LOAD* files for the reload guard "
              f"({e}); skipping that check this cycle.")
        return []
    out.sort()
    return out


def read_cdc_file(key):
    """Read a CDC CSV -> (header_lowercased, data_rows). First row is the header."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    content = obj['Body'].read().decode('utf-8')
    rows = list(csv.reader(io.StringIO(content)))
    if not rows:
        return [], []
    header = [c.lower().strip() for c in rows[0]]
    return header, rows[1:]


# =============================================================================
# PROCESSED/ COPIES (never move) + S3 RETRIES + PER-TABLE MANIFEST
# =============================================================================
# After a file is fully applied, it is COPIED to <table>/processed/. The original is NEVER
# deleted, so a failed or throttled S3 call can't lose a file: the worst case is a missing copy,
# which the next cycle retries (refresh_processed). Re-applying is impossible because files are
# skipped by the high-water mark (cdc_status.last_done_file), not by where they sit.
#
# Each table also gets <table>/processed/_manifest.json with COUNTS only (no file names), e.g.
#   {"table": "s.t", "status": "idle", "cdc_files_in_folder": 120, "pending_apply": 0,
#    "applied_in_folder": 120, "copied_to_processed": 120, "pending_copy": 0,
#    "processed_folder_files": 120, "applied_total_ledger": 120, "all_done": true, ...}
# all_done = nothing left to apply AND nothing left to copy.
#
# NOTE: never DELETE a table's cdc_status row to unblock it (set status='active' instead).
# With the originals kept in place, a missing row would replay the table's files from the start.
import random as _cdc_rnd

S3_COPY_MAX_ATTEMPTS = 6            # per S3 call, on top of boto3's own adaptive retries
S3_COPY_BASE_BACKOFF_SECONDS = 0.5  # exponential with jitter: 0.5, 1, 2, 4, 8 s (x 0.5-1.5)
MANIFEST_NAME = "_manifest.json"
MANIFEST_IDLE_REFRESH_SECONDS = 300 # an idle table refreshes its manifest at most every 5 min
COPY_CATCHUP_MAX_PER_CYCLE = 200    # missing copies re-tried per table per cycle
_S3_PERMANENT = {"NoSuchKey", "404", "NotFound", "NoSuchBucket", "AccessDenied", "403",
                 "InvalidObjectState", "InvalidRequest"}
_manifest_state = {}                # label -> {"sig": <counts>, "at": monotonic}
_manifest_lock = threading.Lock()


def _s3_code(e):
    try:
        return str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", "") or "")
    except Exception:
        return ""


def _s3_retry(fn, what):
    """Run an S3 call with retries + jittered exponential backoff. Permanent errors (missing
    object, access denied) are raised at once; anything else (throttling, 5xx, timeouts,
    connection resets) is retried up to S3_COPY_MAX_ATTEMPTS times."""
    last = None
    for attempt in range(1, S3_COPY_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            if _s3_code(e) in _S3_PERMANENT or attempt == S3_COPY_MAX_ATTEMPTS:
                raise
            delay = S3_COPY_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)) * (0.5 + _cdc_rnd.random())
            print(f"    ↻ S3 {what}: attempt {attempt}/{S3_COPY_MAX_ATTEMPTS} failed "
                  f"({_s3_code(e) or type(e).__name__}); retry in {delay:.1f}s", flush=True)
            time.sleep(delay)
    raise last


def _s3_head_or_none(key):
    """head_object with retries; None if the object does not exist."""
    try:
        return _s3_retry(lambda: s3.head_object(Bucket=BUCKET, Key=key), f"head {key}")
    except Exception as e:
        if _s3_code(e) in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def copy_to_processed(key, dest_prefix):
    """Copy an applied CDC file to dest_prefix (processed/). NEVER deletes the original.
    Idempotent: an existing copy of the same size is left alone. The copy is verified by
    size before it counts. Returns "copied" or "already"; raises after retries are exhausted
    (the caller treats that as non-fatal; refresh_processed retries next cycle)."""
    new_key = dest_prefix + key.rsplit("/", 1)[-1]
    src = _s3_head_or_none(key)
    if src is None:
        raise RuntimeError(f"source {key} not found; cannot copy to {dest_prefix}")
    size = src.get("ContentLength")
    dst = _s3_head_or_none(new_key)
    if dst is not None and dst.get("ContentLength") == size:
        return "already"
    _s3_retry(lambda: s3.copy_object(Bucket=BUCKET, Key=new_key,
                                     CopySource={"Bucket": BUCKET, "Key": key}),
              f"copy {key.rsplit('/', 1)[-1]}")
    dst = _s3_head_or_none(new_key)
    if dst is None or dst.get("ContentLength") != size:
        raise RuntimeError(f"copy of {key} to {new_key} could not be verified "
                           f"(source {size} bytes, copy "
                           f"{None if dst is None else dst.get('ContentLength')} bytes)")
    return "copied"


def _list_cdc_names(prefix):
    """Names of the .csv files directly under prefix (no subfolders), excluding full-load
    LOAD*.csv files. Unlike list_cdc_files, includes files still being written."""
    names, token = set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        resp = _s3_retry(lambda: s3.list_objects_v2(**kw), f"list {prefix}")
        for o in resp.get("Contents", []) or []:
            name = o["Key"][len(prefix):]
            if name.endswith(".csv") and "/" not in name and not name.upper().startswith("LOAD"):
                names.add(name)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return names


def _manifest_due(label, force):
    if force:
        return True
    with _manifest_lock:
        st = _manifest_state.get(label)
    return st is None or (time.monotonic() - st["at"]) >= MANIFEST_IDLE_REFRESH_SECONDS


def _ledger_done_count(conn, label):
    """Files this table has marked 'done' in cdc_file_status (None if it can't be read)."""
    try:
        try:
            conn.rollback()
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute(f"SELECT count(*) FROM {CONTROL_SCHEMA}.cdc_file_status "
                    f"WHERE table_name = %s AND status = %s", (label, "done"))
        n = cur.fetchone()[0]
        cur.close()
        conn.commit()
        return int(n)
    except Exception:
        return None


def refresh_processed(ctx, last_done, status, ledger_done=None, force=False):
    """Retry any missing processed/ copies and write the table's _manifest.json. Runs after
    every busy cycle and at most every MANIFEST_IDLE_REFRESH_SECONDS when idle. NEVER raises:
    a problem here must not affect applying changes."""
    label = ctx.get("label", "?")
    try:
        if not _manifest_due(label, force):
            return None
        base = ctx["prefixes"]["cdc"]
        proc = ctx["prefixes"]["processed"]
        in_folder = _list_cdc_names(base)
        applied = sorted(n for n in in_folder if last_done and (base + n) <= last_done)
        copied = _list_cdc_names(proc)
        missing = [n for n in applied if n not in copied]
        copy_errors = 0
        last_err = None
        for n in missing[:COPY_CATCHUP_MAX_PER_CYCLE]:
            try:
                copy_to_processed(base + n, proc)
                copied.add(n)
            except Exception as e:
                copy_errors += 1
                last_err = str(e)[:300]
        pending_copy = sum(1 for n in applied if n not in copied)
        pending_apply = len(in_folder) - len(applied)
        counts = {
            "status": status,
            "cdc_files_in_folder": len(in_folder),
            "pending_apply": pending_apply,
            "applied_in_folder": len(applied),
            "copied_to_processed": len(applied) - pending_copy,
            "pending_copy": pending_copy,
            "processed_folder_files": len(copied),
            "applied_total_ledger": ledger_done,
            "copy_errors_this_cycle": copy_errors,
            "all_done": pending_apply == 0 and pending_copy == 0,
        }
        with _manifest_lock:
            prev = _manifest_state.get(label)
        if prev is not None and prev["sig"] == counts and not force:
            with _manifest_lock:
                _manifest_state[label] = {"sig": counts, "at": time.monotonic()}
            return counts
        doc = {"table": label, "updated_at": utc_now_iso()}
        doc.update(counts)
        if last_err:
            doc["last_copy_error"] = last_err
        body = (json.dumps(doc, indent=2) + "\n").encode("utf-8")
        _s3_retry(lambda: s3.put_object(Bucket=BUCKET, Key=proc + MANIFEST_NAME, Body=body,
                                        ContentType="application/json"),
                  f"write {proc}{MANIFEST_NAME}")
        with _manifest_lock:
            _manifest_state[label] = {"sig": counts, "at": time.monotonic()}
        if copy_errors or pending_copy:
            print(f"    ⚠️ {label}: {pending_copy} applied file(s) not yet copied to processed/ "
                  f"(originals kept; retried next cycle){': ' + last_err if last_err else ''}",
                  flush=True)
        return counts
    except Exception as e:
        print(f"    ⚠️ {label}: processed/ manifest refresh failed (non-fatal): {e}", flush=True)
        return None


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


class _PositionMoved(Exception):
    """Internal signal (NOT a block, NOT an error): this table's checkpoint in cdc_status is no
    longer where this run left it, so ANOTHER CDC run is applying the same table (a second
    Glue job for the same tables, an old per-task workflow next to the shared one, a hand-made
    copy of the job), or an operator set the table to 'blocked'. Nothing from the step that
    detected it was committed. process_table leaves the table for this cycle; the next cycle
    re-reads the checkpoint and carries on from wherever it now is."""


def _pos(in_progress_file, last_offset, last_done_file):
    """Normalized checkpoint position: (in_progress_file, last_offset, last_done_file).
    last_offset only means something while a file is in progress."""
    ipf = in_progress_file or None
    return (ipf, int(last_offset or 0) if ipf else 0, last_done_file or None)


def _fmt_pos(p):
    f = (p[0] or "-").rsplit("/", 1)[-1]
    d = (p[2] or "-").rsplit("/", 1)[-1]
    return f"in-progress={f}@{p[1]} last-done={d}"


def check_position_cur(cur, table_name, expected, target):
    """POSITION FENCE, run FIRST inside every transaction that moves a table's checkpoint (each
    data chunk, the file-done step, the idle step). Returns:
      "ok"      the checkpoint is still `expected`: this run makes the next step;
      "already" it is exactly `target`: this step is already committed (this run's own
                earlier attempt, e.g. a connection that dropped after COMMIT, or another run
                that made the identical step) -> do NOT apply it again;
    and raises _PositionMoved otherwise, or if the table was set to 'blocked'.
    Why this is enough: every such transaction also UPDATEs the same cdc_status row, so when
    two runs race, DSQL aborts the later committer (OC000); its retry re-reads the checkpoint
    here and sees it moved. Each step therefore commits at most once and in order, however
    many runs apply the table, and an older chunk can never be re-applied over newer data."""
    cur.execute(
        f'SELECT in_progress_file, last_offset, last_done_file, status '
        f'FROM {CONTROL_SCHEMA}.cdc_status WHERE table_name = %s', (table_name,))
    row = cur.fetchone()
    got = _pos(row[0], row[1], row[2]) if row else _pos(None, 0, None)
    if row is not None and str(row[3] or "").lower() == "blocked":
        raise _PositionMoved(f"{table_name} was set to 'blocked' while this run was applying it")
    if got == tuple(expected):
        return "ok"
    if target is not None and got == tuple(target):
        return "already"
    raise _PositionMoved(f"{table_name}: checkpoint is now {_fmt_pos(got)}, but this run "
                         f"left it at {_fmt_pos(expected)}")


# =============================================================================================
# SAFETY GUARDRAILS — shared pure decision helpers (G6/G7/G8/G9). BYTE-IDENTICAL between
# scripts/glue_cdc_continuous.py and scripts/glue_cdc_composite.py (a static test enforces it).
# Each is a pure function (no DB/S3/clock): it takes already-read numbers/strings and returns a
# decision, so the apply paths stay testable offline and the guard logic can never diverge
# between the single-PK and composite engines. All default SAFE (block / refuse). The callers
# wire them into the hot paths and own the actual DSQL/CloudWatch side effects.
# =============================================================================================

def guard_mass_delete(current_count, n_delete, allow_mass_delete,
                      max_fraction, max_rows):
    """G6 CDC mass-delete guard. Decide whether applying `n_delete` net DELETE ops to a table
    that currently has `current_count` rows is a suspected mass-delete that must be BLOCKED
    (apply nothing from this file / cycle) rather than silently wiping the table.

    BLOCK  iff  (not allow_mass_delete)
            AND max_fraction < 1              (fraction >= 1 turns the guard off entirely)
            AND current_count > 0
            AND n_delete > max_fraction * current_count   (removes > the fraction)
            AND n_delete > max_rows                        (AND > the absolute floor)

    Requiring BOTH thresholds means a tiny table's normal churn (e.g. delete 3 of 4 rows) never
    trips it, while a corrupt/misparsed file deleting most of a large table does. Returns
    (blocked: bool, reason: str). reason is "" when not blocked."""
    try:
        cc = int(current_count or 0)
        nd = int(n_delete or 0)
    except (TypeError, ValueError):
        # Unparseable counts -> fail SAFE (block) with a clear reason; the caller surfaces it.
        return True, (f"mass-delete guard could not read counts "
                      f"(current={current_count!r}, deletes={n_delete!r}) — blocking to be safe")
    if allow_mass_delete:
        return False, ""
    if max_fraction is None or float(max_fraction) >= 1.0:
        return False, ""      # guard disabled by setting
    if cc <= 0 or nd <= 0:
        return False, ""
    frac_hit = nd > float(max_fraction) * cc
    rows_hit = nd > int(max_rows)
    if frac_hit and rows_hit:
        pct = (100.0 * nd / cc) if cc else 0.0
        return True, (f"would DELETE {nd:,} of {cc:,} row(s) ({pct:.1f}%), exceeding both "
                      f"cdc_max_delete_fraction={max_fraction} and cdc_max_delete_rows="
                      f"{int(max_rows):,}. Suspected mass delete (bad/corrupt file) vs a real "
                      f"one. Nothing from this file was applied; the table is BLOCKED. Verify "
                      f"against the SOURCE, then to apply this one file set "
                      f"cdc_status.allow_mass_delete=true for this table (UPDATE, never DELETE "
                      f"the row) and set status='active'. See RUNBOOK 'Safety guardrails'.")
    return False, ""


def guard_nopk_delete_bound(match_count, required_deletes):
    """G7 no-PK delete precision. A full-row content DELETE on a no-PK table deletes EVERY row
    that matches the predicate. Under the no-duplicate-rows contract that is one row per D op,
    but if the target holds duplicates a single content DELETE removes more rows than the file's
    D ops intend. Given how many rows currently MATCH the content predicate (`match_count`) and
    how many the file requires removed (`required_deletes`), decide:
      ok=True            matches <= required -> safe to apply the content DELETE as-is;
      ok=False (block)   matches  > required -> would over-delete. DSQL has no ctid and no
                         portable bounded-delete we can verify offline, so the SAFE action is to
                         BLOCK the table with a clear error rather than guess which rows to keep.
    Returns (ok: bool, reason: str)."""
    try:
        m = int(match_count or 0)
        r = int(required_deletes or 0)
    except (TypeError, ValueError):
        return False, (f"no-PK delete guard could not read counts (match={match_count!r}, "
                       f"required={required_deletes!r}) — blocking to be safe")
    if m <= r:
        return True, ""
    return False, (f"a no-PK content-match DELETE matches {m} target row(s) but the file "
                   f"requires only {r} removed — duplicate rows would be over-deleted. The "
                   f"table is BLOCKED (nothing applied). Investigate the duplicates / source, "
                   f"then resume with status='active'. See RUNBOOK 'Safety guardrails' (G7).")


def purge_predicate_is_exact(sql, file_tag_column, file_literal):
    """G7 _cdc_file purge safety. Confirm a no-PK file-reload purge statement deletes ONLY the
    rows tagged with the EXACT current file key — never a prefix/LIKE/other file. Expects the
    canonical shape `DELETE FROM <t> WHERE "<tag>" = <literal>` with the given tag column and
    file literal and no additional predicate. Returns True iff it is an exact single-equality
    on the tag column to this file's literal."""
    flat = " ".join(str(sql).split())
    low = flat.lower()
    if not low.startswith("delete from "):
        return False
    # No wildcard / range / set operators that could widen the match beyond one exact key.
    for bad in (" like ", " ilike ", " in (", " <", " >", " != ", "<>", " or ", " not "):
        if bad in low:
            return False
    needle = f'"{file_tag_column}" = {file_literal}'
    if needle not in flat:
        return False
    # Exactly one WHERE equality: the predicate after WHERE must be just the tag equality.
    where_at = low.find(" where ")
    if where_at < 0:
        return False
    pred = flat[where_at + len(" where "):].strip().rstrip(";").strip()
    return pred == needle


def guard_file_order(pending_key, last_done_file, done_files):
    """G8 ordering / high-water / gap guard. CDC files complete strictly in sort order per
    table, so for a file about to be applied:
      BLOCK if it is NOT strictly after the recorded high-water mark `last_done_file`
             (an older or equal file reappearing -> replay/regression risk), or
      BLOCK if there is a GAP: a known done file sorts AFTER `pending_key` yet `pending_key`
             was never applied (files were applied out of order / one was skipped).
    `done_files` is the set/list of file keys already marked done (the ledger). Returns
    (ok: bool, reason: str)."""
    pk = pending_key or ""
    hw = last_done_file or ""
    if hw and pk <= hw:
        return False, (f"file {pk.rsplit('/', 1)[-1]} sorts at/under the high-water mark "
                       f"{hw.rsplit('/', 1)[-1]} — it is older than, or equal to, the last "
                       f"applied file. Applying it would replay/regress already-applied data. "
                       f"The table is BLOCKED. Investigate the high-water (a hand-edited/reset "
                       f"cdc_status, or a leftover file from an older run). See RUNBOOK "
                       f"'Safety guardrails' (G8).")
    for d in (done_files or []):
        if d and d > pk:
            return False, (f"gap detected: file {pk.rsplit('/', 1)[-1]} would be applied now, "
                           f"but a LATER file {d.rsplit('/', 1)[-1]} is already marked done — "
                           f"a file between them was skipped or arrived late. The table is "
                           f"BLOCKED to avoid applying changes out of order. See RUNBOOK "
                           f"'Safety guardrails' (G8).")
    return True, ""


def guard_new_load_after_cdc(load_file_keys, cdc_started):
    """G8 DMS table-reload guard. Once CDC has started for a table, a NEW `LOAD*` file appearing
    in that table's folder is a DMS full-load RELOAD of the table — it must NOT be ignored and
    must NOT be applied as a CDC change file; it means someone reran the full load under a live
    CDC stream, which would reblank/clobber the target. If `cdc_started` is truthy and any LOAD
    file is present, BLOCK the table. Returns (ok: bool, reason: str)."""
    if not cdc_started:
        return True, ""
    loads = [k for k in (load_file_keys or []) if k]
    if not loads:
        return True, ""
    return False, (f"{len(loads)} new LOAD* file(s) appeared in this table's folder AFTER CDC "
                   f"started (e.g. {loads[0].rsplit('/', 1)[-1]}) — a DMS 'reload table' under a "
                   f"live CDC stream. The table is BLOCKED: a reload would reblank/clobber the "
                   f"target. Do a controlled reload via a NEW DMS task (RUNBOOK §9), not under "
                   f"the running CDC. See RUNBOOK 'Safety guardrails' (G8).")


def guard_drift(live_count, full_load_rows, inserts_applied, deletes_applied, tolerance):
    """G9 drift detector. Compare the live DSQL row count to the independently tracked
    expectation (full_load_rows + inserts_applied − deletes_applied). Returns
    (fired: bool, delta: int, expected: int). `fired` is True when |live − expected| > tolerance.
    `delta` is live − expected (negative == target is SHORT, the dangerous direction)."""
    try:
        live = int(live_count or 0)
        flr = int(full_load_rows or 0)
        ins = int(inserts_applied or 0)
        dels = int(deletes_applied or 0)
        tol = float(tolerance or 0)
    except (TypeError, ValueError):
        # Can't evaluate -> report fired so the caller logs/audits rather than silently passing.
        return True, 0, 0
    expected = flr + ins - dels
    delta = live - expected
    return (abs(delta) > tol), delta, expected



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
        pk_markers = _effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], pk_col)
        pk_val = hex_to_canonical_uuid(pk_raw) if pk_category == 'uuid' else _coerce_null(pk_raw, pk_markers)
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
                _m = _effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], c)
                values[c] = convert_value(raw, col_category.get(c, 'varchar'), _m)
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
        if col_category.get(c) == 'bytea' and v is not None and not _BYTEA_CANONICAL.match(v):
            raise TableBlocked(
                f"BINARY GUARD [{ctx['label']}]: column '{c}' value {repr(v)[:120]} is not "
                f"hexadecimal, which is how DMS writes binary (RAW/BLOB) values. Storing it "
                f"would put the wrong bytes in the table, so the table stops here.")
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


def apply_file(ctx, cdc_key, start_offset, conn_holder, prior_watermark=None, pos_box=None):
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
            f"header, then " + "set cdc_status.status='active' for this table to resume (UPDATE, never DELETE the row).")

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

    # ── G6 MASS-DELETE GUARD (BEFORE any chunk is built/applied, so a trip applies NOTHING
    # from this file). If this file's net DELETE ops would remove more than
    # cdc_max_delete_fraction AND more than cdc_max_delete_rows of the table's CURRENT rows,
    # the table is BLOCKED and the file left in place for an operator to verify vs the source.
    # One-time per-file override: cdc_status.allow_mass_delete=true. Uses the table's own
    # session (conn_holder) read-only; never mutates anything.
    _n_delete = vstats.get("n_delete", 0)
    if _n_delete > 0:
        _cur_cnt, _allow = _read_count_and_allow(conn_holder, dsql_schema, dsql_table, label)
        _blocked, _why = guard_mass_delete(_cur_cnt, _n_delete, _allow,
                                           CDC_MAX_DELETE_FRACTION, CDC_MAX_DELETE_ROWS)
        if _blocked:
            _audit_destructive(conn_holder, label, "cdc_mass_delete_blocked",
                               rows_before=_cur_cnt, rows_deleted=0,
                               reason=f"{cdc_key.split('/')[-1]}: {_why}")
            raise TableBlocked(f"MASS-DELETE GUARD [{label}] {cdc_key.split('/')[-1]}: {_why}")
    # G9: stash this file's net INSERT/DELETE counts so process_table can advance the running
    # expectation counters (cdc_status.inserts_applied/deletes_applied) after the file applies.
    ctx["_last_file_io"] = (vstats.get("n_insert", 0), vstats.get("n_delete", 0))

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
                # POSITION FENCE (see check_position_cur): first statement of the chunk txn.
                if pos_box is not None:
                    _tgt = (cdc_key, new_offset, pos_box["pos"][2])
                    if check_position_cur(cur, label, pos_box["pos"], _tgt) == "already":
                        conn.rollback()
                        pos_box["pos"] = _tgt
                        print(f"    ↷ {label}: rows {idx}-{new_offset} of {cdc_key.split('/')[-1]} "
                              f"are already committed; not applying them again", flush=True)
                        break
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
                if pos_box is not None:
                    pos_box["pos"] = (cdc_key, new_offset, pos_box["pos"][2])
                break
            except (TableBlocked, _PositionMoved):
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
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
            _m = _effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], c)
            vals[c] = convert_value(raw, col_category.get(c, 'varchar'), _m)
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


def apply_file_nonpk(ctx, cdc_key, start_offset, conn_holder, prior_watermark=None, pos_box=None):
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
            f"mapping/header, then " + "set cdc_status.status='active' for this table to resume (UPDATE, never DELETE the row).")

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

    # ── G7 PURGE SAFETY: the file-reload purge must delete ONLY rows tagged with THIS exact
    # file key — never a prefix/LIKE/other file. Assert the canonical exact-equality shape
    # before it is ever executed; a future edit that widened it trips here (fail SAFE: block).
    if not purge_predicate_is_exact(purge_sql, NONPK_FILE_TAG_COLUMN, file_lit):
        raise TableBlocked(
            f"PURGE GUARD [{label}] {cdc_key.split('/')[-1]}: the _cdc_file purge predicate is "
            f"not an exact single-equality on \"{NONPK_FILE_TAG_COLUMN}\" to this file's key — "
            f"refusing to run it (it could delete other files' rows). This is a code bug.")

    # ── G6 MASS-DELETE GUARD (no-PK): a keyless D op is one content DELETE. If the file's net
    # DELETEs exceed cdc_max_delete_fraction AND cdc_max_delete_rows of the table's CURRENT
    # rows, block and apply NOTHING. One-time override: cdc_status.allow_mass_delete=true.
    if delete_ops:
        _cur_cnt, _allow = _read_count_and_allow(conn_holder, dsql_schema, dsql_table, label)
        _blocked, _why = guard_mass_delete(_cur_cnt, len(delete_ops), _allow,
                                           CDC_MAX_DELETE_FRACTION, CDC_MAX_DELETE_ROWS)
        if _blocked:
            _audit_destructive(conn_holder, label, "cdc_mass_delete_blocked",
                               rows_before=_cur_cnt, rows_deleted=0,
                               reason=f"{cdc_key.split('/')[-1]}: {_why}")
            raise TableBlocked(f"MASS-DELETE GUARD [{label}] {cdc_key.split('/')[-1]}: {_why}")
        # ── G7 NO-PK DELETE PRECISION: a full-row content DELETE removes EVERY matching row.
        # Each D net-op intends exactly one row (no-duplicate-rows contract). Count the matches
        # per distinct delete predicate first; if any matches more rows than the D ops sharing
        # it require, over-deletion would occur -> BLOCK (DSQL has no ctid / verifiable bounded
        # delete, so blocking is the SAFE action).
        _ok, _why7 = _nopk_delete_precision_check(conn_holder, label, dsql_schema, dsql_table,
                                                  delete_ops, _content_where)
        if not _ok:
            if guard_action_blocks(CDC_NOPK_OVERMATCH_ACTION):
                _audit_destructive(conn_holder, label, "cdc_nopk_overmatch_blocked",
                                   rows_before=_cur_cnt, rows_deleted=0,
                                   reason=f"{cdc_key.split('/')[-1]}: {_why7}")
                raise TableBlocked(f"NO-PK DELETE GUARD [{label}] {cdc_key.split('/')[-1]}: {_why7}")
            # WARN (default): record + metric; apply the content DELETEs as the file specifies.
            # The G6 mass-delete guard (above, HARD) is the real volume cap; this precision check
            # is a refinement that must not fail a normal run for its own bookkeeping. Set
            # cdc_nopk_overmatch_action=block (or guardrails_mode=strict) to block instead.
            _audit_destructive(conn_holder, label, "cdc_nopk_overmatch_warn",
                               rows_before=_cur_cnt, rows_deleted=0,
                               reason=f"{cdc_key.split('/')[-1]}: {_why7}")
            emit_guard_warn_metric(label, "G7_nopk_overmatch")
            print(f"    ⚠️  G7 {label} {cdc_key.split('/')[-1]}: {_why7} (guardrails_mode=warn — "
                  f"WARNING, applying anyway; set cdc_nopk_overmatch_action=block to block)")
    # G9: stash this file's net INSERT/DELETE counts so process_table can advance the running
    # expectation counters (cdc_status.inserts_applied/deletes_applied) after the file applies.
    ctx["_last_file_io"] = (vstats.get("n_insert", 0), vstats.get("n_delete", 0))

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
                _applied_so_far = total_committed + len(chunk_sql)
                # POSITION FENCE (see check_position_cur): first statement of the chunk txn.
                if pos_box is not None:
                    _tgt = (cdc_key, _applied_so_far, pos_box["pos"][2])
                    if check_position_cur(cur, label, pos_box["pos"], _tgt) == "already":
                        conn.rollback()
                        pos_box["pos"] = _tgt
                        print(f"    ↷ {label}: keyless chunk {ci + 1}/{n_chunks} of "
                              f"{cdc_key.split('/')[-1]} is already committed; not applying it "
                              f"again", flush=True)
                        break
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
                if pos_box is not None:
                    pos_box["pos"] = (cdc_key, _applied_so_far, pos_box["pos"][2])
                break
            except (TableBlocked, _PositionMoved):
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
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
            if isinstance(e, _PositionMoved):
                raise   # another run moved the checkpoint: not retriable, the caller yields
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

    # Guardrail helpers (G9 drift check) read the table identity from ctx; bind them once here.
    dsql_schema, dsql_table = ctx["dsql_schema"], ctx["dsql_table"]

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
        _unpin_dsql_endpoint()   # pinned host stopped working -> re-probe candidates next poll
        print(f"    ⚠️ {label}: could not open DSQL session after {CONNECT_MAX_RETRIES} "
              f"attempts (isolated, retry next poll): {_last_err}")
        return {"table": label, "status": "error", "files": 0, "rows": 0, "error": str(_last_err)}

    def _commit_status(_done_file=None, _expect=None, _target=None, **fields):
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
            # POSITION FENCE: only move the checkpoint if it is still where this run left it.
            if _expect is not None and check_position_cur(c, label, _expect, _target) == "already":
                conn_holder[0].rollback()
                return
            upsert_cdc_status(c, label, **fields)
            if _done_file is not None:
                mark_file_done_cur(c, label, _done_file)
            conn_holder[0].commit()
        run_control_op(conn_holder, label, _do, "commit_status")
        if _target is not None:
            pos_box["pos"] = _target

    applied_rows = 0
    files_done = 0
    chunks_done = 0   # telemetry: total committed chunks (commits) across this table's files
    # For the processed/ copies + manifest refresh in `finally` (status None = skip it).
    _mf = {"last_done": None, "status": None, "busy": False}
    pos_box = {"pos": None}   # this run's view of the table's checkpoint (position fence)
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
        # Where this run found the checkpoint; every step below must start from here.
        pos_box["pos"] = _pos(state["in_progress_file"], state["last_offset"],
                              state["last_done_file"])
        _mf["last_done"] = state["last_done_file"]
        _mf["status"] = state["status"] or "idle"

        if state["status"] == "blocked":
            print(f"  ⛔ {label} is BLOCKED (prior bad row) — skipping. To resume: UPDATE "
                  f"{CONTROL_SCHEMA}.cdc_status SET status='active' WHERE table_name='{label}' "
                  f"(never DELETE the row).")
            return {"table": label, "status": "blocked", "files": 0, "rows": 0}

        files = list_cdc_files(ctx)
        _mf["status"] = "active"
        last_done = state["last_done_file"]
        in_progress = state["in_progress_file"]
        resume_offset = state["last_offset"] if in_progress else 0

        # HIGH-WATER SKIP: drop files already fully applied (<= last_done_file). Files sort
        # by DMS timestamp filename and complete strictly in order (serial), so this is safe.
        pending = [key for key in files if not (last_done and key <= last_done)]
        if not pending:
            _mf["status"] = "idle"
            return {"table": label, "status": "idle", "files": 0, "rows": 0}
        _mf["busy"] = True

        # ── G8 DMS TABLE-RELOAD GUARD: once CDC has advanced for this table (a high-water
        # mark exists OR a file is in progress), a NEW LOAD*.csv appearing in its folder is a
        # DMS 'reload table' under a live CDC stream — block rather than ignore it (a reload
        # would reblank/clobber the target). Checked ONCE per cycle, before any apply.
        _cdc_started = bool(last_done) or bool(in_progress)
        try:
            _load_files_for_g8 = list_load_files(ctx)
        except Exception as _e8:
            # A guard's own read must never raise into the apply path. Warn and treat as "no
            # new LOAD files" (guard passes); the stream keeps flowing.
            _load_files_for_g8 = []
            print(f"    ⚠️  G8 {label}: could not list LOAD* files ({_e8}); skipping the "
                  f"new-LOAD-after-CDC check this cycle (carry on as if it passed).")
        _ok8, _why8 = guard_new_load_after_cdc(_load_files_for_g8, _cdc_started)
        if not _ok8:
            if guard_action_blocks(CDC_FILE_ORDER_ACTION):
                _audit_destructive(conn_holder, label, "cdc_new_load_after_start_blocked",
                                   rows_before=None, rows_deleted=0, reason=_why8)
                _set_blocked_status(conn_holder, label, _why8)
                _mf["status"] = "blocked"
                print(f"    ⛔ {label} BLOCKED: {_why8}")
                return {"table": label, "status": "blocked", "files": 0, "rows": 0,
                        "error": _why8}
            # WARN (default): record + metric, but keep applying this cycle. A new LOAD* after
            # CDC started is unusual, so it is loud; set cdc_file_order_action=block (or
            # guardrails_mode=strict) to stop the table instead.
            _audit_destructive(conn_holder, label, "cdc_new_load_after_start_warn",
                               rows_before=None, rows_deleted=0, reason=_why8)
            emit_guard_warn_metric(label, "G8_new_load")
            print(f"    ⚠️  G8 {label}: {_why8} (guardrails_mode=warn — WARNING, not blocking; "
                  f"set cdc_file_order_action=block to block)")

        # ── G8 ORDERING / HIGH-WATER / GAP GUARD: every pending file must sort strictly AFTER
        # the high-water mark and in order. A file at/under the high-water (replay/regression,
        # e.g. a hand-reset cdc_status) or a gap (a later file already applied) blocks the
        # table. `_done8` grows as we apply, so an out-of-order pending list is caught too.
        _done8 = [last_done] if last_done else []
        for _pk in pending:
            _ok8b, _why8b = guard_file_order(_pk, last_done, _done8)
            if not _ok8b:
                if guard_action_blocks(CDC_FILE_ORDER_ACTION):
                    _audit_destructive(conn_holder, label, "cdc_file_order_blocked",
                                       rows_before=None, rows_deleted=0, reason=_why8b)
                    _set_blocked_status(conn_holder, label, _why8b)
                    _mf["status"] = "blocked"
                    print(f"    ⛔ {label} BLOCKED: {_why8b}")
                    return {"table": label, "status": "blocked", "files": 0, "rows": 0,
                            "error": _why8b}
                # WARN (default): record + metric; keep applying in the natural (sorted) order.
                # A gap/regression is logged so an operator can investigate, but a transient
                # ordering wobble does not stop the stream. cdc_file_order_action=block (or
                # guardrails_mode=strict) blocks instead.
                _audit_destructive(conn_holder, label, "cdc_file_order_warn",
                                   rows_before=None, rows_deleted=0, reason=_why8b)
                emit_guard_warn_metric(label, "G8_file_order")
                print(f"    ⚠️  G8 {label}: {_why8b} (guardrails_mode=warn — WARNING, not "
                      f"blocking; set cdc_file_order_action=block to block)")
            _done8.append(_pk)

        # Guarantee the cdc_status row EXISTS before any chunk runs, so the per-chunk
        # checkpoint (update_cdc_status) is a pure UPDATE on the hot path — no SELECT,
        # no INSERT, no 23505 risk fused to a data transaction. One-time, own txn, with the
        # full control-op retry set.
        def _ensure_row(c):
            ensure_status_row_cur(c, label)
            _seed_full_load_rows_cur(c, label)   # G9: seed the drift baseline (idempotent)
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
        # (ledger markers, checkpoint, done/blocked handling, processed/ copies) is identical.
        _apply_fn = apply_file if ctx["apply_key"] is not None else apply_file_nonpk
        for key in pending:
            start = resume_offset if (in_progress and key == in_progress) else 0
            try:
                n, file_wm, n_chunks = _apply_fn(ctx, key, start, conn_holder,
                                                 prior_watermark=prior_wm, pos_box=pos_box)
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
                _mf["status"] = "schema_changed"
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
                _mf["status"] = "blocked"
                record_exception(label, key, start, "(apply)", str(tb))
                print(f"    ⛔ {label} BLOCKED at {key.split('/')[-1]} @offset {start}: {tb}")
                print(f"       RESUMABLE: fix the cause, then UPDATE {CONTROL_SCHEMA}.cdc_status SET "
                      f"status='active' WHERE table_name='{label}' to resume from offset {start} "
                      f"(file left in place; never DELETE the row).")
                return {"table": label, "status": "blocked", "files": files_done,
                        "rows": applied_rows, "chunks": chunks_done, "error": str(tb)}
            # File fully applied -> advance the high-water + clear in-progress on THE
            # TABLE'S OWN connection (no new handshake), then COPY the S3 file to
            # processed/ (human artifact; DSQL is the source of truth). The original is kept;
            # the high-water mark, not the file's location, stops it being applied again. The
            # same txn stamps the cdc_file_status ledger row 'done' (Option-A lifecycle
            # marker), fused to the high-water advance.
            _commit_status(status="active", last_done_file=key,
                           in_progress_file=None, last_offset=0, _done_file=key,
                           _expect=pos_box["pos"], _target=(None, 0, key))
            _mf["last_done"] = key
            try:
                copy_to_processed(key, ctx["prefixes"]["processed"])
            except Exception as e:
                print(f"    ⚠️ copy to processed/ failed after retries (non-fatal: original kept, "
                      f"retried next cycle; DSQL is source of truth): {e}")
            # G9: advance the running expectation counters for this file (its own short txn on
            # the table's session). ctx["_last_file_io"] was stashed by the apply fn.
            _io = ctx.get("_last_file_io") or (0, 0)
            if _io != (0, 0):
                def _adv(c, _io=_io):
                    _advance_cdc_counters(c, label, _io[0], _io[1])
                    conn_holder[0].commit()
                try:
                    run_control_op(conn_holder, label, _adv, "advance_cdc_counters")
                except Exception as _ce:
                    print(f"    ⚠️ {label}: drift counter advance failed (non-fatal): {_ce}")
                ctx["_last_file_io"] = (0, 0)
            files_done += 1
            resume_offset = 0
            in_progress = None
        # All pending files done -> mark idle (same session).
        _commit_status(status="idle", in_progress_file=None, last_offset=0,
                       _expect=pos_box["pos"], _target=None)
        _mf["status"] = "idle"
        # G9: periodic drift detector (throttled to CDC_DRIFT_CHECK_MINUTES per table). Compares
        # the live DSQL count to full_load_rows+inserts-deletes; on drift logs ERROR, writes
        # audit_log, emits the DsqlRowDrift metric, and (action=block) sets the table 'blocked'.
        if _maybe_run_drift_check(conn_holder, label, dsql_schema, dsql_table) == "blocked":
            _mf["status"] = "blocked"
    except _PositionMoved as pm:
        # Another CDC run is applying this table (or an operator just blocked it). Nothing
        # from the step that noticed was committed. Leave the table to the other run for this
        # cycle; the next cycle re-reads the checkpoint. Two runs on one table stay correct,
        # but it is wasted work and usually a mistake, so say so loudly.
        print(f"    ⚠️ {label}: left for this cycle: {pm}. If this repeats, ANOTHER CDC run "
              f"is applying this table (a second Glue job, an old per-task workflow, or a "
              f"hand-made copy); stop all but one. Each change is still applied once.",
              flush=True)
        _mf["status"] = None   # the run that owns the table writes its manifest
        return {"table": label, "status": "yielded", "files": files_done,
                "rows": applied_rows, "chunks": chunks_done, "error": str(pm)}
    except Exception as e:
        # TABLE-LEVEL SAFETY NET — process_table must NEVER raise, so one table's
        # exhausted/unclassified DSQL error (e.g. a control-op that retried through OCC /
        # transient-server / txn-timeout / pipe and still failed) is ISOLATED to this
        # table. Other tables keep flowing; this table is retried next poll cycle. We do
        # NOT mark it 'blocked' (that's reserved for a genuinely un-appliable row that
        # needs an operator) — 'error' is transient/self-healing. Best-effort log only;
        # the checkpoint already reflects the last committed offset, so resume is exact.
        print(f"    ⚠️ {label}: cycle error (isolated, will retry next poll): {e}")
        if _mf["status"] is not None:
            _mf["status"] = "error"
        return {"table": label, "status": "error", "files": files_done,
                "rows": applied_rows, "chunks": chunks_done, "error": str(e)}
    finally:
        # processed/ copies + manifest: busy cycles always, idle tables at most every 5 min.
        # Never raises (refresh_processed and _ledger_done_count catch everything).
        try:
            if _mf["status"] is not None and _manifest_due(label, _mf["busy"]):
                _ld = (_ledger_done_count(conn_holder[0], label)
                       if conn_holder[0] is not None else None)
                refresh_processed(ctx, _mf["last_done"], _mf["status"], ledger_done=_ld,
                                  force=_mf["busy"])
        except Exception as _me:
            print(f"    ⚠️ {label}: processed/ refresh skipped (non-fatal): {_me}")
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
    files.sort(), and already-applied files are skipped via last_done_file (the high-water
    mark), so an extra cycle with nothing new is cheap.

    Returns True on a normal wake (always True today; reserved for future stop signals)."""
    time.sleep(POLL_INTERVAL)
    return True


def _run_arg(name):
    """Value of --<name> from this run's arguments, or None."""
    for _i, _a in enumerate(sys.argv):
        if _a == "--" + name and _i + 1 < len(sys.argv):
            return sys.argv[_i + 1]
        if _a.startswith("--" + name + "="):
            return _a.split("=", 1)[1]
    return None


def _start_token():
    """Name of the start marker: the startup workflow passes its execution name as
    --startup_execution; a run started any other way falls back to Glue's --JOB_RUN_ID."""
    return _run_arg("startup_execution") or _run_arg("JOB_RUN_ID")


def _task_marker_prefix_key():
    """B23: the TASK-level config prefix (bare S3 key, trailing slash) for the second start-marker
    copy, derived from --cdc_owners_key's dirname. cdc_owners_key is the task registry key
    config/_task/<suffix>/_jobs.json (bare key or s3:// URI); its dirname is config/_task/<suffix>/.
    Returns None when cdc_owners_key is absent. For the MAIN CDC run (CONFIG_PREFIX already the task
    prefix) this equals CONFIG_PREFIX's key, so no second copy is written; for a bg fork run it is
    the task prefix above the fork prefix."""
    _ok = CDC_OWNERS_KEY
    if not _ok:
        return None
    _ok = str(_ok)
    if _ok.startswith("s3://"):
        _ok = _ok[len("s3://"):].partition("/")[2]
    _ok = _ok.lstrip("/")
    _dir = _ok.rsplit("/", 1)[0] if "/" in _ok else ""
    return (_dir + "/") if _dir else ""


def write_started_marker():
    """Tell the startup workflow this run really started: written once, when the run reaches
    its poll loop (drivers installed, DSQL reachable, control tables and manifest loaded).
    Key: <CONFIG_PREFIX>_cdc_started/<start token>.json (+ _latest.json). The workflow waits for
    it before reporting success. Never raises: a failed write only means the workflow reports
    CdcStartNotConfirmed while this run keeps applying changes.

    B23: a bg-fork run's CONFIG_PREFIX is the fork prefix (.../_orchestrator/bg-<slug>/), so the
    marker lands there and the fixed workflow polls exactly that prefix. To also confirm start for
    OLDER (unpatched) workflows — which polled the TASK-level config/_task/<suffix>/_cdc_started/ —
    write a SECOND copy there whenever --cdc_owners_key is present (its dirname is the task prefix).
    The MAIN run writes only once (its CONFIG_PREFIX already IS the task prefix)."""
    try:
        if not str(CONFIG_PREFIX).startswith("s3://"):
            print(f"  (start marker skipped: CONFIG_PREFIX {CONFIG_PREFIX!r} is not an s3:// path)", flush=True)
            return
        _bkt, _, _key = CONFIG_PREFIX[len("s3://"):].partition("/")
        _rid = _start_token()
        _doc = json.dumps({"start_token": _rid, "job_run_id": _run_arg("JOB_RUN_ID"),
                           "started_at": utc_now_iso(),
                           "config_prefix": CONFIG_PREFIX, "dms_task_arn": DMS_TASK_ARN,
                           "control_schema": CONTROL_SCHEMA}, indent=1).encode("utf-8")
        _marker_prefixes = [_key]
        _task_key = _task_marker_prefix_key()
        if _task_key and _task_key != _key:
            _marker_prefixes.append(_task_key)
        _keys = []
        for _pfx in _marker_prefixes:
            if _rid:
                _keys.append(_pfx + f"_cdc_started/{_rid}.json")
            _keys.append(_pfx + "_cdc_started/_latest.json")
        for _k in _keys:
            _s3_retry(lambda _k=_k: s3.put_object(Bucket=_bkt, Key=_k, Body=_doc,
                                                  ContentType="application/json"),
                      f"write {_k}")
        print(f"  [startup] start marker written: s3://{_bkt}/{_keys[0]}"
              + (f" (+ task-level copy under {_task_key}_cdc_started/)" if len(_marker_prefixes) > 1 else ""),
              flush=True)
    except Exception as _e:
        print(f"  ⚠️ start marker not written (non-fatal; CDC keeps running): {_e}", flush=True)


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
    owners = _load_cdc_owners()
    contexts = []
    multi_key = []
    not_owned = []
    for _i, e in enumerate(entries):
        _label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
        # ONE persisted ownership record decides who applies each table. Skip any table this
        # job does not own. Missing record/owner -> owned by "main" (back-compat).
        _owner = owners.get(_label, "main") if owners is not None else "main"
        if _owner != CDC_OWNER_SELF:
            not_owned.append(f"{_label} (owner={_owner})")
            continue
        try:
            print(f"    [startup] context {_i+1}/{len(entries)}: {e.get('dsql_schema')}.{e.get('dsql_table')}", flush=True)
            contexts.append(build_table_context(e))
        except MultiColumnKeyTable as mk:
            multi_key.append(str(mk))
        except Exception as ex:
            print(f"  ⚠️ skipping {e.get('dsql_table')} — config load failed: {ex}", flush=True)
    print(f"  Tables discovered: {len(contexts)}", flush=True)
    if not_owned:
        print(f"  ⏭  {len(not_owned)} table(s) NOT applied by this job ({CDC_OWNER_SELF}); owned by "
              f"another CDC job per _cdc_owners.json:", flush=True)
        for _n in not_owned:
            print(f"       - {_n}", flush=True)
    if multi_key:
        print(f"  ⏭  {len(multi_key)} table(s) NOT applied by this job (multi-column primary key; "
              f"run the separate multi-column-key CDC job for them). Cutover waits until their "
              f"change files are marked done in {CONTROL_SCHEMA}.cdc_file_status:", flush=True)
        for _m in multi_key:
            print(f"       - {_m}", flush=True)
    # Raise ONLY when the manifest truly yields nothing usable: nothing this job owns, no
    # multi-column-key tables, and nothing owned by another CDC job either. If every table is
    # owned by another CDC job (bg-fork or multi-column-key job), this job legitimately has
    # nothing to apply — it must NOT crash. In the startup ASL the MAIN run must still reach
    # its poll loop and write the start marker (CheckCdcStarted polls for it) BEFORE
    # StartForkCdcMap launches the forks; crashing here would strand those forks
    # (CdcRunFailed / CdcStartNotConfirmed) and leave the task with no CDC at all.
    if not contexts and not multi_key and not not_owned:
        raise Exception("No usable tables from the manifest.")
    if not contexts:
        if not_owned and not multi_key:
            print(f"  ℹ️ every table in this task ({len(not_owned)}) is applied by another CDC "
                  f"job (owner != {CDC_OWNER_SELF}, per _cdc_owners.json): this job has nothing "
                  f"to apply and stays idle (cutover stops it).", flush=True)
        elif not_owned:
            print(f"  ℹ️ this job has nothing to apply — every table is owned by another CDC job "
                  f"({len(not_owned)}) or has a multi-column primary key ({len(multi_key)}): it "
                  f"stays idle (cutover stops it).", flush=True)
        else:
            print("  ℹ️ every table in this task has a multi-column primary key: this job has "
                  "nothing to apply and stays idle (cutover stops it).", flush=True)

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
    write_started_marker()

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
            yielded = [r["table"] for r in results if r.get("status") == "yielded"]
            if yielded:
                print(f"  ⚠️ tables another CDC run is also applying (left to it this cycle; "
                      f"stop the extra run): {yielded}")

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
