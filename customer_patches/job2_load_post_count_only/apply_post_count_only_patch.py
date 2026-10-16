#!/usr/bin/env python3
"""Apply the POST-LOAD COUNT-ONLY patch to a deployed job2_load.py.

    python3 apply_post_count_only_patch.py <in job2_load.py> [<out job2_load.py>]

Adds code only (removes nothing). ASCII-only. The result is compiled before it is written.

WHAT IT DOES
  Adds ONE optional Glue arg --post_load_check with values full (default) | count_only, read
  with the SAME optional-arg mechanism the script already uses (the `optional = [...]` list +
  getResolvedOptions in _apply_v6_arg_overrides, exactly like --checksum_mode in job3_validate).
    * absent / empty / "full" -> behaviour is BYTE-IDENTICAL to today.
    * an invalid value        -> a warning is printed and "full" is used (same as checksum_mode).
  In count_only mode, at ALL FOUR post-load sites (the 3 call sites of
  validate_whole_table_after_ranges AND the single-table block in load_one_table):
    * NO per-file range COUNT(*) queries; NO uuid-format scan; NO auth-token-leak scan
      (one line is printed saying they were skipped by post_load_check=count_only);
    * exactly ONE plain `SELECT COUNT(*) FROM schema.table` on a FRESH autocommit connection;
    * a total budget of POST_LOAD_COUNT_BUDGET_SECONDS (600s): a non-mismatch failure is retried
      up to 3 times, 20s apart, only while budget remains;
    * count == expected  -> OK line; count differs -> the table FAILS exactly as today (same
      exception text style with both numbers); the count cannot complete (txn-age / network /
      SSL EOF / connection reset / XX000 / any other infra error / budget exhausted) -> a loud
      WARNING ("row count NOT verified by the load job - verify manually") and the load CONTINUES.
  No change to checkpointing/resume, chunking, writers, or any other path.

HOW
  Four additive, idempotent edits, each guarded by a unique anchor that is asserted present
  EXACTLY ONCE (fail closed: a missing or ambiguous anchor aborts without writing):
    A1  append "post_load_check" to the existing optional-arg list literal;
    A2  add a one-line parse call right after the existing `_apply_v6_arg_overrides()` call,
        and prepend the POST-LOAD COUNT-ONLY block (constant + helpers) before that call;
    A3  insert a guard at the TOP of validate_whole_table_after_ranges' body (covers its 3 call
        sites at once): in count_only mode run the single COUNT(*) and return, skipping the
        per-file range counts and the uuid/leak scans;
    A4  insert the same guard at the TOP of the single-table post-load block in load_one_table.
"""
import hashlib
import sys


def _sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- recognisers
# Lines that MUST be present for this to be the job2_load.py we expect. If any is missing the
# file is not recognised and we refuse (fail closed).
REQUIRED = [
    "def _apply_v6_arg_overrides():",
    "def validate_whole_table_after_ranges(dsql_schema, dsql_table, uuid_cols,",
    "def _run_uuid_and_leak_checks(cursor, dsql_schema, dsql_table, uuid_cols, target_columns):",
    "def _run_whole_table_count(cursor, dsql_schema, dsql_table, expected_total, errors):",
    "def connect_dsql():",
    "def is_txn_timeout(exc):",
    "def is_transient_server_error(exc):",
    "def is_broken_pipe_error(exc):",
    "import ssl",
    "import time",
    "import threading",
    '                "null_values", "null_rules"]',
    "\n_apply_v6_arg_overrides()\n",
    "    # ---- Post-load validation ----\n    conn = connect_dsql()",
]

# ---- Anchor 1: the end of the optional-args list literal. ADD our arg name so the existing
# getResolvedOptions-based parser reads it when present. (The literal is shared with the
# manual-range patch; this only APPENDS, so the two patches are order-independent.)
A1 = '                "null_values", "null_rules"]'
A1_NEW = (
    '                "null_values", "null_rules",\n'
    '                "post_load_check"]'
)

# ---- Anchor 2: the module-level call that applies the v6 overrides. We PREPEND the big block
# (constant + parser + helpers) before the function the call belongs to is... no: we prepend the
# block before the `_apply_v6_arg_overrides()` CALL line and add our parse call right after it, at
# the same import-time point, after CONFIG_PREFIX + constants are final.
A2 = "\n_apply_v6_arg_overrides()\n"

# ---- Anchor 3: the first body line of validate_whole_table_after_ranges (covers its 3 call
# sites). Insert the count-only guard right after the docstring, before `conn = connect_dsql()`.
A3 = (
    "    NO-DUP: guaranteed by the target PRIMARY KEY (23505 at insert); no distinct scan.\"\"\"\n"
    "    conn = connect_dsql()\n"
)
A3_NEW = (
    "    NO-DUP: guaranteed by the target PRIMARY KEY (23505 at insert); no distinct scan.\"\"\"\n"
    "    if _post_load_check_is_count_only():\n"
    "        _post_count_only_check(dsql_schema, dsql_table, expected_total,\n"
    "                               site='validate_whole_table_after_ranges')\n"
    "        return\n"
    "    conn = connect_dsql()\n"
)

# ---- Anchor 4: the single-table (V5 whole-table) post-load block in load_one_table.
A4 = (
    "    # ---- Post-load validation ----\n"
    "    conn = connect_dsql()\n"
)
A4_NEW = (
    "    # ---- Post-load validation ----\n"
    "    if _post_load_check_is_count_only():\n"
    "        _post_count_only_check(dsql_schema, dsql_table, total_written,\n"
    "                               site='load_one_table')\n"
    "        return {\n"
    "            \"table\": f\"{dsql_schema}.{dsql_table}\",\n"
    "            \"rows\": total_written,\n"
    "            \"target_columns\": len(target_columns),\n"
    "            \"skipped_columns\": len(skip_columns),\n"
    "        }\n"
    "    conn = connect_dsql()\n"
)

BLOCK = r'''# =============================================================================
# POST-LOAD COUNT-ONLY PATCH (optional Glue arg --post_load_check full|count_only)
#
# PROBLEM. After a big table's files are loaded, the post-load checks can hang for an hour
# and then FAIL the whole load on a network / DSQL error, although the rows ARE loaded (the
# operator can verify with one COUNT(*) in seconds). The expensive, failure-prone steps are:
#   * validate_whole_table_after_ranges (3 call sites): hundreds of per-file index-range
#     COUNT(*) queries when files are PK-disjoint, then two FULL-TABLE regex/LIKE scans
#     (uuid-format + auth-token-leak). A non-timeout error (network / SSL EOF) is appended to
#     `errors` and FAILS the load.
#   * the single-table block in load_one_table: a bounded probe/COUNT(*) then the same scans.
#
# THIS PATCH. --post_load_check count_only replaces ALL of that, at every site, with ONE plain
# `SELECT COUNT(*) FROM schema.table` on a FRESH autocommit connection, under a hard time
# budget, that NEVER hangs the load and NEVER fails it on an infrastructure error:
#   (1) no per-file range counts; (2) no uuid-format scan and no auth-token-leak scan (the
#   per-row uuid guard already validated every uuid before insert; the leak scan is a full
#   scan) - one line says they were skipped; (3) ONE COUNT(*); (4) total budget =
#   POST_LOAD_COUNT_BUDGET_SECONDS (600s), a failed non-mismatch attempt retried up to
#   POST_LOAD_COUNT_RETRIES times, POST_LOAD_COUNT_RETRY_WAIT seconds apart, only while budget
#   remains; (5) equal -> OK; differs -> table FAILS exactly as today (same text, both numbers);
#   cannot complete (txn-age 300s / network / SSL EOF / connection reset / XX000 / any other
#   infra error / budget exhausted) -> loud WARNING "row count NOT verified by the load job -
#   verify manually" and the load CONTINUES. (6) nothing else changes.
#
# Absent / empty / "full" => POST_LOAD_CHECK is "full" and NONE of this runs: the guards are
# no-ops and every post-load site behaves BYTE-IDENTICALLY to today. An invalid value prints a
# warning and uses "full" (same contract as job3_validate's --checksum_mode).
# =============================================================================
POST_LOAD_CHECK = "full"                  # "full" (default) | "count_only"
POST_LOAD_COUNT_BUDGET_SECONDS = 600      # hard wall-clock budget for the whole count step
POST_LOAD_COUNT_RETRIES = 3               # retries of a non-mismatch failure, within budget
POST_LOAD_COUNT_RETRY_WAIT = 20           # seconds between those retries


def _apply_post_load_check_override():
    """Read the optional --post_load_check arg with the SAME mechanism the script already uses
    for other optional overrides (present-filter + getResolvedOptions). Absent / empty / 'full'
    -> 'full' (byte-identical to today). An invalid value -> warn and use 'full'."""
    global POST_LOAD_CHECK
    if "--post_load_check" not in sys.argv:
        return
    ov = getResolvedOptions(sys.argv, ["post_load_check"])
    raw = ov.get("post_load_check")
    val = str(raw).strip().lower() if isinstance(raw, str) else ""
    if val in ("", "full"):
        POST_LOAD_CHECK = "full"
    elif val == "count_only":
        POST_LOAD_CHECK = "count_only"
        print("  post_load_check=count_only: post-load verification is ONE COUNT(*) per table "
              "(no per-file range counts, no uuid/leak scans); an infrastructure failure WARNS "
              "and does NOT fail the load.")
    else:
        print("  WARNING ignoring invalid post_load_check=%r (use full|count_only); keeping full"
              % (raw,))
        POST_LOAD_CHECK = "full"


def _post_load_check_is_count_only():
    return POST_LOAD_CHECK == "count_only"


def _post_count_is_infra_error(exc):
    """For post_load_check=count_only, ANY exception raised by the COUNT(*) attempt is 'cannot
    complete' (the only non-error outcome is a returned integer). This helper only classifies
    WHICH kind of infrastructure error it is, for the log line; it is never used to turn an
    error into a failure. It explicitly recognises ssl.SSLEOFError / 'EOF occurred in violation
    of protocol' (which is_transient_server_error alone does not), the DSQL 300s txn-age limit,
    connection resets / broken pipes, and XX000 'server unavailable'."""
    try:
        if isinstance(exc, ssl.SSLEOFError):
            return "ssl.SSLEOFError"
    except Exception:
        pass
    msg = str(exc).lower()
    if "eof occurred in violation of protocol" in msg:
        return "ssl.SSLEOFError"
    try:
        if is_txn_timeout(exc):
            return "DSQL 300s transaction-age limit"
    except Exception:
        pass
    try:
        if is_broken_pipe_error(exc):
            return "dropped/broken connection"
    except Exception:
        pass
    try:
        if is_transient_server_error(exc):
            return "XX000 server unavailable / transient"
    except Exception:
        pass
    return type(exc).__name__


def _post_count_one(dsql_schema, dsql_table):
    """Run exactly ONE `SELECT COUNT(*) FROM schema.table` on a FRESH autocommit connection and
    return the integer. The caller owns retries/budget; this opens, counts, and always closes."""
    conn = connect_dsql()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {dsql_schema}.{dsql_table}")
            return int(cur.fetchone()[0])
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


class _PostCountMismatch(Exception):
    """The COUNT(*) completed and DIFFERED from the expected rows (a real data problem)."""


def _post_count_only_check(dsql_schema, dsql_table, expected_total, site="post-load"):
    """COUNT-ONLY post-load verification for ONE table. NEVER hangs and NEVER fails the load on
    an infrastructure error; raises ONLY on a completed-but-different count (so the table fails
    exactly as it does today). Returns a short note string (also used in the per-table status).

    Budget: POST_LOAD_COUNT_BUDGET_SECONDS total. A non-mismatch attempt is retried up to
    POST_LOAD_COUNT_RETRIES times, POST_LOAD_COUNT_RETRY_WAIT apart, only while budget remains.
    For count_only, ANY exception from the count means 'cannot complete' -> WARN, do not fail."""
    label = f"{dsql_schema}.{dsql_table}"
    print(f"    post_load_check=count_only [{label}] @ {site}: skipping per-file range counts, "
          f"uuid-format scan and auth-token-leak scan (per-row uuid guard already validated "
          f"uuids before insert; leak scan is a full-table scan).")
    deadline = time.time() + POST_LOAD_COUNT_BUDGET_SECONDS
    attempt = 0
    last_err = None
    while True:
        attempt += 1
        try:
            actual = _post_count_one(dsql_schema, dsql_table)
        except Exception as e:
            last_err = e
            kind = _post_count_is_infra_error(e)
            remaining = deadline - time.time()
            if attempt <= POST_LOAD_COUNT_RETRIES and remaining > POST_LOAD_COUNT_RETRY_WAIT:
                wait = min(POST_LOAD_COUNT_RETRY_WAIT, max(0.0, remaining))
                print(f"    post_load_check=count_only [{label}]: COUNT(*) attempt {attempt} "
                      f"failed ({kind}: {str(e)[:150]}); retrying in {wait:.0f}s "
                      f"({remaining:.0f}s budget left).")
                time.sleep(wait)
                continue
            note = (f"WARNING post_load_check=count_only [{label}]: COUNT(*) could not complete "
                    f"({kind}: {str(e)[:200]}) within the {POST_LOAD_COUNT_BUDGET_SECONDS}s "
                    f"budget after {attempt} attempt(s); row count NOT verified by the load job "
                    f"- verify manually. The load CONTINUES and the table is marked loaded.")
            print("    " + note)
            _post_count_record_note(label, note)
            return note
        # count completed
        if actual == expected_total:
            note = (f"post_load_check=count_only [{label}]: OK - COUNT(*) {actual:,} == "
                    f"loaded {expected_total:,}.")
            print("    " + note)
            _post_count_record_note(label, note)
            return note
        # count completed and DIFFERS -> fail exactly as today (same text style, both numbers)
        _ = last_err
        raise _PostCountMismatch(
            f"VALIDATION FAILED: ROW COUNT: loaded {expected_total}, target has {actual} "
            f"(post_load_check=count_only whole-table COUNT(*)).")


def _post_count_record_note(label, note):
    """Make the count-only warning/OK note visible in whatever per-table status/summary the job
    already writes, IF that needs no schema change (append to an existing in-memory list keyed by
    table); otherwise just log (already done by the caller). Best-effort and never raises."""
    try:
        store = globals().get("_POST_LOAD_COUNT_NOTES")
        if store is None:
            store = {}
            globals()["_POST_LOAD_COUNT_NOTES"] = store
        store.setdefault(label, []).append(note)
    except Exception:
        pass


'''


def apply(in_path, out_path):
    src = open(in_path).read()
    old_sha = _sha256(src)

    # Idempotent: if already patched, do not patch again. Re-write the output to match the
    # (already patched) input so the caller still gets a file, and report no-op.
    if "POST_LOAD_COUNT_BUDGET_SECONDS" in src or "_post_count_only_check" in src:
        compile(src, "job2_load.py", "exec")
        if out_path != in_path:
            open(out_path, "w").write(src)
        print("already patched (no-op); sha256 unchanged: %s" % old_sha)
        return src

    miss = [r for r in REQUIRED if r not in src]
    assert not miss, (
        "this does not look like the expected job2_load.py; missing anchors: %s" % miss)

    # Fail closed: every anchor must be present EXACTLY ONCE. A modified/ambiguous anchor aborts.
    for name, anc in (("A1 optional-arg list", A1), ("A2 _apply_v6_arg_overrides() call", A2),
                      ("A3 validate_whole_table_after_ranges body", A3),
                      ("A4 single-table post-load block", A4)):
        n = src.count(anc)
        assert n == 1, "anchor %s not found exactly once (found %d) - refusing to patch" % (name, n)

    new = src.replace(A1, A1_NEW)
    # Prepend the block before the call line, and add the parse call right after it.
    new = new.replace(A2, "\n" + BLOCK + "_apply_v6_arg_overrides()\n_apply_post_load_check_override()\n")
    new = new.replace(A3, A3_NEW)
    new = new.replace(A4, A4_NEW)

    # Post-conditions: the patch text must be present exactly once; the arg wired once.
    assert new.count("def _post_count_only_check(") == 1, "block inserted != 1 time"
    # one def + exactly one call => 2 occurrences total, and the call must sit right after the
    # existing v6-override call.
    assert new.count("_apply_post_load_check_override()") == 2, "parse call wiring wrong"
    assert new.count("_apply_v6_arg_overrides()\n_apply_post_load_check_override()\n") == 1, \
        "parse call not wired exactly once after _apply_v6_arg_overrides()"
    assert new.count("def _apply_post_load_check_override():") == 1, "parser def inserted != 1 time"
    assert new.count("_post_count_only_check(dsql_schema, dsql_table, expected_total,\n"
                     "                               site='validate_whole_table_after_ranges')") == 1, \
        "A3 guard (validate_whole_table_after_ranges) not inserted exactly once"
    assert new.count("_post_count_only_check(dsql_schema, dsql_table, total_written,\n"
                     "                               site='load_one_table')") == 1, \
        "A4 guard (single-table post-load block) not inserted exactly once"

    compile(new, "job2_load.py", "exec")
    open(out_path, "w").write(new)
    print("sha256 old: %s" % old_sha)
    print("sha256 new: %s" % _sha256(new))
    print("patched -> %s" % out_path)
    return new


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    _in = sys.argv[1]
    _out = sys.argv[2] if len(sys.argv) == 3 else _in
    apply(_in, _out)
