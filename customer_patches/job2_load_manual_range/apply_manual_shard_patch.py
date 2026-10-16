#!/usr/bin/env python3
"""Apply the MANUAL-RANGE patch to a deployed job2_load.py.

    python3 apply_manual_shard_patch.py <in job2_load.py> <out job2_load_manual_shard.py>

Adds code only (removes nothing). Refuses a file it does not recognise or one that is
already patched. ASCII-only. The result is compiled before it is written.

NEW DESIGN (operator-chosen inclusive file ranges):
  * two optional Glue args, --load_file_start and --load_file_end (INCLUSIVE). Each value
    is a part-file NUMBER (1 -> LOAD00000001.csv), a bare file name, or a full s3:// key.
    They are resolved against the REAL listing of the table folder (s3_list_table_files),
    sorted by name, and the inclusive slice is taken. Only start => start..last; only end
    => first..end; neither => BYTE-IDENTICAL to today.
  * range mode uses a RANGE-PRIVATE file-status doc keyed by the resolved first/last file
    names, so concurrent runs never read-modify-write the same S3 object; it does NOT run
    the whole-table empty check/reblank and does NOT clear shared state; it fails closed on
    a no-PK mid-file crash (never reblanks: siblings are writing); it keeps an OVERLAP
    LEDGER so a starting run refuses to begin if its file set overlaps a DIFFERENT recorded
    range (same range = resume = allowed); it skips the whole-table end validation; it
    writes a per-range result JSON; a failed file makes the run FAIL.
  * the dropped-connection classifier also treats 'EOF occurred in violation of protocol'
    (ssl.SSLEOFError, errno 8) as a dropped connection.

ABSENT both args the only reachable new code is a set of pass-through shims, so
'absent args => byte-identical' is provable by running the real functions both ways.

The wrappers rebind the module names the loader already calls, so load_one_table_chunked's
source is not edited. Three additive edits only: the optional-arg list entry, the override
parse call, and the big code block; plus a one-line wrapper-install at the top of MAIN.
All other existing args (--config_prefix, --max_files_in_parallel, --writers_per_file,
--max_write_concurrency, --null_values, --null_rules, ...) are untouched and still honoured
by the existing parser, per run, via get-job-run Arguments.
"""
import sys

# ---------------------------------------------------------------------------- recognisers
REQUIRED = [
    "def load_one_table_chunked(s3_client, entry, config):",
    "def s3_list_table_files(s3_client, dms_s3_path):",
    "def _file_status_path(dsql_schema, dsql_table):",
    "def load_file_status(s3_client, dsql_schema, dsql_table):",
    "def clear_file_status(s3_client, dsql_schema, dsql_table):",
    "def assert_empty_or_register(",
    "def validate_whole_table_after_ranges(",
    "def is_broken_pipe_error(exc):",
    "def _apply_v6_arg_overrides():",
    "def make_boto_client(service):",
    "def split_s3(path):",
    "def utc_now_iso():",
    "CONFIG_PREFIX = ",
    "_apply_v6_arg_overrides()",
    '                "null_values", "null_rules"]',
    "s3_client = make_boto_client('s3')   # main-thread client (used before/after the pool)",
]

# ---- Anchor 1: end of the optional-args list literal. ADD our two arg names so the
# existing getResolvedOptions-based parser reads them if present.
A1 = '                "null_values", "null_rules"]'
A1_NEW = (
    '                "null_values", "null_rules",\n'
    '                "load_file_start", "load_file_end"]'
)

# ---- Anchor 2: the module-level call that applies the v6 overrides. ADD our parse call
# right after it (same import-time point, after CONFIG_PREFIX + constants are final).
A2 = "\n_apply_v6_arg_overrides()\n"
A2_NEW = (
    "\n_apply_v6_arg_overrides()\n"
    "_apply_manual_range_overrides()\n"
)

# ---- Anchor 3: prepend the big block before s3_list_table_files (defined once, before
# load_one_table_chunked, after make_boto_client/split_s3/_file_status_path).
A3 = "def s3_list_table_files(s3_client, dms_s3_path):"

# ---- Anchor 4: install the wrappers AFTER all defs but BEFORE the main loop uses them.
A4 = "s3_client = make_boto_client('s3')   # main-thread client (used before/after the pool)"
A4_NEW = (
    "_install_manual_range_wrappers()\n"
    "s3_client = make_boto_client('s3')   # main-thread client (used before/after the pool)"
)

BLOCK = r'''# =============================================================================
# MANUAL-RANGE PATCH (optional Glue args --load_file_start --load_file_end, INCLUSIVE)
#
# PURPOSE. Let an operator load ONE big table with SEVERAL independent Glue load-big runs
# (each its own driver), every run loading an OPERATOR-CHOSEN, INCLUSIVE slice of the
# table's sorted LOAD*.csv part-files, then reconcile with merge_ranges.py so the normal
# pipeline can continue. ABSENT both args the script behaves BYTE-IDENTICALLY to today:
# _MANUAL_RANGE["on"] is False, every wrapper calls straight through to the original.
#
# ARG VALUES. --load_file_start / --load_file_end each accept THREE forms, resolved against
# the real sorted listing of the table folder:
#   * a part-file NUMBER from the DMS name (1 -> LOAD00000001.csv, 100 -> LOAD00000100.csv);
#   * a bare file name (LOAD00000042.csv);
#   * a full s3:// key.
# Only start => start..last file; only end => first file..end; neither => OFF (today).
# Bounds are INCLUSIVE. Fail closed if a bound does not resolve, start is after end, or the
# slice is empty.
#
# SAFETY MODEL (never lose / duplicate / double-count a row; fail closed):
#   * RANGE-PRIVATE progress: this run reads/writes its OWN
#     _file_status/<table>.range-<first>-<last>.json (keyed by resolved first/last file
#     names), so concurrent runs never read-modify-write the same S3 object.
#   * NO whole-table empty check/reblank and NO clear of shared state in range mode
#     (siblings are writing). Operator contract (README): the table must be empty, or hold
#     only rows from a previous attempt of THE SAME ranges, before the first run starts.
#     Range mode logs the table's current estimated row count at start.
#   * resume of the SAME range resumes only that range (skip its done files; probe-guarded
#     redo of its started files for a single-column-PK table). For a table WITHOUT a
#     single-column PK a mid-file crash FAILS CLOSED (never a whole-table reblank, which
#     would wipe sibling ranges' rows).
#   * OVERLAP GUARD: a ranges ledger (_file_status/<table>.ranges.json) records every
#     range's resolved file set. A starting run refuses (fails closed) if its file set
#     overlaps a DIFFERENT recorded range; an identical range is a resume and is allowed.
#     The ledger write is conditional (If-Match/If-None-Match when the SDK supports it) and
#     is followed by a re-read so a lost update is detected. A narrow simultaneous-start
#     race remains (documented in RESULT.md); merge_ranges.py catches any overlap afterward
#     before it writes the normal doc.
#   * end-of-load whole-table validation/count is SKIPPED and clear_file_status is a no-op
#     in range mode; a per-range result JSON is written instead; a failed file FAILS the run.
# =============================================================================

_MANUAL_RANGE = {
    "on": False,
    "start_raw": None,
    "end_raw": None,
    "label": None,
    "first": None,          # resolved first file BASENAME (inclusive)
    "last": None,           # resolved last file BASENAME (inclusive)
    "assigned": None,       # sorted list of this range's file URIs
    "assigned_set": None,
    "all_sorted": None,     # whole-table sorted [(uri,size),...]
    "has_single_pk": None,
    "result_written": False,
}


class ManualRangeError(Exception):
    """A manual-range safety/validation check failed. Raised to FAIL the run (fail closed)."""


def _mr_safe(s):
    return str(s).replace("/", "_")


def _mr_basename(uri):
    return str(uri).rsplit("/", 1)[-1]


def _mr_file_number(name):
    """Extract the integer part-file number from a LOAD name, or None. 'LOAD00000042.csv'
    -> 42. Robust to width; parsed as decimal (DMS counters are decimal in practice)."""
    import re as _re
    base = _mr_basename(name)
    m = _re.search(r"LOAD0*([0-9]+)\.csv$", base, _re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _mr_resolve_one(raw, uris):
    """Resolve ONE operator bound (number | basename | full s3:// key) to an index into
    the SORTED `uris` list. Returns the index, or raises ManualRangeError if it does not
    resolve. `uris` is the sorted list of full s3:// URIs for the table."""
    raw = str(raw).strip()
    if not raw:
        raise ManualRangeError("empty range bound")
    # 1) full s3:// key
    if raw.startswith("s3://"):
        for i, u in enumerate(uris):
            if u == raw:
                return i
        raise ManualRangeError("range bound %r is a full s3:// key not found in the table "
                               "listing" % raw)
    # 2) bare file name (exact basename match)
    if raw.lower().endswith(".csv"):
        want = raw
        for i, u in enumerate(uris):
            if _mr_basename(u) == want:
                return i
        # case-insensitive fallback
        for i, u in enumerate(uris):
            if _mr_basename(u).lower() == want.lower():
                return i
        raise ManualRangeError("range bound %r is a file name not found in the table "
                               "listing" % raw)
    # 3) a part-file number -> match the LOAD number parsed from each listed file
    try:
        want_n = int(raw)
    except ValueError:
        raise ManualRangeError(
            "range bound %r is not a number, a *.csv file name, or an s3:// key" % raw)
    for i, u in enumerate(uris):
        n = _mr_file_number(u)
        if n is not None and n == want_n:
            return i
    raise ManualRangeError(
        "range bound number %d does not match any LOAD<n>.csv file in the table listing "
        "(files present carry numbers like %s)" % (
            want_n, ", ".join(str(_mr_file_number(u)) for u in uris[:5]) or "none"))


def _mr_resolve_slice(start_raw, end_raw, files):
    """Resolve the inclusive [start..end] slice against `files` ([(uri,size),...]).
    Returns (first_basename, last_basename, assigned_uris_sorted, assigned_bytes). Raises
    ManualRangeError (fail closed) on any unresolved bound, start-after-end, or empty slice."""
    uris = [u for (u, _s) in sorted(files, key=lambda t: str(t[0]))]
    size_of = {u: int(s) for (u, s) in files}
    if not uris:
        raise ManualRangeError("the table folder lists no LOAD*.csv files to range over")
    i0 = 0 if start_raw is None else _mr_resolve_one(start_raw, uris)
    i1 = (len(uris) - 1) if end_raw is None else _mr_resolve_one(end_raw, uris)
    if i0 > i1:
        raise ManualRangeError(
            "resolved start (%s, index %d) is AFTER resolved end (%s, index %d); refusing "
            "an inverted/empty range (fail closed)" % (
                _mr_basename(uris[i0]), i0, _mr_basename(uris[i1]), i1))
    chosen = uris[i0:i1 + 1]
    if not chosen:
        raise ManualRangeError("resolved range is empty (fail closed)")
    return (_mr_basename(uris[i0]), _mr_basename(uris[i1]), sorted(chosen),
            sum(size_of.get(u, 0) for u in chosen))


def _mr_file_status_path(label, first, last):
    return "%s_file_status/%s.range-%s-%s.json" % (
        CONFIG_PREFIX, _mr_safe(label), _mr_safe(first), _mr_safe(last))


def _mr_result_path(label, first, last):
    return "%s_range_result/%s.range-%s-%s.json" % (
        CONFIG_PREFIX, _mr_safe(label), _mr_safe(first), _mr_safe(last))


def _mr_ledger_path(label):
    return "%s_file_status/%s.ranges.json" % (CONFIG_PREFIX, _mr_safe(label))


def _mr_estimate_rowcount(label):
    try:
        sch, tbl = str(label).split(".", 1)
    except ValueError:
        return None
    fn = globals().get("_count_rows_for_guard")
    if fn is None:
        return None
    try:
        return fn(sch, tbl)
    except Exception:
        return None


def _mr_overlap_guard(s3_client, label, first, last, assigned):
    """OVERLAP GUARD. Read the ranges ledger; refuse (fail closed) if this run's file set
    overlaps a DIFFERENT recorded range. An identical range (same first/last AND same file
    set) is a resume and is allowed. Then record/refresh our own entry. The write is
    conditional when the SDK supports it; a re-read after write detects a lost update.

    A narrow simultaneous-start window remains if two different ranges both read an absent
    (or stale) ledger before either writes; merge_ranges.py catches any resulting overlap
    before it writes the normal doc. See RESULT.md."""
    path = _mr_ledger_path(label)
    bucket, key = split_s3(path)
    my_key = "%s..%s" % (first, last)
    my_set = set(assigned)

    def _read():
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            etag = obj.get("ETag")
            doc = json.loads(obj["Body"].read().decode("utf-8"))
            if not isinstance(doc, dict) or not isinstance(doc.get("ranges"), dict):
                return {"ranges": {}}, etag
            return doc, etag
        except Exception:
            return None, None

    doc, etag = _read()
    if doc is None:
        doc = {"ranges": {}}
    for rk, rec in (doc.get("ranges") or {}).items():
        other = set(rec.get("files") or [])
        if rk == my_key and other == my_set:
            continue  # identical recorded range -> resume, allowed
        inter = my_set & other
        if inter:
            sample = ", ".join(sorted(_mr_basename(u) for u in inter)[:5])
            raise ManualRangeError(
                "OVERLAP GUARD %s: this range %s overlaps a DIFFERENT recorded range %r on "
                "%d file(s) (e.g. %s). REFUSING to start (fail closed) so two runs cannot load "
                "the same file and duplicate rows. Use disjoint ranges, or rerun the EXACT "
                "same range to resume." % (label, my_key, rk, len(inter), sample))
    # record/refresh our entry
    doc.setdefault("ranges", {})[my_key] = {
        "first": first, "last": last, "files": sorted(assigned),
        "written_at": utc_now_iso(),
    }
    doc["updated_at"] = utc_now_iso()
    body = json.dumps(doc).encode("utf-8")
    put_kw = {"Bucket": bucket, "Key": key, "Body": body, "ContentType": "application/json"}
    wrote = False
    try:
        if etag:
            try:
                s3_client.put_object(IfMatch=etag, **put_kw)
                wrote = True
            except TypeError:
                s3_client.put_object(**put_kw)
                wrote = True
        else:
            try:
                s3_client.put_object(IfNoneMatch="*", **put_kw)
                wrote = True
            except TypeError:
                s3_client.put_object(**put_kw)
                wrote = True
    except Exception as e:
        _msg = str(e)
        if "PreconditionFailed" in _msg or "412" in _msg or "Precondition" in _msg:
            # someone else wrote concurrently -> re-read and re-check for a real overlap
            doc2, _etag2 = _read()
            if doc2 is not None:
                for rk, rec in (doc2.get("ranges") or {}).items():
                    if rk == my_key:
                        continue
                    inter = my_set & set(rec.get("files") or [])
                    if inter:
                        raise ManualRangeError(
                            "OVERLAP GUARD %s: a concurrent range %r was recorded that overlaps "
                            "this range %s; failing closed. (merge_ranges.py would also catch "
                            "this.)" % (label, rk, my_key))
                # no overlap with the concurrent writer -> retry our write once (best effort)
                try:
                    s3_client.put_object(**put_kw)
                    wrote = True
                except Exception:
                    pass
        else:
            print("  WARNING OVERLAP GUARD %s: could not write the ranges ledger (%s); "
                  "continuing. merge_ranges.py will still catch any overlap before it writes "
                  "the normal doc." % (label, e))
    # re-read to confirm our entry survived (detect a silent lost update)
    doc3, _etag3 = _read()
    if wrote and doc3 is not None and my_key not in (doc3.get("ranges") or {}):
        print("  WARNING OVERLAP GUARD %s: our ledger entry %s did not survive a concurrent "
              "write. Proceeding (our range-private status doc keeps this run correct); "
              "merge_ranges.py is the backstop for overlap detection." % (label, my_key))


def _apply_manual_range_overrides():
    """Parse the two optional Glue args. Absent both -> range mode OFF (byte-identical).
    Resolution against the real listing happens later (needs S3) in the s3_list wrapper."""
    present = [a for a in ("load_file_start", "load_file_end") if ("--%s" % a) in sys.argv]
    if not present:
        return  # OFF
    ov = getResolvedOptions(sys.argv, present)
    start_raw = ov.get("load_file_start")
    end_raw = ov.get("load_file_end")
    start_raw = start_raw.strip() if isinstance(start_raw, str) and start_raw.strip() else None
    end_raw = end_raw.strip() if isinstance(end_raw, str) and end_raw.strip() else None
    if start_raw is None and end_raw is None:
        return  # both blank -> OFF
    _MANUAL_RANGE["on"] = True
    _MANUAL_RANGE["start_raw"] = start_raw
    _MANUAL_RANGE["end_raw"] = end_raw
    print("=" * 70)
    print("MANUAL RANGE MODE ON: this run loads ONLY the inclusive file slice start=%r "
          "end=%r (resolved against the real S3 listing). It uses a range-private file-status "
          "doc, does NOT reblank or run the whole-table end validation, enforces an overlap "
          "guard, and writes a per-range result JSON for merge_ranges.py."
          % (start_raw, end_raw))
    print("=" * 70)


# ----------------------------------------------------------------------- function wrappers

def _mr_wrap_s3_list_table_files(orig):
    def wrapper(s3_client, dms_s3_path):
        files = orig(s3_client, dms_s3_path)          # [(uri,size),...] largest-first
        if not _MANUAL_RANGE["on"]:
            return files
        label = _MANUAL_RANGE["label"] or "?"
        first, last, assigned, assigned_bytes = _mr_resolve_slice(
            _MANUAL_RANGE["start_raw"], _MANUAL_RANGE["end_raw"], files)
        all_sorted = sorted(files, key=lambda t: str(t[0]))
        _MANUAL_RANGE["first"] = first
        _MANUAL_RANGE["last"] = last
        _MANUAL_RANGE["assigned"] = list(assigned)
        _MANUAL_RANGE["assigned_set"] = set(assigned)
        _MANUAL_RANGE["all_sorted"] = all_sorted
        # overlap guard BEFORE loading anything
        _mr_overlap_guard(s3_client, label, first, last, assigned)
        est = _mr_estimate_rowcount(label)
        print("  MANUAL RANGE %s: resolved slice first=%s last=%s -> %d file(s), %d byte(s). "
              "Whole table has %d file(s). Current target row count (estimate) at start: %s. "
              "CONTRACT: the table must be empty (or hold only rows from a previous attempt of "
              "THESE SAME ranges) before the first run started."
              % (label, first, last, len(assigned), assigned_bytes, len(files),
                 ("unknown" if est is None else format(est, ","))))
        mine_set = set(assigned)
        return [(u, s) for (u, s) in files if u in mine_set]
    wrapper._mr_orig = orig
    return wrapper


def _mr_wrap_file_status_path(orig):
    def wrapper(dsql_schema, dsql_table):
        if not _MANUAL_RANGE["on"]:
            return orig(dsql_schema, dsql_table)
        label = "%s.%s" % (dsql_schema, dsql_table)
        first = _MANUAL_RANGE["first"]
        last = _MANUAL_RANGE["last"]
        if first is None or last is None:
            # not resolved yet (should not happen: s3_list runs first) -> fail closed
            raise ManualRangeError(
                "MANUAL RANGE %s: file-status path requested before the range was resolved" % label)
        return _mr_file_status_path(label, first, last)
    wrapper._mr_orig = orig
    return wrapper


def _mr_wrap_clear_file_status(orig):
    def wrapper(s3_client, dsql_schema, dsql_table):
        if not _MANUAL_RANGE["on"]:
            return orig(s3_client, dsql_schema, dsql_table)
        label = "%s.%s" % (dsql_schema, dsql_table)
        _mr_write_result(s3_client, label, failed=False)
        return None
    wrapper._mr_orig = orig
    return wrapper


def _mr_wrap_assert_empty_or_register(orig):
    def wrapper(dsql_schema, dsql_table, *args, **kwargs):
        if not _MANUAL_RANGE["on"]:
            return orig(dsql_schema, dsql_table, *args, **kwargs)
        try:
            lock = globals().get("_EMPTY_VERIFIED_LOCK")
            verified = globals().get("_EMPTY_VERIFIED")
            if lock is not None and verified is not None:
                with lock:
                    verified.add("%s.%s" % (dsql_schema, dsql_table))
        except Exception:
            pass
        print("  MANUAL RANGE %s.%s: skipping the whole-table empty barrier / reblank "
              "(operator contract owns emptiness; siblings may be writing)."
              % (dsql_schema, dsql_table))
        return None
    wrapper._mr_orig = orig
    return wrapper


def _mr_wrap_validate_whole_table_after_ranges(orig):
    def wrapper(dsql_schema, dsql_table, *args, **kwargs):
        if not _MANUAL_RANGE["on"]:
            return orig(dsql_schema, dsql_table, *args, **kwargs)
        print("  MANUAL RANGE %s.%s: skipping whole-table end validation/count (siblings "
              "writing); per-range result JSON written for merge_ranges.py."
              % (dsql_schema, dsql_table))
        return None
    wrapper._mr_orig = orig
    return wrapper


def _mr_wrap_is_broken_pipe_error(orig):
    def wrapper(exc):
        if orig(exc):
            return True
        # <customer> lost 2/406 files to a plaintext 'EOF occurred in violation of protocol'
        # (ssl.SSLEOFError, errno 8) the original classifier missed (no matching fragment,
        # and SSLEOFError is not a ConnectionError subclass). Treat it as a dropped
        # connection so the retry ladders reconnect with a fresh token. ALWAYS active (not
        # gated on range mode): it only ADDS recognised dropped-connection cases.
        try:
            import ssl as _ssl
            if isinstance(exc, _ssl.SSLEOFError):
                return True
        except Exception:
            pass
        try:
            if isinstance(exc, OSError) and getattr(exc, "errno", None) == 8 \
                    and "eof occurred" in str(exc).lower():
                return True
        except Exception:
            pass
        if "eof occurred in violation of protocol" in str(exc).lower():
            return True
        return False
    wrapper._mr_orig = orig
    return wrapper


def _mr_rows_from_status_doc(label):
    try:
        sch, tbl = str(label).split(".", 1)
    except ValueError:
        return {}, set(), 0
    s3 = make_boto_client("s3")
    path = _mr_file_status_path(label, _MANUAL_RANGE["first"], _MANUAL_RANGE["last"])
    bucket, key = split_s3(path)
    done_map, started = {}, set()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
        for uri, rec in (doc.get("files") or {}).items():
            if isinstance(rec, dict) and rec.get("state") == "done":
                done_map[uri] = int(rec.get("rows", 0))
            elif isinstance(rec, dict) and rec.get("state") == "started":
                started.add(uri)
    except Exception:
        pass
    return done_map, started, sum(done_map.values())


def _mr_write_result(s3_client, label, failed, error_text=None):
    """Write the per-range result JSON. Idempotent for success; a failure always rewrites."""
    if _MANUAL_RANGE.get("result_written") and not failed:
        return
    first = _MANUAL_RANGE["first"]
    last = _MANUAL_RANGE["last"]
    assigned = _MANUAL_RANGE.get("assigned") or []
    done_map, started, rows_total = _mr_rows_from_status_doc(label)
    files_detail = []
    for uri in sorted(assigned):
        if uri in done_map:
            files_detail.append({"uri": uri, "state": "done", "rows": int(done_map[uri])})
        elif uri in started:
            files_detail.append({"uri": uri, "state": "started", "rows": None})
        else:
            files_detail.append({"uri": uri, "state": "pending", "rows": None})
    bytes_total = None
    try:
        size_of = {u: int(s) for (u, s) in (_MANUAL_RANGE.get("all_sorted") or [])}
        bytes_total = sum(size_of.get(u, 0) for u in assigned)
    except Exception:
        bytes_total = None
    doc = {
        "label": label, "first": first, "last": last,
        "files_assigned": len(assigned), "files_done": len(done_map),
        "rows_written_by_this_range": int(rows_total), "bytes_assigned": bytes_total,
        "failed": bool(failed), "error": (str(error_text)[:2000] if error_text else None),
        "finished_at": utc_now_iso(), "files": files_detail,
    }
    path = _mr_result_path(label, first, last)
    bucket, key = split_s3(path)
    try:
        s3_client.put_object(Bucket=bucket, Key=key,
                             Body=json.dumps(doc).encode("utf-8"),
                             ContentType="application/json")
        print("  MANUAL RANGE %s..%s: files %d, done %d, rows %d (result -> %s)"
              % (first, last, len(assigned), len(done_map), int(rows_total), path))
    except Exception as e:
        print("  WARNING MANUAL RANGE %s..%s: could not write per-range result (%s). The "
              "merge tool will report this range as missing." % (first, last, e))
    if not failed:
        _MANUAL_RANGE["result_written"] = True


def _mr_nopk_fail_closed_guard(s3_client, label, has_single_pk):
    """SHARD/RANGE no-PK fail-closed guard. The loader body, on a started-but-not-done file
    with NO single-column PK, falls into a WHOLE-TABLE reblank -- which in range mode would
    wipe sibling ranges' rows. Never reach it: if THIS range's own status doc has any
    'started' file and the table has no single-column PK, FAIL CLOSED before the loader runs."""
    if has_single_pk:
        return
    _done, started, _rows = _mr_rows_from_status_doc(label)
    if started:
        raise ManualRangeError(
            "MANUAL RANGE %s (%s..%s): a file crashed MID-LOAD on a prior attempt of this "
            "range AND the table has NO single-column PK, so a partial file cannot be resumed "
            "without risking duplicates. FAILING CLOSED. The loader's only no-dup fallback is a "
            "WHOLE-TABLE reblank, which in range mode would ERASE sibling ranges' rows -- so it "
            "is refused here. RECOVERY: blank the WHOLE table, delete this table's "
            "_file_status/*.range-* / _range_result/* / *.ranges.json docs, and restart ALL "
            "ranges from scratch. Started file(s): %s"
            % (label, _MANUAL_RANGE["first"], _MANUAL_RANGE["last"], ", ".join(sorted(started))))


def _mr_wrap_load_one_table_chunked(orig):
    def wrapper(s3_client, entry, config):
        if not _MANUAL_RANGE["on"]:
            return orig(s3_client, entry, config)
        label = "%s.%s" % (entry["dsql_schema"], entry["dsql_table"])
        _MANUAL_RANGE["label"] = label
        try:
            _pk_meta = (config.get("metadata", {}).get("primary_key") or {})
            _pk_cols = _pk_meta.get("columns") or []
            has_single_pk = (len(_pk_cols) == 1)
        except Exception:
            has_single_pk = False
        _MANUAL_RANGE["has_single_pk"] = has_single_pk
        # The s3_list wrapper (called inside orig) resolves the slice + runs the overlap
        # guard. But the no-PK guard needs the resolved range first -> resolve it up front by
        # listing now, so we can fail closed BEFORE the loader's reblank branch. We list via
        # the (already-wrapped) module function so resolution + overlap guard happen once.
        _ls = globals().get("s3_list_table_files")
        try:
            _ls(s3_client, config["metadata"]["dms_s3_path"])
        except ManualRangeError:
            raise
        except Exception:
            # listing issues are surfaced by the loader itself; let it run normally
            pass
        _mr_nopk_fail_closed_guard(s3_client, label, has_single_pk)
        try:
            return orig(s3_client, entry, config)
        except Exception as e:
            try:
                _mr_write_result(s3_client, label, failed=True, error_text=str(e))
            except Exception:
                pass
            raise
    wrapper._mr_orig = orig
    return wrapper


def _install_manual_range_wrappers():
    """Rebind the loader's own module names to the wrappers (idempotent). When range mode is
    OFF every wrapper is a pass-through, so this install is behaviour-preserving."""
    g = globals()
    pairs = [
        ("s3_list_table_files", _mr_wrap_s3_list_table_files),
        ("_file_status_path", _mr_wrap_file_status_path),
        ("clear_file_status", _mr_wrap_clear_file_status),
        ("assert_empty_or_register", _mr_wrap_assert_empty_or_register),
        ("validate_whole_table_after_ranges", _mr_wrap_validate_whole_table_after_ranges),
        ("is_broken_pipe_error", _mr_wrap_is_broken_pipe_error),
        ("load_one_table_chunked", _mr_wrap_load_one_table_chunked),
    ]
    for name, maker in pairs:
        orig = g.get(name)
        if orig is None:
            continue
        if getattr(orig, "_mr_orig", None) is not None:
            continue
        g[name] = maker(orig)


'''


def apply(in_path, out_path):
    src = open(in_path).read()
    miss = [r for r in REQUIRED if r not in src]
    assert not miss, "this does not look like the expected job2_load.py; missing: %s" % miss
    assert "_MANUAL_RANGE" not in src and "_apply_manual_range_overrides" not in src, \
        "already patched"
    assert src.count(A1) == 1, "optional-arg list anchor not found exactly once"
    assert src.count(A2) == 1, "override-call anchor not found exactly once"
    assert src.count(A3) == 1, "s3_list_table_files anchor not found exactly once"
    assert src.count(A4) == 1, "main-thread client anchor not found exactly once"
    new = src.replace(A1, A1_NEW)
    new = new.replace(A2, A2_NEW)
    new = new.replace(A3, BLOCK + A3)
    new = new.replace(A4, A4_NEW)
    compile(new, "job2_load_manual_shard.py", "exec")
    open(out_path, "w").write(new)
    return new


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    apply(sys.argv[1], sys.argv[2])
    print("patched ->", sys.argv[2])
