#!/usr/bin/env python3
"""mrl_loader_resolver.py - EXACT COPY of the manual-range resolver functions extracted
from scripts/job2_load_manual_range.py (the patched loader the Glue load job runs).

Bundled with the mrl_plan Lambda ONLY so the plan can run proof (c) (resolver-consistency)
against the SAME resolver the real load uses. tests/test_helper_sync.py diffs this file
against the functions in the patched loader so they can never drift. Do not edit by hand.
"""
import re


class ManualRangeError(Exception):
    """A manual-range safety/validation check failed. Raised to FAIL the run (fail closed)."""

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
