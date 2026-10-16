#!/usr/bin/env python3
"""mrl_split.py - the pure, deterministic file-discovery + range-split core of the MRL plan.

NO AWS, NO Spark, NO network: every function here is a pure function over plain Python data
(the S3 client is injected). The plan Lambda (lambdas/mrl_plan.py) imports this; the tests
exercise it exhaustively. Offline-safe and Linux-safe (no strftime/locale/timezone use).

CONTRACT (see DESIGN.md sections 2 and 5):
  * Canonical ordering = lexicographic by the full s3:// URI (== by basename within one
    folder). This is EXACTLY what the loader's resolver (_mr_resolve_slice) and
    merge_ranges.plan_ranges do, so a range's start/end names resolve identically.
  * Listing accepts only keys DIRECTLY in the table folder whose basename matches a DMS
    full-load name; processed/ and failed/ subfolders and any sub-folder are excluded,
    mirroring the loader's _is_full_load_key plus the fixed merge_ranges decimal rule.
  * split_ranges(n, c): k = min(c, n); the first (n mod k) ranges get floor(n/k)+1 files,
    the rest floor(n/k); contiguous, inclusive, disjoint, ordered; union == the sorted list.
"""

import hashlib
import re


# --------------------------------------------------------------------------- name filters
# AMENDMENT 6: ONE shared allow-list + explicit CDC deny-list, the SINGLE source of truth used
# by the plan, the splitter, reconcile and merge logic. No second regex anywhere.
#
# ALLOW-LIST: a DMS DEFAULT full-load basename, case-insensitive ^load[0-9a-f]+\.csv$ (DMS writes
#   LOAD00000001.csv ...; hex kept because the loader comment says names can be hex). Plus: the
#   key must be a DIRECT child of the table folder, size > 0, and NOT under processed/ or failed/.
# CDC DENY-LIST (defence in depth, tested separately): a DMS CDC change-file basename
#   ^[0-9]{8}-[0-9]+\.csv$ (e.g. 20261002-123456789.csv), anything under processed/ or failed/,
#   _manifest.json, notes.csv, *.tmp, and every name that does not start with 'load'.
_ALLOW_LOAD_RE = re.compile(r"^load[0-9a-f]+\.csv$", re.IGNORECASE)
_DECIMAL_BODY_RE = re.compile(r"^load0*[0-9]+\.csv$", re.IGNORECASE)
_CDC_TIMESTAMP_RE = re.compile(r"^[0-9]{8}-[0-9]+\.csv$")
# Width check: the hex/decimal body between LOAD and .csv.
_LOAD_BODY_RE = re.compile(r"^load([0-9a-f]+)\.csv$", re.IGNORECASE)
# Pattern strings persisted in plan.json (the actual rules used, as text).
ALLOW_PATTERN = r"(?i)^load[0-9a-f]+\.csv$"
CDC_PATTERN = r"^[0-9]{8}-[0-9]+\.csv$"


class MrlSplitError(Exception):
    """A discovery/split consistency check failed. Raised to FAIL CLOSED (never silently)."""


def basename(uri):
    return str(uri).rsplit("/", 1)[-1]


def split_s3(path):
    assert str(path).startswith("s3://"), "expected s3:// uri, got %r" % (path,)
    rest = str(path)[5:]
    b, _, k = rest.partition("/")
    return b, k


def is_cdc_key(key):
    """Explicit CDC deny-list (Amendment 6). True for a DMS CDC change file or a processed/failed
    copy: a timestamp-named basename ^[0-9]{8}-[0-9]+\\.csv$, OR any key under a processed/ or
    failed/ path segment. Used for classification/reporting and the separately-tested second
    gate; is_full_load_key is the primary gate (a CDC name can never match the allow-list)."""
    kl = str(key).lower()
    segments = kl.split("/")
    if "processed" in segments or "failed" in segments:
        return True
    return _CDC_TIMESTAMP_RE.match(segments[-1]) is not None


def is_full_load_key(key, allow_hex=True):
    """THE shared allow-list test (Amendment 6). True ONLY for a DMS default full-load basename
    ^load[0-9a-f]+\\.csv$ (case-insensitive) with NO processed/ or failed/ path segment. Hex is
    accepted by default (DMS names can be hex); pass allow_hex=False to restrict to decimal
    (^load0*[0-9]+\\.csv$). Direct-child and size>0 are enforced by the LISTING (this is a pure
    basename/path test). A CDC timestamp name never matches ^load..., so this can never let a CDC
    file through; is_cdc_key is the explicit, separately-tested deny-list."""
    kl = str(key).lower()
    segments = kl.split("/")
    if "processed" in segments or "failed" in segments:
        return False
    base = segments[-1]
    if allow_hex:
        return _ALLOW_LOAD_RE.match(base) is not None
    return _DECIMAL_BODY_RE.match(base) is not None


def classify_basename(key, size=1, allow_hex=True):
    """Classify a DIRECT-CHILD key for reporting (Amendment 6 counts): returns one of
    'allowed' | 'zero_byte' | 'cdc' | 'other'. 'allowed' requires the allow-list AND size>0;
    a zero-byte allow-list name is 'zero_byte' (reported, not loaded, like the loader). A CDC
    timestamp / processed / failed key is 'cdc'; everything else is 'other'."""
    if is_cdc_key(key):
        return "cdc"
    if is_full_load_key(key, allow_hex=allow_hex):
        return "zero_byte" if int(size) <= 0 else "allowed"
    return "other"


def _load_body_len(name):
    """Length of the digit/hex body of a LOAD<body>.csv name, or None if it doesn't match."""
    m = _LOAD_BODY_RE.match(str(name))
    return len(m.group(1)) if m else None


def loader_is_full_load_key(key):
    """AMENDMENT 2 fact (a): the LOADER's own _is_full_load_key (scripts/job2_load.py) is LOOSER
    than ours - it accepts ANY basename that startswith 'load' and endswith '.csv' (after
    excluding a 'processed'/'failed' path segment). We mirror it EXACTLY so we can compute the
    set the loader WOULD load (S_loader) and compare it to the set we plan (S_plan). Byte-for-byte
    the same rule as the deployed loader: lowercase, split on '/', reject processed/failed, then
    base.startswith('load') and base.endswith('.csv')."""
    kl = str(key).lower()
    segments = kl.split("/")
    if "processed" in segments or "failed" in segments:
        return False
    base = segments[-1]
    return base.startswith("load") and base.endswith(".csv")


def loader_full_load_keys(s3_client, dms_s3_path):
    """S_loader: EXACTLY the set of keys the deployed loader's s3_list_table_files would load
    under the table prefix - i.e. every key (at the root OR in a partition SUB-FOLDER, as DMS
    writes for a parallel full load) whose basename passes the loader's LOOSE _is_full_load_key
    and whose size > 0, excluding processed/ and failed/. Returns the set of RELATIVE keys (the
    part after the table prefix), so a partition sub-folder file like '1/LOAD00000001.csv' is
    represented distinctly from a root 'LOAD00000001.csv'. This is the authoritative set the
    Amendment-2/A3 cross-check compares the plan against (the loader INCLUDES sub-folders; MRL's
    plan is root-only, so any sub-folder LOAD key makes the cross-check FAIL CLOSED)."""
    bucket, prefix = split_s3(dms_s3_path)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    rels = set()
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kw)
        for o in resp.get("Contents", []) or []:
            key = o["Key"]
            size = int(o.get("Size", 0))
            if key.endswith("/") or size <= 0:
                continue  # the loader skips directory placeholders and size<=0
            if loader_is_full_load_key(key):   # loader's LOOSE rule (basename load*.csv, no proc/fail)
                rel = key[len(prefix):] if key.startswith(prefix) else key
                rels.add(rel)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return rels


def assert_plan_equals_loader_set(s3_client, dms_s3_path, planned_files, allow_hex=False):
    """AMENDMENT 2 RULE (fail closed, no guessing): the plan's file set MUST EQUAL the set the
    loader would load.

      S_loader = loader_full_load_keys(...)                 (the loader's LOOSE predicate)
      S_plan   = basenames of `planned_files`               (our STRICT ^load[0-9a-f]+\\.csv$ /
                 decimal listing, uniform width already asserted by the caller)

    If S_loader != S_plan, FAIL CLOSED naming the exact differing names and the fix:
      * a name in S_loader but NOT S_plan (e.g. a stray 'load_notes.csv', or a mixed-width name
        the strict width filter / assert_uniform_width excluded) WOULD be loaded by the loader
        but is not planned -> it would be loaded by no range (never counted) OR, if the operator
        hand-ran a range, double-loaded. Remove or rename it.
      * a name in S_plan but NOT S_loader (should be impossible since strict ⊆ loose, but checked)
        -> investigate.

    Zero tolerance. `planned_files` is the strict [(uri,size)] listing used to build the ranges.
    """
    s_plan = set(basename(u) for u, _s in planned_files)
    s_loader = loader_full_load_keys(s3_client, dms_s3_path)   # relative keys (sub-folders too)
    only_loader = sorted(s_loader - s_plan)
    only_plan = sorted(s_plan - s_loader)
    if only_loader or only_plan:
        parts = []
        if only_loader:
            sub = [r for r in only_loader if "/" in r]
            parts.append(
                "the loader WOULD load %d file(s) the plan did NOT include: %s. This includes any "
                "partition SUB-FOLDER LOAD files (%d: %s) that a DMS PARALLEL full load writes "
                "(e.g. <table>/1/LOAD...csv) - the loader globs the whole table prefix and loads "
                "them, but MRL plans only the direct children, so they would be loaded by NO "
                "range (lost) or double-loaded. It also catches a stray 'load_notes.csv', a hex "
                "name when the plan is decimal-only, or a mixed-width name. FIX: for a parallel "
                "full load, re-export without partition sub-folders (or load via the standard "
                "route); otherwise remove/rename the stray file."
                % (len(only_loader), ", ".join(only_loader[:10]), len(sub),
                   ", ".join(sub[:5]) or "none"))
        if only_plan:
            parts.append(
                "the plan included %d file(s) the loader would NOT load: %s (unexpected; the "
                "strict pattern should be a subset of the loose one). Investigate."
                % (len(only_plan), ", ".join(only_plan[:10])))
        raise MrlSplitError(
            "FAIL CLOSED (Amendment 2/3): the plan's file set does not equal the set the deployed "
            "loader would load for %s. %s A file loaded by the loader but not planned would be "
            "loaded by no range (never counted) or double-loaded; a file planned but not loaded "
            "breaks reconcile. Zero tolerance." % (dms_s3_path, " ".join(parts)))
    return True


def assert_names_allow_listed(names, where, allow_hex=True):
    """AMENDMENT 6: FAIL CLOSED if any basename in `names` is NOT on the allow-list, or matches
    the explicit CDC deny-list. Used by the plan (planned names, range start/end) and by
    reconcile (every progress/done entry) - ONE shared rule, no second regex. `where` names the
    context for the message (file names are safe to show; they are not row values)."""
    bad_cdc = sorted(n for n in names if _CDC_TIMESTAMP_RE.match(str(n)))
    bad_other = sorted(n for n in names
                       if not is_full_load_key(str(n), allow_hex=allow_hex)
                       and n not in bad_cdc)
    if bad_cdc or bad_other:
        parts = []
        if bad_cdc:
            parts.append("CDC change file(s) %s (timestamp pattern %s) - CDC files are NEVER "
                         "loaded" % (", ".join(bad_cdc[:10]), CDC_PATTERN))
        if bad_other:
            parts.append("non-full-load name(s) %s (allow-list is %s)"
                         % (", ".join(bad_other[:10]), ALLOW_PATTERN))
        raise MrlSplitError(
            "FAIL CLOSED (Amendment 6): %s contains file(s) outside the full-load allow-list: %s. "
            "Only DMS default LOAD*.csv full-load files may ever be planned, loaded or "
            "reconciled." % (where, "; ".join(parts)))
    return True


def list_table_files(s3_client, dms_s3_path, allow_hex=True):
    """List the table folder's DMS full-load part-files as a NAME-SORTED list of (uri, size).

    Paginates list_objects_v2 fully (ContinuationToken, no MaxKeys assumption). Only keys
    DIRECTLY in the table's folder are considered (a key with any '/' after the prefix is a
    sub-folder object and is skipped). processed/ and failed/ are excluded by is_full_load_key.
    Zero-byte files are INCLUDED (they are files) but counted separately for the report.
    Returns (files_sorted_by_name, info) where info has counts and the zero-byte list.

    FAIL CLOSED on: duplicate basename across two (case-variant) folders is handled by the
    caller (resolve_table_folder picks exactly one folder); here we only see one folder's
    objects. Mixed-width names -> the caller asserts width via assert_uniform_width.
    """
    bucket, prefix = split_s3(dms_s3_path)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    files = []
    zero_byte = []
    cdc = []        # top-level CDC timestamp files (ignored; counted in the info line)
    other = []      # top-level non-LOAD, non-CDC junk (ignored; counted)
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kw)
        for o in resp.get("Contents", []) or []:
            key = o["Key"]
            size = int(o.get("Size", 0))
            if key.endswith("/"):
                continue  # directory placeholder
            rel = key[len(prefix):] if key.startswith(prefix) else key
            if "/" in rel:
                continue  # object in a SUB-folder (processed/, failed/, partition dir) - not direct
            uri = "s3://%s/%s" % (bucket, key)
            # AMENDMENT 6: classify every DIRECT child with the ONE shared classifier.
            cat = classify_basename(key, size, allow_hex=allow_hex)
            if cat == "allowed":
                files.append((uri, size))
            elif cat == "zero_byte":
                # The LOADER skips size<=0 keys, so a zero-byte LOAD file is NOT loaded. Exclude
                # it from the splittable set (keeps S_plan == S_loader, Amendment 2) but REPORT
                # it; it carries no rows so loading nothing for it loses no data.
                zero_byte.append(uri)
            elif cat == "cdc":
                cdc.append(uri)        # top-level CDC change file -> IGNORED, counted only
            else:
                other.append(uri)      # junk (_manifest.json, notes.csv, load_notes.csv, ...)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    files.sort(key=lambda t: str(t[0]))  # CANONICAL ordering: lexicographic by full URI
    info = {
        "num_files": len(files),
        "zero_byte_files": sorted(zero_byte),
        "zero_byte_count": len(zero_byte),
        "ignored_cdc": len(cdc),
        "ignored_cdc_files": sorted(basename(u) for u in cdc),
        "ignored_other": len(other),
        "ignored_other_files": sorted(basename(u) for u in other),
    }
    return files, info


def resolve_table_folder(s3_client, bucket, cdc_root, dms_schema, dms_table, allow_hex=False):
    """Find the ONE folder (case-insensitive on schema and table segments) that holds this
    table's LOAD files, exactly like the existing CDC/letter-case logic. Returns the resolved
    (schema, table, dms_s3_path). FAIL CLOSED if two case-variant folders BOTH hold LOAD files.

    cdc_root is the DMS endpoint BucketFolder ('.'/'' = flat root).
    """
    root = (cdc_root or "").strip("/. ")
    base_prefix = (root + "/") if root else ""

    def _subfolders(prefix):
        names, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
            if token:
                kw["ContinuationToken"] = token
            resp = s3_client.list_objects_v2(**kw)
            for cp in resp.get("CommonPrefixes", []) or []:
                names.append(cp["Prefix"][len(prefix):].rstrip("/"))
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return names

    # Resolve schema segment (exact, else unique case-insensitive match).
    schemas = _subfolders(base_prefix)
    schema = _pick_ci(schemas, dms_schema)
    if schema is None:
        # No folder at all -> empty at full load; return the nominal path (caller handles n=0).
        return {"schema": dms_schema, "table": dms_table,
                "dms_s3_path": "s3://%s/%s%s/%s/" % (bucket, base_prefix, dms_schema, dms_table),
                "found": False}
    tables = _subfolders("%s%s/" % (base_prefix, schema))
    # Find EVERY case-variant table folder that actually holds LOAD files -> fail if >1.
    matches = [t for t in tables if t.lower() == str(dms_table).lower()]
    holders = []
    for t in matches:
        path = "s3://%s/%s%s/%s/" % (bucket, base_prefix, schema, t)
        files, _ = list_table_files(s3_client, path, allow_hex=allow_hex)
        if files:
            holders.append((t, path, len(files)))
    if len(holders) > 1:
        raise MrlSplitError(
            "FAIL CLOSED: two case-variant folders both hold LOAD files for %s.%s: %s. "
            "Remove/rename one so exactly one folder owns the table's full-load files."
            % (dms_schema, dms_table,
               ", ".join("%s/%s/ (%d files)" % (schema, t, n) for t, _p, n in holders)))
    if holders:
        t, path, _n = holders[0]
        return {"schema": schema, "table": t, "dms_s3_path": path, "found": True}
    # schema folder exists but no table folder with LOAD files -> treat as empty at full load.
    exact_t = _pick_ci(tables, dms_table) or dms_table
    return {"schema": schema, "table": exact_t,
            "dms_s3_path": "s3://%s/%s%s/%s/" % (bucket, base_prefix, schema, exact_t),
            "found": False}


def _pick_ci(names, want):
    """Exact match, else the single case-insensitive match, else None (ambiguous -> None)."""
    if want in names:
        return want
    ci = [n for n in names if n.lower() == str(want).lower()]
    return ci[0] if len(ci) == 1 else None


def assert_unique_basenames(files):
    """FAIL CLOSED if the same basename appears more than once (e.g. two folders merged into one
    listing). Returns the unique count. `files` is [(uri,size)]."""
    seen = {}
    for uri, _s in files:
        b = basename(uri)
        seen.setdefault(b, []).append(uri)
    dups = {b: us for b, us in seen.items() if len(us) > 1}
    if dups:
        sample = "; ".join("%s -> %s" % (b, us) for b, us in list(dups.items())[:5])
        raise MrlSplitError(
            "FAIL CLOSED: %d basename(s) appear more than once in the table listing "
            "(the same file name in two places): %s" % (len(dups), sample))
    return len(seen)


def assert_uniform_width(files):
    """FAIL CLOSED if the LOAD names are not all the same body width. Fixed-width (DMS
    zero-pads) is what makes lexicographic order == numeric order; a mixed-width set could sort
    differently by name vs number, so refuse it (DESIGN.md D1). Empty input is OK (caller
    handles n=0)."""
    widths = set()
    bodies = []            # (basename, body) for the case check below
    for uri, _s in files:
        bn = basename(uri)
        w = _load_body_len(bn)
        if w is None:
            raise MrlSplitError(
                "FAIL CLOSED: %r is not a LOAD<body>.csv name after the listing filter "
                "(internal inconsistency)." % bn)
        widths.add(w)
        bodies.append((bn, _LOAD_BODY_RE.match(bn).group(1)))
    if len(widths) > 1:
        raise MrlSplitError(
            "FAIL CLOSED: the table's LOAD file names are NOT all the same width "
            "(widths seen: %s). Fixed-width names are required so that lexicographic order "
            "equals numeric order; a mixed-width set could split in a different order than the "
            "loader resolves. Investigate the DMS export." % sorted(widths))
    # R21-1: the allow-list is case-insensitive hex (^load[0-9a-f]+$ /i), so a table could carry
    # mixed-case hex bodies (e.g. LOAD0000000A.csv and load0000000a.csv). ASCII sorts uppercase
    # before lowercase, so a mixed-case set could sort by NAME differently than the loader
    # resolves it numerically (and two names that differ only by case are the SAME hex value ->
    # a duplicate). Require one consistent case for any hex letters; pure-decimal bodies are
    # unaffected (no letters). Fail closed naming the offending files.
    upper = [bn for bn, body in bodies if any(ch in "ABCDEF" for ch in body)]
    lower = [bn for bn, body in bodies if any(ch in "abcdef" for ch in body)]
    if upper and lower:
        offenders = sorted(set(upper) | set(lower))
        raise MrlSplitError(
            "FAIL CLOSED: the table mixes UPPER- and lower-case hex LOAD file names, which sort "
            "by name differently than they resolve numerically (and two names differing only by "
            "case are the same hex value - a duplicate). Normalize the DMS export to one case. "
            "Offending files: %s." % ", ".join(offenders[:20]))
    return True


def split_ranges(n, c):
    """Split n items into k = min(c, n) contiguous, inclusive, 0-based index ranges.

    Sizes: the first (n mod k) ranges get floor(n/k)+1 items, the rest floor(n/k). Returns a
    list of (start_index, end_index_inclusive). n=0 -> []. Pure; deterministic.

    Examples (asserted in tests): n=5,c=3 -> [(0,1),(2,3),(4,4)]; n=406,c=4 ->
    102,102,101,101.
    """
    n = int(n)
    c = int(c)
    if c < 1:
        raise MrlSplitError("max_concurrency must be an integer >= 1 (got %r)" % (c,))
    if n < 0:
        raise MrlSplitError("n must be >= 0 (got %r)" % (n,))
    if n == 0:
        return []
    k = min(c, n)
    base = n // k
    rem = n % k
    ranges = []
    start = 0
    for i in range(k):
        size = base + 1 if i < rem else base
        end = start + size - 1
        ranges.append((start, end))
        start = end + 1
    return ranges


def range_sizes(n, c):
    """Convenience: the list of range sizes for (n, c). E.g. range_sizes(5,3) == [2,2,1]."""
    return [e - s + 1 for s, e in split_ranges(n, c)]


def build_ranges(files_sorted, c):
    """Turn a NAME-SORTED [(uri,size)] list into concrete range descriptors by COUNT split.

    Returns a list of dicts: {index, first, last, first_uri, last_uri, files (uris), size,
    bytes}. `first`/`last` are basenames (what the loader resolver matches). Pure.
    """
    n = len(files_sorted)
    idx_ranges = split_ranges(n, c)
    out = []
    for i, (s, e) in enumerate(idx_ranges):
        chunk = files_sorted[s:e + 1]
        uris = [u for u, _sz in chunk]
        out.append({
            "index": i,
            "first": basename(uris[0]),
            "last": basename(uris[-1]),
            "first_uri": uris[0],
            "last_uri": uris[-1],
            "files": uris,
            "size": len(uris),
            "bytes": sum(int(sz) for _u, sz in chunk),
        })
    return out


def build_ranges_by_bytes(files_sorted, c):
    """Optional split_by=bytes path (DESIGN.md D7). Contiguous byte-balanced ranges, each a
    simple inclusive slice in NAME order, reusing the same balancing idea as
    merge_ranges.plan_ranges. Default/required path is build_ranges (by count); this is offered
    only because it is trivial. Pure."""
    files = list(files_sorted)
    m = len(files)
    if m == 0:
        return []
    n = max(1, min(int(c), m))
    total = sum(int(s) for _u, s in files) or 1
    share = total / n
    ranges = []
    cur = []
    cur_bytes = 0
    run_total = 0
    for idx, (uri, size) in enumerate(files):
        cur.append((uri, int(size)))
        cur_bytes += int(size)
        run_total += int(size)
        runs_closed = len(ranges)
        files_left_after = m - (idx + 1)
        runs_left_after_close = n - runs_closed - 1
        if (runs_closed < n - 1 and run_total >= share * (runs_closed + 1)
                and files_left_after >= runs_left_after_close and len(cur) >= 1
                and files_left_after >= 1):
            ranges.append(cur)
            cur, cur_bytes = [], 0
    if cur:
        ranges.append(cur)
    out = []
    for i, chunk in enumerate(ranges):
        uris = [u for u, _s in chunk]
        out.append({
            "index": i,
            "first": basename(uris[0]),
            "last": basename(uris[-1]),
            "first_uri": uris[0],
            "last_uri": uris[-1],
            "files": uris,
            "size": len(uris),
            "bytes": sum(int(s) for _u, s in chunk),
        })
    return out


def prove_ranges(ranges, files_sorted):
    """Triple proof (a) + (b) over the built ranges (part (c), resolver-consistency, is done in
    the plan Lambda / tests with the REAL loader resolver). FAIL CLOSED on any violation.

      (a) sizes differ by at most 1 and sum to n;
      (b) the union of the ranges' files == the sorted list exactly (no overlap, no gap, in
          order).
    """
    n = len(files_sorted)
    sorted_uris = [u for u, _s in files_sorted]
    if not ranges:
        if n == 0:
            return True
        raise MrlSplitError("FAIL CLOSED: no ranges built for %d files." % n)
    sizes = [r["size"] for r in ranges]
    if sum(sizes) != n:
        raise MrlSplitError("FAIL CLOSED: range sizes sum to %d, not %d." % (sum(sizes), n))
    if max(sizes) - min(sizes) > 1:
        raise MrlSplitError(
            "FAIL CLOSED: range sizes differ by more than 1: %s." % sizes)
    # (b) concatenation in range order must equal the sorted list exactly.
    flat = []
    for r in ranges:
        flat.extend(r["files"])
    if flat != sorted_uris:
        # Pinpoint the first divergence for a clear message (no raw data values — these are
        # file URIs, not row values, so they are safe to show).
        for i, (a, b) in enumerate(zip(flat, sorted_uris)):
            if a != b:
                raise MrlSplitError(
                    "FAIL CLOSED: range union diverges from the sorted listing at position %d "
                    "(%s vs %s)." % (i, basename(a), basename(b)))
        raise MrlSplitError(
            "FAIL CLOSED: range union has %d files but the listing has %d."
            % (len(flat), len(sorted_uris)))
    # disjoint check (redundant given the equality above, but explicit)
    if len(set(flat)) != len(flat):
        raise MrlSplitError("FAIL CLOSED: a file appears in more than one range.")
    return True


def listing_sha256(files_sorted):
    """Deterministic sha256 over the SORTED basenames (newline-joined). Used to detect listing
    drift between runs (persisted in plan.json). Pure; stable across platforms."""
    h = hashlib.sha256()
    for uri, _s in files_sorted:
        h.update(basename(uri).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()
