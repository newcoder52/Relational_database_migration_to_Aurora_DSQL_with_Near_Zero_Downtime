#!/usr/bin/env python3
"""merge_ranges.py - reconcile the manual file-range load runs for ONE table and (only with
--write) assemble the NORMAL file-status doc so the normal pipeline continues.

Runs in CloudShell. Stdlib + boto3 only. No Spark, no DSQL.

MODES
  (default, dry-run) reconcile:
      read every per-range result doc (_range_result/<table>.range-*.json) and every
      range-private status doc (_file_status/<table>.range-*.json) for the table, list the
      whole table's LOAD*.csv files, and check that EVERY file is covered EXACTLY ONCE as
      'done' with a row count. Prints per-range and total rows and lists any
      missing / duplicate / failed / overlapping / not-done files. Writes nothing.
  --write:
      only if reconciliation is clean, back up any existing normal doc, then write the
      merged doc at the NORMAL path (_file_status/<table>.json) in the EXACT format
      load_file_status expects ({"files": {uri: {"state":"done","rows":N}, ...}}), so a
      following normal load run prints 'all N file(s) already done ... nothing to load'.
  --plan N:
      read-only. Print suggested inclusive start/end file ranges for N runs balanced by
      bytes from the real S3 listing, ready to copy into start-job-run.

USAGE (single-line; see README for full copy/paste):
  python3 merge_ranges.py --bucket BKT --config-prefix PFX --table SCHEMA.TABLE --dms-prefix S3URI
  python3 merge_ranges.py ... --plan 4
  python3 merge_ranges.py ... --write

ARGS
  --bucket          S3 bucket that holds the config prefix + DMS export (same account/region).
  --config-prefix   the group's config prefix WITHOUT bucket, e.g.
                    <schema>/config/_orchestrator/group-1/  (must end with /). This is the
                    --config_prefix the load runs used, minus the s3://bucket/ part.
  --table           SCHEMA.TABLE, e.g. <schema>.<table> (the DSQL label).
  --dms-prefix      full s3:// URI of the table's DMS folder (same path the load used for
                    this table), e.g. s3://BKT/<schema>/<table> . Used to list the
                    whole table's LOAD*.csv files for the coverage check and for --plan.
  --write           write the merged normal doc (default is dry-run).
  --plan N          print N byte-balanced ranges and exit (read-only).
  --region          AWS region (optional; else boto3 default).
"""
import argparse
import json
import re
import sys

try:
    import boto3
except Exception:
    boto3 = None


def _safe(s):
    return str(s).replace("/", "_")


def _basename(uri):
    return str(uri).rsplit("/", 1)[-1]


def _file_number(name):
    m = re.search(r"LOAD0*([0-9]+)\.csv$", _basename(name), re.IGNORECASE)
    return int(m.group(1)) if m else None


def _split_s3(path):
    assert path.startswith("s3://"), "expected s3:// uri, got %r" % path
    rest = path[5:]
    b, _, k = rest.partition("/")
    return b, k


def _is_full_load_key(key):
    """Mirror of the loader's job2_load._is_full_load_key so the merge tool's whole-table
    file set is EXACTLY the set the loader ranges over. Two independent exclusions:
      1. PATH: reject any key with a 'processed/' or 'failed/' path segment (CDC lifecycle
         folders; full-load files never live there).
      2. NAME: require the basename to match DMS full-load LOAD*.csv (case-insensitive).
    Without this, 'processed/LOAD*.csv' CDC copies would be miscounted as table files and
    the coverage check would report false missing/duplicate files."""
    kl = str(key).lower()
    segments = kl.split("/")
    if "processed" in segments or "failed" in segments:
        return False
    base = segments[-1]
    # decimal DMS full-load name: load0*<digits>.csv (NOT hex) - stricter than the base's
    # startswith/endswith but safe (DMS zero-pads decimal counters), per vet F1.
    return re.match(r"^load0*[0-9]+\.csv$", base) is not None


def list_table_files(s3, dms_prefix):
    """List LOAD*.csv under the table folder -> sorted [(uri,size)] by name. Mirrors the
    loader's s3_list_table_files guards EXACTLY (skip dirs/empty; exclude processed/ & failed/;
    LOAD*.csv basename only), so the whole-table set matches what the loader actually loads."""
    bucket, prefix = _split_s3(dms_prefix)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    files = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            key = o["Key"]
            size = int(o.get("Size", 0))
            if key.endswith("/") or size <= 0:
                continue
            if not _is_full_load_key(key):
                continue
            files.append(("s3://%s/%s" % (bucket, key), size))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    files.sort(key=lambda t: str(t[0]))
    return files


def _get_json(s3, bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def _list_keys(s3, bucket, prefix):
    out = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            out.append(o["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return out


def plan_ranges(files, n):
    """Byte-balanced INCLUSIVE ranges for n runs, each CONTIGUOUS in name order (so start/end
    map to a simple inclusive slice). Deterministic. Returns [(first,last,files,bytes)].

    Walk files in NAME order accumulating bytes; close the current run when its running total
    first reaches share*k (k = runs closed so far + 1), while always leaving enough files for
    the remaining runs. Produces exactly min(n, len(files)) ranges; empty input -> []."""
    files = sorted(files, key=lambda t: str(t[0]))
    m = len(files)
    if m == 0:
        return []
    n = max(1, min(int(n), m))
    total = sum(s for _u, s in files) or 1
    share = total / n
    ranges = []
    cur = []
    cur_bytes = 0
    run_total = 0
    for idx, (uri, size) in enumerate(files):
        cur.append((uri, size))
        cur_bytes += size
        run_total += size
        runs_closed = len(ranges)
        files_left_after = m - (idx + 1)
        runs_left_after_close = n - runs_closed - 1
        # close here if (a) more runs still needed, (b) we have crossed this run's cumulative
        # byte share, and (c) enough files remain to give each remaining run at least one.
        if (runs_closed < n - 1 and run_total >= share * (runs_closed + 1)
                and files_left_after >= runs_left_after_close and len(cur) >= 1
                and files_left_after >= 1):
            ranges.append((_basename(cur[0][0]), _basename(cur[-1][0]),
                           [u for u, _s in cur], cur_bytes))
            cur, cur_bytes = [], 0
    if cur:
        ranges.append((_basename(cur[0][0]), _basename(cur[-1][0]),
                       [u for u, _s in cur], cur_bytes))
    # if we somehow closed fewer than n (all bytes in early files), that is fine: fewer,
    # still disjoint+complete. Guarantee completeness by construction (every file appended once).
    return ranges


def reconcile(s3, bucket, config_prefix, label, dms_prefix):
    """Returns (ok, report_dict). Pure checks; writes nothing."""
    files = list_table_files(s3, dms_prefix)
    all_uris = [u for u, _s in files]
    size_of = {u: s for u, s in files}
    rr_prefix = "%s_range_result/%s.range-" % (config_prefix, _safe(label))
    fs_prefix = "%s_file_status/%s.range-" % (config_prefix, _safe(label))

    result_keys = [k for k in _list_keys(s3, bucket, rr_prefix) if k.endswith(".json")]
    status_keys = [k for k in _list_keys(s3, bucket, fs_prefix) if k.endswith(".json")]

    # Build the merged file->rows map from the authoritative range-private STATUS docs
    # (result docs are a convenience summary; the status doc is what the loader actually
    # wrote per committed file). We key coverage by file uri.
    done_rows = {}          # uri -> rows (first writer wins; conflicts are flagged)
    done_by_range = {}      # range_key -> {uri: rows}
    duplicates = []         # (uri, [range_keys])
    failed_ranges = []      # range_key (any file failed)
    per_range_rows = {}

    seen_file_owner = {}    # uri -> range_key

    for sk in sorted(status_keys):
        rk = sk[len(fs_prefix):][:-len(".json")]   # e.g. LOAD..-LOAD..
        doc = _get_json(s3, bucket, sk) or {}
        rng_done = {}
        for uri, rec in (doc.get("files") or {}).items():
            if isinstance(rec, dict) and rec.get("state") == "done":
                rng_done[uri] = int(rec.get("rows", 0))
        done_by_range[rk] = rng_done
        per_range_rows[rk] = sum(rng_done.values())
        for uri, rows in rng_done.items():
            if uri in seen_file_owner and seen_file_owner[uri] != rk:
                duplicates.append((uri, [seen_file_owner[uri], rk]))
            else:
                seen_file_owner[uri] = rk
            if uri not in done_rows:
                done_rows[uri] = rows

    # failed ranges from result docs
    for rk_key in sorted(result_keys):
        rdoc = _get_json(s3, bucket, rk_key) or {}
        if rdoc.get("failed"):
            failed_ranges.append(rdoc.get("first", "?") + ".." + rdoc.get("last", "?"))

    covered = set(done_rows.keys())
    all_set = set(all_uris)
    missing = sorted(all_set - covered)
    extra = sorted(covered - all_set)   # files loaded that are not in the current listing
    total_rows = sum(done_rows.values())

    ok = (not missing and not duplicates and not failed_ranges and not extra
          and len(covered) == len(all_uris) and len(all_uris) > 0)

    report = {
        "label": label, "total_files": len(all_uris), "covered_files": len(covered),
        "total_rows": total_rows, "ranges": sorted(done_by_range.keys()),
        "per_range_rows": per_range_rows, "missing": missing, "duplicates": duplicates,
        "failed_ranges": failed_ranges, "extra_files": extra, "ok": ok,
        "done_rows": done_rows, "size_of": size_of,
    }
    return ok, report


def print_report(report):
    print("=" * 70)
    print("MERGE RECONCILE %s" % report["label"])
    print("  whole-table files: %d   covered-as-done: %d   total rows: %s"
          % (report["total_files"], report["covered_files"], format(report["total_rows"], ",")))
    print("  ranges found: %d" % len(report["ranges"]))
    for rk in report["ranges"]:
        print("    range %-40s rows %s" % (rk, format(report["per_range_rows"].get(rk, 0), ",")))
    if report["missing"]:
        print("  MISSING (not loaded by any range): %d file(s)" % len(report["missing"]))
        for u in report["missing"][:20]:
            print("    - " + _basename(u))
    if report["duplicates"]:
        print("  DUPLICATE (loaded by >1 range): %d file(s)" % len(report["duplicates"]))
        for u, rks in report["duplicates"][:20]:
            print("    - %s  in %s" % (_basename(u), rks))
    if report["failed_ranges"]:
        print("  FAILED ranges: %s" % ", ".join(report["failed_ranges"]))
    if report["extra_files"]:
        print("  EXTRA (loaded but not in current listing): %d" % len(report["extra_files"]))
    print("  RESULT: %s" % ("CLEAN - safe to --write" if report["ok"]
                            else "NOT CLEAN - refusing to write (fix the above first)"))
    print("=" * 70)


def do_write(s3, bucket, config_prefix, label, report):
    """Write the merged normal doc at _file_status/<table>.json after backing up any
    existing one. Exact load_file_status format."""
    normal_key = "%s_file_status/%s.json" % (config_prefix, _safe(label))
    # back up existing
    existing = _get_json(s3, bucket, normal_key)
    if existing is not None:
        from datetime import datetime, timezone
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bkey = "%s_file_status/%s.json.bak-%s" % (config_prefix, _safe(label), stamp)
        s3.put_object(Bucket=bucket, Key=bkey,
                      Body=json.dumps(existing).encode("utf-8"),
                      ContentType="application/json")
        print("  backed up existing normal doc -> s3://%s/%s" % (bucket, bkey))
    files_doc = {}
    for uri, rows in sorted(report["done_rows"].items()):
        files_doc[uri] = {"state": "done", "rows": int(rows),
                          "at": "merged-by-merge_ranges"}
    merged = {"files": files_doc, "updated_at": "merged-by-merge_ranges",
              "merged_from_ranges": report["ranges"]}
    s3.put_object(Bucket=bucket, Key=normal_key,
                  Body=json.dumps(merged).encode("utf-8"),
                  ContentType="application/json")
    print("  WROTE merged normal doc -> s3://%s/%s (%d files, %s rows)"
          % (bucket, normal_key, len(files_doc), format(report["total_rows"], ",")))
    return normal_key, merged


def main():
    ap = argparse.ArgumentParser(description="reconcile/merge manual file-range load runs")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--config-prefix", required=True,
                    help="group config prefix WITHOUT bucket; must end with /")
    ap.add_argument("--table", required=True, help="SCHEMA.TABLE")
    ap.add_argument("--dms-prefix", required=True, help="s3:// uri of the table's DMS folder")
    ap.add_argument("--write", action="store_true", help="write the merged normal doc")
    ap.add_argument("--plan", type=int, default=0, help="print N byte-balanced ranges and exit")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    if boto3 is None:
        sys.exit("boto3 is required (CloudShell has it preinstalled).")
    cp = args.config_prefix
    if not cp.endswith("/"):
        cp += "/"
    s3 = boto3.client("s3", region_name=args.region) if args.region else boto3.client("s3")

    if args.plan and args.plan > 0:
        files = list_table_files(s3, args.dms_prefix)
        if not files:
            sys.exit("no LOAD*.csv files found under %s" % args.dms_prefix)
        ranges = plan_ranges(files, args.plan)
        print("PLAN %d byte-balanced range(s) for %s (%d files, %s bytes):"
              % (args.plan, args.table, len(files),
                 format(sum(s for _u, s in files), ",")))
        for i, (first, last, fs, by) in enumerate(ranges):
            print("  run %d: --load_file_start %s --load_file_end %s   (%d files, %s bytes)"
                  % (i, first, last, len(fs), format(by, ",")))
        print("Numbers also work, e.g. --load_file_start %s --load_file_end %s"
              % (_file_number(ranges[0][0]), _file_number(ranges[0][1])))
        return

    ok, report = reconcile(s3, args.bucket, cp, args.table, args.dms_prefix)
    print_report(report)
    if args.write:
        if not ok:
            sys.exit("refusing to --write: reconciliation is NOT clean (see above)")
        do_write(s3, args.bucket, cp, args.table, report)
        print("  Next: start a NORMAL load run for this table/group (no range args). It will "
              "see every file already done and print 'nothing to load', then the pipeline "
              "continues (validation, DMS resume, CDC).")
    else:
        print("  (dry-run; re-run with --write to assemble the normal doc once CLEAN)")


if __name__ == "__main__":
    main()
