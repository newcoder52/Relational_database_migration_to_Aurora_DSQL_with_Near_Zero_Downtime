#!/usr/bin/env python3
"""mrl_status.py - READ-ONLY status for an MRL task (CloudShell-friendly, stdlib + boto3 only).

Prints, for one task suffix:
  * the current phase and timestamps (from mrl/state/<suffix>/state.json),
  * the plan summary (per-table file count, concurrency, range sizes) from plan.json,
  * per-range status (done / pending / failed) from each range's result + status docs,
  * the reconcile verdict and the count verdict, if present,
  * the last errors (failed ranges, count mismatches).

Writes NOTHING. Usage (single line):
  python3 mrl_status.py --bucket <bucket> --task-suffix <task-suffix> [--region <region>]
"""
import argparse
import json
import sys

try:
    import boto3
except Exception:
    boto3 = None


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    except Exception:
        return None


def _list(s3, bucket, prefix):
    out, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        out += [o["Key"] for o in resp.get("Contents", []) or []]
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return out


def main():
    ap = argparse.ArgumentParser(description="read-only MRL task status")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--task-suffix", required=True)
    ap.add_argument("--region", default=None)
    args = ap.parse_args()
    if boto3 is None:
        sys.exit("boto3 is required (CloudShell has it preinstalled).")
    s3 = boto3.client("s3", region_name=args.region) if args.region else boto3.client("s3")
    b = args.bucket
    base = "mrl/state/%s/" % args.task_suffix

    print("=" * 70)
    print("MRL STATUS  task-suffix=%s  bucket=%s" % (args.task_suffix, b))
    print("=" * 70)

    state = _get_json(s3, b, base + "state.json")
    if state:
        print("phase: %s   updatedAt: %s   execution: %s"
              % (state.get("phase"), state.get("updatedAt"), state.get("executionName")))
        for ph in (state.get("phases") or [])[-8:]:
            print("    - %s @ %s" % (ph.get("phase"), ph.get("at")))
    else:
        print("no state.json yet (task not started, or a different suffix).")

    plan = _get_json(s3, b, base + "plan.json")
    if plan:
        print("\nplan: planSha=%s  split_by=%s  maxTotalConcurrentRuns=%s"
              % (plan.get("planSha", "")[:12], plan.get("split_by"),
                 plan.get("maxTotalConcurrentRuns")))
        for label, pt in sorted((plan.get("tables") or {}).items()):
            if pt.get("loaded_empty"):
                print("  %-30s loaded-empty (0 files, FullLoadRows=0)" % label)
                continue
            sizes = [r["size"] for r in pt.get("ranges", [])]
            print("  %-30s %d files, concurrency %s -> %s"
                  % (label, pt.get("num_files"), pt.get("concurrency"),
                     ",".join(str(x) for x in sizes)))
            # per-range status from result docs
            cp = pt.get("config_prefix", "")
            if cp.startswith("s3://"):
                cb, cp_key = cp[5:].split("/", 1)
                safe = label.replace("/", "_")
                for rk in sorted(_list(s3, cb, "%s_range_result/%s.range-" % (cp_key, safe))):
                    rdoc = _get_json(s3, cb, rk) or {}
                    print("      range %s..%s  done=%s  failed=%s  rows=%s"
                          % (rdoc.get("first"), rdoc.get("last"),
                             rdoc.get("files_done"), rdoc.get("failed"),
                             rdoc.get("rows_written_by_this_range")))
    else:
        print("\nno plan.json yet (planning not reached).")

    rec = _get_json(s3, b, base + "reconcile.json")
    if rec:
        print("\nreconcile: allClean=%s  merged=%d"
              % (rec.get("allClean"), len(rec.get("mergedWritten") or [])))
        for t in rec.get("tables", []):
            if not t.get("clean") and not t.get("loaded_empty"):
                print("  NOT CLEAN %s: missing=%s duplicates=%s failed=%s extra=%s"
                      % (t["label"], t.get("missing"), t.get("duplicates"),
                         t.get("failed_ranges"), t.get("extra")))

    cnt = _get_json(s3, b, base + "count_result.json")
    if cnt:
        print("\ncount verdict: %s" % cnt.get("verdict"))
        for t in cnt.get("tables", []):
            print("  %-30s %-12s DSQL=%s vs expected=%s"
                  % (t["table"], t["status"], t.get("target"), t.get("expected")))
    print("=" * 70)


if __name__ == "__main__":
    main()
