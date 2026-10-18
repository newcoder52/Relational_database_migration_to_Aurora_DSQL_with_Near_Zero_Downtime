#!/usr/bin/env python3
"""mrl_state Lambda - small helper for the MRL child: observability state.json + load-result
collection.

modes:
  * "write_state": write/merge s3://<bucket>/mrl/state/<suffix>/state.json with {phase,
    updatedAt, executionName, and any extra fields passed in `fields`}. Idempotent, best-effort
    merge (reads the existing doc, updates, writes). Returns {stateKey}.
  * "collect_load": given the LoadRangeMap results (a list of {label, range_index, first, last,
    status:"ok"|"failed", error?}), aggregate and decide. Returns {ok, failed:[...], okCount,
    failedCount}. The state machine routes to MrlLoadFailed when ok=false; the failed list names
    each (table, range) so a rerun of the SAME execution input resumes only those ranges
    (per-range progress docs). NEVER includes a raw row value.
"""

import json
import os
from datetime import datetime, timezone

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_json(s3, bucket, key):
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return None
        raise
    return json.loads(body.decode("utf-8"))


def _write_state(event):
    bucket = event["bucket"]
    suffix = event["taskSuffix"]
    phase = event.get("phase")
    fields = event.get("fields") or {}
    s3 = boto3.client("s3", region_name=REGION)
    key = "mrl/state/%s/state.json" % suffix
    doc = _get_json(s3, bucket, key) or {"taskSuffix": suffix, "phases": []}
    if phase:
        doc["phase"] = phase
        hist = doc.setdefault("phases", [])
        hist.append({"phase": phase, "at": _now()})
        doc["phases"] = hist[-100:]   # bound the history
    if event.get("executionName"):
        doc["executionName"] = event["executionName"]
    for k, v in fields.items():
        doc[k] = v
    doc["updatedAt"] = _now()
    s3.put_object(Bucket=bucket, Key=key,
                  Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                  ContentType="application/json")
    return {"stateKey": "s3://%s/%s" % (bucket, key), "phase": phase}


def _collect_load(event):
    results = event.get("results") or []
    failed = [r for r in results if r.get("status") == "failed"]
    ok = [r for r in results if r.get("status") == "ok"]
    failed_desc = [{"label": r.get("label"), "range_index": r.get("range_index"),
                    "first": r.get("first"), "last": r.get("last"),
                    "error": str(r.get("error") or "")[:500]} for r in failed]
    out = {"ok": len(failed) == 0, "okCount": len(ok), "failedCount": len(failed),
           "failed": failed_desc, "total": len(results)}
    if failed:
        print("(info) mrl_state collect_load: %d of %d range run(s) FAILED: %s"
              % (len(failed), len(results),
                 ", ".join("%s[%s] %s..%s" % (f["label"], f["range_index"], f["first"], f["last"])
                           for f in failed_desc)))
    else:
        print("(info) mrl_state collect_load: all %d range run(s) OK" % len(results))
    return out


def handler(event, context):
    mode = event.get("mode", "write_state")
    if mode == "write_state":
        return _write_state(event)
    if mode == "collect_load":
        return _collect_load(event)
    raise Exception("mrl_state: unknown mode %r (want write_state|collect_load)." % mode)
