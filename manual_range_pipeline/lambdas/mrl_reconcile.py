#!/usr/bin/env python3
"""mrl_reconcile Lambda - RECONCILE phase (copied from merge_ranges.py logic, adapted to the
MRL per-table config prefixes and driven by plan.json).

For every non-empty table in the plan:
  * list the whole table's LOAD files (mrl_split.list_table_files, SAME filter as the loader),
  * read every per-range status doc (_file_status/<label>.range-*.json) and per-range result
    doc (_range_result/<label>.range-*.json) under that table's config prefix,
  * prove EVERY file is covered EXACTLY ONCE as 'done' with a row count: no missing / duplicate
    / failed / extra file (this is the authoritative overlap + completeness backstop),
  * total per-range and per-table rows,
  * optionally (writeMergedStatus) write the merged NORMAL _file_status/<label>.json doc in the
    EXACT format the loader's load_file_status expects, so a later normal load sees the table as
    fully loaded.

Writes s3://<bucket>/mrl/state/<suffix>/reconcile.json with the verdict. FAIL CLOSED (ok=false,
and the SM routes to MrlReconcileFailed) if any table is not clean.

Event: { bucket, taskSuffix, planKey (s3 uri), writeMergedStatus (bool, default by mode) }
Returns: { ok, tables:[{label, total_files, covered, total_rows, missing, duplicates,
  failed_ranges, extra, clean}], allClean, mergedWritten:[labels], reconcileKey, warnings }
"""

import json
import os
from datetime import datetime, timezone

import boto3

import mrl_split as ms

REGION = os.environ.get("AWS_REGION", "us-east-1")


class MrlReconcileError(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_uri(uri):
    assert str(uri).startswith("s3://")
    b, k = str(uri)[5:].split("/", 1)
    return b, k


def _get_json(s3, bucket, key):
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound") or type(e).__name__ == "NoSuchKey":
            return None
        raise
    return json.loads(body.decode("utf-8"))


def _put_json(s3, bucket, key, doc):
    s3.put_object(Bucket=bucket, Key=key,
                  Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                  ContentType="application/json")


def _list_keys(s3, bucket, prefix):
    out, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []) or []:
            out.append(o["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return out


def _safe(label):
    return str(label).replace("/", "_")


def reconcile_table(s3, bucket, cp_key, label, dms_s3_path, allow_hex):
    """Pure-ish reconcile for ONE table. Returns a report dict. cp_key is the table's config
    prefix KEY (no bucket, trailing slash). Mirrors merge_ranges.reconcile."""
    files, _info = ms.list_table_files(s3, dms_s3_path, allow_hex=allow_hex)
    all_uris = [u for u, _s in files]
    all_set = set(all_uris)

    # AMENDMENT 2 (also enforced at plan time): the reconcile's whole-table set MUST EQUAL the
    # set the loader would load. If a stray/mixed-width file appeared after the plan, fail closed
    # here too rather than silently reporting a false missing/extra. This runs BEFORE the
    # coverage check so the clearer Amendment-2 message wins.
    ms.assert_plan_equals_loader_set(s3, dms_s3_path, files, allow_hex=allow_hex)

    safe = _safe(label)
    fs_prefix = "%s_file_status/%s.range-" % (cp_key, safe)
    rr_prefix = "%s_range_result/%s.range-" % (cp_key, safe)
    status_keys = [k for k in _list_keys(s3, bucket, fs_prefix) if k.endswith(".json")]
    result_keys = [k for k in _list_keys(s3, bucket, rr_prefix) if k.endswith(".json")]

    done_rows = {}
    per_range_rows = {}
    seen_owner = {}
    duplicates = []
    failed_ranges = []

    for sk in sorted(status_keys):
        rk = sk[len(fs_prefix):][:-len(".json")]
        doc = _get_json(s3, bucket, sk) or {}
        rng_done = {}
        for uri, rec in (doc.get("files") or {}).items():
            if isinstance(rec, dict) and rec.get("state") == "done":
                rng_done[uri] = int(rec.get("rows", 0))
        per_range_rows[rk] = sum(rng_done.values())
        for uri, rows in rng_done.items():
            if uri in seen_owner and seen_owner[uri] != rk:
                duplicates.append((ms.basename(uri), [seen_owner[uri], rk]))
            else:
                seen_owner[uri] = rk
            if uri not in done_rows:
                done_rows[uri] = rows

    for rkk in sorted(result_keys):
        rdoc = _get_json(s3, bucket, rkk) or {}
        if rdoc.get("failed"):
            failed_ranges.append("%s..%s" % (rdoc.get("first", "?"), rdoc.get("last", "?")))

    # AMENDMENT 6.5: every done/progress entry MUST name an allow-listed full-load file (never a
    # CDC file). A CDC name anywhere in a range progress doc means a bug -> fail closed with the
    # name. Same shared rule as the plan (single source of truth).
    ms.assert_names_allow_listed([ms.basename(u) for u in done_rows],
                                 "the reconcile progress for %s" % label, allow_hex=allow_hex)

    covered = set(done_rows)
    missing = sorted(ms.basename(u) for u in (all_set - covered))
    extra = sorted(ms.basename(u) for u in (covered - all_set))
    total_rows = sum(done_rows.values())
    clean = (not missing and not duplicates and not failed_ranges and not extra
             and len(covered) == len(all_uris) and len(all_uris) > 0)
    return {
        "label": label, "total_files": len(all_uris), "covered": len(covered),
        "total_rows": total_rows, "missing": missing, "duplicates": duplicates,
        "failed_ranges": failed_ranges, "extra": extra, "clean": clean,
        "per_range_rows": per_range_rows, "done_rows": done_rows,
    }


def write_merged_status(s3, bucket, cp_key, label, report):
    """Write the merged NORMAL _file_status/<label>.json (exact load_file_status format)."""
    safe = _safe(label)
    key = "%s_file_status/%s.json" % (cp_key, safe)
    existing = _get_json(s3, bucket, key)
    if existing is not None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        _put_json(s3, bucket, "%s_file_status/%s.json.bak-%s" % (cp_key, safe, stamp), existing)
    files_doc = {u: {"state": "done", "rows": int(r), "at": "merged-by-mrl_reconcile"}
                 for u, r in sorted(report["done_rows"].items())}
    _put_json(s3, bucket, key, {"files": files_doc, "updated_at": "merged-by-mrl_reconcile",
                                "merged_from_ranges": sorted(report["per_range_rows"].keys())})
    return "s3://%s/%s" % (bucket, key)


def handler(event, context):
    bucket = event["bucket"]
    suffix = event["taskSuffix"]
    plan_uri = event["planKey"]
    write_merged = bool(event.get("writeMergedStatus", False))
    cdc_on = bool(event.get("cdc", False))
    # The task config prefix the reused CDC jobs run with (--config_prefix = $.resolved.configPrefix).
    task_config_prefix = event.get("configPrefix") or ("config/_task/%s/" % suffix)
    warnings = []

    pb, pk = _split_uri(plan_uri)
    s3 = boto3.client("s3", region_name=REGION)
    plan = _get_json(s3, pb, pk)
    if not plan:
        raise MrlReconcileError("FAIL CLOSED: plan %s not found; cannot reconcile." % plan_uri)
    allow_hex = bool(plan.get("allow_hex"))

    tables_out = []
    merged_written = []
    task_load_status = {}   # {label: {"status":"done","rows":N}} for the task-level doc (cdc)
    all_clean = True
    for label, pt in sorted((plan.get("tables") or {}).items()):
        if pt.get("loaded_empty"):
            tables_out.append({"label": label, "total_files": 0, "covered": 0, "total_rows": 0,
                               "missing": [], "duplicates": [], "failed_ranges": [], "extra": [],
                               "clean": True, "loaded_empty": True})
            task_load_status[label] = {"status": "done", "rows": 0, "at": "mrl-loaded-empty"}
            continue
        cp_uri = pt.get("config_prefix")
        cb, cp_key = _split_uri(cp_uri)
        dms_s3_path = pt.get("dms_s3_path")
        rep = reconcile_table(s3, cb, cp_key, label, dms_s3_path, allow_hex)
        if not rep["clean"]:
            all_clean = False
        if rep["clean"] and write_merged:
            merged_written.append(write_merged_status(s3, cb, cp_key, label, rep))
        if rep["clean"]:
            task_load_status[label] = {"status": "done", "rows": int(rep["total_rows"]),
                                       "at": "merged-by-mrl_reconcile"}
        # Drop the big done_rows map from the returned/persisted summary (payload size).
        rep_summary = {k: v for k, v in rep.items() if k != "done_rows"}
        tables_out.append(rep_summary)

    # A35 (Amendment 8.4): when cdc=true, the reused CDC jobs run with --config_prefix =
    # config/_task/<suffix>/ and gate on <CONFIG_PREFIX>_load_status.json (REQUIRE_FULL_LOAD_DONE)
    # + harvest each table's "rows" for the G9 drift baseline. Write that doc in the EXACT
    # original format ({"tables": {label:{status:done, rows:N}}}) BEFORE CDC starts, ONLY when
    # reconcile is clean (we never mark a table done past an unclean reconcile). cdc=false writes
    # nothing here unless write_merged_status already wrote the merged per-table doc.
    task_status_written = None
    if cdc_on and all_clean and task_load_status:
        tb, tk = _split_uri(task_config_prefix) if task_config_prefix.startswith("s3://") \
            else (bucket, task_config_prefix)
        tk = tk.rstrip("/") + "/_load_status.json"
        existing = _get_json(s3, tb, tk)
        doc = {"tables": dict((existing or {}).get("tables") or {})}
        doc["tables"].update(task_load_status)
        doc["updated_at"] = "merged-by-mrl_reconcile"
        _put_json(s3, tb, tk, doc)
        task_status_written = "s3://%s/%s" % (tb, tk)

    reconcile_doc = {
        "version": 1, "generated_at": _now(), "taskSuffix": suffix, "planKey": plan_uri,
        "allClean": all_clean, "writeMergedStatus": write_merged, "cdc": cdc_on,
        "taskLoadStatusKey": task_status_written,
        "mergedWritten": merged_written, "tables": tables_out,
    }
    rk = "mrl/state/%s/reconcile.json" % suffix
    _put_json(s3, bucket, rk, reconcile_doc)

    print("(info) mrl_reconcile: %d table(s), allClean=%s, merged=%d -> s3://%s/%s"
          % (len(tables_out), all_clean, len(merged_written), bucket, rk))
    if not all_clean:
        bad = [t["label"] for t in tables_out if not t.get("clean")]
        # Fail closed so the SM routes to MrlReconcileFailed. The reconcile.json holds the
        # per-table missing/duplicate/failed/extra lists (file names only - never row values).
        raise MrlReconcileError(
            "FAIL CLOSED: reconcile is NOT clean for %d table(s): %s. See s3://%s/%s for the "
            "per-table missing / duplicate / failed-range / extra lists. Fix the failed ranges "
            "and rerun the same execution input (finished files are skipped)." %
            (len(bad), ", ".join(bad), bucket, rk))

    return {"ok": True, "allClean": True, "reconcileKey": "s3://%s/%s" % (bucket, rk),
            "mergedWritten": merged_written, "tables": tables_out, "warnings": warnings}
