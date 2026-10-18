#!/usr/bin/env python3
"""mrl_cdc_handoff Lambda - R23-B1 (Amendment 8.4): write the reused CDC jobs' full-load gate doc
at EVERY prefix a CDC job will run with.

Background. When cdc=true, MRL hands the table over to the ORIGINAL route's CDC jobs. Each CDC job
gates on `load_full_load_status()` (REQUIRE_FULL_LOAD_DONE=True), which reads
`<CONFIG_PREFIX>_load_status.json` (+ `<CONFIG_PREFIX>_orchestrator/group-*/_load_status.json`)
UNDER THE --config_prefix it was started with. The MAIN CDC job runs with the task prefix
(config/_task/<suffix>/) - that doc is written by mrl_reconcile (round-1 A35). BUT the reused
`plan_split` forks any composite-PK table (ck-<slug>) or big table (>= file_fanout_threshold files,
default 8; >= 6M rows; >= 1 GB) (bg-<slug>) OUT of the main CDC job into its OWN CDC job, which runs
with --config_prefix = the FORK prefix:
  * ck fork: config/_task/<suffix>/_orchestrator/ck-<slug>/
  * bg fork: the big group prefix config/_task/<suffix>/_orchestrator/group-<k>/
MRL never wrote _load_status.json THERE, so a forked table's CDC job sees "full load not done" and
SKIPS the table on every poll cycle forever (silent CDC data loss); its G9 drift baseline
(_FULL_LOAD_ROWS) also stays empty. The brief's flagship 406-file table (>= 8 files) is a bg fork,
so this is the NORMAL MRL cdc=true case, not a corner case.

This Lambda runs AFTER EnsureForkJobs (the forks exist) and BEFORE ResumeDmsToCdc/StartCdcJob (CDC
starts). For every fork in plan_split's `forks[]`, it writes the SAME original-format doc
(`{"tables": {label: {"status":"done","rows":N}}}` - the exact shape glue_cdc_continuous's
`_read_load_status_doc` parses) at that fork's `config_prefix`, for the ONE table the fork owns
(fork["fork_table"]), using the reconciled row count. Idempotent (merge with any existing doc, keep
other tables). Writes NOTHING when cdc=false or when there are no forks. Never touches the task
prefix (mrl_reconcile owns that).

Event:
  { bucket, taskSuffix, cdc (bool), configPrefix (task prefix, s3 uri or key),
    reconcileKey (s3 uri of mrl/state/<suffix>/reconcile.json), planKey (s3 uri, fallback rows),
    forks: [ { fork_table, config_prefix, kind, fork_slug, rows }... ] }
Returns:
  { ok, cdc, written: [ {label, key, rows} ], forkCount }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


class MrlCdcHandoffError(Exception):
    pass


def _split_uri(uri):
    if str(uri).startswith("s3://"):
        b, k = str(uri)[5:].split("/", 1)
        return b, k
    raise MrlCdcHandoffError("expected an s3:// uri, got %r" % uri)


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


def _rows_by_label(s3, event):
    """Authoritative per-table row counts: the reconcile.json this run just wrote (clean tables
    only). Fall back to the plan's loaded_empty=0. Returns {label: rows}."""
    rows = {}
    rk = event.get("reconcileKey")
    if rk:
        rb, rkey = _split_uri(rk)
        rec = _get_json(s3, rb, rkey)
        if rec:
            for t in rec.get("tables") or []:
                if t.get("clean") or t.get("loaded_empty"):
                    rows[t["label"]] = int(t.get("total_rows", 0) or 0)
    # loaded-empty tables (rows 0) may also come straight from the plan.
    pk = event.get("planKey")
    if pk:
        pb, pkey = _split_uri(pk)
        plan = _get_json(s3, pb, pkey)
        for label, pt in ((plan or {}).get("tables") or {}).items():
            if label not in rows and pt.get("loaded_empty"):
                rows[label] = 0
    return rows


def _write_handoff_at(s3, cp_uri, label, n_rows):
    """Write/merge `{"tables": {label: {status:done, rows:N}}}` at <cp>/_load_status.json.
    Idempotent: keep any other tables already present; refresh this label's entry."""
    b, cp_key = _split_uri(cp_uri)
    key = cp_key.rstrip("/") + "/_load_status.json"
    existing = _get_json(s3, b, key)
    doc = {"tables": dict((existing or {}).get("tables") or {})}
    doc["tables"][label] = {"status": "done", "rows": int(n_rows),
                            "at": "mrl-cdc-fork-handoff"}
    doc["updated_at"] = "mrl-cdc-fork-handoff"
    _put_json(s3, b, key, doc)
    return "s3://%s/%s" % (b, key)


def handler(event, context):
    cdc_on = bool(event.get("cdc", False))
    forks = event.get("forks") or []
    # Defence in depth: this state only runs on the cdc=true path, but never write on cdc=false.
    if not cdc_on:
        print("(info) mrl_cdc_handoff: cdc=false -> nothing written.")
        return {"ok": True, "cdc": False, "written": [], "forkCount": 0}
    if not forks:
        print("(info) mrl_cdc_handoff: no forks -> main CDC job uses the task-prefix handoff; "
              "nothing to write here.")
        return {"ok": True, "cdc": True, "written": [], "forkCount": 0}

    s3 = boto3.client("s3", region_name=REGION)
    rows_by_label = _rows_by_label(s3, event)

    written = []
    missing_rows = []
    for fk in forks:
        label = fk.get("fork_table")
        cp = fk.get("config_prefix")
        if not label or not cp:
            raise MrlCdcHandoffError(
                "FAIL CLOSED: fork %r is missing fork_table/config_prefix; cannot write the CDC "
                "full-load handoff and the fork CDC job would skip the table forever." % fk)
        if label not in rows_by_label:
            # The table this fork owns was NOT reported clean/empty by reconcile. Writing a
            # "done" handoff for it would be a silent lie -> fail closed.
            missing_rows.append(label)
            continue
        uri = _write_handoff_at(s3, cp, label, rows_by_label[label])
        written.append({"label": label, "key": uri, "rows": rows_by_label[label],
                        "kind": fk.get("kind"), "fork_slug": fk.get("fork_slug")})

    if missing_rows:
        raise MrlCdcHandoffError(
            "FAIL CLOSED: %d forked table(s) have no clean reconcile row count, so their CDC "
            "full-load handoff cannot be written (the fork CDC job would skip them forever): %s. "
            "Reconcile must be clean for every forked table before CDC starts." %
            (len(missing_rows), ", ".join(sorted(missing_rows))))

    for w in written:
        print("(info) mrl_cdc_handoff: wrote CDC full-load handoff for %s (%s rows) at %s"
              % (w["label"], w["rows"], w["key"]))
    print("(info) mrl_cdc_handoff: %d fork handoff doc(s) written." % len(written))
    return {"ok": True, "cdc": True, "written": written, "forkCount": len(forks)}
