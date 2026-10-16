#!/usr/bin/env python3
"""mrl_gate_eval Lambda - SAFETY GATE decision (after the count/probe Glue job wrote
probe_result.json).

The gate passes a table when EITHER:
  * the target table is EMPTY (probe said empty), OR
  * the plan's ranges are already recorded as a RESUME of THIS plan: the table's merged normal
    _file_status/<label>.json (or any range status docs) exist AND the persisted plan's sha for
    the table is unchanged (checked in mrl_plan on the re-run). Here we accept non-empty when
    this table already has recorded per-range progress for the SAME plan.

A non-empty table with NO recorded matching plan => FAIL CLOSED with the safe fix (drop and
recreate), overridable only by run input override=true (then a loud WARNING, gate passes).
A missing table (probe exists=false) => FAIL CLOSED (the load would create nothing).

Event: { bucket, taskSuffix, planKey (s3 uri), probeResultKey (s3 uri), override (bool) }
Returns: { ok, gate:"pass"|"fail", perTable:[{label, empty, hasProgress, decision, reason}],
           override, warnings }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


class MrlGateError(Exception):
    pass


def _split(uri):
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


def _has_recorded_progress(s3, cp_uri, label):
    """True if this table's config prefix already holds range status docs or a merged normal
    doc (a prior attempt of THIS plan)."""
    b, cp_key = _split(cp_uri)
    safe = str(label).replace("/", "_")
    for sub in ("_file_status/%s." % safe, "_range_result/%s." % safe):
        resp = s3.list_objects_v2(Bucket=b, Prefix=cp_key + sub, MaxKeys=1)
        if resp.get("Contents"):
            return True
    return False


def handler(event, context):
    bucket = event["bucket"]
    suffix = event["taskSuffix"]
    override = bool(event.get("override", False))
    warnings = []
    s3 = boto3.client("s3", region_name=REGION)

    pb, pk = _split(event["planKey"])
    plan = _get_json(s3, pb, pk)
    if not plan:
        raise MrlGateError("FAIL CLOSED: plan %s not found." % event["planKey"])
    prb, prk = _split(event["probeResultKey"])
    probe = _get_json(s3, prb, prk)
    if not probe:
        raise MrlGateError("FAIL CLOSED: probe result %s not found." % event["probeResultKey"])

    probe_by_label = {r["table"]: r for r in probe.get("tables", [])}
    per_table = []
    gate = "pass"
    for label, pt in sorted((plan.get("tables") or {}).items()):
        if pt.get("loaded_empty"):
            per_table.append({"label": label, "empty": None, "hasProgress": False,
                              "decision": "pass", "reason": "loaded-empty (0 files, FullLoadRows=0)"})
            continue
        pr = probe_by_label.get(label, {})
        exists = pr.get("exists", False)
        empty = pr.get("empty", None)
        cp_uri = pt.get("config_prefix")
        has_progress = _has_recorded_progress(s3, cp_uri, label) if cp_uri else False

        if not exists:
            per_table.append({"label": label, "empty": empty, "hasProgress": has_progress,
                              "decision": "fail",
                              "reason": "target table does not exist in DSQL (create it with "
                                        "the same DDL + PK, then re-run)"})
            gate = "fail"
            continue
        if empty is True:
            per_table.append({"label": label, "empty": True, "hasProgress": has_progress,
                              "decision": "pass", "reason": "target is empty"})
            continue
        # non-empty
        if has_progress:
            per_table.append({"label": label, "empty": False, "hasProgress": True,
                              "decision": "pass",
                              "reason": "non-empty but has recorded per-range progress for THIS "
                                        "plan (resume)"})
            continue
        if override:
            warnings.append(
                "OVERRIDE: table %s is NON-EMPTY with no recorded matching plan, but override=true "
                "was passed - loading onto a non-empty table risks DUPLICATE rows. You have "
                "accepted shared responsibility for the data." % label)
            per_table.append({"label": label, "empty": False, "hasProgress": False,
                              "decision": "pass-override",
                              "reason": "non-empty, no matching plan, bypassed by override=true"})
            continue
        per_table.append({"label": label, "empty": False, "hasProgress": False,
                          "decision": "fail",
                          "reason": "target is NON-EMPTY and has no recorded matching plan. Range "
                                    "mode never blanks a table. Safe fix: DROP and recreate the "
                                    "table EMPTY (same DDL + PK), confirm 0 rows, then re-run; or "
                                    "re-run with override=true only if you accept shared "
                                    "responsibility for possible duplicates."})
        gate = "fail"

    for w in warnings:
        print("(warn) mrl_gate_eval: %s" % w)
    print("(info) mrl_gate_eval: gate=%s over %d table(s)" % (gate, len(per_table)))
    if gate != "pass":
        bad = [t for t in per_table if t["decision"] == "fail"]
        raise MrlGateError(
            "FAIL CLOSED: safety gate refused %d table(s):\n  - %s" %
            (len(bad), "\n  - ".join("%s: %s" % (t["label"], t["reason"]) for t in bad)))
    return {"ok": True, "gate": "pass", "perTable": per_table, "override": override,
            "warnings": warnings}
