#!/usr/bin/env python3
"""mrl_plan Lambda - THE HEART of the MRL child workflow.

For every table to load:
  1. resolve the table's folder (case-insensitive; fail closed on two case-variant holders),
  2. list its LOAD*.csv part-files fully (paginated), prove the list is complete and unique and
     uniform-width,
  3. split the sorted list into contiguous inclusive ranges by the table's max_concurrency,
  4. TRIPLE-PROVE the split (sizes, union, resolver-consistency via the loader's own resolver),
  5. build ONE dedicated single-table config prefix per table under
     s3://<bucket>/mrl/state/<suffix>/tables/<label>/config/ holding exactly that table's
     _manifest_index.json entry + column mapping (COPIED from discovery output, not re-derived),
  6. persist plan.json (sha256 over sorted names, counts, ranges, parameters) under
     s3://<bucket>/mrl/state/<suffix>/plan.json.

RE-RUN: if plan.json exists and the live listing still hashes the same per table, REUSE the
persisted plan. If the listing changed, FAIL CLOSED naming the difference (never silently
re-split a table that may already have loaded ranges).

n=0 (table has no LOAD files): allowed ONLY if the table's DMS FullLoadRows is 0 (mark
loaded-empty, no runs); else FAIL CLOSED (positive row count but no file).

Payload (built by the child SM from $.resolved and the discovery output):
  { bucket, taskSuffix, configPrefix (s3://.../config/_task/<suffix>/),
    cdc_root, max_concurrency (default/per-table), split_by ("count"|"bytes"),
    split_allow_hex (bool), max_files_cap (hard upper bound on concurrency),
    includeTables (optional ["schema.table", ...] include-list),
    csv_null_value, null_values, null_rules,
    load_args_defaults { max_files_in_parallel, writers_per_file } }
Returns: { ok, mrlStatePrefix, planKey, planSha, tables:[{label, config_prefix, num_files,
  ranges:[{index,first,last,size,bytes}], has_single_pk, loaded_empty}], rangeItems:[...],
  maxTotalConcurrentRuns, warnings }
rangeItems is the flat Map input (one per (table,range)); the Map never carries file lists.

NO AWS SDK import at module import beyond boto3; pure logic lives in mrl_split (imported).
Linux-safe; no strftime on years < 1000 etc. (only utc Z timestamps).
"""

import ast
import json
import os
from datetime import datetime, timezone

import boto3

import mrl_split as ms

REGION = os.environ.get("AWS_REGION", "us-east-1")

# The loader-resolver source is bundled in the zip as a sibling .py so we can run proof (c)
# (resolver-consistency) at plan time against the SAME resolver the Glue load job uses.
_RESOLVER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "mrl_loader_resolver.py")


class MrlPlanError(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_cp(config_prefix):
    cp = str(config_prefix or "")
    if cp.startswith("s3://"):
        b, key = cp[5:].split("/", 1)
        return b, key.rstrip("/") + "/"
    return None, cp.rstrip("/") + "/"


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


def table_cp_key_for(mrl_prefix_key, label):
    return "%stables/%s/config/" % (mrl_prefix_key, label)


def _table_has_recorded_ranges(s3, bucket, cp_key, label):
    """True if this table's config prefix already holds ANY per-range status or result doc (a
    prior attempt loaded or started at least one range). Used by the re-run concurrency-change
    guard (AMENDMENT 4.5)."""
    safe = str(label).replace("/", "_")
    for sub in ("_file_status/%s.range-" % safe, "_range_result/%s.range-" % safe):
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=cp_key + sub, MaxKeys=1)
        if resp.get("Contents"):
            return True
    return False


def _load_resolver():
    """AST-extract the loader's resolver (same code the Glue job runs) for proof (c). If the
    resolver file is absent (not bundled), proof (c) is skipped with a recorded warning rather
    than failing the plan — (a)+(b) still run, and the live resolver still governs the real
    load. The deploy script always bundles it."""
    if not os.path.exists(_RESOLVER_FILE):
        return None
    src = open(_RESOLVER_FILE).read()
    tree = ast.parse(src)
    ns = {"re": __import__("re")}
    wanted = {"_mr_basename", "_mr_file_number", "_mr_resolve_one", "_mr_resolve_slice",
              "ManualRangeError"}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in wanted:
            exec(ast.get_source_segment(src, n), ns)
    return ns if wanted <= set(ns) else None


def _prove_resolver(resolver_ns, ranges, files_sorted):
    """Proof (c): re-resolve each range through the loader's own resolver; FAIL CLOSED on any
    mismatch."""
    if resolver_ns is None:
        return False
    reslice = resolver_ns["_mr_resolve_slice"]
    base = resolver_ns["_mr_basename"]
    for r in ranges:
        first, last, assigned, _b = reslice(r["first"], r["last"], files_sorted)
        got = [base(u) for u in assigned]
        want = [ms.basename(u) for u in r["files"]]
        if first != r["first"] or last != r["last"] or got != want:
            raise MrlPlanError(
                "FAIL CLOSED: resolver-consistency proof failed for range %d (%s..%s): the "
                "loader's own resolver selects a different set than the plan assigned."
                % (r["index"], r["first"], r["last"]))
    return True


def _manifest_tables(index_doc):
    return (index_doc or {}).get("tables", []) or []


def _entry_label(e):
    return "%s.%s" % (e.get("dsql_schema"), e.get("dsql_table"))


def _has_single_pk(entry):
    pk = (entry.get("metadata", {}) or {}).get("primary_key") or {}
    cols = pk.get("columns") or entry.get("pk_columns") or []
    mode = pk.get("mode") or entry.get("pk_mode")
    if mode == "composite":
        return False
    return len(cols) == 1


def _r6_target_generated_pk_guard(entry, label):
    """AMENDMENT 3 (R6): fail closed BEFORE launching any Glue range if a primary-key column is
    not loaded from the source (not in target_columns). Such a PK is target-generated (a DEFAULT
    like gen_random_uuid() fires), so a re-run / automatic range re-run / commit-probe resume
    would re-insert the same rows with NEW keys -> silent duplicates. Mirrors the loader's own
    _mr_target_generated_pk_guard so the refusal happens at the plan step too."""
    meta = (entry.get("metadata", {}) or {})
    pk = meta.get("primary_key") or {}
    pk_cols = [str(c) for c in (pk.get("columns") or entry.get("pk_columns") or [])]
    if not pk_cols:
        return
    loaded = {str(c).lower() for c in (entry.get("target_columns") or [])}
    if not loaded:
        return   # no target_columns in this index entry (slim index) -> the loader guard catches it
    missing_pk = [c for c in pk_cols if c.lower() not in loaded]
    if missing_pk:
        raise MrlPlanError(
            "FAIL CLOSED (R6): target-generated primary key for %s -- primary-key column(s) %s "
            "are not loaded from the source (not in target_columns), so a column DEFAULT (e.g. "
            "gen_random_uuid()) would generate the key and a re-run would create SILENT "
            "DUPLICATES. The parallel range pipeline requires every primary-key column to come "
            "from the source. Refusing to launch any range for this table." % (label, missing_pk))


def _full_load_rows(entry, rowcounts, label):
    v = entry.get("full_load_rows")
    if v is None:
        v = (entry.get("metadata", {}) or {}).get("full_load_rows")
    if v is None and isinstance(rowcounts, dict):
        v = rowcounts.get(label.lower())
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def handler(event, context):
    bucket = event["bucket"]
    suffix = event["taskSuffix"]
    src_bucket, src_cp_key = _split_cp(event["configPrefix"])
    src_bucket = src_bucket or bucket
    cdc_root = event.get("cdc_root", ".")
    default_conc = int(event.get("max_concurrency", 3) or 3)   # MRL built-in default is 3
    mc_source = str(event.get("max_concurrency_source") or "built-in default")
    conc_cap = int(event.get("max_files_cap", 0) or 0)   # 0 = no hard cap beyond file count
    split_by = str(event.get("split_by", "count") or "count").strip().lower()
    allow_hex = bool(event.get("split_allow_hex", True))
    include = set(event.get("includeTables") or [])
    load_defaults = event.get("load_args_defaults") or {}
    per_table_conc = event.get("perTableConcurrency") or {}   # {label: c} optional overrides

    s3 = boto3.client("s3", region_name=REGION)
    warnings = []

    # Master index + rowcounts sidecar written by discovery under the SOURCE config prefix.
    index = _get_json(s3, src_bucket, src_cp_key + "_manifest_index.json")
    if not index or not _manifest_tables(index):
        raise MrlPlanError(
            "FAIL CLOSED: master index s3://%s/%s_manifest_index.json has no tables. Discovery "
            "must run first." % (src_bucket, src_cp_key))
    rowcounts_doc = _get_json(s3, src_bucket, src_cp_key + "table_rowcounts.json") or {}
    rowcounts = rowcounts_doc.get("rowcounts") if isinstance(rowcounts_doc, dict) else {}

    mrl_prefix_key = "mrl/state/%s/" % suffix
    plan_key = mrl_prefix_key + "plan.json"
    existing_plan = _get_json(s3, bucket, plan_key)
    resolver_ns = _load_resolver()
    resolver_proved = resolver_ns is not None
    if not resolver_proved:
        warnings.append("loader resolver source not bundled; split proof (c) "
                        "(resolver-consistency) was skipped at plan time (proofs (a)+(b) ran, "
                        "and the Glue load job's own resolver governs the real load).")

    tables_out = []
    range_items = []
    max_total_conc = 0
    plan_tables = {}

    for entry in _manifest_tables(index):
        label = _entry_label(entry)
        if include and label not in include:
            continue
        # AMENDMENT 3 (R6): refuse a TARGET-GENERATED primary key BEFORE any Glue range is
        # launched. If a PK column is not in target_columns (not loaded from the source), the
        # target DEFAULT (e.g. gen_random_uuid()) would mint keys and a re-run would duplicate.
        _r6_target_generated_pk_guard(entry, label)
        dms_schema = entry.get("dms_schema") or entry.get("metadata", {}).get("dms_schema")
        dms_table = entry.get("dms_table") or entry.get("metadata", {}).get("dms_table")
        if not dms_schema or not dms_table:
            raise MrlPlanError("FAIL CLOSED: table %s has no dms_schema/dms_table in the index."
                               % label)

        # Resolve the folder (case-insensitive; fail closed on two case-variant holders).
        loc = ms.resolve_table_folder(s3, src_bucket, cdc_root, dms_schema, dms_table,
                                      allow_hex=allow_hex)
        # AMENDMENT 7.3: discovery (job1) is the SINGLE SOURCE OF TRUTH for the table folder. MRL
        # must NOT re-guess it: assert MRL's own resolution AGREES with discovery's
        # metadata.dms_s3_path; any disagreement fails closed naming BOTH paths.
        disc_path = (entry.get("metadata", {}) or {}).get("dms_s3_path") or entry.get("dms_s3_path")
        if loc.get("found") and disc_path:
            def _norm(p):
                return str(p).rstrip("/").lower()
            if _norm(disc_path) != _norm(loc["dms_s3_path"]):
                raise MrlPlanError(
                    "FAIL CLOSED (Amendment 7): MRL resolved the folder for %s to %s but "
                    "discovery recorded metadata.dms_s3_path=%s. MRL must not re-guess the folder; "
                    "these must agree. Investigate the DMS export / letter case." %
                    (label, loc["dms_s3_path"], disc_path))
        files_sorted, info = ([], {"num_files": 0, "zero_byte_count": 0, "zero_byte_files": [],
                                   "ignored_cdc": 0, "ignored_other": 0})
        if loc["found"]:
            files_sorted, info = ms.list_table_files(s3, loc["dms_s3_path"], allow_hex=allow_hex)

        n = len(files_sorted)
        flr = _full_load_rows(entry, rowcounts, label)

        # n=0 handling (brief section 5).
        if n == 0:
            if flr == 0:
                tables_out.append({"label": label, "config_prefix": None, "num_files": 0,
                                   "ranges": [], "has_single_pk": _has_single_pk(entry),
                                   "loaded_empty": True})
                plan_tables[label] = {"num_files": 0, "sha256": ms.listing_sha256([]),
                                      "ranges": [], "loaded_empty": True,
                                      "has_single_pk": _has_single_pk(entry)}
                continue
            raise MrlPlanError(
                "FAIL CLOSED: table %s has NO LOAD*.csv files under %s but its DMS FullLoadRows "
                "is %s (not 0). A positive row count with no file means the export is missing; "
                "refusing to mark it loaded-empty." % (label, loc["dms_s3_path"], flr))

        # Uniqueness + uniform width (fail closed).
        unique_count = ms.assert_unique_basenames(files_sorted)
        ms.assert_uniform_width(files_sorted)
        if unique_count != n:
            raise MrlPlanError("FAIL CLOSED: %d keys but %d unique basenames for %s."
                               % (n, unique_count, label))

        # AMENDMENT 2: the plan's file set MUST EQUAL the set the loader would load. Compute
        # S_loader (the loader's LOOSE _is_full_load_key) and compare to S_plan (our strict,
        # uniform-width listing). Any difference - a stray 'load_notes.csv', a mixed-width name,
        # a non-strict basename - FAILS CLOSED naming the files + the fix, so a file is never
        # loaded-but-unplanned (double-loaded / uncounted) or planned-but-unloaded.
        ms.assert_plan_equals_loader_set(s3, loc["dms_s3_path"], files_sorted, allow_hex=allow_hex)

        # AMENDMENT 6: every PLANNED name must be on the allow-list and NONE may match the CDC
        # pattern (defence in depth over the listing's own filter). Top-level CDC files in the
        # folder are IGNORED and only reported here, never in the plan/hash/split/count.
        ms.assert_names_allow_listed([ms.basename(u) for u, _s in files_sorted],
                                     "the plan for %s" % label, allow_hex=allow_hex)
        if info.get("ignored_cdc") or info.get("ignored_other") or info.get("zero_byte_count"):
            print("(info) mrl_plan: table %s: ignored %d CDC file(s), %d other non-LOAD file(s), "
                  "%d zero-byte LOAD file(s) (none planned/loaded/counted)."
                  % (label, info.get("ignored_cdc", 0), info.get("ignored_other", 0),
                     info.get("zero_byte_count", 0)))

        sha = ms.listing_sha256(files_sorted)

        # RE-RUN drift check against the persisted plan (never silently re-split).
        # Choose concurrency: per-table override, else the resolved default; clamp to [1, n] and
        # to the hard cap. `effective_c` is the number of PARALLEL RANGE LOADS for this table.
        c = int(per_table_conc.get(label, default_conc))
        if c < 1:
            c = 1
        if conc_cap > 0:
            c = min(c, conc_cap)
        c = min(c, n)

        # RE-RUN SAFETY vs the persisted plan (never silently re-split a partly-loaded table):
        if existing_plan and label in (existing_plan.get("tables") or {}):
            prev = existing_plan["tables"][label]
            if not prev.get("loaded_empty"):
                # (a) listing drift
                if prev.get("sha256") != sha:
                    raise MrlPlanError(
                        "FAIL CLOSED: the LOAD file listing for %s changed since the plan was "
                        "persisted (was sha %s over %d files, now sha %s over %d files). A table "
                        "with already-loaded ranges must NOT be re-split. If this is a deliberate "
                        "fresh reload, delete s3://%s/%s and the table's state under %stables/%s/ "
                        "first." % (label, prev.get("sha256"), prev.get("num_files"), sha, n,
                                    bucket, plan_key, mrl_prefix_key, label))
                # (b) AMENDMENT 4.5: a DIFFERENT max_concurrency would change the ranges. Allowed
                # ONLY if the table has NO loaded range yet AND the target is still empty; else
                # FAIL CLOSED. "No loaded range" = no per-range status/result doc recorded.
                prev_c = prev.get("concurrency")
                if prev_c is not None and int(prev_c) != c:
                    loaded = _table_has_recorded_ranges(s3, bucket, table_cp_key_for(mrl_prefix_key, label), label)
                    if loaded:
                        raise MrlPlanError(
                            "FAIL CLOSED: a plan already exists for %s with %d range(s) "
                            "(max_concurrency %s) and ranges have already been loaded; re-running "
                            "with a DIFFERENT max_concurrency (%d) would change the ranges of a "
                            "partly-loaded table. Re-run with the SAME max_concurrency (%s), or "
                            "start a clean plan: blank the table and delete its state under "
                            "%stables/%s/ (and the plan entry), then re-run." %
                            (label, len(prev.get("ranges") or []), prev_c, c, prev_c,
                             mrl_prefix_key, label))
                    warnings.append(
                        "max_concurrency for %s changed from %s to %d since the persisted plan, "
                        "but the table has no loaded range yet and is being re-planned cleanly."
                        % (label, prev_c, c))

        if split_by == "bytes":
            ranges = ms.build_ranges_by_bytes(files_sorted, c)
        else:
            ranges = ms.build_ranges(files_sorted, c)

        # TRIPLE PROOF: (a)+(b) in prove_ranges, (c) via the loader's own resolver.
        ms.prove_ranges(ranges, files_sorted)
        if resolver_proved:
            _prove_resolver(resolver_ns, ranges, files_sorted)
        # AMENDMENT 6 (b): every range START/END name is on the allow-list (never a CDC name).
        ms.assert_names_allow_listed(
            [r["first"] for r in ranges] + [r["last"] for r in ranges],
            "the range bounds for %s" % label, allow_hex=allow_hex)

        # Arithmetic line (AMENDMENT 4.6): files, effective max_concurrency + source, split.
        print("(info) mrl_plan: table %s: %d files, max_concurrency %d (source: %s) -> %s"
              % (label, n, c, mc_source, ",".join(str(r["size"]) for r in ranges)))

        has_single_pk = _has_single_pk(entry)

        # Build the dedicated single-table config prefix (COPY the per-table config, do not
        # re-derive). The loader reads _manifest_index.json + <dsql_table>_column_mapping.json
        # under its --config_prefix.
        table_cp_key = "%stables/%s/config/" % (mrl_prefix_key, label)
        table_cp_uri = "s3://%s/%s" % (bucket, table_cp_key)
        single_index = {
            "metadata": {"generated_by": "mrl_plan", "source_index":
                         "s3://%s/%s_manifest_index.json" % (src_bucket, src_cp_key),
                         "mrl_table": label},
            "tables": [entry],
        }
        _put_json(s3, bucket, table_cp_key + "_manifest_index.json", single_index)
        # Copy the per-table column mapping object discovery wrote (by dsql_table name).
        mapping_name = "%s_column_mapping.json" % entry.get("dsql_table")
        mapping = _get_json(s3, src_bucket, src_cp_key + mapping_name)
        if mapping is not None:
            _put_json(s3, bucket, table_cp_key + mapping_name, mapping)
        else:
            warnings.append("no column mapping %s found under the source config prefix for %s; "
                            "the loader derives mapping from its index entry." % (mapping_name,
                                                                                   label))
        # Copy the rowcounts sidecar so the count job can read FullLoadRows from the table cp.
        if rowcounts_doc:
            _put_json(s3, bucket, table_cp_key + "table_rowcounts.json", rowcounts_doc)

        range_descs = []
        for r in ranges:
            range_descs.append({"index": r["index"], "first": r["first"], "last": r["last"],
                                "size": r["size"], "bytes": r["bytes"]})
            # Map item: NEVER carries the file list (payload-size safe). STRING values only
            # where they become Glue Arguments (the SM wraps numbers with States.Format too).
            range_items.append({
                "label": label,
                "config_prefix": table_cp_uri,
                "first": r["first"],
                "last": r["last"],
                "range_index": str(r["index"]),
                "has_single_pk": has_single_pk,
            })
        max_total_conc += len(ranges)

        tables_out.append({"label": label, "config_prefix": table_cp_uri, "num_files": n,
                           "ranges": range_descs, "has_single_pk": has_single_pk,
                           "loaded_empty": False, "zero_byte_count": info["zero_byte_count"],
                           "ignored_cdc": info.get("ignored_cdc", 0),
                           "ignored_other": info.get("ignored_other", 0),
                           "full_load_rows": flr, "concurrency": c})
        plan_tables[label] = {"num_files": n, "sha256": sha, "config_prefix": table_cp_uri,
                              "ranges": range_descs, "has_single_pk": has_single_pk,
                              "loaded_empty": False, "concurrency": c, "full_load_rows": flr,
                              "dms_s3_path": loc["dms_s3_path"], "split_by": split_by,
                              "counts": {"allowed": n,
                                         "ignored_cdc": info.get("ignored_cdc", 0),
                                         "ignored_other": info.get("ignored_other", 0),
                                         "zero_byte": info.get("zero_byte_count", 0)}}

    if not tables_out:
        raise MrlPlanError("FAIL CLOSED: no tables selected to plan (include-list matched "
                           "nothing, or the index is empty after filtering).")

    plan_doc = {
        "version": 1,
        "generated_at": _now(),
        "taskSuffix": suffix,
        "bucket": bucket,
        "sourceConfigPrefix": event["configPrefix"],
        "split_by": split_by,
        "allow_hex": allow_hex,
        "allow_pattern": ms.ALLOW_PATTERN,
        "cdc_pattern": ms.CDC_PATTERN,
        "default_concurrency": default_conc,
        "max_concurrency_source": mc_source,
        "load_args_defaults": load_defaults,
        "null": {"csv_null_value": event.get("csv_null_value"),
                 "null_values": event.get("null_values"),
                 "null_rules": event.get("null_rules"),
                 "null_notnull_text": event.get("null_notnull_text")},
        "maxTotalConcurrentRuns": max_total_conc,
        "resolver_proved": resolver_proved,
        "tables": plan_tables,
    }
    # Deterministic plan sha over the sorted table shas (for state tooling / drift at task level).
    import hashlib
    h = hashlib.sha256()
    for lbl in sorted(plan_tables):
        h.update(("%s=%s\n" % (lbl, plan_tables[lbl].get("sha256", ""))).encode("utf-8"))
    plan_doc["planSha"] = h.hexdigest()
    _put_json(s3, bucket, plan_key, plan_doc)

    for w in warnings:
        print("(warn) mrl_plan: %s" % w)
    # AMENDMENT 3.5: the customer guarantees ONE TABLE PER DMS TASK. MRL adds NO check or failure
    # for table count - the per-table loop is general and costs nothing. If a manifest happens to
    # carry several tables, each is planned independently and we print ONE informational line.
    if len(tables_out) > 1:
        print("(info) mrl_plan: task has %d tables; each is planned separately "
              "(one-table-per-task is the documented assumption, not enforced)." % len(tables_out))
    print("(info) mrl_plan: %d table(s), %d total range run(s), planSha %s -> s3://%s/%s"
          % (len(tables_out), max_total_conc, plan_doc["planSha"][:12], bucket, plan_key))

    return {
        "ok": True,
        "mrlStatePrefix": "s3://%s/%s" % (bucket, mrl_prefix_key),
        "planKey": "s3://%s/%s" % (bucket, plan_key),
        "planSha": plan_doc["planSha"],
        "tables": tables_out,
        "rangeItems": range_items,
        "maxTotalConcurrentRuns": max_total_conc,
        "warnings": warnings,
    }
