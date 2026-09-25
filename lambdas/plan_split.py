"""
plan-split Lambda (Phase-1 startup) — the fan-out planner.

Runs AFTER the DMS completion gate (task stopped at STOPPED_AFTER_CACHED_EVENTS, so
FullLoadRows is final and the S3 export is frozen). Splits the tables into GROUPS and
writes one per-group manifest so each group's Glue jobs (load/validate/CDC) run disjoint.

Split rule (ORCHESTRATOR_DESIGN §6/§15):
  - A table gets its OWN group (own big loader) if
        FullLoadRows >= big_table_row_threshold  OR  num_files >= file_fanout_threshold.
  - Remaining (small) tables are LPT bin-packed by row count into the remaining group
    budget (max_groups - #big), balanced so no lane is wasted.
  - Total groups capped at max_groups (the pre-created CDC job pool size). If big tables
    alone exceed max_groups, we still emit one group per big table and log that the CDC
    pool must be >= that count (a config error to fix).

Per-group Glue args (budget-driven, GO BIG):
  - big-table group:   --max_files_in_parallel = min(max_files_in_parallel, num_files)
                       --max_write_concurrency = max(that, 30)  (own file fan-out)
                       worker sizing = big (chosen in the stack, not here)
  - small group:       --max_files_in_parallel = max_files_in_parallel (shared pool)
                       --max_write_concurrency = writers_per_loader (from conn budget)

Reads the master index (config/_manifest_index.json) written by Job1. For each table it
counts LOAD*.csv files under the table's DMS S3 path (ListObjectsV2, metadata only). Writes
each group's manifest to config/_orchestrator/group-<k>/_manifest_index.json (same shape as
the master index) so v16/Job3/v4 read it via --config_prefix. Also writes a durable
_orchestrator/state.json summarizing the plan.

Input event: {
  "bucket", "config_prefix", "cdc_root",
  "big_table_row_threshold", "file_fanout_threshold", "max_files_in_parallel",
  "max_groups", "conn_budget", "min_writers_per_loader", "max_writers_per_loader",
  "map_max_concurrency"
}
Returns: { "groups": [ { "group_index", "config_prefix", "kind", "tables":[labels],
            "rows", "num_files", "load_args": {...}, "cdc_job_name_suffix" } , ... ],
           "group_count", "state_key" }
"""

import json
import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


def _split_s3_uri(uri):
    no = uri.replace("s3://", "")
    parts = no.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _count_load_files(s3, dms_s3_path):
    """Count LOAD*.csv part-files under a table's DMS full-load prefix (metadata only)."""
    b, p = _split_s3_uri(dms_s3_path)
    p = p.rstrip("/") + "/"
    n = 0
    token = None
    while True:
        kw = {"Bucket": b, "Prefix": p}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            name = o["Key"].split("/")[-1]
            if name.upper().startswith("LOAD") and name.lower().endswith(".csv"):
                n += 1
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return n


def handler(event, context):
    # config_prefix may be a full "s3://bucket/key/" URI (per-task SM passes this) OR a bare
    # key with a separate "bucket". Normalize to (bucket, config_prefix-key).
    raw_cp = event["config_prefix"]
    if raw_cp.startswith("s3://"):
        bucket, config_prefix = _split_s3_uri(raw_cp)
        config_prefix = config_prefix.strip("/")
    else:
        bucket = event["bucket"]
        config_prefix = raw_cp.strip("/")
    big_threshold = int(event.get("big_table_row_threshold", 6_000_000))
    file_fanout_threshold = int(event.get("file_fanout_threshold", 8))
    max_files_in_parallel = int(event.get("max_files_in_parallel", 30))
    max_groups = int(event.get("max_groups", 10))
    conn_budget = int(event.get("conn_budget", 900))
    min_writers = int(event.get("min_writers_per_loader", 100))
    max_writers = int(event.get("max_writers_per_loader", 150))
    map_max_conc = int(event.get("map_max_concurrency", 6))

    s3 = boto3.client("s3", region_name=REGION)
    index_key = f"{config_prefix}/_manifest_index.json"
    index = _read_json(s3, bucket, index_key)
    entries = index.get("tables", [])
    if not entries:
        raise Exception(f"Master index s3://{bucket}/{index_key} has no tables — run Job1.")

    # Enrich each entry with FullLoadRows (from the index if Job1 recorded it; else 0) and
    # num_files (ListObjectsV2). The index carries dms_s3_path per entry.
    enriched = []
    for e in entries:
        rows = int(e.get("full_load_rows") or e.get("FullLoadRows") or 0)
        dms_s3_path = e.get("dms_s3_path")
        num_files = _count_load_files(s3, dms_s3_path) if dms_s3_path else 0
        label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
        enriched.append({"entry": e, "label": label, "rows": rows, "num_files": num_files})

    big = [t for t in enriched
           if t["rows"] >= big_threshold or t["num_files"] >= file_fanout_threshold]
    small = [t for t in enriched if t not in big]

    groups = []
    gi = 0
    for t in big:
        groups.append({"group_index": gi, "kind": "big", "members": [t]})
        gi += 1

    # Remaining group budget for small tables (LPT bin-pack by rows).
    remaining_buckets = max(0, max_groups - len(big))
    if small:
        n_buckets = max(1, remaining_buckets) if remaining_buckets > 0 else 1
        buckets = [[] for _ in range(n_buckets)]
        bucket_rows = [0] * n_buckets
        for t in sorted(small, key=lambda x: -x["rows"]):
            j = bucket_rows.index(min(bucket_rows))
            buckets[j].append(t)
            bucket_rows[j] += t["rows"]
        for b in buckets:
            if b:
                groups.append({"group_index": gi, "kind": "small", "members": b})
                gi += 1

    group_count = len(groups)
    if group_count > max_groups:
        print(f"WARNING: computed {group_count} groups > max_groups={max_groups}. The CDC "
              f"job pool (<project>-cdc-group-0..{max_groups-1}) is too small; increase "
              f"orchestrator.max_groups and redeploy FoundationStack.")

    # Writers-per-loader from the connection budget (cross-job pacing lever).
    loaders_in_flight = max(1, min(group_count, map_max_conc,
                                   conn_budget // max(1, min_writers)))
    writers_per_loader = max(min_writers,
                             min(max_writers, conn_budget // max(1, loaders_in_flight)))

    # Write per-group manifests + build the return payload.
    out_groups = []
    for g in groups:
        k = g["group_index"]
        group_prefix = f"{config_prefix}/_orchestrator/group-{k}"
        tables = [m["entry"] for m in g["members"]]
        group_index_doc = {
            "metadata": {
                "generated_by": "plan-split-lambda",
                "group_index": k,
                "kind": g["kind"],
                "source_index": index_key,
            },
            "tables": tables,
        }
        s3.put_object(
            Bucket=bucket, Key=f"{group_prefix}/_manifest_index.json",
            Body=json.dumps(group_index_doc, indent=2).encode("utf-8"),
            ContentType="application/json")

        rows = sum(m["rows"] for m in g["members"])
        nfiles = max((m["num_files"] for m in g["members"]), default=0)
        if g["kind"] == "big":
            load_args = {
                "--max_files_in_parallel": str(min(max_files_in_parallel, max(1, nfiles))),
                "--max_write_concurrency": str(max(min(max_files_in_parallel, max(1, nfiles)), 30)),
            }
        else:
            load_args = {
                "--max_files_in_parallel": str(max_files_in_parallel),
                "--max_write_concurrency": str(writers_per_loader),
            }
        out_groups.append({
            "group_index": k,
            "config_prefix": f"s3://{bucket}/{group_prefix}/",
            "kind": g["kind"],
            "tables": [m["label"] for m in g["members"]],
            "rows": rows,
            "num_files": nfiles,
            "load_args": load_args,
            "cdc_job_name_suffix": str(k),
        })

    state_key = f"{config_prefix}/_orchestrator/state.json"
    s3.put_object(
        Bucket=bucket, Key=state_key,
        Body=json.dumps({
            "group_count": group_count,
            "loaders_in_flight": loaders_in_flight,
            "writers_per_loader": writers_per_loader,
            "groups": out_groups,
        }, indent=2).encode("utf-8"),
        ContentType="application/json")

    return {
        "groups": out_groups,
        "group_count": group_count,
        "loaders_in_flight": loaders_in_flight,
        "writers_per_loader": writers_per_loader,
        "state_key": state_key,
    }
