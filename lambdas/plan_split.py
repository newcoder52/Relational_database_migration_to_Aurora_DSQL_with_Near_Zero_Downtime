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
Returns: { "groups": [ { "group_index", "config_prefix", "kind", "is_fork", "tables":[labels],
            "rows", "num_files", "load_args": {...}, "loadJobName", "loadBigJobName",
            "validateJobName" } , ... ],
           "forks": [ { "kind":"ck"|"bg", "fork_slug", "fork_table", "config_prefix",
                        "cdcJobName", (ck:) "loadJobName","validateJobName","loadRole" }, ... ],
           "cdcOwners": { "<schema.table>": "main"|"ck-<slug>"|"bg-<slug>" },
           "group_count", "fork_count", "bgOverflow", "warnings", "state_key" }
"""

import hashlib
import json
import os
import re

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")

# FORK support (DESIGN_FORK.md). A composite-PK table is forked out of normal grouping with its
# OWN load/validate/cdc jobs (<project>-<suffix>-ck-<slug>-<role>). A BIG single/no-PK table keeps
# its normal "big" group (shared load-big + validate) but gets its OWN CDC job
# (<project>-<suffix>-bg-<slug>-cdc) — CDC-only fork. The ONE ownership record (cdcOwners in the
# task registry _jobs.json) assigns every table to exactly one CDC job: main | ck-<slug> | bg-<slug>.
_FORK_INFIX = "ck"                 # composite-key fork infix (load/validate/cdc)
_BIG_FORK_INFIX = "bg"             # big single/no-PK CDC fork infix (CDC only)
_MAX_COMPOSITE_FORKS_DEFAULT = 8   # params.csv max_composite_forks default
_MAX_BIG_CDC_FORKS_DEFAULT = 8     # params.csv max_big_cdc_forks default (overflow -> main + warn)
_GLUE_NAME_MAX = 255               # Glue job-name hard limit
# Glue concurrent job runs per account (default, adjustable) — sources.md (AWS re:Post). Used to
# WARN when a task's always-on CDC runs (1 main + #ck + #bg) take a large share of the account quota.
# (NOTE: DSQL's "5 CDC streams/cluster" quota is for OUTBOUND change streams and does NOT bound our
# inbound CDC SQL-writer jobs — see scale-1b/CORRECTIONS.md.)
_GLUE_CONCURRENT_RUNS_QUOTA = 30


def _fork_slug(dsql_schema, dsql_table):
    """Deterministic, collision-free slug for a composite table's fork, from its EXACT
    schema.table. A readable lowercased `schema-table` plus an 8-hex sha1 of the exact original
    `schema.table`, so two tables that differ only in case/punctuation (e.g. 'App.Order' vs
    'app_order') still get DISTINCT slugs (the hash differs). Shape: '<readable>-<h8>'."""
    exact = f"{dsql_schema}.{dsql_table}"
    h8 = hashlib.sha1(exact.encode("utf-8")).hexdigest()[:8]
    readable = re.sub(r"[^a-z0-9]+", "-", exact.lower()).strip("-") or "t"
    return f"{readable}-{h8}", h8


def _fork_job_name(project, task_suffix, slug, h8, role=None, infix=_FORK_INFIX):
    """<project>-<task_suffix>-<infix>-<slug>[-<role>] (infix 'ck' composite, 'bg' big), enforcing
    Glue's 255-char limit. If the full name would exceed 255, the READABLE part of the slug is
    truncated while the 8-hex hash and the role suffix are preserved (so the name stays unique and
    valid). role is e.g. 'load', 'load-big', 'validate', 'cdc'; None gives the base fork name."""
    tail = f"-{role}" if role else ""
    prefix = f"{project}-{task_suffix}-{infix}-"
    name = f"{prefix}{slug}{tail}"
    if len(name) <= _GLUE_NAME_MAX:
        return name
    # Truncate the readable middle, keep "-<h8>" + tail. slug == "<readable>-<h8>".
    keep_tail = f"-{h8}{tail}"
    budget = _GLUE_NAME_MAX - len(prefix) - len(keep_tail)
    if budget < 1:
        raise Exception(
            f"Fork job name for {slug!r} ({infix}) cannot fit Glue's {_GLUE_NAME_MAX}-char limit "
            f"even after truncation (project/task name too long). Use a shorter project or task "
            f"name.")
    readable = slug.rsplit(f"-{h8}", 1)[0][:budget].rstrip("-") or "t"
    return f"{prefix}{readable}{keep_tail}"


def _split_s3_uri(uri):
    no = uri.replace("s3://", "")
    parts = no.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))


def _count_load_files(s3, dms_s3_path):
    """(num_files, total_bytes) for LOAD*.csv full-load part-files under a table's DMS prefix
    (metadata only, no data scanned). total_bytes lets plan_split classify a big SINGLE-FILE
    table even when the DMS row count is missing from the index."""
    b, p = _split_s3_uri(dms_s3_path)
    p = p.rstrip("/") + "/"
    n = 0
    total = 0
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
                total += int(o.get("Size", 0))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return n, total


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
    big_bytes_threshold = int(event.get("big_table_bytes_threshold", 1_000_000_000))
    max_files_in_parallel = int(event.get("max_files_in_parallel", 30))
    writers_per_file = int(event.get("writers_per_file", 1))
    max_groups = int(event.get("max_groups", 10))
    conn_budget = int(event.get("conn_budget", 900))
    min_writers = int(event.get("min_writers_per_loader", 100))
    max_writers = int(event.get("max_writers_per_loader", 150))
    map_max_conc = int(event.get("map_max_concurrency", 6))
    max_composite_forks = int(event.get("max_composite_forks", _MAX_COMPOSITE_FORKS_DEFAULT))
    max_big_cdc_forks = int(event.get("max_big_cdc_forks", _MAX_BIG_CDC_FORKS_DEFAULT))
    # project/task_suffix are needed to NAME the per-fork Glue jobs deterministically. The
    # per-task SM passes them; fall back to deriving from config_prefix tail if absent.
    project = event.get("project") or ""
    task_suffix = event.get("taskSuffix") or event.get("task_suffix") or ""
    if not task_suffix:
        # config_prefix is .../config/_task/<suffix>/ -> take the <suffix> segment.
        _parts = [p for p in config_prefix.split("/") if p]
        task_suffix = _parts[-1] if _parts else ""

    s3 = boto3.client("s3", region_name=REGION)
    index_key = f"{config_prefix}/_manifest_index.json"
    index = _read_json(s3, bucket, index_key)
    entries = index.get("tables", [])
    if not entries:
        raise Exception(f"Master index s3://{bucket}/{index_key} has no tables — run Job1.")

    def _is_composite(e):
        return e.get("pk_mode") == "composite" or len(e.get("pk_columns") or []) > 1

    # COMPOSITE FORKS: pull composite-PK tables OUT of the normal grouping. Each becomes its own
    # fork (own load/validate/cdc jobs). Enforce the cap BEFORE building anything (fail early).
    composite_entries = [e for e in entries if _is_composite(e)]
    normal_entries = [e for e in entries if not _is_composite(e)]
    if len(composite_entries) > max_composite_forks:
        names = [f"{e.get('dsql_schema')}.{e.get('dsql_table')}" for e in composite_entries]
        raise Exception(
            f"Task has {len(composite_entries)} composite-PK tables but max_composite_forks="
            f"{max_composite_forks}; each composite table runs its own always-on CDC job. "
            f"Tables: {', '.join(sorted(names))}. Reduce composite tables in this task's DMS "
            f"selection rules, split the task, or raise max_composite_forks in params.csv "
            f"(mind Glue job/concurrent-run and DSQL connection quotas). No jobs were created.")

    # Enrich each entry with FullLoadRows + num_files + total_bytes for grouping. Prefer the
    # values discovery folded into the index (full_load_rows from the DMS sidecar; num_files/
    # total_bytes from its S3 listing); fall back to an S3 count here if the index lacks them
    # (older discovery). Track whether the ROW COUNT was actually known so we can WARN per table
    # with no count and classify it by size instead of silently treating it as small (the bug
    # that kept a 16.3M-row single-file table off the big-table path).
    rowcount_missing = []   # labels with no known FullLoadRows (warned below)

    def _enrich(e):
        label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
        _raw = e.get("full_load_rows", e.get("FullLoadRows"))
        rows_known = _raw is not None
        try:
            rows = int(_raw) if rows_known else 0
        except (TypeError, ValueError):
            rows, rows_known = 0, False
        num_files = e.get("num_files")
        total_bytes = e.get("total_bytes")
        if num_files is None or total_bytes is None:
            dms_s3_path = e.get("dms_s3_path")
            _nf, _tb = _count_load_files(s3, dms_s3_path) if dms_s3_path else (0, 0)
            num_files = _nf if num_files is None else int(num_files)
            total_bytes = _tb if total_bytes is None else int(total_bytes)
        else:
            num_files, total_bytes = int(num_files), int(total_bytes)
        # A non-empty table with no known row count can't be sized by rows -> warn and let the
        # file/byte test below decide (so a big single-file table is still caught).
        if not rows_known and not e.get("empty_at_discovery"):
            rowcount_missing.append(label)
        return {"entry": e, "label": label, "rows": rows, "rows_known": rows_known,
                "num_files": num_files, "total_bytes": total_bytes}

    enriched = [_enrich(e) for e in normal_entries]
    composite_enriched = [_enrich(e) for e in composite_entries]

    def _is_big(t):
        # A table is "big" (own load-big group + own bg CDC job) if ANY size signal crosses a
        # threshold: DMS FullLoadRows, LOAD part-file count, or total full-load bytes. The bytes
        # test is what rescues a huge SINGLE-FILE table whose num_files=1 (< fanout) and whose
        # row count may be unknown — exactly the sporting_event_ticket case.
        return (t["rows"] >= big_threshold
                or t["num_files"] >= file_fanout_threshold
                or t["total_bytes"] >= big_bytes_threshold)

    if rowcount_missing:
        print(f"WARNING: {len(rowcount_missing)} table(s) have NO DMS FullLoadRows in the index "
              f"(the master index carried no row count): {', '.join(sorted(rowcount_missing))}. "
              f"Classifying them by S3 file count (>= {file_fanout_threshold}) and total bytes "
              f"(>= {big_bytes_threshold:,}) instead of row count, so a big single-file table is "
              f"not silently treated as small. Ensure BuildTableList wrote table_rowcounts.json "
              f"(DMS describe_table_statistics) and discovery folded it into the index.")

    big = [t for t in enriched if _is_big(t)]
    small = [t for t in enriched if t not in big]
    for t in big:
        print(f"(info) BIG table {t['label']}: rows={t['rows']:,} "
              f"(known={t['rows_known']}) num_files={t['num_files']} "
              f"total_bytes={t['total_bytes']:,} -> own load-big group + bg CDC job")

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
        print(f"WARNING: computed {group_count} load/validate groups > max_groups={max_groups}; "
              f"consider raising max_groups so small tables bin-pack across more lanes.")

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
            _bf = min(max_files_in_parallel, max(1, nfiles))
            # A big group is a SINGLE big table. Total concurrent DSQL writers =
            # files-in-flight x writers_per_file; size max_write_concurrency to cover it (>=30
            # floor kept for backward compatibility). This is what finally parallelises a huge
            # SINGLE-FILE table: nfiles=1 but writers_per_file>1 -> many writers on that one file.
            load_args = {
                "--max_files_in_parallel": str(_bf),
                "--writers_per_file": str(writers_per_file),
                "--max_write_concurrency": str(max(_bf * max(1, writers_per_file), 30)),
            }
        else:
            load_args = {
                "--max_files_in_parallel": str(max_files_in_parallel),
                "--writers_per_file": str(writers_per_file),
                "--max_write_concurrency": str(writers_per_loader),
            }
        out_groups.append({
            "group_index": k,
            "config_prefix": f"s3://{bucket}/{group_prefix}/",
            "kind": g["kind"],
            "is_fork": False,
            "tables": [m["label"] for m in g["members"]],
            "rows": rows,
            "num_files": nfiles,
            "load_args": load_args,
            # Effective job names so the GroupFanOut Map reads them uniformly for groups + forks.
            # Normal groups use the task's SHARED load/load-big/validate job definitions.
            "loadJobName": f"{project}-{task_suffix}-load",
            "loadBigJobName": f"{project}-{task_suffix}-load-big",
            "validateJobName": f"{project}-{task_suffix}-validate",
        })

    # ---- COMPOSITE FORKS -------------------------------------------------------------------
    # Each composite table is its OWN fork: a one-table manifest + its own load/validate/cdc
    # jobs named <project>-<suffix>-ck-<slug>[-role]. The fork's load+validate run in the SAME
    # GroupFanOut Map (appended to out_groups, flagged is_fork) using the fork's load/validate
    # job names; its CDC job is started separately after ResumeDmsToCdc. A big composite table
    # (same row/file rule) uses the fork's load-big job.
    out_forks = []
    for m in sorted(composite_enriched, key=lambda x: x["label"]):
        e = m["entry"]
        slug, h8 = _fork_slug(e.get("dsql_schema"), e.get("dsql_table"))
        fork_prefix = f"{config_prefix}/_orchestrator/{_FORK_INFIX}-{slug}"
        fork_index_doc = {
            "metadata": {
                "generated_by": "plan-split-lambda",
                "fork_slug": slug,
                "kind": "composite",
                "source_index": index_key,
            },
            "tables": [e],   # exactly ONE composite table
        }
        s3.put_object(
            Bucket=bucket, Key=f"{fork_prefix}/_manifest_index.json",
            Body=json.dumps(fork_index_doc, indent=2).encode("utf-8"),
            ContentType="application/json")

        is_big = _is_big(m)
        nfiles = m["num_files"]
        if is_big:
            _bf = min(max_files_in_parallel, max(1, nfiles))
            load_args = {
                "--max_files_in_parallel": str(_bf),
                "--writers_per_file": str(writers_per_file),
                "--max_write_concurrency": str(max(_bf * max(1, writers_per_file), 30)),
            }
        else:
            load_args = {
                "--max_files_in_parallel": str(max_files_in_parallel),
                "--writers_per_file": str(writers_per_file),
                "--max_write_concurrency": str(writers_per_loader),
            }
        load_role = "load-big" if is_big else "load"
        fork = {
            "kind": "ck",
            "fork_slug": slug,
            "fork_hash": h8,
            "fork_table": m["label"],
            "config_prefix": f"s3://{bucket}/{fork_prefix}/",
            "rows": m["rows"],
            "num_files": nfiles,
            "load_args": load_args,
            "loadJobName": _fork_job_name(project, task_suffix, slug, h8, load_role),
            "validateJobName": _fork_job_name(project, task_suffix, slug, h8, "validate"),
            "cdcJobName": _fork_job_name(project, task_suffix, slug, h8, "cdc"),
            "loadRole": load_role,   # "load" or "load-big" (which template/size the fork load uses)
        }
        out_forks.append(fork)
        # The Map iterates groups+forks for load/validate. A ck fork is a one-table "group" with
        # is_fork=true and its OWN load/validate job names (not the shared ones).
        out_groups.append({
            "group_index": gi,
            "config_prefix": fork["config_prefix"],
            "kind": "composite-big" if is_big else "composite",
            "is_fork": True,
            "fork_slug": slug,
            "tables": [m["label"]],
            "rows": m["rows"],
            "num_files": nfiles,
            "load_args": load_args,
            "loadJobName": fork["loadJobName"],
            "loadBigJobName": fork["loadJobName"],
            "validateJobName": fork["validateJobName"],
        })
        gi += 1

    # ---- BIG-TABLE CDC FORKS (bg) ----------------------------------------------------------
    # Every BIG single/no-PK table (already its own "big" group for load/validate) ALSO gets its
    # OWN CDC job <project>-<suffix>-bg-<slug>-cdc (main CDC script, scoped to that one table's
    # big-group manifest). Load/validate stay as today (shared load-big + validate). A big
    # COMPOSITE table is NOT double-forked — it is already a ck fork above.
    bg_forks = []
    bg_overflow = []   # big tables beyond the cap -> stay on the MAIN CDC job (warn)
    big_single_nopk = [t for t in big]   # 'big' holds only NON-composite tables (composite pulled out)
    for idx, m in enumerate(sorted(big_single_nopk, key=lambda x: x["label"])):
        e = m["entry"]
        slug, h8 = _fork_slug(e.get("dsql_schema"), e.get("dsql_table"))
        if len(bg_forks) >= max_big_cdc_forks:
            bg_overflow.append(m["label"])
            continue
        # The big table already has its own "big" group; its group manifest is the one-table
        # CDC scope. Find that group's config_prefix (the group whose single table is this one).
        grp = next((g for g in out_groups
                    if not g.get("is_fork") and g.get("kind") == "big" and g["tables"] == [m["label"]]),
                   None)
        cdc_cp = grp["config_prefix"] if grp else f"s3://{bucket}/{config_prefix}/_orchestrator/bg-{slug}/"
        if grp is None:
            # Defensive: write a one-table manifest if the big group wasn't found (shouldn't happen).
            s3.put_object(Bucket=bucket,
                          Key=f"{config_prefix}/_orchestrator/bg-{slug}/_manifest_index.json",
                          Body=json.dumps({"metadata": {"generated_by": "plan-split-lambda",
                                                        "fork_slug": slug, "kind": "big"},
                                           "tables": [e]}, indent=2).encode("utf-8"),
                          ContentType="application/json")
        bg_forks.append({
            "kind": "bg",
            "fork_slug": slug,
            "fork_hash": h8,
            "fork_table": m["label"],
            "config_prefix": cdc_cp,
            "rows": m["rows"],
            "num_files": m["num_files"],
            "cdcJobName": _fork_job_name(project, task_suffix, slug, h8, "cdc", infix=_BIG_FORK_INFIX),
        })

    out_forks.extend(bg_forks)
    fork_count = len(out_forks)

    warnings = []
    if bg_overflow:
        warnings.append(
            f"{len(bg_overflow)} big table(s) exceed max_big_cdc_forks={max_big_cdc_forks} and "
            f"stay on the MAIN CDC job (serial apply): {', '.join(sorted(bg_overflow))}. Raise "
            f"max_big_cdc_forks in params.csv to give them their own CDC job (mind the Glue "
            f"concurrent-run and DSQL connection limits below).")

    # Intended per-table CDC owners from THIS plan: ck-/bg-<slug> for forked tables, else 'main'
    # (including bg-overflow tables, which the main job applies).
    intended_owners = {}
    for e in composite_entries:
        s, _h = _fork_slug(e.get("dsql_schema"), e.get("dsql_table"))
        intended_owners[f"{e.get('dsql_schema')}.{e.get('dsql_table')}"] = f"ck-{s}"
    for bf in bg_forks:
        intended_owners[bf["fork_table"]] = f"bg-{bf['fork_slug']}"

    # STABLE OWNERSHIP: once a task's CDC has started, the owner assignment is FROZEN. If a prior
    # registry (_jobs.json) already recorded cdcOwners, KEEP them and WARN on any table whose
    # intended owner now differs (a threshold/row-count change must NOT re-route a table while an
    # old CDC job may still own it — that would double-apply). The authoritative stop/recreate of
    # any genuinely-removed job is handled by ensure_fork_jobs reconcile + cutover (G3).
    owners = dict(intended_owners)
    try:
        prior = _read_json(s3, bucket, f"{config_prefix}/_jobs.json")
    except Exception:
        prior = None
    if isinstance(prior, dict) and prior.get("cdcOwners"):
        prior_owners = prior["cdcOwners"]
        changed = {t: (prior_owners.get(t), intended_owners.get(t, "main"))
                   for t in set(prior_owners) | set(intended_owners)
                   if prior_owners.get(t, "main") != intended_owners.get(t, "main")}
        if changed:
            warnings.append(
                f"CDC ownership is already recorded for this task; KEEPING the existing owners "
                f"(a change while old CDC jobs exist could double-apply). {len(changed)} table(s) "
                f"would differ now (threshold/row-count change): "
                + "; ".join(f"{t}: {old or 'main'}->{new or 'main'}" for t, (old, new)
                            in sorted(changed.items()))
                + ". To re-assign, cut over this task (stops+deletes all CDC jobs) and start fresh.")
        owners = dict(prior_owners)   # frozen

    # CONCURRENT-CDC guardrail: always-on CDC runs for this task = 1 main + #ck + #bg. Warn when
    # that is a large share of the Glue concurrent-job-runs quota, or when the CDC jobs' DSQL
    # connections could approach the cluster limit. (DSQL's 5-CDC-streams quota is OUTBOUND-only
    # and does NOT bound these inbound SQL-writer jobs — see scale-1b/CORRECTIONS.md.)
    n_ck = len(composite_entries)
    n_bg = len(bg_forks)
    total_cdc = 1 + n_ck + n_bg
    if total_cdc >= max(3, _GLUE_CONCURRENT_RUNS_QUOTA // 3):
        warnings.append(
            f"This task runs {total_cdc} always-on CDC jobs (1 main + {n_ck} ck + {n_bg} bg). "
            f"The AWS Glue default is ~{_GLUE_CONCURRENT_RUNS_QUOTA} concurrent job runs per "
            f"account (adjustable) — shared with load/validate and other tasks — and each CDC job "
            f"holds DSQL connections (cluster limit 10,000, rate 100/s). Watch the Glue "
            f"concurrent-run quota and DSQL connections; raise the Glue quota for large fleets.")

    for w in warnings:
        print(f"(warn) {w}")

    state_key = f"{config_prefix}/_orchestrator/state.json"
    s3.put_object(
        Bucket=bucket, Key=state_key,
        Body=json.dumps({
            "group_count": group_count,
            "fork_count": fork_count,
            "composite_fork_count": n_ck,
            "big_fork_count": n_bg,
            "cdcOwners": owners,
            "loaders_in_flight": loaders_in_flight,
            "writers_per_loader": writers_per_loader,
            "groups": out_groups,
            "forks": out_forks,
            "warnings": warnings,
        }, indent=2).encode("utf-8"),
        ContentType="application/json")

    return {
        "groups": out_groups,
        "group_count": group_count,
        "forks": out_forks,
        "fork_count": fork_count,
        "cdcOwners": owners,
        "bgOverflow": bg_overflow,
        "warnings": warnings,
        "loaders_in_flight": loaders_in_flight,
        "writers_per_loader": writers_per_loader,
        "state_key": state_key,
    }
