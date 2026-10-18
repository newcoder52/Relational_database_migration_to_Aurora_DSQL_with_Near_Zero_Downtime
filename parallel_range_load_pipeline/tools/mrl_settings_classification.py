#!/usr/bin/env python3
"""mrl_settings_classification.py - the SINGLE source of truth for how every config/params.csv
key (PIPELINE_KEYS + setup-only + required) is treated by the MRL route (Amendment 9.3). The
README settings-source table and tests/test_params_classification.py are checked against THIS
data so they cannot drift. Each row: key -> (class, where/reason).

classes:
  USED            - MRL reads it and applies it (as a Glue job property or a Glue job argument).
  PASSED_THROUGH  - forwarded to the count job or (cdc=true) the CDC jobs via the reused states.
  IGNORED_HERE    - accepted (never rejected - the file is shared with the original pipeline) but
                    not used by MRL, with the reason.
"""

USED = "USED"
PASSED_THROUGH = "PASSED_THROUGH"
IGNORED_HERE = "IGNORED_HERE"

# One row per key. (class, detail)
CLASSIFICATION = {
    # --- required / identity ---
    "account_id": (USED, "identity; builds Glue job ARNs and the DSQL cluster resource"),
    "region": (USED, "AWS region for every DMS/Glue/S3 call"),
    "project": (USED, "name prefix of the MRL Glue jobs and state machines"),
    "dsql_endpoint": (USED, "DSQL endpoint for the range-load + count jobs (--dsql_endpoint)"),
    "dsql_user": (USED, "range-load + count job arg --dsql_user"),
    "dsql_database": (USED, "range-load + count job arg --dsql_database"),
    "glue_role_arn": (USED, "Role on every MRL Glue job (range-load, count, reused discovery/CDC)"),
    "glue_connection": (USED, "VPC Connections on every MRL Glue job"),
    "control_schema": (PASSED_THROUGH, "reused CDC jobs arg --control_schema (cdc=true only)"),
    "null_values": (USED, "range-load + count job arg --null_values (NULL handling)"),
    "null_rules": (USED, "range-load + count job arg --null_rules (NULL handling)"),
    # --- concurrency / sizing: Amendment 10 A.1 - MRL no longer reads these from params.csv.
    #     They are still VALID keys here (the shared parser accepts/validates them exactly like
    #     the original), but MRL takes sizing/parallelism ONLY from the fleet row / execution
    #     input / built-in default (mrl_defaults). One table per DMS task, so no load-big path. ---
    "map_max_concurrency": (IGNORED_HERE, "A10: parallelism comes from the fleet row / exec input "
                            "/ built-in default (mrl_defaults.MAX_CONCURRENCY=3), not params.csv"),
    "max_files_in_parallel": (IGNORED_HERE, "A10: range-load fan-out from fleet row / exec input / "
                              "built-in default (mrl_defaults.MAX_FILES_IN_PARALLEL=30)"),
    "writers_per_file": (IGNORED_HERE, "A10: from fleet row / exec input / built-in default "
                         "(mrl_defaults.WRITERS_PER_FILE=8)"),
    "conn_budget": (USED, "capacity arithmetic + the count job's --conn_budget cap"),
    "load_big_worker_type": (IGNORED_HERE, "A10: one table per task, no load-big; MRL sizes its "
                             "own range-load job from fleet row / built-in default"),
    "load_big_num_workers": (IGNORED_HERE, "A10: no load-big path (one table per task)"),
    "load_big_timeout_minutes": (IGNORED_HERE, "A10: no load-big path (one table per task)"),
    "glue_version": (USED, "GlueVersion of the MRL Glue jobs"),
    "load_worker_type": (IGNORED_HERE, "A10: range-load WorkerType from fleet row / exec input / "
                         "built-in default (mrl_defaults.LOAD_WORKER_TYPE=G.4X)"),
    "load_num_workers": (IGNORED_HERE, "A10: from fleet row / built-in default "
                         "(mrl_defaults.LOAD_NUM_WORKERS=10)"),
    "load_timeout_minutes": (IGNORED_HERE, "A10: from built-in default "
                             "(mrl_defaults.LOAD_TIMEOUT_MINUTES=2880)"),
    # --- count job sizing: Amendment 10 A.1 - built-in default, not params.csv ---
    "validate_worker_type": (IGNORED_HERE, "A10: count job sized from built-in default "
                             "(mrl_defaults.COUNT_WORKER_TYPE=G.8X)"),
    "validate_num_workers": (IGNORED_HERE, "A10: count job from built-in default "
                             "(mrl_defaults.COUNT_NUM_WORKERS=10)"),
    "validate_timeout_minutes": (IGNORED_HERE, "A10: count job from built-in default "
                                 "(mrl_defaults.COUNT_TIMEOUT_MINUTES=2880)"),
    # --- discovery (reused job1) ---
    "discovery_worker_type": (PASSED_THROUGH, "reused CreateGlueJobs -> discovery job WorkerType"),
    "discovery_num_workers": (PASSED_THROUGH, "reused CreateGlueJobs -> discovery job workers"),
    "discovery_timeout_minutes": (PASSED_THROUGH, "reused CreateGlueJobs -> discovery timeout"),
    # --- CDC-only (cdc=true): reused create_glue_jobs / plan_split / CDC scripts ---
    "cdc_engine": (PASSED_THROUGH, "reused CreateGlueJobs CDC engine selection (cdc=true)"),
    "cdc_spark_fallback": (PASSED_THROUGH, "reused CDC Spark fallback (cdc=true)"),
    "cdc_validation": (PASSED_THROUGH, "reused CDC jobs arg (cdc=true)"),
    "cdc_validation_sample": (PASSED_THROUGH, "reused CDC jobs arg (cdc=true)"),
    "cdc_max_delete_fraction": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "cdc_max_delete_rows": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "cdc_drift_check_minutes": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "cdc_drift_tolerance": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "cdc_drift_action": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "guardrails_mode": (PASSED_THROUGH, "reused CDC/validate guardrail master (cdc=true)"),
    "cdc_file_order_action": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "cdc_nopk_overmatch_action": (PASSED_THROUGH, "reused CDC guardrail (cdc=true)"),
    "max_composite_forks": (PASSED_THROUGH, "reused PlanSplit CDC ownership cap (cdc=true)"),
    "max_big_cdc_forks": (PASSED_THROUGH, "reused PlanSplit CDC ownership cap (cdc=true)"),
    "big_table_row_threshold": (PASSED_THROUGH, "reused PlanSplit big-table classification "
                                "(CDC ownership only, cdc=true)"),
    "file_fanout_threshold": (PASSED_THROUGH, "reused PlanSplit big-table classification (cdc=true)"),
    "big_table_bytes_threshold": (PASSED_THROUGH, "reused PlanSplit big-table classification (cdc=true)"),
    "max_groups": (PASSED_THROUGH, "reused PlanSplit (CDC ownership only; MRL ignores load groups)"),
    "min_writers_per_loader": (PASSED_THROUGH, "reused PlanSplit writer sizing (cdc=true)"),
    "max_writers_per_loader": (PASSED_THROUGH, "reused PlanSplit writer sizing (cdc=true)"),
    # --- IGNORED HERE (validation tuning + original load-group planning) ---
    "validate_rows_per_range": (IGNORED_HERE, "validation range sizing; MRL verifies by COUNT(*), "
                                "not the per-range aggregate validation"),
    "validate_parallelism": (IGNORED_HERE, "validation throughput tuning; not used by the count job"),
    "validate_target_seconds_per_range": (IGNORED_HERE, "validation range time sizing; unused here"),
    "validate_hash": (IGNORED_HERE, "per-value hash scope for the original validation; unused here"),
    "validate_count_check": (IGNORED_HERE, "the original validate's DMS-count gate; MRL's count "
                             "job has its own count_timeout_action + tolerance"),
    "cutover_count_check": (IGNORED_HERE, "original cutover count gate; not part of the MRL load"),
    "max_parallel_tables": (IGNORED_HERE, "original job2 how-many-tables-at-once; MRL loads one "
                            "table's ranges with its own max_concurrency (one table per task)"),
    "per_worker_mem_budget_mb": (IGNORED_HERE, "original job2 driver-memory auto-throttle; MRL "
                                 "sizes the range-load job by worker type/count directly"),
    # --- setup-only (consumed by tools/setup.sh, not pipeline settings) ---
    "subnet_id": (IGNORED_HERE, "setup-only (Glue VPC connection); not an MRL runtime setting"),
    "security_group_id": (IGNORED_HERE, "setup-only (Glue VPC connection)"),
    "manage_iam": (IGNORED_HERE, "setup-only (IAM management mode)"),
    "lambda_role_arn": (IGNORED_HERE, "setup-only (Lambda role; MRL runs under the existing roles)"),
    "sfn_role_arn": (IGNORED_HERE, "setup-only (Step Functions role)"),
}


def markdown_table():
    """Render the classification as a Markdown table (used to generate/verify the README)."""
    lines = ["| params.csv key | class | where / reason |", "|---|---|---|"]
    for k in sorted(CLASSIFICATION):
        cls, detail = CLASSIFICATION[k]
        lines.append("| `%s` | %s | %s |" % (k, cls.replace("_", " "), detail))
    return "\n".join(lines)


if __name__ == "__main__":
    print(markdown_table())
