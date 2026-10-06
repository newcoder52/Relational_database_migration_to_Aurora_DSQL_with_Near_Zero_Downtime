"""
Shared parser for the pipeline parameters CSV (config/params.csv).

ONE place that turns the operator's `parameter,value` CSV into the pipeline.json dict every
`startup`/`cutover` run reads. Both the fleet preflight Lambda (lambdas/preflight_tasks.py) and
the one-time setup script (tools/setup.sh) call it, so the CSV is parsed and validated exactly
the same way wherever it is used, against resolve_task's own settings rules.

Why a CSV: so the whole end-to-end flow's "export" values (account, region, project, DSQL
endpoint/user/db, Glue connection, ...) live in one file next to fleet_tasks.csv in S3, and
nobody retypes an export block.

THE FILE
  s3://<bucket>/config/params.csv   (default name params.csv; the execution input
                                     "paramsFile" can point at a different name in config/)
  Header row:  parameter,value
  One row per key. Blank lines and lines whose first field starts with '#' are ignored.
  Every value is trimmed. A duplicate key or an unknown key is an error (the message lists the
  allowed keys). The BUCKET is deliberately NOT a key: it is the bucket the CSV itself lives in
  (the execution input "bucket"), so it can't disagree with where everything is read from.

KEYS
  Required (no default):
    account_id       12 digits
    region           AWS region, e.g. us-east-1
    project          short name prefix (letters, digits, hyphens)
    dsql_endpoint    Aurora DSQL endpoint, e.g. <cluster>.dsql.<region>.on.aws
  Optional, with defaults:
    dsql_user          = admin
    dsql_database      = postgres
    glue_connection    = ""                 (empty = Glue runs with no VPC connection)
    cdc_engine         = pythonshell
    cdc_spark_fallback = true
    control_schema     = cdc_control
    cdc_validation     = true                (Tier-2 CDC validation on; cutover blocks on failures)
    cdc_validation_sample = 20               (rows re-checked per committed CDC file; 0 = all)
    cdc_max_delete_fraction = 0.5            (G6 mass-delete guard: max fraction of a table one
                                              CDC file may delete; >= 1 disables)
    cdc_max_delete_rows = 100000             (G6: absolute delete floor; both must be crossed)
    cdc_drift_check_minutes = 30             (G9: minutes between DSQL-vs-expected count checks;
                                              0 = off)
    cdc_drift_tolerance = 0                  (G9: allowed row difference before drift fires)
    cdc_drift_action   = warn                (G9: warn | block on drift)
    guardrails_mode    = warn                (master switch: warn | strict. warn never fails a
                                              run for a guard's own bookkeeping; strict restores
                                              fail-closed. Per-guard keys below still override.)
    cdc_file_order_action = warn             (G8 order/gap/new-LOAD: warn | block)
    cdc_nopk_overmatch_action = warn         (G7 no-PK over-match: warn | block)
    validate_count_check = warn              (G10 validate vs DMS FullLoadRows: warn | strict)
    cutover_count_check  = warn              (G10 cutover count equation: warn | strict)
    glue_role_arn      = arn:aws:iam::<account_id>:role/<project>-glue-exec-role
  Setup-only (used by tools/setup.sh; NOT part of pipeline.json):
    subnet_id, security_group_id            both together, or neither (Glue network connection)
    lambda_role_arn    = arn:aws:iam::<account_id>:role/<project>-lambda-exec-role
    sfn_role_arn       = arn:aws:iam::<account_id>:role/<project>-sfn-exec-role
    manage_iam         = true                (true: setup creates/updates the 3 roles as today;
                                              false: setup only READS the 3 existing roles the
                                              customer's IAM team made and never writes IAM)
  Note on the role ARNs: each is validated as an IAM role ARN in account_id. A role PATH is
  allowed (arn:aws:iam::acct:role/some/path/name); the role NAME used for iam calls is the LAST
  segment of the ARN (role_name_from_arn). glue_role_arn stays in pipeline.json (create_glue_jobs
  reads it); lambda_role_arn / sfn_role_arn / manage_iam are setup-only, like subnet_id.
  Derived, never written in the CSV:
    dsql_cluster_id    = the first label of dsql_endpoint

PUBLIC API
  parse(text) -> {
      "params":   {key: value, ...},   # required + optional (defaults applied) + setup-only
      "errors":   [str, ...],          # every problem found, collected; empty == valid
      "warnings": [str, ...],
  }
  to_pipeline_settings(params) -> dict  # the exact pipeline.json dict resolve_task reads
      (only SETTINGS_KNOWN keys; validated with resolve_task._validate_settings; raises
       ParamsError on any problem, including a stray '<' or '>' in a value).

Neither function imports boto3; both are safe to run offline and in tests.
"""

import re

import resolve_task as rt   # same Lambda zip: reuse the per-task workflow's exact settings rules

HEADER = ("parameter", "value")

# Required keys (no default) and their human-facing validation.
REQUIRED = ("account_id", "region", "project", "dsql_endpoint")

# Optional keys with their defaults. glue_role_arn's default is derived from account_id/project
# at parse time (it names those placeholders), so it is handled separately below.
OPTIONAL_DEFAULTS = {
    "dsql_user": "admin",
    "dsql_database": "postgres",
    "glue_connection": "",
    "cdc_engine": "pythonshell",
    "cdc_spark_fallback": "true",
    "control_schema": "cdc_control",
    "cdc_validation": "true",
    "cdc_validation_sample": "20",
    # ── SAFETY GUARDRAILS (data-loss protection; see RUNBOOK "Safety guardrails"). ──
    # cdc_max_delete_fraction / cdc_max_delete_rows (G6): a CDC file (or one poll cycle) whose
    #   net DELETEs would remove MORE than this FRACTION of a table's current rows AND MORE than
    #   this ABSOLUTE count blocks the table and applies nothing from that file. Both thresholds
    #   must be crossed (so a tiny table isn't blocked by normal churn). Set the fraction to a
    #   value >= 1 to turn the guard off. Operator unblocks per-file with
    #   cdc_status.allow_mass_delete=true after verifying against the source.
    "cdc_max_delete_fraction": "0.5",
    "cdc_max_delete_rows": "100000",
    # cdc_drift_check_minutes / cdc_drift_tolerance / cdc_drift_action (G9): every N minutes the
    #   CDC job compares each table's live DSQL count with its tracked expectation
    #   (full_load_rows + inserts_applied − deletes_applied). A difference beyond the tolerance
    #   logs ERROR, writes cdc_control.audit_log, emits the DsqlRowDrift CloudWatch metric, and —
    #   when cdc_drift_action=block — sets the table 'blocked'. tolerance 0 = exact (PK tables);
    #   raise it slightly for no-PK tables. Set cdc_drift_check_minutes to 0 to turn the periodic
    #   check off.
    "cdc_drift_check_minutes": "30",
    "cdc_drift_tolerance": "0",
    "cdc_drift_action": "warn",
    # guardrails_mode (master switch): "warn" (default) or "strict".
    #   warn   — a guardrail may STOP a destructive action (G1 no-blank-after-CDC, G4 blank
    #            sanity, G6 mass-delete) but NEVER fails a run for its own bookkeeping: a missing
    #            permission, a missing control table, a lock it can't take, or a check it can't
    #            compute degrades to a WARNING and the run continues (other tables + all
    #            non-destructive work keep flowing). G2/G3/G5/G7/G8/G9/G10 are WARN by default.
    #   strict — restores fail-closed behaviour for operators who want it (G2 blocks a
    #            no-workflow blank, G3 refuses on a lock it can't take, G7/G8 block, G10 fails).
    #   Individual settings below still override per-guard regardless of the mode.
    "guardrails_mode": "warn",
    # cdc_file_order_action (G8 ordering/gap/new-LOAD-after-CDC): "warn" (default; log + metric,
    #   keep applying in order) or "block" (set the table 'blocked'). strict mode implies block.
    "cdc_file_order_action": "warn",
    # cdc_nopk_overmatch_action (G7 no-PK over-match precision): "warn" (default; apply and warn)
    #   or "block". strict mode implies block.
    "cdc_nopk_overmatch_action": "warn",
    # validate_count_check / cutover_count_check (G10): "warn" (default; DSQL-vs-DMS FullLoadRows
    #   mismatch logs a WARNING, validation/cutover still passes) or "strict" (mismatch fails /
    #   cutover refuses with CountMismatch). DMS counts can legitimately differ (e.g. the source
    #   changed during the load), so warn is the safe default. strict mode implies strict here.
    "validate_count_check": "warn",
    "cutover_count_check": "warn",
    "max_composite_forks": "8",
    "max_big_cdc_forks": "8",
    # Planning thresholds (plan_split fan-out knobs; defaults equal the former ASL literals).
    "big_table_row_threshold": "6000000",
    "file_fanout_threshold": "8",
    "big_table_bytes_threshold": "1000000000",
    "max_groups": "10",
    "map_max_concurrency": "6",
    "max_files_in_parallel": "30",
    "writers_per_file": "8",
    "conn_budget": "900",
    "min_writers_per_loader": "100",
    "max_writers_per_loader": "150",
    # Validation range size: rows per key-range for job3's per-column aggregate validation.
    # Lowered default (10000) so each range query returns under DSQL's 300s txn-age limit AND
    # the client socket read timeout on very large/wide tables; a range that still times out is
    # auto re-split smaller. Raise it for narrow tables to validate faster. (B14.)
    "validate_rows_per_range": "10000",
    # B18: validation throughput controls (no manual tuning needed up to ~1B rows).
    #   validate_parallelism               : concurrent per-range DSQL queries; blank/0 = auto
    #                                        (sized from the validate worker type/count, then
    #                                        hard-capped by conn_budget and DSQL's 10,000-conn
    #                                        cluster limit). This is the main throughput lever.
    #   validate_target_seconds_per_range  : adaptive sizer aims each range query at this many
    #                                        seconds (5-20s band), well under DSQL's 300s limit.
    #   validate_hash                      : per-value md5 scope — all | keys | off. 'all'
    #                                        (default) hashes every text/char/uuid/bytea column
    #                                        (md5 computed ONCE per value); 'keys' only key
    #                                        columns; 'off' uses count+length+min/max only.
    # validate_rows_per_range is now only the STARTING value / upper cap for the time sizer.
    "validate_parallelism": "0",
    "validate_target_seconds_per_range": "12",
    "validate_hash": "all",
    # Glue job SIZING (speed over cost — size up, do not throttle). worker types validated
    # against an allow-list; counts/timeouts validated as ints. The load runs DRIVER-SIDE, so a
    # bigger WORKER TYPE (= bigger driver) is what speeds a big table; executor COUNT only helps
    # the CSV read. Defaults: load-big + validate on G.8X (128 GB driver) for big tables, load on
    # G.4X (64 GB), discovery on G.2X. Timeouts raised to 48 h (max 7 days) for big tables.
    "glue_version": "4.0",
    "discovery_worker_type": "G.2X",
    "discovery_num_workers": "5",
    "discovery_timeout_minutes": "480",
    "load_worker_type": "G.4X",
    "load_num_workers": "10",
    "load_timeout_minutes": "2880",
    "load_big_worker_type": "G.8X",
    "load_big_num_workers": "10",
    "load_big_timeout_minutes": "2880",
    "validate_worker_type": "G.8X",
    "validate_num_workers": "10",
    "validate_timeout_minutes": "2880",
    # job2 driver-side parallelism tuning (how many tables load at once on the driver, and the
    # per-table driver-memory budget used to auto-size that). Raised so a big driver loads many
    # tables at once; the throttle can never silently drop to 1 (see job2_load).
    "max_parallel_tables": "20",
    "per_worker_mem_budget_mb": "1500",
}
# Setup-only keys: consumed by tools/setup.sh, never written to pipeline.json.
#   subnet_id / security_group_id : Glue network connection (both-or-neither).
#   manage_iam                    : "true" (default) = setup creates/updates the 3 roles as it
#                                   always has; "false" = setup only READS the 3 roles the
#                                   customer already has and never makes an IAM write call.
# lambda_role_arn / sfn_role_arn are ALSO setup-only but, like glue_role_arn, have a derived
# default (account_id + project), so they are listed in _DERIVED_ROLE_ARNS below (not here) and
# get a default filled in at parse time.
SETUP_ONLY = ("subnet_id", "security_group_id", "manage_iam",
              "lambda_role_arn", "sfn_role_arn")

# manage_iam's static default (the role ARNs' defaults are derived from account_id/project).
_SETUP_ONLY_DEFAULTS = {
    "manage_iam": "true",
}

# glue_role_arn is optional-with-a-derived-default; list it so it is an allowed (not "unknown")
# key and so to_pipeline_settings carries it through. lambda_role_arn / sfn_role_arn are the same
# idea but setup-only (not in PIPELINE_KEYS).
_DERIVED_DEFAULT = ("glue_role_arn",)

# The three role ARNs that take a derived default of
# arn:aws:iam::<account_id>:role/<project>-<suffix>-exec-role when the operator omits them.
# (key, role-name suffix). glue_role_arn is also in _DERIVED_DEFAULT / PIPELINE_KEYS.
_DERIVED_ROLE_ARNS = (
    ("glue_role_arn", "glue"),
    ("lambda_role_arn", "lambda"),
    ("sfn_role_arn", "sfn"),
)

ALLOWED = (tuple(REQUIRED) + tuple(OPTIONAL_DEFAULTS) + _DERIVED_DEFAULT + SETUP_ONLY
           + ("lambda_role_arn", "sfn_role_arn"))
# De-duplicate while keeping order deterministic (lambda_role_arn / sfn_role_arn appear in both
# SETUP_ONLY and the explicit tuple above only for readability).
ALLOWED = tuple(dict.fromkeys(ALLOWED))

# The subset of params that becomes pipeline.json. Mirrors resolve_task.SETTINGS_KNOWN minus the
# free-text "description"/"settings_version" (which the CSV never carries). account_id and the
# setup-only keys are intentionally absent.
PIPELINE_KEYS = ("project", "region", "dsql_endpoint", "dsql_user", "dsql_database",
                 "glue_role_arn", "glue_connection", "cdc_engine", "cdc_spark_fallback",
                 "control_schema", "cdc_validation", "cdc_validation_sample",
                 "cdc_max_delete_fraction", "cdc_max_delete_rows",
                 "cdc_drift_check_minutes", "cdc_drift_tolerance", "cdc_drift_action",
                 "guardrails_mode", "cdc_file_order_action", "cdc_nopk_overmatch_action",
                 "validate_count_check", "cutover_count_check",
                 "max_composite_forks", "max_big_cdc_forks",
                 "big_table_row_threshold", "file_fanout_threshold", "big_table_bytes_threshold",
                 "max_groups", "map_max_concurrency", "max_files_in_parallel",
                 "writers_per_file", "conn_budget",
                 "min_writers_per_loader", "max_writers_per_loader",
                 "validate_rows_per_range",
                 "validate_parallelism", "validate_target_seconds_per_range", "validate_hash",
                 "glue_version",
                 "discovery_worker_type", "discovery_num_workers", "discovery_timeout_minutes",
                 "load_worker_type", "load_num_workers", "load_timeout_minutes",
                 "load_big_worker_type", "load_big_num_workers", "load_big_timeout_minutes",
                 "validate_worker_type", "validate_num_workers", "validate_timeout_minutes",
                 "max_parallel_tables", "per_worker_mem_budget_mb")

_ACCOUNT_RE = re.compile(r"^\d{12}$")

# An IAM role ARN: arn:aws[partition]:iam::<12-digit account>:role/<path.../><name>. The capture
# group is everything after "role/", which may contain a path (segments separated by '/'); the
# role NAME for iam CLI calls is the LAST '/'-separated segment of that. Matches the same shape
# resolve_task._validate_settings enforces for glue_role_arn.
_ROLE_ARN_RE = re.compile(r"^arn:aws[a-z-]*:iam::(\d{12}):role/(.+)$")


def role_name_from_arn(arn):
    """The IAM role NAME (last path segment) from a role ARN, for iam get-role/put-role-policy.
    arn:aws:iam::123456789012:role/team/path/my-glue-role -> 'my-glue-role'. Returns "" if arn is
    not a role ARN. Used by tools/setup.sh so a role given with a PATH still resolves to its name."""
    m = _ROLE_ARN_RE.match(str(arn or "").strip())
    if not m:
        return ""
    return m.group(2).rsplit("/", 1)[-1]


def role_arn_account(arn):
    """The 12-digit account id embedded in a role ARN, or "" if arn is not a role ARN."""
    m = _ROLE_ARN_RE.match(str(arn or "").strip())
    return m.group(1) if m else ""

# Integer params with (lower, upper) bounds. upper=None means unbounded above. These mirror the
# ranges resolve_task._validate_settings enforces, so parse() can collect the same problems the
# pipeline would otherwise only raise at build time. map_max_concurrency caps at 40 (the Step
# Functions Map concurrency plan_split's loader concurrency also feeds).
_PLANNING_INT_KEYS = (
    ("cdc_validation_sample", 0, None),
    ("cdc_max_delete_rows", 0, None),
    ("cdc_drift_check_minutes", 0, None),
    ("max_composite_forks", 1, None),
    ("max_big_cdc_forks", 1, None),
    ("big_table_row_threshold", 1, None),
    ("file_fanout_threshold", 1, None),
    ("big_table_bytes_threshold", 1, None),
    ("max_groups", 1, None),
    ("map_max_concurrency", 1, 40),
    ("max_files_in_parallel", 1, None),
    ("writers_per_file", 1, None),
    ("conn_budget", 1, None),
    ("min_writers_per_loader", 1, None),
    ("max_writers_per_loader", 1, None),
    ("validate_rows_per_range", 1, None),
    # B18 throughput controls. validate_parallelism 0 = auto-size; else a concrete cap (<=10000,
    # the DSQL cluster connection limit). validate_target_seconds_per_range 1..120 (kept well
    # under DSQL's 300s txn-age limit). validate_hash is an enum, validated in resolve_task.
    ("validate_parallelism", 0, 10000),
    ("validate_target_seconds_per_range", 1, 120),
    # Job sizing counts/timeouts (worker TYPES are validated by resolve_task against an
    # allow-list). num_workers capped at 299 (a per-job sanity cap; the real limit is the
    # account DPU quota, raised in Service Quotas). timeouts 1..10080 min = up to Glue's 7-day
    # max. max_parallel_tables 1..40; per_worker_mem_budget_mb >= 1.
    ("discovery_num_workers", 1, 299),
    ("discovery_timeout_minutes", 1, 10080),
    ("load_num_workers", 1, 299),
    ("load_timeout_minutes", 1, 10080),
    ("load_big_num_workers", 1, 299),
    ("load_big_timeout_minutes", 1, 10080),
    ("validate_num_workers", 1, 299),
    ("validate_timeout_minutes", 1, 10080),
    ("max_parallel_tables", 1, 40),
    ("per_worker_mem_budget_mb", 1, None),
)


class ParamsError(Exception):
    pass


def _allowed_list():
    return ", ".join(sorted(ALLOWED))


def dsql_cluster_id(dsql_endpoint):
    """Derived value: the first label of the DSQL endpoint (never a CSV key)."""
    return str(dsql_endpoint or "").strip().split(".", 1)[0]


def parse(text):
    """Parse the params CSV text. Returns {"params", "errors", "warnings"}; errors are collected
    rather than raised so the caller can show every problem at once."""
    errors, warnings = [], []
    raw = {}            # key -> trimmed value, as read (before defaults)
    seen_lines = {}     # key -> first line number it appeared on (for duplicate messages)

    lines = (text or "").splitlines()

    # Locate the header: the first non-blank, non-comment line. Everything before it must be
    # blank/comment (we already skip those), so a leading comment block is fine.
    header_idx = None
    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        header_idx = i
        cols = [c.strip().lower() for c in _split_row(line)]
        if tuple(cols[:2]) != HEADER:
            errors.append(f"first non-comment line must be the header 'parameter,value' "
                          f"(got {line.strip()!r}).")
        break
    if header_idx is None:
        errors.append("params.csv is empty: expected a 'parameter,value' header and one row "
                      f"per key. Allowed keys: {_allowed_list()}.")
        return {"params": {}, "errors": errors, "warnings": warnings}

    for i in range(header_idx + 1, len(lines)):
        line = lines[i]
        s = line.strip()
        if not s:
            continue
        fields = _split_row(line)
        if fields and str(fields[0]).strip().startswith("#"):
            continue  # comment line
        key = str(fields[0]).strip() if fields else ""
        value = str(fields[1]).strip() if len(fields) > 1 else ""
        lineno = i + 1
        if not key:
            errors.append(f"line {lineno}: no parameter name.")
            continue
        if key not in ALLOWED:
            errors.append(f"line {lineno}: unknown key {key!r}. Allowed keys: {_allowed_list()}.")
            continue
        if key in raw:
            errors.append(f"line {lineno}: duplicate key {key!r} (already set on line "
                          f"{seen_lines[key]}).")
            continue
        raw[key] = value
        seen_lines[key] = lineno

    # Required keys present and non-empty.
    for k in REQUIRED:
        if not str(raw.get(k) or "").strip():
            errors.append(f"missing required key {k!r}. Allowed keys: {_allowed_list()}.")

    # account_id must be 12 digits (checked before it is used to derive glue_role_arn).
    acct = str(raw.get("account_id") or "").strip()
    if acct and not _ACCOUNT_RE.match(acct):
        errors.append(f"account_id must be 12 digits (got {acct!r}).")

    # subnet_id / security_group_id: both or neither.
    have_subnet = bool(str(raw.get("subnet_id") or "").strip())
    have_sg = bool(str(raw.get("security_group_id") or "").strip())
    if have_subnet != have_sg:
        errors.append("subnet_id and security_group_id must be set together or not at all "
                      "(set both for a Glue VPC connection, or neither).")

    # Planning/fork integer keys the operator set must be whole numbers in range (collected here
    # so every problem shows at once, like the checks above; the SAME rules are enforced
    # authoritatively by resolve_task._validate_settings when the pipeline.json is read/built).
    # Only values actually present in the CSV are checked — omitted keys take known-good defaults.
    ints = {}
    for key, lo, hi in _PLANNING_INT_KEYS:
        if key not in raw:
            continue
        s = str(raw[key]).strip()
        if not re.fullmatch(r"-?\d+", s):
            errors.append(f"{key} must be a whole number (got {raw[key]!r}).")
            continue
        n = int(s)
        if n < lo or (hi is not None and n > hi):
            rng = f">= {lo}" if hi is None else f"between {lo} and {hi}"
            errors.append(f"{key} must be {rng} (got {n}).")
            continue
        ints[key] = n
    # Guardrail float keys (G6/G9): non-negative decimals. Only checked when present in the CSV.
    #   cdc_max_delete_fraction : fraction of a table's rows a single file may delete before the
    #                             mass-delete guard trips (>= 1 disables it).
    #   cdc_drift_tolerance     : allowed |live − expected| row difference before drift fires.
    for key in ("cdc_max_delete_fraction", "cdc_drift_tolerance"):
        if key not in raw:
            continue
        s = str(raw[key]).strip()
        try:
            f = float(s)
        except (TypeError, ValueError):
            errors.append(f"{key} must be a non-negative number (got {raw[key]!r}).")
            continue
        if f < 0:
            errors.append(f"{key} must be >= 0 (got {f}).")
    # Guardrail enum key (G9): cdc_drift_action is 'warn' or 'block'.
    if "cdc_drift_action" in raw:
        _da = str(raw["cdc_drift_action"]).strip().lower()
        if _da not in ("warn", "block"):
            errors.append(f"cdc_drift_action must be 'warn' or 'block' (got "
                          f"{raw['cdc_drift_action']!r}).")
    # Guardrail enum keys (ease-guardrails):
    #   guardrails_mode               : warn | strict   (master switch)
    #   cdc_file_order_action (G8)    : warn | block
    #   cdc_nopk_overmatch_action(G7) : warn | block
    #   validate_count_check (G10)    : warn | strict
    #   cutover_count_check (G10)     : warn | strict
    for _k, _allowed in (("guardrails_mode", ("warn", "strict")),
                         ("cdc_file_order_action", ("warn", "block")),
                         ("cdc_nopk_overmatch_action", ("warn", "block")),
                         ("validate_count_check", ("warn", "strict")),
                         ("cutover_count_check", ("warn", "strict"))):
        if _k in raw:
            _v = str(raw[_k]).strip().lower()
            if _v not in _allowed:
                errors.append(f"{_k} must be one of {_allowed} (got {raw[_k]!r}).")
    # min_writers_per_loader <= max_writers_per_loader (only when both parsed cleanly).
    if "min_writers_per_loader" in ints and "max_writers_per_loader" in ints:
        if ints["min_writers_per_loader"] > ints["max_writers_per_loader"]:
            errors.append(f"min_writers_per_loader ({ints['min_writers_per_loader']}) must be "
                          f"<= max_writers_per_loader ({ints['max_writers_per_loader']}).")

    # Assemble the full params dict: defaults first, then the operator's values.
    params = dict(OPTIONAL_DEFAULTS)
    params.update(_SETUP_ONLY_DEFAULTS)
    params.update(raw)
    # The three role ARNs default to arn:aws:iam::<account_id>:role/<project>-<svc>-exec-role when
    # not given (glue stays in pipeline.json; lambda/sfn are setup-only). Only derive when
    # account_id/project are valid so we never emit a half-filled ARN.
    proj = str(params.get("project") or "").strip()
    acct_ok = bool(acct and _ACCOUNT_RE.match(acct))
    for key, svc in _DERIVED_ROLE_ARNS:
        if not str(params.get(key) or "").strip():
            if acct_ok and proj:
                params[key] = f"arn:aws:iam::{acct}:role/{proj}-{svc}-exec-role"

    # Validate each role ARN the operator SET (an omitted one took the derived default above and
    # is trusted). Each must be an IAM role ARN whose embedded account matches account_id — a role
    # in another account can never be assumed by this account's services. A role PATH is allowed;
    # role_name_from_arn pulls the last segment for the iam calls.
    for key, _svc in _DERIVED_ROLE_ARNS:
        if key not in raw:
            continue
        val = str(raw[key]).strip()
        if not _ROLE_ARN_RE.match(val):
            errors.append(f"{key} must be an IAM role ARN "
                          f"(arn:aws:iam::<account_id>:role/<name>; got {val!r}).")
            continue
        arn_acct = role_arn_account(val)
        if acct and _ACCOUNT_RE.match(acct) and arn_acct != acct:
            errors.append(f"{key} is in account {arn_acct} but account_id is {acct}; the role "
                          f"must be in the same account (got {val!r}).")

    # manage_iam must be a boolean literal ("true"/"false", case-insensitive). It is setup-only;
    # resolve_task never sees it. Normalise to the lowercase string.
    mi_raw = str(params.get("manage_iam") or "").strip().lower()
    if mi_raw not in ("true", "false"):
        errors.append(f"manage_iam must be true or false (got "
                      f"{params.get('manage_iam')!r}).")
    else:
        params["manage_iam"] = mi_raw

    return {"params": params, "errors": errors, "warnings": warnings}


def to_pipeline_settings(params):
    """Build the pipeline.json dict resolve_task reads from a parsed params mapping, and validate
    it with resolve_task's own rules. Only SETTINGS_KNOWN keys are emitted; account_id and the
    setup-only keys are dropped. Raises ParamsError on any problem."""
    # Keep only the pipeline keys, trimming strings. A missing optional key falls back to its
    # default so the output is always complete and deterministic.
    cfg = {}
    for k in PIPELINE_KEYS:
        v = params.get(k)
        if v is None:
            v = OPTIONAL_DEFAULTS.get(k, "")
        cfg[k] = v.strip() if isinstance(v, str) else v

    # Required-for-pipeline keys must be present (project/region/dsql_endpoint/glue_role_arn —
    # the four resolve_task itself requires). account_id-derived glue_role_arn may be empty if
    # account_id/project were missing; surface that here rather than letting _validate_settings
    # emit a confusing "not an IAM role ARN" for an empty string.
    missing = [k for k in rt.SETTINGS_REQUIRED if not str(cfg.get(k) or "").strip()]
    if missing:
        raise ParamsError(f"cannot build pipeline settings: missing {sorted(missing)}. "
                          f"Fix params.csv (account_id and project are needed to derive "
                          f"glue_role_arn if you don't set it).")

    # No free-text value may contain '<' or '>' (the same placeholder guard resolve_task applies
    # when it reads pipeline.json; a stray angle bracket would fail every run at ResolveFailed).
    for k, v in cfg.items():
        if isinstance(v, str) and ("<" in v or ">" in v):
            raise ParamsError(f"params.csv value for {k!r} still contains a placeholder "
                              f"({v!r}); replace '<...>' with the real value.")

    # Validate/normalise with resolve_task's exact rules. _validate_settings expects defaults
    # already applied and required keys present (both true here); it raises SettingsError.
    warnings = []
    try:
        cfg = rt._validate_settings(dict(cfg), warnings)
    except rt.SettingsError as e:
        raise ParamsError(str(e))
    return cfg


def _split_row(line):
    """Split one CSV line into fields. Uses the stdlib csv reader so quoted values with commas
    work, falling back to a plain split if the line can't be parsed."""
    import csv
    import io
    try:
        for row in csv.reader(io.StringIO(line)):
            return row
    except Exception:
        pass
    return line.split(",")
