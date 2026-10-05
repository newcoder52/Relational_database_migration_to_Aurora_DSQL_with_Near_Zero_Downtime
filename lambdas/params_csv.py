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
    glue_role_arn      = arn:aws:iam::<account_id>:role/<project>-glue-exec-role
  Setup-only (used by tools/setup.sh for the Glue network connection; NOT part of pipeline.json):
    subnet_id, security_group_id            both together, or neither
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
    "max_composite_forks": "8",
    "max_big_cdc_forks": "8",
    # Planning thresholds (plan_split fan-out knobs; defaults equal the former ASL literals).
    "big_table_row_threshold": "6000000",
    "file_fanout_threshold": "8",
    "max_groups": "10",
    "map_max_concurrency": "6",
    "max_files_in_parallel": "30",
    "conn_budget": "900",
    "min_writers_per_loader": "100",
    "max_writers_per_loader": "150",
}
# Setup-only keys: consumed by tools/setup.sh (Glue network connection), never in pipeline.json.
SETUP_ONLY = ("subnet_id", "security_group_id")

# glue_role_arn is optional-with-a-derived-default; list it so it is an allowed (not "unknown")
# key and so to_pipeline_settings carries it through.
_DERIVED_DEFAULT = ("glue_role_arn",)

ALLOWED = tuple(REQUIRED) + tuple(OPTIONAL_DEFAULTS) + _DERIVED_DEFAULT + SETUP_ONLY

# The subset of params that becomes pipeline.json. Mirrors resolve_task.SETTINGS_KNOWN minus the
# free-text "description"/"settings_version" (which the CSV never carries). account_id and the
# setup-only keys are intentionally absent.
PIPELINE_KEYS = ("project", "region", "dsql_endpoint", "dsql_user", "dsql_database",
                 "glue_role_arn", "glue_connection", "cdc_engine", "cdc_spark_fallback",
                 "control_schema", "cdc_validation", "cdc_validation_sample",
                 "max_composite_forks", "max_big_cdc_forks",
                 "big_table_row_threshold", "file_fanout_threshold", "max_groups",
                 "map_max_concurrency", "max_files_in_parallel", "conn_budget",
                 "min_writers_per_loader", "max_writers_per_loader")

_ACCOUNT_RE = re.compile(r"^\d{12}$")

# Integer params with (lower, upper) bounds. upper=None means unbounded above. These mirror the
# ranges resolve_task._validate_settings enforces, so parse() can collect the same problems the
# pipeline would otherwise only raise at build time. map_max_concurrency caps at 40 (the Step
# Functions Map concurrency plan_split's loader concurrency also feeds).
_PLANNING_INT_KEYS = (
    ("cdc_validation_sample", 0, None),
    ("max_composite_forks", 1, None),
    ("max_big_cdc_forks", 1, None),
    ("big_table_row_threshold", 1, None),
    ("file_fanout_threshold", 1, None),
    ("max_groups", 1, None),
    ("map_max_concurrency", 1, 40),
    ("max_files_in_parallel", 1, None),
    ("conn_budget", 1, None),
    ("min_writers_per_loader", 1, None),
    ("max_writers_per_loader", 1, None),
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
    # min_writers_per_loader <= max_writers_per_loader (only when both parsed cleanly).
    if "min_writers_per_loader" in ints and "max_writers_per_loader" in ints:
        if ints["min_writers_per_loader"] > ints["max_writers_per_loader"]:
            errors.append(f"min_writers_per_loader ({ints['min_writers_per_loader']}) must be "
                          f"<= max_writers_per_loader ({ints['max_writers_per_loader']}).")

    # Assemble the full params dict: defaults first, then the operator's values.
    params = dict(OPTIONAL_DEFAULTS)
    params.update(raw)
    # glue_role_arn default is derived from account_id + project when not given.
    if not str(params.get("glue_role_arn") or "").strip():
        proj = str(params.get("project") or "").strip()
        if acct and _ACCOUNT_RE.match(acct) and proj:
            params["glue_role_arn"] = f"arn:aws:iam::{acct}:role/{proj}-glue-exec-role"

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
