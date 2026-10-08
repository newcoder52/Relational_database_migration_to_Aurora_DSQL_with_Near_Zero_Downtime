# RESULT — override full-bypass + cutover warn + undo M-10 / M-19

## Commit

- **SHA:** `4407893f363424dbe53b8828ce2b4abd79498f25` (`4407893`)
- **Parent:** `d8a749353ce8379077a346817fc91fc0f5424c47` (`d8a7493`)
- **Branch:** `main` (pushed, plain push, no force)
- **Author:** newcoder52

## The 4 changes

1. **Cutover — remove the `StartupOverrideRequiresOverride` refusal.** A cutover started
   **without** override on a task whose startup used override now **PROCEEDS**. It logs a
   warning (ASL `LogStartupOverrideWarning` Pass) and `resolve_task` (cutover mode) appends the
   same warning — naming the startup-override record and the overridden tables — to the cutover
   output `$.resolved.warnings`. The refusal `Fail` state is removed (no orphan). The safety
   ordering (DMS stop → drain → stop CDC runs → delete jobs) is unchanged. ASL path audit and
   payload contract stay green.
2. **Override is a FULL BYPASS of load AND validation failures.** With `override=true`, a group
   whose **load** failed (for any reason — `data_error` *or* `infra`) **or** whose **validation**
   failed continues: `MarkOverrideActive → ResumeDmsToCdc → StartCdcJob → CDC`, ending in
   `TaskSucceededWithOverride`. **No** data-error carve-out and **no** DSQL-vs-DMS count check.
   `job2_load` still classifies and records the per-table failure **kind**
   (`data_error` with `table`/`column`/`file`/`reason`, vs `infra`; unknown → `infra`) in the
   group's `_load_status.json` **before** raising, so the override record and run output carry
   those as **warnings** (the kind feeds the warnings now and a future, separate auto-recovery
   feature later). **No-override behaviour is byte-identical:** a load or validation failure
   stops at `GroupsFailed`.
3. **Undo the M-10 per-table parallelism split.** When `validate_parallelism > 0` is set
   explicitly, each table uses exactly that value (no division by `max_parallel_tables`). The
   auto path (`0`/blank → `VALIDATE_PARALLELISM is None`) stays budget-based (divided across
   `MAX_PARALLEL_TABLES`).
4. **Undo M-19.** `params_csv` no longer rejects an invalid `validate_hash`; it **warns** and
   **falls back to the default** (`all`), never failing the fleet start.

## Runtime files changed (this commit `4407893`)

| File | Deploy target |
|---|---|
| `scripts/job2_load.py` | S3 Glue script `s3://<bucket>/scripts/job2_load.py` |
| `scripts/job3_validate.py` | S3 Glue script `s3://<bucket>/scripts/job3_validate.py` |
| `lambdas/resolve_task.py` | Lambda zip (function `<project>-resolve-task`) |
| `lambdas/params_csv.py` | Lambda zip (used by `<project>-preflight-tasks`; also by `tools/setup.sh` to publish `pipeline.json`) |
| `stepfunctions/startup.asl.json` | State machine `<project>-startup` |
| `stepfunctions/cutover.asl.json` | State machine `<project>-cutover` |

Docs updated: `RUNBOOK.md` (§8 override), `docs/FLEET_LAUNCHER.md`.
Tests updated: `tests/test_override.py`, `tests/test_validate_merge_fixes.py`.

### Also in the parent `d8a7493` (included so the customer does ONE redeploy)

| File | Deploy target |
|---|---|
| `scripts/job3_validate.py` | S3 Glue script (same file; this commit also edits it — one upload covers both) |
| `lambdas/params_csv.py` | Lambda zip (same file; one zip covers both) |

**Net combined runtime surface for ONE redeploy covering `d8a7493` + `4407893`:**
- **S3 Glue scripts:** `scripts/job2_load.py`, `scripts/job3_validate.py`
- **Lambda zip (ALL functions):** rebuilt from `lambdas/*.py` (the changes are in
  `resolve_task.py` + `params_csv.py`; the zip is shared, so **every** function is updated)
- **State machines:** `<project>-startup`, `<project>-cutover` (placeholder-filled)

---

## Exact customer redeploy commands (ONE redeploy = `d8a7493` + `4407893`)

> Simplest: run the pipeline's own installer, which is create-or-update and re-uploads scripts,
> rebuilds the Lambda zip (all functions), and re-fills + updates all 4 state machines:
>
> ```bash
> tools/setup.sh "s3://<bucket>/config/params.csv"      # add --with-drivers only if wheels changed (they didn't)
> ```
>
> The per-component commands below are the exact steps `tools/setup.sh` runs, for a targeted
> redeploy of just the changed surface. Set these first:

```bash
export AWS_PAGER=""
PROJECT="<project>"; REGION="<region>"; ACCOUNT_ID="<account_id>"; BUCKET="<bucket>"
LAMBDA_BASE="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT"
SM_BASE="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role"   # or your customer-named sfn role
```

### 1. S3 Glue scripts (`job2_load.py` + `job3_validate.py`)

```bash
aws s3 cp scripts/job2_load.py    "s3://$BUCKET/scripts/job2_load.py"
aws s3 cp scripts/job3_validate.py "s3://$BUCKET/scripts/job3_validate.py"
```

(Glue jobs read the script from S3 on their next run; no job re-create needed. Uploading all 5 is
also fine: `for f in job1_discovery job2_load job3_validate glue_cdc_continuous glue_cdc_composite; do aws s3 cp "scripts/$f.py" "s3://$BUCKET/scripts/$f.py"; done`.)

### 2. Lambda zip — rebuild once, update ALL functions

The changed Lambda code (`resolve_task.py`, `params_csv.py`) ships in the one shared `fn.zip`, so
rebuild it and update every function (the two edited functions plus the rest, so all stay on the
same code):

```bash
# Build fn.zip from lambdas/*.py + pg8000 (exactly as setup.sh does)
BUILD="$(mktemp -d)"; cp lambdas/*.py "$BUILD/"
python3 -m pip install pg8000 -t "$BUILD/" --quiet
FN_ZIP="$(mktemp -d)/fn.zip"; ( cd "$BUILD" && zip -qr "$FN_ZIP" . )

# Update all 8 functions to the new code
for sfx in resolve-task driver-discovery plan-split create-glue-jobs \
           stop-cdc-run drain-check drop-tags preflight-tasks; do
  aws lambda update-function-code --function-name "$PROJECT-$sfx" \
    --zip-file "fileb://$FN_ZIP" --query FunctionName --output text
  aws lambda wait function-updated --function-name "$PROJECT-$sfx"
done
```

(The only functionally changed functions are `resolve-task` and `preflight-tasks`/`params_csv`;
updating all 8 from the one zip is what `setup.sh` does and keeps them consistent.)

### 3. State machines — fill placeholders and update (`startup` + `cutover`)

```bash
fill_sm () {  # $1 src asl, $2 out
  sed -e "s|<<BUCKET>>|$BUCKET|g" \
      -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
      -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
      -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
      -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
      -e "s|<<STOP_CDC_RUN_LAMBDA_ARN>>|$LAMBDA_BASE-stop-cdc-run|g" \
      -e "s|<<DRAIN_CHECK_LAMBDA_ARN>>|$LAMBDA_BASE-drain-check|g" \
      -e "s|<<DROP_TAGS_LAMBDA_ARN>>|$LAMBDA_BASE-drop-tags|g" \
      "$1" > "$2"
  grep -q "<<" "$2" && { echo "ERROR: placeholder left in $2"; grep -n "<<" "$2"; exit 1; } || true
}

for w in startup cutover; do
  OUT="$(mktemp -d)/$w.filled.asl.json"
  fill_sm "stepfunctions/$w.asl.json" "$OUT"
  ARN="$(aws stepfunctions list-state-machines \
         --query "stateMachines[?name=='$PROJECT-$w'].stateMachineArn" --output text)"
  aws stepfunctions update-state-machine --state-machine-arn "$ARN" \
    --definition "file://$OUT" --role-arn "$SFN_ROLE_ARN"
done
```

> `fleet-startup.asl.json` / `fleet-cutover.asl.json` are **unchanged** this redeploy, so they do
> not need updating. If you prefer to redeploy everything, `tools/setup.sh` re-fills all four.

### Confirm

```bash
aws s3 ls "s3://$BUCKET/scripts/"
aws lambda get-function --function-name "$PROJECT-resolve-task" --query 'Configuration.LastModified'
aws stepfunctions describe-state-machine --state-machine-arn "$SM_BASE:$PROJECT-startup"  --query 'name'
aws stepfunctions describe-state-machine --state-machine-arn "$SM_BASE:$PROJECT-cutover" --query 'name'
```

---

## TESTER_PLAN

All offline (no AWS / Spark / network). Two runners: **pytest** (`../.venv/bin/pytest tests/ -q`,
expect **181 passed**) **and** the self-executing files run **directly** (their `check()` helper
does **not** raise under pytest, so a direct run with a non-zero exit is the real signal):

```bash
python3 tests/test_override.py                 # ==== override tests: 87 passed, 0 failed ====
python3 tests/test_validate_merge_fixes.py     # ==== validate-merge-fixes: 58 passed, 0 failed ====
python3 tests/test_asl_paths.py                # ASL path audit: PASS (exit 0)
python3 tests/test_asl_payload_contract.py     # ==== asl-payload-contract: 187 passed, 0 failed ====
```

> Note: `tests/test_b17_b13.py` (B13 IAM allow-list) reads **all** `iam/*.json`, including
> gitignored `*.filled.json` artifacts other tests generate. Run it from a clean tree
> (`git clean -fdX iam/`) or before the IAM-fixture tests; it is unrelated to this change.

### Change 1 — cutover proceeds with a warning (no refusal)
- `tests/test_override.py :: test_cutover_startup_override_marker_proceeds_with_warning`:
  the `StartupOverrideRequiresOverride` state is **removed**; startup-override marker + no cutover
  override routes `StartupOverrideGate → LogStartupOverrideWarning → CdcValidationPreCheck`
  (proceeds); with cutover override it goes straight to the gate; no startup override is unchanged.
- `:: test_cutover_startup_override_warning_names_record_and_tables`: `resolve_task`
  (`_startup_override_cutover_warning`) names the startup-override record + overridden tables and
  says "proceeding WITHOUT override".

### Change 2 — override full bypass of load + validation failures
- `:: test_startup_override_true_load_failed_continues`: override + load failed (and load+validate)
  → `MarkOverrideActive` → DMS resume → `StartCdcJob` (full bypass, both `data_error` and `infra`
  load failures route identically in the ASL).
- `:: test_startup_override_true_validate_failed_resumes_and_succeeds_with_override`: override +
  validation failed → continues (as today).
- `:: test_startup_no_override_load_or_validate_failed_stops_unchanged`: **no override** + load OR
  validation failure → `GroupsFailed` (byte-identical).
- `:: test_job2_classify_load_error_data_vs_infra`: `job2_load.classify_load_error` tags every
  data-error kind (row-split, bad uuid/cast 22P02, NOT NULL 23502, too-long 22001, dup PK 23505,
  guard violations, malformed CSV) as `data_error` with a reason (+ column/file), and connection/
  ENI/OOM/timeout/unknown as `infra`.
- `:: test_write_override_record_writes_record_and_startup_marker`: the override record lists both
  the validate-failed and the load-failed group, and carries per-table **warnings** naming the
  `data_error`/`infra` kind + reason; warnings are persisted in the record and returned in output.

### Change 3 — explicit validate_parallelism used verbatim
- `tests/test_validate_merge_fixes.py :: test_m10_budget_divided_source`: an explicit
  `validate_parallelism (>0)` → `parallelism = max(1, _default_parallelism())` (no division); the
  auto path keeps `_default_parallelism() // max(1, MAX_PARALLEL_TABLES)`.
- `:: test_m02_parallelism_zero_is_auto` still green (0/blank → auto `None`).

### Change 4 — params_csv warns + falls back on bad validate_hash
- `tests/test_validate_merge_fixes.py :: test_m19_params_csv_validate_hash_enum`: a valid
  `validate_hash` passes; an **invalid** one is **not** in `errors`, **is** in `warnings`, and the
  resulting params fall back to the default `all`.

### Fails-before / passes-after (offline)
Each updated test file **fails** against the original runtime (exit 1 on a direct run) and
**passes** after. Verified by stashing the runtime files and re-running:
`test_validate_merge_fixes.py` (4 fails before → 0 after), `test_override.py` (T5 + load-bypass
fails before → 0 after).
