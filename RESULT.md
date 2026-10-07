# Runtime `override` for startup & cutover — RESULT

**Commit pushed:** `69fb12b` on `main` (parent `d0cd1e7`), as `newcoder52`.
Remote: `Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime`.

## What this adds

An operator can start a **new** execution with input `"override": true` so that when
**validation** fails for any reason (an IP shortage, a timeout, a mismatch the operator accepts)
the run carries on instead of stopping. Default (absent or `false`) behavior is unchanged — the
only new state data on a default run is `$.resolved.override=false` and a pass-through
`$.overrideResolved.active=false`; the side effects, Glue jobs, DMS actions and terminal
`TaskSucceeded`/`CutoverSucceeded` are identical to before.

Override is a **run input, never a `pipeline.json`/`params.csv` setting**, so no setting changes
and `test_docs_params` is unaffected (no new params key). An optional `"overrideReason"` string
is accepted and recorded.

### Startup
1. `resolve_task` normalizes `$.override` once (`_normalize_override`: the boolean `true`, or the
   strings `true`/`1`/`yes`/`y`/`on`, any case; everything else false) and emits it as
   `resolved.override` (+ `resolved.overrideReason`). The `AllGroupsSucceeded` Choice reads
   `$.resolved.override` **IsPresent-guarded**, so the ASL path audit passes and an older
   execution with no field behaves as the default.
2. **Load failure always stops** (`$.groupCheck.anyLoadFailed` → `GroupsFailed`), even with
   override — override covers validation only. (No strong reason was found to let override cover
   load failures, so it does not.)
3. A `validate_failed` group with `override=true` → `MarkOverrideActive` → `ResumeDmsToCdc` →
   `StartCdcJob` → the fork CDC map → `OverrideTerminal` → `WriteStartupOverrideRecord` →
   **`TaskSucceededWithOverride`**. Validation still ran and its results are recorded.
4. `TaskSucceededWithOverride` output (`$.overrideRecord`) lists the overridden groups + tables
   and the per-group validation report paths (`<group config_prefix>/_validation_report.json`),
   plus the record and marker S3 keys.
5. **Re-run with override does not reload done tables:** `job2_load` already skips tables marked
   `"done"` in `_load_status.json` up front, unconditionally (no override branch). Verified by
   test (`test_job2_load_skips_done_tables_regardless_of_override`); no code change was needed.
6. `WriteStartupOverrideRecord` writes `config/_task/<task>/_overrides/<execution>.json` (who =
   execution ARN, when, groups, tables, reason, validation report paths, bypassed gate) **and**
   the stable marker `config/_task/<task>/_overrides/_startup_override.json`.

### Cutover
- With `override=true`, the **refuse-only** gates are bypassed **but logged**:
  `CdcValidationFailed` pre-DMS-stop (`CdcValidationPreGate` → `LogPreValidationOverride`) and
  post-drain (`CdcValidationFinalGate` → `LogFinalValidationOverride`). The **cutover count
  check** (G10 `cutover_count_check=strict`) surfaces as rows in
  `cdc_control.cdc_validation_failures`, which those same gates read — so bypassing the gates
  bypasses-but-logs the cutover count check by the same mechanism (no CDC-script change).
- **Safety ordering is never bypassed:** DMS stop → drain → stop CDC runs → delete jobs are
  unchanged; the override edits only replace the two refuse-gates and the terminal. The final
  override bypass lands on `StopCdcRun` (stop CDC before deleting jobs), proven by
  `test_cutover_safety_ordering_not_bypassed`.
- Ends in **`CutoverSucceededWithOverride`** (via `WriteCutoverOverrideRecord`, which writes a
  per-execution record; `workflow=cutover`, so **no** startup marker).
- **Startup-override marker forces override at cutover:** `resolve_task` (cutover mode) reads the
  marker and returns `startupOverrideUsed`. `StartupOverrideGate` refuses with
  **`StartupOverrideRequiresOverride`** ("pass `override=true` to accept") when the marker is
  present and the cutover was not started with override — so unvalidated data can't be cut over
  silently.

### Fleet
- `preflight_tasks` reads a fleet-level top-level `{"override": true}` (applies to **every**
  task) and a per-task `override` CSV column (blank = false), OR's them, and adds
  `{"override": true}` (+ `overrideReason`) to each child's start input. `fleet_tasks.csv` gets
  an optional `override` column; preflight uses `csv.DictReader` and never rejects an extra
  column. The fleet ASLs pass `fleetInput.$: "$"` already, so no fleet-ASL change was needed.

## Exact runtime files changed & what a customer redeploys

Set once: `PROJECT=<project>`, `BUCKET=<bucket>`, `REGION=<region>`, `ACCOUNT=<account-id>`,
`export AWS_PAGER=""`. `SM_BASE="arn:aws:states:$REGION:$ACCOUNT:stateMachine"`,
`LAMBDA_BASE="arn:aws:lambda:$REGION:$ACCOUNT:function:$PROJECT"`.

| Runtime file | Change | Redeploy |
|---|---|---|
| `stepfunctions/startup.asl.json` | override gate + override terminal + `WriteStartupOverrideRecord` + `TaskSucceededWithOverride`; ResolveTask ResultSelector gains `override`/`overrideReason` | **State machine definition update** (startup) |
| `stepfunctions/cutover.asl.json` | `StartupOverrideGate`, pre/final validation override bypass, override terminal + `WriteCutoverOverrideRecord` + `CutoverSucceededWithOverride`; CutoverResolveTask ResultSelector gains `override`/`overrideReason`/`startupOverrideUsed` | **State machine definition update** (cutover) |
| `lambdas/resolve_task.py` | `_normalize_override`, override keys in `handler_shared`, cutover marker read, new `write_override_record` mode | **Lambda zip update** (`fn.zip`) → `$PROJECT-resolve-task` (and the rest, same zip) |
| `lambdas/preflight_tasks.py` | fleet-level + per-task override → child input | **Lambda zip update** (`fn.zip`) → `$PROJECT-preflight-tasks` |
| `config/fleet_tasks.example.csv` | documents the optional `override` column (example only) | copy-me only — nothing to deploy |

No Glue script (`scripts/*.py`) changed → **no S3 script upload needed**. No IAM change: the
override record is written under `config/_task/.../_overrides/` and the Lambda role already has
`s3:PutObject` on the bucket (`iam/lambda.json` `S3` statement).

### 1. Rebuild & update the Lambda zip (resolve-task + preflight-tasks share the one `fn.zip`)
The simplest supported path is to re-run `tools/setup.sh` from the repo root (it rebuilds
`fn.zip` from every `lambdas/*.py` + pg8000, updates all 8 Lambdas, and updates all 4 state
machines). To update **just** the two changed functions by hand:

```bash
# build fn.zip exactly as setup.sh does: all lambdas/*.py + pg8000
WORK=$(mktemp -d); cp lambdas/*.py "$WORK"/; pip install -q pg8000 -t "$WORK"
( cd "$WORK" && zip -qr fn.zip . )
for sfx in resolve-task preflight-tasks; do
  aws lambda update-function-code --function-name "$PROJECT-$sfx" \
    --zip-file "fileb://$WORK/fn.zip" --region "$REGION" --query FunctionName --output text
  aws lambda wait function-updated --function-name "$PROJECT-$sfx" --region "$REGION"
done
```

### 2. Update the two state-machine definitions (fill placeholders, then update)
`startup.asl.json` / `cutover.asl.json` carry `<<BUCKET>>` and the `<<*_LAMBDA_ARN>>`
placeholders. Fill them exactly as `fill_shared_sm()` in `tools/setup.sh` does, then update:

```bash
for w in startup cutover; do
  sed -e "s|<<BUCKET>>|$BUCKET|g" \
      -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
      -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
      -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
      -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
      -e "s|<<STOP_CDC_RUN_LAMBDA_ARN>>|$LAMBDA_BASE-stop-cdc-run|g" \
      -e "s|<<DRAIN_CHECK_LAMBDA_ARN>>|$LAMBDA_BASE-drain-check|g" \
      -e "s|<<DROP_TAGS_LAMBDA_ARN>>|$LAMBDA_BASE-drop-tags|g" \
      "stepfunctions/$w.asl.json" > "/tmp/$w.filled.asl.json"
  grep -q "<<" "/tmp/$w.filled.asl.json" && { echo "placeholder left"; exit 1; }
  ARN=$(aws stepfunctions list-state-machines --region "$REGION" \
        --query "stateMachines[?name=='$PROJECT-$w'].stateMachineArn" --output text)
  aws stepfunctions update-state-machine --state-machine-arn "$ARN" \
    --definition "file:///tmp/$w.filled.asl.json" --region "$REGION"
done
```

The fleet state machines (`fleet-startup`, `fleet-cutover`) are **unchanged** — no redeploy
needed; they already forward the fleet start input to preflight.

## Using it (also in RUNBOOK §8 "Validation failed — re-run with override")

```bash
# task-level startup with override
aws stepfunctions start-execution \
  --state-machine-arn "$SM_BASE:$PROJECT-startup" \
  --input '{"taskArn":"arn:aws:dms:'"$REGION"':'"$ACCOUNT"':task:<id>","override":true,"overrideReason":"reviewed"}'
# task-level cutover with override (same input shape, -cutover)
# fleet: add top-level "override": true to the fleet input, or set the per-task override column
```

## Tests (all green, offline)

- `tests/test_asl_paths.py` — ASL path audit + reachability + M01 + payload-contract: **PASS**
  (18 Lambda-ResultSelector states audited; the new `write_override_record` mode and the override
  ResultSelector reads are covered).
- `tests/test_asl_payload_contract.py` — 185 checks PASS, incl. the new override-field pins.
- `tests/test_override.py` — 43 checks: startup (override false→GroupsFailed; true→
  TaskSucceededWithOverride w/ CDC started; true+load-failed→GroupsFailed; done-table skip),
  cutover (pre/final gate bypass vs refuse; startup-marker forces override; safety ordering
  intact), resolve_task normalization + record/marker writes, fleet override wiring.
- `tests/test_docs_params.py`: **PASS** (unchanged — override is not a params key).
- Full suite: **168 passed**. Verified in a second fresh clone at `69fb12b`.
