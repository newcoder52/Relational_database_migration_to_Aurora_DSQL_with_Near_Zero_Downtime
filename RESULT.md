# RESULT — customer-controlled NULL handling (`null_values` + `null_rules`)

## Commit

- **sha (runtime change):** `7939e46be0d532f517de748a36fb70ea3000918e` (short `7939e46`)
- parent / base: `152f332` (remote `main` HEAD at start, pinned via `git ls-remote`)
- branch: `main` (plain push, never forced)
- committer: `newcoder52 <aash.798@gmail.com>` (git config only; never in file content)
- `git pull --rebase origin main` before push (nothing new had landed); verified in a 2nd
  fresh clone at `7939e46` (**199 passed**; ASL path audit + payload-contract audit green).

## What the feature does

Two optional `params.csv` settings let a customer control which CSV values DMS wrote become
SQL NULL. They are applied **identically** in the full load, CDC and validation, so validation
never false-flags a correctly-loaded row.

- **`null_values`** (all columns): a `|`-separated list of exact strings that become SQL NULL.
  Blank (default) = today's behaviour exactly — the DMS endpoint's `CsvNullValue`, as resolved by
  `resolve_task` and passed as `--csv_null_value`. Set, it **replaces** that marker list for every
  column.
- **`null_rules`** (per-column overrides): `schema.table.column=VALUES` entries joined by `;`.
  `VALUES` is `none` (nothing in that column becomes NULL from a text value — the text `NULL` is
  **kept as data**) or a `|`-separated list that replaces the default for that one column. Names
  match **case-insensitively** against the DSQL lowercase schema/table/column; value matching is
  exact, case-sensitive, whole-value. Example:
  `cns_schema.orders.status=none; cns_schema.orders.region=NULL|NA`.
- **Precedence** for a column: a `null_rules` entry > `null_values` > the endpoint marker.
- **Empty fields are ALWAYS NULL**, in every mode including `none` (unchanged).
- **Both blank/absent ⇒ byte-identical to today** (proven by `tests/test_null_rules.py`).

The parse/match logic is ONE shared block copied **byte-identically** into the four Glue scripts
(they share no imports); a test asserts the four copies are identical. The Spark `null_marker_expr`
(load/validate) and the Python `_coerce_null` (CDC, including CDC validation sampling) consume the
shared per-column marker resolver `_effective_null_markers`, comparing at the **same comparison
point** as the canonical `_coerce_null` (exact, case-sensitive, no trimming).

## Runtime files changed (8)

Glue scripts (shared null-rules block + column-aware consumers + `--null_values`/`--null_rules`
arg wiring + startup log + unknown-entry warning):

- `scripts/job2_load.py`            — `null_marker_expr` + per-table loop (Spark, per-file load)
- `scripts/job3_validate.py`        — `null_marker_expr` (Spark)
- `scripts/glue_cdc_continuous.py`  — `_coerce_null` incl. CDC validation sampling (Python)
- `scripts/glue_cdc_composite.py`   — `_coerce_null` (Python)

Lambdas (plumbing, mirrors how `--csv_null_value` is wired):

- `lambdas/params_csv.py`      — `null_values`/`null_rules` added to `OPTIONAL_DEFAULTS` + `PIPELINE_KEYS`
- `lambdas/resolve_task.py`    — `SETTINGS_DEFAULTS` + `_validate_null_settings` (malformed ⇒ preflight
  FAILS naming the bad entry) + `nullValues`/`nullRules` in the resolved payload
- `lambdas/create_glue_jobs.py`— sets `--null_values`/`--null_rules` on load/load-big/validate/cdc/
  cdc-spark/cdc-composite/cdc-composite-spark (blank ⇒ `__NULL_UNSET__`, since Glue can't pass an
  empty arg)

State machine:

- `stepfunctions/startup.asl.json` — `nullValues`/`nullRules` into the `ResolveTask` `ResultSelector`
  (so `$.resolved.*` is produced) and `null_values.$`/`null_rules.$` into the three
  `create_glue_jobs` payloads: `CreateGlueJobs` (main), `EnsureForkJobs` (ck/bg forks),
  `CdcDriverFallback` (Spark recreate).

Docs / config (not runtime):

- `RUNBOOK.md` (§3 rows + "when a change takes effect" + pipeline-key count 53→55),
  `config/params.example.csv`, `config/pipeline.example.json`, `docs/MANUAL_SETUP.md` (§3c).

Test (new): `tests/test_null_rules.py`.

## MANUAL setup — exact redeploy

Only the runtime artifacts above changed. From a clean checkout at `7939e46`, with the deployment
env vars you used at setup (`$BUCKET`, `$LAMBDA_BASE` = `arn:aws:lambda:<region>:<acct>:function:<project>`,
`$SM_BASE` = `arn:aws:states:<region>:<acct>:stateMachine`, `$P_PROJECT`), region exported:

1. **Glue scripts → S3** (all four scripts changed):

   ```
   aws s3 cp scripts/job2_load.py           s3://$BUCKET/scripts/job2_load.py
   aws s3 cp scripts/job3_validate.py       s3://$BUCKET/scripts/job3_validate.py
   aws s3 cp scripts/glue_cdc_continuous.py s3://$BUCKET/scripts/glue_cdc_continuous.py
   aws s3 cp scripts/glue_cdc_composite.py  s3://$BUCKET/scripts/glue_cdc_composite.py
   ```
   (Glue job `ScriptLocation` is unchanged, so no job redefinition is needed just to pick up new
   script bytes — the next job run reads the new object.)

2. **Lambda zip (WITH pg8000) → ALL functions.** `create_glue_jobs`, `resolve_task` and
   `params_csv` changed; `preflight_tasks` imports `resolve_task`/`params_csv` from the SAME zip,
   so **rebuild the one shared zip and update every function** (don't cherry-pick). Build exactly
   as setup does — every `lambdas/*.py` plus `pg8000` installed into the zip root:

   ```
   BUILD=$(mktemp -d); cp lambdas/*.py "$BUILD/"
   python3 -m pip install pg8000 -t "$BUILD/" --quiet      # pg8000 package must be in the zip
   (cd "$BUILD" && zip -qr /tmp/fn.zip .)
   ```
   Update **every** function the deployment has (enumerate them; don't assume a hard-coded list):

   ```
   for fn in $(aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$P_PROJECT-')].FunctionName" --output text); do
     aws lambda update-function-code --function-name "$fn" --zip-file fileb:///tmp/fn.zip
   done
   ```
   (The eight functions are resolve-task, driver-discovery, plan-split, create-glue-jobs,
   stop-cdc-run, drain-check, drop-tags, preflight-tasks.)

3. **State machines — only `startup` changed.** Fill the placeholders with the SAME `sed` map setup
   uses (`<<...LAMBDA_ARN>>` → `$LAMBDA_BASE-<name>`, `<<STARTUP/CUTOVER_STATE_MACHINE_ARN>>`,
   `<<BUCKET>>`), then `update-state-machine` **with `--definition` only and NO `--role-arn`** (the
   execution role is unchanged — only the ASL definition changed):

   ```
   sed -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
       -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
       -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
       -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
       -e "s|<<STOP_CDC_RUN_LAMBDA_ARN>>|$LAMBDA_BASE-stop-cdc-run|g" \
       -e "s|<<DRAIN_CHECK_LAMBDA_ARN>>|$LAMBDA_BASE-drain-check|g" \
       -e "s|<<DROP_TAGS_LAMBDA_ARN>>|$LAMBDA_BASE-drop-tags|g" \
       -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|$LAMBDA_BASE-preflight-tasks|g" \
       -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM_BASE:$P_PROJECT-startup|g" \
       -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM_BASE:$P_PROJECT-cutover|g" \
       -e "s|<<BUCKET>>|$BUCKET|g" \
       stepfunctions/startup.asl.json > /tmp/startup.filled.asl.json

   aws stepfunctions update-state-machine \
     --state-machine-arn "$SM_BASE:$P_PROJECT-startup" \
     --definition file:///tmp/startup.filled.asl.json
   ```
   `cutover.asl.json`, `fleet-startup.asl.json`, `fleet-cutover.asl.json` are **unchanged** — do not
   redeploy them. (Fleet SMs delegate to the per-task `startup`; cutover's only `create_glue_jobs`
   calls are `list_fork_cdc`/`delete`, which take no NULL args — same as `--csv_null_value` today.)

## How a customer applies new NULL rules to a task

- **Set the keys in `params.csv` and republish `config/pipeline.json`** (upload `params.csv` →
  rebuild/upload `pipeline.json`, MANUAL_SETUP §3c). A **malformed** `null_rules`/`null_values`
  **fails preflight** naming the bad entry.
- **BEFORE the full load (correct path):** set `null_values`/`null_rules`, republish
  `pipeline.json`, then start the task. The next `startup` run bakes the rules into the
  load/load-big/validate/cdc/ck/bg job `DefaultArguments` at job creation, so the full load, CDC
  and validation all share one rule set.
- **For EXISTING jobs (a task already created):** the rules are **baked at job creation**, so
  republishing `pipeline.json` alone does **not** change already-created jobs. The change takes
  effect only when those jobs are **recreated** — re-run `startup`/`CreateGlueJobs` (which also
  re-runs `EnsureForkJobs` for the `ck`/`bg` forks) — **or** the args are **overridden at run time**
  on `start_job_run`. Do this **before the full load**: changing rules mid-migration applies one
  rule set to already-loaded rows and another to CDC-applied rows, so a value could be stored NULL
  on one side and as text on the other and validation would flag it.
- An entry naming a table/column not in the task is only a **warning** (each job logs the effective
  rules at startup and warns about a column it doesn't have).

## TESTER_PLAN

Offline, no AWS/Spark/boto3. From a clean clone at `7939e46`:

- `python3 tests/test_null_rules.py` — the feature suite. Covers: defaults unchanged; `null_values`
  with 2 values; `null_rules` `none` keeping `NULL` as text; a per-column list; precedence
  (rule > values > endpoint); value case-sensitivity + name case-insensitivity; empty fields
  unchanged; a malformed rule failing preflight (resolve_task + params_csv paths); the **four
  script copies byte-identical**; and **load, validate and CDC agreeing** on the same inputs via a
  tiny Spark-column simulator vs the real `_coerce_null`. Each `check()` raises under pytest.
- Prove it's a real test: run the same file against pristine `152f332` scripts (e.g.
  `REPO_DIR=<pristine>`); it **fails** (no shared block / no `null_values` support).
- Regression gates (must stay green): `tests/test_composite_coerce_null.py`,
  `tests/test_docs_params.py`, `tests/test_asl_paths.py` (ASL audit),
  `tests/test_asl_payload_contract.py` (payload contract), `tests/test_marker_paths.py`.
- Full suite: `python3 -m pytest -q tests/` → **199 passed**.
