# RESULT — fix B20 (cutover can't delete Glue jobs) + B21 (composite CDC slow to stop times out the delete Lambda)

**Commit base:** main HEAD `0d7d2d86dda7c97ae893c366781fd3980f4d4deb` (one docs-only commit above
`8825b41`, verified with `git ls-remote`; built on the actual HEAD).
**Fix commit (runtime + tests + RUNBOOK):** `6c5c4f74aee970f5f2841a66247b4adb068df988`.

Both bugs were found on real AWS at `5e4adf8` and are still present on `0d7d2d8` — the cutover SM
`DeleteGlueJobs` payload and `create_glue_jobs` delete-mode on current main are byte-for-byte the
same code path that failed.

---

## B20 (HIGH) — cutover `DeleteGlueJobs` can't resolve the account id

**Root cause.** `cutover.asl.json`'s `DeleteGlueJobs` passed only
`{mode, bucket, project, taskSuffix}`. `create_glue_jobs` delete mode calls `_account_id(event)`,
which read only `event["account_id"]` or field 4 of `event["dms_task_arn"]`. It got neither, so
`_account_id` returned `""` and the handler raised `delete: cannot resolve account id to read job
tags.` (the account id is needed to build Glue job ARNs for the `get_tags` lookup that selects the
task's jobs by exact tag). Cutover therefore ended in `GlueJobsNotDeleted` with the migration cut
over but the Glue jobs orphaned.

**Fix.**
1. `stepfunctions/cutover.asl.json` — `DeleteGlueJobs` now also passes
   `"dms_task_arn.$": "$.taskArn"` (the cutover start input always carries `taskArn`) and
   `"account_id.$": "$.resolved.accountId"` (cutover-mode `resolve_task` emits `accountId`), plus
   `region.$`/`configPrefix.$` so the delete uses the same account + registry key as the earlier
   `ListForkCdcJobs`.
2. `lambdas/create_glue_jobs.py` — `_account_id(event, context)` now derives the account from, in
   order: `event["account_id"]` → field 4 of `event["dms_task_arn"]` → **field 4 of the Lambda's
   own `context.invoked_function_arn`** → **`sts:GetCallerIdentity`** (cached). The Lambda runs in
   the pipeline account that owns the Glue jobs, so the context ARN is always correct and needs no
   extra IAM; STS is the final backstop. This makes delete / `list_fork_cdc` (and the
   create/ensure/fallback registry+tagging path, which also needs `account_id`) unable to fail on
   a missing account id ever again. The `ensure_fork_jobs` tag-apply block now reuses the single
   resolved `account_id` instead of re-reading only the event.

## B21 (medium) — composite CDC run slow to stop times out the delete Lambda

**Root cause.** `_wait_runs_stopped` in `create_glue_jobs.py` waited `30 × 10 s = 300 s` for a
job's run to leave an active state before deleting it — exactly the delete Lambda's 300 s timeout.
A composite (`ck-*`) CDC run that was slow to stop kept the wait going until `Sandbox.Timedout`
killed the Lambda. **Why the composite run was slow (documented):** `glue_cdc_composite.py`'s poll
loop slept `POLL_INTERVAL` (default 30 s) between cycles with a bare, uninterruptible
`time.sleep(POLL_INTERVAL)` — the single longest interval in which the job could not react to a
stop. The per-file/per-chunk apply path is already short and checkpointed, so the idle poll gap was
the slow part.

**Fix.**
1. `lambdas/create_glue_jobs.py` — `_wait_runs_stopped(..., context)` is now bounded by
   `context.get_remaining_time_in_millis()` minus a 30 s margin and returns `True`/`False` (was
   `None`). Delete mode: if the wait returns `False` (run still active near the time budget), it
   issues a best-effort `batch_stop_job_run` for every STARTING/RUNNING run (`_best_effort_stop_runs`)
   and returns that job under a new **`pending`** list WITHOUT deleting it. The registry is cleared
   only when nothing is pending/failed, so a re-looped delete still finds the pending jobs. No
   Lambda call can exceed its own timeout anymore. With `context=None` (unit tests) the static
   `attempts × delay` budget is used unchanged.
2. `stepfunctions/cutover.asl.json` — a bounded loop: `InitDeleteLoop` → `DeleteGlueJobs` →
   `AllGlueJobsDeleted`. A hard `failed` → `GlueJobsNotDeletedList`; a `pending[0]` → `IncrDeleteLoop`
   → `DeleteBudgetLeft` (counter `< 120`) → `WaitDeleteRetry` (10 s) → `DeleteGlueJobs`; nothing
   pending/failed → `CutoverSucceeded`. Past the ~60-min budget it ends in `GlueJobsNotDeleted`
   (`GlueJobsPendingTimedOut`) naming the still-pending jobs.
3. `scripts/glue_cdc_composite.py` — faster exit on stop (cheap, as the report asked): the
   inter-cycle wait is now `_stop_event.wait(POLL_INTERVAL)` (interruptible) instead of
   `time.sleep`, a `SIGTERM` handler sets `_stop_event` (Glue stops a job with SIGTERM), and the
   main loop condition / wait return both break promptly when a stop is signalled. `StopCdcRun` /
   `StopForkCdcRuns` already issue `batch_stop_job_run` for STARTING/RUNNING runs (verified in
   `lambdas/stop_cdc_run.py`); the delete Lambda now also force-stops as a backstop.

## Input-side payload-contract check (the class of bug B20 is)

Extended `tests/test_asl_payload_contract.py` with an **INPUT-side** audit mirroring the existing
output-side one: for every Lambda-invoking state in all 4 state machines, the ASL
`Parameters.Payload` keys must be a **superset** of the keys that Lambda's mode *requires* from its
event (read as `event["x"]`, or `event.get("x")` with no safe fallback). Requirements are a
declarative `MODE_REQUIRED_INPUT` spec, with an "any-of" form for the account-id class
(`account_id` OR `dms_task_arn`). Includes:
- `test_input_payload_contract_all_state_machines` — audits every state (14+).
- `test_b20_delete_payload_passes_account_resolution` — positive (today's fixed payload passes) **and
  negative** (the pre-fix `{mode,bucket,project,taskSuffix}` payload FAILS the account requirement,
  proving the check catches today's bug).
- `test_input_contract_spec_matches_lambda_source` — drift guard: every declared single-key
  requirement is actually read as `event["key"]` in the mapped Lambda source.

No other input-side gap was found across the 4 state machines: every other Lambda-invoking state
already passes every key its mode requires (audit is green).

## Tests (all offline, no AWS / Spark / network)

- `tests/test_e2e_fixes.py` — new B20/B21 Lambda-behaviour tests with a **fake Glue where a run
  takes N `get_job_runs` calls to stop**: `_account_id` resolves from event / `dms_task_arn` /
  context ARN / STS; `_wait_runs_stopped` returns promptly (never 100×10 s) when the time budget is
  tight; delete returns both slow jobs under `pending` and never calls `delete_job` on a running
  job (invariant asserted by the fake), then a second pass with a full budget deletes them →
  nothing pending → the ASL loop terminates at `CutoverSucceeded`; plus a static check of the
  cutover loop shape.
- `tests/test_asl_payload_contract.py` — the input-side contract above (also exercised by
  `test_asl_paths`, which imports it).

**All required suites green (exit 0):**
`test_asl_paths`, `test_asl_payload_contract` (165), `test_guardrails` (78), `test_e2e_fixes` (80),
`test_b17_b13` (24), `test_b18_validate_throughput` (27), `test_docs_params`, `test_fix6` (51).
Also green: `test_planning_settings`, `test_existing_roles` (66).

## Docs

`RUNBOOK.md` §7 (cutover outcomes), §8 (recovery) and the state-machine reference table now describe
the delete loop / `pending` behaviour and the account-id payload, and state that `GlueJobsNotDeleted`
now self-heals a slow composite CDC stop (force-stop + retry); the only hand step left is the
last-resort "if it keeps failing" one. No advice to work around B20/B21 by hand remains as the
primary path.

## Runtime files changed (scripts/, lambdas/, stepfunctions/, glue-templates/)
- `lambdas/create_glue_jobs.py` — B20 `_account_id` context/STS fallback; B21 bounded
  `_wait_runs_stopped` + `_best_effort_stop_runs` + delete-mode `pending`.
- `scripts/glue_cdc_composite.py` — B21 interruptible inter-cycle wait + SIGTERM handler (faster
  stop).
- `stepfunctions/cutover.asl.json` — B20 `DeleteGlueJobs` payload (account_id + dms_task_arn); B21
  bounded delete loop.
- `glue-templates/` — **no change** (no runtime behaviour there needed changing).

Non-runtime: `RUNBOOK.md`, `tests/test_asl_payload_contract.py`, `tests/test_e2e_fixes.py`.
