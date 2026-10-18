# RESULT — customer_patches/startup_stop_after_load

> ## ⚠️ DO NOT CUT OVER A LOAD-ONLY TASK (operational guardrail)
>
> A `stopAfterLoad` run leaves the task at `STOPPED_AFTER_CACHED_EVENTS` with **CDC never started**.
> The **cutover** state machine has no precondition that CDC ran (a pre-existing design gap, not
> changed by this patch), so cutting over a load-only task does **not refuse** — it validates,
> stops the already-stopped DMS, drains trivially (no CDC manifest ⇒ reported caught-up), stops a
> non-existent CDC run, deletes the jobs and reports **success**, having applied **no post-load
> change data** (silent data loss). **Never cut over a task that ended in `TaskLoadOnlySucceeded`
> (marker `config/_task/<suffix>/_load_only.json`) until a NORMAL startup (without the flag) has
> resumed DMS and started CDC.** Operator check before cutover:
> ```
> aws s3 ls s3://<bucket>/config/_task/<suffix>/_load_only.json     # present => stopped after load
> aws s3 ls s3://<bucket>/config/_task/<suffix>/_cdc_started/       # empty/absent => CDC never ran
> ```
> If the marker exists and `_cdc_started/` is empty, run the normal startup first; only cut over
> once `_cdc_started/` is present. (Reviewer finding F2; cutover code intentionally unchanged.)

## Review fixes applied

* **F1 (HIGH) — FIXED.** `startup_stop_after_load.repo.diff` now includes the
  `tests/test_asl_payload_contract.py` registration of the new `write_load_only_record` Lambda mode
  (`MODE_RETURN_FUNCS` + `MODE_REQUIRED_INPUT`). Proven: a pristine clone @ `bcedcf73` is `200
  passed`; `git apply --check` + `git apply` of the 4-file diff applies clean; the repo's full
  suite is then **200 passed** (previously the 3-file diff left it `2 failed, 198 passed`).
* **F2 (MEDIUM) — DOCUMENTED.** Cutover warning box added to the top of the README and this RESULT,
  with the exact `aws s3 ls` go/no-go check. No cutover code and no apply-script/zip logic changed.

## What was built

An optional top-level boolean `stopAfterLoad` on the fleet/per-task startup input that makes the
startup workflow **stop after the full load**: load every group as today, but skip validation,
do not resume DMS, do not start CDC. Absent/null/false is byte-identical to today. It follows the
existing `override` path end to end.

* `lambdas/resolve_task.py` — strict `_normalize_stop_after_load` (boolean or `"true"/"false"`
  only; mirrors `_normalize_override`'s casing but rejects `"yes"/1/""/…`); `stopAfterLoad` added
  to the resolved dict; new `write_load_only_record` handler + `_load_only.json` marker helper.
* `lambdas/preflight_tasks.py` — fleet-level strict normalize (invalid → `PreflightError` before
  any task starts); per-task passthrough from the fleet flag or an optional `stop_after_load` CSV
  column, with the **TRUE WINS** rule; child input only ever gains `"stopAfterLoad": true`.
* `stepfunctions/startup.asl.json` — `IsLoadOnly`/`MarkLoadOnly`/`MarkNotLoadOnly` resolve a
  guaranteed `$.loadOnly` before the group fan-out; the Map passes `stopAfterLoad` to each group;
  `AfterGroupLoad` skips `GroupValidate` when load-only; `LoadOnlyOrContinue` routes a clean
  load-only run to `RecordLoadOnly` → `TaskLoadOnlySucceeded` and a failed load to `GroupsFailed`
  (override does not rescue); `RecordLoadOnlyFailedButLoaded` keeps the run successful if the
  marker write fails. All new reads are `IsPresent`-guarded.
* `stepfunctions/fleet-startup.asl.json` — **unchanged** (already forwards `fleetInput.$ = $`).
* `apply_stop_after_load_patch.py` + `stop_after_load_transform.py` — deployed-env apply (dry-run
  diff / `--apply` safe order with read-back / `--restore`), sharing the same fail-closed
  transforms as the plain-file path and the tests.
* `startup_stop_after_load.repo.diff` (+ per-file diffs), `README.md`, `test_stop_after_load_patch.py`,
  `test_run_output.txt`, `SHA256SUMS`, `orig/` snapshots.

## What was verified (offline, no AWS)

* **Patch tests**: `python3 customer_patches/startup_stop_after_load/test_stop_after_load_patch.py`
  → **93 checks passed, 0 failed** (24 pytest test functions).
  Includes: strict normalize matrix; preflight fleet/per-task true/false/**invalid rejected before
  any start**; child-input **byte-identical when absent**; **TRUE WINS**; the ASL walk proving
  `GroupValidate`, `ResumeDmsToCdc`, `StartCdcJob`, the fork starts and `WriteStartupOverrideRecord`
  are **unreachable** when the flag is true and reachable on the normal path; **reuse of the repo's
  ASL path audit** on the patched defs; **byte-identical state sequence** vs the original when the
  flag is absent; the apply script against **fake boto3** (dry run changes nothing, apply updates
  lambdas-first with read-back, second apply no-op, restore works, **missing anchor ⇒ nothing
  changed**); **three mutation checks** (remove validate skip / route into `ResumeDmsToCdc` / accept
  `'yes'` — each fails a test); and the **resumability correctness** test.
* **Repo ASL audit on patched defs**: `python3 tests/test_asl_paths.py` → REACHABILITY PASS,
  M01 REGRESSION PASS, PATH AUDIT PASS, PAYLOAD-CONTRACT PASS.
* **Transforms reproduce the shipped files byte-for-byte** and are idempotent; the plain-file diff
  **applies cleanly to a pristine `main`** (`git apply --check`) and reproduces the shipped files.
* **Repo's own full suite, patched tree**: `python3 -m pytest -q tests/` → **200 passed**,
  identical to the pre-patch baseline (**200 passed**).

## What was NOT verified

* **No real AWS**: no `stepfunctions`/`lambda`/`s3` calls were made against a live account; the
  apply/restore paths were exercised only against in-memory fake clients. A live run was not
  performed (per the task: offline only).
* No Spark/Glue job was executed; load/validate/CDC behaviour is asserted via the ASL graph and
  the existing Lambda logic, not a live job run.
* **Cutover was NOT changed and does NOT self-guard against a load-only task** (reviewer F2): it is
  an operational caveat, documented in the warning box above and in the README, not a code change.
  There is no automated refusal — the operator must run a normal startup first (verify via the
  `_cdc_started/` check) before cutting over a task that stopped after load.

## Correctness note (resumability)

A later normal startup after a load-only run is **accepted** (DMS is left
`STOPPED_AFTER_CACHED_EVENTS`, which `_check_startup_task` does **not** classify as "past full
load"); it then skips already-loaded tables (`job2_load` `_load_status.json`), validates, resumes
DMS and starts CDC. No preflight change was needed or made. A task whose startup is still RUNNING is
skipped as today. See README for the full analysis and the `test_later_normal_startup_accepted_after_load_only` test.
