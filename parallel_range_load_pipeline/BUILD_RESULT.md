# MRL PIPELINE — BUILD_RESULT

Builder deliverable for the Manual-Range-Load (MRL) Step Functions pipeline. All work lives only
under `parallel_range_load_pipeline/`. No existing repo file was modified. No AWS calls; offline
build/test only. Nothing pushed.

**Final committed head: `67023a5`** (branch `mrl-pipeline`).
**Promoted via `READY_V2` → `67023a5`.**

---

## 1. What this is

A NEW, SEPARATE parent (fleet) + child (task) Step Functions pair that automates the manual
file-range full load from `customer_patches/job2_load_manual_range/`, for tables too big for the
usual full-load route. It reuses the existing pipeline's discovery + CDC machinery verbatim and
replaces only the load planning/execution. Spec = `BRIEF.md` + `AMENDMENT_1..9.md` (all 9 applied).

## 2. Verdict handoff — reading the TESTER report correctly

The tester (`tester/TESTER_OUTPUT.md`) returned **FAIL at commit `a57aab5`** — the tree
`READY_V2` pointed at **at the moment the vetters read it**. Its own §7 re-verification against my
later head `6d59360` confirms every blocker it found was **already fixed in later commits**. The
failure was a **stale signal**, not a logic defect: I kept committing (`e51830d`, `6d59360`) after
raising `READY_V2`, so the designated SHA lagged the fixes.

**Resolution in this pass:** `READY_V2` has been re-pointed at the current green head `67023a5`,
which contains every B-fix, the A35 CDC-handoff fix, and the tester's non-blocker fixes. Every
tester blocker is re-confirmed closed at HEAD (evidence below).

## 3. Tester findings — disposition at HEAD `67023a5`

| finding | severity | disposition at HEAD | evidence |
|---|---|---|---|
| T-B2 example CSV had `mode` not `cdc` | HIGH | **FIXED** (`e51830d`) | example header now `task_arn,task_suffix,cdc,max_concurrency,…`; `test_deploy_iam.py` loads it under the validator |
| T-B3 deploy didn't sub `<<MRL_SETTINGS_LAMBDA_ARN>>`/`<<PLAN_SPLIT_LAMBDA_ARN>>`, never created `mrl-settings` | CRITICAL | **FIXED** (`e51830d`) | `deploy_mrl.sh` has both sed subs + `create_or_update_fn …-mrl-settings`; 8 Lambdas; `test_deploy_iam.py` asserts no leftover `<<` |
| T-B4 SFN IAM missing invoke on `mrl-settings` | HIGH | **FIXED** (`e51830d`) | `iam/mrl-additions.json` lists `<project>-mrl-settings` in `MrlInvokeMrlLambdas` |
| T-B5 Lambda IAM missing config-CSV reads + `pipeline_parallel_load.json` RW | HIGH | **FIXED** (`e51830d`) | GetObject on 3 config CSVs + Get/Put on `config/pipeline_parallel_load.json` |
| T-B6 IAM wrote forbidden `config/pipeline.json`; over-broad on scripts/templates/_task | MEDIUM | **FIXED** (`e51830d`) | `config/pipeline.json` removed; `scripts/*` + `glue-templates/*` GetObject-only; no Delete on `config/_task/*` |
| T-B7 count Glue job created with no VPC connection | MEDIUM | **FIXED** (`e51830d`) | `deploy_mrl.sh` passes the connection (`CONN_JSON`) on count-job create+update |
| T-B8 Lambda zip not verified to contain modules + pg8000 | MEDIUM | **FIXED** (`e51830d`) | deploy fails closed naming any missing `lambdas/*.py` / `resolve_task.py` / `params_csv.py` / `pg8000/` |
| T-A35 cdc=true: reconcile didn't write original-format `_load_status.json` at CDC prefix | HIGH (cdc=true only) | **FIXED** (`6d59360`) | `mrl_reconcile` writes `config/_task/<suffix>/_load_status.json` in original format when `cdc=true` & clean; `test_cdc_handoff.py` proves it against the REAL extracted CDC `load_full_load_status` |
| T-A3 literal customer name in 2 docs | LOW | **FIXED** (`67023a5`) | customer name scrubbed from `DESIGN.md` + `LIVE_TEST_STEPS.md`; repo-wide grep clean |
| T-A1 blank `map_max_concurrency` fails closed vs Amendment 5(d) default | non-blocking | **KEPT as deliberate decision, documented** (`67023a5`) | DESIGN.md §11.7 — fail-closed is safer & consistent with Amendment 5 item 4; omit key for default 3 |
| T-A2 fleet cap 1000 vs Amendment-3 suggested 200 | non-blocking | **RESOLVED by the user (cap now 200)** (2026-10-16) | `_MAX_ROWS = 200`; over-cap fails closed naming actual vs allowed and "before anything starts"; `test_fleet_cap.py` (200 accepted, 201 rejected before any DMS call, mutation guard) |
| T-A4 two untagged README diagram fences | cosmetic | acknowledged, no functional impact (diagrams, not commands) | — |

The tester also independently **confirmed PASS** for: splitter correctness (19,264+2,000 oracle
cases), file allow/deny-list (10,000 random folders, zero CDC leakage), 5-source max_concurrency
precedence, cdc boolean paths, Amendment-7 discovery-order reuse, multi-table tasks, strict CSV
parsing, and zero leaked secrets/ids.

## 4. Verified BY EXECUTION (this build)

- **`python3 -m pytest tests/ -q` → 114 passed, 0 failed** _(historical build-record count;
  current release-2 count is **268 passed** — see "Stale counts (F3)" below)_ (incl. `test_deploy_iam.py`,
  `test_cdc_handoff.py`, `test_vetter_findings.py`, `test_amendments.py`, mutation/ASL-guard tests).
- **`python3 tools/mrl_simulate.py` → 22 scenarios, 22 passed** _(historical; current release-2:
  **27 scenarios, 27 passed**)_ (incl. #19 CDC-ignored).
- ASL audits (string-arg Glue args, history/payload budget, startup-state byte-identity diff) pass.
- `deploy_mrl.sh --dry-run` clean; `bash -n` clean; `py_compile` on all `.py`; `json.load` on all
  `.json`.
- Only `parallel_range_load_pipeline/` files changed at every commit (verified `git status` each time; no
  existing repo file touched).
- Tester's own cross-check: 102→**114** suite green at HEAD, every B/A35 blocker closed.

## 5. Verified BY READING (not executed against live AWS — by design, offline)

- The reused CDC/DMS startup states are byte-identical to the original route (diff-tested offline),
  so the real cutover works unchanged — but no live DMS/CDC run was performed.
- IAM policy grants are reviewed by reading against each Lambda's actual S3/Lambda calls; not
  exercised against a live IAM boundary.
- Deploy script behavior against real AWS APIs is reasoned from the script + `--dry-run`; no real
  `create-state-machine`/`create-function` was invoked.

## 6. Key decisions

- Canonical file ordering = lexicographic by name, matching the loader resolver exactly (proof-c
  cross-check `assert_plan_equals_loader_set` against the loader's AUTHORITATIVE set, incl. parallel
  partition sub-folder keys).
- Default `max_concurrency = 3`; 5-source precedence (exec input > fleet row >
  params_parallel_load.csv > params.csv > built-in default).
- CDC files allow-list `^load[0-9a-f]+\.csv$` + deny-list `^[0-9]{8}-[0-9]+\.csv$`; CDC files are
  never planned/loaded and are reported as ignored.
- Settings are published to `config/pipeline_parallel_load.json` and PINNED per run at
  `mrl/runs/<exec>/settings.json`; the 3 forbidden config files are never written.
- A35 handoff: on clean `cdc=true` reconcile, write the original-format task `_load_status.json` so
  the reused CDC job's `REQUIRE_FULL_LOAD_DONE` + baseline see the table done.
- Fail-closed everywhere: re-run with a different `max_concurrency` on an already-loaded range, any
  plan≠loader-set divergence, listing drift, duplicate basenames across case-variant folders, blank
  numeric params, over-cap fleet rows.

## 7. Residual risks

- **No live AWS validation.** IAM, deploy, DMS/CDC cutover verified by reading + offline simulation
  only. First real deploy should run `deploy_mrl.sh --dry-run` then a single non-production task per
  `LIVE_TEST_STEPS.md`.
- **Signal discipline.** This build's root failure was promoting `READY_V2` before all fixes
  committed. Mitigated by re-pointing `READY_V2` at the final head `67023a5`; future changes must
  re-touch the signal after committing.
- T-A1 is an intentional deviation from literal amendment wording (documented, fail-closed);
  confirm it matches operator expectations before GA. T-A2 is RESOLVED (fleet cap set to 200 per
  the user's 2026-10-16 decision).

## 8. How to run (offline)

```
cd parallel_range_load_pipeline
python3 -m pytest tests/ -q
python3 tools/mrl_simulate.py
```

## 9. Round 2 findings and dispositions

Three independent round-2 reviewers returned **SAFE WITH THESE FIXES**. Every item is fixed with an
executing fail-before / pass-after test. Suite is **137 passed** (was 114) twice (PYTHONHASHSEED=0
and TZ=UTC LANG=C); simulator **23 scenarios** (adds `3b_cdc_fork_handoff`). All fixes committed
after `19a745b`. _(All counts in this file are historical build-record snapshots; the current
authoritative release-2 counts are **268 passed** and **27 scenarios** — see "Stale counts (F3)" below.)_

| finding | severity | fixed at | evidence (executing) |
|---|---|---|---|
| **R23-B1** CDC full-load handoff written only at the task prefix; a ck/bg FORK CDC job reads its own fork/group prefix, sees the table "not done", and skips it forever (silent data loss). The 406-file flagship is a bg fork. | HIGH (cdc=true) | `f5cc210` | New `mrl_cdc_handoff` Lambda + `CdcForkHandoff` ASL state (after EnsureForkJobs, before ResumeDmsToCdc/StartCdcJob) writes the original-format `_load_status.json` at every fork `config_prefix`. `test_cdc_handoff.py` runs the REAL extracted `load_full_load_status` at a ck prefix and a bg/group prefix (table seen `done`, `_FULL_LOAD_ROWS` seeded) + mutation (no fork write → gate None) + idempotence + cdc=false + not-clean fail-closed. Simulator `3b_cdc_fork_handoff` drives a real ck + bg fork end to end and re-runs the real gate. DESIGN.md §11.5.1. |
| **R22-1** route-conflict guard inert: IAM lacked `states:ListExecutions/DescribeExecution` on the original startup+cutover machines and the guard swallowed AccessDenied. | HIGH | `f5cc210` | (a) `iam/mrl-additions.json` adds least-privilege grants on `<project>-startup`/`-cutover` machines + their executions. (b) `_route_conflict_guard` now FAILS CLOSED on AccessDenied/any listing error (override=true downgrades to a loud warning). `test_preflight.py`: fail-closed on denied list, override downgrade, running original exec, unfinished original load, done-load allowed. `test_deploy_iam.py`: grants present. |
| **M5** `test_count_verdict.py` re-implemented the verdict rollup. | MEDIUM | `f5cc210` | Refactored the rollup into pure `compute_verdict()` that `main()` calls; `test_count_verdict.py` executes the REAL function over equal/mismatch/tolerance/timeout-fail/warn/no-basis + a mutation (`if mismatches:`→`if False:`) that breaks a test. |
| **M7** forbidden-write guard had no negative test. | MEDIUM | `f5cc210` | `test_amendments.py` calls `mrl_settings._put`/`_guard_forbidden` with each forbidden key (params.csv/pipeline.json/fleet_tasks.csv, incl. leading slash) asserting it raises + a mutation guard. |
| **M8** route-conflict guard had no unit test. | MEDIUM | `f5cc210` | `test_preflight.py` seeds a RUNNING original exec and an unfinished `_orchestrator/..._load_status.json`; both fail closed before any DMS call. |
| **R21-2** count job reported `ok` for a loaded table with no comparison basis. | LOW | `f5cc210` | New `count_no_basis_action` (default **fail**); `no_basis` verdict fails closed (warn downgrades). `compute_verdict`/`main()` + `test_count_verdict.py`. |
| **R23-L7** Glue role had `s3:DeleteObject` on `config/_task/*`. | LOW | `f5cc210` | IAM split: Delete only on `mrl/*`; `config/_task/*` is Get/Put only. `test_deploy_iam.py::test_r23l7...`. |
| **R22-2** stale `TaskSucceededCdc` ASL comment (said forks out of scope). | LOW | `f5cc210` | Comment rewritten to state fork CDC jobs + the fork handoff are handled. |
| **R22-3** dead `COUNT_ARGS` in `deploy_mrl.sh`. | LOW | `f5cc210` | Removed. |
| **R23-L2** stale "built-in default 6" comment + plan fallback. | LOW | `f5cc210` | Preflight precedence comment and `mrl_plan` fallback set to 3; README fleet-column default corrected to 3 (omit for default). |
| **R21-1** `assert_uniform_width` accepted mixed-case hex. | LOW | `f5cc210` | Now fails closed naming the files when a table mixes upper/lower-case hex; `test_listing.py::test_r21_1...` + mutation test updated. |
| **R21-3** blank `map_max_concurrency` fails closed vs Amendment 5(e). | INFO (NOT changed — user undecided) | — | Behavior unchanged; documented in DESIGN.md §11.7; README shows omit-the-key for the default 3. |
| **R22-5** untagged README diagram fences. | COSMETIC | — | Left as-is (command blocks are pure ASCII; diagrams are not commands; bash -n not run on README). |

**Round-2 residual risks:** still no live AWS validation — the fork handoff, IAM grants, and CDC
cutover are proven offline (real extracted CDC gate + simulator) but not against live DMS/CDC/IAM.
First real cdc=true run on a composite-PK or ≥8-file table should confirm, via the Glue console,
that each fork CDC job applies changes (not just starts).

## 10. Amendment 12 — deploy hotfix (glue create-job connections)

`tools/deploy_mrl.sh` used the non-existent `aws glue create-job --connections-list` (exit 252).
Fixed to the valid `--connections '{"Connections":[...]}'` (bash array); the dry-run now prints the
EXACT aws command for BOTH create and update branches of every path. New `tests/test_aws_cli_flags.py`
validates every emitted `aws <svc> <op>` against the real botocore model (flags, required members,
JSON structure) with a mutation (re-introducing `--connections-list` fails) and a docs scan.
Fixed at `6cdd56b`.

## 11. Amendment 10 — sizing from fleet/exec/default only + ZERO new IAM

**A (params):** MRL no longer reads sizing/parallelism from `config/params.csv`. `lambdas/mrl_defaults.py`
is the single source of built-in defaults (G.4X / 10 workers / mfip 30 / wpf 8 / 2880 min /
max_concurrency 3; count G.8X). Precedence: execution input > fleet row > built-in default. The
params.csv `map_max_concurrency` layer and `params_key_explicit`/`_explicit_keys` helpers were
removed; every sizing key is reclassified IGNORED_HERE in `tools/mrl_settings_classification.py`
(the shared parser still accepts/validates them; the sync test stays). The README shows a short
"keys MRL uses" table + built-in defaults; the full per-key table moved to DESIGN §11.6.1.

**B (ZERO new IAM):** the three EXISTING roles already cover the DEFAULT deployment (no fleet SM).
`tools/mrl_required_actions.json` is the single source of truth; `tests/test_existing_iam_covers_mrl.py`
scans every boto3 call (fails on an unmapped method) and evaluates each required action against the
base policies. `tools/mrl_fleet_run.py` starts tasks with operator credentials (no state machine,
same Lambdas + byte-equal payloads). The fleet state machine is OPTIONAL (`deploy_mrl.sh
--with-fleet-sm`, default off); its only extra grants live in `iam/mrl-fleet-sm-optional.json`
(replaces the deleted `iam/mrl-additions.json`). `tools/mrl_iam_check.sh` runs
`simulate-principal-policy` per required action. The Lambda zip no longer installs pg8000 (no MRL
Lambda imports it) → no-internet deploy.

### IAM coverage by role (default deployment)

| role | covered by the existing policy? | notes |
|---|---|---|
| `<project>-lambda-exec-role` | **YES — fully covered** | S3 get/put/list on `bucket/*`; glue create/update/get/tag on `*`; iam:PassRole of the glue role; states List/DescribeExecution on `<project>-*` (covers the route-conflict guard on `-startup`/`-cutover`); dms describe `*`; dsql endpoint resolve. The old `MrlStartAndDescribeChildAndFleet` StartExecution grant was UNNEEDED (no MRL Lambda calls start_execution). |
| `<project>-glue-exec-role` | **YES — fully covered** | S3 get/put/delete on `bucket/*`; dsql DbConnectAdmin; dms DescribeTableStatistics — covers the range-load + count jobs. |
| `<project>-sfn-exec-role` | **YES for the default** (mrl-task) | InvokeFunction on `<project>-*`; dms start/describe `*`; glue start/get/batch-stop `*`; the Glue sync event rule; s3:ListBucket `config/_task/*`. **GAP only for the OPTIONAL fleet SM:** startExecution.sync:2 on `<project>-mrl-task` + the StepFunctions-execution event rule — supplied by `iam/mrl-fleet-sm-optional.json`. |

**Changed tests (A10):** `test_amendments.py::test_map_max_concurrency_explicit_vs_default` →
`test_a10_params_csv_does_not_drive_max_concurrency` (params.csv no longer drives max_concurrency;
`params_key_explicit`/`_resolved_params_max_concurrency` removed). `test_params_classification.py`:
`test_readme_table_matches_classification` → `test_design_table_matches_classification` (full table
moved to DESIGN) + new `test_readme_lists_only_used_keys`. `test_deploy_iam.py` rewritten (no
`iam/mrl-additions.json`; B8 now asserts no pip/pg8000; new `--with-fleet-sm`/no-iam-call tests).
`test_iam.py` repointed at `iam/mrl-fleet-sm-optional.json`. New: `test_a10_params.py`,
`test_existing_iam_covers_mrl.py`, `test_mrl_fleet_run.py`.

## 12. Amendment 11 — coexistence in one bucket

`tests/test_coexistence_no_collision.py`: MRL's full create/write/delete inventory never collides
with the original outside the shared `config/_task/<suffix>/`; the only shared writes are the
allow-listed cdc=true handoff docs; MRL deletes nothing outside `mrl/`; no MRL job is named like an
original role. The REAL base `resolve_task._check_owner` was exercised: with MRL artifacts and no
owner marker the original startup **REFUSES** (fail closed); with the matching owner marker it
**REUSES** (route-conflict guard prevents concurrent runs); different owner → refuse — **no silent
overwrite of MRL state (no finding)**. `deploy_mrl.sh` gained a read-only ownership preflight
(WILL CREATE / WILL UPDATE mrl-owned / ABORT) and ends with `EXISTING PIPELINE RESOURCES TOUCHED: 0`,
proven for a full run and a dry run with a fake aws CLI in `tests/test_deploy_touches_zero.py`.
**Existing resources touched: 0.**

## 13. Amendment 13 — no-internet count/probe driver set (live HIGH) + round-4 fixes

**A (HIGH, live-confirmed):** the python-shell count/probe Glue job hung ~20 min in a no-internet
VPC because it got the Spark wheels-on-path set (`$.driverValidation.extraPyFiles`) and Glue pip
then fetched pg8000's `Requires-Dist: scramp` from PyPI. **Fixed (option a):** a new ASL state
`DriverDiscoveryCount` (`prepare_for=pythonshell`, `spark_fallback=false`, `drivers_prefix=driver-cdc`)
runs in the unconditional discovery chain (so cdc=false runs get it too) BEFORE SafetyProbe and
produces the ORIGINAL Requires-Dist-STRIPPED prepared set; `SafetyProbe`/`CountVerify` now read
`$.driverCount.extraPyFiles`, so Glue pip never contacts PyPI. `driver-cdc/` already ships
pg8000+scramp+asn1crypto, so the prepared python-shell set suffices (no Spark count job needed).
Proven offline in `tests/test_count_driver_offline.py` (prepared set installs via `pip --no-index`;
unstripped pg8000 fails with the exact scramp error; mutation). Fail-fast: `.sync` probe/count
states carry `TimeoutSeconds` (probe 900s / count 3600s, overridable) → `MrlProbeTimedOut`/
`MrlCountTimedOut`; deploy aborts if `driver-cdc/` has no `.whl` (A.3); simulator scenario 20
(`20_unstripped_pythonshell_fails_fast`) fails fast at `MrlDriversFailed`.

**B (round 4):** R4-1 deploy now copies `<project>-resolve-task` Runtime+Architectures (fallback
python3.11/x86_64 with a notice) and adds `--lambda-vpc-from-resolve-task` (default off); R4-2 all
docs lead with `tools/mrl_fleet_run.py` and move the fleet-SM start under an "Optional" heading;
R4-3 added the operator rule (never run the original route on an MRL-loaded task) + the
continue-to-CDC order + live-test prerequisites to RUNBOOK/INTEGRATION; R4-4 added
`states:DescribeExecution` to the glue role in `mrl_required_actions.json`; R4-5 single `$COUNT_JOB`
var; R4-6 fixed the stale `iam/mrl-additions.json` message in the preflight route-guard; R4-7
reworded the MAX_CONCURRENCY comment.

**Changed tests (A13):** `test_vetter_findings.py::test_a45_driver_extras_parity` updated (count path
now uses `$.driverCount`) + new `test_a13_probe_count_fail_fast_timeouts`; `test_asl_prod_guards`
fixture gained `driverCount`/timeout fields; `test_deploy_touches_zero.py` gained R4-1 (runtime/arch/
VPC) + A.3 (wheel prerequisite) tests; new `test_count_driver_offline.py`. Simulator scenario 20 added.

## 14. Amendment 14 — invalid ASL field `TimeoutSeconds.$` (live HIGH) + offline validators

**What was wrong:** A13 added `"TimeoutSeconds.$": "$.resolved.probeTimeoutSeconds"` (and the count
one). Step Functions has NO `TimeoutSeconds.$` — the dynamic field is `TimeoutSecondsPath` (a plain
JSONPath string). `create-state-machine sharedtest-mrl-task` failed with
`SCHEMA_VALIDATION_FAILED: Field 'TimeoutSeconds.$' is not supported`.
**Why offline tests missed it:** 180 tests + 24 simulator scenarios never submitted the definition
to the Step Functions schema validator; the simulator's interpreter tolerated the field.
**Fix:** both states now use `TimeoutSecondsPath` → `$.resolved.probeTimeoutSeconds` /
`countTimeoutSeconds`, and the Preflight ResultSelector now carries those as JSON **numbers** into
`$.resolved` (they were missing → would have been a runtime States.Runtime error even once the
field name was fixed). The `States.Timeout` Catch → `MrlProbeTimedOut`/`MrlCountTimedOut` is intact.

**New guards so this cannot recur:**
- `tests/test_asl_schema.py` — a strict offline ASL schema validator over every `*.asl.json`:
  per-Type allowed fields; a `.$` key is illegal as a state-level field and inside Retry/Catch/
  Choice (only valid in payload templates); mutually-exclusive path pairs; StartAt/Next/End/target
  existence; known intrinsics. Mutation: re-introducing `"TimeoutSeconds.$"` fails the test.
- `tools/deploy_mrl.sh` runs `aws stepfunctions validate-state-machine-definition` (read-only)
  before every create/update (also in --dry-run); aborts on a FAIL result printing each diagnostic;
  warns and continues if the op/permission is unavailable (optional `states:ValidateStateMachineDefinition`,
  documented in the RUNBOOK). `tests/test_deploy_touches_zero.py` covers FAIL-aborts / OK-continues /
  unavailable-warns with a fake aws.
- `tests/test_botocore_create_update.py` — validates EVERY create/update on the deploy path against
  the REAL botocore input shapes: count-job update/create JSON (python-shell: MaxCapacity, no
  WorkerType; Connections is the `{"Connections":[...]}` STRUCTURE), `mrl_ensure_job`'s glueetl
  CreateJob/UpdateJob args (WorkerType+NumberOfWorkers, no MaxCapacity), Lambda create args
  (Runtime/Architectures/Description<=256/VpcConfig), and the SFN create/update + the new validate op.

## 15. Amendment 15 — empty-string Glue run argument (live HIGH) + run-arg validators

**What was wrong:** RunRange passed `--null_values.$`/`--null_rules.$` (and `--csv_null_value.$`)
as PER-RUN Glue StartJobRun arguments. A blank setting resolves to the EMPTY STRING; Glue drops an
empty-string argument value, so the mrl-load job failed at launch with
`argument --null_values: expected one argument` (live test 4, both tasks; DSQL targets stayed empty
— fail closed, no damage). The sentinel `__NULL_UNSET__` existed only in `mrl_ensure_job`'s
DefaultArguments, which the per-run arg overrode.
**Why offline tests missed it:** nothing evaluated the ASL Arguments templates with a BLANK value
and asserted non-empty strings; the simulator's fake Glue didn't require non-empty args.
**Fix (option 1a — matches the original):** removed `--null_values.$`/`--null_rules.$`/
`--csv_null_value.$` from RunRange. `EnsureMrlLoadJob` (which runs before LoadRangeMap) sets them,
sentinel-aware, in the job's DefaultArguments and does create-or-UpdateJob, so a changed null
setting is applied (UpdateJob replaces DefaultArguments) — exactly as `create_glue_jobs` does.
Proven equivalent for blank / `\N` / `NULL` / quoted list / non-empty null_rules in the tests.
Audited every Glue run arg in both ASL + the arg-building Lambdas: the three null args were the only
ones that could be empty; all remaining run args resolve to non-empty strings (numbers are
States.Format-wrapped).

**New guards:**
- `tests/test_glue_args_nonempty.py` — evaluates every `glue:startJobRun[.sync]` Arguments template
  against realistic inputs (blank AND non-blank null, cdc true/false) and asserts each value is a
  non-empty string; mutation re-adds `--null_values.$` with a blank input → fails.
- `tests/test_botocore_create_update.py` extended: validates the StartJobRun Arguments map
  (str→non-empty str) via botocore; a required-vs-passed check that every MRL-essential job2 arg is
  in RunRange run args ∪ `mrl_ensure_job` DefaultArguments and the count job's required args are in
  CountVerify; asserts the three null args are DefaultArguments-only (not run args).
- Simulator scenario `21_blank_null_settings_runrange_ok` drives a blank-null load through RunRange
  and asserts no null args were passed per-run.
- Script-side (verified + tested): `job2_load_manual_range.py` treats a missing / `__NULL_UNSET__`
  null arg as unset (like the original); `job_mrl_count.py` never parses null settings and is not
  sent them. Step-5 path review found no other required-arg / S3-path / status-key mismatch.

## 16. Amendment 16 — python-shell count job required `--JOB_NAME` (live HIGH) + argparse validators

**Open question (answered by evidence):** NOTHING changed between 970efb4 and 1a65e52 that affected
this. `git diff 970efb4 1a65e52` touches only the 3-line A15 RunRange null-arg removal; SafetyProbe/
CountVerify args are byte-identical (no `--JOB_NAME` in either), the count-job DefaultArguments never
contained `--JOB_NAME`, and `scripts/job_mrl_count.py` has required `--JOB_NAME` via
`getResolvedOptions(sys.argv, ["JOB_NAME"] + _present)` since the INITIAL commit `cbc0f2f`. So the
`--JOB_NAME` requirement was a **pre-existing latent defect**, unmasked only now that the upstream
blockers (T14A pip, T14B pip-hang, T14C SM-create, T14D `--null_values`) are cleared so the probe
actually launches the pythonshell job and reaches argparse. The "244-303s mrl-count SUCCEEDED" runs
the amendment cites were a **concurrent t14c session** (the T14E tester confirms this path "had NEVER
run live"); the T14D "SafetyProbe PASS" was an implicit inference. Glue auto-injects `--JOB_NAME`
for SPARK jobs but NOT for a pythonshell job started via StartJobRun.

**Fix:** `job_mrl_count.py` no longer requires `--JOB_NAME` — it requests only the options actually
present and reads `JOB_NAME` optionally (default `mrl-count`, used only for log text). No reliance
on the job definition or ASL to supply it. `job2_load_manual_range.py` is glueetl (Spark), where
Glue injects `--JOB_NAME`, so its `getResolvedOptions(sys.argv, ['JOB_NAME'])` is correct (same as
the original job2_load.py that runs in production).

**New guards:**
- `tests/test_script_argparse.py` — a faithful fake `getResolvedOptions` (argparse; requested args
  required; SystemExit(2) on a missing one) runs the REAL count arg block with the EXACT SafetyProbe
  and CountVerify Arguments + DefaultArguments → exit 0 (probe + count); mutation re-requiring
  JOB_NAME → exit 2. Plus: glueetl mandatory options == {JOB_NAME}, pythonshell mandatory == {}.
- `tests/test_count_probe_e2e.py` — runs the REAL probe + count logic with a fake pg8000/boto3:
  empty → gate pass; non-empty-no-progress / missing → `mrl_gate_eval` FAILS CLOSED (raises);
  count equal → `ok`, mismatch → `mismatch` (main() exits non-zero), no-basis → `no_basis` fail.
  Asserts the exact probe JSON and writer↔reader key agreement (`table`/`empty`/`exists`).
- DSQL-connection review (reported): the token path uses `boto3.client("dsql")
  .generate_db_connect_admin_auth_token(host, Region, ExpiresIn)` — confirmed present in the bundled
  boto3 1.42.x under Python 3.9 (and the prepared driver-cdc set validates "botocore knows dsql").
  No writer/reader key, S3-path, or exit-code mismatch found on the probe/count path.

## 17. Amendment 17 — module-level NameError at mrl-load import (live HIGH) + import-smoke net

**What was wrong:** `scripts/job2_load_manual_range.py` CALLED `_apply_manual_range_overrides()` at
module top level (line 838) but DEFINED it ~4400 lines later (5264); worse, the `_MANUAL_RANGE`
state dict it populates was defined at 5034 — both AFTER the call. So the glueetl mrl-load job
crashed at import with `NameError: name '_apply_manual_range_overrides' is not defined` (live T14F,
both tasks; targets stayed empty — fail closed). The 213 tests missed it because they AST-extract
functions and NEVER execute the module top level (the blind spot behind several live defects).
**Progress recorded:** SafetyProbe SUCCEEDED live for the first time (A16 --JOB_NAME fix confirmed);
the null-arg (A15) and no-internet-driver (A13) fixes also confirmed live.

**Fix:** moved ONLY the `_MANUAL_RANGE` dict + the `_apply_manual_range_overrides` def to just
ABOVE the call. Checked everything that def touches at import time — `sys` (import 76),
`getResolvedOptions` (import 87), `_MANUAL_RANGE` (now defined just above) — all bound before the
call. The `_mr_*` wrappers / `ManualRangeError` run later (not at import) and stay in place.

**New safety net:**
- `tests/test_import_smoke.py` — EXECUTES each Glue script's MODULE TOP LEVEL with stubbed
  awsglue/pyspark/boto3/botocore/pg8000 and the REAL argv (RunRange args + ensure_job
  DefaultArguments): job2 (glueetl) runs past the former NameError and stops at the first Spark
  side effect with a StopHere sentinel; job_mrl_count (python-shell) completes. A static check that
  no module-level call precedes its def; mutation (def moved back below the call) fails.
- `tests/test_pyflakes_undefined.py` — pyflakes over the whole MRL tree; FAILS on any
  `undefined name` / used-before-assignment. Current: **0 undefined names** (remaining pyflakes
  items are benign unused-import/unused-local counts in tests/tools).

**Step 3/4 result — no further defects found.** pyflakes: 0 undefined names across lambdas/scripts/
tools/tests. Main-flow key/field review (config load, manifest/column-mapping/rowcounts, range
status/result/ledger, DSQL connect): writer↔reader names all agree — reconcile's
`_file_status/<label>.range-*` & `_range_result/<label>.range-*` prefixes match the loader's
`_mr_file_status_path`/`_mr_result_path` (`_safe`==`_mr_safe`); the per-table `config_prefix` is the
identical `table_cp_uri` across mrl_plan rangeItems, RunRange `--config_prefix`, and reconcile; the
loader reads its per-table config via the copied manifest entry's absolute `config_s3_path` (the
still-present discovery config), and mrl_plan copies `_manifest_index.json` + column-mapping +
rowcounts into the single-table prefix. Drove the import-smoke test to the first Spark call
(Spark/awsglue not installable offline); main-flow beyond that needs a live Spark session.

## 18. Amendment 18 — Spark mrl-load got no DSQL-aware boto3 (live HIGH) + capability rules

**What was wrong:** the glueetl `mrl-load` job's `boto3.client("dsql")` (DSQL IAM token) resolved
against Glue 4.0's bundled botocore, which predates Aurora DSQL → `UnknownServiceError: Unknown
service: 'dsql'` (live T14G, both tasks; ran 123-150s then failed in the writer; targets stayed 0).
`mrl_ensure_job` built the job with only `--extra-py-files` (driver-fullload: pg8000 stack, NO
boto3) and NO `--additional-python-modules`. The pythonshell count/probe job worked because its
prepared driver-cdc set bundles boto3-1.42.97.
**Why offline tests missed it:** boto3's service list is a property of the Glue runtime image, not
the code; nothing compared the MRL job's module delivery with the original's.
**Fix (reuse the original mechanism):** `mrl_ensure_job` now sets `--additional-python-modules` =
boto3/botocore/s3transfer wheels from the driver-cdc set (passed as `cdcExtraPyFiles` =
`$.driverCount.extraPyFiles` in EnsureMrlLoadJob), keeping `--extra-py-files` the pg8000 stack only
(boto3 there breaks botocore's data-dir under Spark). `_SPARK_BOTO3_PREFIXES` + `_spark_boto3_modules`
are a BYTE-IDENTICAL copy of `create_glue_jobs` (sync test). Fails closed at job creation with the
original message if no boto3 wheel is found. The cdc=true route (reused CreateGlueJobs) is unchanged.

**New rules so this class can't recur:**
- `test_a18_dsql_boto3.py`: job-def has `--additional-python-modules` with s3 boto3/botocore/
  s3transfer URIs and `--extra-py-files` has NO boto3; mutation (remove the block) fails; byte-
  identical sync vs `create_glue_jobs._spark_boto3_modules`; parity (same output for the same
  input); a **general capability rule** — every MRL Glue script that creates a `dsql` client must
  have its job deliver a dsql-aware boto3 (glueetl → additional-python-modules; pythonshell →
  prepared driver-cdc extra-py-files); botocore CreateJob validation with the new arg; deploy
  prerequisite now requires boto3+botocore+s3transfer in driver-cdc/ (not just any `.whl`).

**Step 3/4 — no further defects found.** Diff of `../scripts/job2_load.py` (production-proven) vs
the MRL copy: the only differences are the MRL `_mr_*` / `_MANUAL_RANGE` / `_apply_manual_range_overrides`
blocks and the function wrappers; reviewed line by line — all reference defined names (pyflakes 0
undefined), and the per-range `_file_status/<label>.range-*` + `_range_result/<label>.range-*` doc
keys/fields (`failed`/`first`/`last`/`files[].state/rows`) match exactly what `mrl_reconcile` reads
(`_safe`==`_mr_safe`; identical `config_prefix`). Drove the real chain offline
(`test_a18_writer_reconcile.py`): synthesized the exact writer docs → real `mrl_reconcile.handler`
(allClean, merged `_file_status/<label>.json` in the ORIGINAL `load_file_status` format) → real
`mrl_cdc_handoff` (fork-prefix `_load_status.json`); a failed range-result makes reconcile fail
closed. Could not execute Spark itself (not installed); drove everything up to the Spark boundary.

## 19. Amendment 19 — reconcile-false-fail + hex plan (two live HIGHs)

**First live mrl-load COMPLETION (A18 confirmed):** boto3/botocore/s3transfer installed from the
prepared S3 wheels (no PyPI), `boto3.client('dsql')` worked, 3,009 rows written.

**Defect 1 (PROVEN from t14i_evidence/, not guessed) — reconcile fails closed though data is
correct.** The get-job-run Arguments show range mode WAS requested (`--load_file_start/--load_file_end/
--mrl_range_index` present) and the output log confirms `MANUAL RANGE MODE ON`. BUT the small table
(1.57 MB < 10 MiB) was routed to the **V5 whole-table path** (`routing: ... < 10,485,760 B gate ->
V5 whole-table path`; `[WORKER] ... whole_table`), which writes the whole-table `_load_status.json`
and **NO** per-range `_file_status/<label>.range-*` or `_range_result/*` docs (no `result ->` line in
the log). reconcile proves coverage ONLY from those per-range docs → `missing=[LOAD00000001.csv]`,
`allClean=false`. **Fix:** `load_table_auto` now takes the PER-FILE path (`load_one_table_chunked`,
which writes per-file status + the `_range_result` doc) whenever `_MANUAL_RANGE["on"]`, BEFORE the
byte-size/master-switch gates. **Silent success made impossible:** an end-of-job guard FAILS LOUDLY
if range mode was requested but no per-range result doc was written (or if a range arg was passed but
range mode never activated). Reconcile stays strictly fail-closed (coverage must come from the range
docs, never from `_load_status.json`) — documented. The old 'writer↔reconcile contract' test
synthesized docs from the code's constants and so could not see that the live job wrote none; the new
`test_a19_range_routing.py` + `test_a18_writer_reconcile.py` close that gap.

**Defect 2 — plan fails for >9 LOAD files (hex names).** DMS names files in HEX (LOAD0000000A..);
`mrl_preflight` set `splitAllowHex` default **False**, so the plan dropped hex names while the loader
(loose prefix glob) takes them → `assert_plan_equals_loader_set` 'the loader WOULD load ...
LOAD0000000A.csv'. **Fix:** `splitAllowHex` is now an opt-OUT (default **True**) in preflight, and the
`assert_plan_equals_loader_set`/`resolve_table_folder` signature defaults are True; every fail-closed
rule stays (uniform width, uniform CASE, allow-list, CDC deny-list, non-empty). Name-sorted order ==
numeric order for uniform-width upper-case hex (proven at crossing points 9→A, F→10, FF→100, and for
406 files → LOAD00000196), and the loader's name-based `_mr_resolve_slice` picks exactly the planned
hex files (it matches by NAME, not the decimal `_mr_file_number`). The original whole-table loader
sorts by SIZE; MRL range mode uses NAME order (documented). Production-shape tests
(`test_a19_hex_and_range.py`): 12-file t14i-a (c=4), 406-file (c=3→136,135,135), 1-file t14i-b (c=2);
simulator scenarios 22 (12-file hex) + 23 (406-file hex).

**Why offline missed both:** Defect 1 — no test drove the real small-table routing (AST-extract tests
never ran the whole-table vs per-file dispatch). Defect 2 — the split property tests used DECIMAL
names only and `assert_plan_equals_loader_set` was called with the (then-default-False) hex flag.
Added a **drift-checklist test** (`test_drift_checklist.py`) with one executing assertion per known
real-AWS shape (hex names, empty-string args, pythonshell no --JOB_NAME, Glue4 botocore lacks dsql,
TimeoutSecondsPath, module-top import-safe, ResultSelector carries referenced paths, no silent
success without range artifacts). Post-Reconcile states (CountVerify/StateCounted/CDC tail) are
exercised end-to-end through the real handlers by the simulator (cdc=false + cdc=true scenarios); no
`$.X` read without a producer. `split_allow_hex` kept as a strict-validated opt-out in the params file.

---

## Stale counts (F3, review A — corrected release-2)

All suite/scenario counts in this historical build record predate release-2. The current,
authoritative counts for the published folder are **268 passed** (`python3 -m pytest tests/ -q`)
and **27 scenarios, 27 passed** (`python3 tools/mrl_simulate.py`). Install the dev/test
dependencies first: `python3 -m pip install -r requirements-dev.txt` (findings F1/F2).
