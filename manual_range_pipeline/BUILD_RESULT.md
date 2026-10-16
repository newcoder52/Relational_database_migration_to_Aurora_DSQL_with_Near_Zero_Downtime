# MRL PIPELINE — BUILD_RESULT

Builder deliverable for the Manual-Range-Load (MRL) Step Functions pipeline. All work lives only
under `manual_range_pipeline/`. No existing repo file was modified. No AWS calls; offline
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

- **`python3 -m pytest tests/ -q` → 114 passed, 0 failed** (incl. `test_deploy_iam.py`,
  `test_cdc_handoff.py`, `test_vetter_findings.py`, `test_amendments.py`, mutation/ASL-guard tests).
- **`python3 tools/mrl_simulate.py` → 22 scenarios, 22 passed** (incl. #19 CDC-ignored).
- ASL audits (string-arg Glue args, history/payload budget, startup-state byte-identity diff) pass.
- `deploy_mrl.sh --dry-run` clean; `bash -n` clean; `py_compile` on all `.py`; `json.load` on all
  `.json`.
- Only `manual_range_pipeline/` files changed at every commit (verified `git status` each time; no
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
cd manual_range_pipeline
python3 -m pytest tests/ -q
python3 tools/mrl_simulate.py
```

## 9. Round 2 findings and dispositions

Three independent round-2 reviewers returned **SAFE WITH THESE FIXES**. Every item is fixed with an
executing fail-before / pass-after test. Suite is **137 passed** (was 114) twice (PYTHONHASHSEED=0
and TZ=UTC LANG=C); simulator **23 scenarios** (adds `3b_cdc_fork_handoff`). All fixes committed
after `19a745b`.

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
