# MRL PIPELINE — RESULT

A NEW, SEPARATE Step Functions pair (parent `mrl-fleet` + child `mrl-task`) that automates the
manual file-range full load from `customer_patches/job2_load_manual_range/`, reusing the existing
pipeline's Lambdas/Glue jobs where safe and adding its own. The existing startup/cutover/fleet
machines, Lambdas and Glue jobs are NOT changed. Everything new lives under
`parallel_range_load_pipeline/`; no existing repo file was edited.

See `DESIGN.md` for the prefix layout, concurrency/safety analysis and reuse map, and `README.md`
for the operator guide.

## Deliverables

- `stepfunctions/mrl-task.asl.json`, `stepfunctions/mrl-fleet.asl.json` — the child + parent.
- `lambdas/` — `mrl_preflight`, `mrl_plan` (the splitter heart), `mrl_reconcile`, `mrl_gate_eval`,
  `mrl_ensure_job`, `mrl_state`, `mrl_fleet_preflight`, the pure core `mrl_split`, and the
  bundled resolver copy `mrl_loader_resolver` (for the plan's resolver-consistency proof).
- `scripts/` — `job2_load_manual_range.py` (the patched loader, produced by applying the
  delivered patch to the repo's `scripts/job2_load.py`) and `mrl_count.py` (standalone
  count/probe Glue python-shell job).
- `iam/mrl-fleet-sm-optional.json` — SFN grants ONLY for the optional fleet state machine; the default deployment needs no IAM change
  (manage_iam=false).
- `tools/` — `deploy_mrl.sh` (portable, idempotent, `--dry-run`), `mrl_status.py` (read-only),
  `mrl_simulate.py` (THE TESTER SCRIPT).
- `tests/` — the pytest suite + the ASL interpreter `asl_sim.py`.
- `config/` — example `fleet_task_parallel_load.csv` and `params_parallel_load.csv` (MRL-named; MRL never writes the existing pipeline's `config/fleet_tasks.csv` / `config/params.csv`).
- `README.md`, `DESIGN.md`, `RESULT.md`, `LIVE_TEST_STEPS.md`.

## Verified by EXECUTION (offline; no AWS, no Spark, no network)

- **pytest suite: 79 passed, 0 failed** (`python3 -m pytest tests/ -q`). Covers:
  - Splitter property tests: n in 0..300 × c in 1..64 exhaustively (sizes, balance,
    contiguity, inclusivity, determinism), the required examples
    (`5,3→2,2,1`; `7,3→3,2,2`; `4,4→1,1,1,1`; `3,5→1,1,1`; `1,10→1`; `406,4→102,102,101,101`;
    `0→[]`), random n up to 20000, and `c<1` fail-closed.
  - Listing tests with a fake S3: pagination across 5 pages (406 files), processed/failed and
    sub-folder exclusion, hex names gated behind `split_allow_hex`, zero-byte files included +
    reported, non-LOAD files ignored, duplicate-basename fail-closed, mixed-width fail-closed,
    case-insensitive folder resolution, two case-variant holders fail-closed.
  - Resolver-consistency (proof (c)): the REAL `_mr_resolve_slice` extracted from the patched
    loader re-resolves every MRL range to EXACTLY the assigned files (examples + 200 random).
  - Reconcile: complete/missing/duplicate/failed/extra; merged normal doc accepted by the REAL
    `load_file_status` extracted from the patched loader.
  - Count verdict: DMS name mapping (case-insensitive, renamed schema), equal match, off-by-one
    mismatch, tolerance, timeout fail vs warn verdict, single-PK detection.
  - NULL options: valid accepted, malformed rejected at preflight naming the entry, and the
    preflight uses resolve_task's EXACT parser (same function object — no copy drift).
  - Preflight: per-mode DMS settings (cdc needs StopTaskCachedChangesApplied; full_load_only
    accepts a full-load task), AddColumnName + bucket checks, adopt_stopped preconditions
    (accepts NORMAL stop at 100%/0-errored; rejects <100% and errored), capacity
    impossible-setting fail.
  - Plan persistence/re-run: same listing → same plan (reuse); changed listing → fail closed;
    n=0 with FullLoadRows 0 → loaded-empty; n=0 with FullLoadRows>0 → fail closed; single-table
    config prefix created with exactly one table (copied from discovery, not re-derived);
    composite table → has_single_pk false.
  - ASL audits (reusing the repo's audit engine): static path audit (no unguarded path read),
    reachability, every Choice has a Default — on BOTH machines.
  - Production guards: EVERY `glue:startJobRun[.sync]` Arguments value resolves to a `str` on
    BOTH machines (the numbers-must-be-strings rule); history-event budget stays far below
    25,000 at the max poll budget; the LoadRangeMap input carries range descriptors only (no
    file list) and a 3,000-file plan's Map input is < 256 KiB.
  - Mutation checks: disabling the union proof, the uniform-width check, the unique-basename
    check, or the processed/failed listing exclusion each removes the corresponding protection
    (the guards are load-bearing). The delivered patch's own overlap/start-after-end/no-PK
    mutation checks still pass against the MRL-generated patched loader.
  - Fail-closed hygiene: a planted PII-like value never appears in any MRL fail-closed message;
    no MRL source interpolates a row-value variable into a raised message; `listing_sha256` is
    Linux-safe (names only; no strftime/locale/timezone).
  - IAM lint: valid JSON, no `_comment`/stray keys inside statements, only the allowed
    placeholders, no literal 12-digit account id.
  - Copy-sync: the bundled resolver equals the patched loader's resolver functions.
- **ASL interpreter self-check (5 passed)** including a replay of the EXISTING
  `stepfunctions/startup.asl.json` on a happy path that reaches `TaskSucceeded` — this validates
  the interpreter itself against the real production machine.
- **THE TESTER SCRIPT `tools/mrl_simulate.py`: 21 scenarios, 21 passed** — complete runs through
  BOTH MRL machines against fakes (fake DMS stop-reason timeline, fake S3 with real-looking LOAD
  listings, fake Glue recording every started run + Arguments and able to fail a run, simulated
  DSQL counts), driving the REAL MRL child ASL and the REAL MRL Lambda handlers. Covers brief
  section 11 scenarios: (1) full_load_only 406/c4→102,102,101,101; (2) 5/c3→2,2,1;
  (3) full_load_and_cdc ending in DMS resume + CDC start; (4) two tasks in one fleet (fleet
  preflight); (5) adopt_stopped; (6) wrong stop reason → DmsFailed; (7) TablesErrored>0 →
  DmsFailed; (8) a range fails → MrlLoadFailed and (8b) a re-run resumes only the failed range;
  (9) listing drift → MrlPlanFailed; (10) count mismatch by 1 → MrlCountFailed; (11) count
  cannot complete → fail (and warn mode passes); (12) non-empty target without a plan →
  SafetyGateFailed; (13) malformed null_rules → PreflightFailed; (14) duplicate basename across
  case-variant folders → MrlPlanFailed; (15) empty+FullLoadRows=0 ok, empty+FullLoadRows>0 fail;
  (16) throttled Glue start retried; (17) fleet re-trigger while a child is RUNNING → skipped;
  (18) 3,000 files c=10 (payload/event-budget proof). A readable transcript is printed per
  scenario and a PASS/FAIL summary table; the process exits non-zero on any failure.
- The delivered manual-range patch applies cleanly to the repo's `scripts/job2_load.py` and the
  result compiles; the delivered patch's own 55-test suite passes against the MRL-generated
  patched loader.

## Verified by READING only (NOT executed here — no AWS, no Spark, no DSQL)

- That Step Functions resolves `MaxConcurrencyPath`, `startExecution.sync:2`, the `aws-sdk:*`
  service integrations and the intrinsics exactly as the asl_sim models them: the interpreter is
  a faithful but offline model; the real service was not exercised. The startup.asl.json replay
  reduces interpreter risk but is not a live run.
- That the reused Lambdas (`resolve_task` build_table_list, `create_glue_jobs` create,
  `driver_discovery`) behave in production as their code reads: MRL calls them with the SAME
  payload shapes `startup.asl.json` uses (verified by reading both), but they were simulated, not
  invoked against AWS.
- That `job1_discovery` writes the per-table `_manifest_index.json` entry and
  `<table>_column_mapping.json` MRL copies: verified by reading discovery's writer; MRL copies
  those objects verbatim rather than re-deriving them.
- That the patched loader's wrappers intercept the loader at run time inside Glue, and its
  per-file resume / probe-redo / no-PK fail-closed semantics hold against real DSQL: relied upon
  from the delivered patch (its RESULT.md records this was read, not run live).
- The DSQL PrivateLink/public failover, the count job's DSQL connection and `pg8000` behaviour,
  and S3 conditional-put (`If-Match`/`If-None-Match`) semantics: modelled by the fakes; real
  botocore/endpoint behaviour was not exercised live.
- The exact DMS stop-reason strings per mode were taken from the existing `startup.asl.json` /
  `resolve_task` and AWS DMS docs (`STOPPED_AFTER_CACHED_EVENTS`, `STOPPED_AFTER_FULL_LOAD`,
  `FULL_LOAD_ONLY_FINISHED`); not confirmed against a live task. MRL additionally gates on
  `FullLoadProgressPercent==100 && status==stopped`, so a wrong string alone cannot pass a load.

## Remaining risks (honest)

1. **Live AWS behaviour unexercised.** The whole build is offline. `LIVE_TEST_STEPS.md` gives the
   real-AWS test plan (small table, then big, then cdc, then adopt_stopped, plus the failure
   injections that cannot be done offline). The `startExecution.sync:2` fleet integration, the
   `aws-sdk:*` DMS/S3 calls, and the Glue job types should be confirmed on the first real run.
2. **`FULL_LOAD_ONLY_FINISHED` stop reason.** Accepted for full_load_only in addition to
   `STOPPED_AFTER_FULL_LOAD`. The exact string for a pure full-load task should be confirmed on
   the first live full_load_only run; the `FullLoadProgressPercent==100 && stopped` gate protects
   against accepting a non-finished task regardless.
3. **`_load_status.json` is a shared table-level object** written by the loader's `record_outcome`
   (best-effort, last-writer-wins). MRL never reads it for a decision (correctness rests on the
   per-range `_range_result` docs + RECONCILE), so concurrent ranges of one table writing it is
   harmless — but it is a shared writable object; see DESIGN.md §3 / decision D4.
4. **Overlap-guard ledger is advisory in MRL** (DESIGN.md D9): MRL ranges are generated-disjoint
   and triple-proven, and RECONCILE refuses to write the merged normal doc if any file is covered
   more than once, so an overlap can never silently reach the pipeline. The patch's per-operator
   overlap guard remains but is not load-bearing here.
5. **CDC fork tail.** The MRL cdc tail starts the MAIN CDC job and confirms its marker. Composite
   (ck) and big-table (bg) fork CDC jobs are out of MRL's minimal scope — they are handled by the
   existing startup/cutover fork tooling (DESIGN.md §9). A task with composite/big forks that
   needs fork CDC after an MRL cdc load should be driven through the existing startup for the fork
   CDC, or MRL should be extended (a deliberate scope decision, not an accident).

## Decisions for the user (chose the safest option; see DESIGN.md for the rationale)

1. **D1** Split/sort ordering is lexicographic-by-name (matches the loader resolver). MRL asserts
   all LOAD names are the same width and fails closed otherwise.
2. **D2** Listing accepts decimal `^load0*[0-9]+\.csv$` by default (strictest, == merge_ranges);
   hex names require `split_allow_hex=true`.
3. **D3** Per-table config is COPIED from discovery output, never re-derived.
4. **D4** `_load_status.json` is a table-level best-effort doc MRL never reads for a decision.
5. **D5** full_load_only completion accepts `*STOPPED_AFTER_FULL_LOAD*` or
   `*FULL_LOAD_ONLY_FINISHED*` with `FullLoadProgressPercent==100 && status==stopped`.
6. **D6** `adopt_stopped` accepts any stop reason (incl. `NORMAL`) when 100% + 0 errored + all
   tables `Table completed` (the 2026-10-14 case).
7. **D7** `split_by` defaults to `count`; `bytes` is an optional trivial path.
8. **D8** No automatic NOT-NULL detection; an optional read-only `tools/gen_null_rules.py` helper
   is the only offered convenience (not shipped as automatic detection).
9. **D9** The overlap-guard ledger is advisory in MRL; the plan's disjointness proof + RECONCILE
   are the real guard.
10. **D10** MRL writes under its own `mrl/` S3 subtree and its own single-table config prefixes;
    it never writes the existing pipeline's `config/_task/<suffix>/` objects (except the owner
    marker/index that resolve_task itself manages), so the two pipelines cannot collide.
11. **D11 (scope)** The MRL cdc tail starts only the MAIN CDC job; composite/big fork CDC is left
    to the existing tooling. If you need MRL to also start fork CDC jobs, say so and it will be
    added (it would reuse EnsureForkJobs + the startup fork-CDC Map).
12. **D12 (count job engine)** The count/probe job is a Glue python-shell job (no 15-min Lambda
    limit) with its OWN standalone count function copied from the delivered count-only patch;
    `count_timeout_action` defaults to `fail` (never mark success on a count that did not run).

## How to re-run the offline proof

```
cd parallel_range_load_pipeline
python3 -m pytest tests/ -q                 # 79 passed
python3 tools/mrl_simulate.py               # 21 scenarios, all PASS (prints transcripts)
python3 tests/test_asl_sim_selfcheck.py     # interpreter self-check + startup.asl.json replay
```
