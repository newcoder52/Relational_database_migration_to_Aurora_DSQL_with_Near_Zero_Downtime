# RESULT — B19 + B18 fixes

Base: GitHub `main` HEAD **`5e4adf8`** (verified with `git ls-remote`; built on the real HEAD).
Branch work in `~/Downloads/review3d/fix-b19/repo` (fresh clone). Offline only — no AWS account
touched. Small, focused change; `@guardrails` can rebase onto this.

---

## B19 (HIGH) — cutover `CutoverResolveTask` reads Lambda fields resolve_task never returns

### Root cause
`stepfunctions/cutover.asl.json` → `CutoverResolveTask` `ResultSelector` read
`$.Payload.compositeCdcJobName` and `$.Payload.hasCompositeTables`, but `lambdas/resolve_task.py`
(cutover mode) returns neither (`grep` = 0 hits; it emits `cdcJobName` + `maxCompositeForks`).
Step Functions hard-fails a `.$` reference to a missing path → `States.Runtime` at
`CutoverResolveTask`, **before DMS is stopped**. This is the same class as the original B11,
incompletely fixed by `2ce03a2` (which added the fields to the SM but not to the Lambda output).

### Decision & fix (chose: REMOVE the reads)
After the per-table fork redesign, cutover finds a task's fork CDC jobs (`ck-*` / `bg-*`) by
**exact tag + the per-task `_jobs.json` registry** — `ListForkCdcJobs` (mode `list_fork_cdc`) →
`StopForkCdcRuns`. `$.resolved.compositeCdcJobName` / `$.resolved.hasCompositeTables` are **read
nowhere downstream** (confirmed: `grep` of `resolved.compositeCdcJobName` / `resolved.hasCompositeTables`
in cutover = 0 hits). So the two fields are **obsolete dead reads**, not missing data.

→ **Removed the two reads from the `CutoverResolveTask` ResultSelector.** The Lambda is left
unchanged (nothing should start returning fields the fork design no longer uses). Minimal and
correct; `startup` never read them either, which is why startup was unaffected.

### Test gap that let it through, and the fix
`tests/test_asl_paths.py` audits `$.path` *reachability* but treats a Lambda `$.Payload` result
as OPAQUE, so any `$.Payload.<field>` read passed. New audit closes it:

- **`tests/test_asl_payload_contract.py`** — for EVERY state (all 4 state machines incl.
  fleet-startup and fleet-cutover) with a `ResultSelector` reading `$.Payload.<field>`: map the
  `FunctionName` placeholder → Lambda file, read the `Payload.mode`, **statically scan that
  Lambda's returned keys for that mode** (return/`.update`/`out[...]=` dict keys, return-aware),
  and assert every read field is returnable. Audits 16 Lambda-ResultSelector states; covers
  fleet-startup + fleet-cutover `Preflight` and the startup/cutover resolve/create/drain states.
- A **fake-invoke** of `resolve_task` in cutover mode (task WITH composite tables and WITHOUT),
  then runs the real cutover ResultSelector against the output — must resolve with no missing
  `.$` path.
- `tests/test_asl_paths.py` now **also runs** the payload-contract audit, so the single ASL
  suite enforces it.
- Negative-tested: re-adding the two reads makes the new audit FAIL (6 checks); removing them PASS.

---

## B18 (HIGH) — big-table validation `StackOverflowError` + 300 s blowup; now scales to ~1B rows

### Root causes (two, both in `scripts/job3_validate.py`)
1. **Per-value md5 recomputed ~6× (the dominant cost).** `target_range_summary` built one SELECT
   whose per-column hash term, `_hash_sql()`, expanded to 6 strpos terms, **each calling
   `md5(col::text)` again** (`substr(md5(x), i, 1)` for i=1..6). For a 30-text-column table that
   is ~180 md5 calls/row → a 50k-row range ≈ **9M md5 calls**, which is what burned DSQL's 300 s
   transaction-age limit. (A plain `count(*)` over 23M rows in ~90 s is not comparable — it does
   no per-value work.)
2. **`StackOverflowError`.** `source_range_summaries` bucketed source rows with an **N-deep
   `F.when(...).otherwise(...)` chain — one nested `when` per range**. At 8M rows / 10k per range
   ≈ 800 ranges, the Catalyst expression tree overflowed the JVM stack in `collectToPython`.

### Fixes
1. **md5 once per value (derived table).** `_build_target_sql` now wraps the scan:
   `SELECT count(*), <outer aggregates> FROM (SELECT *, md5(<col>) AS h__k, ... FROM t WHERE pred) s`.
   The hash metric projects `md5(col)` once per row; the outer `SUM` applies the **same** 6-hex-digit
   strpos→int conversion to the alias `h__k`. **Verified numerically identical** to the old form
   (test: `4074940024 == 4074940024`) with md5 run **once** per value (500 vs 3000 calls on 500 rows).
   Spark side unchanged.
2. **Metric pruning.** When a column's per-value hash is active, text/char **MIN/MAX are dropped**
   (the hash sum already changes if any value changes; `length` catches truncation) — removing two
   `::text` casts + an ordering per range. MIN/MAX are **kept** only when hashing is off for that
   column (then they are the content signal). Remaining checks and what each catches are documented
   in `build_metrics`.
3. **`validate_hash = all | keys | off`** (default `all`), with the single-md5 query. `keys` hashes
   only PK/key columns; `off` uses count+length+min/max — for very wide / LOB tables.
4. **Bounded Spark plans.** `source_range_summaries` replaces the `F.when` chain with a **broadcast
   range-join** (`key ∈ [lo,hi)` against a tiny range-descriptor DF) — a single plan node whatever
   the range count — and, as a cap, processes at most `VALIDATE_SOURCE_RANGES_PER_PLAN` (50) ranges
   per Spark action. The whole-table case is one grouped aggregate (no per-range expression).
5. **Parallel range workers.** Per-range TARGET (DSQL) queries run on a driver thread pool sized by
   `_default_parallelism()`: from the validate worker type/count, **hard-capped by `conn_budget` and
   1/8 of DSQL's 10,000-connection cluster limit** so validation can never exhaust the cluster.
   `validate_parallelism` (0 = auto) overrides.
6. **Adaptive range size by TIME.** `_AdaptiveRangeSizer` seeds from a width estimate +
   `validate_rows_per_range` and grows/shrinks each range from the MEASURED seconds/range toward
   `validate_target_seconds_per_range` (5–20 s band, << 300 s). `validate_rows_per_range` is now only
   a **starting seed**; the hard cap is `VALIDATE_MAX_ROWS_PER_RANGE` (5M) so the sizer can GROW to
   hit the target on fast tables. A `_probe_and_size` measures one small range before planning.
   Auto re-split on timeout (B14) unchanged.
7. **Throughput logging.** Each table's result now carries `throughput` (ranges, parallelism,
   rows/range, rows/s estimate, slowest range) and the per-table log line prints it.

### Content check design (what remains, bytes/range)
Aggregate (not row-by-row): per range the DSQL query returns **one row** of `count(*)` + a handful
of per-column scalars (counts, sums, lengths, one hash-sum per hashed column, min/max only when a
column isn't hashed). **Expected payload ≈ a few hundred bytes/range**, independent of range row
count — so round-trips are tiny and parallelism, not payload, is the lever. (A pure swap of two
values within one range can still cancel in additive sums — unchanged limitation.)

### Estimated per-range cost — BEFORE vs AFTER (ESTIMATES)
30-column table (~20 text/uuid columns hashed), 50k-row range:
- **BEFORE:** ~20 cols × 6 md5/value × 50k ≈ **6M md5 calls/range** (+ `::text` casts for MIN/MAX),
  routinely **> 300 s** → DSQL txn-age failure (the B18 symptom).
- **AFTER (single md5, MIN/MAX pruned):** ~20 md5/value × 50k ≈ **1M md5 calls/range** (~6× fewer),
  **est. ~5–15 s/range** — in the adaptive target band. (Estimate; real time depends on row width
  and DSQL load. Operators can time one range with the EXPLAIN ANALYZE snippet in RUNBOOK §8.)

### Estimated throughput (ESTIMATES)
- Per range ~ `rows_per_range / seconds_per_range`; the sizer targets ~12 s/range.
- Table rows/s ≈ `parallelism × (rows_per_range / seconds_per_range)`.
  With parallelism 32–64 and ~250k rows per ~12 s range → **~0.6–1.3M rows/s** sustained on a
  wide table, far above the **≥ 50k rows/s** floor. **1B rows** at these rates ≈ **13–25 min** of
  query time (and well under 5.5 h even at the 50k/s floor). Numbers are estimates; the validate
  log prints the measured rows/s per table.

### Tests (`tests/test_b18_validate_throughput.py`, 27 checks)
- Simulated **8M / 100M / 1B** tables: max ranges per Spark plan ≤ 50 (bounded; no stack blow-up),
  every range summarized, chunk count as expected.
- Parallelism sized from the worker and **never exceeds `conn_budget`** nor 1/8 of the 10,000-conn
  cluster limit; an explicit over-ask is clamped.
- Adaptive sizer **converges** to ~12 s/range (and shrinks on a slow range).
- **Single-md5 identical numbers** to the old per-digit form, md5 evaluated once per value.
- Fake-DSQL **throughput**: parallel path ~14–15× faster than sequential.
- Existing `tests/test_fix6.py` / `tests/test_e2e_fixes.py` updated to extract the new
  `_build_target_sql` helper alongside `target_range_summary`.

### Settings (params.csv → pipeline.json → resolve_task → ASL → validate job args)
New keys (defaults): `validate_parallelism=0` (auto), `validate_target_seconds_per_range=12`,
`validate_hash=all`. `validate_rows_per_range` (10000) is now a **starting seed / soft start**, not
a tuning knob. Wired through `params_csv.py`, `resolve_task.py` (emit + validate), the startup ASL
`ResolveTask`/`CreateGlueJobs`/`EnsureForkJobs`, and `create_glue_jobs.py` (validate + ck-validate
job args). `conn_budget` is now also passed to the validate job (caps parallelism).

### Docs
- RUNBOOK §3 + MANUAL_SETUP §3/§3c + `params.example.csv` + `pipeline.example.json`: document the 3
  new keys; `validate_rows_per_range` reworded as a start/cap, not a StackOverflow knob.
- RUNBOOK §8: the big-table validate row now says StackOverflow is **handled** (bounded plans +
  single-md5); explicitly **removed advice to raise `validate_rows_per_range`** — raise
  `validate_parallelism` or set `validate_hash=keys|off` instead. Added an **EXPLAIN ANALYZE**
  snippet so an operator can time one range query (new single-md5 vs old 6×-md5 form) on their table.

---

## All suites green (offline)
`test_asl_paths` (incl. payload-contract), `test_asl_payload_contract` (60), `test_b18_validate_throughput`
(27), `test_b17_b13` (24), `test_docs_params`, `test_e2e_fixes` (55), `test_existing_roles` (66),
`test_fix6` (51), `test_planning_settings` (58). 9/9 suites pass.

## Commit
- Author: `newcoder52 <aash.798@gmail.com>`, plain `git push` (osxkeychain). No tokens/API, never force.
- Commit SHA: **`dd88d83`** (parent `5e4adf8`); pushed `5e4adf8..dd88d83` to `main`; verified in
  a 2nd fresh clone (HEAD `dd88d83`, all suites green).

## Runtime files changed
- `scripts/job3_validate.py` — B18: single-md5 derived table, metric pruning, `validate_hash`,
  bounded broadcast range-join (StackOverflow fix), parallelism sizing, adaptive time sizer,
  throughput logging, new args.
- `lambdas/resolve_task.py` — B18 settings defaults + validation + emit (`validateParallelism`,
  `validateTargetSecondsPerRange`, `validateHash`).
- `lambdas/create_glue_jobs.py` — pass `validate_parallelism` / `validate_target_seconds_per_range`
  / `validate_hash` / `conn_budget` to the validate and ck-validate jobs.
- `lambdas/params_csv.py` — B18 settings defaults, PIPELINE_KEYS, numeric spec.
- `stepfunctions/cutover.asl.json` — **B19:** removed the two obsolete `$.Payload` composite reads.
- `stepfunctions/startup.asl.json` — B18: carry the 3 new validate settings resolve→create/ensure.
- (`glue-templates/` unchanged — validate args are injected by `create_glue_jobs`, not baked in.)


---

# (appended) RESULT — data-safety guardrails (G1–G10)

This work was rebased on top of the B19/B18 fixes above. The guardrails RESULT follows.

Built offline (no AWS touched) on a fresh clone of
`github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime`.
Remote HEAD at start was `5e4adf8` (confirmed with `git ls-remote`), and this work was built on
that HEAD. The final commit sha is recorded in `DONE` (and at the bottom of this file) after the
rebase onto whatever `main` was at push time.

Goal: whatever the (unconfirmable) cause of the weekend incident where DSQL tables went to 0 /
far below the DMS/source counts, the pipeline must **refuse, or stop and flag the table**, never
silently delete — and a loss must be detected within one CDC poll cycle. The scenario matrix is
`WHAT_IF.md`; one+ test per row is in `tests/test_guardrails.py`.

## The guardrails, defaults, and why

| Guard | What it does | Default | Why that default |
|---|---|---|---|
| **G1** No destructive op once CDC started | `job2_load` refuses ANY whole-table/range blank once CDC has started for the table. Checks the task's `_cdc_started` S3 marker AND the table's `cdc_status` row; **fail-closed** if either can't be read | on | A reblank after CDC has applied deltas is exactly the way the whole post-full-load delta set is silently wiped. Fail-closed because "can't tell" must not mean "go ahead and delete" |
| **G2** Destructive blank gated to a workflow / explicit manual flag | A manual (no-workflow) run may still WRITE (empty-table load, per-file resume that deletes nothing). A destructive **blank** in a manual run is refused unless `--allow_manual_destructive=true`; a RUNNING Step Functions execution blanks without the flag. CDC is **not** gated by G2 (CDC never empties a table) | manual blank refused without flag | Operators MUST be able to run jobs by hand (restart CDC after the 7-day timeout, re-run a load/validate). Only the irreversible action is gated, and only with an explicit, audited opt-in |
| **G3** Per-table lock | Before writing, load/CDC take a per-table lock row in `cdc_control.cdc_control_lock` (conditional INSERT / OCC; stale locks expire after the heartbeat timeout). A 2nd load **fails closed**; a 2nd CDC run **skips with a WARNING** (the position fence remains the correctness backstop) | on, 1800s stale timeout | One writer per table prevents concurrent reblank+reload (duplicates) and two DMS tasks / forks clashing on one target |
| **G4** Blank sanity | Before any blank: refuse unless the table was previously attempted by THIS task (owner/resume record), and refuse if its current count > expected `FullLoadRows × (1 + margin)` | on, margin 0.05 | A count above expectation means the table holds rows this run never owned (mis-targeted / shared table) — not ours to empty |
| **G5** Audit log | A row is written to `cdc_control.audit_log` BEFORE every destructive action (blank, range blank, mass delete, `_cdc_file` purge) and on every refusal: time, task, job, run id, execution id, table, action, rows before, rows deleted, reason. `CREATE TABLE` only, no DEFAULT-on-ALTER | always on | The actual weekend op was un-attributable. Writing first means even a crash mid-op leaves a record |
| **G6** CDC mass-delete guard | Per file, if net DELETEs would remove > `cdc_max_delete_fraction` AND > `cdc_max_delete_rows` of the table's current rows, the table is **blocked** and nothing from that file is applied | fraction 0.5, rows 100000 | Both thresholds must trip so a tiny table's normal churn never blocks, while a corrupt file deleting most of a large table does. Operator unblocks one file with `cdc_status.allow_mass_delete=true` after verifying vs source; fraction ≥ 1 turns it off |
| **G7** No-PK delete precision + exact purge | A no-PK content DELETE that would match more rows than its D ops require **blocks** the table (DSQL has no ctid / verifiable bounded delete). The `_cdc_file` purge is asserted to be an exact single-equality on the file key only | always on | Prevents a duplicate-row content DELETE over-deleting, and a future edit widening the purge from touching other files' rows |
| **G8** Ordering / high-water / gap / new-LOAD | CDC refuses a file at/under the high-water (replay/regression), a gap (a later file already done), or a new `LOAD*` file appearing after CDC started (a DMS reload under CDC) | always on | Catches a hand-reset `cdc_status` replaying from zero, skipped files, leftover old-run files, and a DMS "reload table" that would reblank/clobber the target |
| **G9** Drift detector | Every `cdc_drift_check_minutes` the CDC job compares the live DSQL count to `full_load_rows + inserts_applied − deletes_applied` (new counters on `cdc_status`, add-if-missing like B17). Beyond `cdc_drift_tolerance`: ERROR log, `audit_log` row, `DsqlRowDrift` CloudWatch metric, and (if `cdc_drift_action=block`) set the table `blocked` | check 30 min, tolerance 0, action warn | Detects a slow leak from ANY cause within one check window instead of days. tolerance 0 is exact for PK tables; raise slightly for no-PK. `warn` default so detection never itself stops the pipeline unless the operator opts into `block` |
| **G10** Validate & cutover vs DMS | Validate also compares the DSQL count to DMS `describe_table_statistics` FullLoadRows (not only the S3 source) and FAILs on a mismatch (`DMS_COUNT_DIFF`). Cutover's pre-check compares DSQL to `FullLoadRows + Inserts − Deletes` and refuses (`CountMismatch`) beyond tolerance unless an explicit override is given | tolerance 0 | A PASS that only checked the S3 source can hide a shortfall vs the authoritative DMS figure; cutover must not promote a short target |

Composite-key tables get the SAME guards: `scripts/glue_cdc_composite.py` carries the identical
helpers and wiring, and the shared pure helpers are **byte-identical** to
`scripts/glue_cdc_continuous.py` (enforced by `test_shared_helpers_byte_identical`). Spark CDC runs
the same scripts (templates reference them; `test_spark_cdc_uses_same_scripts`).

## New settings (flow params.csv → params_csv.py → pipeline.json → resolve_task → payload → job args)

`cdc_max_delete_fraction` (0.5), `cdc_max_delete_rows` (100000), `cdc_drift_check_minutes` (30),
`cdc_drift_tolerance` (0), `cdc_drift_action` (warn). Added to `OPTIONAL_DEFAULTS` / `PIPELINE_KEYS`
/ validation in `lambdas/params_csv.py`, to `SETTINGS_DEFAULTS` / `_validate_settings` / the payload
in `lambdas/resolve_task.py`, forwarded to every CDC job as `--cdc_*` in `lambdas/create_glue_jobs.py`
and wired through `stepfunctions/startup.asl.json`. Documented in RUNBOOK §3 (pipeline-key count
40 → 45), `config/params.example.csv`, `config/pipeline.example.json`, `docs/MANUAL_SETUP.md`.
`test_docs_params.py` passes (51 ALLOWED keys, 45 pipeline keys).

Load/validate run-flags (not pipeline.json settings): `--allow_manual_destructive`,
`--blank_guard_enabled`, `--blank_expected_margin`, `--startup_execution` / `--startup_execution_arn`
(load, G2), `--dms_task_arn` / `--count_mismatch_tolerance` (validate, G10).

## IAM change

`iam/glue.json` gains `states:DescribeExecution` (new `StatesDescribeExecutionForBlankGuard`
statement). G2's "is a workflow driving this run?" probe calls `describe_execution`; without the
permission the probe fails and the blank fails closed (safe), so a **customer-managed Glue policy
must also grant `states:DescribeExecution`** for a legitimate workflow-driven reblank to be
permitted. The B13 IAM allow-list test already includes this action. No other IAM change.

## Behaviour changes a running deployment would notice

- A load that previously auto-reblanked-on-resume now **also** passes G1/G2/G3/G4 first and writes
  G5 audit rows. On a normal workflow-driven resume (RUNNING execution, previously-attempted table,
  count within expectation, CDC not yet started) it behaves exactly as before. A **console/manual**
  reblank now refuses unless `--allow_manual_destructive=true` is passed.
- CDC now reads the table count before applying a delete-bearing file (G6) and runs a periodic
  drift check (G9) — a few extra read-only round-trips, not on the row hot path.
- CDC can newly set a table `blocked` for G6/G7/G8/G9 reasons (previously only schema/bad-row). The
  unblock is always `UPDATE cdc_control.cdc_status SET status='active' …` (RUNBOOK §"Safety
  guardrails").
- New `cdc_control` objects created on first run: `audit_log` (table), `cdc_control_lock` (table),
  and four `cdc_status` columns (`full_load_rows`, `inserts_applied`, `deletes_applied`,
  `allow_mass_delete`) added if missing (no DEFAULT-on-ALTER). The existing static B17 test still
  passes (no `ADD COLUMN ... DEFAULT` anywhere).
- Validate can newly FAIL a table with `DMS_COUNT_DIFF` (G10) and cutover with `CountMismatch`
  when the DSQL count disagrees with the DMS figure — only when `--dms_task_arn` is wired.

## Tests

`tests/test_guardrails.py` — 78 checks, one+ per WHAT_IF row (manual CDC allowed + lock respected,
manual load onto empty allowed, manual blank refused without the flag and allowed+audited with it,
an 80%-of-1M delete blocked, no-PK over-match blocked, out-of-order/gap/new-LOAD blocked, injected
drift fires + metric + audit, cutover count mismatch fails, every override). All existing suites
stay green: `test_asl_paths`, `test_b17_b13`, `test_docs_params`, `test_e2e_fixes`,
`test_existing_roles`, `test_fix6`, `test_planning_settings` — including `test_e2e_fixes.py` and
`test_b17_b13.py`. All offline; the fake DSQL enforces the 3000-row/txn cap.

## Commit

Final sha: see `DONE`. Built on remote HEAD `5e4adf8`, then rebased onto `main` at push time
(`dd88d83`, the parallel B19/B18 commit), keeping BOTH this work and the B19/B18 changes (RESULT.md
add/add kept both; RUNBOOK pipeline-key count merged to 48; create_glue_jobs + job3_validate kept
both sides' args/globals). Plain `git push` as `newcoder52 <aash.798@gmail.com>`, verified in a
second fresh clone.
