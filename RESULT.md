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
