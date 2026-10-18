# MRL PIPELINE — DESIGN

Manual-Range-Load (MRL): a NEW, SEPARATE Step Functions pair (parent "fleet" + child "task")
that automates the manual file-range full load delivered in
`customer_patches/job2_load_manual_range/`, reusing the existing pipeline's Lambdas and Glue
jobs where safe and adding its own. It does **not** change or replace the existing
startup/cutover/fleet state machines (the customer runs those in production).

Everything new lives under `parallel_range_load_pipeline/` (this folder). No existing repo file is
edited.

---

## 1. Why this exists / what it does

The existing `startup` state machine runs a single whole-table load per table group (one Glue
`load`/`load-big` run per group). For one very large table (~406 part-files, ~110M rows,
~106 GB) a single driver fights its own thread pool. The delivered manual-range patch lets an
operator split that table into several **inclusive file ranges** and load each range with its
own Glue driver, then reconcile with `merge_ranges.py`. The patch is proven offline but is
**operated entirely by hand** (copy/paste `aws glue start-job-run`, manual DROP/recreate,
manual count verification).

MRL automates that hand-operated flow as a Step Functions pair:

- **Parent (fleet):** reads `s3://<bucket>/config/fleet_task_parallel_load.csv` (+ optional
  `s3://<bucket>/config/params_parallel_load.csv`) — in the SAME `config/` folder as the
  existing pipeline's files but under DISTINCT, MRL-specific names (AMENDMENT 1). It validates
  strictly, and runs one **child** execution per row with a bounded `MaxConcurrency`. Re-trigger
  safe; aggregates `{succeeded, failed, skipped}`; fails if any child failed (after the others
  finish). MRL **never** writes `config/params.csv`, `config/pipeline.json` or
  `config/fleet_tasks.csv` (the existing pipeline owns those); it reads its settings from
  `config/params.csv` READ-ONLY (via a byte-identical copy of the original parser) and writes only
  its own `config/pipeline_parallel_load.json`. Precedence: fleet row > `params_parallel_load.csv`
  > `config/params.csv`.
- **Child (task):** preflight → (optionally start) DMS → discovery → **PLAN** (list+split into
  ranges, persist `plan.json`) → **SAFETY GATE** (target empty or matching resume) → **LOAD**
  (a Map over (table,range) each `glue:startJobRun.sync` of a dedicated per-task range job
  built from the patched loader) → **RECONCILE** (merge_ranges logic) → **COUNT** (DSQL vs DMS
  FullLoadRows) → **mode tail** (full_load_only: stop; full_load_and_cdc: resume DMS + start
  CDC like `startup.asl.json`).

---

## 2. Canonical ordering decision (load-bearing)

The loader's `s3_list_table_files` returns files **largest-first by size**, but the
manual-range patch's `_mr_resolve_slice` and `merge_ranges.plan_ranges` both **re-sort by name
lexicographically** before slicing: `sorted(files, key=lambda t: str(t[0]))`. Therefore the
**canonical ordering for the MRL split is lexicographic by the full s3:// URI** (equivalently
by basename, since every file is in one folder). The splitter, the plan Lambda, the loader
patch and merge_ranges all agree on this one ordering. The resolver-consistency test
(`tests/test_resolver_consistency.py`) extracts the REAL patched `_mr_resolve_slice` and proves
every range's resolved first/last/file-set equals what the MRL splitter assigned.

**DMS full-load names are fixed-width decimal** (`LOAD00000001.csv`), so numeric order ==
lexicographic order for the real export. The brief also allows hex names
(`^LOAD[0-9A-Fa-f]+$`). We sort **by name** (not by parsed number) because that is what the
loader's resolver does; we additionally **assert all names are the same width** and fail closed
otherwise (a mixed-width set could make lexicographic != numeric). This is the safest option
and is recorded as decision D1.

### File-name acceptance (listing filter)

We mirror the loader's `_is_full_load_key` for exclusions (reject any key with a `processed/`
or `failed/` path segment; only keys DIRECTLY in the table folder) **and** apply the fixed
`merge_ranges` name rule for the basename. The brief asks for `^LOAD[0-9A-Fa-f]+\.csv$`
(case-insensitive, hex). The loader's own `_is_full_load_key` is looser
(`base.startswith("load") and base.endswith(".csv")`) and `merge_ranges` is stricter
(`^load0*[0-9]+\.csv$`, decimal). We adopt a filter that is **as strict as merge_ranges by
default** (decimal `^load0*[0-9]+\.csv$`) because that is the backstop that decides whether a
load reaches the pipeline, and additionally accept hex (`^load[0-9a-f]+\.csv$`) when the
operator sets `split_allow_hex=true` (params). The strict default means a stray CDC/timestamp
file can never be miscounted as a full-load file. Recorded as decision D2.

---

## 3. S3 prefix / object layout (the heart of the concurrency safety story)

All MRL **state** lives under `s3://<bucket>/mrl/`. The two MRL **input** files live in the
existing `config/` folder under DISTINCT, MRL-specific names (AMENDMENT 1). Nothing MRL writes
collides with the existing pipeline's `config/` objects: MRL never writes `config/params.csv`,
`config/pipeline.json` or `config/fleet_tasks.csv`, and its per-table single-table config
prefixes live under its **own** `mrl/state/.../config/` subtree (never under the existing
pipeline's `config/_task/...`), so the two pipelines can never read-modify-write the same object.

```
s3://<bucket>/
  config/
    fleet_task_parallel_load.csv            # MRL parent input (operator-authored), MRL-named
    params_parallel_load.csv                # optional MRL params (operator-authored), MRL-named
    params.csv                              # EXISTING pipeline's settings - MRL reads READ-ONLY
    pipeline_parallel_load.json             # MRL's OWN settings file (only config/ file MRL writes)
  mrl/
    state/
      <task-suffix>/
        state.json                          # phase, timestamps, execution name, counts
        plan.json                           # the frozen plan (sha256, counts, ranges, params)
        reconcile.json                      # reconcile verdict (written by RECONCILE)
        count_result.json / probe_result.json
        tables/
          <schema>.<table>/
            config/                         # ONE dedicated single-table config prefix per table
              _manifest_index.json          #   exactly ONE table (copied from discovery index)
              <dsql_table>_column_mapping.json
              table_rowcounts.json          #   FullLoadRows sidecar (copied) for count/sizing
              # Per-range writable objects the loader writes live UNDER this prefix:
              _file_status/<label>.range-<first>-<last>.json   # range-private progress doc
              _file_status/<label>.ranges.json                 # overlap-guard ledger (shared)
              _range_result/<label>.range-<first>-<last>.json  # per-range result JSON
              _file_status/<label>.json                        # merged normal doc (RECONCILE --write)
```

### CRITICAL: config_prefix scope and single-table requirement

The range patch (section 8 of the brief) assumes **ONE table per `--config_prefix`** (the
loader globs `_manifest_index.json` for tables under its config prefix). The existing
`plan_split` builds single-table fork prefixes at
`config/_task/<suffix>/_orchestrator/ck-<slug>/_manifest_index.json` with exactly `[entry]`.
MRL does the same but under its own tree:
`mrl/state/<suffix>/tables/<schema>.<table>/config/_manifest_index.json` holding exactly the
one table's entry + its `<dsql_table>_column_mapping.json`, **copied verbatim** from what
discovery wrote (we do NOT re-derive the per-table config — decision D3, matches the brief's
section-8 requirement "copy the per-table config object, do not re-derive").

### CRITICAL: one config_prefix PER TABLE, shared across that table's ranges

All ranges of **one** table share **one** config_prefix. This is required because the loader
resolves start/end names against the real listing under that prefix's table and writes its
range-private status docs there. Concurrency safety across the ranges of one table is proven
by:

1. **Range-private progress docs.** Each range writes only
   `_file_status/<label>.range-<first>-<last>.json` and
   `_range_result/<label>.range-<first>-<last>.json` — keyed by the resolved first/last file
   names, so **no two concurrent ranges share a writable object** (proven by the patch's own
   `test_status_isolation`).
2. **Disjoint ranges by construction.** The MRL splitter produces contiguous, inclusive,
   disjoint ranges whose union is the whole sorted list (triple-proved in the plan Lambda).
   Because the ranges are disjoint, no file is ever loaded by two ranges.
3. **The overlap-guard ledger (`.ranges.json`) is a SHARED writable object.** The patch keeps
   it to refuse overlapping *operator-chosen* ranges. In MRL the ranges are **generated** and
   proven disjoint, and each range still has its own private status/result doc, so **the ledger
   is not load-bearing for correctness** — the plan's disjointness proof + RECONCILE's
   exactly-once proof are the guard. Per the brief's section 8: *"if you give each range its
   own prefix the guard is moot; then the plan's disjointness proof + reconcile are the
   guard: say so explicitly in DESIGN.md."* We say so explicitly here. We keep a per-table
   (not per-range) config prefix so the loader's resolver still sees the full table listing
   (it must, to resolve inclusive slices and to detect the single-PK/no-PK status); the ledger
   remains a shared object but concurrent writes to it are conditional (If-Match) and a lost
   update is harmless because it only affects the advisory guard, never the private status
   docs. The authoritative overlap backstop is RECONCILE (merge_ranges), which refuses to write
   the merged normal doc if any file is covered more than once.

### Objects a single load run writes (verified by reading the patched loader)

- `_file_status/<label>.range-<first>-<last>.json` — range-private (safe; keyed by range).
- `_range_result/<label>.range-<first>-<last>.json` — range-private (safe; keyed by range).
- `_file_status/<label>.ranges.json` — **shared** overlap ledger (advisory only in MRL; see
  above).
- `_load_status.json` under the config prefix — written by `record_outcome`. In range mode the
  loader's `clear_file_status`/`validate_whole_table_after_ranges` are no-ops and
  `assert_empty_or_register` is skipped. `record_outcome` still writes `_load_status.json` at
  the table level. **MRL gives each TABLE its own config prefix, so `_load_status.json` is
  per-table, not per-range**: concurrent *ranges of the same table* could both write it.
  Decision D4: MRL runs **ranges of one table under one dedicated per-task range Glue job**,
  and we accept that `_load_status.json` is a best-effort table-level summary that the loader
  itself treats as non-fatal (its writes are `try/except`, last-writer-wins, and it is NOT used
  for MRL correctness — MRL's correctness rests on the per-range `_range_result` docs +
  RECONCILE). This matches the patch's own model (the patch's `_mr_write_result` writes the
  authoritative per-range doc; `record_outcome`'s `_load_status.json` is a convenience). We
  document this as a known shared object that is **proven safe because MRL never reads it for a
  decision**.

---

## 4. Child state machine — states, Retry/Catch, named Fail states

Order (each step has Retry on transient errors and a Catch to a **distinct named Fail state**
that surfaces `$.error`/the cause):

| # | State(s) | Purpose | Fail state(s) |
|---|----------|---------|---------------|
| 1 | `MrlResolveTask` | preflight (new Lambda `mrl_preflight`): task exists; ARN→suffix; target endpoint bucket == pipeline bucket; AddColumnName=true; capture `csv_null_value`; DMS settings valid for the chosen mode; NULL options parsed (copied parser → fail at preflight); no other RUNNING child for this task; null-rules format | `MrlResolveFailed` |
| 2 | `StartDms`/poll or adopt | start-replication (tolerate already-running) then bounded poll to the expected stop; or `adopt_stopped` verify-and-skip | `DmsStartFailed`, `DmsFailed`, `DmsTimedOut`, `AdoptPreconditionFailed` |
| 3 | `BuildTableList`→`CreateGlueJobs`→`RunDiscovery` | reuse existing resolve_task(build_table_list) + create_glue_jobs(create) + job1_discovery exactly as `startup.asl.json` | `BuildTableListFailed`, `CreateGlueJobsFailed`, `DiscoveryFailed` |
| 4 | `MrlPlan` | new Lambda `mrl_plan`: list each table's LOAD files, prove complete+unique, split by max_concurrency, persist `plan.json` (sha256), build single-table config prefixes | `MrlPlanFailed` |
| 5 | `MrlSafetyGate` | target empty OR plan ranges already recorded as a resume (same ranges); EXISTS probe via count job in probe mode | `MrlSafetyGateFailed` |
| 6 | `EnsureMrlLoadJob`→`LoadRangeMap` | ensure ONE dedicated `<project>-<suffix>-mrl-load` Glue job (MaxConcurrentRuns ≥ max total concurrent runs, tagged); Map over (table,range) `glue:startJobRun.sync`, collect all results, fail with the failed-range list | `MrlLoadFailed` |
| 7 | `MrlReconcile` | new Lambda `mrl_reconcile` (merge_ranges logic): every file done exactly once; per-range/table totals; write `reconcile.json`; optionally write merged normal doc | `MrlReconcileFailed` |
| 8 | `MrlCount` | Glue python-shell count job: DSQL COUNT(*) == DMS FullLoadRows per table (via manifest index mapping) | `MrlCountFailed` |
| 9a | full_load_only | write phase=complete → `Succeed` | — |
| 9b | full_load_and_cdc | resume DMS + start CDC forks like startup (ResumeDmsToCdc / StartCdcJob / StartForkCdcMap) | `MrlCdcFailed` etc. |

Every phase writes `mrl/state/<task-suffix>/state.json`.

### DMS stop reasons per mode (verified against repo + AWS DMS docs)

From `startup.asl.json` (`IsDmsDone`) and `resolve_task._check_startup_task`, and the AWS DMS
S3/replication docs:

- **full_load_and_cdc** task with `StopTaskCachedChangesApplied=true`: DMS finishes the full
  load, applies cached changes, then **stops with `StopReason` containing
  `STOPPED_AFTER_CACHED_EVENTS`**. This is the accepted completion for cdc mode (matches the
  existing `startup.asl.json`). `STOPPED_AFTER_FULL_LOAD` on a cdc task means it stopped BEFORE
  applying cached changes (`StopTaskCachedChangesNotApplied`) → **fail** (matches
  `startup.asl.json` which routes `STOPPED_AFTER_FULL_LOAD` to `DmsFailed`).
- **full_load_only**: a `full-load` migration-type task has no CDC phase; it finishes the full
  load and **stops by itself with `FullLoadProgressPercent=100` and status `stopped`**. The
  DMS stop reason for a pure full-load completion is `FULL_LOAD_ONLY_FINISHED` (per AWS DMS
  docs). MRL accepts, for full_load_only mode, **either** `*STOPPED_AFTER_FULL_LOAD*` **or**
  `*FULL_LOAD_ONLY_FINISHED*` together with `FullLoadProgressPercent=100` and status `stopped`.
  We do NOT guess a single string; we accept the documented set and additionally gate on
  `FullLoadProgressPercent==100 && status==stopped`, so a task that merely paused never passes.
  Recorded as decision D5. (The brief: *"full_load_only => full load finished and task stopped
  (migration type FULL_LOAD stops by itself; FULL_LOAD_AND_CDC tasks with
  StopTaskCachedChangesNotApplied stop with STOPPED_AFTER_FULL_LOAD; accept exactly the
  documented set)"*.)
- **Fail immediately** on `TablesErrored>0`, status `failed`, or a stop reason that does not
  belong to the mode (matches `startup.asl.json` `IsDmsDone`).

### adopt_stopped preconditions (verified)

`dms_start_mode=adopt_stopped`: the task is already **stopped at 100%, 0 errored tables, every
table state `Table completed`**. MRL verifies: status==stopped, `FullLoadProgressPercent==100`,
`TablesErrored==0`, and (for cdc mode) the stop reason belongs to the mode; it does **NOT**
call start-replication. This exists because on 2026-10-14 a task was hand-stopped with
`StopReason NORMAL` and the existing workflow could not continue — so for `adopt_stopped` MRL
accepts a `NORMAL` stop reason **as long as** 100% + 0 errored + all tables completed hold
(the data is fully loaded regardless of how it was stopped). Recorded as decision D6.

---

## 5. Splitter (pure function) — contract

`split_ranges(n, c)` where `n` = unique file count, `c` = max_concurrency (integer ≥ 1):

- `k = min(c, n)`; the first `(n mod k)` ranges get `floor(n/k)+1` files, the rest `floor(n/k)`.
- Ranges are contiguous, inclusive, disjoint, ordered; union == the sorted list.
- `n=0`: returns `[]` (caller decides: allowed only if DMS FullLoadRows==0 → loaded-empty; else
  FAIL CLOSED).
- Pure, deterministic, no I/O.

Required examples (asserted in tests): `n=5,c=3→2,2,1`; `n=7,c=3→3,2,2`; `n=4,c=4→1,1,1,1`;
`n=3,c=5→1,1,1`; `n=1,c=10→1`; `n=406,c=4→102,102,101,101`.

Triple proof inside the plan Lambda: (a) sizes differ by ≤1 and sum to n; (b) union of ranges
== sorted list, no overlap/gap; (c) re-resolving every range's start/end through the SAME
resolver the loader uses yields exactly the assigned files.

`split_by=bytes` is supported only as a trivial optional path (params `split_by=bytes`),
reusing `merge_ranges.plan_ranges` byte-balancing; **default and required behaviour is split by
COUNT**. Recorded as decision D7 (bytes is offered because it's trivial and the delivered
`merge_ranges` already does it, but count is the default and the tested path).

---

## 6. Concurrency / capacity analysis

Per the delivered RESULT.md G.4X math and `resolve_task`/`plan_split` defaults:

- **Glue MaxConcurrentRuns**: Glue defaults to **1 concurrent run per job**. The dedicated MRL
  load job MUST set `ExecutionProperty.MaxConcurrentRuns >= max_total_concurrent_runs`
  (the sum, across all tables loaded at once, of each table's `max_concurrency`). We set it from
  the plan and verify in a test. This is the #1 production bug the brief calls out.
- **Map MaxConcurrency** = `max_total_concurrent_runs` (bounded so sibling ranges don't all
  start at once and trip `Glue.ConcurrentRunsExceededException`). Each item has a Retry for
  `Glue.ConcurrentRunsExceededException`/throttling and a Catch that records the failed range.
- **DSQL connection budget**: `runs × max_files_in_parallel × writers_per_file` ≤ `conn_budget`
  (default 900), subnet IPs (≈1/worker+driver), Glue DPU quota (4000 in the customer env), and
  the G.4X driver memory limit (~1500 MB per concurrent file worker). The preflight prints the
  arithmetic and emits a **WARNING** (fails only on a provably impossible setting, e.g.
  `max_files_in_parallel × writers_per_file` alone already impossible within one run's budget).
- **History event budget**: DMS poll at 60 s with a `dms_max_wait_hours` budget (default 24 →
  1440 polls). Each poll adds a bounded, small number of history events; a test computes the
  worst-case total stays far below 25,000. Payloads stay < 256 KiB: plans and file lists live in
  S3 (`plan.json`), the Map iterates **range descriptors** (table, config_prefix, first, last,
  count — never the file list), so a 406- or 5,000-file list never travels inline.

---

## 7. NULL handling

Default (params blank): today's behaviour — the DMS endpoint `CsvNullValue` marker (usually
`NULL`) and an empty field are NULL, nothing else. Customer options `null_values` and
`null_rules` are passed **unchanged** to each range run, exactly as `create_glue_jobs` sets
`--null_values`/`--null_rules`/`--csv_null_value`. MRL **copies the loader's own parser**
(`resolve_task._validate_null_settings`, which mirrors the scripts' shared null-rules block)
into the preflight Lambda so a malformed setting FAILS AT PREFLIGHT naming the bad entry. An
entry naming a table/column not in the task is a WARNING. No automatic NOT-NULL detection
(offered only as a tiny read-only `tools/gen_null_rules.py` helper — decision D8).

---

## 8. Integration facts verified by reading the code

- **One table per config_prefix**: confirmed from `plan_split` fork prefixes
  (`_orchestrator/ck-<slug>/_manifest_index.json` with `[entry]`) and the loader globbing the
  index. MRL mirrors this under `mrl/state/.../tables/<label>/config/`.
- **Shared writable objects per load run**: `_file_status/<label>.range-*.json`,
  `_range_result/*.json` (range-private, safe), `_file_status/<label>.ranges.json` (shared,
  advisory), `_load_status.json` (table-level, best-effort, not read by MRL). See §3.
- **Per-table config JSON + manifest index under config_prefix**: produced exactly as
  `job1_discovery` writes — MRL **copies** the per-table `_manifest_index.json` entry and the
  `<dsql_table>_column_mapping.json` from discovery's output, never re-derives them.
- **Composite-/no-PK tables**: the patch FAILS CLOSED on a mid-file crash for a table without a
  single-column PK. The plan records `has_single_pk` per table; the README explains what a
  failed range means for such a table (full re-blank + restart all ranges).
- **DSQL connection budget / driver memory**: preflight prints the arithmetic and warns.

---

## 9. Reuse map (what MRL calls from the existing pipeline, and how)

| Existing piece | MRL use | Payload shape |
|---|---|---|
| `resolve_task` (shared startup mode) | **import-copy** the `_check_startup_task`/`_endpoint_contract`/`_validate_null_settings` logic into `mrl_preflight` (do not edit resolve_task). MRL preflight also supports the two dms_start_modes | own event |
| `resolve_task` mode `build_table_list` | **called as-is** (BuildTableList state) | exact `startup.asl.json` payload |
| `create_glue_jobs` mode `create` | **called as-is** (CreateGlueJobs) to build discovery/load/validate/cdc jobs + tags + registry | exact `startup.asl.json` payload |
| `job1_discovery.py` | **run as-is** (RunDiscovery) to write the master index + per-table configs | `--config_prefix` |
| `plan_split` | NOT reused for the range split (that is MRL's own `mrl_plan`); its fork-prefix pattern is the model MRL copies | — |
| `glue-templates/load-big.json` | the **template** MRL copies to build the dedicated `-mrl-load` job (script = `scripts/job2_load_manual_range.py`) | — |
| `merge_ranges.py` logic | copied into `mrl_reconcile` Lambda with a sync test | — |
| count-only validate logic | copied into `scripts/mrl_count.py` (standalone count function only) | — |
| `startup.asl.json` cdc tail (ResumeDmsToCdc/StartCdcJob/StartForkCdcMap) | **mirrored** in the child cdc tail using the same Lambdas (EnsureForkJobs etc.) and payload shapes | same |
| `tools/setup.sh` placeholder convention | `tools/deploy_mrl.sh` fills `<<...>>` with sed and greps for leftovers | — |

---

## 10. Decisions for the user (summary; full list also in RESULT.md)

- **D1** Split/sort ordering is lexicographic-by-name (matches the loader resolver). We assert
  all LOAD names are the same width and fail closed otherwise.
- **D2** Listing accepts decimal `^load0*[0-9]+\.csv$` by default (strictest, == merge_ranges);
  hex names require `split_allow_hex=true`.
- **D3** Per-table config is **copied** from discovery output, never re-derived.
- **D4** `_load_status.json` is a table-level best-effort doc MRL never reads for a decision;
  correctness rests on per-range `_range_result` docs + RECONCILE.
- **D5** full_load_only completion accepts `*STOPPED_AFTER_FULL_LOAD*` or
  `*FULL_LOAD_ONLY_FINISHED*` with `FullLoadProgressPercent==100 && status==stopped`.
- **D6** `adopt_stopped` accepts any stop reason (incl. `NORMAL`) when 100% + 0 errored + all
  tables `Table completed`.
- **D7** `split_by` defaults to `count`; `bytes` is an optional trivial path.
- **D8** No automatic NOT-NULL detection; optional read-only `tools/gen_null_rules.py` helper.
- **D9** The overlap-guard ledger is advisory in MRL (ranges are generated-disjoint + proven);
  the plan's disjointness proof and RECONCILE's exactly-once check are the real guard.
- **D10** MRL writes under its own `mrl/` S3 subtree and its own single-table config prefixes;
  it never writes the existing pipeline's `config/_task/<suffix>/` objects, so the two pipelines
  cannot collide.


---

## 11. Amendments 1-9 (live spec on top of the brief)

### 11.1 Amendment 1 — config-folder safety finding (verified by reading)

The two MRL input files live in `config/` under MRL-specific names
(`config/fleet_task_parallel_load.csv`, `config/params_parallel_load.csv`). The existing
pipeline reads `config/` **by exact key names only** — `preflight_tasks.py` / `fleet-startup.asl.json`
read `config/fleet_tasks.csv`, `config/params.csv`, `config/pipeline.json` by exact key;
`tools/setup.sh` uploads `glue-templates/*.json` and the five scripts by exact name and only ever
does `aws s3 rm` on `driver-cdc/*.whl` (never `s3 sync --delete` on `config/`, `scripts/` or
`glue-templates/`). No existing lambda lists the `config/` folder. Therefore the two extra
MRL-named files (and the MRL scripts/templates under new names) cannot disturb the existing
pipeline, and a later `setup.sh` run cannot delete or overwrite them. MRL never writes
`config/params.csv`, `config/pipeline.json` or `config/fleet_tasks.csv` (enforced by
`mrl_settings._guard_forbidden` and asserted on the fake-S3 write log in the tests).

### 11.2 Amendment 2 + 6 — the plan's file set equals the loader's; CDC files never picked

One shared name test (`mrl_split`): the allow-list `^load[0-9a-f]+\.csv$` (case-insensitive,
hex kept because the loader comment says names can be hex), direct children only, size > 0, not
under `processed/`/`failed/`; plus an explicit CDC deny-list (`^[0-9]{8}-[0-9]+\.csv$`,
`processed/`, `failed/`, non-`load` names). The plan computes S_loader (the loader's loose
`_is_full_load_key`) and S_plan (the strict allow-list) and **fails closed on any difference**
naming the files (a stray `load_notes.csv`, a mixed-width name). Top-level CDC files are IGNORED
and only counted in an info line. plan.json stores both patterns, the counts (allowed / ignored
CDC / ignored other / zero-byte) and the sha over allowed names only. Reconcile re-applies the
same allow-list to every progress/done entry and fails closed on a non-allow-listed name. Proven
by executing the REAL extracted loader resolver against a folder of LOAD + CDC + processed/ +
failed/ + `_manifest.json` + `notes.csv` + `load_notes.csv` + case-mix (`tests/`).

### 11.3 Amendments 3/4/5 — settings source + max_concurrency precedence + default 3

Settings come from the existing `config/params.csv` (READ-ONLY) via a **byte-identical copy** of
`params_csv.parse` / `to_pipeline_settings` (`mrl_params_csv`, sync-tested so it cannot drift).
The parent's FIRST state (`mrl_settings`) builds `config/pipeline_parallel_load.json` with
provenance (params sha256 + ETag), publishes it safely (identical → no write; different →
fail closed if any MRL run is RUNNING, else dated backup under `mrl/backups/`, put + read-back),
and pins `mrl/runs/<exec>/settings.json`; children read the pinned copy, so a params.csv edit
mid-run cannot change a running load. Effective `max_concurrency` (the number of PARALLEL RANGE
LOADS a table's files split into), first match wins: execution input > fleet-row column >
`config/params_parallel_load.csv` > `config/params.csv` `map_max_concurrency` **only if
explicitly present** > built-in default **3**. The parser fills an absent `map_max_concurrency`
with 6, so `mrl_settings.params_key_explicit` re-reads the raw text to tell absent from explicit
6 (recorded as `max_concurrency: {value, source}` + `_explicit_keys`). The plan logs
`N files, max_concurrency C (source: X) -> split`. A re-run with a different effective value than
the persisted plan fails closed once any range is loaded (rebuilds only on a clean empty table).

### 11.4 Amendment 7 — discovery reused verbatim; original step → MRL step

| startup.asl.json state | MRL child | note |
|---|---|---|
| CheckInput | CheckInput (+ route-conflict guard in `mrl_preflight`) | MRL input is `{"input":{"taskArn":...}}`; a guard refuses if the original startup/cutover is RUNNING for the task or an original-route load is in progress |
| ResolveTask | `Preflight` (new `mrl_preflight`) | reads the PINNED settings; resolves cdc + max_concurrency; emits the SAME `$.resolved.*` field names startup emits so the reused states read them identically |
| DriverDiscoveryFullload/Validation/Cdc | **reused verbatim** | prepare the pg8000 wheels / `--extra-py-files` the no-internet jobs need; a missing folder → the original `DriversFailed` |
| StartDmsTask / poll (full-load-and-cdc) | MRL DMS start/poll/adopt | per-mode stop reasons; `adopt_stopped` skips start |
| BuildTableList | **reused verbatim** | the single source of truth for table/folder/full_load_rows |
| CreateGlueJobs | **reused verbatim** | builds discovery + load/validate/cdc jobs + registry + tags |
| RunDiscovery | **reused verbatim** | writes `_manifest_index.json` + per-table config |
| PlanSplit | **replaced** by `MrlPlan` (list+split) for the load; **reused** later for CDC ownership only (cdc=true) | MRL ignores PlanSplit's load groups |
| GroupFanOut (GroupLoad/Big/Validate) | **replaced** by EnsureMrlLoadJob → LoadRangeMap → Reconcile → CountVerify | the MRL divergence point |
| ResumeDmsToCdc / StartCdcJob / CDC-confirm / EnsureForkJobs / StartForkCdcMap | **reused verbatim** (cdc=true tail) | identical Resource/Parameters (diff-tested) so the ORIGINAL cutover (stop_cdc_run / drop_tags) keeps working |

The reused states are proven identical to startup after placeholder normalization by
`tests/test_asl_diff_reused.py`. MRL carries `$.taskArn` (lifted from `$.resolved.taskArn`) so
the reused `$.taskArn` reads resolve exactly as in startup.

### 11.5 Amendment 8 — optional CDC via the `cdc` boolean

CDC is OPTIONAL, default false, switched by a strict boolean `cdc` in the fleet start input
(validated in the first state before any DMS call; replaces the old `mode` column). `cdc=false`
stops after a passing count and never resumes DMS. `cdc=true` runs the reused CDC tail after a
PASSING count (never after a failed range / unclean reconcile / failed count). Preflight fails
closed if the table already has CDC progress (a `cdc_control` row/marker) unless `override=true`
(a reload without resetting CDC progress would skip change files). A DMS-age warning before
resume. The CDC jobs/tags/registry/run-args are byte-for-byte the original route's, so the
ORIGINAL cutover works unchanged.

#### 11.5.1 CDC full-load handoff at EVERY fork/group prefix (R23-B1, Amendment 8.4)

When `cdc=true`, MRL hands the table to the reused CDC jobs, each of which gates on
`load_full_load_status()` (`REQUIRE_FULL_LOAD_DONE=True`) reading `<CONFIG_PREFIX>_load_status.json`
**under the `--config_prefix` it was started with**. The reused `plan_split` forks a composite-PK
table into a `ck-<slug>` CDC job (config prefix `.../_orchestrator/ck-<slug>/`) and a big table
(`>= file_fanout_threshold` files — default 8, `>= 6M` rows, or `>= 1 GB`) into a `bg-<slug>` CDC
job (the big group prefix `.../_orchestrator/group-<k>/`). The brief's flagship 406-file table is a
bg fork, so a forked table is the NORMAL cdc=true case. `mrl_reconcile` writes the original-format
`_load_status.json` only at the TASK prefix (the MAIN job's prefix); a fork CDC job reads its OWN
fork prefix, finds nothing, treats the table as "full load not done", and **skips it forever**
(silent CDC data loss) with an empty G9 drift baseline.

Fix: a dedicated Lambda `mrl_cdc_handoff` and a child-ASL state `CdcForkHandoff`, placed AFTER
`EnsureForkJobs` (the forks now exist) and BEFORE `ResumeDmsToCdc`/`StartCdcJob` (CDC starts). For
every fork in `plan_split`'s `forks[]` it writes the same `{"tables": {label: {"status":"done",
"rows":N}}}` doc (the exact shape `glue_cdc_continuous._read_load_status_doc` parses) at that fork's
`config_prefix`, for the one table the fork owns, using the reconciled row count. It is idempotent
(merges with any existing doc), writes nothing for `cdc=false` or when there are no forks (the main
job uses the task-prefix handoff), and FAILS CLOSED if a forked table has no clean reconcile count
(writing "done" for it would be a lie). The ordering constraint (`reconcile` runs before
`PlanSplit`, so fork prefixes don't exist at reconcile time) is why the fork write is a separate
later state rather than part of reconcile. Proven by `tests/test_cdc_handoff.py` running the REAL
extracted `load_full_load_status` with `CONFIG_PREFIX` set to a `ck-` and a `bg/group` prefix
(table seen `done`, `_FULL_LOAD_ROWS` seeded), a mutation check (no fork write -> gate sees None),
and simulator scenario `3b_cdc_fork_handoff` driving a real ck + bg fork end to end.

### 11.6 Amendment 9 — drivers + scripts location + params values

Drivers use the SAME method as the original (no new folders, no pip, no internet): the three
driver-discovery states feed the range-load job (fullload extras), the count job (validation
extras) and the CDC jobs (cdc extras). New Glue scripts live in the SAME `scripts/` folder under
NEW names (`job2_load_manual_range.py`, `job_mrl_count.py`); new templates in `glue-templates/`
under NEW names (`mrl-load.json`, `mrl-count.json`). `create_glue_jobs` maps a fixed role set to
`glue-templates/<role>.json`, so it cannot build a job from an arbitrary new template without
edits — hence MRL uses the dedicated `mrl_ensure_job` Lambda (an import-copy of the job-definition
logic: same tags, connections, DefaultArguments merge, placeholder substitution, worker sizing),
never editing `create_glue_jobs`. Every params.csv key is accepted; the classification table
(`tools/mrl_settings_classification.py`, machine-checked) classes each key USED / PASSED THROUGH
/ IGNORED HERE.

#### 11.6.1 Full params.csv key classification (Amendment 9 + 10)

Generated from `tools/mrl_settings_classification.py` (a drift test keeps it in sync). Amendment 10 reclassified every sizing/parallelism key to IGNORED HERE: MRL takes those from the fleet row / execution input / built-in default (`lambdas/mrl_defaults.py`), not `config/params.csv`.

| params.csv key | class | where / reason |
|---|---|---|
| `account_id` | USED | identity; builds Glue job ARNs and the DSQL cluster resource |
| `big_table_bytes_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (cdc=true) |
| `big_table_row_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (CDC ownership only, cdc=true) |
| `cdc_drift_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_drift_check_minutes` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_drift_tolerance` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_engine` | PASSED THROUGH | reused CreateGlueJobs CDC engine selection (cdc=true) |
| `cdc_file_order_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_max_delete_fraction` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_max_delete_rows` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_nopk_overmatch_action` | PASSED THROUGH | reused CDC guardrail (cdc=true) |
| `cdc_spark_fallback` | PASSED THROUGH | reused CDC Spark fallback (cdc=true) |
| `cdc_validation` | PASSED THROUGH | reused CDC jobs arg (cdc=true) |
| `cdc_validation_sample` | PASSED THROUGH | reused CDC jobs arg (cdc=true) |
| `conn_budget` | USED | capacity arithmetic + the count job's --conn_budget cap |
| `control_schema` | PASSED THROUGH | reused CDC jobs arg --control_schema (cdc=true only) |
| `cutover_count_check` | IGNORED HERE | original cutover count gate; not part of the MRL load |
| `discovery_num_workers` | PASSED THROUGH | reused CreateGlueJobs -> discovery job workers |
| `discovery_timeout_minutes` | PASSED THROUGH | reused CreateGlueJobs -> discovery timeout |
| `discovery_worker_type` | PASSED THROUGH | reused CreateGlueJobs -> discovery job WorkerType |
| `dsql_database` | USED | range-load + count job arg --dsql_database |
| `dsql_endpoint` | USED | DSQL endpoint for the range-load + count jobs (--dsql_endpoint) |
| `dsql_user` | USED | range-load + count job arg --dsql_user |
| `file_fanout_threshold` | PASSED THROUGH | reused PlanSplit big-table classification (cdc=true) |
| `glue_connection` | USED | VPC Connections on every MRL Glue job |
| `glue_role_arn` | USED | Role on every MRL Glue job (range-load, count, reused discovery/CDC) |
| `glue_version` | USED | GlueVersion of the MRL Glue jobs |
| `guardrails_mode` | PASSED THROUGH | reused CDC/validate guardrail master (cdc=true) |
| `lambda_role_arn` | IGNORED HERE | setup-only (Lambda role; MRL runs under the existing roles) |
| `load_big_num_workers` | IGNORED HERE | A10: no load-big path (one table per task) |
| `load_big_timeout_minutes` | IGNORED HERE | A10: no load-big path (one table per task) |
| `load_big_worker_type` | IGNORED HERE | A10: one table per task, no load-big; MRL sizes its own range-load job from fleet row / built-in default |
| `load_num_workers` | IGNORED HERE | A10: from fleet row / built-in default (mrl_defaults.LOAD_NUM_WORKERS=10) |
| `load_timeout_minutes` | IGNORED HERE | A10: from built-in default (mrl_defaults.LOAD_TIMEOUT_MINUTES=2880) |
| `load_worker_type` | IGNORED HERE | A10: range-load WorkerType from fleet row / exec input / built-in default (mrl_defaults.LOAD_WORKER_TYPE=G.4X) |
| `manage_iam` | IGNORED HERE | setup-only (IAM management mode) |
| `map_max_concurrency` | IGNORED HERE | A10: parallelism comes from the fleet row / exec input / built-in default (mrl_defaults.MAX_CONCURRENCY=3), not params.csv |
| `max_big_cdc_forks` | PASSED THROUGH | reused PlanSplit CDC ownership cap (cdc=true) |
| `max_composite_forks` | PASSED THROUGH | reused PlanSplit CDC ownership cap (cdc=true) |
| `max_files_in_parallel` | IGNORED HERE | A10: range-load fan-out from fleet row / exec input / built-in default (mrl_defaults.MAX_FILES_IN_PARALLEL=30) |
| `max_groups` | PASSED THROUGH | reused PlanSplit (CDC ownership only; MRL ignores load groups) |
| `max_parallel_tables` | IGNORED HERE | original job2 how-many-tables-at-once; MRL loads one table's ranges with its own max_concurrency (one table per task) |
| `max_writers_per_loader` | PASSED THROUGH | reused PlanSplit writer sizing (cdc=true) |
| `min_writers_per_loader` | PASSED THROUGH | reused PlanSplit writer sizing (cdc=true) |
| `null_notnull_text` | USED | range-load + count job arg --null_notnull_text (NULL handling) |
| `null_rules` | USED | range-load + count job arg --null_rules (NULL handling) |
| `null_values` | USED | range-load + count job arg --null_values (NULL handling) |
| `per_worker_mem_budget_mb` | IGNORED HERE | original job2 driver-memory auto-throttle; MRL sizes the range-load job by worker type/count directly |
| `project` | USED | name prefix of the MRL Glue jobs and state machines |
| `region` | USED | AWS region for every DMS/Glue/S3 call |
| `security_group_id` | IGNORED HERE | setup-only (Glue VPC connection) |
| `sfn_role_arn` | IGNORED HERE | setup-only (Step Functions role) |
| `subnet_id` | IGNORED HERE | setup-only (Glue VPC connection); not an MRL runtime setting |
| `validate_count_check` | IGNORED HERE | the original validate's DMS-count gate; MRL's count job has its own count_timeout_action + tolerance |
| `validate_hash` | IGNORED HERE | per-value hash scope for the original validation; unused here |
| `validate_num_workers` | IGNORED HERE | A10: count job from built-in default (mrl_defaults.COUNT_NUM_WORKERS=10) |
| `validate_parallelism` | IGNORED HERE | validation throughput tuning; not used by the count job |
| `validate_rows_per_range` | IGNORED HERE | validation range sizing; MRL verifies by COUNT(*), not the per-range aggregate validation |
| `validate_target_seconds_per_range` | IGNORED HERE | validation range time sizing; unused here |
| `validate_timeout_minutes` | IGNORED HERE | A10: count job from built-in default (mrl_defaults.COUNT_TIMEOUT_MINUTES=2880) |
| `validate_worker_type` | IGNORED HERE | A10: count job sized from built-in default (mrl_defaults.COUNT_WORKER_TYPE=G.8X) |
| `writers_per_file` | IGNORED HERE | A10: from fleet row / exec input / built-in default (mrl_defaults.WRITERS_PER_FILE=8) |

### 11.7 Deliberate decisions where the implementation differs from the amendment wording

- **Blank `map_max_concurrency` (previously Amendment 5(d)).** MOOT since Amendment 10: MRL no
  longer reads `map_max_concurrency` (or any sizing/parallelism) from `config/params.csv`, so its
  value — blank, present or absent — does not affect MRL at all. The shared copied parser keeps the
  original pipeline's own validation of the key unchanged (the sync test still passes); MRL simply
  ignores it. To set MRL's parallel range loads, use the fleet-row / execution-input
  `max_concurrency` (built-in default 3).
- **Fleet row cap = 200 (user decision 2026-10-16; was 1000).** The hard cap is `_MAX_ROWS = 200`,
  set per the user's 2026-10-16 decision ("it should just be 200 max"); it was previously 1000.
  It fails closed above the cap, naming the actual vs. allowed count and stating it fails before
  anything starts (no child execution, no DMS call). This matches Amendment 3 item 6's suggested
  200 and stays well within Step Functions Map / payload limits.

### 11.8 Amendment 13 — count/probe driver set in a no-internet VPC (choice A.1 = option (a))

The MRL count/probe Glue job is PYTHON-SHELL. Glue pip-installs its `--extra-py-files` wheels one
at a time; an UNSTRIPPED wheel whose `Requires-Dist` (pg8000 needs `scramp`) is not yet installed
makes pip reach pypi.org, which hangs ~20 min behind a no-internet VPC firewall (the live T14B
BLOCKER). The job had wrongly been given `$.driverValidation.extraPyFiles` (the Spark wheels-on-path
set). **Chosen fix: option (a)** — keep the job python-shell and feed it the ORIGINAL
`driver_discovery prepare_for=pythonshell` PREPARED set (Requires-Dist stripped into
`driver-cdc-prepared/<fingerprint>/`, validated BEFORE anything runs), the same mechanism the CDC
path uses. A new state `DriverDiscoveryCount` (prepare_for=pythonshell, spark_fallback=false,
drivers_prefix=driver-cdc) runs in the unconditional discovery chain (so cdc=false runs get it too)
BEFORE SafetyProbe, and `SafetyProbe`/`CountVerify` read `$.driverCount.extraPyFiles`. Option (b)
(Spark count job) was NOT needed: `driver-cdc/` already ships pg8000 + scramp + asn1crypto, so the
prepared python-shell set satisfies the count script's imports — proven offline in
`tests/test_count_driver_offline.py` (prepared set installs with `pip --no-index`; the unstripped
set fails with the exact "could not find … scramp" error). Fail-fast: the `.sync` probe/count Glue
states carry `TimeoutSeconds` (probe 900s, count 3600s, overridable) with
`MrlProbeTimedOut`/`MrlCountTimedOut`; the deploy aborts if `driver-cdc/` has no wheels.

### 11.9 Amendment 19 — hex LOAD names + reconcile proof-source

**DMS names its full-load files in HEXADECIMAL** (LOAD00000001..LOAD00000009, LOAD0000000A, ...; the
406-file table runs to LOAD00000196 = 0x196). MRL therefore accepts hex by DEFAULT: `split_allow_hex`
is an **opt-OUT** (default true; `splitAllowHex` default true in preflight). For uniform-width
UPPER-case hex, lexicographic name order == numeric order, so MRL's canonical name-sorted ranges
resolve, in the loader's name-based `_mr_resolve_slice`, to exactly the planned files; the union of
ranges == the loader's whole-table set. Every fail-closed rule is kept: uniform name width per table,
uniform letter CASE per table (a mixed-case set fails closed naming the files), the allow-list regex,
the CDC deny-list, non-empty files. NOTE the ORIGINAL whole-table loader orders by SIZE; MRL **range
mode** orders by NAME (the only order in which a first..last range is well-defined).

**Reconcile proof source (why it stays fail-closed):** reconcile proves per-file coverage ONLY from
the per-range `_file_status/<label>.range-*` + `_range_result/*` docs — it does NOT accept the
whole-table `_load_status.json` as proof, even though that file (and DSQL) may show the rows. Reason:
`_load_status.json` records a whole-table load, not which range covered which files; accepting it
would let a run that skipped a range look complete. Amendment 19 therefore (a) routes range mode to
the per-file loader so those docs are always written, and (b) fails the load LOUDLY if range mode was
requested but no per-range result doc was written — never a silent success without range artifacts.
