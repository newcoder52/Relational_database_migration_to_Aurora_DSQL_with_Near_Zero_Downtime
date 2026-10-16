# MRL — Integration with the broader architecture

How the Manual-Range-Load (MRL) route fits beside the existing DMS→S3→DSQL pipeline. MRL reuses the
original discovery + CDC machinery verbatim and replaces only the load **planning/execution** for a
single big table per DMS task.

---

## 1. Original flow vs MRL flow

```
ORIGINAL (startup) route                         MRL route (one big table per DMS task)
========================                         ======================================
DMS task  --full load-->  S3 (LOAD part-files)   DMS task  --full load-->  S3 (LOAD part-files)
      |                                                 |
      v                                                 v
 startup state machine                            mrl-fleet (parent)  --per row-->  mrl-task (child)
   - ResolveTask / BuildTableList                   [REUSED discovery states, byte-identical]
   - DriverDiscovery (fullload/validation/cdc)        - ResolveTask / BuildTableList
   - CreateGlueJobs                                    - DriverDiscovery (fullload/validation/cdc)
   - RunDiscovery                                      - CreateGlueJobs / RunDiscovery
   - plan_split (groups + ck/bg forks)                        |
   - load groups (Glue)                                       v   [NEW load planning/execution]
   - validation                                        - MrlPlan     (list + split into N ranges)
   - ResumeDmsToCdc + CDC jobs                          - SafetyGate  (target empty / matching resume)
   - cutover                                            - LoadRange   (Map: parallel range Glue runs)
                                                        - Reconcile   (every file covered once)
                                                        - Count       (DSQL vs DMS/discovery)
                                                        - cdc=true: plan_split + EnsureForkJobs
                                                                    + CdcForkHandoff (NEW)
                                                                    + ResumeDmsToCdc + StartCdc(+forks)
                                                        -> hand to the ORIGINAL cutover
```

Both routes start from the **same** DMS full-load output in S3 and run the **same** discovery
states. MRL diverges only at planning: instead of grouped loads it splits one table's LOAD files
into contiguous ranges and loads them in parallel, then reconciles and count-verifies.

---

## 2. Reused verbatim vs new

| REUSED verbatim (original route owns these) | NEW (MRL, under `manual_range_pipeline/`) |
|---|---|
| Discovery states + Lambdas: ResolveTask / BuildTableList, the three DriverDiscovery states, CreateGlueJobs, RunDiscovery | **State machines:** `stepfunctions/mrl-fleet.asl.json`, `stepfunctions/mrl-task.asl.json` |
| `plan_split` + `EnsureForkJobs` (CDC ownership: main / `ck-<slug>` / `bg-<slug>`) | **Lambdas:** `mrl_settings`, `mrl_fleet_preflight`, `mrl_preflight`, `mrl_plan`, `mrl_gate_eval`, `mrl_ensure_job`, `mrl_state`, `mrl_reconcile`, `mrl_cdc_handoff`, plus the pure cores `mrl_split`, `mrl_loader_resolver`, and the byte-identical `mrl_params_csv` |
| CDC scripts (`glue_cdc_continuous.py`) + its full-load gate, tags, registry, run-args | **Glue scripts:** `scripts/job2_load_manual_range.py` (range loader), `scripts/job_mrl_count.py` (count/probe) |
| `ResumeDmsToCdc` (DMS `resume-processing`) and the CDC start-marker confirmation | **Glue templates:** `glue-templates/mrl-load.json`, `glue-templates/mrl-count.json` |
| The original **cutover** state machine (stops CDC jobs, completes handover) | **IAM:** `iam/mrl-additions.json` (extra least-privilege grants only) + `tools/deploy_mrl.sh` |

MRL never edits any existing repo file; it imports/copies what it reuses and adds everything else
under new names.

---

## 3. S3 keys

**Reads:**
- `config/fleet_task_parallel_load.csv` — MRL parent input (fleet rows).
- `config/params_parallel_load.csv` — optional MRL-only params.
- `config/params.csv` — the existing pipeline's settings (READ-ONLY, via the copied parser).
- `config/_task/<suffix>/...` — discovery manifests / column mappings / the original-route
  `_load_status.json` (read-only; the route-conflict guard and CDC handoff read these).
- `scripts/*`, `glue-templates/*` — GetObject only (job definitions).

**Writes:**
- `config/pipeline_parallel_load.json` — MRL's OWN published settings (the only `config/` file MRL
  writes); a per-run pinned copy at `mrl/runs/<exec>/settings.json`.
- `mrl/state/<suffix>/...` — `state.json`, `plan.json`, `reconcile.json`, `count_result.json`,
  per-range `_file_status`/`_range_result`, and the single-table config prefixes.
- `config/_task/<suffix>/_load_status.json` and each fork/group `_load_status.json`
  (the cdc=true full-load handoff — Get/Put, never Delete).

**Never writes (the three forbidden files):** `config/params.csv`, `config/pipeline.json`,
`config/fleet_tasks.csv`.

---

## 4. Shared resources and capacity

MRL shares the account's Glue, DSQL and DMS with the base pipeline.

- **Glue concurrent runs:** `tasks_running_in_parallel × max_concurrency` range loads can be in
  flight (plus count/CDC runs), where `tasks_running_in_parallel = max_parallel_tasks` (default 2).
  Keep total concurrent Glue runs under the account quota.
- **DSQL write capacity:** `max_concurrency × max_files_in_parallel × writers_per_file` concurrent
  writers per table; multiplied again by `max_parallel_tasks`. Preflight prints the capacity
  arithmetic and warns/fails closed on an impossible setting vs the connection budget.
- **DMS:** one task per MRL child; MRL starts/adopts/resumes exactly as the original route does.

Rule of thumb: **concurrent load pressure ≈ tasks × ranges × (files × writers)** — size
`max_parallel_tasks`, `max_concurrency`, `max_files_in_parallel`, `writers_per_file` together.

---

## 5. Route-conflict rules

- One DMS task runs **either** the original route **or** MRL at a time — never both.
- MRL preflight **fails closed** if the original `<project>-startup`/`<project>-cutover` has a
  RUNNING execution for the task, or if an original-route load is in progress under
  `config/_task/<suffix>/_orchestrator/`. If the guard cannot list those executions (AccessDenied)
  it fails closed too (add the `states:ListExecutions/DescribeExecution` grants). `override=true`
  downgrades to a loud warning.

---

## 6. How the original cutover finds CDC jobs MRL started

When `cdc=true`, MRL starts the **same** CDC jobs the original route would: the main CDC job plus
each composite (`ck-<slug>`) and big (`bg-<slug>`) fork, with the **same tags, the same `_jobs.json`
registry entries, and each run's `--config_prefix`**. The original cutover discovers and stops them
by exactly those tags / registry / `--config_prefix` — it needs no MRL-specific knowledge. MRL also
writes the full-load handoff at every fork prefix first, so each fork CDC job actually applies its
changes before cutover.

---

## 7. Going back to the original route

A table can return to the original route at any time: MRL only adds files under `mrl/` and its own
`config/pipeline_parallel_load.json`; it never alters the original-route state under
`config/_task/*`. To revert, stop any MRL execution for the task, (optionally) clear
`mrl/state/<suffix>/`, and run the original startup route as usual. If MRL already started CDC for
the task, let the original cutover complete first (it owns those jobs).

---

## 8. Deliberately NOT included

- **Validation checksum / hash verification:** MRL verifies by `COUNT(*)` (with tolerance / timeout
  / no-basis actions), not the original per-range checksum validation.
- **No-PK tables:** range splitting assumes the loader's resolver ordering; no-PK handling beyond
  what the reused loader provides is out of MRL's minimal scope.
- **Tables with CDC already running:** MRL fails closed rather than reload a table whose CDC has
  progress (a reload without a CDC reset would skip change files). See the RUNBOOK CDC-reset note.
