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

| REUSED verbatim (original route owns these) | NEW (MRL, under `parallel_range_load_pipeline/`) |
|---|---|
| Discovery states + Lambdas: ResolveTask / BuildTableList, the three DriverDiscovery states, CreateGlueJobs, RunDiscovery | **State machines:** `stepfunctions/mrl-fleet.asl.json`, `stepfunctions/mrl-task.asl.json` |
| `plan_split` + `EnsureForkJobs` (CDC ownership: main / `ck-<slug>` / `bg-<slug>`) | **Lambdas:** `mrl_settings`, `mrl_fleet_preflight`, `mrl_preflight`, `mrl_plan`, `mrl_gate_eval`, `mrl_ensure_job`, `mrl_state`, `mrl_reconcile`, `mrl_cdc_handoff`, plus the pure cores `mrl_split`, `mrl_loader_resolver`, `mrl_defaults` (built-in sizing), and the byte-identical `mrl_params_csv` |
| CDC scripts (`glue_cdc_continuous.py`) + its full-load gate, tags, registry, run-args | **Glue scripts:** `scripts/job2_load_manual_range.py` (range loader), `scripts/job_mrl_count.py` (count/probe) |
| `ResumeDmsToCdc` (DMS `resume-processing`) and the CDC start-marker confirmation | **Glue templates:** `glue-templates/mrl-load.json`, `glue-templates/mrl-count.json` |
| The original **cutover** state machine (stops CDC jobs, completes handover) | **Tools:** `tools/deploy_mrl.sh`, `tools/mrl_fleet_run.py` (start tasks with NO state machine), `tools/mrl_iam_check.sh`, `tools/mrl_required_actions.json` |
| (the three EXISTING roles `<project>-lambda/sfn/glue-exec-role`) | **IAM:** NONE for the default deployment (the existing roles already cover it — proven by `tests/test_existing_iam_covers_mrl.py`). `iam/mrl-fleet-sm-optional.json` is needed ONLY for the optional fleet state machine (`--with-fleet-sm`). |

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

---

## 9. Coexistence in one bucket / account / project (Amendment 11)

MRL and the original pipeline share one bucket, account and project. MRL only adds NEW-named
resources and writes under `mrl/` plus the allow-listed cdc=true handoff inside the shared
`config/_task/<suffix>/`. Proven by `tests/test_coexistence_no_collision.py`.

- **No name collision:** every MRL Lambda / Glue job / state machine name contains `mrl` and none
  equals an original name; MRL never creates a job named like an original role
  (`-discovery/-load/-load-big/-validate/-cdc`).
- **Shared prefix:** `config/_task/<suffix>/` is written by the REUSED original discovery exactly
  as the original route does; the ONLY MRL-code writes there are the cdc=true `_load_status.json`
  handoff at the task prefix and each `ck-`/`bg-group-` fork prefix. With cdc=false MRL writes
  nothing of its own there.
- **No destructive effect:** MRL deletes nothing outside `mrl/`; the base `setup.sh` re-run only
  overwrites its own keys (never MRL objects); the original **cutover** deletes the task-tagged
  `<project>-<suffix>-mrl-load` job after its runs stop (desired cleanup), and leaves the untagged
  `<project>-mrl-count` alone.
- **resolve_task with pre-existing MRL state** (tested against the real base code): MRL artifacts
  with no owner marker → the original startup REFUSES (fail closed); with the matching `_task.json`
  marker → it REUSES the folder (the route-conflict guard prevents both routes running at once); a
  different owning task → refuse. No silent overwrite of MRL's loaded state.
- **Deploy ownership preflight:** `deploy_mrl.sh` prints WILL CREATE / WILL UPDATE (mrl-owned) /
  ABORT per target, aborts if a name exists but is not MRL-owned, and ends with
  `EXISTING PIPELINE RESOURCES TOUCHED: 0` (tested with a fake aws CLI in
  `tests/test_deploy_touches_zero.py`).

**Operator rule (R4-3):** the unmodified original route has NO guard against MRL. Because MRL
writes the shared owner marker `config/_task/<suffix>/_task.json`, pointing the ORIGINAL
`<project>-startup` at a task MRL already loaded makes it PROCEED (reuse the folder) and re-run its
full load into the loaded tables. **Do NOT run the original startup/cutover on an MRL-loaded task
unless you intend to continue it.** To continue a `cdc=false` MRL load to CDC, re-run the task
through MRL with `cdc=true` (not the original startup). Verified against `../lambdas/resolve_task.py`.
