# RESULT — RUNBOOK §3 params table restructured by phase / used-by

**Commit base:** main HEAD `8825b413d61872d558ad014eaa7fd345cc0663e7` (verified with `git ls-remote`).

## What changed (docs/tests only — no runtime file touched)

- **RUNBOOK.md §3 "Fill in params.csv"** — the single parameter table is replaced by phase-grouped
  sub-tables (bold sub-headings, in pipeline order: Setup only, Everywhere, Planning / fan-out,
  Full load, Validation, Discovery, CDC). Every row now has the columns
  **Parameter | Required? | Default | Phase(s) | Used by | Meaning**, where **Used by** names the
  actual script / Lambda / state machine and **Phase(s)** lists every phase the key touches. Each
  row states when a change takes effect (republish → future tasks; setup-only → re-run setup; job
  args baked at job creation → only jobs created afterwards). A new **"Which settings affect a
  running task?"** note explains what a running CDC job re-reads (nothing: args captured once at
  run start; the 30s poll loop never re-reads pipeline.json) vs what needs a new task or job
  re-create. Sub-group labels are bold text (not `###`) so the whole table stays inside the §3
  slice the test parses.
- **docs/MANUAL_SETUP.md §3c** — the `params.csv → pipeline.json` mapping table gains the same
  **Phase(s)** and **Used by** columns for every PIPELINE_KEY.
- **tests/test_docs_params.py** — extended: new `_check_phase_usedby` asserts every documented key
  row (all ALLOWED keys in RUNBOOK §3; all PIPELINE_KEYS in MANUAL_SETUP §3c) has a non-empty
  Phase(s) cell and a non-empty Used-by cell. Existing checks unchanged.

## Evidence

Full file:line trace for every key (PIPELINE_KEYS + REQUIRED + SETUP_ONLY + derived
`glue_role_arn` / `dsql_cluster_id`) is in `~/Downloads/review3d/params-map/MAP.md` (not committed).
Flow traced: `params_csv.py` → `pipeline.json` → `resolve_task.py` (`$.resolved.*`, camelCase) →
state-machine payload (`startup.asl.json` / `cutover.asl.json`) → `create_glue_jobs.py` job args
(`--xxx`) / Glue job definition, or `plan_split.py` per-group loader args → the reading script
(`job1_discovery`, `job2_load`, `job3_validate`, `glue_cdc_continuous`, `glue_cdc_composite`) or
Lambda (`drain_check`, `drop_tags`, `stop_cdc_run`, `preflight_tasks`), plus `tools/setup.sh` for
setup-only keys.

## Keys that could NOT be traced
None. Every one of the 54 ALLOWED keys traces to at least one component in the code.

## Keys that are unused / dead
None. All 48 PIPELINE_KEYS reach a script/Lambda or a Glue job definition; all 5 SETUP_ONLY keys
reach `tools/setup.sh`; `account_id` is setup-only and additionally seeds the derived role ARNs;
`glue_role_arn` is in pipeline.json and used at job creation; `dsql_cluster_id` is a derived value
(first label of `dsql_endpoint`), used by setup, correctly documented as "not a CSV key".

## Verification
- All 11 test files pass (exit 0), including `tests/test_docs_params.py`:
  - test_asl_paths, test_asl_payload_contract, test_b17_b13, test_b18_validate_throughput,
    test_docs_params, test_e2e_fixes, test_existing_roles, test_fix6, test_guardrails,
    test_planning_settings.
- Only `RUNBOOK.md`, `docs/MANUAL_SETUP.md`, `tests/test_docs_params.py` modified (`git status`).
- No `<`/`>` introduced in any example VALUE. The angle brackets present in the §3 table are the
  pre-existing default ARN templates (`arn:aws:iam::<account_id>:role/<project>-...-exec-role`), the
  endpoint format descriptor (`<cluster>.dsql.<region>.on.aws`) and the `s3://<bucket>/...` upload
  path — all carried over verbatim from the original doc, none of them an example value a user types
  into `params.csv` (those live in `config/params.example.csv`, which was not changed).
