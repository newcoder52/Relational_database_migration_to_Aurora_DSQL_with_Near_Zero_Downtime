# RESULT — ease the G1–G10 guardrails (never fail a normal run) + fix B22 (IAM MalformedPolicyDocument)

**Commit base:** main HEAD `cbb85014722379e4319c05df45e212accdb105be` (verified with
`git ls-remote`; built on the actual HEAD).
**Fix commit (runtime + tests + docs):** `1d3ed4c98be6aa4b74d56e02b0095d3501a09f63`.

Guiding principle (USER DECISION — "the guardrails are causing issues, ease up and make sure the
scripts do not fail"): **a guardrail may only ever STOP a destructive action** (deleting or
emptying target rows). It must **never fail a load, validate, CDC or cutover run** because of its
own bookkeeping — a missing permission, a missing control table, a lock it can't take, or a check
it can't compute. A new master setting `guardrails_mode = warn | strict` (default `warn`) toggles
fail-closed for operators who want it.

---

## B22 (deploy blocker, found on real AWS) — `_comment` inside IAM Statement objects

**Root cause.** `iam/glue.json` carried a `"_comment"` key **inside** a `Policy` Statement
(`StatesDescribeExecutionForBlankGuard`). IAM validates every policy document against a fixed
schema and rejects any unknown key with `MalformedPolicyDocument`, so `aws iam put-role-policy`
failed and **every setup failed**. The three files also carried a document-level `"_comment"`
(sibling of `RoleName`); those are never sent to IAM, but B22 asks for the explanations to live in
docs, so they were removed too.

**Fix.**
1. Removed the one statement-level `_comment` (the actual blocker) and the three top-level
   `_comment` keys. Surgical line-removal only — array formatting preserved, all three files are
   still valid JSON, zero `_comment` tokens remain. The explanations now live in
   `docs/IAM_POLICY_NOTES.md` (one section per role, including why the Glue role grants
   `states:DescribeExecution`).
2. **Backstop.** `tools/setup.sh` `split_iam` and the `docs/MANUAL_SETUP.md` fill-in step now
   `strip_policy()` each `TrustPolicy`/`Policy`/`VpcPolicy` before `put-role-policy`: only
   `Version/Id/Statement` survive at the document level and only
   `Sid/Effect/Action/NotAction/Resource/NotResource/Principal/NotPrincipal/Condition` inside a
   Statement. A hand-edited file that re-adds a `_comment` still deploys. The manage_iam=false
   `iam-out/` writer reads the stripped `.policy.filled.json`, so it is covered too. Added a
   `MalformedPolicyDocument` warning to the MANUAL_SETUP console-alternative.
3. **Static test** `tests/test_iam_policies.py` (158 checks): every Statement in `iam/*.json` has
   only valid IAM keys, no `_comment` anywhere, and the SHIPPED `strip_policy` in both setup.sh and
   MANUAL_SETUP drops a stray statement- and document-level `_comment` while keeping the valid keys.

**Verified end to end:** the existing `tests/test_existing_roles.py` runs the real `setup.sh`
against the fake AWS CLI and still makes zero/correct IAM calls; a manual exercise that dirtied
`iam/glue.json` with a `_comment` confirmed the shipped strip removed it from the filled policy
(7 statements kept).

---

## Easing the guardrails — before → after

`guardrails_mode = warn` (default) | `strict`. In `warn`, HARD guards (G1/G4/G6) still refuse a
genuinely destructive action (and only that one action — the per-table CONTINUE-ON-FAILURE loop
keeps the rest of the run going); every other guard is advisory (WARNING + `DsqlGuardWarn` /
`DsqlRowDrift` metric + `cdc_control.audit_log` row). Each guard call is wrapped so an exception
(permission denied, DSQL error, missing table, S3 read error) is logged and treated as "passed",
never raised into the main path. `strict` restores the original fail-closed behaviour; per-guard
settings still override.

| Guard | Before (fail-closed) | After — `warn` default | `strict` | Default |
|---|---|---|---|---|
| **G1** (HARD) | Refuse a blank once CDC started; fail-closed if markers unreadable | **Unchanged** — refuses the blank only (run continues) | same | — |
| **G4** (HARD) | Refuse a blank of a table this task didn't load / count > expected×margin | **Unchanged** — refuses the blank only | same | — |
| **G6** (HARD) | Block a CDC file deleting > fraction AND > rows | **Unchanged** — blocks that one table | same | fraction `0.5`, rows `100000` |
| **G2** (soft) | **Refuse** a no-workflow blank unless `--allow_manual_destructive` | **Allow + loud WARNING + audit** (a missing `states:DescribeExecution` / unconfirmable execution no longer fails the run); RUNNING execution or the flag still blank. G1/G4 still apply | Refuse (as before) | `warn` |
| **G3** (soft) | **Fail-closed** on any failed lock acquire | WARN + proceed when the lock can't be taken for a bookkeeping reason; skip ONLY when **another live holder** clearly owns it | Refuse on any failed acquire | `warn` |
| **G5** (soft) | Best-effort audit before every op | Unchanged (best-effort; never fails the action) | same | always on |
| **G7** (soft) | **Block** a no-PK content-DELETE over-match | WARN + apply (G6 is the real volume cap); purge-exactness tripwire still HARD | Block | `cdc_nopk_overmatch_action=warn` |
| **G8** (soft) | **Block** on order/gap/new-`LOAD`-after-CDC | WARN + metric, keep applying in order; the LOAD-list read is wrapped so it can't raise | Block | `cdc_file_order_action=warn` |
| **G9** (soft) | WARN + metric + audit, optional block | Unchanged default (warn); wrapped so it can't raise | `cdc_drift_action=block` | `cdc_drift_action=warn` |
| **G10** (soft) | **Fail** validate / refuse cutover on DSQL ≠ DMS `FullLoadRows (+I−D)` | WARN; validate still PASSES / cutover proceeds (DMS counts can legitimately differ) | `validate_count_check=strict` / `cutover_count_check=strict` | `warn` |

### Control bookkeeping is add-if-missing and non-fatal (item 4 / B17)

`cdc_control.cdc_control_lock`, `cdc_control.audit_log`, and the `cdc_status` counters
(`full_load_rows`, `inserts_applied`, `deletes_applied`, `allow_mass_delete`) are all
`CREATE TABLE IF NOT EXISTS` + `ADD COLUMN` with **no** `DEFAULT` (DSQL rejects `DEFAULT`-on-ALTER
at parse time, SQLSTATE 0A000), so they work on a pre-existing older `cdc_control`. If the
create/upgrade fails, the CDC job now **warns and continues** (the guards that write them are
best-effort).

### Settings (plumbed params.csv → pipeline.json → resolve_task → create_glue_jobs → job args)

`guardrails_mode` (warn|strict), `cdc_file_order_action` (warn|block), `cdc_nopk_overmatch_action`
(warn|block), `validate_count_check` (warn|strict), `cutover_count_check` (warn|strict). All
default to the safe/non-failing value; all documented in RUNBOOK §3 and the "Safety guardrails"
section, and in `docs/MANUAL_SETUP.md` §3c. `guardrails_mode=strict` implies `block`/`strict` for
the per-guard knobs unless they are set explicitly.

---

## Tests

- **New** `tests/test_iam_policies.py` (158) — B22 static validity + both strip backstops.
- **New** `tests/test_ease_guardrails.py` (86) — plumbing of `guardrails_mode` end to end; each
  SOFT guard with an injected failure continues + warns (G2 missing `states:DescribeExecution`,
  G3 connect/lock failure, G8 LOAD-list read wrapped, G9 never raises); HARD guards (G1/G4) still
  refuse only the destructive op in both modes; `take_table_lock` contended-vs-bookkeeping flag;
  CDC `guard_action_blocks` mapping + warn-wiring byte-identical in both engines;
  `guardrails_mode=strict` restores fail-closed; and a full happy-path
  load → validate → CDC → cutover simulation with **no guard firing and no**
  `states:DescribeExecution` **permission** succeeds.
- **Updated** `tests/test_guardrails.py` (80) — G2 is now warn-allows-by-default with a strict
  refusal variant; shared-helper byte-identity list extended with the two new helpers.

All suites green (fresh clone, offline):

```
test_asl_paths 165 · test_asl_payload_contract 165 · test_b17_b13 24 ·
test_b18_validate_throughput 27 · test_docs_params PASS · test_e2e_fixes 80 ·
test_ease_guardrails 86 · test_existing_roles 66 · test_fix6 51 · test_guardrails 80 ·
test_iam_policies 158 · test_planning_settings 58/58
```

---

## Exact runtime files changed

- `scripts/job2_load.py` — `GUARDRAILS_MODE`; G2 warn-by-default + strict in `assert_blank_allowed`;
  G3 `take_table_lock` 3-tuple (`contended`) + warn/strict/contended branching on both reblank paths.
- `scripts/glue_cdc_continuous.py`, `scripts/glue_cdc_composite.py` — `GUARDRAILS_MODE`,
  `CDC_FILE_ORDER_ACTION`, `CDC_NOPK_OVERMATCH_ACTION` globals + overlay; shared
  `guard_action_blocks` + `emit_guard_warn_metric`; G7/G8 warn-vs-block wiring; G8 LOAD-list read
  wrapped; audit_log/counter creation made non-fatal (warn + continue). Shared helpers stay
  byte-identical between the two engines.
- `scripts/job3_validate.py` — `VALIDATE_COUNT_CHECK` / `GUARDRAILS_MODE`; G10 mismatch is a
  WARNING by default, fails only under strict.
- `lambdas/params_csv.py`, `lambdas/resolve_task.py`, `lambdas/create_glue_jobs.py` — new settings
  defaults, enum validation, payload fields, and `--guardrails_mode` / per-guard args wired to the
  load, validate and CDC jobs.
- `stepfunctions/startup.asl.json` — the five new keys produced into `$.resolved` and passed to
  both `CreateGlueJobs` blocks.
- `iam/glue.json`, `iam/lambda.json`, `iam/stepfunctions.json` — removed the `_comment` keys (B22).
- `tools/setup.sh` — `split_iam` strip-unknown-keys backstop (B22).
- `glue-templates/` — none.

Non-runtime: `RUNBOOK.md`, `docs/MANUAL_SETUP.md`, `docs/IAM_POLICY_NOTES.md` (new),
`config/params.example.csv`, `config/pipeline.example.json`, `tests/*`.

---

<!-- B23 fix (rebased on top of the ease-guardrails + B22 fix) -->

# RESULT — fix B23 (`ForkCdcStartNotConfirmed` is a false failure for every fork)

**Base commit:** `cbb85014722379e4319c05df45e212accdb105be` was HEAD when this fix started
(verified with `git ls-remote`); the concurrent **ease-guardrails + B22** fix landed meanwhile, so
this fix was rebased onto `d1bd703483a45bb2929e661f67ab82a8ac0de8c1` (both sides kept).
**Fix commit (runtime + tests + RUNBOOK):** `__FIX_SHA__`

---

## Root cause (confirmed by reading the code)

The startup state machine confirms a CDC run started by polling for a marker in S3. For the
**forks** it polled the wrong prefix:

- `stepfunctions/startup.asl.json` → `StartForkCdcMap/CheckForkStarted` polled the **task-level**
  key `config/_task/<suffix>/_cdc_started/<exec>-ck-<slug>.json`.
- But the same Map starts each fork's CDC job with `--config_prefix` = the **fork** prefix
  (`plan_split` emits `s3://<bucket>/<task cp>/_orchestrator/ck-<slug>/` for ck forks and
  `.../_orchestrator/bg-<slug>/` for bg forks).
- `glue_cdc_composite.py` / `glue_cdc_continuous.py` → `write_started_marker()` writes
  `<CONFIG_PREFIX>_cdc_started/<token>.json`, i.e. under the **fork** prefix.

So the workflow polled `config/_task/<suffix>/_cdc_started/…` while the fork wrote
`…/_orchestrator/ck-<slug>/_cdc_started/…`. They never matched; the Map iteration waited its full
45-min budget (90 × 30 s) and raised `ForkCdcStartNotConfirmed`, **while the fork CDC job was
actually RUNNING and applying changes.** The main CDC job is unaffected (its `--config_prefix` IS
the task prefix, so its poll matches).

**Both fork kinds are affected.** `StartForkCdcMap` iterates all of `$.plan.forks` — composite
(`ck`, `glue_cdc_composite.py`) and big-table (`bg`, `glue_cdc_continuous.py`) — through the same
`CheckForkStarted`, so bg forks had the identical mismatch. Confirmed by `tests/test_marker_paths.py`
(ck and bg both exercised against the real `plan_split` output and the real ASL templates).

---

## Fix (robust; what was chosen and why)

**1 — Preferred: the workflow polls the fork's OWN prefix.** ASL intrinsics can't strip an
arbitrary `s3://bucket/` cleanly, so `lambdas/plan_split.py` now emits **`config_prefix_key`** for
every fork — the bare S3 key of that fork's `config_prefix` with the trailing slash (ck:
`<task cp>/_orchestrator/ck-<slug>/`; bg: derived from the fork's `config_prefix` via
`_split_s3_uri`). `startup.asl.json` passes `config_prefix_key` into the Map item and
`CheckForkStarted` builds the poll key as
`States.Format('{}_cdc_started/{}-ck-{}.json', $.config_prefix_key, $.execName, $.fork_slug)` —
exactly where the fork CDC script writes it. (The `-ck-` token is self-consistent: the ASL passes
the same `<exec>-ck-<slug>` string as `--startup_execution`, so write and poll use one token for
both ck and bg forks.)

**2 — Belt-and-suspenders: the CDC scripts also write a TASK-level copy.** When `--cdc_owners_key`
is present (it is baked into every fork CDC job as a default argument by `create_glue_jobs`, =
`config/_task/<suffix>/_jobs.json`), `write_started_marker()` writes a **second** copy of the
marker under the task-level `_cdc_started/` (derived from `cdc_owners_key`'s dirname). This means
an **older/unpatched** workflow — one still polling the task-level key — also confirms. The main
CDC run writes only once (its `CONFIG_PREFIX` already is the task prefix). Done in both
`glue_cdc_composite.py` and `glue_cdc_continuous.py`.

**3 — `_latest.json` LastModified fallback: deliberately NOT added.** It was marked optional
("only if it's simple"). It isn't simpler than the two fixes above (it needs an extra Map state
plus a HEAD/timestamp compare that ASL does awkwardly), and the exact-token poll under the fork's
own prefix is already deterministic and covered by the task-level dual-write for old workflows.
Skipping it keeps the change minimal.

### After the fix, what the error means
`ForkCdcStartNotConfirmed` now only fires when a fork CDC run genuinely did not reach its poll
loop within 45 min. RUNBOOK §8 and `~/Downloads/review3d/fix-b23/OPERATOR_CHECK.md` explain how to
tell the (now impossible) false alarm from a real failure and that **re-triggering the fleet is
not needed** when the fork run is RUNNING (the task is past full load; cutover finds the fork jobs
by tag).

---

## Tests

`tests/test_marker_paths.py` (new). For MAIN, one CK fork and one BG fork it builds the real plan
with the real `plan_split`, derives the exact S3 Prefix each confirmation state polls by evaluating
the real ASL `States.Format(...)` against the args the Map/StartCdcJob passes, derives the keys
`write_started_marker()` writes from the same args, and asserts the polled key is one the script
writes. Includes:
- a **negative** test that reconstructs the pre-fix task-level poll and proves it did NOT match a
  fork marker (today's bug) — fails if anyone reverts the ASL to the task-level prefix;
- a **positive** test that the fixed fork-prefix poll matches;
- a **back-compat** test that the task-level dual-write makes the old task-level poll match too.

All suites green (2nd fresh clone verified):
`test_asl_paths, test_asl_payload_contract, test_marker_paths, test_guardrails, test_e2e_fixes,
test_b17_b13, test_b18_validate_throughput, test_docs_params, test_fix6, test_existing_roles,
test_planning_settings`.

---

## Runtime files changed
```
lambdas/plan_split.py            (emit config_prefix_key for ck + bg forks)
scripts/glue_cdc_composite.py    (task-level dual-write of start marker; _task_marker_prefix_key)
scripts/glue_cdc_continuous.py   (same, covers main + bg forks)
stepfunctions/startup.asl.json   (CheckForkStarted polls the fork's own config_prefix_key)
```
Non-runtime: `RUNBOOK.md` (§8 `ForkCdcStartNotConfirmed` row), `tests/test_marker_paths.py`.
`glue-templates/`: none. No IAM change (the fork CDC jobs already write the fork prefix today, and
the Glue role already has task-prefix write access used by the main CDC marker).
