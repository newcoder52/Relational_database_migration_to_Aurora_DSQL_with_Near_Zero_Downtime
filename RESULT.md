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
