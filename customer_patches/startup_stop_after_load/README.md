# customer_patches/startup_stop_after_load

> **Included in `main` as of the integration commit** _"stopAfterLoad: integrate load-only
> startup option into lambdas/stepfunctions/tests"_. From that commit on, the real repo files
> (`lambdas/resolve_task.py`, `lambdas/preflight_tasks.py`, `stepfunctions/startup.asl.json`,
> `tests/test_asl_payload_contract.py`) already contain this change, produced by applying
> `startup_stop_after_load.repo.diff` to the pristine `main` files (the resulting bytes match the
> NEW sha256 values in `SHA256SUMS`). **A customer deploying `main` gets `stopAfterLoad` with no
> patching.** This folder is kept for **older, hand-patched deployments** (no git checkout): the
> `apply_stop_after_load_patch.py` script patches a deployed environment in place, and is
> idempotent — a second apply on an already-integrated environment is a no-op. On a current `main`
> checkout the plain-file diff is already applied, so there is nothing to apply from the repo.
>
> **Release candidate `release-1`:** this feature ships to `main` in the Sunday release via the
> merge commit _"Merge remote-tracking branch 'integrate/integrate-pc-sal' into release-1"_, which
> carries the integration commit above (feature commit `98d6c8d`). Verified on the merged tree: a
> **dry run** of `apply_stop_after_load_patch.py` against the merged `stepfunctions/startup.asl.json`,
> `lambdas/resolve_task.py` and `lambdas/preflight_tasks.py` (fake boto3) reports **SAME (no-op)**
> for every state machine and Lambda — `patch_startup_asl` recognises the load-only states are
> already present and returns unchanged (fail-closed: it would raise a clear `PatchError` on a
> half-patch), and no `update_*` call is made.

An **optional, fleet-level run parameter** that makes the startup workflow **stop after the full
load**: `stopAfterLoad`. When it is `true`, every table group **loads** exactly as today
(GroupLoad / GroupLoadBig, all groups, same retries and resume semantics), but **no validation
Glue job is started, DMS is NOT resumed, and CDC is NOT started**. The run ends in a new
`Succeed` terminal, `TaskLoadOnlySucceeded`. When it is absent / `null` / `false`, behaviour is
**byte-identical to today**.

It follows the existing `override` path exactly:
`fleet start input -> preflight_tasks -> child startup input -> resolve_task (resolved dict)
-> startup.asl.json ($.resolved.stopAfterLoad, IsPresent-guarded)`.

> ## ⚠️ DO NOT CUT OVER A LOAD-ONLY TASK
>
> **Never run cutover on a task whose startup ended in `TaskLoadOnlySucceeded`** (i.e. a task that
> has the marker `config/_task/<suffix>/_load_only.json`) **until a normal startup — started
> WITHOUT `stopAfterLoad` — has resumed DMS and started CDC.**
>
> A load-only run leaves the task at `STOPPED_AFTER_CACHED_EVENTS` with **no CDC ever started**.
> The cutover state machine has **no precondition that CDC ran** (a pre-existing design gap, not
> changed by this patch): if you cut over a load-only task it will **not refuse** — it passes the
> validation gate (0 failures), stops the already-stopped DMS task, "drains" trivially (there is no
> CDC manifest, so it is reported caught-up), stops a non-existent CDC run, deletes the Glue jobs
> and reports **success** — even though **no post-full-load change data was ever applied**. The
> result is silent data loss for any row changed after the full load.
>
> This patch deliberately does **not** change cutover code. Guard it operationally:
>
> **Before cutover, check the task is NOT in the load-only steady state:**
> ```
> # 1) Is there a load-only marker? (present => the task stopped after load)
> aws s3 ls s3://<bucket>/config/_task/<suffix>/_load_only.json
> # 2) Has CDC ever started for this task? (present => CDC ran; safe to consider cutover)
> aws s3 ls s3://<bucket>/config/_task/<suffix>/_cdc_started/
> ```
> If `_load_only.json` exists **and** `_cdc_started/` is empty/absent, the task is still load-only:
> **run the normal startup first** (`{"taskArn":"..."}` with no `stopAfterLoad`, or a fleet run
> without the flag). That run resumes DMS and starts CDC; `_cdc_started/` then appears. Only after
> that should the task be cut over. (The `_load_only.json` marker is informational and is not
> deleted by the later startup, so rely on the **presence of `_cdc_started/`** — i.e. that CDC has
> run — as the go/no-go signal, not on the absence of `_load_only.json`.)


## The parameter

Top-level boolean on the **fleet** startup input, spelled exactly like the existing top-level
`override` / `tasksFile`:

```json
{ "bucket": "<pipeline-bucket>", "stopAfterLoad": true }
```

and (the fleet passes it through, like `override`) on the **per-task** startup input:

```json
{ "taskArn": "arn:aws:dms:<region>:<account>:task:<id>", "stopAfterLoad": true }
```

It applies to **every task** of that fleet run.

### Strict (unlike `override`)

`override` is lenient (`"yes"`, `1`, `"on"` are all truthy). `stopAfterLoad` is **strict**: only a
real boolean `true`/`false`, or the strings `"true"`/`"false"` in any case (surrounding spaces
ignored). **Anything else** (`"yes"`, `1`, `""`, `"on"`, `[]`, …) is **rejected in the fleet
PREFLIGHT, before any task is started**, with a message naming the field. The per-task
`ResolveTask` applies the same strict rule (`resolve_task._normalize_stop_after_load`, which
mirrors the string-casing of `_normalize_override` but rejects the rest), so a typo can never
silently turn load-only on or off.

An optional per-task CSV column `stop_after_load` in `fleet_tasks.csv` is also accepted (same
strict rule). **Rule: TRUE WINS.** The flag is a safety reduction (stop earlier, never resume DMS
/ start CDC), so it is ON when **either** the fleet flag **or** the per-task cell asks for it; a
fleet-level `false` can **not** turn off a per-task `true`, and a per-task blank/false can **not**
turn off a fleet-level `true`. The child input only ever gains `"stopAfterLoad": true` — it is
never written `false`, so the **default child input is byte-identical to today**.

## Behaviour when `stopAfterLoad` is true

* RunDiscovery, plan, create jobs and **the LOAD** (GroupLoad / GroupLoadBig, all groups, same
  retries and resume) run exactly as today.
* In each group, after the load finishes the path to `GroupValidate` is **skipped**
  (`AfterGroupLoad` Choice → `GroupOk`); **no validate Glue job is started**.
* After all groups finish:
  * every group loaded OK → **`TaskLoadOnlySucceeded`** (a `Succeed` state). DMS is left
    `STOPPED_AFTER_CACHED_EVENTS`; validation and CDC were **not** run and DMS was **not** resumed.
    An operator-visibility marker `config/_task/<suffix>/_load_only.json` is written
    (`RecordLoadOnly`, reusing `resolve_task` mode `write_load_only_record`); a marker-write
    failure is caught and the run still succeeds (the load succeeded; the marker is informational).
  * any group's **load** failed → **`GroupsFailed`**, exactly as today. **`override` does NOT turn
    a failed load into success when `stopAfterLoad` is set** — load-only never consults
    `$.resolved.override`.
* **No path** calls `ResumeDmsToCdc`, `StartCdcJob`, the CDC fork starts,
  `WriteStartupOverrideRecord`, or the validation Glue job when the flag is true. This is proved by
  an executing ASL walk test (`test_stop_after_load_patch.py`) that walks every reachable path from
  each branch of the flag decision and asserts those states are unreachable.

To **continue** a task after a load-only run, start the **normal** startup for it **without** the
flag (see the correctness note below).

## Correctness: resuming after a load-only run

**Question:** after a load-only run the DMS task is left stopped after the full load (the startup
waited at `IsDmsDone` for `STOPPED_AFTER_CACHED_EVENTS`). Is a **later normal startup** (no flag)
for the same task accepted, and does it then skip already-loaded tables, validate, resume DMS and
start CDC?

**Answer (verified in code + test): YES, it is accepted.** `resolve_task._check_startup_task`
(used by both the per-task `ResolveTask` and the fleet preflight) computes
`past_full_load = pct == 100 AND (status in {running,starting,resuming,modifying} OR (status ==
stopped AND "CACHED_EVENTS" not in reason AND "FULL_LOAD" not in reason))`. A load-only run leaves
the task **stopped with `STOPPED_AFTER_CACHED_EVENTS`**, so `"CACHED_EVENTS" in reason` →
`past_full_load` is **False** → the task is **not** refused as "already finished its full load".
This is exactly the state a fresh full-load-complete task is in, which `IsDmsDone` routes to
`BuildTableList`. The later normal run then:

* **skips already-loaded tables** — `job2_load` reads `_load_status.json` and skips tables whose
  status is `done` (regardless of `override`), so the re-run loads nothing already loaded;
* runs **validation** (the validate job is no longer skipped), **resumes DMS** (`ResumeDmsToCdc`)
  and **starts CDC** (`StartCdcJob`) on the normal path.

So **no preflight change is needed** for resumability, and none was made. Test:
`test_later_normal_startup_accepted_after_load_only` (plus a contrast case proving a task that
genuinely moved past its full load is still refused).

A task whose startup is **still RUNNING** is skipped by the fleet preflight exactly as today
(`skip = "already_running"`); this patch does not change that.

## Files

* `apply_stop_after_load_patch.py` — apply/dry-run/restore against a **deployed** environment
  (no git checkout).
* `stop_after_load_transform.py` — the **shared** fail-closed transforms (used by the apply
  script, the plain-file path, and the tests), so the deployed change and the repo diff are
  provably the **same** change.
* `startup_stop_after_load.repo.diff` — the **plain-file** unified diff against `main` (for people
  who deploy from the repo via `tools/setup.sh`). It covers **all four** changed files:
  `lambdas/resolve_task.py`, `lambdas/preflight_tasks.py`, `stepfunctions/startup.asl.json`, and
  `tests/test_asl_payload_contract.py` (the audit registration — see the note below; without it a
  repo deployer's suite fails). Per-file diffs are also included:
  `*.resolve_task.diff`, `*.preflight_tasks.diff`, `*.startup_asl.diff`,
  `*.test_asl_payload_contract.diff`.
  (`stepfunctions/fleet-startup.asl.json` is **unchanged** — it already forwards the whole fleet
  input to Preflight as `fleetInput.$ = $`, so `stopAfterLoad` reaches preflight and the children
  with no ASL change there.)
* `test_stop_after_load_patch.py` — executing, offline tests (no AWS).
* `test_run_output.txt` — a sample dry-run (against fake clients).
* `SHA256SUMS` — sha256 old→new for each repo file this patch changes.
* `orig/` — snapshots of the files as they are on `main` (the diff/transform baseline).

## Deployed environment (no git checkout)

Default is **DRY RUN** — it prints a unified diff of each state-machine definition and of each
patched `.py`, and **changes nothing**:

```
python3 apply_stop_after_load_patch.py --project <project> --region <region> --bucket <bucket>
```

`--apply` performs it in a **safe order** (Lambdas first, then the `<project>-startup` state
machine, then the `<project>-fleet-startup` state machine), backing up every original (definitions
as `.json`, Lambda code as `.zip`) to `s3://<bucket>/scripts/backup/startup_stop_after_load/<ts>/`
**and** locally first, and **reading each resource back to compare** after it updates it:

```
python3 apply_stop_after_load_patch.py --project <project> --region <region> --bucket <bucket> --apply
```

`--restore <local-backup-dir>` puts everything back from a backup dir:

```
python3 apply_stop_after_load_patch.py --project <project> --region <region> --bucket <bucket> \
    --restore customer_patches/startup_stop_after_load/_backups/<ts>
```

What it does, precisely:

* reads the two definitions with `stepfunctions:DescribeStateMachine` (names `<project>-startup`
  and `<project>-fleet-startup`);
* patches the startup JSON **by anchors on structure + state names** (never on ARNs: the
  `<<PLACEHOLDERS>>` are already filled in a deployed definition). It **fails closed** if an anchor
  is missing/ambiguous or the shape is not recognised — nothing is written. The fleet definition is
  verified recognised and left **unchanged**;
* patches the Lambda code of `<project>-resolve-task` and `<project>-preflight-tasks` by downloading
  the deployed zip (`lambda:GetFunction`), editing **only** `resolve_task.py` / `preflight_tasks.py`
  inside it (every other file — `params_csv.py`, etc. — is kept **byte-identical**), re-zipping and
  calling `lambda:UpdateFunctionCode`;
* never touches any other Lambda or state machine, and **never calls IAM**.

Idempotent: a second `--apply` on an already-patched environment is a **no-op** (the transforms
detect the patch and produce an empty diff).

### Exact IAM actions the operator needs

Only these (no `iam:*`, no wildcards needed):

| Action | On |
|---|---|
| `states:DescribeStateMachine` | `<project>-startup`, `<project>-fleet-startup` |
| `states:UpdateStateMachine` | `<project>-startup` (and `<project>-fleet-startup` only if ever changed; this patch leaves it unchanged) |
| `lambda:GetFunction` | `<project>-resolve-task`, `<project>-preflight-tasks` |
| `lambda:GetFunctionConfiguration` | `<project>-resolve-task`, `<project>-preflight-tasks` |
| `lambda:UpdateFunctionCode` | `<project>-resolve-task`, `<project>-preflight-tasks` |
| `s3:PutObject` | `arn:aws:s3:::<bucket>/scripts/backup/startup_stop_after_load/*` |

(`lambda:GetFunction` returns a presigned URL for the code zip; downloading it needs no extra IAM.)

## Repo deployers (setup.sh path)

If you deploy from the repo, apply `startup_stop_after_load.repo.diff` to a clean checkout of
`main` and redeploy with `tools/setup.sh` as usual:

```
git apply customer_patches/startup_stop_after_load/startup_stop_after_load.repo.diff
```

The diff touches only:

* `lambdas/resolve_task.py` — add strict `_normalize_stop_after_load`, add `stopAfterLoad` to the
  resolved dict, add the `write_load_only_record` handler + `_load_only.json` marker.
* `lambdas/preflight_tasks.py` — strict fleet-level normalize (reject invalid before any start),
  per-task passthrough with the TRUE-WINS rule.
* `stepfunctions/startup.asl.json` — `IsLoadOnly`/`MarkLoadOnly`/`MarkNotLoadOnly` before the group
  fan-out; `stopAfterLoad` on the Map item; `AfterGroupLoad` (skip validate when load-only);
  `LoadOnlyOrContinue` + `RecordLoadOnly` + `RecordLoadOnlyFailedButLoaded` + `TaskLoadOnlySucceeded`.
* `tests/test_asl_payload_contract.py` — register the new `write_load_only_record` Lambda mode (2
  lines; mandatory, see the note below).

`stepfunctions/fleet-startup.asl.json` is **not** changed.

> The diff also updates one line-set in the repo's own audit test
> `tests/test_asl_payload_contract.py` (registering the new `write_load_only_record` Lambda mode in
> `MODE_RETURN_FUNCS` and `MODE_REQUIRED_INPUT`). That audit auto-discovers Lambda modes from the
> patched ASL and **requires** every mode to have a declared return/input contract; the new
> `RecordLoadOnly` state uses that mode, so this registration is **mandatory** — without it a repo
> deployer's own suite fails 2 tests. It is included in `startup_stop_after_load.repo.diff`. No repo
> documentation is edited by this patch.

## Tests

```
python3 customer_patches/startup_stop_after_load/test_stop_after_load_patch.py
# or: python3 -m pytest customer_patches/startup_stop_after_load/test_stop_after_load_patch.py
```

Coverage: strict normalize matrix; preflight (fleet/per-task true/false/invalid, invalid rejected
before any start, child-input byte-identical when absent, TRUE WINS); the ASL walk (validation /
`ResumeDmsToCdc` / `StartCdcJob` / fork starts / `WriteStartupOverrideRecord` unreachable when the
flag is true; reachable on the normal path); **reuse of the repo's ASL path audit on the patched
definitions**; byte-identical state sequence when the flag is absent (same sequence as the
original); the apply script against fake boto3 (dry run changes nothing, apply updates in safe
order with read-back, second apply is a no-op, restore works, missing anchor ⇒ nothing changed);
three mutation checks (remove the validate skip; route the flag into `ResumeDmsToCdc`; accept
`'yes'` in the normalizer — each makes a test fail); and the resumability correctness test.
