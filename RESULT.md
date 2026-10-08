# RESULT — main CDC job must not crash when every table is owned by a fork

## Commit

- **fix sha (runtime change):** `fe3e01effff3770050c0d46cd972f3a2f2c59f69` (short `fe3e01e`)
- parent / base: `64cc24d` (remote `main` HEAD at start, pinned via `git ls-remote`)
- branch: `main` (plain push, no force)
- committer: `newcoder52`

## Root cause

`scripts/glue_cdc_continuous.py` → `main()` builds three lists from the manifest:

- `contexts`  — tables THIS job owns and will apply (owner == `CDC_OWNER_SELF`, default `main`),
- `multi_key` — tables skipped because they have a multi-column primary key (applied by the
  separate composite CDC job),
- `not_owned` — tables whose recorded owner in `_cdc_owners.json` is **another** CDC job
  (a `bg-…` big-table fork or a `ck-…` composite fork).

The guard was:

```python
if not contexts and not multi_key:
    raise Exception("No usable tables from the manifest.")
```

It **ignored `not_owned`**. So the MAIN CDC run of a task whose tables are **all owned by fork
jobs** crashed on startup. Concrete trigger: a task with a single big table — a partitions-auto
source writes many LOAD files, the table is classified "big", its `bg-<slug>` fork owns it, so
the main run sees `contexts=[]`, `multi_key=[]`, `not_owned=[that table]` → raise.

Why that is fatal for the whole task (see `stepfunctions/startup.asl.json`): the startup state
machine starts the **main** CDC job first and must confirm it before fanning out the forks —

```
… → ResumeDmsToCdc → StartCdcJob → GetCdcRun → IsCdcRunAlive
                                 → CheckCdcStarted → IsCdcStarted → StartForkCdcMap (forks)
```

`StartForkCdcMap` (which starts the `bg-`/`ck-` fork that actually **owns the table**) only runs
**after** the main run is alive (`IsCdcRunAlive`) **and** has written its `_cdc_started` marker
(`CheckCdcStarted` polls S3 for it). A crashing main run therefore:

1. fails `IsCdcRunAlive` → terminal state **`CdcRunFailed`**, or
2. never writes the marker → **`CdcStartNotConfirmed`** (`ForkCdcStartNotConfirmed` downstream),

and in either case the forks never start → **no CDC at all** for that task.

## The fix (minimal, no other behavior change)

Raise **only** when the manifest truly yields nothing usable — no owned contexts, no multi-key
tables, **and** nothing owned by another job:

```python
if not contexts and not multi_key and not not_owned:
    raise Exception("No usable tables from the manifest.")
```

When `contexts` is empty but something is owned elsewhere, the main run takes the **existing
all-multi-key idle path**: it logs clearly that every table is owned by another CDC job (listing
them), writes its start marker (task-level + own, exactly as before via `write_started_marker()`),
and enters the poll loop with an empty `contexts` list. The loop does no work each cycle, sleeps,
and is stopped by cutover's `BatchStopJobRun` — identical to how an all-multi-key main run already
behaved. The idle log message is specialized for the three cases (all-forked / mixed / all
multi-key) but the control flow is the same.

Verified: the idle path reaches `write_started_marker()` (the key `CheckCdcStarted` polls) and
then the poll loop, so `IsCdcStarted` passes and `StartForkCdcMap` runs and starts the owning fork.

### Scope checked

- **Spark wrapper/template:** `glue-templates/cdc.json` **and** `glue-templates/cdc-spark.json`
  both run this same `scripts/glue_cdc_continuous.py`, so the one fix covers the Python-shell and
  the Spark CDC job. No separate change needed.
- **Composite script** (`scripts/glue_cdc_composite.py`): already handles `not contexts` by
  logging, writing the start marker, and returning cleanly — it never raises. No equivalent bug.
- **drain_check** (`lambdas/drain_check.py`): ownership-agnostic. It iterates **all in-scope
  manifest tables** and checks `cdc_file_status` per table; "no CDC files" counts as caught up. It
  does **not** assume the main job owns any table. No bug.
- **cutover / stop_cdc_run:** stop the CDC jobs by tag / run id; no assumption that main owns a
  table. No bug.

## Runtime files changed

- `scripts/glue_cdc_continuous.py` — the fix (the `main()` startup guard + idle logging). This is
  the only runtime artifact that changes.

Test added (not a runtime artifact):

- `tests/test_main_cdc_all_forked.py` — offline. Executes the real `main()` source with stubbed
  collaborators. Covers: (1) only table owned by `bg-…` → main does **not** raise, writes its
  start marker, idles; (2) empty ownership → still raises; (3) normal owned table → unchanged;
  plus a forked+multi-key edge case. Fails before the fix, passes after; `check()` raises under
  pytest.

Suites all green: `pytest -q` → **189 passed**; ASL audit (`tests/test_asl_paths.py`) →
**187 payload-contract checks pass**; `tests/test_asl_payload_contract.py` → **7 passed**.

---

## Customer steps

### 1. Deploy the fixed script

Upload the single changed runtime file to the pipeline's scripts prefix (overwrites in place; the
Glue job's `ScriptLocation` already points here, so the **next** run picks it up — no job edit):

```bash
aws s3 cp scripts/glue_cdc_continuous.py s3://$BUCKET/scripts/glue_cdc_continuous.py
```

If this task uses the Spark CDC engine, the same file is the script for the Spark CDC job too —
the one upload covers both.

### 2. Recover a task whose startup ended in CdcRunFailed / CdcStartNotConfirmed

A task that hit this bug has: DMS full load + validation already completed, fork jobs **created**
but **not started**, and no CDC running. You have two options.

#### Option A (recommended): re-run the startup state machine with the same input

Start a **new execution** of the startup state machine with the **same input** as the failed one.

```bash
aws stepfunctions start-execution \
  --state-machine-arn "$STARTUP_SM_ARN" \
  --input "$(aws stepfunctions describe-execution \
               --execution-arn "$FAILED_STARTUP_EXECUTION_ARN" \
               --query input --output text)"
```

Does a new startup re-run load and validation? **Yes — it re-enters them, but they short-circuit.**
The flow is `… → GroupFanOut (runs load + validate per group) → ResumeDmsToCdc → StartCdcJob →
CheckCdcStarted → StartForkCdcMap`. Load (`job2_load.py`) and validate (`job3_validate.py`) are
resume-gated via `_load_status.json`: files/tables already marked `done` are **skipped**, so for a
task that already finished load+validation they complete quickly without reloading or
re-validating data. Then `StartCdcJob` starts the (now fixed) main run, which this time reaches
its poll loop, writes `_cdc_started`, passes `CheckCdcStarted`, and `StartForkCdcMap` starts the
fork that owns the table. Net effect: no data is reloaded; CDC finally starts.

(The DMS task is already in CDC; `ResumeDmsToCdc` is idempotent. If a stale/failed main CDC run is
somehow still present, stop it first so the new start is clean.)

#### Option B: start the CDC jobs by hand

Use this if you do not want to re-drive the state machine. Read two inputs:

- the task registry `s3://$BUCKET/config/_task/<task_suffix>/_jobs.json` — contains `jobs[]` (each
  with `name`, `role`, `table`, `config_prefix`), and `cdcOwners` (table → owner slug such as
  `bg-<slug>` / `ck-<slug>`); unlisted tables default to owner `main`;
- the **failed startup execution** — its input gives `taskArn`, and `resolved.{configPrefix,
  cdcRoot,timestampColumnName,taskSuffix}`; its name is `<execName>` (used to build the fork start
  token).

Start the **main** CDC job first (role `cdc`, owner `main`), with the exact args the state machine
uses (`JobName = $.glue.jobs.cdc` — the `cdc`-role job name in `_jobs.json`):

```bash
aws glue start-job-run --job-name "<cdc-role job name from _jobs.json>" \
  --arguments '{
    "--config_prefix":"<resolved.configPrefix (task config prefix, trailing /)>",
    "--dms_task_arn":"<taskArn>",
    "--cdc_root":"<resolved.cdcRoot>",
    "--timestamp_column":"<resolved.timestampColumnName>",
    "--startup_execution":"<execName>"
  }'
```

The main job needs no `--cdc_owner_self` / `--cdc_owners_key`: it defaults `cdc_owner_self=main`
and derives `cdc_owners_key` from `--config_prefix`. With the fix it will now idle-and-mark-started
even though it owns nothing. Wait until it has written
`s3://$BUCKET/<resolved.configPrefix key>_cdc_started/_latest.json` (a few poll cycles).

Then start **each fork** CDC job listed in `_jobs.json` (`role` `bg-cdc` or `ck-cdc`), one per
entry, using that fork's `config_prefix`. Fork args mirror `StartForkCdcMap`:

```bash
aws glue start-job-run --job-name "<fork cdcJobName from _jobs.json>" \
  --arguments '{
    "--config_prefix":"<fork config_prefix from _jobs.json>",
    "--dms_task_arn":"<taskArn>",
    "--cdc_root":"<resolved.cdcRoot>",
    "--timestamp_column":"<resolved.timestampColumnName>",
    "--startup_execution":"<execName>-ck-<fork_slug>"
  }'
```

Notes:
- The fork start token is `"<execName>-ck-<fork_slug>"` for **both** `bg-` and `ck-` forks (this
  literal `-ck-` form is what the start-marker path contract expects — do not change it).
- A fork's `cdc_owner_self` / `cdc_owners_key` are baked into its job `DefaultArguments` at job
  creation (so it only applies its own table); you do not pass them on the manual start.
- Start the main job **before** the forks (the forks' tables are gated the same way; starting main
  first matches the state-machine ordering and keeps the start markers consistent).

After the forks are running, the normal cutover path (`drain_check` → stop CDC → drop tags) works
unchanged.
