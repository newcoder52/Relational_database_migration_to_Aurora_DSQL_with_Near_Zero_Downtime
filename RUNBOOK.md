# RUNBOOK — deploy and run the Oracle → Aurora DSQL migration pipeline

Follow this top to bottom. It is plain AWS CLI, the same on **macOS, Linux and AWS CloudShell**.

The pipeline runs tasks **one way only: through the fleet.** You trigger `fleet-startup` (and later
`fleet-cutover`) once, with `{"bucket":"<bucket>"}`; the fleet reads
`s3://<bucket>/config/fleet_tasks.csv` and starts the per-task `startup` (or `cutover`) state machine
for every row. **One DMS task is one row in that CSV**; a new wave is just a new
`config/fleet_tasks.csv` and another trigger. You never start a per-task state machine yourself — the
per-task machines appear only in [Reference](#10-reference), as what the fleet runs.

Operator files always live in the one fixed folder `s3://<bucket>/config/` — `params.csv`,
`fleet_tasks.csv`, and the generated `pipeline.json`. The fleet reads `config/params.csv` and
`config/fleet_tasks.csv` from there; there is nothing else to point it at.

- One-time setup (§4) with `tools/setup.sh`: minutes, plus driver wheels.
- Running tasks after that (§5–§7): a few minutes of your time, plus the load and CDC that run on
  their own.

> **New here?** Read [`README.md`](README.md) for the big picture, then come back.
> [`USAGE_GUIDE.md`](USAGE_GUIDE.md) covers day-to-day monitoring once a task is running.
> Setting up **without** `tools/setup.sh`? Every command the script runs is in
> [`docs/MANUAL_SETUP.md`](docs/MANUAL_SETUP.md).

**Where things live.** Three places, don't confuse them:

- **Your local clone** of this GitHub repo — the *code*: `iam/`, `lambdas/`, `scripts/`,
  `glue-templates/`, `stepfunctions/`, `tools/setup.sh`, `config/`. You run every command below from
  the clone's root folder. `tools/setup.sh` reads these files and pushes them to AWS.
- **One S3 bucket** — what the pipeline *reads at run time*: the Glue scripts, templates and driver
  wheels, `config/pipeline.json` (settings), `config/params.csv` and `config/fleet_tasks.csv`, and
  the DMS output CSVs. **The DMS S3 target endpoint must write to this same bucket.**
- **AWS resources** setup creates: 3 IAM roles, 8 Lambda functions, 4 Step Functions state machines,
  and (optional) a Glue network connection.

**Contents**

- [1. Get the code](#1-get-the-code)
- [2. What you need](#2-what-you-need)
- [3. Fill in params.csv](#3-fill-in-paramscsv)
- [4. Set up](#4-set-up)
- [5. Run tasks with the fleet](#5-run-tasks-with-the-fleet)
- [6. Watch progress](#6-watch-progress)
  - [Connect to DSQL and check progress](#connect-to-dsql-and-check-progress)
- [7. Cut over with the fleet](#7-cut-over-with-the-fleet)
- [8. If something fails](#8-if-something-fails)
- [9. Reload a task from scratch](#9-reload-a-task-from-scratch)
- [10. Reference](#10-reference)

---

## 1. Get the code

**You need an AWS shell with:** `git`, **Python 3.9+**, `pip`, `zip`, and **AWS CLI v2**. The simplest
option is **AWS CloudShell** — it has all of them pre-installed. Open CloudShell **in the same AWS
region as your DMS tasks and the pipeline** (the region you'll put in `params.csv`), so the CLI
defaults to it.

**Clone the repo and move into it:**

```bash
git clone https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime.git
cd Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime
```

**Confirm you're in the right folder** — these must all exist:

```bash
ls tools/setup.sh lambdas iam
```

**Every command in this RUNBOOK runs from this folder** (the repo root). `tools/setup.sh` even
refuses to run elsewhere (it looks for `lambdas/params_csv.py`).

**No git / GitHub access from your shell?** On a laptop that can reach GitHub, open the repo page,
choose **Code → Download ZIP**, then in CloudShell use **Actions → Upload file** to upload the ZIP,
and:

```bash
unzip Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime-main.zip
cd Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime-main   # note the -main suffix
ls tools/setup.sh lambdas iam
```

(A ZIP download unpacks into a folder with a `-main` suffix; a `git clone` does not.)

**Getting a newer version later:** `git pull` in this folder (or download a fresh ZIP), then re-run
setup ([§4](#4-set-up)) — it is create-or-update, so it only applies what changed.

---

## 2. What you need

Work through this before setup. Each item is something the pipeline assumes.

- [ ] **The code**, from [§1](#1-get-the-code), with your terminal in the repo root
      (`aws sts get-caller-identity` should print your account).
- [ ] **An S3 bucket** for the pipeline (scripts, templates, driver wheels, settings, task list, and
      the DMS output CSVs all live here). Use an existing bucket or create one — this is the bucket
      you name in setup, and **the DMS S3 target endpoint must write to this same bucket** (the Glue
      jobs read the DMS files from it):
      ```bash
      aws s3api create-bucket --bucket "<bucket>" --region "<region>" \
        --create-bucket-configuration LocationConstraint="<region>"   # omit --create-bucket-configuration in us-east-1
      ```
- [ ] **An Aurora DSQL cluster** and its endpoint — the cluster endpoint from the DSQL console
      (`<cluster>.dsql.<region>.on.aws`). This is the `dsql_endpoint` value in
      [§3](#3-fill-in-paramscsv). If Glue has no internet, it needs a route to DSQL (a DSQL VPC
      endpoint); the pipeline tries the reachable hostname automatically, so you still use the
      console cluster endpoint either way.
- [ ] **Target tables already created** in the target schema, each **with its primary key**. The
      pipeline loads into existing tables; it never creates them. If a table that the DMS task
      replicates has no table in DSQL, discovery now **fails fast** and names it (create it, then
      re-trigger the fleet) rather than silently skipping it. A single-column primary key gets full
      insert/update/delete CDC; a table with a **multi-column** primary key is applied by its own
      per-table composite (`ck`) fork CDC job (see [Rules for the task list](#rules-for-the-task-list)).
- [ ] **No binary (`bytea`) column in a primary key.** DSQL rejects `bytea` in a key
      (`0A000: datatype bytea is not supported in a key`). Map an Oracle `RAW`/`BLOB` **key** column
      to `uuid` (for 16-byte GUID keys) or `text` in the target DDL — not `bytea`. (The pipeline
      itself maps `RAW`/`BLOB` keys to `uuid`.)
- [ ] **At most 9 schemas of your own** in the DSQL database. DSQL allows 10 schemas per database
      (not adjustable) and the pipeline adds `cdc_control`. Count yours:
      `SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg\_%' AND schema_name <> 'information_schema';`
- [ ] **A network path from Glue (and the two DSQL Lambdas) to DSQL**, if your account is locked
      down: a **private subnet** and a **security group** that can reach DSQL (allow all TCP from
      itself; outbound 443 and 5432), an **S3 gateway endpoint** in that subnet's route table, and a
      DSQL VPC endpoint with **private DNS on**. Set `subnet_id`/`security_group_id` in `params.csv`
      so setup builds a Glue network connection. Skip the VPC values if Glue can already reach DSQL.
- [ ] **A DMS task** of type **`full-load-and-cdc`** for every task you'll list. Preflight (and each
      task's startup) checks these *before* DMS starts, so a mistake fails in seconds, not hours:
  - `FullLoadSettings.StopTaskCachedChangesApplied = true` (and `StopTaskCachedChangesNotApplied`
    **not** true).
  - A **short task name** — letters, digits and hyphens, no leading/trailing hyphen, roughly under
    50 characters. It becomes the task's S3 folder and the Glue job names.
  - An **S3 target endpoint** writing to **your pipeline bucket**, with `AddColumnName = true`,
    `TimestampColumnName = dms_timestamp`, `Rfc4180 = true`, and **no** `CompressionType` (plain CSV,
    default flat layout — `DatePartitionEnabled` must not be on).
  - The S3 target endpoint's **`ServiceAccessRole` must be allowed to write to the pipeline bucket.**
    A fresh bucket with a reused DMS role fails at full load (DMS writes nothing). Grant the role at
    least these actions on the bucket and its objects:
    ```json
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject",
                 "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"]
    }
    ```
  - A table mapping with a **convert-lowercase rule for columns** (column names must be lowercase in
    DSQL; schema/table names may be any case). The DSQL schema name must match the DMS target schema
    in lowercase.
  - The DMS task in the **same region** as the pipeline.
  - For tables **without a primary key**: DMS set to emit inserts and deletes only (updates are
    skipped and logged).
  - **NULLs:** a value is stored as NULL only when the field is empty **or** equals the endpoint's
    `CsvNullValue` (DMS writes the literal text `NULL` when `CsvNullValue` is unset). Every other
    text, including `NA`, `NONE` and `N/A`, is stored as text. Don't change `CsvNullValue` partway
    through a migration.

---

## 3. Fill in params.csv

Every value the pipeline needs lives in one `params.csv` (one `parameter,value` per row). Copy the
example, fill it in, and upload it to **`s3://<bucket>/config/params.csv`** — the one fixed location
both `tools/setup.sh` and the fleet's preflight read it from (the task list in §5 goes in the same
`config/` folder). Both read it with the same parser (`lambdas/params_csv.py`).

```bash
cp config/params.example.csv params.csv
# edit params.csv (see the table below), then upload it:
aws s3 cp params.csv "s3://<bucket>/config/params.csv"
```

Header must be exactly `parameter,value`; one row per key; blank lines and lines starting with `#`
are ignored; values are trimmed; a duplicate or unknown key is an error. The **bucket is not a key**
— it is the bucket the CSV itself lives in. `dsql_cluster_id` is **derived** (first label of
`dsql_endpoint`) and must not be listed.

The table below is **derived from the code**, not from prose: for every key it names the actual
script / Lambda / state machine that reads it (**Used by**) and the pipeline phase(s) it acts in
(**Phase(s)**). Phases, in pipeline order, are: **Setup → Preflight → Discovery → Planning →
Full load → Validation → CDC → Cutover**. A key used in more than one phase appears **once**, under
its main phase, with every phase it touches listed in its **Phase(s)** cell. Rows are grouped by
phase under the bold sub-headings below.

**When a change takes effect** (read the per-row note; this is the general rule):
- **Most pipeline keys** take effect for **tasks started after `pipeline.json` is republished**
  (upload `params.csv` → republish `pipeline.json` → the next `startup`/`cutover` run reads it).
- **Setup-only keys** take effect only when you **re-run setup**.
- **Keys baked into Glue job args at job creation** — the worker sizes, worker counts, timeouts,
  `glue_version`, `glue_connection`, and every CDC `--cdc_*` guardrail/validation arg — apply **only
  to Glue jobs created after the change. Already-created jobs keep their old values** until those
  jobs are deleted and re-created (new task startup, or a manual job re-create). See the
  "Which settings affect a running task?" note after the tables.

**Setup only** (not written to `pipeline.json`; used by `tools/setup.sh` — re-run setup for a change to take effect)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `account_id` | **required** | — | Setup | `tools/setup.sh` (role-ARN derivation + IAM calls); `lambdas/params_csv.py` validates it | 12-digit AWS account id (setup/IAM only; never written to `pipeline.json`). `resolve_task` takes the run's account from the DMS task ARN, not this key. Takes effect on the next setup run |
| `subnet_id` | optional (setup-only) | — | Setup | `tools/setup.sh` Step 1b (Glue VPC connection) | private subnet for the Glue VPC connection. Set **both** `subnet_id` and `security_group_id`, or neither. Not in `pipeline.json`. Re-run setup to apply |
| `security_group_id` | optional (setup-only) | — | Setup | `tools/setup.sh` Step 1b (Glue VPC connection) | security group for the Glue VPC connection. Both-or-neither with `subnet_id`. Not in `pipeline.json`. Re-run setup to apply |
| `manage_iam` | optional (setup-only) | `true` | Setup | `tools/setup.sh` (IAM create vs read-only mode) | `true`: setup **creates/updates** the three roles as always. `false`: setup only **reads** the three roles you already have (`iam get-role` + trust check + a best-effort `simulate-principal-policy`) and **never** makes an IAM write call — it writes each role's policy JSON to `iam-out/` for your IAM team instead. See [§4 "Using roles your IAM team already created"](#using-roles-your-iam-team-already-created-manage_iamfalse). Not in `pipeline.json`. Re-run setup to apply |
| `lambda_role_arn` | optional (setup-only) | `arn:aws:iam::<account_id>:role/<project>-lambda-exec-role` | Setup | `tools/setup.sh` (Lambda role; role **name** = last ARN segment) | the role **all 8 Lambdas** run as. Set it to point the Lambdas at a role your IAM team already made. Must be an IAM role ARN in `account_id`; a path is allowed. Not in `pipeline.json`. Re-run setup to apply |
| `sfn_role_arn` | optional (setup-only) | `arn:aws:iam::<account_id>:role/<project>-sfn-exec-role` | Setup | `tools/setup.sh` (state-machine role; role **name** = last ARN segment) | the role **all 4 state machines** run as. Same rules as `lambda_role_arn`. Not in `pipeline.json`. Re-run setup to apply |

**Everywhere** (connection / identity keys read by essentially every component)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `region` | **required** | — | Setup, Discovery, Full load, Validation, CDC, Cutover | `tools/setup.sh`; `resolve_task` (region check); `create_glue_jobs` `--region`; `job1_discovery`, `job2_load`, `job3_validate`, the CDC jobs; `drain_check`/`drop_tags` | AWS region of the DMS tasks and pipeline (must equal the task ARN's region). Takes effect on tasks started after republish; baked into existing Glue job args at creation |
| `project` | **required** | — | Setup, Discovery, Planning, Full load, Validation, CDC, Cutover | `tools/setup.sh` (resource names); `resolve_task` (builds Glue job names `project-suffix-role`); `plan_split` | short prefix (letters, digits, hyphens) for role, Lambda and job names. Takes effect on the next task started after republish (new job names) |
| `dsql_endpoint` | **required** | — | Discovery, Full load, Validation, CDC, Cutover | `resolve_task` (emits `dsqlEndpoint` + candidate list); `create_glue_jobs` `--dsql_endpoint`/`--dsql_endpoint_candidates`; `job1_discovery`, `job2_load`, `job3_validate`, the CDC jobs; `drain_check`/`drop_tags`. `dsql_cluster_id` is derived from it by `tools/setup.sh` | the cluster endpoint from the DSQL console, `<cluster>.dsql.<region>.on.aws`. If Glue has no internet, it needs a route to DSQL (e.g. a DSQL VPC endpoint); the pipeline picks the reachable hostname automatically. Takes effect on tasks started after republish; baked into existing Glue job args at creation |
| `dsql_user` | optional | `admin` | Discovery, Full load, Validation, CDC, Cutover | `create_glue_jobs` `--dsql_user`; `job1_discovery`, `job2_load`, `job3_validate`, the CDC jobs; `drain_check`/`drop_tags` | DSQL user. Takes effect on tasks started after republish; baked into existing Glue job args at creation |
| `dsql_database` | optional | `postgres` | Discovery, Full load, Validation, CDC, Cutover | `create_glue_jobs` `--dsql_database`; `job1_discovery`, `job2_load`, `job3_validate`, the CDC jobs; `drain_check`/`drop_tags` | DSQL database. Takes effect on tasks started after republish; baked into existing Glue job args at creation |
| `control_schema` | optional | `cdc_control` | CDC, Cutover | `create_glue_jobs` `--control_schema` (CDC jobs); `glue_cdc_continuous`/`glue_cdc_composite`; `drain_check` (cutover validation gate query) | DSQL schema for the CDC control tables. Takes effect on CDC jobs created after republish; a running CDC job keeps the schema it started with |
| `glue_connection` | optional | `""` (no VPC) | Setup, Discovery, Full load, Validation, CDC | `tools/setup.sh` Step 1b (creates the connection); `create_glue_jobs` `_connections_for` → each Glue job's `Connections` | the Glue network connection's **exact** name; `""` = Glue runs with no VPC connection. **Baked into the Glue job definition at job creation** — only jobs created after the change use the new value |
| `glue_role_arn` | optional | `arn:aws:iam::<account_id>:role/<project>-glue-exec-role` | Setup, Discovery, Full load, Validation, CDC | `tools/setup.sh`; `create_glue_jobs` (the `Role` on every Glue job) | set only if your Glue role name differs from the default. A role **path** is allowed; the role **name** is the last ARN segment. **Baked into the Glue job definition at job creation** — only jobs created after the change use the new value |

**Planning / fan-out** (read by `plan_split`; most of these never leave the Planning Lambda)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `max_composite_forks` | optional | `8` | Planning | `plan_split` (PlanSplit state; gate) | max composite-PK tables that may be forked out of **one** task (each runs its own always-on CDC job, plus its own load/validate jobs). A task with **more** composite tables than this fails early at startup `PlanSplitFailed` — the cause names the tables — and **no** Glue jobs are created. Raise it (mind Glue job/concurrent-run and DSQL connection quotas) or split the task. Takes effect on tasks started after republish |
| `max_big_cdc_forks` | optional | `8` | Planning | `plan_split` (PlanSplit state) | max **big** single-/no-PK tables that get their **own** CDC job (`bg` fork); each big table keeps the shared `load-big` + `validate`. Big tables **past** the cap are **not** a failure — they stay on the **main** CDC job (serial apply) with a warning. Raise it to give more big tables their own CDC job. Takes effect on tasks started after republish |
| `big_table_row_threshold` | optional | `6000000` | Planning | `plan_split` (big-table classification) | int ≥ 1. A table with **≥ this many** full-load rows is **big** (own `load-big` group **and** its own `bg` CDC job). Lower to treat more tables as big; raise for fewer. Applies only to tasks **started after** this is published; never re-assigns a table whose CDC already started |
| `file_fanout_threshold` | optional | `8` | Planning | `plan_split` (big-table classification) | int ≥ 1. A table with **≥ this many** LOAD part-files is also **big** (same effect as the row threshold). Same apply-after-publish / no-reassign rule |
| `big_table_bytes_threshold` | optional | `1000000000` | Planning | `plan_split` (big-table classification) | int ≥ 1. A table whose **total full-load bytes ≥ this** is also **big**. Rescues a huge **single-file** table (num_files=1) whose DMS row count is missing from the index — so the biggest table still gets its own load-big group + bg CDC job. Takes effect on tasks started after republish |
| `max_groups` | optional | `10` | Planning | `plan_split` (group/pool sizing) | int ≥ 1. Cap on load/validate groups per task (**= the pre-created CDC job pool size**); big tables each take one group, small tables bin-pack into the rest. Raise it to spread small tables across more lanes. Takes effect on tasks started after republish |
| `map_max_concurrency` | optional | `6` | Planning, Full load, Validation | `plan_split` (`GroupFanOut` Map concurrency + writers-per-loader sizing) | int **1–40**. How many groups/forks load+validate **at once** (also the `GroupFanOut` Map concurrency). Higher = faster but more concurrent Glue runs and DSQL connections. Takes effect on tasks started after republish |
| `min_writers_per_loader` | optional | `100` | Planning, Full load | `plan_split` (floor for a group's `--max_write_concurrency` passed to `job2_load`) | int ≥ 1. Floor for a small group's DSQL write concurrency. Must be **≤** `max_writers_per_loader`. Takes effect on tasks started after republish (baked into the loader job args at creation) |
| `max_writers_per_loader` | optional | `150` | Planning, Full load | `plan_split` (ceiling for a group's `--max_write_concurrency` passed to `job2_load`) | int ≥ 1. Ceiling for a small group's DSQL write concurrency. Must be **≥** `min_writers_per_loader`. Takes effect on tasks started after republish (baked into the loader job args at creation) |
| `conn_budget` | optional | `900` | Planning, Full load, Validation | `plan_split` (writers-per-loader sizing → `job2_load`); `create_glue_jobs` `--conn_budget` → `job3_validate` (hard-caps validate parallelism) | int ≥ 1. DSQL connection budget shared across in-flight loaders; sets writers-per-loader (the planner never exceeds it) and hard-caps validation parallelism. Takes effect on tasks started after republish (baked into job args at creation) |

**Full load** (read by `job2_load`; `plan_split` passes the per-group values; sizing/`--arg` values are baked at job creation)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `max_files_in_parallel` | optional | `30` | Planning, Full load | `plan_split` → `--max_files_in_parallel`; `job2_load` | int ≥ 1. Per-loader cap on LOAD files read at once. Baked into the load job args at creation — only jobs created after the change use the new value |
| `writers_per_file` | optional | `8` | Planning, Full load | `plan_split` → `--writers_per_file`; `job2_load` | int ≥ 1. Concurrent DSQL writer threads for **one** part-file. The DMS full load is usually a **single** LOAD*.csv per table, so this is the lever that parallelises a big single-file table; `1` = the old one-writer-per-file behaviour. Bounded by the per-group write cap. Baked into the load job args at creation |
| `max_parallel_tables` | optional | `20` | Full load | `create_glue_jobs` `--max_parallel_tables`; `job2_load` | int 1–40. How many tables load **at once** on the driver thread pool. Baked into the load job args at creation — only jobs created after the change use the new value |
| `per_worker_mem_budget_mb` | optional | `1500` | Full load | `create_glue_jobs` `--per_worker_mem_budget_mb`; `job2_load` | int ≥ 1. Per-table driver-memory budget the auto-throttle uses. The throttle **never silently drops to 1** — it holds a floor even if driver memory is unknown/misreported. Baked into the load job args at creation |
| `load_worker_type` | optional | `G.4X` | Full load | `create_glue_jobs` (job `WorkerType`) | Glue worker type for the normal (small-group) load. **Bigger = bigger driver** (the load runs driver-side), which is what speeds it. Allow-list G.1X/G.2X/G.4X/G.8X/G.12X/G.16X/R.1X/R.2X/R.4X/R.8X. **Baked into the job definition at creation** — only jobs created after the change use the new size |
| `load_num_workers` | optional | `10` | Full load | `create_glue_jobs` (job `NumberOfWorkers`) | int 1–299. Load worker count (executors mostly help the CSV read). Baked into the job definition at creation |
| `load_timeout_minutes` | optional | `2880` | Full load | `create_glue_jobs` (job `Timeout`) | int 1–10080. Load job timeout (48 h default). Baked into the job definition at creation |
| `load_big_worker_type` | optional | `G.8X` | Full load | `create_glue_jobs` (big-load job `WorkerType`) | Glue worker type for the **big-table** load (128 GB driver by default; raise to G.12X/G.16X for very large tables). Same allow-list. Baked into the job definition at creation |
| `load_big_num_workers` | optional | `10` | Full load | `create_glue_jobs` (big-load job `NumberOfWorkers`) | int 1–299. Big-load worker count. Baked into the job definition at creation |
| `load_big_timeout_minutes` | optional | `2880` | Full load | `create_glue_jobs` (big-load job `Timeout`) | int 1–10080. Big-load timeout (raise toward 10080 = 7 days for very large tables). Baked into the job definition at creation |

**Validation** (read by `job3_validate`; sizing/`--arg` values baked at job creation)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `validate_rows_per_range` | optional | `10000` | Validation | `create_glue_jobs` `--validate_rows_per_range`; `job3_validate` | int ≥ 1. **Starting value / upper cap** for job3's adaptive range sizer (shared validate job **and** every composite ck-validate fork). The sizer grows or shrinks each range from the measured query time (see `validate_target_seconds_per_range`), so you do **not** tune this for throughput or StackOverflow — the source side never builds one Spark plan over all ranges. Lower it only to cap the largest range on an unusually wide row. Baked into the validate job args at creation |
| `validate_parallelism` | optional | `0` | Validation | `create_glue_jobs` `--validate_parallelism`; `job3_validate` | int 0–10000. Concurrent per-range DSQL validation queries — **the main throughput lever**. `0` = auto (sized from the validate worker type/count), then **hard-capped** by `conn_budget` and DSQL's 10,000-connection cluster limit, so validation can never exhaust the cluster. Raise for more rows/sec on big tables if the connection budget allows. Baked into the validate job args at creation |
| `validate_target_seconds_per_range` | optional | `12` | Validation | `create_glue_jobs` `--validate_target_seconds_per_range`; `job3_validate` | int 1–120. The adaptive sizer aims each range query at this many seconds (5–20 s band), well under DSQL's 300 s transaction-age limit. Smaller = more, shorter queries; larger = fewer, longer ones. Baked into the validate job args at creation |
| `validate_hash` | optional | `all` | Validation | `create_glue_jobs` `--validate_hash`; `job3_validate` | `all` \| `keys` \| `off`. Scope of the per-value md5 content check (md5 is computed **once** per value). `all` hashes every text/char/uuid/bytea column; `keys` only key columns; `off` uses count + length + min/max only. Use `keys`/`off` to speed validation of very wide or large-object (LOB) tables. Baked into the validate job args at creation |
| `validate_worker_type` | optional | `G.8X` | Validation | `create_glue_jobs` (validate job `WorkerType`) | Glue worker type for validate (big-table COUNT/scan needs a big driver). Same allow-list. **Baked into the job definition at creation** — only jobs created after the change use the new size |
| `validate_num_workers` | optional | `10` | Validation | `create_glue_jobs` (validate job `NumberOfWorkers`) | int 1–299. Validate worker count. Baked into the job definition at creation |
| `validate_timeout_minutes` | optional | `2880` | Validation | `create_glue_jobs` (validate job `Timeout`) | int 1–10080. Validate timeout. Baked into the job definition at creation |

**Discovery** (read by `job1_discovery`; sizing baked at job creation)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `discovery_worker_type` | optional | `G.2X` | Discovery | `create_glue_jobs` (discovery job `WorkerType`) | Glue worker type for discovery. One of G.1X/G.2X/G.4X/G.8X/G.12X/G.16X/R.1X/R.2X/R.4X/R.8X (G.12X+/R.* are newer, higher startup latency — confirm Region/version). **Baked into the job definition at creation** — only jobs created after the change use the new size |
| `discovery_num_workers` | optional | `5` | Discovery | `create_glue_jobs` (discovery job `NumberOfWorkers`) | int 1–299. Discovery worker count. Baked into the job definition at creation |
| `discovery_timeout_minutes` | optional | `480` | Discovery | `create_glue_jobs` (discovery job `Timeout`) | int 1–10080 (Glue 7-day max). Discovery job timeout. Baked into the job definition at creation |
| `glue_version` | optional | `4.0` | Discovery, Full load, Validation, CDC | `create_glue_jobs` (every Spark job's `GlueVersion`) | `4.0` (tested default) or `5.0` (re-test the pg8000/boto3 driver wheels on Python 3.11 first). Applied to the Spark jobs. **Baked into the job definition at creation** — only jobs created after the change use the new version |

**CDC** (read by `glue_cdc_continuous` / `glue_cdc_composite`; every `--cdc_*` arg is baked into the CDC job at creation and read **once** at CDC-run start — a running CDC run never re-reads them)

| Parameter | Required? | Default | Phase(s) | Used by | Meaning |
|---|---|---|---|---|---|
| `cdc_engine` | optional | `pythonshell` | CDC | `create_glue_jobs` (selects the pythonshell vs spark CDC job); `resolve_task` (spark-sticky after a fallback) | `pythonshell` (1 DPU) or `spark` (Glue 4.0, 2 × G.1X). Decides which CDC job definition is created; applies to CDC jobs created after republish |
| `cdc_spark_fallback` | optional | `true` | CDC | `startup` state machine (PrepareDrivers) + `create_glue_jobs` (re-create as Spark) | `true`: on a Python-shell CDC driver failure the startup re-creates that task's CDC job as Spark; `false`: stop at `DriversFailed` / `CdcRunFailed`. Applies to tasks started after republish |
| `cdc_validation` | optional | `true` | CDC, Cutover | `create_glue_jobs` `--cdc_validation`; `glue_cdc_continuous`/`glue_cdc_composite` (writes failures); `drain_check` (cutover gate reads them) | Tier-2 CDC validation: each CDC job re-reads a sample of every committed file's rows by key and records persistent mismatches in `cdc_control.cdc_validation_failures`. Cutover **stops** at `CdcValidationFailed` if any unresolved failure exists. Set `false` to disable. Baked into the CDC job args at creation; a running CDC run keeps its startup value |
| `cdc_validation_sample` | optional | `20` | CDC | `create_glue_jobs` `--cdc_validation_sample`; `glue_cdc_continuous`/`glue_cdc_composite` | rows re-checked per committed CDC file (`0` = check every change — expensive). Baked into the CDC job args at creation; a running CDC run keeps its startup value |
| `cdc_max_delete_fraction` | optional | `0.5` | CDC | `create_glue_jobs` `--cdc_max_delete_fraction`; `glue_cdc_continuous`/`glue_cdc_composite` (**G6**) | **G6 mass-delete guard**: a single CDC file (or one poll cycle) whose net DELETEs would remove more than this fraction of a table's current rows **and** more than `cdc_max_delete_rows` is **blocked** and nothing from that file is applied. Set `>= 1` to disable the guard. Baked into the CDC job args at creation; a running CDC run keeps its startup value |
| `cdc_max_delete_rows` | optional | `100000` | CDC | `create_glue_jobs` `--cdc_max_delete_rows`; `glue_cdc_continuous`/`glue_cdc_composite` (**G6**) | **G6**: the absolute delete floor; both thresholds must be crossed, so a tiny table is never blocked by normal churn. Baked into the CDC job args at creation; a running CDC run keeps its startup value |
| `cdc_drift_check_minutes` | optional | `30` | CDC | `create_glue_jobs` `--cdc_drift_check_minutes`; `glue_cdc_continuous`/`glue_cdc_composite` (**G9**, periodic in-job timer) | **G9 drift detector**: minutes between live-DSQL-count vs expected (`full_load_rows + inserts − deletes`) checks per table. `0` = off. Baked into the CDC job args at creation; the running CDC run checks on this interval using the value it started with |
| `cdc_drift_tolerance` | optional | `0` | CDC | `create_glue_jobs` `--cdc_drift_tolerance` (also validate `--count_mismatch_tolerance`); `glue_cdc_continuous`/`glue_cdc_composite` (**G9**) | **G9**: allowed row difference before drift fires (`0` = exact, for PK tables; raise slightly for no-PK tables). Baked into the CDC/validate job args at creation; a running CDC run keeps its startup value |
| `cdc_drift_action` | optional | `warn` | CDC | `create_glue_jobs` `--cdc_drift_action`; `glue_cdc_continuous`/`glue_cdc_composite` (**G9**) | **G9**: `warn` (log ERROR + CloudWatch `DsqlRowDrift` + `cdc_control.audit_log`) or `block` (also set the table `blocked`). Baked into the CDC job args at creation; a running CDC run keeps its startup value |
| `guardrails_mode` | optional | `warn` | Load, Validate, CDC | `create_glue_jobs` `--guardrails_mode`; `job2_load` / `job3_validate` / `glue_cdc_continuous` / `glue_cdc_composite` (all guards) | **Master switch.** `warn` (default): a guardrail may STOP a destructive action (**G1**/**G4**/**G6**) but NEVER fails a run for its own bookkeeping — a missing permission (e.g. `states:DescribeExecution`), a missing control table, a lock it can't take, or a check it can't compute degrades to a WARNING and the run continues. `strict`: restores fail-closed behaviour (**G2**/**G3**/**G7**/**G8**/**G10** block). The per-guard keys below still override |
| `cdc_file_order_action` | optional | `warn` | CDC | `create_glue_jobs` `--cdc_file_order_action`; `glue_cdc_continuous`/`glue_cdc_composite` (**G8**) | **G8** order/gap/new-`LOAD`-after-CDC: `warn` (log + `DsqlGuardWarn` metric, keep applying in order) or `block` (set the table `blocked`). `guardrails_mode=strict` implies `block` |
| `cdc_nopk_overmatch_action` | optional | `warn` | CDC | `create_glue_jobs` `--cdc_nopk_overmatch_action`; `glue_cdc_continuous`/`glue_cdc_composite` (**G7**) | **G7** no-PK content-DELETE over-match: `warn` (apply and warn + metric) or `block` (block the table). `guardrails_mode=strict` implies `block` |
| `validate_count_check` | optional | `warn` | Validate | `create_glue_jobs` `--validate_count_check`; `job3_validate` (**G10**) | **G10** validate vs DMS `FullLoadRows`: `warn` (a mismatch logs a WARNING but validation still PASSES — DMS counts can legitimately differ) or `strict` (mismatch FAILS validation). `guardrails_mode=strict` implies `strict` |
| `cutover_count_check` | optional | `warn` | Cutover | `cutover` state machine `DsqlCountCheck` (reads it via resolve_task); the cutover count equation (**G10**) | **G10** cutover count equation (`FullLoadRows + I − D`): `warn` (a mismatch logs a WARNING but cutover PROCEEDS) or `strict` (cutover refuses with `CountMismatch`). `guardrails_mode=strict` implies `strict` |

### Which settings affect a running task?

Everything above is captured by the component **when it starts**, not continuously:

- A **running CDC job** reads **all** its `--cdc_*` args **once** at run start (`getResolvedOptions`)
  and keeps them for the life of the run. Its 30-second poll loop re-lists S3 for new CDC files but
  **does not** re-read `params.csv` / `pipeline.json` or its own job args. The **G9 drift check**
  (`cdc_drift_check_minutes` / `cdc_drift_tolerance` / `cdc_drift_action`) and the **G6 mass-delete
  guard** (`cdc_max_delete_fraction` / `cdc_max_delete_rows`), as well as `cdc_validation` /
  `cdc_validation_sample`, therefore use the value the run started with. To change any of them for a
  table whose CDC is already running you must **stop that CDC run and start a new one** (and, if the
  value is baked into the job definition, re-create the CDC job).
- **Worker sizes, worker counts, timeouts, `glue_version`, `glue_connection`, `glue_role_arn`** are
  part of the **Glue job definition**, written by `create_glue_jobs` at job-creation time. Changing
  them in `params.csv` and republishing affects **only jobs created afterwards**; an already-created
  discovery/load/validate/CDC job keeps its old definition until it is deleted and re-created (a new
  task startup, or a manual re-create).
- **`plan_split` / planning keys** (`max_groups`, `map_max_concurrency`, the `big_table_*` and
  `*_writers_per_loader` / `conn_budget` / `writers_per_file` / `max_files_in_parallel` knobs) are
  consumed when a task's **PlanSplit** runs at startup; a task already past planning is unaffected.
- **Setup-only keys** (`account_id`, `subnet_id`, `security_group_id`, `manage_iam`,
  `lambda_role_arn`, `sfn_role_arn`) only act when you **re-run `tools/setup.sh`**.

In short: republishing `params.csv` → `pipeline.json` changes **future** task runs and **future** job
creations. It never reaches into a job or CDC run that is already in flight.

Fifty-three keys end up in `config/pipeline.json`: `project`, `region`, `dsql_endpoint`, `dsql_user`,
`dsql_database`, `glue_role_arn`, `glue_connection`, `cdc_engine`, `cdc_spark_fallback`,
`control_schema`, `cdc_validation`, `cdc_validation_sample`, `cdc_max_delete_fraction`,
`cdc_max_delete_rows`, `cdc_drift_check_minutes`, `cdc_drift_tolerance`, `cdc_drift_action`,
`guardrails_mode`, `cdc_file_order_action`, `cdc_nopk_overmatch_action`, `validate_count_check`,
`cutover_count_check`,
`max_composite_forks`,
`max_big_cdc_forks`, `big_table_row_threshold`, `file_fanout_threshold`, `big_table_bytes_threshold`,
`max_groups`, `map_max_concurrency`, `max_files_in_parallel`, `writers_per_file`, `conn_budget`,
`min_writers_per_loader`, `max_writers_per_loader`, `validate_rows_per_range`,
`validate_parallelism`, `validate_target_seconds_per_range`, `validate_hash`, `glue_version`,
`discovery_worker_type`,
`discovery_num_workers`, `discovery_timeout_minutes`, `load_worker_type`, `load_num_workers`,
`load_timeout_minutes`, `load_big_worker_type`, `load_big_num_workers`, `load_big_timeout_minutes`,
`validate_worker_type`, `validate_num_workers`, `validate_timeout_minutes`, `max_parallel_tables`,
`per_worker_mem_budget_mb`. `account_id`, `subnet_id`, `security_group_id`, `lambda_role_arn`,
`sfn_role_arn` and `manage_iam` are used only by setup.

---

## 4. Set up

**Two ways to do setup — pick one:**

- **Option A — `tools/setup.sh` (recommended).** One command creates/updates everything from
  `params.csv`. Needs a bash shell (CloudShell is ideal). The rest of this section is Option A.
- **Option B — manual, step by step ([`docs/MANUAL_SETUP.md`](docs/MANUAL_SETUP.md)).** The exact
  same resources, created one at a time with the CLI (or the console). Choose B when you **can't run
  `setup.sh`**: no bash/CloudShell allowed; a change-controlled environment where each resource must
  be created (and reviewed) individually; or a CloudFormation/Terraform shop that wants the exact
  commands and names to port. Option B is complete and standalone — it repeats the get-the-code,
  bucket and params steps so you can follow it on its own.

Both produce the identical result (same role/Lambda/state-machine names, same S3 paths).

**Run all of this from the root of your local clone** ([§1](#1-get-the-code)). `tools/setup.sh`
checks it is in the repo root (it looks for `lambdas/params_csv.py`), reads the code files from the
clone — `iam/*.json`, `lambdas/*.py`, `scripts/*.py`, `glue-templates/*.json`,
`stepfunctions/*.asl.json` — and uploads/creates everything in AWS from them. One command does it
all (create-or-update, so re-running only fixes drift):

```bash
# recommended: dry-run first to see every AWS command without running any
tools/setup.sh "s3://<bucket>/config/params.csv" --dry-run              # reads a LOCAL params.csv (see note)
tools/setup.sh "s3://<bucket>/config/params.csv" --with-drivers         # the real run
```

- The **bucket comes from the `s3://` path** of `params.csv` — `params.csv` never names the bucket.
  To read `params.csv` from a local file instead, pass the bucket explicitly:
  ```bash
  tools/setup.sh params.csv --bucket "<bucket>" --with-drivers
  ```
- `--with-drivers` also stages the Glue driver wheels (needed once; see below). Omit it to skip them.
- `--dry-run` prints every AWS command without running any. A dry-run against a **local** CSV is
  fully offline; `--dry-run` with an `s3://…/params.csv` path can't read the CSV offline — download
  it first and pass the local path with `--bucket`.

### What setup takes from where

Every row below is `local file in your clone` → `what it becomes in AWS`:

| Local (in the clone) | Becomes |
|---|---|
| `iam/glue.json`, `iam/lambda.json`, `iam/stepfunctions.json` | **3 IAM roles**: `<project>-glue-exec-role`, `<project>-lambda-exec-role`, `<project>-sfn-exec-role` (one per service; all 8 Lambdas use the lambda role, all 4 state machines the sfn role; a `glue-vpc` inline policy is added only with a VPC connection) |
| `lambdas/*.py` (+ pg8000, zipped) | **8 Lambda functions** `<project>-{resolve-task,driver-discovery,plan-split,create-glue-jobs,stop-cdc-run,drain-check,drop-tags,preflight-tasks}` |
| `scripts/*.py` (4 files) | `s3://<bucket>/scripts/` |
| `glue-templates/*.json` (6 files, `<<BUCKET>>` filled) | `s3://<bucket>/glue-templates/` |
| driver wheels (downloaded; `--with-drivers`) | `s3://<bucket>/driver-fullload/`, `s3://<bucket>/driver-validation/`, `s3://<bucket>/driver-cdc/` |
| `config/params.csv` (via `lambdas/params_csv.py`) | `s3://<bucket>/config/pipeline.json` (the settings every run reads) |
| `stepfunctions/*.asl.json` (`<<…>>` filled) | **4 state machines** `<project>-{startup,cutover,fleet-startup,fleet-cutover}` |
| (VPC only) `subnet_id` + `security_group_id` | a **Glue network connection** named by `glue_connection` |

`config/pipeline.json` is published under the [safe-publish rule](#the-safe-publish-rule-how-pipelinejson-is-published-from-paramscsv)
below (a dated backup is kept first).

### Confirm it worked

`setup.sh` prints a summary; then spot-check (set your real values):

```bash
PROJECT="<project>"; REGION="<region>"; BUCKET="<bucket>"; export AWS_PAGER=""
aws iam list-roles --query "Roles[?starts_with(RoleName,'$PROJECT-')].RoleName" --output table                     # 3
aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table   # 8
aws stepfunctions list-state-machines --query "stateMachines[?starts_with(name,'$PROJECT-')].name" --output table   # 4
aws s3 ls "s3://$BUCKET/scripts/"            # 5 Glue scripts
aws s3 ls "s3://$BUCKET/glue-templates/"     # 8 templates
aws s3 cp "s3://$BUCKET/config/pipeline.json" -   # the published settings
```

### Driver wheels (what `--with-drivers` stages)

The Glue jobs can't reach PyPI from a locked-down VPC, so their Python libraries are staged in S3 as
`.whl` files in three folders. `--with-drivers` downloads and uploads them for you; you only need to
run it again if the wheel set changes.

| Folder | Used by | Wheels |
|---|---|---|
| `driver-fullload/` | discovery + load (Spark, Python 3.10) | the **pg8000 stack** (5): `pg8000`, `scramp`, `asn1crypto`, `python_dateutil`, `six` — **no boto3/botocore** |
| `driver-validation/` | validate (Spark, Python 3.10) | the **pg8000 stack** (5) — **no boto3/botocore** |
| `driver-cdc/` | CDC (Python **shell**, Python 3.9) | the pg8000 stack **and** the boto3 set (`boto3`, `botocore`, `jmespath`, `s3transfer`, `urllib3`), built for Python 3.9 (10 wheels; one version per package) |

A boto3/botocore wheel in `driver-fullload/` or `driver-validation/` breaks the Spark jobs with
`DataNotFoundError: endpoints`; they get boto3 from `driver-cdc/`. You never prepare the CDC wheels
by hand — before DMS starts, the startup checks `driver-cdc/` for Python 3.9 and writes install-ready
copies to `driver-cdc-prepared/<fingerprint>/` (with a `MANIFEST.txt` for your security team). Your
`driver-cdc/` files are never changed.

### The safe-publish rule (how `pipeline.json` is published from `params.csv`)

`setup.sh` and the fleet's preflight apply the **same** conservative rule — settings are never
changed out from under a run:

- The candidate `pipeline.json` is built and validated from `params.csv` first. Any problem (missing
  required key, `account_id` not 12 digits, both-or-neither VPC pair, an unknown/duplicate key, a
  value still holding `<`/`>`, or `project` not matching the fleet) fails before anything is written.
- If the candidate **equals** the live `config/pipeline.json`, nothing is written.
- If it **differs** and nothing is running (startup only), the live file is backed up to a dated key
  `config/pipeline.json.<UTC>`, the new one is published, then read back and verified.
- If **anything is running** — any `startup`/`cutover`/`fleet-startup`/`fleet-cutover` execution — or
  it's a cutover, it stops and writes nothing. Publish new settings with a startup fleet (or re-run
  setup) first, then cut over.
- If executions **can't be listed**, it fails closed (writes nothing).

> Prefer not to use `params.csv`? [`docs/MANUAL_SETUP.md`](docs/MANUAL_SETUP.md) shows how to write
> `pipeline.json` by hand. Don't copy `config/pipeline.example.json` as-is: its `description` line
> contains `<bucket>`, and any value with `<`/`>` is rejected, so a copied-as-is file fails every run
> at `ResolveFailed`.

### Using roles your IAM team already created (`manage_iam=false`)

By default setup **creates** three roles (`<project>-{glue,lambda,sfn}-exec-role`). If your IAM team
owns role creation — or you simply aren't allowed to create roles — set `manage_iam=false` and point
setup at the roles that already exist:

```
manage_iam,false
glue_role_arn,arn:aws:iam::ACCOUNT_ID:role/your-glue-role
lambda_role_arn,arn:aws:iam::ACCOUNT_ID:role/your-lambda-role
sfn_role_arn,arn:aws:iam::ACCOUNT_ID:role/your-sfn-role
```

With `manage_iam=false` setup makes **no IAM write call at all** — no `create-role`,
`update-assume-role-policy`, `put-role-policy`, `attach-role-policy` or any `delete-*`. The only IAM
calls are reads: `get-role`, `list-role-policies`, `list-attached-role-policies`, and a best-effort
`simulate-principal-policy`. The Lambdas are created with `--role <lambda_role_arn>` and the state
machines with `--role-arn <sfn_role_arn>`; everything else (Glue connection, scripts, templates,
`pipeline.json`, state machines) is done exactly as with `manage_iam=true`.

**What your IAM team must attach.** Each role needs the trust below and the matching permissions:

| Role (`*_role_arn`) | Trust must allow (`sts:AssumeRole`) | Attach |
|---|---|---|
| `glue_role_arn` | `glue.amazonaws.com` | the Glue inline policy (S3 bucket, DSQL connect, logs, DMS describe); **plus** the VPC networking statements when a Glue connection is configured |
| `lambda_role_arn` | `lambda.amazonaws.com` | the Lambda inline policy (S3, DMS/DSQL describe, Glue job management, `states:*` for the pipeline machines) whose `iam:PassRole` **names the Glue role**; **plus** the managed policy `AWSLambdaVPCAccessExecutionRole` when a Glue VPC connection is used |
| `sfn_role_arn` | `states.amazonaws.com` | the Step Functions inline policy (invoke the pipeline Lambdas, run/stop Glue jobs, DMS start/stop, start/verify the child machines) |

**Where setup writes the policy files.** Setup fills the three policies with your **real** role names
and ARNs and writes them next to your clone:

```
iam-out/<glue-role-name>.policy.json      # inline policy JSON to attach to the Glue role
iam-out/<lambda-role-name>.policy.json     # inline policy JSON for the Lambda role (PassRole -> Glue role)
iam-out/<sfn-role-name>.policy.json        # inline policy JSON for the Step Functions role
iam-out/README-IAM.txt                     # per role: the trust it needs and exactly what to attach
```

Hand `iam-out/` to your IAM team. The path printed at the end of Step 1 tells you where it is.

**Fail-closed checks (nothing is created until these pass).**

- If any of the three roles is **missing**, setup stops before changing anything and names it.
- If a role's **trust** doesn't allow its service principal, setup stops and prints the exact trust
  statement to add (e.g. `{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}`).
- The best-effort `simulate-principal-policy` check warns if a role is missing a permission its policy
  needs (or if the simulate call itself is denied). By default setup then **stops**; pass
  `--skip-permission-check` to continue anyway (e.g. when your account can't call `iam:Simulate*`).

A role given with a **path** works: `arn:aws:iam::<acct>:role/team/sub/your-role` is read and
created-against as role name `your-role` (the last segment), while the Lambdas/state machines are
pointed at the full ARN.

> `tools/setup.sh … --dry-run` works in both modes and prints the role each Lambda (`--role`) and each
> state machine (`--role-arn`) will use, without making any AWS call.

---

## 5. Run tasks with the fleet

*Per wave of tasks. Setup (§4) doesn't change. The fleet is the only way to start a task — there is
no "start one by hand" path. One task is one row in `fleet_tasks.csv`; many tasks are many rows.*

### Write `fleet_tasks.csv`

One row per DMS task. Header `task_arn,task_suffix,adopt_existing_folder`
([example](config/fleet_tasks.example.csv)):

- **`task_arn`** (required) — the DMS task ARN.
- **`task_suffix`** (optional) — leave blank to use the folder the pipeline would pick anyway: the
  one recorded for this task (a renamed DMS task keeps its first folder), else the DMS task name.
  Set it only when you want a different folder and job-name stem.
- **`adopt_existing_folder`** (optional, startup only) — `true` for a task whose folder holds files
  from an earlier run but has no owner record.

```csv
task_arn,task_suffix,adopt_existing_folder
arn:aws:dms:us-east-1:123456789012:task:ABCDEF1234567890,,
arn:aws:dms:us-east-1:123456789012:task:GHIJKL0987654321,orders-cdc,
arn:aws:dms:us-east-1:123456789012:task:MNOPQR1122334455,,true
```

The table list for each task is built **automatically** from the DMS task after its full load (from
`describe_table_statistics` and the task's table mappings), so there is nothing to upload. To load
fewer tables, change the DMS task's selection rules. A table with no rows at full load is included
and loaded empty, with a warning; the CDC job picks it up when DMS creates the folder.

### Start the fleet

```bash
PROJECT="<project>"; REGION="<region>"; ACCOUNT_ID="<account id>"; BUCKET="<bucket>"
export AWS_PAGER=""
aws s3 cp fleet_tasks.csv "s3://$BUCKET/config/fleet_tasks.csv"
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
aws stepfunctions start-execution --state-machine-arn "$SM:$PROJECT-fleet-startup" \
  --input "{\"bucket\":\"$BUCKET\"}"
```

The fleet is started with `{"bucket":"<bucket>"}` and always reads from the
fixed `config/` folder in that bucket: the settings `config/pipeline.json`, and the task list
`config/fleet_tasks.csv`. It runs **preflight** (checks every task), then starts one
`$PROJECT-startup` execution per task, **5 at a time**, and confirms each got past its own input
checks. A new wave is just a new `config/fleet_tasks.csv` and another trigger.

### Read the fleet result

| Ends at | Meaning | What to do |
|---|---|---|
| `FleetStarted` | every task was started (or skipped as already started) and each started one was still running 30 s later — i.e. past its own input checks. **It does not mean the migrations succeeded.** | watch each `$PROJECT-startup` child ([§6](#6-watch-progress)) |
| `FleetStartIncomplete` | at least one task did **not** start. `results.tasks` in the output lists each with `status` (`started`/`skipped`/`not_started`) and the error for `not_started`. The others started | fix the listed tasks, trigger the fleet again — already-started tasks are skipped |
| `PreflightFailed` | a problem was found **before anything started** (settings, task list, or specific rows). **Nothing started** | fix every listed problem, trigger the fleet again |
| `MissingFleetInput` | the input lacked `bucket` as a string | start again with `{"bucket":"<bucket>"}` |

If a `config/params.csv` is present, a **startup** fleet (and only when nothing is running) safely
(re)publishes `config/pipeline.json` from it before starting tasks — the same
[safe-publish rule](#the-safe-publish-rule-how-pipelinejson-is-published-from-paramscsv) as setup. A
cutover fleet never publishes.

### Trigger the fleet again after a partial failure

Trigger `fleet-startup` with the same input. Preflight **skips** tasks already under way, so only the
rest start:

- `already_running` — the task's `$PROJECT-startup` already has a RUNNING execution.
- `past_full_load` — the pipeline started this task before and DMS has finished its full load.

Even without the skip (e.g. the preflight role can't list executions — it prints a warning), the
per-task `startup` refuses a second run of a task that is already running.

### Rules for the task list

- **Each source table belongs to exactly one task.** All tasks share the one S3 layout, so two tasks
  with the same table would collide in the same folder.
- **Each row needs its own folder.** Two rows that resolve to the same folder (same DMS name, or the
  same `task_suffix`) are rejected by preflight — give one a different `task_suffix`.
- **Reusing a deleted task's name:** the pipeline refuses a folder another task ARN created (its old
  status files would make CDC skip tables this task never loaded). Preflight fails that row with the
  `aws s3 mv` command to archive the old folder to `config/_archive/`.
- **Per-table fork CDC jobs.** After discovery, each table is assigned one CDC owner (recorded in
  `config/_task/<task>/_jobs.json` → `cdcOwners`), and startup creates/updates the matching jobs:
  - a **composite (multi-column) PK** table gets its own **`ck` fork** — a dedicated load, validate
    and CDC job `$PROJECT-$TASK_NAME-ck-<slug>-{load,validate,cdc}` (CDC via
    `scripts/glue_cdc_composite.py`), scoped to that one table;
  - a **big** single-/no-PK table (FullLoadRows ≥ the big-table threshold — default **6,000,000
    rows or 8+ part-files**, both `params.csv` settings; see
    [Tuning big tables and fan-out](#tuning-big-tables-and-fan-out)) gets
    its own **`bg` CDC job** `$PROJECT-$TASK_NAME-bg-<slug>-cdc` (main CDC script) while keeping the
    shared `load-big` + `validate` jobs;
  - the **main CDC job** applies the remaining small single-/no-PK tables.
  No table is applied by two jobs. Startup starts and confirms every fork CDC job next to the main
  one (each with its own start marker), recreates any missing fork job, and reports stale ones;
  cutover drains every table, stops every CDC run (main + all forks) and deletes all the task's jobs.
  Jobs are found by their exact tags + the registry, never by name prefix. Caps (params.csv):
  `max_composite_forks` (default 8 — a task with more composite tables fails early, naming them) and
  `max_big_cdc_forks` (default 8 — big tables past the cap stay on the main CDC job with a warning).
  A task with no composite and no big tables gets no fork jobs.
- **Schema limit:** a startup task may load into at most **9** distinct DSQL schemas (DSQL allows 10
  per database; `cdc_control` uses one). This is enforced per task while its table list is built
  (before any Glue job); preflight also estimates it from each task's selection rules and fails early
  if the explicit schema names already exceed 9. Schemas already in the database from other tasks
  also count toward the 10, but the pipeline can't see them — it prints a warning with the count it
  does see.

---

## 6. Watch progress

`FleetStarted` only means each per-task run got past its input checks. Watch each `$PROJECT-startup`
execution in the Step Functions console (the run name starts with the task name). In order, each
child: checks the task before starting DMS → checks/prepares the CDC drivers → starts DMS and waits
for the full load (`STOPPED_AFTER_CACHED_EVENTS`, up to 24 h) → builds the table list → creates the
task's Glue jobs → discovers, loads and validates each group → resumes DMS into CDC and starts the
CDC job → confirms CDC reached its poll loop. Success means the full load is in DSQL and validated,
and CDC is applying changes. (The exact steps and all fail states are in [Reference](#10-reference).)

**Check that CDC is applying (per task):**

```bash
PROJECT="<project>"; REGION="<region>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"               # the folder/job stem for this task
JOB="$PROJECT-$TASK_NAME-cdc"
RUN=$(aws glue get-job-runs --job-name "$JOB" --max-items 1 --query 'JobRuns[0].Id' --output text)
aws glue get-job-run --job-name "$JOB" --run-id "$RUN" --query 'JobRun.JobRunState'
if [ "$(aws glue get-job --job-name "$JOB" --query Job.Command.Name --output text)" = pythonshell ]; then
  LG="/aws-glue/python-jobs/output"; else LG="/aws-glue/jobs/output"; fi
aws logs tail "$LG" --log-stream-names "$RUN" --since 1h | grep -E "Full-load gate|poll loop|CANNOT" | tail -5
```

Healthy: `RUNNING`, then `Full-load gate: N/N table(s) marked 'done'` and `entering poll loop`. Per
table: `SELECT table_name, status, error FROM cdc_control.cdc_status;` — to run that query (and the
others cutover and recovery need) see [Connect to DSQL and check progress](#connect-to-dsql-and-check-progress)
below.

If a child run stops at any Fail state, go to [§8](#8-if-something-fails) — what to do depends on
**where** it stopped.

### Connect to DSQL and check progress

The progress queries, the cutover finish-by-hand step ([§7](#7-cut-over-with-the-fleet)) and the
recovery rows ([§8](#8-if-something-fails)) all need a SQL session on DSQL — run these from
CloudShell in the pipeline's region.

**Connect from CloudShell.** DSQL takes an IAM auth token as the password (no stored password) and
`psql` must use TLS. Install `psql` if missing, then connect with your `params.csv` values
(`dsql_endpoint`; `dsql_user` default `admin`; `dsql_database` default `postgres`):

```bash
REGION="<region>"; DSQL_ENDPOINT="<dsql_endpoint>"
DSQL_USER="admin"; DSQL_DATABASE="postgres"          # dsql_user / dsql_database from params.csv
command -v psql >/dev/null || sudo dnf install -y postgresql15   # CloudShell: install the client once
# admin user (the pipeline's default) -> admin token:
export PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token \
  --region "$REGION" --expires-in 3600 --hostname "$DSQL_ENDPOINT")"
# NON-admin dsql_user instead -> drop 'admin' from the command:
#   export PGPASSWORD="$(aws dsql generate-db-connect-auth-token \
#     --region "$REGION" --expires-in 3600 --hostname "$DSQL_ENDPOINT")"
psql "host=$DSQL_ENDPOINT user=$DSQL_USER dbname=$DSQL_DATABASE sslmode=require"
```

> CloudShell must reach the DSQL **public** endpoint. If DSQL is only reachable through a VPC
> endpoint (no internet route), run `psql` from a host inside that VPC (e.g. a CloudShell VPC
> environment or an EC2 instance in the subnet).

**Progress queries** (control-table schema `cdc_control`, the `control_schema` default; `table_name`
is the lowercased `<schema>.<table>`):

```sql
-- per-table status: active (applying) / idle (caught up) / blocked (needs attention),
-- the last fully-applied CDC file, and the error if blocked
SELECT table_name, status, last_done_file, error FROM cdc_control.cdc_status ORDER BY table_name;

-- files still to apply vs. already applied, for one table (status 'done' = applied)
SELECT status, count(*) FROM cdc_control.cdc_file_status
WHERE table_name = '<schema>.<table>' GROUP BY status;

-- unresolved CDC validation failures per table (cutover is blocked while any exist):
SELECT table_name, count(*) AS unresolved FROM cdc_control.cdc_validation_failures
WHERE resolved IS NOT TRUE GROUP BY table_name ORDER BY unresolved DESC;

-- detail for one table (what mismatched, which file, which key):
SELECT failure_time, cdc_file, pk_value, failure_type, details
FROM cdc_control.cdc_validation_failures
WHERE resolved IS NOT TRUE AND table_name = '<schema>.<table>' ORDER BY failure_time;
```

A table is **caught up** when its `cdc_status.status` is `idle` (the CDC job marks it `idle` once no
pending files remain). To **resume a blocked table** after fixing the cause, set it back to
`active` — never `DELETE` the row (applied files stay in the folder and would all replay):

```sql
UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema>.<table>';
```

**The SQL the cutover finish-by-hand step needs** ([§7](#7-cut-over-with-the-fleet) /
[§8](#8-if-something-fails)) — drop the internal `_cdc_file` tracking column from each of the task's
target tables (quoted identifiers, as the cutover's `drop-tags` step runs it):

```sql
ALTER TABLE "<schema>"."<table>" DROP COLUMN IF EXISTS "_cdc_file";
```

**Glue job logs** (CloudWatch): the CDC Python-shell job logs to `/aws-glue/python-jobs/output` and
`/aws-glue/python-jobs/error`; the Spark load/validate (and Spark CDC) jobs log to
`/aws-glue/jobs/output`. The run id is the Glue job-run id (see the `aws logs tail` command above).

---

## 7. Cut over with the fleet

*Per wave, when you're ready to switch the application to Aurora DSQL. Like startup, cutover runs
through the fleet only. **Cutover is irreversible per task.**†*

### Before you start — in order, for every task in the CSV

Cutover stops DMS first, so any source change after that is never migrated — getting this order
wrong loses data silently.

- [ ] **Stop writes to the source** for every table of every task in the CSV (maintenance mode, or
      make the source read-only).
- [ ] **Let DMS deliver the last changes** for each task: its **CDCLatencySource** and
      **CDCLatencyTarget** are near zero, and you've waited past any `CdcMaxBatchInterval` so the last
      change file has landed in S3.
- [ ] **No table is blocked** for any task:
      `SELECT table_name FROM cdc_control.cdc_status WHERE status='blocked';` returns nothing (run it
      per [Connect to DSQL and check progress](#connect-to-dsql-and-check-progress)). If not,
      fix the cause and unblock ([§8](#8-if-something-fails)) first.
- [ ] **Each task's CDC run is RUNNING** ([§6](#6-watch-progress)). Cutover doesn't verify this; if a
      run isn't running, that task's cutover stops DMS and waits the full drain budget (~12 h) before
      failing.
- [ ] **Composite-key (multi-column-PK) tables:** their ongoing changes are tracked by the separate
      per-table composite (`ck`) and big (`bg`) fork CDC jobs, created and started automatically when
      the task has any (see [Rules for the task list](#rules-for-the-task-list)). Confirm each fork
      job's run is RUNNING too before cutover. Only cut
      over once you've accounted for that — their full load is in DSQL, but no changes since full
      load were applied.

The fleet checks **inputs, not readiness** — the cutover preflight only confirms each task exists, is
listed once, and was started by the pipeline (has an owner record). Cutting over a task whose CDC has
not caught up is on you.

### Start it

Use the same `config/fleet_tasks.csv`, or replace it with only the rows you want to cut over now
(`adopt_existing_folder` is ignored at cutover):

```bash
PROJECT="<project>"; REGION="<region>"; ACCOUNT_ID="<account id>"; BUCKET="<bucket>"
export AWS_PAGER=""
aws s3 cp fleet_tasks.csv "s3://$BUCKET/config/fleet_tasks.csv"
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
aws stepfunctions start-execution --state-machine-arn "$SM:$PROJECT-fleet-cutover" \
  --input "{\"bucket\":\"$BUCKET\"}"
```

The result states are the same as a startup fleet ([§5](#5-run-tasks-with-the-fleet)):
`FleetStarted` means each cutover **started**, not that it finished — watch each child. Each
`$PROJECT-cutover` child, for its one task: stops the DMS task (up to ~1 h), waits until each table's
last CDC file is applied (`DrainCheck`; up to ~12 h), stops this task's CDC run, drops the internal
`_cdc_file` column, and deletes this task's Glue jobs (the five `$PROJECT-$TASK_NAME-{discovery,load,load-big,validate,cdc}`, plus each per-table fork job `ck-<slug>-{load,validate,cdc}` / `bg-<slug>-cdc`, found by this task's tags). It finds the task by its ARN, so a
renamed task still cuts over its original folder and jobs.

> **† A cutover fleet re-triggers cutover for EVERY task in the CSV**, including ones already cut
> over. The cutover preflight has no "already cut over" skip — it skips only a task whose
> `$PROJECT-cutover` is *currently running* (`already_running`). Re-running `fleet-cutover` with a
> task whose cutover failed partway is now **safe**: cutover describes the DMS task first and skips
> the stop if it is already stopped, and every later step is idempotent (drain re-checks, stop-CDC
> is a no-op when nothing runs, `_cdc_file` is dropped with `IF EXISTS`, Glue deletes treat an
> already-gone job as deleted). A task whose cutover already **succeeded** has nothing left to do;
> re-running it simply drains (0 files), finds nothing to stop/drop/delete, and succeeds again —
> but it is tidiest to remove fully-cut-over tasks from the CSV.

Each child ends at one of:

| Ends at | Meaning | What to do |
|---|---|---|
| `CutoverSucceeded` | done | point the application at Aurora DSQL |
| `CutoverSucceededWithOverride` | done, but this cutover was started with `override=true` — it bypassed one or more validation gates (`CdcValidationFailed` pre/final, the cutover count check) and/or accepted a startup that itself used override. The safety ordering (DMS stop → drain → stop CDC runs → delete jobs) was **not** bypassed. An override audit record was written to `config/_task/<suffix>/_overrides/<execution>.json` | point the application at Aurora DSQL, and review the override record — the data was accepted past a validation result on your explicit instruction |
| `GlueJobsNotDeleted` | data is cut over; only deleting a Glue job failed. Cutover retries deletion in a loop — a job whose CDC run is still stopping is force-stopped (`batch-stop-job-run`) and retried on the next pass (named under `pending`), so a composite (`ck-*`) CDC run that is slow to stop no longer times the delete Lambda out. You only land here if a job could not be deleted for another reason, or the pending jobs did not stop within the ~60-min delete budget (the error names them; the jobs are `$PROJECT-$TASK_NAME-{discovery,load,load-big,validate,cdc}`, plus each per-table fork job `ck-<slug>-*` / `bg-<slug>-cdc`, found by this task's tags) | just re-run cutover for this task — it is idempotent and resumes the delete loop (a job already gone counts as deleted). Only if it keeps failing, stop any lingering run and delete by hand: `aws glue batch-stop-job-run --job-name <name> --job-run-ids <id>` then `aws glue delete-job --job-name <name>` |
| `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing changed for this task | fix the error shown, re-run cutover for this task via the fleet |
| `CdcValidationFailed` | unresolved CDC-validation discrepancies blocked cutover (pre-check: nothing touched; final check: DMS stopped, CDC run/`_cdc_file`/Glue jobs all untouched) | review `cdc_control.cdc_validation_failures`, clear each reviewed row with `UPDATE … SET resolved=true` (never `DELETE`), then re-run cutover ([§8](#8-if-something-fails)). **Or**, if you have reviewed the discrepancy and accept it, start a new cutover with `override=true` ([§8 "Validation failed — re-run with override"](#validation-failed--re-run-with-override)) |
| `StartupOverrideRequiresOverride` | this task's **startup** was finished with `override=true` (its data was accepted past a validation failure; see `config/_task/<suffix>/_overrides/`), so cutting it over without acknowledging that would move unvalidated data. Nothing was touched — DMS and CDC are still running | review the startup override record, and if you accept the data, start a new cutover with `override=true` ([§8 "Validation failed — re-run with override"](#validation-failed--re-run-with-override)) |
| `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`) | DMS is stopped; a table's last file wasn't applied within ~12 h | fix the cause ([§8](#8-if-something-fails)), then **re-run cutover for this task** — it skips the already-stopped DMS and re-drains |
| `CutoverFailed` at a step **after DMS was stopped** | DMS is stopped | open the failed state, fix the cause, then **re-run cutover for this task** — it is now re-runnable (skips the already-stopped DMS, every later step is idempotent) |

**After a successful cutover, these remain for each task** (nothing deletes them): the stopped DMS
task and its endpoints; `config/_task/<task name>/` and the `config/_task_index/` record; every CDC
file and its `processed/` copy; and the `cdc_control` rows. The same DMS task can't be migrated again
([§9](#9-reload-a-task-from-scratch)). Delete what you no longer need once you're confident.

---

## 8. If something fails

Start at the fleet level, then open the child executions that failed. **Where a child stopped decides
what to do:** stopped **before** DMS is resumed into CDC → fix and **re-trigger the fleet** (the task
isn't past full load, so it isn't skipped). Stopped **after** DMS is in CDC → the fleet would skip it
(`past_full_load`), so **recover that one task by hand**.

| State / error (fleet or child) | Cause | What to do |
|---|---|---|
| **Fleet** `MissingFleetInput` | the input lacked `bucket` as a string | trigger again with `{"bucket":"<bucket>"}` |
| **Fleet** `PreflightFailed` | a problem before anything started: a `config/pipeline.json`/`config/params.csv` problem, a missing/empty or bad/duplicate task row, a folder owned by another task, too many DSQL schemas in a task, or (with `config/params.csv`) a settings change blocked because a run is in progress / a cutover / executions can't be listed | fix each problem the cause lists; nothing started, so trigger the fleet again |
| **Fleet** `FleetStartIncomplete` | some tasks didn't start (`results.tasks` names them) | fix those tasks, trigger the fleet again — started/running/past-full-load tasks are skipped |
| **Fleet** `FleetFailed` | the fan-out itself failed (rare) | re-trigger; if it recurs, check the sfn role (§4) can start and describe the per-task executions |
| **Startup** `ResolveFailed` (`SettingsError`) | a `pipeline.json` problem: missing file/key, a value with `<`/`>`, a non-ARN `glue_role_arn`, or a region ≠ the task ARN's region | fix `params.csv`/`pipeline.json`, re-trigger the fleet |
| **Startup** `ResolveFailed` (`TaskCheckError`) | a DMS task setting: not `full-load-and-cdc`, `StopTaskCachedChangesApplied` not true, `AddColumnName` false, wrong target bucket, or the task is past its full load | fix the DMS task/endpoint, re-trigger the fleet |
| **Startup** `ResolveFailed` (`FolderOwnerError`) | `config/_task/<name>/` was made by a different task ARN, holds files from an older run with no owner, or a `task_suffix` differs from the recorded one | archive the folder (the error gives the `aws s3 mv`), use the recorded suffix, or set `adopt_existing_folder=true` for this task's own pre-shared-workflow files |
| **Startup** `DriversFailed` (`DriverCheckError`) | a `driver-cdc/` wheel can't run on Python 3.9 (scramp 1.4.7+, boto3/botocore 1.43+, urllib3 2.x), two versions of one package, a missing package, or a Spark driver folder without pg8000 | the error names the wheel; fix the folder (§4 driver wheels) and re-trigger the fleet |
| **Startup** `DmsFailed` (`DmsTaskFailed`) | DMS failed or a table errored during full load | fix in the DMS console (**Table statistics** + CloudWatch; reload the errored table). A task can only be (re)started while it hasn't finished its full load — else see [§9](#9-reload-a-task-from-scratch) |
| **Startup** `DmsTimedOut` (`DmsPollBudgetExceeded`) | DMS didn't reach `STOPPED_AFTER_CACHED_EVENTS` within 24 h — usually a task already past its full load, or stopped partway, or a genuinely long load | check the DMS task; reload with a new DMS task if needed ([§9](#9-reload-a-task-from-scratch)) |
| **Startup** `DmsStartFailed` | `startReplicationTask` genuinely failed and the task is **not** running/starting and **not** already at a completed full load (bad endpoint/table-mapping, task in an unstartable state, etc.). This now fails **fast** with the real DMS error instead of being masked by a ~24 h poll that then reported a misleading timeout | read the DMS error in the execution (`$.startError`) and the describe result, fix the DMS task/endpoint, re-trigger the fleet (the task isn't past full load, so it isn't skipped) |
| **Startup** `BuildTableListFailed` | building the table list from the DMS task failed: a table didn't load cleanly, a table-mapping transformation the pipeline can't reproduce for S3 folder names, or more than 9 distinct DSQL schemas | the error names the tables/schemas; fix the source or the DMS task's rules, re-trigger the fleet. No Glue jobs were created |
| **Startup** BuildTableList logs `(warn) … is NOT in the task's current selection rules — IGNORING it` | a table DMS still reports in `describe_table_statistics` (its stats/S3 folder linger from a prior run) is **not** matched by the task's **current** selection rules | nothing to fix — the table is **ignored** (never loaded, validated or CDC-applied, and not in the manifest/discovery index). The table list is built from DMS stats **cross-checked against the live selection rules**, not from leftover S3 folders. The warning gives the exact `aws s3 rm s3://…/<schema>/<table>/ --recursive` to delete the stale folder if you want the storage back |
| **Startup** `GroupsFailed` at load, child log shows `♻ RESUME … auto-reblanking whole table` on a **composite-PK** table | a composite table was previously loaded (target non-empty) and is being reloaded on resume | nothing to fix — the reblank now pages by the **full composite key tuple**, so every DELETE transaction stays under DSQL's ~3000-row cap and the composite table reloads cleanly (earlier this failed with `54000: transaction row limit exceeded` because it paged by a single non-unique key column). A no-PK table's reblank additionally **auto-shrinks** its batch on a row/size-limit error |
| **Startup** `GroupsFailed`, or `PipelineFailed` at `CreateGlueJobs`/`RunDiscovery`/`PlanSplit`/`GroupFanOut` | DMS full load is in S3; DMS is paused at `STOPPED_AFTER_CACHED_EVENTS` | fix the cause (the failed group's Glue log has it), re-trigger the fleet — finished files/tables are skipped; the task isn't past full load, so it isn't skipped |
| **Startup** validation (`GroupValidate`) fails or logs a re-split | the validate Glue job compares every column S3-vs-DSQL per key range | **Empty source tables now PASS** on every path — a 0-row source with a 0-row target is `0 == 0` (holds for composite and fork-validate tables too, whether or not discovery flagged the table empty); only an empty source whose DSQL target has rows is a real mismatch. **Large/wide tables (B18 — `StackOverflowError` in validate, now handled):** validation no longer builds one Spark plan over hundreds of ranges — the source side uses a broadcast range-join (O(1) plan depth) and bounded per-plan chunks, so a 1B-row table at the default range size does not overflow the JVM stack. The per-value md5 content hash is computed **once per value** (a derived table), not ~6× per value, so a range query no longer burns the 300 s limit on md5. Ranges are sized by **time** (adaptive, aiming `validate_target_seconds_per_range`) and run in parallel (`validate_parallelism`, auto-sized, capped by `conn_budget` + DSQL's 10,000-connection limit); a range that still hits DSQL's 300 s limit or a client read timeout is **auto re-split** and retried (log: `validation re-split … after a transaction-age/timeout error`). **Do NOT raise `validate_rows_per_range` to avoid a StackOverflow or to speed validation** — it is only a starting value / cap; raise `validate_parallelism` for throughput, or set `validate_hash=keys\|off` for very wide/LOB tables. The validate log prints per-table throughput (ranges, parallelism, rows/s). Data is unaffected (full load already matched) |
| **Startup** `PipelineFailed` at `ResumeDmsToCdc` | load done and validated; DMS probably still paused | **don't re-trigger the fleet for this task.** If DMS is still stopped, resume it: `aws dms start-replication-task --replication-task-arn "$TASK_ARN" --start-replication-task-type resume-processing`, then **start the CDC job by hand** (below) |
| **Startup** `CdcRunFailed`/`CdcRunEnded`/`CdcFallbackFailed`, or `PipelineFailed` at `StartCdcJob`/`GetCdcRun`/`CheckCdcStarted` | load done; **DMS is in CDC**, capturing changes to S3 | **don't re-trigger the fleet** (it skips this task). Check whether a CDC run is already RUNNING ([§6](#6-watch-progress)); if not, fix the cause in the CDC log and **start the CDC job by hand** (below). Nothing is lost while it's down — DMS keeps writing change files |
| **Startup** `CdcStartNotConfirmed` | the CDC run is running but didn't write its start marker in 45 min | check the CDC log ([§6](#6-watch-progress)). If it shows `entering poll loop`, CDC is fine and the marker couldn't be written — check the Glue role can write `config/_task/<task>/_cdc_started/` |
| **Startup** `PlanSplitFailed` | planning the forks failed: more composite tables than `max_composite_forks` (the cause lists them), or the master index was unreadable | reduce composite tables in the task's selection rules / split the task / raise `max_composite_forks` in params.csv, then re-trigger the fleet. No Glue jobs were created. (Big tables past `max_big_cdc_forks` do NOT fail — they stay on the main CDC job with a warning) |
| **Startup** `EnsureForkJobsFailed` | creating a fork's load/validate/cdc jobs after discovery failed (template/engine/role problem), OR a Glue job with that name already exists with different/absent tags (a hand-made or other-task job) and was refused, OR listing/tagging Glue jobs failed | the Lambda error names it; remove/rename the conflicting job or fix the IAM/template, then re-trigger the fleet — nothing is past full load |
| **Startup** `ForkCdcStartNotConfirmed` | a fork CDC job (`ck-*` composite or `bg-*` big-table) didn't confirm its start marker in 45 min (the main CDC job is running). **Before the B23 fix this was a FALSE alarm for EVERY fork**: the workflow polled the task-level `config/_task/<task>/_cdc_started/<exec>-ck-<slug>.json`, but a fork run writes its marker under its OWN prefix (`.../_orchestrator/ck-<slug>/_cdc_started/` or `.../_orchestrator/bg-<slug>/_cdc_started/`), so it never matched and the fork run was actually RUNNING fine. After the fix the workflow polls the fork's own prefix (and the CDC scripts also drop a second copy at the task level), so this error now means a REAL failure | **First confirm it is the false alarm, not a real failure.** `aws glue get-job-runs --job-name <project>-<task>-ck-<slug>-cdc --max-results 1` (or `-bg-<slug>-cdc`) → if `JobRunState` is `RUNNING`, the fork is fine. Confirm its marker exists: `aws s3 ls s3://<bucket>/<task cp>/_orchestrator/ck-<slug>/_cdc_started/`. Confirm it is applying: on DSQL `SELECT table_name,status,last_applied_at FROM cdc_control.cdc_status WHERE table_name='<schema.table>';`. **If the fork run is RUNNING, the task is fine — full load is done; do NOT re-trigger the fleet.** Cutover finds the fork CDC jobs by tag regardless. If the fork run is NOT running, check its log for `entering poll loop` and that the Glue role can write the fork prefix, then start it by hand with `--config_prefix=<fork prefix>`. Full command list: `OPERATOR_CHECK.md` |
| **Startup** execution shows `CdcDriverFallback` then succeeds | the Python-shell drivers failed; the job is now Spark | nothing to fix. The reason is in `config/_task/<task name>/_cdc_engine.json`; fix `driver-cdc/` and delete that file to go back to Python shell |
| **Cutover** `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing changed | fix the error, re-run cutover for this task via the fleet (keep only this task in the CSV, or remove already-cut-over tasks first) |
| **Cutover** `CdcDrainTimedOut`, or `CutoverFailed`/`GlueJobsNotDeleted` **after DMS was stopped** | DMS is stopped (or fully cut over bar one job delete) | fix the cause, then **re-run cutover for this task**. Cutover is now re-runnable: it describes the DMS task first and skips the stop when it is already stopped (`InvalidResourceStateFault` is also tolerated), re-drains, and every later step is idempotent (stop-CDC no-op when nothing runs, `_cdc_file` drop `IF EXISTS`, Glue delete treats an already-gone job as deleted, a fork job already absent is fine). Job deletion runs in a **loop**: a CDC run still stopping (e.g. a slow composite `ck-*` run) is force-stopped and retried on the next pass rather than timing the delete Lambda out — so `GlueJobsNotDeleted` now means only that the loop's ~60-min budget ran out or a non-pending delete error occurred (the pending jobs are named in the error) |
| **Cutover** `CdcValidationFailed` (from `CdcValidationFailedPre`, **before** DMS is stopped) | nothing touched — DMS still running, CDC still running | investigate the unresolved rows (query below), confirm each is explained/benign, then clear them and re-run cutover: `UPDATE cdc_control.cdc_validation_failures SET resolved = true WHERE table_name = '<schema>.<table>';` (never `DELETE` — keep the audit) |
| **Cutover** `CdcValidationFailed` (from `CdcValidationFailedFinal`, **after** the drain) | DMS is **stopped**; the CDC run, the `_cdc_file` column and the Glue jobs are **untouched** | same `UPDATE … SET resolved = true` after review, then re-run cutover (it re-stops DMS idempotently, re-drains, re-checks). The query: `SELECT table_name, count(*) FROM cdc_control.cdc_validation_failures WHERE resolved IS NOT TRUE GROUP BY table_name;` |
| **CDC** a table is `blocked` in `cdc_control.cdc_status` | a `DROP COLUMN` on the source, or a row DSQL rejected (e.g. NULL into NOT NULL) | fix the cause, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';` ([how to connect](#connect-to-dsql-and-check-progress)) — CDC resumes. **Never delete the row** (applied files stay and would all be replayed) |
| **CDC** `MASS-DELETE GUARD` — a table is `blocked`, nothing applied from the file (**G6**) | one CDC file's net DELETEs would remove more than `cdc_max_delete_fraction` **and** more than `cdc_max_delete_rows` of the table — a suspected bad/corrupt file vs a real mass delete | verify against the SOURCE. If the deletes are REAL, allow this one file: `UPDATE cdc_control.cdc_status SET allow_mass_delete=true, status='active' WHERE table_name='<schema.table>';` then CDC applies it and the flag self-clears for the next file. If BOGUS, re-export the file from DMS. See §"Safety guardrails" |
| **CDC** `NO-PK DELETE GUARD` — a table is `blocked` (**G7**, only when `cdc_nopk_overmatch_action=block` / `guardrails_mode=strict`) | a no-PK content-match DELETE would remove more rows than the file's D ops intend (duplicate rows). In the default `warn` mode this is a WARNING + `DsqlGuardWarn` metric and the file still applies | investigate the duplicate rows / source; if blocked, once resolved `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';` |
| **CDC** file-order / high-water / gap / new-LOAD (**G8**; WARNING by default, `blocked` only when `cdc_file_order_action=block` / `guardrails_mode=strict`) | a file older than the high-water, a gap, or a new `LOAD*` file after CDC started (a DMS reload under CDC) | check for a hand-edited/reset `cdc_status`, a leftover file from an older run, or a DMS "reload table". Do a controlled reload via a NEW DMS task ([§9](#9-reload-a-task-from-scratch)); if blocked, then `UPDATE cdc_control.cdc_status SET status='active' …` |
| **CDC** `ROW DRIFT` ERROR / a table `blocked` when `cdc_drift_action=block` (**G9**) | the live DSQL count diverged from `full_load_rows + inserts_applied − deletes_applied` beyond `cdc_drift_tolerance` | the drift is logged + written to `cdc_control.audit_log` + emitted as the `DsqlRowDrift` metric. Investigate the table against the source; if explained, raise `cdc_drift_tolerance` or set the table `active`; if real loss, reload via a new DMS task ([§9](#9-reload-a-task-from-scratch)) |
| **Load** `BLANK GUARD G1/G2/G3/G4` — a whole-table or range blank was refused | **G1** CDC already started for the table; **G2** a manual (no-workflow) run tried to blank without `--allow_manual_destructive=true`; **G3** another run holds the table lock; **G4** the table was not previously attempted by this task, or its count is above `expected×(1+margin)` | read the audit row in `cdc_control.audit_log` (the `action` ends `_refused_*`). For a legitimate MANUAL reblank re-run the load with `--allow_manual_destructive=true` (G1/G4 still apply); for a CDC-started table reload via a NEW DMS task ([§9](#9-reload-a-task-from-scratch)); for a lock, let the other run finish or clear a stale row in `cdc_control.cdc_control_lock` |
| **Validate** `mismatch` with a `DMS_COUNT_DIFF` (**G10**) | the DSQL count disagrees with DMS `describe_table_statistics` FullLoadRows beyond `count_mismatch_tolerance` (not only the S3 source count) | the target is short/over vs the authoritative DMS figure — reload the table ([§9](#9-reload-a-task-from-scratch)) or investigate the DMS task before cutover |
| **Cutover** `CountMismatch` (**G10**) | per table, the DSQL count ≠ DMS `FullLoadRows + Inserts − Deletes` beyond tolerance | investigate the short/over table; reload if needed, or pass the explicit cutover count-override only when the difference is understood and benign |
| **CDC** run ends with no error after ~7 days | the 7-day Glue timeout (the 10080-minute maximum) | start the CDC job by hand (below); it resumes from where it left off. Cut over before 7 days where you can |
| **CDC** Spark job: `DataNotFoundError: endpoints` | a boto3/botocore wheel is in `driver-fullload/` or `driver-validation/` | remove it; those folders hold the 5 pg8000 wheels only |
| **CDC/Glue** `Unknown service: 'dsql'` | `driver-cdc/` lacks a current boto3 set | re-stage drivers (§4), re-trigger the fleet |
| **Glue** `Can't create a connection to host ...dsql... port 5432` or `Name or service not known` | the job isn't in your VPC, or Glue has no route to DSQL | ensure `glue_connection` is set and Glue has a route to DSQL (a DSQL VPC endpoint); the pipeline tries the reachable DSQL hostname automatically. Re-trigger. Check: `aws glue get-job --job-name <job> --query Job.Connections` |
| **Setup** an `aws` command seems to hang | the CLI pager is waiting | `export AWS_PAGER=""` and re-run; whatever you Ctrl-C'd was still created |
| **Setup** `create-function`: *role cannot be assumed by Lambda* | the role is seconds old | wait 10 s and re-run `tools/setup.sh` |

**Start the CDC job by hand** (fully supported — e.g. restart after the 7-day Glue timeout, or
re-run after unblocking a table). Pass `--config_prefix` so cutover can find and stop the run, and
`--startup_execution` so the run writes its start marker under a stable name (any string you
choose; use `manual-<something>` so it is recognisable):

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"
CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-cdc" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\",\"--startup_execution\":\"manual-$(date +%Y%m%d-%H%M%S)\"}" \
  --query JobRunId --output text
```

Then check it as in [§6](#6-watch-progress). A manual CDC run is safe to start: CDC never empties a
table (so the blank guards do not apply to it), and it is still covered by **G3** (the per-table
lock + position fence stop two CDC runs clashing on one table), **G6** (mass-delete), **G8**
(ordering) and **G9** (drift). Only run ONE CDC job per task at a time; a second one for the same
tables will skip each table with a warning (lock/fence) rather than double-apply.

**Re-run a LOAD for one table by hand** — allowed. A manual load may WRITE onto an empty table or
per-file-resume (which deletes nothing). It will REFUSE a destructive whole-table blank unless you
pass `--allow_manual_destructive=true` (and even then **G1** no-blank-after-CDC and **G4** count
sanity still apply, and a `manual override` row is written to `cdc_control.audit_log`):

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"
CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
# Plain manual load (writes onto an empty table / resumes files; never blanks):
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-load" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}" --query JobRunId --output text
# Manual load that is ALLOWED to reblank (destructive) — use only when you intend to empty+reload:
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-load" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\",\"--allow_manual_destructive\":\"true\"}" \
  --query JobRunId --output text
```

**Finish a cutover by hand** — now rarely needed: cutover is **re-runnable**, so after a failure
once DMS is stopped the simplest recovery is to **re-run cutover for this task** (it skips the
already-stopped DMS and every later step is idempotent). Use the manual steps below only if you
cannot re-run it (set `TASK_NAME` to the folder/job stem). The SQL steps (checking status, dropping
`_cdc_file`) use a DSQL session — see
[Connect to DSQL and check progress](#connect-to-dsql-and-check-progress):

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"
# 1. Confirm every table of this task is caught up (none 'blocked'):
#      SELECT table_name, status FROM cdc_control.cdc_status;
# 2. Stop this task's CDC run (if still running)
RUN=$(aws glue get-job-runs --job-name "$PROJECT-$TASK_NAME-cdc" \
  --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text)
[ -n "$RUN" ] && aws glue batch-stop-job-run --job-name "$PROJECT-$TASK_NAME-cdc" --job-run-ids "$RUN"
# 3. In DSQL, for each of this task's tables (quote the names):
#      ALTER TABLE "<schema>"."<table>" DROP COLUMN IF EXISTS "_cdc_file";
# 4. Delete this task's Glue jobs: the five shared jobs, plus every per-table fork job
#    (ck-<slug>-{load,validate,cdc} and bg-<slug>-cdc). Find the forks by this task's tags
#    (never by name prefix — another task's name may be a prefix of this one):
for r in discovery load load-big validate cdc; do
  aws glue delete-job --job-name "$PROJECT-$TASK_NAME-$r"
done
# fork jobs (tagged dsql_pipeline_project=$PROJECT, dsql_pipeline_task=$TASK_NAME):
for j in $(aws glue list-jobs --query 'JobNames[]' --output text | tr '\t' '\n'); do
  tags=$(aws glue get-tags --resource-arn "arn:aws:glue:$REGION:$ACCOUNT:job/$j" --query 'Tags' --output json 2>/dev/null)
  echo "$tags" | grep -q "\"dsql_pipeline_task\": \"$TASK_NAME\"" \
    && echo "$tags" | grep -q "\"dsql_pipeline_project\": \"$PROJECT\"" \
    && echo "$tags" | grep -q "dsql_pipeline_fork" \
    && aws glue delete-job --job-name "$j"
done
```

### Time one validation range query on your own table (EXPLAIN ANALYZE)

If validation feels slow, time a single range query the way job3 runs it — a per-column
aggregate over one key range, with md5 computed **once per value** in a derived table. Connect
to DSQL (psql with an IAM auth token) and run, substituting your schema/table, key column and a
range that holds ~10k–50k rows:

```sql
-- One range: count + a cheap per-value hash sum (md5 computed ONCE per value in the subquery).
EXPLAIN (ANALYZE, TIMING)
SELECT count(*),
       COALESCE(SUM( (strpos('0123456789abcdef', substr(h_c,1,1))-1)::bigint*1048576
                   + (strpos('0123456789abcdef', substr(h_c,2,1))-1)::bigint*65536
                   + (strpos('0123456789abcdef', substr(h_c,3,1))-1)::bigint*4096
                   + (strpos('0123456789abcdef', substr(h_c,4,1))-1)::bigint*256
                   + (strpos('0123456789abcdef', substr(h_c,5,1))-1)::bigint*16
                   + (strpos('0123456789abcdef', substr(h_c,6,1))-1)::bigint ), 0)
FROM (SELECT md5(some_text_col::text) AS h_c
      FROM your_schema.your_table
      WHERE id >= 1000000 AND id < 1050000) s;   -- one 50k-row key range
```

The `Execution Time` line is the per-range cost. Compare it to the **old** form (md5 recomputed
per hex digit) to see the win:

```sql
-- OLD (SLOW): md5(some_text_col::text) is evaluated 6x per row (one per substr position).
EXPLAIN (ANALYZE, TIMING)
SELECT count(*),
       COALESCE(SUM( (strpos('0123456789abcdef', substr(md5(some_text_col::text),1,1))-1)::bigint*1048576
                   + (strpos('0123456789abcdef', substr(md5(some_text_col::text),2,1))-1)::bigint*65536
                   -- ...4 more md5(...) calls...
                   ), 0)
FROM your_schema.your_table
WHERE id >= 1000000 AND id < 1050000;
```

Aim for each range query to land in the 5–20 s band (`validate_target_seconds_per_range`); the
job sizes ranges automatically to hit it. If a single range is still slow, raise
`validate_parallelism` (throughput) or set `validate_hash=keys` / `off` for very wide / large-
object tables — do **not** raise `validate_rows_per_range`.

---

### Validation failed — re-run with override

When validation fails for a reason you have **reviewed and accept** — an IP shortage, a timeout,
or a count/by-key mismatch you understand — you do not have to clear control rows and re-run the
whole thing. Start a **new** execution with `"override": true` and the run carries on instead of
stopping.

Override covers **validation only**. A **load** failure (a table status `failed`) still stops the
run even with override; override never resumes DMS on a load failure, never skips a safety step,
and never changes the default (absent/`false`) run — a run without `override` behaves exactly as
before. Finished files and tables are still skipped on the re-run (a table already marked `done`
is **not** reloaded), so override only changes the validation gate decision.

Each override run writes an audit record to
`config/_task/<task>/_overrides/<execution>.json` (who = execution ARN, when, which groups and
tables, and the validation report paths). A **startup** override also writes
a stable marker `config/_task/<task>/_overrides/_startup_override.json`, which makes a later
cutover of that task **refuse** (`StartupOverrideRequiresOverride`) unless cutover is **also**
started with `override=true` — so unvalidated data can never be cut over silently.

**Risk note.** `override=true` means an operator has chosen to accept data that failed a
validation check. Only use it after you have looked at the discrepancy (the validation report
and/or `cdc_control.cdc_validation_failures`) and understand why it is safe. The override is
recorded with your execution ARN for the audit trail.

New end states: a startup that overrode a validation failure ends in
**`TaskSucceededWithOverride`** (its output lists the overridden groups and tables and the
validation report paths); a cutover started with override ends in
**`CutoverSucceededWithOverride`**.

**Start a task-level startup with override** (set `TASK_ARN`):

```bash
PROJECT="<project>"; export AWS_PAGER=""
SM="arn:aws:states:<region>:<account>:stateMachine:$PROJECT-startup"
TASK_ARN="arn:aws:dms:<region>:<account>:task:<id>"
aws stepfunctions start-execution --state-machine-arn "$SM" \
  --input "{\"taskArn\":\"$TASK_ARN\",\"override\":true}" \
  --query executionArn --output text
```

**Start a task-level cutover with override** (same input shape, `$PROJECT-cutover`):

```bash
PROJECT="<project>"; export AWS_PAGER=""
SM="arn:aws:states:<region>:<account>:stateMachine:$PROJECT-cutover"
TASK_ARN="arn:aws:dms:<region>:<account>:task:<id>"
aws stepfunctions start-execution --state-machine-arn "$SM" \
  --input "{\"taskArn\":\"$TASK_ARN\",\"override\":true}" \
  --query executionArn --output text
```

**Fleet-wide override** — add a top-level `"override": true` to the fleet start input to turn it
on for **every** task in the CSV, or set the per-task `override` column to `true` for just some
rows (blank = false). Fleet-level and per-task are OR'd:

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
# startup fleet, override every task:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<region>:<account>:stateMachine:$PROJECT-fleet-startup" \
  --input "{\"bucket\":\"$BUCKET\",\"override\":true}" \
  --query executionArn --output text
# cutover fleet, override every task:
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<region>:<account>:stateMachine:$PROJECT-fleet-cutover" \
  --input "{\"bucket\":\"$BUCKET\",\"override\":true}" \
  --query executionArn --output text
```

Per-task CSV override (blank = false; an unknown column is never rejected by preflight):

```csv
task_arn,task_suffix,adopt_existing_folder,override
arn:aws:dms:us-east-1:123456789012:task:ABCDEF1234567890,,,
arn:aws:dms:us-east-1:123456789012:task:STUVWX3344556677,,,true
```

---

## 9. Reload a task from scratch

To load a task again, everything from the earlier attempt must go **together** — otherwise leftover
status makes the pipeline skip or replay work. There is no in-place reload; a reload needs a DMS task
that hasn't finished its full load.

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"            # recorded folder/job stem (task_suffix, or the DMS name)
TASK_ARN="<the DMS task ARN>"
CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
```

1. Stop this task's CDC run and the DMS task:
   ```bash
   RUN=$(aws glue get-job-runs --job-name "$PROJECT-$TASK_NAME-cdc" \
     --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text)
   [ -n "$RUN" ] && aws glue batch-stop-job-run --job-name "$PROJECT-$TASK_NAME-cdc" --job-run-ids "$RUN"
   aws dms stop-replication-task --replication-task-arn "$TASK_ARN"
   ```
2. Delete each table's DMS folder, **including `processed/`**. If your DMS S3 target endpoint has a
   `BucketFolder`, prefix it; an empty value means none:
   `aws s3 rm "s3://$BUCKET/<schema>/<table>/" --recursive`
   (with a BucketFolder: `aws s3 rm "s3://$BUCKET/<bucketFolder>/<schema>/<table>/" --recursive`)
3. Delete the task's run state: `aws s3 rm "${CONFIG_PREFIX}_orchestrator/" --recursive`
4. In DSQL, drop and re-create the target tables, and delete their rows from **all six** control
   tables: `cdc_status`, `cdc_file_status`, `cdc_chunk_log`, `cdc_apply_exceptions`,
   `cdc_validation_failures`, `cdc_skipped_ops`. Only delete `cdc_control` rows **together with**
   step 2 — applied change files stay in the folder and would otherwise all be replayed.
5. **Reload with a DMS task that hasn't run yet, through the fleet.** The pipeline refuses a task
   past its full load, so create a new DMS task with the same settings and table mapping (a new name
   gives it a new folder), add its ARN as a row in `fleet_tasks.csv`, and trigger `fleet-startup`
   ([§5](#5-run-tasks-with-the-fleet)); the table list is rebuilt from the new task automatically. If
   you reuse the old name, archive the old folder first:
   `aws s3 mv "${CONFIG_PREFIX}" "s3://$BUCKET/config/_archive/$TASK_NAME-$(date +%Y%m%d%H%M)/" --recursive`

## Safety guardrails

Ten guardrails stop a "what if" from silently losing or corrupting target rows, and surface any
loss within one CDC poll cycle. **A guardrail may only ever STOP a destructive action** (deleting
or emptying target rows). It must **never fail a load, validate, CDC or cutover run because of its
own bookkeeping** — a missing permission (e.g. `states:DescribeExecution`), a missing control
table, a lock it can't take, or a check it can't compute. Those degrade to a WARNING and the run
keeps going (other tables + all non-destructive work keep flowing). Full scenario matrix:
`WHAT_IF.md`.

### `guardrails_mode` (master switch) — `warn` (default) | `strict`

- **`warn` (default)** — the three HARD guards (G1, G4, G6) still refuse a genuinely destructive
  action, but they only ever stop THAT action (one table's blank / one file's apply); the rest of
  the run continues. Every other guard (G2, G3, G5, G7, G8, G9, G10) is **advisory**: it logs a
  WARNING (+ a `DsqlGuardWarn` / `DsqlRowDrift` CloudWatch metric + a `cdc_control.audit_log` row)
  and proceeds. A guard that hits an exception (permission denied, DSQL error, missing table, S3
  read error) is wrapped so it can't raise into the main path — it logs the reason and carries on
  as if it passed.
- **`strict`** — restores the original fail-closed behaviour for operators who want it: G2 refuses
  a no-workflow blank, G3 refuses a lock it can't take, G7/G8 block the table, G10 fails
  validation / refuses cutover. Individual per-guard settings still override the mode.

### Each guard: before (fail-closed) → after (this change)

| Guard | What it stops (HARD = always refuses the destructive op) | `warn` default (after) | `strict` | Where / override |
|---|---|---|---|---|
| **G1** (HARD) | A load reblank once CDC has started (would wipe CDC deltas) | Refuses the blank in both modes. If it can't read the marker/`cdc_status` it is fail-closed on the blank only — the rest of the run continues | same | load — `--blank_guard_enabled false` (master off) |
| **G4** (HARD) | Blanking a table this task did not load, or whose count > `expected×(1+margin)` | Refuses the blank in both modes | same | load — `--blank_expected_margin` |
| **G6** (HARD) | A CDC file deleting > `cdc_max_delete_fraction` **and** > `cdc_max_delete_rows` of a table | Blocks that one table, nothing applied, in both modes | same | CDC — `cdc_max_delete_fraction>=1` (off); per-file `cdc_status.allow_mass_delete=true` |
| **G2** (soft) | A destructive blank from a run **no workflow started** | **Allows + loud WARNING + audit** (a missing `states:DescribeExecution` or an unconfirmable execution must not fail a legitimate reblank; G1/G4 still apply). A RUNNING execution or `--allow_manual_destructive=true` always blanks | Refuses unless a RUNNING execution or the manual flag | load — `guardrails_mode`, `--allow_manual_destructive=true` |
| **G3** (soft) | Two runs writing one table at once | **If it can't take the lock for a bookkeeping reason (no connection, lock table can't be created): WARN + proceed.** Skips the table ONLY when ANOTHER LIVE holder clearly owns the lock. CDC side keeps the position-fence backstop | Refuses on any failed acquire | load + CDC — `guardrails_mode`, lock timeout `LOCK_HEARTBEAT_TIMEOUT_SECONDS` |
| **G5** (soft) | An un-attributable destructive op | Best-effort `cdc_control.audit_log` row BEFORE every blank/mass-delete/purge (and refusal); a write failure never fails the action | same (always best-effort) | load + CDC — — |
| **G7** (soft) | A no-PK content DELETE over-matching duplicates (precision check); the `_cdc_file` purge touching other files | **WARN + apply** (G6 is the real volume cap; this is a refinement). The purge-exactness static assertion is unchanged | Blocks the table | CDC — `cdc_nopk_overmatch_action=warn\|block` |
| **G8** (soft) | A file older than the high-water, a gap, or a new `LOAD*` after CDC started (DMS reload) | **WARN + metric, keep applying in order** | Blocks the table | CDC — `cdc_file_order_action=warn\|block` |
| **G9** (soft) | Slow drift vs `full_load_rows + inserts − deletes` | WARN + `audit_log` + `DsqlRowDrift` metric (already the default; wrapped so it can't raise) | `cdc_drift_action=block` sets the table blocked | CDC — `cdc_drift_check_minutes=0` (off), `cdc_drift_tolerance`, `cdc_drift_action=warn\|block` |
| **G10** (soft) | Validate passing while DSQL ≠ DMS `FullLoadRows`; cutover with DSQL ≠ `FullLoadRows + I − D` | **WARN; validation still PASSES / cutover proceeds** (DMS counts can legitimately differ — e.g. the source changed during the load) | `validate_count_check=strict` fails validation; `cutover_count_check=strict` refuses cutover (`CountMismatch`) | validate + cutover — `validate_count_check`, `cutover_count_check`, `count_mismatch_tolerance` |

All of the new control bookkeeping the guards add — `cdc_control.cdc_control_lock`,
`cdc_control.audit_log`, and the `cdc_status` counter columns (`full_load_rows`,
`inserts_applied`, `deletes_applied`, `allow_mass_delete`) — is created **add-if-missing**
(`CREATE TABLE IF NOT EXISTS`; `ADD COLUMN` with **no** `DEFAULT`, matching B17, so it works on a
pre-existing older `cdc_control`). If that create/upgrade fails, the job **warns and continues**;
the guards that use it are best-effort.

### Reading the audit log

Every destructive action (and refusal) writes one row FIRST, so even a crash mid-op is attributable:

```sql
SELECT event_time, table_name, action, rows_before, rows_deleted, reason
FROM cdc_control.audit_log ORDER BY event_time DESC LIMIT 50;
```

`action` names the op (`auto_reblank_on_resume`, `cdc_mass_delete_blocked`, `drift_detected`,
`..._refused_cdc_started`, `..._manual_override`, …). `rows_before` is the count just before the op.

### Unblocking (always UPDATE, never DELETE the control row)

- **Mass delete (G6)** — verify against the source, then for the one next file:
  `UPDATE cdc_control.cdc_status SET allow_mass_delete=true, status='active' WHERE table_name='<schema.table>';`
- **Drift (G9)** — investigate; if explained, raise `cdc_drift_tolerance` (or set the table
  `active` if it was blocked); if real loss, reload via a new DMS task ([§9](#9-reload-a-task-from-scratch)).
- **Lock (G3)** — let the other run finish, or if a run died holding it, clear the stale row:
  `DELETE FROM cdc_control.cdc_control_lock WHERE table_name='<schema.table>';` (the only control
  row it is safe to DELETE — it is a lease, not state).
- **Order / high-water / new-LOAD (G8)** — WARN by default (the stream keeps applying in order); a
  `cdc_control.audit_log` row (`cdc_file_order_warn` / `cdc_new_load_after_start_warn`) + a
  `DsqlGuardWarn` metric flag it for investigation. Set `cdc_file_order_action=block` (or
  `guardrails_mode=strict`) to block instead; then after the cause is fixed,
  `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';`.
  **Never DELETE a `cdc_status` row** (the high-water is lost → every applied file replays).

### Daily count check

Once a day (and before cutover), confirm no table is blocked and no drift is pending:

```sql
-- any blocked table needs attention:
SELECT table_name, status, error FROM cdc_control.cdc_status WHERE status = 'blocked';
-- live vs expected per table (expected = full_load_rows + inserts − deletes):
SELECT table_name, full_load_rows, inserts_applied, deletes_applied,
       (COALESCE(full_load_rows,0)+COALESCE(inserts_applied,0)-COALESCE(deletes_applied,0)) AS expected
FROM cdc_control.cdc_status ORDER BY table_name;
-- recent drift / destructive events:
SELECT event_time, table_name, action, rows_before, rows_deleted
FROM cdc_control.audit_log WHERE action IN ('drift_detected') ORDER BY event_time DESC LIMIT 20;
```

Compare each table's live DSQL `count(*)` to `expected`; a persistent gap is a loss to investigate
against the source. The CDC job also emits the `DsqlRowDrift` CloudWatch metric (namespace
`GlueCDC/NonPK`, dimension `Table`) — alarm on it for always-on detection.

### Supported manual runs

Operators can run jobs by hand; the guards keep them safe. See
[§8 "Start the CDC job by hand" / "Re-run a LOAD for one table by hand"](#8-if-something-fails)
for the exact commands. In short: a manual **CDC** run is always allowed (G3/G6/G8/G9 still cover
it); a manual **load/validate** may always WRITE; only a destructive **blank** in a manual load is
gated and needs `--allow_manual_destructive=true` (with G1/G4 still enforced and a `manual override`
audit row written).

---

## 10. Reference

### The S3 layout

One bucket, fixed folder names (baked into the templates):

```
s3://<bucket>/
├── scripts/                  # the 5 Glue scripts
├── glue-templates/           # the 8 Glue job templates
├── driver-fullload/          # pg8000 stack only — Spark discovery + load
├── driver-validation/        # pg8000 stack only — Spark validate
├── driver-cdc/               # pg8000 stack + boto3 set for Python 3.9 (CDC)
├── driver-cdc-prepared/      # written by the startup: install-ready copies of driver-cdc/
├── <schema>/<table>/         # written by DMS (under the endpoint's BucketFolder, if any)
└── config/
    ├── pipeline.json         # settings read by every run and by the fleet
    ├── params.csv            # the one parameters file (built into pipeline.json)
    ├── fleet_tasks.csv       # the task list the fleet reads
    ├── _task_index/          # task ARN -> folder name
    └── _task/<task name>/    # one folder per DMS task (table list, owner record, group status, CDC markers)
```

### Tuning big tables and fan-out

The fan-out planner (`plan_split`) and the load jobs are driven by `params.csv` settings (full rows,
defaults and valid ranges in [§3](#3-fill-in-paramscsv)): the big-table thresholds
(`big_table_row_threshold`, `file_fanout_threshold`, `big_table_bytes_threshold`), the grouping/
concurrency knobs (`max_groups`, `map_max_concurrency`, `max_files_in_parallel`,
`conn_budget`, `min`/`max_writers_per_loader`), the single-file writer lever (`writers_per_file`),
and the job sizing (`*_worker_type` / `*_num_workers` / `*_timeout_minutes`, `glue_version`,
`max_parallel_tables`, `per_worker_mem_budget_mb`). The planning-threshold defaults equal the values
the pipeline used before they were settings, so leaving them out plans exactly as before. Two rules
apply to the planning thresholds:

- a change takes effect only for tasks **started after** the new `pipeline.json` is published
  (settings are read once per run, at `ResolveTask`);
- a change **never re-assigns** a table whose CDC job already started — the owners recorded in
  `config/_task/<task>/_jobs.json` win, and `plan_split` only logs a warning if a new threshold
  *would* move a table (so a tuning change can't double-apply a table). To actually re-assign, cut
  the task over (which stops and deletes its CDC jobs) and start it fresh.

What to change, and why:

- **Treat more/fewer tables as "big"** (own `load-big` group + own `bg` CDC job): lower/raise
  `big_table_row_threshold` (rows), `file_fanout_threshold` (part-files), or
  `big_table_bytes_threshold` (total full-load bytes). The **bytes** test is what rescues a huge
  **single-file** table (num_files=1) whose DMS row count is missing from the index — without it a
  16 M-row single-file table was being planned as "small" (no `load-big`, no `bg` CDC job). More big
  tables = more always-on `bg` CDC jobs (watch `max_big_cdc_forks` and the Glue concurrent-run quota).
- **Spread small tables across more lanes:** raise `max_groups` (= the CDC job pool size).
- **Go faster at the cost of more concurrency:** raise `map_max_concurrency` (1–40). Each extra
  concurrent loader uses more Glue DPU and opens more DSQL connections at once.
- **Pace DSQL writes:** `conn_budget` is the connection budget the planner shares across in-flight
  loaders; `min_writers_per_loader`/`max_writers_per_loader` bound each loader's write concurrency.
  The planner never exceeds `conn_budget` — if `max_writers_per_loader × map_max_concurrency`
  exceeds it, per-loader writers are squeezed below `max_writers_per_loader` (resolve warns).

### Tuning for speed (worker sizes + single-file parallelism)

The full load runs **driver-side**: `job2_load.py` streams each table's rows to the Glue **driver**
(`toLocalIterator`) and writes to DSQL with `pg8000` from driver threads. Executors only parse the
CSV read. So the levers that actually speed a load are the **driver size** (= the **worker type**,
because Glue sizes the driver like the workers) and the **write concurrency**, not the executor
count.

- **`writers_per_file` (default 8)** — the single most important lever for a BIG table. DMS usually
  writes one `LOAD00000001.csv` per table (a serial full load), and the per-file fan-out opens one
  worker per file — so a multi-GB single-file table used to load with **one** DSQL connection
  committing 3,000-row transactions serially (~1,700–2,000 rows/s). `writers_per_file` splits that
  one file's row stream across N concurrent writers, each its own connection, each committing
  disjoint 3,000-row chunks (no-dup via the PK + disjoint chunks; no-loss via the unchanged
  `rows_read == committed` gate). Set `1` for the old one-writer-per-file behaviour.
- **Worker types** — `load_big_worker_type`/`validate_worker_type` default to **G.8X** (128 GB
  driver) for big tables, `load_worker_type` to **G.4X** (64 GB), `discovery_worker_type` to
  **G.2X**. For very large tables raise to **G.12X / G.16X** (192 / 256 GB) — newer types with
  higher startup latency, so confirm they exist in your Region and Glue version. Counts
  (`*_num_workers`) mostly help the CSV read; raising the **type** helps the writes.
- **`max_parallel_tables` (default 20)** — how many tables load **at once** on the driver. The
  memory auto-throttle sizes this by driver free memory / `per_worker_mem_budget_mb` (default 1500),
  but it **never silently drops to 1** when the container memory reading is unknown or misreported
  — it holds a floor and logs the decision. On a bigger driver, more tables load concurrently.
- **Timeouts** — `load_timeout_minutes`/`load_big_timeout_minutes`/`validate_timeout_minutes`
  default to **2880** (48 h); raise toward **10080** (Glue's 7-day maximum) for very large tables.
- **`glue_version` (default 4.0)** — the scripts and driver wheels are tested on Glue 4.0. `5.0` is
  accepted but re-test the `pg8000`/`boto3` wheels on its Python 3.11 first.

Estimating load time for a table: `seconds ≈ rows ÷ (writers_per_file × rows_per_txn ÷ sec_per_txn)`,
where `rows_per_txn ≤ 3000` (DSQL cap, smaller for wide/LOB rows) and `sec_per_txn` is the measured
per-commit latency (≈0.4 s for narrow rows, ≈1.8 s for wide ones in the test cluster). E.g. a 16 M-row
narrow table at `writers_per_file=8`: 16e6 ÷ (8 × 3000 ÷ 0.4) ≈ 270 s, versus ≈2 h single-threaded.

Mind the quotas when raising any of these: the **Glue concurrent-job-runs** quota (default
**2,000/account**, adjustable), the **max task DPUs/account** (us-east-1 and us-east-2 = **1,000**,
adjustable — raise it in Service Quotas before a big migration), and DSQL connections (10,000/cluster,
100/s). A G.8X run = 8 DPU/node, so `load_big_num_workers × 8 + driver` must fit the DPU budget across
all concurrent groups. See [Limits](#limits).

### Limits

- **DSQL schemas:** at most **9 of your own** per fleet task (DSQL allows 10 per database;
  `cdc_control` uses one).
- **Task name:** letters, digits and hyphens, no leading/trailing hyphen, roughly under 50 chars (it
  becomes the S3 folder and Glue job-name stem).
- **Fleet concurrency:** 5 tasks start at a time; one startup fans out up to `map_max_concurrency`
  (default 6) groups at once, so a single task can ask for up to 6 × `load_big_num_workers` (default
  10) G.8X workers on its big groups ≈ 60 nodes × 8 DPU ≈ 480 DPU — check the **max task DPUs/account**
  quota (us-east-1/us-east-2 default **1,000**, adjustable) and the **Glue concurrent-job-runs**
  quota (default **2,000/account**, adjustable) before a large wave. Each load run also opens up to
  `max_write_concurrency` DSQL connections.
- **Always-on CDC jobs per task:** each task runs **1 main CDC job + up to `max_composite_forks`
  `ck` CDC jobs + up to `max_big_cdc_forks` `bg` CDC jobs** (defaults 8 + 8, so up to 17 CDC runs for
  one task), each holding its own DSQL connections. These run **concurrently** with the task's
  load/validate runs and with every other task — all against the **AWS Glue concurrent-job-runs
  quota** (default 2,000 per account, adjustable) and the DSQL connection limits (cluster 10,000, rate
  100/s). `plan_split` warns when a task's CDC count is a large share of the quota; raise the Glue
  quota (and watch DSQL connections) before fanning out many tasks or many forks at once.
- **Fleet size:** one fleet execution handles up to a few hundred tasks (the Map's results stay under
  Step Functions' 256 KB state limit to roughly 400 tasks). Split bigger lists.
- **CDC runtime:** a CDC Glue run stops after 7 days (the 10080-minute Glue maximum) — restart it by
  hand ([§8](#8-if-something-fails)), or cut over before 7 days.

### Per-task Glue job sizes

The load/discovery/validate templates each allow 10 concurrent runs; the CDC template allows 1.
These are the **default** sizes — every worker type, count and timeout is a `params.csv` setting
(see [§3](#3-fill-in-paramscsv) and [Tuning for speed](#tuning-for-speed-worker-sizes--single-file-parallelism)),
so you can size up for a big migration without editing templates:

| Job | Default size | Timeout | Setting keys |
|---|---|---|---|
| discovery | 5 × G.2X | 480 min | `discovery_worker_type` / `discovery_num_workers` / `discovery_timeout_minutes` |
| load | 10 × G.4X | 2880 min | `load_worker_type` / `load_num_workers` / `load_timeout_minutes` |
| load-big | 10 × G.8X | 2880 min | `load_big_worker_type` / `load_big_num_workers` / `load_big_timeout_minutes` |
| validate | 10 × G.8X | 2880 min | `validate_worker_type` / `validate_num_workers` / `validate_timeout_minutes` |
| CDC | 1 DPU (Python shell) or 2 × G.1X (Spark) | 10080 min | (fixed) |

The load runs driver-side, so a bigger worker **type** (= bigger driver) is what speeds a big
table; `writers_per_file` (default 8) parallelises a single big part-file. Allowed worker types:
G.1X, G.2X, G.4X, G.8X, G.12X, G.16X, R.1X, R.2X, R.4X, R.8X (G.12X+ and R.* are newer, higher
startup latency — confirm Region/Glue-version availability).

<details>
<summary>What each per-task state-machine step runs (what the fleet runs per task)</summary>

| Step | Runs | Does |
|---|---|---|
| `ResolveTask` / `CutoverResolveTask` | `resolve-task` | reads `pipeline.json` and the DMS task; startup also checks the task and records the folder owner |
| `DriverDiscoveryFullload` / `…Validation` / `…Cdc` | `driver-discovery` | checks each `driver-*` folder; for a Python-shell CDC job, prepares `driver-cdc/` into `driver-cdc-prepared/<fingerprint>/`. Runs before DMS starts |
| `StartDmsTask` → `IsDmsDone` | DMS API | start DMS, poll (30 s × 2880 ≈ 24 h) until `STOPPED_AFTER_CACHED_EVENTS` |
| `BuildTableList` | `resolve-task` | build the table list from `describe_table_statistics` + the task mappings; enforce ≤ 9 DSQL schemas |
| `CreateGlueJobs` | `create-glue-jobs` | create `<project>-<task>-<role>` jobs from `glue-templates/` in your Glue connection |
| `RunDiscovery` | Glue job 1 | writes `_manifest_index.json` (with each table's `pk_mode`) |
| `PlanSplit` | `plan-split` | assigns each table a CDC owner (`main`/`ck-<slug>`/`bg-<slug>`), writes per-group + per-fork manifests under `_orchestrator/`, and the plan's fork list |
| `EnsureForkJobs` | `create-glue-jobs` | after discovery: create/update each fork's jobs (composite → `ck-<slug>-{load,validate,cdc}`; big single/no-PK → `bg-<slug>-cdc`), tag them, write the task registry `_jobs.json`; recreate missing, report stale |
| `GroupFanOut` | Glue jobs 2 and 3 | load then validate each group AND each `ck` fork (up to 6 at once) |
| `ResumeDmsToCdc`, `StartCdcJob` | DMS API, Glue | resume DMS into CDC; start the main CDC job (with `--config_prefix` as a run argument) |
| `GetCdcRun` / `CheckCdcStarted` | Glue, S3 | wait up to 45 min for the main CDC run's start marker |
| `StartForkCdcMap` | Glue, S3 | start EACH fork CDC job (ck-* and bg-*) and wait up to 45 min for each one's own start marker |
| `CdcDriverFallback` → `UseSparkCdcJob` | `create-glue-jobs` | on a driver failure, re-create the CDC job as Spark (once) |
| cutover `StopCdcDmsTask` → `IsCdcTaskStopped` | DMS API | stop the DMS task (poll 15 s × 240 ≈ 1 h) |
| cutover `DrainCheck` | `drain-check` | wait (10 s × 4320 ≈ 12 h) until each table's latest CDC file is applied |
| cutover `StopCdcRun` | `stop-cdc-run` | stop this task's MAIN CDC run (found by `--config_prefix`) |
| cutover `ListForkCdcJobs` → `StopForkCdcRuns` | `create-glue-jobs`, `stop-cdc-run` | find the task's fork CDC jobs (ck-*/bg-*) by exact tag + registry, stop each run (tolerant if a job is absent) |
| cutover `DropTags` | `drop-tags` | drop the `_cdc_file` column |
| cutover `DeleteGlueJobs` → `AllGlueJobsDeleted` | `create-glue-jobs` | delete this task's Glue jobs — the five shared jobs plus every per-table fork job (ck-*/bg-*), found by exact tag + the registry (needs the account id to read job tags; the payload passes `account_id`/`dms_task_arn` and the Lambda also self-derives from its own context ARN / STS). `ListForkCdcJobs` runs first to stop their runs; idempotent. A job whose CDC run is still stopping is force-stopped and returned under `pending`; `AllGlueJobsDeleted` loops back (Wait → `DeleteGlueJobs`) until nothing is pending or a ~60-min budget runs out → `GlueJobsNotDeleted`. Any other delete error → `GlueJobsNotDeleted` |

**CDC correctness:** a table with a real primary key (or a declared `logical_key`) gets correct
inserts, updates and deletes. A table without one gets inserts and deletes; updates are skipped and
logged to `cdc_control.cdc_skipped_ops`. A table with a multi-column (composite) primary key is
listed at startup and skipped by the CDC job — it is loaded and validated, but its ongoing changes
are not applied ([Rules for the task list](#rules-for-the-task-list)).

**Control tables (in `control_schema`, default `cdc_control`):** `cdc_status`, `cdc_file_status`,
`cdc_chunk_log`, `cdc_apply_exceptions`, `cdc_validation_failures`, `cdc_skipped_ops`.

</details>

<details>
<summary>What each fleet step runs, and the fail states</summary>

| Step | Runs | Does |
|---|---|---|
| `CheckFleetInput` | — | requires `bucket` as a string, else `MissingFleetInput` |
| `Preflight` | `preflight-tasks` | reads `config/fleet_tasks.csv` and checks every task (reusing `resolve_task`'s rules); any problem → `PreflightFailed`, nothing started. If `config/params.csv` is present, builds `config/pipeline.json` from it and (startup only, nothing running) publishes it under the [safe-publish rule](#the-safe-publish-rule-how-pipelinejson-is-published-from-paramscsv), reporting `paramsPublished`/`backupKey`/`paramsReason` |
| `FanOut` (Map, 5 at a time) | `sfn:startExecution`, `sfn:describeExecution` | per row: skip if `already_running` / `past_full_load`; else start the per-task `startup`/`cutover`, wait 30 s, confirm it is RUNNING/SUCCEEDED |
| `EvalNotStarted` → `FleetStarted` / `FleetStartIncomplete` | — | `FleetStartIncomplete` if any task is `not_started`, else `FleetStarted` |

**Fleet** (`fleet-startup`, `fleet-cutover`): `MissingFleetInput`, `PreflightFailed` (nothing
started) · `FleetStartIncomplete` (some not started; the rest were) · `FleetFailed` (the fan-out
itself failed). Success `FleetStarted` means every task started or was skipped as already started —
not that the migrations finished.

**Per-task startup:** `MissingTaskArn`, `ResolveFailed`, `DriversFailed` (before DMS) · `DmsFailed`
(`DmsTaskFailed`), `DmsTimedOut` (`DmsPollBudgetExceeded`), `BuildTableListFailed`, `PlanSplitFailed`, `EnsureForkJobsFailed`, `GroupsFailed`,
`PipelineFailed` (before DMS resumes) · `CdcRunFailed`, `CdcRunEnded`, `CdcStartNotConfirmed`,
`ForkCdcStartNotConfirmed`,
`CdcFallbackFailed`, `PipelineFailed` (after DMS is in CDC).

**Per-task cutover:** `MissingTaskArn`, `ResolveFailed` (nothing touched) · `CutoverFailed` (DMS may
be stopped — check the failed step), `CdcDrainTimedOut` (`CdcDrainBudgetExceeded`), `GlueJobsNotDeleted`
(fully cut over bar one job delete).

**Override success states (runtime `override=true`):** startup ends in `TaskSucceededWithOverride`
instead of `TaskSucceeded` when it carried a `validate_failed` group past the gate; cutover ends in
`CutoverSucceededWithOverride` instead of `CutoverSucceeded` when it bypassed a validation gate
and/or accepted a startup override. A cutover of a task whose startup used override, started
**without** `override`, refuses at `StartupOverrideRequiresOverride` (nothing touched); if writing
the startup override record fails, startup ends at `OverrideRecordNotWritten`. See
[§8 "Validation failed — re-run with override"](#validation-failed--re-run-with-override).

See [§8](#8-if-something-fails) for the recovery keyed to each state, and
[`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md) for the fleet reference.

</details>

<details>
<summary>Where each value comes from</summary>

**Read from `config/pipeline.json` at run time** (by every per-task run and the fleet's preflight)
— all fourteen keys `resolve_task` builds into its resolved output:
`project`, `region`, `dsql_endpoint`, `dsql_user`, `dsql_database`, `glue_role_arn`,
`glue_connection`, `cdc_engine`, `cdc_spark_fallback`, `control_schema`, `cdc_validation`,
`cdc_validation_sample`, `max_composite_forks`, `max_big_cdc_forks`. Which component acts on each:

- `resolve_task` reads and validates **all fourteen** (`_load_settings` / `_validate_settings`) and
  resolves the task's names, S3 layout and `dsql_endpoint` candidates (it derives the private
  PrivateLink hostname itself; the Glue jobs/Lambdas try each form).
- `plan_split` reads `max_composite_forks` and `max_big_cdc_forks` (passed via the resolved
  `maxCompositeForks` / `maxBigCdcForks`) to cap composite `ck` forks (over the cap → startup
  `PlanSplitFailed`) and big `bg` CDC forks (over the cap → stay on the main CDC job with a warning).
  It also reads the eight planning thresholds — `big_table_row_threshold`, `file_fanout_threshold`,
  `max_groups`, `map_max_concurrency`, `max_files_in_parallel`, `conn_budget`,
  `min_writers_per_loader`, `max_writers_per_loader` (resolved as `bigTableRowThreshold` etc.) — to
  decide which tables are big, how many groups to pack into, and the loader/writer concurrency. The
  resolved `mapMaxConcurrency` also drives the `GroupFanOut` Map's `MaxConcurrencyPath`.
- `create_glue_jobs` sets `cdc_validation` / `cdc_validation_sample` on every CDC job it creates
  (as `--cdc_validation` / `--cdc_validation_sample`), and builds all the task's jobs under
  `glue_role_arn` / `glue_connection` with the chosen `cdc_engine`.
- the CDC scripts (`scripts/glue_cdc_continuous.py`, `scripts/glue_cdc_composite.py`) read
  `cdc_validation` / `cdc_validation_sample` (Tier-2 validation) and `control_schema` (the DSQL
  schema that holds the control tables).

Per task, `config/_task/<task name>/_cdc_engine.json` (an automatic switch to Spark) overrides
`cdc_engine`.

**Worked out per task, from the row's `taskArn`** (plus `taskSuffix` / `adoptExistingFolder`): the
task name (the DMS task's name, or `task_suffix`; after the first startup, the name recorded in
`config/_task_index/<task id>.json`), its config folder `config/_task/<task name>/`, its Glue
job names `<project>-<task name>-{discovery,load,load-big,validate,cdc}` (plus per-table fork jobs
`ck-<slug>-{load,validate,cdc}` / `bg-<slug>-cdc`), its owner record
`_task.json`, and its S3 layout (from the DMS S3 endpoint's `BucketFolder`, `TimestampColumnName`,
`CsvNullValue`, …).

The `<<…>>` blanks in `stepfunctions/`, `glue-templates/` and `iam/` are filled by `tools/setup.sh`;
see [`docs/MANUAL_SETUP.md`](docs/MANUAL_SETUP.md) to fill them by hand.

</details>

More: [`USAGE_GUIDE.md`](USAGE_GUIDE.md) (monitoring, manual runs) and
[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md) (architecture, limitations, DDL matrix).
