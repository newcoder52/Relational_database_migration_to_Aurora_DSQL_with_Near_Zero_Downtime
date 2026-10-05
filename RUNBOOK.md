# RUNBOOK — deploy and run the Oracle → Aurora DSQL migration pipeline

Follow this top to bottom. It is plain AWS CLI, the same on **macOS, Linux and AWS CloudShell**.

The pipeline runs tasks **one way only: through the fleet.** You trigger `fleet-startup` (and later
`fleet-cutover`) once, with `{"bucket":"<bucket>","inputPrefix":"config"}`; the fleet reads
`s3://<bucket>/config/fleet_tasks.csv` and starts the per-task `startup` (or `cutover`) state machine
for every row. **One DMS task is one row in that CSV**; a new wave is just a new
`config/fleet_tasks.csv` and another trigger. You never start a per-task state machine yourself — the
per-task machines appear only in [Reference](#10-reference), as what the fleet runs.

`inputPrefix` is **always `"config"`** — it is the fixed folder that holds everything the operator
provides (`params.csv`, `fleet_tasks.csv`), so the fleet reads `config/params.csv` and
`config/fleet_tasks.csv`. Pass it exactly as `"config"` on every trigger.

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
- [ ] **An Aurora DSQL cluster** and its endpoint. The public form is `<cluster>.dsql.<region>.on.aws`.
      **Inside a VPC with no internet** (Glue reaches DSQL through a VPC endpoint), use the DSQL VPC
      endpoint's **private DNS name** `<cluster>.dsql-<id>.<region>.on.aws` (note the `dsql-<id>`) —
      the public name times out at discovery. Find it with `aws ec2 describe-vpc-endpoints` for
      service `com.amazonaws.<region>.dsql-<id>` (the endpoint must have private DNS enabled). This is
      the `dsql_endpoint` value in [§3](#3-fill-in-paramscsv).
- [ ] **Target tables already created** in the target schema. The pipeline loads into existing
      tables; it never creates them. A single-column primary key gets full insert/update/delete CDC;
      a table with a **multi-column** primary key is skipped by the CDC job (see
      [Rules for the task list](#rules-for-the-task-list)).
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

| Parameter | Required? | Default | Meaning |
|---|---|---|---|
| `account_id` | **required** | — | 12-digit AWS account id (setup/IAM only; never written to `pipeline.json`) |
| `region` | **required** | — | AWS region of the DMS tasks and pipeline (must equal the task ARN's region) |
| `project` | **required** | — | short prefix (letters, digits, hyphens) for role, Lambda and job names |
| `dsql_endpoint` | **required** | — | Aurora DSQL endpoint, `<cluster>.dsql.<region>.on.aws`; inside a VPC with no internet use the VPC endpoint's private DNS name `<cluster>.dsql-<id>.<region>.on.aws` ([§2](#2-what-you-need)) |
| `dsql_user` | optional | `admin` | DSQL user |
| `dsql_database` | optional | `postgres` | DSQL database |
| `glue_connection` | optional | `""` (no VPC) | the Glue network connection's **exact** name; `""` = Glue runs with no VPC connection |
| `cdc_engine` | optional | `pythonshell` | `pythonshell` (1 DPU) or `spark` (Glue 4.0, 2 × G.1X) |
| `cdc_spark_fallback` | optional | `true` | `true`: on a Python-shell CDC driver failure the startup re-creates that task's CDC job as Spark; `false`: stop at `DriversFailed` / `CdcRunFailed` |
| `control_schema` | optional | `cdc_control` | DSQL schema for the CDC control tables |
| `glue_role_arn` | optional | `arn:aws:iam::<account_id>:role/<project>-glue-exec-role` | set only if your Glue role name differs from the default |
| `subnet_id` | optional (setup-only) | — | private subnet for the Glue VPC connection. Set **both** `subnet_id` and `security_group_id`, or neither. Not written to `pipeline.json` |
| `security_group_id` | optional (setup-only) | — | security group for the Glue VPC connection. Both-or-neither with `subnet_id`. Not written to `pipeline.json` |

Ten keys end up in `config/pipeline.json`: `project`, `region`, `dsql_endpoint`, `dsql_user`,
`dsql_database`, `glue_role_arn`, `glue_connection`, `cdc_engine`, `cdc_spark_fallback`,
`control_schema`. `account_id`, `subnet_id` and `security_group_id` are used only by setup.

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
aws s3 ls "s3://$BUCKET/scripts/"            # 4 Glue scripts
aws s3 ls "s3://$BUCKET/glue-templates/"     # 6 templates
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
  --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}"
```

The fleet is started with `{"bucket":"<bucket>","inputPrefix":"config"}` and always reads from the
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
| `MissingFleetInput` | the input lacked `bucket` or `inputPrefix` as a string | start again with `{"bucket":"<bucket>","inputPrefix":"config"}` |

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
- **Tables with a multi-column (composite) primary key** are loaded and validated normally, but the
  CDC job does **not** apply their ongoing changes: it detects the composite key, lists those tables
  at startup, and leaves them untouched (nothing applies their inserts/updates/deletes). Treat this
  as a current limitation — a composite-key table's full load is correct, but it will not track
  changes during CDC.
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
> endpoint (the `<cluster>.dsql-<id>.<region>.on.aws` form, [§2](#2-what-you-need)), run `psql` from
> a host inside that VPC (e.g. a CloudShell VPC environment or an EC2 instance in the subnet).

**Progress queries** (control-table schema `cdc_control`, the `control_schema` default; `table_name`
is the lowercased `<schema>.<table>`):

```sql
-- per-table status: active (applying) / idle (caught up) / blocked (needs attention),
-- the last fully-applied CDC file, and the error if blocked
SELECT table_name, status, last_done_file, error FROM cdc_control.cdc_status ORDER BY table_name;

-- files still to apply vs. already applied, for one table (status 'done' = applied)
SELECT status, count(*) FROM cdc_control.cdc_file_status
WHERE table_name = '<schema>.<table>' GROUP BY status;
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
- [ ] **Composite-key (multi-column-PK) tables:** remember the CDC job does not track their changes
      (it lists and skips them; see [Rules for the task list](#rules-for-the-task-list)). Only cut
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
  --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}"
```

The result states are the same as a startup fleet ([§5](#5-run-tasks-with-the-fleet)):
`FleetStarted` means each cutover **started**, not that it finished — watch each child. Each
`$PROJECT-cutover` child, for its one task: stops the DMS task (up to ~1 h), waits until each table's
last CDC file is applied (`DrainCheck`; up to ~12 h), stops this task's CDC run, drops the internal
`_cdc_file` column, and deletes this task's five Glue jobs. It finds the task by its ARN, so a
renamed task still cuts over its original folder and jobs.

> **† A cutover fleet re-triggers cutover for EVERY task in the CSV**, including ones already cut
> over. The cutover preflight has no "already cut over" skip — it skips only a task whose
> `$PROJECT-cutover` is *currently running* (`already_running`). Re-running `fleet-cutover` with an
> already-cut-over task still in the CSV starts a **fresh** cutover for it, which then fails because
> its first step stops a DMS task that is already stopped (the DMS API rejects it). **Remove
> already-cut-over tasks from the CSV before triggering `fleet-cutover` again.**

Each child ends at one of:

| Ends at | Meaning | What to do |
|---|---|---|
| `CutoverSucceeded` | done | point the application at Aurora DSQL |
| `GlueJobsNotDeleted` | data is cut over; only deleting a Glue job failed (named in the error; the five jobs are `$PROJECT-$TASK_NAME-{discovery,load,load-big,validate,cdc}`) | delete it by hand: `aws glue delete-job --job-name <name>`. **Do not re-list this task in a cutover fleet** |
| `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing changed for this task | fix the error shown, re-run cutover for this task via the fleet |
| `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`) | DMS is stopped; a table's last file wasn't applied within ~12 h | fix the cause ([§8](#8-if-something-fails)), then **finish by hand** ([§8](#8-if-something-fails)) |
| `CutoverFailed` at a step **after DMS was stopped** | DMS is stopped | open the failed state, fix it, then **finish by hand** ([§8](#8-if-something-fails)) |

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
| **Fleet** `MissingFleetInput` | the input lacked `bucket` or `inputPrefix` as a string | trigger again with `{"bucket":"<bucket>","inputPrefix":"config"}` |
| **Fleet** `PreflightFailed` | a problem before anything started: a `config/pipeline.json`/`config/params.csv` problem, a missing/empty or bad/duplicate task row, a folder owned by another task, too many DSQL schemas in a task, or (with `config/params.csv`) a settings change blocked because a run is in progress / a cutover / executions can't be listed | fix each problem the cause lists; nothing started, so trigger the fleet again |
| **Fleet** `FleetStartIncomplete` | some tasks didn't start (`results.tasks` names them) | fix those tasks, trigger the fleet again — started/running/past-full-load tasks are skipped |
| **Fleet** `FleetFailed` | the fan-out itself failed (rare) | re-trigger; if it recurs, check the sfn role (§4) can start and describe the per-task executions |
| **Startup** `ResolveFailed` (`SettingsError`) | a `pipeline.json` problem: missing file/key, a value with `<`/`>`, a non-ARN `glue_role_arn`, or a region ≠ the task ARN's region | fix `params.csv`/`pipeline.json`, re-trigger the fleet |
| **Startup** `ResolveFailed` (`TaskCheckError`) | a DMS task setting: not `full-load-and-cdc`, `StopTaskCachedChangesApplied` not true, `AddColumnName` false, wrong target bucket, or the task is past its full load | fix the DMS task/endpoint, re-trigger the fleet |
| **Startup** `ResolveFailed` (`FolderOwnerError`) | `config/_task/<name>/` was made by a different task ARN, holds files from an older run with no owner, or a `task_suffix` differs from the recorded one | archive the folder (the error gives the `aws s3 mv`), use the recorded suffix, or set `adopt_existing_folder=true` for this task's own pre-shared-workflow files |
| **Startup** `DriversFailed` (`DriverCheckError`) | a `driver-cdc/` wheel can't run on Python 3.9 (scramp 1.4.7+, boto3/botocore 1.43+, urllib3 2.x), two versions of one package, a missing package, or a Spark driver folder without pg8000 | the error names the wheel; fix the folder (§4 driver wheels) and re-trigger the fleet |
| **Startup** `DmsFailed` (`DmsTaskFailed`) | DMS failed or a table errored during full load | fix in the DMS console (**Table statistics** + CloudWatch; reload the errored table). A task can only be (re)started while it hasn't finished its full load — else see [§9](#9-reload-a-task-from-scratch) |
| **Startup** `DmsTimedOut` (`DmsPollBudgetExceeded`) | DMS didn't reach `STOPPED_AFTER_CACHED_EVENTS` within 24 h — usually a task already past its full load, or stopped partway, or a genuinely long load | check the DMS task; reload with a new DMS task if needed ([§9](#9-reload-a-task-from-scratch)) |
| **Startup** `BuildTableListFailed` | building the table list from the DMS task failed: a table didn't load cleanly, a table-mapping transformation the pipeline can't reproduce for S3 folder names, or more than 9 distinct DSQL schemas | the error names the tables/schemas; fix the source or the DMS task's rules, re-trigger the fleet. No Glue jobs were created |
| **Startup** `GroupsFailed`, or `PipelineFailed` at `CreateGlueJobs`/`RunDiscovery`/`PlanSplit`/`GroupFanOut` | DMS full load is in S3; DMS is paused at `STOPPED_AFTER_CACHED_EVENTS` | fix the cause (the failed group's Glue log has it), re-trigger the fleet — finished files/tables are skipped; the task isn't past full load, so it isn't skipped |
| **Startup** `PipelineFailed` at `ResumeDmsToCdc` | load done and validated; DMS probably still paused | **don't re-trigger the fleet for this task.** If DMS is still stopped, resume it: `aws dms start-replication-task --replication-task-arn "$TASK_ARN" --start-replication-task-type resume-processing`, then **start the CDC job by hand** (below) |
| **Startup** `CdcRunFailed`/`CdcRunEnded`/`CdcFallbackFailed`, or `PipelineFailed` at `StartCdcJob`/`GetCdcRun`/`CheckCdcStarted` | load done; **DMS is in CDC**, capturing changes to S3 | **don't re-trigger the fleet** (it skips this task). Check whether a CDC run is already RUNNING ([§6](#6-watch-progress)); if not, fix the cause in the CDC log and **start the CDC job by hand** (below). Nothing is lost while it's down — DMS keeps writing change files |
| **Startup** `CdcStartNotConfirmed` | the CDC run is running but didn't write its start marker in 45 min | check the CDC log ([§6](#6-watch-progress)). If it shows `entering poll loop`, CDC is fine and the marker couldn't be written — check the Glue role can write `config/_task/<task>/_cdc_started/` |
| **Startup** execution shows `CdcDriverFallback` then succeeds | the Python-shell drivers failed; the job is now Spark | nothing to fix. The reason is in `config/_task/<task name>/_cdc_engine.json`; fix `driver-cdc/` and delete that file to go back to Python shell |
| **Cutover** `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing changed | fix the error, re-run cutover for this task via the fleet (keep only this task in the CSV, or remove already-cut-over tasks first) |
| **Cutover** `CdcDrainTimedOut`, or `CutoverFailed`/`GlueJobsNotDeleted` **after DMS was stopped** | DMS is stopped (or fully cut over bar one job delete) | fix the cause, then **finish by hand** (below). **Do not re-list this task in a cutover fleet** — its first step would fail on the already-stopped DMS task |
| **CDC** a table is `blocked` in `cdc_control.cdc_status` | a `DROP COLUMN` on the source, or a row DSQL rejected (e.g. NULL into NOT NULL) | fix the cause, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';` ([how to connect](#connect-to-dsql-and-check-progress)) — CDC resumes. **Never delete the row** (applied files stay and would all be replayed) |
| **CDC** run ends with no error after ~7 days | the 7-day Glue timeout (the 10080-minute maximum) | start the CDC job by hand (below); it resumes from where it left off. Cut over before 7 days where you can |
| **CDC** Spark job: `DataNotFoundError: endpoints` | a boto3/botocore wheel is in `driver-fullload/` or `driver-validation/` | remove it; those folders hold the 5 pg8000 wheels only |
| **CDC/Glue** `Unknown service: 'dsql'` | `driver-cdc/` lacks a current boto3 set | re-stage drivers (§4), re-trigger the fleet |
| **Glue** `Can't create a connection to host ...dsql... port 5432` or `Name or service not known` | the job isn't in your VPC, or `dsql_endpoint` is the public name inside a no-internet VPC | set `glue_connection` and the VPC-endpoint `dsql_endpoint` ([§2](#2-what-you-need)/[§3](#3-fill-in-paramscsv)), re-trigger. Check: `aws glue get-job --job-name <job> --query Job.Connections` |
| **Setup** an `aws` command seems to hang | the CLI pager is waiting | `export AWS_PAGER=""` and re-run; whatever you Ctrl-C'd was still created |
| **Setup** `create-function`: *role cannot be assumed by Lambda* | the role is seconds old | wait 10 s and re-run `tools/setup.sh` |

**Start the CDC job by hand** (per task; the job keeps its saved settings — pass `--config_prefix` as
a run argument so cutover can find and stop the run):

```bash
PROJECT="<project>"; BUCKET="<bucket>"; export AWS_PAGER=""
TASK_NAME="<task name>"
CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-cdc" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}" --query JobRunId --output text
```

Then check it as in [§6](#6-watch-progress). **Don't use the console's Run button** for the CDC job:
a console run has no `--config_prefix`, so cutover would not find and stop it.

**Finish a cutover by hand** (after any failure once DMS is stopped; set `TASK_NAME` to the
folder/job stem). The SQL steps (checking status, dropping `_cdc_file`) use a DSQL session — see
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
# 4. Delete this task's five Glue jobs
for r in discovery load load-big validate cdc; do
  aws glue delete-job --job-name "$PROJECT-$TASK_NAME-$r"
done
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

---

## 10. Reference

### The S3 layout

One bucket, fixed folder names (baked into the templates):

```
s3://<bucket>/
├── scripts/                  # the 4 Glue scripts
├── glue-templates/           # the 6 Glue job templates
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

### Limits

- **DSQL schemas:** at most **9 of your own** per fleet task (DSQL allows 10 per database;
  `cdc_control` uses one).
- **Task name:** letters, digits and hyphens, no leading/trailing hyphen, roughly under 50 chars (it
  becomes the S3 folder and Glue job-name stem).
- **Fleet concurrency:** 5 tasks start at a time; one startup fans out up to 6 groups at once, so a
  single task can ask for up to 6 × 20 = 120 G.8X workers on its big groups — check your Glue
  concurrent-run and DPU quotas before a large wave. Each load run also opens up to 150 DSQL
  connections (`max_write_concurrency`).
- **Fleet size:** one fleet execution handles up to a few hundred tasks (the Map's results stay under
  Step Functions' 256 KB state limit to roughly 400 tasks). Split bigger lists.
- **CDC runtime:** a CDC Glue run stops after 7 days (the 10080-minute Glue maximum) — restart it by
  hand ([§8](#8-if-something-fails)), or cut over before 7 days.

### Per-task Glue job sizes

The load/discovery/validate templates each allow 10 concurrent runs; the CDC template allows 1:

| Job | Workers | | Job | Workers |
|---|---|---|---|---|
| discovery | 5 × G.2X | | validate | 10 × G.4X |
| load | 10 × G.4X | | CDC | 1 DPU (Python shell) or 2 × G.1X (Spark) |
| load-big | 20 × G.8X | | | |

<details>
<summary>What each per-task state-machine step runs (what the fleet runs per task)</summary>

| Step | Runs | Does |
|---|---|---|
| `ResolveTask` / `CutoverResolveTask` | `resolve-task` | reads `pipeline.json` and the DMS task; startup also checks the task and records the folder owner |
| `DriverDiscoveryFullload` / `…Validation` / `…Cdc` | `driver-discovery` | checks each `driver-*` folder; for a Python-shell CDC job, prepares `driver-cdc/` into `driver-cdc-prepared/<fingerprint>/`. Runs before DMS starts |
| `StartDmsTask` → `IsDmsDone` | DMS API | start DMS, poll (30 s × 2880 ≈ 24 h) until `STOPPED_AFTER_CACHED_EVENTS` |
| `BuildTableList` | `resolve-task` | build the table list from `describe_table_statistics` + the task mappings; enforce ≤ 9 DSQL schemas |
| `CreateGlueJobs` | `create-glue-jobs` | create `<project>-<task>-<role>` jobs from `glue-templates/` in your Glue connection |
| `RunDiscovery` | Glue job 1 | writes `_manifest_index.json` |
| `PlanSplit` | `plan-split` | writes per-group manifests under `_orchestrator/group-<n>/` |
| `GroupFanOut` | Glue jobs 2 and 3 | load then validate each group (up to 6 groups at once) |
| `ResumeDmsToCdc`, `StartCdcJob` | DMS API, Glue | resume DMS into CDC; start the CDC job (with `--config_prefix` as a run argument) |
| `GetCdcRun` / `CheckCdcStarted` | Glue, S3 | wait up to 45 min for the CDC run's start marker |
| `CdcDriverFallback` → `UseSparkCdcJob` | `create-glue-jobs` | on a driver failure, re-create the CDC job as Spark (once) |
| cutover `StopCdcDmsTask` → `IsCdcTaskStopped` | DMS API | stop the DMS task (poll 15 s × 240 ≈ 1 h) |
| cutover `DrainCheck` | `drain-check` | wait (10 s × 4320 ≈ 12 h) until each table's latest CDC file is applied |
| cutover `StopCdcRun` | `stop-cdc-run` | stop this task's CDC run (found by `--config_prefix`) |
| cutover `DropTags` | `drop-tags` | drop the `_cdc_file` column |
| cutover `DeleteGlueJobs` → `AllGlueJobsDeleted` | `create-glue-jobs` | delete this task's five Glue jobs; any failure → `GlueJobsNotDeleted` |

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
| `CheckFleetInput` | — | requires `bucket` and `inputPrefix` (use `"config"`) as strings, else `MissingFleetInput` |
| `Preflight` | `preflight-tasks` | reads `config/fleet_tasks.csv` and checks every task (reusing `resolve_task`'s rules); any problem → `PreflightFailed`, nothing started. If `config/params.csv` is present, builds `config/pipeline.json` from it and (startup only, nothing running) publishes it under the [safe-publish rule](#the-safe-publish-rule-how-pipelinejson-is-published-from-paramscsv), reporting `paramsPublished`/`backupKey`/`paramsReason` |
| `FanOut` (Map, 5 at a time) | `sfn:startExecution`, `sfn:describeExecution` | per row: skip if `already_running` / `past_full_load`; else start the per-task `startup`/`cutover`, wait 30 s, confirm it is RUNNING/SUCCEEDED |
| `EvalNotStarted` → `FleetStarted` / `FleetStartIncomplete` | — | `FleetStartIncomplete` if any task is `not_started`, else `FleetStarted` |

**Fleet** (`fleet-startup`, `fleet-cutover`): `MissingFleetInput`, `PreflightFailed` (nothing
started) · `FleetStartIncomplete` (some not started; the rest were) · `FleetFailed` (the fan-out
itself failed). Success `FleetStarted` means every task started or was skipped as already started —
not that the migrations finished.

**Per-task startup:** `MissingTaskArn`, `ResolveFailed`, `DriversFailed` (before DMS) · `DmsFailed`
(`DmsTaskFailed`), `DmsTimedOut` (`DmsPollBudgetExceeded`), `BuildTableListFailed`, `GroupsFailed`,
`PipelineFailed` (before DMS resumes) · `CdcRunFailed`, `CdcRunEnded`, `CdcStartNotConfirmed`,
`CdcFallbackFailed`, `PipelineFailed` (after DMS is in CDC).

**Per-task cutover:** `MissingTaskArn`, `ResolveFailed` (nothing touched) · `CutoverFailed` (DMS may
be stopped — check the failed step), `CdcDrainTimedOut` (`CdcDrainBudgetExceeded`), `GlueJobsNotDeleted`
(fully cut over bar one job delete).

See [§8](#8-if-something-fails) for the recovery keyed to each state, and
[`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md) for the fleet reference.

</details>

<details>
<summary>Where each value comes from</summary>

**Read from `config/pipeline.json` at run time** (by every per-task run and the fleet's preflight):
`project`, `region`, `dsql_endpoint`, `dsql_user`, `dsql_database`, `glue_role_arn`,
`glue_connection`, `cdc_engine`, `cdc_spark_fallback`, `control_schema`. Per task,
`config/_task/<task name>/_cdc_engine.json` (an automatic switch to Spark) overrides `cdc_engine`.

**Worked out per task, from the row's `taskArn`** (plus `taskSuffix` / `adoptExistingFolder`): the
task name (the DMS task's name, or `task_suffix`; after the first startup, the name recorded in
`config/_task_index/<task id>.json`), its config folder `config/_task/<task name>/`, its five Glue
job names `<project>-<task name>-{discovery,load,load-big,validate,cdc}`, its owner record
`_task.json`, and its S3 layout (from the DMS S3 endpoint's `BucketFolder`, `TimestampColumnName`,
`CsvNullValue`, …).

The `<<…>>` blanks in `stepfunctions/`, `glue-templates/` and `iam/` are filled by `tools/setup.sh`;
see [`docs/MANUAL_SETUP.md`](docs/MANUAL_SETUP.md) to fill them by hand.

</details>

More: [`USAGE_GUIDE.md`](USAGE_GUIDE.md) (monitoring, manual runs) and
[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md) (architecture, limitations, DDL matrix).
