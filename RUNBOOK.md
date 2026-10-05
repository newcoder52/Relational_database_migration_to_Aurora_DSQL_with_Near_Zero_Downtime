# RUNBOOK — deploy and run the Oracle → Aurora DSQL migration pipeline

Follow this top to bottom. It is plain AWS CLI you paste in order — no CDK, no CloudFormation.
Every command is written to work the same on **macOS, Linux and AWS CloudShell**.

The pipeline runs tasks **one way only: through the fleet.** You never start a task's
`startup` or `cutover` state machine yourself — the fleet starts them for you, one per task, from a
CSV. One DMS task is one row in that CSV; many tasks are many rows. Starting a per-task state
machine by hand is not a supported path and is not documented here (the per-task machines appear
once, in [Reference](#reference), as what the fleet runs).

- One-time setup (Steps 1–4): about **1–2 hours**. It always includes the fleet.
- Running tasks after that (Steps 5–6): a few minutes of your time, plus the load and CDC that run
  on their own.

> **New here?** Read [`README.md`](README.md) for the big picture first.
> [`USAGE_GUIDE.md`](USAGE_GUIDE.md) covers day-to-day monitoring once a task is running.

**Contents**

- [What you're building](#what-youre-building)
- [Prerequisites checklist](#prerequisites-checklist)
- [Values: what setup needs vs. what running tasks needs](#values-what-setup-needs-vs-what-running-tasks-needs)
- [The S3 layout](#the-s3-layout)
- **One-time setup:**
  [Step 1 — IAM roles](#step-1--create-the-iam-roles) ·
  [Step 1b — Glue network connection](#step-1b--create-the-glue-network-connection-vpc-only) ·
  [Step 2 — Lambda functions](#step-2--create-the-lambda-functions) ·
  [Step 3a — scripts & templates](#step-3a--scripts-and-job-templates) ·
  [Step 3b — driver wheels](#step-3b--driver-wheels) ·
  [Step 3c — pipeline settings](#step-3c--pipeline-settings) ·
  [Step 4 — state machines](#step-4--create-the-state-machines)
- **Run tasks:**
  [Step 5 — run tasks with the fleet](#step-5--run-tasks-with-the-fleet) ·
  [Step 6 — cut over with the fleet](#step-6--cut-over-with-the-fleet)
- [Rules for the task list](#rules-for-the-task-list)
- [Capacity and overlap](#capacity-and-overlap)
- [If a run fails: how to continue](#if-a-run-fails-how-to-continue)
- [Clean-slate reload](#clean-slate-reload)
- [Upgrading an existing deployment](#upgrading-an-existing-deployment)
- [Known issues (temporary)](#known-issues-temporary)
- [Troubleshooting](#troubleshooting)
- [Reference](#reference)

---

## What you're building

An automated pipeline that copies an Oracle database into **Amazon Aurora DSQL**, keeps it in
sync while the application keeps running, and lets you cut over with almost no downtime.

- **AWS DMS** reads Oracle and writes the rows to **Amazon S3** as CSV files.
- **AWS Glue** jobs discover, load, validate and then continuously apply those files into
  Aurora DSQL.
- **AWS Step Functions** runs the sequence. Each task runs through a shared **`startup`** state
  machine and a shared **`cutover`** state machine. **You never start those two yourself** — the
  fleet does.
- The **fleet** is two more state machines — **`fleet-startup`** and **`fleet-cutover`** — plus a
  **`preflight-tasks`** Lambda. You trigger a fleet once with `{"bucket","inputPrefix"}`; it reads
  a **`fleet_tasks.csv`** list and starts the per-task `startup` (or `cutover`) for every row.
  **This is the only way to start or cut over a task.** One DMS task = one row; many tasks = many
  rows.
- **One S3 bucket** holds everything the pipeline needs: scripts, templates, driver wheels,
  settings, the task list and per-task state.

**One-time vs. running tasks:**

| | What | Steps |
|---|---|---|
| **Once** | IAM roles (incl. the fleet and preflight roles), the 7 pipeline Lambdas + the preflight Lambda, files and settings in S3, the per-task `startup`/`cutover` state machines **and** the `fleet-startup`/`fleet-cutover` state machines | 1–4 |
| **Running tasks** | stage each task's table list, write `fleet_tasks.csv`, trigger `fleet-startup`; later trigger `fleet-cutover` | 5–6 |

**Terms used below:**

- **full load** — the one-time bulk copy of the rows that already exist. **CDC** (change data
  capture) — the stream of inserts, updates and deletes made after that, applied until you cut
  over.
- **`STOPPED_AFTER_CACHED_EVENTS`** — the DMS status that means "full load done, later changes
  captured and paused." The startup waits for it before loading into DSQL.
- **cutover** — the final switch of the application to Aurora DSQL.
- A table is **caught up** when its row in `cdc_control.cdc_status` shows no pending work.
- **the fleet** — `fleet-startup` / `fleet-cutover`. **a per-task run** — the `startup` /
  `cutover` execution the fleet starts for one task.

The pipeline **loads into tables that already exist** in DSQL; it never creates your target
tables.

---

## Prerequisites checklist

Work through this before Step 1. Each box is a thing the pipeline assumes.

- [ ] **AWS CLI** installed and configured (`aws sts get-caller-identity` prints your account),
      and **Python 3 with pip**. The simplest option is **AWS CloudShell**, which already has the
      CLI, Python 3, pip, `git` and `zip`.
- [ ] **This repo on your machine**, and your terminal in its root folder. In CloudShell:
      `git clone <this repo>` then `cd` into it. (No GitHub access from CloudShell? Zip the repo,
      upload it with **Actions → Upload file**, and unzip it.)
- [ ] **An Aurora DSQL cluster**, and its endpoint (looks like `abcd.dsql.us-east-1.on.aws`).
- [ ] **Target tables already created** in the target schema, each ideally with a **single-column
      primary key**. Tables with a **multi-column** primary key are skipped by the main CDC job and
      need a separate CDC job ([see the rules](#rules-for-the-task-list)).
- [ ] **At most 9 schemas of your own** in the DSQL database. DSQL allows 10 schemas per database
      (not adjustable) and the pipeline adds `cdc_control`. Count yours:
      `SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg\_%' AND schema_name <> 'information_schema';`
- [ ] **A network path from Glue (and two Lambdas) to DSQL**, if your account is locked down: a
      **private subnet** and a **security group** that can reach DSQL, plus an **S3 gateway
      endpoint** in that subnet's route table. Step 1b uses them. Skip the VPC steps if Glue can
      already reach DSQL.
- [ ] **A DMS task** of type **`full-load-and-cdc`** for every DMS task you'll list. The boxes
      marked ✔ are checked by preflight (and again by each task's startup) *before* DMS starts, so a
      mistake fails in seconds instead of hours:
  - [ ] ✔ `FullLoadSettings.StopTaskCachedChangesApplied = true`
        (and `StopTaskCachedChangesNotApplied` **not** true)
  - [ ] a **short task name** — letters, digits and hyphens, no leading/trailing hyphen, roughly
        under 50 characters. It becomes the task's S3 folder and the Glue job names.
  - [ ] ✔ an **S3 target endpoint** that writes to **your pipeline bucket**, with ✔ `AddColumnName
        = true`, `TimestampColumnName = dms_timestamp`, `Rfc4180 = true`, and **no** `CompressionType`
        (plain CSV). `DatePartitionEnabled` is not required.
  - [ ] a table mapping with a **convert-lowercase rule for columns**. Schema and table names may
        be any case; column names must be lowercase in DSQL. (A missing lowercase rule is only a
        warning, but get it right.) The DSQL schema name must match the DMS target schema in
        lowercase.
  - [ ] the DMS task is in the **same region** as the pipeline (`region` in Step 3c).
  - [ ] for tables **without a primary key**: DMS set to emit inserts and deletes only (updates
        are skipped and logged).
  - [ ] **NULLs:** the pipeline stores a value as NULL only when the field is empty **or** equals
        the endpoint's `CsvNullValue` (DMS writes the literal text `NULL` when you leave
        `CsvNullValue` unset). Every other text, including `NA`, `NONE` and `N/A`, is stored as
        text. Don't change `CsvNullValue` partway through a migration.

---

## Values: what setup needs vs. what running tasks needs

The pipeline deliberately splits its values into two groups.

**Setup (Steps 1–4) needs the "export" values below.** They name your account, bucket, region,
DSQL cluster and (if used) your VPC. You set them once per terminal session. CloudShell forgets
them when it reconnects, so re-run the block in a new shell.

```bash
# ---- edit these ----
export BUCKET="my-migration-bucket"          # pipeline bucket (no s3://, no trailing slash)
export ACCOUNT_ID="123456789012"             # 12-digit AWS account id
export REGION="us-east-1"                     # AWS region
export PROJECT="dms-dsql"                     # short prefix for role, Lambda and job names
export DSQL_ENDPOINT="abcd.dsql.us-east-1.on.aws"
export DSQL_CLUSTER_ID="abcd"                 # first label of the endpoint
export DSQL_USER="admin"
export DSQL_DATABASE="postgres"

# ---- VPC: only if Glue must run inside your VPC to reach DSQL ----
export SUBNET_ID="subnet-0abc1234"            # private subnet: route to DSQL + an S3 gateway endpoint
export SECURITY_GROUP_ID="sg-0abc1234"        # allows all TCP from itself; outbound 443 and 5432
export GLUE_CONNECTION="$PROJECT-vpc"         # EXACT name of the Glue network connection
                                              # (Step 1b, or one made in the console); "" = no VPC

# ---- derived — don't edit ----
export AWS_PAGER=""                           # stops the CLI pager from looking like a "hang"
export AWS_DEFAULT_REGION="$REGION"
export SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-lambda-exec-role"
export GLUE_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-glue-exec-role"
```

**Running tasks (Steps 5–6) needs almost nothing to type.** The fleet is triggered with
`{"bucket","inputPrefix"}` and reads everything else from S3:

- **`bucket`** — your pipeline bucket.
- **`inputPrefix`** — the folder that holds `fleet_tasks.csv` (normally `config`).
- **the CSV** — `fleet_tasks.csv` (`task_arn`, optional `task_suffix`, optional
  `adopt_existing_folder`).
- **the table lists** — each task's `table_manifest.csv`, staged in S3 (Step 5b).

Everything else — region, DSQL endpoint/user/database, Glue role, Glue connection, CDC engine — is
read at run time from `config/pipeline.json`, which every per-task run already reads. The fleet
reads that same file; it never writes it.

> <a id="load-values"></a>**Coming back in a new shell to run or cut over tasks?** You only need
> `BUCKET` and `AWS_PAGER=""` to trigger a fleet. If you also want `PROJECT` / `REGION` in the shell
> (handy for the watch and manual-recovery commands later), load them from `pipeline.json` (edit
> only the bucket):
>
> ```bash
> export AWS_PAGER="" BUCKET="my-migration-bucket"
> eval "$(aws s3 cp s3://$BUCKET/config/pipeline.json - | python3 -c '
> import json, sys
> c = json.load(sys.stdin)
> print("export PROJECT=" + c["project"])
> print("export REGION=" + c["region"])
> print("export AWS_DEFAULT_REGION=" + c["region"])')"
> echo "PROJECT=$PROJECT REGION=$REGION"
> ```

> <a id="params-csv"></a>**A single parameters CSV (`params.csv`).** Instead of hand-editing the
> export block above, you can keep every "export" value in one `params.csv` (one `parameter,value`
> per row) **next to `fleet_tasks.csv`** in S3, so nobody retypes an export block. Copy
> [`config/params.example.csv`](config/params.example.csv), fill it in, and upload it to
> `s3://<bucket>/<inputPrefix>/params.csv`. Two things read it, exactly the same way (the shared
> parser `lambdas/params_csv.py`):
>
> - **`tools/setup.sh`** reads it at one-time setup to build everything, including
>   `config/pipeline.json` ([Step 3c](#step-3c--pipeline-settings)).
> - **the fleet's preflight** reads it at run time and, when it is safe (startup only, nothing
>   running), publishes `config/pipeline.json` from it (the [safe-publish rule](#step-3c--pipeline-settings)).
>
> The **BUCKET is deliberately not a key** in `params.csv`: it is the bucket the CSV itself lives in
> (the fleet's `bucket` input / `setup.sh`'s bucket), so it can't disagree with where everything is
> read from. If you prefer the hand-typed export block, it still works unchanged — `params.csv` is
> optional.
>
> **params.csv is offline-tested; real-AWS test pending.** The parser, the setup-script dry-run and
> the preflight safe-publish logic all pass an offline test suite (fake S3 / Step Functions / DMS);
> they have **not** yet been run against live AWS. Run one small live fleet first.

---

## The S3 layout

One bucket, fixed folder names (already baked into the templates):

```
s3://$BUCKET/
├── scripts/                  # the 4 Glue scripts                              (Step 3a)
├── glue-templates/           # the 6 Glue job templates                        (Step 3a)
├── driver-fullload/          # pg8000 stack only — Spark discovery + load      (Step 3b)
├── driver-validation/        # pg8000 stack only — Spark validate              (Step 3b)
├── driver-cdc/               # pg8000 stack + boto3 set for Python 3.9 (CDC)   (Step 3b)
├── driver-cdc-prepared/      # written by the startup: install-ready copies of driver-cdc/
├── <schema>/<table>/         # written by DMS (under the endpoint's BucketFolder, if any)
└── config/
    ├── pipeline.json         # settings read by every run and by the fleet     (Step 3c)
    ├── fleet_tasks.csv       # the task list the fleet reads                    (Step 5)
    ├── _task_index/          # written by the startup: task ARN -> folder name
    └── _task/<task name>/    # one folder per DMS task
        ├── table_manifest.csv        # you upload this                         (Step 5b)
        ├── _task.json                # written by the startup: which task ARN owns the folder
        ├── _manifest_index.json      # written by discovery
        ├── _orchestrator/group-<n>/  # per-group load status and validation report
        ├── _cdc_started/             # written by the CDC job when it reaches its poll loop
        └── _cdc_engine.json          # only if this task's CDC job was switched to Spark
```

---

## Step 1 — create the IAM roles

*One-time, about 15 minutes. Needs the export block.*

**Goal:** every role the pipeline runs as — the three core roles (Glue, Lambda, Step Functions)
**and** the three fleet roles (the preflight Lambda's role and the two fleet state-machine roles).
The fleet is always part of the deployment, so its roles are created here alongside the others.

The files in `iam/` contain blanks (`<<REGION>>`, `<<ACCOUNT_ID>>`, `<<BUCKET>>`,
`<<DSQL_CLUSTER_ID>>`, `<<PROJECT>>`, `<<GLUE_EXEC_ROLE_NAME>>`). **Fill them in first:** a role
created from an unfilled file would trust or allow a literal `<<...>>` string. The loop writes
filled copies (`iam/*.filled.json`) and leaves the originals untouched, so a later `git pull`
never conflicts.

```bash
# 1. Fill in the blanks for EVERY iam file (core + fleet). Portable: no sed -i.
for f in iam/*.json; do
  case "$f" in *.filled.json) continue ;; esac
  sed -e "s|<<REGION>>|$REGION|g" -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" \
      -e "s|<<BUCKET>>|$BUCKET|g" -e "s|<<DSQL_CLUSTER_ID>>|$DSQL_CLUSTER_ID|g" \
      -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<GLUE_EXEC_ROLE_NAME>>|$PROJECT-glue-exec-role|g" "$f" > "${f%.json}.filled.json"
done
grep -l "<<" iam/*.filled.json && echo "STOP: a placeholder is still unfilled" \
                               || echo "no placeholders left in any iam file"

# 2. The three core roles (Glue, Lambda, Step Functions) and their policies
for r in glue lambda sfn; do
  aws iam create-role --role-name "$PROJECT-$r-exec-role" \
    --assume-role-policy-document "file://iam/$r-exec-role.trust.filled.json" \
    --query Role.RoleName --output text
  aws iam put-role-policy --role-name "$PROJECT-$r-exec-role" \
    --policy-name "$r" --policy-document "file://iam/$r-exec-role.policy.filled.json"
done

# 3. The preflight-tasks role (the fleet's eighth Lambda runs as this; its trust file needs no fill)
aws iam create-role --role-name "$PROJECT-preflight-tasks-role" \
  --assume-role-policy-document file://iam/preflight-tasks-role.trust.json \
  --query Role.RoleName --output text
aws iam put-role-policy --role-name "$PROJECT-preflight-tasks-role" \
  --policy-name preflight --policy-document file://iam/preflight-tasks-role.policy.filled.json

# 4. The two fleet state-machine roles (fleet-startup, fleet-cutover)
for w in startup cutover; do
  aws iam create-role --role-name "$PROJECT-fleet-$w-role" \
    --assume-role-policy-document "file://iam/fleet-$w-role.trust.filled.json" \
    --query Role.RoleName --output text
  aws iam put-role-policy --role-name "$PROJECT-fleet-$w-role" \
    --policy-name fleet --policy-document "file://iam/fleet-$w-role.policy.filled.json"
done

# 5. VPC only (GLUE_CONNECTION is not ""): let Glue make network interfaces in your subnet
aws iam put-role-policy --role-name "$PROJECT-glue-exec-role" \
  --policy-name glue-vpc --policy-document file://iam/glue-exec-role.vpc-addon.policy.filled.json
```

**Verify:** all six roles exist:
`for r in glue-exec lambda-exec sfn-exec preflight-tasks fleet-startup fleet-cutover; do aws iam get-role --role-name "$PROJECT-$r-role" --query Role.RoleName --output text 2>/dev/null || echo "MISSING: $PROJECT-$r-role"; done`

> **Already created a role from an unfilled file?** Run part 1 again, then
> `aws iam update-assume-role-policy --role-name "$PROJECT-<role>" --policy-document file://iam/<role>.trust.filled.json`
> for the affected role, and run its `put-role-policy` again — it overwrites.

---

## Step 1b — create the Glue network connection (VPC only)

*Skip this step entirely if `GLUE_CONNECTION` is `""`.*

**Goal:** let the Glue jobs run inside your VPC so they can reach DSQL. A Glue job joins a VPC
through a **Glue network connection**; the pipeline attaches the one you name here to every job it
creates.

```bash
# Glue needs the security group to allow all TCP from itself (Spark workers talk to each other)
aws ec2 authorize-security-group-ingress --group-id "$SECURITY_GROUP_ID" --protocol tcp \
  --port 0-65535 --source-group "$SECURITY_GROUP_ID" 2>/dev/null || echo "self-ingress rule already present"

AZ=$(aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" \
  --query "Subnets[0].AvailabilityZone" --output text)

aws glue create-connection --connection-input "{
  \"Name\": \"$GLUE_CONNECTION\",
  \"ConnectionType\": \"NETWORK\",
  \"ConnectionProperties\": {},
  \"PhysicalConnectionRequirements\": {
    \"SubnetId\": \"$SUBNET_ID\",
    \"SecurityGroupIdList\": [\"$SECURITY_GROUP_ID\"],
    \"AvailabilityZone\": \"$AZ\"
  }
}"
```

**Verify:**
`aws glue get-connection --name "$GLUE_CONNECTION" --query Connection.PhysicalConnectionRequirements`
shows your subnet and security group.

**The subnet also needs:**

- an **S3 gateway endpoint** in its route table (jobs read scripts, wheels and CSVs from S3);
- a route to DSQL: a DSQL VPC endpoint with **private DNS on** (its security group allowing inbound
  5432 from `$SECURITY_GROUP_ID`), or NAT;
- optional, for the CDC job only: reach to the **DMS API** (so a renamed column is detected) and
  **CloudWatch** (one metric), via VPC endpoints or NAT. Without them CDC still applies every
  change — each call just gives up after a few seconds and the log says once what is turned off.

Don't add the connection to a job in the Glue console: the pipeline rewrites each job's whole
definition on every run, so there is nothing to edit there by hand. Put the name in
`pipeline.json` instead (Step 3c).

---

## Step 2 — create the Lambda functions

*One-time, about 10 minutes. Re-run it whenever `lambdas/` changes.*

**Goal:** the **eight** small functions the state machines call — the seven the per-task `startup`
and `cutover` use, plus the fleet's **`preflight-tasks`**. They all use **one zip**; the seven core
functions share the **Lambda role** from Step 1, and `preflight-tasks` runs on its own
**preflight-tasks role** (also from Step 1).

| Function | Handler | Runs as | What it does |
|---|---|---|---|
| `$PROJECT-resolve-task` | `resolve_task.handler` | lambda-exec-role | reads `config/pipeline.json` and the DMS task; works out the task's folder, job names and S3 layout; checks the task before DMS starts; records the folder owner |
| `$PROJECT-driver-discovery` | `driver_discovery.handler` | lambda-exec-role | checks the `driver-*` folders; prepares the `driver-cdc/` wheels for a Python-shell CDC job |
| `$PROJECT-plan-split` | `plan_split.handler` | lambda-exec-role | splits the task's tables into balanced load groups |
| `$PROJECT-create-glue-jobs` | `create_glue_jobs.handler` | lambda-exec-role | creates (and at cutover deletes) the task's Glue jobs; re-creates the CDC job as Spark on a driver failure |
| `$PROJECT-stop-cdc-run` | `stop_cdc_run.handler` | lambda-exec-role | stops this task's CDC run at cutover |
| `$PROJECT-drain-check` | `drain_check.handler` | lambda-exec-role | **connects to DSQL:** waits until the last CDC file is applied |
| `$PROJECT-drop-tags` | `drop_tags.handler` | lambda-exec-role | **connects to DSQL:** drops the internal `_cdc_file` tracking column at cutover |
| `$PROJECT-preflight-tasks` | `preflight_tasks.handler` | preflight-tasks-role | the fleet's first step: reads `fleet_tasks.csv` and checks every task before any per-task run starts (reuses `resolve_task`'s rules) |

**1. Build the zip.** It holds every `.py` from `lambdas/` (including `preflight_tasks.py`) plus the
**`pg8000`** library, which `drain-check` and `drop-tags` import to connect to DSQL. pg8000 is pure
Python, so building it on any machine is fine. It is installed into a separate build folder so the
repo's `lambdas/` is never touched.

```bash
rm -rf _lambda_build fn.zip && mkdir _lambda_build
cp lambdas/*.py _lambda_build/
python3 -m pip install pg8000 -t _lambda_build/ --quiet
(cd _lambda_build && zip -qr ../fn.zip .)
unzip -l fn.zip | grep -cE ' (resolve_task\.py|prepare_cdc_wheels\.py|preflight_tasks\.py|pg8000/__init__\.py)$'  # must print 4
```

The count must be **4** (both key scripts, the preflight Lambda and pg8000 are all in the zip). If
it is not, don't deploy.

**2. Create the seven core functions, or update them if they already exist** (safe to re-run):

```bash
for spec in resolve-task:resolve_task driver-discovery:driver_discovery plan-split:plan_split \
            create-glue-jobs:create_glue_jobs stop-cdc-run:stop_cdc_run \
            drain-check:drain_check drop-tags:drop_tags; do
  NAME="$PROJECT-${spec%%:*}"; HANDLER="${spec##*:}.handler"
  if aws lambda get-function --function-name "$NAME" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$NAME" --zip-file fileb://fn.zip \
      --query FunctionName --output text
    aws lambda wait function-updated --function-name "$NAME"
    aws lambda update-function-configuration --function-name "$NAME" --handler "$HANDLER" \
      --runtime python3.12 --memory-size 1024 --timeout 300 --query FunctionName --output text
    aws lambda wait function-updated --function-name "$NAME"
  else
    aws lambda create-function --function-name "$NAME" --zip-file fileb://fn.zip \
      --handler "$HANDLER" --runtime python3.12 --memory-size 1024 --timeout 300 \
      --role "$LAMBDA_ROLE_ARN" --query FunctionName --output text
    aws lambda wait function-active-v2 --function-name "$NAME"
  fi
done
```

If `create-function` says *"The role defined for the function cannot be assumed by Lambda"*, the
role from Step 1 is a few seconds old — wait 10 seconds and run the loop again.

**3. Create (or update) the fleet's `preflight-tasks` function** on the preflight role (same zip):

```bash
NAME="$PROJECT-preflight-tasks"
if aws lambda get-function --function-name "$NAME" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$NAME" --zip-file fileb://fn.zip \
    --query FunctionName --output text
  aws lambda wait function-updated --function-name "$NAME"
  aws lambda update-function-configuration --function-name "$NAME" \
    --handler preflight_tasks.handler --runtime python3.12 --memory-size 256 --timeout 300 \
    --query FunctionName --output text
  aws lambda wait function-updated --function-name "$NAME"
else
  aws lambda create-function --function-name "$NAME" --zip-file fileb://fn.zip \
    --handler preflight_tasks.handler --runtime python3.12 --memory-size 256 --timeout 300 \
    --role "arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-preflight-tasks-role" \
    --query FunctionName --output text
  aws lambda wait function-active-v2 --function-name "$NAME"
fi
```

**4. VPC only:** `drain-check` and `drop-tags` need the same network path as Glue:

```bash
aws iam attach-role-policy --role-name "$PROJECT-lambda-exec-role" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
for n in drain-check drop-tags; do
  aws lambda update-function-configuration --function-name "$PROJECT-$n" \
    --vpc-config "SubnetIds=$SUBNET_ID,SecurityGroupIds=$SECURITY_GROUP_ID" --query FunctionName --output text
  aws lambda wait function-updated --function-name "$PROJECT-$n"
done
```

**Verify:** all eight are listed:
`aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table`

> **Using the console instead?** For each of the seven core functions: **Author from scratch**,
> runtime **Python 3.12**, **Use an existing role** → `$PROJECT-lambda-exec-role`; upload the same
> `fn.zip`; set the handler from the table; set **Memory 1024 MB** and **Timeout 5 min**. For
> `preflight-tasks`, use `$PROJECT-preflight-tasks-role`, handler `preflight_tasks.handler`, **Memory
> 256 MB**, **Timeout 5 min**. The 1024 MB / 300 s matters for `driver-discovery` (preparing the CDC
> wheels) — the error message points back to this step.

---

## Step 3a — scripts and job templates

*One-time, about 5 minutes. Re-run it whenever `scripts/` or `glue-templates/` changes — the Glue
scripts in S3 are what the jobs actually run.*

```bash
# The 4 Glue scripts
for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py; do
  aws s3 cp "scripts/$f" "s3://$BUCKET/scripts/$f"
done

# The 6 job templates, with <<BUCKET>> filled in. Filled copies go to _filled/; the repo files
# are left alone (create-glue-jobs also fills <<BUCKET>> at run time, so this is belt-and-braces).
mkdir -p _filled/glue-templates
for f in glue-templates/*.json; do sed -e "s|<<BUCKET>>|$BUCKET|g" "$f" > "_filled/$f"; done
grep -l "<<" _filled/glue-templates/*.json || echo "no placeholders left in templates"
aws s3 cp _filled/glue-templates/ "s3://$BUCKET/glue-templates/" --recursive \
  --exclude "*" --include "*.json"
```

**Verify** that S3 holds the same scripts as your repo (any mismatch means a job would run an old
script):

```bash
for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py; do
  if command -v sha256sum >/dev/null 2>&1; then H="sha256sum"; else H="shasum -a 256"; fi
  L=$($H < "scripts/$f" | cut -c1-16)
  S=$(aws s3 cp "s3://$BUCKET/scripts/$f" - | $H | cut -c1-16)
  [ "$L" = "$S" ] && echo "OK    $f" || echo "STALE $f  (repo $L, S3 $S)"
done
```

Every line must say `OK`. (`sha256sum` is on Linux and CloudShell; macOS uses `shasum -a 256` — the
line above picks whichever exists.)

---

## Step 3b — driver wheels

*One-time, about 10 minutes.*

The Glue jobs can't reach PyPI (a locked-down VPC blocks it), so their Python libraries are staged
in S3 as `.whl` files, in **three** folders:

| Folder | Used by | Put in it | Never put in it |
|---|---|---|---|
| `driver-fullload/` | discovery + load (Spark, Python 3.10) | the **pg8000 stack** (5 wheels) | boto3 / botocore |
| `driver-validation/` | validate (Spark, Python 3.10) | the **pg8000 stack** (5 wheels) | boto3 / botocore |
| `driver-cdc/` | CDC (Python **shell**, Python 3.9) | the pg8000 stack **and** the boto3 set, built for Python 3.9 (10 wheels) | two versions of any package |

- **pg8000 stack (5):** `pg8000`, `scramp`, `asn1crypto`, `python_dateutil`, `six`
- **boto3 set (5):** `boto3`, `botocore`, `jmespath`, `s3transfer`, `urllib3`

Why the split matters:

- A boto3/botocore wheel in `driver-fullload/` or `driver-validation/` breaks the Spark jobs with
  `DataNotFoundError: endpoints`. The Spark jobs get boto3 a different way, from `driver-cdc/`.
- Glue Python shell runs **Python 3.9**. boto3/botocore 1.43+ and scramp 1.4.7+ need 3.10, and on
  3.9 botocore needs urllib3 below 1.27. The download below pins those.
- You **don't** prepare the CDC wheels by hand. Before DMS starts, the startup checks `driver-cdc/`
  for Python 3.9, then writes install-ready copies to `driver-cdc-prepared/<fingerprint>/` (with a
  `MANIFEST.txt` of every change, for your security team), once per wheel set. Your files in
  `driver-cdc/` are never changed.

**Download** on a machine that can reach PyPI (your laptop or CloudShell). The flags pin **Glue's**
platform and Python, not your machine's:

```bash
rm -rf _drv _cdc
PLAT310="--platform manylinux2014_x86_64 --python-version 310 --only-binary=:all:"
PLAT39="--platform manylinux2014_x86_64 --python-version 39 --only-binary=:all:"

# Spark jobs (Python 3.10): the pg8000 stack
python3 -m pip download pg8000 $PLAT310 -d _drv/

# CDC job (Python 3.9): pg8000 stack + boto3 set, capped to releases that still support 3.9
python3 -m pip download "pg8000>=1.31,<1.32" "scramp>=1.4.5,<1.4.7" \
  "boto3>=1.35,<1.43" "botocore>=1.35,<1.43" "urllib3>=1.25.4,<1.27" $PLAT39 -d _cdc/
```

**Upload.** `driver-cdc/` is cleared first, because two versions of one package there is an error:

```bash
aws s3 cp _drv/ "s3://$BUCKET/driver-fullload/"   --recursive --exclude "*" --include "*.whl"
aws s3 cp _drv/ "s3://$BUCKET/driver-validation/" --recursive --exclude "*" --include "*.whl"
aws s3 rm "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
aws s3 cp _cdc/ "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
```

**Verify** (and run the same check the startup runs, locally — standard Python only, no AWS):

```bash
aws s3 ls "s3://$BUCKET/driver-fullload/"     # 5 wheels, no boto3/botocore
aws s3 ls "s3://$BUCKET/driver-validation/"   # 5 wheels, no boto3/botocore
aws s3 ls "s3://$BUCKET/driver-cdc/"          # 10 wheels, one version per package
rm -rf /tmp/_cdc_check && python3 lambdas/prepare_cdc_wheels.py _cdc/ /tmp/_cdc_check/ --python 3.9
```

The last command must end with **`PASS`**.

---

## Step 3c — pipeline settings

*One-time, about 5 minutes.*

Every per-task `startup` and `cutover` run — and the fleet's preflight — reads
`s3://$BUCKET/config/pipeline.json`. An edit applies to runs started **after** it, not to runs
already going.

> **params.csv is offline-tested; real-AWS test pending.** The `params.csv` → `pipeline.json` flow
> below (the parser, `tools/setup.sh --dry-run` and the fleet's safe-publish) passes an offline test
> suite only; it has **not** been run against live AWS yet. Run one small live fleet first.

**The recommended way: one `params.csv`, built by `tools/setup.sh`.** Put every "export" value in a
single `params.csv` and let setup build `pipeline.json` (and everything else) from it, so nobody
retypes an export block.

1. Copy the example and fill it in:

   ```bash
   cp config/params.example.csv params.csv
   # edit params.csv: set account_id, region, project, dsql_endpoint (and any optional keys)
   ```

2. Upload it next to `fleet_tasks.csv` in S3:

   ```bash
   aws s3 cp params.csv "s3://$BUCKET/config/params.csv"
   ```

3. Run the one-command setup (idempotent — safe to re-run). It can read the CSV straight from S3:

   ```bash
   tools/setup.sh s3://$BUCKET/config/params.csv [--with-drivers] [--dry-run]
   ```

   or from a local path (then pass the bucket, because the CSV never names it):

   ```bash
   tools/setup.sh params.csv --bucket "$BUCKET" [--with-drivers] [--dry-run]
   ```

`setup.sh` does Steps 1–4 in one pass (create-or-update, so re-running only fixes drift):

- fills `iam/*.json` and creates/updates **all 6 roles** (glue, lambda, sfn, preflight-tasks,
  fleet-startup, fleet-cutover);
- creates the Glue network connection when `subnet_id`/`security_group_id` are set (Step 1b);
- builds `fn.zip` (every `lambdas/*.py` + pg8000) and creates/updates **all 8 Lambdas**;
- uploads the 4 Glue scripts and the 6 Glue job templates; with `--with-drivers`, also stages the
  driver wheels (Step 3b);
- **builds `config/pipeline.json` from `params.csv` and publishes it** under the safe-publish rule
  below;
- creates/updates **all 4 state machines** (startup, cutover, fleet-startup, fleet-cutover).

`--dry-run` prints every AWS command without running any (no AWS calls). Note: `--dry-run` with an
`s3://…/params.csv` path can't read the CSV offline — download it first and pass the local path with
`--bucket`. A dry-run against a **local** CSV is fully offline.

### What goes in `params.csv`

Header must be exactly `parameter,value`; one row per key; blank lines and lines starting with `#`
are ignored; values are trimmed; a duplicate or unknown key is an error. `dsql_cluster_id` is
**derived** (first label of `dsql_endpoint`) and must not be listed. `glue_role_arn` defaults to
`arn:aws:iam::<account_id>:role/<project>-glue-exec-role` when omitted.

| Parameter | Required? | Default | Meaning |
|---|---|---|---|
| `account_id` | **required** | — | 12-digit AWS account id (setup/IAM only; never written to `pipeline.json`) |
| `region` | **required** | — | AWS region of the DMS tasks and pipeline (must equal the task ARN's region) |
| `project` | **required** | — | short prefix (letters, digits, hyphens) for role, Lambda and job names |
| `dsql_endpoint` | **required** | — | Aurora DSQL endpoint, e.g. `<cluster>.dsql.<region>.on.aws` |
| `dsql_user` | optional | `admin` | DSQL user |
| `dsql_database` | optional | `postgres` | DSQL database |
| `glue_connection` | optional | `""` (no VPC) | the Glue network connection's **exact** name; `""` = Glue runs with no VPC connection |
| `cdc_engine` | optional | `pythonshell` | `pythonshell` (1 DPU) or `spark` (Glue 4.0, 2 × G.1X); `GlueETL`/`pyspark` normalise to `spark` |
| `cdc_spark_fallback` | optional | `true` | `true`: on a Python-shell CDC driver failure the startup re-creates that task's CDC job as Spark; `false`: stop at `DriversFailed` / `CdcRunFailed` |
| `control_schema` | optional | `cdc_control` | DSQL schema for the CDC control tables |
| `glue_role_arn` | optional | derived (see above) | the Glue role from Step 1; set only if your Glue role name differs from `<project>-glue-exec-role` |
| `subnet_id` | optional (setup-only) | — | private subnet for the Glue VPC connection (Step 1b). Set **both** `subnet_id` and `security_group_id`, or neither. Not written to `pipeline.json` |
| `security_group_id` | optional (setup-only) | — | security group for the Glue VPC connection. Both-or-neither with `subnet_id`. Not written to `pipeline.json` |

The ten keys that end up in `pipeline.json` are `project`, `region`, `dsql_endpoint`, `dsql_user`,
`dsql_database`, `glue_role_arn`, `glue_connection`, `cdc_engine`, `cdc_spark_fallback`,
`control_schema`. `account_id`, `subnet_id` and `security_group_id` are used only by setup and are
never written into `pipeline.json`.

### The safe-publish rule (how `pipeline.json` is published from `params.csv`)

Both `setup.sh` and the fleet's preflight apply the **same** rule when `params.csv` is present. It
is deliberately conservative — it never changes settings out from under a run:

- **Preflight validates `params.csv` first.** Any parse/validation problem (missing required key,
  12-digit `account_id`, both-or-neither VPC pair, an unknown/duplicate key, a value still holding
  `<`/`>`, the project not matching the fleet's state machine) → `PreflightFailed`, nothing written,
  nothing started.
- **If the candidate equals the live `config/pipeline.json`,** nothing is written
  (`paramsPublished=false`).
- **If it differs and it's a startup with nothing running,** the live `pipeline.json` is backed up
  to a dated key `config/pipeline.json.<UTC>` (only when one already exists), the new file is
  published, then read back and verified (`paramsPublished=true`).
- **If anything is running — any `startup`, `cutover`, `fleet-startup` or `fleet-cutover` execution
  (other than this fleet run) — or it's a cutover,** it stops with `PreflightFailed` naming what's
  running (cutover never publishes). Publish new settings with a startup fleet (or by hand) first,
  then cut over.
- **`params.csv` and a second `<inputPrefix>/pipeline.json` together** → `PreflightFailed` (ambiguous
  — the CSV builds `config/pipeline.json` for you, so remove the local copy).
- **If executions can't be listed** (e.g. the preflight role lacks `states:ListExecutions`), it
  **fails closed**: `PreflightFailed`, nothing written.
- **No `params.csv`** → exactly today's behaviour (the fleet reads the existing `config/pipeline.json`
  and the old local-vs-canonical guard is unchanged).

### Fallback: write `pipeline.json` by hand

If you prefer not to use `params.csv`, you can still write `pipeline.json` from the export block.
Don't hand-copy `config/pipeline.example.json` (see the note below):

```bash
: "${PROJECT:?}" "${REGION:?}" "${DSQL_ENDPOINT:?}" "${GLUE_ROLE_ARN:?}" "${GLUE_CONNECTION?}"
python3 - <<'EOF'
import json, os
cfg = {
    "project": os.environ["PROJECT"],
    "region": os.environ["REGION"],
    "dsql_endpoint": os.environ["DSQL_ENDPOINT"],
    "dsql_user": os.environ.get("DSQL_USER") or "admin",
    "dsql_database": os.environ.get("DSQL_DATABASE") or "postgres",
    "glue_role_arn": os.environ["GLUE_ROLE_ARN"],
    "glue_connection": os.environ.get("GLUE_CONNECTION", ""),
    "cdc_engine": "pythonshell",
    "cdc_spark_fallback": True,
    "control_schema": "cdc_control",
}
open("pipeline.json", "w").write(json.dumps(cfg, indent=2) + "\n")
print(json.dumps(cfg, indent=2))
EOF
aws s3 cp pipeline.json "s3://$BUCKET/config/pipeline.json"
```

> **Don't copy `config/pipeline.example.json` as-is.** Its `description` line contains `<bucket>`,
> and the pipeline rejects any value with `<` or `>` in it, so a run would fail at `ResolveFailed`
> ([Known issues](#known-issues-temporary)). The generator above omits `description`, so it is safe
> (and `params.csv` never emits `description` at all). Keep hand-edited values trimmed (no stray
> spaces) and never set `dsql_user`, `dsql_database` or `control_schema` to an empty string — a
> blank there is kept, not defaulted, and fails later.

Before changing a live file by hand, keep a dated copy (setup.sh and the fleet do this for you):
`aws s3 cp "s3://$BUCKET/config/pipeline.json" "s3://$BUCKET/config/pipeline.json.$(date +%Y%m%d%H%M)"`

---

## Step 4 — create the state machines

*One-time, about 10 minutes. Re-run it whenever `stepfunctions/` changes.*

**Goal:** all four state machines:

- the per-task **`startup`** (full load → validate → start CDC) and **`cutover`** (drain the last
  changes and clean up), shared by every task. **The fleet starts these; you never start them
  yourself.**
- the **`fleet-startup`** and **`fleet-cutover`** launchers you *do* trigger (Steps 5–6). Each
  reads `fleet_tasks.csv`, checks every task, and starts the matching per-task run for each row.

Create them in that order (a fleet machine refers to the per-task machine it launches).

**1. Fill in the per-task machines' blanks** — `<<BUCKET>>` and the seven Lambda ARNs. Everything
else is read at run time from `pipeline.json` and the DMS task.

```bash
: "${PROJECT:?}" "${REGION:?}" "${ACCOUNT_ID:?}" "${BUCKET:?}"
LAMBDA_BASE="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT"
for f in startup cutover; do
  sed -e "s|<<BUCKET>>|$BUCKET|g" \
      -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
      -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
      -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
      -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
      -e "s|<<STOP_CDC_RUN_LAMBDA_ARN>>|$LAMBDA_BASE-stop-cdc-run|g" \
      -e "s|<<DRAIN_CHECK_LAMBDA_ARN>>|$LAMBDA_BASE-drain-check|g" \
      -e "s|<<DROP_TAGS_LAMBDA_ARN>>|$LAMBDA_BASE-drop-tags|g" \
      "stepfunctions/$f.asl.json" > "$f.filled.asl.json"
done
grep "<<" startup.filled.asl.json cutover.filled.asl.json || echo "no placeholders left (per-task)"
```

**2. Fill in the fleet machines' blanks** — `<<PROJECT>>`, the preflight Lambda ARN and the two
per-task state-machine ARNs:

```bash
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
for w in startup cutover; do
  sed -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT-preflight-tasks|g" \
      -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM:$PROJECT-startup|g" \
      -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM:$PROJECT-cutover|g" \
      "stepfunctions/fleet-$w.asl.json" > "fleet-$w.filled.asl.json"
done
grep "<<" fleet-startup.filled.asl.json fleet-cutover.filled.asl.json || echo "no placeholders left (fleet)"
```

**3. Create the two per-task machines, or update them if they exist** (safe to re-run). They run on
the Step Functions role from Step 1:

```bash
for f in startup cutover; do
  ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?name=='$PROJECT-$f'].stateMachineArn" --output text)
  if [ -n "$ARN" ]; then
    aws stepfunctions update-state-machine --state-machine-arn "$ARN" \
      --definition "file://$f.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  else
    aws stepfunctions create-state-machine --name "$PROJECT-$f" \
      --definition "file://$f.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  fi
done
```

**4. Create the two fleet machines, or update them if they exist.** Each runs on its own fleet role
from Step 1:

```bash
for w in startup cutover; do
  ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?name=='$PROJECT-fleet-$w'].stateMachineArn" --output text)
  if [ -n "$ARN" ]; then
    aws stepfunctions update-state-machine --state-machine-arn "$ARN" \
      --definition "file://fleet-$w.filled.asl.json" \
      --role-arn "arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-fleet-$w-role"
  else
    aws stepfunctions create-state-machine --name "$PROJECT-fleet-$w" \
      --definition "file://fleet-$w.filled.asl.json" \
      --role-arn "arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-fleet-$w-role"
  fi
done
```

**Verify:** all four names are listed:
`aws stepfunctions list-state-machines --query "stateMachines[?starts_with(name,'$PROJECT-')].name" --output table`

> **Did Steps 1–3 in the console?** Set `PROJECT`, `REGION`, `ACCOUNT_ID`, `BUCKET` and
> `SFN_ROLE_ARN` to match what you created, then confirm every Lambda exists before this step:
> `for n in resolve-task driver-discovery plan-split create-glue-jobs stop-cdc-run drain-check drop-tags preflight-tasks; do aws lambda get-function --function-name "$PROJECT-$n" --query Configuration.FunctionName --output text 2>/dev/null || echo "MISSING: $PROJECT-$n"; done`

Setup is complete. From here on you only ever trigger `fleet-startup` and `fleet-cutover`.

---

## Step 5 — run tasks with the fleet

*Per wave of tasks. Steps 1–4 don't change. The fleet is the only way to start a task — there is
no "start one by hand" path. **One task is just one row in `fleet_tasks.csv`;** many tasks are many
rows.*

In a fresh shell, load `BUCKET` and `AWS_PAGER=""` ([Coming back in a new shell](#load-values)).

### 5a — list the tasks

Trigger a fleet with `{"bucket","inputPrefix"}` only. The DMS task ARNs go in the CSV, not on the
command line.

### 5b — upload each task's table list

For **every** task you'll list, stage its table list at
`s3://$BUCKET/config/_task/<task name>/table_manifest.csv`. `<task name>` is the DMS task's name
(or the `task_suffix` you give it in the CSV). A CSV with a header row and two columns,
`dms_schema,dms_table` — each table **as DMS writes it to S3** (i.e. after any rename in the DMS
table mapping). Letter case doesn't matter: discovery finds DMS's folder in any case, and the DSQL
target is the same names in lowercase.

```csv
dms_schema,dms_table
TARGET_SCHEMA,MY_TABLE
target_schema,another_table
```

```bash
# repeat per task; TASK_NAME is the DMS task name (or its task_suffix)
aws s3 cp table_manifest.csv "s3://$BUCKET/config/_task/<TASK_NAME>/table_manifest.csv"
```

A table with no folder yet (no rows at full load) is loaded empty, with a warning; the CDC job
picks it up when DMS creates the folder. If **none** of a task's tables has a folder, that task's
discovery fails and lists the folders it did find, so a wrong name can't quietly load nothing.
Preflight refuses to start a task whose `table_manifest.csv` is missing or empty.

### 5c — write `fleet_tasks.csv`

One row per DMS task. Header `task_arn,task_suffix,adopt_existing_folder`:

- **`task_arn`** (required) — the DMS task ARN.
- **`task_suffix`** (optional) — leave blank to use the folder the pipeline would pick anyway: the
  one recorded for this task (a renamed DMS task keeps its first folder), else the DMS task name.
  Set it only when you want a different folder and job-name stem than the DMS task's name.
- **`adopt_existing_folder`** (optional, startup only) — `true` for a task whose folder holds files
  from a run made before the shared state machines existed but has no owner record.

A **single task** is one data row:

```csv
task_arn,task_suffix,adopt_existing_folder
arn:aws:dms:us-east-1:123456789012:task:ABCDEF1234567890,,
```

**Many tasks** are more rows (example in
[`config/fleet_tasks.example.csv`](config/fleet_tasks.example.csv)):

```csv
task_arn,task_suffix,adopt_existing_folder
arn:aws:dms:us-east-1:123456789012:task:ABCDEF1234567890,,
arn:aws:dms:us-east-1:123456789012:task:GHIJKL0987654321,orders-cdc,
arn:aws:dms:us-east-1:123456789012:task:MNOPQR1122334455,,true
```

### 5d — start the fleet

Upload the list and trigger `fleet-startup`:

```bash
aws s3 cp fleet_tasks.csv "s3://$BUCKET/config/fleet_tasks.csv"
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
aws stepfunctions start-execution --state-machine-arn "$SM:$PROJECT-fleet-startup" \
  --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}"
```

`inputPrefix` is the folder holding `fleet_tasks.csv` (normally `config`). To use another file
name, add `"tasksFile":"wave2.csv"` to the input. The fleet reads `config/pipeline.json` (the same
file every task uses — it never writes it) and `config/<inputPrefix>/fleet_tasks.csv`.

The fleet first runs **preflight** (checks every task), then starts one `$PROJECT-startup`
execution per task, **5 at a time**, and confirms each got past its own input checks.

### 5e — read the fleet result

The `fleet-startup` execution ends at one of:

| Ends at | Meaning | What to do |
|---|---|---|
| `FleetStarted` | every task was started (or skipped as already started), and each started one was still running 30 s later — i.e. past its own input checks. **It does not mean the migrations succeeded.** | watch each `$PROJECT-startup` child execution ([5f](#5f--watch-each-task)) |
| `FleetStartIncomplete` | at least one task did **not** start. The execution output's `results.tasks` lists each task with `status` (`started`, `skipped`, `not_started`) and, for `not_started`, the error or the child's status. The others were started | fix the listed tasks, trigger the fleet again with the same input — tasks already started are skipped (see below) |
| `PreflightFailed` | a problem was found **before anything started** — the shared settings, the task list, or one or more rows. The cause lists every problem by row. **Nothing was started** | fix every listed problem, then trigger the fleet again |
| `MissingFleetInput` | the input didn't have both `bucket` and `inputPrefix` as strings | start again with `{"bucket":"...","inputPrefix":"config"}` |

Preflight checks, for every row: the shared settings file (all of `resolve_task`'s settings
checks); a DMS task ARN in the pipeline's region, listed once, that exists; a folder name short
enough for the Glue job names; no two rows on the same folder; a folder not owned by a different
task. For a startup fleet it also runs each task's pre-start checks (`full-load-and-cdc`,
`StopTaskCachedChangesApplied=true`, `AddColumnName=true`, the pipeline bucket, not past its full
load), confirms each `table_manifest.csv` is staged and non-empty, and that the fleet's table lists
use at most 9 distinct DSQL schemas.

### 5f — watch each task

`FleetStarted` only means each per-task run got past its input checks. Watch each
`$PROJECT-startup` execution in the Step Functions console (the run name starts with the task
name). Each child does exactly what it would if you had started it with `{"taskArn":"..."}`:

1. **Check the task** (seconds): reads `pipeline.json` and checks the DMS task **before starting
   it** — `full-load-and-cdc`, `StopTaskCachedChangesApplied=true`, `AddColumnName=true`, writes to
   the pipeline bucket, same region, not already past its full load. Any problem → **`ResolveFailed`**
   with the reason; DMS is untouched. A second startup for the same task while one is running also
   stops here.
2. **Check the driver wheels** (seconds; about a minute the first time): for a Python-shell CDC
   job, checks and prepares `driver-cdc/` (Step 3b). A wrong or missing wheel → **`DriversFailed`**,
   naming it; DMS is untouched. With `cdc_spark_fallback` on, a problem that only affects Python
   shell (e.g. a wheel built for 3.10) doesn't stop the run: if boto3, botocore and s3transfer are
   usable, this task's CDC job is built as Spark instead.
3. **Start DMS and wait for the full load** (`STOPPED_AFTER_CACHED_EVENTS`; polled every 30 s for up
   to **24 h**). Then create this task's Glue jobs.
4. **Discovery, then load and validate** each table group. If any group fails → **`GroupsFailed`**,
   and DMS stays paused, so CDC never starts on an incomplete load.
5. **Resume DMS into CDC and start the CDC job.** It applies changes to a table only once that
   table's load is marked `done` (`config/_task/<task name>/_orchestrator/group-<n>/_load_status.json`).
6. **Confirm CDC started** (polled every 30 s for up to **45 min**): the run succeeds when the CDC
   job writes `_cdc_started/<startup execution name>.json` on reaching its poll loop. If the CDC run
   fails first → **`CdcRunFailed`** (with Glue's error); if it stops → **`CdcRunEnded`**; if it never
   confirms → **`CdcStartNotConfirmed`**.
7. **Spark fallback, once** (`cdc_spark_fallback` on, Python-shell CDC job): if the CDC run failed
   because Glue couldn't install or import its drivers (pip/`pypi.org` timeouts,
   `CalledProcessError`, a missing or wrong-Python wheel, `No module named 'pg8000'`,
   `Unknown service: 'dsql'`), the CDC job is re-created **with the same name** as Spark, started
   with the same arguments, and confirmed as in step 6. Any other error (DSQL, permissions, data)
   is not retried. The switch is recorded in `config/_task/<task name>/_cdc_engine.json`; later
   startups of this task build Spark straight away. Delete that file to go back to Python shell.

If a child run stops at any Fail state, go to
[If a run fails](#if-a-run-fails-how-to-continue) — what to do depends on **where** it stopped.

### 5g — trigger the fleet again after a partial failure

Trigger `fleet-startup` with the same input. Preflight **skips** tasks that are already under way,
so only the rest start:

- `already_running` — the task's `$PROJECT-startup` already has a RUNNING execution.
- `past_full_load` — the pipeline started this task before (it has an owner record) and DMS has
  finished its full load, i.e. its startup already ran through.

Even without the skip (for example if the preflight Lambda can't list executions — it then prints a
warning), the per-task `startup` refuses a second run of a task that is already running.

### 5h — check that CDC is applying (per task)

```bash
TASK_NAME="<task name>"               # the folder/job stem for this task
JOB="$PROJECT-$TASK_NAME-cdc"
RUN=$(aws glue get-job-runs --job-name "$JOB" --max-items 1 --query 'JobRuns[0].Id' --output text)
aws glue get-job-run --job-name "$JOB" --run-id "$RUN" --query 'JobRun.JobRunState'
if [ "$(aws glue get-job --job-name "$JOB" --query Job.Command.Name --output text)" = pythonshell ]; then
  LG="/aws-glue/python-jobs/output"; else LG="/aws-glue/jobs/output"; fi
aws logs tail "$LG" --log-stream-names "$RUN" --since 1h | grep -E "Full-load gate|poll loop|CANNOT" | tail -5
```

Healthy: `RUNNING`, then `Full-load gate: N/N table(s) marked 'done'` and `entering poll loop`.
To watch per table: `SELECT table_name, status, error FROM cdc_control.cdc_status;` (more queries
in `USAGE_GUIDE.md`).

---

## Step 6 — cut over with the fleet

*Per wave, when you're ready to switch the application to Aurora DSQL. Like startup, cutover runs
through the fleet only. Use the same `fleet_tasks.csv`, or a CSV of just the tasks you want to cut
over.*

### Before you start — checklist (per task in the CSV)

Do these **in order, for every task listed in the CSV you're about to cut over**. Cutover stops
DMS first, so any change made on the source after that is never migrated — getting this order
wrong loses data silently.

- [ ] **Stop writes to the source** for every table of every task in the CSV (put the application
      in maintenance mode or make the source read-only).
- [ ] **Let DMS deliver the last changes** for each task. In the DMS console, the task's
      **CDCLatencySource** and **CDCLatencyTarget** are near zero, and (if the endpoint batches)
      you have waited past its `CdcMaxBatchInterval` so the last change file has landed in S3.
- [ ] **No table is blocked** for any task:
      `SELECT table_name FROM cdc_control.cdc_status WHERE status='blocked';` returns nothing. If it
      doesn't, fix the cause and unblock ([Troubleshooting → CDC](#cdc)) before cutting over.
- [ ] **Each task's CDC run is RUNNING** (check as in [5h](#5h--check-that-cdc-is-applying-per-task)).
      Cutover does not verify this; if a run is not running, that task's cutover stops DMS and then
      waits the full drain budget (~12 h) before failing.
- [ ] **Multi-column-PK tables:** their separate CDC job is running and has caught up (it must mark
      their latest change files `done` in `cdc_control.cdc_file_status`).

**The fleet does not check readiness — it checks inputs.** The preflight for a cutover fleet only
confirms each task exists, is listed once, and was started by the pipeline (has an owner record).
Cutting over a task whose CDC has not caught up is on you.

### Start it

Use the same `fleet_tasks.csv`, or write a CSV with only the rows you want to cut over now (the
same three columns; `adopt_existing_folder` is ignored at cutover). Upload it and trigger
`fleet-cutover`:

```bash
aws s3 cp fleet_tasks.csv "s3://$BUCKET/config/fleet_tasks.csv"
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
aws stepfunctions start-execution --state-machine-arn "$SM:$PROJECT-fleet-cutover" \
  --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}"
```

To cut over a different list, point `tasksFile` at it (e.g. `"tasksFile":"wave2.csv"`). The fleet
runs preflight, then starts one `$PROJECT-cutover` execution per task, 5 at a time, and confirms
each got past its input checks. The result states are the same as a startup fleet
([5e](#5e--read-the-fleet-result)): `FleetStarted`, `FleetStartIncomplete`, `PreflightFailed`,
`MissingFleetInput` — again, `FleetStarted` means each cutover **started**, not that it finished;
watch each child.

> **A cutover fleet re-triggers cutover for EVERY task in the CSV — including ones that have
> already cut over.** Unlike the startup fleet (which skips tasks past their full load), the cutover
> preflight has no "already cut over" skip: it skips only a task whose `$PROJECT-cutover` is
> *currently running* (`already_running`). A task that finished its cutover earlier has no such
> guard, so re-running `fleet-cutover` with it still in the CSV starts a **fresh** cutover for it —
> which then fails, because [cutover can't be re-run once DMS is stopped](#known-issues-temporary).
> **Remove already-cut-over tasks from the CSV before triggering `fleet-cutover` again**, or use a
> CSV of just the tasks still to cut over.

### What each per-task cutover does

Each `$PROJECT-cutover` child, for its one task: stops the DMS task (up to ~1 h to reach
`stopped`), waits until each table's last CDC file is applied (`DrainCheck`; up to ~12 h), stops
this task's CDC run, drops the internal `_cdc_file` column, and deletes this task's five Glue jobs.
It finds the task by its ARN, so a renamed task still cuts over its original folder and jobs. Each
child ends at one of:

| Ends at | Meaning | What to do |
|---|---|---|
| `CutoverSucceeded` | done | point the application at Aurora DSQL |
| `GlueJobsNotDeleted` | the data is cut over; only deleting a Glue job failed (named in the error; the five jobs are `$PROJECT-$TASK_NAME-{discovery,load,load-big,validate,cdc}`) | delete it by hand: `aws glue delete-job --job-name <name>`. **Do not re-list this task in a cutover fleet** |
| `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing changed for this task | fix the error shown in the execution, then re-run cutover for this task via the fleet |
| `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`) | DMS is stopped; a table's last file wasn't applied within ~12 h | fix the cause ([Troubleshooting → Cutover](#cutover)), then **finish by hand** (below) |
| `CutoverFailed` at a step **after DMS was stopped** | DMS is stopped | the Cause says "see execution history"; open the failed state to find the step. Fix it, then **finish by hand** (below) |

**Important: once a task's DMS is stopped, do not put that task back in a cutover fleet.** The
cutover's first step stops the DMS task, which the DMS API rejects for a task that is already
stopped, so a fresh cutover for it just ends at `CutoverFailed` within a couple of minutes without
doing anything ([Known issues](#known-issues-temporary)). Finish that task's remaining steps **by
hand** instead (set `TASK_NAME` to the task's folder/job stem):

```bash
TASK_NAME="<task name>"
# 1. Confirm every table of this task is caught up:
#      SELECT table_name, status FROM cdc_control.cdc_status;   -- none should be 'blocked'
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

**After a successful cutover, these remain for each task** (nothing deletes them): the stopped DMS
task and its endpoints; `config/_task/<task name>/` and the `config/_task_index/` record; every CDC
file and its `processed/` copy in the bucket; the `cdc_control` rows; and any separate
multi-column-PK CDC job (cutover never stops it — stop it yourself). The same DMS task can't be
migrated again ([Clean-slate reload](#clean-slate-reload)). Delete what you no longer need once you
are confident in the cutover.

---

## Rules for the task list

- **Each source table belongs to exactly one task.** All tasks share the one S3 layout, so two
  tasks with the same table would collide in the same folder.
- **Each row needs its own folder.** Two rows that resolve to the same folder (same DMS name, or
  the same `task_suffix`) are rejected by preflight — give one a different `task_suffix`.
- **Reusing a deleted task's name:** the pipeline refuses a folder that another task ARN created
  (its old status files would make CDC skip tables this task never loaded). Preflight fails that
  row with the `aws s3 mv` command to archive the old folder to `config/_archive/`.
- **Tables with a multi-column primary key** are left alone by the main CDC job (it lists them at
  startup). Run your own separate CDC job for them; it must write `cdc_control.cdc_file_status` the
  same way the main job does — `table_name` = `<dsql_schema>.<dsql_table>`, `cdc_file` = the S3 key
  of the change file (or ending with its file name), and `status='done'` (or
  `all_rows_committed=true`). Cutover waits for the newest change file of each such table to be
  marked done, and never stops or deletes that separate job.
- **Schema limit:** DSQL allows 10 schemas per database and `cdc_control` uses one, so a startup
  fleet's table lists may use at most 9 distinct DSQL schemas. Schemas already in the database from
  other tasks also count toward the limit of 10, but preflight can't see them — it prints a warning
  with the count it does see.

---

## Capacity and overlap

Per-task Glue job sizes (each template allows 10 concurrent runs):

| Job | Workers | | Job | Workers |
|---|---|---|---|---|
| discovery | 5 × G.2X | | validate | 10 × G.4X |
| load | 10 × G.4X | | CDC | 1 DPU (Python shell) or 2 × G.1X (Spark) |
| load-big | 20 × G.8X | | | |

The fleet starts **5 tasks at a time** (`MaxConcurrency` in its `FanOut`), so each wave of 5 creates
its Glue jobs together. One startup fans out up to **6 groups at once**, so a single task can ask
for up to 6 × 20 = **120 G.8X workers** on its big groups. Check your Glue concurrent-run and DPU
quotas before cutting a large wave loose. Each load run also opens up to `max_write_concurrency`
(default 150) DSQL connections.

A single `fleet-startup` or `fleet-cutover` execution handles up to a few hundred tasks comfortably
(the Map's results stay under Step Functions' 256 KB state limit up to roughly 400 tasks). Split
bigger lists into separate CSVs and triggers.

What the pipeline guarantees when runs overlap:

| Situation | What happens |
|---|---|
| Many tasks started together by the fleet | they run independently; the first to need the CDC drivers prepares them, the rest reuse that set; several CDC jobs creating the shared `cdc_control` tables at once retry automatically |
| The fleet triggered twice for the **same** task | preflight skips a task whose per-task run is already running (`already_running`); a startup fleet also skips a task past its full load (`past_full_load`). Even without the skip, the per-task run stops at `ResolveFailed` before touching DMS or S3 |
| The same CDC job started twice | Glue refuses the second run (one run at a time) |
| Two **different** CDC jobs applying the same table | each change is still applied once, in order; the losing run logs `another CDC run is also applying`. Find and stop the extra job |
| A load job started by hand during a run of the same group | **not guarded**; tables with a primary key are safe, keyless tables can get duplicate rows. Don't start load jobs by hand during a run |

---

## If a run fails: how to continue

There are two levels. The **fleet** execution tells you whether each task's run *started*; the
per-task **child** execution tells you how far that task got. Start at the fleet level, then open
the child executions that failed.

### Fleet level (`fleet-startup` / `fleet-cutover`)

| Fleet ends at | What it means | How to continue |
|---|---|---|
| `MissingFleetInput` | the input lacked `bucket` or `inputPrefix` (as strings) | trigger again with `{"bucket":"...","inputPrefix":"config"}` |
| `PreflightFailed` | a problem was found before anything started (settings, task list, or specific rows); **nothing started** | fix every problem the cause lists by row, then trigger the fleet again |
| `FleetStartIncomplete` | some tasks didn't start (`results.tasks` names them with `not_started` and the error/child status); the rest **did** start | fix those tasks, then trigger the fleet again with the same input — tasks already running (and startup tasks past full load) are skipped |
| `FleetStarted` | every task started or was skipped as already started | nothing to do at the fleet level; watch the child executions below |
| `FleetFailed` | the fan-out itself failed unexpectedly (rare) | re-trigger the fleet; if it recurs, check the fleet role (Step 1) can start and describe the per-task executions |

A started task's outcome is **not** in the fleet result — open its child `$PROJECT-startup` /
`$PROJECT-cutover` execution and use the tables below.

### Child startup (`$PROJECT-startup`)

**Where the child stopped decides what to do.** The execution graph shows the failed step and its
error.

| Stopped at | What's already done | How to continue |
|---|---|---|
| `MissingTaskArn` | nothing | shouldn't happen via the fleet (it always passes `taskArn`); re-check `fleet_tasks.csv` |
| `ResolveFailed`, `DriversFailed` | nothing; **DMS was not started** | fix the cause in the error, then **re-trigger the fleet** (this task restarts; already-running / past-full-load tasks are skipped) |
| `GroupsFailed`, or `PipelineFailed` at `CreateGlueJobs` / `RunDiscovery` / `PlanSplit` / `GroupFanOut` | DMS full load is in S3; DMS is paused at `STOPPED_AFTER_CACHED_EVENTS` | fix the cause (the failed group's Glue log has it), then **re-trigger the fleet** — finished files and tables are skipped; the task is **not** yet past full load, so it is not skipped |
| `DmsFailed` (error `DmsTaskFailed`) | DMS failed or a table errored during the full load | fix it in the DMS console (task → **Table statistics** and CloudWatch; reload the errored table). A task can only be (re)started by the fleet while it hasn't finished its full load; see [Clean-slate reload](#clean-slate-reload) if DMS is already past it |
| `DmsTimedOut` (error `DmsPollBudgetExceeded`) | DMS didn't reach `STOPPED_AFTER_CACHED_EVENTS` within 24 h | usually a DMS task already past its full load, or stopped partway ([Known issues](#known-issues-temporary)); or a genuinely long load. Check the DMS task; reload with a new DMS task if needed |
| `PipelineFailed` at `ResumeDmsToCdc` | load done and validated; DMS probably still paused | **do not re-trigger the fleet for this task** (it would be skipped as past full load, or stop at `ResolveFailed`). Check the DMS task; if still stopped, resume it: `aws dms start-replication-task --replication-task-arn "$TASK_ARN" --start-replication-task-type resume-processing`, then **start the CDC job by hand** (below) |
| `CdcRunFailed`, `CdcRunEnded`, `CdcFallbackFailed`, or `PipelineFailed` at `StartCdcJob` / `GetCdcRun` / `CheckCdcStarted` | load done; **DMS is in CDC**, capturing changes to S3 | **don't re-trigger the fleet for this task** — it is past its full load, so the fleet skips it (`past_full_load`) and a direct per-task run would stop at `ResolveFailed`. First check whether a CDC run is already RUNNING ([5h](#5h--check-that-cdc-is-applying-per-task)); if not, fix the cause in the CDC log and **start the CDC job by hand** (below). Nothing is lost while it's down: DMS keeps writing change files |
| `CdcStartNotConfirmed` | the CDC run is running but didn't write its start marker in 45 min | check the CDC log ([5h](#5h--check-that-cdc-is-applying-per-task)). If it shows `entering poll loop`, CDC is fine and the marker couldn't be written — see [Troubleshooting → CDC](#cdc) |

**Rule of thumb:** stopped **before** DMS is resumed into CDC → fix and **re-trigger the fleet**
(the task isn't past full load, so it isn't skipped). Stopped **after** DMS is in CDC → the fleet
would skip it, so **recover that one task by hand**.

**Start the CDC job by hand** (per task; set `TASK_NAME` and `CONFIG_PREFIX`). The job keeps its
saved settings; pass `--config_prefix` as a run argument so cutover can find and stop the run:

```bash
TASK_NAME="<task name>"
CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-cdc" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}" --query JobRunId --output text
```

Then check it as in [5h](#5h--check-that-cdc-is-applying-per-task). **Don't use the console's Run
button** for the CDC job: a console run has no `--config_prefix` run argument, so cutover would not
find and stop it.

### Child cutover (`$PROJECT-cutover`)

| Stopped at | What's already done | How to continue |
|---|---|---|
| `MissingTaskArn`, `ResolveFailed` | nothing | fix the cause; re-run cutover for this task **via the fleet** (keep only this task in the CSV, or leave it in — already-cut-over tasks must be removed first) |
| `CutoverFailed` **while DMS is still running** (at `StopCdcDmsTask`, before it stopped) | nothing changed | fix the error, re-run cutover for this task **via the fleet** |
| `CdcDrainTimedOut` | DMS stopped; drain didn't finish in ~12 h; nothing stopped/dropped/deleted | fix the drain cause ([Troubleshooting → Cutover](#cutover)), let CDC catch up, then **finish by hand** (Step 6) — **do not** put this task back in a cutover fleet |
| `CutoverFailed` at `DrainCheck` / `StopCdcRun` / `DropTags` | **DMS is stopped** | fix the error (often missing pg8000 or VPC on the two DSQL Lambdas), then **finish by hand** (Step 6) — **do not** re-list it in a cutover fleet |
| `GlueJobsNotDeleted` | fully cut over; only a Glue job delete failed | `aws glue delete-job --job-name <name>` — don't re-run cutover for this task |

**Rule of thumb:** `CutoverFailed` **while DMS is still running** (or `ResolveFailed`) → safe to
re-run cutover for that task **through the fleet**. Any failure **after DMS was stopped** → the DMS
task is already stopped, so a fresh cutover can't run; **finish that task by hand** with the Step 6
block.

---

## Clean-slate reload

To load a task again from scratch, everything from the earlier attempt must go **together** —
otherwise leftover status makes the pipeline skip or replay work. There is no supported in-place
reload; a reload needs a DMS task that hasn't finished its full load.

Set `TASK_NAME` to the task's recorded folder/job stem (its `task_suffix`, or the DMS name) and
`TASK_ARN` / `CONFIG_PREFIX` for it:

```bash
TASK_NAME="<task name>"
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
2. Delete each table's DMS folder, **including `processed/`**. If your DMS S3 target endpoint has
   a `BucketFolder`, prefix it (the same `cdcRoot` the pipeline derives from the endpoint — an
   empty value / `.` means none, so omit it):
   `aws s3 rm "s3://$BUCKET/<bucketFolder>/<schema>/<table>/" --recursive`
   (no BucketFolder: `aws s3 rm "s3://$BUCKET/<schema>/<table>/" --recursive`)
3. Delete the task's run state: `aws s3 rm "${CONFIG_PREFIX}_orchestrator/" --recursive`
4. In DSQL, drop and re-create the target tables, and delete their rows from **all six** control
   tables: `cdc_status`, `cdc_file_status`, `cdc_chunk_log`, `cdc_apply_exceptions`,
   `cdc_validation_failures`, `cdc_skipped_ops`. Only delete `cdc_control` rows **together with**
   step 2 — applied change files stay in the folder and would otherwise all be replayed.
5. **Reload with a DMS task that hasn't run yet, through the fleet.** The pipeline refuses a task
   past its full load, so create a new DMS task with the same settings and table mapping (a new
   name gives it a new folder), stage its `table_manifest.csv` ([5b](#5b--upload-each-tasks-table-list)),
   add its ARN as a row in `fleet_tasks.csv`, and trigger `fleet-startup` ([Step 5](#step-5--run-tasks-with-the-fleet)).
   If you reuse the old name, archive the old `config/_task/<name>/` folder first
   (`aws s3 mv "${CONFIG_PREFIX}" "s3://$BUCKET/config/_archive/$TASK_NAME-$(date +%Y%m%d%H%M)/" --recursive`).

> USAGE_GUIDE §8 describes the S3/DSQL purge but not the "new DMS task" requirement — follow this
> section for the DMS part.

---

## Upgrading an existing deployment

When you pull a newer version of this repo, redo the one-time steps in this order. Each is safe to
re-run.

1. **Step 1 part 1 and all the `put-role-policy` commands** (policies gain new permissions over
   time). Skip `create-role`; the roles already exist. This covers the fleet and preflight policies
   too — they're in the same loop.
2. **Step 2** — rebuild `fn.zip` (with pg8000 and `preflight_tasks.py`), run the create-or-update
   loop for the seven core functions, **and** the create-or-update block for `preflight-tasks`.
   This resets memory/timeout (1024 MB / 300 s for the core seven, 256 MB / 300 s for preflight).
3. **Step 3a**, including the checksum check. **The Glue scripts are a separate upload:** if you
   update the Lambdas and state machines but not `scripts/`, the jobs keep running the old scripts.
4. **Step 4** — the create-or-update branches update all four state machines (`startup`, `cutover`,
   `fleet-startup`, `fleet-cutover`) in place.
5. **A CDC run that is already running keeps its old script.** Stop it, wait for `STOPPED`, and
   start it by hand ([If a run fails](#if-a-run-fails-how-to-continue)) so it picks up the new
   script.

Tasks still running on older **per-task** state machines (`$PROJECT-startup-<suffix>`) keep working
with the new Lambdas, including their cutover. Leave them; use the fleet for new tasks.

---

## Known issues (temporary)

These are known bugs in the current code. Each has a safe workaround below; this list exists so it
can be deleted row by row as the bugs are fixed.

| # | Issue | Workaround until fixed |
|---|---|---|
| 1 | **Cutover can't be re-run once DMS is stopped.** The cutover's first step stops the DMS task; on a task that is already stopped the DMS API rejects it, so a fresh cutover ends at `CutoverFailed` within ~2 min without finishing. The cutover fleet has no "already cut over" skip, so re-listing such a task triggers exactly this failed re-run. | After any cutover failure **after** DMS was stopped (`CdcDrainTimedOut`, `CutoverFailed` at a later step, or `GlueJobsNotDeleted`), **finish that task by hand** with the Step 6 block, and **remove it from the CSV** before the next `fleet-cutover`. |
| 2 | **A real DMS start failure makes a task's startup wait 24 h.** If DMS can't start (e.g. a task stopped partway through its full load, or an endpoint that fails its test), that task's startup polls for 24 h and ends at `DmsTimedOut`. | Only list DMS tasks that have never run (or were cleaned via [clean-slate reload](#clean-slate-reload) with a new task). If you hit the 24 h wait, stop that child execution, fix DMS, and re-trigger the fleet. |
| 3 | **CDC Glue runs stop after 7 days.** Every CDC job has a 7-day (10080-minute) Glue timeout — the Glue maximum. A long migration's CDC run ends on its own. | Watch the CDC run ([5h](#5h--check-that-cdc-is-applying-per-task)). If it stops with no error after ~7 days, start it again by hand ([If a run fails](#if-a-run-fails-how-to-continue)); it resumes from where it left off. Cut over before 7 days where you can. |
| 4 | **`config/pipeline.example.json` fails the placeholder check.** Its `description` line contains `<bucket>`, and any value with `<`/`>` is rejected, so a copied-as-is template makes every run (and preflight) fail at `ResolveFailed`. | Generate `pipeline.json` with the Step 3c script (it omits `description`). If you must hand-edit, delete the `description` key, or any value containing `<` or `>`. |
| 5 | **A console "Run" of the CDC job is not stopped by cutover.** Cutover finds the CDC run by its `--config_prefix` **run** argument; a console run (or a `start-job-run` without that argument) has none, so cutover leaves it running. | Always start the CDC job with `--arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}"` (the block in [If a run fails](#if-a-run-fails-how-to-continue)). Never use the console Run button for the CDC job. If one slips through, stop it with `aws glue batch-stop-job-run` after cutover. |

> The **parameters CSV** ([`params.csv`](#params-csv)) is not a bug. It has shipped (offline-tested;
> real-AWS test pending): copy [`config/params.example.csv`](config/params.example.csv), upload it
> as `s3://$BUCKET/config/params.csv`, and `tools/setup.sh` and the fleet build `config/pipeline.json`
> from it ([Step 3c](#step-3c--pipeline-settings)). The hand-edited export block still works if you
> prefer it.

---

## Troubleshooting

Grouped by where the problem shows up. For the next step after a failed run, see
[If a run fails](#if-a-run-fails-how-to-continue).

### Setup

| Symptom | Cause | Fix |
|---|---|---|
| An `aws` command seems to hang until **Ctrl-C** (e.g. a loop creates only one Lambda) | the CLI pager is waiting for you | `export AWS_PAGER=""` and run it again; whatever you Ctrl-C'd was still created |
| `create-function`: *The role defined for the function cannot be assumed by Lambda* | the role is seconds old | wait 10 s and re-run the Step 2 loop |
| `CreateGlueJobs`: `not authorized to perform: iam:PassRole` | the Lambda policy still has `<<GLUE_EXEC_ROLE_NAME>>` | redo Step 1 part 1 and `put-role-policy` for the Lambda role |
| A Glue job carries a literal `<<BUCKET>>` | templates uploaded unfilled **and** the Lambda substitution was bypassed | redo Step 3a; the Lambda also refuses any leftover `<<...>>` at run time |

### Fleet

| Symptom | Cause | Fix |
|---|---|---|
| `MissingFleetInput` | started without both `bucket` and `inputPrefix` as strings | start with `{"bucket":"...","inputPrefix":"config"}` |
| `PreflightFailed` | the cause lists one or more problems: a `pipeline.json` problem, a missing/empty task list, a bad or duplicate row, a folder owned by another task, a missing `table_manifest.csv`, or too many DSQL schemas | fix each listed problem (settings in Step 3c, the CSV in [5c](#5c--write-fleet_taskscsv), the table lists in [5b](#5b--upload-each-tasks-table-list)); nothing started, so just trigger the fleet again |
| `PreflightFailed`: `s3://.../pipeline.json differs from .../config/pipeline.json` | a second `pipeline.json` next to the task list differs from the one every task reads | make `config/pipeline.json` the settings you want (keep a dated copy first), or remove the copy next to the task list |
| `PreflightFailed`: `params.csv has N problem(s)` | `params.csv` failed parse/validation (missing required key, `account_id` not 12 digits, `subnet_id`/`security_group_id` not both-or-neither, an unknown or duplicate key, or a value still holding `<`/`>`) | fix each listed problem in `params.csv` ([Step 3c](#step-3c--pipeline-settings)), re-upload it, trigger the fleet again |
| `PreflightFailed`: `sets project=… but this fleet runs the …` | `params.csv`'s `project` ≠ the project of the fleet's per-task state machine | set `project` in `params.csv` to match the fleet you're running, re-upload, trigger again |
| `PreflightFailed`: `both s3://…/params.csv and s3://…/pipeline.json exist` | a `pipeline.json` next to the task list is ambiguous when `params.csv` builds `config/pipeline.json` | remove the `<inputPrefix>/pipeline.json` copy; keep only `params.csv`, trigger again |
| `PreflightFailed`: `would change …/pipeline.json, but these runs are in progress` | a startup fleet's `params.csv` differs from live settings while a `startup`/`cutover`/fleet execution is running | wait for the named runs to finish (settings are never changed under a running task), then trigger the fleet again |
| `PreflightFailed`: `settings are never changed at cutover` | a **cutover** fleet's `params.csv` would change `config/pipeline.json` | publish the new settings with a **startup** fleet (or by hand) first, then cut over |
| `PreflightFailed`: `could not list running executions to safely change …/pipeline.json` | the preflight role can't `states:ListExecutions` on the four workflows, so the safe-publish fails closed | add `states:ListExecutions` for `$PROJECT-{startup,cutover,fleet-startup,fleet-cutover}` to the preflight role (Step 1), trigger again |
| `FleetStartIncomplete` | at least one task didn't start | open `results.tasks` in the execution output; fix the `not_started` tasks, re-trigger the fleet (started ones are skipped) |
| preflight warning: `could not list running executions` | the preflight role can't `states:ListExecutions`/`DescribeExecution` | the fleet still starts every task and the per-task duplicate-run guard still refuses second runs; add the permissions (Step 1) to get the skip-already-running behaviour back |

### Startup checks (`ResolveFailed`, `DriversFailed`)

These show up on a per-task child execution.

| Symptom | Cause | Fix |
|---|---|---|
| `ResolveFailed` (`SettingsError`) | a `pipeline.json` problem: missing file or required key, a value with leftover `<`/`>`, a non-ARN `glue_role_arn`, or a region that differs from the task ARN's region | the error names it; fix `pipeline.json` (Step 3c) and re-trigger the fleet |
| `ResolveFailed` (`TaskCheckError`) | a DMS task setting: not `full-load-and-cdc`, `StopTaskCachedChangesApplied` not true, `AddColumnName` false, wrong target bucket, or the task is past its full load | fix the DMS task/endpoint, then re-trigger the fleet |
| `ResolveFailed`: `Another startup run is already running for this DMS task` | a startup for this task is still running | wait for it or stop it, then re-trigger the fleet (it skips a running task) |
| `ResolveFailed` (`FolderOwnerError`) | `config/_task/<name>/` was created by a different task ARN, or holds files from an older run with no owner record, or a `task_suffix` differs from the recorded one | archive the folder (the error gives the `aws s3 mv` command) or use the recorded suffix; if the files are this task's own pre-shared-workflow run, set `adopt_existing_folder=true` in the row. Changed the DMS name after a failed first startup? delete `config/_task_index/<task id>.json` and the old `_task.json` |
| `DriversFailed` (`DriverCheckError`) | a `driver-cdc/` wheel can't run on Python 3.9 (scramp 1.4.7+, boto3/botocore 1.43+, urllib3 2.x), two versions of one package, a missing package, or a Spark driver folder without pg8000 | the error names the wheel and what to use; fix the folder (Step 3b) and re-trigger the fleet |
| `DriversFailed`: `prepare_cdc_wheels.py is missing from this Lambda's zip` | `fn.zip` was built from an old `lambdas/` | rebuild and redeploy (Step 2) |
| `DriverDiscoveryCdc`: `Task timed out`, or out of memory | the driver-discovery function still has 128 MB / a short timeout | re-run the Step 2 loop (it sets 1024 MB / 300 s) |

### Full load and validation

| Symptom | Cause | Fix |
|---|---|---|
| Discovery: `None of the N table(s) in this task has a DMS folder` | the table list names a schema/table DMS didn't write (often the source schema when the mapping renames it), a different BucketFolder, or DMS hasn't finished | the error lists the folders that exist; use those names (any case) in the table list and re-trigger the fleet |
| Discovery log: `no folder for table '<name>'` | the table had no rows at full load (normal), or the name is wrong | if the source table has rows, fix its name ([5b](#5b--upload-each-tasks-table-list)) and re-trigger the fleet |
| Spark job: `DataNotFoundError: endpoints` | a boto3/botocore wheel is in `driver-fullload/` or `driver-validation/` | remove it; those folders hold the 5 pg8000 wheels only |
| Any Glue job: `Unknown service: 'dsql'` | `driver-cdc/` lacks a current boto3 set (the Spark jobs take boto3 from it too) | upload it (Step 3b) and re-trigger the fleet |
| Glue job: `Can't create a connection to host ...dsql... port 5432`, or `Name or service not known` | the job isn't in your VPC | create the connection (Step 1b), set `glue_connection` (3c), re-trigger the fleet. Check: `aws glue get-job --job-name <job> --query Job.Connections` |
| `GroupsFailed` (`GroupLoadOrValidateFailed`) | a group's load or validation failed | open `GroupFanOut` in the child execution, read the group's Glue log, fix, re-trigger the fleet |
| Load "succeeded" with 0 rows | a stale `_load_status.json` marks tables done | **only if the startup stopped before `ResumeDmsToCdc` and no CDC run is running:** archive the state with `aws s3 mv "${CONFIG_PREFIX}_orchestrator/" "s3://$BUCKET/config/_archive/$TASK_NAME-$(date +%Y%m%d%H%M)/" --recursive`, then re-trigger the fleet. (If CDC is already running, this would stall it — [clean-slate reload](#clean-slate-reload) instead.) |
| Validation `CONTENT_DIFF` on a column | stored values differ from what DMS wrote (the report names the column, the check and both values) | read the group's `_validation_report.json`; compare a few rows in Oracle and DSQL. Usual causes: a mapping/type mismatch, or rounding by a narrower DSQL type |
| Validation: `No full-load status file` | the group's `_load_status.json` is missing | re-trigger the fleet (it reloads the group) |
| Validation log: `per-value hash check disabled` | the cluster rejected `md5()` | nothing to do; the other checks still run |
| Load or CDC stops with `BINARY GUARD` | a binary (RAW/BLOB) column holds a value that isn't hex, which is how DMS writes binary | check the DMS mapping for that column |
| `NA`/`NONE` text shows as NULL in DSQL | rows loaded by a version before the NULL-marker fix | deploy the current scripts (3a) and reload or correct the rows |

### CDC

| Symptom | Cause | Fix |
|---|---|---|
| `CdcRunFailed` / `CdcRunEnded` | the CDC run failed or stopped right after starting (the error is Glue's) | read the CDC run's log, fix, start the CDC job by hand |
| The run ends with no error after ~7 days | the 7-day Glue timeout ([Known issues](#known-issues-temporary) #3) | start the CDC job by hand; it resumes |
| Execution shows `CdcDriverFallback` and then succeeds | the Python-shell drivers failed; the job is now Spark | nothing to fix. The reason is in `config/_task/<task name>/_cdc_engine.json`; fix `driver-cdc/` and delete that file to go back to Python shell for this task |
| `CdcFallbackFailed` | re-creating the CDC job as Spark failed (often another run of it is active) | stop the other run (`aws glue batch-stop-job-run`), switch it with `python3 tools/switch_cdc_engine.py --job "$PROJECT-$TASK_NAME-cdc" --region "$REGION" --bucket "$BUCKET" --to spark --yes` (omit `--yes` for a dry run), then start the CDC job by hand |
| `CdcStartNotConfirmed`, but the log shows `entering poll loop` | the CDC job can't write its start marker, or the workflow can't see it | check the Glue role can write `config/_task/<task>/_cdc_started/`. If instead the run ended at `PipelineFailed` at `CheckCdcStarted`, the Step Functions role is missing the `ConfirmCdcStarted` statement — redo Step 1 |
| CDC fails `...whl installation failed ... CalledProcessError` after ~20 min, or logs `pypi.org` timeouts | the run was started with the raw `driver-cdc/` list (an old per-task workflow, or a hand start with `--extra-py-files`) | start it without `--extra-py-files`, so it uses the prepared list saved on the job |
| CDC keeps logging `full load not done ... waiting` | the CDC script in S3 is old, or a table's load isn't done | re-check Step 3a's checksums, then `aws s3 ls "${CONFIG_PREFIX}_orchestrator/" --recursive | grep _load_status` |
| A table is `blocked` in `cdc_control.cdc_status` | a `DROP COLUMN` on the source, or a row DSQL rejected (e.g. NULL into NOT NULL) | fix the cause, then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>';` — CDC resumes where it stopped. **Never delete the row**: applied files stay in the folder and would all be replayed |
| CDC log: `another CDC run is also applying` | two different CDC jobs apply the same table | find and stop the extra one; no change was applied twice |
| CDC log: `The DMS API is not reachable` or `CloudWatch is not reachable` | no VPC endpoint or NAT for that service | nothing is lost. Add a DMS endpoint if you want column renames detected (otherwise a renamed column is added as a new one) |
| `processed/_manifest.json` `pending_copy` doesn't drop | copies to `processed/` keep failing (throttling, permissions) | nothing is lost (originals are kept and retried); see `last_copy_error` in the manifest |
| DSQL rejects a `CREATE SCHEMA` with a schema-limit error when CDC starts | the database already has 10 schemas; `cdc_control` can't be created | drop unused schemas (DSQL allows 10 per database, not adjustable; keep ≤ 9 of your own), then start the CDC job by hand |

### Cutover

| Symptom | Cause | Fix |
|---|---|---|
| `CutoverFailed` at `StopCdcDmsTask`, DMS already stopped | a cutover ran again for a task whose DMS was already stopped ([Known issues](#known-issues-temporary) #1) | don't re-list it in a cutover fleet; finish by hand (Step 6) |
| `CdcDrainTimedOut` for a multi-column-key table (named in `DrainCheck`'s output) | the separate CDC job for those tables isn't running or doesn't mark files `done` under the same `<schema>.<table>` | run it until it catches up, then finish by hand (Step 6) |
| `CdcDrainTimedOut` for a normal table | that table is `blocked`, or the CDC run stopped | check `cdc_control.cdc_status` and the CDC log; fix, start the CDC job by hand, let it catch up, finish by hand (Step 6) |
| `CutoverFailed` at `DrainCheck`: `No module named 'pg8000'` | `fn.zip` was built without pg8000 | rebuild and redeploy (Step 2), then finish by hand (Step 6) |
| `CutoverFailed` at `DrainCheck`: timed out connecting to DSQL | the two DSQL Lambdas aren't in your VPC | Step 2 part 4, then finish by hand (Step 6) |
| `GlueJobsNotDeleted` | deleting a Glue job failed (named in the error, e.g. missing `glue:DeleteJob`), or the delete step's Lambda errored | fix it, then `aws glue delete-job --job-name <name>` for each of the five jobs still present |
| A console-started CDC run keeps going after cutover | the run had no `--config_prefix` ([Known issues](#known-issues-temporary) #5) | `aws glue batch-stop-job-run --job-name "$PROJECT-$TASK_NAME-cdc" --job-run-ids <id>` |

More: [`USAGE_GUIDE.md`](USAGE_GUIDE.md) (monitoring, manual runs) and
[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md) (architecture, limitations, DDL matrix).

---

## Reference

<details>
<summary>How the pieces fit: the fleet and the per-task machines</summary>

You only ever trigger the fleet. For each row in `fleet_tasks.csv`, the fleet runs one per-task
machine:

- **`$PROJECT-fleet-startup`** → one **`$PROJECT-startup`** execution per task
  (`{"taskArn", "taskSuffix"?, "adoptExistingFolder"?}`).
- **`$PROJECT-fleet-cutover`** → one **`$PROJECT-cutover`** execution per task
  (`{"taskArn", "taskSuffix"?}`).

The per-task `startup` and `cutover` machines are **what the fleet runs**, not something you start.
Their inputs and steps are documented here only so you can read a child execution's graph when
following [If a run fails](#if-a-run-fails-how-to-continue).

</details>

<details>
<summary>Blanks in the repo files, and where each value comes from</summary>

**Filled by you, once:**

| Blank | In | Meaning | Example |
|---|---|---|---|
| `<<BUCKET>>` | per-task state machines, job templates, IAM policies | your pipeline bucket | `my-migration-bucket` |
| `<<*_LAMBDA_ARN>>` | per-task state machines | the 7 Lambda ARNs (Step 4) | `arn:aws:lambda:…:function:dms-dsql-resolve-task` |
| `<<ACCOUNT_ID>>`, `<<REGION>>`, `<<PROJECT>>` | IAM policies, fleet state machines (Step 1, Step 4) | account, region, name prefix | `123456789012`, `us-east-1`, `dms-dsql` |
| `<<DSQL_CLUSTER_ID>>` | IAM policies (Step 1) | first label of the DSQL endpoint | `abcd` |
| `<<GLUE_EXEC_ROLE_NAME>>` | Lambda policy (`iam:PassRole`) | Glue role name | `dms-dsql-glue-exec-role` |
| `<<PREFLIGHT_TASKS_LAMBDA_ARN>>`, `<<STARTUP_STATE_MACHINE_ARN>>`, `<<CUTOVER_STATE_MACHINE_ARN>>` | fleet state machines (Step 4) | the preflight Lambda and the two per-task state machine ARNs | — |

**Read from `config/pipeline.json` at run time** (by every per-task run and by the fleet's
preflight): `project`, `region`, `dsql_endpoint`, `dsql_user`, `dsql_database`, `glue_role_arn`,
`glue_connection`, `cdc_engine`, `cdc_spark_fallback`, `control_schema`. Per task,
`config/_task/<task name>/_cdc_engine.json` (written by an automatic switch to Spark) overrides
`cdc_engine`.

**Worked out per task, from the row's `taskArn` (plus `taskSuffix` / `adoptExistingFolder`):**

| Value | From | Example |
|---|---|---|
| task name | the DMS task's name, or `task_suffix`; after the first startup, the name recorded for the ARN in `config/_task_index/<task id>.json` | `task-orders-02` |
| config folder | `s3://<bucket>/config/_task/<task name>/` | `.../config/_task/task-orders-02/` |
| Glue job names | `<project>-<task name>-{discovery,load,load-big,validate,cdc}` | `dms-dsql-task-orders-02-cdc` |
| folder owner record | `config/_task/<task name>/_task.json` | written at first startup (before DMS starts) |
| S3 layout | the DMS S3 endpoint (`BucketFolder`, `TimestampColumnName`, `CsvNullValue`, …) | `cdcRoot` = `.` with no BucketFolder |

</details>

<details>
<summary>What each per-task state-machine step runs (what the fleet runs per task)</summary>

| Step | Runs | Does |
|---|---|---|
| `ResolveTask` / `CutoverResolveTask` | `resolve-task` | reads `pipeline.json` and the DMS task; startup also checks the task and records the folder owner |
| `DriverDiscoveryFullload` / `…Validation` / `…Cdc` | `driver-discovery` | checks each `driver-*` folder; for a Python-shell CDC job, prepares `driver-cdc/` into `driver-cdc-prepared/<fingerprint>/`. Runs before DMS starts |
| `StartDmsTask` → `IsDmsDone` | DMS API | start DMS, poll (30 s × 2880 ≈ 24 h) until `STOPPED_AFTER_CACHED_EVENTS` |
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
logged to `cdc_control.cdc_skipped_ops`. Multi-column-PK tables are skipped by the main CDC job and
need a separate job ([Rules for the task list](#rules-for-the-task-list)).

**Control tables (in `control_schema`, default `cdc_control`):** `cdc_status`, `cdc_file_status`,
`cdc_chunk_log`, `cdc_apply_exceptions`, `cdc_validation_failures`, `cdc_skipped_ops`.

</details>

<details>
<summary>What each fleet step runs</summary>

| Step | Runs | Does |
|---|---|---|
| `CheckFleetInput` | — | requires `bucket` and `inputPrefix` as strings, else `MissingFleetInput` |
| `Preflight` | `preflight-tasks` | reads `fleet_tasks.csv` and checks every task (reusing `resolve_task`'s rules); any problem → `PreflightFailed`, nothing started. If `params.csv` sits next to the task list, it also builds `config/pipeline.json` from it and (startup only, nothing running) publishes it under the [safe-publish rule](#step-3c--pipeline-settings), reporting `paramsPublished`/`backupKey`/`paramsReason` in its output |
| `FanOut` (Map, 5 at a time) | `sfn:startExecution`, `sfn:describeExecution` | per row: skip if `already_running` / `past_full_load`; else start the per-task `startup`/`cutover`, wait 30 s, confirm it is RUNNING/SUCCEEDED |
| `EvalNotStarted` → `FleetStarted` / `FleetStartIncomplete` | — | `FleetStartIncomplete` if any task is `not_started`, else `FleetStarted` |

</details>

<details>
<summary>Fail states, by state machine</summary>

**Fleet (`fleet-startup`, `fleet-cutover`):** `MissingFleetInput`, `PreflightFailed` (nothing
started) · `FleetStartIncomplete` (some tasks not started; the rest were) · `FleetFailed` (the
fan-out itself failed). Success: `FleetStarted` (every task started or skipped as already started —
not that the migrations finished).

**Per-task startup:** `MissingTaskArn`, `ResolveFailed`, `DriversFailed` (before DMS starts) ·
`DmsFailed` (error `DmsTaskFailed`), `DmsTimedOut` (error `DmsPollBudgetExceeded`), `GroupsFailed`,
`PipelineFailed` (before DMS resumes) · `CdcRunFailed`, `CdcRunEnded`, `CdcStartNotConfirmed`,
`CdcFallbackFailed`, `PipelineFailed` (after DMS is in CDC).

**Per-task cutover:** `MissingTaskArn`, `ResolveFailed` (nothing touched) · `CutoverFailed` (DMS
may be stopped — check the failed step), `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`),
`GlueJobsNotDeleted` (fully cut over bar one job delete).

See [If a run fails](#if-a-run-fails-how-to-continue) for the recovery keyed to each state.

</details>
