# RUNBOOK — deploy and run the Oracle → Aurora DSQL migration pipeline

Follow this top to bottom. It is plain AWS CLI you paste in order — no CDK, no CloudFormation.
Every command is written to work the same on **macOS, Linux and AWS CloudShell**.

- One-time setup (Steps 1–4): about **1–2 hours**.
- Each migration task after that (Steps 5–6): a few minutes of your time, plus the load and CDC
  that run on their own.

> **New here?** Read [`README.md`](README.md) for the big picture first.
> [`USAGE_GUIDE.md`](USAGE_GUIDE.md) covers day-to-day monitoring once a task is running.

**Contents**

- [What you're building](#what-youre-building)
- [Prerequisites checklist](#prerequisites-checklist)
- [Values: what setup needs vs. what running a task needs](#values-what-setup-needs-vs-what-running-a-task-needs)
- [The S3 layout](#the-s3-layout)
- **One-time setup:**
  [Step 1 — IAM roles](#step-1--create-the-iam-roles) ·
  [Step 1b — Glue network connection](#step-1b--create-the-glue-network-connection-vpc-only) ·
  [Step 2 — Lambda functions](#step-2--create-the-lambda-functions) ·
  [Step 3a — scripts & templates](#step-3a--scripts-and-job-templates) ·
  [Step 3b — driver wheels](#step-3b--driver-wheels) ·
  [Step 3c — pipeline settings](#step-3c--pipeline-settings) ·
  [Step 4 — state machines](#step-4--create-the-two-state-machines)
- **Per task:**
  [Step 5 — run one task by hand](#step-5--run-one-task-by-hand) ·
  [Step 6 — cut over](#step-6--cut-over) ·
  [Running many tasks with the fleet](#running-many-tasks-with-the-fleet)
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
- **AWS Step Functions** runs the sequence. **Two state machines — a `startup` and a `cutover` —
  are shared by every task.** You start them with a single DMS task's ARN. Nothing runs on a
  schedule.
- An optional **fleet launcher** (two more state machines) starts the `startup` or `cutover` for
  a whole list of tasks from one trigger. If you have more than a handful of tasks, this is the
  normal way to run them (see [Running many tasks](#running-many-tasks-with-the-fleet)).
- **One S3 bucket** holds everything the pipeline needs: scripts, templates, driver wheels,
  settings and per-task state.

**One-time vs. per task:**

| | What | Steps |
|---|---|---|
| **Once** | IAM roles, Lambda functions, files and settings in S3, the two state machines (and the fleet, if you want it) | 1–4 |
| **Per DMS task** | upload the task's table list, start the `startup`; later, start the `cutover` | 5–6 |

**Terms used below:**

- **full load** — the one-time bulk copy of the rows that already exist. **CDC** (change data
  capture) — the stream of inserts, updates and deletes made after that, applied until you cut
  over.
- **`STOPPED_AFTER_CACHED_EVENTS`** — the DMS status that means "full load done, later changes
  captured and paused." The startup waits for it before loading into DSQL.
- **cutover** — the final switch of the application to Aurora DSQL.
- A table is **caught up** when its row in `cdc_control.cdc_status` shows no pending work.

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
      need a separate CDC job ([see below](#running-many-tasks-with-the-fleet)).
- [ ] **At most 9 schemas of your own** in the DSQL database. DSQL allows 10 schemas per database
      (not adjustable) and the pipeline adds `cdc_control`. Count yours:
      `SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg\_%' AND schema_name <> 'information_schema';`
- [ ] **A network path from Glue (and two Lambdas) to DSQL**, if your account is locked down: a
      **private subnet** and a **security group** that can reach DSQL, plus an **S3 gateway
      endpoint** in that subnet's route table. Step 1b uses them. Skip the VPC steps if Glue can
      already reach DSQL.
- [ ] **A DMS task** of type **`full-load-and-cdc`**. The boxes marked ✔ are checked by the startup
      *before* it starts DMS, so a mistake fails in seconds instead of hours:
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

## Values: what setup needs vs. what running a task needs

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

> <a id="load-values"></a>**Coming back in a new shell to run or cut over a task?** You do **not**
> need the whole block. Running a task needs only `BUCKET`, `PROJECT` and the DMS **task ARN** —
> and `BUCKET` / `PROJECT` are already in `config/pipeline.json`. Paste this to load them (edit only
> the bucket):
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
>
> That is all Steps 5 and 6 need, on top of the task ARN you set in 5a.

**Running a task (Steps 5–6) needs only the DMS task ARN** (plus `BUCKET` and `PROJECT`, loaded
above). Everything else — region, DSQL endpoint/user/database, Glue role, Glue connection, CDC
engine — is read at run time from `config/pipeline.json`. The **fleet** needs even less to start:
`{"bucket", "inputPrefix"}`.

> <a id="params-csv"></a>**Planned change — a single parameters CSV (not built yet).** A planned
> enhancement lets you keep every "export" value above in a `params.csv` (one `key,value` per row)
> next to `fleet_tasks.csv` in S3, so nobody retypes an export block. When it lands, **its one
> setup change goes exactly here**: replace the hand-edited export block with a single line that
> reads `params.csv` and exports the same variables, and have the fleet build `config/pipeline.json`
> from the same file. Nothing else in this runbook changes. **This does not exist today — ignore it
> until the [Known issues](#known-issues-temporary) note says it has shipped.**

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
    ├── pipeline.json         # settings read by every run                      (Step 3c)
    ├── fleet_tasks.csv       # the task list, if you use the fleet             (fleet)
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

*One-time, about 10 minutes. Needs the export block.*

**Goal:** the three roles the pipeline runs as — Glue, Lambda and Step Functions.

The files in `iam/` contain blanks (`<<REGION>>`, `<<ACCOUNT_ID>>`, `<<BUCKET>>`,
`<<DSQL_CLUSTER_ID>>`, `<<PROJECT>>`, `<<GLUE_EXEC_ROLE_NAME>>`). **Fill them in first:** a role
created from an unfilled file would trust or allow a literal `<<...>>` string. The loop writes
filled copies (`iam/*.filled.json`) and leaves the originals untouched, so a later `git pull`
never conflicts.

```bash
# 1. Fill in the blanks (portable: no sed -i)
for f in iam/*.json; do
  case "$f" in *.filled.json) continue ;; esac
  sed -e "s|<<REGION>>|$REGION|g" -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" \
      -e "s|<<BUCKET>>|$BUCKET|g" -e "s|<<DSQL_CLUSTER_ID>>|$DSQL_CLUSTER_ID|g" \
      -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<GLUE_EXEC_ROLE_NAME>>|$PROJECT-glue-exec-role|g" "$f" > "${f%.json}.filled.json"
done
grep -l "<<" iam/glue-exec-role.*.filled.json iam/lambda-exec-role.*.filled.json \
            iam/sfn-exec-role.*.filled.json || echo "no placeholders left in the 3 core roles"

# 2. Create the three roles and attach their policies
for r in glue lambda sfn; do
  aws iam create-role --role-name "$PROJECT-$r-exec-role" \
    --assume-role-policy-document "file://iam/$r-exec-role.trust.filled.json" \
    --query Role.RoleName --output text
  aws iam put-role-policy --role-name "$PROJECT-$r-exec-role" \
    --policy-name "$r" --policy-document "file://iam/$r-exec-role.policy.filled.json"
done

# 3. VPC only (GLUE_CONNECTION is not ""): let Glue make network interfaces in your subnet
aws iam put-role-policy --role-name "$PROJECT-glue-exec-role" \
  --policy-name glue-vpc --policy-document file://iam/glue-exec-role.vpc-addon.policy.filled.json
```

**Verify:** `aws iam get-role --role-name "$PROJECT-glue-exec-role" --query Role.Arn --output text`
prints the role ARN.

> **Already created a role from an unfilled file?** Run part 1 again, then
> `aws iam update-assume-role-policy --role-name "$PROJECT-glue-exec-role" --policy-document file://iam/glue-exec-role.trust.filled.json`
> (and the same for `lambda` and `sfn`), and run the `put-role-policy` commands again — they
> overwrite.

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

**Goal:** the seven small functions the two state machines call. They all use **one zip** and the
**Lambda role** from Step 1; they differ only by name and handler.

| Function | Handler | What it does |
|---|---|---|
| `$PROJECT-resolve-task` | `resolve_task.handler` | reads `config/pipeline.json` and the DMS task; works out the task's folder, job names and S3 layout; checks the task before DMS starts; records the folder owner |
| `$PROJECT-driver-discovery` | `driver_discovery.handler` | checks the `driver-*` folders; prepares the `driver-cdc/` wheels for a Python-shell CDC job |
| `$PROJECT-plan-split` | `plan_split.handler` | splits the task's tables into balanced load groups |
| `$PROJECT-create-glue-jobs` | `create_glue_jobs.handler` | creates (and at cutover deletes) the task's Glue jobs; re-creates the CDC job as Spark on a driver failure |
| `$PROJECT-stop-cdc-run` | `stop_cdc_run.handler` | stops this task's CDC run at cutover |
| `$PROJECT-drain-check` | `drain_check.handler` | **connects to DSQL:** waits until the last CDC file is applied |
| `$PROJECT-drop-tags` | `drop_tags.handler` | **connects to DSQL:** drops the internal `_cdc_file` tracking column at cutover |

**1. Build the zip.** It holds every `.py` from `lambdas/` plus the **`pg8000`** library, which
`drain-check` and `drop-tags` import to connect to DSQL. pg8000 is pure Python, so building it on
any machine is fine. It is installed into a separate build folder so the repo's `lambdas/` is never
touched.

```bash
rm -rf _lambda_build fn.zip && mkdir _lambda_build
cp lambdas/*.py _lambda_build/
python3 -m pip install pg8000 -t _lambda_build/ --quiet
(cd _lambda_build && zip -qr ../fn.zip .)
unzip -l fn.zip | grep -cE ' (resolve_task\.py|prepare_cdc_wheels\.py|pg8000/__init__\.py)$'   # must print 3
```

The count must be **3** (both key scripts and pg8000 are in the zip). If it is not, don't deploy.

**2. Create the functions, or update them if they already exist** (safe to re-run):

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

The update branch does **not** change `--role`, so it is safe to run even if you later add other
`$PROJECT-*` functions (such as the fleet's `preflight-tasks`) on their own roles. If
`create-function` says *"The role defined for the function cannot be assumed by Lambda"*, the role
from Step 1 is a few seconds old — wait 10 seconds and run the loop again.

**3. VPC only:** `drain-check` and `drop-tags` need the same network path as Glue:

```bash
aws iam attach-role-policy --role-name "$PROJECT-lambda-exec-role" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
for n in drain-check drop-tags; do
  aws lambda update-function-configuration --function-name "$PROJECT-$n" \
    --vpc-config "SubnetIds=$SUBNET_ID,SecurityGroupIds=$SECURITY_GROUP_ID" --query FunctionName --output text
  aws lambda wait function-updated --function-name "$PROJECT-$n"
done
```

**Verify:** all seven are listed (eight once you add the fleet's `preflight-tasks`):
`aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table`

> **Using the console instead?** For each function: **Author from scratch**, runtime **Python
> 3.12**, **Use an existing role** → `$PROJECT-lambda-exec-role`; upload the same `fn.zip`; set the
> handler from the table; set **Memory 1024 MB** and **Timeout 5 min**. The 1024 MB / 300 s matters
> for `driver-discovery` (preparing the CDC wheels) — the error message points back to this step.

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

Every `startup` and `cutover` run reads `s3://$BUCKET/config/pipeline.json`. An edit applies to
runs started **after** it, not to runs already going. Write it from your variables (don't hand-copy
the example file — see the note below):

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

| Key | Meaning |
|---|---|
| `project` | prefix of the Lambda and Glue job names; the same `$PROJECT` as Steps 1–2 |
| `region` | region of the DMS tasks and the pipeline (must equal the task ARN's region) |
| `dsql_endpoint`, `dsql_user`, `dsql_database` | the Aurora DSQL target |
| `glue_role_arn` | the Glue role from Step 1 |
| `glue_connection` | the Glue network connection's **exact** name (a console-made one may read e.g. `Network connection 1`); several, comma-separated; `""` = no VPC |
| `cdc_engine` | `pythonshell` (default; 1 DPU) or `spark` (Glue 4.0, 2 × G.1X) |
| `cdc_spark_fallback` | `true` (default): if a Python-shell CDC job's drivers fail, the startup re-creates that task's CDC job as Spark and carries on. `false`: stop at `DriversFailed` / `CdcRunFailed` |
| `control_schema` | DSQL schema for the CDC control tables (default `cdc_control`) |

> **Don't copy `config/pipeline.example.json` as-is.** Its `description` line contains `<bucket>`,
> and the pipeline rejects any value with `<` or `>` in it, so a run would fail at `ResolveFailed`
> ([Known issues](#known-issues-temporary)). The generator above omits `description`, so it is safe.
> Keep hand-edited values trimmed (no stray spaces) and never set `dsql_user`, `dsql_database` or
> `control_schema` to an empty string — a blank there is kept, not defaulted, and fails later.

Before changing a live file, keep a dated copy:
`aws s3 cp "s3://$BUCKET/config/pipeline.json" "s3://$BUCKET/config/pipeline.json.$(date +%Y%m%d%H%M)"`

---

## Step 4 — create the two state machines

*One-time, about 5 minutes. Re-run it whenever `stepfunctions/` changes.*

**Goal:** the **startup** state machine (full load → validate → start CDC) and the **cutover**
state machine (drain the last changes and clean up), both shared by every task. Their definition
files have only two kinds of blanks — `<<BUCKET>>` and the seven Lambda ARNs. Everything else is
read at run time from `pipeline.json` and the DMS task.

> **Did Steps 1–3 in the console?** Set `PROJECT`, `REGION`, `ACCOUNT_ID`, `BUCKET` and
> `SFN_ROLE_ARN` to match what you created, then confirm every Lambda exists:
> `for n in resolve-task driver-discovery plan-split create-glue-jobs stop-cdc-run drain-check drop-tags; do aws lambda get-function --function-name "$PROJECT-$n" --query Configuration.FunctionName --output text 2>/dev/null || echo "MISSING: $PROJECT-$n"; done`

**1. Fill in the blanks** (writes `startup.filled.asl.json` and `cutover.filled.asl.json`):

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
grep "<<" startup.filled.asl.json cutover.filled.asl.json || echo "no placeholders left"
```

**2. Create them, or update them if they exist** (safe to re-run):

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

**Verify:** both names are listed:
`aws stepfunctions list-state-machines --query "stateMachines[?starts_with(name,'$PROJECT-')].name" --output table`

> **Going to run many tasks?** Deploy the fleet launcher now
> ([Running many tasks](#running-many-tasks-with-the-fleet)). It adds an eighth Lambda, three
> roles and two more state machines, and is the normal way to start more than a few tasks.

---

## Step 5 — run one task by hand

*Per DMS task. Steps 1–4 don't change. To start many tasks at once, use the
[fleet](#running-many-tasks-with-the-fleet) instead.*

In a fresh shell, load your values first ([Coming back in a new shell](#load-values)) — you need
`BUCKET`, `PROJECT`, `REGION` and `AWS_PAGER=""`.

### 5a — pick the task

```bash
export TASK_ARN="arn:aws:dms:us-east-1:123456789012:task:XXXXXXXXXXXXXXXX"   # the DMS task
export TASK_NAME=$(aws dms describe-replication-tasks \
  --filters "Name=replication-task-arn,Values=$TASK_ARN" \
  --query 'ReplicationTasks[0].ReplicationTaskIdentifier' --output text)
export CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
echo "task=$TASK_NAME  folder=$CONFIG_PREFIX  jobs=$PROJECT-$TASK_NAME-*"
```

The task's **name** is its folder and the stem of its Glue job names. Don't rename a DMS task while
it is being migrated. (If a task has already been through its first startup under a different name
or a `taskSuffix`, the pipeline keeps the name it recorded for the ARN in
`config/_task_index/`; `TASK_NAME` above shows the *current* DMS name, which only differs if you
renamed it — in that case use the recorded name for the folder and job names.)

### 5b — upload the task's table list

A CSV with a header row and two columns, `dms_schema,dms_table` — each table **as DMS writes it to
S3** (i.e. after any rename in the DMS table mapping). Letter case doesn't matter: discovery finds
DMS's folder in any case, and the DSQL target is the same names in lowercase.

```csv
dms_schema,dms_table
TARGET_SCHEMA,MY_TABLE
target_schema,another_table
```

```bash
aws s3 cp table_manifest.csv "${CONFIG_PREFIX}table_manifest.csv"
```

A table with no folder yet (no rows at full load) is loaded empty, with a warning; the CDC job
picks it up when DMS creates the folder. If **none** of the tables has a folder, discovery fails
and lists the folders it did find, so a wrong name can't quietly load nothing.

### 5c — start it

```bash
STARTUP_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-startup'].stateMachineArn" --output text)
aws stepfunctions start-execution --state-machine-arn "$STARTUP_ARN" \
  --name "$(printf '%.60s' "$TASK_NAME")-$(date +%Y%m%d%H%M)" \
  --input "{\"taskArn\":\"$TASK_ARN\"}"
```

Watch it in the Step Functions console (the run name starts with the task name). In order:

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
   table's load is marked `done` (`${CONFIG_PREFIX}_orchestrator/group-<n>/_load_status.json`).
6. **Confirm CDC started** (polled every 30 s for up to **45 min**): the run succeeds when the CDC
   job writes `_cdc_started/<startup execution name>.json` (the Step Functions run name from 5c) on
   reaching its poll loop. If the CDC run fails first →
   **`CdcRunFailed`** (with Glue's error); if it stops → **`CdcRunEnded`**; if it never confirms →
   **`CdcStartNotConfirmed`**.
7. **Spark fallback, once** (`cdc_spark_fallback` on, Python-shell CDC job): if the CDC run failed
   because Glue couldn't install or import its drivers (pip/`pypi.org` timeouts,
   `CalledProcessError`, a missing or wrong-Python wheel, `No module named 'pg8000'`,
   `Unknown service: 'dsql'`), the CDC job is re-created **with the same name** as Spark, started
   with the same arguments, and confirmed as in step 6. Any other error (DSQL, permissions, data)
   is not retried. The switch is recorded in `${CONFIG_PREFIX}_cdc_engine.json`; later startups of
   this task build Spark straight away. Delete that file to go back to Python shell.

If the run stops at any Fail state, go to
[If a run fails](#if-a-run-fails-how-to-continue) — what to do depends on **where** it stopped.

> **Optional input keys** (rarely needed): `"taskSuffix": "<name>"` uses a different folder and job
> name than the DMS task's name; `"adoptExistingFolder": true` lets the task reuse a folder that
> already holds this task's files from a run made before the shared state machines existed.

### 5d — check that CDC is applying

```bash
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

## Step 6 — cut over

*Per task, when you're ready to switch the application to Aurora DSQL.*

### Before you start — checklist

Do these **in order**. Cutover stops DMS first, so any change made on the source after that is
never migrated — getting this order wrong loses data silently.

- [ ] **Stop writes to the source** for this task's tables (put the application in maintenance
      mode or make the source read-only). For a fleet cutover, stop writes for **every** table of
      **every** task in the list.
- [ ] **Let DMS deliver the last changes.** In the DMS console, the task's **CDCLatencySource** and
      **CDCLatencyTarget** are near zero, and (if the endpoint batches) you have waited past its
      `CdcMaxBatchInterval` so the last change file has landed in S3.
- [ ] **No table is blocked.** `SELECT table_name FROM cdc_control.cdc_status WHERE status='blocked';`
      returns nothing. If it doesn't, fix the cause and unblock
      ([Troubleshooting → CDC](#cdc)) before cutting over.
- [ ] **The CDC run is RUNNING** (check as in 5d). Cutover does not verify this; if the run is not
      running, cutover stops DMS and then waits the full drain budget (~12 h) before failing.
- [ ] **Multi-column-PK tables:** their separate CDC job is running and has caught up (it must mark
      their latest change files `done` in `cdc_control.cdc_file_status`).

### Start it

With `TASK_ARN` and `TASK_NAME` set as in 5a:

```bash
CUTOVER_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-cutover'].stateMachineArn" --output text)
aws stepfunctions start-execution --state-machine-arn "$CUTOVER_ARN" \
  --name "cutover-$(printf '%.50s' "$TASK_NAME")-$(date +%Y%m%d%H%M)" \
  --input "{\"taskArn\":\"$TASK_ARN\"}"
```

It stops the DMS task (up to ~1 h to reach `stopped`), waits until each table's last CDC file is
applied (`DrainCheck`; up to ~12 h), stops this task's CDC run, drops the internal `_cdc_file`
column, and deletes this task's five Glue jobs. It finds the task by its ARN, so a renamed task
still cuts over its original folder and jobs. Other tasks are not affected.

| Ends at | Meaning | What to do |
|---|---|---|
| `CutoverSucceeded` | done | point the application at Aurora DSQL |
| `GlueJobsNotDeleted` | the data is cut over; only deleting a Glue job failed (named in the error, or the Lambda's error if the delete step itself failed; the five jobs are `$PROJECT-$TASK_NAME-{discovery,load,load-big,validate,cdc}`) | delete it by hand: `aws glue delete-job --job-name <name>`. **Do not re-run the cutover** |
| `ResolveFailed`, or `CutoverFailed` **while DMS is still running** | nothing has changed yet | fix the error shown in the execution, start the cutover again |
| `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`) | DMS is stopped; a table's last file wasn't applied within ~12 h | fix the cause ([Troubleshooting → Cutover](#cutover)), then **finish by hand** (below) |
| `CutoverFailed` at a step **after DMS was stopped** | DMS is stopped | the Cause says "see execution history"; open the failed state to find the step. Fix it, then **finish by hand** (below) |

**Important: once DMS is stopped, do not start the cutover again.** Its first step stops the DMS
task, which the DMS API rejects for a task that is already stopped, so a re-run just ends at
`CutoverFailed` within a couple of minutes without doing anything
([Known issues](#known-issues-temporary)). Finish the remaining steps by hand instead:

```bash
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

**After a successful cutover, these remain** (nothing deletes them): the stopped DMS task and its
endpoints; `config/_task/$TASK_NAME/` and the `config/_task_index/` record; every CDC file and its
`processed/` copy in the bucket; the `cdc_control` rows; and any separate multi-column-PK CDC job
(cutover never stops it — stop it yourself). The same DMS task can't be migrated again
([Clean-slate reload](#clean-slate-reload)). Delete what you no longer need once you are confident
in the cutover.

---

## Running many tasks with the fleet

If you have more than a handful of DMS tasks, don't run Step 5 by hand for each one. The **fleet
launcher** starts the shared `startup` (or `cutover`) for a whole list of tasks from one trigger.
It checks every task first, starts one per-task execution per task (5 at a time), confirms each got
past its own input checks, and finishes in minutes. Each task's migration then runs on its own,
exactly as if you had started it with `{"taskArn": "..."}`. Full details, including the result
meanings, are in [`docs/FLEET_LAUNCHER.md`](docs/FLEET_LAUNCHER.md).

> The fleet on `main` is the reworked launcher. It has been exercised in a state-machine simulator
> but **not yet run live on AWS**. Before using it for a large wave, run one small live fleet of a
> task or two first.

### Deploy the fleet (once)

This adds an **eighth** Lambda (`$PROJECT-preflight-tasks`), three roles and two state machines
(`$PROJECT-fleet-startup`, `$PROJECT-fleet-cutover`). `preflight_tasks.py` is already inside
`fn.zip` from Step 2 (it reuses `resolve_task`). Needs the export block.

```bash
# 1. Fill the fleet IAM files (Step 1's loop already produced these *.filled.json too)
for f in iam/preflight-tasks-role.policy.json iam/fleet-startup-role.policy.json iam/fleet-cutover-role.policy.json; do
  sed -e "s|<<REGION>>|$REGION|g" -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" \
      -e "s|<<BUCKET>>|$BUCKET|g" -e "s|<<PROJECT>>|$PROJECT|g" "$f" > "${f%.json}.filled.json"
done

# 2. preflight-tasks role + function (its trust file needs no fill)
aws iam create-role --role-name "$PROJECT-preflight-tasks-role" \
  --assume-role-policy-document file://iam/preflight-tasks-role.trust.json \
  --query Role.RoleName --output text
aws iam put-role-policy --role-name "$PROJECT-preflight-tasks-role" --policy-name preflight \
  --policy-document file://iam/preflight-tasks-role.policy.filled.json
sleep 10
aws lambda create-function --function-name "$PROJECT-preflight-tasks" --runtime python3.12 \
  --handler preflight_tasks.handler --zip-file fileb://fn.zip --timeout 300 --memory-size 256 \
  --role "arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-preflight-tasks-role" \
  --query FunctionName --output text
aws lambda wait function-active-v2 --function-name "$PROJECT-preflight-tasks"

# 3. The two fleet Step Functions roles (their trust files only need ACCOUNT_ID)
sed -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" iam/fleet-startup-role.trust.json > fleet-trust.filled.json
for w in startup cutover; do
  aws iam create-role --role-name "$PROJECT-fleet-$w-role" \
    --assume-role-policy-document file://fleet-trust.filled.json --query Role.RoleName --output text
  aws iam put-role-policy --role-name "$PROJECT-fleet-$w-role" --policy-name fleet \
    --policy-document "file://iam/fleet-$w-role.policy.filled.json"
done

# 4. The two fleet state machines
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
for w in startup cutover; do
  sed -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT-preflight-tasks|g" \
      -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM:$PROJECT-startup|g" \
      -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM:$PROJECT-cutover|g" \
      "stepfunctions/fleet-$w.asl.json" > "fleet-$w.filled.asl.json"
  grep "<<" "fleet-$w.filled.asl.json" || echo "no placeholders left in fleet-$w"
  aws stepfunctions create-state-machine --name "$PROJECT-fleet-$w" \
    --definition "file://fleet-$w.filled.asl.json" \
    --role-arn "arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-fleet-$w-role"
done
```

### Run the fleet

1. Build the task list CSV — `task_arn` (required), `task_suffix` (optional), `adopt_existing_folder`
   (optional, startup only). Example in
   [`config/fleet_tasks.example.csv`](config/fleet_tasks.example.csv):

   ```csv
   task_arn,task_suffix,adopt_existing_folder
   arn:aws:dms:us-east-1:123456789012:task:ABCDEF1234567890,,
   arn:aws:dms:us-east-1:123456789012:task:GHIJKL0987654321,orders-cdc,
   ```

2. For a **startup** fleet, stage each task's `table_manifest.csv` first (Step 5b for every task).
3. Upload the list and start the fleet:

   ```bash
   aws s3 cp fleet_tasks.csv "s3://$BUCKET/config/fleet_tasks.csv"
   SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"
   aws stepfunctions start-execution --state-machine-arn "$SM:$PROJECT-fleet-startup" \
     --input "{\"bucket\":\"$BUCKET\",\"inputPrefix\":\"config\"}"
   ```

   The fleet reads `config/pipeline.json` (the same file every task uses — it does not write it)
   and `config/<inputPrefix>/fleet_tasks.csv`. `FleetStarted` means every task got past its input
   checks, **not** that the migrations succeeded — watch each `$PROJECT-startup` /
   `$PROJECT-cutover` execution. For a **cutover** fleet, do the whole
   [Before you start checklist](#before-you-start--checklist) for every task in the list first.

### Rules for many tasks

- **Each source table belongs to exactly one task.** All tasks share the one S3 layout, so two
  tasks with the same table would collide in the same folder.
- **Reusing a deleted task's name:** the startup refuses a folder that another task ARN created
  (its old status files would make CDC skip tables this task never loaded). The error gives the
  `aws s3 mv` command to archive the old folder to `config/_archive/`.
- **Tables with a multi-column primary key** are left alone by the main CDC job (it lists them at
  startup). Run your own separate CDC job for them; it must write `cdc_control.cdc_file_status` the
  same way the main job does — `table_name` = `<dsql_schema>.<dsql_table>`, `cdc_file` = the S3 key
  of the change file (or ending with its file name), and `status='done'` (or
  `all_rows_committed=true`). Cutover waits for the newest change file of each such table to be
  marked done, and never stops or deletes that separate job.

### Capacity and overlap

Per-task Glue job sizes (each template allows 10 concurrent runs):

| Job | Workers | | Job | Workers |
|---|---|---|---|---|
| discovery | 5 × G.2X | | validate | 10 × G.4X |
| load | 10 × G.4X | | CDC | 1 DPU (Python shell) or 2 × G.1X (Spark) |
| load-big | 20 × G.8X | | | |

One startup fans out up to **6 groups at once**, so a single task can ask for up to 6 × 20 = **120
G.8X workers** on its big groups. Check your Glue concurrent-run and DPU quotas before starting
several full loads together. Each load run also opens up to `max_write_concurrency` (default 150)
DSQL connections.

What the pipeline guarantees when runs overlap:

| Situation | What happens |
|---|---|
| Many tasks started together | they run independently; the first to need the CDC drivers prepares them, the rest reuse that set; several CDC jobs creating the shared `cdc_control` tables at once retry automatically |
| A second startup (or cutover) for the **same** task while one is running | stops at `ResolveFailed` before touching DMS or S3 (needs `states:ListExecutions`/`DescribeExecution` on the Lambda role, Step 1) |
| The same CDC job started twice | Glue refuses the second run (one run at a time) |
| Two **different** CDC jobs applying the same table | each change is still applied once, in order; the losing run logs `another CDC run is also applying`. Find and stop the extra job |
| A load job started by hand during a run of the same group | **not guarded**; tables with a primary key are safe, keyless tables can get duplicate rows. Don't start load jobs by hand during a run |

---

## If a run fails: how to continue

**Where the run stopped decides what to do.** In the Step Functions console, the execution graph
shows the failed step and its error. Startup first, then cutover.

### Startup

| Stopped at | What's already done | How to continue |
|---|---|---|
| `MissingTaskArn` | nothing | start with `--input '{"taskArn":"..."}'` (5c) |
| `ResolveFailed`, `DriversFailed` | nothing; **DMS was not started** | fix the cause in the error, start again with the same input |
| `GroupsFailed`, or `PipelineFailed` at `CreateGlueJobs` / `RunDiscovery` / `PlanSplit` / `GroupFanOut` | DMS full load is in S3; DMS is paused at `STOPPED_AFTER_CACHED_EVENTS` | fix the cause (the failed group's Glue log has it), start again with the same input — finished files and tables are skipped |
| `DmsFailed` (error `DmsTaskFailed`) | DMS failed or a table errored during the full load | fix it in the DMS console (task → **Table statistics** and the task's CloudWatch log; reload the errored table). The startup can only start a task that hasn't finished its full load; see [Clean-slate reload](#clean-slate-reload) if DMS is already past it |
| `DmsTimedOut` (error `DmsPollBudgetExceeded`) | DMS didn't reach `STOPPED_AFTER_CACHED_EVENTS` within 24 h | usually a DMS task that was already past its full load, or was stopped partway (see [Known issues](#known-issues-temporary)); or a genuinely long load. Check the DMS task; reload with a new DMS task if needed |
| `PipelineFailed` at `ResumeDmsToCdc` | load done and validated; DMS probably still paused | check the DMS task. If it's still stopped, resume it: `aws dms start-replication-task --replication-task-arn "$TASK_ARN" --start-replication-task-type resume-processing`, then start the CDC job by hand (below) |
| `CdcRunFailed`, `CdcRunEnded`, `CdcFallbackFailed`, or `PipelineFailed` at `StartCdcJob` / `GetCdcRun` / `CheckCdcStarted` | load done; **DMS is in CDC**, capturing changes to S3 | **don't start the startup again** — it would stop at `ResolveFailed` (the task is past its full load). First check whether a CDC run is already RUNNING (5d); if not, fix the cause in the CDC run's log and start the CDC job by hand (below). Nothing is lost while it's down: DMS keeps writing change files |
| `CdcStartNotConfirmed` | the CDC run is running but didn't write its start marker in 45 min | check the CDC log (5d). If it shows `entering poll loop`, CDC is fine and the marker couldn't be written — see [Troubleshooting → CDC](#cdc) |

**Start the CDC job by hand** (needs `TASK_NAME` and `CONFIG_PREFIX` from 5a). The job keeps its
saved settings; pass `--config_prefix` as a run argument so cutover can find and stop the run:

```bash
aws glue start-job-run --job-name "$PROJECT-$TASK_NAME-cdc" \
  --arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}" --query JobRunId --output text
```

Then check it as in 5d. **Don't use the console's Run button** for the CDC job: a console run has
no `--config_prefix` run argument, so cutover would not find and stop it.

### Cutover

| Stopped at | What's already done | How to continue |
|---|---|---|
| `MissingTaskArn`, `ResolveFailed` | nothing | fix the input/cause, start the cutover again |
| `CutoverFailed` **while DMS is still running** (at `StopCdcDmsTask`, before it stopped) | nothing changed | fix the error, start the cutover again |
| `CdcDrainTimedOut` | DMS stopped; drain didn't finish in ~12 h; nothing stopped/dropped/deleted | fix the drain cause ([Troubleshooting → Cutover](#cutover)), let CDC catch up, then **finish by hand** (Step 6) — don't re-run |
| `CutoverFailed` at `DrainCheck` / `StopCdcRun` / `DropTags` | **DMS is stopped** | fix the error (often missing pg8000 or VPC on the two DSQL Lambdas), then **finish by hand** (Step 6) — don't re-run |
| `GlueJobsNotDeleted` | fully cut over; only a Glue job delete failed | `aws glue delete-job --job-name <name>` — don't re-run the cutover |

---

## Clean-slate reload

To load a task again from scratch, everything from the earlier attempt must go **together** —
otherwise leftover status makes the pipeline skip or replay work. There is no supported in-place
reload; a reload needs a DMS task that hasn't finished its full load.

1. Stop this task's CDC run and the DMS task (use the task's **recorded** name for the job stem if
   it was renamed or started with a `taskSuffix` — see [5a](#5a--pick-the-task)):
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
5. **Start with a DMS task that hasn't run yet.** The startup refuses a task past its full load, so
   create a new DMS task with the same settings and table mapping (a new name gives it a new
   folder), and run it as in Step 5. If you reuse the old name, archive the old
   `config/_task/<name>/` folder first (`aws s3 mv ... s3://$BUCKET/config/_archive/<name>-<date>/ --recursive`).

> USAGE_GUIDE §8 describes the S3/DSQL purge but not the "new DMS task" requirement — follow this
> section for the DMS part.

---

## Upgrading an existing deployment

When you pull a newer version of this repo, redo the one-time steps in this order. Each is safe to
re-run.

1. **Step 1 part 1 and the `put-role-policy` commands** (policies gain new permissions over time).
   Skip `create-role`; the roles already exist. If you deployed the fleet, re-put its policies too.
2. **Step 2** (rebuild `fn.zip` with pg8000 and run the create-or-update loop). This updates every
   function's code **and** resets memory/timeout to 1024 MB / 300 s. If the fleet is deployed, also
   update it: `aws lambda update-function-code --function-name "$PROJECT-preflight-tasks" --zip-file fileb://fn.zip`
   then `aws lambda wait function-updated --function-name "$PROJECT-preflight-tasks"`.
3. **Step 3a**, including the checksum check. **The Glue scripts are a separate upload:** if you
   update the Lambdas and state machines but not `scripts/`, the jobs keep running the old scripts.
4. **Step 4** (updates both shared state machines; redeploy the fleet state machines the same way
   with `update-state-machine` if deployed).
5. **A CDC run that is already running keeps its old script.** Stop it, wait for `STOPPED`, and
   start it by hand ([If a run fails](#if-a-run-fails-how-to-continue)) so it picks up the new script.

Tasks still running on older **per-task** state machines (`$PROJECT-startup-<suffix>`) keep working
with the new Lambdas, including their cutover. Leave them; use the shared pair for new tasks.

---

## Known issues (temporary)

These are known bugs in the current code. Each has a safe workaround below; this list exists so it
can be deleted row by row as the bugs are fixed.

| # | Issue | Workaround until fixed |
|---|---|---|
| 1 | **Cutover can't be re-run once DMS is stopped.** The cutover's first step stops the DMS task; on a task that is already stopped the DMS API rejects it, so a re-run ends at `CutoverFailed` within ~2 min without finishing. | After any cutover failure that happened **after** DMS was stopped (`CdcDrainTimedOut`, `CutoverFailed` at a later step, or `GlueJobsNotDeleted`), **finish by hand** with the Step 6 block — don't start the cutover again. |
| 2 | **A real DMS start failure makes the startup wait 24 h.** If DMS can't start (e.g. a task stopped partway through its full load, or an endpoint that fails its test), the startup polls for 24 h and ends at `DmsTimedOut`. | Don't start the pipeline on a DMS task that has already run partway. Use a task that has never run, or [clean-slate reload](#clean-slate-reload) with a new task. If you hit the 24 h wait, stop the execution, fix DMS, and start again. |
| 3 | **CDC Glue runs stop after 7 days.** Every CDC job has a 7-day (10080-minute) Glue timeout — the Glue maximum. A long migration's CDC run ends on its own. | Watch the CDC run (5d). If it stops with no error after ~7 days, start it again by hand ([If a run fails](#if-a-run-fails-how-to-continue)); it resumes from where it left off. Cut over before 7 days where you can. |
| 4 | **`config/pipeline.example.json` fails the placeholder check.** Its `description` line contains `<bucket>`, and any value with `<`/`>` is rejected, so a copied-as-is template makes every run fail at `ResolveFailed`. | Generate `pipeline.json` with the Step 3c script (it omits `description`). If you must hand-edit, delete the `description` key, or any value containing `<` or `>`. |
| 5 | **A console "Run" of the CDC job is not stopped by cutover.** Cutover finds the CDC run by its `--config_prefix` **run** argument; a console run (or a `start-job-run` without that argument) has none, so cutover leaves it running. | Always start the CDC job with `--arguments "{\"--config_prefix\":\"$CONFIG_PREFIX\"}"` (the block in [If a run fails](#if-a-run-fails-how-to-continue)). Never use the console Run button for the CDC job. If one slips through, stop it with `aws glue batch-stop-job-run` after cutover. |

> The **parameters CSV** ([planned change](#params-csv)) is not a bug — it is a not-yet-built
> enhancement. Remove its note and wire up the one setup line once it ships.

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

### Startup checks (`ResolveFailed`, `DriversFailed`)

| Symptom | Cause | Fix |
|---|---|---|
| `MissingTaskArn` | started without input | start with `--input '{"taskArn":"arn:aws:dms:..."}'` |
| `ResolveFailed` (`SettingsError`) | a `pipeline.json` problem: missing file or required key, a value with leftover `<`/`>`, a non-ARN `glue_role_arn`, or a region that differs from the task ARN's region | the error names it; fix `pipeline.json` (Step 3c) and start again |
| `ResolveFailed` (`TaskCheckError`) | a DMS task setting: not `full-load-and-cdc`, `StopTaskCachedChangesApplied` not true, `AddColumnName` false, wrong target bucket, or the task is past its full load | fix the DMS task/endpoint, then start again |
| `ResolveFailed`: `Another startup run is already running for this DMS task` | a startup for this task is still running | wait for it or stop it, then start again |
| `ResolveFailed` (`FolderOwnerError`) | `config/_task/<name>/` was created by a different task ARN, or holds files from an older run with no owner record, or you passed a `taskSuffix` that differs from the recorded one | archive the folder (the error gives the `aws s3 mv` command) or use the recorded suffix; if the files are this task's own pre-shared-workflow run, start with `"adoptExistingFolder": true`. Changed the DMS name after a failed first startup? delete `config/_task_index/<task id>.json` and the old `_task.json` |
| `DriversFailed` (`DriverCheckError`) | a `driver-cdc/` wheel can't run on Python 3.9 (scramp 1.4.7+, boto3/botocore 1.43+, urllib3 2.x), two versions of one package, a missing package, or a Spark driver folder without pg8000 | the error names the wheel and what to use; fix the folder (Step 3b) and start again |
| `DriversFailed`: `prepare_cdc_wheels.py is missing from this Lambda's zip` | `fn.zip` was built from an old `lambdas/` | rebuild and redeploy (Step 2) |
| `DriverDiscoveryCdc`: `Task timed out`, or out of memory | the driver-discovery function still has 128 MB / a short timeout | re-run the Step 2 loop (it sets 1024 MB / 300 s) |

### Full load and validation

| Symptom | Cause | Fix |
|---|---|---|
| Discovery: `None of the N table(s) in this task has a DMS folder` | the table list names a schema/table DMS didn't write (often the source schema when the mapping renames it), a different BucketFolder, or DMS hasn't finished | the error lists the folders that exist; use those names (any case) in the table list and start again |
| Discovery log: `no folder for table '<name>'` | the table had no rows at full load (normal), or the name is wrong | if the source table has rows, fix its name (5b) and start again |
| Spark job: `DataNotFoundError: endpoints` | a boto3/botocore wheel is in `driver-fullload/` or `driver-validation/` | remove it; those folders hold the 5 pg8000 wheels only |
| Any Glue job: `Unknown service: 'dsql'` | `driver-cdc/` lacks a current boto3 set (the Spark jobs take boto3 from it too) | upload it (Step 3b) and start again |
| Glue job: `Can't create a connection to host ...dsql... port 5432`, or `Name or service not known` | the job isn't in your VPC | create the connection (Step 1b), set `glue_connection` (3c), start again. Check: `aws glue get-job --job-name <job> --query Job.Connections` |
| `GroupsFailed` (`GroupLoadOrValidateFailed`) | a group's load or validation failed | open `GroupFanOut` in the execution, read the group's Glue log, fix, start again |
| Load "succeeded" with 0 rows | a stale `_load_status.json` marks tables done | **only if the startup stopped before `ResumeDmsToCdc` and no CDC run is running:** archive the state with `aws s3 mv "${CONFIG_PREFIX}_orchestrator/" "s3://$BUCKET/config/_archive/$TASK_NAME-$(date +%Y%m%d%H%M)/" --recursive`, then start again. (If CDC is already running, this would stall it — [clean-slate reload](#clean-slate-reload) instead.) |
| Validation `CONTENT_DIFF` on a column | stored values differ from what DMS wrote (the report names the column, the check and both values) | read the group's `_validation_report.json`; compare a few rows in Oracle and DSQL. Usual causes: a mapping/type mismatch, or rounding by a narrower DSQL type |
| Validation: `No full-load status file` | the group's `_load_status.json` is missing | start the startup again (it reloads the group) |
| Validation log: `per-value hash check disabled` | the cluster rejected `md5()` | nothing to do; the other checks still run |
| Load or CDC stops with `BINARY GUARD` | a binary (RAW/BLOB) column holds a value that isn't hex, which is how DMS writes binary | check the DMS mapping for that column |
| `NA`/`NONE` text shows as NULL in DSQL | rows loaded by a version before the NULL-marker fix | deploy the current scripts (3a) and reload or correct the rows |

### CDC

| Symptom | Cause | Fix |
|---|---|---|
| `CdcRunFailed` / `CdcRunEnded` | the CDC run failed or stopped right after starting (the error is Glue's) | read the CDC run's log, fix, start the CDC job by hand |
| The run ends with no error after ~7 days | the 7-day Glue timeout ([Known issues](#known-issues-temporary) #3) | start the CDC job by hand; it resumes |
| Execution shows `CdcDriverFallback` and then succeeds | the Python-shell drivers failed; the job is now Spark | nothing to fix. The reason is in `${CONFIG_PREFIX}_cdc_engine.json`; fix `driver-cdc/` and delete that file to go back to Python shell for this task |
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
| `CutoverFailed` at `StopCdcDmsTask`, DMS already stopped | you re-ran a cutover after DMS was stopped ([Known issues](#known-issues-temporary) #1) | don't re-run; finish by hand (Step 6) |
| `CdcDrainTimedOut` for a multi-column-key table (named in `DrainCheck`'s output) | the separate CDC job for those tables isn't running or doesn't mark files `done` under the same `<schema>.<table>` | run it until it catches up, then finish by hand (Step 6) |
| `CdcDrainTimedOut` for a normal table | that table is `blocked`, or the CDC run stopped | check `cdc_control.cdc_status` and the CDC log; fix, start the CDC job by hand, let it catch up, finish by hand (Step 6) |
| `CutoverFailed` at `DrainCheck`: `No module named 'pg8000'` | `fn.zip` was built without pg8000 | rebuild and redeploy (Step 2), then finish by hand (Step 6) |
| `CutoverFailed` at `DrainCheck`: timed out connecting to DSQL | the two DSQL Lambdas aren't in your VPC | Step 2 part 3, then finish by hand (Step 6) |
| `GlueJobsNotDeleted` | deleting a Glue job failed (named in the error, e.g. missing `glue:DeleteJob`), or the delete step's Lambda errored | fix it, then `aws glue delete-job --job-name <name>` for each of the five jobs still present |
| A console-started CDC run keeps going after cutover | the run had no `--config_prefix` ([Known issues](#known-issues-temporary) #5) | `aws glue batch-stop-job-run --job-name "$PROJECT-$TASK_NAME-cdc" --job-run-ids <id>` |

More: [`USAGE_GUIDE.md`](USAGE_GUIDE.md) (monitoring, manual runs) and
[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md) (architecture, limitations, DDL matrix).

---

## Reference

<details>
<summary>Blanks in the repo files, and where each value comes from</summary>

**Filled by you, once:**

| Blank | In | Meaning | Example |
|---|---|---|---|
| `<<BUCKET>>` | state machines, job templates, IAM policies | your pipeline bucket | `my-migration-bucket` |
| `<<*_LAMBDA_ARN>>` | state machines | the 7 Lambda ARNs (Step 4) | `arn:aws:lambda:…:function:dms-dsql-resolve-task` |
| `<<ACCOUNT_ID>>`, `<<REGION>>`, `<<PROJECT>>` | IAM policies (Step 1) | account, region, name prefix | `123456789012`, `us-east-1`, `dms-dsql` |
| `<<DSQL_CLUSTER_ID>>` | IAM policies (Step 1) | first label of the DSQL endpoint | `abcd` |
| `<<GLUE_EXEC_ROLE_NAME>>` | Lambda policy (`iam:PassRole`) | Glue role name | `dms-dsql-glue-exec-role` |
| `<<PREFLIGHT_TASKS_LAMBDA_ARN>>`, `<<STARTUP/CUTOVER_STATE_MACHINE_ARN>>` | fleet state machines (fleet deploy) | the preflight Lambda and the two shared state machine ARNs | — |

**Read from `config/pipeline.json` at run time:** `project`, `region`, `dsql_endpoint`,
`dsql_user`, `dsql_database`, `glue_role_arn`, `glue_connection`, `cdc_engine`,
`cdc_spark_fallback`, `control_schema`. Per task, `config/_task/<task name>/_cdc_engine.json`
(written by an automatic switch to Spark) overrides `cdc_engine`.

**Worked out per task, from `{"taskArn": "..."}`:**

| Value | From | Example |
|---|---|---|
| task name | the DMS task's name, or `taskSuffix`; after the first startup, the name recorded for the ARN in `config/_task_index/<task id>.json` | `task-orders-02` |
| config folder | `s3://<bucket>/config/_task/<task name>/` | `.../config/_task/task-orders-02/` |
| Glue job names | `<project>-<task name>-{discovery,load,load-big,validate,cdc}` | `dms-dsql-task-orders-02-cdc` |
| folder owner record | `config/_task/<task name>/_task.json` | written at first startup (before DMS starts) |
| S3 layout | the DMS S3 endpoint (`BucketFolder`, `TimestampColumnName`, `CsvNullValue`, …) | `cdcRoot` = `.` with no BucketFolder |

</details>

<details>
<summary>What each state machine step runs</summary>

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
need a separate job ([Running many tasks](#running-many-tasks-with-the-fleet)).

**Control tables (in `control_schema`, default `cdc_control`):** `cdc_status`, `cdc_file_status`,
`cdc_chunk_log`, `cdc_apply_exceptions`, `cdc_validation_failures`, `cdc_skipped_ops`.

</details>

<details>
<summary>Fail states, by state machine</summary>

**Startup:** `MissingTaskArn`, `ResolveFailed`, `DriversFailed` (before DMS starts) · `DmsFailed`
(error `DmsTaskFailed`), `DmsTimedOut` (error `DmsPollBudgetExceeded`), `GroupsFailed`,
`PipelineFailed` (before DMS resumes) · `CdcRunFailed`, `CdcRunEnded`, `CdcStartNotConfirmed`,
`CdcFallbackFailed`, `PipelineFailed` (after DMS is in CDC).

**Cutover:** `MissingTaskArn`, `ResolveFailed` (nothing touched) · `CutoverFailed` (DMS may be
stopped — check the failed step), `CdcDrainTimedOut` (error `CdcDrainBudgetExceeded`),
`GlueJobsNotDeleted` (fully cut over bar one job delete).

See [If a run fails](#if-a-run-fails-how-to-continue) for the recovery keyed to each state.

</details>
