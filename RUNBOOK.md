# RUNBOOK — Deploy & Run the Oracle → Aurora DSQL Migration Pipeline

A step-by-step deploy guide you can follow top to bottom. No CDK, no CloudFormation —
just AWS CLI commands you paste in order. Plan for **~1–2 hours** for the first-time
one-time setup (Steps 1–4), then a few minutes per migration task after that.

> **New here?** Read [`README.md`](README.md) first for the big picture, and
> [`USAGE_GUIDE.md`](USAGE_GUIDE.md) for day-to-day operation. This file is the deploy
> checklist.

---

## What you're building (read once — 2 minutes)

You're standing up an automated pipeline that copies data from an Oracle database into
**Amazon Aurora DSQL**, keeps it in sync while the app keeps running, and lets you cut over
with almost no downtime.

The moving parts:

- **AWS DMS** reads Oracle and writes the data to **Amazon S3** as CSV files.
- **AWS Glue** jobs read those CSVs and load / validate / continuously apply them into Aurora DSQL.
- **AWS Step Functions** is the conductor — it runs the whole sequence for you. There are **two
  state machines shared by all tasks** (startup and cutover); you start them with a DMS task's ARN.
  Each run creates that task's Glue jobs and deletes them at cutover. Nothing runs on a schedule;
  you start it by hand.
- **One S3 bucket** holds everything the pipeline needs (scripts, templates, driver files,
  manifests). It's the single source of truth.

**Mental model:** `Oracle → DMS → S3 (CSV) → Glue → Aurora DSQL`, orchestrated by Step Functions.

**The one-time vs. per-task split:**
- **Steps 1–4 (one-time):** create IAM roles, Lambdas, stage files and settings to S3, and create
  the two state machines. Do this once.
- **Steps 5–6 (per migration task):** upload the task's table list and start the startup state
  machine with the task's ARN; later, the same for cutover. Repeat for each DMS task.

**A few terms you'll see** (so nothing below is a surprise):
- **full load** = the one-time bulk copy of all existing rows. **CDC** (change data capture) = the
  ongoing stream of inserts/updates/deletes that happen *after* the full load, kept flowing until cutover.
- **`STOPPED_AFTER_CACHED_EVENTS`** = the DMS status meaning "full load done, changes captured and paused" — the pipeline waits for this before loading into DSQL.
- **cutover** = the final switch: CDC has caught up, so you point your app at Aurora DSQL and stop the old flow.
- **idle** (in the CDC control tables) = that table is fully caught up, nothing pending.
- **state machine** = an AWS Step Functions workflow — the "conductor" that runs the steps for you.

---

## Before you start — prerequisites checklist

Tick all of these before Step 1. The pipeline **loads data into tables that already exist** —
it never creates target tables.

- [ ] **AWS CLI installed and configured** (`aws sts get-caller-identity` returns your account).
- [ ] **An Aurora DSQL cluster** exists, and you know its endpoint (e.g. `abcd.dsql.us-east-1.on.aws`).
- [ ] **A network path from Glue to DSQL**, if your account is locked down (Glue's default network
      can't reach your DSQL cluster). You need a **private subnet** and a **security group** that can
      reach DSQL (through a DSQL VPC endpoint with private DNS on, or a NAT gateway), and the subnet's
      route table needs an **S3 gateway endpoint**. You'll put their IDs in `SUBNET_ID` and
      `SECURITY_GROUP_ID` below, and Step 1b turns them into a Glue network connection. If Glue can
      already reach DSQL, skip this.
- [ ] **Target tables already created in DSQL** — every table you plan to migrate must exist in
      the target schema, with a single-column primary key where possible (best for CDC + validation).
      Tables with a multi-column primary key are left to a separate CDC job (see "Running many tasks").
- [ ] **No more than 9 schemas of your own in the DSQL database.** DSQL allows at most 10 schemas per
      database (not configurable), and the CDC job adds one, `cdc_control`. Over the limit, CDC fails
      at startup with `54000` and the startup run ends at `CdcRunFailed`. Count them with
      `SELECT count(*) FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema') AND schema_name NOT LIKE 'pg_%';`
- [ ] **A DMS task** of type **`full-load-and-cdc`** (the startup checks the settings marked ✔ before
      starting DMS, so a mistake fails in seconds) with:
  - `StopTaskCachedChangesApplied = true` ✔
  - A short **task name** (letters, digits, hyphens, under ~50 characters): it becomes the task's
    folder and Glue job names.
  - An **S3 target endpoint** writing to **your pipeline bucket** ✔ with: `AddColumnName=true` ✔, `TimestampColumnName=dms_timestamp`,
    `Rfc4180=true`, `DatePartitionEnabled=false`, and **no** custom `CdcPath`.
  - A table mapping that **lowercases** schema/table/column names.
  - For any **table without a primary key**: configure DMS to emit **insert/delete only**
    (updates are skipped and logged, not applied).
- [ ] **`pip` and Python 3** available locally (to download the driver files in Step 3).
- [ ] This repo cloned locally (you'll run `aws` commands from its root, referencing `iam/`,
      `lambdas/`, `glue-templates/`, `stepfunctions/`, `scripts/`).

---

## Fill in your values ONCE (then copy-paste the rest)

Set these environment variables in your terminal. **Every command below uses them**, so you
never hand-edit commands — you just paste. (These persist only for your current terminal
session; re-run this block if you open a new terminal.)

```bash
# ---- edit these to your values ----
export BUCKET="my-migration-bucket"          # your ONE source-of-truth S3 bucket (no s3://, no slash)
export ACCOUNT_ID="123456789012"             # your 12-digit AWS account id
export REGION="us-east-1"                     # your AWS region
export PROJECT="dms-dsql"                     # short prefix for role/job names — pick anything; USE THE SAME VALUE EVERYWHERE
export DSQL_ENDPOINT="abcd.dsql.us-east-1.on.aws"   # your Aurora DSQL endpoint host
export DSQL_CLUSTER_ID="abcd"                 # first label of the endpoint (before ".dsql")
export DSQL_USER="admin"
export DSQL_DATABASE="postgres"
export AWS_PAGER=""                            # stops the AWS CLI pager from making commands appear to "hang"

# ---- network: only if Glue must run inside your VPC to reach DSQL (see prerequisites) ----
export SUBNET_ID="subnet-0abc1234"            # private subnet with a route to DSQL + an S3 gateway endpoint
export SECURITY_GROUP_ID="sg-0abc1234"        # must allow all TCP from itself; outbound 443 and 5432
export GLUE_CONNECTION="$PROJECT-vpc"         # EXACT name of your Glue network connection (Step 1b, or one made
                                              # in the console, e.g. "Network connection 1"). "" = no VPC

# ---- derived (do not edit) ----
export SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-lambda-exec-role"
export GLUE_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-glue-exec-role"
echo "Glue role: $GLUE_ROLE_ARN"
```

Per-task values (the DMS task ARN and name) are set later, in Step 5.

> **Why so few values?** The S3 folder names (`scripts/`, `glue-templates/`, `driver-*`,
> `config/`) are **fixed** and already baked into the templates. Per task you only give the DMS
> task's ARN; its name becomes its folder and job names.

---

## The S3 layout (what you're creating in Step 3)

One bucket, fixed folders. You don't invent any prefixes:

```
s3://$BUCKET/
├── scripts/                  # the 4 Glue scripts
├── glue-templates/           # the 6 Glue job templates (CDC has a Python shell and a Spark one)
├── driver-fullload/          # DSQL driver wheels only        (Spark: discovery + load)
├── driver-validation/        # DSQL driver wheels only        (Spark: validate)
├── driver-cdc/               # DSQL wheels + boto3/botocore, Python 3.9, prepared (CDC)
├── cdc/                      # DMS writes CSVs here — you do NOT upload this
└── config/
    ├── pipeline.json         # settings for every task (Step 3c)
    ├── _task_index/          # written at runtime: task ARN -> folder name
    └── _task/<task name>/    # one folder per DMS task (the DMS task's name)
        ├── table_manifest.csv    # you stage this (Step 5b)
        ├── _task.json            # written at first startup: which task ARN owns the folder
        └── _manifest_index.json  # Job 1 writes this at runtime
```

---

## Step 1 — Create the IAM roles (one-time, ~10 min)

**Goal:** create the three roles the pipeline runs as — one for Glue, one for the Lambdas,
one for Step Functions.

**Do this** (from the repo root):

```bash
# Glue execution role
aws iam create-role --role-name $PROJECT-glue-exec-role \
  --assume-role-policy-document file://iam/glue-exec-role.trust.json
aws iam put-role-policy --role-name $PROJECT-glue-exec-role \
  --policy-name glue --policy-document file://iam/glue-exec-role.policy.json
# Only if you use a VPC (GLUE_CONNECTION is set): lets Glue create network interfaces in your subnet
aws iam put-role-policy --role-name $PROJECT-glue-exec-role \
  --policy-name glue-vpc --policy-document file://iam/glue-exec-role.vpc-addon.policy.json

# Lambda execution role
aws iam create-role --role-name $PROJECT-lambda-exec-role \
  --assume-role-policy-document file://iam/lambda-exec-role.trust.json
aws iam put-role-policy --role-name $PROJECT-lambda-exec-role \
  --policy-name lambda --policy-document file://iam/lambda-exec-role.policy.json

# Step Functions execution role
aws iam create-role --role-name $PROJECT-sfn-exec-role \
  --assume-role-policy-document file://iam/sfn-exec-role.trust.json
aws iam put-role-policy --role-name $PROJECT-sfn-exec-role \
  --policy-name sfn --policy-document file://iam/sfn-exec-role.policy.json
```

> The policy files use `<<REGION>>`, `<<ACCOUNT_ID>>`, `<<BUCKET>>`, `<<DSQL_CLUSTER_ID>>`,
> `<<PROJECT>>` and `<<GLUE_EXEC_ROLE_NAME>>`
> placeholders. Substitute your values first — quick one-liner:
> ```bash
> sed -i '' -e "s/<<REGION>>/$REGION/g" -e "s/<<ACCOUNT_ID>>/$ACCOUNT_ID/g" \
>           -e "s/<<BUCKET>>/$BUCKET/g" -e "s/<<DSQL_CLUSTER_ID>>/$DSQL_CLUSTER_ID/g" \
>           -e "s/<<PROJECT>>/$PROJECT/g" \
>           -e "s/<<GLUE_EXEC_ROLE_NAME>>/$PROJECT-glue-exec-role/g" iam/*.json
> grep "<<" iam/*.json    # must print nothing, or a policy will point at a literal <<...>> name
> ```
> (On Linux, use `sed -i` without the `''`.)

**Verify:** `aws iam get-role --role-name $PROJECT-glue-exec-role` returns the role.

---

## Step 1b — Create the Glue network connection (one-time, only if you use a VPC)

**Skip this step if `GLUE_CONNECTION` is `""`.**

**Goal:** let the Glue jobs run inside your VPC so they can reach DSQL. Glue jobs don't take a
subnet or security group directly; they join a VPC through a **Glue network connection**. The
pipeline attaches this connection to every job it creates.

> Don't add the connection to the jobs in the Glue console. The pipeline rewrites each job's
> definition on every run, so a manual change is lost.

**Do this:**

```bash
# Glue requires the security group to allow all TCP from itself (Spark workers talk to each other)
aws ec2 authorize-security-group-ingress --group-id $SECURITY_GROUP_ID --protocol tcp --port 0-65535 \
  --source-group $SECURITY_GROUP_ID --region $REGION --no-cli-pager 2>/dev/null || echo "rule already exists"

AZ=$(aws ec2 describe-subnets --subnet-ids $SUBNET_ID --region $REGION --no-cli-pager \
  --query "Subnets[0].AvailabilityZone" --output text)

aws glue create-connection --region $REGION --no-cli-pager --connection-input "{
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
```bash
aws glue get-connection --name $GLUE_CONNECTION --region $REGION --no-cli-pager \
  --query "Connection.PhysicalConnectionRequirements"
```

**The subnet also needs:**
- an **S3 gateway endpoint** in its route table (the jobs load their scripts, wheels and CSVs from S3)
- a route to DSQL: a DSQL VPC endpoint with **private DNS on**, or NAT. If you use an endpoint, its
  security group must allow inbound 5432 from `$SECURITY_GROUP_ID`
- for the CDC job: the DMS and CloudWatch APIs, through VPC endpoints or NAT

> **The two cutover Lambdas connect to DSQL too.** `drain-check` and `drop-tags` need the same
> network path. After Step 2, put them in the VPC:
> ```bash
> aws iam attach-role-policy --role-name $PROJECT-lambda-exec-role \
>   --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
> for n in drain-check drop-tags; do
>   aws lambda update-function-configuration --function-name $PROJECT-$n --region $REGION --no-cli-pager \
>     --vpc-config SubnetIds=$SUBNET_ID,SecurityGroupIds=$SECURITY_GROUP_ID --query FunctionName
> done
> ```

---

## Step 2 — Create the 7 Lambda functions (one-time, ~15 min)

**Goal:** deploy the small orchestration Lambdas the state machine calls.

All 7 use the **same zip** (all the `.py` files from `lambdas/`) and the **same execution role**
(`$LAMBDA_ROLE_ARN` from Step 1); they differ only by **handler** (which `.py` file's `handler`
function runs) and by **name**. Here's the full set:

| Function name | Handler | What it does |
|---|---|---|
| `$PROJECT-resolve-task` | `resolve_task.handler` | reads the DMS S3 target endpoint settings |
| `$PROJECT-driver-discovery` | `driver_discovery.handler` | lists the driver wheels in each `driver-*` folder |
| `$PROJECT-plan-split` | `plan_split.handler` | splits the table list into balanced load groups |
| `$PROJECT-create-glue-jobs` | `create_glue_jobs.handler` | creates the task's Glue jobs from the templates |
| `$PROJECT-stop-cdc-run` | `stop_cdc_run.handler` | stops the CDC Glue run at cutover |
| `$PROJECT-drain-check` | `drain_check.handler` | **(talks to DSQL)** waits until the last CDC file is applied |
| `$PROJECT-drop-tags` | `drop_tags.handler` | **(talks to DSQL)** drops the `_cdc_file` column at cutover |

Do it **either** with the CLI loop (fast) **or** in the Console (click-through). Both produce the
same 7 functions.

### Option A — CLI (fast, recommended)

```bash
# Zip all lambda code (all .py at the zip root)
cd lambdas && zip -r ../fn.zip . && cd ..

# Create the 7 functions (same zip, different handler each)
for spec in \
  "resolve-task:resolve_task.handler" \
  "driver-discovery:driver_discovery.handler" \
  "plan-split:plan_split.handler" \
  "create-glue-jobs:create_glue_jobs.handler" \
  "stop-cdc-run:stop_cdc_run.handler" \
  "drain-check:drain_check.handler" \
  "drop-tags:drop_tags.handler" ; do
    NAME="${spec%%:*}"; HANDLER="${spec##*:}"
    aws lambda create-function --function-name "$PROJECT-$NAME" \
      --runtime python3.12 --handler "$HANDLER" --memory-size 1024 --timeout 300 \
      --role "$LAMBDA_ROLE_ARN" --zip-file fileb://fn.zip --no-cli-pager
done
```

### Option B — AWS Console (manual, click-through)

---

Do this once **per function** in the table above (7 times), changing only the **name** and
**handler** each time:

1. Go to **AWS Console → Lambda → Create function**.
2. Choose **Author from scratch**.
3. **Function name:** the name from the table (e.g. `dms-dsql-resolve-task`).
4. **Runtime:** **Python 3.12**.
5. **Architecture:** `x86_64` (default).
6. Expand **Change default execution role → Use an existing role**, and pick your
   **`<PROJECT>-lambda-exec-role`** (created in Step 1). *(Not "Create a new role" — you want the
   role that already has the S3/DMS/DSQL/Glue permissions.)*
7. Click **Create function**.
8. On the function page: **Code** tab → **Upload from → .zip file** → upload the `fn.zip` you
   built above (or drag the `lambdas/` files in). **Runtime settings → Edit → Handler:** set it to
   the handler from the table (e.g. `resolve_task.handler`).
9. **Configuration → General configuration → Edit:** set **Memory** to **1024 MB** and **Timeout** to
   **5 min** (300 s). (The driver-discovery function needs this the first time it prepares the CDC
   drivers; the same values are fine for all seven.)
10. **Save.** Repeat for the remaining functions.

### Option C — AWS CloudShell (build in the browser, deploy from S3)

Use this if you're working entirely in the browser (no local machine) — AWS **CloudShell**
already has `aws`, `python`, `pip`, `zip`, and `git` installed. The trick for CloudShell is that
function code over ~50 MB (or when you'd rather not keep it in the shell) is deployed **from an S3
object** with `--code S3Bucket=...,S3Key=...` instead of `--zip-file`.

> **⭐ Already built `fn.zip` on your PC (with `pg8000` inside) and uploaded it to S3? Start here.**
> You do NOT need to clone, `pip install`, or zip anything — the code is already in S3. Just open
> CloudShell and run this one block. It assumes your zip is at
> `s3://<your-bucket>/lambda-code/fn.zip` — change `ZIP_KEY` if you used a different path.
>
> ```bash
> # 1) Set your values (a fresh CloudShell has none of these)
> export BUCKET="my-migration-bucket"                 # the bucket where fn.zip already lives
> export PROJECT="dms-dsql"                            # YOUR project prefix (same one used in Steps 1-2)
> export ACCOUNT_ID="123456789012"
> export LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-lambda-exec-role"
> export ZIP_KEY="lambda-code/fn.zip"                  # the S3 key where YOUR fn.zip lives
> export AWS_PAGER=""                                  # stops the CLI pager from making each command "hang"
>
> # 2) Confirm the zip is actually there
> aws s3 ls "s3://$BUCKET/$ZIP_KEY"
>
> # 3) Create all 7 functions FROM the S3 zip (no local files needed)
> for spec in \
>   "resolve-task:resolve_task.handler" \
>   "driver-discovery:driver_discovery.handler" \
>   "plan-split:plan_split.handler" \
>   "create-glue-jobs:create_glue_jobs.handler" \
>   "stop-cdc-run:stop_cdc_run.handler" \
>   "drain-check:drain_check.handler" \
>   "drop-tags:drop_tags.handler" ; do
>     NAME="${spec%%:*}"; HANDLER="${spec##*:}"
>     aws lambda create-function --function-name "$PROJECT-$NAME" \
>       --runtime python3.12 --handler "$HANDLER" --memory-size 1024 --timeout 300 \
>       --role "$LAMBDA_ROLE_ARN" \
>       --code S3Bucket="$BUCKET",S3Key="$ZIP_KEY" --no-cli-pager
> done
>
> # 4) Verify all 7 exist
> aws lambda list-functions \
>   --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table
> ```
> That's the whole thing for your case. Because `pg8000` is already inside your `fn.zip`, the two
> DSQL functions (`drain-check`, `drop-tags`) are covered — no layer needed. Skip the numbered
> steps below (they're for building the zip from scratch). Next stop: **Step 3 — stage files to S3**
> (if not done) and **Step 4 — create the state machines**.

---

If instead you're starting from nothing in CloudShell, follow the numbered steps:

1. **Open CloudShell** (icon in the AWS Console top bar), then get the code and build the zip:
   ```bash
   # set the same variables you used elsewhere
   export BUCKET="my-migration-bucket"; export PROJECT="dms-dsql"
   export LAMBDA_ROLE_ARN="arn:aws:iam::123456789012:role/dms-dsql-lambda-exec-role"

   # get the lambda code (clone the repo, or upload lambdas/ via CloudShell "Actions -> Upload file")
   git clone https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime.git
   cd Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime/lambdas

   # (the two DSQL functions need pg8000 — bundle it in so all functions share one zip)
   pip install pg8000 -t .
   zip -r ../fn.zip .
   cd ..
   ```

   **Already have the repo downloaded on your PC?** You don't need to `git clone` again — get your
   local copy into CloudShell one of two ways:

   - **Upload the folder into CloudShell**, then build the zip there: in CloudShell click
     **Actions → Upload file** and upload your local `fn.zip` (if you already zipped it on the PC),
     or a zip of the `lambdas/` folder; then unzip/`cd` into it and run the `pip install pg8000 -t .`
     + `zip -r ../fn.zip .` steps above. *(CloudShell's Upload file takes a single file, so zip the
     folder on your PC first.)*
   - **Upload straight to S3 from your PC** (skip building in CloudShell entirely): on your PC, in
     the repo's `lambdas/` folder, run `pip install pg8000 -t .` then zip it, and upload:
     ```bash
     # aws s3 cp  <SOURCE: local file on your PC>  <DESTINATION: S3 path>
     aws s3 cp ~/Downloads/dms-s3-glue-dsql-migration/lambdas/fn.zip s3://$BUCKET/lambda-code/fn.zip
     ```
     Then open CloudShell and jump straight to **step 3** below (create the functions from S3) —
     the zip is already in the bucket, so step 2 is done.
2. **Upload the zip to S3** (CloudShell has no persistent local storage you can point Lambda at,
   so stage it in your bucket):
   ```bash
   # aws s3 cp  <SOURCE: the fn.zip you just built>  <DESTINATION: S3 path>
   aws s3 cp fn.zip s3://$BUCKET/lambda-code/fn.zip
   ```
   > **What is `lambda-code/`?** It's just an arbitrary **staging folder (prefix)** for the zip —
   > **not** one of the pipeline's fixed folders, and **not** read at runtime. You don't need to
   > create it first (`aws s3 cp` makes it), and you can name it anything (`deploy/`, `lambda-zip/`,
   > …) as long as the same key is used in the `--code S3Key=...` below. Safe to delete after the
   > functions are created.
3. **Create the 7 functions from the S3 object** (note `--code` instead of `--zip-file`):
   ```bash
   # If you came straight here (uploaded the zip to S3 from your PC) and this is a FRESH
   # CloudShell, set these three variables first — the loop below uses them:
   export BUCKET="my-migration-bucket"
   export PROJECT="dms-dsql"
   export LAMBDA_ROLE_ARN="arn:aws:iam::123456789012:role/dms-dsql-lambda-exec-role"
   export AWS_PAGER=""     # stops the CLI pager from making each create-function "hang"

   for spec in \
     "resolve-task:resolve_task.handler" \
     "driver-discovery:driver_discovery.handler" \
     "plan-split:plan_split.handler" \
     "create-glue-jobs:create_glue_jobs.handler" \
     "stop-cdc-run:stop_cdc_run.handler" \
     "drain-check:drain_check.handler" \
     "drop-tags:drop_tags.handler" ; do
       NAME="${spec%%:*}"; HANDLER="${spec##*:}"
       aws lambda create-function --function-name "$PROJECT-$NAME" \
         --runtime python3.12 --handler "$HANDLER" --memory-size 1024 --timeout 300 \
         --role "$LAMBDA_ROLE_ARN" \
         --code S3Bucket=$BUCKET,S3Key=lambda-code/fn.zip --no-cli-pager
   done
   ```
   > Make sure the `S3Key` here (`lambda-code/fn.zip`) matches the path you uploaded the zip to
   > (Step 2, or your PC upload). If you used a different prefix/name, change it in both places.
   > To **update** a function's code later after re-uploading the zip:
   > `aws lambda update-function-code --function-name "$PROJECT-<name>" --s3-bucket $BUCKET --s3-key lambda-code/fn.zip`

Because `pg8000` is bundled into `fn.zip` here, the two DSQL functions (`drain-check`, `drop-tags`)
are already covered — no separate layer needed. The `lambda-code/` prefix is just a staging spot;
it's not read at runtime (only at create/update time), so you can delete it afterward if you like.

---

### The two DSQL functions need the `pg8000` library

`drain-check` and `drop-tags` connect to Aurora DSQL, so they need the `pg8000` Python package.
The other 5 functions do **not**. If you skip this, those two fail at **cutover (Step 6)** with
`No module named 'pg8000'`. Two ways to provide it:

**Option 1 — bundle pg8000 into the zip (simplest):** install it alongside the code before zipping,
so it's inside `fn.zip` for all functions.
```bash
cd lambdas
pip install pg8000 -t .        # installs pg8000 (+scramp, asn1crypto) into this folder
zip -r ../fn.zip .             # now the zip contains the lambda code AND pg8000
cd ..
```

**Option 2 — a Lambda layer (cleaner; keeps function zips small):** build a `pg8000` layer once and
attach it to just the two DSQL functions.
```bash
# Build the layer zip (Lambda expects libs under python/)
mkdir -p layer/python
pip install pg8000 -t layer/python/
cd layer && zip -r ../pg8000-layer.zip python && cd ..

# Publish the layer
LAYER_ARN=$(aws lambda publish-layer-version --layer-name pg8000 \
  --zip-file fileb://pg8000-layer.zip \
  --compatible-runtimes python3.12 \
  --query LayerVersionArn --output text)

# Attach it to the two DSQL functions
aws lambda update-function-configuration --function-name "$PROJECT-drain-check" --layers "$LAYER_ARN"
aws lambda update-function-configuration --function-name "$PROJECT-drop-tags"  --layers "$LAYER_ARN"
```
> In the Console, the layer equivalent is: function page → scroll to **Layers → Add a layer →
> Custom layers →** pick `pg8000` → the version → **Add**. Do it for `drain-check` and `drop-tags`.

**Verify:** `aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName"`
lists all 7.

---

## Step 3 — Stage files to S3 (one-time, ~20 min)

**Goal:** put the scripts, templates, and driver files in the bucket. This is where the
driver-folder rules matter most — read the callout.

### 3a — Scripts and templates

```bash
# The 4 Glue scripts
aws s3 cp scripts/ s3://$BUCKET/scripts/ --recursive \
  --exclude "*" --include "job1_discovery.py" --include "job2_load.py" \
  --include "job3_validate.py" --include "glue_cdc_continuous.py"

# The 6 job templates (substitute <<BUCKET>> inside them first)
sed -i '' "s/<<BUCKET>>/$BUCKET/g" glue-templates/*.json      # Linux/CloudShell: drop the ''
aws s3 cp glue-templates/ s3://$BUCKET/glue-templates/ --recursive --exclude "*" --include "*.json"
```

> **Upgrading an existing deployment? Upload the scripts too.** The Glue scripts in
> `s3://$BUCKET/scripts/` are a separate upload from the Lambdas and state machines. If you
> redeploy the Lambdas and workflows but not the scripts, the jobs keep running the OLD scripts
> (a real test run hit an old `job2_load.py` this way and failed at its summary with
> `KeyError: 'table'`). On every upgrade, redo 3a, then check that S3 matches your copy of the repo:
>
> ```bash
> for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py; do
>   L=$(sha256sum < scripts/$f | cut -c1-16)
>   S=$(aws s3 cp s3://$BUCKET/scripts/$f - | sha256sum | cut -c1-16)
>   [ "$L" = "$S" ] && echo "OK    $f" || echo "STALE $f  (repo $L, S3 $S)"
> done
> ```
>
> Every line must say `OK`. Glue reads a script only when a run starts, so a CDC run that is
> already running keeps the old script: stop it, wait for `STOPPED`, and start a new run
> (with `--config_prefix`).
>
> Also check that the two cutover Lambdas have `pg8000`, either in a layer or inside the zip
> (Step 2). Without it, cutover fails at `DrainCheck` with `No module named 'pg8000'`:
>
> ```bash
> for f in drain-check drop-tags; do
>   LAYERS=$(aws lambda get-function-configuration --function-name $PROJECT-$f \
>     --query 'Layers[].Arn' --output text --no-cli-pager)
>   curl -s -o /tmp/$f.zip "$(aws lambda get-function --function-name $PROJECT-$f \
>     --query Code.Location --output text --no-cli-pager)"
>   N=$(unzip -l /tmp/$f.zip | grep -c 'pg8000/')
>   echo "$f: pg8000 files in zip=$N, layers=[$LAYERS]"
> done
> ```
>
> Each function needs either `pg8000 files in zip` above 0 or a `pg8000` layer listed.
>
> Upgrading to the version that prepares the CDC drivers automatically also needs, once:
>
> ```bash
> aws lambda update-function-configuration --function-name $PROJECT-driver-discovery \
>   --memory-size 1024 --timeout 300 --no-cli-pager
> # the startup workflow now checks that the CDC run started (needs s3:ListBucket on config/_task/*)
> aws iam put-role-policy --role-name $PROJECT-sfn-exec-role \
>   --policy-name sfn --policy-document file://iam/sfn-exec-role.policy.json   # filled in as in Step 1
> # resolve-task now refuses a second run for the same task (needs states:ListExecutions/DescribeExecution)
> aws iam put-role-policy --role-name $PROJECT-lambda-exec-role \
>   --policy-name lambda --policy-document file://iam/lambda-exec-role.policy.json   # filled in as in Step 1
> ```

### 3b — Driver files (the part people get wrong)

The Glue jobs can't reach PyPI, so their Python dependencies are staged in S3 as `.whl`
files. There are **three** driver folders because each job type needs a different set:

| Folder | Used by | Put in it | Do NOT put in it |
|---|---|---|---|
| `driver-fullload/` | discovery + load (Spark, Python 3.10) | **pg8000 stack only** (5 wheels) | ❌ boto3 / botocore |
| `driver-validation/` | validate (Spark, Python 3.10) | **pg8000 stack only** (5 wheels) | ❌ boto3 / botocore |
| `driver-cdc/` | CDC (Python shell, **Python 3.9**) | pg8000 stack **+** boto3 set, downloaded for Python 3.9 and **prepared** (below) | — |

- **pg8000 stack (5 wheels):** `pg8000`, `scramp`, `asn1crypto`, `python_dateutil`, `six`
- **boto3 set (cdc only):** `boto3`, `botocore`, `jmespath`, `s3transfer`, `urllib3`

> ⚠️ **Why the split matters:** the pipeline loads **every** wheel it finds in a folder. If a
> `boto3`/`botocore` wheel ends up in `driver-fullload/` or `driver-validation/`, it breaks the
> Spark jobs with `DataNotFoundError: endpoints`. Keep those two folders to the 5 pg8000 wheels
> only. (The Spark jobs get their boto3 from `driver-cdc/` a different way; the CDC job needs it
> bundled because Glue's built-in boto3 is too old to know Aurora DSQL.)

> ⚠️ **`driver-cdc/` must be the Python 3.9 set.** Glue Python shell jobs run Python 3.6 or 3.9
> only. boto3/botocore dropped Python 3.9 at 1.43, scramp at 1.4.7, and on Python 3.9 botocore needs
> urllib3 below 1.27. The `pip download` below pins those.

**You don't prepare the CDC wheels by hand.** Glue's pip installs the CDC job's wheels one at a
time, and a wheel that lists a dependency not installed yet makes pip ask pypi.org, which a firewall
blocks (~20 min, then `CalledProcessError`). The startup workflow handles this for every task, before
DMS starts:
- the `driver-discovery` Lambda checks the `driver-cdc/` set for Python 3.9 (one version per package,
  each wheel allows Python 3.9, dependency versions in range, all 10 packages present, botocore
  knows `dsql`). A wrong or missing wheel stops the run at **`DriversFailed`**, naming the wheel and
  what to download instead. DMS is not started;
- it then writes install-safe copies (dependency lists removed, everything else unchanged) to
  `driver-cdc-prepared/<fingerprint>/`, with `MANIFEST.txt` (original and new checksum of each wheel
  and every line removed, for your security team). This happens **once per wheel set**; later tasks
  reuse it in seconds. Your files in `driver-cdc/` are never changed;
- every CDC job is created with the prepared list, so a console **Run** uses it too.

Wheels you already prepared by hand are fine: they pass through unchanged. The Spark jobs don't
need any of this (Glue puts their wheels on the Python path without pip).

**Download the wheels** on a machine that can reach PyPI (your laptop, or CloudShell). They're
pinned to Glue's platform, not your laptop's:

```bash
rm -rf _drv _cdc
PLAT310="--platform manylinux2014_x86_64 --python-version 310 --only-binary=:all:"
PLAT39="--platform manylinux2014_x86_64 --python-version 39 --only-binary=:all:"

# Spark jobs (Python 3.10): the 5 pg8000-stack wheels
pip download pg8000 $PLAT310 -d _drv/

# CDC job (Python 3.9): pg8000 stack + boto3 set, capped to releases that support 3.9
pip download "pg8000>=1.31,<1.32" "scramp>=1.4.5,<1.4.7" \
  "boto3>=1.35,<1.43" "botocore>=1.35,<1.43" "urllib3>=1.25.4,<1.27" $PLAT39 -d _cdc/
```

**Upload to the three folders.** Clear `driver-cdc/` first: two versions of one package in that
folder is an error.

```bash
# Spark folders = pg8000 stack ONLY
aws s3 cp _drv/ s3://$BUCKET/driver-fullload/   --recursive --exclude "*" --include "*.whl"
aws s3 cp _drv/ s3://$BUCKET/driver-validation/ --recursive --exclude "*" --include "*.whl"
# CDC folder = the Python 3.9 set, as downloaded
aws s3 rm s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"
aws s3 cp _cdc/ s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"
```

**Verify:**

```bash
aws s3 ls s3://$BUCKET/driver-fullload/     # expect 5 wheels, ZERO boto3/botocore
aws s3 ls s3://$BUCKET/driver-validation/   # expect 5 wheels, ZERO boto3/botocore
aws s3 ls s3://$BUCKET/driver-cdc/          # expect 10 wheels, one per package:
#   asn1crypto boto3 botocore jmespath pg8000 python_dateutil s3transfer scramp six urllib3
```

> **Optional: check the CDC set before uploading.** The Lambda runs the same code as
> `lambdas/prepare_cdc_wheels.py`, which you can run locally (standard Python only, no AWS access):
> `python3 lambdas/prepare_cdc_wheels.py _cdc/ /tmp/_cdc_check/ --python 3.9` must end with `PASS`.

### 3c — Pipeline settings (`config/pipeline.json`, once for all tasks)

Every startup and cutover run reads this one file, so these values are set once for every task.
An edit applies to runs started afterwards, not to runs already going. This writes the file from
the variables you set at the top:

```bash
: "${PROJECT:?}" "${REGION:?}" "${BUCKET:?}" "${DSQL_ENDPOINT:?}" "${GLUE_ROLE_ARN:?}" "${GLUE_CONNECTION?}"
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
    "control_schema": "cdc_control",
}
open("pipeline.json", "w").write(json.dumps(cfg, indent=2) + "\n")
print(json.dumps(cfg, indent=2))
EOF
aws s3 cp pipeline.json s3://$BUCKET/config/pipeline.json
```

| Key | What it is |
|---|---|
| `project` | prefix of your Lambda and Glue job names; must be the same `$PROJECT` as Steps 1–2 |
| `region` | AWS region of the DMS tasks and the pipeline |
| `dsql_endpoint`, `dsql_user`, `dsql_database` | the Aurora DSQL target |
| `glue_role_arn` | the Glue execution role from Step 1 |
| `glue_connection` | the Glue network connection the jobs run in, by its **exact** name (one made in the console may be called e.g. `Network connection 1`). Several: comma-separated. `""` = no VPC |
| `cdc_engine` | `pythonshell` (default; 1 DPU, ~$0.44/h per task) or `spark` (Glue 4.0, 2 × G.1X, ~$0.88/h per task; loads its drivers the same way as the full-load jobs) |
| `control_schema` | DSQL schema for the CDC control tables (default `cdc_control`) |

A template with every key is in [`config/pipeline.example.json`](config/pipeline.example.json).
Before editing the live file later, keep a dated copy:
`aws s3 cp s3://$BUCKET/config/pipeline.json s3://$BUCKET/config/pipeline.json.$(date +%Y%m%d%H%M)`.

> You do **not** upload anything to `s3://$BUCKET/cdc/` — DMS writes there itself (that's your
> DMS task's S3 target). Each task's table list is uploaded per task, in Step 5.

---

## Step 4 — Create the two state machines (one-time, ~5 min)

**Goal:** set up the two "conductors" that run every migration task:

- **startup** — runs the whole migration: full load → validate → switch DMS to CDC → start the continuous CDC job.
- **cutover** — run later, when you're ready to switch over: drains the last changes and cleans up.

**Both are shared by all tasks.** You start them with just a task's ARN (Step 5). At runtime they
read everything else from `config/pipeline.json` (Step 3c) and from the DMS task itself — including
the task's **name**, which becomes its folder (`config/_task/<task name>/`) and the middle of its
Glue job names (`$PROJECT-<task name>-load`, …). So the definition files in `stepfunctions/` have
only two kinds of blanks: `<<BUCKET>>` and the Lambda ARNs. You fill them in once.

> **Updating an existing deployment?** Redeploy the Lambda code first (Step 2: same zip, all 7
> functions), because the shared state machines need the current `resolve-task` and
> `create-glue-jobs`. Tasks already running on older **per-task** state machines
> (`$PROJECT-startup-<suffix>`) keep working with the new Lambdas, including their cutover — leave
> them as they are and use the shared pair for new tasks.

> **Coming in cold (did Steps 1–3 in the console)?** Set just these, matching the names you
> actually created, then run the name check:
>
> ```bash
> export PROJECT="dms-dsql" REGION="us-east-1" ACCOUNT_ID="123456789012" BUCKET="my-migration-bucket"
> export SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role" AWS_PAGER=""
> for n in resolve-task driver-discovery plan-split create-glue-jobs stop-cdc-run drain-check drop-tags; do
>   printf "%-42s " "$PROJECT-$n"
>   aws lambda get-function --function-name "$PROJECT-$n" \
>     --query "Configuration.FunctionName" --output text --no-cli-pager 2>/dev/null \
>     || echo "❌ NOT FOUND"
> done
> aws s3 cp s3://$BUCKET/config/pipeline.json - | head -20      # Step 3c must be done
> ```
>
> Every Lambda line should echo its own name. A **`❌ NOT FOUND`** means `$PROJECT` doesn't match
> what you named the functions: fix `$PROJECT` (or the function names) before 4a.

### 4a — Fill in the blanks

```bash
: "${PROJECT:?set PROJECT}" "${REGION:?set REGION}" "${ACCOUNT_ID:?set ACCOUNT_ID}" "${BUCKET:?set BUCKET}"
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
grep '<<' startup.filled.asl.json cutover.filled.asl.json      # must print nothing
```

> **How the Lambda ARNs are built:** `arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT-<name>`,
> e.g. `arn:aws:lambda:us-east-1:123456789012:function:dms-dsql-resolve-task`. This works because
> you created the functions as `$PROJECT-<name>` in Step 2. If an ARN points at a name that doesn't
> exist, the run fails with `Lambda function not found`.

### 4b — Register them (creates them, or updates them if they already exist)

```bash
for f in startup cutover; do
  ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?name=='$PROJECT-$f'].stateMachineArn" --output text --no-cli-pager)
  if [ -n "$ARN" ]; then
    aws stepfunctions update-state-machine --state-machine-arn "$ARN" \
      --definition file://$f.filled.asl.json --no-cli-pager
  else
    aws stepfunctions create-state-machine --name "$PROJECT-$f" \
      --definition file://$f.filled.asl.json --role-arn "$SFN_ROLE_ARN" --no-cli-pager
  fi
done

aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-startup' || name=='$PROJECT-cutover'].name" \
  --output table --no-cli-pager                                  # expect both names
```

---

## Step 5 — Run a migration task (per task)

**Goal:** migrate one DMS task. Repeat this step for every task; nothing from Steps 1–4 changes.

### 5a — Pick the task

```bash
export TASK_ARN="arn:aws:dms:us-east-1:123456789012:task:XXXX"     # the DMS task to migrate
export TASK_NAME=$(aws dms describe-replication-tasks \
  --filters Name=replication-task-arn,Values=$TASK_ARN \
  --query 'ReplicationTasks[0].ReplicationTaskIdentifier' --output text --no-cli-pager)
export CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_NAME/"
echo "task=$TASK_NAME  folder=$CONFIG_PREFIX  jobs=$PROJECT-$TASK_NAME-*"
```

The DMS task's **name** is its folder and its Glue job names. Keep names short (under ~50
characters) and use letters, digits and hyphens — DMS allows nothing else. Don't rename a task
while it's being migrated (if you do, the pipeline keeps using the name it started with).

### 5b — Stage the task's table list

Create a CSV listing the tables this task migrates, then upload it. Header row required, two
columns (`dms_schema,dms_table`); the pipeline lowercases them to find the DSQL target:

```csv
dms_schema,dms_table
SRC_SCHEMA,MY_TABLE
SRC_SCHEMA,ANOTHER_TABLE
```

```bash
aws s3 cp table_manifest.csv "${CONFIG_PREFIX}table_manifest.csv"
```

### 5c — Start it

```bash
STARTUP_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-startup'].stateMachineArn" --output text --no-cli-pager)

aws stepfunctions start-execution --state-machine-arn "$STARTUP_ARN" \
  --name "${TASK_NAME:0:60}-$(date +%Y%m%d%H%M)" \
  --input "{\"taskArn\":\"$TASK_ARN\"}" --no-cli-pager
```

The run name starts with the task name, so you can find each task's runs in the Step Functions
console.

**What happens (in order), so you can follow along in the console:**
1. **Check the task (seconds).** Reads `config/pipeline.json`, works out the folder and Glue job
   names from the task name, and checks the DMS task **before starting it**: `full-load-and-cdc`,
   `StopTaskCachedChangesApplied=true`, `AddColumnName=true`, writing to the pipeline bucket, and not
   already past its full load. It also records which task owns the folder
   (`config/_task/<task name>/_task.json`). Any problem stops the run at **`ResolveFailed`** with the
   reason, and DMS is never started.
2. **Check the driver files (seconds; about a minute the first time).** Lists the three driver
   folders; for a Python-shell CDC job, checks `driver-cdc/` for Python 3.9 and prepares it (Step 3b).
   A wrong or missing wheel stops the run at **`DriversFailed`** with the wheel named, and DMS is
   never started.
3. Start the DMS task → wait for full load to finish (`STOPPED_AFTER_CACHED_EVENTS`), then create
   this task's Glue jobs from the templates (the CDC job as Python shell or Spark, per `cdc_engine`).
4. Run **Job 1** (discovery) → **Job 2** (load) → **Job 3** (validate), per table group.
   **If any group's load or validation fails, the run stops here (`GroupsFailed`) and DMS stays
   paused**, so CDC never starts on top of an incomplete load.
5. Resume DMS into ongoing CDC and start the **continuous CDC job**. It applies changes to each
   table only once that table's load is marked `done` in its group's status file
   (`${CONFIG_PREFIX}_orchestrator/group-<n>/_load_status.json`).
6. **Confirm CDC really started.** Every 30 s the run checks the CDC job: it succeeds only once the
   job writes `config/_task/<task name>/_cdc_started/<execution name>.json`, which it does on
   reaching its poll loop (drivers installed, DSQL reachable, manifest loaded). If the CDC run fails
   or stops first, the run ends at **`CdcRunFailed`** (with Glue's error) or **`CdcRunEnded`**; if
   it never confirms within 45 minutes, **`CdcStartNotConfirmed`**. The full load is done and DMS
   is capturing changes in all three cases, so you only need to fix and restart the CDC job.

> **If a step fails:** fix the cause, then **start a new execution with the same input.** Each
> stage skips already-completed work (via S3 status files), and job creation is idempotent — so a
> re-run safely resumes from where it stopped. A second startup for a task whose startup is
> still running stops at **`ResolveFailed`** ("Another startup run is already running"), so a
> task can't be loaded twice.

> **Optional input keys** (rarely needed): `"taskSuffix": "<name>"` uses a different folder/job name
> than the DMS task name; `"adoptExistingFolder": true` lets a task use a folder that already holds
> files from a run made before the shared state machines (only if those files are this task's).

### 5d — Check that CDC is applying

The run shows **Succeed** once the CDC job has reached its poll loop (step 6 above). To see it
applying changes, check the CDC job's log:

```bash
JOB=$PROJECT-$TASK_NAME-cdc
RUN=$(aws glue get-job-runs --job-name $JOB --max-items 1 --query 'JobRuns[0].Id' --output text --no-cli-pager)
aws glue get-job-run --job-name $JOB --run-id $RUN --query 'JobRun.JobRunState' --no-cli-pager
LG=$([ "$(aws glue get-job --job-name $JOB --query Job.Command.Name --output text --no-cli-pager)" = pythonshell ] \
  && echo /aws-glue/python-jobs/output || echo /aws-glue/jobs/output)
aws logs tail $LG --log-stream-names $RUN --since 1h | grep -E "Full-load gate|poll loop|CANNOT" | tail -5
```

Healthy: `RUNNING`, then `Full-load gate: N/N table(s) marked 'done'` and `entering poll loop`.
(See `USAGE_GUIDE.md` → Monitoring for the `cdc_control` queries to watch CDC progress.)

---

## Step 6 — Cut over (per task, when you're ready to switch the app)

**Goal:** once CDC has caught up (all tables idle, source ≈ target row counts), finalize and
switch the application to Aurora DSQL.

**Do this** (with `TASK_ARN` and `TASK_NAME` set as in 5a):

```bash
CUTOVER_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-cutover'].stateMachineArn" --output text --no-cli-pager)

aws stepfunctions start-execution --state-machine-arn "$CUTOVER_ARN" \
  --name "cutover-${TASK_NAME:0:50}-$(date +%Y%m%d%H%M)" \
  --input "{\"taskArn\":\"$TASK_ARN\"}" --no-cli-pager
```

It stops the DMS task, drain-checks that the last CDC file was applied, stops this task's CDC
run, removes the pipeline's internal `_cdc_file` tracking column, and deletes this task's Glue
jobs. It finds the task's folder and jobs by its ARN, so it works even if the DMS task was renamed.
**Then you** repoint your application to Aurora DSQL. Other tasks are unaffected.

---

## Doing more than one task

Steps 1–4 are done once. For each additional DMS task, repeat **Step 5** (pick the task, stage
its table list, start) and later **Step 6**. Nothing else changes.

- Each task gets its own folder, its own Glue jobs and its own CDC run. One task failing or
  cutting over never affects another.
- **Each source table belongs to exactly one task.** All tasks share the same S3 target, and DMS
  writes each table to its own `<schema>/<table>/` folder, so two tasks with the same table would
  write into the same files.
- **Stagger startups** (or raise your account's Glue concurrency limit): each load job can use up
  to 10 G.4X workers, and several full loads at once can hit the limit.
- **Reusing a deleted task's name:** the startup refuses to use a folder another task ARN created
  (its old status files would make CDC skip tables this task never loaded). Archive the old folder
  first — the error message gives the command.

---

## Running many tasks, and what happens if runs overlap

Run as many tasks at once as you like: start one startup execution per DMS task. Each task has its
own folder, its own five Glue jobs and its own CDC job. What the pipeline guarantees when runs
overlap:

| Situation | What happens |
|---|---|
| Many tasks started together | They run independently. The first task to need the CDC drivers prepares them (about a minute); the others reuse that set or, if they start in the same moment, prepare an identical copy. Many CDC jobs creating the shared `cdc_control` tables at once retry automatically. |
| A second startup for the same task while the first is still running | Stops at `ResolveFailed` before touching DMS or S3. The same applies to two cutover runs for one task. |
| The same CDC job started twice (console **Run** plus the workflow, say) | Glue refuses the second run: the CDC jobs allow one run at a time. |
| Two **different** CDC jobs applying the same table (an old per-task workflow next to the shared one, a hand-made copy, two DMS tasks that include the same table) | Each change is still applied once, in order: every step checks that the table's progress in `cdc_control.cdc_status` is where this run left it, and DSQL rejects the slower of two simultaneous steps. The run that loses that check leaves the table for that cycle and logs `another CDC run is applying this table`. This is wasted work, so find and stop the extra job. Cutover only stops this task's own CDC job. |
| A table set to `blocked` while a run is applying it | The run stops that table at its next step. |
| A table with a multi-column primary key | The CDC job leaves it alone and lists it at startup; run your separate multi-column-key CDC job for it. Cutover waits for it (see Troubleshooting). |
| A load job started by hand while the workflow runs the same group | **Not guarded.** Tables with a primary key are safe (duplicate rows are rejected), but tables without one can get duplicate rows, which validation then reports. Don't start load jobs by hand during a run. |

Limits to plan for: Glue's concurrent-run and DPU quotas, and DSQL's connection quota. Each load
group opens up to `max_writers_per_loader` connections (100-150 by default), so ten tasks loading
at the same time can open a few thousand.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Each `aws` command seems to **hang** until you press **Ctrl-C**, then the next one runs (e.g. only 1 Lambda created per loop) | the AWS CLI **pager** is paging the JSON output and waiting for you to quit it | run `export AWS_PAGER=""` (and/or add `--no-cli-pager`) before the loop, then re-run it — each command returns on its own. Any functions you already Ctrl-C'd were still created; the re-run reports them as already-exists and fills in the rest |
| Spark job fails `DataNotFoundError: endpoints` | a `boto3`/`botocore` wheel leaked into `driver-fullload/` or `driver-validation/` | remove it — those folders hold the 5 pg8000 wheels ONLY (re-check Step 3b verify) |
| Any Glue job fails `UnknownServiceError: Unknown service: 'dsql'` | Glue 4.0's bundled boto3 predates DSQL, and `driver-cdc/` is missing the modern boto3 set (the Spark jobs also get boto3 from there) | upload the boto3 set to `driver-cdc/` (Step 3b) and redeploy the Lambdas |
| Glue job fails `InterfaceError: Can't create a connection to host ...dsql... port 5432` | the job isn't running inside your VPC (no Glue connection attached) | create the connection (Step 1b), put its exact name in `glue_connection` in `config/pipeline.json` (Step 3c), then start the task again. Check: `aws glue get-job --job-name <job> --query Job.Connections` |
| Startup stops at `ResolveFailed` | the task or settings check failed before DMS was started | the execution's error says why: a DMS task setting (`StopTaskCachedChangesApplied`, `AddColumnName`, wrong bucket, task already past full load), a `pipeline.json` problem (missing file, missing key, leftover `<...>`), or the folder owner check (next row). Fix it and start again with the same input |
| `ResolveFailed` with `FolderOwnerError` | `config/_task/<task name>/` was created by a different task ARN (a deleted task's name was reused), or holds files from an older run with no owner record | archive the folder (the error gives the `aws s3 mv` command) or pass a different `taskSuffix`; if the files are this task's own, start with `"adoptExistingFolder": true` |
| Startup stops at `MissingTaskArn` | started without input | start with `--input '{"taskArn":"arn:aws:dms:..."}'` (Step 5c) |
| Startup stops at `DriversFailed` (`DriverCheckError`) | a `driver-cdc/` wheel can't work in the Python-shell CDC job (needs Python 3.10, e.g. scramp 1.4.7+ or boto3/botocore 1.43+; urllib3 2.x; two versions of one package; a missing package; a `.zip`), or a driver folder has no pg8000 | the error names the wheel and what to use instead. Fix `driver-cdc/` (Step 3b) and start again with the same input; DMS was not started |
| Startup ends at `CdcRunFailed` with `54000` and a schema-count message | the DSQL database already has 10 schemas, so the CDC job can't create `cdc_control` | drop unused schemas (test leftovers), or migrate into fewer schemas; DSQL's limit of 10 schemas per database can't be raised |
| `ResolveFailed`: `Another startup run is already running for this DMS task` | a startup for this task is still running (or stuck) | wait for it, or stop it in the Step Functions console, then start again |
| Cutover `DrainCheck` keeps listing a table with a multi-column primary key in `pending` | this CDC job doesn't apply those tables (its startup log lists them); the separate multi-column-key CDC job isn't running, or doesn't mark files `done` in `cdc_control.cdc_file_status` under the same `<schema>.<table>` name | start that job and let it catch up; the drain check then passes |
| CDC log: `another CDC run is applying this table` | two different CDC jobs (or runs) are applying the same table | find the extra one: `aws glue get-job-runs` on each CDC job, or look for an old per-task workflow's job. Stop all but this task's own CDC job. No change was applied twice |
| `DriversFailed` with `prepare_cdc_wheels.py is missing from this Lambda's zip` | the Lambdas were updated from an `fn.zip` built before this file was added to `lambdas/` | rebuild `fn.zip` from the current `lambdas/` folder and update the functions (Step 2) |
| `DriverDiscoveryCdc` fails with `Task timed out` or out of memory | the driver-discovery Lambda still has the old 128 MB / 2 min settings (the first preparation unpacks botocore) | `aws lambda update-function-configuration --function-name $PROJECT-driver-discovery --memory-size 1024 --timeout 300`, then start again |
| Startup ends at `CdcRunFailed` or `CdcRunEnded` | the CDC run failed or stopped right after starting (the error is Glue's own message) | full load is done and DMS is capturing changes. Read the CDC run's log, fix the cause, then start the CDC job with `--config_prefix` (USAGE_GUIDE → Monitoring) |
| Startup ends at `CdcStartNotConfirmed` | the CDC run is still running but never wrote its start marker within 45 min | check the CDC log for `entering poll loop` and `start marker`. If the log shows the poll loop, the run is fine: check that the Step Functions role has the `ConfirmCdcStarted` statement (Step 1) and the Glue role can write to `config/_task/` |
| CDC job fails `...whl installation failed ... CalledProcessError` after ~20 min, or its log shows `pypi.org` timeouts | the job was started with the raw `driver-cdc/` list (an older per-task workflow, or a hand-made start) | use the shared startup (it saves the prepared list on the job), or start the job without `--extra-py-files` so it uses the saved list |
| `CreateGlueJobs` fails `not authorized to perform: iam:PassRole` | `<<GLUE_EXEC_ROLE_NAME>>` wasn't replaced in the Lambda policy | re-run the Step 1 fill-in command and `put-role-policy` for the Lambda role |
| A driver job fails "no pg8000" | driver folder empty or wrong-platform wheels | re-run the platform-pinned `pip download` (Step 3b) and re-upload |
| Load "SUCCEEDED" but 0 rows loaded | a stale per-group `_load_status.json` marks tables done | `aws s3 rm ${CONFIG_PREFIX}_orchestrator/ --recursive` (the group plan is rebuilt on the next run), then re-run |
| Startup run ends in `GroupsFailed` (`GroupLoadOrValidateFailed`) | a table group's load or validation failed, so the run stopped before resuming DMS | open the `GroupFanOut` step in the execution to see which group failed, read that Glue job run's log, fix it, and start a new execution (finished files and tables are skipped) |
| CDC job keeps logging `full load not done ... waiting` | the CDC job can't see the per-group status files, or a table's load really isn't done | make sure `s3://$BUCKET/scripts/glue_cdc_continuous.py` is the current version (it reads `_orchestrator/group-*/_load_status.json`), then check `aws s3 ls ${CONFIG_PREFIX}_orchestrator/ --recursive \| grep _load_status` |
| `processed/_manifest.json` shows `pending_copy` that doesn't go down | S3 copies to `processed/` keep failing (throttling, permissions) | nothing is lost (originals are kept and retried each cycle); see `last_copy_error` in the manifest and the CDC log for `copy to processed/ failed` |
| CDC runs but applies 0 rows | CDC looking in the wrong S3 folder | confirm the DMS S3 target matches where the CDC job reads (auto-derived; see USAGE_GUIDE) |
| Text values such as `NA` or `NONE` are NULL in DSQL | rows loaded or changed by a version before the NULL-marker fix (which treated them as NULL) | deploy the current scripts (Step 3a), then reload the affected tables or correct the rows (USAGE_GUIDE §4b has the query to find them) |
| A table shows `blocked` in `cdc_control.cdc_status` | a `DROP COLUMN` on the source, or a row DSQL rejected (e.g. NULL into a NOT NULL column) | fix the cause (drop the column on the DSQL target / allow NULL or fix the source row), then `UPDATE cdc_control.cdc_status SET status='active' WHERE table_name='<schema.table>'`; CDC resumes the blocked file at its saved offset. Never delete the row: applied files stay in the folder and would all be replayed |
| `drain-check` / `drop-tags` Lambda errors | missing `pg8000` on those two Lambdas | attach a pg8000 layer or bundle it into the zip (Step 2) |

For deeper operation, monitoring queries, schema-change handling, and clean-slate reloads, see
**[`USAGE_GUIDE.md`](USAGE_GUIDE.md)**. For architecture, known limitations, and the DDL
support matrix, see **[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md)**.

---

## Reference — what runs, and the runtime contract

<details>
<summary>Blanks in the repo files, and where each value comes from (click to expand)</summary>

**Filled once, by you:**

| Blank | Where | Meaning | Example |
|---|---|---|---|
| `<<BUCKET>>` | state machines, job templates, IAM policies | your pipeline bucket (no `s3://`, no slash) | `my-migration-bucket` |
| `<<*_LAMBDA_ARN>>` | state machines | the 7 Lambda ARNs (Step 2), built in Step 4a | `arn:aws:lambda:…:function:dms-dsql-resolve-task` |
| `<<ACCOUNT_ID>>` / `<<REGION>>` / `<<PROJECT>>` | IAM policies (Step 1) | account, region, name prefix | `123456789012` / `us-east-1` / `dms-dsql` |
| `<<DSQL_CLUSTER_ID>>` | IAM policies (Step 1) | first label of the DSQL endpoint | `abcd` |
| `<<GLUE_EXEC_ROLE_NAME>>` | Lambda IAM policy (`iam:PassRole`) | Glue role name | `dms-dsql-glue-exec-role` |

**Read from `config/pipeline.json` at runtime (Step 3c):** `project`, `region`, `dsql_endpoint`,
`dsql_user`, `dsql_database`, `glue_role_arn`, `glue_connection`, `cdc_engine`, `control_schema`.

**Worked out per task at runtime, from the start input `{"taskArn": "..."}`:**

| Value | From | Example |
|---|---|---|
| task name / suffix | the DMS task's name (or the input's `taskSuffix`; after the first startup, the name recorded for the task's ARN) | `task-orders-02` |
| config folder | `s3://<bucket>/config/_task/<task name>/` | `s3://my-migration-bucket/config/_task/task-orders-02/` |
| Glue job names | `<project>-<task name>-{discovery,load,load-big,validate,cdc}` | `dms-dsql-task-orders-02-cdc` |
| folder owner record | `config/_task/<task name>/_task.json` (task ARN that owns the folder) | written at first startup |
| name-by-ARN record | `config/_task_index/<task id>.json` (keeps a renamed task on its original folder) | written at first startup |
| S3 layout | the DMS task's S3 target endpoint (`BucketFolder`, `TimestampColumnName`, …) | `cdcRoot` = `.` when there is no BucketFolder |

</details>

<details>
<summary>What the state machines run (click to expand)</summary>

**Lambdas** (invoked by the ARNs you filled in at Step 4a):

| SM step | Lambda (handler) | Does |
|---|---|---|
| `ResolveTask` / `CutoverResolveTask` | `resolve_task.handler` | reads `config/pipeline.json` and the DMS task → task name, folder, job names, S3 settings from the endpoint; startup also checks the task before DMS starts and records the folder owner. (Without a `mode` it behaves as before, for older per-task state machines.) |
| `DriverDiscovery` (×3) | `driver_discovery.handler` | lists each `driver-*/*.whl` → per-job wheel lists (fails if no pg8000; only `driver-cdc/` includes boto3/botocore). Runs before DMS starts. For a Python-shell CDC job it also checks `driver-cdc/` for Python 3.9 and returns prepared copies from `driver-cdc-prepared/<fingerprint>/` (uses `prepare_cdc_wheels.py`, bundled in the same zip) |
| `GetCdcRun` / `CheckCdcStarted` | (Glue and S3 directly) | after `StartCdcJob`: wait until the CDC run writes `_cdc_started/<execution name>.json`; fail on a failed or stopped run |
| `CreateGlueJobs` | `create_glue_jobs.handler` | reads `glue-templates/<role>.json` (CDC: `cdc.json` or `cdc-spark.json` per `cdc_engine`), creates `<project>-<task name>-<role>` Glue jobs in your Glue connection |
| `PlanSplit` | `plan_split.handler` | reads `_manifest_index.json` → per-group manifests |
| (cutover) `DrainCheck` | `drain_check.handler` | waits until the latest CDC file is applied (needs pg8000) |
| (cutover) `StopCdcRun` | `stop_cdc_run.handler` | stops this task's CDC Glue run |
| (cutover) `DropTags` | `drop_tags.handler` | drops the `_cdc_file` column on this task's tables (needs pg8000) |

**Glue jobs** are created at runtime named `<project>-<task name>-{discovery,load,load-big,validate,cdc}`
and deleted at cutover — they never accumulate.

**CDC correctness:** Tier-1 (real PK or a declared `logical_key`) = correct insert/update/delete;
Tier-2 (keyless) = insert + delete applied, update skipped and logged to `cdc_control.cdc_skipped_ops`.

</details>
