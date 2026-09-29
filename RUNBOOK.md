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
- **AWS Step Functions** is the conductor — it runs the whole sequence for you. You create
  **one state machine per DMS task**, and it creates its own Glue jobs when it runs and
  deletes them at cutover. Nothing runs on a schedule; you start it by hand.
- **One S3 bucket** holds everything the pipeline needs (scripts, templates, driver files,
  manifests). It's the single source of truth.

**Mental model:** `Oracle → DMS → S3 (CSV) → Glue → Aurora DSQL`, orchestrated by Step Functions.

**The one-time vs. per-task split:**
- **Steps 1–4 (one-time):** create IAM roles, Lambdas, and stage files to S3. Do this once.
- **Steps 5–6 (per migration task):** point a state machine at a DMS task and run it. Repeat
  for each DMS task you migrate.

---

## Before you start — prerequisites checklist

Tick all of these before Step 1. The pipeline **loads data into tables that already exist** —
it never creates target tables.

- [ ] **AWS CLI installed and configured** (`aws sts get-caller-identity` returns your account).
- [ ] **An Aurora DSQL cluster** exists, and you know its endpoint (e.g. `abcd.dsql.us-east-1.on.aws`).
- [ ] **Target tables already created in DSQL** — every table you plan to migrate must exist in
      the target schema, with a single-column primary key where possible (best for CDC + validation).
- [ ] **A DMS task** of type **`full-load-and-cdc`** with:
  - `StopTaskCachedChangesApplied = true`
  - An **S3 target endpoint** with: `AddColumnName=true`, `TimestampColumnName=dms_timestamp`,
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

# ---- per migration task (change when you start a new task) ----
export TASK_SUFFIX="abc"                      # short unique tag for THIS task (names its folder + jobs)
export TASK_ARN="arn:aws:dms:us-east-1:123456789012:task:XXXX"   # the DMS task ARN to migrate

# ---- derived (do not edit) ----
export CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_SUFFIX/"
export SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-lambda-exec-role"
export GLUE_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-glue-exec-role"
echo "Config prefix for this task: $CONFIG_PREFIX"
```

> **Why so few values?** The S3 folder names (`scripts/`, `glue-templates/`, `driver-*`,
> `cdc/`, `config/`) are **fixed** and already baked into the templates. You only ever set the
> bucket name and a per-task suffix — everything else is derived.

---

## The S3 layout (what you're creating in Step 3)

One bucket, fixed folders. You don't invent any prefixes:

```
s3://$BUCKET/
├── scripts/                  # the 4 Glue scripts
├── glue-templates/           # the 5 Glue job-definition templates
├── driver-fullload/          # DSQL driver wheels only        (Spark: discovery + load)
├── driver-validation/        # DSQL driver wheels only        (Spark: validate)
├── driver-cdc/               # DSQL wheels + boto3/botocore    (Python-shell: CDC)
├── cdc/                      # DMS writes CSVs here — you do NOT upload this
└── config/
    └── _task/$TASK_SUFFIX/   # one folder per DMS task
        ├── table_manifest.csv    # you stage this (list of tables)
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

> The policy files use `<<REGION>>`, `<<ACCOUNT_ID>>`, `<<BUCKET>>`, `<<DSQL_CLUSTER_ID>>`
> placeholders. Substitute your values first — quick one-liner:
> ```bash
> sed -i '' -e "s/<<REGION>>/$REGION/g" -e "s/<<ACCOUNT_ID>>/$ACCOUNT_ID/g" \
>           -e "s/<<BUCKET>>/$BUCKET/g" -e "s/<<DSQL_CLUSTER_ID>>/$DSQL_CLUSTER_ID/g" iam/*.json
> ```
> (On Linux, use `sed -i` without the `''`.)

**Verify:** `aws iam get-role --role-name $PROJECT-glue-exec-role` returns the role.

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
      --runtime python3.12 --handler "$HANDLER" --timeout 120 \
      --role "$LAMBDA_ROLE_ARN" --zip-file fileb://fn.zip
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
9. **Configuration → General configuration → Edit → Timeout:** set to **2 min** (120 s).
10. **Save.** Repeat for the remaining functions.

### Option C — AWS CloudShell (build in the browser, deploy from S3)

Use this if you're working entirely in the browser (no local machine) — AWS **CloudShell**
already has `aws`, `python`, `pip`, `zip`, and `git` installed. The trick for CloudShell is that
function code over ~50 MB (or when you'd rather not keep it in the shell) is deployed **from an S3
object** with `--code S3Bucket=...,S3Key=...` instead of `--zip-file`.

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
         --runtime python3.12 --handler "$HANDLER" --timeout 120 \
         --role "$LAMBDA_ROLE_ARN" \
         --code S3Bucket=$BUCKET,S3Key=lambda-code/fn.zip
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

# The 5 job templates (substitute <<BUCKET>> inside them first)
sed -i '' "s/<<BUCKET>>/$BUCKET/g" glue-templates/*.json      # Linux: drop the ''
aws s3 cp glue-templates/ s3://$BUCKET/glue-templates/ --recursive --exclude "*" --include "*.json"
```

### 3b — Driver files (the part people get wrong)

The Glue jobs can't reach PyPI, so their Python dependencies are staged in S3 as `.whl`
files. There are **three** driver folders because each job type needs a different set:

| Folder | Used by | Put in it | Do NOT put in it |
|---|---|---|---|
| `driver-fullload/` | discovery + load (Spark) | **pg8000 stack only** (5 wheels) | ❌ boto3 / botocore |
| `driver-validation/` | validate (Spark) | **pg8000 stack only** (5 wheels) | ❌ boto3 / botocore |
| `driver-cdc/` | CDC (Python-shell) | pg8000 stack **+** boto3/botocore | — |

- **pg8000 stack (5 wheels):** `pg8000`, `scramp`, `asn1crypto`, `python_dateutil`, `six`
- **boto3 set (cdc only):** `boto3`, `botocore`, `jmespath`, `s3transfer`, `urllib3`

> ⚠️ **Why the split matters:** the pipeline loads **every** wheel it finds in a folder. If a
> `boto3`/`botocore` wheel ends up in `driver-fullload/` or `driver-validation/`, it breaks the
> Spark jobs with `DataNotFoundError: endpoints`. Keep those two folders to the 5 pg8000 wheels
> only. (The Spark jobs get their boto3 a different way; the CDC job needs it bundled because
> Glue's built-in boto3 is too old to know Aurora DSQL.)

**Download the wheels** — pinned to Glue's runtime (Linux x86_64 / Python 3.10), NOT your
laptop's OS, or they may fail to load in Glue:

```bash
PLAT="--platform manylinux2014_x86_64 --python-version 310 --only-binary=:all:"

pip download pg8000 $PLAT -d _drv/                              # -> the 5 pg8000-stack wheels
pip download "boto3>=1.34.0" "botocore>=1.34.0" $PLAT -d _boto3/  # -> boto3 set
```

**Upload to the three folders:**

```bash
# Spark folders = pg8000 stack ONLY
aws s3 cp _drv/ s3://$BUCKET/driver-fullload/   --recursive --exclude "*" --include "*.whl"
aws s3 cp _drv/ s3://$BUCKET/driver-validation/ --recursive --exclude "*" --include "*.whl"
# CDC folder = pg8000 stack + boto3 set
aws s3 cp _drv/   s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"
aws s3 cp _boto3/ s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"
```

**Verify (do this — it's the #1 source of run failures):**

```bash
aws s3 ls s3://$BUCKET/driver-fullload/     # expect 5 wheels, ZERO boto3/botocore
aws s3 ls s3://$BUCKET/driver-validation/   # expect 5 wheels, ZERO boto3/botocore
aws s3 ls s3://$BUCKET/driver-cdc/          # expect pg8000 stack + boto3/botocore
```

### 3c — Your table list (the manifest)

Create a CSV listing the tables to migrate for this task, then upload it. Header row required,
two columns (`dms_schema,dms_table`); the pipeline lowercases them to find the DSQL target:

```csv
dms_schema,dms_table
SRC_SCHEMA,MY_TABLE
SRC_SCHEMA,ANOTHER_TABLE
```

```bash
aws s3 cp table_manifest.csv "${CONFIG_PREFIX}table_manifest.csv"
```

> You do **not** upload anything to `s3://$BUCKET/cdc/` — DMS writes there itself (that's your
> DMS task's S3 target).

---

## Step 4 — Create the state machines for this task (per task, ~10 min)

**Goal:** create the two Step Functions state machines that run this task — **startup**
(full load → validate → start CDC) and **cutover** (drain → finalize).

The template files in `stepfunctions/` (`startup.asl.json`, `cutover.asl.json`) ship with
`<<PLACEHOLDER>>` tokens where your account-specific values need to go. This step **fills those
placeholders in** with the variables you set earlier, writes out ready-to-use copies, and
registers them as state machines.

### 4a — Fill in the templates

The command below runs `sed` (a find-and-replace tool) over each template. Each
`-e "s|<<PLACEHOLDER>>|value|g"` rule means *"replace every `<<PLACEHOLDER>>` with this value"*
(the `|` is just the separator, and `g` = replace all occurrences). It loops over both templates
and writes a filled-in copy per task (e.g. `startup.abc.asl.json`), leaving the originals untouched
so you can reuse them for the next task.

```bash
for f in startup cutover; do
  sed -e "s|<<TASK_ARN>>|$TASK_ARN|g" \
      -e "s|<<TASK_SUFFIX>>|$TASK_SUFFIX|g" \
      -e "s|<<CONFIG_PREFIX>>|$CONFIG_PREFIX|g" \
      -e "s|<<BUCKET>>|$BUCKET|g" -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<REGION>>|$REGION|g" -e "s|<<ACCOUNT_ID>>|$ACCOUNT_ID|g" \
      "stepfunctions/$f.asl.json" > "$f.$TASK_SUFFIX.asl.json"
done
```

**What each placeholder becomes** (using the example values from the "Fill in your values" block):

| Placeholder in the template | Replaced with your variable | Example result |
|---|---|---|
| `<<TASK_ARN>>` | `$TASK_ARN` | `arn:aws:dms:us-east-1:123456789012:task:XXXX` |
| `<<TASK_SUFFIX>>` | `$TASK_SUFFIX` | `abc` |
| `<<CONFIG_PREFIX>>` | `$CONFIG_PREFIX` | `s3://my-migration-bucket/config/_task/abc/` |
| `<<BUCKET>>` | `$BUCKET` | `my-migration-bucket` |
| `<<PROJECT>>` | `$PROJECT` | `dms-dsql` |
| `<<REGION>>` | `$REGION` | `us-east-1` |
| `<<ACCOUNT_ID>>` | `$ACCOUNT_ID` | `123456789012` |

So a template line like:
```json
"Resource": "<<TASK_ARN>>",
```
becomes, in the generated `startup.abc.asl.json`:
```json
"Resource": "arn:aws:dms:us-east-1:123456789012:task:XXXX",
```

> **One manual edit — the Lambda ARNs.** The templates also reference the 7 Lambdas as
> `<<RESOLVE_TASK_LAMBDA_ARN>>`, `<<DRIVER_DISCOVERY_LAMBDA_ARN>>`, `<<PLAN_SPLIT_LAMBDA_ARN>>`,
> `<<CREATE_GLUE_JOBS_LAMBDA_ARN>>`, `<<STOP_CDC_RUN_LAMBDA_ARN>>`, `<<DRAIN_CHECK_LAMBDA_ARN>>`,
> `<<DROP_TAGS_LAMBDA_ARN>>`. These aren't in the `sed` loop above because they vary per function.
> Get each one and paste it into the generated `.asl.json` files (find/replace in your editor).
> **Use the ARNs of the functions YOU created** — they carry whatever `$PROJECT` you chose
> (e.g. if `PROJECT=acme-mig`, the ARN is `…:function:acme-mig-resolve-task`). Do NOT paste the
> example `dms-dsql-…` ARNs unless that's actually your project name. The state machine invokes
> Lambdas by these ARNs (not by `$PROJECT`), so the name inside each ARN must exactly match the
> function name from Step 2, or you'll get `Lambda function not found` at runtime.
> To list them:
> ```bash
> aws lambda list-functions \
>   --query "Functions[?starts_with(FunctionName,'$PROJECT-')].[FunctionName,FunctionArn]" --output table
> ```
> Each ARN looks like: `arn:aws:lambda:us-east-1:123456789012:function:dms-dsql-resolve-task`.
> Tip: confirm no placeholders remain before creating the machine —
> `grep '<<' startup.$TASK_SUFFIX.asl.json` should print **nothing**.

> **Using a custom `$PROJECT`? Three things in the state machine must carry it** (all resource
> names built from your project prefix):
> 1. **`<<PROJECT>>` token** — used to build the Glue job names (`<PROJECT>-<TASK_SUFFIX>-…`).
>    ✅ Handled automatically by the `sed` loop above (it substitutes `<<PROJECT>>` → `$PROJECT`).
> 2. **The 7 Lambda ARNs** — each ends in `…:function:<PROJECT>-<name>`. ⚠️ Manual — paste YOUR
>    functions' ARNs (Step 2), not the example `dms-dsql-…`.
> 3. **`<<GLUE_EXEC_ROLE_ARN>>`** — the Glue role the created jobs run as, named
>    `<PROJECT>-glue-exec-role` (Step 1). ⚠️ Manual — paste `arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-glue-exec-role`.
>
> As long as the **same `$PROJECT` value** was used in Step 1 (roles), Step 2 (functions), and
> here, these line up. A mismatch shows up at runtime as `Lambda function not found` or an
> IAM/role error — not at create time.

### 4b — Register the two state machines

`--name` is what the machine is called in the console; `--definition file://…` is the filled-in
file you just generated; `--role-arn` is the Step Functions execution role from Step 1
(`$SFN_ROLE_ARN`, which resolves to `arn:aws:iam::123456789012:role/dms-dsql-sfn-exec-role`).

```bash
aws stepfunctions create-state-machine --name "$PROJECT-startup-$TASK_SUFFIX" \
  --definition file://startup.$TASK_SUFFIX.asl.json --role-arn "$SFN_ROLE_ARN"
aws stepfunctions create-state-machine --name "$PROJECT-cutover-$TASK_SUFFIX" \
  --definition file://cutover.$TASK_SUFFIX.asl.json --role-arn "$SFN_ROLE_ARN"
```

With the example values, the first command creates a state machine named
`dms-dsql-startup-abc` from `startup.abc.asl.json`.

**Verify:** `aws stepfunctions list-state-machines --query "stateMachines[?contains(name,'$TASK_SUFFIX')].name"`
shows both.

---

## Step 5 — Run the migration (per task)

**Goal:** kick off the startup state machine. It does everything: full load → validate →
switch DMS to CDC → start the continuous CDC job.

**Do this:**

```bash
STARTUP_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-startup-$TASK_SUFFIX'].stateMachineArn" --output text)

aws stepfunctions start-execution --state-machine-arn "$STARTUP_ARN" \
  --name run-$(date +%Y%m%d-%H%M%S)
```

**What happens (in order), so you can follow along in the Step Functions console:**
1. Start the DMS task → wait for full load to finish (`STOPPED_AFTER_CACHED_EVENTS`).
2. Resolve the DMS endpoint's S3 settings automatically.
3. Discover driver files (×3 folders) and create this task's Glue jobs from the templates.
4. Run **Job 1** (discovery) → **Job 2** (load) → **Job 3** (validate), per table group.
5. Resume DMS into ongoing CDC and start the **continuous CDC job**.

**Verify:** the execution reaches a **Succeed** state; full load is now in DSQL, validated, and
CDC is live and applying ongoing changes. (See `USAGE_GUIDE.md` → Monitoring for the
`cdc_control` queries to watch CDC progress.)

> **If a step fails:** fix the cause, then **just start the execution again.** Each stage skips
> already-completed work (via S3 status files), and job creation is idempotent — so a re-run
> safely resumes from where it stopped.

---

## Step 6 — Cut over (per task, when you're ready to switch the app)

**Goal:** once CDC has caught up (all tables idle, source ≈ target row counts), finalize and
switch the application to Aurora DSQL.

**Do this:**

```bash
CUTOVER_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='$PROJECT-cutover-$TASK_SUFFIX'].stateMachineArn" --output text)

aws stepfunctions start-execution --state-machine-arn "$CUTOVER_ARN" \
  --name cutover-$(date +%Y%m%d-%H%M%S)
```

It stops CDC, drain-checks that the last CDC file was applied, removes the pipeline's internal
`_cdc_file` tracking column, and deletes this task's Glue jobs. **Then you** stop the DMS task
and repoint your application to Aurora DSQL. Other tasks are unaffected.

---

## Doing more than one task

Steps 1–3 are done once. For each additional DMS task, just re-set the **per-task** variables
and repeat Steps 4–6:

```bash
export TASK_SUFFIX="def"
export TASK_ARN="arn:aws:dms:...:task:YYYY"
export CONFIG_PREFIX="s3://$BUCKET/config/_task/$TASK_SUFFIX/"
# then: stage that task's table_manifest.csv (Step 3c), create its state machines (Step 4), run (Step 5).
```

Each task is fully isolated — its own state machine, its own Glue jobs, its own config folder.
One task failing or cutting over never affects another.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Spark job fails `DataNotFoundError: endpoints` | a `boto3`/`botocore` wheel leaked into `driver-fullload/` or `driver-validation/` | remove it — those folders hold the 5 pg8000 wheels ONLY (re-check Step 3b verify) |
| CDC job fails `UnknownServiceError: dsql` | `driver-cdc/` is missing modern boto3/botocore | upload the boto3 set to `driver-cdc/` (Step 3b) |
| A driver job fails "no pg8000" | driver folder empty or wrong-platform wheels | re-run the platform-pinned `pip download` (Step 3b) and re-upload |
| Load "SUCCEEDED" but 0 rows loaded | stale `_load_status.json` marks tables done | delete `${CONFIG_PREFIX}_load_status.json` and re-run |
| CDC runs but applies 0 rows | CDC looking in the wrong S3 folder | confirm the DMS S3 target matches where the CDC job reads (auto-derived; see USAGE_GUIDE) |
| A table shows `blocked` in `cdc_control.cdc_status` | a `DROP COLUMN` on the source | drop the column on the DSQL target, clear the table's `cdc_status` row, restart CDC |
| `drain-check` / `drop-tags` Lambda errors | missing `pg8000` on those two Lambdas | attach a pg8000 layer or bundle it into the zip (Step 2) |

For deeper operation, monitoring queries, schema-change handling, and clean-slate reloads, see
**[`USAGE_GUIDE.md`](USAGE_GUIDE.md)**. For architecture, known limitations, and the DDL
support matrix, see **[`ENGINEERING_RECORD.md`](ENGINEERING_RECORD.md)**.

---

## Reference — what runs, and the runtime contract

<details>
<summary>Placeholders used across the templates (click to expand)</summary>

| Placeholder | Meaning | Example |
|---|---|---|
| `<<BUCKET>>` | your source-of-truth bucket (no `s3://`, no slash) | `my-migration-bucket` |
| `<<ACCOUNT_ID>>` | 12-digit AWS account | `123456789012` |
| `<<REGION>>` | region | `us-east-1` |
| `<<PROJECT>>` | prefix for job/role names | `dms-dsql` |
| `<<DSQL_ENDPOINT>>` / `<<DSQL_CLUSTER_ID>>` | DSQL endpoint host / its first label | `abcd.dsql.us-east-1.on.aws` / `abcd` |
| `<<DSQL_USER>>` / `<<DSQL_DATABASE>>` | DSQL user / db | `admin` / `postgres` |
| `<<GLUE_EXEC_ROLE_ARN>>` | Glue exec role ARN (Step 1) | `arn:aws:iam::…:role/dms-dsql-glue-exec-role` |
| `<<*_LAMBDA_ARN>>` | the 7 Lambda ARNs (Step 2) | `arn:aws:lambda:…:function:dms-dsql-resolve-task` |
| `<<TASK_ARN>>` | the DMS task this SM drives | `arn:aws:dms:…:task:ABC` |
| `<<TASK_SUFFIX>>` | short per-task tag (names folder + jobs) | `abc` |
| `<<CONFIG_PREFIX>>` | `s3://<<BUCKET>>/config/_task/<<TASK_SUFFIX>>/` | `s3://my-migration-bucket/config/_task/abc/` |

</details>

<details>
<summary>What the state machine resolves at runtime (click to expand)</summary>

**Lambdas** (invoked by the ARNs you paste into the template):

| SM step | Lambda (handler) | Does |
|---|---|---|
| `ResolveTask` | `resolve_task.handler` | reads the DMS S3 target endpoint → derives S3 base + settings |
| `DriverDiscovery` (×3) | `driver_discovery.handler` | lists each `driver-*/*.whl` → per-job `--extra-py-files` (fails if no pg8000; only `driver-cdc/` includes boto3/botocore) |
| `CreateGlueJobs` | `create_glue_jobs.handler` | reads `glue-templates/<role>.json`, creates `<<PROJECT>>-<<TASK_SUFFIX>>-<role>` Glue jobs |
| `PlanSplit` | `plan_split.handler` | reads `_manifest_index.json` → per-group manifests |
| (cutover) `DrainCheck` | `drain_check.handler` | waits until the latest CDC file is applied (needs pg8000) |
| (cutover) `StopCdcRun` | `stop_cdc_run.handler` | stops this task's CDC Glue run |
| (cutover) `DropTags` | `drop_tags.handler` | drops the `_cdc_file` column on this task's tables (needs pg8000) |

**Glue jobs** are created at runtime named `<<PROJECT>>-<<TASK_SUFFIX>>-{discovery,load,load-big,validate,cdc}`
and deleted at cutover — they never accumulate.

**CDC correctness:** Tier-1 (real PK or a declared `logical_key`) = correct insert/update/delete;
Tier-2 (keyless) = insert + delete applied, update skipped and logged to `cdc_control.cdc_skipped_ops`.

</details>
