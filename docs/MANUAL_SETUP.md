# Manual setup — the pipeline without `tools/setup.sh` (Option B)

Use this when you **can't run `tools/setup.sh`**: no bash/CloudShell allowed, a change-controlled
environment where each resource is created (and reviewed) one at a time, or a
CloudFormation/Terraform shop that wants the exact commands and names to port. It creates **exactly
what `setup.sh` creates**, in the same order, with the same names — see the *Replaces in setup.sh*
note on each step.

This page is **standalone**: it repeats getting the code, the bucket and `params.csv` so you can
follow it on its own. Run everything **from the repo root**.

> **Before you start** you also need the same AWS prerequisites as Option A, which this page does not
> repeat in full: an **Aurora DSQL cluster**, the **target tables already created**, **≤ 9 schemas of
> your own**, a **network path from Glue/Lambdas to DSQL** if locked down, and a **`full-load-and-cdc`
> DMS task** per source whose S3 target endpoint writes to the bucket below. See
> [RUNBOOK §2 "What you need"](../RUNBOOK.md#2-what-you-need) for the full checklist (including the
> exact DMS task settings preflight requires).

> **Console instead of the CLI?** Every step can be done in the AWS console — what matters is the
> **resource names, files and S3 paths** below (they are what the pipeline looks up). Where a step
> needs a trick to work in the console (the IAM trust/policy split), it is called out.

---

## Get the code

You need an AWS shell with `git`, **Python 3.9+**, `pip`, `zip`, and **AWS CLI v2** (AWS CloudShell
has all of them — open it in the **same region** as your DMS tasks). Then:

```bash
git clone https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime.git
cd Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime
ls tools/setup.sh lambdas iam        # confirm you're in the repo root
```

No git/GitHub access? Download the repo ZIP (GitHub **Code → Download ZIP**), upload it to CloudShell
(**Actions → Upload file**), then `unzip …-main.zip` and `cd …-main` (the ZIP folder gets a `-main`
suffix). **Every command below runs from this folder.** For a newer version later: `git pull` (or
re-download) and redo these steps (all are create-or-update).

## The S3 bucket

One bucket holds everything the pipeline reads at run time. Use an existing one or create it; **the
DMS S3 target endpoint must write to this same bucket**:

```bash
aws s3api create-bucket --bucket "<bucket>" --region "<region>" \
  --create-bucket-configuration LocationConstraint="<region>"   # omit --create-bucket-configuration in us-east-1
```

## Fill in params.csv (the source of the settings)

You don't upload `params.csv` for manual setup, but it is the single source for the values below.
Copy the example and fill it in:

```bash
cp config/params.example.csv params.csv
```

| Parameter | Required? | Default | Meaning |
|---|---|---|---|
| `account_id` | **required** | — | 12-digit AWS account id (setup/IAM only; never in `pipeline.json`) |
| `region` | **required** | — | AWS region of the DMS tasks and pipeline (must equal the task ARN's region) |
| `project` | **required** | — | short prefix (letters, digits, hyphens) for role, Lambda and job names |
| `dsql_endpoint` | **required** | — | Aurora DSQL endpoint `<cluster>.dsql.<region>.on.aws`; inside a VPC with no internet use the VPC endpoint's private DNS name `<cluster>.dsql-<id>.<region>.on.aws` |
| `dsql_user` | optional | `admin` | DSQL user |
| `dsql_database` | optional | `postgres` | DSQL database |
| `glue_connection` | optional | `""` (no VPC) | the Glue network connection's **exact** name; `""` = no VPC |
| `cdc_engine` | optional | `pythonshell` | `pythonshell` (1 DPU) or `spark` (Glue 4.0, 2 × G.1X) |
| `cdc_spark_fallback` | optional | `true` | on a Python-shell CDC driver failure, re-create that task's CDC job as Spark |
| `control_schema` | optional | `cdc_control` | DSQL schema for the CDC control tables |
| `glue_role_arn` | optional | `arn:aws:iam::<account_id>:role/<project>-glue-exec-role` | set only if your Glue role name differs |
| `subnet_id` | optional (setup-only) | — | private subnet for the Glue VPC connection; both-or-neither with `security_group_id` |
| `security_group_id` | optional (setup-only) | — | security group for the Glue VPC connection; both-or-neither with `subnet_id` |

Now export those values for the commands below (CloudShell forgets them on reconnect — re-run in a
new shell):

```bash
# ---- from params.csv ----
export ACCOUNT_ID="123456789012"             # account_id
export REGION="us-east-1"                     # region
export PROJECT="dms-dsql"                     # project
export DSQL_ENDPOINT="abcd.dsql.us-east-1.on.aws"   # dsql_endpoint (VPC/no-internet: abcd.dsql-<id>.us-east-1.on.aws)
export DSQL_CLUSTER_ID="abcd"                 # first label of dsql_endpoint (DERIVED — not a params key)
export DSQL_USER="admin"                      # dsql_user
export DSQL_DATABASE="postgres"               # dsql_database

# ---- the bucket (NOT a params.csv key: it's where everything lives) ----
export BUCKET="my-migration-bucket"

# ---- VPC: only if Glue must run inside your VPC to reach DSQL ----
export SUBNET_ID="subnet-0abc1234"            # subnet_id
export SECURITY_GROUP_ID="sg-0abc1234"        # security_group_id
export GLUE_CONNECTION="$PROJECT-vpc"         # glue_connection (EXACT name); "" = no VPC

# ---- derived — don't edit ----
export AWS_PAGER=""
export AWS_DEFAULT_REGION="$REGION"
export SFN_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-sfn-exec-role"
export LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-lambda-exec-role"
export GLUE_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$PROJECT-glue-exec-role"
```

---

## Step 1 — IAM roles (3)

*Replaces in setup.sh: Step 1 (`split_iam` + `create_or_update_role`).* **One role per service:**
`<project>-glue-exec-role`, `<project>-lambda-exec-role`, `<project>-sfn-exec-role`. All 8 Lambdas run
as the lambda role; all 4 state machines as the sfn role.

`iam/` holds three combined files — `iam/glue.json`, `iam/lambda.json`, `iam/stepfunctions.json` —
each a single JSON `{"RoleName","TrustPolicy","Policy"[,"VpcPolicy"]}` (the Glue file carries a
`VpcPolicy`) with blanks `<<REGION>>`, `<<ACCOUNT_ID>>`, `<<BUCKET>>`, `<<DSQL_CLUSTER_ID>>`,
`<<PROJECT>>`, `<<GLUE_EXEC_ROLE_NAME>>`. **Fill and split them first** — a role made from an unfilled
file would trust/allow a literal `<<...>>`.

```bash
# 1. Fill the blanks and SPLIT each combined file into trust/policy/vpc parts (python3, like setup.sh).
for f in iam/glue.json iam/lambda.json iam/stepfunctions.json; do
  REGION="$REGION" ACCOUNT_ID="$ACCOUNT_ID" BUCKET="$BUCKET" DSQL_CLUSTER_ID="$DSQL_CLUSTER_ID" \
  PROJECT="$PROJECT" GLUE_ROLE_NAME="$PROJECT-glue-exec-role" \
  python3 - "$f" <<'PY'
import json, os, re, sys
src = sys.argv[1]
subs = {"<<REGION>>": os.environ["REGION"], "<<ACCOUNT_ID>>": os.environ["ACCOUNT_ID"],
        "<<BUCKET>>": os.environ["BUCKET"], "<<DSQL_CLUSTER_ID>>": os.environ["DSQL_CLUSTER_ID"],
        "<<PROJECT>>": os.environ["PROJECT"], "<<GLUE_EXEC_ROLE_NAME>>": os.environ["GLUE_ROLE_NAME"]}
raw = open(src, encoding="utf-8").read()
for k, v in subs.items(): raw = raw.replace(k, v)
left = re.findall(r"<<[^>]*>>", raw)
if left: sys.exit("STOP: placeholder(s) left in %s: %s" % (src, ", ".join(sorted(set(left)))))
doc = json.loads(raw)
base = os.path.splitext(os.path.basename(src))[0]   # glue | lambda | stepfunctions
def dump(obj, suffix):
    open("iam/%s.%s.filled.json" % (base, suffix), "w", encoding="utf-8").write(json.dumps(obj, indent=2) + "\n")
dump(doc["TrustPolicy"], "trust"); dump(doc["Policy"], "policy")
if doc.get("VpcPolicy"): dump(doc["VpcPolicy"], "vpc")
print("ROLE=%s" % doc["RoleName"])
PY
done

# 2. Create (or update) the three roles. The inline policy name equals the role name.
for svc in glue lambda stepfunctions; do
  case "$svc" in glue|lambda) rname="$PROJECT-$svc-exec-role" ;; stepfunctions) rname="$PROJECT-sfn-exec-role" ;; esac
  if aws iam get-role --role-name "$rname" >/dev/null 2>&1; then
    aws iam update-assume-role-policy --role-name "$rname" --policy-document "file://iam/$svc.trust.filled.json"
  else
    aws iam create-role --role-name "$rname" \
      --assume-role-policy-document "file://iam/$svc.trust.filled.json" --query Role.RoleName --output text
  fi
  aws iam put-role-policy --role-name "$rname" --policy-name "$rname" --policy-document "file://iam/$svc.policy.filled.json"
done

# 3. VPC only (GLUE_CONNECTION not ""): add the Glue networking inline policy 'glue-vpc'.
aws iam put-role-policy --role-name "$PROJECT-glue-exec-role" \
  --policy-name glue-vpc --policy-document file://iam/glue.vpc.filled.json
```

**No python3 (console alternative).** Open each of `iam/glue.json`, `iam/lambda.json`,
`iam/stepfunctions.json` in a text editor and replace the `<<...>>` blanks by hand. In the IAM
console → **Roles → Create role → Custom trust policy**, paste that file's **`TrustPolicy`** block;
name the role exactly `<project>-glue-exec-role` / `<project>-lambda-exec-role` /
`<project>-sfn-exec-role`; then **Add inline policy → JSON**, paste the file's **`Policy`** block, and
name the inline policy the same as the role. For the Glue role in a VPC, add a second inline policy
named `glue-vpc` with the file's **`VpcPolicy`** block. (The split is only needed because trust and
permission policies go in different console fields.)

**Verify:** `for r in glue-exec lambda-exec sfn-exec; do aws iam get-role --role-name "$PROJECT-$r-role" --query Role.RoleName --output text 2>/dev/null || echo "MISSING: $PROJECT-$r-role"; done`

---

## Step 1b — Glue network connection (VPC only)

*Replaces in setup.sh: Step 1b.* Skip entirely if `GLUE_CONNECTION` is `""`. A Glue job joins a VPC
through a **Glue network connection**; the pipeline attaches the one you name here to every job.

```bash
aws ec2 authorize-security-group-ingress --group-id "$SECURITY_GROUP_ID" --protocol tcp \
  --port 0-65535 --source-group "$SECURITY_GROUP_ID" 2>/dev/null || echo "self-ingress rule already present"

AZ=$(aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" --query "Subnets[0].AvailabilityZone" --output text)

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

The subnet also needs an **S3 gateway endpoint** in its route table and a route to DSQL (a DSQL VPC
endpoint with **private DNS on**, SG allowing inbound 5432 from `$SECURITY_GROUP_ID`, or NAT).
**Verify:** `aws glue get-connection --name "$GLUE_CONNECTION" --query Connection.PhysicalConnectionRequirements`

---

## Step 2 — Lambda functions (8)

*Replaces in setup.sh: Step 2 (`fn.zip` build + the 8-function loop).* All eight run as
`<project>-lambda-exec-role`, **python3.12, 1024 MB, 300 s** (same as `setup.sh`).

| Function | Handler | What it does |
|---|---|---|
| `$PROJECT-resolve-task` | `resolve_task.handler` | reads `config/pipeline.json` + the DMS task; folder/job names/S3 layout; checks the task before DMS starts; records the folder owner; builds the table list |
| `$PROJECT-driver-discovery` | `driver_discovery.handler` | checks the `driver-*` folders; prepares the `driver-cdc/` wheels |
| `$PROJECT-plan-split` | `plan_split.handler` | splits the task's tables into balanced load groups |
| `$PROJECT-create-glue-jobs` | `create_glue_jobs.handler` | creates (and at cutover deletes) the task's Glue jobs; re-creates CDC as Spark on fallback |
| `$PROJECT-stop-cdc-run` | `stop_cdc_run.handler` | stops this task's CDC run at cutover |
| `$PROJECT-drain-check` | `drain_check.handler` | **connects to DSQL:** waits until the last CDC file is applied |
| `$PROJECT-drop-tags` | `drop_tags.handler` | **connects to DSQL:** drops the `_cdc_file` column at cutover |
| `$PROJECT-preflight-tasks` | `preflight_tasks.handler` | the fleet's first step: reads `config/fleet_tasks.csv` and checks every task |

**1. Build the zip** — every `.py` from `lambdas/` plus **`pg8000`** (which `drain-check`/`drop-tags`
import). pg8000 is pure Python. It installs into a separate folder so `lambdas/` is untouched.

```bash
rm -rf _lambda_build fn.zip && mkdir _lambda_build
cp lambdas/*.py _lambda_build/
python3 -m pip install pg8000 -t _lambda_build/ --quiet
(cd _lambda_build && zip -qr ../fn.zip .)
unzip -l fn.zip | grep -cE ' (resolve_task\.py|prepare_cdc_wheels\.py|preflight_tasks\.py|pg8000/__init__\.py)$'  # must print 4
```

**2. Create (or update) all eight** (safe to re-run):

```bash
for spec in resolve-task:resolve_task driver-discovery:driver_discovery plan-split:plan_split \
            create-glue-jobs:create_glue_jobs stop-cdc-run:stop_cdc_run drain-check:drain_check \
            drop-tags:drop_tags preflight-tasks:preflight_tasks; do
  NAME="$PROJECT-${spec%%:*}"; HANDLER="${spec##*:}.handler"
  if aws lambda get-function --function-name "$NAME" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$NAME" --zip-file fileb://fn.zip --query FunctionName --output text
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
role is seconds old — wait 10 s and re-run.

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

No custom environment variables are needed — the Lambdas read the region from the `AWS_REGION`
Lambda provides, and everything else from `config/pipeline.json` at run time.
**Verify:** `aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table`

---

## Step 3a — scripts and job templates

*Replaces in setup.sh: Step 3a.*

```bash
for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py; do
  aws s3 cp "scripts/$f" "s3://$BUCKET/scripts/$f"
done
mkdir -p _filled/glue-templates
for f in glue-templates/*.json; do sed -e "s|<<BUCKET>>|$BUCKET|g" "$f" > "_filled/$f"; done
grep -l "<<" _filled/glue-templates/*.json || echo "no placeholders left in templates"
aws s3 cp _filled/glue-templates/ "s3://$BUCKET/glue-templates/" --recursive --exclude "*" --include "*.json"
```

**Verify** S3 matches the repo scripts (a mismatch means a job runs an old script):

```bash
for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py; do
  if command -v sha256sum >/dev/null 2>&1; then H="sha256sum"; else H="shasum -a 256"; fi
  L=$($H < "scripts/$f" | cut -c1-16); S=$(aws s3 cp "s3://$BUCKET/scripts/$f" - | $H | cut -c1-16)
  [ "$L" = "$S" ] && echo "OK    $f" || echo "STALE $f  (repo $L, S3 $S)"
done
```

---

## Step 3b — driver wheels

*Replaces in setup.sh: Step 3b (`--with-drivers`).* Three folders: `driver-fullload/` and
`driver-validation/` get the **pg8000 stack** (5 wheels, no boto3/botocore); `driver-cdc/` gets the
pg8000 stack **and** the boto3 set, built for Python 3.9 (10 wheels). A boto3/botocore wheel in the
Spark folders breaks them with `DataNotFoundError: endpoints`.

```bash
rm -rf _drv _cdc
PLAT310="--platform manylinux2014_x86_64 --python-version 310 --only-binary=:all:"
PLAT39="--platform manylinux2014_x86_64 --python-version 39 --only-binary=:all:"
python3 -m pip download pg8000 $PLAT310 -d _drv/
python3 -m pip download "pg8000>=1.31,<1.32" "scramp>=1.4.5,<1.4.7" \
  "boto3>=1.35,<1.43" "botocore>=1.35,<1.43" "urllib3>=1.25.4,<1.27" $PLAT39 -d _cdc/

aws s3 cp _drv/ "s3://$BUCKET/driver-fullload/"   --recursive --exclude "*" --include "*.whl"
aws s3 cp _drv/ "s3://$BUCKET/driver-validation/" --recursive --exclude "*" --include "*.whl"
aws s3 rm "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
aws s3 cp _cdc/ "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
```

**Verify** (the last line runs the startup's own check, offline — no AWS):

```bash
aws s3 ls "s3://$BUCKET/driver-fullload/"     # 5 wheels, no boto3/botocore
aws s3 ls "s3://$BUCKET/driver-validation/"   # 5 wheels, no boto3/botocore
aws s3 ls "s3://$BUCKET/driver-cdc/"          # 10 wheels, one version per package
rm -rf /tmp/_cdc_check && python3 lambdas/prepare_cdc_wheels.py _cdc/ /tmp/_cdc_check/ --python 3.9   # ends PASS
```

---

## Step 3c — config/pipeline.json

*Replaces in setup.sh: Step 3c (build from `params.csv` + publish).* Ten of the `params.csv` values
become `config/pipeline.json`. Each key maps straight across (defaults applied where you left a row
out); `account_id`, `subnet_id`, `security_group_id` are **not** written. Don't copy
`config/pipeline.example.json` as-is — its `description` holds `<bucket>`, and any value with `<`/`>`
is rejected at run time.

| `config/pipeline.json` key | from `params.csv` | default |
|---|---|---|
| `project` | `project` | — |
| `region` | `region` | — |
| `dsql_endpoint` | `dsql_endpoint` | — |
| `dsql_user` | `dsql_user` | `admin` |
| `dsql_database` | `dsql_database` | `postgres` |
| `glue_role_arn` | `glue_role_arn` | `arn:aws:iam::<account_id>:role/<project>-glue-exec-role` |
| `glue_connection` | `glue_connection` | `""` |
| `cdc_engine` | `cdc_engine` | `pythonshell` |
| `cdc_spark_fallback` | `cdc_spark_fallback` | `true` |
| `control_schema` | `control_schema` | `cdc_control` |

Write it from the export block and upload it to the fixed key `config/pipeline.json`:

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

Never set `dsql_user`, `dsql_database` or `control_schema` to an empty string — a blank there is
kept, not defaulted, and fails later. Before changing a live file, keep a dated copy:
`aws s3 cp "s3://$BUCKET/config/pipeline.json" "s3://$BUCKET/config/pipeline.json.$(date +%Y%m%d%H%M)"`

---

## Step 4 — state machines (4)

*Replaces in setup.sh: Step 4 (`fill_shared_sm` / `fill_fleet_sm` + `create_or_update_sm`).* The
per-task **`startup`**/**`cutover`** (the fleet starts them) and the **`fleet-startup`**/
**`fleet-cutover`** launchers you trigger. All four run on `$SFN_ROLE_ARN`. Create them in this order
(a fleet machine refers to the per-task machine it launches).

```bash
: "${PROJECT:?}" "${REGION:?}" "${ACCOUNT_ID:?}" "${BUCKET:?}"
LAMBDA_BASE="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$PROJECT"
SM="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine"

# 1. Fill the per-task machines' blanks — <<BUCKET>> and the seven Lambda ARNs.
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

# 2. Fill the fleet machines' blanks — <<PROJECT>>, the preflight Lambda ARN, the two per-task ARNs.
for w in startup cutover; do
  sed -e "s|<<PROJECT>>|$PROJECT|g" \
      -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|$LAMBDA_BASE-preflight-tasks|g" \
      -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM:$PROJECT-startup|g" \
      -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM:$PROJECT-cutover|g" \
      "stepfunctions/fleet-$w.asl.json" > "fleet-$w.filled.asl.json"
done
grep "<<" fleet-startup.filled.asl.json fleet-cutover.filled.asl.json || echo "no placeholders left (fleet)"

# 3. Create (or update) the two per-task machines.
for f in startup cutover; do
  ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='$PROJECT-$f'].stateMachineArn" --output text)
  if [ -n "$ARN" ]; then
    aws stepfunctions update-state-machine --state-machine-arn "$ARN" --definition "file://$f.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  else
    aws stepfunctions create-state-machine --name "$PROJECT-$f" --definition "file://$f.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  fi
done

# 4. Create (or update) the two fleet machines.
for w in startup cutover; do
  ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='$PROJECT-fleet-$w'].stateMachineArn" --output text)
  if [ -n "$ARN" ]; then
    aws stepfunctions update-state-machine --state-machine-arn "$ARN" --definition "file://fleet-$w.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  else
    aws stepfunctions create-state-machine --name "$PROJECT-fleet-$w" --definition "file://fleet-$w.filled.asl.json" --role-arn "$SFN_ROLE_ARN"
  fi
done
```

---

## Check it worked

The same checks as the RUNBOOK:

```bash
aws iam list-roles --query "Roles[?starts_with(RoleName,'$PROJECT-')].RoleName" --output table                     # 3
aws lambda list-functions --query "Functions[?starts_with(FunctionName,'$PROJECT-')].FunctionName" --output table   # 8
aws stepfunctions list-state-machines --query "stateMachines[?starts_with(name,'$PROJECT-')].name" --output table   # 4
aws s3 ls "s3://$BUCKET/scripts/"            # 4 Glue scripts
aws s3 ls "s3://$BUCKET/glue-templates/"     # 6 templates
aws s3 cp "s3://$BUCKET/config/pipeline.json" -   # the settings every run reads
```

Setup is complete. Upload `config/fleet_tasks.csv` and trigger `fleet-startup` with
`{"bucket":"<bucket>","inputPrefix":"config"}`
([RUNBOOK §5](../RUNBOOK.md#5-run-tasks-with-the-fleet)).
