#!/usr/bin/env bash
# tools/setup.sh — one-time (idempotent) setup of the Oracle -> Aurora DSQL migration pipeline
# from a single parameters CSV. Run from the repo root.
#
# Usage:
#   tools/setup.sh s3://<bucket>/config/params.csv [--with-drivers] [--dry-run]
#   tools/setup.sh <local-path-to-params.csv> --bucket <bucket> [--with-drivers] [--dry-run]
#
# Operator files live in ONE fixed folder: s3://<bucket>/config/ (params.csv, fleet_tasks.csv,
# and the generated pipeline.json). An s3:// params path MUST be s3://<bucket>/config/params.csv;
# any other key is rejected with a clear message.
#
# What it does (create-or-update, safe to re-run), mirroring RUNBOOK Steps 1-4:
#   1. Fill the 3 combined iam/*.json files and create/update all 3 roles — one per service
#      (glue-exec, lambda-exec, sfn-exec). The glue VpcPolicy block is applied as a separate
#      inline policy only when a VPC connection is configured.
#   1b. Create the Glue network connection, if subnet_id/security_group_id are set.
#   2. Build fn.zip (every lambdas/*.py + pg8000) and create/update all 8 Lambdas
#      (resolve-task, driver-discovery, plan-split, create-glue-jobs, stop-cdc-run,
#       drain-check, drop-tags, preflight-tasks).
#   3a. Upload the 4 Glue scripts and the 6 <<BUCKET>>-filled Glue job templates to S3.
#   3b. --with-drivers only: download the driver wheels and upload them to the driver-* folders.
#   3c. Build config/pipeline.json from the CSV (lambdas/params_csv.py) and publish it, but only
#       when no startup/cutover/fleet execution is running (or it is unchanged) — a dated backup
#       is kept first.
#   4. Create/update all 4 state machines (startup, cutover, fleet-startup, fleet-cutover).
#
# Portable: works on macOS, Linux and AWS CloudShell. No `sed -i`. AWS_PAGER is cleared.
# --dry-run prints every AWS command without running any.

set -euo pipefail
export AWS_PAGER=""

# ---------------------------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------------------------
PARAMS_ARG=""
BUCKET_ARG=""
WITH_DRIVERS=0
DRY_RUN=0

usage() {
  echo "Usage: tools/setup.sh s3://<bucket>/config/params.csv [--with-drivers] [--dry-run]" >&2
  echo "       tools/setup.sh <local params.csv> --bucket <bucket> [--with-drivers] [--dry-run]" >&2
  echo "       (operator files live in s3://<bucket>/config/: params.csv, fleet_tasks.csv, pipeline.json)" >&2
  exit 2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --with-drivers) WITH_DRIVERS=1; shift ;;
    --dry-run)      DRY_RUN=1; shift ;;
    --bucket)       BUCKET_ARG="${2:-}"; shift 2 ;;
    -h|--help)      usage ;;
    -*)             echo "unknown option: $1" >&2; usage ;;
    *)              if [ -z "$PARAMS_ARG" ]; then PARAMS_ARG="$1"; else echo "unexpected arg: $1" >&2; usage; fi; shift ;;
  esac
done

[ -n "$PARAMS_ARG" ] || usage
[ -f "lambdas/params_csv.py" ] || { echo "run this from the repo root (lambdas/params_csv.py not found)" >&2; exit 2; }

# ---------------------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------------------
# run: execute an AWS (or other) command, or just print it in --dry-run.
run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'DRYRUN:'; printf ' %q' "$@"; printf '\n'
  else
    "$@"
  fi
}

# capture: run a command for its stdout when we need the value to proceed (e.g. an ARN lookup).
# In --dry-run it still prints the command (as a comment) and echoes nothing, so the script keeps
# going without touching AWS.
capture() {
  if [ "$DRY_RUN" -eq 1 ]; then
    { printf 'DRYRUN(read):'; printf ' %q' "$@"; printf '\n'; } >&2
    echo ""
  else
    "$@"
  fi
}

TMPDIR_SETUP="$(mktemp -d 2>/dev/null || mktemp -d -t setup)"
cleanup() { rm -rf "$TMPDIR_SETUP" 2>/dev/null || true; }
trap cleanup EXIT

# ---------------------------------------------------------------------------------------------
# Resolve bucket + local copy of params.csv
# ---------------------------------------------------------------------------------------------
BUCKET=""
PARAMS_LOCAL=""
case "$PARAMS_ARG" in
  s3://*)
    # s3://bucket/key... -> bucket is the first path segment, key is the rest.
    rest="${PARAMS_ARG#s3://}"
    BUCKET="${rest%%/*}"
    KEY="${rest#*/}"
    [ -n "$BUCKET" ] || { echo "could not read bucket from $PARAMS_ARG" >&2; exit 2; }
    # Operator files live in the fixed folder config/. The params CSV must be at config/params.csv
    # (same folder as fleet_tasks.csv and the generated pipeline.json).
    if [ "$KEY" != "config/params.csv" ]; then
      echo "ERROR: params.csv must be at s3://$BUCKET/config/params.csv (got key '$KEY')." >&2
      echo "       Operator files live in the fixed folder config/. Upload it there and re-run:" >&2
      echo "       aws s3 cp params.csv s3://$BUCKET/config/params.csv" >&2
      echo "       tools/setup.sh s3://$BUCKET/config/params.csv" >&2
      exit 2
    fi
    PARAMS_LOCAL="$TMPDIR_SETUP/params.csv"
    if [ "$DRY_RUN" -eq 1 ]; then
      { printf 'DRYRUN(read):'; printf ' %q' aws s3 cp "$PARAMS_ARG" "$PARAMS_LOCAL"; printf '\n'; } >&2
      echo "ERROR: --dry-run with an s3:// params path cannot read the CSV offline." >&2
      echo "       Download it first and pass the local path with --bucket $BUCKET." >&2
      exit 2
    fi
    aws s3 cp "$PARAMS_ARG" "$PARAMS_LOCAL" >/dev/null
    ;;
  *)
    [ -f "$PARAMS_ARG" ] || { echo "params file not found: $PARAMS_ARG" >&2; exit 2; }
    [ -n "$BUCKET_ARG" ] || { echo "a local params path needs --bucket <bucket>" >&2; usage; }
    BUCKET="$BUCKET_ARG"
    PARAMS_LOCAL="$PARAMS_ARG"
    ;;
esac

# ---------------------------------------------------------------------------------------------
# Parse + validate the CSV with the SAME module the fleet uses, and export the values as shell
# variables. One python call does the validation and prints `export KEY=value` lines; any error
# aborts here (fail closed) before a single AWS call.
# ---------------------------------------------------------------------------------------------
EXPORTS="$(python3 - "$PARAMS_LOCAL" <<'PY'
import sys, shlex
sys.path.insert(0, "lambdas")
import params_csv as pc

text = open(sys.argv[1], encoding="utf-8").read()
parsed = pc.parse(text)
if parsed["errors"]:
    sys.stderr.write("params.csv has problems:\n  - " + "\n  - ".join(parsed["errors"]) + "\n")
    sys.exit(1)
p = parsed["params"]
try:
    settings = pc.to_pipeline_settings(p)
except pc.ParamsError as e:
    sys.stderr.write("params.csv: %s\n" % e)
    sys.exit(1)

def emit(k, v):
    print("export %s=%s" % (k, shlex.quote(str(v))))

emit("P_ACCOUNT_ID", p["account_id"])
emit("P_REGION", settings["region"])
emit("P_PROJECT", settings["project"])
emit("P_DSQL_ENDPOINT", settings["dsql_endpoint"])
emit("P_DSQL_CLUSTER_ID", pc.dsql_cluster_id(settings["dsql_endpoint"]))
emit("P_GLUE_ROLE_ARN", settings["glue_role_arn"])
emit("P_GLUE_CONNECTION", settings["glue_connection"])
emit("P_SUBNET_ID", p.get("subnet_id", "") or "")
emit("P_SECURITY_GROUP_ID", p.get("security_group_id", "") or "")
for w in parsed["warnings"]:
    sys.stderr.write("(warn) %s\n" % w)
PY
)" || { echo "aborting: fix params.csv and re-run." >&2; exit 1; }
eval "$EXPORTS"

# Derived names
GLUE_ROLE_NAME="$P_PROJECT-glue-exec-role"
LAMBDA_ROLE_ARN="arn:aws:iam::$P_ACCOUNT_ID:role/$P_PROJECT-lambda-exec-role"
SFN_ROLE_ARN="arn:aws:iam::$P_ACCOUNT_ID:role/$P_PROJECT-sfn-exec-role"
LAMBDA_BASE="arn:aws:lambda:$P_REGION:$P_ACCOUNT_ID:function:$P_PROJECT"
SM_BASE="arn:aws:states:$P_REGION:$P_ACCOUNT_ID:stateMachine"
HAVE_VPC=0
[ -n "$P_SUBNET_ID" ] && [ -n "$P_SECURITY_GROUP_ID" ] && HAVE_VPC=1

echo "=== setup for project=$P_PROJECT region=$P_REGION bucket=$BUCKET (dry-run=$DRY_RUN, with-drivers=$WITH_DRIVERS) ==="

# ---------------------------------------------------------------------------------------------
# Fill helpers
# ---------------------------------------------------------------------------------------------
# IAM templates are filled+split by split_iam() in Step 1 (python3). Glue templates and the
# state-machine definitions are filled inline in their own steps below. There is no shared
# fill_file() any more.

# =============================================================================================
# Step 1 — IAM roles (3: one combined file + one role per service)
# =============================================================================================
echo "--- Step 1: IAM roles ---"
mkdir -p "$TMPDIR_SETUP/iam"

# One combined file per service. Each holds {"RoleName","TrustPolicy","Policy"[,"VpcPolicy"]}.
#   iam/glue.json         -> <project>-glue-exec-role  (all Glue jobs; VpcPolicy only when VPC)
#   iam/lambda.json       -> <project>-lambda-exec-role (all 8 Lambdas, incl. preflight-tasks)
#   iam/stepfunctions.json-> <project>-sfn-exec-role   (all 4 state machines, incl. the fleets)
IAM_FILES="iam/glue.json iam/lambda.json iam/stepfunctions.json"

# split_iam SRC OUTDIR
#   Fills the <<...>> placeholders in the combined file SRC and writes, into OUTDIR:
#     <base>.trust.filled.json, <base>.policy.filled.json, and (if a VpcPolicy block exists)
#     <base>.vpc.filled.json. Prints one line "ROLE=<name> VPC=<0|1>" on stdout so the caller
#     knows the role name and whether a VpcPolicy was present. Fails (exit 1) if any <<...>>
#     placeholder is left, matching fill_file's fail-closed behaviour. Portable: python3 only.
split_iam() {
  src="$1"; outdir="$2"
  P_REGION="$P_REGION" P_ACCOUNT_ID="$P_ACCOUNT_ID" P_BUCKET="$BUCKET" \
  P_DSQL_CLUSTER_ID="$P_DSQL_CLUSTER_ID" P_PROJECT="$P_PROJECT" \
  P_GLUE_ROLE_NAME="$GLUE_ROLE_NAME" \
  python3 - "$src" "$outdir" <<'PY'
import json, os, re, sys
src, outdir = sys.argv[1], sys.argv[2]
subs = {
    "<<REGION>>": os.environ["P_REGION"],
    "<<ACCOUNT_ID>>": os.environ["P_ACCOUNT_ID"],
    "<<BUCKET>>": os.environ["P_BUCKET"],
    "<<DSQL_CLUSTER_ID>>": os.environ["P_DSQL_CLUSTER_ID"],
    "<<PROJECT>>": os.environ["P_PROJECT"],
    "<<GLUE_EXEC_ROLE_NAME>>": os.environ["P_GLUE_ROLE_NAME"],
}
raw = open(src, encoding="utf-8").read()
for k, v in subs.items():
    raw = raw.replace(k, v)
left = re.findall(r"<<[^>]*>>", raw)
if left:
    sys.stderr.write("ERROR: placeholders left in %s: %s\n" % (src, ", ".join(sorted(set(left)))))
    sys.exit(1)
doc = json.loads(raw)                       # also validates JSON
role = doc["RoleName"]
base = os.path.splitext(os.path.basename(src))[0]
def dump(obj, suffix):
    path = os.path.join(outdir, "%s.%s.filled.json" % (base, suffix))
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, indent=2) + "\n")
dump(doc["TrustPolicy"], "trust")
dump(doc["Policy"], "policy")
have_vpc = 1 if doc.get("VpcPolicy") else 0
if have_vpc:
    dump(doc["VpcPolicy"], "vpc")
print("ROLE=%s VPC=%d" % (role, have_vpc))
PY
}

create_or_update_role() {
  rname="$1"; trust_filled="$2"; policy_filled="$3"
  if [ "$DRY_RUN" -eq 0 ] && aws iam get-role --role-name "$rname" >/dev/null 2>&1; then
    run aws iam update-assume-role-policy --role-name "$rname" \
      --policy-document "file://$trust_filled"
  else
    run aws iam create-role --role-name "$rname" \
      --assume-role-policy-document "file://$trust_filled" \
      --query Role.RoleName --output text
  fi
  # stable inline policy name per role (same value as before the merge)
  run aws iam put-role-policy --role-name "$rname" --policy-name "$rname" \
    --policy-document "file://$policy_filled"
}

for f in $IAM_FILES; do
  base="$(basename "${f%.json}")"             # glue | lambda | stepfunctions
  # Fill + split into $TMPDIR_SETUP/iam and, per the RUNBOOK convention, next to the repo file
  # too (iam/<base>.{trust,policy,vpc}.filled.json; .gitignore excludes *.filled.json).
  meta="$(split_iam "$f" "$TMPDIR_SETUP/iam")"
  split_iam "$f" "iam" >/dev/null
  rname="${meta#ROLE=}"; rname="${rname%% *}"
  have_vpc_file="${meta##*VPC=}"
  trust_filled="$TMPDIR_SETUP/iam/$base.trust.filled.json"
  policy_filled="$TMPDIR_SETUP/iam/$base.policy.filled.json"
  create_or_update_role "$rname" "$trust_filled" "$policy_filled"

  # Glue VPC add-on: a separate inline policy (name 'glue-vpc'), applied only when a VPC
  # connection is configured AND the file actually carries a VpcPolicy block.
  if [ "$base" = "glue" ] && [ "$HAVE_VPC" -eq 1 ] && [ "$have_vpc_file" = "1" ]; then
    run aws iam put-role-policy --role-name "$rname" --policy-name glue-vpc \
      --policy-document "file://$TMPDIR_SETUP/iam/$base.vpc.filled.json"
  fi
done

# Upgrade path: the former per-component roles are no longer used. We do NOT delete them (a live
# e2e run may still reference them); just tell the operator how to remove them once this update
# is verified.
if [ "$DRY_RUN" -eq 0 ]; then
  for old in preflight-tasks-role fleet-startup-role fleet-cutover-role; do
    if aws iam get-role --role-name "$P_PROJECT-$old" >/dev/null 2>&1; then
      echo "NOTE: role '$P_PROJECT-$old' is no longer used (its permissions were merged into $P_PROJECT-lambda-exec-role / $P_PROJECT-sfn-exec-role)." >&2
      echo "      After verifying this update, remove it with:" >&2
      echo "        aws iam delete-role-policy --role-name $P_PROJECT-$old --policy-name $P_PROJECT-$old 2>/dev/null; aws iam delete-role --role-name $P_PROJECT-$old" >&2
    fi
  done
fi

# =============================================================================================
# Step 1b — Glue network connection (VPC only)
# =============================================================================================
if [ "$HAVE_VPC" -eq 1 ] && [ -n "$P_GLUE_CONNECTION" ]; then
  echo "--- Step 1b: Glue network connection $P_GLUE_CONNECTION ---"
  run aws ec2 authorize-security-group-ingress --group-id "$P_SECURITY_GROUP_ID" \
    --protocol tcp --port 0-65535 --source-group "$P_SECURITY_GROUP_ID" || \
    echo "(self-ingress rule already present or not permitted; continuing)"
  AZ="$(capture aws ec2 describe-subnets --subnet-ids "$P_SUBNET_ID" \
        --query 'Subnets[0].AvailabilityZone' --output text)"
  CONN_INPUT="{\"Name\":\"$P_GLUE_CONNECTION\",\"ConnectionType\":\"NETWORK\",\"ConnectionProperties\":{},\"PhysicalConnectionRequirements\":{\"SubnetId\":\"$P_SUBNET_ID\",\"SecurityGroupIdList\":[\"$P_SECURITY_GROUP_ID\"],\"AvailabilityZone\":\"${AZ:-AZ_LOOKED_UP_AT_RUNTIME}\"}}"
  if [ "$DRY_RUN" -eq 0 ] && aws glue get-connection --name "$P_GLUE_CONNECTION" >/dev/null 2>&1; then
    run aws glue update-connection --name "$P_GLUE_CONNECTION" --connection-input "$CONN_INPUT"
  else
    run aws glue create-connection --connection-input "$CONN_INPUT"
  fi
else
  echo "--- Step 1b: no VPC connection (subnet_id/security_group_id not set) — skipped ---"
fi

# =============================================================================================
# Step 2 — Lambda zip (+ pg8000) and the 8 functions
# =============================================================================================
echo "--- Step 2: Lambda functions ---"
FN_ZIP="$TMPDIR_SETUP/fn.zip"
BUILD="$TMPDIR_SETUP/_lambda_build"
if [ "$DRY_RUN" -eq 1 ]; then
  echo "DRYRUN: build $FN_ZIP from lambdas/*.py + pg8000 (pip install pg8000 -t ...; zip)"
else
  rm -rf "$BUILD"; mkdir -p "$BUILD"
  cp lambdas/*.py "$BUILD/"
  python3 -m pip install pg8000 -t "$BUILD/" --quiet
  ( cd "$BUILD" && zip -qr "$FN_ZIP" . )
  n="$(unzip -l "$FN_ZIP" | grep -cE ' (resolve_task\.py|params_csv\.py|pg8000/__init__\.py)$' || true)"
  [ "$n" -eq 3 ] || { echo "ERROR: fn.zip is missing resolve_task.py / params_csv.py / pg8000 (found $n/3)" >&2; exit 1; }
  echo "built $FN_ZIP (resolve_task.py, params_csv.py and pg8000 present)"
fi

# name-suffix : handler-module
LAMBDA_SPECS="
resolve-task:resolve_task
driver-discovery:driver_discovery
plan-split:plan_split
create-glue-jobs:create_glue_jobs
stop-cdc-run:stop_cdc_run
drain-check:drain_check
drop-tags:drop_tags
preflight-tasks:preflight_tasks
"

# All 8 Lambdas (incl. preflight-tasks) run on the one shared lambda-exec-role.
while IFS=: read -r sfx mod; do
  [ -n "$sfx" ] || continue
  name="$P_PROJECT-$sfx"; handler="$mod.handler"; role="$LAMBDA_ROLE_ARN"
  if [ "$DRY_RUN" -eq 0 ] && aws lambda get-function --function-name "$name" >/dev/null 2>&1; then
    run aws lambda update-function-code --function-name "$name" --zip-file "fileb://$FN_ZIP" \
      --query FunctionName --output text
    run aws lambda wait function-updated --function-name "$name"
    run aws lambda update-function-configuration --function-name "$name" --handler "$handler" \
      --runtime python3.12 --memory-size 1024 --timeout 300 --query FunctionName --output text
    run aws lambda wait function-updated --function-name "$name"
  else
    run aws lambda create-function --function-name "$name" --zip-file "fileb://$FN_ZIP" \
      --handler "$handler" --runtime python3.12 --memory-size 1024 --timeout 300 \
      --role "$role" --query FunctionName --output text
    run aws lambda wait function-active-v2 --function-name "$name"
  fi
done <<EOF
$LAMBDA_SPECS
EOF

# VPC-attach the two DSQL Lambdas
if [ "$HAVE_VPC" -eq 1 ]; then
  run aws iam attach-role-policy --role-name "$P_PROJECT-lambda-exec-role" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
  for n in drain-check drop-tags; do
    run aws lambda update-function-configuration --function-name "$P_PROJECT-$n" \
      --vpc-config "SubnetIds=$P_SUBNET_ID,SecurityGroupIds=$P_SECURITY_GROUP_ID" \
      --query FunctionName --output text
    run aws lambda wait function-updated --function-name "$P_PROJECT-$n"
  done
fi

# =============================================================================================
# Step 3a — Glue scripts and job templates to S3
# =============================================================================================
echo "--- Step 3a: scripts and templates to S3 ---"
for f in job1_discovery.py job2_load.py job3_validate.py glue_cdc_continuous.py glue_cdc_composite.py; do
  run aws s3 cp "scripts/$f" "s3://$BUCKET/scripts/$f"
done
mkdir -p "$TMPDIR_SETUP/glue-templates"
for f in glue-templates/*.json; do
  out="$TMPDIR_SETUP/glue-templates/$(basename "$f")"
  # templates only carry <<BUCKET>>
  sed -e "s|<<BUCKET>>|$BUCKET|g" "$f" > "$out"
  if grep -q "<<" "$out"; then echo "ERROR: placeholder left in $out" >&2; grep -n "<<" "$out" >&2; exit 1; fi
  run aws s3 cp "$out" "s3://$BUCKET/glue-templates/$(basename "$f")"
done

# =============================================================================================
# Step 3b — driver wheels (--with-drivers only)
# =============================================================================================
if [ "$WITH_DRIVERS" -eq 1 ]; then
  echo "--- Step 3b: driver wheels ---"
  DRV="$TMPDIR_SETUP/_drv"; CDC="$TMPDIR_SETUP/_cdc"
  PLAT310="--platform manylinux2014_x86_64 --python-version 310 --only-binary=:all:"
  PLAT39="--platform manylinux2014_x86_64 --python-version 39 --only-binary=:all:"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "DRYRUN: pip download pg8000 $PLAT310 -d $DRV/"
    echo "DRYRUN: pip download pg8000(3.9 pins) + boto3 set $PLAT39 -d $CDC/"
  else
    rm -rf "$DRV" "$CDC"; mkdir -p "$DRV" "$CDC"
    # shellcheck disable=SC2086
    python3 -m pip download pg8000 $PLAT310 -d "$DRV/"
    # shellcheck disable=SC2086
    python3 -m pip download "pg8000>=1.31,<1.32" "scramp>=1.4.5,<1.4.7" \
      "boto3>=1.35,<1.43" "botocore>=1.35,<1.43" "urllib3>=1.25.4,<1.27" $PLAT39 -d "$CDC/"
  fi
  run aws s3 cp "$DRV/" "s3://$BUCKET/driver-fullload/"   --recursive --exclude "*" --include "*.whl"
  run aws s3 cp "$DRV/" "s3://$BUCKET/driver-validation/" --recursive --exclude "*" --include "*.whl"
  run aws s3 rm "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
  run aws s3 cp "$CDC/" "s3://$BUCKET/driver-cdc/" --recursive --exclude "*" --include "*.whl"
else
  echo "--- Step 3b: driver wheels — skipped (pass --with-drivers to stage them) ---"
fi

# =============================================================================================
# Step 3c — build config/pipeline.json from the CSV and publish it (same guard as the fleet)
# =============================================================================================
echo "--- Step 3c: pipeline.json from params.csv ---"
PIPELINE_LOCAL="$TMPDIR_SETUP/pipeline.json"
python3 - "$PARAMS_LOCAL" "$PIPELINE_LOCAL" <<'PY'
import sys, json
sys.path.insert(0, "lambdas")
import params_csv as pc
parsed = pc.parse(open(sys.argv[1], encoding="utf-8").read())
if parsed["errors"]:
    sys.stderr.write("params.csv: " + "; ".join(parsed["errors"]) + "\n"); sys.exit(1)
settings = pc.to_pipeline_settings(parsed["params"])
open(sys.argv[2], "w", encoding="utf-8").write(json.dumps(settings, indent=2) + "\n")
PY

PUBLISH_SETTINGS=1
# Is the live file already identical? Then no write.
if [ "$DRY_RUN" -eq 0 ]; then
  LIVE="$TMPDIR_SETUP/pipeline.live.json"
  if aws s3 cp "s3://$BUCKET/config/pipeline.json" "$LIVE" >/dev/null 2>&1; then
    if python3 - "$PIPELINE_LOCAL" "$LIVE" <<'PY'
import sys, json
a = json.load(open(sys.argv[1])); b = json.load(open(sys.argv[2]))
sys.exit(0 if a == b else 1)
PY
    then
      echo "config/pipeline.json already matches params.csv; nothing to publish."
      PUBLISH_SETTINGS=0
    else
      # Different: only publish if no startup/cutover/fleet execution is running.
      RUNNING=""
      for sm in startup cutover fleet-startup fleet-cutover; do
        arn="$SM_BASE:$P_PROJECT-$sm"
        c="$(aws stepfunctions list-executions --state-machine-arn "$arn" \
             --status-filter RUNNING --max-items 1 \
             --query 'length(executions)' --output text 2>/dev/null || echo "ERR")"
        if [ "$c" = "ERR" ]; then
          echo "WARNING: could not list executions of $arn; refusing to change settings (fail closed)." >&2
          PUBLISH_SETTINGS=0; break
        fi
        if [ -n "$c" ] && [ "$c" != "0" ] && [ "$c" != "None" ]; then
          RUNNING="$RUNNING $P_PROJECT-$sm"
        fi
      done
      if [ -n "$RUNNING" ]; then
        echo "WARNING: settings differ but these workflows are running:$RUNNING — not changing config/pipeline.json." >&2
        PUBLISH_SETTINGS=0
      fi
    fi
  else
    echo "no live config/pipeline.json yet; will publish."
  fi
fi

if [ "$PUBLISH_SETTINGS" -eq 1 ]; then
  if [ "$DRY_RUN" -eq 0 ]; then
    STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    if aws s3 ls "s3://$BUCKET/config/pipeline.json" >/dev/null 2>&1; then
      run aws s3 cp "s3://$BUCKET/config/pipeline.json" "s3://$BUCKET/config/pipeline.json.$STAMP"
    fi
  fi
  run aws s3 cp "$PIPELINE_LOCAL" "s3://$BUCKET/config/pipeline.json"
fi

# =============================================================================================
# Step 4 — state machines (4)
# =============================================================================================
echo "--- Step 4: state machines ---"

fill_shared_sm() {
  src="$1"; out="$2"
  sed -e "s|<<BUCKET>>|$BUCKET|g" \
      -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
      -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
      -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
      -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
      -e "s|<<STOP_CDC_RUN_LAMBDA_ARN>>|$LAMBDA_BASE-stop-cdc-run|g" \
      -e "s|<<DRAIN_CHECK_LAMBDA_ARN>>|$LAMBDA_BASE-drain-check|g" \
      -e "s|<<DROP_TAGS_LAMBDA_ARN>>|$LAMBDA_BASE-drop-tags|g" \
      "$src" > "$out"
  if grep -q "<<" "$out"; then echo "ERROR: placeholder left in $out" >&2; grep -n "<<" "$out" >&2; exit 1; fi
}

fill_fleet_sm() {
  src="$1"; out="$2"
  sed -e "s|<<PROJECT>>|$P_PROJECT|g" \
      -e "s|<<PREFLIGHT_TASKS_LAMBDA_ARN>>|$LAMBDA_BASE-preflight-tasks|g" \
      -e "s|<<STARTUP_STATE_MACHINE_ARN>>|$SM_BASE:$P_PROJECT-startup|g" \
      -e "s|<<CUTOVER_STATE_MACHINE_ARN>>|$SM_BASE:$P_PROJECT-cutover|g" \
      "$src" > "$out"
  if grep -q "<<" "$out"; then echo "ERROR: placeholder left in $out" >&2; grep -n "<<" "$out" >&2; exit 1; fi
}

create_or_update_sm() {
  name="$1"; def_file="$2"; role_arn="$3"
  arn=""
  if [ "$DRY_RUN" -eq 0 ]; then
    arn="$(aws stepfunctions list-state-machines \
           --query "stateMachines[?name=='$name'].stateMachineArn" --output text 2>/dev/null || echo "")"
  fi
  if [ -n "$arn" ] && [ "$arn" != "None" ]; then
    run aws stepfunctions update-state-machine --state-machine-arn "$arn" \
      --definition "file://$def_file" --role-arn "$role_arn"
  else
    run aws stepfunctions create-state-machine --name "$name" \
      --definition "file://$def_file" --role-arn "$role_arn"
  fi
}

for w in startup cutover; do
  out="$TMPDIR_SETUP/$w.filled.asl.json"
  fill_shared_sm "stepfunctions/$w.asl.json" "$out"
  create_or_update_sm "$P_PROJECT-$w" "$out" "$SFN_ROLE_ARN"
done
for w in startup cutover; do
  out="$TMPDIR_SETUP/fleet-$w.filled.asl.json"
  fill_fleet_sm "stepfunctions/fleet-$w.asl.json" "$out"
  create_or_update_sm "$P_PROJECT-fleet-$w" "$out" "$SFN_ROLE_ARN"
done

# =============================================================================================
# Summary
# =============================================================================================
echo ""
echo "=== setup summary ==="
echo "project         : $P_PROJECT"
echo "region          : $P_REGION"
echo "bucket          : $BUCKET"
echo "roles (3)       : $P_PROJECT-{glue,lambda,sfn}-exec-role (one per service; lambda runs all 8 Lambdas incl. preflight-tasks, sfn runs all 4 state machines incl. the fleets)"
echo "lambdas (8)     : $P_PROJECT-{resolve-task,driver-discovery,plan-split,create-glue-jobs,stop-cdc-run,drain-check,drop-tags,preflight-tasks}"
echo "state machines(4): $P_PROJECT-{startup,cutover,fleet-startup,fleet-cutover}"
_tmpl_files=(glue-templates/*.json)
echo "glue scripts    : 5 uploaded to s3://$BUCKET/scripts/"
echo "glue templates  : ${#_tmpl_files[@]} uploaded to s3://$BUCKET/glue-templates/"
if [ "$WITH_DRIVERS" -eq 1 ]; then echo "drivers         : staged to driver-fullload/validation/cdc"; else echo "drivers         : NOT staged (re-run with --with-drivers)"; fi
if [ "$HAVE_VPC" -eq 1 ]; then echo "glue connection : $P_GLUE_CONNECTION ($P_SUBNET_ID / $P_SECURITY_GROUP_ID)"; else echo "glue connection : none (no VPC)"; fi
if [ "$PUBLISH_SETTINGS" -eq 1 ]; then echo "pipeline.json   : published from params.csv"; else echo "pipeline.json   : left unchanged (identical, running, or dry-run)"; fi
if [ "$DRY_RUN" -eq 1 ]; then echo "(dry-run: no AWS calls were made)"; fi
echo "done."
