#!/usr/bin/env bash
# deploy_mrl.sh - deploy the Manual-Range-Load (MRL) pipeline ALONGSIDE the existing pipeline.
#
# It NEVER edits an existing pipeline resource. It only ADDS: the MRL Lambdas, the MRL count
# Glue job, the two MRL state machines, and uploads the patched loader script + the count script.
# Everything runs under the EXISTING customer roles (manage_iam=false convention): this script
# does NOT create or modify any IAM role. DEFAULT deployment needs NO IAM change (verify with tools/mrl_iam_check.sh); the OPTIONAL --with-fleet-sm needs iam/mrl-fleet-sm-optional.json.
#
# Portable (macOS / Linux / CloudShell). set -euo pipefail. AWS_PAGER="". --dry-run prints every
# command without running it. Idempotent (create-or-update). Fills <<...>> placeholders with sed
# and greps for any leftover '<<' (fail closed). No smart quotes; single-line commands.
#
# Usage:
#   tools/deploy_mrl.sh --bucket <bucket> --project <project> --region <region> \
#       --account-id <account-id> --glue-role-arn <arn> --sfn-role-arn <arn> \
#       --lambda-role-arn <arn> [--dsql-endpoint <host>] [--dsql-endpoint-candidates <csv>] \
#       [--dsql-user admin] [--dsql-database postgres] [--glue-connection <name>] [--dry-run]
#
# The DSQL/connection values default from config/pipeline.json conventions if omitted; pass them
# explicitly for a VPC/PrivateLink deployment. Run from the parallel_range_load_pipeline/ folder root.

set -euo pipefail
export AWS_PAGER=""

DRY_RUN=0
BUCKET=""; PROJECT=""; REGION=""; ACCOUNT_ID=""
GLUE_ROLE_ARN=""; SFN_ROLE_ARN=""; LAMBDA_ROLE_ARN=""
DSQL_ENDPOINT=""; DSQL_ENDPOINT_CANDIDATES=""; DSQL_USER="admin"; DSQL_DATABASE="postgres"
GLUE_CONNECTION=""
WITH_FLEET_SM=0
LAMBDA_VPC_FROM_RESOLVE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --with-fleet-sm) WITH_FLEET_SM=1 ;;
    --bucket) BUCKET="$2"; shift ;;
    --project) PROJECT="$2"; shift ;;
    --region) REGION="$2"; shift ;;
    --account-id) ACCOUNT_ID="$2"; shift ;;
    --glue-role-arn) GLUE_ROLE_ARN="$2"; shift ;;
    --sfn-role-arn) SFN_ROLE_ARN="$2"; shift ;;
    --lambda-role-arn) LAMBDA_ROLE_ARN="$2"; shift ;;
    --lambda-vpc-from-resolve-task) LAMBDA_VPC_FROM_RESOLVE=1 ;;
    --dsql-endpoint) DSQL_ENDPOINT="$2"; shift ;;
    --dsql-endpoint-candidates) DSQL_ENDPOINT_CANDIDATES="$2"; shift ;;
    --dsql-user) DSQL_USER="$2"; shift ;;
    --dsql-database) DSQL_DATABASE="$2"; shift ;;
    --glue-connection) GLUE_CONNECTION="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

for v in BUCKET PROJECT REGION ACCOUNT_ID GLUE_ROLE_ARN SFN_ROLE_ARN LAMBDA_ROLE_ARN; do
  if [ -z "$(eval echo \$$v)" ]; then echo "ERROR: --$(echo $v | tr A-Z_ a-z- | sed 's/^-//') is required" >&2; exit 2; fi
done
: "${DSQL_ENDPOINT:=${PROJECT}-cluster.dsql.${REGION}.on.aws}"
: "${DSQL_ENDPOINT_CANDIDATES:=$DSQL_ENDPOINT}"

HERE="$(cd "$(dirname "$0")/.." && pwd)"   # parallel_range_load_pipeline/
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

run() {
  echo "+ $*"
  if [ "$DRY_RUN" -eq 0 ]; then "$@"; fi
}

SM_BASE="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine"
LAMBDA_BASE="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${PROJECT}"
TASK_SM_ARN="${SM_BASE}:${PROJECT}-mrl-task"
COUNT_JOB="${PROJECT}-mrl-count"   # single source for the count job name (preflight + create)

echo "=== MRL deploy: project=$PROJECT region=$REGION bucket=$BUCKET (dry-run=$DRY_RUN) ==="

# --- 0. OWNERSHIP PREFLIGHT (read-only; Amendment 11.2) -------------------------------------
# For every target MRL would create/update, check whether a resource of that name EXISTS and
# whether it is MRL-OWNED. An existing resource is only updated if MRL-owned (Lambda: description
# contains 'dsql-mrl'; Glue job: tag dsql_pipeline_mrl; state machine: tag dsql_pipeline_mrl).
# ABORT (exit 1, nothing changed) if a target NAME exists but is NOT MRL-owned. New resources get
# the ownership marker at creation. All MRL names contain 'mrl', so EXISTING (non-MRL) resources
# are never targeted -> the final 'EXISTING PIPELINE RESOURCES TOUCHED' count must be 0.
MRL_MARKER="dsql-mrl"
NON_MRL_TOUCHED=0        # counts aws create/update commands aimed at a NON-mrl name (must stay 0)
ABORT_NAMES=()

_is_mrl_name() { case "$1" in *mrl*) return 0 ;; *) return 1 ;; esac; }

preflight_lambda() {   # name
  local name="$1" exists owned="no"
  if aws lambda get-function --region "$REGION" --function-name "$name" >/dev/null 2>&1; then
    exists="yes"
    local desc
    desc="$(aws lambda get-function-configuration --region "$REGION" --function-name "$name" \
            --query 'Description' --output text 2>/dev/null || echo "")"
    case "$desc" in *"$MRL_MARKER"*) owned="yes" ;; esac
  else
    exists="no"
  fi
  _classify "lambda" "$name" "$exists" "$owned"
}
preflight_glue() {     # name
  local name="$1" exists owned="no"
  if aws glue get-job --region "$REGION" --job-name "$name" >/dev/null 2>&1; then
    exists="yes"
    local arn="arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/${name}"
    aws glue get-tags --region "$REGION" --resource-arn "$arn" \
      --query 'Tags.dsql_pipeline_mrl' --output text 2>/dev/null | grep -q . && owned="yes"
  else exists="no"; fi
  _classify "glue-job" "$name" "$exists" "$owned"
}
preflight_sm() {       # name
  local name="$1" exists owned="no" arn
  arn="$(aws stepfunctions list-state-machines --region "$REGION" \
         --query "stateMachines[?name=='$name'].stateMachineArn" --output text 2>/dev/null || echo "")"
  if [ -n "$arn" ] && [ "$arn" != "None" ]; then
    exists="yes"
    aws stepfunctions list-tags-for-resource --region "$REGION" --resource-arn "$arn" \
      --query "tags[?key=='dsql_pipeline_mrl'].value" --output text 2>/dev/null | grep -q . && owned="yes"
  else exists="no"; fi
  _classify "state-machine" "$name" "$exists" "$owned"
}
_classify() {          # kind name exists owned
  local kind="$1" name="$2" exists="$3" owned="$4" verdict
  if [ "$exists" = "no" ]; then verdict="WILL CREATE"
  elif [ "$owned" = "yes" ]; then verdict="WILL UPDATE (mrl-owned)"
  else
    verdict="ABORT (name exists, NOT mrl-owned)"
    ABORT_NAMES+=("$kind:$name")
  fi
  if ! _is_mrl_name "$name"; then NON_MRL_TOUCHED=$((NON_MRL_TOUCHED+1)); fi
  printf "  %-14s %-34s %s\n" "$kind" "$name" "$verdict"
}

echo "--- Step 0: ownership preflight (read-only) ---"
# wrong-project guard: the base pipeline's resolve-task Lambda must exist, or MRL won't integrate.
if ! aws lambda get-function --region "$REGION" --function-name "${PROJECT}-resolve-task" >/dev/null 2>&1; then
  echo "  WARN: ${PROJECT}-resolve-task not found - is --project correct? MRL integrates with the"
  echo "        EXISTING pipeline's roles/resources; a wrong project name would not integrate."
fi
echo "  WILL CREATE / WILL UPDATE (mrl-owned) / WILL NOT TOUCH:"
for fn in settings preflight plan reconcile cdc-handoff gate-eval ensure-job state fleet-preflight; do
  preflight_lambda "${PROJECT}-mrl-${fn}"
done
preflight_glue "$COUNT_JOB"
preflight_sm "${PROJECT}-mrl-task"
[ "$WITH_FLEET_SM" -eq 1 ] && preflight_sm "${PROJECT}-mrl-fleet"
echo "  (per-task Glue job ${PROJECT}-<suffix>-mrl-load is created at run time by mrl_ensure_job,"
echo "   tagged dsql_pipeline_mrl; the count job above carries dsql_pipeline_mrl=count.)"
if [ "${#ABORT_NAMES[@]}" -gt 0 ]; then
  echo "ABORT: these target names already exist and are NOT MRL-owned (nothing was changed):" >&2
  for n in "${ABORT_NAMES[@]}"; do echo "  - $n" >&2; done
  exit 1
fi

# --- 0b. Driver-wheel prerequisite (A13-A.3; read-only) ------------------------------------
# The MRL count/probe job is PYTHON-SHELL and gets the Requires-Dist-STRIPPED driver-cdc set
# (prepared at run time by DriverDiscoveryCount via prepare_for=pythonshell) so Glue's pip never
# contacts PyPI in a no-internet VPC. Verify driver-cdc/ exists and holds .whl files; abort if
# missing (the count/probe job would otherwise hang ~20 min on a PyPI timeout).
echo "--- Step 0b: driver-wheel prerequisite (count job + mrl-load DSQL boto3 use driver-cdc/) ---"
CDC_LS="$(aws s3 ls "s3://$BUCKET/driver-cdc/" --region "$REGION" 2>/dev/null)" || CDC_LS=""
CDC_WHL_COUNT="$(printf '%s\n' "$CDC_LS" | grep -c '\.whl$')" || CDC_WHL_COUNT=0
CDC_WHL_COUNT="${CDC_WHL_COUNT:-0}"
echo "  count/probe job wheel set: driver-cdc/  (Requires-Dist-stripped at run time to driver-cdc-prepared/<fingerprint>/)"
echo "  mrl-load (Spark) DSQL boto3: --additional-python-modules = boto3/botocore/s3transfer from driver-cdc/"
echo "  driver-cdc/ .whl files found: $CDC_WHL_COUNT"
if [ "$DRY_RUN" -eq 0 ]; then
  if [ "$CDC_WHL_COUNT" -lt 1 ]; then
    echo "ABORT: s3://$BUCKET/driver-cdc/ has no .whl files. Stage the DSQL driver wheels there" >&2
    echo "       (pg8000 + scramp + asn1crypto + boto3 + botocore + s3transfer set) before" >&2
    echo "       deploying - the python-shell count/probe job needs them and must NOT fetch from" >&2
    echo "       PyPI in the VPC. See RUNBOOK 3b." >&2
    exit 1
  fi
  # A18: the Spark mrl-load job delivers a DSQL-aware boto3 via --additional-python-modules taken
  # from driver-cdc/ (Glue 4.0's bundled botocore has no 'dsql' service). Require boto3, botocore
  # AND s3transfer wheels; abort naming any missing one.
  for pkg in boto3 botocore s3transfer; do
    if ! printf '%s\n' "$CDC_LS" | grep -qiE "(^| )${pkg}-[0-9].*\.whl$"; then
      echo "ABORT: s3://$BUCKET/driver-cdc/ has no ${pkg} wheel. The Spark mrl-load job needs a" >&2
      echo "       DSQL-aware boto3 via --additional-python-modules (boto3 + botocore + s3transfer)" >&2
      echo "       from driver-cdc/; Glue 4.0's bundled botocore has no 'dsql' client. Stage the" >&2
      echo "       wheels (RUNBOOK 3b) and re-deploy." >&2
      exit 1
    fi
  done
fi

# --- 1. Upload the patched loader script + the count script (own keys; never overwrite the
#        shared scripts/job2_load.py) -------------------------------------------------------
echo "--- Step 1: upload scripts ---"
run aws s3 cp --region "$REGION" "$HERE/scripts/job2_load_manual_range.py" \
  "s3://$BUCKET/scripts/job2_load_manual_range.py"
run aws s3 cp --region "$REGION" "$HERE/scripts/job_mrl_count.py" \
  "s3://$BUCKET/scripts/job_mrl_count.py"
# NEW templates under NEW names (never overwrite an existing glue-templates key); fill <<BUCKET>>.
for t in mrl-load mrl-count; do
  out="$TMP/$t.json"
  sed -e "s|<<BUCKET>>|$BUCKET|g" "$HERE/glue-templates/$t.json" > "$out"
  if grep -q "<<" "$out"; then echo "ERROR: placeholder left in $out" >&2; grep -n "<<" "$out" >&2; exit 1; fi
  run aws s3 cp --region "$REGION" "$out" "s3://$BUCKET/glue-templates/$t.json"
done

# --- 2. Build the MRL Lambda zip (the MRL lambdas + the REUSED resolve_task/params_csv from the
#        base pipeline; include mrl_loader_resolver for the plan proof). ---
# A12/A10 C.5: NO MRL Lambda imports pg8000 (only the Glue scripts do, and they get pg8000 via the
# Glue python-shell runtime / VPC connection). So the zip needs NO dependency-install step ->
# deployment needs NO internet. (If a future MRL Lambda imports pg8000, re-add a staged-wheel
# verify step and document the offline alternative: stage wheels on a laptop and upload.)
echo "--- Step 2: build Lambda zip (MRL lambdas + reused resolve_task/params_csv; no pip) ---"
ZIP="$TMP/mrl_lambda.zip"
STAGE="$TMP/stage"; mkdir -p "$STAGE"
cp "$HERE"/lambdas/*.py "$STAGE/"
# reuse the base pipeline's resolve_task.py + params_csv.py (imported by the MRL preflight /
# mrl_params_csv). MANDATORY: fail closed if the base layout does not have them (B8).
for base in resolve_task.py params_csv.py; do
  if [ -f "$HERE/../lambdas/$base" ]; then
    cp "$HERE/../lambdas/$base" "$STAGE/"
  else
    echo "ERROR: $HERE/../lambdas/$base not found - the MRL Lambda zip REQUIRES the base "\
         "pipeline's resolve_task.py and params_csv.py. Run deploy from the repo (the base "\
         "pipeline must be present)." >&2
    exit 1
  fi
done
if [ "$DRY_RUN" -eq 0 ]; then
  # B8 VERIFY: every MRL module + the reused parsers are present, else fail closed.
  python3 - "$STAGE" "$HERE/lambdas" <<'PYV'
import os, sys
stage, lam = sys.argv[1], sys.argv[2]
need = set(os.path.basename(f) for f in os.listdir(lam) if f.endswith(".py"))
need |= {"resolve_task.py", "params_csv.py"}
have = set(os.listdir(stage))
missing = sorted(need - have)
if missing:
    sys.exit("ERROR: MRL Lambda zip is missing module(s): %s" % missing)
print("(info) zip verified: %d MRL modules + resolve_task/params_csv present (no pg8000 needed)"
      % len(need))
PYV
  ( cd "$STAGE" && zip -qr "$ZIP" . )
else
  echo "+ (dry-run) stage MRL lambdas + resolve_task/params_csv ; verify modules ; zip -> $ZIP"
fi

# R4-1: copy the EXISTING <project>-resolve-task Runtime + Architectures for the new Lambdas so
# they match the tested base (falls back to python3.11 / x86_64 with a printed notice). VpcConfig
# is copied ONLY with --lambda-vpc-from-resolve-task (default OFF): MRL Lambdas open no in-VPC
# socket (they call S3/DMS/Glue/StepFunctions/Logs), so no VPC is needed unless the customer's
# SCP requires Lambdas in a VPC - and then S3/Glue/DMS/StepFunctions/CloudWatch VPC endpoints (or
# a NAT) must exist.
LAMBDA_RUNTIME="python3.11"; LAMBDA_ARCH="x86_64"
LAMBDA_VPC_ARGS=()
_rt="$(aws lambda get-function-configuration --region "$REGION" \
       --function-name "${PROJECT}-resolve-task" --query 'Runtime' --output text 2>/dev/null || echo "")"
if [ -n "$_rt" ] && [ "$_rt" != "None" ]; then
  LAMBDA_RUNTIME="$_rt"
  _arch="$(aws lambda get-function-configuration --region "$REGION" \
           --function-name "${PROJECT}-resolve-task" --query 'Architectures[0]' --output text 2>/dev/null || echo "")"
  [ -n "$_arch" ] && [ "$_arch" != "None" ] && LAMBDA_ARCH="$_arch"
  echo "(info) copying ${PROJECT}-resolve-task Runtime=$LAMBDA_RUNTIME Architectures=$LAMBDA_ARCH for the new MRL Lambdas"
else
  echo "(info) ${PROJECT}-resolve-task not found; using default Runtime=$LAMBDA_RUNTIME Architectures=$LAMBDA_ARCH for the new MRL Lambdas"
fi
if [ "$LAMBDA_VPC_FROM_RESOLVE" -eq 1 ]; then
  _subnets="$(aws lambda get-function-configuration --region "$REGION" \
              --function-name "${PROJECT}-resolve-task" \
              --query 'VpcConfig.SubnetIds | join(`,`, @)' --output text 2>/dev/null || echo "")"
  _sgs="$(aws lambda get-function-configuration --region "$REGION" \
          --function-name "${PROJECT}-resolve-task" \
          --query 'VpcConfig.SecurityGroupIds | join(`,`, @)' --output text 2>/dev/null || echo "")"
  if [ -n "$_subnets" ] && [ "$_subnets" != "None" ]; then
    LAMBDA_VPC_ARGS=(--vpc-config "SubnetIds=$_subnets,SecurityGroupIds=$_sgs")
    echo "(info) --lambda-vpc-from-resolve-task: attaching VpcConfig SubnetIds=$_subnets SecurityGroupIds=$_sgs"
    echo "       (ensure S3/Glue/DMS/StepFunctions/CloudWatch VPC endpoints or a NAT exist)"
  else
    echo "(info) --lambda-vpc-from-resolve-task set but ${PROJECT}-resolve-task has no VpcConfig; creating MRL Lambdas with no VPC"
  fi
fi

create_or_update_fn() {
  name="$1"; handler="$2"
  _fn_update() {
    run aws lambda update-function-code --region "$REGION" --function-name "$name" --zip-file "fileb://$ZIP"
  }
  _fn_create() {
    run aws lambda create-function --region "$REGION" --function-name "$name" \
      --runtime "$LAMBDA_RUNTIME" --architectures "$LAMBDA_ARCH" --role "$LAMBDA_ROLE_ARN" \
      --handler "$handler" --description "dsql-mrl MRL Lambda ($name)" \
      --timeout 900 --memory-size 512 --zip-file "fileb://$ZIP" \
      ${LAMBDA_VPC_ARGS[@]+"${LAMBDA_VPC_ARGS[@]}"}
  }
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  [if $name EXISTS] it will run:"; _fn_update
    echo "  [if $name does NOT exist] it will run:"; _fn_create
    return
  fi
  if aws lambda get-function --region "$REGION" --function-name "$name" >/dev/null 2>&1; then
    _fn_update
  else
    _fn_create
  fi
}

echo "--- Step 3: MRL Lambdas ---"
create_or_update_fn "${PROJECT}-mrl-settings"        "mrl_settings.handler"
create_or_update_fn "${PROJECT}-mrl-preflight"       "mrl_preflight.handler"
create_or_update_fn "${PROJECT}-mrl-plan"            "mrl_plan.handler"
create_or_update_fn "${PROJECT}-mrl-reconcile"       "mrl_reconcile.handler"
create_or_update_fn "${PROJECT}-mrl-cdc-handoff"     "mrl_cdc_handoff.handler"
create_or_update_fn "${PROJECT}-mrl-gate-eval"       "mrl_gate_eval.handler"
create_or_update_fn "${PROJECT}-mrl-ensure-job"      "mrl_ensure_job.handler"
create_or_update_fn "${PROJECT}-mrl-state"           "mrl_state.handler"
create_or_update_fn "${PROJECT}-mrl-fleet-preflight" "mrl_fleet_preflight.handler"

# --- 4. The MRL count Glue PYTHON-SHELL job (DSQL via pg8000; needs the VPC connection) -----
echo "--- Step 4: MRL count Glue job ---"

# A12: the count job is a python-shell job that reaches DSQL via pg8000, so it needs the SAME VPC
# connection as the load job when one is configured. Build the optional connection flag as a bash
# ARRAY (no unquoted word-splitting). create-job uses the top-level --connections flag whose shape
# is {"Connections":[...]}; update-job carries the SAME structure under JobUpdate.Connections.
# NOTE: `aws glue create-job` has NO connections-list flag (that invalid flag was the exit-252 defect).
COUNT_SCRIPT="s3://$BUCKET/scripts/job_mrl_count.py"
CONN_CREATE_ARGS=()
CONN_JSON=""
if [ -n "$GLUE_CONNECTION" ]; then
  CONN_CREATE_ARGS=(--connections "{\"Connections\":[\"$GLUE_CONNECTION\"]}")
  CONN_JSON=",\"Connections\":{\"Connections\":[\"$GLUE_CONNECTION\"]}"
fi

count_create() {
  run aws glue create-job --region "$REGION" --name "$COUNT_JOB" --role "$GLUE_ROLE_ARN" \
    --command "{\"Name\":\"pythonshell\",\"PythonVersion\":\"3.9\",\"ScriptLocation\":\"$COUNT_SCRIPT\"}" \
    --glue-version "3.0" --max-capacity 1.0 --timeout 2880 \
    --execution-property "{\"MaxConcurrentRuns\":10}" \
    --default-arguments "{\"--enable-continuous-cloudwatch-log\":\"true\"}" \
    ${CONN_CREATE_ARGS[@]+"${CONN_CREATE_ARGS[@]}"} \
    --tags "dsql_pipeline_project=$PROJECT,dsql_pipeline_mrl=count"
}
count_update() {
  run aws glue update-job --region "$REGION" --job-name "$COUNT_JOB" --job-update \
    "{\"Role\":\"$GLUE_ROLE_ARN\",\"Command\":{\"Name\":\"pythonshell\",\"PythonVersion\":\"3.9\",\"ScriptLocation\":\"$COUNT_SCRIPT\"},\"GlueVersion\":\"3.0\",\"MaxCapacity\":1.0,\"Timeout\":2880,\"ExecutionProperty\":{\"MaxConcurrentRuns\":10},\"DefaultArguments\":{\"--enable-continuous-cloudwatch-log\":\"true\"}$CONN_JSON}"
}

if [ "$DRY_RUN" -eq 1 ]; then
  # A12: a dry run must exercise the SAME code path as a real run - print the EXACT aws command
  # for BOTH branches, clearly labelled, so neither branch can hide a bad flag.
  echo "  [if $COUNT_JOB does NOT exist] it will run:"
  count_create
  echo "  [if $COUNT_JOB EXISTS] it will run:"
  count_update
else
  if aws glue get-job --region "$REGION" --job-name "$COUNT_JOB" >/dev/null 2>&1; then
    count_update
  else
    count_create
  fi
fi

# --- 5. Fill the two state machines' placeholders and create/update them --------------------
echo "--- Step 5: MRL state machines ---"
fill_sm() {
  src="$1"; out="$2"
  sed \
    -e "s|<<BUCKET>>|$BUCKET|g" \
    -e "s|<<MRL_SETTINGS_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-settings|g" \
    -e "s|<<MRL_PREFLIGHT_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-preflight|g" \
    -e "s|<<PLAN_SPLIT_LAMBDA_ARN>>|$LAMBDA_BASE-plan-split|g" \
    -e "s|<<MRL_PLAN_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-plan|g" \
    -e "s|<<MRL_RECONCILE_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-reconcile|g" \
    -e "s|<<MRL_CDC_HANDOFF_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-cdc-handoff|g" \
    -e "s|<<MRL_GATE_EVAL_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-gate-eval|g" \
    -e "s|<<MRL_ENSURE_JOB_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-ensure-job|g" \
    -e "s|<<MRL_STATE_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-state|g" \
    -e "s|<<MRL_FLEET_PREFLIGHT_LAMBDA_ARN>>|$LAMBDA_BASE-mrl-fleet-preflight|g" \
    -e "s|<<RESOLVE_TASK_LAMBDA_ARN>>|$LAMBDA_BASE-resolve-task|g" \
    -e "s|<<DRIVER_DISCOVERY_LAMBDA_ARN>>|$LAMBDA_BASE-driver-discovery|g" \
    -e "s|<<CREATE_GLUE_JOBS_LAMBDA_ARN>>|$LAMBDA_BASE-create-glue-jobs|g" \
    -e "s|<<MRL_COUNT_JOB_NAME>>|$COUNT_JOB|g" \
    -e "s|<<MRL_TASK_STATE_MACHINE_ARN>>|$TASK_SM_ARN|g" \
    -e "s|<<GLUE_ROLE_ARN>>|$GLUE_ROLE_ARN|g" \
    -e "s|<<GLUE_CONNECTION>>|$GLUE_CONNECTION|g" \
    -e "s|<<DSQL_ENDPOINT>>|$DSQL_ENDPOINT|g" \
    -e "s|<<DSQL_ENDPOINT_CANDIDATES>>|$DSQL_ENDPOINT_CANDIDATES|g" \
    -e "s|<<DSQL_USER>>|$DSQL_USER|g" \
    -e "s|<<DSQL_DATABASE>>|$DSQL_DATABASE|g" \
    "$src" > "$out"
  if grep -q "<<" "$out"; then
    echo "ERROR: unresolved placeholder in $out:" >&2; grep -n "<<" "$out" >&2; exit 1
  fi
}

# A14.3: READ-ONLY pre-check - submit the FILLED definition to the Step Functions validator before
# any create/update (also in --dry-run when credentials exist). Abort on a FAIL result; on a
# missing operation (older CLI) or AccessDenied, WARN and continue (the permission is optional).
validate_sm_definition() {
  name="$1"; def="$2"
  local out rc=0
  out="$(aws stepfunctions validate-state-machine-definition --region "$REGION" \
         --type STANDARD --definition "file://$def" 2>&1)" || rc=$?
  if [ "$rc" -ne 0 ]; then
    case "$out" in
      *"Invalid choice"*|*"not a valid command"*|*"ValidateStateMachineDefinition"*"not"*|*"UnknownOperation"*|*"argument operation"*)
        echo "  WARNING: could not pre-validate $name definition (validate-state-machine-definition "
        echo "           unavailable in this CLI). Continuing; this check is optional." ;;
      *AccessDenied*|*"not authorized"*)
        echo "  WARNING: could not pre-validate $name definition (AccessDenied on "
        echo "           states:ValidateStateMachineDefinition - optional). Continuing." ;;
      *) echo "  WARNING: could not pre-validate $name definition ($out). Continuing." ;;
    esac
    return 0
  fi
  local result
  result="$(printf '%s' "$out" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("result",""));
[print("  DIAG %s %s: %s @ %s" % (x.get("severity"),x.get("code"),x.get("message"),x.get("location",""))) for x in d.get("diagnostics",[])]' 2>/dev/null || echo "")"
  if printf '%s' "$result" | grep -qi '^FAIL'; then
    echo "ABORT: Step Functions rejected the $name definition (nothing created):" >&2
    printf '%s\n' "$result" | sed '1d' >&2
    exit 1
  fi
  echo "  (validated $name definition: result OK)"
}

create_or_update_sm() {
  name="$1"; def="$2"
  validate_sm_definition "$name" "$def"
  _sm_update() {
    run aws stepfunctions update-state-machine --region "$REGION" \
      --state-machine-arn "arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:$name" \
      --definition "file://$def" --role-arn "$SFN_ROLE_ARN"
  }
  _sm_create() {
    run aws stepfunctions create-state-machine --region "$REGION" --name "$name" \
      --definition "file://$def" --role-arn "$SFN_ROLE_ARN" \
      --tags "key=dsql_pipeline_mrl,value=$name"
  }
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  [if $name EXISTS] it will run:"; _sm_update
    echo "  [if $name does NOT exist] it will run:"; _sm_create
    return
  fi
  arn="$(aws stepfunctions list-state-machines --region "$REGION" \
         --query "stateMachines[?name=='$name'].stateMachineArn" --output text 2>/dev/null || echo "")"
  if [ -n "$arn" ] && [ "$arn" != "None" ]; then
    run aws stepfunctions update-state-machine --region "$REGION" --state-machine-arn "$arn" \
      --definition "file://$def" --role-arn "$SFN_ROLE_ARN"
  else
    _sm_create
  fi
}

# The child (task) machine MUST exist before the fleet (parent) references its ARN.
fill_sm "$HERE/stepfunctions/mrl-task.asl.json"  "$TMP/mrl-task.filled.json"
create_or_update_sm "${PROJECT}-mrl-task" "$TMP/mrl-task.filled.json"
if [ "$WITH_FLEET_SM" -eq 1 ]; then
  # OPTIONAL fleet state machine. Its parent->child hand-off needs the extra SFN-role grants in
  # iam/mrl-fleet-sm-optional.json. The DEFAULT deployment omits it; start tasks with
  # tools/mrl_fleet_run.py (operator credentials, no new IAM).
  fill_sm "$HERE/stepfunctions/mrl-fleet.asl.json" "$TMP/mrl-fleet.filled.json"
  create_or_update_sm "${PROJECT}-mrl-fleet" "$TMP/mrl-fleet.filled.json"
else
  echo "  (skipping the OPTIONAL fleet state machine; pass --with-fleet-sm to create it. The"
  echo "   default way to start many tasks is: python3 tools/mrl_fleet_run.py --bucket $BUCKET ...)"
fi

echo "=== MRL deploy complete (dry-run=$DRY_RUN). IAM: NONE for the default deployment (the"
echo "    existing roles already cover it - verify with tools/mrl_iam_check.sh). Upload"
echo "    config/fleet_task_parallel_load.csv, then start tasks with:"
echo "      python3 tools/mrl_fleet_run.py --bucket $BUCKET --project $PROJECT --region $REGION"
if [ "$WITH_FLEET_SM" -eq 1 ]; then
  echo "    (fleet state machine deployed: attach iam/mrl-fleet-sm-optional.json, then start"
  echo "     ${PROJECT}-mrl-fleet with {\"bucket\":\"$BUCKET\"}). ==="
else
  echo "    === "
fi

# Amendment 11.2: every MRL target name contains 'mrl' and no create/update was aimed at a
# non-MRL name, so no existing (original) pipeline resource was touched.
echo "EXISTING PIPELINE RESOURCES TOUCHED: ${NON_MRL_TOUCHED}"
if [ "${NON_MRL_TOUCHED}" -ne 0 ]; then
  echo "ERROR: deploy targeted ${NON_MRL_TOUCHED} non-MRL resource name(s) - this must be 0." >&2
  exit 1
fi
