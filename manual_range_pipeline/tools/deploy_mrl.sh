#!/usr/bin/env bash
# deploy_mrl.sh - deploy the Manual-Range-Load (MRL) pipeline ALONGSIDE the existing pipeline.
#
# It NEVER edits an existing pipeline resource. It only ADDS: the MRL Lambdas, the MRL count
# Glue job, the two MRL state machines, and uploads the patched loader script + the count script.
# Everything runs under the EXISTING customer roles (manage_iam=false convention): this script
# does NOT create or modify any IAM role; your IAM team attaches iam/mrl-additions.json first.
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
# explicitly for a VPC/PrivateLink deployment. Run from the manual_range_pipeline/ folder root.

set -euo pipefail
export AWS_PAGER=""

DRY_RUN=0
BUCKET=""; PROJECT=""; REGION=""; ACCOUNT_ID=""
GLUE_ROLE_ARN=""; SFN_ROLE_ARN=""; LAMBDA_ROLE_ARN=""
DSQL_ENDPOINT=""; DSQL_ENDPOINT_CANDIDATES=""; DSQL_USER="admin"; DSQL_DATABASE="postgres"
GLUE_CONNECTION=""

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --bucket) BUCKET="$2"; shift ;;
    --project) PROJECT="$2"; shift ;;
    --region) REGION="$2"; shift ;;
    --account-id) ACCOUNT_ID="$2"; shift ;;
    --glue-role-arn) GLUE_ROLE_ARN="$2"; shift ;;
    --sfn-role-arn) SFN_ROLE_ARN="$2"; shift ;;
    --lambda-role-arn) LAMBDA_ROLE_ARN="$2"; shift ;;
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

HERE="$(cd "$(dirname "$0")/.." && pwd)"   # manual_range_pipeline/
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

run() {
  echo "+ $*"
  if [ "$DRY_RUN" -eq 0 ]; then "$@"; fi
}

SM_BASE="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine"
LAMBDA_BASE="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${PROJECT}"
TASK_SM_ARN="${SM_BASE}:${PROJECT}-mrl-task"

echo "=== MRL deploy: project=$PROJECT region=$REGION bucket=$BUCKET (dry-run=$DRY_RUN) ==="

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

# --- 2. Build the MRL Lambda zip (include pg8000 + the MRL lambdas + the REUSED resolve_task/
#        params_csv from the base pipeline; include mrl_loader_resolver for the plan proof). ---
echo "--- Step 2: build Lambda zip (must include pg8000 for the count path's DSQL access if used) ---"
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
# pg8000 + its deps (scramp, asn1crypto) for any Lambda DSQL access (brief §10: never ship a zip
# WITHOUT pg8000).
if [ "$DRY_RUN" -eq 0 ]; then
  python3 -m pip install --quiet --target "$STAGE" pg8000 scramp asn1crypto \
    || { echo "ERROR: could not pip install pg8000/scramp/asn1crypto into the Lambda zip. A "\
              "no-network build must NOT ship an incomplete zip (brief §10). Build where pip "\
              "can reach the wheels, or vendor them into lambdas/." >&2; exit 1; }
  # B8 VERIFY: every MRL module + the reused parsers + pg8000 are present, else fail closed.
  python3 - "$STAGE" "$HERE/lambdas" <<'PYV'
import os, sys
stage, lam = sys.argv[1], sys.argv[2]
need = set(os.path.basename(f) for f in os.listdir(lam) if f.endswith(".py"))
need |= {"resolve_task.py", "params_csv.py"}
have = set(os.listdir(stage))
missing = sorted(need - have)
if missing:
    sys.exit("ERROR: MRL Lambda zip is missing module(s): %s" % missing)
if "pg8000" not in have:
    sys.exit("ERROR: MRL Lambda zip is missing pg8000/ (brief §10: never ship a zip without it).")
print("(info) zip verified: %d MRL modules + resolve_task/params_csv + pg8000 present" % len(need))
PYV
  ( cd "$STAGE" && zip -qr "$ZIP" . )
else
  echo "+ (dry-run) pip install pg8000 scramp asn1crypto -> $STAGE ; verify modules+pg8000 ; zip -> $ZIP"
fi

create_or_update_fn() {
  name="$1"; handler="$2"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "+ (dry-run) create/update lambda $name (handler $handler) from $ZIP role $LAMBDA_ROLE_ARN"
    return
  fi
  if aws lambda get-function --region "$REGION" --function-name "$name" >/dev/null 2>&1; then
    run aws lambda update-function-code --region "$REGION" --function-name "$name" --zip-file "fileb://$ZIP"
  else
    run aws lambda create-function --region "$REGION" --function-name "$name" \
      --runtime python3.11 --role "$LAMBDA_ROLE_ARN" --handler "$handler" \
      --timeout 900 --memory-size 512 --zip-file "fileb://$ZIP"
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
COUNT_JOB="${PROJECT}-mrl-count"
count_job_cmd() {
  cat <<EOF
+ create/update Glue python-shell job $COUNT_JOB
    ScriptLocation s3://$BUCKET/scripts/job_mrl_count.py   Role $GLUE_ROLE_ARN
    MaxConcurrentRuns 10   (one per task's count/probe; MRL runs one at a time per task)
EOF
}
if [ "$DRY_RUN" -eq 1 ]; then
  count_job_cmd
else
  # B7: the count job is a python-shell job that reaches DSQL via pg8000, so it needs the SAME
  # VPC connection as the load job when one is configured.
  CONN_JSON=""
  [ -n "$GLUE_CONNECTION" ] && CONN_JSON=",\"Connections\":{\"Connections\":[\"$GLUE_CONNECTION\"]}"
  if aws glue get-job --region "$REGION" --job-name "$COUNT_JOB" >/dev/null 2>&1; then
    run aws glue update-job --region "$REGION" --job-name "$COUNT_JOB" --job-update \
      "{\"Role\":\"$GLUE_ROLE_ARN\",\"Command\":{\"Name\":\"pythonshell\",\"PythonVersion\":\"3.9\",\"ScriptLocation\":\"s3://$BUCKET/scripts/job_mrl_count.py\"},\"GlueVersion\":\"3.0\",\"MaxCapacity\":1.0,\"Timeout\":2880,\"ExecutionProperty\":{\"MaxConcurrentRuns\":10},\"DefaultArguments\":{\"--enable-continuous-cloudwatch-log\":\"true\"}$CONN_JSON}"
  else
    CONN_CREATE=""
    [ -n "$GLUE_CONNECTION" ] && CONN_CREATE="--connections-list $GLUE_CONNECTION"
    run aws glue create-job --region "$REGION" --name "$COUNT_JOB" --role "$GLUE_ROLE_ARN" \
      --command "{\"Name\":\"pythonshell\",\"PythonVersion\":\"3.9\",\"ScriptLocation\":\"s3://$BUCKET/scripts/job_mrl_count.py\"}" \
      --glue-version "3.0" --max-capacity 1.0 --timeout 2880 \
      --execution-property "{\"MaxConcurrentRuns\":10}" \
      --default-arguments "{\"--enable-continuous-cloudwatch-log\":\"true\"}" \
      $CONN_CREATE \
      --tags "dsql_pipeline_project=$PROJECT,dsql_pipeline_mrl=count"
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

create_or_update_sm() {
  name="$1"; def="$2"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "+ (dry-run) create/update state machine $name from $def role $SFN_ROLE_ARN"
    return
  fi
  arn="$(aws stepfunctions list-state-machines --region "$REGION" \
         --query "stateMachines[?name=='$name'].stateMachineArn" --output text 2>/dev/null || echo "")"
  if [ -n "$arn" ] && [ "$arn" != "None" ]; then
    run aws stepfunctions update-state-machine --region "$REGION" --state-machine-arn "$arn" \
      --definition "file://$def" --role-arn "$SFN_ROLE_ARN"
  else
    run aws stepfunctions create-state-machine --region "$REGION" --name "$name" \
      --definition "file://$def" --role-arn "$SFN_ROLE_ARN"
  fi
}

# The child (task) machine MUST exist before the fleet (parent) references its ARN.
fill_sm "$HERE/stepfunctions/mrl-task.asl.json"  "$TMP/mrl-task.filled.json"
create_or_update_sm "${PROJECT}-mrl-task" "$TMP/mrl-task.filled.json"
fill_sm "$HERE/stepfunctions/mrl-fleet.asl.json" "$TMP/mrl-fleet.filled.json"
create_or_update_sm "${PROJECT}-mrl-fleet" "$TMP/mrl-fleet.filled.json"

echo "=== MRL deploy complete (dry-run=$DRY_RUN). Next: attach iam/mrl-additions.json to the"
echo "    existing roles, upload config/fleet_task_parallel_load.csv, then start ${PROJECT}-mrl-fleet with"
echo "    {\"bucket\":\"$BUCKET\"}. ==="
