#!/usr/bin/env bash
# mrl_iam_check.sh - Amendment 10 B.4: verify (read-only) that the EXISTING roles already cover
# every IAM action the DEFAULT MRL deployment needs, so the operator can confirm "NO IAM CHANGE
# NEEDED" before deploying. CloudShell-friendly (aws cli + python3). Makes NO changes.
#
# It discovers the three role ARNs from the EXISTING pipeline resources, then runs
# `aws iam simulate-principal-policy` for every (role, action, resource) in the SINGLE source of
# truth tools/mrl_required_actions.json (the same file tests/test_existing_iam_covers_mrl.py uses).
#
# NOTE: SimulatePrincipalPolicy does NOT fully evaluate permission boundaries or SCPs - it checks
# the identity-based policies. Treat ALLOWED here as necessary but confirm boundaries/SCP with
# your security team if in doubt. If simulate itself is denied (AccessDenied), this script falls
# back to printing the attached/inline policy NAMES and says so (it does NOT claim success).
#
# Usage: mrl_iam_check.sh --project <p> --region <r> --account-id <a> --bucket <b>
#        [--glue-job <name>]   (a <project>-*-discovery or -load job to read the Glue role from;
#                               only needed if none can be auto-discovered)
set -euo pipefail
export AWS_PAGER=""

PROJECT=""; REGION=""; ACCOUNT_ID=""; BUCKET=""; GLUE_JOB=""
while [ $# -gt 0 ]; do
  case "$1" in
    --project) PROJECT="$2"; shift ;;
    --region) REGION="$2"; shift ;;
    --account-id) ACCOUNT_ID="$2"; shift ;;
    --bucket) BUCKET="$2"; shift ;;
    --glue-job) GLUE_JOB="$2"; shift ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
  shift
done
for v in PROJECT REGION ACCOUNT_ID BUCKET; do
  if [ -z "$(eval echo \$$v)" ]; then echo "ERROR: --$(echo $v | tr A-Z_ a-z- | sed 's/^-//') is required" >&2; exit 2; fi
done

HERE="$(cd "$(dirname "$0")" && pwd)"
REQ_JSON="$HERE/mrl_required_actions.json"
DSQL_CLUSTER_ID="${DSQL_CLUSTER_ID:-*}"   # resource glob is fine for a read-only simulate

role_arn_from_lambda() {   # Lambda role from the existing <project>-resolve-task function
  aws lambda get-function-configuration --region "$REGION" \
    --function-name "${PROJECT}-resolve-task" --query "Role" --output text 2>/dev/null || echo ""
}
role_arn_from_sfn() {      # SFN role from the existing <project>-startup state machine
  local arn
  arn="$(aws stepfunctions list-state-machines --region "$REGION" \
         --query "stateMachines[?name=='${PROJECT}-startup'].stateMachineArn" --output text 2>/dev/null || echo "")"
  [ -z "$arn" ] || [ "$arn" = "None" ] && { echo ""; return; }
  aws stepfunctions describe-state-machine --region "$REGION" --state-machine-arn "$arn" \
    --query "roleArn" --output text 2>/dev/null || echo ""
}
role_arn_from_glue() {     # Glue role from an existing <project>-*-discovery / -load job
  local job="$GLUE_JOB"
  if [ -z "$job" ]; then
    job="$(aws glue list-jobs --region "$REGION" \
           --query "JobNames[?contains(@, '${PROJECT}-') && (contains(@,'-discovery')||contains(@,'-load'))] | [0]" \
           --output text 2>/dev/null || echo "")"
  fi
  [ -z "$job" ] || [ "$job" = "None" ] && { echo ""; return; }
  aws glue get-job --region "$REGION" --job-name "$job" --query "Job.Role" --output text 2>/dev/null || echo ""
}

echo "=== MRL IAM coverage check (read-only) project=$PROJECT region=$REGION ==="
LAMBDA_ROLE="$(role_arn_from_lambda)"
SFN_ROLE="$(role_arn_from_sfn)"
GLUE_ROLE="$(role_arn_from_glue)"
echo "Discovered roles:"
echo "  lambda : ${LAMBDA_ROLE:-<not found: is --project correct? the base pipeline must be deployed>}"
echo "  sfn    : ${SFN_ROLE:-<not found>}"
echo "  glue   : ${GLUE_ROLE:-<not found>}"
if [ -z "$LAMBDA_ROLE" ]; then
  echo "WARN: ${PROJECT}-resolve-task not found - the base pipeline may not be deployed, or --project"
  echo "      is wrong. MRL integrates with those existing roles, so fix this before deploying."
fi

# Emit (role_key, role_arn, action, resource) lines from the JSON with placeholders substituted.
mapfile -t LINES < <(python3 - "$REQ_JSON" "$PROJECT" "$REGION" "$ACCOUNT_ID" "$BUCKET" \
                       "$DSQL_CLUSTER_ID" "$LAMBDA_ROLE" "$SFN_ROLE" "$GLUE_ROLE" <<'PY'
import json, sys
req, project, region, acct, bucket, dsql, lam, sfn, glue = sys.argv[1:10]
subs = {"<<PROJECT>>": project, "<<REGION>>": region, "<<ACCOUNT_ID>>": acct,
        "<<BUCKET>>": bucket, "<<DSQL_CLUSTER_ID>>": dsql}
def sub(s):
    for a,b in subs.items(): s=s.replace(a,b)
    return s
role_arn = {"lambda": lam, "stepfunctions": sfn, "glue": glue}
doc = json.load(open(req))
for role, entries in doc["default"].items():
    arn = role_arn.get(role, "")
    for e in entries:
        print("\t".join([role, arn, sub(e["action"]), sub(e["resource"])]))
PY
)

DENIED=0; SKIPPED_SIM=0; CHECKED=0
printf "\n%-14s %-34s %-55s %s\n" "ROLE" "ACTION" "RESOURCE" "RESULT"
for line in "${LINES[@]}"; do
  IFS=$'\t' read -r role arn action resource <<<"$line"
  if [ -z "$arn" ] || [ "$arn" = "None" ]; then
    printf "%-14s %-34s %-55s %s\n" "$role" "$action" "${resource:0:55}" "SKIP(role not found)"
    continue
  fi
  CHECKED=$((CHECKED+1))
  dec="$(aws iam simulate-principal-policy --policy-source-arn "$arn" \
        --action-names "$action" --resource-arns "$resource" \
        --query "EvaluationResults[0].EvalDecision" --output text 2>/dev/null || echo "SIM_ERROR")"
  if [ "$dec" = "SIM_ERROR" ]; then
    SKIPPED_SIM=$((SKIPPED_SIM+1))
    printf "%-14s %-34s %-55s %s\n" "$role" "$action" "${resource:0:55}" "SIM_DENIED?"
  elif [ "$dec" = "allowed" ]; then
    printf "%-14s %-34s %-55s %s\n" "$role" "$action" "${resource:0:55}" "ALLOWED"
  else
    DENIED=$((DENIED+1))
    printf "%-14s %-34s %-55s %s\n" "$role" "$action" "${resource:0:55}" "DENIED($dec)"
  fi
done

echo ""
if [ "$SKIPPED_SIM" -gt 0 ]; then
  echo "NOTE: aws iam simulate-principal-policy was not permitted for $SKIPPED_SIM check(s)"
  echo "      (AccessDenied on the simulate call itself). Falling back to listing the attached/"
  echo "      inline policy names so you can review them manually - NOT claiming success:"
  for role_arn in "$LAMBDA_ROLE" "$SFN_ROLE" "$GLUE_ROLE"; do
    [ -z "$role_arn" ] && continue
    rn="${role_arn##*/}"
    echo "  role $rn:"
    aws iam list-attached-role-policies --role-name "$rn" \
      --query "AttachedPolicies[].PolicyName" --output text 2>/dev/null | tr '\t' '\n' | sed 's/^/    attached: /' || true
    aws iam list-role-policies --role-name "$rn" \
      --query "PolicyNames" --output text 2>/dev/null | tr '\t' '\n' | sed 's/^/    inline:   /' || true
  done
  echo "VERDICT: COULD NOT VERIFY (simulate denied); review the policies above manually."
  echo "         Remember SimulatePrincipalPolicy also does not evaluate boundaries/SCP fully."
  exit 3
fi

if [ "$DENIED" -eq 0 ] && [ "$CHECKED" -gt 0 ]; then
  echo "VERDICT: NO IAM CHANGE NEEDED (all $CHECKED default-deployment actions ALLOWED)."
  echo "         (SimulatePrincipalPolicy does not fully evaluate boundaries/SCP - confirm if unsure.)"
  echo "         The OPTIONAL fleet state machine still needs iam/mrl-fleet-sm-optional.json."
  exit 0
else
  echo "VERDICT: $DENIED action(s) DENIED - send iam/mrl-fleet-sm-optional.json (if deploying the"
  echo "         fleet SM) or the denied lines above to your IAM team. Do NOT deploy until resolved."
  exit 1
fi
