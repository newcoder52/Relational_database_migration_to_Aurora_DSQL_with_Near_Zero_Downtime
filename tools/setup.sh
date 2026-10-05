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
SKIP_PERM_CHECK=0

usage() {
  echo "Usage: tools/setup.sh s3://<bucket>/config/params.csv [--with-drivers] [--dry-run] [--skip-permission-check]" >&2
  echo "       tools/setup.sh <local params.csv> --bucket <bucket> [--with-drivers] [--dry-run] [--skip-permission-check]" >&2
  echo "       (operator files live in s3://<bucket>/config/: params.csv, fleet_tasks.csv, pipeline.json)" >&2
  echo "       --skip-permission-check : with manage_iam=false, continue even if the best-effort" >&2
  echo "                                 iam simulate-principal-policy check reports a denied action." >&2
  exit 2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --with-drivers)          WITH_DRIVERS=1; shift ;;
    --dry-run)               DRY_RUN=1; shift ;;
    --skip-permission-check) SKIP_PERM_CHECK=1; shift ;;
    --bucket)                BUCKET_ARG="${2:-}"; shift 2 ;;
    -h|--help)               usage ;;
    -*)                      echo "unknown option: $1" >&2; usage ;;
    *)                       if [ -z "$PARAMS_ARG" ]; then PARAMS_ARG="$1"; else echo "unexpected arg: $1" >&2; usage; fi; shift ;;
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

# run_role_retry: like run(), but on a fresh account a just-created IAM role takes a few seconds to
# propagate, so the first create-function / create-state-machine / glue create-* can fail with an
# "role cannot be assumed" / InvalidParameterValueException propagation error even though the role
# exists. Retry ONLY those errors, with bounded backoff (~2 min total), then give up and surface
# the real error. Any OTHER (non-propagation) failure is returned immediately — no retry, no
# masking. Idempotent and portable (POSIX sh constructs; no bashisms beyond what the script uses).
# In --dry-run it just prints the command (no AWS call), like run().
run_role_retry() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'DRYRUN:'; printf ' %q' "$@"; printf '\n'
    return 0
  fi
  # retry on these substrings only (IAM role propagation to Lambda / SFN / Glue)
  _delays="5 10 15 20 30 40"      # ~2 min of bounded backoff across 6 retries
  _attempt=0
  while :; do
    _err="$TMPDIR_SETUP/.role_retry.err"
    if "$@" 2>"$_err"; then
      cat "$_err" >&2 || true
      return 0
    fi
    _msg="$(cat "$_err" 2>/dev/null || true)"
    case "$_msg" in
      *"cannot be assumed"*|*"not authorized to perform: iam:PassRole"*|\
      *"InvalidParameterValueException"*"role"*|*"Invalid principal in policy"*)
        # pick the Nth delay without unquoted word-splitting
        _next="$(printf '%s\n' "$_delays" | tr ' ' '\n' | sed -n "$((_attempt+1))p")"
        if [ -z "$_next" ]; then
          echo "ERROR: IAM role still not usable after retrying ~2 min:" >&2
          printf '%s\n' "$_msg" >&2
          return 1
        fi
        echo "(waiting ${_next}s for IAM role propagation, then retrying: $1 $2 ...)" >&2
        sleep "$_next"
        _attempt=$((_attempt+1))
        ;;
      *)
        # Not a propagation error — surface it immediately and fail (no retry).
        printf '%s\n' "$_msg" >&2
        return 1
        ;;
    esac
  done
}

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
# Role ARNs the Lambdas / state machines are pointed at, plus the role NAME (last ARN segment,
# path-aware) each iam call uses. lambda_role_arn / sfn_role_arn are setup-only (not in settings).
emit("P_LAMBDA_ROLE_ARN", p["lambda_role_arn"])
emit("P_SFN_ROLE_ARN", p["sfn_role_arn"])
emit("P_GLUE_ROLE_NAME", pc.role_name_from_arn(settings["glue_role_arn"]))
emit("P_LAMBDA_ROLE_NAME", pc.role_name_from_arn(p["lambda_role_arn"]))
emit("P_SFN_ROLE_NAME", pc.role_name_from_arn(p["sfn_role_arn"]))
emit("P_MANAGE_IAM", p["manage_iam"])
for w in parsed["warnings"]:
    sys.stderr.write("(warn) %s\n" % w)
PY
)" || { echo "aborting: fix params.csv and re-run." >&2; exit 1; }
eval "$EXPORTS"

# Derived names. The three role ARNs and their role NAMES now come from the CSV (params_csv
# derives the <project>-{glue,lambda,sfn}-exec-role defaults when the operator omits them, so a
# fully-defaulted CSV yields exactly the former values). The role NAME is the last ARN segment
# (path-aware), used for every iam get-role / put-role-policy / create-role call.
GLUE_ROLE_ARN="$P_GLUE_ROLE_ARN"
GLUE_ROLE_NAME="$P_GLUE_ROLE_NAME"
LAMBDA_ROLE_ARN="$P_LAMBDA_ROLE_ARN"
LAMBDA_ROLE_NAME="$P_LAMBDA_ROLE_NAME"
SFN_ROLE_ARN="$P_SFN_ROLE_ARN"
SFN_ROLE_NAME="$P_SFN_ROLE_NAME"
MANAGE_IAM="$P_MANAGE_IAM"
LAMBDA_BASE="arn:aws:lambda:$P_REGION:$P_ACCOUNT_ID:function:$P_PROJECT"
SM_BASE="arn:aws:states:$P_REGION:$P_ACCOUNT_ID:stateMachine"
HAVE_VPC=0
[ -n "$P_SUBNET_ID" ] && [ -n "$P_SECURITY_GROUP_ID" ] && HAVE_VPC=1

echo "=== setup for project=$P_PROJECT region=$P_REGION bucket=$BUCKET (dry-run=$DRY_RUN, with-drivers=$WITH_DRIVERS, manage_iam=$MANAGE_IAM) ==="
echo "roles in use    : glue=$GLUE_ROLE_ARN"
echo "                  lambda=$LAMBDA_ROLE_ARN"
echo "                  sfn=$SFN_ROLE_ARN"

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

# split_iam SRC OUTDIR ROLE_NAME_OVERRIDE
#   Fills the <<...>> placeholders in the combined file SRC and writes, into OUTDIR:
#     <base>.trust.filled.json, <base>.policy.filled.json, and (if a VpcPolicy block exists)
#     <base>.vpc.filled.json. ROLE_NAME_OVERRIDE (if non-empty) replaces the template's RoleName
#     so a customer-named role (from <svc>_role_arn) is created/updated/filled instead of the
#     <project>-<svc>-exec-role default. Prints one line "ROLE=<name> VPC=<0|1> PRINCIPAL=<svc>"
#     on stdout: the (possibly overridden) role name, whether a VpcPolicy was present, and the
#     single service principal the TrustPolicy grants AssumeRole to (glue/lambda/states .amazonaws
#     .com) so the caller can check an existing role's trust. Fails (exit 1) if any <<...>>
#     placeholder is left. Portable: python3 only.
split_iam() {
  src="$1"; outdir="$2"; name_override="${3:-}"
  P_REGION="$P_REGION" P_ACCOUNT_ID="$P_ACCOUNT_ID" P_BUCKET="$BUCKET" \
  P_DSQL_CLUSTER_ID="$P_DSQL_CLUSTER_ID" P_PROJECT="$P_PROJECT" \
  P_GLUE_ROLE_NAME="$GLUE_ROLE_NAME" P_ROLE_NAME_OVERRIDE="$name_override" \
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
override = os.environ.get("P_ROLE_NAME_OVERRIDE", "").strip()
role = override if override else doc["RoleName"]
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
# The single service principal this role must trust (one Service string in the trust policy).
principal = ""
for st in doc["TrustPolicy"].get("Statement", []):
    pr = st.get("Principal", {}).get("Service")
    if isinstance(pr, str):
        principal = pr
    elif isinstance(pr, list) and pr:
        principal = pr[0]
print("ROLE=%s VPC=%d PRINCIPAL=%s" % (role, have_vpc, principal))
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

# role_name_for_base BASE -> the role NAME this service's role should use (ARN-derived; a
# fully-defaulted CSV gives back <project>-<svc>-exec-role, so manage_iam=true is unchanged).
role_name_for_base() {
  case "$1" in
    glue)         echo "$GLUE_ROLE_NAME" ;;
    lambda)       echo "$LAMBDA_ROLE_NAME" ;;
    stepfunctions) echo "$SFN_ROLE_NAME" ;;
  esac
}
# required_principal_for_base BASE -> the service principal that role's trust MUST allow.
required_principal_for_base() {
  case "$1" in
    glue)          echo "glue.amazonaws.com" ;;
    lambda)        echo "lambda.amazonaws.com" ;;
    stepfunctions) echo "states.amazonaws.com" ;;
  esac
}

# trust_allows ROLE_NAME PRINCIPAL -> prints "YES"/"NO". Reads the live role's AssumeRolePolicy
# document and checks any Allow sts:AssumeRole statement whose Principal.Service contains
# PRINCIPAL. Best-effort / read-only (iam get-role). In --dry-run it cannot read, so prints
# "DRYRUN".
trust_allows() {
  _rn="$1"; _principal="$2"
  if [ "$DRY_RUN" -eq 1 ]; then echo "DRYRUN"; return 0; fi
  _doc="$(aws iam get-role --role-name "$_rn" \
          --query 'Role.AssumeRolePolicyDocument' --output json 2>/dev/null || echo "")"
  [ -n "$_doc" ] || { echo "NO"; return 0; }
  P_PRINCIPAL="$_principal" python3 - "$_doc" <<'PY'
import json, os, sys
want = os.environ["P_PRINCIPAL"]
try:
    doc = json.loads(sys.argv[1])
except Exception:
    print("NO"); raise SystemExit(0)
ok = False
for st in doc.get("Statement", []) if isinstance(doc, dict) else []:
    if st.get("Effect") != "Allow":
        continue
    act = st.get("Action", [])
    acts = [act] if isinstance(act, str) else list(act or [])
    if not any(a in ("sts:AssumeRole", "sts:*", "*") for a in acts):
        continue
    svc = st.get("Principal", {}).get("Service", [])
    svcs = [svc] if isinstance(svc, str) else list(svc or [])
    if want in svcs:
        ok = True
        break
print("YES" if ok else "NO")
PY
}

# -------- Step 1, mode-independent prep: fill + split all 3 combined files (role NAME override) --
# IAM_OUT holds the customer-facing policy files written in manage_iam=false mode.
IAM_OUT="iam-out"
declare -a _ROLE_BASES=() _ROLE_NAMES=() _ROLE_PRINCIPALS=() _ROLE_HASVPC=()
for f in $IAM_FILES; do
  base="$(basename "${f%.json}")"             # glue | lambda | stepfunctions
  override="$(role_name_for_base "$base")"
  # Fill + split into $TMPDIR_SETUP/iam and, per the RUNBOOK convention, next to the repo file
  # too (iam/<base>.{trust,policy,vpc}.filled.json; .gitignore excludes *.filled.json).
  meta="$(split_iam "$f" "$TMPDIR_SETUP/iam" "$override")"
  split_iam "$f" "iam" "$override" >/dev/null
  rname="${meta#ROLE=}"; rname="${rname%% *}"
  have_vpc_file="${meta##*VPC=}"; have_vpc_file="${have_vpc_file%% *}"
  principal="${meta##*PRINCIPAL=}"
  _ROLE_BASES+=("$base"); _ROLE_NAMES+=("$rname")
  _ROLE_PRINCIPALS+=("$principal"); _ROLE_HASVPC+=("$have_vpc_file")
done

if [ "$MANAGE_IAM" = "true" ]; then
  # ------------------------------------------------------------------------------------------
  # manage_iam=true — create/update the 3 roles (unchanged behaviour; byte-identical AWS calls
  # when the three ARNs are defaulted, since the role names equal <project>-<svc>-exec-role).
  # ------------------------------------------------------------------------------------------
  i=0
  for base in "${_ROLE_BASES[@]}"; do
    rname="${_ROLE_NAMES[$i]}"
    have_vpc_file="${_ROLE_HASVPC[$i]}"
    trust_filled="$TMPDIR_SETUP/iam/$base.trust.filled.json"
    policy_filled="$TMPDIR_SETUP/iam/$base.policy.filled.json"
    create_or_update_role "$rname" "$trust_filled" "$policy_filled"
    if [ "$base" = "glue" ] && [ "$HAVE_VPC" -eq 1 ] && [ "$have_vpc_file" = "1" ]; then
      run aws iam put-role-policy --role-name "$rname" --policy-name glue-vpc \
        --policy-document "file://$TMPDIR_SETUP/iam/$base.vpc.filled.json"
    fi
    i=$((i+1))
  done

  # Upgrade path: the former per-component roles are no longer used. We do NOT delete them (a
  # live e2e run may still reference them); just tell the operator how to remove them once this
  # update is verified.
  if [ "$DRY_RUN" -eq 0 ]; then
    for old in preflight-tasks-role fleet-startup-role fleet-cutover-role; do
      if aws iam get-role --role-name "$P_PROJECT-$old" >/dev/null 2>&1; then
        echo "NOTE: role '$P_PROJECT-$old' is no longer used (its permissions were merged into $P_PROJECT-lambda-exec-role / $P_PROJECT-sfn-exec-role)." >&2
        echo "      After verifying this update, remove it with:" >&2
        echo "        aws iam delete-role-policy --role-name $P_PROJECT-$old --policy-name $P_PROJECT-$old 2>/dev/null; aws iam delete-role --role-name $P_PROJECT-$old" >&2
      fi
    done
  fi
else
  # ------------------------------------------------------------------------------------------
  # manage_iam=false — use the customer's EXISTING roles. NEVER call create-role,
  # update-assume-role-policy, put-role-policy, attach-role-policy or any delete-*. The ONLY iam
  # calls here are READS: get-role, list-role-policies, list-attached-role-policies, and
  # simulate-principal-policy (best-effort).
  # ------------------------------------------------------------------------------------------
  echo "manage_iam=false: using the roles you already have; setup will NOT create or modify any IAM role."

  # (b) Every role must already exist. Fail before touching anything, naming a missing role.
  if [ "$DRY_RUN" -eq 0 ]; then
    i=0; missing_roles=""
    for base in "${_ROLE_BASES[@]}"; do
      rname="${_ROLE_NAMES[$i]}"
      if ! aws iam get-role --role-name "$rname" >/dev/null 2>&1; then
        missing_roles="$missing_roles $base:$rname"
      fi
      i=$((i+1))
    done
    if [ -n "$missing_roles" ]; then
      echo "ERROR: manage_iam=false but these role(s) do not exist (iam get-role failed) — nothing was changed:" >&2
      for pair in $missing_roles; do
        echo "         ${pair%%:*} role: ${pair#*:}" >&2
      done
      echo "       Ask your IAM team to create them, or set the matching *_role_arn in params.csv, then re-run." >&2
      exit 1
    fi
  else
    for base in "${_ROLE_BASES[@]}"; do :; done
    echo "DRYRUN(read): aws iam get-role for each of ${_ROLE_NAMES[*]} (would fail-closed if any is missing)"
  fi

  # (c) Trust check: each role must trust its service principal. Fail (before any change) naming
  # the role, and print the EXACT trust statement to add.
  if [ "$DRY_RUN" -eq 0 ]; then
    i=0; trust_fail=0
    for base in "${_ROLE_BASES[@]}"; do
      rname="${_ROLE_NAMES[$i]}"
      principal="$(required_principal_for_base "$base")"
      verdict="$(trust_allows "$rname" "$principal")"
      if [ "$verdict" != "YES" ]; then
        trust_fail=1
        echo "ERROR: role '$rname' ($base) does not trust $principal — it cannot be assumed by that service." >&2
        echo "       Add this statement to the role's trust policy (aws iam update-assume-role-policy), then re-run:" >&2
        cat >&2 <<TRUST
       {
         "Effect": "Allow",
         "Principal": { "Service": "$principal" },
         "Action": "sts:AssumeRole"
       }
TRUST
      fi
      i=$((i+1))
    done
    if [ "$trust_fail" -ne 0 ]; then
      echo "       (no IAM, Lambda, Glue or Step Functions resource was created or changed.)" >&2
      exit 1
    fi
  else
    i=0
    for base in "${_ROLE_BASES[@]}"; do
      rname="${_ROLE_NAMES[$i]}"; principal="$(required_principal_for_base "$base")"
      echo "DRYRUN(read): aws iam get-role --role-name $rname (would require trust for $principal)"
      i=$((i+1))
    done
  fi

  # (d) Fill the three policies with the REAL role names/ARNs and write them where the IAM team
  # can attach them, plus a README-IAM.txt. The glue VPC add-on is included on the Glue role only
  # when a VPC connection is configured. The Lambda policy's iam:PassRole already names the Glue
  # role (via <<GLUE_EXEC_ROLE_NAME>> = $GLUE_ROLE_NAME, filled above).
  rm -rf "$IAM_OUT"; mkdir -p "$IAM_OUT"
  i=0
  : > "$TMPDIR_SETUP/readme_iam.txt"
  {
    echo "IAM for this pipeline (manage_iam=false) — give these to your IAM team."
    echo "setup.sh does NOT create or modify any role; it only reads the three roles below."
    echo ""
    echo "project : $P_PROJECT    account : $P_ACCOUNT_ID    region : $P_REGION"
    echo ""
  } >> "$TMPDIR_SETUP/readme_iam.txt"
  for base in "${_ROLE_BASES[@]}"; do
    rname="${_ROLE_NAMES[$i]}"
    have_vpc_file="${_ROLE_HASVPC[$i]}"
    principal="$(required_principal_for_base "$base")"
    # Combined per-role policy file: the inline Policy, plus (Glue + VPC only) the VPC add-on
    # statements appended, so the IAM team attaches ONE JSON per role.
    out="$IAM_OUT/$rname.policy.json"
    add_vpc=0
    if [ "$base" = "glue" ] && [ "$HAVE_VPC" -eq 1 ] && [ "$have_vpc_file" = "1" ]; then add_vpc=1; fi
    P_POLICY="$TMPDIR_SETUP/iam/$base.policy.filled.json" \
    P_VPC="$TMPDIR_SETUP/iam/$base.vpc.filled.json" P_ADD_VPC="$add_vpc" \
    python3 - "$out" <<'PY'
import json, os, sys
out = sys.argv[1]
pol = json.load(open(os.environ["P_POLICY"], encoding="utf-8"))
if os.environ.get("P_ADD_VPC") == "1":
    vpc = json.load(open(os.environ["P_VPC"], encoding="utf-8"))
    pol.setdefault("Statement", []).extend(vpc.get("Statement", []))
open(out, "w", encoding="utf-8").write(json.dumps(pol, indent=2) + "\n")
PY
    # README-IAM.txt entry for this role.
    {
      echo "ROLE: $rname   (the $base role; ARN must be in account $P_ACCOUNT_ID)"
      echo "  trust must allow : $principal   (sts:AssumeRole)"
      echo "  attach INLINE policy JSON : $IAM_OUT/$rname.policy.json"
      if [ "$base" = "lambda" ]; then
        echo "  note: this policy's iam:PassRole names the Glue role '$GLUE_ROLE_NAME' (so the Lambdas can pass it to Glue jobs)."
        if [ "$HAVE_VPC" -eq 1 ]; then
          echo "  managed policy to ATTACH (VPC on) : arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
        fi
      fi
      if [ "$base" = "glue" ] && [ "$add_vpc" -eq 1 ]; then
        echo "  note: the VPC networking statements are INCLUDED in the policy JSON above (a Glue connection is configured)."
      fi
      echo ""
    } >> "$TMPDIR_SETUP/readme_iam.txt"
    i=$((i+1))
  done
  cp "$TMPDIR_SETUP/readme_iam.txt" "$IAM_OUT/README-IAM.txt"
  echo "wrote per-role policy files and README-IAM.txt to: $IAM_OUT/"
  echo "  (one <rolename>.policy.json per role + README-IAM.txt listing trust + what to attach)"

  # (e) Best-effort permissions check with iam simulate-principal-policy. For each role, simulate
  # the key actions its filled policy grants. If any action is denied (or the simulate call is
  # itself denied), print a WARNING; then stop unless --skip-permission-check was passed.
  # Key actions per role (a representative subset of what each policy grants).
  perm_warn=0
  if [ "$DRY_RUN" -eq 0 ]; then
    i=0
    for base in "${_ROLE_BASES[@]}"; do
      rname="${_ROLE_NAMES[$i]}"
      case "$base" in
        glue)          actions="s3:GetObject s3:PutObject dsql:DbConnectAdmin logs:PutLogEvents" ;;
        lambda)        actions="s3:GetObject s3:PutObject glue:CreateJob glue:StartJobRun iam:PassRole states:ListExecutions" ;;
        stepfunctions) actions="lambda:InvokeFunction glue:StartJobRun states:StartExecution dms:StartReplicationTask" ;;
      esac
      role_arn="arn:aws:iam::$P_ACCOUNT_ID:role/$rname"
      sim="$(aws iam simulate-principal-policy --policy-source-arn "$role_arn" \
             --action-names $actions \
             --query 'EvaluationResults[?EvalDecision!=`allowed`].[EvalActionName,EvalDecision]' \
             --output text 2>"$TMPDIR_SETUP/.sim.err" || echo "__SIMERR__")"
      if [ "$sim" = "__SIMERR__" ]; then
        perm_warn=1
        echo "WARNING: could not run iam simulate-principal-policy for role '$rname' (the call was denied or errored):" >&2
        sed 's/^/           /' "$TMPDIR_SETUP/.sim.err" >&2 || true
        echo "         Could not verify these actions: $actions" >&2
      elif [ -n "$sim" ]; then
        perm_warn=1
        echo "WARNING: role '$rname' is MISSING permissions its pipeline policy needs (simulate said not allowed):" >&2
        printf '%s\n' "$sim" | sed 's/^/           /' >&2
      else
        echo "permissions check: role '$rname' allows the key actions ($actions)."
      fi
      i=$((i+1))
    done
  else
    echo "DRYRUN(read): aws iam simulate-principal-policy for each role against its key actions (best-effort)."
  fi
  if [ "$perm_warn" -ne 0 ]; then
    if [ "$SKIP_PERM_CHECK" -eq 1 ]; then
      echo "WARNING: continuing despite the permission warning(s) above because --skip-permission-check was given." >&2
    else
      echo "ERROR: stopping before creating anything because the permission check above reported a problem." >&2
      echo "       Ask your IAM team to fix the role policy, or re-run with --skip-permission-check to continue anyway." >&2
      echo "       (no IAM, Lambda, Glue or Step Functions resource was created or changed.)" >&2
      exit 1
    fi
  fi
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
        --query 'Subnets[0].AvailabilityZone' --output text 2>/dev/null || true)"
  # describe-subnets can fail (bad/unreadable subnet) or print "None"; the `|| true` keeps set -e
  # from aborting here so we can give a clear message. In --dry-run `capture` echoes "" by design,
  # so skip the hard check there (nothing is created).
  if [ "$DRY_RUN" -eq 0 ] && { [ -z "$AZ" ] || [ "$AZ" = "None" ]; }; then
    echo "ERROR: could not resolve the Availability Zone for subnet '$P_SUBNET_ID'." >&2
    echo "       Check that subnet_id is correct and that you have ec2:DescribeSubnets, then re-run." >&2
    exit 1
  fi
  # Build the Glue connection input as JSON with python3 (json.dumps) rather than shell string
  # interpolation, so a quote/brace/backslash in glue_connection/subnet/sg (none are character-
  # validated upstream) can never produce malformed JSON (F-M1). Values are passed via env.
  CONN_INPUT="$(P_GLUE_CONNECTION="$P_GLUE_CONNECTION" P_SUBNET_ID="$P_SUBNET_ID" \
    P_SECURITY_GROUP_ID="$P_SECURITY_GROUP_ID" P_AZ="${AZ:-}" python3 - <<'PY'
import json, os
pcr = {
    "SubnetId": os.environ["P_SUBNET_ID"],
    "SecurityGroupIdList": [os.environ["P_SECURITY_GROUP_ID"]],
}
az = os.environ.get("P_AZ", "")
if az:
    pcr["AvailabilityZone"] = az
print(json.dumps({
    "Name": os.environ["P_GLUE_CONNECTION"],
    "ConnectionType": "NETWORK",
    "ConnectionProperties": {},
    "PhysicalConnectionRequirements": pcr,
}))
PY
)"
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
  # Verify EVERY lambdas/*.py made it into the zip (all 8 handlers + shared modules like
  # params_csv.py / prepare_cdc_wheels.py / preflight_tasks.py), derived from the folder rather
  # than a hard-coded list, plus pg8000. Fail naming exactly what is missing (F-M3). The check is
  # done in python3 (portable, no fragile grep over the unzip columns): compare the set of required
  # names against the basenames/paths zipimport actually stored.
  if ! unzip -Z1 "$FN_ZIP" 2>/dev/null > "$TMPDIR_SETUP/fn.zip.names"; then
    unzip -l "$FN_ZIP" | awk 'NR>3{ $1=$2=$3=""; sub(/^ +/,""); if ($0!="") print }' \
      > "$TMPDIR_SETUP/fn.zip.names"
  fi
  python3 - "$TMPDIR_SETUP/fn.zip.names" lambdas/*.py <<'PY'
import os, sys
names_file, srcs = sys.argv[1], sys.argv[2:]
entries = [ln.strip() for ln in open(names_file, encoding="utf-8") if ln.strip()]
bases = {os.path.basename(e.rstrip("/")) for e in entries}
required = [os.path.basename(s) for s in srcs]          # every lambdas/*.py
missing = [r for r in required if r not in bases]
# pg8000 must be present as a package (any pg8000/ entry in the zip)
if not any(e == "pg8000/__init__.py" or e.startswith("pg8000/") for e in entries):
    missing.append("pg8000/__init__.py")
if missing:
    sys.stderr.write("ERROR: fn.zip is missing: " + " ".join(missing) + "\n")
    sys.exit(1)
print("built fn.zip (all %d lambdas/*.py + pg8000 present)" % len(required))
PY
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
    run_role_retry aws lambda create-function --function-name "$name" --zip-file "fileb://$FN_ZIP" \
      --handler "$handler" --runtime python3.12 --memory-size 1024 --timeout 300 \
      --role "$role" --query FunctionName --output text
    run aws lambda wait function-active-v2 --function-name "$name"
  fi
done <<EOF
$LAMBDA_SPECS
EOF

# VPC-attach the two DSQL Lambdas
# VPC-attach the two DSQL Lambdas
if [ "$HAVE_VPC" -eq 1 ]; then
  # Attaching AWSLambdaVPCAccessExecutionRole is an IAM WRITE: only do it when we manage IAM.
  # With manage_iam=false the IAM team attaches it (README-IAM.txt / Step 1 says so).
  if [ "$MANAGE_IAM" = "true" ]; then
    run aws iam attach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
  else
    echo "manage_iam=false: NOT attaching AWSLambdaVPCAccessExecutionRole to '$LAMBDA_ROLE_NAME' — your IAM team must attach it (see $IAM_OUT/README-IAM.txt)."
  fi
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
    run_role_retry aws stepfunctions create-state-machine --name "$name" \
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
# Step 4b — legacy objects NOTE (never deleted; just reported with the delete command)
# =============================================================================================
# Old per-task workflows were named <project>-startup-* / <project>-cutover-* (e.g.
# <project>-startup-task-full-cdc-01); the current code uses the four fixed machines above. If any
# such legacy machine still exists, list it with the delete command — but NEVER delete it here.
# Also note old Glue jobs named <project>-* that lack the dsql_pipeline_project tag (the new code
# refuses to reuse an untagged job). Read-only; runs in both IAM modes.
if [ "$DRY_RUN" -eq 0 ]; then
  echo "--- Step 4b: legacy objects (reported only, never deleted) ---"
  LEGACY_SM="$(aws stepfunctions list-state-machines \
      --query "stateMachines[?starts_with(name,'$P_PROJECT-startup-') || starts_with(name,'$P_PROJECT-cutover-')].name" \
      --output text 2>/dev/null || echo "")"
  if [ -n "$LEGACY_SM" ] && [ "$LEGACY_SM" != "None" ]; then
    echo "NOTE: these look like OLD per-task state machines (not used by the current pipeline). Review, then delete them yourself — setup will NEVER delete them:" >&2
    for nm in $LEGACY_SM; do
      arn="$(aws stepfunctions list-state-machines --query "stateMachines[?name=='$nm'].stateMachineArn" --output text 2>/dev/null || echo "")"
      echo "        $nm" >&2
      echo "          aws stepfunctions delete-state-machine --state-machine-arn ${arn:-<arn>}" >&2
    done
  else
    echo "legacy state machines : none found (<project>-startup-* / <project>-cutover-*)."
  fi
  # Old Glue jobs named <project>-* with no dsql_pipeline_project tag.
  UNTAGGED_JOBS="$(P_PROJECT="$P_PROJECT" python3 - <<'PY' 2>/dev/null || true
import json, os, subprocess
proj = os.environ["P_PROJECT"]
def aws(*a):
    return subprocess.run(["aws", *a], capture_output=True, text=True)
r = aws("glue", "list-jobs", "--output", "json")
if r.returncode != 0:
    raise SystemExit(0)
try:
    names = json.loads(r.stdout).get("JobNames", [])
except Exception:
    raise SystemExit(0)
out = []
for n in names:
    if not n.startswith(proj + "-"):
        continue
    gj = aws("glue", "get-job", "--job-name", n, "--query", "Job.DefaultArguments", "--output", "json")
    # tags live on the resource, not DefaultArguments; use get-tags on the job ARN.
    region = os.environ.get("P_REGION", "")
    # Resolve account/region via the job ARN is overkill offline; use get-tags best-effort.
    acct = os.environ.get("P_ACCOUNT_ID", "")
    arn = "arn:aws:glue:%s:%s:job/%s" % (os.environ.get("P_REGION", ""), acct, n)
    tg = aws("glue", "get-tags", "--resource-arn", arn, "--query", "Tags", "--output", "json")
    tags = {}
    if tg.returncode == 0:
        try:
            tags = json.loads(tg.stdout) or {}
        except Exception:
            tags = {}
    if "dsql_pipeline_project" not in tags:
        out.append(n)
print("\n".join(out))
PY
)"
  if [ -n "$UNTAGGED_JOBS" ]; then
    echo "NOTE: these Glue jobs are named '$P_PROJECT-*' but have NO dsql_pipeline_project tag — the new code refuses to reuse them. Review/remove them yourself (never auto-deleted):" >&2
    for jn in $UNTAGGED_JOBS; do
      echo "        $jn" >&2
      echo "          aws glue delete-job --job-name $jn" >&2
    done
  else
    echo "untagged legacy Glue jobs : none found."
  fi
else
  echo "--- Step 4b: legacy objects — skipped in --dry-run (needs read calls) ---"
fi

# =============================================================================================
# Summary
# =============================================================================================
echo ""
echo "=== setup summary ==="
echo "project         : $P_PROJECT"
echo "region          : $P_REGION"
echo "bucket          : $BUCKET"
if [ "$MANAGE_IAM" = "true" ]; then
  echo "roles (3)       : setup created/updated glue=$GLUE_ROLE_NAME lambda=$LAMBDA_ROLE_NAME sfn=$SFN_ROLE_NAME (one per service)"
else
  echo "roles (3)       : used EXISTING glue=$GLUE_ROLE_NAME lambda=$LAMBDA_ROLE_NAME sfn=$SFN_ROLE_NAME (manage_iam=false; not created or modified)"
fi
echo "lambdas (8)     : $P_PROJECT-{resolve-task,driver-discovery,plan-split,create-glue-jobs,stop-cdc-run,drain-check,drop-tags,preflight-tasks} — all run with role $LAMBDA_ROLE_ARN"
echo "state machines(4): $P_PROJECT-{startup,cutover,fleet-startup,fleet-cutover} — all run with role $SFN_ROLE_ARN"
_tmpl_files=(glue-templates/*.json)
echo "glue scripts    : 5 uploaded to s3://$BUCKET/scripts/"
echo "glue templates  : ${#_tmpl_files[@]} uploaded to s3://$BUCKET/glue-templates/"
if [ "$WITH_DRIVERS" -eq 1 ]; then echo "drivers         : staged to driver-fullload/validation/cdc"; else echo "drivers         : NOT staged (re-run with --with-drivers)"; fi
if [ "$HAVE_VPC" -eq 1 ]; then echo "glue connection : $P_GLUE_CONNECTION ($P_SUBNET_ID / $P_SECURITY_GROUP_ID)"; else echo "glue connection : none (no VPC)"; fi
if [ "$PUBLISH_SETTINGS" -eq 1 ]; then echo "pipeline.json   : published from params.csv"; else echo "pipeline.json   : left unchanged (identical, running, or dry-run)"; fi
if [ "$DRY_RUN" -eq 1 ]; then echo "(dry-run: no AWS calls were made)"; fi
echo "done."
