# deployment_fingerprint

Read-only inventory of what is deployed for the DMS -> Aurora DSQL pipeline (S3 scripts / templates / config,
Lambda packages, Step Functions definitions, Glue jobs). It records the sha256 of every deployed file plus
feature markers, so each file can be matched to an exact commit of this repo and a precise upgrade plan
can be written. It **only reads** AWS and writes **nothing** to AWS.

## Run (AWS CloudShell, in the account and region that hold the pipeline)

    python3 deployment_fingerprint.py --bucket <pipeline-bucket> --project <project-prefix> --region <region>
    cat deployment_fingerprint_*/report.txt

`--project` is the prefix used in the Lambda, state machine and Glue job names. Add `--bundle` to also write a
zip with copies of the deployed files (that zip is **not** masked), and `--extra-prefix <folder>/` to hash one
more S3 folder.

IAM it uses (read-only): `sts:GetCallerIdentity`, `s3:ListBucket`, `s3:GetObject`, `lambda:ListFunctions`,
`lambda:GetFunction`, `states:ListStateMachines`, `states:DescribeStateMachine`, `glue:GetJobs`. A denied call
is recorded in the report as an ERROR for that section and the rest still runs.

## What the report contains
* sha256 + size + last-modified of each S3 file under `scripts/`, `glue-templates/`, `config/`, `lambdas/`,
  `stepfunctions/`, `tools/`, `iam/` (and any folder whose name looks like scripts / glue / templates);
  per-task state folders (`config/_task/...`) and processed/failed folders are skipped; backup objects are listed by name only.
* The shared Lambda package: sha256 of each source file inside the zip.
* Step Functions: raw and normalized definition hashes, state count, the exact `ResolveTask` ResultSelector key list.
* Glue jobs: script location, worker type/count, timeout, DefaultArguments (secret-looking keys redacted).
* Feature markers found in each file (for example `post_load_check`, `null_notnull_text`, `stopAfterLoad`).
* A **call-before-definition** check on every Python file (a module that calls a function before it is defined fails at import).

Twelve-digit account ids are masked in `report.txt` / `report.json`.

## Self-test (no AWS needed)

    python3 selftest.py

Runs the script against a fake boto3 and checks masking, redaction, skipping and the call-before-definition detector.
