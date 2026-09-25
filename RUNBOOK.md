# Manual Deploy Kit — DMS → S3 → Glue → Aurora DSQL (one Step Functions per DMS task)

Fully manual: **no CDK, no CloudFormation, no bootstrap.** The customer creates the IAM
roles, the Lambdas, and **one Step Functions state machine per DMS task** (task ARN hardcoded
in the SM template). The S3 bucket is the source of truth — it holds the Glue scripts, the
Glue job-definition templates, the driver wheels, and each task's manifest CSV. (Lambda code
is uploaded straight to Lambda in Step 2, not to S3.) **The state machine creates and manages
its own Glue jobs at runtime** from the S3 templates, and deletes them at cutover. No
dispatcher, no task_id CSV.

## S3 layout — ONE bucket, FIXED folders
There is exactly **one bucket** and a **fixed folder layout** inside it. You do not invent
prefixes — pick your bucket name, create these folders, done. The templates already use these
exact folder names, so the only S3 thing you edit is the **bucket name** (`<<BUCKET>>`) and,
per task, the **task suffix** (`<<TASK_SUFFIX>>`).

```
s3://<<BUCKET>>/
├── scripts/                       # the 4 Glue scripts (keep filenames)
│   ├── job1_discovery.py
│   ├── job2_load.py
│   ├── job3_validate.py
│   └── glue_cdc_continuous.py
├── glue-templates/                # the 5 job templates (keep filenames)
│   ├── discovery.json  load.json  load-big.json  validate.json  cdc.json
├── driver-fullload/               # DSQL drivers only (pg8000,scramp,asn1crypto,...) — Spark load/discovery
│   └── *.whl
├── driver-validation/             # DSQL drivers only — Spark validate
│   └── *.whl
├── driver-cdc/                    # DSQL drivers + MODERN boto3/botocore (dsql-aware) — pythonshell CDC
│   └── *.whl
├── cdc/                           # DMS writes CDC files here (your DMS S3 target)
│   └── <SCHEMA>/<TABLE>/...
└── config/
    └── _task/<<TASK_SUFFIX>>/      # ONE folder per DMS task
        ├── table_manifest.csv     # you stage this (list of tables for the task)
        └── _manifest_index.json   # Job1 writes this (+ per-table mapping JSONs)
```

So the full config prefix for a task is always `s3://<<BUCKET>>/config/_task/<<TASK_SUFFIX>>/`.

## Placeholders — what you actually edit
**Edit ONCE (same across all tasks):**
| Placeholder | Meaning | Example |
|---|---|---|
| `<<BUCKET>>` | your one source-of-truth bucket name (no `s3://`, no slash) | `my-migration-bucket` |
| `<<ACCOUNT_ID>>` | 12-digit AWS account | `123456789012` |
| `<<REGION>>` | region | `us-east-1` |
| `<<PROJECT>>` | short project name (prefix for job/role names) | `dms-dsql` |
| `<<DSQL_ENDPOINT>>` | DSQL endpoint host | `abcd.dsql.us-east-1.on.aws` |
| `<<DSQL_CLUSTER_ID>>` | DSQL cluster id (first label of endpoint) | `abcd` |
| `<<DSQL_USER>>` / `<<DSQL_DATABASE>>` | DSQL user / db | `admin` / `postgres` |
| `<<SOURCE_SECRET_ARN>>` | source DB secret ARN | `arn:aws:secretsmanager:...` |
| `<<GLUE_EXEC_ROLE_ARN>>` | Glue exec role ARN (from Step 1) | `arn:aws:iam::...:role/dms-dsql-glue-exec-role` |
| `<<*_LAMBDA_ARN>>` | the 7 Lambda ARNs (from Step 2) | `arn:aws:lambda:...:function:dms-dsql-resolve-task` |

**Edit PER TASK:**
| Placeholder | Meaning | Example |
|---|---|---|
| `<<TASK_ARN>>` | the DMS task ARN this SM drives | `arn:aws:dms:...:task:ABC` |
| `<<TASK_SUFFIX>>` | short unique per-task suffix (names the task's folder + Glue jobs) | `abc` |
| `<<CONFIG_PREFIX>>` | this task's config prefix — always `s3://<<BUCKET>>/config/_task/<<TASK_SUFFIX>>/` | `s3://my-migration-bucket/config/_task/abc/` |

**Nothing else to edit for S3.** The folder names `scripts/`, `glue-templates/`, `drivers/`,
and `cdc/` are already **hardcoded in the state-machine templates** — you do not touch them.
(If you ever rename one of those folders in your bucket, update the matching literal in
`stepfunctions/startup.asl.json` / `cutover.asl.json` to match.)

## Prerequisites (customer)
- DMS tasks are **`full-load-and-cdc`**, `StopTaskCachedChangesApplied=true`, S3 target with
  **default layout (no custom CdcPath)**, `AddColumnName=true`, `TimestampColumnName=dms_timestamp`,
  `Rfc4180=true`. For **no-PK tables**: emit **insert/delete only** (updates skipped+logged).
- **Target DDL already applied in DSQL**: every table in the manifest must already exist in
  Aurora DSQL (target schema is authoritative for column set + order). The pipeline does
  target-side data loading only — it never creates target tables.

### Step 0 — Stage the files in S3 (before anything else)
Set your bucket once, then copy into the fixed folders from the layout above. **Keep every
filename verbatim** — the templates reference the scripts by name.

```
BUCKET=my-migration-bucket        # <-- your bucket name

# 1) the 4 Glue scripts  ->  s3://$BUCKET/scripts/
aws s3 cp scripts/ s3://$BUCKET/scripts/ --recursive \
  --exclude "*" --include "job1_discovery.py" --include "job2_load.py" \
  --include "job3_validate.py" --include "glue_cdc_continuous.py"

# 2) the 5 job templates  ->  s3://$BUCKET/glue-templates/
#    (first substitute <<BUCKET>> inside each glue-templates/*.json)
sed -i '' "s/<<BUCKET>>/$BUCKET/g" glue-templates/*.json
aws s3 cp glue-templates/ s3://$BUCKET/glue-templates/ --recursive --exclude "*" --include "*.json"

# 3) driver wheels  ->  three PER-JOB folders (each Glue job type gets exactly what it needs)
#    Rationale: the glueetl (Spark) jobs — discovery/load/validate — must NOT get boto3 wheels
#    on --extra-py-files (wheels break botocore's data-dir resolution under Spark ->
#    "DataNotFoundError: endpoints"); they get a dsql-aware boto3 via --additional-python-modules.
#    The CDC job is pythonshell (NOT Spark) and DOES need boto3 wheels on --extra-py-files (the
#    in-script shim promotes them ahead of Glue's bundled, too-old boto3). So the driver folders
#    are split by job type and driver-discovery is called once per folder by the startup SM.
#
#    (a) DSQL driver: pg8000 + its deps (scramp, asn1crypto, python-dateutil, six)
pip download pg8000 -d _drv/            # pulls pg8000 + scramp + asn1crypto (+ dateutil, six)
#    (b) MODERN boto3/botocore for the CDC pythonshell job only (dsql-aware)
pip download "boto3>=1.34.0" "botocore>=1.34.0" -d _boto3/

# driver-fullload/ and driver-validation/  = DSQL drivers ONLY (Spark jobs)
aws s3 cp _drv/ s3://$BUCKET/driver-fullload/   --recursive --exclude "*" --include "*.whl"
aws s3 cp _drv/ s3://$BUCKET/driver-validation/ --recursive --exclude "*" --include "*.whl"
# driver-cdc/  = DSQL drivers + modern boto3/botocore (pythonshell CDC job)
aws s3 cp _drv/   s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"
aws s3 cp _boto3/ s3://$BUCKET/driver-cdc/ --recursive --exclude "*" --include "*.whl"

# 4) per-task table manifest  ->  s3://$BUCKET/config/_task/<TASK_SUFFIX>/table_manifest.csv
aws s3 cp table_manifest.csv s3://$BUCKET/config/_task/abc/table_manifest.csv   # abc = this task's suffix
```

`cdc/` is written by DMS (your DMS task's S3 target = `s3://$BUCKET/cdc/`); you don't upload it.

**Manifest CSV** (`config/_task/<TASK_SUFFIX>/table_manifest.csv`) — the authoritative list of
tables for **this task**. Header row required; two columns:
```
dms_schema,dms_table
SRC_SCHEMA,MY_TABLE
SRC_SCHEMA,ANOTHER_TABLE
```
`dsql_schema`/`dsql_table` are derived as the lowercase of these. Job1 reads this file from
the task's `<<CONFIG_PREFIX>>` and writes `_manifest_index.json` + per-table mapping JSONs
back to the same prefix; plan-split, load, validate, and CDC all key off that prefix.

## Step 1 — IAM (once)
Create three roles from `iam/` (substitute placeholders):
```
aws iam create-role --role-name <<PROJECT>>-glue-exec-role   --assume-role-policy-document file://iam/glue-exec-role.trust.json
aws iam put-role-policy --role-name <<PROJECT>>-glue-exec-role --policy-name glue --policy-document file://iam/glue-exec-role.policy.json
aws iam create-role --role-name <<PROJECT>>-lambda-exec-role --assume-role-policy-document file://iam/lambda-exec-role.trust.json
aws iam put-role-policy --role-name <<PROJECT>>-lambda-exec-role --policy-name lambda --policy-document file://iam/lambda-exec-role.policy.json
aws iam create-role --role-name <<PROJECT>>-sfn-exec-role    --assume-role-policy-document file://iam/sfn-exec-role.trust.json
aws iam put-role-policy --role-name <<PROJECT>>-sfn-exec-role --policy-name sfn --policy-document file://iam/sfn-exec-role.policy.json
```

## Step 2 — Lambdas (once)
Zip `lambdas/` (all `.py` at the zip root) and create 7 functions. For the DSQL Lambdas
(`drain-check`, `drop-tags`) attach a **pg8000 layer** (or bundle pg8000 into the zip) — they
connect to DSQL. Example (repeat per function; handler = `<file>.handler`):
```
cd lambdas && zip -r ../fn.zip . && cd ..
aws lambda create-function --function-name <<PROJECT>>-resolve-task \
  --runtime python3.12 --handler resolve_task.handler --timeout 120 \
  --role arn:aws:iam::<<ACCOUNT_ID>>:role/<<PROJECT>>-lambda-exec-role --zip-file fileb://fn.zip
# repeat for: driver_discovery.handler (driver-discovery), plan_split.handler (plan-split),
#   create_glue_jobs.handler (create-glue-jobs), stop_cdc_run.handler (stop-cdc-run),
#   drain_check.handler (drain-check, +pg8000 layer), drop_tags.handler (drop-tags, +pg8000 layer)
```

## Step 3 — Per-task startup state machine (repeat PER DMS TASK)
1. Copy `stepfunctions/startup.asl.json`, substitute all `<<PLACEHOLDERS>>` — especially
   `<<TASK_ARN>>`, `<<TASK_SUFFIX>>`, `<<CONFIG_PREFIX>>` (= `s3://<<BUCKET>>/config/_task/<<TASK_SUFFIX>>/`),
   and the `<<*_LAMBDA_ARN>>` values from Step 2.
2. Create the machine:
```
aws stepfunctions create-state-machine --name <<PROJECT>>-startup-<<TASK_SUFFIX>> \
  --definition file://startup.<<TASK_SUFFIX>>.asl.json \
  --role-arn arn:aws:iam::<<ACCOUNT_ID>>:role/<<PROJECT>>-sfn-exec-role
```
3. Start it (nothing auto-runs):
```
aws stepfunctions start-execution --state-machine-arn <arn-from-create> --name run-$(date +%Y%m%d-%H%M%S)
```
It then: start DMS → gate (`STOPPED_AFTER_CACHED_EVENTS`) → resolve S3 path → driver-discovery
→ **create this task's Glue jobs from S3 templates** → Job1 discovery → plan-split →
Map(load→validate) → resume CDC → start the CDC job. CDC runs continuously (~1 week).

**Resume on failure:** just start the machine again — Job1/load/validate/CDC skip completed
work via the status files; `create-glue-jobs` updates existing jobs idempotently.

## Step 4 — Per-task cutover (repeat PER DMS TASK, anytime in the week)
1. Copy `stepfunctions/cutover.asl.json`, substitute placeholders (same `<<TASK_ARN>>`,
   `<<TASK_SUFFIX>>`, `<<CONFIG_PREFIX>>`, Lambda ARNs).
2. Create + start it when ready to cut this task over:
```
aws stepfunctions create-state-machine --name <<PROJECT>>-cutover-<<TASK_SUFFIX>> \
  --definition file://cutover.<<TASK_SUFFIX>>.asl.json \
  --role-arn arn:aws:iam::<<ACCOUNT_ID>>:role/<<PROJECT>>-sfn-exec-role
aws stepfunctions start-execution --state-machine-arn <arn-from-create> --name cutover-$(date +%Y%m%d-%H%M%S)
```
It then: stop DMS CDC → poll stopped → drain-check (wait until this task's latest S3 CDC file
is applied) → stop this task's CDC run → drop `_cdc_file` on this task's tables → **delete this
task's Glue jobs** → done. Other tasks are unaffected.

## What the Step Functions expects at runtime (the contract)
The startup SM is self-contained (task ARN baked in) but **resolves these by name/key at
runtime** — they must exist exactly as below or the execution fails:

**Lambdas** (invoked by ARN you paste into the template; create with these logical roles):
| SM step | Lambda (handler) | Reads / does |
|---|---|---|
| `ResolveTask` | `resolve_task.handler` | `describe-endpoints` on the task's S3 target → derives `dmsS3Base`, `s3Bucket` |
| `DriverDiscovery` (×3) | `driver_discovery.handler` | called once per driver folder — lists `s3://<<BUCKET>>/driver-fullload/*.whl`, `driver-validation/*.whl`, `driver-cdc/*.whl` → per-job-type `--extra-py-files` (fails if no pg8000 wheel; only `driver-cdc/` includes boto3/botocore) |
| `CreateGlueJobs` | `create_glue_jobs.handler` | reads `glue-templates/<role>.json`, creates `<<PROJECT>>-<<TASK_SUFFIX>>-<role>` Glue jobs |
| `PlanSplit` | `plan_split.handler` | reads `config/_task/<<TASK_SUFFIX>>/_manifest_index.json` → per-group manifests |
| (cutover) `DrainCheck` | `drain_check.handler` | connects DSQL; waits until latest CDC file applied (needs pg8000) |
| (cutover) `StopCdcRun` | `stop_cdc_run.handler` | stops this task's CDC Glue run only |
| (cutover) `DropTags` | `drop_tags.handler` | connects DSQL; drops `_cdc_file` on this task's tables (needs pg8000) |

**S3 keys** the SM/Lambdas resolve (all under `s3://<<BUCKET>>/`):
- `scripts/{job1_discovery,job2_load,job3_validate,glue_cdc_continuous}.py`
- `glue-templates/{discovery,load,load-big,validate,cdc}.json`
- `driver-fullload/*.whl` and `driver-validation/*.whl` (DSQL drivers only: pg8000 + scramp + asn1crypto + python-dateutil + six)
- `driver-cdc/*.whl` (the same DSQL drivers **plus** modern boto3/botocore — the pythonshell CDC job)
- `config/_task/<<TASK_SUFFIX>>/table_manifest.csv` (you stage) → `.../_manifest_index.json` (Job1 writes)

**Glue jobs** are NOT pre-created — `CreateGlueJobs` makes them at runtime named
`<<PROJECT>>-<<TASK_SUFFIX>>-{discovery,load,load-big,validate,cdc}`; the later `RunDiscovery`,
`GroupLoad(Big)`, `GroupValidate`, `StartCdcJob` steps reference those exact names via
`$.glue.jobs.<role>`. Cutover deletes them.

## Notes
- **Isolation:** one SM per task = independent execution, failure, and resume. Cutting over /
  failing one task never touches another.
- **Glue jobs are per-task, created at runtime** (`<<PROJECT>>-<<TASK_SUFFIX>>-{discovery,load,
  load-big,validate,cdc}`) and deleted at cutover — no accumulation.
- **Firewall:** drivers come only from S3 (`--extra-py-files`); the pipeline never uses PyPI.
- **CDC correctness:** Tier-1 (PK or declared `logical_key`) = correct I/U/D; Tier-2 (keyless)
  = INSERT+DELETE applied, UPDATE skipped + logged to `cdc_control.cdc_skipped_ops`.
