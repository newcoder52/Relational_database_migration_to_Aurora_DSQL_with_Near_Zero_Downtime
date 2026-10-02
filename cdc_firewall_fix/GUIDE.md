# CDC Firewall Fix Kit: Apply Guide

**For:** Glue CDC job (`glue_cdc_continuous.py`) failing in a VPC that has no internet access.
**Applies to:** the Oracle → DMS → S3 → Glue → Aurora DSQL pipeline (repo `newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime`, `main` @ `c2a5a12`).
**Last updated:** Fri Oct 2, 2026

---

## 1. The problem in one paragraph

The CDC job is a Glue **Python shell** job (Python 3.9). Before the script starts, Glue
**pip-installs** every wheel in `--extra-py-files`, **one wheel at a time**. When a wheel lists a
dependency that isn't installed yet (boto3 → botocore, python_dateutil → six, pg8000 → scramp, …), pip
looks for it on **pypi.org**, even though that dependency's wheel was also downloaded from S3. Behind a firewall that
call times out, and after about 20 minutes the job fails with:

```
CommandFailedException: /tmp/glue-python-libs-XXXX/<some>.whl installation failed after 2th retry
due to exception: CalledProcessError
...  Connection to pypi.org timed out ... No matching distribution found for botocore
```

The **full-load jobs are not affected**: they are Spark jobs, and Spark puts `--extra-py-files` straight
on the Python path without pip.

A second, separate problem: **scramp 1.4.7 and later need Python 3.10**, so they can never install on the CDC
job's Python 3.9. Use **scramp 1.4.6**.

---

## 2. What's in this kit

```
cdc_firewall_fix/
|-- GUIDE.md                            <- this file
|-- SHA256SUMS                          <- checksums of every file in the kit
|-- plan_a_python_shell/                <- PLAN A (try first): keep the CDC job as Python shell
|   |-- prepare_cdc_wheels.py           validates the wheels, then strips their dependency lists so pip never goes online
|   `-- scramp-1.4.6-py3-none-any.whl   Python-3.9-compatible scramp (matches PyPI's published checksum)
|-- plan_b_spark/                       <- PLAN B (backup): run the same CDC script as a Spark job
|   `-- switch_cdc_engine.py            switches the existing CDC job Python shell <-> Spark, in place
`-- repo_changes/                       <- OPTIONAL: make Plan B selectable in the pipeline permanently
    |-- lambdas/create_glue_jobs.py     adds cdc_engine = pythonshell | spark
    |-- glue-templates/cdc-spark.json   Spark job template for CDC
    `-- stepfunctions/startup.asl.json  StartCdcJob no longer passes a wheel list
```

| | **Plan A: Python shell + stripped wheels** | **Plan B: CDC as Spark** |
|---|---|---|
| What changes | Only the wheels in `driver-cdc/` | Only the CDC job's type and driver settings |
| CDC script | unchanged | unchanged (same file) |
| How drivers load | Glue's pip installs the local wheels, with nothing to look up online | Same as full load (already works behind the firewall) |
| Cost | ~$0.44/h (1 DPU) | ~$0.88/h (2 × G.1X) |
| Status | Offline pip test passed; real Glue install passed (in an account *with* internet) | Real Glue Spark run passed end-to-end on Oct 2 (in an account *with* internet): drivers installed, inserts/updates/deletes applied |
| Undo | restore `driver-cdc-original/` | `--to pythonshell --yes` |

**Order:** run the pre-flight (§4) → Plan A (§5) → only if Plan A fails, Plan B (§6).

**Which plan to use:**
- **Plan A** is the default: cheapest, and the job type stays as designed. Use it unless one of the reasons below applies.
- **Go to Plan B** if any of these is true:
  - Plan A's run still shows `pypi.org` or `CalledProcessError` in the error log.
  - You can't get a Python 3.9 wheel set that passes `prepare_cdc_wheels.py`.
  - The security team won't accept modified wheels.
  - CDC must run for many months: Plan B runs Python 3.10, which still gets boto3 updates.
- **Plan B costs about twice as much per hour.** It loads drivers the same way the customer's full-load jobs
  already do behind the firewall, so it has the strongest evidence of working there.

---

## 3. Get the kit into CloudShell

The kit is published in the pipeline's GitHub repo, in its own `cdc_firewall_fix/` folder. It is
self-contained: nothing else in the repo needs to change to use it.

Open **CloudShell** in the account and region you're fixing, then:

```bash
cd ~ && rm -rf dsql-kit
git clone --depth 1 https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime.git dsql-kit
export KIT=~/dsql-kit/cdc_firewall_fix
cd $KIT && sha256sum -c SHA256SUMS && cd ~      # every line must say OK
```

**To use one exact, reviewed version** instead of the latest, check out that commit (whoever
reviewed the kit gives you the commit ID):

```bash
cd ~ && rm -rf dsql-kit
git clone https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime.git dsql-kit
git -C ~/dsql-kit checkout <commit-id>
export KIT=~/dsql-kit/cdc_firewall_fix
cd $KIT && sha256sum -c SHA256SUMS && cd ~
```

**If `git clone` is blocked**, download the repo as an archive instead:

```bash
cd ~ && rm -rf dsql-kit && mkdir dsql-kit
curl -fsSL https://github.com/newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime/archive/refs/heads/main.tar.gz \
  | tar xz -C dsql-kit --strip-components=1
export KIT=~/dsql-kit/cdc_firewall_fix
cd $KIT && sha256sum -c SHA256SUMS && cd ~
```

**Optional:** confirm the bundled scramp wheel is PyPI's official file. Its published sha256 is
`a0cf9d2b4624b69bac5432dd69fecfc55a542384fe73c3a23ed9b138cda484e1`:

```bash
sha256sum $KIT/plan_a_python_shell/scramp-1.4.6-py3-none-any.whl
```

> CloudShell keeps files in `~` between sessions, but **environment variables are lost**. If the
> session restarts, re-run the `export KIT=...` line above and the variables block in §4.

---

## 4. Variables and pre-flight (≈ 2 minutes, changes nothing)

**Set once per CloudShell session.** Replace every `<...>` with your values.

```bash
export AWS_PAGER=""
export R=<region>                                    # e.g. us-east-1
export B=<pipeline-bucket>                           # the bucket with driver-cdc/, scripts/, config/
export JOB=<project>-<task_suffix>-cdc               # CDC Glue job, e.g. dms-dsql-mytask-cdc
export VPC=<vpc-id>                                  # VPC of the Glue connection the jobs use
export KIT=~/dsql-kit/cdc_firewall_fix               # from section 3

# read the task's config prefix and DMS task ARN from the job itself (no typing mistakes)
export CFG=$(aws glue get-job --job-name $JOB --region $R \
  --query 'Job.DefaultArguments."--config_prefix"' --output text)
export TASK_ARN=$(aws glue get-job --job-name $JOB --region $R \
  --query 'Job.DefaultArguments."--dms_task_arn"' --output text)
echo "CFG=$CFG"; echo "TASK_ARN=$TASK_ARN"           # neither may be "None"
```

**P1. Job type, Python version, network connection:**

```bash
aws glue get-job --job-name $JOB --region $R \
  --query '[Job.Command.Name, Job.Command.PythonVersion, Job.Connections.Connections]'
# expect: ["pythonshell", "3.9", ["<your Glue connection>"]]
```

**P2. No CDC run is active** (a stuck old run must be stopped first):

```bash
aws glue get-job-runs --job-name $JOB --region $R --max-items 5 \
  --query 'JobRuns[].[Id,JobRunState,StartedOn]' --output table
# If any row is RUNNING / STARTING / WAITING:
#   aws glue batch-stop-job-run --job-name $JOB --region $R --job-run-ids <Id>
```

**P3. DMS is still capturing changes.** Leave it running; it must keep writing changes to S3 so Oracle's archive logs aren't lost:

```bash
aws dms describe-replication-tasks --region $R \
  --filters Name=replication-task-arn,Values=$TASK_ARN \
  --query 'ReplicationTasks[0].[Status,StopReason]'
# expect "running"
```

**P4. Full-load status.** CDC only applies changes to a table whose full load is `done`:

```bash
for k in $(aws s3api list-objects-v2 --bucket $B --region $R \
    --query "Contents[?ends_with(Key,'_load_status.json')].Key" --output text); do
  echo "== $k"; aws s3 cp "s3://$B/$k" - --region $R | head -c 1500; echo
done
```

A table that isn't `done` (e.g. one whose load failed) will make CDC start but **skip that
table**. That's a data issue, not a wheel issue; finish its reload first.

**P5. Private AWS endpoints the VPC has** (for interpreting errors later):

```bash
aws ec2 describe-vpc-endpoints --region $R --filters Name=vpc-id,Values=$VPC \
  --query 'VpcEndpoints[].ServiceName' --output text | tr '\t' '\n'
# needed: ...s3 and ...dsql   optional: ...dms and ...monitoring (CloudWatch)
```

Missing `dms` or `monitoring` doesn't stop CDC. Only schema-change detection and one metric
need them, and both are best-effort.

---

## 5. Plan A: keep Python shell, strip the wheels (≈ 10 minutes)

### A1. Back up the original wheels (only the first time)

The backup goes in a folder **next to** `driver-cdc/`, never inside it: the pipeline collects every wheel
under `driver-cdc/`, subfolders included.

```bash
if aws s3 ls s3://$B/driver-cdc-original/ --region $R >/dev/null 2>&1; then
  echo "backup already exists - not overwriting it"
else
  aws s3 sync s3://$B/driver-cdc/ s3://$B/driver-cdc-original/ --region $R
fi
aws s3 ls s3://$B/driver-cdc-original/ --region $R
```

### A2. Build the input set from the originals, with scramp swapped

```bash
rm -rf ~/cdc_in ~/cdc_out && mkdir ~/cdc_in
aws s3 cp s3://$B/driver-cdc-original/ ~/cdc_in/ --recursive \
  --exclude "*" --include "*.whl" --region $R --only-show-errors
rm -f ~/cdc_in/scramp-*.whl
cp $KIT/plan_a_python_shell/scramp-1.4.6-py3-none-any.whl ~/cdc_in/
ls ~/cdc_in        # expect 10 wheels, one per package:
# asn1crypto boto3 botocore jmespath pg8000 python_dateutil s3transfer scramp six urllib3
```

### A3. Validate and strip

```bash
python3 $KIT/plan_a_python_shell/prepare_cdc_wheels.py ~/cdc_in ~/cdc_out --python 3.9
echo "exit=$?"     # must be exit=0 and end with "PASS"
```

What a good run prints: Step 1 lists all 10 wheels, then `PASS`. Step 2 shows **7 wheels stripped and 3
copied unchanged** (asn1crypto, jmespath, six). Step 3 ends with `PASS  10 wheel(s) written`.

**If Step 1 says FAIL, nothing was written.** The message names the wheel. Common cases:

| Message | Fix |
|---|---|
| `scramp-1.4.x needs Python >=3.10` | you didn't swap scramp; redo A2 |
| `boto3-1.43.x needs Python >=3.10` | boto3/botocore too new; get 1.42.x (below) |
| `botocore ... needs urllib3<1.27 ... folder has urllib3 2.x` | get urllib3 1.26.x (below) |
| `two versions of X` | delete one from `~/cdc_in` |
| `X needs Y, but no Y wheel is in the folder` | add Y's wheel |

To re-download a correct Python 3.9 set, run this **on any machine that can reach PyPI** (e.g. your laptop), then
upload the files into `~/cdc_in` and repeat A3:

```bash
pip download "pg8000>=1.31,<1.32" "scramp>=1.4.5,<1.4.7" \
  "boto3>=1.35,<1.43" "botocore>=1.35,<1.43" "urllib3>=1.25.4,<1.27" \
  --platform manylinux2014_x86_64 --python-version 39 --only-binary=:all: -d cdc_in/
```

### A4. Replace the wheels in `driver-cdc/`

```bash
aws s3 rm s3://$B/driver-cdc/ --recursive --exclude "*" --include "*.whl" --region $R
aws s3 cp ~/cdc_out/ s3://$B/driver-cdc/ --recursive --exclude "*" --include "*.whl" --region $R
aws s3 cp ~/cdc_out/MANIFEST.txt s3://$B/driver-cdc-original/MANIFEST.txt --region $R
aws s3 ls s3://$B/driver-cdc/ --region $R       # expect the same 10 names, scramp-1.4.6
```

`MANIFEST.txt` lists the original and new checksum of every wheel and every line removed. Give it
to the security team if they ask why the wheels no longer match PyPI's published checksums.

### A5. Start the CDC job by hand

**Do not re-run the startup workflow** while DMS is streaming changes: it waits ~24 h for DMS to stop and then fails.
Start the job directly. The command passes three things as **run arguments**:
- `--extra-py-files`: the new wheel list. It replaces the job's saved list, which still names scramp-1.4.17.
- `--config_prefix`: required, because the cutover workflow finds this run by it. A run started without it is not stopped at cutover.
- `--dms_task_arn`

```bash
WHEELS=$(aws s3api list-objects-v2 --bucket $B --prefix driver-cdc/ --region $R \
  --query "Contents[?ends_with(Key,'.whl')].Key" --output text \
  | tr '\t' '\n' | sed "s#^#s3://$B/#" | paste -sd, -)
echo "$WHEELS" | tr , '\n'                        # check: 10 lines, scramp-1.4.6

ARGS=$(python3 -c 'import json,sys; print(json.dumps({"--config_prefix":sys.argv[1],"--dms_task_arn":sys.argv[2],"--extra-py-files":sys.argv[3]}))' \
  "$CFG" "$TASK_ARN" "$WHEELS")
RUN=$(aws glue start-job-run --job-name $JOB --region $R --arguments "$ARGS" \
  --query JobRunId --output text); echo "RUN=$RUN"
```

### A6. Watch it (decide within about 5 minutes, not 20)

```bash
aws glue get-job-run --job-name $JOB --run-id $RUN --region $R --query 'JobRun.JobRunState'
# Python shell logs: error log = pip installs; output log = the script
aws logs tail /aws-glue/python-jobs/error  --log-stream-names $RUN --region $R --since 30m | tail -40
aws logs tail /aws-glue/python-jobs/output --log-stream-names $RUN --region $R --since 30m | tail -40
```

**Success looks like:**
1. Error log: `Processing /tmp/glue-python-libs-…/<wheel>` and `Successfully installed …` for all 10, **no `pypi.org`**.
   Warnings like `awscli 1.23.5 requires botocore==1.25.5, but you have 1.42.97` are **expected and harmless**.
2. Output log: `[startup]` lines, `DDL watcher started`, then `entering poll loop`.
3. Files start moving into each table's `processed/` folder in S3, and the `cdc_control` tables appear in DSQL.

**If you see `pypi.org` or `CalledProcessError` in the error log**, stop the run (P2) and send that log
for review. Then go to Plan B. **If installs succeed but the script fails**, the problem isn't the wheels. Send the output log.

**Undo Plan A:** restore the originals:

```bash
aws s3 rm s3://$B/driver-cdc/ --recursive --exclude "*" --include "*.whl" --region $R
aws s3 cp s3://$B/driver-cdc-original/ s3://$B/driver-cdc/ --recursive --exclude "*" --include "*.whl" --region $R
```

---

## 6. Plan B: run CDC as a Spark job (backup)

Same script, same job name, same connection and arguments. Only the job type changes, plus how
drivers are delivered, which becomes identical to the full-load jobs:
- `--extra-py-files` = `driver-fullload/` wheels (put on the Python path, no pip)
- boto3/botocore/s3transfer from `driver-cdc/` via `--additional-python-modules`

Glue can't change a job's type in place, so the tool **deletes and re-creates the job under the same name**.
- It saves the old definition to a JSON file first.
- It restores the original automatically if the re-create fails.
- It refuses while a run is active.
- It runs as a **dry run** unless you add `--yes`.

### B1. Stop any active CDC run (see P2), then do a dry run

```bash
python3 $KIT/plan_b_spark/switch_cdc_engine.py --job $JOB --region $R --bucket $B --to spark
```

Check the printed definition: `glueetl`, Glue `4.0`, `G.1X` × 2, your Glue connection kept. Only
`--JOB_NAME` (removed: Glue sets it for Spark), `--extra-py-files`, `--additional-python-modules` and
`--job-bookmark-option` should change.

It **refuses** if `driver-fullload/` has no pg8000, if `driver-fullload/` contains boto3 or botocore (that breaks Spark
with `DataNotFoundError: endpoints`), or if `driver-cdc/` lacks any of boto3, botocore or s3transfer.

### B2. Apply

```bash
mkdir -p ~/cdc_job_backups
python3 $KIT/plan_b_spark/switch_cdc_engine.py --job $JOB --region $R --bucket $B --to spark \
  --yes --backup-dir ~/cdc_job_backups
# expect "DONE: ... is now glueetl, connections ['<your Glue connection>']"
```

### B3. Start it with the command the tool prints

It includes `--config_prefix` and `--dms_task_arn` as run arguments. Equivalent:

```bash
ARGS=$(python3 -c 'import json,sys; print(json.dumps({"--config_prefix":sys.argv[1],"--dms_task_arn":sys.argv[2]}))' "$CFG" "$TASK_ARN")
RUN=$(aws glue start-job-run --job-name $JOB --region $R --arguments "$ARGS" \
  --query JobRunId --output text); echo "RUN=$RUN"
```

### B4. Watch it

Spark jobs log to different groups than Python shell:

```bash
aws glue get-job-run --job-name $JOB --run-id $RUN --region $R --query 'JobRun.JobRunState'
aws logs tail /aws-glue/jobs/error  --log-stream-names $RUN --region $R --since 30m | tail -40
aws logs tail /aws-glue/jobs/output --log-stream-names $RUN --region $R --since 30m | tail -40
```

Success: no `DataNotFoundError` / `UnknownServiceError`, then the script's `[startup]` lines and
`entering poll loop`. Spark takes 1–2 minutes longer to start than Python shell.

### B5. Undo Plan B

The switch tool refuses while a run is `STOPPING`; wait until it shows `STOPPED`.

```bash
# stop the Spark run first (P2), then:
python3 $KIT/plan_b_spark/switch_cdc_engine.py --job $JOB --region $R --bucket $B --to pythonshell \
  --yes --backup-dir ~/cdc_job_backups
```

> ⚠️ **While the job is on Spark, don't re-run the startup workflow with the currently deployed
> Lambdas.** The deployed `create-glue-jobs` Lambda always rebuilds CDC as Python shell, and Glue can't
> change a job's type with an update. Either switch back first (B5), or deploy `repo_changes/` (§7) with
> `CDC_ENGINE=spark`.

---

## 7. Optional: make the engine choice permanent (`repo_changes/`)

Only needed if Plan B becomes the long-term setup, or you want the startup workflow to build CDC
as Spark. These files are based on GitHub `main` @ `c2a5a12`, so don't mix them with older copies.

| File | Change | Deploy |
|---|---|---|
| `lambdas/create_glue_jobs.py` | new `cdc_engine` (`pythonshell` default, or `spark`), also settable by Lambda env var `CDC_ENGINE`. Skips `--JOB_NAME` on Spark. Spark CDC gets the `driver-fullload/` list and boto3 via `--additional-python-modules`. Switching type = delete + re-create (refused while a run is active). | update the `$PROJECT-create-glue-jobs` Lambda code (zip the `.py` at the **root** of the zip, not inside a folder) |
| `glue-templates/cdc-spark.json` | Spark CDC template: Glue 4.0, G.1X × 2, 7-day timeout, 1 concurrent run | `aws s3 cp cdc-spark.json s3://$B/glue-templates/` |
| `stepfunctions/startup.asl.json` | `StartCdcJob` no longer passes `--extra-py-files`, so the job uses the list `CreateGlueJobs` just saved. Required for Spark: passing the `driver-cdc/` list to a Spark job breaks it. | fill the `<<...>>` blanks exactly as in RUNBOOK Step 4, then `update-state-machine` |

To choose Spark through the Lambda, **merge** `CDC_ENGINE` into the existing environment variables. `update-function-configuration`
replaces all of them, and `GLUE_CONNECTIONS` may already be set:

```bash
FN=<project>-create-glue-jobs                # e.g. dms-dsql-create-glue-jobs
CUR=$(aws lambda get-function-configuration --function-name $FN --region $R --query 'Environment.Variables' --output json)
NEW=$(python3 -c 'import json,sys; d=json.loads(sys.argv[1] if sys.argv[1]!="null" else "{}"); d["CDC_ENGINE"]="spark"; print(json.dumps({"Variables":d}))' "$CUR")
aws lambda update-function-configuration --function-name $FN --region $R --environment "$NEW"
```

---

## 8. Testing in your own account (before touching the customer)

A Glue run in an account **with internet** does **not** prove the firewall case: pip silently fills
any gaps from PyPI. Use these instead.

**Plan A, offline proof** (any Mac/Linux with Python 3.9; `--no-index` blocks PyPI like the firewall):

```bash
python3 prepare_cdc_wheels.py cdc_in/ cdc_out/ --python 3.9
python3 -m venv /tmp/v1
for w in boto3 python_dateutil pg8000 s3transfer scramp botocore jmespath urllib3 six asn1crypto; do
  /tmp/v1/bin/pip install --no-index cdc_out/${w}-*.whl || echo "FAILED: $w"
done
/tmp/v1/bin/python -c "import boto3,pg8000; boto3.client('dsql',region_name='us-east-2'); print('OK', boto3.__version__)"
```

The order is deliberately the worst case: boto3 before botocore, python_dateutil before six. Done
Oct 2: all 10 installed with no PyPI contact, and `dsql` client + pg8000 OK on Python 3.9.6.

**Plan B, Spark run** (internet is fine here; this tests that the job runs, not the network):
1. Switch a test CDC job with `switch_cdc_engine.py` (dry run, then `--yes`).
2. Start it with the printed command.
3. Confirm boto3/botocore/s3transfer install, the `dsql` client works, and `entering poll loop` appears.
4. Apply one INSERT, UPDATE and DELETE at the source, and confirm all three reach DSQL with `all_rows_committed=true` in `cdc_file_status`.
5. Switch back with `--to pythonshell --yes`.

Done Oct 2, all 5 steps passed:
- boto3/botocore/s3transfer 1.42.97/0.16.1 installed from the S3 wheels. jmespath, python-dateutil, urllib3 and six were already in Glue 4.0, so pip had nothing to fetch, and there were no `pypi.org` lines.
- The script started on Python 3.10, connected to DSQL and created the control tables. No `DataNotFoundError` or `UnknownServiceError`.
- An INSERT, UPDATE and DELETE all reached DSQL, with `all_rows_committed=true`.
- Switching back restored the job exactly (Python shell, 3.9, Glue 3.0, 1 DPU).
- The only extra log line was an `aiobotocore` version-conflict notice. It's harmless: the install finished with exit code 0.

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `...whl installation failed after 2th retry ... CalledProcessError`, `pypi.org timed out` | a wheel still lists dependencies and pip went online | Plan A not applied, or the run used the old wheel list. Check A4 and that `$WHEELS` names the new files |
| `requires a different Python: 3.9 not in '>=3.10'` | a wheel needs Python 3.10 (scramp 1.4.7+, boto3 1.43+) | swap it (A2 / re-download) and re-run A3 |
| `UnknownServiceError: Unknown service: 'dsql'` | an old boto3 was used | Python shell: `driver-cdc/` must have boto3 1.42.x. Spark: `--additional-python-modules` must list the boto3 wheels |
| `DataNotFoundError: endpoints` (Spark) | boto3 wheels are on a Spark job's `--extra-py-files` | `driver-fullload/` must have no boto3/botocore; use `switch_cdc_engine.py` or `repo_changes/` |
| `awscli ... requires botocore==..., but you have ...` | Glue's built-in awscli expects its old botocore | harmless warning; ignore it |
| CDC running, but a table never applies | that table's full load isn't `done` (P4) | finish/reload that table |
| Startup workflow fails at `CreateGlueJobs` after switching to Spark | the deployed Lambda tries to update a Spark job as Python shell | switch back (B5), or deploy `repo_changes/` with `CDC_ENGINE=spark` |
| `switch_cdc_engine.py` says `REFUSED: active run(s)` | a CDC run is still active | stop it (P2), then retry |
| CDC run not stopped at cutover | the run was started without `--config_prefix` in its run arguments | always start with the A5 / B3 commands |

---

## 10. What's proven and what isn't

| Claim | Evidence |
|---|---|
| S3 wheel download works in the customer's VPC | customer's failed log: all 10 downloaded |
| Customer CDC job runs inside the VPC with no internet | its pip call timed out on pypi.org |
| VPC can reach DSQL | full load connects through the same Glue connection |
| Stripped wheels install with no PyPI | offline `--no-index` test, Python 3.9.6, worst-case order |
| Glue installs stripped wheels and starts the script; awscli warnings harmless | real Glue Python shell test run, Oct 2 (account **with** internet) |
| `prepare_cdc_wheels.py` rejects bad sets | tested on the real PyPI wheels: scramp 1.4.17, urllib3 2.6.3, missing six, duplicates |
| `switch_cdc_engine.py` / `create_glue_jobs.py` Spark logic | 30 checks against simulated Glue/S3 using the customer's job definition |
| CDC script runs as Spark on Python 3.10 and applies inserts/updates/deletes; switch tool's switch and rollback work | real Glue Spark test run, Oct 2 (account **with** internet) |
| Spark driver delivery needs no internet | same mechanism the customer's full-load jobs already use behind the firewall |
| **Not proven:** stripped wheels in the customer's firewalled Glue | the customer's next run (Plan A) |

**Long-term note:** boto3/botocore 1.42 is the last line that supports Python 3.9. Glue Python shell can't
go past 3.9, so these drivers won't get new fixes. That's fine for a migration-length run; if CDC
must run for many months, Plan B (Python 3.10) is the better long-term home.
