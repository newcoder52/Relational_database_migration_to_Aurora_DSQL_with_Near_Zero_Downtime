# IAM policy notes

The three combined IAM files — `iam/glue.json`, `iam/lambda.json`, `iam/stepfunctions.json` —
each hold one JSON object `{"RoleName", "TrustPolicy", "Policy"[, "VpcPolicy"]}`. `tools/setup.sh`
(and the `docs/MANUAL_SETUP.md` fill-in step) split that object and send **only** the inner
`TrustPolicy` / `Policy` / `VpcPolicy` policy documents to `aws iam put-role-policy` /
`update-assume-role-policy`.

IAM validates every policy document against a fixed schema and rejects any unknown key with
`MalformedPolicyDocument`. The only keys allowed inside a policy document are:

- **Document level:** `Version`, `Id`, `Statement`.
- **Statement level:** `Sid`, `Effect`, `Action`, `NotAction`, `Resource`, `NotResource`,
  `Principal`, `NotPrincipal`, `Condition`.

Therefore the IAM JSON files MUST NOT carry `_comment` (or any other extra key) inside
`TrustPolicy` / `Policy` / `VpcPolicy` or their statements. The explanations that used to live
in those files as `_comment` keys are recorded below instead. (`tools/setup.sh` and the manual
fill-in step also strip any unknown keys as a backstop, and `tests/test_iam_policies.py` asserts
every statement contains only valid IAM keys — see B22 in `RESULT.md`.)

## `iam/glue.json` — `<project>-glue-exec-role`

Glue job execution role (all Glue jobs: discovery, load, validate, CDC). `TrustPolicy` + `Policy`
are always applied by `tools/setup.sh`. `VpcPolicy` is applied as a SEPARATE inline policy (name
`glue-vpc`) ONLY when a Glue network connection is configured (`subnet_id` + `security_group_id`
set, i.e. `glue_connection != ""`).

**Statement `StatesDescribeExecutionForBlankGuard` (`states:DescribeExecution`).** This grant is
used by the G2 safety guardrail: the load job (`job2_load`) checks whether
`--startup_execution`'s Step Functions execution is RUNNING before it treats a whole-table/range
blank as workflow-driven. As of the "ease guardrails" change (see `RESULT.md`), G2 is **warn by
default**: if this permission is missing the load job logs a WARNING and continues (it does not
fail the run), and in the default `guardrails_mode=warn` a workflow-started reblank is still
allowed when `--startup_execution` is present even if the execution cannot be described. Granting
`states:DescribeExecution` lets the guard positively confirm a RUNNING execution; it is no longer
required for a normal run to succeed. Under `guardrails_mode=strict` the historical fail-closed
behaviour is restored and the grant matters again.

## `iam/lambda.json` — `<project>-lambda-exec-role`

Shared execution role for ALL 8 Lambdas (`resolve-task`, `driver-discovery`, `plan-split`,
`create-glue-jobs`, `stop-cdc-run`, `drain-check`, `drop-tags`, `preflight-tasks`). Union of the
former `lambda-exec-role` and `preflight-tasks-role` policies, deduped. The former preflight
statements are strict subsets of the broader lambda statements they merged into:
`ReadSettingsTaskListsAndFolders` (`s3:GetObject`/`ListBucket`) and
`PublishPipelineSettingsFromParamsCsv` (`s3:PutObject` on `config/pipeline.json(.*)`) are covered
by the `S3` block (`GetObject`/`PutObject` on `<<BUCKET>>/*` + `ListBucket` on the bucket); the
preflight DMS describe set is a subset of `DmsDescribe`; the preflight states List/Describe (the 4
named machines + startup/cutover executions) are subsets of the `<<PROJECT>>-*` / `<<PROJECT>>-*:*`
blocks. No new wildcards were introduced. `setup.sh` attaches `AWSLambdaVPCAccessExecutionRole` to
this role when a VPC is used (as before).

## `iam/stepfunctions.json` — `<project>-sfn-exec-role`

Shared execution role for ALL 4 state machines (`startup`, `cutover`, `fleet-startup`,
`fleet-cutover`). Union of the former `sfn-exec-role`, `fleet-startup-role` and `fleet-cutover-role`
policies, deduped. The former fleet `InvokePreflightLambda` (`lambda:InvokeFunction` on
`function:<<PROJECT>>-preflight-tasks`) is a strict subset of `InvokeLambdas`
(`function:<<PROJECT>>-*`). The two fleet StartExecution/DescribeExecution statements are merged
into `StartChildren` / `VerifyChildren`, scoped to exactly the two child machines the fleets launch
(`<<PROJECT>>-startup` and `<<PROJECT>>-cutover`) and their executions — no `<<PROJECT>>-*` wildcard
was introduced for `states:*`. `TrustPolicy` adds the `aws:SourceAccount` condition that the former
fleet roles carried; the former `sfn-exec-role` trust had no condition, so this is a tightening
(same account only), not a loosening.
