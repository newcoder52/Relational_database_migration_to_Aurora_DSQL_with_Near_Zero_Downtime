#!/usr/bin/env python3
"""mrl_fleet_run.py - Amendment 10 B.2: start many MRL tasks WITHOUT the optional fleet state
machine, from AWS CloudShell, using the OPERATOR's own credentials (so no new IAM is involved).

It reproduces exactly what stepfunctions/mrl-fleet.asl.json does, by calling the SAME Lambdas with
the SAME payloads (the payload templates are defined once here and asserted byte-equal against the
ASL Parameters by tests/test_mrl_fleet_run.py):
  1. <project>-mrl-settings  (PublishSettings) - build/publish/pin settings + validate the optional
     max_concurrency / cdc STRICTLY, before any DMS call;
  2. <project>-mrl-fleet-preflight - validate the fleet CSV + build one child input per task;
  3. start one <project>-mrl-task execution per returned task input (skipping preflight 'skip'
     tasks), at most maxParallelTasks running at once (poll describe-execution every 30s; start the
     next as one finishes). Deterministic child names (re-run safe: running/finished tasks are
     skipped by the preflight exactly as for the fleet machine). Ctrl-C never stops running
     children. Exit 0 only if every STARTED child SUCCEEDED, else 1.

Args: --bucket --project --region [--max-concurrency N] [--cdc true|false] [--override]
      [--dry-run] [--no-wait] [--poll-seconds N]
"""
import argparse
import json
import sys
import time

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None

POLL_DEFAULT = 30


class FleetRunError(Exception):
    pass


def _lambda_arn(project, region, account, name):
    return "arn:aws:lambda:%s:%s:function:%s-%s" % (region, account, project, name)


def _task_sm_arn(project, region, account):
    return "arn:aws:states:%s:%s:stateMachine:%s-mrl-task" % (region, account, project)


# ---- payload templates (the SINGLE source; the ASL-equivalence test checks these) -----------
def build_settings_payload(bucket, raw_input, fleet_exec_name, fleet_exec_arn,
                           fleet_sm_arn, task_sm_arn):
    """Mirror of mrl-fleet PublishSettings Parameters.Payload (rawInput.$ = $ = the whole start
    input, so max_concurrency/cdc/override ride along exactly as the fleet start input)."""
    return {
        "bucket": bucket,
        "rawInput": raw_input,
        "fleetExecutionName": fleet_exec_name,
        "fleetExecutionArn": fleet_exec_arn,
        "fleetStateMachineArn": fleet_sm_arn,
        "taskStateMachineArn": task_sm_arn,
    }


def build_fleet_preflight_payload(bucket, task_sm_arn, fleet_exec_id, pinned_settings_key,
                                  exec_max_concurrency, exec_cdc):
    """Mirror of mrl-fleet FleetPreflight Parameters.Payload."""
    return {
        "bucket": bucket,
        "stateMachineArn": task_sm_arn,
        "fleetExecutionId": fleet_exec_id,
        "pinnedSettingsKey": pinned_settings_key,
        "execMaxConcurrency": exec_max_concurrency,
        "execCdc": exec_cdc,
    }


def build_child_start(task_sm_arn, task_item):
    """Mirror of mrl-fleet FanOut StartChild Parameters (Name + Input from the preflight item)."""
    return {"StateMachineArn": task_sm_arn, "Name": task_item["runName"],
            "Input": json.dumps(task_item["input"])}


def _invoke(lam, arn, payload):
    resp = lam.invoke(FunctionName=arn, Payload=json.dumps(payload).encode())
    body = resp["Payload"].read()
    out = json.loads(body.decode("utf-8")) if body else {}
    if resp.get("FunctionError") or (isinstance(out, dict) and out.get("errorMessage")):
        raise FleetRunError("Lambda %s failed: %s" % (arn, out))
    return out


def _parse_cdc(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "false"):
        return s == "true"
    raise FleetRunError("--cdc must be true or false (got %r)" % v)


def _validate_max_concurrency(v):
    if v is None:
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        raise FleetRunError("--max-concurrency must be an integer 1..40 (got %r)" % v)
    if n < 1 or n > 40:
        raise FleetRunError("--max-concurrency must be 1..40 (got %d)" % n)
    return n


def run(args, clients=None):
    """clients: optional dict {'lambda':..,'sfn':..,'sts':..} for tests (fake boto3)."""
    # STRICT validation BEFORE any Lambda/DMS call (same as PublishSettings does).
    exec_mc = _validate_max_concurrency(args.max_concurrency)
    exec_cdc = _parse_cdc(args.cdc)

    if clients is None:
        lam = boto3.client("lambda", region_name=args.region)
        sfn = boto3.client("stepfunctions", region_name=args.region)
        sts = boto3.client("sts", region_name=args.region)
        account = sts.get_caller_identity()["Account"]
    else:
        lam, sfn = clients["lambda"], clients["sfn"]
        account = clients.get("account", "123456789012")

    task_sm = _task_sm_arn(args.project, args.region, account)
    # the raw start input a fleet start-execution would carry
    raw_input = {"bucket": args.bucket}
    if exec_mc is not None:
        raw_input["max_concurrency"] = exec_mc
    if exec_cdc is not None:
        raw_input["cdc"] = exec_cdc
    if args.override:
        raw_input["override"] = True

    fleet_name = "mrl-fleet-run-%s" % int(time.time()) if not getattr(args, "_fixed_name", None) \
        else args._fixed_name

    # 1. PublishSettings
    settings_payload = build_settings_payload(
        args.bucket, raw_input, fleet_name, "local:%s" % fleet_name, "local:fleet-run", task_sm)
    if args.dry_run:
        print("[dry-run] would invoke %s-mrl-settings with:" % args.project)
        print("  " + json.dumps(settings_payload))
    settings = {} if args.dry_run else _invoke(
        lam, _lambda_arn(args.project, args.region, account, "mrl-settings"), settings_payload)
    pinned = settings.get("pinnedSettingsKey")
    s_mc = settings.get("execMaxConcurrency", exec_mc)
    s_cdc = settings.get("execCdc", exec_cdc)

    # 2. FleetPreflight
    pf_payload = build_fleet_preflight_payload(
        args.bucket, task_sm, "local:%s" % fleet_name, pinned, s_mc, s_cdc)
    if args.dry_run:
        print("[dry-run] would invoke %s-mrl-fleet-preflight with:" % args.project)
        print("  " + json.dumps(pf_payload))
        print("[dry-run] no child executions will be started.")
        return 0
    pf = _invoke(lam, _lambda_arn(args.project, args.region, account, "mrl-fleet-preflight"),
                 pf_payload)
    tasks = pf.get("tasks", [])
    max_parallel = int(pf.get("maxParallelTasks", 2))

    to_start = [t for t in tasks if not t.get("skip")]
    skipped = [t for t in tasks if t.get("skip")]
    for t in skipped:
        print("SKIP  %-20s (%s)" % (t.get("taskSuffix") or t["taskArn"], t.get("skip")))

    # 3. start children, at most max_parallel at once; poll until done (unless --no-wait).
    results = {}            # suffix -> {execArn, status, cause}
    running = {}            # execArn -> suffix
    pending = list(to_start)
    poll = getattr(args, "poll_seconds", POLL_DEFAULT)
    poll = POLL_DEFAULT if poll is None else int(poll)

    def _start(item):
        p = build_child_start(task_sm, item)
        try:
            r = sfn.start_execution(stateMachineArn=p["StateMachineArn"], name=p["Name"],
                                    input=p["Input"])
            running[r["executionArn"]] = item["taskSuffix"] or item["taskArn"]
            print("START %-20s %s" % (item["taskSuffix"] or item["taskArn"], r["executionArn"]))
        except Exception as e:
            code = type(e).__name__
            if "ExecutionAlreadyExists" in code or "ExecutionAlreadyExists" in str(e):
                print("SKIP  %-20s (already running: ExecutionAlreadyExists)"
                      % (item["taskSuffix"] or item["taskArn"]))
                results[item["taskSuffix"] or item["taskArn"]] = {"status": "SKIPPED",
                                                                  "cause": "already running"}
            else:
                raise

    # fill up to the limit
    while pending and len(running) < max_parallel:
        _start(pending.pop(0))

    if getattr(args, "no_wait", False):
        print("--no-wait: started %d, %d pending (re-run to continue; running tasks are skipped)."
              % (len(running), len(pending)))
        return 0

    try:
        while running:
            time.sleep(poll)
            for ex_arn in list(running):
                d = sfn.describe_execution(executionArn=ex_arn)
                st = d.get("status")
                if st in ("RUNNING",):
                    continue
                suffix = running.pop(ex_arn)
                cause = ""
                if st != "SUCCEEDED":
                    cause = _failure_cause(sfn, ex_arn)
                results[suffix] = {"execArn": ex_arn, "status": st, "cause": cause}
                print("DONE  %-20s %s %s" % (suffix, st, ("- " + cause) if cause else ""))
                while pending and len(running) < max_parallel:
                    _start(pending.pop(0))
    except KeyboardInterrupt:
        print("\nCtrl-C: leaving %d running child execution(s) untouched. Re-run to resume."
              % len(running))
        return 1

    # table + exit code
    print("\n%-22s %-10s %s" % ("task", "status", "execution / cause"))
    ok = True
    for suffix, r in sorted(results.items()):
        print("%-22s %-10s %s" % (suffix, r["status"],
                                  r.get("execArn", "") + ((" " + r["cause"]) if r.get("cause") else "")))
        if r["status"] not in ("SUCCEEDED", "SKIPPED"):
            ok = False
    return 0 if ok else 1


def _failure_cause(sfn, ex_arn):
    """Best-effort failing state/cause from the execution history."""
    try:
        h = sfn.get_execution_history(executionArn=ex_arn, reverseOrder=True, maxResults=20)
        for ev in h.get("events", []):
            for key in ("executionFailedEventDetails", "taskFailedEventDetails",
                        "stateEnteredEventDetails"):
                det = ev.get(key)
                if det and (det.get("error") or det.get("cause")):
                    return "%s: %s" % (det.get("error", ""), str(det.get("cause", ""))[:120])
    except Exception:
        pass
    return "FAILED"


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="Start MRL tasks without the fleet state machine.")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--max-concurrency", dest="max_concurrency", default=None)
    ap.add_argument("--cdc", default=None)
    ap.add_argument("--override", action="store_true")
    ap.add_argument("--dry-run", dest="dry_run", action="store_true")
    ap.add_argument("--no-wait", dest="no_wait", action="store_true")
    ap.add_argument("--poll-seconds", dest="poll_seconds", type=int, default=POLL_DEFAULT)
    return ap.parse_args(argv)


def main(argv=None):
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        return run(args)
    except FleetRunError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
