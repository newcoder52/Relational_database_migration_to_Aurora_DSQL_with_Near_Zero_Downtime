#!/usr/bin/env python3
"""Amendment 10 B.2 - tests for tools/mrl_fleet_run.py (the no-state-machine fleet runner).

Key test: the payloads it sends to mrl-settings / mrl-fleet-preflight / the child start are
BYTE-EQUAL to the ASL Parameters of PublishSettings / FleetPreflight / FanOut.StartChild. The ASL
parameter templates are EVALUATED here (not hand-duplicated) and compared to the runner's builders.
Plus: max_parallel gating (5 tasks, limit 2), skip handling, a failed child => exit 1 while others
finish, invalid --max-concurrency fails before any Lambda, --dry-run starts nothing.
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(PKG, "tools"))

import mrl_fleet_run as fr  # noqa: E402

FLEET_ASL = json.load(open(os.path.join(PKG, "stepfunctions", "mrl-fleet.asl.json")))


# ---- evaluate an ASL Parameters template against a context (minimal $ / $$ resolver) --------
def _resolve_path(path, data, ctx):
    if path == "$":
        return data
    if path.startswith("$$."):
        cur = ctx
        parts = path[3:].split(".")
    elif path.startswith("$."):
        cur = data
        parts = path[2:].split(".")
    else:
        return path
    for p in parts:
        cur = cur[p]
    return cur


def _eval_params(params, data, ctx):
    out = {}
    for k, v in params.items():
        if k.endswith(".$"):
            out[k[:-2]] = _resolve_path(v, data, ctx)
        elif isinstance(v, dict):
            out[k] = _eval_params(v, data, ctx)
        else:
            out[k] = v
    return out


def test_settings_payload_byte_equal_to_asl():
    task_sm = "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"
    # the whole fleet start input ($) a start-execution would carry
    start_input = {"bucket": "b", "max_concurrency": 4, "cdc": True}
    ctx = {"Execution": {"Name": "fleetX", "Id": "arn:exec:fleetX"},
           "StateMachine": {"Id": "arn:sm:fleet"}}
    asl_params = FLEET_ASL["States"]["PublishSettings"]["Parameters"]["Payload"]
    # the ASL template has a literal taskStateMachineArn placeholder; substitute it as deploy does
    asl_params = json.loads(json.dumps(asl_params).replace("<<MRL_TASK_STATE_MACHINE_ARN>>", task_sm))
    asl_eval = _eval_params(asl_params, start_input, ctx)
    runner = fr.build_settings_payload("b", start_input, "fleetX", "arn:exec:fleetX",
                                       "arn:sm:fleet", task_sm)
    assert runner == asl_eval, "settings payload differs from ASL:\n%s\n%s" % (runner, asl_eval)


def test_fleet_preflight_payload_byte_equal_to_asl():
    task_sm = "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"
    data = {"bucket": "b",
            "settings": {"pinnedSettingsKey": "s3://b/mrl/runs/e/settings.json",
                         "execMaxConcurrency": 4, "execCdc": True}}
    ctx = {"Execution": {"Id": "arn:exec:fleetX"}}
    asl_params = FLEET_ASL["States"]["FleetPreflight"]["Parameters"]["Payload"]
    asl_params = json.loads(json.dumps(asl_params).replace("<<MRL_TASK_STATE_MACHINE_ARN>>", task_sm))
    asl_eval = _eval_params(asl_params, data, ctx)
    runner = fr.build_fleet_preflight_payload("b", task_sm, "arn:exec:fleetX",
                                              "s3://b/mrl/runs/e/settings.json", 4, True)
    assert runner == asl_eval, "preflight payload differs from ASL:\n%s\n%s" % (runner, asl_eval)


def test_child_start_matches_asl_startchild():
    task_sm = "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"
    item = {"taskArn": "arn:dms:t", "taskSuffix": "t", "runName": "mrl-t", "skip": None,
            "input": {"taskArn": "arn:dms:t", "taskSuffix": "t"}}
    # ASL StartChild Parameters: StateMachineArn (literal), Name.$=$.runName, Input.$=$.childInput
    asl = FLEET_ASL["States"]["FanOut"]["Iterator"]["States"]["StartChild"]["Parameters"]
    assert asl["Name.$"] == "$.runName" and asl["Input.$"] == "$.childInput"
    runner = fr.build_child_start(task_sm, item)
    assert runner["StateMachineArn"] == task_sm
    assert runner["Name"] == "mrl-t"
    assert json.loads(runner["Input"]) == item["input"]


# ---- fake boto3 for the orchestration tests -----------------------------------------------
class _FakeLambda:
    def __init__(self, tasks, max_parallel=2):
        self.tasks = tasks
        self.max_parallel = max_parallel
        self.invocations = []

    def invoke(self, FunctionName, Payload):
        self.invocations.append(FunctionName)
        body = json.loads(Payload.decode())
        if "mrl-settings" in FunctionName:
            out = {"pinnedSettingsKey": "s3://b/mrl/runs/e/settings.json",
                   "execMaxConcurrency": body["rawInput"].get("max_concurrency"),
                   "execCdc": body["rawInput"].get("cdc"), "published": True}
        elif "mrl-fleet-preflight" in FunctionName:
            out = {"tasks": self.tasks, "count": len(self.tasks),
                   "maxParallelTasks": self.max_parallel, "toStart": len(self.tasks), "skipped": 0}
        else:
            out = {}

        class _R:
            def read(self_inner):
                return json.dumps(out).encode()
        return {"Payload": _R()}


class _FakeSfn:
    def __init__(self, fail_suffixes=None, max_running_seen=None):
        self.started = []
        self.state = {}          # execArn -> remaining polls before done
        self.fail = set(fail_suffixes or [])
        self._n = 0
        self.max_running_seen = max_running_seen if max_running_seen is not None else [0]
        self._running = 0

    def start_execution(self, stateMachineArn, name, input):
        self._n += 1
        arn = "arn:exec:%s:%d" % (name, self._n)
        self.started.append((name, arn))
        self.state[arn] = {"suffix": name, "left": 1, "input": json.loads(input)}
        self._running += 1
        self.max_running_seen[0] = max(self.max_running_seen[0], self._running)
        return {"executionArn": arn}

    def describe_execution(self, executionArn):
        s = self.state[executionArn]
        if s["left"] > 0:
            s["left"] -= 1
            return {"status": "RUNNING"}
        self._running -= 1
        suffix = s["suffix"].replace("mrl-", "")
        return {"status": "FAILED" if suffix in self.fail else "SUCCEEDED"}

    def get_execution_history(self, executionArn, reverseOrder=True, maxResults=20):
        return {"events": [{"executionFailedEventDetails": {"error": "MrlLoadFailed",
                                                            "cause": "range failed"}}]}


def _args(**kw):
    base = dict(bucket="b", project="demo", region="us-east-1", max_concurrency=None, cdc=None,
                override=False, dry_run=False, no_wait=False, poll_seconds=0, _fixed_name="fleetX")
    base.update(kw)
    return types.SimpleNamespace(**base)


def _item(suffix, skip=None):
    return {"taskArn": "arn:dms:%s" % suffix, "taskSuffix": "mrl-%s" % suffix,
            "runName": "mrl-%s" % suffix, "skip": skip,
            "input": {"taskArn": "arn:dms:%s" % suffix}}


def test_max_parallel_gating_5_tasks_limit_2():
    tasks = [_item("t%d" % i) for i in range(5)]
    lam = _FakeLambda(tasks, max_parallel=2)
    seen = [0]
    sfn = _FakeSfn(max_running_seen=seen)
    rc = fr.run(_args(), clients={"lambda": lam, "sfn": sfn, "account": "123456789012"})
    assert rc == 0
    assert len(sfn.started) == 5, "all 5 should eventually start"
    assert seen[0] <= 2, "never more than max_parallel=2 running at once (saw %d)" % seen[0]


def test_skip_handling():
    tasks = [_item("a"), _item("b", skip="already_running"), _item("c")]
    lam = _FakeLambda(tasks, max_parallel=3)
    sfn = _FakeSfn()
    rc = fr.run(_args(), clients={"lambda": lam, "sfn": sfn, "account": "123456789012"})
    assert rc == 0
    started = [n for n, _ in sfn.started]
    assert "mrl-b" not in started, "a skipped task must not be started"
    assert set(started) == {"mrl-a", "mrl-c"}


def test_failed_child_exits_1_and_others_finish():
    tasks = [_item("ok1"), _item("bad"), _item("ok2")]
    lam = _FakeLambda(tasks, max_parallel=3)
    sfn = _FakeSfn(fail_suffixes={"bad"})
    rc = fr.run(_args(), clients={"lambda": lam, "sfn": sfn, "account": "123456789012"})
    assert rc == 1, "a failed child must make exit code 1"
    assert len(sfn.started) == 3, "the other tasks still start/finish"


def test_invalid_max_concurrency_fails_before_any_lambda():
    lam = _FakeLambda([], max_parallel=2)
    sfn = _FakeSfn()
    try:
        fr.run(_args(max_concurrency="99"), clients={"lambda": lam, "sfn": sfn,
                                                     "account": "123456789012"})
        assert False, "max_concurrency 99 must be rejected"
    except fr.FleetRunError as e:
        assert "1..40" in str(e)
    assert lam.invocations == [], "NO Lambda may be invoked when validation fails"


def test_dry_run_starts_nothing():
    tasks = [_item("a"), _item("b")]
    lam = _FakeLambda(tasks, max_parallel=2)
    sfn = _FakeSfn()
    rc = fr.run(_args(dry_run=True), clients={"lambda": lam, "sfn": sfn, "account": "123456789012"})
    assert rc == 0
    assert sfn.started == [], "--dry-run must start no executions"
    # dry-run prints the settings payload but does not invoke the fleet-preflight Lambda
    assert not any("fleet-preflight" in i for i in lam.invocations)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d fleet-run test(s) passed" % len(fns))
