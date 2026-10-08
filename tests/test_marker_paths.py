#!/usr/bin/env python3
"""B23 start-marker path contract: the S3 key the startup state machine POLLS to confirm a CDC
run started must equal the key that CDC run's script actually WRITES, given the exact arguments
the state machine passes to the job.

The B23 bug
-----------
`startup.asl.json`'s `StartForkCdcMap/CheckForkStarted` polled the TASK-level key
    config/_task/<suffix>/_cdc_started/<exec>-ck-<slug>.json
but it starts each fork's CDC job with `--config_prefix` = the FORK prefix
    s3://bucket/<task cp>/_orchestrator/ck-<slug>/
and the CDC scripts' `write_started_marker()` writes under `<CONFIG_PREFIX>_cdc_started/<token>.json`
— i.e. under the FORK prefix, NOT the task prefix. So the workflow never saw the marker, waited
45 min and failed with `ForkCdcStartNotConfirmed` while the fork CDC job was actually RUNNING. Both
ck (composite, glue_cdc_composite.py) and bg (big-table, glue_cdc_continuous.py) forks were hit.

What this test does
-------------------
For MAIN, one CK fork and one BG fork it:
  1. builds the real plan with the real `plan_split` (so fork `config_prefix` / `config_prefix_key`
     / `cdcJobName` are the real values),
  2. derives from the real ASL the exact S3 Prefix each confirmation state POLLS (evaluating the
     state's `States.Format(...)` against the args the Map/StartCdcJob passes), and
  3. derives from the real CDC-script logic the set of keys `write_started_marker()` WRITES, given
     the same `--config_prefix` / `--startup_execution` / `--cdc_owners_key` the ASL passes,
and asserts the polled key is one the script writes. A NEGATIVE test reconstructs the pre-fix
task-level poll and asserts it would NOT match for a fork (the bug), and that the fix does.

Static, offline (no AWS, no Spark, no network). Run: python3 tests/test_marker_paths.py
"""
import io
import json
import os
import re
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "lambdas"))

# Minimal boto3 stub so plan_split imports (it only uses boto3.client, which we monkeypatch).
_boto3 = types.ModuleType("boto3")
_boto3.client = lambda *a, **k: None
sys.modules["boto3"] = _boto3
import plan_split as ps  # noqa: E402

BUCKET = "b"
TASK_SUFFIX = "mytask"
TASK_CP = f"s3://{BUCKET}/config/_task/{TASK_SUFFIX}/"
TASK_CP_KEY = f"config/_task/{TASK_SUFFIX}/"
EXEC = "arn-exec-0001"
RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append(bool(cond))
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"   <- {str(extra)[:400]}" if (not cond and extra) else ""))
    # Under pytest, surface a failed check as a real failure (see H6 note).
    import os as _os
    if not cond and "PYTEST_CURRENT_TEST" in _os.environ:
        raise AssertionError(f"{name}" + (f": {extra}" if extra else ""))


# ---------------------------------------------------------------------------------------------
# tiny States.Format evaluator — substitutes {} left-to-right with the given JSONPath values
# ---------------------------------------------------------------------------------------------
def eval_states_format(expr, scope):
    """Evaluate a `States.Format('tmpl', $.a, $.b, ...)` ASL intrinsic. `scope` maps a JSONPath
    (e.g. "$.config_prefix_key") to its value. Mirrors ASL: each {} consumes the next arg."""
    m = re.match(r"^States\.Format\(\s*'(?P<tmpl>(?:[^'\\]|\\.)*)'\s*(?:,\s*(?P<args>.*))?\)$",
                 expr.strip(), re.S)
    assert m, f"not a States.Format: {expr!r}"
    tmpl = m.group("tmpl").replace("\\'", "'")
    args_str = (m.group("args") or "").strip()
    args = [a.strip() for a in args_str.split(",")] if args_str else []
    vals = []
    for a in args:
        assert a in scope, f"unknown arg {a!r} in {expr!r} (scope keys: {sorted(scope)})"
        vals.append(str(scope[a]))
    out, i = [], 0
    for part in tmpl.split("{}"):
        out.append(part)
        if i < len(vals):
            out.append(vals[i]); i += 1
    return "".join(out)


# ---------------------------------------------------------------------------------------------
# model of the CDC scripts' write_started_marker(): the KEYS it writes, given the run's args.
# This mirrors scripts/glue_cdc_{composite,continuous}.py exactly (both share the logic).
# ---------------------------------------------------------------------------------------------
def script_marker_keys(config_prefix, startup_execution, cdc_owners_key=None):
    assert config_prefix.startswith("s3://")
    key = config_prefix[len("s3://"):].partition("/")[2]
    rid = startup_execution  # --startup_execution (falls back to JOB_RUN_ID; the SM always passes it)
    prefixes = [key]
    # task-level dual-write derived from --cdc_owners_key's dirname (B23)
    if cdc_owners_key:
        ok = cdc_owners_key
        if ok.startswith("s3://"):
            ok = ok[len("s3://"):].partition("/")[2]
        ok = ok.lstrip("/")
        d = ok.rsplit("/", 1)[0] if "/" in ok else ""
        task_key = (d + "/") if d else ""
        if task_key and task_key != key:
            prefixes.append(task_key)
    keys = []
    for pfx in prefixes:
        if rid:
            keys.append(pfx + f"_cdc_started/{rid}.json")
        keys.append(pfx + "_cdc_started/_latest.json")
    return set(keys)


# ---------------------------------------------------------------------------------------------
# plan_split fake S3 + runner (same shape as test_planning_settings.py)
# ---------------------------------------------------------------------------------------------
class PlanS3:
    def __init__(self, entries):
        self.objects = {f"config/_task/{TASK_SUFFIX}/_manifest_index.json":
                        json.dumps({"tables": entries}).encode()}
        self.puts = []

    def get_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            e = Exception("NoSuchKey"); e.response = {"Error": {"Code": "NoSuchKey"}}
            raise e
        return {"Body": types.SimpleNamespace(read=lambda d=self.objects[Key]: d)}

    def put_object(self, Bucket=None, Key=None, Body=None, **k):
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode()
        self.puts.append(Key)

    def list_objects_v2(self, Bucket=None, Prefix=None, ContinuationToken=None):
        return {"Contents": [{"Key": Prefix.rstrip("/") + "/LOAD00000001.csv"}], "IsTruncated": False}


def _entry(table, pk_mode="single", rows=1000):
    cols = {"single": ["id"], "composite": ["a", "b"], "none": []}[pk_mode]
    return {"dsql_schema": "app", "dsql_table": table, "pk_mode": pk_mode,
            "pk_columns": cols, "full_load_rows": rows, "dms_s3_path": f"s3://{BUCKET}/app/{table}/"}


def run_plan(entries):
    s3 = PlanS3(entries)
    _boto3.client = lambda svc, **k: s3
    event = {"bucket": BUCKET, "config_prefix": TASK_CP, "project": "proj",
             "taskSuffix": TASK_SUFFIX, "cdc_root": "cdc",
             "max_composite_forks": 8, "max_big_cdc_forks": 8}
    return ps.handler(event, None)


# ---------------------------------------------------------------------------------------------
# load the real ASL confirmation states
# ---------------------------------------------------------------------------------------------
def load_asl_states():
    asl = json.load(open(os.path.join(REPO, "stepfunctions", "startup.asl.json")))
    st = asl["States"]
    main_prefix = st["CheckCdcStarted"]["Parameters"]["Prefix.$"]
    fork_prefix = st["StartForkCdcMap"]["Iterator"]["States"]["CheckForkStarted"]["Parameters"]["Prefix.$"]
    map_params = st["StartForkCdcMap"]["Parameters"]
    fork_start_args = (st["StartForkCdcMap"]["Iterator"]["States"]["StartForkCdcJob"]
                       ["Parameters"]["Arguments"])
    main_start_args = st["StartCdcJob"]["Parameters"]["Arguments"]
    return main_prefix, fork_prefix, map_params, fork_start_args, main_start_args


# =============================================================================================
# TESTS
# =============================================================================================
def test_main_marker_contract():
    main_prefix_tmpl, _, _, _, main_start_args = load_asl_states()
    # The MAIN CDC job runs with --config_prefix = the TASK prefix, --startup_execution = exec name.
    scope = {"$.resolved.taskSuffix": TASK_SUFFIX, "$$.Execution.Name": EXEC}
    polled = eval_states_format(main_prefix_tmpl, scope)
    written = script_marker_keys(config_prefix=TASK_CP, startup_execution=EXEC, cdc_owners_key=None)
    check("MAIN: ASL polls --config_prefix=task uses $$.Execution.Name token",
          main_start_args.get("--config_prefix.$") == "$.resolved.configPrefix"
          and main_start_args.get("--startup_execution.$") == "$$.Execution.Name",
          main_start_args)
    check("MAIN: polled key == a key the main CDC script writes", polled in written,
          {"polled": polled, "written": sorted(written)})


def _fork_scope(fork, map_params):
    """Build the JSONPath scope the StartForkCdcMap Iterator sees for one fork item."""
    item = {"$$.Map.Item.Value." + k: v for k, v in fork.items()}
    scope = {}
    for dest, src in map_params.items():
        # dest like "config_prefix_key.$"; src like "$$.Map.Item.Value.config_prefix_key"
        name = dest[:-2] if dest.endswith(".$") else dest
        if src == "$$.Execution.Name":
            scope["$." + name] = EXEC
        elif src == "$.taskArn":
            scope["$." + name] = "arn:aws:dms:us-east-1:111122223333:task:ABC"
        elif src == "$.resolved.taskSuffix":
            scope["$." + name] = TASK_SUFFIX
        elif src == "$.resolved.cdcRoot":
            scope["$." + name] = "cdc"
        elif src == "$.resolved.timestampColumnName":
            scope["$." + name] = "dms_timestamp"
        elif src in item:
            scope["$." + name] = item[src]
        else:
            scope["$." + name] = item.get(src, src)
    return scope


def _one_fork(out, kind):
    forks = [f for f in out["forks"] if f.get("kind") == kind]
    assert forks, f"plan produced no {kind} fork"
    return forks[0]


def test_fork_marker_contract():
    _, fork_prefix_tmpl, map_params, fork_start_args, _ = load_asl_states()
    # one composite table -> ck fork; one big single-PK table -> bg fork
    entries = [_entry("ck_tab", "composite", rows=1000),
               _entry("big_tab", "single", rows=7_000_000),
               _entry("s1", "single")]
    out = run_plan(entries)

    # The ASL must pass config_prefix_key into the Map item (the fix) and use it in the poll.
    check("ASL StartForkCdcMap passes config_prefix_key to the item",
          map_params.get("config_prefix_key.$") == "$$.Map.Item.Value.config_prefix_key",
          map_params)
    check("ASL CheckForkStarted polls under $.config_prefix_key (fork prefix), not task-level",
          "$.config_prefix_key" in fork_prefix_tmpl and "config/_task/" not in fork_prefix_tmpl,
          fork_prefix_tmpl)

    for kind, label, cp_name in (("ck", "ck_tab", "composite"), ("bg", "big_tab", "big")):
        fork = _one_fork(out, kind)
        check(f"{kind}: plan_split emits config_prefix_key (bare key, trailing slash)",
              fork.get("config_prefix_key", "").endswith("/")
              and not fork["config_prefix_key"].startswith("s3://"),
              fork.get("config_prefix_key"))
        # precise: config_prefix == s3://BUCKET/<config_prefix_key>
        check(f"{kind}: config_prefix_key is exactly config_prefix minus s3://bucket/",
              fork["config_prefix"] == f"s3://{BUCKET}/" + fork["config_prefix_key"],
              {"cp": fork["config_prefix"], "cpk": fork["config_prefix_key"]})

        scope = _fork_scope(fork, map_params)
        polled = eval_states_format(fork_prefix_tmpl, scope)

        # The job is started with --config_prefix = fork cp and --startup_execution = <exec>-ck-<slug>
        start_tok_tmpl = fork_start_args["--startup_execution.$"]
        start_tok = eval_states_format(start_tok_tmpl, scope)
        cp_arg = scope[fork_start_args["--config_prefix.$"]] if fork_start_args["--config_prefix.$"].startswith("$.") else None
        check(f"{kind}: ASL starts the fork job with --config_prefix = fork config_prefix",
              cp_arg == fork["config_prefix"], {"cp_arg": cp_arg, "fork_cp": fork["config_prefix"]})

        # The fork CDC job carries --cdc_owners_key as a baked-in default job argument
        # (create_glue_jobs sets it = config/_task/<suffix>/_jobs.json for every CDC spec), so the
        # script also writes the task-level copy. Model both.
        owners_key = f"config/_task/{TASK_SUFFIX}/_jobs.json"
        written = script_marker_keys(config_prefix=fork["config_prefix"],
                                     startup_execution=start_tok, cdc_owners_key=owners_key)
        check(f"{kind}: polled key == a key the fork CDC script writes (fork prefix)",
              polled in written, {"polled": polled, "written": sorted(written)})
        # the fork-prefix marker is the primary match; confirm it is under the fork prefix
        check(f"{kind}: polled key is under the fork's own _orchestrator/{kind}-<slug>/ prefix",
              "/_orchestrator/" in polled and polled.startswith(fork["config_prefix_key"]),
              polled)


def test_negative_old_task_level_poll_was_the_bug():
    """Reconstruct the PRE-FIX poll (task-level) and prove it did NOT match the fork marker,
    and that the fixed fork-prefix poll DOES. This fails if someone reverts the ASL to the
    task-level prefix."""
    _, fork_prefix_tmpl, map_params, fork_start_args, _ = load_asl_states()
    entries = [_entry("ck_tab", "composite", rows=1000), _entry("s1", "single")]
    out = run_plan(entries)
    fork = _one_fork(out, "ck")
    scope = _fork_scope(fork, map_params)
    start_tok = eval_states_format(fork_start_args["--startup_execution.$"], scope)

    # Fork CDC script with NO task-level dual-write (older script) writes ONLY under the fork prefix.
    fork_only = script_marker_keys(fork["config_prefix"], start_tok, cdc_owners_key=None)

    # The buggy, pre-fix task-level poll key:
    old_poll = eval_states_format(
        "States.Format('config/_task/{}/_cdc_started/{}-ck-{}.json', $.taskSuffix, $.execName, $.fork_slug)",
        {"$.taskSuffix": TASK_SUFFIX, "$.execName": EXEC, "$.fork_slug": fork["fork_slug"]})
    check("NEGATIVE: the OLD task-level poll key is NOT what the fork script writes (this was B23)",
          old_poll not in fork_only, {"old_poll": old_poll, "fork_only": sorted(fork_only)})

    # The FIXED poll key IS what the fork script writes.
    new_poll = eval_states_format(fork_prefix_tmpl, scope)
    check("POSITIVE: the FIXED fork-prefix poll key IS what the fork script writes",
          new_poll in fork_only, {"new_poll": new_poll, "fork_only": sorted(fork_only)})

    # And the task-level dual-write makes the OLD poll succeed too (older workflows confirm).
    with_dual = script_marker_keys(fork["config_prefix"], start_tok,
                                   cdc_owners_key=f"config/_task/{TASK_SUFFIX}/_jobs.json")
    check("BACK-COMPAT: with the task-level dual-write, the OLD task-level poll now matches too",
          old_poll in with_dual, {"old_poll": old_poll, "with_dual": sorted(with_dual)})


def main():
    test_main_marker_contract()
    test_fork_marker_contract()
    test_negative_old_task_level_poll_was_the_bug()
    passed = sum(1 for r in RESULTS if r)
    failed = sum(1 for r in RESULTS if not r)
    print(f"\n==== marker-paths: {passed} passed, {failed} failed ====")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
