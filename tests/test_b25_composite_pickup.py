#!/usr/bin/env python3
"""B25 regression — composite (ck) CDC job new-file pickup must NOT stall after batch 1.

ROOT CAUSE (reproduced here against the SHIPPED code): scripts/glue_cdc_composite.py referenced a
module-level flag `_OPTIONAL_API_DOWN["cloudwatch"]` inside its best-effort CloudWatch metric
emitters (emit_drift_metric for G9 drift, emit_guard_warn_metric for the G7/G8 soft guards) but
never DEFINED `_OPTIONAL_API_DOWN` (glue_cdc_continuous.py defines it; the composite fork dropped
the definition). That reference sits OUTSIDE the functions' own try/except, so the first time a
drift or a soft-guard warn fired, emit_* raised `NameError: name '_OPTIONAL_API_DOWN' is not
defined`. The NameError propagated into process_table's table-level safety net and was logged as a
transient "cycle error (isolated, will retry next poll)". Because the condition that fires the
metric (persistent drift and/or a brand-new pending file tripping a soft guard) recurs every poll,
the table crashed every cycle BEFORE applying the newly-arrived files — so after batch 1 the job
stayed RUNNING but consumed nothing (files_in_control stuck at 1). The live Glue log for run
jr_09755678... shows exactly this: one "500 row(s)" apply, then "❌ ROW DRIFT", then
"name '_OPTIONAL_API_DOWN' is not defined" on every subsequent 30s cycle.

MAIN (glue_cdc_continuous.py) DEFINES _OPTIONAL_API_DOWN, so it never had the bug (and applied all
3 batches in the real run). This test covers BOTH engines so the latent shape can't reappear.

Approach (mirrors the project harness): exec the SHIPPED functions (list_cdc_files, guard_drift,
emit_drift_metric, emit_guard_warn_metric) into a namespace seeded with a fake S3 + fake CloudWatch
+ fake DSQL, then drive 3 poll cycles where 3 CDC files for one composite table arrive over the 3
cycles. Assert: no emitter ever raises NameError; all 3 files are applied IN ORDER EXACTLY ONCE;
the table ends idle. On the UNFIXED code the first cycle's emit_drift_metric raises NameError and
the file-pickup loop stalls (files 2/3 never applied) — exactly B25.

Run: python3 tests/test_b25_composite_pickup.py   (REPO_DIR overridable)
"""
import ast
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO
COMP = "scripts/glue_cdc_composite.py"
CONT = "scripts/glue_cdc_continuous.py"

_passed = 0
_failed = 0


def check(cond, msg):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"[PASS] {msg}")
    else:
        _failed += 1
        print(f"[FAIL] {msg}")
        # Under pytest the direct-run main()/sys.exit path never runs, so a failed check must
        # surface as a real test failure (otherwise pytest reports PASS even when assertions
        # fail). Direct `python3 tests/<f>.py` runs are unaffected (PYTEST_CURRENT_TEST unset),
        # so the count-and-continue summary still works.
        import os as _os
        if "PYTEST_CURRENT_TEST" in _os.environ:
            raise AssertionError(msg)


# ---------------------------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------------------------
class _FakeS3:
    """Minimal list_objects_v2 over an in-memory key->LastModified map. No pagination, no
    Delimiter semantics needed (all keys live directly under the one prefix)."""
    def __init__(self):
        self.objs = {}  # key -> datetime(UTC)

    def put(self, key, when):
        self.objs[key] = when

    def list_objects_v2(self, Bucket=None, Prefix="", Delimiter=None, ContinuationToken=None):
        contents = [{"Key": k, "LastModified": lm}
                    for k, lm in sorted(self.objs.items()) if k.startswith(Prefix)]
        return {"Contents": contents, "IsTruncated": False}


class _FakeCloudWatch:
    """Records put_metric_data calls; never raises. If the SHIPPED emitter is broken (undefined
    _OPTIONAL_API_DOWN) the NameError happens BEFORE we're ever called, so a count of 0 with the
    guard having fired is itself the failure signal."""
    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kw):
        self.calls.append(kw)


def _module_assign_src(script_rel, name):
    """Return the SHIPPED source text of each module-level `name = ...` assignment (a list; empty
    if the module never defines it). Used to exec the real _OPTIONAL_API_DOWN definition — the
    exact statement the B25 fix adds — into the test namespace rather than injecting a fake one."""
    src = H.read_source(script_rel)
    tree = ast.parse(src)
    out = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    out.append(ast.get_source_segment(src, node))
    return out


def _module_consts(script_rel):
    """Pull the handful of module-level constants the exec'd functions read, straight from the
    shipped source via AST (so the test tracks the real values)."""
    src = H.read_source(script_rel)
    tree = ast.parse(src)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in ("MIN_FILE_AGE_SECONDS",):
                try:
                    out[name] = ast.literal_eval(node.value)
                except Exception:
                    pass
    return out


def _build_ns(script_rel, s3, cw):
    """exec the shipped file-pickup + metric + guard functions into a fake-seeded namespace."""
    consts = _module_consts(script_rel)
    injected = {
        "s3": s3,
        "cloudwatch": cw,
        "BUCKET": "fake-bucket",
        "REGION": "us-east-1",
        "CONTROL_SCHEMA": "cdc_control_test",
        "MIN_FILE_AGE_SECONDS": consts.get("MIN_FILE_AGE_SECONDS", 15),
        "CDC_DRIFT_TOLERANCE": 0.0,
        "datetime": datetime,
        "timezone": timezone,
        "timedelta": timedelta,
        "print": lambda *a, **k: None,   # keep cycle output quiet; errors still raise
    }
    # NOTE: we deliberately DO NOT inject _OPTIONAL_API_DOWN. The shipped module must define it
    # itself. We exec the SHIPPED module-level `_OPTIONAL_API_DOWN = {...}` statement (if present)
    # into the namespace — exactly where the real fix lives. On the UNFIXED composite that
    # statement is absent, so the exec'd emitters hit a NameError at call time (B25).
    ns = dict(injected)
    for stmt in _module_assign_src(script_rel, "_OPTIONAL_API_DOWN"):
        exec(compile(stmt, f"<{script_rel}:_OPTIONAL_API_DOWN>", "exec"), ns)
    fn_src = H.extract_defs(
        os.path.join(REPO, script_rel),
        ["list_cdc_files", "guard_drift", "emit_drift_metric", "emit_guard_warn_metric",
         "guard_file_order", "guard_new_load_after_cdc", "guard_action_blocks"],
    )
    for name in ("list_cdc_files", "guard_drift", "emit_drift_metric", "emit_guard_warn_metric",
                 "guard_file_order", "guard_new_load_after_cdc", "guard_action_blocks"):
        exec(compile(fn_src[name], f"<{script_rel}:{name}>", "exec"), ns)
    return ns


def _ctx(label="dms_sample_v8.name_data", prefix="DMS_SAMPLE/NAME_DATA/"):
    return {"label": label, "prefixes": {"cdc": prefix, "processed": prefix + "processed/"}}


# ---------------------------------------------------------------------------------------------
# The reproduction: 3 files over 3 cycles, exactly-once, in order, end idle.
# ---------------------------------------------------------------------------------------------
def _run_three_cycle_pickup(engine_label, script_rel):
    s3 = _FakeS3()
    cw = _FakeCloudWatch()
    ns = _build_ns(script_rel, s3, cw)
    list_cdc_files = ns["list_cdc_files"]
    guard_drift = ns["guard_drift"]
    emit_drift_metric = ns["emit_drift_metric"]
    emit_guard_warn_metric = ns["emit_guard_warn_metric"]

    ctx = _ctx()
    pfx = ctx["prefixes"]["cdc"]
    # Three CDC files, DMS-timestamp names, all old enough to not be deferred (> MIN_FILE_AGE).
    old = datetime.now(timezone.utc) - timedelta(seconds=120)
    f1 = pfx + "20261006-145653545.csv"
    f2 = pfx + "20261006-151607446.csv"
    f3 = pfx + "20261006-152759306.csv"
    arrivals = {1: f1, 2: f2, 3: f3}   # file N becomes visible at the start of cycle N

    # Fake DSQL state the real cycle would read/write.
    applied_order = []       # the order files were marked done (must == [f1, f2, f3])
    last_done = [None]       # high-water mark (cdc_status.last_done_file)
    full_load_rows = [6955]  # pre-existing baseline rows (reproduces the +6955 drift in the log)

    nameerror = {"hit": False, "msg": ""}

    for cycle in (1, 2, 3):
        # A new file lands in S3 at the start of this cycle (what DMS does between polls).
        s3.put(arrivals[cycle], old)

        try:
            # 1) The REAL file lister (the thing that "stalled"): it must keep seeing new files.
            files = list_cdc_files(ctx)
            pending = [k for k in files if not (last_done[0] and k <= last_done[0])]

            # 2) The soft-guard WARN path exactly as process_table runs it: a brand-new pending
            #    file after CDC started trips guard_file_order's "gap/regression" branch in some
            #    runs and the new-LOAD guard in others; in WARN mode process_table calls
            #    emit_guard_warn_metric. We exercise that SHIPPED emitter on every busy cycle.
            if pending and last_done[0]:
                emit_guard_warn_metric(ctx["label"], "G8_file_order")

            # 3) Apply each pending file in order, exactly once (model the apply as "mark done").
            for key in pending:
                applied_order.append(key)
                last_done[0] = key
                full_load_rows[0] += 1  # one net insert per file (arbitrary, drives drift)

            # 4) The REAL G9 drift check tail of the cycle: in the live run the baseline made
            #    guard_drift fire and process_table called emit_drift_metric — the exact line
            #    that raised NameError on the unfixed composite.
            live = full_load_rows[0] + 1000      # pretend DSQL has more rows than tracked
            fired, delta, _exp = guard_drift(live, full_load_rows[0], 0, 0, 0.0)
            if fired:
                emit_drift_metric(ctx["label"], delta)
        except NameError as e:
            nameerror["hit"] = True
            nameerror["msg"] = str(e)
            break

    # End-state: the table is "idle" iff every S3 file has been applied and nothing is pending.
    files_final = list_cdc_files(ctx)
    pending_final = [k for k in files_final if not (last_done[0] and k <= last_done[0])]

    lbl = engine_label
    check(not nameerror["hit"],
          f"{lbl}: no NameError from the shipped metric emitters across 3 cycles "
          f"(got: {nameerror['msg']!r})")
    check(applied_order == [f1, f2, f3],
          f"{lbl}: all 3 files applied IN ORDER exactly once "
          f"(got {[k.rsplit('/',1)[-1] for k in applied_order]})")
    check(len(applied_order) == len(set(applied_order)),
          f"{lbl}: each file applied EXACTLY ONCE (no dup)")
    check(pending_final == [],
          f"{lbl}: table ends IDLE (0 pending after cycle 3)")
    # The CloudWatch emitters must have actually been reached (proves we exercised the crash path,
    # not that we skipped it): a drift metric per busy cycle + guard-warn on cycles 2 & 3.
    check(len(cw.calls) >= 3,
          f"{lbl}: shipped emitters reached CloudWatch (>=3 metric puts; got {len(cw.calls)})")


# ---------------------------------------------------------------------------------------------
# Static regression guard: the module global must exist in BOTH engines.
# ---------------------------------------------------------------------------------------------
def _defines_optional_api_down(script_rel):
    src = H.read_source(script_rel)
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "_OPTIONAL_API_DOWN":
                    return True
    return False


def test_optional_api_down_defined_both_engines():
    check(_defines_optional_api_down(COMP),
          "composite: _OPTIONAL_API_DOWN defined at module level (B25 fix)")
    check(_defines_optional_api_down(CONT),
          "main: _OPTIONAL_API_DOWN defined at module level (never regress)")


def test_composite_three_cycle_pickup():
    _run_three_cycle_pickup("composite", COMP)


def test_main_three_cycle_pickup():
    _run_three_cycle_pickup("main", CONT)


if __name__ == "__main__":
    test_optional_api_down_defined_both_engines()
    test_composite_three_cycle_pickup()
    test_main_three_cycle_pickup()
    print(f"\n{_passed} passed, {_failed} failed")
    sys.exit(1 if _failed else 0)
