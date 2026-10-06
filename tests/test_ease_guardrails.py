#!/usr/bin/env python3
"""Offline tests for the "ease the guardrails" change (no AWS, no Spark, no network).

Principle under test: a guardrail may only ever STOP a destructive action (deleting/emptying
target rows). It must NEVER fail a load/validate/CDC/cutover run because of its OWN bookkeeping
(a missing permission, a missing control table, a lock it can't take, or a check it can't
compute). The master setting guardrails_mode = warn (default) | strict toggles fail-closed.

Mirrors the project harness: parse each shipped script with `ast`, exec the named functions into
a namespace seeded with fakes, so the logic under test is the SHIPPED logic.

Coverage:
  * guardrails_mode plumbs through params_csv -> resolve_task -> create_glue_jobs -> job args.
  * SOFT load guards:
      G2 missing states:DescribeExecution (describe failed) + --startup_execution -> warn-allows;
         strict -> refuses.
      G3 lock take_table_lock returns a 3rd 'contended' flag; a live contender vs a bookkeeping
         failure are distinguished; warn proceeds on bookkeeping failure, strict refuses, a live
         contender refuses in both.
  * HARD load guards (G1/G4) still refuse the destructive blank in BOTH modes.
  * SOFT CDC guards (G7/G8): guard_action_blocks maps warn->don't-block, block->block; the apply
    wiring branches on it, writes a *_warn audit + emits DsqlGuardWarn, and never raises in warn.
  * G9 drift is warn by default (already) and never raises.
  * G10 validate warn vs strict decision.
  * Happy path: a Glue role WITHOUT states:DescribeExecution, defaults everywhere, no guard fires
    -> a workflow-driven reblank is allowed and NOTHING refuses.

Run: python3 tests/test_ease_guardrails.py   (REPO_DIR overridable)
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO
CONT = "scripts/glue_cdc_continuous.py"
COMP = "scripts/glue_cdc_composite.py"
LOAD = "scripts/job2_load.py"
VAL = "scripts/job3_validate.py"

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


# =============================================================================================
# Plumbing: guardrails_mode + the per-guard knobs flow end to end.
# =============================================================================================
def test_params_csv_defaults_and_validation():
    import importlib
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    pc = importlib.import_module("params_csv")
    importlib.reload(pc)
    d = pc.OPTIONAL_DEFAULTS
    for k, v in (("guardrails_mode", "warn"), ("cdc_file_order_action", "warn"),
                 ("cdc_nopk_overmatch_action", "warn"), ("validate_count_check", "warn"),
                 ("cutover_count_check", "warn")):
        check(d.get(k) == v, f"params_csv default {k}={v!r} (got {d.get(k)!r})")
    for k in ("guardrails_mode", "cdc_file_order_action", "cdc_nopk_overmatch_action",
              "validate_count_check", "cutover_count_check"):
        check(k in pc.PIPELINE_KEYS, f"params_csv: {k} is a pipeline key")
    # Validation: a bad enum is rejected; a good one accepted. Match the specific enum message
    # (the generic "missing required key" error lists ALL allowed keys, so a plain substring
    # match would spuriously hit).
    def _has_enum_err(errs, key):
        return any(e.startswith(f"{key} must be one of") for e in errs)
    _bad = _params_errors(pc, "guardrails_mode,loud\n")
    check(_has_enum_err(_bad, "guardrails_mode"), "params_csv: invalid guardrails_mode rejected")
    _ok = _params_errors(pc, "guardrails_mode,strict\n")
    check(not _has_enum_err(_ok, "guardrails_mode"),
          "params_csv: guardrails_mode=strict accepted (no enum error)")
    _bado = _params_errors(pc, "cdc_file_order_action,warnish\n")
    check(_has_enum_err(_bado, "cdc_file_order_action"),
          "params_csv: invalid cdc_file_order_action rejected")


def _params_errors(pc, body_row):
    """Run params_csv.parse() on a minimal CSV (header + one row) and return its error list."""
    text = "parameter,value\n" + body_row
    res = pc.parse(text)
    return list(res.get("errors", []))


def test_resolve_task_payload_has_mode():
    src = H.read_source("lambdas/resolve_task.py")
    for key in ('"guardrailsMode"', '"cdcFileOrderAction"', '"cdcNopkOvermatchAction"',
                '"validateCountCheck"', '"cutoverCountCheck"'):
        check(key in src, f"resolve_task payload includes {key}")
    for d in ('"guardrails_mode": "warn"', '"cdc_file_order_action": "warn"',
              '"validate_count_check": "warn"', '"cutover_count_check": "warn"'):
        check(d in src, f"resolve_task default {d}")


def test_create_glue_jobs_passes_mode_to_each_job():
    src = H.read_source("lambdas/create_glue_jobs.py")
    # CDC jobs get the master mode + the two CDC per-guard actions.
    for a in ("--guardrails_mode", "--cdc_file_order_action", "--cdc_nopk_overmatch_action"):
        check(a in src, f"create_glue_jobs passes {a} to CDC jobs")
    # Load + validate get the master mode; validate gets the count-check knob.
    check(src.count('args["--guardrails_mode"]') >= 2,
          "create_glue_jobs sets --guardrails_mode on load AND validate (>=2 assignments)")
    check('args["--validate_count_check"]' in src,
          "create_glue_jobs passes --validate_count_check to validate")


# =============================================================================================
# SOFT load guards — the SHIPPED assert_blank_allowed with injected fakes (as test_guardrails).
# =============================================================================================
def _load_blank_guard(ov):
    inj = {
        "CONTROL_SCHEMA": "cdc_control", "BLANK_GUARD_ENABLED": True,
        "BLANK_EXPECTED_MARGIN": 0.05, "ALLOW_MANUAL_DESTRUCTIVE": False,
        "GUARDRAILS_MODE": "warn", "STARTUP_EXECUTION": None, "STARTUP_EXECUTION_ARN": None,
        "_cdc_started_result": (False, ""), "_workflow_result": ("none", "manual run"),
        "audit_calls": [], "prints": [],
    }
    inj.update(ov)
    src = H.read_source(LOAD)
    t = ast.parse(src)
    want = {"assert_blank_allowed", "guard_blank_count_sane"}
    defs = {nd.name: ast.get_source_segment(src, nd)
            for nd in t.body if isinstance(nd, ast.FunctionDef) and nd.name in want}
    ns = dict(inj)
    ns["_cdc_has_started"] = lambda a, b: ns["_cdc_started_result"]
    ns["_workflow_execution_running"] = lambda: ns["_workflow_result"]
    ns["write_load_audit"] = lambda *a, **k: ns["audit_calls"].append((a, k))
    for n in ("guard_blank_count_sane", "assert_blank_allowed"):
        exec(compile(defs[n], f"<{LOAD}:{n}>", "exec"), ns)
    return ns


def test_g2_missing_describe_permission_warn_allows():
    """Injected failure: a Glue role without states:DescribeExecution. _workflow returns
    'not_running' with the permission reason; --startup_execution present. warn -> ALLOWED."""
    ns = _load_blank_guard({
        "_workflow_result": ("not_running", "could not describe the Step Functions execution "
                             "(AccessDenied: states:DescribeExecution)."),
        "STARTUP_EXECUTION": "startup-xyz", "GUARDRAILS_MODE": "warn"})
    ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")   # no raise
    joined = " ".join(str(a) + str(k) for (a, k) in ns["audit_calls"])
    check("g2_warn_allowed" in joined,
          "G2 soft: a missing states:DescribeExecution does NOT fail the run (warn-allows + audits)")


def test_g2_missing_describe_permission_strict_refuses():
    ns = _load_blank_guard({
        "_workflow_result": ("not_running", "could not describe (AccessDenied)."),
        "STARTUP_EXECUTION": "startup-xyz", "GUARDRAILS_MODE": "strict"})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G2 strict: expected a refusal when the execution can't be confirmed")
    except Exception as e:
        check("G2" in str(e), "G2 strict: refuses a blank it can't confirm is workflow-driven")


def test_g1_hard_refuses_in_both_modes():
    for mode in ("warn", "strict"):
        ns = _load_blank_guard({"_cdc_started_result": (True, "cdc_status row exists for s.t"),
                                "_workflow_result": ("running", "RUNNING"),
                                "GUARDRAILS_MODE": mode})
        try:
            ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
            check(False, f"G1[{mode}]: expected refusal after CDC started")
        except Exception as e:
            check("G1" in str(e), f"G1 HARD[{mode}]: refuses a blank once CDC has started")


def test_g4_hard_refuses_in_both_modes():
    for mode in ("warn", "strict"):
        ns = _load_blank_guard({"_workflow_result": ("running", "RUNNING"),
                                "GUARDRAILS_MODE": mode})
        try:
            ns["assert_blank_allowed"]("s", "t", 10_000, 100, True, "auto_reblank")  # count>>exp
            check(False, f"G4[{mode}]: expected refusal when count >> expected")
        except Exception as e:
            check("G4" in str(e), f"G4 HARD[{mode}]: refuses a blank far above the expected count")


def test_g1_undecidable_refuses_only_the_blank_not_the_run():
    """G1 can't decide (unreadable marker) -> fail-closed on the BLANK (raises a per-table
    BLANK GUARD G1), which the per-table CONTINUE-ON-FAILURE loop logs while the rest of the run
    keeps going. Assert the refusal is a per-table exception, not a job-wide abort signal."""
    ns = _load_blank_guard({"_cdc_started_result": (True, "could not read the _cdc_started marker"),
                            "_workflow_result": ("running", "RUNNING"), "GUARDRAILS_MODE": "warn"})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G1 undecidable: expected a per-table refusal")
    except Exception as e:
        check("G1" in str(e) and "BLANK GUARD" in str(e),
              "G1 undecidable: refuses ONLY the blank (per-table BLANK GUARD exception)")
    # The load loop continues on a per-table failure (documented contract in job2_load header).
    src = H.read_source(LOAD)
    check("CONTINUE-ON-FAILURE" in src,
          "load: a failed table is logged and the loop continues (rest of the run proceeds)")


# =============================================================================================
# G3 lock: the SHIPPED take_table_lock 3-tuple + caller branching.
# =============================================================================================
def _load_take_lock(fake_cur_rows, insert_raises=False, connect_raises=False):
    """Exec the shipped take_table_lock with a fake control cursor/conn."""
    class _Cur:
        def __init__(self):
            self._q = list(fake_cur_rows)
            self._last = None
        def execute(self, sql, params=None):
            self._last = sql.lower()
            if "insert into" in self._last and insert_raises:
                raise Exception("duplicate key value violates unique constraint (pk)")
        def fetchone(self):
            return self._q.pop(0) if self._q else None
        def close(self):
            pass
    cur = _Cur()
    conn = type("C", (), {"autocommit": True, "cursor": lambda self: cur,
                          "close": lambda self: None})()

    def _control_cur():
        if connect_raises:
            raise Exception("DSQL connect failed (resolver/credential error)")
        return conn, cur

    inj = {
        "CONTROL_SCHEMA": "cdc_control", "_HELD_TABLE_LOCKS": set(),
        "LOCK_HEARTBEAT_TIMEOUT_SECONDS": 1800,
        "utc_now_iso": lambda: "2026-01-01T00:00:00+00:00",
        "datetime": __import__("datetime"), "_control_cur": _control_cur,
        "_run_arg_load": lambda name: {"JOB_NAME": "j", "JOB_RUN_ID": "r"}.get(name),
    }
    ns = H.load_defs(LOAD, ["take_table_lock", "_lock_is_stale"], inj)
    return ns


def test_g3_take_lock_returns_contended_flag():
    # No existing row -> acquire; contended False.
    ns = _load_take_lock([None])         # SELECT owner,heartbeat -> None
    acquired, who, contended = ns["take_table_lock"]("s", "t")
    check(acquired and not contended, "G3: a free lock is acquired (contended=False)")
    # A fresh (non-stale) holder owned by someone else -> NOT acquired, contended True.
    ns2 = _load_take_lock([("other:run", "2099-01-01T00:00:00+00:00")])
    acq2, why2, cont2 = ns2["take_table_lock"]("s", "t")
    check((not acq2) and cont2, "G3: a live foreign holder -> contended=True (a real second writer)")
    # Lost the INSERT race (PK clash) -> contended True.
    ns3 = _load_take_lock([None], insert_raises=True)
    acq3, why3, cont3 = ns3["take_table_lock"]("s", "t")
    check((not acq3) and cont3, "G3: losing the INSERT race -> contended=True")
    # Can't even connect (bookkeeping failure) -> NOT contended.
    ns4 = _load_take_lock([None], connect_raises=True)
    acq4, why4, cont4 = ns4["take_table_lock"]("s", "t")
    check((not acq4) and (not cont4),
          "G3: a connect failure is a bookkeeping failure (contended=False)")


def test_g3_caller_warns_on_bookkeeping_failure_refuses_on_contender():
    """Static: the reblank caller distinguishes _contended (raise in both modes) from a
    bookkeeping failure (warn+proceed in warn, raise in strict)."""
    src = H.read_source(LOAD)
    check("_locked, _lk, _contended = take_table_lock(" in src,
          "G3: the caller unpacks the 3-tuple (acquired, reason, contended)")
    check("if _contended:" in src, "G3: the caller branches on a live contender")
    check('GUARDRAILS_MODE == "strict"' in src,
          "G3: strict mode still refuses a lock it couldn't take")
    check("lock_unavailable_warn" in src,
          "G3: warn mode audits a 'lock unavailable (bookkeeping)' + proceeds")
    # Both destructive reblank paths carry the same 3-way branch.
    check(src.count("if _contended:") >= 2,
          "G3: BOTH reblank paths (resume + no-PK mid-file) use the contended/warn/strict branch")


# =============================================================================================
# SOFT CDC guards (G7/G8): guard_action_blocks + wiring.
# =============================================================================================
def test_guard_action_blocks_mapping():
    for eng in (CONT, COMP):
        ns = H.load_defs(eng, ["guard_action_blocks"], {})
        f = ns["guard_action_blocks"]
        check(f("block") is True, f"{os.path.basename(eng)}: guard_action_blocks('block') is True")
        for warnish in ("warn", "WARN", "", "anything", None):
            check(f(warnish) is False,
                  f"{os.path.basename(eng)}: guard_action_blocks({warnish!r}) is False (safe warn)")


def test_g7_g8_wiring_warns_by_default_in_both_engines():
    for eng in (CONT, COMP):
        src = H.read_source(eng)
        # G8 new-LOAD + file-order and G7 nopk all branch on guard_action_blocks(...).
        check(src.count("guard_action_blocks(CDC_FILE_ORDER_ACTION)") >= 2,
              f"{os.path.basename(eng)}: G8 new-LOAD + file-order gate on CDC_FILE_ORDER_ACTION")
        check("guard_action_blocks(CDC_NOPK_OVERMATCH_ACTION)" in src,
              f"{os.path.basename(eng)}: G7 gates on CDC_NOPK_OVERMATCH_ACTION")
        # warn branch: a *_warn audit action + the DsqlGuardWarn metric, no raise/block.
        for warn_action in ("cdc_new_load_after_start_warn", "cdc_file_order_warn",
                            "cdc_nopk_overmatch_warn"):
            check(warn_action in src,
                  f"{os.path.basename(eng)}: warn branch audits {warn_action}")
        check(src.count("emit_guard_warn_metric(") >= 3,
              f"{os.path.basename(eng)}: warn branches emit the DsqlGuardWarn metric (>=3)")
        # A guard's own read must not raise into the apply path: list_load_files is wrapped.
        check("could not list LOAD* files" in src,
              f"{os.path.basename(eng)}: the G8 LOAD-list read is wrapped (can't raise into apply)")


def test_strict_mode_blocks_cdc_guards():
    """strict sets CDC_FILE_ORDER_ACTION/CDC_NOPK_OVERMATCH_ACTION to 'block' in the overlay."""
    for eng in (CONT, COMP):
        src = H.read_source(eng)
        check('CDC_FILE_ORDER_ACTION = "block"' in src
              and 'CDC_NOPK_OVERMATCH_ACTION = "block"' in src,
              f"{os.path.basename(eng)}: guardrails_mode=strict flips the CDC guards to block")


# ---- G9 drift stays warn by default and never raises (reuse the shipped runner) ----
def test_g9_drift_runner_never_raises_and_warns_by_default():
    inj = {
        "CDC_DRIFT_CHECK_MINUTES": 30, "CDC_DRIFT_TOLERANCE": 0.0, "CDC_DRIFT_ACTION": "warn",
        "CONTROL_SCHEMA": "cdc_control", "_DRIFT_LAST_RUN": {}, "time": __import__("time"),
    }
    src = H.read_source(CONT)
    t = ast.parse(src)
    want = {"_maybe_run_drift_check", "guard_drift"}
    defs = {nd.name: ast.get_source_segment(src, nd)
            for nd in t.body if isinstance(nd, ast.FunctionDef) and nd.name in want}
    ns = dict(inj)
    ns["emit_drift_metric"] = lambda label, delta: None
    ns["_audit_destructive"] = lambda *a, **k: None
    ns["_set_blocked_status"] = lambda *a, **k: None
    exec(compile(defs["guard_drift"], "<guard_drift>", "exec"), ns)
    exec(compile(defs["_maybe_run_drift_check"], "<drift>", "exec"), ns)

    class _Cur:
        def __init__(self, conn):
            self.conn = conn
            self._last = None
        def execute(self, sql, params=None):
            self._last = "count" if "count(*)" in sql.lower() else "counters"
        def fetchone(self):
            return (self.conn.live,) if self._last == "count" else self.conn.counters
        def close(self):
            pass

    class _Conn:
        live = 100
        counters = (1000, 0, 0)   # big shortfall
        def cursor(self):
            return _Cur(self)
        def commit(self):
            pass
        def rollback(self):
            pass
    res = ns["_maybe_run_drift_check"]([_Conn(), 0.0], "s.t", "s", "t")
    check(res is None, "G9: a big drift with action=warn returns None (warn, NOT blocked)")


# =============================================================================================
# G10 validate warn vs strict (shipped pure fn + the warn/strict decision in the script).
# =============================================================================================
def test_g10_validate_warn_vs_strict_static():
    src = H.read_source(VAL)
    check('VALIDATE_COUNT_CHECK = "warn"' in src, "G10 validate: default is warn")
    check('VALIDATE_COUNT_CHECK == "strict"' in src,
          "G10 validate: only strict sets status='mismatch' (warn keeps PASS)")
    check("G10 WARNING (validate_count_check=warn)" in src,
          "G10 validate: warn mode records a WARNING note and does NOT fail the table")


def test_g10_pure_equation_unchanged():
    ns = H.load_defs(VAL, ["guard_count_vs_dms"], {})
    ok, delta, expected, why = ns["guard_count_vs_dms"](900, 1000, 0, 0, 0)
    check((not ok) and delta == -100 and expected == 1000, "G10 pure: validate shortfall detected")
    ok2, _, exp2, _ = ns["guard_count_vs_dms"](1140, 1000, 200, 50, 0)
    check((not ok2) and exp2 == 1150, "G10 pure: cutover equation FullLoadRows+I-D unchanged")
    ok3, _, _, _ = ns["guard_count_vs_dms"](1140, 1000, 200, 50, 100)
    check(ok3, "G10 pure: a tolerance band passes a near-match (explicit override)")


# =============================================================================================
# HAPPY PATH: no guard fires, a Glue role WITHOUT states:DescribeExecution, all defaults warn.
# =============================================================================================
def test_happy_path_no_guard_fires_no_extra_permission():
    """A full normal run: load reblank under a RUNNING workflow, CDC applies files in order with
    small deletes, validate counts match. Nothing refuses; the missing describe permission is
    irrelevant because the execution IS confirmed RUNNING (and even if it weren't, warn allows).
    """
    # Load: RUNNING workflow, previously attempted, count == expected -> ALLOWED (no raise).
    ns = _load_blank_guard({"_workflow_result": ("running", "execution OK is RUNNING"),
                            "GUARDRAILS_MODE": "warn"})
    try:
        ns["assert_blank_allowed"]("s", "t", 1000, 1000, True, "auto_reblank")
        check(True, "HAPPY: a workflow-driven reblank with matching counts is ALLOWED")
    except Exception as e:
        check(False, f"HAPPY: the load gate refused a normal reblank: {e}")

    # CDC: small delete not blocked; in-order file allowed; no new LOAD; drift within tolerance.
    cns = H.load_defs(CONT, ["guard_mass_delete", "guard_file_order",
                             "guard_new_load_after_cdc", "guard_drift"], {})
    blocked, _ = cns["guard_mass_delete"](1_000_000, 10, False, 0.5, 100_000)
    check(not blocked, "HAPPY: a tiny CDC delete is not blocked (G6)")
    ok8, _ = cns["guard_file_order"]("CDC/00009.csv", "CDC/00008.csv", ["CDC/00008.csv"])
    check(ok8, "HAPPY: the next in-order CDC file is allowed (G8)")
    okL, _ = cns["guard_new_load_after_cdc"]([], True)
    check(okL, "HAPPY: no new LOAD* after CDC started -> allowed (G8)")
    fired, _, _ = cns["guard_drift"](1040, 1000, 50, 10, 5)
    check(not fired, "HAPPY: live==expected within tolerance -> no drift (G9)")

    # Validate: DSQL == FullLoadRows -> pass; and with NO DMS figure it's a no-op pass.
    vns = H.load_defs(VAL, ["guard_count_vs_dms"], {})
    okv, _, _, _ = vns["guard_count_vs_dms"](1000, 1000, 0, 0, 0)
    check(okv, "HAPPY: validate count matches DMS FullLoadRows -> PASS (G10)")
    okv2, _, _, _ = vns["guard_count_vs_dms"](1000, None, 0, 0, 0)
    check(okv2, "HAPPY: no DMS figure (no describe permission) -> G10 no-op PASS (never fails)")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        try:
            globals()[fn]()
        except Exception as e:
            global _failed
            _failed += 1
            print(f"[FAIL] {fn} raised {type(e).__name__}: {e}")
    print(f"\n==== ease-guardrails: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
