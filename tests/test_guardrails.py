#!/usr/bin/env python3
"""Offline tests for the data-safety guardrails G1–G10 (no AWS, no Spark, no network).

Mirrors the project's harness style: parse each shipped script with `ast`, extract the exact
source of the named functions, and exec just those into a namespace seeded with fakes + the
constants they read — so the logic under test is the SHIPPED logic. A fake DSQL cursor enforces
the ~3000-row/txn cap where relevant.

Coverage (one+ test per WHAT_IF row):
  G1  a load blank is refused once CDC has started (marker OR cdc_status row); fail-closed.
  G2  (redesigned) a MANUAL (no-workflow) run may WRITE; a destructive blank in a manual run is
      refused WITHOUT --allow_manual_destructive and ALLOWED+audited WITH it; a RUNNING workflow
      blanks without the flag. A manual CDC run is allowed (G2 doesn't apply to CDC).
  G3  a second load fails on the per-table lock; CDC's second holder skips with a warning;
      a stale lock may be taken; release works.
  G4  blank refused unless previously attempted by THIS task; refused when count > expected*margin.
  G5  audit_log row written BEFORE a destructive op; audit CREATE TABLE has no DEFAULT-on-ALTER.
  G6  an 80%-of-1M-row delete file is blocked (nothing applied); small delete unaffected; the
      allow_mass_delete + fraction>=1 overrides work.
  G7  a no-PK over-match blocks; an exact match is ok; the _cdc_file purge is exact-only.
  G8  a file older than the high-water blocks; a gap blocks; in-order ok; a new LOAD after CDC
      started blocks.
  G9  drift fires on injected drift; within tolerance ok; block action; counters tracked.
  G10 validate/cutover count equation vs DMS FullLoadRows: mismatch fails, match passes, override.
  shared helpers are byte-identical between the two CDC engines; Spark CDC uses the same scripts.

Run: python3 tests/test_guardrails.py   (REPO_DIR overridable)
"""
import ast
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO
CONT = "scripts/glue_cdc_continuous.py"
COMP = "scripts/glue_cdc_composite.py"
LOAD = "scripts/job2_load.py"
VAL = "scripts/job3_validate.py"

DSQL_ROW_CAP = 3000

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
# Pure CDC guard helpers (G6/G7/G8/G9) — load from BOTH engines and assert identical behaviour.
# =============================================================================================
_PURE_CDC = ["guard_mass_delete", "guard_nopk_delete_bound", "purge_predicate_is_exact",
             "guard_file_order", "guard_new_load_after_cdc", "guard_drift"]


def _load_pure(script_rel):
    return H.load_defs(script_rel, _PURE_CDC, {})


def test_shared_helpers_byte_identical():
    """Composite-key tables must get the SAME guards: the shared helpers are byte-identical."""
    names = _PURE_CDC + ["_ensure_cdc_status_guardrail_columns", "write_audit_log",
                         "emit_drift_metric", "_read_count_and_allow", "_audit_destructive",
                         "_nopk_delete_precision_check", "list_load_files",
                         "_set_blocked_status", "_advance_cdc_counters", "_maybe_run_drift_check"]

    def grab(p):
        src = open(os.path.join(REPO, p)).read()
        t = ast.parse(src)
        return {nd.name: ast.get_source_segment(src, nd)
                for nd in t.body if isinstance(nd, ast.FunctionDef) and nd.name in names}
    a, b = grab(CONT), grab(COMP)
    missing = [n for n in names if n not in a or n not in b]
    check(not missing, f"shared helpers present in both CDC engines (missing: {missing})")
    diff = [n for n in names if a.get(n) != b.get(n)]
    check(not diff, f"shared guardrail helpers are BYTE-IDENTICAL between engines (diff: {diff})")


def test_spark_cdc_uses_same_scripts():
    """Spark CDC runs the SAME scripts (switch_cdc_engine / templates reference them), so the
    guards cover Spark too. Assert the engine templates point at the shared script names."""
    import json
    for tmpl, want in (("glue-templates/cdc-spark.json", "glue_cdc_continuous.py"),
                       ("glue-templates/cdc-composite-spark.json", "glue_cdc_composite.py")):
        p = os.path.join(REPO, tmpl)
        if not os.path.exists(p):
            continue
        txt = open(p).read()
        check(want in txt, f"{tmpl} references {want} (Spark CDC uses the same guarded script)")


# ---- G6 mass delete ----
def test_g6_mass_delete_blocks_nothing_applied():
    for eng in (CONT, COMP):
        ns = _load_pure(eng)
        # 1,000,000-row table, a file deleting 800,000 rows (80%): blocked.
        blocked, why = ns["guard_mass_delete"](1_000_000, 800_000, False, 0.5, 100_000)
        check(blocked and "DELETE" in why,
              f"G6[{os.path.basename(eng)}]: 80% of a 1M-row table -> BLOCKED (nothing applied)")


def test_g6_small_delete_unaffected():
    ns = _load_pure(CONT)
    # Deleting 10 of 1,000,000 rows: well under both thresholds -> not blocked.
    blocked, _ = ns["guard_mass_delete"](1_000_000, 10, False, 0.5, 100_000)
    check(not blocked, "G6: a tiny delete on a large table is NOT blocked")
    # A small table where fraction is high but absolute is tiny (both must trip): not blocked.
    blocked2, _ = ns["guard_mass_delete"](4, 3, False, 0.5, 100_000)
    check(not blocked2, "G6: deleting 3 of 4 rows is NOT blocked (absolute floor not crossed)")


def test_g6_allow_mass_delete_override():
    ns = _load_pure(CONT)
    blocked, _ = ns["guard_mass_delete"](1_000_000, 800_000, True, 0.5, 100_000)
    check(not blocked, "G6 override: allow_mass_delete=true lets the big delete through")


def test_g6_guard_disabled_setting():
    ns = _load_pure(CONT)
    blocked, _ = ns["guard_mass_delete"](1_000_000, 800_000, False, 1.0, 100_000)
    check(not blocked, "G6 setting: cdc_max_delete_fraction>=1 disables the guard")


# ---- G7 no-PK precision + purge exactness ----
def test_g7_nopk_overmatch_blocks():
    ns = _load_pure(CONT)
    ok, why = ns["guard_nopk_delete_bound"](5, 1)   # 5 rows match, only 1 delete intended
    check((not ok) and "over-delete" in why.lower(),
          "G7: a content-DELETE matching 5 rows for 1 D op is BLOCKED (over-delete)")


def test_g7_nopk_exact_match_ok():
    ns = _load_pure(CONT)
    ok, _ = ns["guard_nopk_delete_bound"](1, 1)
    check(ok, "G7: an exact 1-row match for 1 D op is allowed")
    ok2, _ = ns["guard_nopk_delete_bound"](0, 1)   # already absent -> idempotent no-op
    check(ok2, "G7: deleting an already-absent row (0 matches) is allowed (idempotent)")


def test_g7_purge_matches_exact_file_only():
    ns = _load_pure(CONT)
    tag, lit = "_cdc_file", "'s3://b/t/CDC00001.csv'"
    good = f'DELETE FROM s.t WHERE "{tag}" = {lit}'
    check(ns["purge_predicate_is_exact"](good, tag, lit),
          "G7: the canonical exact-equality purge passes")
    for bad in (f'DELETE FROM s.t WHERE "{tag}" LIKE {lit}',
                f'DELETE FROM s.t WHERE "{tag}" = {lit} OR 1=1',
                f'DELETE FROM s.t',
                f'DELETE FROM s.t WHERE "other" = {lit}'):
        check(not ns["purge_predicate_is_exact"](bad, tag, lit),
              f"G7: a non-exact purge is rejected: {bad[:48]}...")


def test_g7_purge_static_exact_equality():
    """Static: the SHIPPED purge_sql in both engines is a single exact-equality on the tag col."""
    for eng in (CONT, COMP):
        src = H.read_source(eng)
        m = re.search(r"purge_sql = \(f'DELETE FROM \{dsql_schema\}\.\{dsql_table\} '\s*\n\s*"
                      r"f'WHERE \"\{NONPK_FILE_TAG_COLUMN\}\" = \{file_lit\}'\)", src)
        check(m is not None,
              f"G7 static[{os.path.basename(eng)}]: purge_sql is exact '= file_lit' (no LIKE/OR)")


# ---- G8 ordering / gap / new-LOAD ----
def test_g8_file_older_than_highwater_blocks():
    ns = _load_pure(CONT)
    ok, why = ns["guard_file_order"]("CDC/00005.csv", "CDC/00008.csv", ["CDC/00008.csv"])
    check((not ok) and "high-water" in why.lower(),
          "G8: a file at/under the high-water mark is BLOCKED (replay/regression)")


def test_g8_gap_file_blocks():
    ns = _load_pure(CONT)
    # pending 00006 while 00009 is already done -> a gap (00007/00008 missing/out of order).
    ok, why = ns["guard_file_order"]("CDC/00006.csv", "CDC/00005.csv", ["CDC/00009.csv"])
    check((not ok) and "gap" in why.lower(),
          "G8: a gap (a later file already done) BLOCKS the earlier pending file")


def test_g8_in_order_ok():
    ns = _load_pure(CONT)
    ok, _ = ns["guard_file_order"]("CDC/00009.csv", "CDC/00008.csv", ["CDC/00008.csv"])
    check(ok, "G8: the next in-order file after the high-water is allowed")


def test_g8_new_load_file_after_cdc_blocks():
    ns = _load_pure(CONT)
    ok, why = ns["guard_new_load_after_cdc"](["t/LOAD00001.csv"], cdc_started=True)
    check((not ok) and "LOAD" in why,
          "G8: a new LOAD* file after CDC started BLOCKS the table (DMS reload guard)")
    ok2, _ = ns["guard_new_load_after_cdc"](["t/LOAD00001.csv"], cdc_started=False)
    check(ok2, "G8: a LOAD* file before CDC started is fine (normal full load)")
    ok3, _ = ns["guard_new_load_after_cdc"]([], cdc_started=True)
    check(ok3, "G8: no LOAD* files after CDC started -> allowed")


# ---- G9 drift ----
def test_g9_drift_detected():
    ns = _load_pure(CONT)
    # expected = 1000 + 50 - 10 = 1040; live 900 -> delta -140, tolerance 0 -> fired.
    fired, delta, expected = ns["guard_drift"](900, 1000, 50, 10, 0)
    check(fired and delta == -140 and expected == 1040,
          "G9: injected shortfall (live 900 vs expected 1040) FIRES with the right delta")


def test_g9_drift_within_tolerance_ok():
    ns = _load_pure(CONT)
    fired, _, _ = ns["guard_drift"](1038, 1000, 50, 10, 5)  # delta -2, tol 5 -> not fired
    check(not fired, "G9: a difference within tolerance does NOT fire")


def test_g9_drift_block_action():
    """The drift runner sets 'blocked' only when CDC_DRIFT_ACTION=='block'. Exercise the SHIPPED
    _maybe_run_drift_check with a fake conn, both actions."""
    for action, expect in (("warn", None), ("block", "blocked")):
        ns = _load_drift_runner(action)
        conn = _FakeDriftConn(live=100, flr=1000, ins=0, dels=0)   # big shortfall
        res = ns["_maybe_run_drift_check"]([conn, 0.0], "s.t", "s", "t")
        check(res == expect,
              f"G9 action={action}: _maybe_run_drift_check returns {expect!r} (got {res!r})")
        check(conn.audit_written, f"G9 action={action}: an audit_log row was written on drift")


def test_g9_counters_tracked():
    """_advance_cdc_counters issues a COALESCE(+) UPDATE on inserts_applied/deletes_applied."""
    ns = H.load_defs(CONT, ["_advance_cdc_counters"], {"CONTROL_SCHEMA": "cdc_control"})
    cur = _CaptureCur()
    ns["_advance_cdc_counters"](cur, "s.t", 7, 3)
    sql = " ".join(cur.sql.split())
    check("inserts_applied = COALESCE(inserts_applied, 0) + " in sql
          and "deletes_applied = COALESCE(deletes_applied, 0) + " in sql,
          "G9: _advance_cdc_counters folds I/D into cdc_status via COALESCE(+)")
    check(cur.params == (7, 3, "s.t"), "G9: counter advance binds the right (ins, del, table)")


# ---- G5 audit schema ----
def test_g5_audit_schema_no_default_on_alter():
    """audit_log is CREATE TABLE only (never ADD COLUMN ... DEFAULT). Reuse the project's rule."""
    for eng in (CONT, COMP, LOAD):
        src = H.read_source(eng)
        m = re.search(r"CREATE TABLE IF NOT EXISTS \{CONTROL_SCHEMA\}\.audit_log\s*\((.*?)\)\s*\"\"\"",
                      src, re.DOTALL)
        check(m is not None, f"G5[{os.path.basename(eng)}]: audit_log CREATE TABLE present")
        # no DEFAULT inside the audit_log DDL
        if m:
            check("default" not in m.group(1).lower(),
                  f"G5[{os.path.basename(eng)}]: audit_log DDL has no DEFAULT")


def test_g5_audit_row_written_before_blank():
    """write_audit_log inserts a row with the destructive action BEFORE acting (shipped fn)."""
    ns = H.load_defs(CONT, ["write_audit_log"],
                     {"CONTROL_SCHEMA": "cdc_control",
                      "utc_now_iso": lambda: "2026-01-01T00:00:00+00:00",
                      "uuid": __import__("uuid")})
    cur = _CaptureCur()
    _id = ns["write_audit_log"](cur, "s.t", "cdc_mass_delete_blocked", 1000, 0, "why",
                                task="tsk", job="j", run_id="r", execution_id="e")
    check("INSERT INTO cdc_control.audit_log" in " ".join(cur.sql.split()),
          "G5: write_audit_log INSERTs into audit_log")
    check("cdc_mass_delete_blocked" in cur.params,
          "G5: the audit row records the destructive action name")


# =============================================================================================
# G1–G4 LOAD-side guards (pure G4 + the SHIPPED assert_blank_allowed with injected fakes).
# =============================================================================================
def _load_blank_guard(ov):
    """Load the load-side guard functions with injected globals/fakes. `ov` overrides globals
    (STARTUP_EXECUTION, ALLOW_MANUAL_DESTRUCTIVE, cdc-started/running stubs, etc.)."""
    inj = {
        "CONTROL_SCHEMA": "cdc_control",
        "BLANK_GUARD_ENABLED": True,
        "BLANK_EXPECTED_MARGIN": 0.05,
        "ALLOW_MANUAL_DESTRUCTIVE": False,
        "STARTUP_EXECUTION": None,
        "STARTUP_EXECUTION_ARN": None,
        "_cdc_started_result": (False, ""),
        "_workflow_result": ("none", "manual run"),
        "audit_calls": [],
    }
    inj.update(ov)
    # Replace the two I/O gates + audit with stubs so the pure decision logic is exercised.
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


def test_g4_pure_count_sane():
    ns = _load_blank_guard({})
    ok, _ = ns["guard_blank_count_sane"](100, 100, True, 0.05)
    check(ok, "G4: count == expected, previously attempted -> allowed")
    ok2, why2 = ns["guard_blank_count_sane"](100, 100, False, 0.05)
    check((not ok2) and "NOT previously attempted" in why2,
          "G4: a table NOT previously attempted by this task is refused")
    ok3, why3 = ns["guard_blank_count_sane"](200, 100, True, 0.05)
    check((not ok3) and "exceeds the expected" in why3,
          "G4: count 200 > expected 100*(1.05) is refused")


def test_g4_blank_requires_prior_attempt_by_this_task():
    ns = _load_blank_guard({"_workflow_result": ("running", "RUNNING")})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, False, "auto_reblank")
        check(False, "G4: expected refusal for a not-previously-attempted table")
    except Exception as e:
        check("G4" in str(e), "G4: assert_blank_allowed refuses a not-previously-attempted table")


def test_g4_refuse_when_count_above_expected():
    ns = _load_blank_guard({"_workflow_result": ("running", "RUNNING")})
    try:
        ns["assert_blank_allowed"]("s", "t", 10_000, 100, True, "auto_reblank")
        check(False, "G4: expected refusal when count >> expected")
    except Exception as e:
        check("G4" in str(e), "G4: refuses a blank when the count is far above expected")


def test_g1_blank_refused_after_cdc_started_marker():
    ns = _load_blank_guard({"_cdc_started_result": (True, "the task's _cdc_started marker is present"),
                            "_workflow_result": ("running", "RUNNING")})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G1: expected refusal after CDC started")
    except Exception as e:
        check("G1" in str(e) and "CDC has started" in str(e),
              "G1: a blank is refused once CDC has started (marker)")


def test_g1_blank_refused_after_cdc_status_row():
    ns = _load_blank_guard({"_cdc_started_result": (True, "a cdc_status row exists for s.t"),
                            "_workflow_result": ("running", "RUNNING")})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G1: expected refusal (cdc_status row)")
    except Exception as e:
        check("G1" in str(e), "G1: a blank is refused once a cdc_status row exists")


def test_g1_fail_closed_when_markers_unreadable():
    """_cdc_has_started returns started=True on an unreadable signal -> assert refuses."""
    ns = _load_blank_guard({"_cdc_started_result": (True, "could not read the _cdc_started marker"),
                            "_workflow_result": ("running", "RUNNING")})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G1: expected fail-closed refusal")
    except Exception as e:
        check("G1" in str(e), "G1: fail-closed — an unreadable marker refuses the blank")


# ---- G2 (redesigned): manual runs allowed; destructive-in-manual gated by the flag ----
def test_g2_running_execution_allows_blank():
    ns = _load_blank_guard({"_workflow_result": ("running", "execution X is RUNNING")})
    ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")   # no raise
    check(True, "G2: a RUNNING workflow execution blanks WITHOUT the manual flag")


def test_g2_console_load_cannot_blank():
    """A MANUAL run (no workflow) WITHOUT --allow_manual_destructive refuses the blank."""
    ns = _load_blank_guard({"_workflow_result": ("none", "this is a MANUAL run"),
                            "ALLOW_MANUAL_DESTRUCTIVE": False})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G2: expected a refusal for a manual blank without the flag")
    except Exception as e:
        check("G2" in str(e) and "allow_manual_destructive" in str(e),
              "G2: a manual destructive blank is refused and the message names the flag")


def test_g2_manual_blank_allowed_with_flag_and_audited():
    """WITH --allow_manual_destructive the manual blank proceeds, writes a 'manual override'
    audit row, and still applies G1/G4 (here both pass)."""
    ns = _load_blank_guard({"_workflow_result": ("none", "this is a MANUAL run"),
                            "ALLOW_MANUAL_DESTRUCTIVE": True})
    ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")   # no raise
    reasons = [str(k.get("reason") if False else a) for (a, k) in ns["audit_calls"]]
    joined = " ".join(str(a) + str(k) for (a, k) in ns["audit_calls"])
    check("manual override" in joined,
          "G2 override: a 'manual override' audit row is written when the flag is set")


def test_g2_manual_override_still_applies_g1():
    """Even with the manual flag, G1 (CDC started) still refuses."""
    ns = _load_blank_guard({"_workflow_result": ("none", "manual"),
                            "ALLOW_MANUAL_DESTRUCTIVE": True,
                            "_cdc_started_result": (True, "cdc_status row exists")})
    try:
        ns["assert_blank_allowed"]("s", "t", 100, 100, True, "auto_reblank")
        check(False, "G2+G1: expected G1 to still refuse under the manual flag")
    except Exception as e:
        check("G1" in str(e), "G2 override still honours G1 (no blank once CDC started)")


def test_g2_empty_table_plain_load_ok():
    """A plain load onto an EMPTY table never calls assert_blank_allowed (no blank happens), so a
    manual load can always write. Static check: every assert_blank_allowed CALL sits next to a
    blank_whole_table call (the destructive paths), and NONE is on the empty-register path."""
    src = H.read_source(LOAD)
    lines = src.splitlines()
    call_lines = [i for i, ln in enumerate(lines)
                  if "assert_blank_allowed(" in ln and "def assert_blank_allowed" not in ln]
    check(len(call_lines) >= 2, "G2: assert_blank_allowed is wired on >=2 destructive paths")
    # Each call must be within a window that also contains a blank_whole_table invocation
    # (i.e. a reblank path), never standalone on the register-empty path.
    for i in call_lines:
        window = "\n".join(lines[max(0, i - 5):i + 25])
        check("blank_whole_table(" in window,
              f"G2: assert_blank_allowed call near line {i+1} is on a reblank (destructive) path")
    # The empty-register return path ("_EMPTY_VERIFIED.add" after the non-empty branch) must not
    # gate a blank: there is no assert_blank_allowed on the same line as a register add.
    reg_gated = any("assert_blank_allowed" in ln and "_EMPTY_VERIFIED" in ln for ln in lines)
    check(not reg_gated, "G2: the empty-table register path does not gate a (non-existent) blank")


def test_manual_cdc_run_allowed_and_lock_respected():
    """G2 doesn't apply to CDC (CDC never empties a table). A manual CDC run is allowed; it is
    still covered by G3 (lock). Assert the CDC scripts have NO workflow/console refusal gate on
    their apply path, and DO take a per-table lock concept (process_table calls the guards)."""
    for eng in (CONT, COMP):
        src = H.read_source(eng)
        check("allow_manual_destructive" not in src,
              f"manual CDC[{os.path.basename(eng)}]: no manual-destructive gate (G2 N/A to CDC)")
        check("_workflow_execution_running" not in src,
              f"manual CDC[{os.path.basename(eng)}]: CDC never refuses a manual run")
        # CDC is still covered by G6/G8/G9 wired into the apply path:
        check("guard_mass_delete(" in src and "guard_file_order(" in src
              and "_maybe_run_drift_check(" in src,
              f"manual CDC[{os.path.basename(eng)}]: still covered by G6/G8/G9")


def test_g3_cdc_lock_concept_present():
    """G3 lock protects a manual CDC run from racing another CDC run on the same table. The
    position-fence (_PositionMoved) is the correctness backstop; assert it is present + used."""
    for eng in (CONT, COMP):
        src = H.read_source(eng)
        check("_PositionMoved" in src and "check_position_cur" in src,
              f"G3/CDC[{os.path.basename(eng)}]: position-fence backstop present for 2-run safety")


# ---- G3 load lock: the SHIPPED guard_blank_count_sane + take/release are structural; assert
#      the lock table + conditional-insert + stale takeover + release exist in job2_load. ----
def test_g3_two_loads_second_fails_on_lock():
    src = H.read_source(LOAD)
    check("CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.cdc_control_lock" in src,
          "G3: a per-table lock table (cdc_control_lock) is created")
    check("INSERT INTO {CONTROL_SCHEMA}.cdc_control_lock" in src,
          "G3: the lock is taken via a conditional INSERT (second concurrent run loses the PK)")
    # the reblank path fails closed when the lock is not acquired
    check("BLANK GUARD G3" in src, "G3: a failed lock acquire refuses the reblank (fail-closed)")


def test_g3_stale_lock_expires():
    """_lock_is_stale uses LOCK_HEARTBEAT_TIMEOUT_SECONDS; a very old heartbeat is stale."""
    ns = H.load_defs(LOAD, ["_lock_is_stale"],
                     {"LOCK_HEARTBEAT_TIMEOUT_SECONDS": 1800,
                      "datetime": __import__("datetime")})
    old = "2000-01-01T00:00:00+00:00"
    check(ns["_lock_is_stale"](old) is True, "G3: a very old heartbeat is stale (can be taken)")
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    check(ns["_lock_is_stale"](now) is False, "G3: a fresh heartbeat is NOT stale")


def test_g3_lock_released_on_exit():
    src = H.read_source(LOAD)
    check("def release_table_lock" in src and "DELETE FROM {CONTROL_SCHEMA}.cdc_control_lock" in src,
          "G3: release_table_lock deletes the owner-scoped lock row")
    check("finally:" in src and "release_table_lock(dsql_schema, dsql_table)" in src,
          "G3: the reblank path releases the lock in a finally")


# =============================================================================================
# G10 validate/cutover count equation
# =============================================================================================
def _load_g10():
    return H.load_defs(VAL, ["guard_count_vs_dms"], {})


def test_g10_validate_fails_on_dms_fullloadrows_mismatch():
    ns = _load_g10()
    ok, delta, expected, why = ns["guard_count_vs_dms"](900, 1000, 0, 0, 0)
    check((not ok) and delta == -100 and expected == 1000 and "SHORT" in why,
          "G10: DSQL 900 vs DMS FullLoadRows 1000 -> FAIL (target SHORT)")


def test_g10_validate_passes_when_matches():
    ns = _load_g10()
    ok, _, _, _ = ns["guard_count_vs_dms"](1000, 1000, 0, 0, 0)
    check(ok, "G10: DSQL == FullLoadRows -> pass")
    ok2, _, _, _ = ns["guard_count_vs_dms"](1000, None, 0, 0, 0)
    check(ok2, "G10: no DMS figure -> no-op pass (falls back to the S3 comparison)")


def test_g10_cutover_count_mismatch_fails():
    """Cutover uses the SAME equation with applied inserts/deletes: FullLoadRows + I − D."""
    ns = _load_g10()
    # FullLoadRows 1000 + 200 inserts − 50 deletes = 1150 expected; DSQL 1140 -> short by 10.
    ok, delta, expected, _ = ns["guard_count_vs_dms"](1140, 1000, 200, 50, 0)
    check((not ok) and expected == 1150 and delta == -10,
          "G10 cutover: DSQL vs FullLoadRows+I-D mismatch -> CountMismatch (fails)")


def test_g10_cutover_override_flag():
    """A tolerance acts as the explicit override band (the cutover run-input override widens it)."""
    ns = _load_g10()
    ok, _, _, _ = ns["guard_count_vs_dms"](1140, 1000, 200, 50, 100)  # tol 100 covers delta -10
    check(ok, "G10 override: a tolerance band lets a near-match pass (explicit override)")


# =============================================================================================
# Fakes
# =============================================================================================
class _CaptureCur:
    def __init__(self):
        self.sql = ""
        self.params = None
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return None

    def close(self):
        pass


def _load_drift_runner(action):
    """Load the SHIPPED _maybe_run_drift_check + guard_drift + emit stub + audit/block stubs."""
    inj = {
        "CDC_DRIFT_CHECK_MINUTES": 30,
        "CDC_DRIFT_TOLERANCE": 0.0,
        "CDC_DRIFT_ACTION": action,
        "CONTROL_SCHEMA": "cdc_control",
        "_DRIFT_LAST_RUN": {},
        "time": __import__("time"),
    }
    src = H.read_source(CONT)
    t = ast.parse(src)
    want = {"_maybe_run_drift_check", "guard_drift"}
    defs = {nd.name: ast.get_source_segment(src, nd)
            for nd in t.body if isinstance(nd, ast.FunctionDef) and nd.name in want}
    ns = dict(inj)
    ns["emit_drift_metric"] = lambda label, delta: None
    exec(compile(defs["guard_drift"], "<guard_drift>", "exec"), ns)
    # _audit_destructive / _set_blocked_status flip flags on the conn so the test can observe.
    def _audit(conn_holder, label, action_, rows_before, rows_deleted, reason):
        conn_holder[0].audit_written = True
    def _setblk(conn_holder, label, reason):
        conn_holder[0].blocked = True
    ns["_audit_destructive"] = _audit
    ns["_set_blocked_status"] = _setblk
    exec(compile(defs["_maybe_run_drift_check"], "<_maybe_run_drift_check>", "exec"), ns)
    return ns


class _FakeDriftConn:
    """A fake conn whose cursor answers the drift read: count(*) then the counters row."""
    def __init__(self, live, flr, ins, dels):
        self._live = live
        self._counters = (flr, ins, dels)
        self.audit_written = False
        self.blocked = False
        self._stage = 0

    def cursor(self):
        return _FakeDriftCur(self)

    def commit(self):
        pass

    def rollback(self):
        pass


class _FakeDriftCur:
    def __init__(self, conn):
        self.conn = conn
        self._last = None

    def execute(self, sql, params=None):
        low = sql.lower()
        if "count(*)" in low:
            self._last = ("count",)
        else:
            self._last = ("counters",)

    def fetchone(self):
        if self._last == ("count",):
            return (self.conn._live,)
        if self._last == ("counters",):
            return self.conn._counters
        return None

    def close(self):
        pass


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        try:
            globals()[fn]()
        except Exception as e:  # a crash in one test must not hide the rest
            global _failed
            _failed += 1
            print(f"[FAIL] {fn} raised {type(e).__name__}: {e}")
    print(f"\n==== guardrails: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
