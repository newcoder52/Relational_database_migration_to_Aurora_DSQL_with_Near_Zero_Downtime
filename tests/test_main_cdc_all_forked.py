#!/usr/bin/env python3
"""Regression: the MAIN CDC job must NOT crash when every table it sees is owned by another
CDC job (a bg-/ck- fork or the multi-column-key job).

The bug
-------
`scripts/glue_cdc_continuous.py` `main()` raised

    Exception("No usable tables from the manifest.")

whenever `not contexts and not multi_key`, IGNORING `not_owned`. So the MAIN run of a task
whose tables are ALL owned by fork jobs crashed on startup. Concrete case: a task with ONE
big table — a partitions-auto source writes many LOAD files, the table is classified "big",
and its bg fork owns it. In the startup state machine (stepfunctions/startup.asl.json) the
MAIN CDC run must reach its poll loop and write its `_cdc_started` marker (which
`CheckCdcStarted` polls) BEFORE `StartForkCdcMap` launches the forks. A crash here means:
  - `IsCdcRunAlive` -> `CdcRunFailed`, or the marker is never written -> `CdcStartNotConfirmed`;
  - `StartForkCdcMap` never runs, so the fork that OWNS the only table never starts;
  - the task ends up with NO CDC at all.

The fix
-------
Raise ONLY when the manifest truly yields nothing usable:
    not contexts  and  not multi_key  and  not not_owned
Otherwise (every table owned by another job, and/or multi-column-key tables) the MAIN run
takes the SAME idle path the all-multi-key case already used: write the start marker, enter
the poll loop with an empty `contexts` list, and stay idle until cutover stops it.

What this test does
-------------------
It executes the REAL, current `main()` source (extracted from the script by AST so the test
tracks whatever the file actually contains) in a namespace where every collaborator main()
calls is stubbed offline — no AWS, no DSQL, no Spark, no network. `wait_for_wake` is stubbed
to break the otherwise-infinite poll loop after a couple of iterations (simulating a SIGTERM /
BatchStopJobRun from cutover), and `write_started_marker` is stubbed to record that it ran.

Scenarios:
  1. ALL-FORKED  — the only table is owned by `bg-bigtbl` -> main must NOT raise, must write
                   its start marker, and must idle (reach the poll loop with 0 contexts).
  2. EMPTY       — manifest/ownership yield nothing at all (no owned, no multi-key, no
                   not-owned) -> main MUST still raise "No usable tables from the manifest."
  3. NORMAL      — main owns its one table -> unchanged: no raise, marker written, loop runs
                   with that table in `contexts`.

Run directly:   python3 tests/test_main_cdc_all_forked.py
Under pytest:   a failed check() raises AssertionError (so `check()` must raise under pytest).
"""
import ast
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
SCRIPT = os.path.join(REPO, "scripts", "glue_cdc_continuous.py")

RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append(bool(cond))
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"   <- {str(extra)[:400]}" if (not cond and extra) else ""))
    # Under pytest, surface a failed check as a real failure.
    if not cond and "PYTEST_CURRENT_TEST" in os.environ:
        raise AssertionError(f"{name}" + (f": {extra}" if extra else ""))


# ---------------------------------------------------------------------------------------------
# Extract the REAL main() source from the script (so we exercise the shipped logic, not a copy).
# ---------------------------------------------------------------------------------------------
def _load_main_code():
    with open(SCRIPT, "r", encoding="utf-8") as f:
        src = f.read()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            seg = ast.get_source_segment(src, node)
            # Compile just the function def; executing it binds `main` into our namespace.
            return compile(seg, SCRIPT, "exec")
    raise AssertionError("could not find def main() in the script")


MAIN_CODE = _load_main_code()


class _StopLoop(Exception):
    """Raised by the stubbed wait_for_wake to end the otherwise-infinite idle poll loop,
    standing in for cutover's BatchStopJobRun / SIGTERM."""


class MultiColumnKeyTable(Exception):
    pass


def _run_main(entries, owners, owner_self="main", max_wakes=2):
    """Execute the real main() with offline stubs. Returns a dict describing what happened:
      - raised:            the Exception main() raised, or None
      - marker_written:    True iff write_started_marker() was called (idle/normal path)
      - reached_loop:      True iff the poll loop body ran at least once
      - contexts_in_loop:  number of contexts the loop iterated (0 == idle)
    """
    state = {"marker_written": False, "reached_loop": False, "contexts_in_loop": None,
             "wakes": 0}

    def write_started_marker():
        state["marker_written"] = True

    def wait_for_wake():
        # Called at the END of each poll cycle. Count cycles, then stop (as cutover would).
        state["wakes"] += 1
        if state["wakes"] >= max_wakes:
            raise _StopLoop()
        return True

    def process_table(ctx, load_status_map):
        # The loop only runs process_table when contexts is non-empty (NORMAL case). Record
        # that the loop actually iterated real tables and return a quiet (no-work) result.
        state["reached_loop"] = True
        return {"table": ctx["label"], "status": "ok", "files": 0, "rows": 0, "chunks": 0}

    def load_manifest():
        return list(entries)

    def _load_cdc_owners():
        return owners

    def build_table_context(e):
        label = f"{e.get('dsql_schema')}.{e.get('dsql_table')}"
        if e.get("_multi_key"):
            raise MultiColumnKeyTable(label + " (multi-column PK)")
        return {"label": label, "dms_table": e.get("dsql_table", "t")}

    def ddl_watcher(dms_tables):
        return None

    def load_full_load_status():
        return {}

    # A no-op threading.Thread so starting the DDL watcher does nothing offline.
    class _FakeThread:
        def __init__(self, *a, **k):
            pass

        def start(self):
            pass

    g = {
        "__builtins__": __builtins__,
        # ── stubbed collaborators ───────────────────────────────────────────────
        "ensure_control_tables": lambda: None,
        "load_manifest": load_manifest,
        "_load_cdc_owners": _load_cdc_owners,
        "build_table_context": build_table_context,
        "load_full_load_status": load_full_load_status,
        "ddl_watcher": ddl_watcher,
        "write_started_marker": write_started_marker,
        "wait_for_wake": wait_for_wake,
        "process_table": process_table,
        "MultiColumnKeyTable": MultiColumnKeyTable,
        # ── globals main() reads ────────────────────────────────────────────────
        "CDC_OWNER_SELF": owner_self,
        "REQUIRE_FULL_LOAD_DONE": False,   # skip the S3 gate diagnostic branch
        "RUN_FOREVER": True,
        "MAX_IDLE_HOURS": 4,
        "POLL_INTERVAL": 30,
        "DDL_WATCH_INTERVAL": 10,
        "MAX_PARALLEL_TABLES": 1,
        "MIN_FILE_AGE_SECONDS": 15,
        "CONFIG_PREFIX": "s3://b/config/_task/x/",
        "DSQL_ENDPOINT": "ep",
        "CONTROL_SCHEMA": "cdc_control",
        "DMS_TASK_ARN": "arn:task",
        # main()'s finally block sets _stop_event; give it a real Event.
        "_stop_event": __import__("threading").Event(),
        # ── modules main() uses ─────────────────────────────────────────────────
        "time": __import__("time"),
        "threading": types.SimpleNamespace(Thread=_FakeThread),
        "datetime": __import__("datetime").datetime,
        "timezone": __import__("datetime").timezone,
        "ThreadPoolExecutor": None,   # unused when MAX_PARALLEL_TABLES <= 1
        "as_completed": None,
    }

    exec(MAIN_CODE, g)             # binds main into g
    # Make time.sleep a no-op so the (stubbed) loop never actually waits.
    g["time"] = types.SimpleNamespace(sleep=lambda *_a, **_k: None,
                                      time=__import__("time").time,
                                      monotonic=__import__("time").monotonic)

    result = {"raised": None, **state}
    try:
        g["main"]()
    except _StopLoop:
        pass  # expected: idle/normal loop broken by our fake cutover stop
    except Exception as e:   # noqa: BLE001 — we want to capture main()'s own raise
        result["raised"] = e
    # copy final state
    result.update({k: state[k] for k in ("marker_written", "reached_loop", "contexts_in_loop",
                                          "wakes")})
    return result


# =============================================================================================
# Scenarios
# =============================================================================================
def test_all_forked():
    """ONE big table, owned by a bg fork. Main must idle, NOT raise, and write the marker."""
    entries = [{"dsql_schema": "s", "dsql_table": "bigtbl"}]
    owners = {"s.bigtbl": "bg-bigtbl"}
    r = _run_main(entries, owners, owner_self="main")
    check("all-forked: main does NOT raise", r["raised"] is None, repr(r["raised"]))
    check("all-forked: start marker written (CheckCdcStarted will see it)",
          r["marker_written"] is True)
    check("all-forked: enters poll loop and idles (no contexts applied)",
          r["reached_loop"] is False and r["wakes"] >= 1,
          f"reached_loop={r['reached_loop']} wakes={r['wakes']}")


def test_empty_still_raises():
    """Nothing usable at all: no owned, no multi-key, no not-owned -> still raise."""
    entries = []
    owners = {}
    r = _run_main(entries, owners, owner_self="main")
    ok = isinstance(r["raised"], Exception) and "No usable tables" in str(r["raised"])
    check("empty manifest: main STILL raises 'No usable tables from the manifest.'", ok,
          repr(r["raised"]))
    check("empty manifest: start marker NOT written (it crashed before the loop)",
          r["marker_written"] is False)


def test_normal_unchanged():
    """Main owns its one table: behaviour unchanged — no raise, marker written, loop runs it."""
    entries = [{"dsql_schema": "s", "dsql_table": "mine"}]
    owners = {"s.mine": "main"}
    r = _run_main(entries, owners, owner_self="main")
    check("normal: main does NOT raise", r["raised"] is None, repr(r["raised"]))
    check("normal: start marker written", r["marker_written"] is True)
    check("normal: poll loop processes the owned table", r["reached_loop"] is True)


def test_forked_plus_multikey():
    """Edge: no owned contexts, but a mix of not-owned AND multi-key tables -> idle, not raise."""
    entries = [{"dsql_schema": "s", "dsql_table": "bigtbl"},
               {"dsql_schema": "s", "dsql_table": "cktbl", "_multi_key": True}]
    owners = {"s.bigtbl": "bg-bigtbl"}  # cktbl -> owner "main" (default), then multi-key
    r = _run_main(entries, owners, owner_self="main")
    check("forked+multikey: main does NOT raise", r["raised"] is None, repr(r["raised"]))
    check("forked+multikey: start marker written", r["marker_written"] is True)
    check("forked+multikey: idles (no owned contexts applied)", r["reached_loop"] is False)


def main():
    test_all_forked()
    test_empty_still_raises()
    test_normal_unchanged()
    test_forked_plus_multikey()
    total = len(RESULTS)
    passed = sum(1 for x in RESULTS if x)
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)


if __name__ == "__main__":
    main()
