#!/usr/bin/env python3
"""G9 ROW DRIFT false-alarm regression — the drift expectation must include the full-load baseline.

ROOT CAUSE (reproduced against the SHIPPED code): the G9 drift check compares the live DSQL row
count to expected = full_load_rows + inserts_applied − deletes_applied. But NOTHING ever wrote
cdc_status.full_load_rows — the full-load row count lives only in _load_status.json ("rows"), and
_advance_cdc_counters only ever touched inserts_applied / deletes_applied. So full_load_rows stayed
NULL -> COALESCE(...,0) -> expected omitted the entire full load, and the FIRST drift check fired a
false "+<full_load_rows>" alarm. The live composite run logged
`ROW DRIFT [dms_sample_v8.name_data]: live 7,455 vs expected 500 ... delta +6,955` where 6,955 is
exactly the full-load row count recorded in that fork's _load_status.json. The SAME latent bug is
in the main engine (its log: `sport_type live 4,564 vs expected 501 ... delta +4,063`).

FIX (both engines): _read_load_status_doc harvests each table's "rows" into the module map
_FULL_LOAD_ROWS; process_table seeds cdc_status.full_load_rows from it ONCE (idempotent, NULL-only)
via _seed_full_load_rows_cur, so the expectation includes the baseline and persists.

This test uses the SHIPPED functions (fix6 harness style) over a fake S3 + fake DSQL cursor:
  1. _read_load_status_doc harvests "rows" into _FULL_LOAD_ROWS.
  2. _seed_full_load_rows_cur writes full_load_rows ONLY when NULL (idempotent).
  3. End-to-end: full load N, then 3 files of inserts/updates/deletes -> guard_drift NOT fired
     (updates must not move the counters; existing-row upserts count as inserts matching the row
     delta). Rows removed OUT OF BAND -> guard_drift STILL fires (real drift, the dangerous
     negative delta). Covered for composite AND main.

Run: python3 tests/test_drift_baseline.py   (REPO_DIR overridable)
"""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

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
    """get_object returns one in-memory _load_status.json body."""
    def __init__(self, body_bytes):
        self._body = body_bytes

    def get_object(self, Bucket=None, Key=None):
        class _B:
            def __init__(self, b):
                self._b = b

            def read(self):
                return self._b
        return {"Body": _B(self._body), "LastModified": "2026-10-06T00:00:00Z"}


class _FakeRow:
    """A fake cdc_status row the fake cursor reads/updates. live = fake DSQL count(*)."""
    def __init__(self, live):
        self.live = live
        self.full_load_rows = None    # starts NULL, exactly like a fresh ck/main fork
        self.inserts_applied = 0
        self.deletes_applied = 0
        self.blocked = False


class _FakeDriftConn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        return _FakeDriftCur(self.row)

    def commit(self):
        pass

    def rollback(self):
        pass


class _FakeDriftCur:
    """Answers the SHIPPED SQL by shape: count(*), the counters SELECT, and the two UPDATEs
    (_advance_cdc_counters / _seed_full_load_rows_cur). It honours the `full_load_rows IS NULL`
    guard so the idempotency of the seed is actually exercised."""
    def __init__(self, row):
        self.row = row
        self._last = None

    def execute(self, sql, params=None):
        low = " ".join(sql.lower().split())
        p = params or ()
        if "count(*)" in low:
            self._last = ("count",)
        elif low.startswith("select coalesce(full_load_rows"):
            self._last = ("counters",)
        elif "set full_load_rows" in low:
            # _seed_full_load_rows_cur: ... SET full_load_rows=%s WHERE table_name=%s AND
            # full_load_rows IS NULL  -> only applies when currently NULL (idempotent).
            if self.row.full_load_rows is None:
                self.row.full_load_rows = int(p[0])
            self._last = ("update",)
        elif "set inserts_applied" in low:
            self.row.inserts_applied += int(p[0])
            self.row.deletes_applied += int(p[1])
            self._last = ("update",)
        else:
            self._last = ("other",)

    def fetchone(self):
        if self._last == ("count",):
            return (self.row.live,)
        if self._last == ("counters",):
            return (self.row.full_load_rows or 0, self.row.inserts_applied,
                    self.row.deletes_applied)
        return None

    def close(self):
        pass


# ---------------------------------------------------------------------------------------------
# Load the SHIPPED functions for one engine into a fake-seeded namespace.
# ---------------------------------------------------------------------------------------------
def _load_engine(script_rel, s3, full_load_rows_map, drift_action="warn"):
    src = H.read_source(script_rel)
    tree = ast.parse(src)
    want = {"_read_load_status_doc", "_advance_cdc_counters", "_seed_full_load_rows_cur",
            "guard_drift", "_maybe_run_drift_check"}
    defs = {nd.name: ast.get_source_segment(src, nd)
            for nd in tree.body if isinstance(nd, ast.FunctionDef) and nd.name in want}
    missing = want - set(defs)
    if missing:
        # The fix is not present (e.g. _seed_full_load_rows_cur missing) — surface it as a clean
        # failed check rather than crashing the whole run.
        return None, missing

    ns = {
        "s3": s3,
        "json": __import__("json"),
        "time": __import__("time"),
        "CONTROL_SCHEMA": "cdc_control_test",
        "CDC_DRIFT_CHECK_MINUTES": 30,
        "CDC_DRIFT_TOLERANCE": 0.0,
        "CDC_DRIFT_ACTION": drift_action,
        "_DRIFT_LAST_RUN": {},
        "_FULL_LOAD_ROWS": full_load_rows_map,   # the SHIPPED module map (same object)
        "emit_drift_metric": lambda label, delta: None,
        "_audit_destructive": lambda *a, **k: None,
        "_set_blocked_status": lambda ch, label, reason: setattr(ch[0].row, "blocked", True),
        "print": lambda *a, **k: None,
    }
    for name in ("guard_drift", "_read_load_status_doc", "_advance_cdc_counters",
                 "_seed_full_load_rows_cur", "_maybe_run_drift_check"):
        exec(compile(defs[name], f"<{script_rel}:{name}>", "exec"), ns)
    return ns, set()


# ---------------------------------------------------------------------------------------------
# The scenario
# ---------------------------------------------------------------------------------------------
def _run_engine(engine, script_rel):
    import json
    label = "dms_sample.name_data"
    N = 6955   # full-load baseline (the number that was being lost -> false +6955 drift)

    # _load_status.json exactly as Job 2 writes it (status + rows).
    body = json.dumps({"tables": {label: {"status": "done", "rows": N}}}).encode("utf-8")
    s3 = _FakeS3(body)
    flr_map = {}   # the module's _FULL_LOAD_ROWS, starts empty

    ns, missing = _load_engine(script_rel, s3, flr_map)
    if missing:
        check(False, f"{engine}: drift-baseline fix present (missing {sorted(missing)})")
        return

    # 1) Harvest the baseline from _load_status.json (SHIPPED _read_load_status_doc).
    _, status_map = ns["_read_load_status_doc"]("b", "k")
    check(status_map.get(label) == "done", f"{engine}: load status parsed (status=done)")
    check(flr_map.get(label) == N,
          f"{engine}: full-load rows harvested into _FULL_LOAD_ROWS ({flr_map.get(label)} == {N})")

    # 2) Seed cdc_status.full_load_rows (idempotent). Model the live target starting at N.
    row = _FakeRow(live=N)
    ch = [_FakeDriftConn(row)]

    def _ensure(c):
        ns["_seed_full_load_rows_cur"](c, label)
    _ensure(ch[0].cursor())
    check(row.full_load_rows == N, f"{engine}: seed wrote full_load_rows={N}")
    # idempotent: a second seed (e.g. if _FULL_LOAD_ROWS changed) must NOT clobber the value
    flr_map[label] = 999999
    _ensure(ch[0].cursor())
    check(row.full_load_rows == N, f"{engine}: seed is idempotent (NULL-only; not clobbered)")
    flr_map[label] = N

    # 3) Apply 3 CDC files of mixed DML. Counters move ONLY for net inserts/deletes; an UPDATE
    #    (and an upsert of a row that already exists) is net-zero on row count, so it must NOT
    #    advance the counters. We feed each file's (net_insert, net_delete) just as the apply fn
    #    computes it, and move the live target by the same net so a correct pipeline has 0 drift.
    #    file1: +500 insert, 0 update(s) (net 0), 0 delete
    #    file2: +300 insert, 120 updates (net 0), -50 delete
    #    file3: +200 insert,  80 updates (net 0), -30 delete
    files = [(500, 0), (300, 50), (200, 30)]
    for n_ins, n_del in files:
        ns["_advance_cdc_counters"](ch[0].cursor(), label, n_ins, n_del)
        row.live += (n_ins - n_del)   # the real target row count after this file

    exp_ins = sum(f[0] for f in files)
    exp_del = sum(f[1] for f in files)
    check(row.inserts_applied == exp_ins and row.deletes_applied == exp_del,
          f"{engine}: counters track inserts/deletes only "
          f"(ins={row.inserts_applied}, del={row.deletes_applied}); updates net-zero")

    # 4) Drift check on a CORRECT pipeline -> NO false alarm.
    res = ns["_maybe_run_drift_check"](ch, label, "dms_sample", "name_data")
    fired, delta, expected = ns["guard_drift"](row.live, row.full_load_rows,
                                               row.inserts_applied, row.deletes_applied, 0.0)
    check(expected == N + exp_ins - exp_del,
          f"{engine}: expected includes the full-load baseline "
          f"({expected} == {N}+{exp_ins}-{exp_del})")
    check(not fired and delta == 0,
          f"{engine}: NO false drift after full load + inserts/updates/deletes (delta={delta})")
    check(res is None and not row.blocked,
          f"{engine}: drift check quiet on a healthy table (not blocked)")

    # 5) REAL drift: 100 rows vanish out of band (someone truncated/deleted directly). The live
    #    count drops below expected -> drift MUST fire (negative delta = the dangerous direction).
    row.live -= 100
    fired2, delta2, _ = ns["guard_drift"](row.live, row.full_load_rows,
                                          row.inserts_applied, row.deletes_applied, 0.0)
    check(fired2 and delta2 == -100,
          f"{engine}: REAL drift still fires when rows are removed out of band (delta={delta2})")

    # 6) Also prove the OLD behaviour would have been a false alarm: with full_load_rows unseeded
    #    (NULL->0) the healthy table reports a huge positive delta == the baseline. This is what
    #    the bug produced; the fix removes it.
    fired_bug, delta_bug, _ = ns["guard_drift"](N + exp_ins - exp_del, 0, exp_ins, exp_del, 0.0)
    check(fired_bug and delta_bug == N,
          f"{engine}: unseeded baseline reproduces the false +{N} alarm (delta={delta_bug})")


def test_composite_drift_baseline():
    _run_engine("composite", COMP)


def test_main_drift_baseline():
    _run_engine("main", CONT)


if __name__ == "__main__":
    for _fn in (test_composite_drift_baseline, test_main_drift_baseline):
        try:
            _fn()
        except Exception as _e:  # a crash in one must not hide the rest
            _failed += 1
            print(f"[FAIL] {_fn.__name__} raised {type(_e).__name__}: {_e}")
    print(f"\n{_passed} passed, {_failed} failed")
    sys.exit(1 if _failed else 0)
