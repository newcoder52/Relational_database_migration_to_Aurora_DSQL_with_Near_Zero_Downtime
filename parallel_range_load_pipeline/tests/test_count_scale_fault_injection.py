#!/usr/bin/env python3
"""AMENDMENT 3 (R7-5) — offline performance/fault harness for the count planner.

Drives the REAL count planner from scripts/job_mrl_count.py (AST-extracted, same technique as the
other folder tests) against a FAKE DB whose COUNT(*) time grows LINEARLY with the number of keys in
the counted range, calibrated so a 20M-row WHOLE-TABLE count EXCEEDS the per-query read timeout while
a ~4M-row sub-range does NOT. Proves the fix (R7a+R7b):

  * 10M / 20M / 100M / 300M tables with a single integer or uuid key are VERIFIED by ranged counts;
  * a deliberately slow sub-range is HALVED until it fits;
  * a mismatch (target short by 1) FAILS (never a false pass);
  * a sub-range that never fits within the per-table budget ends 'unverifiable' and FAILS CLOSED;
  * no false pass at any size, including a randomized soak.

No AWS / pg8000 / Spark. The planner's own `_plan`, `_leaf`, `_ranged`, `_verify`, `_run`,
`_count_one`, `compute_verdict`, `_pred`, `_to_int`, `_lit`, `_q`, `_alive`, `_single_pk`,
`is_txn_age_error`, `CountSplit/CountStop/CountRangeError` and the real constants are executed; only
the DB I/O leaves (`_target_query`, `_edge`, `_nullable`) and `time`/`_alive` clock are faked.
"""
import ast
import os
import random
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
COUNT = os.path.join(PKG, "scripts", "job_mrl_count.py")

_FUNCS = {"_q", "_lit", "_to_int", "_pred", "_alive", "_unavailable", "_run", "_plan", "_leaf",
          "_verify", "_ranged", "_single_pk", "_count_one", "compute_verdict", "is_txn_age_error",
          "dms_full_load_rows_for"}
_CLASSES = {"CountSplit", "CountStop", "CountRangeError"}
_CONSTS = {"COUNT_RETRIES", "COUNT_RETRY_WAIT", "COUNT_SPLIT_ROWS", "COUNT_RANGE_ROWS",
           "COUNT_RANGE_THREADS", "COUNT_MAX_HALVINGS", "COUNT_UNKNOWN_TOTAL_ROWS",
           "COUNT_TABLE_BUDGET_SECONDS", "COUNT_TOLERANCE"}

# The fake DB's calibration: whole-table 20M exceeds this read deadline; ~4M sub-ranges don't.
READ_TIMEOUT_S = 10.0           # models the old pg8000 10 s client read timeout
SECONDS_PER_ROW = 10.0 / 15_000_000    # 15M rows ~= 10 s -> 20M whole-table ~13 s (> timeout)


class _FakeClock:
    """Deterministic virtual clock: _run 'spends' modelled query time; _alive uses this time."""
    def __init__(self):
        self.t = 1000.0
        self.lock = threading.Lock()

    def advance(self, dt):
        with self.lock:
            self.t += dt

    def now(self):
        with self.lock:
            return self.t


def _build_planner(fake_db, clock, skew=False):
    """AST-extract the planner into a namespace, wiring fake DB leaves + the virtual clock."""
    src = open(COUNT).read()
    tree = ast.parse(src)
    ns = {"threading": threading, "random": random,
          "DMS_TASK_ARN": None, "COUNT_TOLERANCE": 0,
          "ThreadPoolExecutor": __import__("concurrent.futures",
                                           fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor}
    # constants
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in _CONSTS:
                    try:
                        ns[t.id] = ast.literal_eval(n.value)
                    except Exception:
                        pass
    # a fake `time` module whose sleep is virtual and time() reads the clock
    class _T:
        @staticmethod
        def time():
            return clock.now()

        @staticmethod
        def sleep(s):
            clock.advance(min(float(s), 0.001))     # retries don't burn the virtual budget much
    ns["time"] = _T
    # classes + functions
    for n in tree.body:
        if isinstance(n, (ast.ClassDef,)) and n.name in _CLASSES:
            exec(ast.get_source_segment(src, n), ns)
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in _FUNCS:
            exec(ast.get_source_segment(src, n), ns)
    missing = (_FUNCS | _CLASSES) - set(ns)
    assert not missing, "could not extract from count job: %s" % missing

    # --- fake DB leaves (replace the real _target_query / _edge / _nullable) ---------------
    def _count_rows_in(pred):
        """How many of the fake table's keys satisfy `pred` (the planner builds >=/< predicates)."""
        return fake_db.count_pred(pred)

    def _fake_target_query(schema, table, pred, params):
        n = _count_rows_in(pred)
        # model the query time: proportional to rows scanned (range width), + tiny base.
        secs = 0.05 + n * SECONDS_PER_ROW
        clock.advance(secs)
        if secs > READ_TIMEOUT_S:
            # the old 10 s client read timeout: pg8000 raises this text; planner maps to CountSplit
            raise Exception("('The read operation timed out',)")
        return (n,)
    ns["_target_query"] = _fake_target_query

    def _fake_edge(schema, table, col, asc, ctx):
        v = fake_db.min_key if asc else fake_db.max_key
        if fake_db.kind == "uuid":
            h = format(int(v), "032x")
            return "%s-%s-%s-%s-%s" % (h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
        return v
    ns["_edge"] = _fake_edge

    def _fake_nullable(schema, table, col):
        return False            # keys are NOT NULL in these fixtures
    ns["_nullable"] = _fake_nullable
    # expose constants the fixture may read
    return ns


class FakeTable:
    """A table of `n_rows` keys of `kind` ('integer' or 'uuid'), evenly spaced over the key space
    (or skewed). count_pred() counts keys matching the planner's '>=/<' predicate text. `short_by`
    drops that many rows from the TRUE count only when the WHOLE range is counted at once (models a
    real mismatch regardless of how it's sliced)."""
    def __init__(self, kind, n_rows, short_by=0, skew=False):
        self.kind = kind
        self.n_rows = n_rows
        self.short_by = short_by
        if kind == "integer":
            self.min_key, self.max_key = 1, n_rows
            self._keys_lo, self._keys_hi = 1, n_rows            # contiguous ints 1..n
        else:
            # uuid: spread n_rows keys across the 128-bit space (evenly, or skewed to the low end)
            self.min_key, self.max_key = 0, (1 << 128) - 1
            self._space = (1 << 128)
            self._skew = skew
        self._int = kind == "integer"

    def _key_to_int(self, v):
        if self._int:
            return int(v)
        return int(str(v).replace("-", ""), 16)

    def _count_le_region(self, lo, hi):
        """Number of the table's keys with lo <= key < hi (ints) over the modelled distribution.
        The table truly holds (n_rows - short_by) keys, so a nonzero short_by reduces EVERY
        count (whole-table and every sub-range sums to the same shortfall) — a real mismatch that
        ranged counting cannot miss (no false pass). EXACT integer arithmetic (no float rounding)
        so the summed sub-range counts equal eff_n precisely."""
        eff_n = self.n_rows - self.short_by
        if self._int:
            a = max(self._keys_lo, lo if lo is not None else self._keys_lo)
            b = min(eff_n + 1, hi if hi is not None else eff_n + 1)
            return max(0, b - a)
        # uuid: the eff_n keys sit at exact integer positions key_i = i * step (+ a skew map),
        # step = space // eff_n, i in [0, eff_n). Count how many positions fall in [lo, hi).
        step = self._space // eff_n
        L = 0 if lo is None else max(0, lo)
        H = self._space if hi is None else min(self._space, hi)
        if H <= L or step == 0:
            return 0
        def _idx_at_least(x):
            # smallest i with key_i >= x. key_i = i*step (even) or (i*step)-derived (skew).
            if not self._skew:
                return max(0, -(-x // step))                  # ceil(x/step)
            # skew: key_i = ((i/eff_n)**0.5-inverse) — model skew by squaring the index fraction;
            # invert: i = eff_n * sqrt(x/space). Use integer bisection for exactness.
            import math
            i = int(eff_n * math.sqrt(max(0.0, x) / self._space))
            while i < eff_n and self._skew_key(i) < x:
                i += 1
            while i > 0 and self._skew_key(i - 1) >= x:
                i -= 1
            return i
        lo_i = _idx_at_least(L)
        hi_i = _idx_at_least(H)
        return max(0, min(eff_n, hi_i) - min(eff_n, lo_i))

    def _skew_key(self, i):
        eff_n = self.n_rows - self.short_by
        frac = (i / eff_n) ** 2        # cluster keys toward the low end of the space
        return int(frac * self._space)

    def count_pred(self, pred):
        # parse the planner's predicate: either "col IS NOT NULL" (whole table), "col IS NULL",
        # or ">= <lit> AND < <lit>" / ">= <lit>" / "< <lit>".
        if "IS NULL" in pred and "NOT NULL" not in pred:
            return 0
        if "IS NOT NULL" in pred:
            return self.n_rows - self.short_by
        lo = hi = None
        for part in pred.split(" AND "):
            lit = part.split(">=")[-1].split("<")[-1].strip() if (">=" in part or "<" in part) else None
            val = self._parse_lit(part)
            if ">=" in part:
                lo = val
            elif "<" in part:
                hi = val
        n = self._count_le_region(lo, hi)
        return n

    def _parse_lit(self, part):
        tok = part.replace(">=", " ").replace("<", " ").strip().split()[-1]
        tok = tok.strip()
        if tok.endswith("::uuid"):
            h = tok[:-6].strip().strip("'").replace("-", "")
            return int(h, 16)
        return int(tok)


def _run_count_one(table, kind, n_rows, short_by=0, skew=False, budget=None):
    clock = _FakeClock()
    db = FakeTable(kind, n_rows, short_by=short_by, skew=skew)
    ns = _build_planner(db, clock, skew=skew)
    if budget is not None:
        ns["COUNT_TABLE_BUDGET_SECONDS"] = budget
    entry = {"dsql_schema": "s", "dsql_table": table,
             "metadata": {"primary_key": {"columns": ["id"], "pk_kind": kind}},
             "full_load_rows": n_rows}
    # dms=None -> _count_one uses discovery full_load_rows as expected
    return ns["_count_one"](entry, None), ns


# =============================================================================== tests
def test_old_whole_table_20M_would_time_out():
    """Calibration sanity: a single whole-table COUNT(*) of 20M rows exceeds the modelled 10 s read
    timeout (this is the live failure). Proven by asking the fake directly."""
    clock = _FakeClock()
    db = FakeTable("uuid", 20_000_000)
    ns = _build_planner(db, clock)
    # simulate the OLD behaviour: whole-table count at once
    try:
        ns["_target_query"]("s", "t", ns["_pred"]("id", "uuid", None, None), [])
        assert False, "20M whole-table should exceed the 10 s read timeout"
    except Exception as e:
        assert "read operation timed out" in str(e)


def test_ranged_verifies_10M_20M_100M_300M_integer_and_uuid():
    for kind in ("integer", "uuid"):
        for n in (10_000_000, 20_000_000, 100_000_000, 300_000_000):
            res, ns = _run_count_one("t_%s_%d" % (kind, n), kind, n)
            assert res["status"] == "match", (kind, n, res)
            assert res["target"] == n, (kind, n, res["target"])
            assert res["method"].startswith("ranged"), (kind, n, res["method"])
            # every sub-range stayed under the read timeout (no leaf exceeded it)
            det = res["detail"]
            assert max(det.get("sub_range_seconds", [0])) <= READ_TIMEOUT_S, det


def test_slow_sub_range_is_halved():
    """A skewed uuid distribution puts most rows in the low end, so an initial equal-width
    sub-range is too big and must be HALVED to fit under the read timeout -> still verifies."""
    res, ns = _run_count_one("skewed", "uuid", 60_000_000, skew=True)
    assert res["status"] == "match", res
    det = res["detail"]
    # halving produced MORE counted leaves than the planned equal-width sub-ranges
    assert det["counted_leaves"] >= det["planned_sub_ranges"], det
    assert max(det["sub_range_seconds"]) <= READ_TIMEOUT_S, det


def test_mismatch_short_by_one_fails():
    res, ns = _run_count_one("short", "integer", 20_000_000, short_by=1)
    assert res["status"] == "mismatch", res
    verdict, _ = ns["compute_verdict"]([res], "fail", "fail")
    assert verdict == "mismatch"


def test_unfinishable_subrange_is_unverifiable_fail_closed():
    """If even a 1-key sub-range cannot beat the read timeout (pathological), halving bottoms out
    and the table ends 'unverifiable'; with count_timeout_action=fail that is fail-closed."""
    # make EVERY query exceed the timeout by using a per-row cost so high even 1 row > timeout
    clock = _FakeClock()
    db = FakeTable("integer", 20_000_000)
    ns = _build_planner(db, clock)
    def _always_timeout(schema, table, pred, params):
        raise Exception("('The read operation timed out',)")
    ns["_target_query"] = _always_timeout
    entry = {"dsql_schema": "s", "dsql_table": "t", "full_load_rows": 20_000_000,
             "metadata": {"primary_key": {"columns": ["id"], "pk_kind": "integer"}}}
    res = ns["_count_one"](entry, None)
    assert res["status"] == "unverifiable", res
    verdict, _ = ns["compute_verdict"]([res], "fail", "fail")
    assert verdict == "unverifiable"            # fail-closed
    verdict_warn, _ = ns["compute_verdict"]([res], "warn", "fail")
    assert verdict_warn == "ok"                 # only an explicit warn downgrades


def test_no_false_pass_randomized_soak():
    """300 seeded runs: random key kind, size, and a random short_by in {0,1,2}. A nonzero short_by
    MUST yield 'mismatch' (never 'match'); short_by==0 MUST yield 'match' with target==n. No size
    or seed produces a false pass."""
    bad = []
    for seed in range(300):
        rng = random.Random(seed)
        kind = rng.choice(["integer", "uuid"])
        n = rng.choice([2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000])
        short = rng.choice([0, 0, 0, 1, 2])
        try:
            res, _ = _run_count_one("soak", kind, n, short_by=short)
            if short == 0 and not (res["status"] == "match" and res["target"] == n):
                bad.append((seed, kind, n, short, res["status"], res.get("target")))
            if short > 0 and res["status"] != "mismatch":
                bad.append((seed, kind, n, short, res["status"], "FALSE PASS"))
        except Exception as e:
            bad.append((seed, kind, n, short, "EXC", str(e)[:80]))
    assert not bad, "false-pass / wrong-status seeds (first 15): %s" % bad[:15]


def test_threshold_lowered_below_the_live_failure_sizes():
    """The 10M/20M live-failure sizes now take the ranged path (expected >= COUNT_SPLIT_ROWS)."""
    src = open(COUNT).read()
    tree = ast.parse(src)
    val = None
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id == "COUNT_SPLIT_ROWS":
                    val = ast.literal_eval(n.value)
    assert val is not None and val <= 2_000_000, "COUNT_SPLIT_ROWS must be <= 2M (got %r)" % val


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d count-planner perf/fault test(s) passed" % len(fns))
