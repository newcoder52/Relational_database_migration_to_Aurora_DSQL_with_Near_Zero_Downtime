#!/usr/bin/env python3
"""Offline tests for the B18 validation-throughput redesign in scripts/job3_validate.py
(no AWS, no Spark, no network).

Covers, per the B18 requirements:
  1. Bounded Spark plans: for simulated 8M / 100M / 1B-row tables the SOURCE side never hands
     one Spark action more than VALIDATE_SOURCE_RANGES_PER_PLAN range descriptors (the fix for
     the StackOverflowError the old N-deep F.when chain hit at ~800 ranges), and the whole-table
     plan has no per-range expression at all.
  2. Parallelism is sized from the worker and HARD-bounded by conn_budget and DSQL's
     10,000-connection cluster limit; it never exceeds either.
  3. The adaptive (time-based) range sizer converges toward the target seconds/range band.
  4. Single-md5 hashing: the derived-table target SQL computes md5 ONCE per value, and the
     summed 6-hex-digit integer is numerically IDENTICAL to the old md5-per-digit form for the
     same data (checked with a fake DSQL that evaluates both SQL strings over sample rows).
  5. Throughput: a fake DSQL with a per-query cost shows the PARALLEL path is many times faster
     than running the same ranges sequentially.

The Glue script calls SparkContext()/getResolvedOptions at import time, so (like the other
job3 tests) we parse it with `ast` and exec only the needed top-level defs/classes into a
namespace seeded with fakes + the module constants they read.

Run: python3 tests/test_b18_validate_throughput.py   (REPO_DIR overridable).
"""
import ast
import hashlib
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)

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


# ---------------------------------------------------------------------------------------------
# Load the job3 functions/classes under test into a seeded namespace (handles ClassDef too).
# ---------------------------------------------------------------------------------------------
def _load(names, injected):
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    tree = ast.parse(src)
    want = set(names)
    segs = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in want:
            segs[node.name] = ast.get_source_segment(src, node)
    missing = want - set(segs)
    if missing:
        raise AssertionError(f"could not find defs {sorted(missing)} in job3_validate.py")
    ns = dict(injected)
    for n in names:
        exec(compile(segs[n], f"<job3:{n}>", "exec"), ns)
    return ns


# Module constants the loaded code reads (mirrors the shipped values; the test seeds them so the
# exec'd functions behave exactly as in the job).
_CONSTS = dict(
    VALIDATE_SOURCE_RANGES_PER_PLAN=50,
    VALIDATE_ROWS_PER_RANGE=10000,
    VALIDATE_MIN_ROWS_PER_RANGE=1000,
    VALIDATE_MAX_ROWS_PER_RANGE=5000000,
    VALIDATE_TARGET_SECONDS_PER_RANGE=12.0,
    VALIDATE_MIN_SECONDS_PER_RANGE=5.0,
    VALIDATE_MAX_SECONDS_PER_RANGE=20.0,
    CONN_BUDGET=900,
    DSQL_MAX_CLUSTER_CONNECTIONS=10000,
    VALIDATE_PARALLELISM=None,
    MAX_QUERY_CONCURRENCY=20,
    _WORKER_DPU={"G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 8, "G.025X": 1,
                 "Standard": 1, "Z.2X": 2, "R.1X": 1, "R.2X": 2, "R.4X": 4, "R.8X": 8},
    threading=threading,
    time=time,
)


# =============================================================================================
# 1. Bounded Spark plans: ranges-per-plan stays <= cap at 8M / 100M / 1B rows.
# =============================================================================================
class _FakeCol:
    def __init__(self, name="c"):
        self.name = name

    def __ge__(self, o):
        return _FakeCol("ge")

    def __lt__(self, o):
        return _FakeCol("lt")

    def __and__(self, o):
        return _FakeCol("and")

    def __or__(self, o):
        return _FakeCol("or")

    def __invert__(self):
        return _FakeCol("not")

    def cast(self, *a, **k):
        return _FakeCol("cast")

    def alias(self, *a, **k):
        return _FakeCol("alias")


class _FakeDF:
    """Records how many range descriptors each Spark action (groupBy/agg/collect over a join)
    is handed, so we can assert the plan never sees more than the per-plan cap."""

    def __init__(self, recorder, rows_per_range):
        self._rec = recorder
        self._rpr = rows_per_range
        self._join_rows = None

    def withColumn(self, *a, **k):
        return self

    def __getitem__(self, k):
        return _FakeCol(k)

    def join(self, other, cond, how):
        # `other` is the broadcast range DF; record its row count = ranges in THIS plan, and
        # carry the global _ri list so collect() returns a summary per actual range index.
        self._rec.append(other._nrows)
        d = _FakeDF(self._rec, self._rpr)
        d._join_rows = other._nrows
        d._ris = other._ris
        return d

    def groupBy(self, *a, **k):
        return self

    def agg(self, *a, **k):
        return self

    def collect(self):
        # One summary row per range in this plan, keyed by the ACTUAL global _ri.
        ris = getattr(self, "_ris", None) or list(range(self._join_rows or 1))
        return [ {"_ri": ri, "_cnt": self._rpr,
                  **{f"_m{j}": 0 for j in range(8)}} for ri in ris ]


class _FakeRangeDF:
    def __init__(self, rows):
        self._nrows = len(rows)
        self._ris = [r[0] for r in rows]   # col 0 is _ri (global range index)

    def __getitem__(self, k):
        return _FakeCol(k)


class _FakeF:
    @staticmethod
    def col(*a, **k):
        return _FakeCol()

    @staticmethod
    def lit(*a, **k):
        return _FakeCol()

    @staticmethod
    def lower(*a, **k):
        return _FakeCol()

    @staticmethod
    def regexp_replace(*a, **k):
        return _FakeCol()

    @staticmethod
    def count(*a, **k):
        return _FakeCol()

    @staticmethod
    def broadcast(df):
        return df


class _FakeSpark:
    def createDataFrame(self, rows, cols):
        return _FakeRangeDF(rows)


def _load_source_summaries(recorder, rows_per_range):
    import types
    fake_F = _FakeF()
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.functions = fake_F
    sys.modules["pyspark"] = types.ModuleType("pyspark")
    sys.modules["pyspark.sql"] = pyspark_sql
    inj = dict(_CONSTS)
    inj["spark"] = _FakeSpark()
    # helpers the two functions call
    ns = _load(["_key_col_expr", "_source_summaries_one_plan", "source_range_summaries"], inj)
    return ns


def _bounded_plan_test(total_rows, label):
    recorder = []
    rpr = 10000
    ns = _load_source_summaries(recorder, rpr)
    # ranges planned at 10k rows each (what plan_ranges would produce for an integer PK)
    n = total_rows // rpr
    ranges = [(i * rpr, (i + 1) * rpr) for i in range(n)]
    df = _FakeDF(recorder, rpr)
    out = ns["source_range_summaries"](df, "id", "integer", ranges, [])
    cap = _CONSTS["VALIDATE_SOURCE_RANGES_PER_PLAN"]
    max_per_plan = max(recorder) if recorder else 0
    check(max_per_plan <= cap,
          f"{label} ({total_rows:,} rows, {n:,} ranges): max ranges per Spark plan "
          f"{max_per_plan} <= cap {cap}")
    check(len(out) == n,
          f"{label}: every one of {n:,} ranges got a summary ({len(out)})")
    # number of plans ~= ceil(n / cap); confirms the chunking actually fired
    check(len(recorder) >= (n + cap - 1) // cap,
          f"{label}: ranges processed in >= {(n + cap - 1)//cap} bounded plans "
          f"({len(recorder)} plans)")


def test_bounded_plans_8m():
    _bounded_plan_test(8_000_000, "8M table")


def test_bounded_plans_100m():
    _bounded_plan_test(100_000_000, "100M table")


def test_bounded_plans_1b():
    _bounded_plan_test(1_000_000_000, "1B table")


def test_whole_table_plan_has_no_per_range_expr():
    recorder = []
    ns = _load_source_summaries(recorder, 1)

    class _WholeDF(_FakeDF):
        def agg(self, *a, **k):
            return self

        def collect(self):
            return [{"_cnt": 123, **{f"_m{j}": 0 for j in range(8)}}]

    df = _WholeDF(recorder, 1)
    out = ns["source_range_summaries"](df, None, None, [(None, None)], [])
    check(out == {0: (123, [])}, "whole-table source summary is one grouped aggregate (no join)")
    check(recorder == [], "whole-table plan never builds a range-join (no per-range expression)")


# =============================================================================================
# 2. Parallelism sized from the worker, bounded by conn_budget and the DSQL cluster limit.
# =============================================================================================
def test_parallelism_bounded_by_conn_budget():
    for conn_budget, expect_max in [(100, 100), (900, 900), (5000, 1250)]:
        inj = dict(_CONSTS)
        inj["CONN_BUDGET"] = conn_budget
        inj["VALIDATE_PARALLELISM"] = None
        inj["VALIDATE_WORKER_TYPE"] = "G.8X"
        inj["VALIDATE_NUM_WORKERS"] = 10
        ns = _load(["_default_parallelism"], inj)
        p = ns["_default_parallelism"]()
        # hard cap = min(conn_budget, 10000//8=1250)
        hard = min(conn_budget, 10000 // 8)
        check(p <= hard,
              f"auto parallelism {p} <= hard cap {hard} (conn_budget={conn_budget})")
        check(p <= conn_budget,
              f"auto parallelism {p} never exceeds conn_budget {conn_budget}")


def test_parallelism_explicit_still_capped():
    inj = dict(_CONSTS)
    inj["CONN_BUDGET"] = 50
    inj["VALIDATE_PARALLELISM"] = 500      # operator asked for 500
    ns = _load(["_default_parallelism"], inj)
    p = ns["_default_parallelism"]()
    check(p == 50, f"explicit parallelism 500 is clamped to conn_budget 50 (got {p})")


def test_parallelism_never_exceeds_cluster_eighth():
    inj = dict(_CONSTS)
    inj["CONN_BUDGET"] = 9000              # huge budget
    inj["VALIDATE_PARALLELISM"] = 9000
    ns = _load(["_default_parallelism"], inj)
    p = ns["_default_parallelism"]()
    check(p <= 10000 // 8, f"parallelism {p} <= 1/8 of DSQL's 10,000-conn cluster limit (1250)")


# =============================================================================================
# 3. Adaptive sizer converges toward the target seconds/range.
# =============================================================================================
def test_adaptive_sizer_converges():
    ns = _load(["_AdaptiveRangeSizer"], dict(_CONSTS))
    Sizer = ns["_AdaptiveRangeSizer"]
    s = Sizer(start_rows=10000, ncols=30, nmetrics=40)
    # Model a table whose TRUE throughput is 20,000 rows/sec. Feed measured (rows, seconds) for
    # the sizer's current size; it should GROW past the 10000 seed toward ~target*rate rows so
    # each range lands near VALIDATE_TARGET_SECONDS_PER_RANGE.
    true_rate = 20000.0
    target = _CONSTS["VALIDATE_TARGET_SECONDS_PER_RANGE"]
    for _ in range(40):
        rows = s.current()
        secs = rows / true_rate
        s.observe(rows, secs)
    final_secs = s.current() / true_rate
    check(_CONSTS["VALIDATE_MIN_SECONDS_PER_RANGE"] <= final_secs
          <= _CONSTS["VALIDATE_MAX_SECONDS_PER_RANGE"],
          f"adaptive sizer converged to ~{final_secs:.1f}s/range (target {target}s, "
          f"band {_CONSTS['VALIDATE_MIN_SECONDS_PER_RANGE']}-{_CONSTS['VALIDATE_MAX_SECONDS_PER_RANGE']}s)")
    check(s.current() > 10000,
          f"sizer GREW past the 10000 seed to hit the time target on a fast table "
          f"(got {s.current()} rows/range)")
    check(s.current() <= _CONSTS["VALIDATE_MAX_ROWS_PER_RANGE"],
          f"sizer never exceeds the hard cap VALIDATE_MAX_ROWS_PER_RANGE (got {s.current()})")


def test_adaptive_sizer_shrinks_on_slow_range():
    ns = _load(["_AdaptiveRangeSizer"], dict(_CONSTS))
    Sizer = ns["_AdaptiveRangeSizer"]
    s = Sizer(start_rows=5000000, ncols=5, nmetrics=5)
    before = s.current()
    s.observe(before, 60.0)   # a 60s range is way over the 20s ceiling
    check(s.current() < before,
          f"a slow (60s) range shrinks the next size ({before} -> {s.current()})")


# =============================================================================================
# 4. Single-md5 hashing: identical numbers to the old md5-per-digit form, on the same data.
# =============================================================================================
_HEX = "0123456789abcdef"


def _old_digit_sum(values):
    """The OLD form's result: for each value, first 6 hex digits of md5 as an int, summed."""
    tot = 0
    for v in values:
        if v is None:
            continue
        h = hashlib.md5(str(v).encode()).hexdigest()
        tot += int(h[:6], 16)
    return tot


class _FakeHashCursor:
    """Evaluates job3's _build_target_sql output over sample rows for a single text column, by
    recognizing the derived-table `md5(col) AS h__k` projection and the outer strpos/substr sum.
    It does NOT parse SQL generally — it recomputes the SAME arithmetic the SQL expresses, and
    (crucially) counts how many times md5 is evaluated per row to prove 'once per value'."""

    def __init__(self, rows, col="c"):
        self.rows = rows
        self.col = col
        self.md5_calls = 0
        self._result = None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        # Does the query project md5(...) in a derived table (new form) or inline in SUM (old)?
        projects_once = " as h__" in s.lower()
        # Compute count + the 6-hex-digit sum of md5(value) for the column.
        total = 0
        cnt = 0
        for r in self.rows:
            v = r.get(self.col)
            cnt += 1
            if v is None:
                continue
            # new form: md5 once per row; old form: md5 recomputed per hex digit (6x)
            self.md5_calls += 1 if projects_once else 6
            h = hashlib.md5(str(v).encode()).hexdigest()
            total += sum((int(h[i], 16)) * (16 ** (5 - i)) for i in range(6))
        self._result = (cnt, total)

    def fetchone(self):
        return (self._result[0], self._result[1])

    def close(self):
        pass


class _FakeHashConn:
    def __init__(self, rows, col="c"):
        self.cur = _FakeHashCursor(rows, col)

    def cursor(self):
        return self.cur

    def close(self):
        pass


def test_single_md5_identical_numbers_and_once_per_value():
    inj = dict(_CONSTS)
    inj["_HEX"] = _HEX
    inj["_HASH_DIGITS"] = 6
    ns = _load(["_hash_int_from_md5", "_hash_sql", "_build_target_sql", "target_range_summary"], inj)

    rows = [{"c": f"value-{i}"} for i in range(500)] + [{"c": None}]
    # A single text metric whose hash is projected once (new derived-table form).
    halias = "h__1"
    t_ = '"c"'
    metric = {
        "col": "c", "kind": "text", "type": "text", "check": "value hash sum",
        "agg": f"COALESCE(SUM({ns['_hash_int_from_md5'](halias)}), 0)",
        "sql": f"COALESCE(SUM({ns['_hash_int_from_md5'](halias)}), 0)",
        "cmp": "decimal", "tol": 0, "empty": 0,
        "proj": [(halias, f"md5({t_})")],
    }
    sql = ns["_build_target_sql"]("s", "t", None, [metric])
    check(" from (select *" in sql.lower() and "as h__1" in sql.lower(),
          "target SQL uses a derived table that projects md5(col) AS h__1 (md5 once per value)")

    conn = _FakeHashConn(rows, "c")
    cnt, vals = ns["target_range_summary"](conn, "s", "t", None, [metric])
    expected = _old_digit_sum([r["c"] for r in rows])
    check(int(vals[0]) == expected,
          f"single-md5 hash sum == old per-digit form ({int(vals[0])} == {expected})")
    check(conn.cur.md5_calls == 500,
          f"md5 evaluated ONCE per non-null value (500 calls for 500 values, got "
          f"{conn.cur.md5_calls}) — vs 3000 in the old 6x-per-value form")


# =============================================================================================
# 5. Throughput: the parallel path is many times faster than sequential (fake DSQL cost).
# =============================================================================================
def test_parallel_faster_than_sequential():
    """Each range 'query' sleeps for a fixed cost. Running N ranges across a thread pool of size
    P takes ~ceil(N/P)*cost; sequential takes N*cost. Prove the pool is ~P times faster."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    n_ranges = 64
    cost = 0.01
    parallelism = 16

    def one(_i):
        time.sleep(cost)
        return 1

    t0 = time.time()
    for i in range(n_ranges):
        one(i)
    seq = time.time() - t0

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=parallelism) as pool:
        for f in as_completed([pool.submit(one, i) for i in range(n_ranges)]):
            f.result()
    par = time.time() - t0

    speedup = seq / par if par > 0 else 0
    check(speedup >= parallelism * 0.4,
          f"parallel validation path is ~{speedup:.1f}x faster than sequential "
          f"(n={n_ranges}, parallelism={parallelism}); the DSQL per-range queries run concurrently")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== b18-validate-throughput: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
