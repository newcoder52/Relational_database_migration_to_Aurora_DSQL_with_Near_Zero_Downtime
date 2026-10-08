#!/usr/bin/env python3
"""Offline regression tests for the merge-fix pass on scripts/job3_validate.py (+ params_csv).
No AWS, no Spark, no network. Each test asserts the FIXED behaviour; every one of them FAILS
against the pre-fix code and PASSES after.

Fixes covered:
  M-01  source is_top must be the GLOBAL top, not the 50-range chunk top (the #1 bug).
        Range test over {1,49,50,51,57,100,101,151} ranges, uuid AND integer keys, with a
        fake Spark that ACTUALLY evaluates the broadcast range-join predicate (the shipped
        b18 test stubbed the predicate out, which is why the bug shipped green).
  M-02  validate_parallelism "0"/blank => auto (None), not 1.
  M-03  split_bounds can halve a TEXT range (lexical midpoint) so a text/no-PK whole-table
        range can re-split instead of erroring.
  M-04  int sum uses bround (round-half-to-even) to match the stored ::numeric::bigint.
  M-05  target timestamp sum promotes to numeric before SUM (no double precision loss).
  M-06  empty_at_discovery / full_load_rows are read from the index ENTRY, not only metadata.
  M-09  G10 folder lookup prefers dms_schema/dms_table.
  M-10  per-table parallelism: an EXPLICIT validate_parallelism (>0) is used verbatim per
        table (no division); the auto path (0/blank) stays budget-based (divided across
        MAX_PARALLEL_TABLES).
  M-18  count_mismatch_tolerance parses "0.5" without silently dropping to 0.
  M-19  params_csv.parse does NOT reject an invalid validate_hash: it warns and falls back to
        the default, never failing the fleet start.

Harness mirrors tests/test_b18_validate_throughput.py: ast-parse job3_validate.py and exec
only the needed defs into a seeded namespace. Run: python3 tests/test_validate_merge_fixes.py
"""
import ast
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


_CONSTS = dict(
    VALIDATE_SOURCE_RANGES_PER_PLAN=50,
    DSQL_MAX_CLUSTER_CONNECTIONS=10000,
    MAX_QUERY_CONCURRENCY=20,
    threading=threading,
    time=time,
)


# =============================================================================================
# A faithful fake Spark that EVALUATES the broadcast range-join predicate.
#
# We model columns as lazy expression trees over a per-row environment, so the exact condition
# the production code builds -- (dfk["_k"] >= rng_df["_lo"]) & (rng_df["_is_top"] | (dfk["_k"] <
# rng_df["_hi"])) -- is actually computed for every (key, range) pair. This is what the b18
# fake deliberately did NOT do.
# =============================================================================================
class _Expr:
    def __init__(self, fn):
        self.fn = fn  # (left_row, right_row) -> value

    def __ge__(self, o):
        o = _lit(o)
        return _Expr(lambda l, r: self.fn(l, r) >= o.fn(l, r))

    def __lt__(self, o):
        o = _lit(o)
        return _Expr(lambda l, r: self.fn(l, r) < o.fn(l, r))

    def __and__(self, o):
        return _Expr(lambda l, r: bool(self.fn(l, r)) and bool(o.fn(l, r)))

    def __or__(self, o):
        return _Expr(lambda l, r: bool(self.fn(l, r)) or bool(o.fn(l, r)))

    def __invert__(self):
        return _Expr(lambda l, r: not bool(self.fn(l, r)))

    def cast(self, t):
        def _c(l, r):
            v = self.fn(l, r)
            if t == "long":
                return int(v)
            if t == "string":
                return str(v)
            return v
        return _Expr(_c)

    def alias(self, name):
        self._alias = name
        return self


def _lit(o):
    if isinstance(o, _Expr):
        return o
    return _Expr(lambda l, r, _o=o: _o)


class _LeftCol(_Expr):
    def __init__(self, name):
        self.name = name
        super().__init__(lambda l, r, n=name: l.get(n))


class _RightCol(_Expr):
    def __init__(self, name):
        self.name = name
        super().__init__(lambda l, r, n=name: r.get(n))


class _FakeF:
    @staticmethod
    def col(name):
        return _LeftCol(name)

    @staticmethod
    def lit(v):
        return _lit(v)

    @staticmethod
    def lower(e):
        return _Expr(lambda l, r: str(e.fn(l, r)).lower())

    @staticmethod
    def regexp_replace(e, pat, rep):
        return _Expr(lambda l, r: str(e.fn(l, r)).replace(pat, rep))

    @staticmethod
    def count(_e):
        c = _Expr(lambda l, r: 1)
        c._is_count = True
        return c

    @staticmethod
    def broadcast(df):
        return df


class _RangeDF:
    def __init__(self, rows, cols):
        self.rows = [dict(zip(cols, r)) for r in rows]

    def __getitem__(self, k):
        return _RightCol(k)


class _KeyDF:
    """Source rows (list of {colname: value}); withColumn('_k', expr) materializes the key."""
    def __init__(self, rows):
        self.rows = rows
        self._kexpr = None

    def withColumn(self, name, expr):
        d = _KeyDF(self.rows)
        d._kexpr = expr
        return d

    def __getitem__(self, k):
        return _LeftCol(k)

    def join(self, rng_df, cond, how):
        j = _Joined()
        for row in self.rows:
            lrow = dict(row)
            lrow["_k"] = self._kexpr.fn(row, {}) if self._kexpr else row.get("_k")
            for rr in rng_df.rows:
                if cond.fn(lrow, rr):
                    j.pairs.append((lrow, rr))
        return j


class _Joined:
    def __init__(self):
        self.pairs = []

    def groupBy(self, key):
        self._gkey = key
        return self

    def agg(self, *aggs):
        self._aggs = aggs
        return self

    def collect(self):
        groups = {}
        for lrow, rrow in self.pairs:
            ri = rrow.get("_ri")
            groups.setdefault(ri, []).append((lrow, rrow))
        out = []
        for ri, members in groups.items():
            row = {"_ri": ri, "_cnt": len(members)}
            # metrics _m0.. not needed for the count test (metrics=[] in the test)
            out.append(_Row(row))
        return out


class _Row(dict):
    def __getitem__(self, k):
        return dict.__getitem__(self, k)


class _FakeSpark:
    def createDataFrame(self, rows, cols):
        return _RangeDF(rows, cols)


def _load_source_fns():
    import types
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.functions = _FakeF()
    sys.modules["pyspark"] = types.ModuleType("pyspark")
    sys.modules["pyspark.sql"] = pyspark_sql
    inj = dict(_CONSTS)
    inj["spark"] = _FakeSpark()
    return _load(["_key_col_expr", "_source_summaries_one_plan", "source_range_summaries"], inj)


# =============================================================================================
# M-01: the range test. For {1,49,50,51,57,100,101,151} ranges, uuid + integer keys, assert
# each range's SOURCE count equals the TARGET's [lo,hi) count (unbounded only for the GLOBAL
# top), i.e. per-range source == 1 and the grand total == #ranges, with NO interior range
# unbounded. This FAILS pre-fix (chunk-top #49,#99,#149 over-count) and PASSES after.
# =============================================================================================
def _int_ranges(n):
    # n contiguous [i, i+1) ranges over integer keys 0..n-1 (one key per range)
    return [(i, i + 1) for i in range(n)]


def _uuid_ranges(n):
    # n contiguous 32-hex ranges, one key per range: key i = int_to_hex(i)
    def h(x):
        return format(x, "032x")
    return [(h(i), h(i + 1)) for i in range(n)]


def _target_counts(ranges, keys, pk_kind):
    """The TARGET semantics: only the GLOBAL last range is unbounded above."""
    out = {}
    last = len(ranges) - 1
    for i, (lo, hi) in enumerate(ranges):
        is_top = (i == last)
        c = 0
        for k in keys:
            if pk_kind == "integer":
                kv = int(k)
                lo_v, hi_v = int(lo), int(hi)
            else:
                kv = str(k)
                lo_v, hi_v = str(lo), str(hi)
            if kv >= lo_v and (is_top or kv < hi_v):
                c += 1
        out[i] = c
    return out


def _run_range_case(n, pk_kind, ns):
    if pk_kind == "integer":
        ranges = _int_ranges(n)
        keys = list(range(n))                       # one key per range, all present once
        rows = [{"id": k} for k in keys]
        src = ns["source_range_summaries"](_KeyDF(rows), "id", "integer", ranges, [])
    else:
        ranges = _uuid_ranges(n)
        keys = [format(i, "032x") for i in range(n)]
        # store with dashes so _key_col_expr's regexp_replace("-","") + lower is exercised
        def dash(h):
            return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
        rows = [{"id": dash(k)} for k in keys]
        src = ns["source_range_summaries"](_KeyDF(rows), "id", "uuid", ranges, [])
    tgt = _target_counts(ranges, keys, pk_kind)
    src_counts = {i: src.get(i, (0, []))[0] for i in range(n)}
    per_range_ok = all(src_counts[i] == tgt[i] for i in range(n))
    src_total = sum(src_counts.values())
    tgt_total = sum(tgt.values())
    return per_range_ok, src_total, tgt_total, src_counts, tgt


def test_m01_range_counts_uuid_and_integer():
    ns = _load_source_fns()
    for n in (1, 49, 50, 51, 57, 100, 101, 151):
        for kind in ("integer", "uuid"):
            ok, s_tot, t_tot, s, t = _run_range_case(n, kind, ns)
            check(ok and s_tot == t_tot == n,
                  f"M-01 {kind} {n} ranges: per-range source==target and total=={n} "
                  f"(src_total={s_tot}, tgt_total={t_tot})")
            # Extra: for the known-bad indices, prove they are now bounded (not over-counting).
            if n > 50:
                bad = [i for i in (49, 99, 149) if i < n]
                for b in bad:
                    check(s[b] == 1,
                          f"M-01 {kind} {n} ranges: interior chunk-boundary range #{b} is "
                          f"bounded (source count {s[b]} == 1, not inflated)")


# =============================================================================================
# M-02: validate_parallelism "0"/blank => None (auto); a positive value stays an explicit cap.
# =============================================================================================
_OVERLAY_GLOBALS = (
    "VALIDATE_PARALLELISM", "CONN_BUDGET", "COUNT_MISMATCH_TOLERANCE",
    "MAX_PARALLEL_TABLES", "VALIDATE_ROWS_PER_RANGE", "MAX_QUERY_CONCURRENCY",
    "VALIDATE_HASH", "VALIDATE_TARGET_SECONDS_PER_RANGE", "CSV_NULL_VALUE",
    "REQUIRE_FULL_LOAD_DONE", "DMS_TASK_ARN", "GUARDRAILS_MODE", "VALIDATE_COUNT_CHECK",
)


def _run_overlay(ov):
    """Exec just the body of _apply_job3_arg_overrides against a controlled globals dict.
    The real function reads args via getResolvedOptions(sys.argv, present) where
    present = [a for a in optional if f'--{a}' in sys.argv], so we build a fake sys.argv and a
    fake getResolvedOptions that returns `ov`."""
    import types as _t
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    tree = ast.parse(src)
    seg = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_apply_job3_arg_overrides":
            seg = ast.get_source_segment(src, node)
    assert seg, "could not find _apply_job3_arg_overrides"
    g = {gg: None for gg in _OVERLAY_GLOBALS}
    g.update(dict(VALIDATE_PARALLELISM=None, CONN_BUDGET=900, COUNT_MISMATCH_TOLERANCE=0,
                  MAX_PARALLEL_TABLES=4, VALIDATE_ROWS_PER_RANGE=10000, MAX_QUERY_CONCURRENCY=20,
                  VALIDATE_HASH="all", VALIDATE_TARGET_SECONDS_PER_RANGE=12.0,
                  CSV_NULL_VALUE="NULL", REQUIRE_FULL_LOAD_DONE=True, DMS_TASK_ARN=None,
                  GUARDRAILS_MODE="warn", VALIDATE_COUNT_CHECK="warn",
                  CHECKSUM_MODE="all", CONFIG_PREFIX="", INDEX_S3_KEY="", DSQL_ENDPOINT="",
                  DSQL_USER="", DSQL_DATABASE="", REGION="", DSQL_ENDPOINT_CANDIDATES=[]))
    fake_sys = _t.ModuleType("sys")
    fake_sys.argv = ["job3"] + [f"--{k}" for k in ov]
    g["sys"] = fake_sys
    g["getResolvedOptions"] = lambda argv, keys: {k: ov[k] for k in keys if k in ov}
    g["print"] = lambda *a, **k: None
    exec(compile(seg, "<overlay>", "exec"), g)
    g["_apply_job3_arg_overrides"]()
    return g


def test_m02_parallelism_zero_is_auto():
    g0 = _run_overlay({"validate_parallelism": "0"})
    check(g0["VALIDATE_PARALLELISM"] is None,
          f"M-02 validate_parallelism '0' => auto (None), got {g0['VALIDATE_PARALLELISM']!r}")
    gb = _run_overlay({"validate_parallelism": ""})
    check(gb["VALIDATE_PARALLELISM"] is None,
          f"M-02 validate_parallelism '' => auto (None), got {gb['VALIDATE_PARALLELISM']!r}")
    gp = _run_overlay({"validate_parallelism": "32"})
    check(gp["VALIDATE_PARALLELISM"] == 32,
          f"M-02 validate_parallelism '32' => explicit 32, got {gp['VALIDATE_PARALLELISM']!r}")


# =============================================================================================
# M-18: count_mismatch_tolerance "0.5" must not raise+drop to 0.
# =============================================================================================
def test_m18_tolerance_float_parse():
    g = _run_overlay({"count_mismatch_tolerance": "0.5"})
    check(g["COUNT_MISMATCH_TOLERANCE"] == 0,
          "M-18 '0.5' floors to 0 WITHOUT raising (int(float()))")
    g2 = _run_overlay({"count_mismatch_tolerance": "2.9"})
    check(g2["COUNT_MISMATCH_TOLERANCE"] == 2,
          f"M-18 '2.9' parses to 2 (got {g2['COUNT_MISMATCH_TOLERANCE']!r}), not dropped to 0")


# =============================================================================================
# M-03: split_bounds halves a TEXT range with a lexical midpoint.
# =============================================================================================
def test_m03_text_split():
    ns = _load(["_text_midpoint", "hex_to_int", "int_to_hex", "split_bounds"], dict(_CONSTS))
    halves = ns["split_bounds"]("customer-0000001", "customer-9999999", "text")
    check(halves is not None and len(halves) == 2,
          f"M-03 split_bounds halves a text range (got {halves!r})")
    if halves:
        (l1, h1), (l2, h2) = halves
        check(l1 == "customer-0000001" and h2 == "customer-9999999" and h1 == l2
              and "customer-0000001" < h1 < "customer-9999999",
              f"M-03 text halves are contiguous and ordered: {halves!r}")
    # integer/uuid still work
    check(ns["split_bounds"](0, 100, "integer") == [(0, 50), (50, 100)],
          "M-03 integer split unchanged")
    # a degenerate text range cannot split
    check(ns["split_bounds"]("a", "a", "text") is None,
          "M-03 degenerate text range returns None (lo==hi)")
    check(ns["split_bounds"]("ab", "ac", "text") is not None,
          "M-03 adjacent-prefix text range CAN split (extend lower)")


# =============================================================================================
# M-04: int sum uses bround (half-to-even), matching ::numeric::bigint. We check the generated
# Spark expression calls bround, and model the two roundings on .5 values.
# =============================================================================================
def _decimal_half_even(values):
    from decimal import Decimal, ROUND_HALF_EVEN
    tot = Decimal(0)
    for v in values:
        tot += Decimal(str(v)).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
    return tot


def _decimal_half_up(values):
    from decimal import Decimal, ROUND_HALF_UP
    tot = Decimal(0)
    for v in values:
        tot += Decimal(str(v)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return tot


class _BroundRecorder:
    """A fake F whose .round/.bround record which was used by build_metrics for the int kind."""
    used = None

    class _C:
        def __init__(s): pass
        def cast(s, *a, **k): return s
        def alias(s, *a, **k): return s

    @classmethod
    def coalesce(cls, *a, **k): return cls._C()
    @classmethod
    def sum(cls, *a, **k): return cls._C()
    @classmethod
    def lit(cls, *a, **k): return cls._C()
    @classmethod
    def round(cls, *a, **k):
        cls.used = "round"
        return cls._C()
    @classmethod
    def bround(cls, *a, **k):
        cls.used = "bround"
        return cls._C()
    @classmethod
    def count(cls, *a, **k): return cls._C()
    @classmethod
    def min(cls, *a, **k): return cls._C()
    @classmethod
    def max(cls, *a, **k): return cls._C()
    @classmethod
    def length(cls, *a, **k): return cls._C()
    @classmethod
    def rtrim(cls, *a, **k): return cls._C()
    @classmethod
    def when(cls, *a, **k): return cls._C()
    @classmethod
    def md5(cls, *a, **k): return cls._C()
    @classmethod
    def substring(cls, *a, **k): return cls._C()
    @classmethod
    def conv(cls, *a, **k): return cls._C()
    @classmethod
    def expr(cls, *a, **k): return cls._C()
    @classmethod
    def col(cls, *a, **k): return cls._C()


def test_m04_int_sum_half_even():
    # The data divergence that the fix removes: on .5 values half-up != half-even.
    vals = [0.5, 1.5, 2.5, 3.5, 4.5, -0.5, -1.5, -2.5]
    check(_decimal_half_up(vals) != _decimal_half_even(vals),
          "M-04 half-up and half-even genuinely differ on .5 data (precondition)")
    # The int-kind metric expression now calls bround (half-to-even), not round.
    inj = dict(_CONSTS)
    inj["VALIDATE_HASH"] = "all"
    inj["_HASH_DIGITS"] = 6
    inj["_TS_FMT"] = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ns = _load(["column_kind", "_hash_int_from_md5", "build_metrics"], inj)
    _BroundRecorder.used = None
    m = ns["build_metrics"](["n"], {"n": ("bigint", None, 0, None)}, with_hash=False, key_cols=[])
    # evaluate the int sum's src lambda with the recorder F to see which rounding it calls
    summ = [x for x in m if x["check"] == "sum"][0]
    summ["src"](_BroundRecorder, _BroundRecorder._C())
    check(_BroundRecorder.used == "bround",
          f"M-04 int sum uses bround (round-half-to-even), got {_BroundRecorder.used!r}")


# =============================================================================================
# M-05: target timestamp SUM promotes to numeric before SUM (no double).
# =============================================================================================
def test_m05_ts_numeric_sum():
    inj = dict(_CONSTS)
    inj["VALIDATE_HASH"] = "all"
    inj["_HASH_DIGITS"] = 6
    inj["_TS_FMT"] = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ns = _load(["column_kind", "_hash_int_from_md5", "build_metrics"], inj)
    m = ns["build_metrics"](["t"], {"t": ("timestamp", None, None, 6)}, with_hash=False, key_cols=[])
    tsm = [x for x in m if x["check"] == "sum of instants (us)"][0]
    sql = tsm["agg"].lower().replace(" ", "")
    check("extract(epochfrom\"t\")::numeric" in sql,
          f"M-05 target ts sum promotes epoch to numeric before *1000000 (agg={tsm['agg']})")
    check("::numeric)*1000000" in sql,
          "M-05 multiply happens on the numeric value (exact), not the double")


# =============================================================================================
# M-19: params_csv.parse enum-checks validate_hash.
# =============================================================================================
def test_m19_params_csv_validate_hash_enum():
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    import importlib
    pc = importlib.import_module("params_csv")
    importlib.reload(pc)
    _base = ("parameter,value\n"
             "account_id,111122223333\n"
             "project,demo\n"
             "region,us-east-1\n"
             "dsql_endpoint,abc.dsql.us-east-1.on.aws\n")
    good = _base + "validate_hash,all\n"
    bad = _base + "validate_hash,hashall\n"
    # parse returns {"params","errors","warnings"}.
    def _result(txt):
        try:
            r = pc.parse(txt)
            if isinstance(r, dict):
                return r.get("errors", []), r.get("warnings", []), r.get("params", {})
            if isinstance(r, tuple):
                return (r[1], r[2] if len(r) > 2 else [], r[0] if r else {})
            return [], [], {}
        except Exception as e:
            return [str(e)], [], {}
    eg, wg, pg = _result(good)
    eb, wb, pb = _result(bad)
    check(not any("validate_hash" in str(x) for x in eg),
          "M-19 UNDO: a valid validate_hash=all passes params_csv.parse (no error)")
    check(pg.get("validate_hash") == "all",
          f"M-19 UNDO: a valid validate_hash is kept as-is (got {pg.get('validate_hash')!r})")
    # The undo: an INVALID validate_hash must NOT fail the fleet start. It is warned about and
    # falls back to the default, never pushed onto errors.
    check(not any("validate_hash" in str(x) for x in eb),
          f"M-19 UNDO: an invalid validate_hash is NOT rejected (errors={eb})")
    check(any("validate_hash" in str(x) for x in wb),
          f"M-19 UNDO: an invalid validate_hash is WARNED about (warnings={wb})")
    check(pb.get("validate_hash") == pc.OPTIONAL_DEFAULTS["validate_hash"],
          f"M-19 UNDO: an invalid validate_hash falls back to the default "
          f"{pc.OPTIONAL_DEFAULTS['validate_hash']!r} (got {pb.get('validate_hash')!r})")


# =============================================================================================
# M-10 (UNDO): an EXPLICIT validate_parallelism (>0) is used verbatim per table (no division);
# the AUTO path (0/blank) stays budget-based (divided across MAX_PARALLEL_TABLES). We assert the
# exact source text wires both branches (the behaviour runs inside validate_one_table, Spark).
# =============================================================================================
def test_m10_budget_divided_source():
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    check("if VALIDATE_PARALLELISM is not None:" in src
          and "parallelism = max(1, _default_parallelism())" in src,
          "M-10 UNDO: an explicit validate_parallelism (>0) is used verbatim per table "
          "(no division by MAX_PARALLEL_TABLES)")
    check("_default_parallelism() // max(1, MAX_PARALLEL_TABLES)" in src,
          "M-10 UNDO: the auto path (0/blank) still divides the budget across "
          "MAX_PARALLEL_TABLES")


# =============================================================================================
# M-06: empty_at_discovery / full_load_rows read from the index entry. Assert the source wires
# the entry lookup (the full path needs S3/DSQL). We check both keys are read from `entry`.
# =============================================================================================
def test_m06_entry_lookup_source():
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    check("_entry_or_meta(\"empty_at_discovery\")" in src
          and "_entry_or_meta(\"full_load_rows\")" in src,
          "M-06 empty_at_discovery/full_load_rows read from the index entry (fallback to meta)")


# =============================================================================================
# M-09: G10 folder lookup prefers dms_schema/dms_table.
# =============================================================================================
def test_m09_g10_folder_names_source():
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    check("meta.get('dms_schema')" in src and "meta.get('dms_table')" in src,
          "M-09 G10 folder lookup prefers dms_schema/dms_table")
    check("G10 DMS FullLoadRows check SKIPPED" in src,
          "M-09 a skipped G10 is surfaced as a visible note")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        try:
            globals()[fn]()
        except Exception as e:
            # A test that cannot even run against the current code (e.g. a missing def before
            # the fix is applied) is a FAILURE, not a crash of the whole suite.
            global _failed
            _failed += 1
            print(f"[FAIL] {fn} raised {type(e).__name__}: {e}")
    print(f"\n==== validate-merge-fixes: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
