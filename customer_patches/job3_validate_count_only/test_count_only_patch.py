#!/usr/bin/env python3
"""Offline tests for the count-only patch to job3_validate.py (no AWS, no Spark, no network).

  python3 test_count_only_patch.py --patched job3_validate_count_only.py \
          [--original job3_validate.py] [--applier apply_count_only_patch.py] \
          [--cases 1500] [--engine both|duckdb|python]

It loads the patch code out of the PATCHED FILE ITSELF (ast-extract, like the repo's own tests),
runs it against a fake DSQL (a real SQL engine - DuckDB - when installed, and a small pure-python
engine always), with fault injection: statement timeouts, XX000 'server unavailable', transient
errors, a time budget. The one invariant checked on every random case: a table is either counted
EXACTLY or passed with a warning - a wrong number is never produced.
"""
import argparse, ast, builtins, contextlib, io, json, random, re, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

PASSED = FAILED = 0


class _FakeSys:
    """stand-in for the sys module the applier script sees (argv + exit)"""
    def __init__(self, argv):
        self.argv = argv

    @staticmethod
    def exit(msg=None):
        raise SystemExit(msg)


def check(cond, msg):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print("[PASS] " + msg)
    else:
        FAILED += 1
        print("[FAIL] " + msg)


def quiet(f, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return f(*a, **k)


# ------------------------------------------------------------------ load the patch code
def load_ns(path):
    src = open(path).read()
    tree = ast.parse(src)
    ns = {"time": time, "threading": threading, "ThreadPoolExecutor": ThreadPoolExecutor,
          "CHECKSUM_MODE": "off", "DMS_TASK_ARN": "arn:test", "COUNT_MISMATCH_TOLERANCE": 0}
    exec("import socket", ns)
    names = {"count_only_validate", "CountRangeError", "CountSplit", "CountStop", "is_txn_age_error"}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and (n.name.startswith("_cnt_") or n.name in names):
            exec(ast.get_source_segment(src, n), ns)
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id.startswith("COUNT_") and t.id != "COUNT_MISMATCH_TOLERANCE":
                    exec(ast.get_source_segment(src, n), ns)
    ns["COUNT_RETRY_WAIT"] = 0
    return ns, src


DEFAULTS = {}


def reset(ns):
    for k, v in DEFAULTS.items():
        ns[k] = v
    ns["COUNT_MISMATCH_TOLERANCE"] = 0
    ns["CHECKSUM_MODE"] = "off"
    ns["DMS_TASK_ARN"] = "arn:test"
    ns["COUNT_UNKNOWN_TOTAL_ROWS"] = 1      # unit-test sizing follows the expected count (real default 600M)


# ------------------------------------------------------------------ fake DSQL engines
def uuid_text(k):
    return "%08x-%04x-%04x-%04x-%012x" % (k >> 96, (k >> 80) & 0xffff, (k >> 64) & 0xffff, (k >> 48) & 0xffff, k & 0xffffffffffff)


class Base:
    """Fault injection shared by both engines."""
    def __init__(self):
        self.limit = None        # a query over more rows than this -> statement timeout
        self.xx_limit = None     # a query over more rows than this -> XX000 server unavailable
        self.fail_first = 0      # first N queries -> transient ConnectionError
        self.fail_after = None   # after N queries every query -> ValueError (not a size error)
        self.delay = 0.0
        self.queries = 0
        self.preds = []
        self.lock = threading.Lock()
        self.not_null = True
        self.shape = "msg"      # how a size error is reported: msg | dict57014 | dict54000 | socket

    def size_error(self):
        if self.shape == "dict57014":
            raise Exception({'S': 'ERROR', 'C': '57014', 'M': 'canceling statement due to statement timeout'})
        if self.shape == "dict54000":
            raise Exception({'S': 'ERROR', 'C': '54000', 'M': 'transaction age limit exceeded'})
        if self.shape == "socket":
            import socket
            raise socket.timeout("read operation timed out")
        raise Exception("canceling statement due to statement timeout")

    def inject(self, n, pred=None):
        with self.lock:
            self.queries += 1
            self.preds.append(pred)
            if self.fail_first > 0:
                self.fail_first -= 1
                raise ConnectionError("connection reset by peer")
            if self.fail_after is not None and self.queries > self.fail_after:
                raise ValueError("boom")
        if self.delay:
            time.sleep(self.delay)
        if self.limit is not None and n > self.limit:
            self.size_error()
        if self.xx_limit is not None and n > self.xx_limit:
            raise Exception({'S': 'ERROR', 'C': 'XX000', 'M': 'server unavailable'})


class PyEngine(Base):
    name = "python"

    def __init__(self):
        super().__init__()
        self.kind, self.keys, self.nulls = "integer", [], 0

    def load(self, kind, keys, nulls=0, not_null=None):
        self.kind, self.keys, self.nulls = kind, sorted(keys), nulls
        self.not_null = (nulls == 0) if not_null is None else not_null

    def truth(self):
        return len(self.keys) + self.nulls

    @staticmethod
    def _val(tok):
        tok = tok.strip()
        if tok.endswith("::uuid"):
            return int(tok[1:-7].replace("-", ""), 16)
        return int(tok)

    def n_for(self, pred):
        if pred is None:
            return len(self.keys) + self.nulls
        if pred.endswith("IS NOT NULL"):
            return len(self.keys)
        if pred.endswith("IS NULL"):
            return self.nulls
        lo = hi = None
        m = re.search(r">= ('[0-9a-f-]{36}'::uuid|-?\d+)", pred)
        if m:
            lo = self._val(m.group(1))
        m = re.search(r"< ('[0-9a-f-]{36}'::uuid|-?\d+)", pred)
        if m:
            hi = self._val(m.group(1))
        import bisect
        a = bisect.bisect_left(self.keys, lo) if lo is not None else 0
        z = bisect.bisect_left(self.keys, hi) if hi is not None else len(self.keys)
        return max(0, z - a)

    def count(self, pred):
        n = self.n_for(pred)
        self.inject(n, pred)
        return n

    def edge(self, asc):
        if not self.keys:
            return None
        k = self.keys[0] if asc else self.keys[-1]
        return k if self.kind == "integer" else uuid_text(k)

    def nullable(self):
        return "NO" if self.not_null else "YES"


class DuckEngine(Base):
    name = "duckdb"

    def __init__(self):
        super().__init__()
        import duckdb
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA s")
        self.kind = "integer"

    def load(self, kind, keys, nulls=0, not_null=None):
        keys = list(keys)
        self.kind = kind
        self.not_null = (nulls == 0) if not_null is None else not_null
        self.con.execute("DROP TABLE IF EXISTS s.t")
        ty = "BIGINT" if kind == "integer" else "UUID"
        self.con.execute('CREATE TABLE s.t ("id" %s%s)' % (ty, " NOT NULL" if self.not_null else ""))
        if keys:
            self.con.executemany("INSERT INTO s.t VALUES (?)", [((int(k),) if kind == "integer" else (uuid_text(k),)) for k in keys])
        for _ in range(nulls):
            self.con.execute("INSERT INTO s.t VALUES (NULL)")

    def truth(self):
        return self.con.execute("SELECT count(*) FROM s.t").fetchone()[0]

    def count(self, pred):
        c = self.con.cursor()
        n = c.execute("SELECT count(*) FROM (SELECT * FROM s.t%s) x" % (" WHERE " + pred if pred else "")).fetchone()[0]
        self.inject(n, pred)
        return n

    def edge(self, asc):
        r = self.con.cursor().execute('SELECT "id" FROM s.t WHERE "id" IS NOT NULL ORDER BY "id" %s LIMIT 1' % ("ASC" if asc else "DESC")).fetchone()
        return None if not r else r[0]

    def nullable(self):
        r = self.con.cursor().execute("SELECT is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?", ("s", "t", "id")).fetchone()
        return r[0] if r else None


def wire(ns, eng, expected):
    ns["_target_query"] = lambda schema, table, pred, metrics: (eng.count(pred), [])
    ns["dms_full_load_rows_for"] = lambda *a: expected
    ns["make_boto_client"] = lambda s: object()

    class Cur:
        def execute(s, sql, params=None):
            if "information_schema.columns" in sql:
                s.r = (eng.nullable(),)
            elif "ORDER BY" in sql:
                with eng.lock:
                    eng.queries += 1
                    eng.preds.append("EDGE")
                    if eng.fail_after is not None and eng.queries > eng.fail_after:
                        raise ValueError("boom")
                v = eng.edge("ASC" in sql)
                s.r = (v,) if v is not None else None
            else:
                raise AssertionError("unexpected SQL from patch: " + sql)

        def fetchone(s):
            return s.r

        def close(s):
            pass

    class Conn:
        def cursor(s):
            return Cur()

        def close(s):
            pass
    ns["connect_dsql"] = lambda autocommit=True: Conn()


def call(ns, eng, kind, expected, entry=None, meta=None, rangeable=True, pk_meta=None, pk_col="id"):
    wire(ns, eng, expected)
    return ns["count_only_validate"]("s.t", "s", "t", entry or {}, meta or {}, [pk_col] if pk_col else [],
                                      pk_meta if pk_meta is not None else {"pk_kind": kind}, rangeable, pk_col if rangeable else None, kind if rangeable else None)


# ------------------------------------------------------------------ data generators
def gen_keys(rnd, kind, n, style):
    hi = (2 ** 63 - 2) if kind == "integer" else (2 ** 128 - 1)
    if n == 0:
        return []
    if style == "dense":
        ks = set(range(1, n + 1)) if kind == "integer" else {i * 7919 for i in range(1, n + 1)}
    elif style == "sparse":
        ks = {rnd.randrange(0, hi) for _ in range(n)}
    elif style == "cluster":
        c = [rnd.randrange(0, hi) for _ in range(3)]
        ks = {min(hi, max(0, rnd.choice(c) + rnd.randrange(-n, n + 1))) for _ in range(n)}
    elif style == "extremes":
        ks = {0, hi} | {rnd.randrange(0, hi) for _ in range(max(0, n - 2))}
    else:
        if kind == "uuid":
            base = rnd.randrange(0, 2 ** 48)
            ks = {((base + rnd.randrange(0, 5000)) << 80) | rnd.randrange(0, 2 ** 80) for _ in range(n)}
        else:
            x, ks = 1, set()
            for _ in range(n):
                x += rnd.randrange(1, 50)
                ks.add(x)
    return sorted(ks)


# ------------------------------------------------------------------ the tests
def run_engine_tests(ns, E, cases, seed):
    nm = E.name
    eng = E()
    P = lambda s: "[%s] %s" % (nm, s)
    # 1 whole table fits
    reset(ns); eng.limit = eng.xx_limit = None; eng.load("integer", range(1, 1001))
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000 and "warning" not in r, P("whole-table count that fits"))
    check(json.dumps(r) is not None and {"table", "status", "source_rows", "target_rows", "mismatches", "mismatch_count", "columns_compared", "columns_differing", "checksum_mode"} <= set(r), P("result has every key the job's summary/report reads and is JSON-serialisable"))
    # 2 short / extra
    r = quiet(call, ns, eng, "integer", 990)
    check(r["status"] == "mismatch" and r["mismatches"][0]["type"] == "COUNT_DIFF" and r["target_rows"] == 1000, P("extra rows vs expected -> table FAILS"))
    r = quiet(call, ns, eng, "integer", 1010)
    check(r["status"] == "mismatch", P("short vs expected -> table FAILS"))
    ns["COUNT_MISMATCH_TOLERANCE"] = 5
    check(quiet(call, ns, eng, "integer", 997)["status"] == "match" and quiet(call, ns, eng, "integer", 1010)["status"] == "mismatch", P("tolerance honoured (3 off ok, 10 off fails)"))
    ns["COUNT_MISMATCH_TOLERANCE"] = 0
    # 3 whole-table statement timeout -> ranges
    reset(ns); ns["COUNT_RANGE_ROWS"] = 50; eng.limit = 300; eng.queries = 0
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000 and eng.queries > 5, P("whole-table timeout -> falls back to key ranges, exact"))
    # 4 whole-table XX000 -> ranges
    reset(ns); ns["COUNT_RANGE_ROWS"] = 50; eng.limit = None; eng.xx_limit = 300
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000, P("whole-table XX000 'server unavailable' -> falls back to key ranges, exact"))
    # 4b the other ways a too-big query is reported
    for shape, label in (("dict57014", "pg8000 error dict, SQLSTATE 57014"), ("dict54000", "pg8000 error dict, SQLSTATE 54000 transaction age"), ("socket", "client-side socket read timeout")):
        reset(ns); ns["COUNT_RANGE_ROWS"] = 50; eng.load("integer", range(1, 1001)); eng.limit = 300; eng.xx_limit = None; eng.shape = shape
        r = quiet(call, ns, eng, "integer", 1000)
        check(r["status"] == "match" and r["target_rows"] == 1000, P("size error reported as %s -> split into ranges, exact" % label))
    eng.shape = "msg"; eng.limit = None
    # 5 big expected -> straight to ranges (first count has a bound, no whole-table query)
    reset(ns); ns.update(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); eng.limit = eng.xx_limit = None; eng.preds = []; eng.queries = 0
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] == 1000 and None not in eng.preds, P("expected >= split size -> ranges from the start, never a whole-table COUNT"))
    # 6 ranges time out too -> halving
    reset(ns); ns.update(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); eng.limit = 30
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000, P("ranges that time out are halved and summed exactly"))
    reset(ns); ns.update(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); eng.limit = None; eng.xx_limit = 30
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000, P("ranges that fail with XX000 are halved and summed exactly"))
    # 7 NULL keys, NOT NULL key
    reset(ns); ns.update(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); eng.limit = eng.xx_limit = None
    eng.load("integer", range(1, 1001), nulls=7)
    r = quiet(call, ns, eng, "integer", 1007)
    check(r["target_rows"] == 1007 and r["status"] == "match", P("NULL-key rows are counted once (nullable key column)"))
    eng.load("integer", range(1, 1001)); eng.preds = []
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] == 1000 and not any(p and p.endswith("IS NULL") for p in eng.preds), P("NOT NULL key column: no slow 'IS NULL' scan is issued"))
    # 8 rows outside sampled min/max still counted
    real_edge = ns["_cnt_edge"]
    ns["_cnt_edge"] = lambda s, t, c, asc, ctx: 100 if asc else 900
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] == 1000, P("rows below the sampled minimum / above the sampled maximum are still counted"))
    ns["_cnt_edge"] = real_edge
    # 9 transient errors, never-clearing errors
    reset(ns); eng.limit = eng.xx_limit = None; eng.fail_first = 2
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] == 1000, P("2 transient errors are retried, then exact"))
    eng.fail_first = 99
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["status"] == "match" and r["target_rows"] is None and "COUNT NOT COMPLETED" in r["warning"], P("errors that never clear -> pass with WARNING (move on to CDC)"))
    eng.fail_first = 0
    # 10 empty / single / extremes
    reset(ns); ns["COUNT_SPLIT_ROWS"] = 1
    eng.load("integer", []); r = quiet(call, ns, eng, "integer", 0)
    check(r["status"] == "match" and r["target_rows"] == 0, P("empty table"))
    eng.load("integer", [5]); r = quiet(call, ns, eng, "integer", 1)
    check(r["status"] == "match" and r["target_rows"] == 1, P("single-row table"))
    eng.load("uuid", [0, 2 ** 128 - 1]); ns["COUNT_RANGE_ROWS"] = 1; r = quiet(call, ns, eng, "uuid", 2)
    check(r["status"] == "match" and r["target_rows"] == 2, P("uuid keys at 0000.. and ffff.."))
    eng.load("integer", [2 ** 63 - 2, 1]); r = quiet(call, ns, eng, "integer", 2)
    check(r["status"] == "match" and r["target_rows"] == 2, P("bigint keys near the 64-bit limit"))
    # 11 no expected count / discovery fallback / no ARN
    reset(ns); eng.load("integer", range(1, 51)); ns["DMS_TASK_ARN"] = None
    r = quiet(call, ns, eng, "integer", None, entry={"full_load_rows": 50})
    check(r["status"] == "match" and "discovery full_load_rows" in r["notes"][0] and "warning" not in r, P("no DMS ARN -> compares with discovery's saved row count"))
    r = quiet(call, ns, eng, "integer", None)
    check(r["status"] == "match" and r["source_rows"] is None and "warning" in r, P("no expected count anywhere -> counted, not compared, WARNING"))
    ns["CHECKSUM_MODE"] = "aggregate"
    check(quiet(call, ns, eng, "integer", 50) is None, P("checksum_mode != off -> patch steps aside (normal validation runs)"))
    ns["CHECKSUM_MODE"] = "off"
    # 12 key choice
    reset(ns); eng.load("integer", range(1, 1001)); ns.update(COUNT_SPLIT_ROWS=1, COUNT_RANGE_ROWS=100); eng.preds = []
    r = quiet(call, ns, eng, "integer", 1000, rangeable=False, pk_meta={"pk_kind": "integer"})
    check(r["target_rows"] == 1000 and any(p and ">=" in p for p in eng.preds), P("single-column key that validation could not range (rangeable False) is still split by the patch"))
    eng.preds = []
    r = quiet(call, ns, eng, "integer", 1000, rangeable=False, pk_meta={"pk_kind": "text"}, pk_col="id")
    check(r["target_rows"] == 1000 and None in eng.preds, P("text / keyless table: one whole-table count"))
    # 13 abort + budget
    reset(ns); ns.update(COUNT_SPLIT_ROWS=1, COUNT_RANGE_ROWS=2, COUNT_RETRIES=0, COUNT_RANGE_THREADS=2)
    eng.load("integer", range(1, 2001)); eng.queries = 0; eng.fail_after = 10; t0 = time.time()
    r = quiet(call, ns, eng, "integer", 2000)
    check(r["target_rows"] is None and "warning" in r and eng.queries < 40, P("one range failing stops the other ranges (queries issued: %d of ~1000 planned)" % eng.queries))
    eng.fail_after = None
    reset(ns); ns.update(COUNT_SPLIT_ROWS=1, COUNT_RANGE_ROWS=1, COUNT_RANGE_THREADS=2, COUNT_TABLE_BUDGET_SECONDS=0.4)
    eng.load("integer", range(1, 3001)); eng.delay = 0.02; eng.queries = 0; t0 = time.time()
    r = quiet(call, ns, eng, "integer", 3000); el = time.time() - t0
    check(r["target_rows"] is None and "warning" in r and el < 5 and eng.queries < 200, P("time budget stops counting (%.1fs, %d queries) and passes with WARNING" % (el, eng.queries)))
    eng.delay = 0.0
    time.sleep(0.3)
    # 14 plan failure / internal error -> never a wrong number, never a crash
    reset(ns); ns.update(COUNT_SPLIT_ROWS=1, COUNT_RANGE_ROWS=100); eng.load("integer", range(1, 1001))
    real_plan = ns["_cnt_plan"]
    ns["_cnt_plan"] = lambda g0, g1, t, p: [(None, 500, g0, 500), (400, None, 400, g1 + 1)]       # overlap
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] is None and "RANGE CHECK FAILED" in r["warning"] and r["status"] == "match", P("overlapping ranges -> caught by the self-check, count NOT used, WARNING"))
    ns["_cnt_plan"] = real_plan
    ns["_cnt_edge"] = lambda *a: (_ for _ in ()).throw(NameError("simulated bug"))
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] is None and "warning" in r, P("unexpected exception inside the patch -> WARNING, table does not crash the job"))
    ns["_cnt_edge"] = real_edge
    check(not ns["is_txn_age_error"](ns["CountStop"]("count-only stopped (time budget used up or another range failed)")), P("stop message is not mistaken for a timeout"))

    # 15 randomized battery
    rnd = random.Random(seed)
    stats = {"exact": 0, "warned": 0, "wrong": 0}
    bad = []
    for case in range(cases):
        kind = rnd.choice(["integer", "uuid"]); style = rnd.choice(["dense", "sparse", "cluster", "extremes", "v7"])
        n = rnd.choice([0, 1, 2, 3, 5, 10, 40, 150, 400]); keys = gen_keys(rnd, kind, n, style)
        nulls = rnd.choice([0, 0, 0, 3])
        eng.load(kind, keys, nulls=nulls, not_null=rnd.choice([None, None, False]))
        tr = eng.truth()
        reset(ns)
        ns.update(COUNT_RANGE_ROWS=rnd.choice([1, 2, 5, 17, 50, 200]), COUNT_SPLIT_ROWS=rnd.choice([1, 10 ** 9]),
                  COUNT_RANGE_THREADS=rnd.choice([1, 4, 8]), COUNT_MAX_HALVINGS=rnd.choice([20, 40, 60]))
        eng.limit = eng.xx_limit = None; eng.fail_first = 0; eng.fail_after = None
        mode = rnd.choice(["ok", "timeout", "xx", "both", "flaky"])
        lim = rnd.choice([tr + 1, max(1, tr // 2), max(1, tr // 7), 3, 1])
        if mode in ("timeout", "both"):
            eng.limit = lim
        if mode in ("xx", "both"):
            eng.xx_limit = rnd.choice([lim, max(1, tr // 3)])
        if mode == "flaky":
            eng.fail_first = rnd.choice([1, 2, 3])
        r = quiet(call, ns, eng, kind, tr)
        if r["target_rows"] is None:
            stats["warned"] += 1
        else:
            stats["exact"] += 1
            if r["target_rows"] != tr or r["status"] != "match":
                stats["wrong"] += 1
                bad.append((case, kind, style, n, nulls, mode, r["target_rows"], tr))
    check(stats["wrong"] == 0, P("%d random cases (int+uuid keys, dense/sparse/clustered/extreme/v7, NULL keys, timeouts, XX000, transient errors, 1/4/8 threads): exact=%d warned=%d WRONG=%d %s" % (cases, stats["exact"], stats["warned"], stats["wrong"], bad[:2])))

    # 16 boundary edges: keys exactly at / next to every planned boundary
    bad_edge = 0; n_edge = 0
    for kind in ("integer", "uuid"):
        for total, per in ((100, 10), (1000, 7), (64, 8), (97, 5), (13, 3), (2, 1)):
            gmin, gmax = (1, 10 ** 6) if kind == "integer" else (2 ** 100, 2 ** 100 + 10 ** 6)
            plan = ns["_cnt_plan"](gmin, gmax, total, per)
            ks = {gmin, gmax}
            for lo, hi, elo, ehi in plan:
                for v in (elo - 1, elo, elo + 1, ehi - 1):
                    if gmin <= v <= gmax:
                        ks.add(v)
            eng.load(kind, sorted(ks)); tr = eng.truth(); reset(ns); ns.update(COUNT_RANGE_ROWS=per, COUNT_SPLIT_ROWS=1)
            eng.limit = eng.xx_limit = None; eng.fail_first = 0
            r = quiet(call, ns, eng, kind, tr); n_edge += 1
            if r["target_rows"] != tr:
                bad_edge += 1
    check(bad_edge == 0, P("keys exactly on / beside every planned boundary: %d cases, %d wrong" % (n_edge, bad_edge)))



class ModelEngine(Base):
    """A 'table' of N dense integer keys 1..N with a cost model: a query over r rows takes
    r/1e6 * SEC_PER_M simulated seconds (SCALE real seconds per simulated second), and is cancelled at
    STMT simulated seconds like DSQL's statement timeout. serialized=True makes every query share ONE
    server (no parallel speed-up) - the worst case."""
    name = "model"

    def __init__(self, n, scale, sec_per_m=2.0, stmt=240.0, serialized=False):
        super().__init__()
        self.n, self.scale, self.sec_per_m, self.stmt = n, scale, sec_per_m, stmt
        self.serialized = serialized
        self.server = threading.Lock()
        self.kind = "integer"
        self.hang = False

    def truth(self):
        return self.n

    def n_for(self, pred):
        if pred is None:
            return self.n
        if pred.endswith("IS NOT NULL"):
            return self.n
        if pred.endswith("IS NULL"):
            return 0
        lo, hi = 1, self.n + 1
        m = re.search(r">= (-?\d+)", pred)
        if m:
            lo = max(1, int(m.group(1)))
        m = re.search(r"< (-?\d+)", pred)
        if m:
            hi = min(self.n + 1, int(m.group(1)))
        return max(0, hi - lo)

    def count(self, pred):
        n = self.n_for(pred)
        with self.lock:
            self.queries += 1
        if self.hang:
            time.sleep(self.hang)
        cost = n / 1e6 * self.sec_per_m
        if cost > self.stmt:
            time.sleep(self.stmt * self.scale)
            raise Exception("canceling statement due to statement timeout")
        if self.serialized:
            with self.server:
                time.sleep(cost * self.scale)
        else:
            time.sleep(cost * self.scale)
        return n

    def edge(self, asc):
        return 1 if asc else self.n

    def nullable(self):
        return "NO"


def run_timing_tests(ns):
    SC = 0.01                                    # 1 simulated second = 10 ms real (was 2 ms: scheduling jitter looked like tens of simulated seconds)
    BUD = 900.0                                  # the 15-minute limit, in simulated seconds
    check(DEFAULTS["COUNT_TABLE_BUDGET_SECONDS"] <= BUD, "[timing] shipped per-table time limit is %s seconds (must be <= %d = 15 minutes)" % (DEFAULTS["COUNT_TABLE_BUDGET_SECONDS"], BUD))
    cases = [("300M rows, expected known, parallel server", dict(n=300_000_000), 300_000_000),
             ("300M rows, expected known, ONE shared server (no parallel speed-up)", dict(n=300_000_000, serialized=True), 300_000_000),
             ("300M rows, expected count unknown", dict(n=300_000_000, serialized=True), None),
             ("300M rows, DMS count far too low (20M)", dict(n=300_000_000), 20_000_000),
             ("300M rows, server 2x slower, parallel", dict(n=300_000_000, sec_per_m=4.0), 300_000_000),
             ("99M rows (whole-table count path), expected known", dict(n=99_000_000), 99_000_000)]
    for name, kw, exp in cases:
        reset(ns)
        ns.update(COUNT_UNKNOWN_TOTAL_ROWS=DEFAULTS["COUNT_UNKNOWN_TOTAL_ROWS"], COUNT_TABLE_BUDGET_SECONDS=BUD * SC)
        eng = ModelEngine(scale=SC, **kw)
        t0 = time.time()
        r = quiet(call, ns, eng, "integer", exp)
        sim = (time.time() - t0) / SC
        counted = r.get("target_rows")
        ok_time = sim <= BUD * 1.05                 # 5% tolerance for thread-scheduling jitter in the stopwatch
        exact = (counted == eng.n) or (counted is None)
        msg = "%s: %.0f simulated seconds (limit %d, +5%% jitter allowed), %d queries, result=%s %s" % (
            name, sim, BUD, eng.queries, r["status"], "counted exactly" if counted == eng.n else ("passed with warning" if counted is None else "WRONG %r" % counted))
        check(ok_time and exact, "[timing] " + msg)
        if exp == eng.n:
            check(counted == eng.n, "[timing] %s: finished the count (not a warning)" % name)
        time.sleep(0.05)
    # a query that never returns must not hold the table past the limit
    reset(ns); ns.update(COUNT_TABLE_BUDGET_SECONDS=0.4)
    eng = PyEngine(); eng.load("integer", range(1, 2001)); eng.delay = 1.5
    t0 = time.time(); r = quiet(call, ns, eng, "integer", 2000); el = time.time() - t0
    check(el < 0.9 and r["status"] == "match" and r["target_rows"] is None and "TIMED OUT" in r["warning"],
          "[timing] a query that hangs: table returned after %.2fs with a TIMED OUT warning (limit 0.4s), not after the query's 1.5s" % el)
    time.sleep(1.8); eng.delay = 0.0
    # DMS lookup that hangs counts against the same limit
    reset(ns); ns.update(COUNT_TABLE_BUDGET_SECONDS=0.4)
    eng = PyEngine(); eng.load("integer", range(1, 101))
    wire(ns, eng, 100); ns["dms_full_load_rows_for"] = lambda *a: time.sleep(1.5) or 100
    t0 = time.time(); r = ns["count_only_validate"]("s.t", "s", "t", {}, {}, ["id"], {"pk_kind": "integer"}, True, "id", "integer"); el = time.time() - t0
    quiet(lambda: None)
    check(el < 0.9 and r["target_rows"] is None, "[timing] a hanging DMS row-count lookup is inside the same limit (%.2fs)" % el)
    time.sleep(1.8)
    # expected count too low: ranges after a whole-table timeout are sized for a bigger table
    reset(ns); ns.update(COUNT_UNKNOWN_TOTAL_ROWS=1000, COUNT_RANGE_ROWS=100, COUNT_SPLIT_ROWS=10 ** 9)
    eng = PyEngine(); eng.load("integer", range(1, 1001)); eng.limit = 300; eng.queries = 0
    r = quiet(call, ns, eng, "integer", 20)
    check(r["target_rows"] == 1000 and r["status"] == "mismatch" and eng.queries >= 8, "[timing] expected count too low (20, real 1000): counted exactly in ranges sized for a larger table, reported as a mismatch (queries: %d)" % eng.queries)
    # unknown expected count: ONE plain whole-table COUNT(*) when it fits (under 100M rows)
    reset(ns); ns.update(COUNT_UNKNOWN_TOTAL_ROWS=1000, COUNT_RANGE_ROWS=100, COUNT_SPLIT_ROWS=10 ** 9)
    eng = PyEngine(); eng.load("integer", range(1, 1001)); eng.preds = []; eng.queries = 0; ns["DMS_TASK_ARN"] = None
    r = quiet(call, ns, eng, "integer", None)
    check(r["target_rows"] == 1000 and eng.queries == 1 and eng.preds == [None], "[timing] unknown expected count, table fits: exactly ONE plain whole-table COUNT(*) (no ranges, no key-bound queries)")
    eng.limit = 300; eng.preds = []; eng.queries = 0
    r = quiet(call, ns, eng, "integer", None)
    check(r["target_rows"] == 1000 and eng.preds[0] is None and eng.queries > 3, "[timing] unknown expected count, whole-table COUNT(*) too big for DSQL: falls back to key ranges, exact")
    reset(ns); ns.update(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100)
    eng = PyEngine(); eng.load("integer", range(1, 1001)); eng.preds = []
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] == 1000 and None not in eng.preds, "[timing] known expected >= split size: still key ranges from the start (never a whole-table COUNT)")
    reset(ns); ns.update(COUNT_SPLIT_ROWS=10 ** 9)
    eng = PyEngine(); eng.load("integer", range(1, 1001)); eng.preds = []; eng.queries = 0
    r = quiet(call, ns, eng, "integer", 1000)
    check(r["target_rows"] == 1000 and eng.queries == 1 and eng.preds == [None], "[timing] known expected under the split size: exactly ONE plain whole-table COUNT(*)")


def run_math_tests(ns):
    V, Pn = ns["_cnt_verify"], ns["_cnt_plan"]
    plan = Pn(1, 1000, 100, 10)
    leaves = [(elo, ehi, lo, hi, 5) for lo, hi, elo, ehi in plan]
    try:
        V(leaves, 1, 1000); ok = True
    except Exception:
        ok = False
    check(ok, "untouched plan passes the self-check")

    def caught(ls, gmin=1, gmax=1000):
        try:
            V(ls, gmin, gmax); return False
        except ns["CountRangeError"]:
            return True
    l2 = list(leaves); a = l2[4]; l2[4] = (a[0] + 1, a[1], a[2], a[3], a[4])
    l3 = list(leaves); a = l3[0]; l3[0] = (a[0], a[1], 5, a[3], a[4])
    l4 = list(leaves); a = l4[-1]; l4[-1] = (a[0], a[1], a[2], 1001, a[4])
    l5 = [(elo, ehi, lo, (None if i == 4 else hi), 5) for i, (lo, hi, elo, ehi) in enumerate(plan)]
    for name, ls in (("range dropped", leaves[:3] + leaves[4:]), ("range counted twice", leaves[:4] + [leaves[3]] + leaves[4:]),
                     ("gap at a boundary", l2), ("first range not open at bottom", l3), ("last range not open at top", l4),
                     ("old bug: a mid-list range left unbounded above", l5), ("nothing counted", [])):
        check(caught(ls), "self-check catches: " + name)
    check(len(plan) == 10 and plan[0][0] is None and plan[-1][1] is None and all(p[0] is not None for p in plan[1:]) and all(p[1] is not None for p in plan[:-1]), "plan: 10 ranges, only the first is open below and only the last is open above")
    for kind_total, per in ((723_000_000, 20_000_000), (1_000_000_000, 20_000_000), (10 ** 9, 7), (50, 3)):
        for gmin, gmax in ((5, 10 ** 12 + 17), (2 ** 100, 2 ** 128 - 1), (1, 10 ** 9)):
            p = Pn(gmin, gmax, kind_total, per) if kind_total <= 10 ** 7 or per > 100 else None
            if p is None:
                continue
            adj = all(p[i][1] == p[i + 1][0] and p[i][3] == p[i + 1][2] for i in range(len(p) - 1))
            width = sum(x[3] - x[2] for x in p) == (gmax - gmin + 1)
            check(adj and width and p[0][0] is None and p[-1][1] is None, "plan %d rows @ %d/range over [%d..]: %d ranges, adjacent, widths sum to the key span" % (kind_total, per, gmin, len(p)))
    check(len(Pn(1, 10 ** 9, 723_000_000, 20_000_000)) == 37, "723M rows @ 20M per range -> 37 ranges")
    check(ns["_cnt_pred"]("id", "integer", None, None) == '"id" IS NOT NULL' and ns["_cnt_pred"]("id", "integer", None, 50) == '"id" < 50' and ns["_cnt_pred"]("id", "integer", 50, None) == '"id" >= 50', "predicate text: open ends have no bound, unbounded range excludes NULL keys")
    check(ns["_cnt_pred"]("id", "uuid", 2 ** 100, 2 ** 100 + 5) == "\"id\" >= '00000010-0000-0000-0000-000000000000'::uuid AND \"id\" < '00000010-0000-0000-0000-000000000005'::uuid", "predicate text: uuid bounds are canonical, cast ::uuid, half-open [lo, hi)")


def run_file_tests(patched_src, original_path, applier_path, ns):
    # static: names the patch uses must exist in the module (or be defined by the patch)
    tree = ast.parse(patched_src)
    patch_defs = set()
    mod_names = set(dir(builtins))
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
            mod_names.add(n.name)
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                for x in ast.walk(t):
                    if isinstance(x, ast.Name):
                        mod_names.add(x.id)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for a in n.names:
                mod_names.add((a.asname or a.name).split(".")[0])
        for x in ast.walk(n):
            if isinstance(x, ast.Global):
                mod_names.update(x.names)
    undefined = set()
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and (n.name.startswith("_cnt_") or n.name in ("count_only_validate", "CountRangeError", "CountSplit", "CountStop")):
            local = set()
            for x in ast.walk(n):
                if isinstance(x, ast.arg):
                    local.add(x.arg)
                elif isinstance(x, ast.Name) and isinstance(x.ctx, (ast.Store, ast.Del)):
                    local.add(x.id)
                elif isinstance(x, (ast.FunctionDef, ast.ClassDef)):
                    local.add(x.name)
                elif isinstance(x, ast.ExceptHandler) and x.name:
                    local.add(x.name)
                elif isinstance(x, (ast.Import, ast.ImportFrom)):
                    for a_ in x.names:
                        local.add((a_.asname or a_.name).split(".")[0])
            for x in ast.walk(n):
                if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Load) and x.id not in local and x.id not in mod_names:
                    undefined.add(x.id)
    check(not undefined, "static check: every name the patch uses is defined in the module (undefined: %s)" % sorted(undefined))
    # no reuse of the validation range builders
    seg = "\n".join(ast.get_source_segment(patched_src, n) for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and (n.name.startswith("_cnt_") or n.name in ("count_only_validate",)))
    reused = [h for h in ("plan_ranges", "plan_ranges_hex", "split_bounds", "_range_predicate_sql", "hex_to_int", "int_to_hex", "target_range_resplit", "_AdaptiveRangeSizer") if re.search(r"\b" + h + r"\b", seg)]
    check(not reused, "the patch does not reuse the validation range builders (found: %s)" % reused)
    if original_path:
        orig = open(original_path).read()
        import difflib
        removed = [l for l in difflib.unified_diff(orig.splitlines(), patched_src.splitlines(), lineterm="", n=0) if l.startswith("-") and not l.startswith("---")]
        check(not removed, "patched file only ADDS lines to the original (removed lines: %d)" % len(removed))
        # collisions with existing names
        otree = ast.parse(orig); onames = set()
        for n in otree.body:
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
                onames.add(n.name)
            elif isinstance(n, ast.Assign):
                onames.update(x.id for t in n.targets for x in ast.walk(t) if isinstance(x, ast.Name))
        new_names = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))} | {x.id for n in tree.body if isinstance(n, ast.Assign) for t in n.targets for x in ast.walk(t) if isinstance(x, ast.Name)}
        redefined = [n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in onames and (n.name.startswith("_cnt_") or n.name.startswith("Count") or n.name == "count_only_validate")]
        check(not redefined, "no existing name is redefined by the patch (%s)" % redefined)
        if applier_path:
            import os, tempfile
            tmp = tempfile.mkdtemp()
            out = os.path.join(tmp, "out.py")
            apns = {"__name__": "applier_under_test"}
            exec(compile(open(applier_path).read(), "applier", "exec"), apns)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    same = apns["apply"](original_path, out) == patched_src and open(out).read() == patched_src
            except Exception:
                same = False
            check(same, "applier output on the original script is byte-identical to the shipped patched script")
            for label, content in (("an already-patched file", patched_src), ("an unrelated file", "print('hi')\n"),
                                   ("a file missing a required anchor", orig.replace("def is_txn_age_error(", "def is_txn_age_error_x("))):
                p = os.path.join(tmp, "in.py"); open(p, "w").write(content)
                try:
                    apns["apply"](p, os.path.join(tmp, "o2.py")); refused = False
                except AssertionError:
                    refused = True
                check(refused, "applier refuses " + label)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--patched", required=True); ap.add_argument("--original"); ap.add_argument("--applier")
    ap.add_argument("--cases", type=int, default=1500); ap.add_argument("--seed", type=int, default=20261015)
    ap.add_argument("--engine", default="both")
    a = ap.parse_args(argv)
    ns, src = load_ns(a.patched)
    DEFAULTS.update({k: ns[k] for k in list(ns) if k.startswith("COUNT_") and k != "COUNT_MISMATCH_TOLERANCE" and k != "COUNT_RETRY_WAIT"})
    DEFAULTS["COUNT_RETRY_WAIT"] = 0
    run_math_tests(ns)
    run_timing_tests(ns)
    engines = []
    if a.engine in ("both", "python"):
        engines.append(PyEngine)
    if a.engine in ("both", "duckdb"):
        try:
            import duckdb  # noqa
            engines.append(DuckEngine)
        except Exception:
            print("(duckdb not installed - skipping the real-SQL-engine run)")
    for E in engines:
        run_engine_tests(ns, E, a.cases, a.seed)
    run_file_tests(src, a.original, a.applier, ns)
    print("\n%d passed, %d failed" % (PASSED, FAILED))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
