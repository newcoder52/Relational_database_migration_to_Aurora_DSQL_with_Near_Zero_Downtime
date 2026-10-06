#!/usr/bin/env python3
"""Offline tests for the real-E2E mixed-run fixes (no AWS, no Spark, no network).

Covers the four bugs the real-AWS mixed test found:

  B15  composite-PK auto-reblank-on-resume pages by the FULL key tuple so every reblank txn
       stays <= the DSQL ~3000-row/txn cap (a fake DSQL enforces the cap); single-PK behaviour
       unchanged; the no-PK path adaptively shrinks on a row/size-limit error.
  B14  validation treats a CLIENT read timeout as a re-split trigger (not fatal); the socket
       read timeout is raised above the server statement_timeout; the default range size was
       lowered; the validate_rows_per_range params key flows resolve_task -> ASL -> the
       validate (and ck-validate fork) job args.
  B7r  a 0-row source with a 0-row target PASSES on every validate path (incl. the composite/
       fork path) even without discovery's empty-at-discovery flag; a 0-row source with a
       non-empty target still FAILS.
  B16  a table DMS still reports but that the task's CURRENT selection does not include is
       IGNORED with a warning (not loaded/validated/CDC-applied); a selected empty table is
       still included.

Run: python3 tests/test_e2e_fixes.py   (REPO_DIR overridable)
"""
import ast
import json
import os
import re
import sys
import types
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO

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
# A fake DSQL connection that ENFORCES the ~3000 row-modifications-per-transaction cap, so a
# reblank that tries to delete more than the cap in one txn raises SQLSTATE 54000 exactly like
# real DSQL. The table is a list of key tuples (one per row).
# =============================================================================================
DSQL_ROW_CAP = 3000


class _DsqlTxnTooLarge(Exception):
    def __init__(self, n):
        super().__init__({"C": "54000",
                          "M": f"transaction row limit exceeded ({n} > {DSQL_ROW_CAP})"})


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self._result = None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        low = s.lower()
        if low.startswith("select") and " from " in low:
            self._do_select(s, params)
        elif low.startswith("delete"):
            self._do_delete(s, params)
        else:
            self._result = []

    # ---- SELECT key window (single column IN-subquery OR composite tuple list) ----
    def _do_select(self, s, params):
        # grab the LIMIT
        m = re.search(r"limit\s+(\d+)", s, re.I)
        lim = int(m.group(1)) if m else len(self.conn.rows)
        # which key columns are being selected (between SELECT and FROM)
        proj = s[len("SELECT"):s.lower().index(" from ")].strip()
        cols = [c.strip().strip('"') for c in proj.split(",")]
        ordered = sorted(self.conn.rows)
        window = ordered[:lim]
        # project the requested columns (rows are stored as full tuples in key order)
        self._result = [tuple(row[self.conn.key_index[c]] for c in cols) for row in window]
        self.rowcount = len(self._result)

    def _do_delete(self, s, params):
        low = s.lower()
        if " in (select" in low:
            # single/no-PK path: DELETE ... WHERE "col" IN (SELECT "col" ... ORDER BY col LIMIT n)
            m = re.search(r"limit\s+(\d+)", s, re.I)
            lim = int(m.group(1))
            col = re.search(r'where\s+"([^"]+)"\s+in', s, re.I).group(1)
            ci = self.conn.key_index[col]
            ordered = sorted(self.conn.rows)
            # the <= lim DISTINCT values of `col` in the window...
            distinct_vals, seen = [], set()
            for row in ordered:
                v = row[ci]
                if v not in seen:
                    seen.add(v)
                    distinct_vals.append(v)
                if len(distinct_vals) >= lim:
                    break
            doomed = [row for row in self.conn.rows if row[ci] in set(distinct_vals)]
            self._commit_delete(doomed)
        elif " or (" in low or low.rstrip().endswith(")") and params:
            # composite OR-of-ANDs with bound params: each group is the full key tuple
            klen = len(self.conn.key_cols)
            tuples = [tuple(params[i:i + klen]) for i in range(0, len(params), klen)]
            doomed = [row for row in self.conn.rows if tuple(row) in set(tuples)]
            self._commit_delete(doomed)
        else:
            self._result = []
            self.rowcount = 0

    def _commit_delete(self, doomed):
        n = len(doomed)
        if n > DSQL_ROW_CAP:
            # real DSQL rejects the whole txn; nothing is deleted
            raise _DsqlTxnTooLarge(n)
        remove = set(id(r) for r in doomed)
        self.conn.rows = [r for r in self.conn.rows if id(r) not in remove]
        self.conn.max_txn_rows = max(self.conn.max_txn_rows, n)
        self.rowcount = n

    def fetchall(self):
        return self._result or []

    def fetchone(self):
        return (self._result or [None])[0]

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows, key_cols):
        self.key_cols = list(key_cols)
        self.key_index = {c: i for i, c in enumerate(self.key_cols)}
        self.rows = [tuple(r) for r in rows]
        self.autocommit = False
        self.max_txn_rows = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _load_b15(fake_conn_holder):
    """Extract the real B15 functions and run them against a shared fake DSQL connection."""
    inj = {
        "MIN_CHUNK_SIZE": 25,
        "DSQL_MAX_ROWS_PER_TXN": DSQL_ROW_CAP,
        "OCC_MAX_RETRIES": 3, "SERVER_MAX_RETRIES": 3, "MAX_CHUNK_RETRIES": 3,
        "CHUNK_RETRY_BACKOFF_SECONDS": 0,
        "occ_backoff_seconds": lambda a: 0, "server_backoff_seconds": lambda a: 0,
        "is_occ_conflict": lambda e: False,
        "is_transient_server_error": lambda e: False,
        "is_broken_pipe_error": lambda e: False,
        "_invalidate_dsql_token": lambda: None,
        "time": types.SimpleNamespace(sleep=lambda *_a, **_k: None),
        "print": lambda *a, **k: None,
        # all paths reuse the one fake connection so deletes accumulate on the same table
        "connect_dsql": lambda: fake_conn_holder["conn"],
    }
    ns = H.load_defs(
        "scripts/job2_load.py",
        ["is_txn_timeout", "is_dsql_txn_too_large",
         "blank_whole_table", "blank_whole_table_composite"],
        inj)
    return ns


# ---------------------------------------------------------------------------
# B15
# ---------------------------------------------------------------------------
def test_b15_classifier():
    holder = {"conn": _FakeConn([], ["a"])}
    ns = _load_b15(holder)
    row_err = {"C": "54000", "M": "transaction row limit exceeded"}
    age_err = {"C": "54000", "M": "transaction age limit of 300s exceeded"}
    check(ns["is_dsql_txn_too_large"](Exception(row_err)) is True,
          "B15: 54000 'transaction row limit' classified as txn-too-large")
    check(ns["is_dsql_txn_too_large"](Exception("transaction too large")) is True,
          "B15: 'transaction too large' classified as txn-too-large")
    check(ns["is_dsql_txn_too_large"](Exception(age_err)) is False,
          "B15: the 300s AGE limit is NOT misclassified as the row/size limit")
    check(ns["is_dsql_txn_too_large"](Exception("some other error")) is False,
          "B15: an unrelated error is not txn-too-large")


def test_b15_composite_10k_two_distinct_first_col():
    """A composite (region, seq) table with 10k rows whose FIRST column has only 2 distinct
    values. The OLD code paged by the first column (any_col) and would delete ~5000 rows in one
    txn -> 54000. The fix pages by the FULL key tuple, so every txn is <= batch (<=3000) rows
    and the whole table is cleared."""
    rows = [("east" if i % 2 == 0 else "west", i) for i in range(10000)]  # 2 distinct regions
    holder = {"conn": _FakeConn(rows, ["region", "seq"])}
    ns = _load_b15(holder)
    removed = ns["blank_whole_table"]("s", "t", pk_col=None,
                                      any_col="region", pk_cols=["region", "seq"], batch=2000)
    check(removed == 10000, f"B15: composite reblank cleared all rows ({removed} == 10000)")
    check(len(holder["conn"].rows) == 0, "B15: composite table is empty after reblank")
    check(holder["conn"].max_txn_rows <= DSQL_ROW_CAP,
          f"B15: every composite reblank txn stayed <= {DSQL_ROW_CAP} "
          f"(max was {holder['conn'].max_txn_rows})")


def test_b15_composite_direct_helper():
    rows = [("east" if i % 2 == 0 else "west", i) for i in range(7000)]
    holder = {"conn": _FakeConn(rows, ["region", "seq"])}
    ns = _load_b15(holder)
    removed = ns["blank_whole_table_composite"]("s", "t", ["region", "seq"], batch=2500)
    check(removed == 7000 and len(holder["conn"].rows) == 0,
          "B15: blank_whole_table_composite clears the whole table")
    check(holder["conn"].max_txn_rows <= DSQL_ROW_CAP,
          "B15: composite helper never exceeds the DSQL row cap")


def test_b15_single_pk_unchanged():
    """A single-column PK with 8000 unique values still pages by the PK, each txn <= batch."""
    rows = [(i,) for i in range(8000)]
    holder = {"conn": _FakeConn(rows, ["id"])}
    ns = _load_b15(holder)
    removed = ns["blank_whole_table"]("s", "t", pk_col="id", any_col="id",
                                      pk_cols=["id"], batch=2000)
    check(removed == 8000 and len(holder["conn"].rows) == 0,
          "B15: single-PK reblank still clears the whole table")
    check(holder["conn"].max_txn_rows <= DSQL_ROW_CAP,
          "B15: single-PK reblank txns stay under the cap (unchanged behaviour)")


def test_b15_nopk_adaptive_shrink():
    """A no-PK table paged by a duplicate-heavy column: 2 distinct values x 2500 rows each. A
    window of 2 distinct values would delete 5000 rows (> cap) -> the fix HALVES the batch and
    retries until a window fits (1 value = 2500 <= 3000), clearing the table."""
    rows = [("A" if i < 2500 else "B", i) for i in range(5000)]
    holder = {"conn": _FakeConn(rows, ["c", "rowid"])}
    ns = _load_b15(holder)
    # no pk_cols, no pk_col -> no-PK path paging by any_col="c"
    removed = ns["blank_whole_table"]("s", "t", pk_col=None, any_col="c",
                                      pk_cols=None, batch=2000)
    check(removed == 5000 and len(holder["conn"].rows) == 0,
          "B15: no-PK reblank clears the table via adaptive shrink")
    check(holder["conn"].max_txn_rows <= DSQL_ROW_CAP,
          f"B15: adaptive shrink kept every txn <= {DSQL_ROW_CAP} "
          f"(max {holder['conn'].max_txn_rows})")


def test_b15_nopk_single_value_over_cap_fails_clearly():
    """A no-PK table where ONE value of the paging column maps to > cap rows cannot be deleted
    in one DSQL txn — the fix fails with a clear message rather than looping forever."""
    rows = [("same", i) for i in range(4000)]  # one value, 4000 rows > 3000 cap
    holder = {"conn": _FakeConn(rows, ["c", "rowid"])}
    ns = _load_b15(holder)
    raised = ""
    try:
        ns["blank_whole_table"]("s", "t", pk_col=None, any_col="c", pk_cols=None, batch=2000)
    except Exception as e:
        raised = str(e)
    check("per-transaction limit" in raised and "'c'" in raised,
          "B15: a single over-cap value fails with a clear, actionable message")


def test_b15_callers_pass_full_key():
    """Every assert_empty_or_register CALL in job2 passes pk_cols (the FULL key list) so the
    composite path is reachable from the real load orchestrators (not just the helper)."""
    src = H.read_source("scripts/job2_load.py")
    # the barrier accepts pk_cols and forwards it to blank_whole_table
    check("def assert_empty_or_register(" in src and "pk_cols=None" in src,
          "B15: assert_empty_or_register takes pk_cols")
    check("any_col=any_col, pk_cols=pk_cols)" in src,
          "B15: the barrier forwards pk_cols to blank_whole_table")
    # AST: every Call to assert_empty_or_register passes a pk_cols keyword
    tree = ast.parse(src)
    calls, with_cols = 0, 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == "assert_empty_or_register":
            calls += 1
            if any(kw.arg == "pk_cols" for kw in node.keywords):
                with_cols += 1
    check(calls >= 3 and with_cols == calls,
          f"B15: all {calls} assert_empty_or_register call sites pass pk_cols ({with_cols} do)")


# ---------------------------------------------------------------------------
# B14
# ---------------------------------------------------------------------------
def _load_is_txn_age():
    ns = H.load_defs("scripts/job3_validate.py", ["is_txn_age_error"], {})
    return ns["is_txn_age_error"]


def test_b14_read_timeout_is_resplit_trigger():
    is_age = _load_is_txn_age()
    import socket
    check(is_age(socket.timeout("timed out")) is True,
          "B14: a socket.timeout is a re-split trigger")
    check(is_age(Exception("The read operation timed out")) is True,
          "B14: 'The read operation timed out' is a re-split trigger")
    check(is_age(Exception({"C": "57014", "M": "statement timeout"})) is True,
          "B14: the server statement_timeout (57014) is still a trigger")
    check(is_age(Exception("some unrelated error")) is False,
          "B14: an unrelated error is NOT a re-split trigger")


def test_b14_resplit_on_client_timeout():
    """A fake per-range query that raises a CLIENT read timeout for ranges wider than LIMIT must
    be re-split by target_range_resplit until each sub-query fits, and the summed count equals
    the whole-table truth (so a client timeout no longer fails the table)."""
    N = 8_055_226
    LIMIT = 200_000
    calls = {"timeouts": 0, "max_width": 0}

    def sim(schema, table, pred, metrics):
        lo, hi = 0, N
        if pred:
            ge = re.search(r">= (\d+)", pred)
            lt = re.search(r"< (\d+)", pred)
            if ge:
                lo = int(ge.group(1))
            hi = int(lt.group(1)) if lt else N
        width = hi - lo
        calls["max_width"] = max(calls["max_width"], width)
        if width > LIMIT:
            calls["timeouts"] += 1
            raise Exception("The read operation timed out")   # CLIENT-side, not 54000
        s = (hi - 1) * hi // 2 - (lo - 1) * lo // 2 if hi > lo else 0
        return (hi - lo, [Decimal(s)])

    inj = {"Decimal": Decimal, "VALIDATE_MAX_RESPLIT_DEPTH": 8, "_target_query": sim}
    ns = H.load_defs(
        "scripts/job3_validate.py",
        ["hex_to_int", "int_to_hex", "hex_to_canonical_uuid", "_sql_str_literal",
         "_range_predicate_sql", "is_txn_age_error", "combine_metric", "combine_summaries",
         "split_bounds", "target_range_resplit"], inj)
    cnt, vals = ns["target_range_resplit"]("s", "t", "id", "integer", 0, N, True, [{"check": "sum"}], 0)
    check(cnt == N, f"B14: re-split total count == whole table ({cnt} == {N})")
    check(vals[0] == Decimal((N - 1) * N // 2), "B14: re-split summed metric == whole-table sum")
    check(calls["timeouts"] >= 1, "B14: the oversized range client-timed-out at least once (then split)")


def test_b14_socket_timeout_above_statement_timeout():
    src = H.read_source("scripts/job3_validate.py")
    check("VALIDATE_SOCKET_READ_TIMEOUT" in src and "settimeout(float(VALIDATE_SOCKET_READ_TIMEOUT))" in src,
          "B14: the connection's socket read timeout is set from VALIDATE_SOCKET_READ_TIMEOUT")
    ns = {}
    # evaluate the two constants in isolation to confirm socket > statement timeout
    m_st = re.search(r"VALIDATE_STATEMENT_TIMEOUT_MS\s*=\s*(\d+)", src)
    m_so = re.search(r"VALIDATE_SOCKET_READ_TIMEOUT\s*=\s*int\(VALIDATE_STATEMENT_TIMEOUT_MS\s*/\s*1000\)\s*\+\s*(\d+)",
                     src)
    check(bool(m_st and m_so), "B14: both timeout constants are defined with the expected shape")
    if m_st and m_so:
        st_s = int(m_st.group(1)) / 1000.0
        so_s = int(m_st.group(1)) / 1000.0 + int(m_so.group(1))
        check(so_s > st_s,
              f"B14: socket read timeout ({so_s}s) is ABOVE the server statement_timeout ({st_s}s)")


def test_b14_default_lowered():
    src = H.read_source("scripts/job3_validate.py")
    m = re.search(r"^VALIDATE_ROWS_PER_RANGE\s*=\s*(\d+)", src, re.M)
    check(bool(m) and int(m.group(1)) == 10000,
          f"B14: default validate_rows_per_range lowered to 10000 (got {m and m.group(1)})")


def test_b14_param_flows_resolve_to_jobs():
    # params_csv knows the key (OPTIONAL_DEFAULTS + PIPELINE_KEYS + int bounds)
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    _b = types.ModuleType("boto3"); _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    import importlib
    pc = importlib.import_module("params_csv")
    importlib.reload(pc)
    check("validate_rows_per_range" in pc.OPTIONAL_DEFAULTS,
          "B14: params_csv OPTIONAL_DEFAULTS has validate_rows_per_range")
    check("validate_rows_per_range" in pc.PIPELINE_KEYS,
          "B14: params_csv PIPELINE_KEYS has validate_rows_per_range")
    check(pc.OPTIONAL_DEFAULTS["validate_rows_per_range"] == "10000",
          "B14: params_csv default is 10000")

    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    check(rt.SETTINGS_DEFAULTS.get("validate_rows_per_range") == 10000,
          "B14: resolve_task default is 10000")

    # resolve_task emits validateRowsPerRange in the resolved payload
    rtsrc = H.read_source("lambdas/resolve_task.py")
    check('"validateRowsPerRange": cfg["validate_rows_per_range"]' in rtsrc,
          "B14: resolve_task emits validateRowsPerRange in the resolved payload")

    # startup.asl wires it (ResolveTask RS + CreateGlueJobs + EnsureForkJobs)
    import json
    asl = json.load(open(os.path.join(REPO, "stepfunctions", "startup.asl.json")))
    S = asl["States"]
    check("validateRowsPerRange.$" in S["ResolveTask"]["ResultSelector"],
          "B14: ASL ResolveTask ResultSelector produces validateRowsPerRange")
    check(S["CreateGlueJobs"]["Parameters"]["Payload"].get("validateRowsPerRange.$")
          == "$.resolved.validateRowsPerRange",
          "B14: ASL CreateGlueJobs forwards validateRowsPerRange")
    check(S["EnsureForkJobs"]["Parameters"]["Payload"].get("validateRowsPerRange.$")
          == "$.resolved.validateRowsPerRange",
          "B14: ASL EnsureForkJobs forwards validateRowsPerRange (ck validate forks)")

    # create_glue_jobs injects --validate_rows_per_range for validate AND ck-validate
    cgsrc = H.read_source("lambdas/create_glue_jobs.py")
    check('if role in ("validate", "ck-validate"):' in cgsrc
          and 'args["--validate_rows_per_range"] = str(_vrr)' in cgsrc,
          "B14: create_glue_jobs sets --validate_rows_per_range for validate + ck-validate")

    # job3 overlay accepts the arg
    j3 = H.read_source("scripts/job3_validate.py")
    check('"validate_rows_per_range"' in j3 and "VALIDATE_ROWS_PER_RANGE = max(1, int(ov" in j3,
          "B14: job3 overlay reads --validate_rows_per_range")


# ---------------------------------------------------------------------------
# B7-residual (empty composite / fork validate)
# ---------------------------------------------------------------------------
class _V1S3:
    def __init__(self, present):
        self.present = present

    def list_objects_v2(self, Bucket=None, Prefix=None, MaxKeys=None):
        return {"KeyCount": 1 if self.present else 0}


class _V1Conn:
    def __init__(self, target_count):
        self.target_count = target_count

    def cursor(self):
        return self

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return [self.target_count]

    def close(self):
        pass


def _b7_decide(empty_at_discovery, source_present, target_count):
    inj = {"Decimal": Decimal,
           "connect_dsql": lambda autocommit=True: _V1Conn(target_count)}
    ns = H.load_defs("scripts/job3_validate.py",
                     ["split_s3", "s3_prefix_has_objects", "_build_target_sql",
                      "target_range_summary"], inj)
    s3 = _V1S3(source_present)
    dms_s3_path = "s3://bucket/DMS_SAMPLE/TICKET_PURCHASE_HIST"
    b, p = ns["split_s3"](dms_s3_path)
    present = bool(dms_s3_path) and ns["s3_prefix_has_objects"](s3, b, p)
    if present:
        return ("would_read_s3", None)
    conn = ns["connect_dsql"](autocommit=True)
    tgt_cnt, _ = ns["target_range_summary"](conn, "dms_sample", "t", None, [])
    if int(tgt_cnt) == 0:
        return ("match", 0)
    if empty_at_discovery:
        return ("mismatch", int(tgt_cnt))
    return ("error", None)


def test_b7_residual():
    # empty composite table NOT flagged empty-at-discovery, 0 target -> PASS (the exact mixed-run case)
    check(_b7_decide(False, False, 0) == ("match", 0),
          "B7r: empty source + 0 target PASSES without the empty-at-discovery flag (composite/fork path)")
    # flagged empty, 0 target -> still PASS
    check(_b7_decide(True, False, 0) == ("match", 0),
          "B7r: empty source + 0 target PASSES when flagged too")
    # 0 source + NON-empty target, flagged -> FAIL (mismatch)
    check(_b7_decide(True, False, 9)[0] == "mismatch",
          "B7r: empty source + non-empty target FAILS (mismatch) when flagged empty")
    # 0 source + NON-empty target, NOT flagged -> clear error (not a pass)
    check(_b7_decide(False, False, 9)[0] == "error",
          "B7r: empty source + non-empty target + no flag -> clear error (never a silent pass)")
    # the real validate_one_table keys the 0==0 PASS on the target count, not the flag
    j3 = H.read_source("scripts/job3_validate.py")
    check("if int(tgt_cnt) == 0:" in j3 and "0 == 0, PASS" in j3,
          "B7r: validate_one_table PASSES a 0-target empty source regardless of the flag")


# ---------------------------------------------------------------------------
# B16 (table list from live selection, not leftover S3 folders)
# ---------------------------------------------------------------------------
def _load_resolve():
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    _b = types.ModuleType("boto3"); _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    import importlib
    rt = importlib.import_module("resolve_task")
    importlib.reload(rt)
    return rt


def test_b16_selection_includes():
    rt = _load_resolve()
    rules = [
        {"rule-type": "selection", "rule-action": "include",
         "object-locator": {"schema-name": "DMS_SAMPLE", "table-name": "%"}},
        {"rule-type": "selection", "rule-action": "exclude",
         "object-locator": {"schema-name": "DMS_SAMPLE", "table-name": "TICKET_PURCHASE_HIST"}},
    ]
    check(rt._selection_includes(rules, "DMS_SAMPLE", "SPORT_TYPE") is True,
          "B16: a table matched by an include rule is selected")
    check(rt._selection_includes(rules, "DMS_SAMPLE", "TICKET_PURCHASE_HIST") is False,
          "B16: a table removed by a later exclude rule is NOT selected (leftover)")
    check(rt._selection_includes(rules, "OTHER", "X") is False,
          "B16: a table in no selection rule is NOT selected")
    # include-only, explicit table list
    rules2 = [{"rule-type": "selection", "rule-action": "include",
               "object-locator": {"schema-name": "S", "table-name": "A"}}]
    check(rt._selection_includes(rules2, "S", "A") is True
          and rt._selection_includes(rules2, "S", "B") is False,
          "B16: explicit single-table include selects only that table")


class _B16Dms:
    def __init__(self, table_mappings, stats):
        self._tm = table_mappings
        self._stats = stats

    def describe_replication_tasks(self, Filters=None, WithoutSettings=None):
        return {"ReplicationTasks": [{"TableMappings": self._tm}]}

    def describe_table_statistics(self, **kw):
        return {"TableStatistics": self._stats, "Marker": None}


class NoSuchKey(Exception):
    pass


class _B16S3:
    def __init__(self):
        self.puts = {}

    def get_object(self, Bucket=None, Key=None):
        raise NoSuchKey("NoSuchKey")

    def put_object(self, Bucket=None, Key=None, Body=None, ContentType=None):
        self.puts[Key] = Body.decode() if isinstance(Body, (bytes, bytearray)) else Body


def test_b16_build_table_list_ignores_unselected():
    rt = _load_resolve()
    import json
    tm = json.dumps({"rules": [
        {"rule-type": "selection", "rule-id": "1", "rule-name": "1", "rule-action": "include",
         "object-locator": {"schema-name": "DMS_SAMPLE", "table-name": "%"}},
        {"rule-type": "selection", "rule-id": "2", "rule-name": "2", "rule-action": "exclude",
         "object-locator": {"schema-name": "DMS_SAMPLE", "table-name": "TICKET_PURCHASE_HIST"}},
        {"rule-type": "transformation", "rule-id": "9", "rule-name": "9",
         "rule-target": "schema", "rule-action": "convert-lowercase",
         "object-locator": {"schema-name": "%"}},
        {"rule-type": "transformation", "rule-id": "10", "rule-name": "10",
         "rule-target": "table", "rule-action": "convert-lowercase",
         "object-locator": {"schema-name": "%", "table-name": "%"}},
    ]})
    stats = [
        {"SchemaName": "DMS_SAMPLE", "TableName": "SPORT_TYPE",
         "TableState": "Table completed", "FullLoadRows": 3012},
        {"SchemaName": "DMS_SAMPLE", "TableName": "NAME_DATA",
         "TableState": "Table completed", "FullLoadRows": 5373},
        # leftover from a prior run: still in stats, removed from selection
        {"SchemaName": "DMS_SAMPLE", "TableName": "TICKET_PURCHASE_HIST",
         "TableState": "Table completed", "FullLoadRows": 0},
    ]
    dms = _B16Dms(tm, stats)
    s3 = _B16S3()
    # patch boto3.client to return our fakes
    rt.boto3.client = lambda svc, region_name=None: (dms if svc == "dms" else s3)
    event = {"taskArn": "arn:aws:dms:us-east-1:111111111111:task:ABC", "bucket": "mybucket",
             "region": "us-east-1", "taskSuffix": "mix", "cdc_root": "."}
    out = rt.handler_build_table_list(event, None)
    check(out["count"] == 2, f"B16: only the 2 selected tables are in the manifest (got {out['count']})")
    manifest = s3.puts.get("config/_task/mix/table_manifest.csv", "")
    check("ticket_purchase_hist" not in manifest,
          "B16: the leftover (unselected) table is NOT in the manifest")
    check("sport_type" in manifest and "name_data" in manifest,
          "B16: the selected tables ARE in the manifest")
    check(out.get("ignoredUnselected") == ["dms_sample.ticket_purchase_hist"],
          "B16: the ignored table is reported")
    warned = " ".join(out.get("warnings", []))
    check("ticket_purchase_hist" in warned and "aws s3 rm" in warned,
          "B16: a WARNING names the leftover table and suggests aws s3 rm")


def test_b16_selected_empty_table_still_included():
    """A SELECTED table with 0 rows (empty, FullLoadRows=0) is still included (loaded empty) —
    only UNSELECTED leftovers are dropped."""
    rt = _load_resolve()
    import json
    tm = json.dumps({"rules": [
        {"rule-type": "selection", "rule-id": "1", "rule-name": "1", "rule-action": "include",
         "object-locator": {"schema-name": "DMS_SAMPLE", "table-name": "%"}},
        {"rule-type": "transformation", "rule-id": "9", "rule-name": "9",
         "rule-target": "schema", "rule-action": "convert-lowercase",
         "object-locator": {"schema-name": "%"}},
        {"rule-type": "transformation", "rule-id": "10", "rule-name": "10",
         "rule-target": "table", "rule-action": "convert-lowercase",
         "object-locator": {"schema-name": "%", "table-name": "%"}},
    ]})
    stats = [
        {"SchemaName": "DMS_SAMPLE", "TableName": "SPORT_TYPE",
         "TableState": "Table completed", "FullLoadRows": 10},
        {"SchemaName": "DMS_SAMPLE", "TableName": "EMPTY_SEL",
         "TableState": "Table completed", "FullLoadRows": 0},
    ]
    dms = _B16Dms(tm, stats)
    s3 = _B16S3()
    rt.boto3.client = lambda svc, region_name=None: (dms if svc == "dms" else s3)
    event = {"taskArn": "arn:aws:dms:us-east-1:111111111111:task:ABC", "bucket": "mybucket",
             "region": "us-east-1", "taskSuffix": "mix", "cdc_root": "."}
    out = rt.handler_build_table_list(event, None)
    check(out["count"] == 2, "B16: the selected empty table IS included (count 2)")
    manifest = s3.puts.get("config/_task/mix/table_manifest.csv", "")
    check("empty_sel" in manifest, "B16: a selected 0-row table stays in the manifest")
    check(not out.get("ignoredUnselected"),
          "B16: nothing ignored when every reported table is selected")

# =============================================================================================
# B20 + B21 — create_glue_jobs delete-mode account resolution + bounded stop-wait (fake Glue).
#
# B20: delete/list_fork_cdc must resolve the account id even when the ASL payload omits it,
#      by falling back to the Lambda's own context.invoked_function_arn, then STS.
# B21: _wait_runs_stopped must never block past the Lambda timeout — it is bounded by
#      context.get_remaining_time_in_millis(); delete mode batch-stops a still-running job and
#      returns it under "pending" (not deleted) so the ASL loop retries it. With a fake Glue
#      where a run takes N get_job_runs calls to go from RUNNING -> STOPPED, NO single Lambda
#      call exceeds its (tiny) time budget, and looping delete eventually deletes everything.
# =============================================================================================
import importlib as _il
import time as _time


def _load_create_glue_jobs():
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    _b = types.ModuleType("boto3")
    _b.client = lambda *a, **k: None
    sys.modules["boto3"] = _b
    cgj = _il.import_module("create_glue_jobs")
    _il.reload(cgj)
    return cgj


class _FakeGlueExc:
    class EntityNotFoundException(Exception):
        pass


class _FakeGlue:
    """Minimal Glue stub. Each job's run goes RUNNING for `stop_after_calls` get_job_runs reads
    (or until batch_stop_job_run flips it immediately), then STOPPED. Records deletes + stops."""

    def __init__(self, jobs, stop_after_calls=0, stop_on_signal=True):
        # jobs: {name: calls_remaining_running}
        self._run_state = dict(jobs)
        self._stop_after = stop_after_calls
        self._stop_on_signal = stop_on_signal
        self.exceptions = _FakeGlueExc()
        self.deleted = []
        self.stopped = []
        self._calls = {}

    def get_job_runs(self, JobName=None, MaxResults=50, NextToken=None):
        self._calls[JobName] = self._calls.get(JobName, 0) + 1
        left = self._run_state.get(JobName, 0)
        state = "RUNNING" if left > 0 else "SUCCEEDED"
        if left > 0:
            self._run_state[JobName] = left - 1   # each poll moves it one step toward stopped
        return {"JobRuns": [{"Id": f"{JobName}:run", "JobRunState": state}]}

    def batch_stop_job_run(self, JobName=None, JobRunIds=None):
        self.stopped.append(JobName)
        if self._stop_on_signal:
            self._run_state[JobName] = 0          # a stop signal makes the next poll see STOPPED
        return {"SuccessfulSubmissions": [{"JobName": JobName, "JobRunId": i}
                                          for i in (JobRunIds or [])]}

    def delete_job(self, JobName=None):
        if self._run_state.get(JobName, 0) > 0:
            # Guard the invariant the fix guarantees: delete is only called on a stopped job.
            raise AssertionError(f"delete_job called on {JobName} while its run is still RUNNING")
        self.deleted.append(JobName)
        return {}

    def list_jobs(self, MaxResults=200, NextToken=None):
        return {"JobNames": []}   # delete mode uses the registry union; tag selection returns []


class _FakeS3Empty:
    """No registry object -> _read_registry returns {} (NoSuchKey). put/get for registry clear."""
    class _Exc(Exception):
        pass

    def get_object(self, Bucket=None, Key=None):
        e = Exception("missing")
        e.response = {"Error": {"Code": "NoSuchKey"}}
        raise e

    def put_object(self, **k):
        return {"ETag": '"x"'}


class _Ctx:
    def __init__(self, arn, remaining_ms):
        self.invoked_function_arn = arn
        self._rem = remaining_ms

    def get_remaining_time_in_millis(self):
        return self._rem


def test_b20_account_from_context_and_sts():
    cgj = _load_create_glue_jobs()
    ctx = _Ctx("arn:aws:lambda:us-east-1:222233334444:function:sharedtest-create-glue-jobs", 300000)
    # 1) explicit event wins
    check(cgj._account_id({"account_id": "111111111111"}, ctx) == "111111111111",
          "B20: _account_id prefers the explicit event account_id")
    # 2) dms_task_arn field 4
    check(cgj._account_id({"dms_task_arn": "arn:aws:dms:us-east-1:555566667777:task:X"}, ctx)
          == "555566667777",
          "B20: _account_id derives the account from dms_task_arn when account_id is absent")
    # 3) neither in event -> the Lambda's OWN context ARN
    check(cgj._account_id({}, ctx) == "222233334444",
          "B20: _account_id falls back to context.invoked_function_arn (account can't be empty)")
    # 4) no event, no context -> STS (cached). Patch sts.get_caller_identity.
    cgj._STS_ACCOUNT_CACHE["id"] = None

    class _Sts:
        def get_caller_identity(self):
            return {"Account": "888899990000"}
    cgj.boto3 = types.SimpleNamespace(client=lambda *a, **k: _Sts())
    check(cgj._account_id({}, None) == "888899990000",
          "B20: _account_id falls back to sts:GetCallerIdentity when event+context are empty")
    cgj._STS_ACCOUNT_CACHE["id"] = None


def test_b21_wait_runs_stopped_bounded_by_remaining_time():
    cgj = _load_create_glue_jobs()
    # A run that would take ~100 polls (never stops within the budget). With only ~1.2s of
    # Lambda time left (margin 30s), the wait must NOT loop to the Lambda timeout: it returns
    # False promptly (still active) after bounding on remaining time.
    glue = _FakeGlue({"j": 100}, stop_on_signal=False)
    ctx = _Ctx("arn:aws:lambda:us-east-1:1:function:f", 300000)  # plenty, but see below
    t0 = _time.monotonic()
    # Force the time bound to bite: remaining just above the margin so (now+delay) >= deadline
    ctx._rem = cgj._WAIT_MARGIN_MS + 500
    ok = cgj._wait_runs_stopped(glue, "j", attempts=100, delay=10, context=ctx)
    elapsed = _time.monotonic() - t0
    check(ok is False, "B21: a run still active at the time budget returns False (not blocked)")
    check(elapsed < 5, f"B21: the bounded wait returns promptly (no 100x10s block); {elapsed:.2f}s")

    # With context=None the static attempts*delay budget still applies and a run that is already
    # stopped returns True at once.
    check(cgj._wait_runs_stopped(_FakeGlue({"k": 0}), "k", attempts=3, delay=0, context=None)
          is True,
          "B21: an already-stopped run returns True (static budget path, context=None)")


def _run_delete(cgj, glue, ctx):
    """Invoke create_glue_jobs delete mode against a fake Glue + a registry listing `jobs`."""
    # Patch the registry read to return our two jobs (so delete's `found` = these names).
    jobs = sorted(glue._run_state)
    cgj._read_registry = lambda s3, b, k: ({"jobs": [{"name": n} for n in jobs]}, None)
    cgj._update_registry = lambda *a, **k: None
    cgj._select_task_jobs_by_tag = lambda *a, **k: set()
    cgj.boto3 = types.SimpleNamespace(
        client=lambda svc, region_name=None: (glue if svc == "glue" else _FakeS3Empty()))
    event = {"mode": "delete", "bucket": "b", "project": "sharedtest", "taskSuffix": "t",
             "account_id": "111111111111"}
    return cgj.handler(event, ctx)


def test_b21_delete_pending_then_loop_terminates():
    cgj = _load_create_glue_jobs()
    ctx = _Ctx("arn:aws:lambda:us-east-1:1:function:f", 300000)

    # Two composite CDC jobs whose runs take 2 polls to actually stop, but a stop SIGNAL
    # (batch_stop_job_run) flips them stopped on the next poll. Tiny time budget so the bounded
    # wait returns pending on the FIRST pass (proving no single call blocks), then the loop's
    # next pass (fresh budget) finds them stopped and deletes them.
    glue = _FakeGlue({"sharedtest-t-ck-nd-cdc": 2, "sharedtest-t-ck-sd-cdc": 2},
                     stop_on_signal=True)

    # PASS 1: starve the time budget so the wait bails to pending (and batch-stops the runs).
    ctx._rem = cgj._WAIT_MARGIN_MS + 200
    r1 = _run_delete(cgj, glue, ctx)
    check(sorted(r1.get("pending", [])) == ["sharedtest-t-ck-nd-cdc", "sharedtest-t-ck-sd-cdc"],
          "B21: delete returns both slow jobs under 'pending' (not deleted) on the first pass")
    check(r1.get("deleted") == [], "B21: nothing deleted while runs are still stopping")
    check(sorted(set(glue.stopped)) == ["sharedtest-t-ck-nd-cdc", "sharedtest-t-ck-sd-cdc"],
          "B21: delete issued batch_stop_job_run for each still-running job")
    check(glue.deleted == [], "B21: no delete_job on a running job (invariant held by the fake)")

    # PASS 2: full budget. The runs are now stopped (the stop signal flipped them) -> deleted,
    # nothing pending -> the ASL loop's AllGlueJobsDeleted takes Default -> CutoverSucceeded.
    ctx._rem = 300000
    r2 = _run_delete(cgj, glue, ctx)
    check(sorted(r2.get("deleted", [])) == ["sharedtest-t-ck-nd-cdc", "sharedtest-t-ck-sd-cdc"],
          "B21: the next delete pass (fresh budget) deletes the now-stopped jobs")
    check(r2.get("pending") == [] and r2.get("failed") == [],
          "B21: no pending/failed remain -> ASL loop terminates at CutoverSucceeded")


def test_b21_asl_cutover_loops_delete_until_empty_or_budget():
    """The cutover ASL loop: AllGlueJobsDeleted -> (pending) IncrDeleteLoop -> DeleteBudgetLeft
    -> WaitDeleteRetry -> DeleteGlueJobs, bounded by a counter; else -> CutoverSucceeded; a hard
    'failed' -> GlueJobsNotDeletedList. Static shape check so the loop can't silently regress."""
    cut = json.load(open(os.path.join(REPO, "stepfunctions", "cutover.asl.json")))
    s = cut["States"]
    for st in ("InitDeleteLoop", "DeleteGlueJobs", "AllGlueJobsDeleted", "IncrDeleteLoop",
               "DeleteBudgetLeft", "WaitDeleteRetry", "GlueJobsPendingTimedOut"):
        check(st in s, f"B21: cutover has loop state {st}")
    choices = {c.get("Next") for c in s["AllGlueJobsDeleted"].get("Choices", [])}
    check("IncrDeleteLoop" in choices,
          "B21: AllGlueJobsDeleted routes a 'pending' job back into the delete loop")
    check(s["AllGlueJobsDeleted"].get("Default") == "CutoverSucceeded",
          "B21: AllGlueJobsDeleted default (nothing pending/failed) succeeds")
    check(s["WaitDeleteRetry"]["Next"] == "DeleteGlueJobs",
          "B21: the loop waits then calls DeleteGlueJobs again")
    check(s["DeleteBudgetLeft"].get("Default") == "GlueJobsPendingTimedOut",
          "B21: past the budget the loop ends in GlueJobsNotDeleted naming the pending jobs")
    # the pending choice guards on $.deleteJobs.Payload.pending[0]
    pend = [c for c in s["AllGlueJobsDeleted"]["Choices"]
            if c.get("Next") == "IncrDeleteLoop"][0]
    check(pend.get("Variable") == "$.deleteJobs.Payload.pending[0]",
          "B21: the loop condition reads the delete Lambda's pending list")



def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== e2e-fixes: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
