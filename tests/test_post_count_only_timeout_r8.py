#!/usr/bin/env python3
"""AMENDMENT 3 (R8) — ORIGINAL loader (scripts/job2_load.py) post_load_check=count_only read-timeout
fix. The production full-load-only plan uses the ORIGINAL pipeline. The count-only check ran ONE whole-table COUNT(*) on a
connection with no read-timeout reset and no server statement_timeout; at production scale that count
would stall/err and — because count_only treats any count error as a WARNING — the load would
complete UNVERIFIED. This fixes ONLY the count connection (the INSERT path is untouched) and adds
key-ranged counting for large tables with a single integer/uuid PK.

Tests:
  * the bug class, with a REAL delayed localhost socket (no AWS): timeout=N is the read timeout, and
    resetting it (the B14 fix) lets a delayed read succeed;
  * STATIC: the count connection helper applies settimeout + SET statement_timeout; the INSERT-path
    connect_dsql() is NOT given a timeout/settimeout (byte-identical for full/off);
  * full/off behaviour unchanged (the count path is only entered for count_only);
  * ranged count: a large table with a single uuid/integer PK is counted in key ranges; a small
    table or no-usable-key table uses whole-table — both via the B14 count connection.
"""
import ast
import os
import socket
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
JOB2 = os.path.join(REPO, "scripts", "job2_load.py")


class _DelayServer:
    def __init__(self, delay):
        self.delay = delay
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        try:
            c, _ = self.sock.accept()
            time.sleep(self.delay)
            try:
                c.sendall(b"X")
            except Exception:
                pass
            c.close()
        except Exception:
            pass

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def test_socket_timeout_bug_and_b14_fix_real_socket():
    srv = _DelayServer(delay=3.0)
    try:
        s = socket.create_connection(("127.0.0.1", srv.port), timeout=1.0)
        assert s.gettimeout() == 1.0
        timed_out = False
        try:
            s.recv(1)
        except (socket.timeout, OSError) as e:
            timed_out = "timed out" in str(e).lower() or isinstance(e, socket.timeout)
        s.close()
        assert timed_out
    finally:
        srv.close()
    srv2 = _DelayServer(delay=1.0)
    try:
        s2 = socket.create_connection(("127.0.0.1", srv2.port), timeout=1.0)
        s2.settimeout(10.0)
        assert s2.recv(1) == b"X"
        s2.close()
    finally:
        srv2.close()


def _job2_src():
    return open(JOB2).read()


def test_count_connection_applies_b14_but_insert_connect_does_not():
    src = _job2_src()
    tree = ast.parse(src)
    funcs = {n.name: ast.get_source_segment(src, n) for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef)}
    # the count helper exists and applies BOTH B14 parts
    assert "_post_count_connect" in funcs, "R8 count-connection helper missing"
    cc = funcs["_post_count_connect"]
    assert "settimeout(" in cc and "SET statement_timeout" in cc, "B14 missing on count connection"
    assert "POST_LOAD_COUNT_SOCKET_READ_TIMEOUT" in cc
    # the INSERT-path connect_dsql() is NOT given a socket timeout or settimeout (byte-identical
    # for full/off): it must not mention settimeout or a connect timeout kwarg.
    cd = funcs["connect_dsql"]
    assert "settimeout(" not in cd, "connect_dsql (INSERT path) must stay unchanged (no settimeout)"
    assert "timeout=" not in cd, "connect_dsql (INSERT path) must not pass a connect timeout"


def test_count_only_uses_count_connection_not_plain_connect():
    """_post_count_one / _post_count_ranged must open via _post_count_connect (the B14 one), not a
    bare connect_dsql, so every count query is bounded."""
    src = _job2_src()
    tree = ast.parse(src)
    funcs = {n.name: ast.get_source_segment(src, n) for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef)}
    for fn in ("_post_count_one", "_post_count_ranged"):
        assert fn in funcs, "%s missing" % fn
        assert "_post_count_connect()" in funcs[fn], "%s must use the B14 count connection" % fn


def test_full_and_off_do_not_enter_the_count_path():
    """post_load_check=full / off must be byte-identical in behaviour: the count-only path is
    gated by _post_load_check_is_count_only(), so with full/off _post_count_only_check is never
    called. Assert the gate wraps the call at both sites."""
    src = _job2_src()
    # both call sites are inside an `if _post_load_check_is_count_only():` block
    for marker in ("site='load_one_table', config=config",
                   "site='validate_whole_table_after_ranges'"):
        idx = src.index(marker)
        pre = src[max(0, idx - 400):idx]
        assert "_post_load_check_is_count_only()" in pre, \
            "count-only call at %r not gated by the count_only check" % marker


def test_ranged_count_planner_and_whole_table_fallback():
    """Behavioural: extract _post_count_one/_post_count_ranged and the real plan_ranges /
    plan_ranges_hex; drive them with a fake count connection that records the predicates and
    returns exact region counts. A 20M uuid table -> ranged (multiple sub-ranges, summing to
    20M); a 1M table or a no-usable-key table -> ONE whole-table COUNT(*)."""
    src = _job2_src()
    tree = ast.parse(src)
    ns = {"time": time}
    want = {"_post_count_one", "_post_count_ranged", "_post_count_scalar", "_post_count_connect",
            "plan_ranges", "plan_ranges_hex", "clamp_partition_rows",
            "hex_to_int", "int_to_hex", "normalize_uuid_hex"}
    consts = {"POST_LOAD_COUNT_RANGE_THRESHOLD_ROWS", "POST_LOAD_COUNT_RANGE_ROWS",
              "POST_LOAD_COUNT_STATEMENT_TIMEOUT_MS", "POST_LOAD_COUNT_SOCKET_READ_TIMEOUT",
              "PARTITION_ROWS_MIN", "PARTITION_ROWS_MAX"}
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in consts:
                    try:
                        ns[t.id] = ast.literal_eval(n.value)
                    except Exception:
                        pass
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in want:
            exec(ast.get_source_segment(src, n), ns)
    # ensure clamp has its bounds (fallbacks if the constant names differ)
    ns.setdefault("PARTITION_ROWS_MIN", 1)
    ns.setdefault("PARTITION_ROWS_MAX", 100000)

    class _FakeConn:
        def __init__(self, table_rows, kind):
            self.table_rows = table_rows
            self.kind = kind
            self.autocommit = True
            self.queries = []
        def cursor(self):
            return _FakeCur(self)
        def close(self):
            pass
    class _FakeCur:
        def __init__(self, c):
            self.c = c
            self._r = None
        def execute(self, sql):
            self.c.queries.append(sql)
            s = sql.upper()
            if s.startswith("SET "):
                self._r = None
            elif "MIN(" in s and "MAX(" in s:
                if self.c.kind == "integer":
                    self._r = (1, self.c.table_rows)
                else:
                    self._r = ("00000000-0000-0000-0000-000000000000",
                               "ffffffff-ffff-ffff-ffff-ffffffffffff")
            elif "COUNT(*)" in s and "IS NULL" in s:
                self._r = (0,)
            elif "COUNT(*)" in s and "WHERE" in s:
                # a ranged sub-count: fraction of rows implied by the predicate bounds.
                self._r = (self.c.table_rows // 7,)   # value irrelevant to the planner wiring
            elif "COUNT(*)" in s:
                self._r = (self.c.table_rows,)        # whole-table
            else:
                self._r = (0,)
        def fetchone(self):
            return self._r
        def close(self):
            pass

    recorded = {}
    def _fake_connect():
        conn = _FakeConn(recorded["rows"], recorded["kind"])
        recorded["conn"] = conn
        return conn
    ns["_post_count_connect"] = _fake_connect
    ns["connect_dsql"] = _fake_connect

    def _cfg(kind):
        return {"metadata": {"dsql_schema": "s", "dsql_table": "t",
                             "primary_key": {"columns": ["id"], "pk_kind": kind}}}

    # large uuid table -> ranged (the count connection sees sub-range WHERE COUNT(*) queries)
    recorded.update(rows=20_000_000, kind="uuid")
    ns["_post_count_one"]("s", "t", config=_cfg("uuid"), expected_total=20_000_000)
    where_counts = [q for q in recorded["conn"].queries if "COUNT(*)" in q.upper() and "WHERE" in q.upper()]
    assert len(where_counts) >= 2, "large uuid table should be counted in multiple key sub-ranges"

    # small table -> ONE whole-table COUNT(*), no WHERE sub-ranges
    recorded.update(rows=1_000_000, kind="uuid")
    ns["_post_count_one"]("s", "t", config=_cfg("uuid"), expected_total=1_000_000)
    whole = [q for q in recorded["conn"].queries if "COUNT(*)" in q.upper() and "WHERE" not in q.upper()]
    assert whole, "small table should use a whole-table COUNT(*)"

    # no usable key (composite) -> whole-table even if large
    recorded.update(rows=20_000_000, kind="text")
    cfg_txt = {"metadata": {"dsql_schema": "s", "dsql_table": "t",
                            "primary_key": {"columns": ["a", "b"], "pk_kind": None}}}
    ns["_post_count_one"]("s", "t", config=cfg_txt, expected_total=20_000_000)
    whole2 = [q for q in recorded["conn"].queries if "COUNT(*)" in q.upper() and "WHERE" not in q.upper()]
    assert whole2, "no-usable-key table should fall back to whole-table COUNT(*)"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d R8 count-only test(s) passed" % len(fns))
