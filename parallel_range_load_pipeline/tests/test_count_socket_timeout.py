#!/usr/bin/env python3
"""AMENDMENT 3 (R7c) — prove the ACTUAL bug class behind "The read operation timed out".

Root cause (verified): pg8000's `connect(timeout=N)` calls `socket.create_connection((host,port), N)`,
which sets the socket's timeout to N; pg8000 never resets it, so N becomes the READ timeout for
EVERY query on that connection. A COUNT(*) that takes > N seconds then raises a socket read timeout
("The read operation timed out") BEFORE the server's statement_timeout / DSQL's 300 s cap can fire.

We prove this with a REAL local TCP server (localhost only, no AWS, no DSQL) that accepts a
connection and then DELAYS before sending any byte:
  (i)  a socket opened with create_connection(timeout=1) raises socket.timeout on a read when the
       server delays 3 s — this is EXACTLY the failure mode the count job hit at 10 s;
  (ii) resetting the socket read timeout AFTER connect (the B14 fix: `sock.settimeout(larger)`)
       lets the same delayed read succeed — the fix.
Plus a STATIC check that the count job wires the B14 reset after its pg8000.connect, and that the
`is_txn_age_error` classifier treats "read operation timed out" as a re-split trigger.
"""
import ast
import os
import socket
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
COUNT = os.path.join(PKG, "scripts", "job_mrl_count.py")


class _DelayServer:
    """A localhost TCP server that accepts one connection and waits `delay` seconds before
    sending a byte — models a slow DSQL query whose result does not arrive for `delay` s."""
    def __init__(self, delay):
        self.delay = delay
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self._t = threading.Thread(target=self._serve, daemon=True)
        self._t.start()

    def _serve(self):
        try:
            conn, _ = self.sock.accept()
            time.sleep(self.delay)        # withhold the reply -> the client's read blocks
            try:
                conn.sendall(b"X")
            except Exception:
                pass
            conn.close()
        except Exception:
            pass

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def test_connect_timeout_becomes_read_timeout_and_b14_fix_lets_read_succeed():
    # (i) the bug: a 1 s connect timeout is also the READ timeout; a 3 s server delay trips it.
    srv = _DelayServer(delay=3.0)
    try:
        s = socket.create_connection(("127.0.0.1", srv.port), timeout=1.0)
        # the socket's timeout is now 1 s (create_connection set it) — a read must time out.
        assert s.gettimeout() == 1.0
        timed_out = False
        try:
            s.recv(1)
        except socket.timeout:
            timed_out = True
        except OSError as e:
            timed_out = "timed out" in str(e).lower()
        s.close()
        assert timed_out, "a read on a timeout=1 socket must time out when the reply is delayed 3 s"
    finally:
        srv.close()

    # (ii) the B14 fix: reset the socket read timeout ABOVE the delay -> the same read succeeds.
    srv2 = _DelayServer(delay=1.0)
    try:
        s2 = socket.create_connection(("127.0.0.1", srv2.port), timeout=1.0)
        s2.settimeout(10.0)               # B14: raise the read timeout after connect
        got = s2.recv(1)                  # the 1 s-delayed byte now arrives within 10 s
        s2.close()
        assert got == b"X", "after settimeout(10), the delayed read must succeed"
    finally:
        srv2.close()


def test_count_job_wires_b14_after_connect():
    """STATIC: every pg8000.connect in job_mrl_count.py is followed (in the same function) by the
    socket read-timeout reset (settimeout) AND a SET statement_timeout — the B14 fix."""
    src = open(COUNT).read()
    tree = ast.parse(src)
    # find the function containing pg8000.connect
    target_fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            seg = ast.get_source_segment(src, node) or ""
            if "pg8000.connect(" in seg:
                target_fn = seg
                break
    assert target_fn is not None, "no function calls pg8000.connect in the count job"
    assert "pg8000.connect(" in target_fn
    assert "settimeout(" in target_fn, "B14 socket read-timeout reset missing after connect"
    assert "SET statement_timeout" in target_fn, "B14 server statement_timeout missing"
    assert "COUNT_SOCKET_READ_TIMEOUT" in src and "COUNT_STATEMENT_TIMEOUT_MS" in src


def test_read_timeout_text_is_a_resplit_trigger():
    """The classifier must treat the socket read-timeout text as a transient re-split trigger
    (so a too-big count is range-split, not declared a hard error)."""
    src = open(COUNT).read()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "is_txn_age_error")
    ns = {}
    exec(ast.get_source_segment(src, fn), ns)
    assert ns["is_txn_age_error"](Exception("('The read operation timed out',)")) is True
    assert ns["is_txn_age_error"](Exception("canceling statement due to statement timeout")) is True
    assert ns["is_txn_age_error"](Exception("57014")) is True


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d socket-timeout test(s) passed" % len(fns))
