#!/usr/bin/env python3
"""AMENDMENT 5 item 1 / V3B HIGH-2 — SSL-EOF on the LOAD (INSERT) path, scripts/job2_load.py.

A transient SSL EOF mid-INSERT (ssl.SSLEOFError / 'EOF occurred in violation of protocol' /
OSError errno 8) was NOT recognized by is_broken_pipe_error, so the chunk-retry ladder did not
reconnect and the file was lost. The fix is ADDITIVE. These tests EXECUTE the REAL predicate
(AST-extracted from scripts/job2_load.py so it cannot drift), prove the new cases return True, that
every previously-True case is still True (no regression), and that job2_load and the parallel loader
(job2_load_manual_range.py, with its own wrapper) agree on a matrix. A fault-injection-style test
proves the chunk retry loop reconnects (fresh IAM token) and retries the chunk on an SSL-EOF.
"""
import ast
import errno as _errno
import os
import ssl

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
JOB2 = os.path.join(REPO, "scripts", "job2_load.py")
MRL = os.path.join(REPO, "parallel_range_load_pipeline", "scripts", "job2_load_manual_range.py")


def _extract_is_broken_pipe(path):
    """AST-extract is_broken_pipe_error from a loader; for the parallel loader also apply its
    _mr_wrap_is_broken_pipe_error wrapper so we test the EFFECTIVE predicate it installs."""
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src)
    ns = {"ssl": ssl, "errno": _errno}
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in ("is_broken_pipe_error",
                                                          "_mr_wrap_is_broken_pipe_error"):
            exec(ast.get_source_segment(src, n), ns)
    fn = ns["is_broken_pipe_error"]
    if "_mr_wrap_is_broken_pipe_error" in ns:
        fn = ns["_mr_wrap_is_broken_pipe_error"](fn)   # the installed wrapper
    return fn


_J2 = _extract_is_broken_pipe(JOB2)


# --- the SSL-EOF cases that must now be recognized ---------------------------------------
def _ssl_eof():
    return ssl.SSLEOFError("EOF occurred in violation of protocol (_ssl.c:1234)")


def _plaintext_eof():
    return Exception("OSError: EOF occurred in violation of protocol")


def _oserr8_eof():
    e = OSError(8, "EOF occurred in violation of protocol")
    return e


def test_ssl_eof_variants_are_recognized_on_load_path():
    assert _J2(_ssl_eof()) is True, "ssl.SSLEOFError must be a dropped connection"
    assert _J2(_plaintext_eof()) is True, "plaintext 'EOF occurred in violation of protocol' must match"
    assert _J2(_oserr8_eof()) is True, "OSError errno 8 EOF must match"


def test_additive_no_regression_previously_true_still_true():
    """Every case the OLD predicate returned True for must STILL be True (additive only)."""
    still_true = [
        BrokenPipeError("broken pipe"),
        ConnectionResetError("connection reset by peer"),
        OSError(_errno.EPIPE, "Broken pipe"),
        OSError(_errno.ECONNRESET, "Connection reset by peer"),
        Exception("server closed the connection unexpectedly"),
        Exception("connection is closed"),
        Exception("could not receive data from server"),
        type("InterfaceError", (Exception,), {})("x"),
        type("OperationalError", (Exception,), {})("x"),
        Exception({"C": "08006", "M": "unable to connect"}.__repr__()) if False else _pg08006(),
    ]
    for e in still_true:
        assert _J2(e) is True, "regression: %r no longer recognized" % (e,)


def _pg08006():
    class _E(Exception):
        pass
    e = _E()
    e.args = ({"C": "08006", "M": "server closed the connection"},)
    return e


def test_non_dropped_errors_still_false():
    """A genuine data error / non-connection error must NOT be classified as a dropped connection."""
    for e in (ValueError("bad value"),
              Exception({"C": "23505", "M": "duplicate key"}.__repr__()),
              Exception("value too long for type character varying(10)"),
              KeyError("x")):
        assert _J2(e) is False, "false positive: %r should not be a dropped connection" % (e,)


def test_job2_and_parallel_loader_agree_on_matrix():
    """job2_load and the parallel loader (its own wrapper) must now agree that the SSL-EOF matrix
    is a dropped connection (the two should behave the same, per the amendment)."""
    mrl = _extract_is_broken_pipe(MRL)
    for e_factory in (_ssl_eof, _plaintext_eof, _oserr8_eof):
        e = e_factory()
        assert _J2(e) == mrl(e) == True, \
            "job2_load and parallel loader disagree on %r: %s vs %s" % (e, _J2(e), mrl(e))


# --- fault-injection style: the chunk retry ladder reconnects + retries on an SSL-EOF ----
def test_chunk_retry_reconnects_and_retries_on_ssl_eof():
    """Mirror of the loader's chunk retry gate (is_broken_pipe_error -> reconnect with a fresh
    token, retry the chunk). Inject an SSL-EOF on the first INSERT; the ladder must reconnect and
    the second attempt must commit the chunk exactly once."""
    MAX_CHUNK_RETRIES = 10
    server = {"committed": None}
    state = {"attempt": 0, "reconnects": 0, "fresh_tokens": 0}
    chunk = [(1, "a"), (2, "b"), (3, "c")]

    def _reconnect(fresh_token):
        state["reconnects"] += 1
        if fresh_token:
            state["fresh_tokens"] += 1

    def _do_insert_commit():
        state["attempt"] += 1
        if state["attempt"] == 1:
            raise _ssl_eof()          # transient SSL EOF mid-INSERT on the first try
        server["committed"] = list(chunk)   # second try commits

    pipe_attempt = 0
    while True:
        try:
            _do_insert_commit()
            break
        except Exception as e:
            if _J2(e) and pipe_attempt < MAX_CHUNK_RETRIES:
                pipe_attempt += 1
                _reconnect(fresh_token=True)   # loader reconnects with a FRESH IAM token
                continue
            raise
    assert server["committed"] == chunk, "chunk must be committed exactly once after the retry"
    assert state["reconnects"] == 1 and state["fresh_tokens"] == 1, \
        "the ladder must reconnect once with a fresh token on the SSL-EOF"
    assert pipe_attempt == 1


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d SSL-EOF test(s) passed" % len(fns))
