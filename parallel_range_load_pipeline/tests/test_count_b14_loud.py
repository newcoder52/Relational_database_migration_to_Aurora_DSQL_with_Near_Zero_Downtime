#!/usr/bin/env python3
"""AMENDMENT 3 (R10 F-Q2) — a skipped B14 socket-read-timeout reset must be LOUD, not silent.

If a future pg8000 moves `_usock`, the reset is skipped and the connection silently reverts to the
10 s connect-probe read timeout (the exact bug). This test loads the REAL job_mrl_count.connect_dsql
under stubbed boto3/pg8000/awsglue, injects a fake connection object with NO `_usock`, and asserts
(a) a WARN line is printed and (b) the connection is still returned (the count still runs).
"""
import contextlib
import io
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
COUNT_DIR = os.path.join(PKG, "scripts")


def _load_count_module():
    """Import parallel_range_load_pipeline/scripts/job_mrl_count.py under minimal stubs."""
    saved_mods = {}
    for name in ("boto3", "pg8000", "awsglue", "awsglue.utils"):
        saved_mods[name] = sys.modules.get(name)
    # boto3.client(...).generate_db_connect_admin_auth_token(...)
    b = types.ModuleType("boto3")
    b.client = lambda *a, **k: types.SimpleNamespace(
        generate_db_connect_admin_auth_token=lambda *a, **k: "tok")
    sys.modules["boto3"] = b
    pg = types.ModuleType("pg8000")
    pg.connect = lambda *a, **k: None   # replaced per-test below
    sys.modules["pg8000"] = pg
    ag = types.ModuleType("awsglue")
    agu = types.ModuleType("awsglue.utils")
    agu.getResolvedOptions = lambda argv, opts: {o: "x" for o in opts}
    ag.utils = agu
    sys.modules["awsglue"] = ag
    sys.modules["awsglue.utils"] = agu

    saved_argv = sys.argv
    sys.argv = ["job_mrl_count.py", "--JOB_NAME", "mrl-count",
                "--config_prefix", "s3://b/config/_task/t/",
                "--dsql_endpoint", "c.dsql.us-east-1.on.aws", "--dsql_user", "admin",
                "--dsql_database", "postgres", "--region", "us-east-1"]
    saved_path = list(sys.path)
    sys.path.insert(0, COUNT_DIR)
    try:
        import importlib
        if "job_mrl_count" in sys.modules:
            del sys.modules["job_mrl_count"]
        mod = importlib.import_module("job_mrl_count")
        return mod
    finally:
        sys.argv = saved_argv
        sys.path[:] = saved_path
        for name, old in saved_mods.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


class _FakeCursorOK:
    def execute(self, *a, **k):
        return None
    def close(self):
        return None


class _FakeConnNoUsock:
    """A pg8000-like connection object WITHOUT `_usock` (and no `_c`), so the B14 reset is
    skipped — which must now print a WARN."""
    autocommit = True
    def cursor(self):
        return _FakeCursorOK()
    def close(self):
        return None


def test_missing_usock_is_loud_and_count_still_connects():
    mod = _load_count_module()
    # force the pinned-host path so connect_dsql just calls _make(host) once
    mod._PINNED_HOST["host"] = "c.dsql.us-east-1.on.aws"
    mod.pg8000.connect = lambda *a, **k: _FakeConnNoUsock()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        conn = mod.connect_dsql()
    out = buf.getvalue()
    assert isinstance(conn, _FakeConnNoUsock), "connect_dsql must still return a usable connection"
    assert "WARN B14" in out and "_usock" in out, \
        "a skipped socket read-timeout reset must print a LOUD WARN B14 line; got: %r" % out


def test_set_statement_timeout_failure_is_loud():
    """If SET statement_timeout raises, that too must be LOUD (not silently swallowed)."""
    mod = _load_count_module()
    mod._PINNED_HOST["host"] = "c.dsql.us-east-1.on.aws"

    class _CurRaises:
        def execute(self, *a, **k):
            raise RuntimeError("GUC rejected")
        def close(self):
            return None

    class _ConnWithUsock:
        autocommit = True
        _usock = type("S", (), {"settimeout": lambda self, v: None})()
        def cursor(self):
            return _CurRaises()
        def close(self):
            return None

    mod.pg8000.connect = lambda *a, **k: _ConnWithUsock()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        conn = mod.connect_dsql()
    out = buf.getvalue()
    assert conn is not None
    assert "WARN B14" in out and "statement_timeout" in out, \
        "a failed SET statement_timeout must print a LOUD WARN B14 line; got: %r" % out


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d B14-loud test(s) passed" % len(fns))
