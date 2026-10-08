#!/usr/bin/env python3
"""Offline regression test for H1 (rf-06-01 / rf-06-06): the cutover validation gate — and the
startup fork-job builder — must receive and USE the DSQL endpoint candidate list, so the
VPC/no-internet customer (whose reachable DSQL host is the PRIVATE candidate, not the public
given endpoint) can run cutover and build fork jobs.

Two halves:
  1) drain_check._validation_gate must try EVERY candidate (not only the given endpoint). We stub
     boto3/pg8000 so no network is needed and record which hosts a connect was attempted on.
  2) The state machines must PASS dsql_endpoint_candidates: cutover CdcValidationPreCheck /
     CdcValidationFinalCheck and startup EnsureForkJobs payloads must carry
     "dsql_endpoint_candidates.$": "$.resolved.dsqlEndpointCandidates".

FAILS before the fix (gate connected to the given endpoint only; the three payloads omitted the
field) and PASSES after. Run directly: python3 tests/test_validation_gate_candidates.py
"""
import importlib
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
LAMBDAS = os.path.join(REPO, "lambdas")
SF = os.path.join(REPO, "stepfunctions")

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
        import os as _os
        if "PYTEST_CURRENT_TEST" in _os.environ:
            raise AssertionError(msg)


_ATTEMPTED = []


def _load_drain_check():
    sys.path.insert(0, LAMBDAS)
    # Fake boto3: S3 returns a manifest index with one table so the gate reaches the DSQL connect.
    _b = types.ModuleType("boto3")

    class _S3:
        def get_object(self, Bucket=None, Key=None):
            body = json.dumps({"tables": [{"dsql_schema": "app", "dsql_table": "t"}]}).encode()
            return {"Body": types.SimpleNamespace(read=lambda d=body: d)}

    class _Dsql:
        def generate_db_connect_admin_auth_token(self, host, Region=None, ExpiresIn=None):
            return "tok-for-" + str(host)

    def _client(service, **k):
        return _Dsql() if service == "dsql" else _S3()

    _b.client = _client
    sys.modules["boto3"] = _b

    # Fake pg8000: connecting to the FIRST host raises (simulate the public name being unreachable
    # from inside the VPC); a later candidate succeeds. Record every attempted host.
    _pg = types.ModuleType("pg8000")

    class _Conn:
        def cursor(self):
            class _C:
                def execute(self, *a, **k): pass
                def fetchone(self): return (0,)
                def fetchall(self): return []
                def close(self): pass
            return _C()

        def close(self): pass

    def _connect(host=None, **k):
        _ATTEMPTED.append(host)
        if len(_ATTEMPTED) == 1:
            raise OSError("connection timed out (public name unreachable from the VPC)")
        return _Conn()

    _pg.connect = _connect
    _pg.dbapi = types.SimpleNamespace(connect=_connect)
    sys.modules["pg8000"] = _pg
    sys.modules["pg8000.native"] = types.ModuleType("pg8000.native")

    dc = importlib.import_module("drain_check")
    importlib.reload(dc)
    return dc


def test_validation_gate_tries_candidates():
    _ATTEMPTED.clear()
    try:
        dc = _load_drain_check()
    except Exception as e:   # pragma: no cover - environment missing a dep
        check(False, f"could not import drain_check offline ({type(e).__name__}: {e})")
        return
    event = {
        "mode": "validation_gate",
        "config_prefix": "s3://b/config/_task/t/",
        "dsql_endpoint": "public.dsql.us-east-1.on.aws",
        "dsql_endpoint_candidates": "private.svc-id.us-east-1.on.aws,public.dsql.us-east-1.on.aws",
        "dsql_user": "admin", "dsql_database": "postgres", "control_schema": "cdc_control",
    }
    try:
        out = dc._validation_gate(event)
    except Exception as e:
        check(False, f"_validation_gate raised instead of failing over to a candidate: {e}")
        return
    check(len(_ATTEMPTED) >= 2,
          f"_validation_gate tried MORE than the given endpoint (failed over): {_ATTEMPTED}")
    check(any("private" in (h or "") for h in _ATTEMPTED),
          f"_validation_gate attempted the PRIVATE candidate: {_ATTEMPTED}")
    check(isinstance(out, dict) and out.get("ok") is True,
          f"_validation_gate succeeded via a candidate (ok=True): {out}")


def _payload(sm_file, state):
    d = json.load(open(os.path.join(SF, sm_file)))
    return json.dumps(d["States"][state]["Parameters"]["Payload"])


def test_asl_payloads_pass_candidates():
    want = '"dsql_endpoint_candidates.$": "$.resolved.dsqlEndpointCandidates"'
    for sm, st in [("cutover.asl.json", "CdcValidationPreCheck"),
                   ("cutover.asl.json", "CdcValidationFinalCheck"),
                   ("startup.asl.json", "EnsureForkJobs")]:
        pl = _payload(sm, st)
        check("dsql_endpoint_candidates" in pl,
              f"{sm}:{st} payload passes dsql_endpoint_candidates")
        check("$.resolved.dsqlEndpointCandidates" in pl,
              f"{sm}:{st} candidates come from $.resolved.dsqlEndpointCandidates")


def main():
    for fn in sorted(g for g in list(globals()) if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== validation-gate candidates: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
