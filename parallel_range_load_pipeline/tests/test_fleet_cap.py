#!/usr/bin/env python3
"""Executing tests for the fleet row cap (user decision 2026-10-16: cap = 200, not 1000).

Covers, against the REAL mrl_fleet_preflight.handler:
  - exactly 200 rows are ACCEPTED (cap is not off-by-one),
  - 201 rows are REJECTED and the rejection happens BEFORE any DMS call (no describe_replication_tasks,
    no SFN list) and BEFORE any child execution,
  - the failure message names BOTH the actual count (201) and the allowed count (200),
  - a mutation check: if the cap were restored to 1000, the 201-row rejection would stop firing.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_fleet_preflight as fp  # noqa: E402
from mrl_fakes import FakeS3      # noqa: E402

VALID_ARN = "arn:aws:dms:us-east-1:123456789012:task:TASK%08d"


class _PoisonDms:
    """Any DMS call means the cap check let execution proceed past the fleet CSV — a bug.
    Every method raises so an over-cap run that reaches DMS fails LOUDLY in the test."""
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError(
                "mrl_fleet_preflight called dms.%s past the row cap — the over-cap check must "
                "fail BEFORE any DMS call" % name)
        return _boom


def _fleet_csv(n):
    """A valid fleet CSV with n task rows (header uses the Amendment-8 `cdc` column)."""
    lines = ["task_arn"]
    lines += [VALID_ARN % i for i in range(1, n + 1)]
    return "\n".join(lines) + "\n"


def _seed(n):
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key=fp.FLEET_TASKS_KEY, Body=_fleet_csv(n).encode())
    return s3


def _install(s3):
    """Point the module's boto3 at FakeS3 for s3 and a poison client for everything else
    (dms/sfn). If the cap check runs first, dms/sfn are never touched."""
    def client(service, **kw):
        if service == "s3":
            return s3
        return _PoisonDms()
    fp.boto3 = type("b", (), {"client": staticmethod(client)})


def _event():
    return {"bucket": "b", "stateMachineArn":
            "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task"}


def test_cap_is_200():
    """The constant itself is 200 (user decision 2026-10-16)."""
    assert fp._MAX_ROWS == 200, "fleet row cap must be 200 (user decision 2026-10-16)"


def test_201_rows_rejected_before_any_dms_call():
    """201 rows -> fail closed; the message names 201 and 200; NO dms/sfn call happens."""
    s3 = _seed(201)
    _install(s3)
    try:
        fp.handler(_event(), None)
        assert False, "201 rows (> cap 200) must fail closed before anything starts"
    except fp.MrlFleetError as e:
        msg = str(e)
        assert "201" in msg, "message must name the actual row count 201: %r" % msg
        assert "200" in msg, "message must name the allowed max 200: %r" % msg
        assert "before anything starts" in msg, \
            "message must state it fails before anything starts: %r" % msg
    # (_PoisonDms would have raised AssertionError if any dms call had been attempted.)


def test_exactly_200_rows_accepted():
    """Exactly 200 rows must NOT trip the cap (boundary). 200 valid ARNs resolve past the cap
    into per-row DMS describe — so here we DO allow DMS, returning a stopped full-load task."""
    s3 = _seed(200)
    stopped = [{"Status": "stopped", "StopReason": "Stop Reason FULL_LOAD_ONLY_FINISHED",
                "MigrationType": "full-load"}]

    class _OkDms:
        def describe_replication_tasks(self, Filters=None, WithoutSettings=False):
            return {"ReplicationTasks": [dict(stopped[0],
                    ReplicationTaskArn=Filters[0]["Values"][0])]}

    def client(service, **kw):
        if service == "s3":
            return s3
        return _OkDms()
    fp.boto3 = type("b", (), {"client": staticmethod(client)})
    # Must not raise the cap error. (It may still build child inputs; we only assert the cap
    # boundary is inclusive of 200.)
    out = fp.handler(_event(), None)
    # 200 rows accepted -> 200 child task inputs built (none skipped: no running children).
    assert len(out.get("tasks", [])) == 200, \
        "exactly 200 rows must be accepted (got %d child inputs)" % len(out.get("tasks", []))


def test_mutation_restoring_1000_breaks_the_201_test():
    """Guard: if the cap regressed to 1000, 201 rows would NOT trip the row cap. Prove that by
    temporarily restoring 1000 and asserting the cap message stops firing (the run instead
    proceeds past the cap into per-row DMS resolution, producing a DIFFERENT, non-cap error)."""
    s3 = _seed(201)
    _install(s3)  # poison dms: per-row describe fails, proving we got PAST the cap check
    saved = fp._MAX_ROWS
    try:
        fp._MAX_ROWS = 1000
        cap_msg_fired = False
        got_past_cap = False
        try:
            fp.handler(_event(), None)
        except fp.MrlFleetError as e:
            m = str(e)
            if "rows (max" in m or "before anything starts" in m:
                cap_msg_fired = True
            if "describe_replication_tasks failed" in m:
                got_past_cap = True  # reached the per-row loop -> past the cap
        assert not cap_msg_fired, \
            "with cap=1000, 201 rows must NOT trip the row cap (mutation must break the 201 test)"
        assert got_past_cap, \
            "with cap=1000, 201 rows should proceed past the cap into per-row DMS resolution"
    finally:
        fp._MAX_ROWS = saved
    # and confirm the real cap value is restored to 200
    assert fp._MAX_ROWS == 200


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d fleet-cap test(s) passed" % len(fns))
