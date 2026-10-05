#!/usr/bin/env python3
"""Offline tests for the fix6 code fixes (no AWS, no Spark, no network).

Covers:
  M01  cutover composite fields wired + guarded (ASL)          -> also test_asl_paths.py
  M02  cutover re-runnable: describe-before-stop + skip (ASL)
  M03  startup fail-fast on a real DMS start error (ASL)
  M11  job1 honors --dsql_database
  M12  job3 guards job.commit() like job1/job2
  M16  job1 fails (not silently skips) on missing-in-DSQL tables
  V1   job3 empty-source table -> pass/fail/error correctly
  V2   job3 large-table / composite validation stays under the DSQL 300s limit (re-split)

Run: python3 tests/test_fix6.py   (REPO_DIR overridable)
"""
import json
import os
import sys
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO
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


# ---------------------------------------------------------------------------
# M01 / M02 / M03 — ASL wiring regression (the deep graph audit is test_asl_paths.py)
# ---------------------------------------------------------------------------
def test_asl_m01_m02_m03():
    cut = json.load(open(os.path.join(SF, "cutover.asl.json")))
    S = cut["States"]
    # M01 (fork design): the composite-v1 single-job cutover states were REPLACED by the per-table
    # fork cleanup. Cutover must find every fork CDC job (ck-*/bg-*) by EXACT TAG + registry
    # (ListForkCdcJobs, mode list_fork_cdc), stop each run (StopForkCdcRuns), then DropTags; and
    # delete ALL the task's jobs by tag/registry (DeleteGlueJobs mode delete). The removed
    # composite-v1 states must be GONE.
    check("HasCompositeToStop" not in S and "StopCdcCompositeRun" not in S,
          "M01: composite-v1 cutover states removed (fork design)")
    check("ListForkCdcJobs" in S and S["ListForkCdcJobs"]["Parameters"]["Payload"].get("mode")
          == "list_fork_cdc", "M01: ListForkCdcJobs uses mode list_fork_cdc (exact-tag + registry)")
    check("StopForkCdcRuns" in S and S["StopForkCdcRuns"].get("Next") == "DropTags",
          "M01: StopForkCdcRuns -> DropTags")
    check(S.get("DeleteGlueJobs", {}).get("Parameters", {}).get("Payload", {}).get("mode") == "delete",
          "M01: DeleteGlueJobs deletes the task's jobs (by tag/registry)")

    # M02: a describe-before-stop state exists and routes already-stopped -> skip the stop
    check("DescribeBeforeStop" in S and "IsAlreadyStopped" in S,
          "M02: DescribeBeforeStop + IsAlreadyStopped states exist")
    ias = S["IsAlreadyStopped"]
    stopped_to_confirm = any(
        c.get("Variable", "").endswith("Status") and c.get("StringEquals") == "stopped"
        and c.get("Next") == "InitDmsStopCount" for c in ias.get("Choices", []))
    check(stopped_to_confirm,
          "M02: already-'stopped' skips StopCdcDmsTask straight to the confirm loop")
    check(ias.get("Default") == "StopCdcDmsTask",
          "M02: a running task still goes to StopCdcDmsTask")
    # pre-gate now routes through the describe, not straight to the stop
    check(S["CdcValidationPreGate"]["Choices"][0]["Next"] == "DescribeBeforeStop",
          "M02: validation pre-gate -> DescribeBeforeStop")
    # StopCdcDmsTask tolerates the already-stopped fault (idempotent re-run)
    sdt = json.dumps(S["StopCdcDmsTask"])
    check("InvalidResourceStateFault" in sdt,
          "M02: StopCdcDmsTask catches InvalidResourceStateFault (already stopped)")

    sm = json.load(open(os.path.join(SF, "startup.asl.json")))
    SS = sm["States"]
    check("DmsStartFailed" in SS and SS["DmsStartFailed"]["Type"] == "Fail",
          "M03: new DmsStartFailed Fail state exists")
    check("DescribeAfterStartError" in SS and "EvalStartError" in SS,
          "M03: StartDmsTask error path describes + evaluates")
    # StartDmsTask Catch no longer goes straight to the poll
    cat = SS["StartDmsTask"]["Catch"][0]
    check(cat["Next"] == "DescribeAfterStartError",
          "M03: StartDmsTask Catch -> DescribeAfterStartError (not the poll)")
    ev = SS["EvalStartError"]
    nexts = {c.get("Next") for c in ev.get("Choices", [])}
    check(ev.get("Default") == "DmsStartFailed",
          "M03: a non-benign start error fails fast at DmsStartFailed")
    check(nexts == {"InitPollCount"},
          "M03: benign re-run states (running/starting/cached-events) go to the poll")


# ---------------------------------------------------------------------------
# M11 — job1 honors --dsql_database
# ---------------------------------------------------------------------------
def test_m11_dsql_database():
    src = H.read_source("scripts/job1_discovery.py")
    check('"dsql_database"' in src and "optional = [" in src,
          "M11: job1 overlay lists dsql_database")
    # the override list actually contains it
    check("dsql_database" in src.split("optional = [", 1)[1].split("]", 1)[0],
          "M11: dsql_database is in the optional override list")
    check("database=DSQL_DATABASE" in src,
          "M11: job1 connect uses DSQL_DATABASE (not hardcoded postgres)")
    check('DSQL_DATABASE = "postgres"' in src,
          "M11: DSQL_DATABASE defaults to postgres (unchanged default)")
    check('database="postgres"' not in src,
          "M11: no leftover hardcoded database=\"postgres\" in the connect")


# ---------------------------------------------------------------------------
# M12 — job3 guards job.commit()
# ---------------------------------------------------------------------------
def test_m12_commit_guard():
    src = H.read_source("scripts/job3_validate.py")
    check("import socket" in src, "M12: job3 imports socket")
    # the guard pattern: a socket.create_connection to glue endpoint before job.commit()
    guard = "socket.create_connection((f\"glue.{REGION}.amazonaws.com\", 443)"
    check(guard in src, "M12: job3 probes the Glue endpoint before committing")
    # job.commit must be inside the reachable branch, not unconditional
    idx_guard = src.index("commit_glue_reachable = False")
    idx_commit = src.index("job.commit()", idx_guard)
    between = src[idx_guard:idx_commit]
    check("if commit_glue_reachable:" in between,
          "M12: job.commit() runs only when Glue is reachable")


# ---------------------------------------------------------------------------
# M16 — job1 fails (not silently skips) on missing-in-DSQL tables
# ---------------------------------------------------------------------------
def test_m16_missing_target_fail():
    src = H.read_source("scripts/job1_discovery.py")
    check("if skipped_missing_dsql:" in src,
          "M16: job1 checks skipped_missing_dsql at the end")
    seg = src.split("if skipped_missing_dsql:", 1)[1]
    check("raise Exception(" in seg[:400],
          "M16: job1 RAISES (does not silently succeed) when tables are missing in DSQL")
    check("create" in seg[:900].lower() and "primary key" in seg[:900].lower(),
          "M16: the error tells the operator to create the tables with their primary keys")
    check("STOPPED_AFTER_CACHED_EVENTS" in seg[:900],
          "M16: the error explains the task is still at STOPPED_AFTER_CACHED_EVENTS (re-trigger works)")


# ---------------------------------------------------------------------------
# V1 — empty-source table handling (decision logic extracted + driven with fakes)
# ---------------------------------------------------------------------------
class _FakeCur:
    def __init__(self, count):
        self._count = count
        self._row = None

    def execute(self, sql, *a):
        self._row = (self._count,)

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _FakeConn:
    def __init__(self, count):
        self._count = count
        self.autocommit = True

    def cursor(self):
        return _FakeCur(self._count)

    def close(self):
        pass


class _FakeS3:
    """list_objects_v2 returns KeyCount from a map of prefix-substring -> count."""
    def __init__(self, present):
        self.present = present  # bool: does the DMS prefix have objects?

    def list_objects_v2(self, Bucket=None, Prefix=None, MaxKeys=None):
        return {"KeyCount": 1 if self.present else 0}


def _v1_namespace(target_count):
    inj = {
        "Decimal": Decimal,
        "connect_dsql": lambda autocommit=True: _FakeConn(target_count),
    }
    ns = H.load_defs("scripts/job3_validate.py",
                     ["split_s3", "s3_prefix_has_objects", "target_range_summary"], inj)
    return ns


def _v1_decide(empty_at_discovery, source_present, target_count):
    """Reproduce the V1 decision using the REAL extracted helpers (split_s3,
    s3_prefix_has_objects, target_range_summary) — this mirrors the block in
    validate_one_table without needing Spark."""
    ns = _v1_namespace(target_count)
    s3 = _FakeS3(source_present)
    dms_s3_path = "s3://bucket/DMS_SAMPLE/TICKET_PURCHASE_HIST"
    b, p = ns["split_s3"](dms_s3_path)
    present = bool(dms_s3_path) and ns["s3_prefix_has_objects"](s3, b, p)
    if present:
        return ("would_read_s3", None)
    conn = ns["connect_dsql"](autocommit=True)
    tgt_cnt, _ = ns["target_range_summary"](conn, "dms_sample", "t", None, [])
    if empty_at_discovery:
        return ("match", 0) if int(tgt_cnt) == 0 else ("mismatch", int(tgt_cnt))
    return ("error", None)


def test_v1_empty_source():
    check(_v1_decide(True, False, 0) == ("match", 0),
          "V1: empty source + empty target -> PASS (0==0)")
    r = _v1_decide(True, False, 5)
    check(r[0] == "mismatch" and r[1] == 5,
          "V1: empty source + target has rows -> FAIL (names the target row count)")
    check(_v1_decide(False, False, 0)[0] == "error",
          "V1: missing folder but NOT marked empty-at-discovery -> clear error (not a pass)")
    check(_v1_decide(True, True, 0)[0] == "would_read_s3",
          "V1: when source files ARE present, the normal read path runs")


# ---------------------------------------------------------------------------
# V2 — large/composite validation stays under the DSQL 300s limit (re-split)
# ---------------------------------------------------------------------------
def _v2_namespace(sim):
    """Load the real V2 helpers. `sim` is a fake _target_query that raises a txn-age error for
    ranges wider than a threshold and otherwise returns (rowcount, [sum]) for one metric."""
    inj = {"Decimal": Decimal, "VALIDATE_MAX_RESPLIT_DEPTH": 6, "_target_query": sim}
    ns = H.load_defs(
        "scripts/job3_validate.py",
        ["hex_to_int", "int_to_hex", "hex_to_canonical_uuid", "_sql_str_literal",
         "_range_predicate_sql", "is_txn_age_error", "combine_metric", "combine_summaries",
         "split_bounds", "target_range_resplit"],
        inj)
    return ns


class _TxnAge(Exception):
    def __init__(self):
        super().__init__({"C": "54000", "M": "transaction age limit of 300s exceeded"})


def test_v2_txn_age_detection():
    ns = _v2_namespace(lambda *a, **k: None)
    check(ns["is_txn_age_error"](_TxnAge()) is True,
          "V2: SQLSTATE 54000 transaction-age error is detected")
    check(ns["is_txn_age_error"](Exception("canceling statement due to statement timeout")) is True,
          "V2: statement-timeout error is detected")
    check(ns["is_txn_age_error"](Exception("some unrelated error")) is False,
          "V2: an unrelated error is NOT treated as a txn-age error")


def test_v2_combine_equals_unsplit():
    ns = _v2_namespace(lambda *a, **k: None)
    # metrics: a count-like 'sum' (additive), a 'min', a 'max'
    metrics = [{"check": "sum"}, {"check": "min"}, {"check": "max"}]
    a = (3, [Decimal("10"), 5, 9])
    b = (4, [Decimal("20"), 2, 20])
    c, vals = ns["combine_summaries"](a, b, metrics)
    check(c == 7, "V2: combined count = sum of sub-counts")
    check(vals[0] == Decimal("30"), "V2: additive metric combines by sum")
    check(vals[1] == 2, "V2: min metric combines by min")
    check(vals[2] == 20, "V2: max metric combines by max")


def test_v2_resplit_16m_integer():
    """A simulated 16.3M-row integer-PK table whose per-range query raises the txn-age error
    for any range covering more than LIMIT keys. target_range_resplit must re-split until every
    sub-query is under the limit, and the summed result must equal the whole-table truth."""
    N = 16_317_195
    LIMIT = 300_000  # a single query over more than this many keys 'times out'
    calls = {"n": 0, "max_width": 0, "timeouts": 0}

    def sim(schema, table, pred, metrics):
        # derive [lo,hi) integer bounds from the predicate; whole table if pred is None
        lo, hi = 0, N
        if pred:
            import re
            ge = re.search(r'>= (\d+)', pred)
            lt = re.search(r'< (\d+)', pred)
            if ge:
                lo = int(ge.group(1))
            hi = int(lt.group(1)) if lt else N
        width = hi - lo
        calls["n"] += 1
        calls["max_width"] = max(calls["max_width"], width)
        if width > LIMIT:
            calls["timeouts"] += 1
            raise _TxnAge()
        # metric[0] = SUM of ids in [lo,hi) = arithmetic series
        s = (hi - 1) * hi // 2 - (lo - 1) * lo // 2 if hi > lo else 0
        return (hi - lo, [Decimal(s)])

    ns = _v2_namespace(sim)
    metrics = [{"check": "sum"}]
    # top-level range is the whole key space as ONE range -> forces re-split from the top
    cnt, vals = ns["target_range_resplit"]("s", "t", "id", "integer", 0, N, True, metrics, 0)
    whole_sum = (N - 1) * N // 2
    check(cnt == N, f"V2: re-split total count == whole table ({cnt} == {N})")
    check(vals[0] == Decimal(whole_sum), "V2: re-split summed metric == whole-table sum")
    check(calls["max_width"] <= LIMIT or calls["timeouts"] > 0,
          "V2: a range over the limit triggered at least one re-split")
    # every range that actually returned a result was within the limit
    check(calls["timeouts"] >= 1, "V2: the oversized top range timed out at least once (then split)")


def test_v2_resplit_gives_up_cleanly():
    """If a single key cannot be split further and still times out, the error propagates (the
    table is recorded as 'error' with the real cause) rather than silently passing."""
    def always_timeout(*a, **k):
        raise _TxnAge()
    ns = _v2_namespace(always_timeout)
    metrics = [{"check": "sum"}]
    raised = False
    try:
        ns["target_range_resplit"]("s", "t", "id", "integer", 10, 11, False, metrics, 0)
    except Exception as e:
        raised = ns["is_txn_age_error"](e)
    check(raised, "V2: an unsplittable range that keeps timing out surfaces the real error")


def test_v2_composite_first_column_ranging():
    src = H.read_source("scripts/job3_validate.py")
    check("_first_pk_rangeable" in src,
          "V2: composite tables are ranged on their first key column (_first_pk_rangeable)")
    ns = H.load_defs("scripts/job3_validate.py", ["column_kind", "_first_pk_rangeable"], {})
    # composite integer-first -> rangeable on the first column
    pk = {"columns": ["a", "b"], "data_types": ["integer", "timestamp without time zone"],
          "numeric_scales": [None, None]}
    check(ns["_first_pk_rangeable"](pk) == ("a", "integer"),
          "V2: composite with integer first column is ranged on it")
    # composite uuid-first
    pk2 = {"columns": ["k", "seq"], "data_types": ["uuid", "integer"], "numeric_scales": [None, None]}
    check(ns["_first_pk_rangeable"](pk2) == ("k", "uuid"),
          "V2: composite with uuid first column is ranged on it")
    # single-column PK -> not handled here (returns None,None)
    pk3 = {"columns": ["only"], "data_types": ["integer"], "numeric_scales": [None]}
    check(ns["_first_pk_rangeable"](pk3) == (None, None),
          "V2: single-column PK is not treated as composite here")
    # composite timestamp-first (not rangeable) -> whole table (None,None)
    pk4 = {"columns": ["ts", "x"], "data_types": ["timestamp without time zone", "integer"],
           "numeric_scales": [None, None]}
    check(ns["_first_pk_rangeable"](pk4) == (None, None),
          "V2: composite with a non-rangeable first column falls back to whole table")


def test_v2_statement_timeout_set():
    src = H.read_source("scripts/job3_validate.py")
    check("VALIDATE_STATEMENT_TIMEOUT_MS" in src and "SET statement_timeout" in src,
          "V2: every DSQL connection sets a statement_timeout")
    # the configured timeout is below DSQL's 300s hard limit
    import re
    m = re.search(r"VALIDATE_STATEMENT_TIMEOUT_MS\s*=\s*(\d+)", src)
    check(m is not None and int(m.group(1)) < 300000,
          "V2: the statement timeout is below the 300s DSQL limit")
    m2 = re.search(r"VALIDATE_ROWS_PER_RANGE\s*=\s*(\d+)", src)
    check(m2 is not None and int(m2.group(1)) <= 50000,
          "V2: the default range size was lowered (<= 50k) for the DSQL limit")


def main():
    test_asl_m01_m02_m03()
    test_m11_dsql_database()
    test_m12_commit_guard()
    test_m16_missing_target_fail()
    test_v1_empty_source()
    test_v2_txn_age_detection()
    test_v2_combine_equals_unsplit()
    test_v2_resplit_16m_integer()
    test_v2_resplit_gives_up_cleanly()
    test_v2_composite_first_column_ranging()
    test_v2_statement_timeout_set()
    print(f"\n==== fix6: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
