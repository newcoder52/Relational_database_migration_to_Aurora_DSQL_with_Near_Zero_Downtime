#!/usr/bin/env python3
"""mrl_count.py - STANDALONE Glue PYTHON-SHELL job: count verification + emptiness probe for
the MRL pipeline. No Spark, no S3 CSV read. pg8000 + IAM-token DSQL (same as the existing
Lambdas/jobs). Runs as a Glue Python Shell job (no 15-min Lambda limit).

TWO MODES (argument --mrl_mode):
  * "count" (default): for each table in the config prefix's _manifest_index.json, compute
    DSQL COUNT(*) and compare it to the DMS FullLoadRows for that table (matched by the
    manifest's dms_schema/dms_table, case-insensitive, since DMS schema names can be renamed by
    transformation rules). Writes a per-table result JSON and an aggregate verdict JSON to the
    MRL state folder. Mismatch or unverifiable (when count_timeout_action=fail, the default)
    => the job EXITS NON-ZERO naming the tables and both numbers (never a raw row value).
  * "probe": for each table, a CHEAP EXISTS probe (SELECT 1 ... LIMIT 1) -> empty/non-empty.
    Writes a per-table + aggregate result JSON to the state folder. Always exits 0 (the state
    machine's safety gate reads the JSON and decides). Used by the SAFETY GATE before loading.

Count algorithm (copied from customer_patches/job3_validate_count_only): range counting by key
to stay under DSQL's 300s per-txn limit; XX000 / server-unavailable / txn-age fallback to key
ranges and halving; bounded retries; a per-table hard wall-clock budget. Never logs a row value.

Arguments (all STRINGS, set by create-glue-jobs / the state machine):
  --JOB_NAME, --config_prefix (s3://.../tables/<label>/config/ for one table, or a task config
  prefix with many tables), --dsql_endpoint, --dsql_endpoint_candidates, --dsql_user,
  --dsql_database, --region, --dms_task_arn, --mrl_mode (count|probe),
  --mrl_result_prefix (s3://.../mrl/state/<suffix>/), --count_tolerance (default 0),
  --count_timeout_action (fail|warn, default fail).
"""

import json
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import pg8000
from awsglue.utils import getResolvedOptions


# --------------------------------------------------------------------------- args
_OPT = ["config_prefix", "dsql_endpoint", "dsql_endpoint_candidates", "dsql_user",
        "dsql_database", "region", "dms_task_arn", "mrl_mode", "mrl_result_prefix",
        "count_tolerance", "count_timeout_action", "count_no_basis_action"]
# A16: this is a PYTHON-SHELL Glue job. Glue auto-injects --JOB_NAME for SPARK (glueetl) jobs but
# NOT for a python-shell job started via StartJobRun, and SafetyProbe/CountVerify (and the job's
# DefaultArguments) do not pass it. The script does not need JOB_NAME (no job bookkeeping), so
# request ONLY the options actually present and read JOB_NAME optionally (log text only).
_present = [a for a in _OPT if ("--%s" % a) in sys.argv]
if "--JOB_NAME" in sys.argv:
    _present = _present + ["JOB_NAME"]
args = getResolvedOptions(sys.argv, _present)
JOB_NAME = args.get("JOB_NAME", "mrl-count")

CONFIG_PREFIX = args["config_prefix"]
DSQL_ENDPOINT = args.get("dsql_endpoint", "")
DSQL_ENDPOINT_CANDIDATES = args.get("dsql_endpoint_candidates", "")
DSQL_USER = args.get("dsql_user", "admin")
DSQL_DATABASE = args.get("dsql_database", "postgres")
REGION = args.get("region", "us-east-1")
DMS_TASK_ARN = args.get("dms_task_arn") or None
MRL_MODE = (args.get("mrl_mode") or "count").strip().lower()
MRL_RESULT_PREFIX = args.get("mrl_result_prefix", "")
COUNT_TOLERANCE = int(args.get("count_tolerance", "0") or "0")
COUNT_TIMEOUT_ACTION = (args.get("count_timeout_action") or "fail").strip().lower()
# R21-2: a LOADED table with NO comparison basis (no DMS FullLoadRows and no discovery
# full_load_rows) must NOT silently pass. Default 'fail' (never report ok on a count with no
# basis); 'warn' downgrades to a loud warning (operator accepts the risk).
COUNT_NO_BASIS_ACTION = (args.get("count_no_basis_action") or "fail").strip().lower()

DSQL_CANDIDATE_CONNECT_TIMEOUT = 10

# count-only algorithm knobs (copied values from the delivered count patch)
COUNT_RETRIES = 3
COUNT_RETRY_WAIT = 20
COUNT_SPLIT_ROWS = 100_000_000
COUNT_RANGE_ROWS = 20_000_000
COUNT_RANGE_THREADS = 4
COUNT_MAX_HALVINGS = 40
COUNT_UNKNOWN_TOTAL_ROWS = 600_000_000
COUNT_TABLE_BUDGET_SECONDS = 900


def make_boto_client(service):
    return boto3.client(service, region_name=REGION)


# === DSQL ENDPOINT FAILOVER HELPER (byte-identical to drain_check's block) =================
def dsql_candidate_list(candidates_csv, given_endpoint):
    out = []
    for raw in (candidates_csv or "").split(","):
        host = raw.strip()
        if host and host not in out:
            out.append(host)
    g = (given_endpoint or "").strip()
    if g and g not in out:
        out.append(g)
    return out


def dsql_connect_first(candidates, make_conn, log=None):
    errors = []
    for host in candidates:
        try:
            conn = make_conn(host)
            if log:
                log(host)
            return conn, host
        except Exception as e:   # noqa: BLE001
            errors.append((host, "%s: %s" % (type(e).__name__, e)))
    tried = "; ".join("%s -> %s" % (h, err) for h, err in errors) or "(no candidates)"
    raise RuntimeError(
        "Could not connect to Aurora DSQL on any candidate hostname [" + tried + "]. "
        "Hint: Glue needs a network route to DSQL: a DSQL VPC endpoint with private DNS, or "
        "internet/NAT.")
# === END DSQL ENDPOINT FAILOVER HELPER ======================================================


_PINNED_HOST = {"host": None}


def connect_dsql(autocommit=True):
    dsql = make_boto_client("dsql")
    ctx = ssl.create_default_context()

    def _make(host):
        token = dsql.generate_db_connect_admin_auth_token(host, Region=REGION, ExpiresIn=3600)
        conn = pg8000.connect(host=host, port=5432, database=DSQL_DATABASE, user=DSQL_USER,
                              password=token, ssl_context=ctx,
                              timeout=DSQL_CANDIDATE_CONNECT_TIMEOUT)
        conn.autocommit = autocommit
        return conn

    if _PINNED_HOST["host"]:
        return _make(_PINNED_HOST["host"])
    cands = dsql_candidate_list(DSQL_ENDPOINT_CANDIDATES, DSQL_ENDPOINT)
    conn, host = dsql_connect_first(cands, _make,
                                    log=lambda h: print("mrl_count: DSQL reachable on %s" % h))
    _PINNED_HOST["host"] = host
    return conn


def _target_query(schema, table, pred, params):
    """Run one SELECT count(*) with an optional predicate; returns the single row. Own small
    helper (the count-only patch reuses job3's; MRL is standalone)."""
    where = (" WHERE " + pred) if pred else ""
    sql = 'SELECT count(*) FROM "%s"."%s"%s' % (schema, table, where)
    conn = None
    try:
        conn = connect_dsql(autocommit=True)
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            return cur.fetchone()
        finally:
            cur.close()
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def is_txn_age_error(e):
    """DSQL 300s txn-age (57014) / statement timeout / client read timeout -> re-split trigger."""
    s = str(e).lower()
    return ("57014" in s or "statement timeout" in s or "canceling statement" in s
            or "read operation timed out" in s or "transaction" in s and "age" in s)


def dms_full_load_rows_for(dms, task_arn, dms_schema, dms_table):
    """DMS FullLoadRows for a table, matched case-insensitively on schema/table. None if not
    found."""
    marker = None
    want = (str(dms_schema).lower(), str(dms_table).lower())
    while True:
        kw = {"ReplicationTaskArn": task_arn, "MaxRecords": 500}
        if marker:
            kw["Marker"] = marker
        resp = dms.describe_table_statistics(**kw)
        for st in resp.get("TableStatistics", []) or []:
            if (str(st.get("SchemaName") or "").lower(),
                    str(st.get("TableName") or "").lower()) == want:
                v = st.get("FullLoadRows")
                return int(v) if v is not None else None
        marker = resp.get("Marker")
        if not marker:
            break
    return None


# ---- count-only range engine (copied algorithm, standalone) ------------------------------
class CountRangeError(Exception):
    pass


class CountSplit(Exception):
    pass


class CountStop(Exception):
    pass


def _q(col):
    return '"' + str(col).replace('"', '""') + '"'


def _lit(kind, v):
    if kind == "integer":
        return str(int(v))
    h = format(int(v), "032x")
    return "'%s-%s-%s-%s-%s'::uuid" % (h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])


def _to_int(kind, v):
    if kind == "integer":
        return int(v)
    return int(str(v).replace("-", "").strip().lower(), 16)


def _pred(col, kind, lo, hi):
    p = []
    if lo is not None:
        p.append("%s >= %s" % (_q(col), _lit(kind, lo)))
    if hi is not None:
        p.append("%s < %s" % (_q(col), _lit(kind, hi)))
    return " AND ".join(p) if p else "%s IS NOT NULL" % _q(col)


def _alive(ctx):
    if ctx["abort"].is_set() or time.time() > ctx["deadline"]:
        ctx["abort"].set()
        raise CountStop("count stopped (time budget used up or another range failed)")


def _unavailable(e):
    s = str(e).lower()
    return "xx000" in s or "server unavailable" in s or "54000" in s


def _run(schema, table, pred, ctx):
    attempt = 0
    while True:
        _alive(ctx)
        try:
            return int(_target_query(schema, table, pred, [])[0])
        except Exception as e:
            if is_txn_age_error(e):
                raise CountSplit(str(e)[:200])
            attempt += 1
            if attempt > COUNT_RETRIES:
                if _unavailable(e):
                    raise CountSplit(str(e)[:200])
                raise
            print("    count retry %d/%d on %s.%s after %s"
                  % (attempt, COUNT_RETRIES, schema, table, type(e).__name__))
            time.sleep(max(0.0, min(COUNT_RETRY_WAIT, ctx["deadline"] - time.time())))


def _edge(schema, table, col, asc, ctx):
    sql = 'SELECT %s FROM "%s"."%s" WHERE %s IS NOT NULL ORDER BY %s %s LIMIT 1' % (
        _q(col), schema, table, _q(col), _q(col), "ASC" if asc else "DESC")
    conn = None
    try:
        conn = connect_dsql(autocommit=True)
        cur = conn.cursor()
        try:
            cur.execute(sql)
            r = cur.fetchone()
        finally:
            cur.close()
        return None if (not r or r[0] is None) else r[0]
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _nullable(schema, table, col):
    conn = None
    try:
        conn = connect_dsql(autocommit=True)
        cur = conn.cursor()
        try:
            cur.execute("SELECT is_nullable FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
                        (schema, table, col))
            r = cur.fetchone()
        finally:
            cur.close()
        return not (r and str(r[0]).strip().upper() == "NO")
    except Exception:
        return True
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _plan(gmin, gmax, total, per):
    span = gmax - gmin + 1
    n = max(1, -(-int(total) // max(1, int(per))))
    n = min(n, span)
    bounds = [gmin + (i * span) // n for i in range(1, n)]
    prev = gmin
    for bd in bounds:
        if not (bd > prev and bd <= gmax):
            raise CountRangeError("boundaries not strictly increasing")
        prev = bd
    edges = [None] + bounds + [None]
    eff = [gmin] + bounds + [gmax + 1]
    rngs = [(edges[i], edges[i + 1], eff[i], eff[i + 1]) for i in range(n)]
    if len(rngs) != len(bounds) + 1:
        raise CountRangeError("ranges != boundaries + 1")
    return rngs


def _leaf(schema, table, col, kind, lo, hi, elo, ehi, depth, leaves, ctx):
    try:
        n = _run(schema, table, _pred(col, kind, lo, hi), ctx)
        leaves.append((elo, ehi, lo, hi, n))
        return
    except CountSplit as e:
        too_big = e
    if depth >= COUNT_MAX_HALVINGS or (ehi - elo) < 2:
        raise too_big
    mid = elo + (ehi - elo) // 2
    if not (elo < mid < ehi):
        raise CountRangeError("bad split point")
    _leaf(schema, table, col, kind, lo, mid, elo, mid, depth + 1, leaves, ctx)
    _leaf(schema, table, col, kind, mid, hi, mid, ehi, depth + 1, leaves, ctx)


def _verify(leaves, gmin, gmax):
    if not leaves:
        raise CountRangeError("no ranges counted")
    ls = sorted(leaves, key=lambda x: x[0])
    if ls[0][0] != gmin or ls[0][2] is not None:
        raise CountRangeError("first range not open at bottom / not at min key")
    if ls[-1][1] != gmax + 1 or ls[-1][3] is not None:
        raise CountRangeError("last range not open at top / not past max key")
    for i, (elo, ehi, lo, hi, _n) in enumerate(ls):
        if not (elo < ehi):
            raise CountRangeError("empty/inverted range")
        if i > 0 and ls[i - 1][1] != elo:
            raise CountRangeError("gap/overlap between ranges")


def _ranged(schema, table, col, kind, expected, ctx, after_timeout=False):
    lo_raw = _edge(schema, table, col, True, ctx)
    hi_raw = _edge(schema, table, col, False, ctx)
    nulls = _run(schema, table, "%s IS NULL" % _q(col), ctx) \
        if _nullable(schema, table, col) else 0
    if lo_raw is None or hi_raw is None:
        return nulls
    gmin, gmax = _to_int(kind, lo_raw), _to_int(kind, hi_raw)
    if gmax < gmin:
        raise CountRangeError("max key below min key")
    total = max(int(expected or 0), COUNT_UNKNOWN_TOTAL_ROWS) if (after_timeout or not expected) \
        else int(expected)
    rngs = _plan(gmin, gmax, total, COUNT_RANGE_ROWS)
    per_range = [[] for _ in rngs]

    def _one(i):
        try:
            _alive(ctx)
            lo, hi, elo, ehi = rngs[i]
            _leaf(schema, table, col, kind, lo, hi, elo, ehi, 0, per_range[i], ctx)
        except BaseException:
            ctx["abort"].set()
            raise

    errs = []
    with ThreadPoolExecutor(max_workers=COUNT_RANGE_THREADS) as pool:
        futs = [pool.submit(_one, i) for i in range(len(rngs))]
        for f in futs:
            try:
                f.result()
            except Exception as e:
                errs.append(e)
    if errs:
        real = [e for e in errs if not isinstance(e, CountStop)]
        raise (real or errs)[0]
    leaves = [x for part in per_range for x in part]
    _verify(leaves, gmin, gmax)
    return sum(x[4] for x in leaves) + nulls


def _single_pk(entry):
    pk = (entry.get("metadata", {}) or {}).get("primary_key") or {}
    cols = pk.get("columns") or []
    kind = pk.get("pk_kind") or pk.get("kind")
    if pk.get("mode") == "composite" or len(cols) != 1:
        return None, None
    return cols[0], (kind if kind in ("integer", "uuid") else None)


def _count_one(entry, dms):
    dsql_schema = entry.get("dsql_schema")
    dsql_table = entry.get("dsql_table")
    label = "%s.%s" % (dsql_schema, dsql_table)
    meta = entry.get("metadata", {}) or {}
    dms_schema = meta.get("dms_schema") or entry.get("dms_schema") or dsql_schema
    dms_table = meta.get("dms_table") or entry.get("dms_table") or dsql_table

    expected, basis = None, None
    if DMS_TASK_ARN:
        try:
            expected = dms_full_load_rows_for(dms, DMS_TASK_ARN, dms_schema, dms_table)
            basis = "DMS FullLoadRows"
        except Exception:
            expected = None
    if expected is None:
        v = entry.get("full_load_rows") or meta.get("full_load_rows")
        if v is not None:
            expected, basis = int(v), "discovery full_load_rows"

    ck, kk = _single_pk(entry)
    can_range = bool(ck) and kk in ("integer", "uuid")
    ctx = {"abort": threading.Event(), "deadline": time.time() + COUNT_TABLE_BUDGET_SECONDS}
    tgt, err = None, None
    try:
        if can_range and expected is not None and expected >= COUNT_SPLIT_ROWS:
            tgt = _ranged(dsql_schema, dsql_table, ck, kk, expected, ctx)
        else:
            try:
                tgt = _run(dsql_schema, dsql_table, None, ctx)
            except CountSplit:
                if not can_range:
                    raise
                tgt = _ranged(dsql_schema, dsql_table, ck, kk, expected, ctx, after_timeout=True)
    except Exception as e:
        err = e

    if err is not None:
        return {"table": label, "status": "unverifiable", "expected": expected,
                "target": None, "basis": basis, "reason": "%s: %s" % (type(err).__name__,
                                                                      str(err)[:200])}
    if expected is None:
        return {"table": label, "status": "no_expected", "expected": None, "target": tgt,
                "basis": None, "reason": "no DMS/discovery row count to compare against"}
    ok = abs(int(tgt) - int(expected)) <= COUNT_TOLERANCE
    return {"table": label, "status": "match" if ok else "mismatch", "expected": int(expected),
            "target": int(tgt), "basis": basis}


def _probe_one(entry):
    dsql_schema = entry.get("dsql_schema")
    dsql_table = entry.get("dsql_table")
    label = "%s.%s" % (dsql_schema, dsql_table)
    conn = None
    try:
        conn = connect_dsql(autocommit=True)
        cur = conn.cursor()
        try:
            cur.execute('SELECT 1 FROM "%s"."%s" LIMIT 1' % (dsql_schema, dsql_table))
            row = cur.fetchone()
        finally:
            cur.close()
        return {"table": label, "empty": row is None, "exists": True}
    except Exception as e:
        # Table missing -> treat as not existing (the safety gate fails closed on it).
        return {"table": label, "empty": None, "exists": False,
                "reason": "%s: %s" % (type(e).__name__, str(e)[:200])}
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _s3():
    return make_boto_client("s3")


def _read_index():
    cp = CONFIG_PREFIX
    assert cp.startswith("s3://")
    b, key = cp[5:].split("/", 1)
    key = key.rstrip("/") + "/_manifest_index.json"
    obj = _s3().get_object(Bucket=b, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8")).get("tables", []) or []


def _write_result(name, doc):
    if not MRL_RESULT_PREFIX:
        print("(warn) no --mrl_result_prefix; result not persisted: %s" % name)
        return
    assert MRL_RESULT_PREFIX.startswith("s3://")
    b, key = MRL_RESULT_PREFIX[5:].split("/", 1)
    key = key.rstrip("/") + "/" + name
    _s3().put_object(Bucket=b, Key=key,
                     Body=(json.dumps(doc, indent=2) + "\n").encode("utf-8"),
                     ContentType="application/json")
    print("(info) wrote s3://%s/%s" % (b, key))


def compute_verdict(results, timeout_action="fail", no_basis_action="fail"):
    """PURE verdict rollup (M5): turn per-table count results into the overall verdict + the list
    of fatal tables. No I/O, no globals - so a test can execute the REAL rollup. Returns
    (verdict, info) where verdict is one of ok|mismatch|unverifiable|no_basis and info has the
    per-category table lists. A mismatch always fails. An unverifiable (count did not run) fails
    when timeout_action='fail'. A no_expected (loaded table with no DMS/discovery basis) fails
    when no_basis_action='fail' (R21-2). Precedence: mismatch > unverifiable > no_basis > ok."""
    ta = (timeout_action or "fail").strip().lower()
    na = (no_basis_action or "fail").strip().lower()
    mismatches = [r for r in results if r["status"] == "mismatch"]
    unverifiable = [r for r in results if r["status"] == "unverifiable"]
    no_expected = [r for r in results if r["status"] == "no_expected"]
    verdict = "ok"
    if mismatches:
        verdict = "mismatch"
    elif unverifiable and ta == "fail":
        verdict = "unverifiable"
    elif no_expected and na == "fail":
        verdict = "no_basis"
    return verdict, {"mismatches": mismatches, "unverifiable": unverifiable,
                     "no_expected": no_expected}


def main():
    entries = _read_index()
    if not entries:
        raise SystemExit("mrl_count: no tables in %s_manifest_index.json" % CONFIG_PREFIX)
    dms = make_boto_client("dms") if DMS_TASK_ARN else None

    if MRL_MODE == "probe":
        results = [_probe_one(e) for e in entries]
        doc = {"mode": "probe", "config_prefix": CONFIG_PREFIX, "tables": results,
               "all_empty": all(r["empty"] is True for r in results),
               "any_missing": any(not r["exists"] for r in results)}
        _write_result("probe_result.json", doc)
        # Probe never exits non-zero; the state machine's safety gate reads the JSON.
        print("(info) mrl_count probe: %s" % json.dumps(doc))
        return

    # count mode
    results = [_count_one(e, dms) for e in entries]
    verdict, info = compute_verdict(results, COUNT_TIMEOUT_ACTION, COUNT_NO_BASIS_ACTION)
    mismatches = info["mismatches"]
    unverifiable = info["unverifiable"]
    no_expected = info["no_expected"]
    doc = {"mode": "count", "config_prefix": CONFIG_PREFIX, "tolerance": COUNT_TOLERANCE,
           "count_timeout_action": COUNT_TIMEOUT_ACTION,
           "count_no_basis_action": COUNT_NO_BASIS_ACTION, "tables": results, "verdict": verdict}
    _write_result("count_result.json", doc)
    for r in results:
        print("    count [%s]: %s expected=%s target=%s (%s)"
              % (r["table"], r["status"], r.get("expected"), r.get("target"), r.get("basis")))
    if mismatches:
        raise SystemExit("mrl_count: COUNT MISMATCH for %d table(s): %s. Never marking success "
                         "on a mismatch." %
                         (len(mismatches), ", ".join("%s (DSQL=%s vs %s=%s)"
                          % (m["table"], m["target"], m["basis"], m["expected"])
                          for m in mismatches)))
    if unverifiable and COUNT_TIMEOUT_ACTION == "fail":
        raise SystemExit("mrl_count: COUNT COULD NOT COMPLETE for %d table(s): %s. "
                         "count_timeout_action=fail, so failing closed (never mark success on a "
                         "count that did not run). Set count_timeout_action=warn to downgrade." %
                         (len(unverifiable), ", ".join(u["table"] for u in unverifiable)))
    if no_expected and COUNT_NO_BASIS_ACTION == "fail":
        # R21-2: a loaded table with no DMS/discovery basis cannot be verified -> fail closed by
        # default (never report ok on a count with no basis). count_no_basis_action=warn downgrades.
        raise SystemExit("mrl_count: NO COMPARISON BASIS for %d loaded table(s): %s. There is no "
                         "DMS FullLoadRows and no discovery full_load_rows to check the target "
                         "against, so the load CANNOT be verified. count_no_basis_action=fail, so "
                         "failing closed. Set count_no_basis_action=warn to downgrade (accepting "
                         "an unverified load)." %
                         (len(no_expected), ", ".join(u["table"] for u in no_expected)))
    if no_expected:
        print("(warn) mrl_count: %d table(s) had no DMS/discovery row count to compare: %s "
              "(count_no_basis_action=warn)."
              % (len(no_expected), ", ".join(u["table"] for u in no_expected)))
    print("(info) mrl_count: verdict=%s over %d table(s)" % (verdict, len(results)))


if __name__ == "__main__":
    main()
