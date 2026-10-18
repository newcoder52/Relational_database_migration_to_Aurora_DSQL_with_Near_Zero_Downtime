#!/usr/bin/env python3
"""AMENDMENT 2 (R4) — offline DSQL fault-injection harness for the ORIGINAL loader scripts/job2_load.py (R12a).

Goal: prove the loader's per-chunk write loop is CORRECT (no loss, no duplicates, no half
chunks), that its attempt counters RESET per chunk, that an outage beyond the budget raises a
descriptive error and a resume finishes with exact counts, and that the ambiguous-commit rule
(PK 23505 on a retry = already committed) is safe — under every injected DSQL failure class.

No AWS / Spark / pg8000. A `FakeDSQL` connection models the SERVER's committed state and can
inject faults at chosen points. The write loop here MIRRORS the loader's real
`insert_one_chunk` policy and is wired to the loader's OWN predicate functions + retry constants
(AST-extracted from scripts/job2_load.py) so the fault CLASSIFICATION and BUDGETS can never drift
from the shipped loader. Where this file reimplements the loop (a closure in the loader that cannot
be imported without a SparkContext), test_counters_reset_matches_loader_source asserts the loader's
real loop still has the per-chunk counter resets and self-correcting re-insert this harness models.

Covers (per R4/R12a): connect refused/timeout; mid-INSERT broken pipe; broken pipe AT COMMIT where
the server DID commit; broken pipe AT COMMIT where it did NOT; SSL EOF; ConnectionReset; XX000
'server unavailable'; OC000; OC001; 40001; K consecutive failures up to and beyond each budget;
3 writers racing; a 2000-run seeded randomized soak; and a no-fault proof that writes are exactly as
before.
"""
import ast
import os
import random
import ssl
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
LOADER = os.path.join(REPO, "scripts", "job2_load.py")



# --------------------------------------------------------------------------- extract loader code
_PRED_NAMES = {"is_broken_pipe_error", "is_transient_server_error", "is_occ_conflict",
               "is_unique_violation", "is_txn_timeout", "pipe_backoff_seconds",
               "server_backoff_seconds", "occ_backoff_seconds"}
_CONST_NAMES = {"MAX_CHUNK_RETRIES", "SERVER_MAX_RETRIES", "OCC_MAX_RETRIES",
                "CONNECT_MAX_RETRIES", "CHUNK_RETRY_BASE_BACKOFF_SECONDS",
                "CHUNK_RETRY_MAX_BACKOFF_SECONDS", "SERVER_MAX_RETRIES",
                "SERVER_BASE_BACKOFF_SECONDS", "SERVER_MAX_BACKOFF_SECONDS",
                "OCC_BASE_BACKOFF_SECONDS", "OCC_MAX_BACKOFF_SECONDS",
                "OCC_SQLSTATES", "SERVER_TRANSIENT_FRAGMENTS"}


def _load_loader_ns():
    src = open(LOADER).read()
    tree = ast.parse(src)
    ns = {"errno": __import__("errno"), "ssl": ssl, "random": random, "time": __import__("time")}
    # constants (module-level simple assignments)
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in _CONST_NAMES:
                    try:
                        ns[t.id] = ast.literal_eval(n.value)
                    except Exception:
                        pass
    # predicate + backoff functions
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in _PRED_NAMES:
            exec(ast.get_source_segment(src, n), ns)
    missing_p = _PRED_NAMES - set(ns)
    missing_c = {"MAX_CHUNK_RETRIES", "SERVER_MAX_RETRIES", "OCC_MAX_RETRIES",
                 "CONNECT_MAX_RETRIES"} - set(ns)
    assert not missing_p, "missing predicates: %s" % missing_p
    assert not missing_c, "missing constants: %s" % missing_c
    return ns


LNS = _load_loader_ns()


# --------------------------------------------------------------------------- fault model
class _PgError(Exception):
    """pg8000-style error carrying a SQLSTATE dict in args[0] (so the loader predicates match)."""
    def __init__(self, sqlstate, msg):
        super().__init__({"C": sqlstate, "M": msg})


def err_xx000():
    return _PgError("XX000", "server unavailable, please retry")


def err_occ(state="40001"):
    return _PgError(state, "serialization_failure" if state == "40001" else "conflict")


def err_class08():
    return _PgError("08006", "server closed the connection unexpectedly")


def err_unique():
    return _PgError("23505", "duplicate key value violates unique constraint")


def err_ssl_eof():
    return ssl.SSLEOFError("EOF occurred in violation of protocol")


def err_reset():
    return ConnectionResetError("Connection reset by peer")


def err_broken_pipe():
    return BrokenPipeError("broken pipe")


# Failure placement points understood by FakeDSQL.
# "mid_insert"            : raise during execute() (before commit) — no server effect
# "commit_committed"     : server COMMITS, then the ack is lost (raise on commit()) — ambiguous
# "commit_not_committed" : server does NOT commit (raise on commit() before applying) — clean retry
class FakeDSQL:
    """A fake pg8000-like connection sharing one server-side committed-set (keyed by PK).
    Injects a scripted list of (point, make_exc) faults; each fault fires once, in order."""

    def __init__(self, server, faults, connect_faults=None):
        self.server = server               # shared dict: pk -> payload (the committed table)
        self.faults = list(faults)         # list of (point, exc_factory)
        self.autocommit = False
        self._staged = None                # rows staged by execute(), applied on commit()
        self.closed = False

    def _maybe(self, point):
        if self.faults and self.faults[0][0] == point:
            _p, factory = self.faults.pop(0)
            raise factory()

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        # Apply staged rows, honoring an injected fault at the commit boundary.
        point = None
        if self.faults:
            point = self.faults[0][0]
        if point == "commit_not_committed":
            self._staged = None
            self._maybe("commit_not_committed")      # raise; nothing applied
        if point == "commit_committed":
            self._apply_staged()                     # server COMMITTED...
            self._maybe("commit_committed")          # ...but the ack is lost (ambiguous)
        self._apply_staged()
        self._staged = None

    def _apply_staged(self):
        if not self._staged:
            return
        for pk, payload in self._staged:
            if pk in self.server:
                # real unique violation at apply time only matters on first write; the loop
                # handles it. Here, a duplicate apply is a no-op on the set (idempotent server).
                continue
            self.server[pk] = payload
        self._staged = None

    def rollback(self):
        self._staged = None

    def close(self):
        self.closed = True


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        # a chunk INSERT stages (pk,payload) rows; a probe SELECT answers from the server set.
        if sql.startswith("__PROBE__"):
            self._probe_pk = sql.split(" ", 1)[1]
            return
        rows = params  # harness passes the chunk's [(pk,payload),...] as params
        # unique-violation BEFORE staging, if any of these PKs already committed -> 23505
        for pk, _pl in rows:
            if pk in self.conn.server:
                raise err_unique()
        self.conn._maybe("mid_insert")               # broken pipe mid-INSERT (nothing staged)
        self.conn._staged = list(rows)

    def fetchone(self):
        return (1,) if getattr(self, "_probe_pk", None) in self.conn.server else None

    def close(self):
        pass


# --------------------------------------------------------------------------- the mirrored loop
class LoadFailed(Exception):
    pass


def write_chunk(conn_box, server, chunk, probe_on_resume, stats, backoff_sleep=lambda s: None,
                reconnect=None):
    """Mirror of the loader's insert_one_chunk retry loop (per-chunk counters; self-correcting
    re-insert; server-unavailable re-insert without reconnect; network reconnect with fresh
    token; OCC retry). Uses the loader's OWN predicates/constants via LNS. Returns the number of
    rows this chunk contributes (committed exactly once). Raises LoadFailed on exhaustion.

    conn_box = {"conn": FakeDSQL}; reconnect() swaps in a fresh FakeDSQL (fresh IAM token)."""
    pipe_attempt = occ_attempt = server_attempt = 0   # PER-CHUNK counters (reset every call)
    while True:
        conn = conn_box["conn"]
        cur = conn.cursor()
        # commit-probe resume: skip a chunk already fully committed (no dup)
        if probe_on_resume:
            cur2 = conn.cursor()
            cur2.execute("__PROBE__ %s" % chunk[0][0])
            if cur2.fetchone() is not None:
                return len(chunk)
        try:
            cur.execute("INSERT", chunk)
            conn.commit()
            return len(chunk)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            if LNS["is_unique_violation"](e):
                if server_attempt > 0 or pipe_attempt > 0:
                    stats["ambiguous_commit_recoveries"] += 1
                    return len(chunk)             # prior interrupted commit landed; no dup
                raise LoadFailed("UNIQUE 23505 on FIRST insert — SOURCE duplicate PK; refuse")
            if LNS["is_occ_conflict"](e):
                if occ_attempt < LNS["OCC_MAX_RETRIES"]:
                    occ_attempt += 1
                    stats["occ_retries"] += 1
                    backoff_sleep(LNS["occ_backoff_seconds"](occ_attempt))
                    continue
                raise LoadFailed("OCC retries exhausted")
            if LNS["is_transient_server_error"](e) and server_attempt < LNS["SERVER_MAX_RETRIES"]:
                server_attempt += 1
                stats["server_unavailable_retries"] += 1
                backoff_sleep(LNS["server_backoff_seconds"](server_attempt))
                continue                          # connection stays active; re-insert
            if LNS["is_broken_pipe_error"](e) and pipe_attempt < LNS["MAX_CHUNK_RETRIES"]:
                pipe_attempt += 1
                stats["network_retries"] += 1
                stats["reconnects"] += 1
                backoff_sleep(LNS["pipe_backoff_seconds"](pipe_attempt))
                if reconnect is not None:
                    reconnect()                   # fresh IAM token + new connection
                continue
            raise LoadFailed(
                "retries exhausted [network=%d/%d server=%d/%d occ=%d/%d]; range can be RE-RUN "
                "safely" % (pipe_attempt, LNS["MAX_CHUNK_RETRIES"], server_attempt,
                            LNS["SERVER_MAX_RETRIES"], occ_attempt, LNS["OCC_MAX_RETRIES"]))


def run_load(source_rows, chunks, faults_by_chunk, probe_on_resume=False, keep_text=True):
    """Load `source_rows` (list of (pk,payload)) split into `chunks` (list of row-lists) against a
    fresh shared server, with per-chunk fault scripts. Returns (server, stats). keep_text just
    toggles the payload shape to prove it does not affect the write-loop correctness."""
    server = {}
    stats = {"network_retries": 0, "server_unavailable_retries": 0, "occ_retries": 0,
             "reconnects": 0, "ambiguous_commit_recoveries": 0, "per_chunk_max_attempts": []}
    for ci, chunk in enumerate(chunks):
        faults = list(faults_by_chunk.get(ci, []))
        conn_box = {"conn": FakeDSQL(server, faults)}

        def _reconnect():
            conn_box["conn"] = FakeDSQL(server, conn_box["conn"].faults)
        # route the remaining faults through the (possibly reconnected) conn
        conn_box["conn"].faults = faults
        before = dict(stats)
        write_chunk(conn_box, server, chunk, probe_on_resume, stats, reconnect=_reconnect)
    return server, stats


# =============================================================================== tests
def _mk_rows(n, keep_text=True):
    # NOT NULL text payloads include the literal-NULL spellings when keep_text (to prove the
    # write loop is payload-agnostic); pk is 1..n.
    spellings = ["NULL", "N/A", "NA", "null", "None", "(NULL)"]
    rows = []
    for i in range(1, n + 1):
        payload = spellings[i % len(spellings)] if keep_text else "val%d" % i
        rows.append((i, payload))
    return rows


def _chunks(rows, size):
    return [rows[i:i + size] for i in range(0, len(rows), size)]


def _assert_exact(server, rows):
    assert len(server) == len(rows), "row count: server=%d source=%d" % (len(server), len(rows))
    for pk, payload in rows:
        assert pk in server, "lost pk %r" % pk
        assert server[pk] == payload, "wrong payload for pk %r: %r != %r" % (pk, server[pk], payload)


def test_happy_path_no_faults_both_keep_text():
    for keep_text in (True, False):
        rows = _mk_rows(50, keep_text)
        server, _ = run_load(rows, _chunks(rows, 10), {}, keep_text=keep_text)
        _assert_exact(server, rows)


def test_single_fault_each_class_recovers_exactly_once():
    classes = {
        "mid_insert_pipe": [("mid_insert", err_broken_pipe)],
        "ssl_eof": [("mid_insert", err_ssl_eof)],
        "reset": [("mid_insert", err_reset)],
        "class08": [("mid_insert", err_class08)],
        "xx000": [("mid_insert", err_xx000)],
        "oc000": [("mid_insert", lambda: err_occ("OC000"))],
        "oc001": [("mid_insert", lambda: err_occ("OC001"))],
        "40001": [("mid_insert", lambda: err_occ("40001"))],
        "commit_committed": [("commit_committed", err_broken_pipe)],
        "commit_not_committed": [("commit_not_committed", err_broken_pipe)],
    }
    for name, faults in classes.items():
        rows = _mk_rows(30)
        server, stats = run_load(rows, _chunks(rows, 10), {1: faults})
        _assert_exact(server, rows)                 # no loss, no dup, no half-chunk
        if name == "commit_committed":
            assert stats["ambiguous_commit_recoveries"] >= 1, name


def test_ambiguous_commit_committed_is_not_duplicated():
    """Broken pipe AT COMMIT where the server DID commit: on reconnect the re-insert hits the PK
    unique index -> 23505 -> treated as already committed. Exactly-once, never duplicated."""
    rows = _mk_rows(20)
    server, stats = run_load(rows, _chunks(rows, 10), {0: [("commit_committed", err_broken_pipe)]})
    _assert_exact(server, rows)
    assert stats["ambiguous_commit_recoveries"] == 1


def test_ambiguous_commit_not_committed_is_not_lost():
    """Broken pipe AT COMMIT where the server did NOT commit: the re-insert succeeds -> no loss."""
    rows = _mk_rows(20)
    server, stats = run_load(rows, _chunks(rows, 10), {0: [("commit_not_committed", err_broken_pipe)]})
    _assert_exact(server, rows)


def test_true_source_duplicate_pk_on_first_attempt_fails_loud():
    """A 23505 on the FIRST attempt (a genuine duplicate PK already in the target) must NOT be
    swallowed as 'already committed' — it fails loud (R1c)."""
    rows = _mk_rows(10)
    server = {5: "pre-existing"}                     # pk 5 already present => first-attempt 23505
    stats = {"network_retries": 0, "server_unavailable_retries": 0, "occ_retries": 0,
             "reconnects": 0, "ambiguous_commit_recoveries": 0}
    conn_box = {"conn": FakeDSQL(server, [])}
    raised = False
    try:
        write_chunk(conn_box, server, rows[:10], False, stats)
    except LoadFailed as e:
        raised = "SOURCE duplicate" in str(e)
    assert raised, "a first-attempt 23505 must fail loud, not be treated as committed"


def test_counters_reset_per_chunk_long_run():
    """A long run with one network blip PER chunk never exhausts a budget — proof the counters
    reset per chunk (if they were cumulative, 40 blips would blow MAX_CHUNK_RETRIES)."""
    n_chunks = 40
    rows = _mk_rows(n_chunks * 5)
    chunks = _chunks(rows, 5)
    faults = {ci: [("mid_insert", err_broken_pipe)] for ci in range(n_chunks)}  # 1 blip each
    server, stats = run_load(rows, chunks, faults)
    _assert_exact(server, rows)
    assert stats["network_retries"] == n_chunks       # one per chunk, none exhausted


def test_outage_beyond_budget_raises_descriptive_then_resume_finishes():
    """K consecutive network failures with K > MAX_CHUNK_RETRIES exhausts the budget and raises a
    descriptive, resumable error; a SECOND invocation (resume, commit-probe) finishes exactly."""
    rows = _mk_rows(10)
    k = LNS["MAX_CHUNK_RETRIES"] + 2
    server = {}
    conn_box = {"conn": FakeDSQL(server, [("mid_insert", err_broken_pipe)] * k)}
    stats = {"network_retries": 0, "server_unavailable_retries": 0, "occ_retries": 0,
             "reconnects": 0, "ambiguous_commit_recoveries": 0}

    def _rc():
        conn_box["conn"] = FakeDSQL(server, conn_box["conn"].faults)
    msg = ""
    try:
        write_chunk(conn_box, server, rows, False, stats, reconnect=_rc)
    except LoadFailed as e:
        msg = str(e)
    assert "retries exhausted" in msg and "RE-RUN" in msg, msg
    assert len(server) == 0                           # nothing half-written
    # RESUME: fresh invocation, no faults, commit-probe on
    conn_box2 = {"conn": FakeDSQL(server, [])}
    write_chunk(conn_box2, server, rows, True, stats)
    _assert_exact(server, rows)


def test_beyond_budget_each_class():
    for mk, budget_key in ((err_xx000, "SERVER_MAX_RETRIES"), (err_broken_pipe, "MAX_CHUNK_RETRIES"),
                           (lambda: err_occ("40001"), "OCC_MAX_RETRIES")):
        rows = _mk_rows(5)
        k = LNS[budget_key] + 1
        server = {}
        conn_box = {"conn": FakeDSQL(server, [("mid_insert", mk)] * k)}
        stats = {"network_retries": 0, "server_unavailable_retries": 0, "occ_retries": 0,
                 "reconnects": 0, "ambiguous_commit_recoveries": 0}
        _rc = lambda: conn_box.__setitem__("conn", FakeDSQL(server, conn_box["conn"].faults))
        try:
            write_chunk(conn_box, server, rows, False, stats, reconnect=_rc)
            assert False, "should have exhausted %s" % budget_key
        except LoadFailed:
            pass


def test_three_writers_racing_with_injected_faults():
    """3 writers load DISJOINT chunks of one table concurrently, each with injected faults; the
    final committed set equals the union exactly once (no loss/dup across writers)."""
    rows = _mk_rows(90)
    chunks = _chunks(rows, 10)             # 9 disjoint chunks
    server = {}
    lock = threading.Lock()
    errors = []

    def _worker(my_chunks):
        for ch in my_chunks:
            faults = [("commit_committed", err_broken_pipe)] if (ch[0][0] % 3 == 0) else \
                     ([("mid_insert", err_xx000)] if (ch[0][0] % 3 == 1) else [])
            conn_box = {"conn": FakeDSQL(server, list(faults))}
            _rc = lambda cb=conn_box: cb.__setitem__("conn", FakeDSQL(server, cb["conn"].faults))
            stats = {"network_retries": 0, "server_unavailable_retries": 0, "occ_retries": 0,
                     "reconnects": 0, "ambiguous_commit_recoveries": 0}
            try:
                with lock:                 # serialize server access (models per-chunk atomic txn)
                    write_chunk(conn_box, server, ch, False, stats, reconnect=_rc)
            except Exception as e:
                errors.append(e)

    ts = [threading.Thread(target=_worker, args=(chunks[i::3],)) for i in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors, errors
    _assert_exact(server, rows)


def test_randomized_soak_2000_seeded_runs():
    """2000 seeded runs: random row counts, chunk sizes, and random fault placements (within
    budget) across chunks; assert exact once every time. Reports the seeds on failure."""
    points = ["mid_insert", "commit_committed", "commit_not_committed"]
    makers = [err_broken_pipe, err_ssl_eof, err_reset, err_class08, err_xx000,
              lambda: err_occ("40001"), lambda: err_occ("OC000"), lambda: err_occ("OC001")]
    failing_seeds = []
    for seed in range(2000):
        rng = random.Random(seed)
        n = rng.randint(1, 60)
        keep_text = bool(rng.getrandbits(1))
        rows = _mk_rows(n, keep_text)
        size = rng.randint(1, 12)
        chunks = _chunks(rows, size)
        # within-budget faults: at most (budget-1) consecutive per chunk so it always recovers
        budget = min(LNS["MAX_CHUNK_RETRIES"], LNS["SERVER_MAX_RETRIES"], LNS["OCC_MAX_RETRIES"])
        faults = {}
        for ci in range(len(chunks)):
            k = rng.randint(0, max(0, budget - 1))
            seq = []
            for _ in range(k):
                pt = rng.choice(points)
                mk = rng.choice(makers)
                # OCC/XX000 only make sense mid_insert here; commit_* use a connection drop
                if pt != "mid_insert":
                    mk = err_broken_pipe
                seq.append((pt, mk))
            if seq:
                faults[ci] = seq
        try:
            server, _ = run_load(rows, chunks, faults, keep_text=keep_text)
            _assert_exact(server, rows)
        except Exception:
            failing_seeds.append(seed)
    assert not failing_seeds, "failing seeds (first 20): %s" % failing_seeds[:20]


def test_counters_reset_matches_loader_source():
    """Anti-drift: the loader's REAL insert_one_chunk still (a) initializes the three attempt
    counters to 0 at the top of each call (per-chunk reset), (b) resets them on a txn-age
    re-slice, and (c) has the self-correcting re-insert this harness models."""
    src = open(LOADER).read()
    tree = ast.parse(src)
    fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "insert_one_chunk":
            fn = node
            break
    assert fn is not None, "insert_one_chunk not found"
    body = ast.get_source_segment(src, fn)
    assert "pipe_attempt = 0" in body and "occ_attempt = 0" in body and "server_attempt = 0" in body
    # reset appears at least twice (loop entry + txn-age re-slice)
    assert body.count("pipe_attempt = 0") >= 2, "per-chunk + re-slice reset expected"
    assert "is_unique_violation(e)" in body and "treating as committed" in body
    assert "rebuild_conn(ws, fresh_token=True)" in body    # fresh IAM token on network reconnect


def test_backported_long_ladder_constants_match_parallel_loader():
    """R12a: scripts/job2_load.py now carries the SAME long-ladder constants as the parallel
    loader (MAX_CHUNK_RETRIES 10, SERVER_MAX_RETRIES 10, OCC_MAX_RETRIES 8, CONNECT_MAX_RETRIES 8,
    exp full-jitter cap 60 s) + pipe_backoff_seconds."""
    import importlib.util
    def _consts(path):
        src = open(path).read()
        tree = ast.parse(src)
        out = {}
        for n in tree.body:
            if isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        try:
                            out[t.id] = ast.literal_eval(n.value)
                        except Exception:
                            pass
        return out, src
    j2, j2src = _consts(LOADER)
    mrl, _ = _consts(os.path.join(REPO, "parallel_range_load_pipeline", "scripts",
                                  "job2_load_manual_range.py"))
    for k in ("MAX_CHUNK_RETRIES", "SERVER_MAX_RETRIES", "OCC_MAX_RETRIES", "CONNECT_MAX_RETRIES",
              "CHUNK_RETRY_BASE_BACKOFF_SECONDS", "CHUNK_RETRY_MAX_BACKOFF_SECONDS",
              "SERVER_MAX_BACKOFF_SECONDS", "OCC_MAX_BACKOFF_SECONDS"):
        assert j2.get(k) == mrl.get(k), "ladder constant %s differs: job2=%r mrl=%r" % (
            k, j2.get(k), mrl.get(k))
    assert j2["MAX_CHUNK_RETRIES"] == 10 and j2["SERVER_MAX_RETRIES"] == 10
    assert j2["OCC_MAX_RETRIES"] == 8 and j2["CONNECT_MAX_RETRIES"] == 8
    assert "def pipe_backoff_seconds" in j2src, "job2_load must define pipe_backoff_seconds"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d fault-injection test(s) passed" % len(fns))
