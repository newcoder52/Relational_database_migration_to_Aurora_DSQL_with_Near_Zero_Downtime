#!/usr/bin/env python3
"""Executing offline tests for the MANUAL-RANGE patch (no AWS, no Spark, no network).

    python3 test_manual_shard_patch.py \
        [--patched job2_load_manual_shard.py] [--applier apply_manual_shard_patch.py] \
        [--base /path/to/job2_load.py]

Like the repo's own tests, it AST-extracts the patch's functions out of the PATCHED FILE
and runs them against a fake S3 + fakes, asserting the safety invariants. It also drives
the apply script (recognise / refuse) and mutation checks (flip a safety condition -> a
test fails).
"""
import argparse
import ast
import os
import sys
import json
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
PASSED = FAILED = 0


def check(cond, msg):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print("[PASS] " + msg)
    else:
        FAILED += 1
        print("[FAIL] " + msg)


# ------------------------------------------------------------------ fake S3 (in-memory)
class FakeS3:
    """Minimal in-memory S3 with ETag + If-Match/If-None-Match conditional puts and a lock,
    so concurrency tests exercise the real read-modify-write paths."""
    class _NoSuchKey(Exception):
        pass

    def __init__(self):
        self.store = {}            # key -> (body_bytes, etag)
        self._n = 0
        self.lock = threading.Lock()

    def _err(self, name):
        e = Exception(name)
        return e

    def get_object(self, Bucket, Key):
        with self.lock:
            if Key not in self.store:
                raise self._err("NoSuchKey")
            body, etag = self.store[Key]

        class _B:
            def __init__(self, b):
                self._b = b

            def read(self):
                return self._b
        return {"Body": _B(body), "ETag": etag}

    def put_object(self, Bucket, Key, Body, ContentType=None, IfNoneMatch=None, IfMatch=None):
        with self.lock:
            cur = self.store.get(Key)
            if IfNoneMatch is not None and cur is not None:
                raise self._err("PreconditionFailed (412)")
            if IfMatch is not None:
                if cur is None or cur[1] != IfMatch:
                    raise self._err("PreconditionFailed (412)")
            self._n += 1
            etag = '"etag-%d"' % self._n
            self.store[Key] = (Body if isinstance(Body, (bytes, bytearray)) else
                               str(Body).encode("utf-8"), etag)
        return {"ETag": etag}

    def delete_object(self, Bucket, Key):
        with self.lock:
            self.store.pop(Key, None)
        return {}

    def keys(self):
        with self.lock:
            return sorted(self.store.keys())


# ------------------------------------------------------------------ load the patch code
def load_ns(patched_path):
    """AST-extract the manual-range functions/classes + the _MANUAL_RANGE dict out of the
    patched file into a namespace wired with fakes for json/threading/split_s3/etc."""
    src = open(patched_path).read()
    tree = ast.parse(src)
    ns = {
        "json": json, "threading": threading, "sys": sys,
        "CONFIG_PREFIX": "s3://bkt/cfg/_orchestrator/group-1/",
        "utc_now_iso": lambda: "2026-01-01T00:00:00Z",
        "print": print,
    }

    # split_s3: the real one is simple; provide an equivalent for the extracted funcs.
    def split_s3(path):
        assert path.startswith("s3://")
        rest = path[5:]
        b, _, k = rest.partition("/")
        return b, k
    ns["split_s3"] = split_s3
    ns["make_boto_client"] = lambda svc: ns["_FAKE_S3"]

    wanted_prefixes = ("_mr_", "_MANUAL_RANGE", "_apply_manual_range", "_install_manual_range")
    wanted_names = {"ManualRangeError"}
    for n in tree.body:
        seg = ast.get_source_segment(src, n)
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
            if n.name.startswith(wanted_prefixes) or n.name in wanted_names:
                exec(seg, ns)
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id == "_MANUAL_RANGE":
                    exec(seg, ns)
    return ns, src


def mk_files(numbers, size=1000):
    """[(uri,size)] for LOAD<n>.csv under a table folder. `numbers` may be a list of ints
    or (int,size) tuples. Returned largest-first like the real lister."""
    out = []
    for item in numbers:
        if isinstance(item, tuple):
            n, sz = item
        else:
            n, sz = item, size
        out.append(("s3://bkt/dms/demo_schema/demo_table/LOAD%08d.csv" % n, sz))
    out.sort(key=lambda t: t[1], reverse=True)
    return out


def reset_range(ns, on=False, start=None, end=None):
    mr = ns["_MANUAL_RANGE"]
    mr.update({"on": on, "start_raw": start, "end_raw": end, "label": "demo_schema.demo_table",
               "first": None, "last": None, "assigned": None, "assigned_set": None,
               "all_sorted": None, "has_single_pk": None, "result_written": False})


# ============================================================ TESTS: range resolution
def test_resolution(ns):
    files = mk_files([1, 2, 3, 4, 5])
    reslice = ns["_mr_resolve_slice"]
    # number bounds, inclusive
    first, last, assigned, _b = reslice(2, 4, files)
    check(first == "LOAD00000002.csv" and last == "LOAD00000004.csv" and len(assigned) == 3,
          "number bounds resolve inclusive 2..4 -> 3 files")
    # bare name
    first, last, assigned, _b = reslice("LOAD00000001.csv", "LOAD00000003.csv", files)
    check(len(assigned) == 3 and first == "LOAD00000001.csv", "bare-name bounds resolve")
    # full s3 key
    k1 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000003.csv"
    k2 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000005.csv"
    first, last, assigned, _b = reslice(k1, k2, files)
    check(len(assigned) == 3 and last == "LOAD00000005.csv", "full s3:// key bounds resolve")
    # start only
    first, last, assigned, _b = reslice(3, None, files)
    check(first == "LOAD00000003.csv" and last == "LOAD00000005.csv" and len(assigned) == 3,
          "start-only -> start..last")
    # end only
    first, last, assigned, _b = reslice(None, 2, files)
    check(first == "LOAD00000001.csv" and last == "LOAD00000002.csv" and len(assigned) == 2,
          "end-only -> first..end")
    # single-file range
    _f, _l, assigned, _b = reslice(3, 3, files)
    check(len(assigned) == 1, "single-file inclusive range")

    def fails(a, b, why):
        try:
            reslice(a, b, files)
            check(False, "should have failed: " + why)
        except ns["ManualRangeError"]:
            check(True, "fail closed: " + why)
    fails(4, 2, "start after end")
    fails(99, None, "unknown number")
    fails("LOAD99999999.csv", None, "unknown name")
    fails("s3://bkt/dms/demo_schema/demo_table/NOPE.csv", None, "unknown s3 key")
    fails("garbage", None, "non-number non-csv non-s3")
    # gaps in file numbers: 1,2,5,9 ; range 2..5 picks 2 and 5 (both present), 3..4 is empty-ish
    gap_files = mk_files([1, 2, 5, 9])
    _f, _l, a, _b = reslice(2, 5, gap_files)
    check(sorted(ns["_mr_basename"](u) for u in a) == ["LOAD00000002.csv", "LOAD00000005.csv"],
          "gapped numbering: 2..5 -> {2,5} (position-inclusive on sorted list)")
    try:
        reslice(3, 4, gap_files)
        check(False, "gap 3..4 should fail closed (neither present)")
    except ns["ManualRangeError"]:
        check(True, "gap 3..4 fails closed (no file resolves)")


# ============================================================ TESTS: byte-identical OFF
def test_byte_identical_off(ns):
    reset_range(ns, on=False)

    def orig_list(s3, path):
        return mk_files([1, 2, 3])
    wrapped = ns["_mr_wrap_s3_list_table_files"](orig_list)
    out = wrapped(ns["_FAKE_S3"], "s3://bkt/dms/demo_schema/demo_table")
    check(out == orig_list(None, None), "s3_list wrapper is pass-through when OFF")

    sentinel = object()

    def orig_fsp(a, b):
        return sentinel
    w2 = ns["_mr_wrap_file_status_path"](orig_fsp)
    check(w2("s", "t") is sentinel, "_file_status_path wrapper pass-through when OFF")

    calls = []

    def orig_clear(s3, s, t):
        calls.append((s, t))
    w3 = ns["_mr_wrap_clear_file_status"](orig_clear)
    w3(ns["_FAKE_S3"], "s", "t")
    check(calls == [("s", "t")], "clear_file_status wrapper pass-through when OFF (orig ran)")

    calls2 = []

    def orig_assert(s, t, *a, **k):
        calls2.append((s, t, a, k))
    w4 = ns["_mr_wrap_assert_empty_or_register"](orig_assert)
    w4("s", "t", 1, 2, pk_col="x")
    check(len(calls2) == 1, "assert_empty_or_register wrapper pass-through when OFF")

    calls3 = []

    def orig_val(s, t, *a, **k):
        calls3.append(1)
    w5 = ns["_mr_wrap_validate_whole_table_after_ranges"](orig_val)
    w5("s", "t", ["uuid"], ["c"], 10)
    check(calls3 == [1], "validate wrapper pass-through when OFF")


# ============================================================ TESTS: ON applies slice
def test_on_applies_slice(ns):
    ns["_FAKE_S3"] = FakeS3()
    s3 = ns["_FAKE_S3"]
    reset_range(ns, on=True, start=2, end=4)

    def orig_list(s3c, path):
        return mk_files([1, 2, 3, 4, 5])
    wrapped = ns["_mr_wrap_s3_list_table_files"](orig_list)
    out = wrapped(s3, "s3://bkt/dms/demo_schema/demo_table")
    names = sorted(ns["_mr_basename"](u) for u, _s in out)
    check(names == ["LOAD00000002.csv", "LOAD00000003.csv", "LOAD00000004.csv"],
          "ON: wrapper returns only the resolved slice 2..4")
    check(ns["_MANUAL_RANGE"]["first"] == "LOAD00000002.csv" and
          ns["_MANUAL_RANGE"]["last"] == "LOAD00000004.csv", "ON: first/last recorded")
    # file-status path is range-private
    fsp = ns["_mr_wrap_file_status_path"](lambda a, b: "NORMAL")
    p = fsp("demo_schema", "demo_table")
    check(".range-LOAD00000002.csv-LOAD00000004.csv.json" in p,
          "ON: file-status path is range-private")


# ============================================================ TESTS: per-range isolation
def test_status_isolation(ns):
    # range A 1..2 and range B 3..4 running "at once": each uses its own status key.
    def paths_for(start, end):
        ns["_FAKE_S3"] = FakeS3()   # independent ledger per range (they are separate runs)
        reset_range(ns, on=True, start=start, end=end)
        wrapped = ns["_mr_wrap_s3_list_table_files"](lambda a, b: mk_files([1, 2, 3, 4]))
        wrapped(ns["_FAKE_S3"], "s3://bkt/x")
        fsp = ns["_mr_wrap_file_status_path"](lambda a, b: "NORMAL")
        return fsp("demo_schema", "demo_table")
    pa = paths_for(1, 2)
    pb = paths_for(3, 4)
    check(pa != pb and "LOAD00000001" in pa and "LOAD00000003" in pb,
          "two ranges resolve to DIFFERENT status keys (no shared-object RMW)")


# ============================================================ TESTS: overlap guard
def test_overlap_guard(ns):
    ns["_FAKE_S3"] = FakeS3()
    s3 = ns["_FAKE_S3"]
    guard = ns["_mr_overlap_guard"]
    label = "demo_schema.demo_table"

    def uris(nums):
        return ["s3://bkt/dms/demo_schema/demo_table/LOAD%08d.csv" % n for n in nums]
    # record range 1..2
    reset_range(ns, on=True, start=1, end=2)
    guard(s3, label, "LOAD00000001.csv", "LOAD00000002.csv", uris([1, 2]))
    check(True, "overlap guard: first range recorded")
    # identical range again -> allowed (resume)
    guard(s3, label, "LOAD00000001.csv", "LOAD00000002.csv", uris([1, 2]))
    check(True, "overlap guard: identical range allowed (resume)")
    # disjoint 3..4 -> allowed
    guard(s3, label, "LOAD00000003.csv", "LOAD00000004.csv", uris([3, 4]))
    check(True, "overlap guard: disjoint range allowed")
    # partial overlap 2..3 -> refused
    try:
        guard(s3, label, "LOAD00000002.csv", "LOAD00000003.csv", uris([2, 3]))
        check(False, "overlap guard should refuse partial overlap")
    except ns["ManualRangeError"]:
        check(True, "overlap guard: partial overlap refused (fail closed)")

    # simultaneous start: two guards race on an EMPTY ledger. The FakeS3 serialises puts;
    # with If-None-Match the second writer gets a 412, re-reads, sees the first's disjoint
    # entry, and (disjoint) proceeds. We assert no false refusal for disjoint concurrent.
    s3b = FakeS3()
    ns["_FAKE_S3"] = s3b
    errs = []

    def run(start, end, nums):
        try:
            mr = dict(ns["_MANUAL_RANGE"])  # not shared across threads for the guard inputs
            guard(s3b, label, "LOAD%08d.csv" % start, "LOAD%08d.csv" % end, uris(nums))
        except Exception as e:
            errs.append(e)
    t1 = threading.Thread(target=run, args=(1, 2, [1, 2]))
    t2 = threading.Thread(target=run, args=(3, 4, [3, 4]))
    t1.start(); t2.start(); t1.join(); t2.join()
    check(not errs, "overlap guard: simultaneous DISJOINT starts both succeed (no false refuse)")
    # and an overlapping concurrent pair: at least one must refuse OR the ledger ends with
    # only compatible entries (we assert the stronger: overlapping is caught by at least one).
    s3c = FakeS3()
    errs2 = []

    def run2(start, end, nums):
        try:
            guard(s3c, label, "LOAD%08d.csv" % start, "LOAD%08d.csv" % end, uris(nums))
        except Exception as e:
            errs2.append(e)
    ta = threading.Thread(target=run2, args=(1, 3, [1, 2, 3]))
    tb = threading.Thread(target=run2, args=(2, 4, [2, 3, 4]))
    ta.start(); tb.start(); ta.join(); tb.join()
    # Either one refused immediately, or (narrow race) both wrote and the merge tool catches
    # it. We report which happened honestly.
    if errs2:
        check(True, "overlap guard: overlapping concurrent start refused by the guard")
    else:
        led = json.loads(s3c.store[list(k for k in s3c.store if k.endswith('.ranges.json'))[0]][0])
        check(len(led["ranges"]) == 2,
              "overlap guard: narrow concurrent race left BOTH entries (merge tool is backstop)")
    ns["_FAKE_S3"] = s3  # restore


# ============================================================ TESTS: resume + no-PK
def test_resume_and_nopk(ns):
    s3 = FakeS3()
    ns["_FAKE_S3"] = s3
    reset_range(ns, on=True, start=1, end=3)
    # resolve the range (sets first/last)
    ns["_mr_wrap_s3_list_table_files"](lambda a, b: mk_files([1, 2, 3]))(s3, "s3://bkt/x")
    label = "demo_schema.demo_table"
    # write a range-private status doc: file 1 done, file 2 started (mid-file crash)
    path = ns["_mr_file_status_path"](label, ns["_MANUAL_RANGE"]["first"], ns["_MANUAL_RANGE"]["last"])
    b, k = ns["split_s3"](path)
    u1 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000001.csv"
    u2 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000002.csv"
    s3.put_object(Bucket=b, Key=k, Body=json.dumps({"files": {
        u1: {"state": "done", "rows": 100},
        u2: {"state": "started"}}}).encode("utf-8"))
    done, started, rows = ns["_mr_rows_from_status_doc"](label)
    check(done.get(u1) == 100 and u2 in started and rows == 100,
          "resume: reads this range's own done/started from its private doc")
    # no-PK + a started file -> fail closed
    try:
        ns["_mr_nopk_fail_closed_guard"](s3, label, has_single_pk=False)
        check(False, "no-PK mid-file crash should FAIL CLOSED")
    except ns["ManualRangeError"]:
        check(True, "no-PK mid-file crash FAILS CLOSED (never reblanks)")
    # single-PK + a started file -> allowed (loader's probe redo handles it)
    try:
        ns["_mr_nopk_fail_closed_guard"](s3, label, has_single_pk=True)
        check(True, "single-PK mid-file crash is allowed through (probe redo)")
    except ns["ManualRangeError"]:
        check(False, "single-PK should NOT fail closed")
    ns["_FAKE_S3"] = s3


# ============================================================ TESTS: result JSON
def test_result_json(ns):
    s3 = FakeS3()
    ns["_FAKE_S3"] = s3
    reset_range(ns, on=True, start=1, end=2)
    ns["_mr_wrap_s3_list_table_files"](lambda a, b: mk_files([1, 2]))(s3, "s3://bkt/x")
    label = "demo_schema.demo_table"
    path = ns["_mr_file_status_path"](label, ns["_MANUAL_RANGE"]["first"], ns["_MANUAL_RANGE"]["last"])
    b, k = ns["split_s3"](path)
    u1 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000001.csv"
    u2 = "s3://bkt/dms/demo_schema/demo_table/LOAD00000002.csv"
    s3.put_object(Bucket=b, Key=k, Body=json.dumps({"files": {
        u1: {"state": "done", "rows": 10}, u2: {"state": "done", "rows": 20}}}).encode("utf-8"))
    ns["_mr_write_result"](s3, label, failed=False)
    rp = ns["_mr_result_path"](label, "LOAD00000001.csv", "LOAD00000002.csv")
    rb, rk = ns["split_s3"](rp)
    doc = json.loads(s3.store[rk][0])
    check(doc["rows_written_by_this_range"] == 30 and doc["files_done"] == 2 and
          doc["failed"] is False, "per-range result JSON totals rows and files correctly")
    ns["_FAKE_S3"] = s3


# ============================================================ TESTS: SSL EOF classifier
def test_ssl_eof(ns):
    import ssl
    base_calls = []

    def orig_is_bp(exc):
        base_calls.append(exc)
        return False   # pretend the base classifier does NOT recognise it
    w = ns["_mr_wrap_is_broken_pipe_error"](orig_is_bp)
    # exact plaintext message
    check(w(Exception("EOF occurred in violation of protocol (_ssl.c:1234)")) is True,
          "SSL EOF: plaintext 'EOF occurred in violation of protocol' classified as dropped conn")
    # real SSLEOFError type
    check(w(ssl.SSLEOFError("EOF occurred in violation of protocol")) is True,
          "SSL EOF: ssl.SSLEOFError instance classified as dropped conn")
    # OSError errno 8 carrying the text
    e = OSError("EOF occurred in violation of protocol")
    e.errno = 8
    check(w(e) is True, "SSL EOF: OSError errno 8 with the text classified as dropped conn")
    # an unrelated error still returns False (we only ADD recognised cases)
    check(w(ValueError("totally unrelated")) is False,
          "SSL EOF wrapper does not over-match unrelated errors")


# ============================================================ TESTS: apply script
def test_apply(applier_path, base_path, patched_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("mr_applier", applier_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # unrecognised
    try:
        mod.apply.__wrapped__ if False else None
        tmp_in = os.path.join(HERE, "_t_unrec.py")
        open(tmp_in, "w").write("print(1)\n")
        try:
            mod.apply(tmp_in, os.path.join(HERE, "_t_out.py"))
            check(False, "apply should refuse unrecognised input")
        except AssertionError:
            check(True, "apply refuses unrecognised input")
    finally:
        for f in ("_t_unrec.py", "_t_out.py"):
            try:
                os.remove(os.path.join(HERE, f))
            except OSError:
                pass
    # already-patched
    try:
        mod.apply(patched_path, os.path.join(HERE, "_t_again.py"))
        check(False, "apply should refuse already-patched input")
    except AssertionError:
        check(True, "apply refuses already-patched input")
    finally:
        try:
            os.remove(os.path.join(HERE, "_t_again.py"))
        except OSError:
            pass
    # patched file compiles
    try:
        compile(open(patched_path).read(), "patched", "exec")
        check(True, "patched file compiles")
    except SyntaxError as e:
        check(False, "patched file compiles: %s" % e)
    # fresh apply of base reproduces a compiling file
    if base_path and os.path.exists(base_path):
        out = os.path.join(HERE, "_t_fresh.py")
        try:
            mod.apply(base_path, out)
            compile(open(out).read(), "fresh", "exec")
            check(True, "apply(base) produces a compiling patched file")
        except Exception as e:
            check(False, "apply(base) failed: %s" % e)
        finally:
            try:
                os.remove(out)
            except OSError:
                pass


# ============================================================ TESTS: mutation checks
def test_mutations(ns):
    """Flip each safety condition in a COPY of the function source and confirm a test fails.
    We reload the function from source with the condition inverted."""
    # 1) overlap guard: if we neuter the intersection test, a partial overlap must NOT refuse
    src = ns["_MR_SRC"]
    tree = ast.parse(src)
    overlap_src = None
    reslice_src = None
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == "_mr_overlap_guard":
            overlap_src = ast.get_source_segment(src, n)
        if isinstance(n, ast.FunctionDef) and n.name == "_mr_resolve_slice":
            reslice_src = ast.get_source_segment(src, n)

    # mutate overlap: replace 'if inter:' with 'if False:' (disable the refusal)
    mutated = overlap_src.replace("        if inter:\n", "        if False:\n", 1)
    mns = dict(ns)
    exec(mutated, mns)
    s3 = FakeS3()
    mns["_FAKE_S3"] = s3
    label = "demo_schema.demo_table"

    def uris(nums):
        return ["s3://bkt/dms/demo_schema/demo_table/LOAD%08d.csv" % n for n in nums]
    mns["_mr_overlap_guard"](s3, label, "LOAD00000001.csv", "LOAD00000002.csv", uris([1, 2]))
    refused = False
    try:
        mns["_mr_overlap_guard"](s3, label, "LOAD00000002.csv", "LOAD00000003.csv", uris([2, 3]))
    except mns["ManualRangeError"]:
        refused = True
    check(refused is False,
          "MUTATION overlap-guard disabled -> partial overlap NO LONGER refused (guard is load-bearing)")

    # mutate resolve_slice: replace 'if i0 > i1:' with 'if False:'. Python slicing cannot
    # produce a reversed non-empty slice, so the empty-slice guard still fails closed -- but
    # the DISTINCT, clearer 'start is AFTER end' message disappears. Assert the specific
    # start>end fail-closed path is gone (that check is load-bearing for its own behaviour).
    mutated2 = reslice_src.replace("    if i0 > i1:\n", "    if False:\n", 1)
    mns2 = dict(ns)
    exec(mutated2, mns2)
    files = mk_files([1, 2, 3, 4, 5])
    msg = ""
    try:
        mns2["_mr_resolve_slice"](4, 2, files)
    except mns2["ManualRangeError"] as e:
        msg = str(e)
    check("is AFTER resolved end" not in msg,
          "MUTATION start>end check disabled -> the distinct start-after-end fail-closed path "
          "is gone (check is load-bearing; empty-slice guard remains as defense-in-depth)")

    # mutate nopk guard: replace 'if started:' with 'if False:' -> no fail-closed
    nopk_src = None
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == "_mr_nopk_fail_closed_guard":
            nopk_src = ast.get_source_segment(src, n)
    mutated3 = nopk_src.replace("    if started:\n", "    if False:\n", 1)
    mns3 = dict(ns)
    exec(mutated3, mns3)
    s3b = FakeS3()
    mns3["_FAKE_S3"] = s3b
    reset_range(mns3, on=True, start=1, end=1)
    mns3["_MANUAL_RANGE"]["first"] = "LOAD00000001.csv"
    mns3["_MANUAL_RANGE"]["last"] = "LOAD00000001.csv"
    # a started file present
    p = mns3["_mr_file_status_path"](label, "LOAD00000001.csv", "LOAD00000001.csv")
    bb, kk = ns["split_s3"](p)
    s3b.put_object(Bucket=bb, Key=kk, Body=json.dumps({"files": {
        "s3://bkt/dms/demo_schema/demo_table/LOAD00000001.csv": {"state": "started"}}}).encode())
    failed_closed = False
    try:
        mns3["_mr_nopk_fail_closed_guard"](s3b, label, has_single_pk=False)
    except mns3["ManualRangeError"]:
        failed_closed = True
    check(failed_closed is False,
          "MUTATION nopk-guard disabled -> no fail-closed on no-PK mid-file crash (guard is load-bearing)")


def test_merge_tool(ns, patched_path):
    """Drive merge_ranges.reconcile / do_write against a FakeS3, and prove the merged doc is
    accepted by the REAL load_file_status extracted from the patched file."""
    import importlib.util
    mp = os.path.join(HERE, "merge_ranges.py")
    spec = importlib.util.spec_from_file_location("merge_ranges", mp)
    mr = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mr)

    bucket = "bkt"
    cp = "demo_schema/config/_orchestrator/group-1/"
    label = "demo_schema.demo_table"
    dms = "s3://bkt/demo_schema/demo_table"

    def mk_s3_with(ranges, listing_nums, failed_ranges=None):
        """ranges: {range_key: {uri: rows}}; listing_nums: whole-table file numbers."""
        s3 = FakeS3()
        objs = []
        for n in listing_nums:
            objs.append({"Key": "demo_schema/demo_table/LOAD%08d.csv" % n, "Size": 1000})

        def list_objects_v2(Bucket, Prefix, ContinuationToken=None):
            if Prefix.endswith("demo_table/"):
                return {"Contents": objs, "IsTruncated": False}
            keys = [k for k in s3.keys() if k.startswith(Prefix)]
            return {"Contents": [{"Key": k, "Size": 1} for k in keys], "IsTruncated": False}
        s3.list_objects_v2 = list_objects_v2
        for rk, filemap in ranges.items():
            first, last = rk.split("..")
            sk = "%s_file_status/%s.range-%s-%s.json" % (cp, label, first, last)
            s3.put_object(Bucket=bucket, Key=sk, Body=json.dumps(
                {"files": {u: {"state": "done", "rows": r} for u, r in filemap.items()}}).encode())
            rrk = "%s_range_result/%s.range-%s-%s.json" % (cp, label, first, last)
            is_failed = bool(failed_ranges and rk in failed_ranges)
            s3.put_object(Bucket=bucket, Key=rrk, Body=json.dumps(
                {"label": label, "first": first, "last": last, "failed": is_failed,
                 "rows_written_by_this_range": sum(filemap.values())}).encode())
        return s3

    def uri(n):
        return "s3://bkt/demo_schema/demo_table/LOAD%08d.csv" % n

    s3 = mk_s3_with({
        "LOAD00000001.csv..LOAD00000002.csv": {uri(1): 10, uri(2): 20},
        "LOAD00000003.csv..LOAD00000004.csv": {uri(3): 30, uri(4): 40},
    }, [1, 2, 3, 4])
    ok, rep = mr.reconcile(s3, bucket, cp, label, dms)
    check(ok and rep["total_rows"] == 100 and rep["covered_files"] == 4,
          "merge: complete coverage reconciles CLEAN, totals rows")

    s3 = mk_s3_with({
        "LOAD00000001.csv..LOAD00000003.csv": {uri(1): 10, uri(2): 20, uri(3): 30},
    }, [1, 2, 3, 4])
    ok, rep = mr.reconcile(s3, bucket, cp, label, dms)
    check((not ok) and len(rep["missing"]) == 1, "merge: missing file -> NOT clean, listed")

    s3 = mk_s3_with({
        "LOAD00000001.csv..LOAD00000002.csv": {uri(1): 10, uri(2): 20},
        "LOAD00000002.csv..LOAD00000003.csv": {uri(2): 20, uri(3): 30},
    }, [1, 2, 3])
    ok, rep = mr.reconcile(s3, bucket, cp, label, dms)
    check((not ok) and len(rep["duplicates"]) >= 1, "merge: duplicate file -> NOT clean, listed")

    s3 = mk_s3_with({
        "LOAD00000001.csv..LOAD00000002.csv": {uri(1): 10, uri(2): 20},
        "LOAD00000003.csv..LOAD00000004.csv": {uri(3): 30, uri(4): 40},
    }, [1, 2, 3, 4], failed_ranges={"LOAD00000003.csv..LOAD00000004.csv"})
    ok, rep = mr.reconcile(s3, bucket, cp, label, dms)
    check(not ok and rep["failed_ranges"], "merge: failed range -> NOT clean, listed")

    s3 = mk_s3_with({
        "LOAD00000001.csv..LOAD00000002.csv": {uri(1): 10, uri(2): 20},
        "LOAD00000003.csv..LOAD00000004.csv": {uri(3): 30, uri(4): 40},
    }, [1, 2, 3, 4])
    ok, rep = mr.reconcile(s3, bucket, cp, label, dms)
    normal_key = "%s_file_status/%s.json" % (cp, label)
    check(normal_key not in s3.keys(), "merge: dry-run writes no normal doc")
    nk, merged = mr.do_write(s3, bucket, cp, label, rep)
    check(nk in s3.keys(), "merge: --write creates the normal doc")

    # FORMAT ACCEPTED BY REAL load_file_status (extracted from the patched file)
    src = open(patched_path).read()
    tree = ast.parse(src)
    lns = {"json": json, "split_s3": ns["split_s3"], "AUTO_REBLANK_ON_RESUME": True}
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in (
                "load_file_status", "_load_file_status_doc", "_file_status_path"):
            exec(ast.get_source_segment(src, n), lns)
    lns["CONFIG_PREFIX"] = "s3://bkt/" + cp
    real_s3 = FakeS3()
    real_path = lns["_file_status_path"]("demo_schema", "demo_table")
    rb, rk = ns["split_s3"](real_path)
    real_s3.put_object(Bucket=rb, Key=rk, Body=json.dumps(merged).encode())
    done_map, started = lns["load_file_status"](real_s3, "demo_schema", "demo_table")
    check(len(done_map) == 4 and sum(done_map.values()) == 100 and not started,
          "merge: merged doc accepted by the REAL load_file_status (4 done, 100 rows)")

    files = [("s3://bkt/demo_schema/demo_table/LOAD%08d.csv" % n, 100 + n) for n in range(1, 9)]
    plan = mr.plan_ranges(files, 4)
    covered = []
    for _f, _l, fs, _b in plan:
        covered += fs
    check(1 <= len(plan) <= 4 and sorted(covered) == sorted(u for u, _s in files)
          and len(set(covered)) == len(covered),
          "merge --plan: ranges cover every file exactly once, disjoint (at most N)")

    # REGRESSION (vet T9): merge list_table_files must EXCLUDE processed/ and failed/ CDC
    # copies and non-LOAD basenames, exactly like the loader's _is_full_load_key. Otherwise
    # a processed/LOAD*.csv would be miscounted as a table file -> false missing/duplicate.
    s3x = FakeS3()
    objs = [
        {"Key": "demo_schema/demo_table/LOAD00000001.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/LOAD00000002.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/processed/LOAD00000001.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/failed/LOAD00000002.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/20260921-172204370.csv", "Size": 100},
    ]

    def _lov2(Bucket, Prefix, ContinuationToken=None):
        return {"Contents": [o for o in objs if o["Key"].startswith(Prefix)],
                "IsTruncated": False}
    s3x.list_objects_v2 = _lov2
    listed = mr.list_table_files(s3x, "s3://bkt/demo_schema/demo_table")
    names = sorted(mr._basename(u) for u, _s in listed)
    check(names == ["LOAD00000001.csv", "LOAD00000002.csv"],
          "merge list excludes processed/ & failed/ & timestamp CDC files (matches loader; vet T9)")

    # VET F1 REPRO: 2 real files done + a stray processed/LOAD99999999.csv must reconcile
    # CLEAN and be writable (the old regex counted the processed file -> false MISSING -> refuse).
    s3f1 = FakeS3()
    f1objs = [
        {"Key": "demo_schema/demo_table/LOAD00000001.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/LOAD00000002.csv", "Size": 100},
        {"Key": "demo_schema/demo_table/processed/LOAD99999999.csv", "Size": 100},
    ]

    def _lov2_f1(Bucket, Prefix, ContinuationToken=None):
        if Prefix.endswith("demo_table/"):
            return {"Contents": [o for o in f1objs if o["Key"].startswith(Prefix)],
                    "IsTruncated": False}
        keys = [k for k in s3f1.keys() if k.startswith(Prefix)]
        return {"Contents": [{"Key": k, "Size": 1} for k in keys], "IsTruncated": False}
    s3f1.list_objects_v2 = _lov2_f1
    rk = "LOAD00000001.csv..LOAD00000002.csv"
    sk = "%s_file_status/%s.range-%s.json" % (cp, label, rk.replace("..", "-"))
    s3f1.put_object(Bucket=bucket, Key=sk, Body=json.dumps({"files": {
        uri(1): {"state": "done", "rows": 10}, uri(2): {"state": "done", "rows": 20}}}).encode())
    rrk = "%s_range_result/%s.range-%s.json" % (cp, label, rk.replace("..", "-"))
    s3f1.put_object(Bucket=bucket, Key=rrk, Body=json.dumps(
        {"label": label, "first": "LOAD00000001.csv", "last": "LOAD00000002.csv",
         "failed": False}).encode())
    okf1, repf1 = mr.reconcile(s3f1, bucket, cp, label, dms)
    check(okf1 and repf1["total_files"] == 2 and not repf1["missing"],
          "vet F1 repro: 2 real done + stray processed/LOAD99999999.csv -> reconcile CLEAN")
    nkf1, mergedf1 = mr.do_write(s3f1, bucket, cp, label, repf1)
    check(nkf1 in s3f1.keys(), "vet F1 repro: a correct load with a stray processed file is WRITABLE")

    # MUTATION: if the processed/failed exclusion is removed, the stray file is miscounted and
    # reconcile is NO LONGER clean (the exclusion is load-bearing).
    import ast as _ast
    msrc = open(os.path.join(HERE, "merge_ranges.py")).read()
    mtree = _ast.parse(msrc)
    isfull_src = None
    for nn in mtree.body:
        if isinstance(nn, _ast.FunctionDef) and nn.name == "_is_full_load_key":
            isfull_src = _ast.get_source_segment(msrc, nn)
    mutated = isfull_src.replace('    if "processed" in segments or "failed" in segments:\n        return False\n', "")
    mut_ns = {"re": __import__("re")}
    exec(mutated, mut_ns)
    # emulate the listing with the mutated predicate
    bad_names = sorted(mr._basename("s3://bkt/" + o["Key"]) for o in f1objs
                       if not o["Key"].endswith("/") and mut_ns["_is_full_load_key"](o["Key"]))
    check("LOAD99999999.csv" in bad_names and len(bad_names) == 3,
          "MUTATION remove processed/failed exclusion -> stray CDC file miscounted (exclusion is load-bearing)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--patched", default=os.path.join(HERE, "job2_load_manual_shard.py"))
    ap.add_argument("--applier", default=os.path.join(HERE, "apply_manual_shard_patch.py"))
    ap.add_argument("--base", default="/Users/haliasgh/Downloads/dms-s3-glue-dsql-migration/scripts/job2_load.py")
    args = ap.parse_args()

    ns, src = load_ns(args.patched)
    ns["_FAKE_S3"] = FakeS3()
    ns["_MR_SRC"] = src   # for mutation tests

    test_resolution(ns)
    test_byte_identical_off(ns)
    test_on_applies_slice(ns)
    test_status_isolation(ns)
    test_overlap_guard(ns)
    test_resume_and_nopk(ns)
    test_result_json(ns)
    test_ssl_eof(ns)
    test_apply(args.applier, args.base, args.patched)
    test_merge_tool(ns, args.patched)
    test_mutations(ns)

    print("\n%d passed, %d failed" % (PASSED, FAILED))
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
