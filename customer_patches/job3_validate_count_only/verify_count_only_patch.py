#!/usr/bin/env python3
"""Check that the count-only patch is correctly applied to a job3_validate.py (offline, standard library only).

  python3 verify_count_only_patch.py FILE [--backup ORIGINAL] [--applier apply_count_only_patch.py] [--job NAME ...]

FILE      the patched script to check: a local path, or s3://bucket/scripts/job3_validate.py
          (downloaded with the aws cli). Check the copy in S3 - it is the one Glue runs.
--backup  the deployed script BEFORE patching (job3_validate.bak.py). Proves the only change is the patch.
--applier the apply_count_only_patch.py you used (default: the one in the current folder).
--job     a Glue validate job name (repeatable): checks the job reads this script and has checksum_mode=off.
Exit code 0 = every check passed, 1 = at least one FAIL.
"""
import argparse
import ast
import builtins
import contextlib
import difflib
import hashlib
import io
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

EXPECTED_BLOCK_SHA256 = "d941ebbde41677bfa3f44ac14a62a484471bff6228f06a5ef26241c4cf9443a8"
PUBLISHED_FULL_SHA256 = "e561fe603b570457a8bd21480d330d2eb415c4dce68fa5c1e538a19e5a457276"
ORIGINAL_REPO_SHA256 = "51fa217fdeff88d15aaa1146569ed71c7a05a637f11ef94c1c05607de46dfa51"
PATCH_DEFS = ("CountRangeError", "CountSplit", "CountStop", "count_only_validate")
CALL_VARS = ["label", "dsql_schema", "dsql_table", "entry", "meta", "pk_cols", "pk_meta",
             "rangeable", "pk_col", "pk_kind"]
HELPERS = {"is_txn_age_error": 1, "_target_query": 4, "connect_dsql": 0,
           "dms_full_load_rows_for": 4, "make_boto_client": 1}
RESULTS = []


def rec(status, msg):
    RESULTS.append((status, msg))
    print("[%s] %s" % (status, msg))


def sha(b):
    return hashlib.sha256(b).hexdigest()


def aws(args, binary=False):
    import subprocess
    r = subprocess.run(["aws"] + args, capture_output=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or b"").decode("utf-8", "replace").strip()[:300])
    return r.stdout if binary else r.stdout.decode("utf-8")


def is_patch_name(n):
    return n.startswith("_cnt_") or n in PATCH_DEFS


def module_names(tree):
    names, stack = set(), list(tree.body)
    while stack:
        n = stack.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(n.name)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for a in n.names:
                names.add((a.asname or a.name).split(".")[0])
        elif isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            for t in (n.targets if isinstance(n, ast.Assign) else [n.target]):
                for x in ast.walk(t):
                    if isinstance(x, ast.Name):
                        names.add(x.id)
        elif isinstance(n, (ast.If, ast.Try, ast.With, ast.For, ast.While)):
            for fld in ("body", "orelse", "finalbody", "handlers"):
                for c in getattr(n, fld, None) or []:
                    stack.extend(c.body if isinstance(c, ast.ExceptHandler) else [c])
    return names


def undefined_names(tree):
    mod = module_names(tree) | set(dir(builtins))
    bad = set()
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and is_patch_name(n.name):
            local = set()
            for x in ast.walk(n):
                if isinstance(x, ast.arg):
                    local.add(x.arg)
                elif isinstance(x, ast.Name) and isinstance(x.ctx, (ast.Store, ast.Del)):
                    local.add(x.id)
                elif isinstance(x, (ast.FunctionDef, ast.ClassDef)):
                    local.add(x.name)
                elif isinstance(x, ast.ExceptHandler) and x.name:
                    local.add(x.name)
                elif isinstance(x, (ast.Import, ast.ImportFrom)):
                    for a in x.names:
                        local.add((a.asname or a.name).split(".")[0])
            for x in ast.walk(n):
                if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Load) and x.id not in local and x.id not in mod:
                    bad.add(x.id)
    return sorted(bad)


def check_helpers(tree):
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    for name, minpos in HELPERS.items():
        f = fns.get(name)
        if f is None:
            rec("FAIL", "your script has no module-level function %s(), which the patch calls" % name)
            continue
        npos = len(f.args.posonlyargs) + len(f.args.args)
        ok = npos >= minpos
        if name == "connect_dsql":
            ok = ok and any(a.arg == "autocommit" for a in f.args.args + f.args.kwonlyargs)
        rec("PASS" if ok else "FAIL", "%s() has the signature the patch expects (%d positional args%s)" % (
            name, npos, ", autocommit" if name == "connect_dsql" else ""))


def check_call_site(tree):
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "validate_one_table"]
    if len(fns) != 1:
        rec("FAIL", "validate_one_table() not found exactly once")
        return
    fn = fns[0]
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "count_only_validate"]
    if len(calls) != 1:
        rec("FAIL", "count_only_validate() is called %d times inside validate_one_table (must be exactly 1)" % len(calls))
        return
    call = calls[0]
    assigned = {}
    for n in ast.walk(fn):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
            assigned[n.id] = min(assigned.get(n.id, 10 ** 9), n.lineno)
        elif isinstance(n, ast.arg):
            assigned[n.arg] = min(assigned.get(n.arg, 10 ** 9), fn.lineno)
    missing = [v for v in CALL_VARS if v not in assigned or assigned[v] >= call.lineno]
    rec("PASS" if not missing else "FAIL",
        "the call at line %d only uses variables already set earlier in validate_one_table%s" % (
            call.lineno, "" if not missing else " (NOT set before the call: %s)" % missing))
    args = [a.id if isinstance(a, ast.Name) else "?" for a in call.args]
    rec("PASS" if args == CALL_VARS else "FAIL", "the call passes the 10 expected arguments in order")


def constants(tree):
    want = {"COUNT_TABLE_BUDGET_SECONDS": None, "COUNT_SPLIT_ROWS": None, "COUNT_RANGE_ROWS": None,
            "COUNT_RANGE_THREADS": None, "COUNT_RETRIES": None}
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in want:
                    try:
                        want[t.id] = ast.literal_eval(n.value)
                    except Exception:
                        pass
    return want


# ------------------------------------------------------------------ run the patch against a fake DSQL
class Fake(object):
    def __init__(self, keys, nulls=0, not_null=True):
        self.keys, self.nulls, self.not_null = sorted(keys), nulls, not_null
        self.limit = self.xx = None
        self.fail_first, self.always_fail, self.delay = 0, False, 0.0
        self.queries, self.preds, self.lock = 0, [], threading.Lock()

    def n_for(self, pred):
        if pred is None:
            return len(self.keys) + self.nulls
        if pred.endswith("IS NOT NULL"):
            return len(self.keys)
        if pred.endswith("IS NULL"):
            return self.nulls
        lo = hi = None
        m = re.search(r">= (-?\d+)", pred)
        if m:
            lo = int(m.group(1))
        m = re.search(r"< (-?\d+)", pred)
        if m:
            hi = int(m.group(1))
        return sum(1 for k in self.keys if (lo is None or k >= lo) and (hi is None or k < hi))

    def count(self, pred):
        n = self.n_for(pred)
        with self.lock:
            self.queries += 1
            self.preds.append(pred)
            if self.always_fail or self.fail_first > 0:
                if self.fail_first > 0:
                    self.fail_first -= 1
                raise ConnectionError("connection reset by peer")
        if self.delay:
            time.sleep(self.delay)
        if self.limit is not None and n > self.limit:
            raise Exception("canceling statement due to statement timeout")
        if self.xx is not None and n > self.xx:
            raise Exception({"S": "ERROR", "C": "XX000", "M": "server unavailable"})
        return n

    def connect(self, autocommit=True):
        eng = self

        class Cur(object):
            def execute(s, sql, params=None):
                if "information_schema" in sql:
                    s.r = ("NO" if eng.not_null else "YES",)
                elif "ORDER BY" in sql:
                    s.r = None if not eng.keys else ((eng.keys[0] if "ASC" in sql else eng.keys[-1]),)
                else:
                    raise AssertionError("unexpected SQL from the patch: " + sql)

            def fetchone(s):
                return s.r

            def close(s):
                pass

        class Conn(object):
            def cursor(s):
                return Cur()

            def close(s):
                pass
        return Conn()


def build_ns(src):
    tree = ast.parse(src)
    ns = {"time": time, "threading": threading, "ThreadPoolExecutor": ThreadPoolExecutor,
          "CHECKSUM_MODE": "off", "DMS_TASK_ARN": "arn:test", "COUNT_MISMATCH_TOLERANCE": 0}
    for n in tree.body:
        keep = False
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
            keep = is_patch_name(n.name) or n.name == "is_txn_age_error"
        elif isinstance(n, ast.Assign):
            keep = any(isinstance(t, ast.Name) and t.id.startswith("COUNT_") and t.id != "COUNT_MISMATCH_TOLERANCE"
                       for t in n.targets)
        if keep:
            exec(ast.get_source_segment(src, n), ns)
    return ns


def smoke(src):
    out = []
    ns = build_ns(src)
    defaults = {k: v for k, v in ns.items() if k.startswith("COUNT_") and k != "COUNT_MISMATCH_TOLERANCE"}

    def reset(**kw):
        ns.update(defaults)
        ns.update(COUNT_RETRY_WAIT=0, CHECKSUM_MODE="off", DMS_TASK_ARN="arn:test")
        ns.update(kw)

    def run(fake, expected):
        ns["_target_query"] = lambda schema, table, pred, metrics: (fake.count(pred), [])
        ns["connect_dsql"] = fake.connect
        ns["dms_full_load_rows_for"] = lambda *a: expected
        ns["make_boto_client"] = lambda s: object()
        with contextlib.redirect_stdout(io.StringIO()):
            return ns["count_only_validate"]("s.t", "s", "t", {}, {}, ["id"], {"pk_kind": "integer"}, True, "id", "integer")

    def case(label, fn):
        try:
            ok, detail = fn()
        except Exception as e:
            ok, detail = False, "raised %s: %s" % (type(e).__name__, str(e)[:120])
        out.append((ok, "%s%s" % (label, "" if ok else "  -> " + str(detail)[:200])))

    def c1():
        reset(); f = Fake(range(1, 1001)); r = run(f, 1000)
        return r["status"] == "match" and r["target_rows"] == 1000 and "warning" not in r, r

    def c2():
        reset(); r = run(Fake(range(1, 1001)), 1010)
        return r["status"] == "mismatch" and r["mismatches"][0]["type"] == "COUNT_DIFF", r

    def c3():
        reset(COUNT_RANGE_ROWS=50); f = Fake(range(1, 1001)); f.limit = 300; r = run(f, 1000)
        return r["target_rows"] == 1000 and f.queries > 5, r

    def c4():
        reset(COUNT_RANGE_ROWS=50); f = Fake(range(1, 1001)); f.xx = 300; r = run(f, 1000)
        return r["target_rows"] == 1000, r

    def c5():
        reset(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); f = Fake(range(1, 1001)); r = run(f, 1000)
        return r["target_rows"] == 1000 and None not in f.preds, r

    def c6():
        reset(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); f = Fake(range(1, 1001)); f.limit = 30; r = run(f, 1000)
        return r["target_rows"] == 1000, r

    def c7():
        reset(COUNT_SPLIT_ROWS=500, COUNT_RANGE_ROWS=100); f = Fake(range(1, 1001), nulls=7, not_null=False); r = run(f, 1007)
        return r["target_rows"] == 1007 and r["status"] == "match", r

    def c8():
        reset(); f = Fake(range(1, 1001)); f.fail_first = 2; r = run(f, 1000)
        return r["target_rows"] == 1000, r

    def c9():
        reset(); f = Fake(range(1, 1001)); f.always_fail = True; r = run(f, 1000)
        return r["status"] == "match" and r["target_rows"] is None and "warning" in r, r

    def c10():
        reset(CHECKSUM_MODE="aggregate"); r = run(Fake(range(1, 101)), 100)
        return r is None, r

    def c11():
        reset(COUNT_TABLE_BUDGET_SECONDS=0.3); f = Fake(range(1, 101)); f.delay = 1.0
        t0 = time.time(); r = run(f, 100); el = time.time() - t0
        return el < 0.9 and r["target_rows"] is None and "TIMED OUT" in r.get("warning", ""), (el, r)

    def c12():
        reset(COUNT_SPLIT_ROWS=1, COUNT_RANGE_ROWS=100); f = Fake(range(1, 1001))
        real = ns["_cnt_plan"]
        ns["_cnt_plan"] = lambda g0, g1, t, p: [(None, 500, g0, 500), (400, None, 400, g1 + 1)]
        try:
            r = run(f, 1000)
        finally:
            ns["_cnt_plan"] = real
        return r["target_rows"] is None and "RANGE CHECK FAILED" in r.get("warning", ""), r

    def c13():
        reset(DMS_TASK_ARN=None); f = Fake(range(1, 1001)); r = run(f, None)
        return r["target_rows"] == 1000 and f.queries == 1 and f.preds == [None], (r, f.preds)

    for label, fn in (
        ("count equals the expected row count -> table passes, exact count", c1),
        ("count differs from the expected row count -> table FAILS (COUNT_DIFF)", c2),
        ("whole-table COUNT hits the statement timeout -> key ranges, exact", c3),
        ("whole-table COUNT fails with XX000 'server unavailable' -> key ranges, exact", c4),
        ("table expected at the split size or more -> key ranges from the start, no whole-table COUNT", c5),
        ("ranges that time out are halved and summed exactly", c6),
        ("NULL-key rows are counted exactly once", c7),
        ("two transient errors are retried, then exact", c8),
        ("errors that never clear -> table passes with a WARNING, no count", c9),
        ("checksum_mode not 'off' -> patch steps aside (normal validation runs)", c10),
        ("a query that hangs -> returns at the time limit with a TIMED OUT warning", c11),
        ("overlapping ranges -> caught by the self-check, count not used, WARNING", c12),
        ("expected count unknown and table fits -> exactly one plain COUNT(*)", c13)):
        case(label, fn)
    return out


# ------------------------------------------------------------------ main
def main(argv=None):
    ap = argparse.ArgumentParser(description="Verify the count-only patch in a job3_validate.py")
    ap.add_argument("file")
    ap.add_argument("--backup")
    ap.add_argument("--applier")
    ap.add_argument("--job", action="append", default=[])
    a = ap.parse_args(argv)
    del RESULTS[:]

    # 1. the file under test
    try:
        raw = aws(["s3", "cp", a.file, "-"], binary=True) if a.file.startswith("s3://") else open(a.file, "rb").read()
        src = raw.decode("utf-8")
    except Exception as e:
        rec("FAIL", "cannot read %s: %s" % (a.file, e))
        return summary()
    file_sha = sha(raw)
    rec("INFO", "file: %s  (%d bytes, sha256 %s)" % (a.file, len(raw), file_sha))
    if file_sha == PUBLISHED_FULL_SHA256:
        rec("INFO", "identical to the published full script (customer_patches/job3_validate_count_only/job3_validate.py)")
    try:
        tree = ast.parse(src)
        compile(src, a.file, "exec")
        rec("PASS", "the file is valid Python (parses and compiles)")
    except SyntaxError as e:
        rec("FAIL", "SYNTAX ERROR line %s: %s" % (e.lineno, e.msg))
        return summary()

    # 2. the reference patch text
    cands = [a.applier] if a.applier else ["apply_count_only_patch.py"]
    try:
        cands.append(os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), "apply_count_only_patch.py"))
    except Exception:
        pass
    ref = None
    for c in cands:
        if c and os.path.exists(c):
            ns = {"__name__": "applier_ref"}
            try:
                exec(compile(open(c, encoding="utf-8").read(), c, "exec"), ns)
                ref = (c, ns["BLOCK"], ns["CALL"], ns["A1"], ns["A2"])
            except Exception as e:
                rec("FAIL", "cannot load %s: %s" % (c, e))
            break
    if ref is None:
        rec("FAIL", "apply_count_only_patch.py not found next to this script. Download it from the same GitHub folder first.")
        return summary()
    path, BLOCK, CALL, A1, A2 = ref
    got = hashlib.sha256(BLOCK.encode("utf-8")).hexdigest()
    rec("PASS" if got == EXPECTED_BLOCK_SHA256 else "FAIL",
        "%s carries the published patch text (block sha256 %s...)" % (os.path.basename(path), got[:12]))

    # 3. the patch text is in the file, once, in the right place
    n = src.count(BLOCK)
    rec("PASS" if n == 1 else "FAIL", "the patch block is present exactly once, character for character (found %d)" % n)
    n = src.count(BLOCK + A1)
    rec("PASS" if n == 1 else "FAIL", "the block sits directly above 'def validate_one_table' (found %d)" % n)
    n = src.count(A2 + "\n" + CALL)
    rec("PASS" if n == 1 else "FAIL", "the call sits directly under the 'target table not found in DSQL' return (found %d)" % n)
    d = len(re.findall(r"^def count_only_validate\(", src, re.M))
    c = src.count("count_only_validate(")
    rec("PASS" if (d == 1 and c == 2) else "FAIL", "count_only_validate is defined once and called once (defs %d, total mentions %d)" % (d, c))

    # 4. nothing else changed
    if a.backup:
        try:
            bak = open(a.backup, "rb").read().decode("utf-8")
            rev = src.replace(BLOCK + A1, A1, 1).replace(A2 + "\n" + CALL, A2, 1)
            if rev == bak:
                rec("PASS", "removing the patch gives back your backup byte for byte: the patch is the ONLY change (backup sha256 %s...)" % sha(bak.encode("utf-8"))[:12])
            else:
                diff = list(difflib.unified_diff(bak.splitlines(), rev.splitlines(), "backup", "file-without-patch", n=0, lineterm=""))
                rec("FAIL", "the file differs from your backup in more than the patch (%d diff lines). First: %s" % (len(diff), " | ".join(diff[2:6])[:300]))
            if sha(bak.encode("utf-8")) == ORIGINAL_REPO_SHA256:
                rec("INFO", "your backup is the unmodified repo version of job3_validate.py")
        except Exception as e:
            rec("FAIL", "cannot read backup %s: %s" % (a.backup, e))
    else:
        rec("WARN", "no --backup given: cannot prove that nothing else in the file changed")

    # 5. integration with the rest of YOUR script
    und = undefined_names(tree)
    rec("PASS" if not und else "FAIL", "every name the patch uses exists in your script%s" % ("" if not und else " (MISSING: %s)" % und))
    check_helpers(tree)
    check_call_site(tree)
    k = constants(tree)
    rec("INFO", "settings in the file: " + ", ".join("%s=%s" % (x, k[x]) for x in sorted(k)))
    b = k.get("COUNT_TABLE_BUDGET_SECONDS")
    rec("PASS" if (isinstance(b, (int, float)) and 0 < b <= 900) else "FAIL", "per-table time limit is %s seconds (must be 900 or less)" % b)

    # 6. run the patch's own code against a fake DSQL
    try:
        for ok, label in smoke(src):
            rec("PASS" if ok else "FAIL", "behaviour: " + label)
    except Exception as e:
        rec("FAIL", "could not run the behaviour tests: %s: %s" % (type(e).__name__, str(e)[:200]))

    # 7. the Glue jobs
    if a.job:
        import json
        for j in a.job:
            try:
                info = json.loads(aws(["glue", "get-job", "--job-name", j, "--output", "json"]))["Job"]
                loc = info["Command"]["ScriptLocation"]
                da = info.get("DefaultArguments") or {}
                body = aws(["s3", "cp", loc, "-"], binary=True)
            except Exception as e:
                rec("FAIL", "job %s: cannot read it (%s)" % (j, e))
                continue
            rec("PASS" if sha(body) == file_sha else "FAIL", "job %s reads %s -> %s" % (
                j, loc, "the file you verified" if sha(body) == file_sha else "a DIFFERENT script than the one verified"))
            cm = da.get("--checksum_mode")
            rec("PASS" if cm == "off" else "FAIL", "job %s: --checksum_mode = %r (must be 'off', otherwise the patch does nothing)" % (j, cm))
            rec("PASS" if da.get("--dms_task_arn") else "WARN", "job %s: --dms_task_arn %s" % (j, "is set" if da.get("--dms_task_arn") else "missing: the patch will compare with discovery's row count instead of DMS"))
            rec("INFO", "job %s: %s x %s" % (j, info.get("WorkerType"), info.get("NumberOfWorkers")))
    else:
        rec("WARN", "no --job given: not checked that your Glue validate jobs read this script and have checksum_mode=off")
    return summary()


def summary():
    f = sum(1 for s, _ in RESULTS if s == "FAIL")
    p = sum(1 for s, _ in RESULTS if s == "PASS")
    w = sum(1 for s, _ in RESULTS if s == "WARN")
    print("")
    print("RESULT: %s  (%d passed, %d failed, %d warnings)" % (
        "PATCH OK" if f == 0 else "PROBLEMS FOUND - DO NOT RUN", p, f, w))
    return 1 if f else 0


if __name__ == "__main__":
    sys.exit(main())
