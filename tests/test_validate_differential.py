#!/usr/bin/env python3
"""Differential / property test for scripts/job3_validate.py.

REAL Spark on the SOURCE side (the actual broadcast range-join, apply_transform, build_metrics
`src` aggregates) vs a FAITHFUL Python model of the TARGET side (what the loader stores through
its ::type casts, then the exact DSQL aggregate each metric's `agg` SQL computes). The target
model's DSQL semantics are PINNED from real-DSQL probes run this session (dsql_probe.py):
  * numeric -> {bigint,integer,smallint} rounds HALF-AWAY-FROM-ZERO  (0.5->1, 2.5->3, -2.5->-3);
  * char(n)::text is TRIMMED, not blank-padded (length('ab'::char(5))=2);
  * EXTRACT(EPOCH FROM ts) is NUMERIC (exact), *1000000 is exact;
  * md5()/strpos() available; the 6-hex-digit hash int matches _hash_int_from_md5;
  * text/char MIN/MAX use BYTE (C) ordering (matches Spark binary ordering for ASCII);
  * bytea_output='hex'; boolean renders 'true'/'false'; session TimeZone=UTC.

For CORRECT data the source and target summaries MUST agree on EVERY range. For each planted
corruption (drop/add a row, change one value, swap two values across ranges, change a value in a
specific range position incl. first/last/#49/#99) at least one metric MUST differ on the
affected range (else a false pass).

Deterministic (fixed seeds), offline, no AWS/network. Target: < 60s.
Run:  python3 tests/test_validate_differential.py       (prints a summary; exits non-zero on fail)
      pytest tests/test_validate_differential.py         (check() raises)
"""
import ast
import os
import sys
import math
import random
import hashlib
import threading
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN, getcontext

getcontext().prec = 80

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)

_passed = 0
_failed = 0
_fail_msgs = []


def check(cond, msg):
    global _passed, _failed
    if cond:
        _passed += 1
    else:
        _failed += 1
        _fail_msgs.append(msg)
        print(f"[FAIL] {msg}")
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise AssertionError(msg)


# --------------------------------------------------------------------------------------------
# Load the REAL functions from job3_validate.py (AST-extract, exec into a seeded namespace with
# real Spark functions injected). Same technique as tests/test_validate_merge_fixes.py.
# --------------------------------------------------------------------------------------------
def _load_job3(names, injected):
    with open(os.path.join(REPO, "scripts", "job3_validate.py")) as fh:
        src = fh.read()
    tree = ast.parse(src)
    want = set(names)
    segs = {}
    assigns = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in want:
            segs[node.name] = ast.get_source_segment(src, node)
        # also capture simple module-level constant assignments we need
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    assigns[tgt.id] = ast.get_source_segment(src, node)
    missing = want - set(segs)
    if missing:
        raise AssertionError(f"could not find defs {sorted(missing)} in job3_validate.py")
    ns = dict(injected)
    # inject the module constants the functions reference
    for cname in ("_TYPED_CATEGORIES", "CSV_NULL_VALUE", "_TS_FMT", "_HEX", "_HASH_DIGITS",
                  "TIMESTAMP_INPUT_FORMATS", "TIMESTAMP_TZ_FORMATS", "VALIDATE_SOURCE_RANGES_PER_PLAN",
                  "VALIDATE_HASH", "UUID_CANONICAL_RE", "UUID_RAW_HEX_RE"):
        if cname in assigns and cname not in ns:
            try:
                exec(compile(assigns[cname], f"<job3:{cname}>", "exec"), ns)
            except Exception:
                pass
    # NULL settings globals default to today's behaviour (no customer null_values/null_rules)
    ns.setdefault("NULL_VALUES", [])
    ns.setdefault("NULL_RULES", {})
    ns.setdefault("CSV_NULL_VALUE", "NULL")
    ns.setdefault("VALIDATE_HASH", "all")
    ns.setdefault("VALIDATE_SOURCE_RANGES_PER_PLAN", 50)
    ns.setdefault("_HASH_DIGITS", 6)
    ns.setdefault("_HEX", "0123456789abcdef")
    ns.setdefault("_TS_FMT", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    for n in names:
        exec(compile(segs[n], f"<job3:{n}>", "exec"), ns)
    return ns


# --------------------------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------------------------
def _spark():
    os.environ.setdefault("JAVA_HOME", os.popen("/usr/libexec/java_home -v 17 2>/dev/null").read().strip())
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("validate-diff")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.host", "127.0.0.1")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


# --------------------------------------------------------------------------------------------
# FAITHFUL TARGET MODEL  (what the loader stores, then the exact per-metric DSQL aggregate)
# DSQL semantics pinned from dsql_probe.py this session.
# --------------------------------------------------------------------------------------------
_HASH_DIGITS = 6
_HEX = "0123456789abcdef"


def _md5_hash_int(s):
    """int of the first 6 hex digits of md5(utf8(s)) -- matches _hash_int_from_md5 / Spark md5."""
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:_HASH_DIGITS], 16)


def _numeric_to_int(sv):
    """Model '%s::numeric::bigint' on DSQL: round half AWAY FROM ZERO at FULL precision."""
    d = Decimal(sv)
    return int(d.quantize(Decimal("1"), rounding=ROUND_HALF_UP))  # ROUND_HALF_UP = away-from-zero


def _epoch_us(iso):
    """Exact microseconds since epoch for an ISO 'yyyy-MM-dd HH:mm:ss(.ffffff)' (UTC)."""
    from datetime import datetime, timezone
    s = iso
    if "." in s:
        base, frac = s.split(".")
        frac = (frac + "000000")[:6]
    else:
        base, frac = s, "000000"
    dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    secs = int(dt.timestamp())
    return secs * 1000000 + int(frac)


def _epoch_us_date(iso):
    from datetime import datetime, timezone
    dt = datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000000


def target_summary_model(stored_rows, columns, target_types, with_hash, key_cols, column_kind,
                          VALIDATE_HASH):
    """Compute, per the EXACT metric definitions in build_metrics, the TARGET aggregate values
    over `stored_rows` (list of dicts col->stored python value or None). Mirrors each `agg` SQL.
    Returns the list of metric values in build_metrics order (so it lines up with the source)."""
    key_cols = {str(c).lower() for c in (key_cols or [])}
    vals = []
    for c in columns:
        dt, prec, scale, dtp = target_types[c]
        kind = column_kind(dt)
        colvals = [r.get(c) for r in stored_rows]
        nonnull = [v for v in colvals if v is not None]
        hash_this = bool(with_hash) and (VALIDATE_HASH == "all"
                                         or (VALIDATE_HASH == "keys" and c.lower() in key_cols))
        # non-null count (every column)
        vals.append(("count", c, len(nonnull)))
        if kind in ("text", "char", "uuid", "bytea"):
            def _as_text(v):
                return v  # stored value already the ::text form (char trimmed per DSQL probe)
            if kind != "uuid":
                vals.append(("len", c, sum(len(_as_text(v)) for v in nonnull)))
            if hash_this:
                vals.append(("hash", c, sum(_md5_hash_int(_as_text(v)) for v in nonnull)))
            else:
                mn = min(nonnull) if nonnull else None
                mx = max(nonnull) if nonnull else None
                vals.append(("min", c, mn))
                vals.append(("max", c, mx))
        elif kind == "bool":
            vals.append(("truecnt", c, sum(1 for v in nonnull if v is True or v == "true")))
        elif kind == "int":
            vals.append(("sum", c, sum(int(v) for v in nonnull)))
        elif kind == "numeric":
            exact = scale is not None and 0 <= int(scale) <= 18 and (prec is None or int(prec) <= 31)
            s_eff = int(scale) if exact else 10
            tot = Decimal(0)
            for v in nonnull:
                tot += Decimal(str(v)).quantize(Decimal(1).scaleb(-s_eff))
            vals.append(("sum", c, tot))
        elif kind == "float":
            vals.append(("sum", c, sum(float(v) for v in nonnull)))
        elif kind in ("ts", "date"):
            tot = 0
            for v in nonnull:
                tot += _epoch_us(v) if kind == "ts" else _epoch_us_date(v)
            vals.append(("sum", c, tot))
    return vals


# --------------------------------------------------------------------------------------------
# Data generation. Each table: a PK (uuid/integer/text/none) + payload columns of varied types.
# We generate the DMS CSV *string* form (what Spark reads) AND the loader-stored python value
# (what the target holds). For CORRECT data the two are the loader transform of the same datum.
# --------------------------------------------------------------------------------------------
def _rand_uuid(rng):
    h = "".join(rng.choice("0123456789abcdef") for _ in range(32))
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _gen_table(rng, pk_kind, nrows, col_spec):
    """Return (csv_rows, stored_rows, pk_src_col, columns, target_types, type_categories).

    csv_rows: list of dict col->string (DMS CSV cell text, '' or 'NULL' for null markers).
    stored_rows: list of dict col->python stored value (None for NULL) = loader transform.
    """
    columns = [c for c, _ in col_spec]
    target_types = {}
    type_categories = {}
    # DSQL information_schema-style (data_type, numeric_precision, numeric_scale, datetime_precision)
    TYPEMAP = {
        "int": ("bigint", 64, 0, None), "intcol": ("integer", 32, 0, None),
        "num2": ("numeric", 20, 2, None), "num_big": ("numeric", 40, 25, None),
        "float": ("double precision", None, None, None), "real": ("real", None, None, None),
        "text": ("character varying", None, None, None), "char5": ("character", None, None, None),
        "uuid": ("uuid", None, None, None), "bool": ("boolean", None, None, None),
        "date": ("date", None, None, None), "ts6": ("timestamp without time zone", None, None, 6),
        "bytea": ("bytea", None, None, None),
    }
    CATMAP = {
        "int": "bigint", "intcol": "integer", "num2": "numeric", "num_big": "numeric",
        "float": "float", "real": "real", "text": None, "char5": None, "uuid": "uuid",
        "bool": "boolean", "date": "date", "ts6": "timestamptz", "bytea": "bytea",
    }
    for c, t in col_spec:
        target_types[c] = TYPEMAP[t]
        type_categories[c] = CATMAP[t]

    # build PK values (sorted/unique for integer & uuid so ranges are clean)
    if pk_kind == "integer":
        base = rng.sample(range(-5, 10_000_000), nrows) if nrows else []
        pkvals = sorted(base)
        pk_src_col = "_pk"
    elif pk_kind == "uuid":
        pkvals = sorted({_rand_uuid(rng) for _ in range(nrows)})
        while len(pkvals) < nrows:
            pkvals.append(_rand_uuid(rng))
        pkvals = sorted(pkvals)[:nrows]
        pk_src_col = "_pk"
    elif pk_kind == "text":
        alph = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        s = {"".join(rng.choice(alph) for _ in range(rng.randint(3, 10))) for _ in range(nrows * 2)}
        pkvals = sorted(s)[:nrows]
        pk_src_col = "_pk"
    else:
        pkvals = [None] * nrows
        pk_src_col = None

    csv_rows, stored_rows = [], []
    for i in range(nrows):
        craw, sraw = {}, {}
        if pk_kind == "integer":
            craw[pk_src_col] = str(pkvals[i]); sraw[pk_src_col] = pkvals[i]
        elif pk_kind == "uuid":
            # mix raw-hex (no dashes) and upper/lower to exercise canonicalization
            u = pkvals[i]
            form = rng.choice(["canon", "upper", "nodash"])
            craw[pk_src_col] = {"canon": u, "upper": u.upper(), "nodash": u.replace("-", "")}[form]
            sraw[pk_src_col] = u  # stored canonical lower
        elif pk_kind == "text":
            craw[pk_src_col] = pkvals[i]; sraw[pk_src_col] = pkvals[i]
        for c, t in col_spec:
            cell, stored = _gen_cell(rng, t, i)
            craw[c] = cell; sraw[c] = stored
        csv_rows.append(craw); stored_rows.append(sraw)
    cols = ([pk_src_col] if pk_src_col else []) + columns
    # PK column type (for its own metric): integer->bigint, uuid->uuid, text->varchar
    if pk_src_col:
        if pk_kind == "integer":
            target_types[pk_src_col] = ("bigint", 64, 0, None); type_categories[pk_src_col] = "bigint"
        elif pk_kind == "uuid":
            target_types[pk_src_col] = ("uuid", None, None, None); type_categories[pk_src_col] = "uuid"
        else:
            target_types[pk_src_col] = ("character varying", None, None, None); type_categories[pk_src_col] = None
    return csv_rows, stored_rows, pk_src_col, cols, target_types, type_categories


def _gen_cell(rng, t, i):
    """Return (csv_string, stored_value) for one cell of type t."""
    if t in ("int", "intcol"):
        v = rng.randint(-10**6, 10**6)
        if rng.random() < 0.15:  # fractional source for an integer target (loader rounds)
            frac = rng.choice([".5", ".4999995", ".5000004", ".49", ".51", ".9999995"])
            s = f"{v}{frac}"
            stored = _numeric_to_int(s)
            return s, stored
        return str(v), v
    if t == "num2":
        v = Decimal(rng.randint(-10**8, 10**8)) / Decimal(100)
        return (f"{v:.2f}", v.quantize(Decimal("0.01")))
    if t == "num_big":
        # scale 25, prec 40 -> inexact path (s_eff=10, tol=1e-10 * rows)
        v = Decimal(rng.randint(-10**12, 10**12)) / Decimal(10**12)
        return (f"{v:.12f}", v)
    if t in ("float", "real"):
        v = rng.uniform(-1e6, 1e6)
        return (repr(v), v)
    if t == "text":
        choices = ["hello", "a,b", 'q"q', "x\ny", "naïve", "日本語", "  pad  ", "NA", "N/A", "null", "", "NULL"]
        s = rng.choice(choices)
        stored = None if (s == "" or s == "NULL") else s
        return s, stored
    if t == "char5":
        s = rng.choice(["ab", "abcde", "a", "", "NULL", "  x"])
        # typed category? char is NOT in _TYPED_CATEGORIES (category None) -> text rules, no trim
        stored = None if (s == "" or s == "NULL") else s  # DSQL char(n)::text is TRIMMED == input here (<=5, no trailing pad in csv)
        return s, stored
    if t == "uuid":
        u = _rand_uuid(rng)
        form = rng.choice(["canon", "upper", "nodash", "", "NULL"])
        if form in ("", "NULL"):
            return form, None
        cell = {"canon": u, "upper": u.upper(), "nodash": u.replace("-", "")}[form]
        return cell, u
    if t == "bool":
        v = rng.choice([True, False])
        return ("true" if v else "false", v)
    if t == "date":
        y = rng.randint(1970, 2970); m = rng.randint(1, 12); d = rng.randint(1, 28)
        s = f"{y:04d}-{m:02d}-{d:02d}"
        return s, s
    if t == "ts6":
        y = rng.randint(1970, 2970); mo = rng.randint(1, 12); d = rng.randint(1, 28)
        hh = rng.randint(0, 23); mm = rng.randint(0, 59); ss = rng.randint(0, 59); us = rng.randint(0, 999999)
        s = f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mm:02d}:{ss:02d}.{us:06d}"
        return s, s
    if t == "bytea":
        n = rng.randint(0, 4)
        hexs = "".join(rng.choice("0123456789abcdef") for _ in range(n * 2))
        cell = "\\x" + hexs
        return cell, cell  # stored as '\xHEX' text form (both sides hash the same text)
    raise ValueError(t)


# --------------------------------------------------------------------------------------------
# Range membership on the TARGET side, modeling _range_predicate_sql semantics exactly:
#   integer : lo <= pk < hi (hi omitted on global top)
#   uuid    : canonical-uuid >= lo AND < hi, compared as 32-hex lowercase (byte order)
#   text    : pk >= lo AND < hi (byte order)
# --------------------------------------------------------------------------------------------
def _pk_key(pk_kind, stored_pk):
    if stored_pk is None:
        return None
    if pk_kind == "integer":
        return int(stored_pk)
    if pk_kind == "uuid":
        return str(stored_pk).lower().replace("-", "")
    return str(stored_pk)


def _in_range(key, lo, hi, is_top, pk_kind):
    if key is None:
        return False
    if pk_kind == "integer":
        lo_i, hi_i = int(lo), int(hi)
        return key >= lo_i and (is_top or key < hi_i)
    # uuid: ranges carry hex strings; text: raw strings. byte/lexical order either way.
    lo_s, hi_s = str(lo), str(hi)
    return key >= lo_s and (is_top or key < hi_s)


def _run_target(stored_rows, ranges, pk_kind, columns_for_metrics, target_types, with_hash,
                key_cols, column_kind, VALIDATE_HASH):
    """Target model per range, mirroring the WHERE predicate + per-metric aggregate."""
    out = {}
    total = len(ranges)
    for ri, (lo, hi) in enumerate(ranges):
        is_top = (ri == total - 1)
        if pk_kind is None or lo is None:
            rows = stored_rows
        else:
            rows = [r for r in stored_rows
                    if _in_range(_pk_key(pk_kind, r.get("_pk")), lo, hi, is_top, pk_kind)]
        mvals = target_summary_model(rows, columns_for_metrics, target_types, with_hash,
                                     key_cols, column_kind, VALIDATE_HASH)
        out[ri] = (len(rows), [v for _, _, v in mvals])
    return out


def _compare(job3, ranges, src, tgt, metrics):
    """Mirror the REAL per-range compare loop (job3_validate.py ~L1949): a missing range on
    either side defaults to (0, empty_vals); a COUNT_DIFF short-circuits that range (no metric
    check); otherwise each metric is compared with the real _same. Returns disagreements as
    (range_index, 'count'|metric_index, s, t)."""
    diffs = []
    empty_vals = [mt["empty"] for mt in metrics]
    for ri in range(len(ranges)):
        sc, sv = src.get(ri, (0, empty_vals))
        tc, tv = tgt.get(ri, (0, empty_vals))
        if int(sc) != int(tc):
            diffs.append((ri, "count", sc, tc))
            continue
        for j, m in enumerate(metrics):
            if not job3["_same"](m, sv[j], tv[j], sc):
                diffs.append((ri, j, sv[j], tv[j]))
    return diffs


# --------------------------------------------------------------------------------------------
# Parameters (keep deterministic & fast)
# --------------------------------------------------------------------------------------------
PER_RANGE = 20


# --------------------------------------------------------------------------------------------
# TEST MATRIX
# --------------------------------------------------------------------------------------------
_PAYLOAD = [("c_int", "int"), ("c_intc", "intcol"), ("c_num2", "num2"), ("c_numbig", "num_big"),
            ("c_float", "float"), ("c_real", "real"), ("c_text", "text"), ("c_char", "char5"),
            ("c_uuid", "uuid"), ("c_bool", "bool"), ("c_date", "date"), ("c_ts", "ts6"),
            ("c_bytea", "bytea")]


def _plan_per(n):
    return max(1, math.ceil(n / PER_RANGE))


def _run_source2(spark, job3, csv_rows, cols, pk_src_col, pk_kind, columns_for_metrics,
                 target_types, type_categories, with_hash, key_cols, want_ranges=None):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    data = [tuple(r.get(c) for c in cols) for r in csv_rows]
    df = spark.createDataFrame(data, schema)
    df = job3["apply_transform"](df, columns_for_metrics, type_categories)
    metrics = job3["build_metrics"](columns_for_metrics, target_types, with_hash=with_hash,
                                    key_cols=key_cols)
    if pk_kind is None:
        n = df.count(); ranges = [(None, None)]
        src = job3["source_range_summaries"](df, pk_src_col, None, ranges, metrics)
        return ranges, src, metrics, n
    mn, mx, n = job3["spark_pk_bounds"](df, pk_src_col, pk_kind)
    if not n:
        return [], {}, metrics, 0
    # choose `per` so plan_ranges yields ~want_ranges ranges (n_ranges = ceil(n/per))
    per = max(1, math.ceil(n / want_ranges)) if want_ranges else _plan_per(n)
    if pk_kind == "uuid":
        ranges = job3["plan_ranges_hex"](str(mn), str(mx), n, per)
    elif pk_kind == "integer":
        ranges = job3["plan_ranges"](mn, mx, n, per)
    else:
        ranges = [(mn, mx)]
    src = job3["source_range_summaries"](df, pk_src_col, pk_kind, ranges, metrics)
    return ranges, src, metrics, n


def run_case(spark, job3, pk_kind, nrows, with_hash=True, hash_scope="all", seed=0, want_ranges=None):
    rng = random.Random(seed)
    job3["VALIDATE_HASH"] = hash_scope
    csv_rows, stored_rows, pk_src_col, cols, ttypes, tcats = _gen_table(rng, pk_kind, nrows, _PAYLOAD)
    cols_for_metrics = cols  # includes pk col
    key_cols = [pk_src_col] if pk_src_col else []
    column_kind = job3["column_kind"]
    ranges, src, metrics, n = _run_source2(spark, job3, csv_rows, cols, pk_src_col, pk_kind,
                                           cols_for_metrics, ttypes, tcats, with_hash, key_cols,
                                           want_ranges=want_ranges)
    tgt = _run_target(stored_rows, ranges, pk_kind, cols_for_metrics, ttypes, with_hash,
                      key_cols, column_kind, hash_scope)
    return dict(csv_rows=csv_rows, stored_rows=stored_rows, pk_src_col=pk_src_col, cols=cols,
                ttypes=ttypes, tcats=tcats, key_cols=key_cols, ranges=ranges, src=src, tgt=tgt,
                metrics=metrics, n=n, column_kind=column_kind, hash_scope=hash_scope,
                with_hash=with_hash, pk_kind=pk_kind)


def main():
    spark = _spark()
    from pyspark.sql import functions as _F
    from pyspark.sql.functions import (
        col, when, lit, lower, trim, upper, concat, substring, coalesce,
        to_timestamp, date_format, regexp_replace, length,
    )
    _spark_names = dict(col=col, when=when, lit=lit, lower=lower, trim=trim, upper=upper,
                        concat=concat, substring=substring, coalesce=coalesce,
                        to_timestamp=to_timestamp, date_format=date_format,
                        regexp_replace=regexp_replace, length=length, F=_F)
    job3 = _load_job3(
        ["apply_transform", "null_marker_expr", "build_metrics", "column_kind",
         "spark_pk_bounds", "_key_col_expr", "_source_summaries_one_plan",
         "source_range_summaries", "plan_ranges", "plan_ranges_hex", "hex_to_int", "int_to_hex",
         "hex_to_canonical_uuid", "normalize_uuid_hex", "_range_predicate_sql", "_sql_str_literal",
         "_same", "_effective_null_markers", "_null_rules_unknown_warnings",
         "_hash_int_from_md5", "_hash_sql",
         "pre_clean_timestamp", "_strip_offset_expr", "normalize_timestamp"],
        dict(threading=threading, math=math, Decimal=Decimal, spark=spark,
             _HASH_DIGITS=6, _HEX="0123456789abcdef", _TS_FMT="yyyy-MM-dd HH:mm:ss.SSSSSS",
             **_spark_names))
    # inject spark into source funcs namespace
    job3["spark"] = spark

    RANGE_COUNTS = [1, 2, 49, 50, 51, 99, 100, 101, 150, 151]
    PK_KINDS = ["uuid", "integer", "text", None]

    # ---- (1) CORRECTNESS: source == target on every range, every pk kind & range count ----
    print("== correctness (no corruption) ==")
    for pk in PK_KINDS:
        counts = RANGE_COUNTS if pk in ("uuid", "integer") else [1]
        for rc in counts:
            # 3 rows per range so ranges #49/#99/#150 actually exist and are non-empty
            nrows = (rc * 3) if pk in ("uuid", "integer") else 60
            if pk is None:
                nrows = 40
            case = run_case(spark, job3, pk, nrows, seed=1000 + rc + (hash(pk) % 97),
                            want_ranges=(rc if pk in ("uuid", "integer") else None))
            diffs = _compare(job3, case["ranges"], case["src"], case["tgt"], case["metrics"])
            check(not diffs, f"CORRECT {pk} nrows={nrows} ranges={len(case['ranges'])}(want {rc}): "
                             f"{len(diffs)} false diff(s): {diffs[:3]}")

    # edge sizes 0/1/2 rows
    for pk in PK_KINDS:
        for nrows in [0, 1, 2]:
            if pk is None and nrows == 0:
                continue
            try:
                case = run_case(spark, job3, pk, nrows, seed=2000 + nrows)
            except Exception as e:
                check(False, f"edge {pk} nrows={nrows} raised {e}")
                continue
            diffs = _compare(job3, case["ranges"], case["src"], case["tgt"], case["metrics"])
            check(not diffs, f"CORRECT edge {pk} nrows={nrows}: {diffs[:3]}")

    # hash scopes keys/off
    for scope in ("keys", "off"):
        case = run_case(spark, job3, "uuid", 150, hash_scope=scope, seed=3000, want_ranges=50)
        diffs = _compare(job3, case["ranges"], case["src"], case["tgt"], case["metrics"])
        check(not diffs, f"CORRECT uuid hash={scope}: {diffs[:3]}")

    # ---- (2) CORRUPTION: planted errors MUST be caught (no false pass) ----
    print("== corruption (must be detected) ==")
    for pk in ("uuid", "integer", "text"):
        want = 100 if pk != "text" else None
        nrows = 300 if pk != "text" else 60
        base = run_case(spark, job3, pk, nrows, seed=5000, want_ranges=want)
        nr = len(base["ranges"])
        # positions to corrupt: first, last, #49, #99 (if present), a middle one
        positions = sorted({0, nr - 1, min(49, nr - 1), min(99, nr - 1), nr // 2})
        for pos in positions:
            lo, hi = base["ranges"][pos]
            is_top = (pos == nr - 1)
            idxs = [i for i, r in enumerate(base["stored_rows"])
                    if base["pk_kind"] is None or
                    _in_range(_pk_key(base["pk_kind"], r.get("_pk")), lo, hi, is_top, base["pk_kind"])]
            if not idxs:
                continue
            _assert_detect(job3, spark, base, pk, nrows, pos, kind="change", idx=idxs[0])
            _assert_detect(job3, spark, base, pk, nrows, pos, kind="drop", idx=idxs[0])
            _assert_detect(job3, spark, base, pk, nrows, pos, kind="add", idx=idxs[0], lohi=(lo, hi, is_top))
        if nr >= 2:
            _assert_swap(job3, spark, base, pk, nrows)

    spark.stop()
    print(f"\n==== validate-differential: {_passed} passed, {_failed} failed ====")
    if _failed:
        sys.exit(1)


def _recompute_target(job3, base, stored_rows):
    return _run_target(stored_rows, base["ranges"], base["pk_kind"], base["cols"], base["ttypes"],
                       base["with_hash"], base["key_cols"], base["column_kind"], base["hash_scope"])


def _assert_detect(job3, spark, base, pk, nrows, pos, kind, idx, lohi=None):
    """Corrupt the TARGET stored_rows only, recompute target, compare to the (good) source."""
    import copy
    stored = copy.deepcopy(base["stored_rows"])
    label = f"{pk} pos={pos} {kind}"
    if kind == "change":
        # change a payload value that participates in a per-range metric
        r = stored[idx]
        # pick c_int (sum) and c_text (hash/len) to flip
        if r.get("c_int") is not None:
            r["c_int"] = int(r["c_int"]) + 1
        else:
            r["c_int"] = 1
    elif kind == "drop":
        del stored[idx]
    elif kind == "add":
        lo, hi, is_top = lohi
        newr = copy.deepcopy(stored[idx])
        # ensure its pk is inside [lo,hi): reuse the same pk (duplicate) -> count differs
        stored.append(newr)
    tgt2 = _recompute_target(job3, base, stored)
    diffs = _compare(job3, base["ranges"], base["src"], tgt2, base["metrics"])
    # the diff must touch range `pos` (or, for drop/add changing counts, that range)
    touched = any(d[0] == pos for d in diffs)
    check(touched, f"CORRUPTION UNDETECTED ({label}): expected a diff in range {pos}, got {diffs[:3]}")


def _assert_swap(job3, spark, base, pk, nrows):
    """Swap a hashed value between a row in range 0 and a row in the last range. Additive sums on
    a column could cancel globally, but PER-RANGE the hash/len/sum of each affected range changes."""
    import copy
    nr = len(base["ranges"])
    stored = copy.deepcopy(base["stored_rows"])
    def _row_in(pos):
        lo, hi = base["ranges"][pos]; is_top = pos == nr - 1
        for i, r in enumerate(stored):
            if _in_range(_pk_key(base["pk_kind"], r.get("_pk")), lo, hi, is_top, base["pk_kind"]):
                return i
        return None
    i0, i1 = _row_in(0), _row_in(nr - 1)
    if i0 is None or i1 is None or i0 == i1:
        return
    # swap the text payload (hashed) -> each range's hash sum changes unless values equal
    if stored[i0].get("c_text") == stored[i1].get("c_text"):
        stored[i1]["c_text"] = (stored[i1].get("c_text") or "") + "Z"
    stored[i0]["c_text"], stored[i1]["c_text"] = stored[i1]["c_text"], stored[i0]["c_text"]
    tgt2 = _recompute_target(job3, base, stored)
    diffs = _compare(job3, base["ranges"], base["src"], tgt2, base["metrics"])
    check(any(d[0] in (0, nr - 1) for d in diffs),
          f"SWAP UNDETECTED ({pk}): expected diff in range 0 or {nr-1}, got {diffs[:3]}")


if __name__ == "__main__":
    main()

