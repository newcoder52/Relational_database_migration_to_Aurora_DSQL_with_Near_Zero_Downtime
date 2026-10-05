#!/usr/bin/env python3
"""Offline tests for the planning-threshold settings (big_table_row_threshold, file_fanout_threshold,
max_groups, map_max_concurrency, max_files_in_parallel, conn_budget, min/max_writers_per_loader).

These eight knobs used to be fixed literals in the PlanSplit state of stepfunctions/startup.asl.json;
they are now params.csv settings resolved per task. This test proves:

  1. ASL WIRING: PlanSplit now reads each knob from $.resolved.* (no literals left), ResolveTask's
     ResultSelector produces every $.resolved.* field PlanSplit reads, and GroupFanOut's Map
     concurrency honours the setting via MaxConcurrencyPath = $.resolved.mapMaxConcurrency.
  2. RESOLVE: resolve_task returns every knob in its resolved payload, with the camelCase names the
     ASL expects, and an old pipeline.json (none of the knobs set) resolves to the former defaults.
  3. GOLDEN DEFAULT PLAN: plan_split with the defaults produces a plan byte-identical to plan_split
     fed the former fixed literals (6000000/8/30/10/900/100/150/6) — behaviour is unchanged.
  4. TUNING TAKES EFFECT: lowering big_table_row_threshold on a FIRST run makes a mid-size table
     big; and the connection/writer maths respond to conn_budget.
  5. OWNERSHIP STABILITY: changing big_table_row_threshold BETWEEN two runs does NOT move a table
     that already has a recorded CDC owner (the recorded owners win, with a warning).
  6. VALIDATION: bad type / out-of-range / min>max are rejected (params_csv.parse collected errors
     AND resolve_task._validate_settings raised errors); the cross-check warning fires.

Static + fake-S3, no AWS/boto3 network. Run: python3 tests/test_planning_settings.py (exit 0 clean).
REPO_DIR overridable.
"""
import json
import os
import re
import sys
import types

REPO = os.environ.get("REPO_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

# Stub boto3 before importing the lambdas (they import boto3 at module load).
_boto3 = types.ModuleType("boto3")
_boto3.client = lambda *a, **k: None
sys.modules["boto3"] = _boto3

import params_csv as pc          # noqa: E402
import resolve_task as rt        # noqa: E402
import plan_split as ps          # noqa: E402

RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append(bool(cond))
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"   <- {str(extra)[:300]}" if (not cond and extra) else ""))


# The eight planning knobs and their former fixed-literal values (defaults must equal these).
FORMER_LITERALS = {
    "big_table_row_threshold": 6000000, "file_fanout_threshold": 8, "max_files_in_parallel": 30,
    "max_groups": 10, "conn_budget": 900, "min_writers_per_loader": 100,
    "max_writers_per_loader": 150, "map_max_concurrency": 6,
}
# resolved-payload (camelCase) names the ASL reads.
RESOLVED_NAMES = {
    "big_table_row_threshold": "bigTableRowThreshold", "file_fanout_threshold": "fileFanoutThreshold",
    "max_files_in_parallel": "maxFilesInParallel", "max_groups": "maxGroups",
    "conn_budget": "connBudget", "min_writers_per_loader": "minWritersPerLoader",
    "max_writers_per_loader": "maxWritersPerLoader", "map_max_concurrency": "mapMaxConcurrency",
}


# =============================================================================================
# 1. ASL WIRING
# =============================================================================================
def test_asl_wiring():
    asl = json.load(open(os.path.join(REPO, "stepfunctions", "startup.asl.json")))
    states = asl["States"]
    plan = states["PlanSplit"]
    payload = plan["Parameters"]["Payload"]

    # Every knob is read from $.resolved.* (the ".$" form), none left as a literal.
    for knob, cam in RESOLVED_NAMES.items():
        key = knob + ".$"
        check(f"ASL PlanSplit reads {knob} from resolved",
              payload.get(key) == f"$.resolved.{cam}", payload.get(key))
        check(f"ASL PlanSplit has NO literal {knob}", knob not in payload, payload.get(knob))

    # ResolveTask's ResultSelector produces every $.resolved.* field PlanSplit reads.
    rsel = states["ResolveTask"]["ResultSelector"]
    for knob, cam in RESOLVED_NAMES.items():
        check(f"ASL ResolveTask produces resolved.{cam}",
              (cam + ".$") in rsel, sorted(rsel))

    # GroupFanOut Map concurrency is the setting (MaxConcurrencyPath), not a literal, and the two
    # are mutually exclusive per the ASL spec.
    gfo = states["GroupFanOut"]
    check("ASL GroupFanOut uses MaxConcurrencyPath = resolved.mapMaxConcurrency",
          gfo.get("MaxConcurrencyPath") == "$.resolved.mapMaxConcurrency", gfo.get("MaxConcurrencyPath"))
    check("ASL GroupFanOut has NO literal MaxConcurrency (mutually exclusive)",
          "MaxConcurrency" not in gfo, gfo.get("MaxConcurrency"))


# =============================================================================================
# 2. RESOLVE defaults + payload
# =============================================================================================
class _S3:
    def __init__(self, text):
        self.text = text

    def get_object(self, Bucket=None, Key=None):
        import io
        return {"Body": io.BytesIO(self.text.encode())}


def _old_pipeline():
    return {"project": "dms-dsql", "region": "us-east-1",
            "dsql_endpoint": "abcd.dsql.us-east-1.on.aws", "dsql_user": "admin",
            "dsql_database": "postgres",
            "glue_role_arn": "arn:aws:iam::123456789012:role/dms-dsql-glue-exec-role",
            "glue_connection": "", "cdc_engine": "pythonshell", "cdc_spark_fallback": True,
            "control_schema": "cdc_control"}


def test_resolve_defaults_and_payload():
    cfg = rt._load_settings(_S3(json.dumps(_old_pipeline())), "b", "config/pipeline.json", [])
    for knob, val in FORMER_LITERALS.items():
        check(f"resolve: old pipeline.json default {knob}={val}", cfg.get(knob) == val, cfg.get(knob))
        check(f"resolve: {knob} is a real int", isinstance(cfg.get(knob), int))


# =============================================================================================
# fake S3 for plan_split (reused from the fork-plan test shape)
# =============================================================================================
class PlanS3:
    def __init__(self, entries, extra=None):
        self.objects = {"config/_task/mytask/_manifest_index.json":
                        json.dumps({"tables": entries}).encode()}
        for k, v in (extra or {}).items():
            self.objects[k] = v if isinstance(v, bytes) else v.encode()
        self.puts = []

    def get_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            e = Exception("NoSuchKey"); e.response = {"Error": {"Code": "NoSuchKey"}}
            raise e
        return {"Body": types.SimpleNamespace(read=lambda d=self.objects[Key]: d)}

    def put_object(self, Bucket=None, Key=None, Body=None, **k):
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode()
        self.puts.append(Key)

    def list_objects_v2(self, Bucket=None, Prefix=None, ContinuationToken=None):
        return {"Contents": [{"Key": Prefix.rstrip("/") + "/LOAD00000001.csv"}],
                "IsTruncated": False}


def _entry(table, pk_mode="single", rows=1000):
    cols = {"single": ["id"], "composite": ["a", "b"], "none": []}[pk_mode]
    return {"dsql_schema": "app", "dsql_table": table, "pk_mode": pk_mode,
            "pk_columns": cols, "full_load_rows": rows, "dms_s3_path": f"s3://b/app/{table}/"}


def _event(knobs=None):
    e = {"bucket": "b", "config_prefix": "s3://b/config/_task/mytask/",
         "project": "proj", "taskSuffix": "mytask", "cdc_root": "cdc",
         "max_composite_forks": 8, "max_big_cdc_forks": 8}
    e.update(knobs or {})
    return e


def _run(entries, knobs=None, prior_owners=None):
    extra = {}
    if prior_owners is not None:
        extra["config/_task/mytask/_jobs.json"] = json.dumps({"cdcOwners": prior_owners}).encode()
    s3 = PlanS3(entries, extra)
    _boto3.client = lambda svc, **k: s3
    out = ps.handler(_event(knobs), None)
    return out, s3


def _plan_signature(out):
    """A comparable signature of a plan: groups (minus their bucket-specific config_prefix) + forks
    + owners + concurrency maths. Enough to prove two plans are the same."""
    def _g(g):
        g = dict(g)
        g.pop("config_prefix", None)
        return g

    def _f(f):
        f = dict(f)
        f.pop("config_prefix", None)
        return f
    return {
        "groups": [_g(g) for g in out["groups"]],
        "forks": [_f(f) for f in out["forks"]],
        "cdcOwners": out["cdcOwners"],
        "loaders_in_flight": out["loaders_in_flight"],
        "writers_per_loader": out["writers_per_loader"],
        "group_count": out["group_count"],
        "fork_count": out["fork_count"],
    }


# =============================================================================================
# 3. GOLDEN DEFAULT PLAN — defaults == former literals
# =============================================================================================
def test_golden_default_plan():
    entries = [
        _entry("big1", "single", rows=7_000_000),
        _entry("s1", "single"), _entry("s2", "single"), _entry("s3t", "single"),
        _entry("nolog", "none"), _entry("ck1", "composite"),
    ]
    # Plan with NO knobs in the event -> plan_split falls back to its own built-in defaults
    # (which equal the former literals).
    out_default, _ = _run(entries, knobs=None)
    # Plan with the knobs passed explicitly as the former literal values.
    out_literal, _ = _run(entries, knobs=dict(FORMER_LITERALS))
    check("golden: default plan == former-literal plan (byte-identical signature)",
          _plan_signature(out_default) == _plan_signature(out_literal),
          json.dumps({"default": _plan_signature(out_default),
                      "literal": _plan_signature(out_literal)})[:400])


# =============================================================================================
# 4. TUNING TAKES EFFECT
# =============================================================================================
def test_tuning_takes_effect():
    # A 3,000,000-row table is small at the default threshold (6,000,000) but big at 2,000,000.
    entries = [_entry("mid", "single", rows=3_000_000), _entry("s1", "single")]
    out_default, _ = _run(entries, knobs=None)
    big_default = [g for g in out_default["groups"] if g.get("kind") == "big"]
    check("tuning: mid table is NOT big at default threshold", not big_default, big_default)

    out_low, _ = _run(entries, knobs={"big_table_row_threshold": 2_000_000})
    big_low = [g for g in out_low["groups"] if g.get("kind") == "big"]
    check("tuning: mid table becomes big when threshold lowered to 2,000,000",
          any(g["tables"] == ["app.mid"] for g in big_low), big_low)
    check("tuning: lowered threshold gives mid its own bg CDC fork",
          out_low["cdcOwners"].get("app.mid", "main").startswith("bg-"), out_low["cdcOwners"])

    # conn_budget feeds writers_per_loader: a tiny budget squeezes writers toward min_writers.
    out_tight, _ = _run([_entry("s1"), _entry("s2")],
                        knobs={"conn_budget": 100, "min_writers_per_loader": 100,
                               "max_writers_per_loader": 150, "map_max_concurrency": 6})
    check("tuning: tight conn_budget pins writers_per_loader at the floor (100)",
          out_tight["writers_per_loader"] == 100, out_tight["writers_per_loader"])


# =============================================================================================
# 5. OWNERSHIP STABILITY when big_table_row_threshold changes between runs
# =============================================================================================
def test_ownership_stable_on_threshold_change():
    # Run 1: big1 is big at the default threshold -> gets a bg CDC owner.
    entries1 = [_entry("big1", "single", rows=7_000_000), _entry("s1", "single")]
    out1, _ = _run(entries1, knobs=None)
    owner1 = out1["cdcOwners"].get("app.big1", "main")
    check("stability: run1 big1 owned by a bg fork", owner1.startswith("bg-"), out1["cdcOwners"])

    # Run 2: operator RAISES big_table_row_threshold to 10,000,000 so big1 (7M) is now "small".
    # The recorded owners (_jobs.json) must win -> big1 KEEPS its bg owner, with a warning.
    out2, _ = _run(entries1, knobs={"big_table_row_threshold": 10_000_000},
                   prior_owners=out1["cdcOwners"])
    owner2 = out2["cdcOwners"].get("app.big1", "main")
    check("stability: run2 (threshold raised) KEEPS big1's bg owner (no re-assign)",
          owner2 == owner1, {"owner1": owner1, "owner2": owner2})
    check("stability: a warning explains the kept assignment",
          any("KEEPING the existing owners" in w for w in out2["warnings"]), out2["warnings"])


# =============================================================================================
# 6. VALIDATION — params_csv (collected) + resolve_task (raised) + cross-check warning
# =============================================================================================
MIN_CSV = ("parameter,value\naccount_id,123456789012\nregion,us-east-1\nproject,dms-dsql\n"
           "dsql_endpoint,abcd.dsql.us-east-1.on.aws\n")


def test_validation():
    bad_type = pc.parse(MIN_CSV + "max_groups,abc\n")
    check("validate: non-int max_groups collected", any("max_groups must be a whole number" in e
          for e in bad_type["errors"]), bad_type["errors"])

    oor = pc.parse(MIN_CSV + "map_max_concurrency,99\n")
    check("validate: map_max_concurrency out of range (1-40)",
          any("between 1 and 40" in e for e in oor["errors"]), oor["errors"])

    zero = pc.parse(MIN_CSV + "big_table_row_threshold,0\n")
    check("validate: big_table_row_threshold=0 rejected",
          any("big_table_row_threshold must be >= 1" in e for e in zero["errors"]), zero["errors"])

    minmax = pc.parse(MIN_CSV + "min_writers_per_loader,200\nmax_writers_per_loader,100\n")
    check("validate: min_writers > max_writers rejected",
          any("must be <= max_writers_per_loader" in e for e in minmax["errors"]), minmax["errors"])

    # resolve_task enforces the SAME rules reading pipeline.json.
    try:
        rt._load_settings(_S3(json.dumps(dict(_old_pipeline(), max_groups="abc"))),
                          "b", "k", [])
        check("validate: resolve_task rejects non-int max_groups", False)
    except rt.SettingsError as e:
        check("validate: resolve_task rejects non-int max_groups", "max_groups" in str(e), str(e))

    try:
        rt._load_settings(_S3(json.dumps(dict(_old_pipeline(), min_writers_per_loader=200,
                                              max_writers_per_loader=100))), "b", "k", [])
        check("validate: resolve_task rejects min>max", False)
    except rt.SettingsError as e:
        check("validate: resolve_task rejects min>max",
              "min_writers_per_loader" in str(e), str(e))

    # Cross-check WARNING (not an error): max_writers * map_max_concurrency > conn_budget.
    w = []
    rt._load_settings(_S3(json.dumps(dict(_old_pipeline(), conn_budget=100,
                                          max_writers_per_loader=150, map_max_concurrency=6))),
                      "b", "k", w)
    check("validate: writers x concurrency > conn_budget warns",
          any("exceeds conn_budget" in x for x in w), w)

    # Big conn_budget warning.
    w2 = []
    rt._load_settings(_S3(json.dumps(dict(_old_pipeline(), conn_budget=8000))), "b", "k", w2)
    check("validate: oversized conn_budget warns about the DSQL connection limit",
          any("large share of DSQL" in x for x in w2), w2)


def main():
    test_asl_wiring()
    test_resolve_defaults_and_payload()
    test_golden_default_plan()
    test_tuning_takes_effect()
    test_ownership_stable_on_threshold_change()
    test_validation()
    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed")
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
