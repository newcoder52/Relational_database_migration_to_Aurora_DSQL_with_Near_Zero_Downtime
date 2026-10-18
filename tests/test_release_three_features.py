"""release-1 integration test: the THREE merged optional features working TOGETHER, offline.

Features:
  A1  post_load_check = count_only   (scripts/job2_load.py; opt-in via a Glue job
                                       default_argument, per deployment -- NOT emitted by
                                       create_glue_jobs / the ASL)
  A2  stopAfterLoad   = true         (resolve_task/preflight/startup.asl.json control flag:
                                       after LOAD, SKIP validate; no validate job is started)
  B   null_notnull_text = keep_text  (params.csv -> resolve_task -> create_glue_jobs ->
                                       --null_notnull_text on load/load-big/validate/cdc)

What this proves, by execution, at the level the offline harness allows:
  (i)  resolve_task resolves all three together (settings + child input).
  (ii) create_glue_jobs builds the load / load-big / validate / cdc DefaultArguments with the
       correct SENTINEL values when all three are on, with post_load_check=count_only delivered
       as a template default_argument (an operator opt-in).
  (iii) the startup ASL, walked with stopAfterLoad=true, skips GroupValidate (load-only), while
        --null_notnull_text=keep_text still rides on the load job args.
  (iv) NO Glue DefaultArguments value is ever an empty string (Glue cannot pass "" ; the pipeline
       uses sentinels __NULL_UNSET__ / __EMPTY__). Checked for every role, flags on AND off.

No AWS / Glue / DSQL / Spark / Step Functions are called; a fake boto3 captures create_job.
"""
import json
import os
import sys
import types
import importlib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)
LAMBDAS = os.path.join(REPO, "lambdas")
SF_DIR = os.path.join(REPO, "stepfunctions")
GLUE_TMPL = os.path.join(REPO, "glue-templates")
sys.path.insert(0, LAMBDAS)
sys.path.insert(0, os.path.join(REPO, "tests"))

import pytest  # noqa: E402


# --------------------------------------------------------------------------- fake boto3
class _FakeExceptions:
    class EntityNotFoundException(Exception):
        pass


class _FakeGlue:
    def __init__(self, captured):
        self.captured = captured
        self.exceptions = _FakeExceptions()

    def get_job(self, JobName):
        raise self.exceptions.EntityNotFoundException(JobName)

    def create_job(self, **kw):
        self.captured[kw["Name"]] = kw
        return {"Name": kw["Name"]}

    def update_job(self, **kw):
        # JobUpdate carries the same shape under "JobUpdate"
        upd = kw.get("JobUpdate", {})
        self.captured[kw["JobName"]] = {"Name": kw["JobName"], **upd}
        return {"JobName": kw["JobName"]}

    def get_tags(self, **kw):
        return {"Tags": {}}

    def tag_resource(self, **kw):
        return {}

    def get_job_runs(self, **kw):
        return {"JobRuns": []}

    def start_job_run(self, **kw):
        return {"JobRunId": "r"}


class _FakeS3:
    """Serves glue-templates + scripts HEAD from the local repo; registry in memory."""
    def __init__(self):
        self.store = {}

    def get_object(self, Bucket, Key):
        # templates: glue_templates_prefix/<role>.json
        fname = Key.rsplit("/", 1)[-1]
        tmpl_path = os.path.join(GLUE_TMPL, fname)
        if Key in self.store:
            body = self.store[Key]
        elif os.path.exists(tmpl_path) and fname.endswith(".json"):
            body = open(tmpl_path, "rb").read()
        else:
            raise _no_such_key(Key)

        class _B:
            def read(self_inner):
                return body
        return {"Body": _B(), "ETag": '"etag"'}

    def put_object(self, Bucket, Key, Body, **kw):
        self.store[Key] = Body if isinstance(Body, bytes) else str(Body).encode()
        return {"ETag": '"etag"'}

    def head_object(self, Bucket, Key):
        return {"ContentLength": 1}

    def list_objects_v2(self, **kw):
        return {"Contents": []}


class _NoSuchKey(Exception):
    def __init__(self, key):
        super().__init__(f"NoSuchKey: {key}")
        self.response = {"Error": {"Code": "NoSuchKey"}}


def _no_such_key(key):
    return _NoSuchKey(key)


def _install_fake_boto3(captured):
    glue = _FakeGlue(captured)
    s3 = _FakeS3()
    sts = types.SimpleNamespace(
        get_caller_identity=lambda: {"Account": "111111111111"})

    b = types.ModuleType("boto3")

    def _client(svc, **k):
        if svc == "glue":
            return glue
        if svc == "s3":
            return s3
        if svc == "sts":
            return sts
        return types.SimpleNamespace()
    b.client = _client
    sys.modules["boto3"] = b
    return b, glue, s3


def _load_cgj():
    _install_fake_boto3({})
    cgj = importlib.import_module("create_glue_jobs")
    importlib.reload(cgj)
    return cgj


# fake wheels so Spark roles (load/load-big/validate) satisfy _spark_boto3_modules offline
_FAKE_WHEELS = ",".join([
    "s3://b/driver-cdc/boto3-1.34-py3-none-any.whl",
    "s3://b/driver-cdc/botocore-1.34-py3-none-any.whl",
    "s3://b/driver-cdc/s3transfer-0.10-py3-none-any.whl",
])


def _run_create(cgj, flags_on=True, post_load_check=None):
    """Drive create_glue_jobs mode=create (all 5 roles) once with a fake boto3 and return the
    captured {job_name: create_job_kwargs}. flags_on toggles the three new settings. When
    post_load_check is given it is injected into the load/load-big TEMPLATE default_arguments in
    the fake S3 (the operator opt-in path)."""
    captured = {}
    b, glue, s3 = _install_fake_boto3(captured)
    importlib.reload(cgj)
    # Re-grab the reloaded module's boto3 (reload re-binds module globals to the stub we set).
    ev = {
        "mode": "create",
        "bucket": "mybucket",
        "project": "demoproj",
        "taskSuffix": "t1",
        "region": "us-east-1",
        "account_id": "111111111111",
        "glue_templates_prefix": "glue-templates",
        "scripts_prefix": "scripts",
        "configPrefix": "config/_task/t1/",
        "config_prefix": "config/_task/t1/",
        "glue_role_arn": "arn:aws:iam::111111111111:role/DemoGlueRole",
        "dsql_endpoint": "demo.dsql.us-east-1.on.aws",
        "dsql_user": "admin",
        "dsql_database": "postgres",
        "cdc_root": "cdc",
        "dms_task_arn": "arn:aws:dms:us-east-1:111111111111:task:ABCDEFT1",
        "cdcExtraPyFiles": _FAKE_WHEELS,
    }
    if flags_on:
        ev.update({
            "null_notnull_text": "keep_text",
            "null_values": "",     # blank -> must travel as __NULL_UNSET__, never ""
            "null_rules": "",      # blank -> __NULL_UNSET__
            "csv_null_value": "",  # blank -> __EMPTY__
            "stopAfterLoad": True,
        })
    # Seed templates into the fake S3 (load/load-big may carry the operator's post_load_check).
    for role in ("discovery", "load", "load-big", "validate", "cdc"):
        tmpl = json.load(open(os.path.join(GLUE_TMPL, f"{role}.json")))
        if post_load_check and role in ("load", "load-big"):
            tmpl.setdefault("default_arguments", {})["--post_load_check"] = post_load_check
        s3.put_object(Bucket="mybucket", Key=f"glue-templates/{role}.json",
                      Body=json.dumps(tmpl).encode())
    cgj.handler(ev, None)
    return captured



# --------------------------------------------------------------------------- (iv) helper
def _assert_no_empty_values(args, where):
    empties = {k: v for k, v in args.items() if v == ""}
    assert not empties, f"{where}: Glue DefaultArguments has empty-string value(s): {empties}"


# =============================================================================== tests
def test_resolve_all_three_together():
    """resolve_task resolves null_notnull_text=keep_text and stopAfterLoad=true together."""
    b = types.ModuleType("boto3"); b.client = lambda *a, **k: None
    sys.modules["boto3"] = b
    rt = importlib.import_module("resolve_task"); importlib.reload(rt)
    # stopAfterLoad true
    assert rt._normalize_stop_after_load({"stopAfterLoad": True}) is True
    assert rt._normalize_stop_after_load({"stopAfterLoad": "true"}) is True
    # null_notnull_text keep_text survives settings validation
    cfg = {
        "project": "p", "region": "us-east-1",
        "glue_role_arn": "arn:aws:iam::111111111111:role/R",
        "dsql_endpoint": "x.dsql.us-east-1.on.aws",
        "null_notnull_text": "keep_text",
    }
    merged = dict(rt.SETTINGS_DEFAULTS); merged.update(cfg)
    warnings = []
    rt._validate_settings(merged, warnings)
    assert merged["null_notnull_text"] == "keep_text", merged.get("null_notnull_text")


def _role_of(job_name):
    # job name = _job_name(project, suffix, role) -> ends with the role token
    for role in ("load-big", "load", "validate", "cdc", "discovery"):
        if job_name.endswith(role):
            return role
    return None


def test_create_glue_jobs_all_three_sentinels():
    """load / load-big / validate / cdc DefaultArguments when all three features are on:
    --null_notnull_text=keep_text everywhere; blank null_values/null_rules as __NULL_UNSET__;
    blank csv_null_value as __EMPTY__; post_load_check=count_only delivered as a template
    default_argument (operator opt-in) reaches the load jobs as --post_load_check count_only.
    NEVER an empty-string value."""
    cgj = _load_cgj()
    captured = _run_create(cgj, flags_on=True, post_load_check="count_only")
    assert captured, "no jobs captured"
    seen = {}
    for name, kw in captured.items():
        role = _role_of(name)
        seen[role] = role
        args = kw["DefaultArguments"]
        _assert_no_empty_values(args, f"{role}:{name}")
        if role in ("load", "load-big", "validate", "cdc"):
            assert args.get("--null_notnull_text") == "keep_text", (role, args.get("--null_notnull_text"))
            assert args.get("--null_values") == "__NULL_UNSET__", (role, args.get("--null_values"))
            assert args.get("--null_rules") == "__NULL_UNSET__", (role, args.get("--null_rules"))
            assert args.get("--csv_null_value") == "__EMPTY__", (role, args.get("--csv_null_value"))
        if role in ("load", "load-big"):
            assert args.get("--post_load_check") == "count_only", (role, args.get("--post_load_check"))
    assert {"load", "load-big", "validate", "cdc"} <= set(seen), seen


def test_no_empty_default_argument_values_flags_on_and_off():
    """For EVERY role, DefaultArguments never contains an empty string -- with the new flags
    ON (keep_text / blanks) and with them absent (today's behaviour)."""
    cgj = _load_cgj()
    for flags_on in (True, False):
        captured = _run_create(cgj, flags_on=flags_on,
                               post_load_check="count_only" if flags_on else None)
        assert captured, f"no jobs captured (flags_on={flags_on})"
        for name, kw in captured.items():
            _assert_no_empty_values(kw["DefaultArguments"],
                                    f"{_role_of(name)}:{name}:flags_on={flags_on}")


def test_startup_asl_walk_stopafterload_skips_validate_with_keep_text_on_load():
    """Walk the merged startup ASL with stopAfterLoad=true: the group load-only path reaches
    GroupOk and NOT GroupValidate (no validate job started), independently of null_notnull_text
    which rides on the load job args (checked above)."""
    sys.path.insert(0, os.path.join(REPO, "tests"))
    import test_override as TOV
    sm = json.load(open(os.path.join(SF_DIR, "startup.asl.json")))
    # locate the per-group iterator that contains AfterGroupLoad
    def find_states_with(key):
        found = []
        def rec(node):
            if isinstance(node, dict):
                if key in node.get("States", {}):
                    found.append(node["States"])
                for v in node.values():
                    rec(v)
            elif isinstance(node, list):
                for v in node:
                    rec(v)
        rec(sm)
        return found
    iters = find_states_with("AfterGroupLoad")
    assert iters, "startup ASL has no AfterGroupLoad state (stopAfterLoad wiring missing)"
    states = iters[0]
    _, visited = TOV.walk({"States": states}, "AfterGroupLoad", {"stopAfterLoad": True}, max_steps=500)
    assert "GroupValidate" not in visited, f"load-only still reached GroupValidate: {sorted(visited)}"
    # and the normal path (flag false) DOES validate
    _, visited_n = TOV.walk({"States": states}, "AfterGroupLoad", {"stopAfterLoad": False}, max_steps=500)
    assert "GroupValidate" in visited_n, f"normal path did not reach GroupValidate: {sorted(visited_n)}"


if __name__ == "__main__":
    import pytest as _p
    raise SystemExit(_p.main([__file__, "-v"]))
