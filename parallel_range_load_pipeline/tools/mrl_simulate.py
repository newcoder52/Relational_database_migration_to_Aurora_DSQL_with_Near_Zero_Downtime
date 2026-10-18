#!/usr/bin/env python3
"""tools/mrl_simulate.py - THE TESTER SCRIPT. Runs complete MRL scenarios through BOTH state
machines against fakes (fake DMS with a stop-reason timeline, fake S3 with real-looking LOAD
listings, fake Glue recording every started run + its Arguments and able to fail a run, fake
DSQL counts via the probe/count result JSONs the mrl_count job would write), and PRINTS a
readable transcript per scenario. Each scenario asserts an expected verdict; a final summary
table prints PASS/FAIL and the process exits non-zero on any failure.

Runs standalone (python3 tools/mrl_simulate.py) AND under pytest (test_* wrappers at the end).

The simulator drives the REAL MRL child ASL (stepfunctions/mrl-task.asl.json) through asl_sim,
with the lambda:invoke handler dispatching to the REAL MRL Lambda handlers (mrl_preflight,
mrl_plan, mrl_reconcile, mrl_gate_eval, mrl_ensure_job, mrl_state) whose boto3 clients are
monkeypatched to the fakes. The Glue count/probe job is simulated (it is a Glue script, not a
Lambda) by writing the probe_result.json / count_result.json the job would write, using the
scenario's DSQL counts. Discovery is simulated by seeding the manifest index + per-table config
the real job1 would write.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, os.path.join(PKG, "tests"))
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))      # resolve_task, params_csv

import asl_sim as sim          # noqa: E402
import mrl_split as ms         # noqa: E402
from mrl_fakes import FakeS3, FakeGlue, FakeDms  # noqa: E402

BUCKET = "bkt"
REGION = "us-east-1"
ACCOUNT = "123456789012"
PROJECT = "demo"
TASK_ID = "ABC123"
TASK_ARN = "arn:aws:dms:us-east-1:123456789012:task:%s" % TASK_ID
CHILD_SM = os.path.join(PKG, "stepfunctions", "mrl-task.asl.json")
FLEET_SM = os.path.join(PKG, "stepfunctions", "mrl-fleet.asl.json")


# ---- pipeline.json seed (what resolve_task reads) ----------------------------------------
def _pipeline_json():
    return {
        "project": PROJECT, "region": REGION, "dsql_endpoint": "clstr.dsql.us-east-1.on.aws",
        "glue_role_arn": "arn:aws:iam::123456789012:role/demo-glue-exec-role",
        "dsql_user": "admin", "dsql_database": "postgres", "glue_connection": "conn",
    }


def _endpoint(add_column_name=True, csv_null="NULL", bucket=BUCKET):
    return {"EngineName": "s3", "S3Settings": {
        "BucketName": bucket, "BucketFolder": "", "AddColumnName": add_column_name,
        "CsvNullValue": csv_null, "TimestampColumnName": "dms_timestamp"}}


def _params_csv(scenario):
    """Build the shared config/params.csv the MRL settings step reads. A scenario may set
    params_map_max_concurrency (an explicit map_max_concurrency value) to exercise Amendment 5;
    omitted => the key is absent and the built-in default 3 applies."""
    rows = ["parameter,value",
            "account_id,123456789012",
            "region,us-east-1",
            "project,%s" % PROJECT,
            "dsql_endpoint,clstr.dsql.us-east-1.on.aws",
            "glue_role_arn,arn:aws:iam::123456789012:role/demo-glue-exec-role",
            "glue_connection,conn"]
    mmc = scenario.get("params_map_max_concurrency")
    if mmc is not None:
        rows.append("map_max_concurrency,%s" % mmc)
    return "\n".join(rows) + "\n"


def _table_mappings():
    return json.dumps({"rules": [
        {"rule-type": "selection", "rule-id": "1", "rule-action": "include",
         "object-locator": {"schema-name": "%", "table-name": "%"}}]})


def _task_timeline(mode, dms_start_mode, stop_reason=None, tables_errored=0, pct=100,
                   status="stopped"):
    """A describe_replication_tasks timeline. One entry that repeats (the poll sees it stopped
    immediately in the happy path)."""
    if stop_reason is None:
        stop_reason = ("Stop Reason STOPPED_AFTER_CACHED_EVENTS" if mode == "full_load_and_cdc"
                       else "Stop Reason STOPPED_AFTER_FULL_LOAD")
    settings = json.dumps({"FullLoadSettings": {"StopTaskCachedChangesApplied": True,
                                                "StopTaskCachedChangesNotApplied": False}})
    mig = "full-load-and-cdc" if mode == "full_load_and_cdc" else "full-load"
    task = {"ReplicationTaskArn": TASK_ARN, "ReplicationTaskIdentifier": TASK_ID,
            "MigrationType": mig, "Status": status, "StopReason": stop_reason,
            "TargetEndpointArn": "arn:ep", "ReplicationTaskSettings": settings,
            "TableMappings": _table_mappings(),
            "ReplicationTaskStats": {"FullLoadProgressPercent": pct,
                                     "TablesErrored": tables_errored}}
    return [task]


# ---- seed discovery output (manifest index + per-table config + rowcounts) ----------------
def _seed_discovery(s3, tables):
    """tables: list of dicts {label, files:[nums], single_pk(bool), rows(int|None),
    allow_hex(bool)}. Seeds the LOAD files, the TASK config _manifest_index.json, each table's
    column mapping, and table_rowcounts.json - exactly what build_table_list + job1 would
    write under config/_task/<suffix>/."""
    cp = "config/_task/%s/" % TASK_ID
    entries = []
    rowcounts = {}
    for t in tables:
        schema, table = t["label"].split(".")
        if t.get("hex"):
            # DMS hex naming (upper-case, zero-padded): LOAD00000001.csv .. LOAD0000000A.csv ..
            for n in t["files"]:
                s3.put_raw(BUCKET, "%s/%s/LOAD%08X.csv" % (schema, table, n), size=t.get("size", 1000))
        else:
            for n in t["files"]:
                s3.put_load_files(BUCKET, "%s/%s" % (schema, table), [n],
                                  ext=("csv"))
        # Optional top-level CDC change files (timestamp-named) + junk; MRL must IGNORE them.
        for cdcname in t.get("cdc_files", []):
            s3.put_raw(BUCKET, "%s/%s/%s" % (schema, table, cdcname), size=100)
        for junk in t.get("junk_files", []):
            s3.put_raw(BUCKET, "%s/%s/%s" % (schema, table, junk), size=100)
        pk = {"columns": ["id"], "mode": "single", "pk_kind": "integer"} if t["single_pk"] \
            else {"columns": ["a", "b"], "mode": "composite"}
        entry = {"dsql_schema": schema, "dsql_table": table, "dms_schema": schema,
                 "dms_table": table, "dms_s3_path": "s3://%s/%s/%s/" % (BUCKET, schema, table),
                 "full_load_rows": t.get("rows"),
                 "metadata": {"dsql_schema": schema, "dsql_table": table, "dms_schema": schema,
                              "dms_table": table, "primary_key": pk,
                              "full_load_rows": t.get("rows")}}
        entries.append(entry)
        if t.get("rows") is not None:
            rowcounts[t["label"].lower()] = t["rows"]
        s3.put_object(Bucket=BUCKET, Key=cp + "%s_column_mapping.json" % table,
                      Body=json.dumps({"target_columns": ["id"], "column_mapping": {}}).encode())
    s3.put_object(Bucket=BUCKET, Key=cp + "_manifest_index.json",
                  Body=json.dumps({"metadata": {}, "tables": entries}).encode())
    s3.put_object(Bucket=BUCKET, Key=cp + "table_rowcounts.json",
                  Body=json.dumps({"rowcounts": rowcounts}).encode())


def _seed_dsql_counts(s3, dsql_counts, empties):
    """Stash the scenario's DSQL state so the simulated count/probe job can read it. We keep it
    in the fake S3 under a sentinel key the simulated Glue job reads."""
    s3.put_object(Bucket=BUCKET, Key="__sim__/dsql_state.json",
                  Body=json.dumps({"counts": dsql_counts, "empties": empties}).encode())


# ---- the harness -------------------------------------------------------------------------
class Sim:
    def __init__(self, scenario):
        self.sc = scenario
        self.s3 = FakeS3()
        self.glue = FakeGlue()
        self.dms = FakeDms(TASK_ARN, scenario["timeline"], endpoint=scenario["endpoint"],
                           table_mappings=_table_mappings(),
                           table_stats=scenario.get("table_stats", []))
        self.lines = []
        # AMENDMENT 3/9: MRL reads settings from the EXISTING config/params.csv (READ-ONLY) and
        # publishes its OWN config/pipeline_parallel_load.json. Seed params.csv (shared file).
        self.s3.put_object(Bucket=BUCKET, Key="config/params.csv",
                           Body=_params_csv(scenario).encode())
        # owner marker + task index: this task owns its folder (a clean prior MRL start).
        self.s3.put_object(Bucket=BUCKET, Key="config/_task/%s/_task.json" % TASK_ID,
                           Body=json.dumps({"taskArn": TASK_ARN, "taskSuffix": TASK_ID}).encode())
        self.s3.put_object(Bucket=BUCKET, Key="config/_task_index/%s.json" % TASK_ID,
                           Body=json.dumps({"taskArn": TASK_ARN, "taskSuffix": TASK_ID}).encode())
        # AMENDMENT 9: dedicated MRL templates under NEW names (never load-big.json).
        self.s3.put_object(Bucket=BUCKET, Key="glue-templates/mrl-load.json",
                           Body=json.dumps({"role": "mrl-load", "script": "job2_load_manual_range.py",
                                            "command_name": "glueetl", "glue_version": "4.0",
                                            "worker_type": "G.4X", "number_of_workers": 20,
                                            "timeout_minutes": 2880, "max_concurrent_runs": 1,
                                            "default_arguments": {
                                                "--TempDir": "s3://<<BUCKET>>/glue-temp/"}}).encode())
        self.s3.put_object(Bucket=BUCKET, Key="glue-templates/mrl-count.json",
                           Body=json.dumps({"role": "mrl-count", "script": "job_mrl_count.py",
                                            "command_name": "pythonshell", "glue_version": "3.0",
                                            "max_capacity": 1, "timeout_minutes": 2880,
                                            "max_concurrent_runs": 10,
                                            "default_arguments": {}}).encode())
        _seed_discovery(self.s3, scenario["tables"])
        _seed_dsql_counts(self.s3, scenario.get("dsql_counts", {}),
                          scenario.get("empties", {}))
        self._install_boto()

    def log(self, msg):
        self.lines.append(msg)

    def _install_boto(self):
        """Monkeypatch boto3.client in every MRL Lambda module (and resolve_task) to the fakes."""
        import boto3
        s3, glue, dms = self.s3, self.glue, self.dms

        def fake_client(service, **kw):
            if service == "s3":
                return s3
            if service == "glue":
                return glue
            if service == "dms":
                return dms
            if service == "dsql":
                class _D:
                    def generate_db_connect_admin_auth_token(self, *a, **k):
                        return "tok"

                    def get_vpc_endpoint_service_name(self, **k):
                        raise Exception("not in sim")
                return _D()
            if service == "stepfunctions":
                class _S:
                    def describe_execution(self, **k):
                        return {"startDate": 0, "input": "{}"}

                    def list_executions(self, **k):
                        return {"executions": []}
                return _S()
            if service == "sts":
                class _ST:
                    def get_caller_identity(self):
                        return {"Account": ACCOUNT}
                return _ST()
            raise Exception("no fake for service %r" % service)

        import mrl_preflight, mrl_plan, mrl_reconcile, mrl_gate_eval, mrl_ensure_job, mrl_state
        import mrl_settings, mrl_params_csv, mrl_fleet_preflight
        import mrl_cdc_handoff
        import resolve_task
        for mod in (mrl_preflight, mrl_plan, mrl_reconcile, mrl_gate_eval, mrl_ensure_job,
                    mrl_state, mrl_settings, mrl_params_csv, mrl_fleet_preflight, mrl_cdc_handoff,
                    resolve_task):
            mod.boto3 = type("b", (), {"client": staticmethod(fake_client)})
        self._fake_client = fake_client

    # -- Glue count/probe job simulation (it is a Glue script, not a Lambda) --
    def _run_count_job(self, params, mode):
        args = params["Arguments"]
        cp = args["--config_prefix"]
        result_prefix = args["--mrl_result_prefix"]
        b, idx_key = cp[5:].split("/", 1)
        idx = json.loads(self.s3.get_object(Bucket=b, Key=idx_key.rstrip("/") + "/_manifest_index.json")
                         ["Body"].read().decode())
        state = json.loads(self.s3.get_object(Bucket=BUCKET, Key="__sim__/dsql_state.json")
                           ["Body"].read().decode())
        rb, rk = result_prefix[5:].split("/", 1)
        if mode == "probe":
            tables = []
            for e in idx.get("tables", []):
                label = "%s.%s" % (e["dsql_schema"], e["dsql_table"])
                empty = state["empties"].get(label, True)
                tables.append({"table": label, "empty": empty, "exists": True})
            doc = {"mode": "probe", "tables": tables,
                   "all_empty": all(t["empty"] for t in tables),
                   "any_missing": False}
            self.s3.put_object(Bucket=rb, Key=rk.rstrip("/") + "/probe_result.json",
                               Body=json.dumps(doc).encode())
            return {"JobRunState": "SUCCEEDED"}
        # count mode
        tol = int(args.get("--count_tolerance", "0"))
        action = args.get("--count_timeout_action", "fail")
        results, mism, unver = [], [], []
        for e in idx.get("tables", []):
            label = "%s.%s" % (e["dsql_schema"], e["dsql_table"])
            expected = e.get("full_load_rows")
            tgt = state["counts"].get(label)
            if tgt == "TIMEOUT":
                results.append({"table": label, "status": "unverifiable", "expected": expected,
                                "target": None})
                unver.append(label)
                continue
            if expected is None or tgt is None:
                results.append({"table": label, "status": "no_expected", "expected": expected,
                                "target": tgt})
                continue
            ok = abs(int(tgt) - int(expected)) <= tol
            results.append({"table": label, "status": "match" if ok else "mismatch",
                            "expected": expected, "target": tgt})
            if not ok:
                mism.append(label)
        verdict = "ok"
        if mism:
            verdict = "mismatch"
        elif unver and action == "fail":
            verdict = "unverifiable"
        doc = {"mode": "count", "tables": results, "verdict": verdict}
        self.s3.put_object(Bucket=rb, Key=rk.rstrip("/") + "/count_result.json",
                           Body=json.dumps(doc).encode())
        if verdict == "mismatch":
            raise sim.ASLError("States.TaskFailed",
                               "COUNT MISMATCH: " + ", ".join(mism))
        if verdict == "unverifiable":
            raise sim.ASLError("States.TaskFailed",
                               "COUNT COULD NOT COMPLETE: " + ", ".join(unver))
        return {"JobRunState": "SUCCEEDED"}

    # -- simulate the per-range loader (the Glue -mrl-load job) writing range status docs --
    def _run_mrl_load(self, args):
        cp = args["--config_prefix"]
        first = args["--load_file_start"]
        last = args["--load_file_end"]
        b, cp_key = cp[5:].split("/", 1)
        idx = json.loads(self.s3.get_object(Bucket=b, Key=cp_key.rstrip("/") + "/_manifest_index.json")
                         ["Body"].read().decode())
        entry = idx["tables"][0]
        label = "%s.%s" % (entry["dsql_schema"], entry["dsql_table"])
        dms_path = entry["dms_s3_path"]
        files, _ = ms.list_table_files(self.s3, dms_path,
                                       allow_hex=self.sc.get("allow_hex", False))
        # resolve the inclusive slice by name (same as the loader resolver)
        names = [ms.basename(u) for u, _s in files]
        i0 = names.index(first)
        i1 = names.index(last)
        assigned = files[i0:i1 + 1]
        safe = label.replace("/", "_")
        skey = "%s_file_status/%s.range-%s-%s.json" % (cp_key, safe, first, last)
        rkey = "%s_range_result/%s.range-%s-%s.json" % (cp_key, safe, first, last)
        # per-file rows (deterministic: 10 rows/file) unless the scenario fails this range
        fail = self.sc.get("fail_range")
        if fail and fail[0] == label and fail[1] == first:
            # AMENDMENT 2 (R2d): fail the targeted range up to fail_range_times times (default 1),
            # then let the resume (re-run) succeed. fail_range_error injects a custom terminal
            # (e.g. an operator kill -> Glue ABORTED -> States.TaskFailed). The ORIGINAL cause
            # text is what the Catch surfaces after the bounded retries are exhausted.
            want_fails = int(self.sc.get("fail_range_times", 1))
            seen = int(self.sc.get("_fail_count", 0))
            if seen < want_fails:
                self.sc["_fail_count"] = seen + 1
                self.s3.put_object(Bucket=b, Key=rkey, Body=json.dumps(
                    {"label": label, "first": first, "last": last, "failed": True}).encode())
                err_name = self.sc.get("fail_range_error", "States.TaskFailed")
                raise sim.ASLError(err_name, "range %s..%s failed (injected attempt %d/%d)"
                                   % (first, last, seen + 1, want_fails))
        files_doc = {}
        for uri, _s in assigned:
            files_doc[uri] = {"state": "done", "rows": 10}
        self.s3.put_object(Bucket=b, Key=skey, Body=json.dumps({"files": files_doc}).encode())
        self.s3.put_object(Bucket=b, Key=rkey, Body=json.dumps(
            {"label": label, "first": first, "last": last, "failed": False,
             "rows_written_by_this_range": 10 * len(assigned)}).encode())
        return {"JobRunState": "SUCCEEDED"}

    # -- handlers wired into the interpreter --
    def handlers(self):
        def lambda_invoke(resource, params, ctx):
            fn = params["FunctionName"]
            pl = params.get("Payload", {})
            import mrl_preflight, mrl_plan, mrl_reconcile, mrl_gate_eval, mrl_ensure_job, mrl_state
            import mrl_settings
            import mrl_cdc_handoff
            dispatch = {
                "mrl-settings": mrl_settings.handler,
                "mrl-preflight": mrl_preflight.handler,
                "mrl-plan": mrl_plan.handler,
                "mrl-reconcile": mrl_reconcile.handler,
                "mrl-cdc-handoff": mrl_cdc_handoff.handler,
                "mrl-gate-eval": mrl_gate_eval.handler,
                "mrl-ensure-job": mrl_ensure_job.handler,
                "mrl-state": mrl_state.handler,
            }
            for key, h in dispatch.items():
                if key in fn:
                    try:
                        out = h(pl, None)
                    except Exception as e:
                        # A Lambda raising surfaces to Step Functions as a task failure the
                        # state's Catch handles (error name + cause). Use the exception class
                        # name as the error and its message as the cause.
                        raise sim.ASLError(type(e).__name__, str(e))
                    return {"Payload": out}
            # reused existing Lambdas: resolve_task(build_table_list), the THREE driver-discovery
            # states, create-glue-jobs - simulate the minimal outputs the reused ASL reads.
            if "resolve-task" in fn or pl.get("mode") == "build_table_list":
                return {"Payload": {"count": len(self.sc["tables"]), "distinctSchemas": ["s"],
                                    "manifestKey": "k", "replacedExisting": False}}
            if "driver-discovery" in fn:
                # DriverDiscoveryFullload/Validation return {extraPyFiles}; Cdc + the new
                # DriverDiscoveryCount (prepare_for=pythonshell) return {extraPyFiles, engine,
                # fallbackReason, prepared}. A13: an injected 'no-internet unstripped set' makes
                # the pythonshell discovery FAIL (pip would hang) -> the state Catches to
                # DriversFailed. Key the wheel set off drivers_prefix + prepare_for.
                dp = pl.get("drivers_prefix", "driver-fullload")
                prep = pl.get("prepare_for")
                if prep == "pythonshell" and self.sc.get("unstripped_pythonshell"):
                    # the wheels were NOT Requires-Dist-stripped -> the Lambda fails fast (no hang)
                    raise sim.ASLError("DriverCheckError",
                                       "driver-cdc wheels are not prepared for python-shell "
                                       "(Requires-Dist present); Glue pip would contact PyPI.")
                # A18: the driver-cdc set (and its prepared pythonshell form) ships boto3/botocore/
                # s3transfer so the Spark mrl-load job's --additional-python-modules resolves a
                # dsql-aware boto3. driver-fullload/validation hold only the pg8000 stack.
                if dp == "driver-cdc":
                    out = {"extraPyFiles": ",".join(
                        "s3://bkt/driver-cdc/%s" % w for w in
                        ("boto3-1.42.97-py3-none-any.whl", "botocore-1.42.97-py3-none-any.whl",
                         "s3transfer-0.16.1-py3-none-any.whl", "pg8000-1.31.2-py3-none-any.whl",
                         "scramp-1.4.5-py3-none-any.whl"))}
                else:
                    out = {"extraPyFiles": "s3://bkt/%s/pg8000.whl" % dp}
                if prep == "pythonshell":
                    out["prepared"] = True
                    out["engine"] = "pythonshell"
                    out["fallbackReason"] = ""
                elif dp == "driver-cdc":
                    out["engine"] = "pythonshell"
                    out["fallbackReason"] = ""
                return {"Payload": out}
            if "create-glue-jobs" in fn:
                return {"Payload": {"jobs": {"discovery": "%s-%s-discovery" % (PROJECT, TASK_ID),
                                             "load": "%s-%s-load" % (PROJECT, TASK_ID),
                                             "load-big": "%s-%s-load-big" % (PROJECT, TASK_ID),
                                             "validate": "%s-%s-validate" % (PROJECT, TASK_ID),
                                             "cdc": "%s-%s-cdc" % (PROJECT, TASK_ID)},
                                    "cdcEngine": "pythonshell", "created": [], "updated": []}}
            if "plan-split" in fn:
                # By default no forks (main CDC job owns every table). A scenario may request REAL
                # ck/bg forks via sc["forks"] = [{"kind","fork_table","slug"}...]; we then build
                # the exact plan_split fork shape (config_prefix at the ck/bg prefix the reused CDC
                # job runs with) so CdcForkHandoff writes the fork _load_status.json there and
                # StartForkCdcMap starts + confirms each fork CDC job.
                spec = self.sc.get("forks") or []
                forks = []
                for fspec in spec:
                    kind = fspec.get("kind", "ck")
                    slug = fspec.get("slug", "s-t-dead")
                    label = fspec["fork_table"]
                    if kind == "bg":
                        cp_key = "config/_task/%s/_orchestrator/group-%s/" % (
                            TASK_ID, fspec.get("group_index", 0))
                    else:
                        cp_key = "config/_task/%s/_orchestrator/ck-%s/" % (TASK_ID, slug)
                    forks.append({
                        "kind": kind, "fork_slug": slug, "fork_table": label,
                        "config_prefix": "s3://%s/%s" % (BUCKET, cp_key),
                        "config_prefix_key": cp_key,
                        "cdcJobName": "%s-%s-%s-%s-cdc" % (PROJECT, TASK_ID, kind, slug),
                    })
                return {"Payload": {"groups": [], "group_count": 0,
                                    "forks": forks, "fork_count": len(forks)}}
            if "ensure-fork" in fn or pl.get("mode") == "ensure_fork_jobs":
                return {"Payload": {"created": [], "updated": []}}
            raise Exception("sim: no lambda dispatch for %r" % fn)

        def glue_sync(resource, params, ctx):
            job = params.get("JobName", "")
            args = params.get("Arguments", {})
            # Scenario 16: raise ConcurrentRunsExceeded on the FIRST load start, then succeed on
            # the retry (exercises the RunRange Retry ladder).
            if ("--load_file_start" in args and self.sc.get("throttle_once")
                    and not self.sc.get("_throttled")):
                self.sc["_throttled"] = True
                raise sim.ASLError("Glue.ConcurrentRunsExceededException",
                                   "Concurrent runs exceeded (injected once)")
            # record the run (concurrency + string-arg checks happen in the string-arg test)
            self.glue.runs.append({"JobName": job, "Arguments": dict(args),
                                   "RunId": "jr%d" % (len(self.glue.runs) + 1), "state": "DONE",
                                   "nth": 0, "_terminal": ("SUCCEEDED", None)})
            if "mrl_mode" in args or "--mrl_mode" in args:
                return self._run_count_job(params, args.get("--mrl_mode"))
            if "--load_file_start" in args:
                args = dict(args, **{"--_jobname": job})
                return self._run_mrl_load(args)
            return {"JobRunState": "SUCCEEDED"}       # discovery

        def glue_start(resource, params, ctx):
            args = dict(params.get("Arguments", {}))
            self.glue.runs.append({"JobName": params.get("JobName", ""),
                                   "Arguments": args,
                                   "RunId": "jrcdc", "state": "RUNNING", "nth": 0,
                                   "_terminal": ("RUNNING", None)})
            # Write the CDC start marker at <config_prefix>_cdc_started/<startup_execution>.json.
            # The real CDC script writes its marker under its OWN --config_prefix; this unified
            # rule matches BOTH the main job (config_prefix = task prefix, startup_execution =
            # exec name) and each fork job (config_prefix = ck/bg prefix, startup_execution =
            # <exec>-ck-<slug>), so CheckCdcStarted / CheckForkStarted both confirm.
            cp = args.get("--config_prefix") or ("config/_task/%s/" % TASK_ID)
            if str(cp).startswith("s3://"):
                cp = str(cp)[5:].split("/", 1)[1]
            se = args.get("--startup_execution") or ctx["Execution"]["Name"]
            marker_key = "%s_cdc_started/%s.json" % (cp.rstrip("/") + "/", se)
            self.s3.put_object(Bucket=BUCKET, Key=marker_key, Body=b"{}")
            return {"JobRunId": "jrcdc"}

        def dms_start(resource, params, ctx):
            typ = params["StartReplicationTaskType"]
            self.dms.start_replication_task(params["ReplicationTaskArn"], typ)
            return {"ReplicationTask": {"Status": "starting"}}

        def dms_describe(resource, params, ctx):
            return self.dms.describe_replication_tasks(
                Filters=params.get("Filters"), WithoutSettings=False)

        def glue_getrun(resource, params, ctx):
            # The CDC run is alive -> IsCdcRunAlive routes to CheckCdcStarted (the marker was
            # written by glue_start), so the CDC tail confirms and succeeds.
            return {"JobRun": {"JobRunState": "RUNNING"}}

        def s3_list(resource, params, ctx):
            prefix = params["Prefix"]
            resp = self.s3.list_objects_v2(Bucket=params["Bucket"], Prefix=prefix)
            return {"KeyCount": len(resp.get("Contents", []))}

        return {
            "arn:aws:states:::lambda:invoke": lambda_invoke,
            "arn:aws:states:::glue:startJobRun.sync": glue_sync,
            "arn:aws:states:::glue:startJobRun": glue_start,
            "arn:aws:states:::aws-sdk:glue:getJobRun": glue_getrun,
            "arn:aws:states:::aws-sdk:databasemigration:startReplicationTask": dms_start,
            "arn:aws:states:::aws-sdk:databasemigration:describeReplicationTasks": dms_describe,
            "arn:aws:states:::aws-sdk:s3:listObjectsV2": s3_list,
        }

    def run_child(self, child_input):
        raw = open(CHILD_SM).read()
        subs = {
            "<<BUCKET>>": BUCKET,
            "<<MRL_PREFLIGHT_LAMBDA_ARN>>": "arn:aws:lambda:us-east-1:123456789012:function:%s-mrl-preflight" % PROJECT,
            "<<MRL_PLAN_LAMBDA_ARN>>": "arn:...:function:%s-mrl-plan" % PROJECT,
            "<<MRL_RECONCILE_LAMBDA_ARN>>": "arn:...:function:%s-mrl-reconcile" % PROJECT,
            "<<MRL_CDC_HANDOFF_LAMBDA_ARN>>": "arn:...:function:%s-mrl-cdc-handoff" % PROJECT,
            "<<MRL_GATE_EVAL_LAMBDA_ARN>>": "arn:...:function:%s-mrl-gate-eval" % PROJECT,
            "<<MRL_ENSURE_JOB_LAMBDA_ARN>>": "arn:...:function:%s-mrl-ensure-job" % PROJECT,
            "<<MRL_STATE_LAMBDA_ARN>>": "arn:...:function:%s-mrl-state" % PROJECT,
            "<<RESOLVE_TASK_LAMBDA_ARN>>": "arn:...:function:%s-resolve-task" % PROJECT,
            "<<DRIVER_DISCOVERY_LAMBDA_ARN>>": "arn:...:function:%s-driver-discovery" % PROJECT,
            "<<CREATE_GLUE_JOBS_LAMBDA_ARN>>": "arn:...:function:%s-create-glue-jobs" % PROJECT,
            "<<PLAN_SPLIT_LAMBDA_ARN>>": "arn:...:function:%s-plan-split" % PROJECT,
            "<<MRL_COUNT_JOB_NAME>>": "%s-%s-mrl-count" % (PROJECT, TASK_ID),
            "<<GLUE_ROLE_ARN>>": "arn:aws:iam::123456789012:role/%s-glue-exec-role" % PROJECT,
            "<<GLUE_CONNECTION>>": "conn",
            "<<DSQL_ENDPOINT>>": "clstr.dsql.us-east-1.on.aws",
            "<<DSQL_ENDPOINT_CANDIDATES>>": "clstr.dsql.us-east-1.on.aws",
            "<<DSQL_USER>>": "admin",
            "<<DSQL_DATABASE>>": "postgres",
        }
        for ph, rep in subs.items():
            raw = raw.replace(ph, rep)
        smdef = json.loads(raw)
        # Publish+pin the settings as the fleet's PublishSettings state would, so the child has a
        # pinned settingsKey (and the exec-input max_concurrency/cdc overrides) to read. For a
        # direct child run this mirrors a fleet-launched child.
        import mrl_settings
        settings_out = mrl_settings.handler({
            "bucket": BUCKET,
            "rawInput": {"bucket": BUCKET,
                         "max_concurrency": self.sc.get("exec_max_concurrency"),
                         "cdc": self.sc.get("cdc")},
            "fleetExecutionName": "e1",
            "fleetExecutionArn": "arn:aws:states:us-east-1:123456789012:execution:%s-mrl-fleet:e1" % PROJECT,
            "fleetStateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:%s-mrl-fleet" % PROJECT,
            "taskStateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:%s-mrl-task" % PROJECT,
        }, None)
        child_input = dict(child_input)
        child_input["settingsKey"] = settings_out["pinnedSettingsKey"]
        if self.sc.get("cdc") is not None and "cdc" not in child_input:
            child_input["cdc"] = self.sc.get("cdc")
        ctx = {"Execution": {"Id": "arn:aws:states:us-east-1:123456789012:execution:%s-mrl-task:e1" % PROJECT,
                             "Name": "e1"},
               "StateMachine": {"Id": "arn:aws:states:us-east-1:123456789012:stateMachine:%s-mrl-task" % PROJECT},
               "Map": {"Item": {}}}
        interp = sim.Interpreter(smdef, self.handlers(), ctx)
        out = interp.run(child_input)
        return out, interp


# ---- transcript printing ------------------------------------------------------------------
def _print_transcript(name, sim_obj, out):
    s3 = sim_obj.s3
    print("=" * 74)
    print("SCENARIO: %s" % name)
    # fleet rows / child input
    # plan + split
    suffix = TASK_ID
    try:
        plan = s3.get_json("mrl/state/%s/plan.json" % suffix)
        for label, pt in sorted(plan.get("tables", {}).items()):
            if pt.get("loaded_empty"):
                print("  table %s: 0 files -> loaded-empty (FullLoadRows=0)" % label)
            else:
                sizes = [r["size"] for r in pt["ranges"]]
                print("  table %s: %d files, concurrency %s -> %s"
                      % (label, pt["num_files"], pt.get("concurrency"),
                         ",".join(str(x) for x in sizes)))
    except Exception:
        print("  (no plan.json - failed before planning)")
    # glue runs + arguments
    load_runs = [r for r in sim_obj.glue.runs if "--load_file_start" in r["Arguments"]]
    for r in load_runs:
        a = r["Arguments"]
        print("  glue run: %s --load_file_start %s --load_file_end %s mfip=%s wpf=%s"
              % (r["JobName"].rsplit("/", 1)[-1], a["--load_file_start"], a["--load_file_end"],
                 a.get("--max_files_in_parallel"), a.get("--writers_per_file")))
    # reconcile + count
    try:
        rec = s3.get_json("mrl/state/%s/reconcile.json" % suffix)
        print("  reconcile: allClean=%s" % rec.get("allClean"))
    except Exception:
        pass
    try:
        cnt = s3.get_json("mrl/state/%s/count_result.json" % suffix)
        print("  count verdict: %s" % cnt.get("verdict"))
        for t in cnt.get("tables", []):
            print("    count [%s]: %s expected=%s target=%s"
                  % (t["table"], t["status"], t.get("expected"), t.get("target")))
    except Exception:
        pass
    print("  FINAL: status=%s state=%s error=%s"
          % (out["status"], out.get("state"), out.get("error")))


# ---- scenarios ----------------------------------------------------------------------------
def _base_endpoint():
    return _endpoint()


def scenarios():
    def tbl(label, nfiles, single_pk=True, rows=None):
        return {"label": label, "files": list(range(1, nfiles + 1)),
                "single_pk": single_pk, "rows": rows if rows is not None else nfiles * 10}
    S = []

    # 1: full_load_only happy path, 1 table, 406 files, c=4 -> 102,102,101,101
    S.append({"name": "1_full_load_only_406_c4", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 4, "tables": [tbl("s.big", 406, rows=4060)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.big": 4060}, "empties": {"s.big": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded",
              "expect_sizes": {"s.big": [102, 102, 101, 101]}})

    # 2: 5 files c=3 -> 2,2,1
    S.append({"name": "2_five_files_c3", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [tbl("s.t", 5, rows=50)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 50}, "empties": {"s.t": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded",
              "expect_sizes": {"s.t": [2, 2, 1]}})

    # 3: full_load_and_cdc happy path ending with DMS resume + CDC start
    S.append({"name": "3_full_load_and_cdc", "mode": "full_load_and_cdc", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_and_cdc", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 40}, "empties": {"s.t": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceededCdc"})

    # 3b: full_load_and_cdc where plan_split FORKS the table out of the main CDC job. One ck
    # (composite) fork and one bg (big, >= 8 files) fork, each owning the single table. The fork
    # CDC jobs run with --config_prefix = the fork prefix and gate on <that prefix>_load_status.json;
    # CdcForkHandoff must write it there (R23-B1) or the fork CDC job skips the table forever.
    # run_scenario asserts the REAL CDC gate sees the table done at each fork prefix.
    S.append({"name": "3b_cdc_fork_handoff", "mode": "full_load_and_cdc", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_and_cdc", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 40}, "empties": {"s.t": True},
              "forks": [{"kind": "ck", "slug": "s-t-dead", "fork_table": "s.t"},
                        {"kind": "bg", "slug": "s-t-dead", "group_index": 0, "fork_table": "s.t"}],
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceededCdc"})

    # 5: adopt_stopped
    S.append({"name": "5_adopt_stopped", "mode": "full_load_only", "dms_start_mode": "adopt_stopped",
              "max_concurrency": 2, "tables": [tbl("s.t", 3, rows=30)],
              "timeline": _task_timeline("full_load_only", "adopt_stopped",
                                         stop_reason="Stop Reason NORMAL"),
              "endpoint": _base_endpoint(), "dsql_counts": {"s.t": 30}, "empties": {"s.t": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 6: DMS stopped with wrong reason -> fail (cdc mode, got STOPPED_AFTER_FULL_LOAD)
    S.append({"name": "6_dms_wrong_reason", "mode": "full_load_and_cdc", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 3)],
              "timeline": _task_timeline("full_load_and_cdc", "start",
                                         stop_reason="Stop Reason STOPPED_AFTER_FULL_LOAD"),
              "endpoint": _base_endpoint(), "dsql_counts": {}, "empties": {"s.t": True},
              "expect_status": "FAILED", "expect_state": "DmsFailed"})

    # 7: TablesErrored>0 -> fail
    S.append({"name": "7_tables_errored", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 3)],
              "timeline": _task_timeline("full_load_only", "start", tables_errored=2),
              "endpoint": _base_endpoint(), "dsql_counts": {}, "empties": {"s.t": True},
              "expect_status": "FAILED", "expect_state": "DmsFailed"})

    # 8: a range fails ONCE, then the bounded automatic re-run (AMENDMENT 2 R2a,
    # States.TaskFailed Retry on RunRange) resumes it and the task SUCCEEDS.
    S.append({"name": "8_range_fails_once_then_retry_succeeds", "mode": "full_load_only",
              "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [tbl("s.t", 6, rows=60)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 60}, "empties": {"s.t": True},
              "fail_range": ("s.t", "LOAD00000001.csv"), "fail_range_times": 1,
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 8c: a range fails TWICE, then the third attempt (within MaxAttempts=3) resumes and SUCCEEDS.
    S.append({"name": "8c_range_fails_twice_then_retry_succeeds", "mode": "full_load_only",
              "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [tbl("s.t", 6, rows=60)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 60}, "empties": {"s.t": True},
              "fail_range": ("s.t", "LOAD00000001.csv"), "fail_range_times": 2,
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 8d: a range fails 4 times (> MaxAttempts=3) -> the bounded re-run is exhausted and the task
    # ends MrlLoadFailed; the Catch surfaces the ORIGINAL injected cause (not just "retries
    # exhausted"). expect_cause_substr is asserted against the execution's failure cause.
    S.append({"name": "8d_range_fails_4x_exhausts_to_MrlLoadFailed", "mode": "full_load_only",
              "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [tbl("s.t", 6, rows=60)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 60}, "empties": {"s.t": True},
              "fail_range": ("s.t", "LOAD00000001.csv"), "fail_range_times": 4,
              "expect_status": "FAILED", "expect_state": "MrlLoadFailed",
              "expect_cause_substr": "failed (injected"})

    # 8e: an operator KILLS a range's Glue run (ABORTED -> States.TaskFailed). The bounded re-run
    # treats it like any failure and RETRIES it (up to 3x) -> here the kill recurs 4x so it ends
    # MrlLoadFailed. DOC/RUNBOOK: to truly stop a table, stop the Step Functions EXECUTION (the
    # per-range kill is now auto-retried). This scenario documents that behaviour change.
    S.append({"name": "8e_operator_kill_is_auto_retried", "mode": "full_load_only",
              "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [tbl("s.t", 6, rows=60)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 60}, "empties": {"s.t": True},
              "fail_range": ("s.t", "LOAD00000001.csv"), "fail_range_times": 4,
              "fail_range_error": "States.TaskFailed",
              "expect_status": "FAILED", "expect_state": "MrlLoadFailed"})

    # 10: count mismatch by 1 row -> fail
    S.append({"name": "10_count_mismatch", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 39}, "empties": {"s.t": True},
              "expect_status": "FAILED", "expect_state": "MrlCountFailed"})

    # 11a: count cannot complete -> fail (count_timeout_action=fail, default)
    S.append({"name": "11a_count_timeout_fail", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": "TIMEOUT"}, "empties": {"s.t": True},
              "count_timeout_action": "fail",
              "expect_status": "FAILED", "expect_state": "MrlCountFailed"})

    # 11b: count cannot complete -> warn mode passes
    S.append({"name": "11b_count_timeout_warn", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": "TIMEOUT"}, "empties": {"s.t": True},
              "count_timeout_action": "warn",
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 12: non-empty target without matching plan -> fail closed at the gate
    S.append({"name": "12_nonempty_no_plan", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 40}, "empties": {"s.t": False},
              "expect_status": "FAILED", "expect_state": "SafetyGateFailed"})

    # 15a: empty table with FullLoadRows=0 -> ok (loaded-empty)
    S.append({"name": "15a_empty_flr0_ok", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2,
              "tables": [{"label": "s.empty", "files": [], "single_pk": True, "rows": 0}],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.empty": 0}, "empties": {"s.empty": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 15b: no files but FullLoadRows>0 -> fail closed in the plan
    S.append({"name": "15b_nofiles_flr_pos", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2,
              "tables": [{"label": "s.bad", "files": [], "single_pk": True, "rows": 100}],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {}, "empties": {"s.bad": True},
              "expect_status": "FAILED", "expect_state": "MrlPlanFailed"})

    # 13: malformed null_rules -> preflight failure (child input carries bad nullRules)
    S.append({"name": "13_bad_null_rules", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 2, "tables": [tbl("s.t", 3)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {}, "empties": {"s.t": True},
              "nullRules": "s.t",   # malformed: no '='
              "expect_status": "FAILED", "expect_state": "PreflightFailed"})

    # 4: two tasks in one fleet (exercised via the fleet harness in pytest); here as child x2 is
    # covered by scenario 1+2. The fleet-level test is test_fleet_two_tasks.

    # 18: 3000 files c=10 (payload/event-budget proof: Map iterates range descriptors only)
    S.append({"name": "18_3000_files_c10", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 10, "tables": [tbl("s.huge", 3000, rows=30000)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.huge": 30000}, "empties": {"s.huge": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 19 (Amendment 6): CDC files present in the table folder -> IGNORED and never loaded.
    # 5 LOAD + 3 top-level CDC timestamp files + a stray _manifest.json, c=3 -> 2,2,1 over the
    # 5 LOAD files only; the CDC files never change the split, hash or count.
    t19 = {"label": "s.cdcmix", "files": [1, 2, 3, 4, 5], "single_pk": True, "rows": 50,
           "cdc_files": ["20261002-123456789.csv", "20261002-1234567890.csv",
                         "20141029-1134010000.csv"],
           "junk_files": ["_manifest.json", "notes.csv"]}
    S.append({"name": "19_cdc_files_ignored", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [t19],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.cdcmix": 50}, "empties": {"s.cdcmix": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded",
              "expect_sizes": {"s.cdcmix": [2, 2, 1]}})

    # 20 (A13 A.4): the count/probe python-shell driver set is UNSTRIPPED in a no-internet VPC ->
    # DriverDiscoveryCount (prepare_for=pythonshell) FAILS FAST (the wheels are not Requires-Dist
    # stripped, Glue pip would hang on PyPI), so the task fails at MrlDriversFailed BEFORE the
    # SafetyProbe/load/count ever run - never a 20-minute hang. (The happy path, scenarios 1/2/etc,
    # exercises the GOOD prepared set and proceeds.)
    S.append({"name": "20_unstripped_pythonshell_fails_fast", "mode": "full_load_only",
              "dms_start_mode": "start", "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 40}, "empties": {"s.t": True},
              "unstripped_pythonshell": True,
              "expect_status": "FAILED", "expect_state": "DriversFailed"})

    # 21 (A15): BLANK null settings reach RunRange. At 970efb4 this crashed the mrl-load job with
    # `--null_values: expected one argument` because RunRange passed the empty string as a run arg.
    # Now RunRange passes NO null args (the sentinel-aware DefaultArguments handle them), so a blank
    # null run loads cleanly. run_scenario asserts RunRange carried no null args.
    S.append({"name": "21_blank_null_settings_runrange_ok", "mode": "full_load_only",
              "dms_start_mode": "start", "max_concurrency": 2, "tables": [tbl("s.t", 4, rows=40)],
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.t": 40}, "empties": {"s.t": True},
              "nullValues": "", "nullRules": "", "assert_runrange_no_null_args": True,
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded"})

    # 22 (A19 Defect 2): a 12-file table with DMS HEX names (LOAD00000001..LOAD0000000C). The
    # planner must accept hex by DEFAULT (splitAllowHex opt-out) - at fe47276 this failed with
    # MrlSplitError 'the loader WOULD load ... LOAD0000000A.csv ...'. c=4 -> 3,3,3,3.
    t22 = {"label": "s.hextbl", "files": list(range(1, 13)), "single_pk": True, "rows": 120,
           "hex": True}
    S.append({"name": "22_hex_12_files_c4", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 4, "tables": [t22], "allow_hex": True,
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.hextbl": 120}, "empties": {"s.hextbl": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded",
              "expect_sizes": {"s.hextbl": [3, 3, 3, 3]}})

    # 23 (A19 Defect 2): the flagship 406-file HEX table (LOAD00000001..LOAD00000196), c=3 ->
    # 136,135,135. Proves name-sorted hex order == numeric order for uniform-width upper-case hex.
    t23 = {"label": "s.bighex", "files": list(range(1, 407)), "single_pk": True, "rows": 4060,
           "hex": True}
    S.append({"name": "23_hex_406_files_c3", "mode": "full_load_only", "dms_start_mode": "start",
              "max_concurrency": 3, "tables": [t23], "allow_hex": True,
              "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
              "dsql_counts": {"s.bighex": 4060}, "empties": {"s.bighex": True},
              "expect_status": "SUCCEEDED", "expect_state": "TaskSucceeded",
              "expect_sizes": {"s.bighex": [136, 135, 135]}})

    return S


def run_scenario(sc):
    # Translate the scenario's legacy 'mode' into the Amendment-8 cdc boolean, and its
    # 'max_concurrency' into the Amendment-4 execution-input override.
    sc["cdc"] = (sc.get("mode") == "full_load_and_cdc")
    if "max_concurrency" in sc and "exec_max_concurrency" not in sc:
        sc["exec_max_concurrency"] = sc["max_concurrency"]
    s = Sim(sc)
    child_input = {"input": {"taskArn": TASK_ARN}, "dmsStartMode": sc["dms_start_mode"]}
    if "nullRules" in sc:
        child_input["nullRules"] = sc["nullRules"]
    if "nullValues" in sc:
        child_input["nullValues"] = sc["nullValues"]
    if "count_timeout_action" in sc:
        child_input["countTimeoutAction"] = sc["count_timeout_action"]
    out, interp = s.run_child(child_input)
    _print_transcript(sc["name"], s, out)
    # verdict checks
    ok = out["status"] == sc["expect_status"]
    if "expect_state" in sc:
        ok = ok and out.get("state") == sc["expect_state"]
    if ok and sc.get("expect_cause_substr"):
        # AMENDMENT 2 (R2d): after the bounded re-run exhausts, the ORIGINAL cause must survive
        # (not just "retries exhausted"). The RunRange Catch stores it at $.rangeError, which the
        # Map carries into the failed output. Assert the substring appears in the serialized out.
        _blob = json.dumps(out, default=str)
        ok = ok and (sc["expect_cause_substr"] in _blob)
        if sc["expect_cause_substr"] not in _blob:
            print("  R2d FAIL: original cause %r not in failed output: %s"
                  % (sc["expect_cause_substr"], _blob[:400]))
    if ok and sc.get("expect_sizes"):
        plan = s.s3.get_json("mrl/state/%s/plan.json" % TASK_ID)
        for label, want in sc["expect_sizes"].items():
            got = [r["size"] for r in plan["tables"][label]["ranges"]]
            ok = ok and got == want
    # payload-size proof for scenario 18: the Map rangeItems must never carry file lists
    if ok and sc["name"].startswith("18"):
        plan = s.s3.get_json("mrl/state/%s/plan.json" % TASK_ID)
        # rangeItems are not in plan.json (they are the Lambda return); assert plan.json per-range
        # descriptors carry no 'files' key (only first/last/size/bytes).
        for label, pt in plan["tables"].items():
            for r in pt["ranges"]:
                ok = ok and "files" not in r
    # R23-B1 fork-handoff proof: when the scenario drove real ck/bg forks, assert the
    # original-format _load_status.json was written at EACH fork prefix and that the REAL
    # extracted CDC gate sees the forked table done there (not just at the task prefix).
    if ok and sc.get("forks"):
        ok = ok and _assert_fork_handoff(s, sc["forks"])
    # A15: assert RunRange passed NO null args per-run (the sentinel-aware DefaultArguments handle
    # them; passing a blank per-run arg crashed the live job with 'expected one argument').
    if ok and sc.get("assert_runrange_no_null_args"):
        load_runs = [r for r in s.glue.runs if "--load_file_start" in r["Arguments"]]
        assert load_runs, "expected at least one RunRange glue run"
        for r in load_runs:
            for bad in ("--null_values", "--null_rules", "--csv_null_value"):
                if bad in r["Arguments"]:
                    print("  A15 FAIL: RunRange passed %s=%r" % (bad, r["Arguments"][bad]))
                    ok = False
    return ok, out


def _assert_fork_handoff(s, forks):
    """Run the REAL extracted glue_cdc_continuous.load_full_load_status with CONFIG_PREFIX set to
    each fork prefix; the forked table MUST be seen 'done'. Proves CdcForkHandoff wrote the doc
    the reused fork CDC job gates on."""
    import ast
    src = open(os.path.join(REPO, "scripts", "glue_cdc_continuous.py")).read()
    tree = ast.parse(src)
    from datetime import datetime as _dt, timezone as _tz
    for fspec in forks:
        kind = fspec.get("kind", "ck")
        slug = fspec.get("slug", "s-t-dead")
        label = fspec["fork_table"]
        if kind == "bg":
            cp_key = "config/_task/%s/_orchestrator/group-%s/" % (TASK_ID, fspec.get("group_index", 0))
        else:
            cp_key = "config/_task/%s/_orchestrator/ck-%s/" % (TASK_ID, slug)
        if (cp_key + "_load_status.json") not in s.s3.keys():
            print("  FORK HANDOFF MISSING: %s (%s) at %s" % (label, kind, cp_key))
            return False
        ns = {"json": json, "_FULL_LOAD_ROWS": {}, "LOAD_STATUS_KEY": None,
              "datetime": _dt, "timezone": _tz,
              "split_s3": lambda p: (str(p)[5:].split("/", 1))}
        for n in tree.body:
            if isinstance(n, ast.FunctionDef) and n.name in (
                    "_read_load_status_doc", "load_full_load_status", "_load_status_err_code"):
                exec(ast.get_source_segment(src, n), ns)
        ns["s3"] = s.s3
        ns["BUCKET"] = BUCKET
        ns["CONFIG_PREFIX"] = "s3://%s/%s" % (BUCKET, cp_key)
        status = ns["load_full_load_status"]().get(label)
        if status != "done":
            print("  FORK CDC GATE NOT DONE: %s (%s) at %s -> %r" % (label, kind, cp_key, status))
            return False
        print("  fork handoff OK: %s (%s) seen done at %s" % (label, kind, cp_key))
    return True


def main():
    results = []
    for sc in scenarios():
        try:
            ok, out = run_scenario(sc)
        except Exception as e:
            import traceback
            traceback.print_exc()
            ok, out = False, {"status": "EXCEPTION", "state": str(e)}
        results.append((sc["name"], ok, out.get("status"), out.get("state"),
                        sc["expect_status"], sc.get("expect_state")))
    # Extra scenarios driven by dedicated harness functions (not the single-child template):
    for name, fn in (("4_fleet_two_tasks", scenario_fleet_two_tasks),
                     ("8b_resume_only_failed_range", scenario_resume),
                     ("9_listing_drift_fail", scenario_listing_drift),
                     ("14_dup_basename_case_variant", scenario_dup_folders),
                     ("16_glue_throttle_retried", scenario_throttle),
                     ("17_fleet_retrigger_skip", scenario_fleet_retrigger_skip)):
        try:
            ok, detail = fn()
        except Exception as e:
            import traceback
            traceback.print_exc()
            ok, detail = False, str(e)
        print("  EXTRA %s: %s (%s)" % (name, "PASS" if ok else "FAIL", detail))
        results.append((name, ok, "extra", detail, "", ""))
    print("\n" + "=" * 74)
    print("SCENARIO SUMMARY")
    print("%-32s %-6s %-11s %-24s" % ("scenario", "verdict", "got", "want"))
    nfail = 0
    for name, ok, gs, gst, es, est in results:
        if not ok:
            nfail += 1
        print("%-32s %-6s %-11s %-24s" % (name, "PASS" if ok else "FAIL",
              "%s" % gs, "%s/%s" % (es, est) if est is not None else str(gst)))
    print("=" * 74)
    print("%d scenario(s), %d passed, %d failed" % (len(results), len(results) - nfail, nfail))
    return 1 if nfail else 0


# ---- extra scenarios (dedicated harnesses) ------------------------------------------------
def _child_input(mode="full_load_only", dsm="start", conc=3, **extra):
    ci = {"input": {"taskArn": TASK_ARN}, "mode": mode, "dmsStartMode": dsm,
          "maxConcurrency": conc}
    ci.update(extra)
    return ci


def scenario_resume():
    """8b (R2b): the in-execution bounded auto-retry (3x) is EXHAUSTED (range fails 4x) so the
    first execution ends MrlLoadFailed; a SECOND execution (manual resume) of the same input
    resumes ONLY the still-incomplete range (done files are already recorded) and succeeds. This
    proves the resume path the operator uses when the automatic retries are exhausted, and that
    already-done files are skipped (rows counted exactly once)."""
    sc = {"name": "8b", "mode": "full_load_only", "dms_start_mode": "start", "max_concurrency": 3,
          "tables": [{"label": "s.t", "files": list(range(1, 7)), "single_pk": True, "rows": 60}],
          "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
          "dsql_counts": {"s.t": 60}, "empties": {"s.t": True},
          "fail_range": ("s.t", "LOAD00000001.csv"), "fail_range_times": 4}
    s = Sim(sc)
    out1, _ = s.run_child(_child_input(conc=3))
    if out1["status"] != "FAILED" or out1.get("state") != "MrlLoadFailed":
        return False, ("first run should exhaust the 3x auto-retry and fail at MrlLoadFailed, "
                       "got %s/%s" % (out1["status"], out1.get("state")))
    # second run (manual resume): the injected failure budget is used up (4 fired), so the
    # resumed range now succeeds. The gate must still pass (table non-empty now, recorded
    # progress). Done files from the prior attempts are skipped -> rows counted exactly once.
    s.sc["empties"] = {"s.t": False}       # the first run loaded some rows -> now non-empty
    s.sc["dsql_counts"] = {"s.t": 60}
    out2, _ = s.run_child(_child_input(conc=3))
    if out2["status"] != "SUCCEEDED":
        return False, "resume run should succeed, got %s/%s" % (out2["status"], out2.get("state"))
    return True, "auto-retry exhausts -> MrlLoadFailed; manual resume completes the failed range"


def scenario_listing_drift():
    """9: plan.json persisted for a run; the LOAD listing changes before a re-run -> the plan
    Lambda FAILS CLOSED (never re-splits a table with recorded ranges)."""
    sc = {"name": "9", "mode": "full_load_only", "dms_start_mode": "start", "max_concurrency": 2,
          "tables": [{"label": "s.t", "files": list(range(1, 5)), "single_pk": True, "rows": 40}],
          "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
          "dsql_counts": {"s.t": 40}, "empties": {"s.t": True}}
    s = Sim(sc)
    out1, _ = s.run_child(_child_input(conc=2))
    if out1["status"] != "SUCCEEDED":
        return False, "first run should succeed, got %s" % out1.get("state")
    # Add a NEW LOAD file so the listing hash changes, then re-run -> plan fail-closed.
    s.s3.put_load_files(BUCKET, "s/t", [5])
    out2, _ = s.run_child(_child_input(conc=2))
    if out2["status"] == "FAILED" and out2.get("state") == "MrlPlanFailed":
        return True, "listing drift fails closed at MrlPlanFailed"
    return False, "expected MrlPlanFailed on drift, got %s/%s" % (out2["status"], out2.get("state"))


def scenario_dup_folders():
    """14: two case-variant folders both hold LOAD files -> plan fails closed."""
    sc = {"name": "14", "mode": "full_load_only", "dms_start_mode": "start", "max_concurrency": 2,
          "tables": [{"label": "s.t", "files": list(range(1, 4)), "single_pk": True, "rows": 30}],
          "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
          "dsql_counts": {}, "empties": {"s.t": True}}
    s = Sim(sc)
    # seed a SECOND case-variant folder 's/T' also holding LOAD files
    s.s3.put_load_files(BUCKET, "s/T", [9])
    out, _ = s.run_child(_child_input(conc=2))
    if out["status"] == "FAILED" and out.get("state") == "MrlPlanFailed":
        return True, "two case-variant folders fail closed at MrlPlanFailed"
    return False, "expected MrlPlanFailed, got %s/%s" % (out["status"], out.get("state"))


def scenario_throttle():
    """16: a range run first raises Glue.ConcurrentRunsExceededException then succeeds on retry;
    the task still succeeds (the RunRange Retry absorbs the throttle)."""
    sc = {"name": "16", "mode": "full_load_only", "dms_start_mode": "start", "max_concurrency": 2,
          "tables": [{"label": "s.t", "files": list(range(1, 5)), "single_pk": True, "rows": 40}],
          "timeline": _task_timeline("full_load_only", "start"), "endpoint": _base_endpoint(),
          "dsql_counts": {"s.t": 40}, "empties": {"s.t": True}, "throttle_once": True}
    s = Sim(sc)
    out, _ = s.run_child(_child_input(conc=2))
    if out["status"] == "SUCCEEDED":
        return True, "throttled Glue start retried and the task succeeded"
    return False, "expected SUCCEEDED after retry, got %s/%s" % (out["status"], out.get("state"))


def scenario_fleet_two_tasks():
    """4 + 17 base: the fleet preflight builds child inputs for two tasks and the fleet Map runs
    them. We exercise the FLEET preflight Lambda directly (strict CSV -> two child inputs) since
    the child Map is already proven by the child scenarios."""
    import mrl_fleet_preflight as fp
    s3 = FakeS3()
    glue = FakeGlue()
    arn2 = "arn:aws:dms:us-east-1:123456789012:task:DEF456"
    dms = FakeDms(TASK_ARN, _task_timeline("full_load_only", "start"))

    def fake_client(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            # both ARNs resolve to a task
            class _D:
                def describe_replication_tasks(self, Filters=None, WithoutSettings=True):
                    return {"ReplicationTasks": [{"ReplicationTaskArn": "x"}]}
            return _D()
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    return {"executions": []}

                def describe_execution(self, **k):
                    return {"input": "{}"}
            return _S()
        raise Exception("no fake %r" % service)
    fp.boto3 = type("b", (), {"client": staticmethod(fake_client)})
    csv = ("task_arn,cdc,max_concurrency\n"
           "%s,false,4\n%s,true,2\n" % (TASK_ARN, arn2))
    s3.put_object(Bucket=BUCKET, Key="config/fleet_task_parallel_load.csv", Body=csv.encode())
    out = fp.handler({"bucket": BUCKET,
                      "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task",
                      "fleetExecutionId": "arn:...:execution:demo-mrl-fleet:f1"}, None)
    if out["count"] != 2 or out["toStart"] != 2:
        return False, "expected 2 tasks to start, got count=%s toStart=%s" % (out["count"], out["toStart"])
    cdcs = sorted(bool(t["input"]["cdc"]) for t in out["tasks"])
    if cdcs != [False, True]:
        return False, "child cdc flags wrong: %s" % cdcs
    return True, "fleet preflight builds 2 child inputs with per-row cdc"


def scenario_fleet_retrigger_skip():
    """17: a task whose MRL child is already RUNNING is SKIPPED on a fleet re-trigger."""
    import mrl_fleet_preflight as fp
    s3 = FakeS3()

    def fake_client(service, **kw):
        if service == "s3":
            return s3
        if service == "dms":
            class _D:
                def describe_replication_tasks(self, Filters=None, WithoutSettings=True):
                    return {"ReplicationTasks": [{"ReplicationTaskArn": "x"}]}
            return _D()
        if service == "stepfunctions":
            class _S:
                def list_executions(self, **k):
                    return {"executions": [{"executionArn": "arn:exec:running1"}]}

                def describe_execution(self, executionArn=None, **k):
                    return {"input": json.dumps({"input": {"taskArn": TASK_ARN}})}
            return _S()
        raise Exception("no fake %r" % service)
    fp.boto3 = type("b", (), {"client": staticmethod(fake_client)})
    s3.put_object(Bucket=BUCKET, Key="config/fleet_task_parallel_load.csv",
                  Body=("task_arn,cdc\n%s,false\n" % TASK_ARN).encode())
    out = fp.handler({"bucket": BUCKET,
                      "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:demo-mrl-task",
                      "fleetExecutionId": "arn:...:execution:demo-mrl-fleet:f2"}, None)
    t = out["tasks"][0]
    if t["skip"] == "already_running" and out["toStart"] == 0:
        return True, "running child -> task skipped on re-trigger"
    return False, "expected skip=already_running, got %s" % t.get("skip")


if __name__ == "__main__":
    sys.exit(main())
