#!/usr/bin/env python3
"""B27 regression — cutover is idempotent once its Glue jobs are already deleted.

ROOT CAUSE: cutover deletes the task's CDC job(s) at the end (DeleteGlueJobs). A SECOND cutover of
an already-cut-over task reaches StopCdcRun again, but stop_cdc_run.py called glue.get_job_runs on
a job the first cutover deleted -> Glue EntityNotFoundException ("Job not found") -> the StopCdcRun
state's `States.ALL` Catch routed to CutoverFailed. The per-fork stop (StopForkCdcRuns) already
tolerated EntityNotFound in the ASL, but the main StopCdcRun did not, and the Lambda itself raised.

FIX:
  * stop_cdc_run.py: wrap get_job_runs; on EntityNotFoundException return a clean no-op
    {stopped:[], matched:0, alreadyStopped:true, jobMissing:true}. ONLY EntityNotFound is
    swallowed — AccessDenied / Throttling still propagate (fail closed). This makes BOTH the
    per-task StopCdcRun and each per-fork StopOneForkCdcRun idempotent at the Lambda level.
  * cutover.asl.json StopCdcRun: add a Glue.EntityNotFoundException Retry(MaxAttempts:0) + a Catch
    that treats it as a no-op (Next: ListForkCdcJobs), so even an un-redeployed Lambda can't fail
    cutover. The States.ALL Catch -> CutoverFailed is kept for real errors.

Already-idempotent (verified here): create_glue_jobs delete/list_fork_cdc (tag selection via
_get_job_tags swallows EntityNotFound, delete_job swallows it, list_jobs just won't return a
deleted job) and drain_check (no Glue calls at all — S3 + DSQL only).

Run: python3 tests/test_b27_cutover_idempotent.py   (REPO_DIR overridable)
"""
import importlib
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)

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


# ---------------------------------------------------------------------------------------------
# Fake Glue
# ---------------------------------------------------------------------------------------------
class _GlueExc:
    class EntityNotFoundException(Exception):
        pass

    class AccessDeniedException(Exception):
        pass

    class ThrottlingException(Exception):
        pass


class _FakeGlue:
    """Models get_job_runs for stop_cdc_run. Modes:
       - "missing"   : raise EntityNotFoundException (job deleted by a prior cutover)
       - "denied"    : raise AccessDeniedException (a REAL error -> must propagate)
       - "throttle"  : raise ThrottlingException (a REAL error -> must propagate)
       - "running"   : one RUNNING run whose --config_prefix matches
       - "stopped"   : one SUCCEEDED run (already stopped)
    """
    def __init__(self, mode, config_prefix="s3://b/config/_task/t/"):
        self.mode = mode
        self.cfg = config_prefix
        self.exceptions = _GlueExc()
        self.stopped = []

    def get_job_runs(self, JobName=None, MaxResults=200, NextToken=None):
        if self.mode == "missing":
            raise self.exceptions.EntityNotFoundException("Job not found: " + str(JobName))
        if self.mode == "denied":
            raise self.exceptions.AccessDeniedException("not authorized to GetJobRuns")
        if self.mode == "throttle":
            raise self.exceptions.ThrottlingException("Rate exceeded")
        state = "RUNNING" if self.mode == "running" else "SUCCEEDED"
        return {"JobRuns": [{"Id": f"{JobName}:run",
                             "JobRunState": state,
                             "Arguments": {"--config_prefix": self.cfg}}]}

    def batch_stop_job_run(self, JobName=None, JobRunIds=None):
        self.stopped.append((JobName, tuple(JobRunIds or ())))
        return {"SuccessfulSubmissions": []}


def _load_stop_cdc_run():
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    b = types.ModuleType("boto3")
    b.client = lambda *a, **k: None
    sys.modules["boto3"] = b
    mod = importlib.import_module("stop_cdc_run")
    importlib.reload(mod)
    return mod


# ---------------------------------------------------------------------------------------------
# 1) Lambda-level idempotency + fail-closed
# ---------------------------------------------------------------------------------------------
def test_stop_cdc_run_missing_job_is_noop():
    mod = _load_stop_cdc_run()
    g = _FakeGlue("missing")
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: g)
    out = mod.handler({"cdcJobName": "proj-cdc", "configPrefix": "s3://b/config/_task/t/"}, None)
    check(out.get("alreadyStopped") is True and out.get("jobMissing") is True
          and out.get("stopped") == [] and out.get("matched") == 0,
          f"re-cutover: deleted CDC job -> clean no-op (alreadyStopped, jobMissing): {out}")
    check(g.stopped == [], "re-cutover: no batch_stop issued for a missing job")


def test_stop_cdc_run_missing_fork_job_is_noop():
    # The SAME Lambda stops each fork CDC run; a deleted fork job must also be a no-op.
    mod = _load_stop_cdc_run()
    g = _FakeGlue("missing")
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: g)
    out = mod.handler({"cdcJobName": "proj-ck-name-data-abc-cdc",
                       "configPrefix": "s3://b/config/_task/t/"}, None)
    check(out.get("jobMissing") is True and out.get("alreadyStopped") is True,
          f"re-cutover: deleted FORK CDC job -> clean no-op: {out}")


def test_stop_cdc_run_access_denied_propagates():
    mod = _load_stop_cdc_run()
    g = _FakeGlue("denied")
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: g)
    raised = None
    try:
        mod.handler({"cdcJobName": "proj-cdc", "configPrefix": "s3://b/config/_task/t/"}, None)
    except Exception as e:
        raised = type(e).__name__
    check(raised == "AccessDeniedException",
          f"real error AccessDenied still FAILS CLOSED (raised {raised})")


def test_stop_cdc_run_throttling_propagates():
    mod = _load_stop_cdc_run()
    g = _FakeGlue("throttle")
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: g)
    raised = None
    try:
        mod.handler({"cdcJobName": "proj-cdc", "configPrefix": "s3://b/config/_task/t/"}, None)
    except Exception as e:
        raised = type(e).__name__
    check(raised == "ThrottlingException",
          f"real error Throttling still FAILS CLOSED (raised {raised})")


def test_stop_cdc_run_running_job_still_stops():
    # The fix must NOT change the normal (first cutover) behaviour: a RUNNING matching run stops.
    mod = _load_stop_cdc_run()
    g = _FakeGlue("running", config_prefix="s3://b/config/_task/t/")
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: g)
    out = mod.handler({"cdcJobName": "proj-cdc", "configPrefix": "s3://b/config/_task/t/"}, None)
    check(out.get("stopped") == ["proj-cdc:run"] and out.get("jobMissing") is False
          and out.get("matched") == 1,
          f"first cutover: a RUNNING matching run is stopped exactly once: {out}")
    check(g.stopped and g.stopped[0][0] == "proj-cdc",
          "first cutover: batch_stop_job_run issued for the matching run")


# ---------------------------------------------------------------------------------------------
# 2) ASL StopCdcRun tolerates EntityNotFound as a no-op; real errors still fail closed
# ---------------------------------------------------------------------------------------------
def _cutover_asl():
    with open(os.path.join(REPO, "stepfunctions", "cutover.asl.json")) as fh:
        return json.load(fh)


def test_asl_stopcdcrun_catches_entitynotfound_noop():
    asl = _cutover_asl()
    st = asl["States"]["StopCdcRun"]
    catches = st.get("Catch", [])
    enf = [c for c in catches if "Glue.EntityNotFoundException" in c.get("ErrorEquals", [])]
    allc = [c for c in catches if "States.ALL" in c.get("ErrorEquals", [])]
    check(len(enf) == 1 and enf[0]["Next"] == "ListForkCdcJobs",
          "ASL StopCdcRun: EntityNotFound caught as no-op -> ListForkCdcJobs")
    check(len(allc) == 1 and allc[0]["Next"] == "CutoverFailed",
          "ASL StopCdcRun: real errors still fail closed -> CutoverFailed")
    # EntityNotFound Catch must come BEFORE the States.ALL catch (first match wins).
    idx_enf = next(i for i, c in enumerate(catches)
                   if "Glue.EntityNotFoundException" in c.get("ErrorEquals", []))
    idx_all = next(i for i, c in enumerate(catches)
                   if "States.ALL" in c.get("ErrorEquals", []))
    check(idx_enf < idx_all, "ASL StopCdcRun: EntityNotFound Catch precedes the States.ALL Catch")
    # The Retry must NOT retry a missing job (MaxAttempts 0 for EntityNotFound).
    enf_retry = [r for r in st.get("Retry", [])
                 if "Glue.EntityNotFoundException" in r.get("ErrorEquals", [])]
    check(len(enf_retry) == 1 and enf_retry[0].get("MaxAttempts") == 0,
          "ASL StopCdcRun: EntityNotFound Retry MaxAttempts=0 (no pointless retry on a gone job)")


def test_asl_fork_stop_still_tolerates_entitynotfound():
    # Regression guard: the pre-existing per-fork EntityNotFound no-op must stay.
    asl = _cutover_asl()
    fork = asl["States"]["StopForkCdcRuns"]["Iterator"]["States"]["StopOneForkCdcRun"]
    enf = [c for c in fork.get("Catch", [])
           if "Glue.EntityNotFoundException" in c.get("ErrorEquals", [])]
    check(len(enf) == 1 and enf[0]["Next"] in ("ForkStopDone",),
          "ASL StopOneForkCdcRun: still catches EntityNotFound as a no-op")


def test_asl_recutover_reaches_success_with_no_jobs():
    # Trace the re-cutover happy path statically: with no running/failed/pending jobs the
    # DeleteGlueJobs Choice (AllGlueJobsDeleted) default routes to CutoverOverrideTerminal,
    # whose default is the normal CutoverSucceeded and whose override branch
    # (resolved.override==true) writes the override record and ends in
    # CutoverSucceededWithOverride.
    asl = _cutover_asl()
    choice = asl["States"]["AllGlueJobsDeleted"]
    check(choice["Default"] == "CutoverOverrideTerminal",
          "ASL: AllGlueJobsDeleted default (no failed/pending) -> CutoverOverrideTerminal")
    term = asl["States"]["CutoverOverrideTerminal"]
    check(term["Default"] == "CutoverSucceeded",
          "ASL: CutoverOverrideTerminal default (no override) -> CutoverSucceeded")
    ov_next = term["Choices"][0]["Next"]
    check(ov_next == "WriteCutoverOverrideRecord",
          "ASL: CutoverOverrideTerminal override branch -> WriteCutoverOverrideRecord")
    check(asl["States"][ov_next]["Next"] == "CutoverSucceededWithOverride",
          "ASL: WriteCutoverOverrideRecord -> CutoverSucceededWithOverride")
    # StopCdcRun's EntityNotFound no-op leads into ListForkCdcJobs -> StopForkCdcRuns -> DropTags
    # -> InitDeleteLoop -> DeleteGlueJobs, i.e. the normal tail. Confirm the chain exists.
    for name in ("ListForkCdcJobs", "StopForkCdcRuns", "DropTags", "InitDeleteLoop",
                 "DeleteGlueJobs", "CutoverOverrideTerminal", "CutoverSucceeded",
                 "CutoverSucceededWithOverride"):
        check(name in asl["States"], f"ASL: re-cutover tail state present: {name}")


# ---------------------------------------------------------------------------------------------
# 3) create_glue_jobs delete/list stay idempotent when jobs are already gone
# ---------------------------------------------------------------------------------------------
def test_create_glue_jobs_handles_missing_job_tags():
    sys.path.insert(0, os.path.join(REPO, "lambdas"))
    b = types.ModuleType("boto3")
    b.client = lambda *a, **k: None
    sys.modules["boto3"] = b
    cgj = importlib.import_module("create_glue_jobs")
    importlib.reload(cgj)

    class _G:
        class EntityNotFoundException(Exception):
            pass

        def __init__(self):
            self.exceptions = types.SimpleNamespace(
                EntityNotFoundException=_G.EntityNotFoundException)

        def get_tags(self, ResourceArn=None):
            # A job that was listed but deleted a moment later -> EntityNotFound on get_tags.
            raise self.exceptions.EntityNotFoundException("Job not found")

    tags = cgj._get_job_tags(_G(), "us-east-1", "111111111111", "gone-job")
    check(tags == {}, "create_glue_jobs._get_job_tags: EntityNotFound -> {} (job skipped, no crash)")


if __name__ == "__main__":
    for _n in sorted(g for g in dict(globals()) if g.startswith("test_")):
        try:
            globals()[_n]()
        except Exception as _e:
            _failed += 1
            print(f"[FAIL] {_n} raised {type(_e).__name__}: {_e}")
    print(f"\n{_passed} passed, {_failed} failed")
    sys.exit(1 if _failed else 0)
