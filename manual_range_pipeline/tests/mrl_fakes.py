#!/usr/bin/env python3
"""Shared in-memory fakes for the MRL offline test suite (no AWS, no Spark, no network).

Everything here is deterministic and Linux-safe. The fakes model exactly the S3 / Glue / DMS
behaviours the MRL code depends on, including list_objects_v2 pagination, conditional puts
(If-Match / If-None-Match with ETags), Glue job registry + run state, and a DMS task with a
scriptable stop-reason timeline.
"""

import json
import threading


# --------------------------------------------------------------------------- Fake S3
class FakeS3:
    """In-memory S3 with ETag + conditional puts and page-limited list_objects_v2."""

    def __init__(self, page_size=1000):
        self.store = {}          # key -> (body_bytes, etag)
        self._n = 0
        self.page_size = int(page_size)
        self.lock = threading.Lock()

    # -- object ops --
    def put_object(self, Bucket, Key, Body, ContentType=None, IfNoneMatch=None, IfMatch=None):
        with self.lock:
            cur = self.store.get(Key)
            if IfNoneMatch is not None and cur is not None:
                raise self._precondition()
            if IfMatch is not None and (cur is None or cur[1] != IfMatch):
                raise self._precondition()
            self._n += 1
            etag = '"etag-%d"' % self._n
            body = Body if isinstance(Body, (bytes, bytearray)) else str(Body).encode("utf-8")
            self.store[Key] = (bytes(body), etag)
            return {"ETag": etag}

    def get_object(self, Bucket, Key):
        with self.lock:
            if Key not in self.store:
                raise self._nosuchkey()
            body, etag = self.store[Key]

        class _B:
            def __init__(self, b):
                self._b = b

            def read(self):
                return self._b
        import datetime as _dt
        return {"Body": _B(body), "ETag": etag, "LastModified": _dt.datetime(2026,1,1)}

    def delete_object(self, Bucket, Key):
        with self.lock:
            self.store.pop(Key, None)
        return {}

    def head_object(self, Bucket, Key):
        with self.lock:
            if Key not in self.store:
                raise self._nosuchkey()
            return {"ContentLength": len(self.store[Key][0])}

    # -- listing with pagination + delimiter --
    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None, Delimiter=None,
                        MaxKeys=None):
        with self.lock:
            keys = sorted(k for k in self.store if k.startswith(Prefix))
        if Delimiter:
            # Group into CommonPrefixes at the first delimiter after Prefix.
            commons = []
            seen = set()
            contents = []
            for k in keys:
                rest = k[len(Prefix):]
                if Delimiter in rest:
                    cp = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                    if cp not in seen:
                        seen.add(cp)
                        commons.append({"Prefix": cp})
                else:
                    contents.append(k)
            # (delimiter listings are small in our tests; no pagination needed)
            return {
                "Contents": [self._obj(k) for k in contents],
                "CommonPrefixes": commons,
                "IsTruncated": False,
            }
        # Paginated content listing.
        start = 0
        if ContinuationToken is not None:
            start = int(ContinuationToken)
        page = keys[start:start + self.page_size]
        nxt = start + self.page_size
        truncated = nxt < len(keys)
        return {
            "Contents": [self._obj(k) for k in page],
            "IsTruncated": truncated,
            "NextContinuationToken": str(nxt) if truncated else None,
            "KeyCount": len(page),
        }

    def _obj(self, k):
        return {"Key": k, "Size": len(self.store[k][0])}

    def get_paginator(self, op):
        """Minimal paginator matching botocore's list_objects_v2 paginate() shape."""
        fake = self

        class _Pag:
            def paginate(self, Bucket, Prefix="", **kw):
                token = None
                while True:
                    resp = fake.list_objects_v2(Bucket=Bucket, Prefix=Prefix,
                                                ContinuationToken=token)
                    yield resp
                    if resp.get("IsTruncated"):
                        token = resp.get("NextContinuationToken")
                    else:
                        break
        if op != "list_objects_v2":
            raise Exception("FakeS3 paginator only supports list_objects_v2 (got %r)" % op)
        return _Pag()

    # -- helpers to seed LOAD files (size is the stored body length) --
    def put_load_files(self, bucket, folder, numbers, width=8, size=1000, ext="csv",
                       prefix="LOAD"):
        """Seed LOAD part-files. `numbers` is a list of ints or (int, size) tuples."""
        for item in numbers:
            if isinstance(item, tuple):
                num, sz = item
            else:
                num, sz = item, size
            name = "%s%0*d.%s" % (prefix, width, num, ext)
            key = "%s/%s" % (folder.rstrip("/"), name)
            self.store[key] = (b"x" * int(sz), '"etag-seed-%s"' % name)

    def put_raw(self, bucket, key, size=100):
        self.store[key] = (b"y" * int(size), '"etag-raw"')

    # -- errors --
    def _nosuchkey(self):
        e = Exception("NoSuchKey")
        e.response = {"Error": {"Code": "NoSuchKey"}}
        return e

    def _precondition(self):
        e = Exception("PreconditionFailed (412)")
        e.response = {"Error": {"Code": "PreconditionFailed"}}
        return e

    def keys(self):
        with self.lock:
            return sorted(self.store)

    def get_json(self, key):
        return json.loads(self.get_object(Bucket="b", Key=key)["Body"].read().decode("utf-8"))


# --------------------------------------------------------------------------- Fake Glue
class FakeGlue:
    """Records every started run with its full Arguments; can fail specific runs; tracks
    MaxConcurrentRuns per job and raises ConcurrentRunsExceededException when exceeded."""

    class _Err(Exception):
        def __init__(self, code, msg=""):
            super().__init__(msg or code)
            self.code = code
            self.response = {"Error": {"Code": code}}

    def _raise(self, code, msg=""):
        """Raise an exception whose CLASS NAME is `code`, so Lambda handlers that branch on
        type(e).__name__ (e.g. AlreadyExistsException / EntityNotFoundException) match a real
        botocore client error."""
        exc_type = type(code, (self._Err,), {})
        raise exc_type(code, msg)

    def __init__(self):
        self.jobs = {}                  # name -> {"MaxConcurrentRuns": int, "Tags": {...}, ...}
        self.runs = []                  # list of {"JobName","RunId","Arguments","state",...}
        self._run_seq = 0
        self.fail_runs = {}             # (job_name, nth_run_index) -> error_message
        self._active = {}               # job -> current active count (for concurrency sim)
        self.lock = threading.Lock()

    def create_job(self, Name, MaxConcurrentRuns=1, Tags=None, **kw):
        with self.lock:
            if Name in self.jobs:
                self._raise("AlreadyExistsException", "already exists")
            self.jobs[Name] = {"MaxConcurrentRuns": int(MaxConcurrentRuns),
                               "Tags": dict(Tags or {}), **kw}
        return {"Name": Name}

    def update_job(self, JobName=None, JobUpdate=None, Name=None, MaxConcurrentRuns=None, **kw):
        name = JobName or Name
        upd = JobUpdate or kw
        with self.lock:
            j = self.jobs.setdefault(name, {"MaxConcurrentRuns": 1, "Tags": {}})
            ep = (upd or {}).get("ExecutionProperty") or {}
            if ep.get("MaxConcurrentRuns") is not None:
                j["MaxConcurrentRuns"] = int(ep["MaxConcurrentRuns"])
            if MaxConcurrentRuns is not None:
                j["MaxConcurrentRuns"] = int(MaxConcurrentRuns)
        return {}

    def get_job(self, Name):
        with self.lock:
            if Name not in self.jobs:
                self._raise("EntityNotFoundException", "not found")
            return {"Job": dict(self.jobs[Name], Name=Name)}

    def get_tags(self, ResourceArn):
        name = str(ResourceArn).rsplit("/", 1)[-1]
        with self.lock:
            j = self.jobs.get(name)
            return {"Tags": dict(j.get("Tags", {})) if j else {}}

    def tag_resource(self, ResourceArn, TagsToAdd):
        name = str(ResourceArn).rsplit("/", 1)[-1]
        with self.lock:
            j = self.jobs.setdefault(name, {"MaxConcurrentRuns": 1, "Tags": {}})
            j.setdefault("Tags", {}).update(TagsToAdd or {})
        return {}

    def start_job_run(self, JobName, Arguments=None, **kw):
        with self.lock:
            j = self.jobs.get(JobName)
            maxc = (j or {}).get("MaxConcurrentRuns", 1)
            active = self._active.get(JobName, 0)
            if active >= maxc:
                self._raise("ConcurrentRunsExceededException",
                                "Concurrent runs exceeded for job %s" % JobName)
            self._run_seq += 1
            rid = "jr_%08d" % self._run_seq
            nth = sum(1 for r in self.runs if r["JobName"] == JobName)
            rec = {"JobName": JobName, "RunId": rid, "Arguments": dict(Arguments or {}),
                   "state": "RUNNING", "nth": nth}
            self.runs.append(rec)
            self._active[JobName] = active + 1
            # Decide terminal state now (sync fakes resolve immediately on get_job_run).
            fail_msg = self.fail_runs.get((JobName, nth))
            rec["_terminal"] = ("FAILED", fail_msg) if fail_msg else ("SUCCEEDED", None)
        return {"JobRunId": rid}

    def get_job_run(self, JobName, RunId, **kw):
        with self.lock:
            for r in self.runs:
                if r["JobName"] == JobName and r["RunId"] == RunId:
                    state, msg = r["_terminal"]
                    if r["state"] == "RUNNING":
                        r["state"] = state
                        self._active[JobName] = max(0, self._active.get(JobName, 1) - 1)
                    out = {"JobRun": {"Id": RunId, "JobName": JobName,
                                      "JobRunState": r["state"]}}
                    if msg:
                        out["JobRun"]["ErrorMessage"] = msg
                    return out
            self._raise("EntityNotFoundException", "no such run")

    def started_args(self, job_name=None):
        """All Arguments maps started (optionally for one job), in start order."""
        return [r["Arguments"] for r in self.runs
                if job_name is None or r["JobName"] == job_name]


# --------------------------------------------------------------------------- Fake DMS
class FakeDms:
    """A DMS task with a scriptable timeline of describe_replication_tasks responses and
    describe_table_statistics. `timeline` is a list of task dicts returned on successive
    describe calls (the last one repeats)."""

    def __init__(self, task_arn, timeline, table_stats=None, endpoint=None, table_mappings=None):
        self.task_arn = task_arn
        self.timeline = list(timeline)
        self._i = 0
        self.table_stats = table_stats or []
        self.endpoint = endpoint or {}
        self.table_mappings = table_mappings
        self.started = []
        self.resumed = []

    def describe_replication_tasks(self, Filters=None, WithoutSettings=False):
        task = dict(self.timeline[min(self._i, len(self.timeline) - 1)])
        self._i += 1
        task.setdefault("ReplicationTaskArn", self.task_arn)
        if self.table_mappings is not None and not WithoutSettings:
            task.setdefault("TableMappings", self.table_mappings)
        return {"ReplicationTasks": [task]}

    def start_replication_task(self, ReplicationTaskArn, StartReplicationTaskType):
        if StartReplicationTaskType == "resume-processing":
            self.resumed.append(ReplicationTaskArn)
        else:
            self.started.append((ReplicationTaskArn, StartReplicationTaskType))
        return {"ReplicationTask": {"ReplicationTaskArn": ReplicationTaskArn}}

    def describe_table_statistics(self, ReplicationTaskArn, MaxRecords=500, Marker=None):
        return {"TableStatistics": list(self.table_stats)}

    def describe_endpoints(self, Filters=None):
        return {"Endpoints": [self.endpoint]}
