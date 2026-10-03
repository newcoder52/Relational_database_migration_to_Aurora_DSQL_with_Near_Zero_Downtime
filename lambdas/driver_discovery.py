"""
driver-discovery Lambda (startup workflow).

Firewall-safe driver delivery: the driver wheels are staged in s3://<bucket>/<drivers_prefix>/
(downloaded once, see RUNBOOK Step 3b). This Lambda lists them and returns a comma-separated
list of their S3 URIs, which create-glue-jobs saves on each Glue job as --extra-py-files, so
Glue never fetches from PyPI (blocked in a locked-down VPC; S3 is reachable). If the pg8000
wheel is absent -> FAIL FAST.

AUTOMATIC PREPARATION FOR THE PYTHON-SHELL CDC JOB  (event "prepare_for": "pythonshell")
  Glue Python shell (Python 3.9) pip-installs every --extra-py-files wheel, one at a time. A
  wheel that declares a dependency (Requires-Dist) not installed yet makes pip ask pypi.org,
  which times out behind a firewall (~20 min, then CalledProcessError). So for the Python-shell
  CDC job this Lambda:
    1. validates the wheel set for Python 3.9 (one version per package, Requires-Python allows
       3.9, dependency versions in range, the 10 required packages present, botocore knows
       'dsql') -- BEFORE DMS starts, so a wrong wheel fails the run in seconds with a clear
       message instead of failing CDC hours later;
    2. strips Requires-Dist from each wheel (prepare_cdc_wheels.py, the same tested code as the
       command-line tool), verifies every rebuilt wheel, and uploads the set to
       s3://<bucket>/<prepared_prefix>/<fingerprint>/ with MANIFEST.txt and, last, _READY.json;
    3. returns the PREPARED list. The originals in <drivers_prefix>/ are never changed.
  The fingerprint is derived from the source wheels (name, size, ETag) plus PREP_VERSION, so the
  work runs once per wheel set; every later task reuses the prepared folder. Wheels that are
  already stripped pass through unchanged. Any other "prepare_for" value (e.g. "spark") or none
  (older per-task workflows) returns the plain list, exactly as before.

Input event: { "bucket": "...", "drivers_prefix": "driver-cdc",
               "prepare_for": "pythonshell" (optional), "prepared_prefix": "driver-cdc-prepared" (optional) }
Returns:     { "extraPyFiles": "s3://...whl,...", "wheels": [...], "count": N,
               "prepared": bool, "reused": bool, "fingerprint": "...", "preparedPrefix": "s3://...",
               "sourceExtraPyFiles": "s3://<drivers_prefix>/...whl,..." }
Needs: s3:ListBucket, s3:GetObject, s3:PutObject on the bucket; ~1 GB memory and a 300 s timeout
for the one-time preparation (botocore is large).
"""

import hashlib
import json
import os
import shutil
import time

import boto3

try:
    import prepare_cdc_wheels as pcw    # ships in the same zip (lambdas/prepare_cdc_wheels.py)
except ImportError:                      # an older fn.zip built before this file existed
    pcw = None

REGION = os.environ.get("AWS_REGION", "us-east-1")
WORK_DIR = os.environ.get("DRIVER_PREP_WORK_DIR", "/tmp")
PREP_VERSION = "1"            # bump if the preparation logic changes (forces a fresh prepare)
CDC_PYTHON = (3, 9)            # Glue Python shell
# Everything the CDC script imports, plus their dependencies on Python 3.9.
CDC_REQUIRED = ("asn1crypto", "boto3", "botocore", "jmespath", "pg8000", "python-dateutil",
                "s3transfer", "scramp", "six", "urllib3")


class DriverCheckError(Exception):
    """The driver-cdc/ wheels can't work for the Python-shell CDC job (message says why)."""


def _list_wheels(s3, bucket, prefix):
    objs, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []) or []:
            k = o["Key"]
            if k.lower().endswith(".whl") or k.lower().endswith(".zip"):
                objs.append({"key": k, "size": o.get("Size"), "etag": (o.get("ETag") or "").strip('"')})
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return sorted(objs, key=lambda o: o["key"])


def handler(event, context):
    bucket = event["bucket"]
    drivers_prefix = (event.get("drivers_prefix") or "drivers").strip("/") + "/"

    s3 = boto3.client("s3", region_name=REGION)
    objs = _list_wheels(s3, bucket, drivers_prefix)
    wheels = [f"s3://{bucket}/{o['key']}" for o in objs]

    has_pg8000 = any("pg8000" in w.rsplit("/", 1)[-1].lower() for w in wheels)
    if not has_pg8000:
        raise Exception(
            f"No pg8000 wheel found under s3://{bucket}/{drivers_prefix}. Stage the DSQL "
            f"driver wheels there (e.g. `pip download pg8000 -d drivers/` then upload the "
            f"pg8000, scramp, asn1crypto wheels). PyPI is not used at runtime by design.")

    plain = {"extraPyFiles": ",".join(wheels), "wheels": wheels, "count": len(wheels)}
    if str(event.get("prepare_for") or "").strip().lower() != "pythonshell":
        return plain

    if pcw is None:
        raise DriverCheckError("prepare_cdc_wheels.py is missing from this Lambda's zip. Rebuild "
                               "fn.zip from the repo's current lambdas/ folder and update the "
                               "driver-discovery function (RUNBOOK Step 2).")
    prepared_prefix = (event.get("prepared_prefix") or "driver-cdc-prepared").strip("/") + "/"
    if prepared_prefix.startswith(drivers_prefix) or drivers_prefix.startswith(prepared_prefix):
        raise DriverCheckError(f"prepared_prefix {prepared_prefix} must not overlap drivers_prefix "
                               f"{drivers_prefix} (the prepared copies would be listed as inputs).")
    out = _prepare(s3, bucket, drivers_prefix, prepared_prefix, objs)
    out["sourceExtraPyFiles"] = plain["extraPyFiles"]
    return out


def _fingerprint(objs):
    h = hashlib.sha256(f"prep-v{PREP_VERSION} py{CDC_PYTHON[0]}.{CDC_PYTHON[1]}\n".encode())
    for o in objs:
        h.update(f"{o['key'].rsplit('/', 1)[-1]}|{o['size']}|{o['etag']}\n".encode())
    return h.hexdigest()[:20]


def _reuse(s3, bucket, out_prefix):
    """Return the wheel list from an existing complete preparation, or None."""
    try:
        ready = json.loads(s3.get_object(Bucket=bucket, Key=out_prefix + "_READY.json")["Body"].read())
    except Exception as e:
        code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    present = {o["key"] for o in _list_wheels(s3, bucket, out_prefix)}
    want = [w for w in ready.get("wheels", [])]
    if want and all(w.split("/", 3)[3] in present for w in want):
        return want
    return None     # incomplete (files deleted by hand?) -> prepare again


def _extra_rules(dists):
    """Checks that still matter when the input wheels were already stripped (their Requires-Dist
    is gone, so pcw._drv_check can't see the dependency ranges)."""
    probs = []
    for need in CDC_REQUIRED:
        if pcw._drv_norm(need) not in dists:
            probs.append(f"{need} is missing from the folder (the CDC job needs all of: "
                         f"{', '.join(CDC_REQUIRED)}).")
    u = dists.get("urllib3")
    if u and not pcw._drv_satisfies(u["version"], "<1.27"):
        probs.append(f"urllib3 {u['version']} can't be used by botocore on Python 3.9; use a "
                     f"1.26.x release (e.g. urllib3 1.26.20).")
    b3, bc = dists.get("boto3"), dists.get("botocore")
    if b3 and bc:
        r3, rc = pcw._drv_release(b3["version"]), pcw._drv_release(bc["version"])
        if r3[:2] != rc[:2] or pcw._drv_cmp(rc, r3) < 0:
            probs.append(f"boto3 {b3['version']} and botocore {bc['version']} don't match; "
                         f"download them together (same 1.x release, botocore >= boto3).")
    return probs


def _prepare(s3, bucket, drivers_prefix, prepared_prefix, objs):
    fp = _fingerprint(objs)
    out_prefix = f"{prepared_prefix}{fp}/"
    base = {"prepared": True, "fingerprint": fp, "preparedPrefix": f"s3://{bucket}/{out_prefix}"}

    reused = _reuse(s3, bucket, out_prefix)
    if reused:
        print(f"driver-cdc: reusing prepared wheels at s3://{bucket}/{out_prefix} ({len(reused)} files)")
        return dict(base, extraPyFiles=",".join(reused), wheels=reused, count=len(reused), reused=True)

    bad_names = [o["key"] for o in objs if not o["key"].lower().endswith(".whl")]
    if bad_names:
        raise DriverCheckError("driver-cdc/ for the Python-shell CDC job must hold only .whl "
                               f"files; remove: {', '.join(bad_names)}")
    work = os.path.join(WORK_DIR, f"drvprep-{fp}-{int(time.time() * 1000)}")
    src_dir, dst_dir = os.path.join(work, "in"), os.path.join(work, "out")
    os.makedirs(src_dir)
    os.makedirs(dst_dir)
    try:
        paths = []
        for o in objs:
            p = os.path.join(src_dir, o["key"].rsplit("/", 1)[-1])
            if os.path.exists(p):
                raise DriverCheckError(f"two files named {os.path.basename(p)} under "
                                       f"s3://{bucket}/{drivers_prefix}; keep one.")
            s3.download_file(bucket, o["key"], p)
            paths.append(p)

        # 1. validate (nothing is written to S3 if any check fails)
        dists, problems = pcw._drv_check(paths, CDC_PYTHON)
        problems += _extra_rules(dists)
        bc = dists.get("botocore")
        if bc and not pcw._botocore_has_dsql(bc["path"]):
            problems.append(f"botocore {bc['version']} has no 'dsql' service; use boto3/botocore "
                            f"1.35 or later (but below 1.43, which needs Python 3.10).")
        if problems:
            raise DriverCheckError(
                f"The wheels in s3://{bucket}/{drivers_prefix} won't work for the Python-shell "
                f"CDC job (Python 3.9). Nothing was started. Fix: " + " | ".join(problems) +
                " (see RUNBOOK Step 3b for the exact pip download command).")

        # 2. strip + verify
        manifest = [f"# driver-cdc wheels with Requires-Dist removed for Glue Python shell 3.9",
                    f"# source: s3://{bucket}/{drivers_prefix}  fingerprint: {fp}", ""]
        vprobs, records = [], []
        for w in sorted(dists.values(), key=lambda d: d["norm"]):
            dst = os.path.join(dst_dir, w["file"])
            removed = pcw.rebuild_wheel(w["path"], dst)
            vprobs += pcw.verify_wheel(dst, w["name"], w["version"])
            records.append({"file": w["file"], "name": w["name"], "version": w["version"],
                            "sha256_source": pcw._file_sha256(w["path"]),
                            "sha256_prepared": pcw._file_sha256(dst),
                            "removed_requires_dist": removed})
            manifest += [w["file"], f"  original sha256: {records[-1]['sha256_source']}",
                         f"  new sha256:      {records[-1]['sha256_prepared']}"]
            manifest += [f"  removed: {r}" for r in removed] or ["  removed: (none)"]
            manifest.append("")
        if vprobs:
            raise DriverCheckError("Preparing the driver-cdc wheels failed verification: " +
                                   " | ".join(vprobs))

        # 3. upload: wheels, MANIFEST.txt, then _READY.json LAST (marks the set complete)
        uris = []
        for r in records:
            key = out_prefix + r["file"]
            s3.upload_file(os.path.join(dst_dir, r["file"]), bucket, key)
            uris.append(f"s3://{bucket}/{key}")
        s3.put_object(Bucket=bucket, Key=out_prefix + "MANIFEST.txt",
                      Body="\n".join(manifest).encode("utf-8"), ContentType="text/plain")
        ready = {"fingerprint": fp, "python": "3.9", "source": f"s3://{bucket}/{drivers_prefix}",
                 "prepared_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 "wheels": uris, "files": records}
        s3.put_object(Bucket=bucket, Key=out_prefix + "_READY.json",
                      Body=json.dumps(ready, indent=1).encode("utf-8"), ContentType="application/json")
        stripped = sum(1 for r in records if r["removed_requires_dist"])
        print(f"driver-cdc: prepared {len(uris)} wheels ({stripped} stripped, "
              f"{len(uris) - stripped} unchanged) -> s3://{bucket}/{out_prefix}")
        return dict(base, extraPyFiles=",".join(uris), wheels=uris, count=len(uris), reused=False)
    finally:
        shutil.rmtree(work, ignore_errors=True)
