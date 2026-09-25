"""
driver-discovery Lambda (Phase-1 startup).

Firewall-safe driver delivery: the customer stages the DB driver wheel(s) in
s3://<bucket>/<drivers_prefix>/ (e.g. `pip download pg8000 -d drivers/` then upload). This
Lambda LISTS those wheels and returns a comma-separated list of their S3 URIs, which the
orchestrator injects into each Glue job as --extra-py-files (Spark) / the Python-Shell
library path — so Glue never fetches from PyPI (blocked in a locked-down VPC; S3 is
reachable). If the pg8000 wheel is absent -> FAIL FAST (don't silently fall back to PyPI).

Input event: { "bucket": "...", "drivers_prefix": "drivers" }
Returns: { "extraPyFiles": "s3://.../pg8000-*.whl,s3://.../scramp-*.whl,...",
           "wheels": [ "s3://..." ], "count": N }
"""

import os

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")


def handler(event, context):
    bucket = event["bucket"]
    drivers_prefix = (event.get("drivers_prefix") or "drivers").strip("/") + "/"

    s3 = boto3.client("s3", region_name=REGION)
    wheels = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": drivers_prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            k = o["Key"]
            if k.lower().endswith(".whl") or k.lower().endswith(".zip"):
                wheels.append(f"s3://{bucket}/{k}")
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    has_pg8000 = any("pg8000" in w.rsplit("/", 1)[-1].lower() for w in wheels)
    if not has_pg8000:
        raise Exception(
            f"No pg8000 wheel found under s3://{bucket}/{drivers_prefix}. Stage the DSQL "
            f"driver wheels there (e.g. `pip download pg8000 -d drivers/` then upload the "
            f"pg8000, scramp, asn1crypto wheels). PyPI is not used at runtime by design.")

    return {
        "extraPyFiles": ",".join(sorted(wheels)),
        "wheels": sorted(wheels),
        "count": len(wheels),
    }
