#!/usr/bin/env python3
"""Apply (or dry-run / restore) the stopAfterLoad patch to a DEPLOYED pipeline.

stopAfterLoad is an OPTIONAL top-level boolean on the fleet/per-task startup input
({"bucket":"<b>","stopAfterLoad":true}). When true the startup workflow STOPS AFTER THE FULL
LOAD: every group loads exactly as today (same retries + resume), but no validation Glue job is
started, DMS is NOT resumed and CDC is NOT started. Absent/null/false is BYTE-IDENTICAL to today.

This script patches a deployed environment that has NO git checkout. It:
  * reads the two state-machine definitions with stepfunctions:DescribeStateMachine
    (names <project>-startup and <project>-fleet-startup);
  * patches the startup definition by ANCHORS on structure + state names (never on ARNs: the
    <<PLACEHOLDERS>> are already filled in a deployed definition), fail-closed if an anchor is
    missing/ambiguous or the shape is not recognised; the fleet definition is UNCHANGED (verified);
  * patches the Lambda code of <project>-resolve-task and <project>-preflight-tasks by downloading
    the deployed code zip (lambda:GetFunction), editing ONLY resolve_task.py / preflight_tasks.py
    inside it (every other file — params_csv.py, etc. — is kept byte-identical), re-zipping and
    calling lambda:UpdateFunctionCode;
  * saves every original (definitions as .json, code as .zip) to
    s3://<bucket>/scripts/backup/startup_stop_after_load/<timestamp>/ AND to a local dir;
  * DEFAULT IS DRY RUN: prints a unified diff of each definition and each patched .py, changes
    NOTHING. --apply performs it in a SAFE ORDER (Lambdas first, then the startup SM, then the
    fleet SM) and after each update reads the resource back and compares. --restore <dir> puts
    everything back from a backup dir.

AWS actions used (and ONLY these): stepfunctions:DescribeStateMachine,
stepfunctions:UpdateStateMachine, lambda:GetFunction, lambda:GetFunctionConfiguration,
lambda:UpdateFunctionCode, s3:PutObject. It never calls iam and never touches any other
Lambda or state machine.

Usage:
  dry-run (default):  python3 apply_stop_after_load_patch.py --project P --region R --bucket B
  apply:              python3 apply_stop_after_load_patch.py --project P --region R --bucket B --apply
  restore:            python3 apply_stop_after_load_patch.py --project P --region R --bucket B \
                          --restore <local-backup-dir>

Config is via flags only (no baked-in customer values; this is a public repo).

The actual patch transforms live in stop_after_load_transform.py and are SHARED with the
plain-file path and the tests, so the deployed patch and the repo diff are the same change.
"""
import argparse
import difflib
import io
import json
import os
import sys
import time
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stop_after_load_transform as X  # noqa: E402


# ------------------------------------------------------------------ small helpers
def _diff(old_text, new_text, label):
    return "".join(difflib.unified_diff(
        old_text.splitlines(keepends=True), new_text.splitlines(keepends=True),
        fromfile=f"a/{label}", tofile=f"b/{label}"))


def _is_noop(diff_text):
    return diff_text.strip() == ""


def _write_local(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
    with open(path, mode) as fh:
        fh.write(data)


# ------------------------------------------------------------------ zip editing
def patch_zip_member(zip_bytes, member_name, transform):
    """Return (new_zip_bytes, old_src, new_src). Rewrites member_name with transform(old_src);
    EVERY other member is copied byte-identical (same name, no mode/zinfo drift beyond content).
    Fails closed if the member is absent."""
    zin = zipfile.ZipFile(io.BytesIO(zip_bytes), "r")
    names = zin.namelist()
    if member_name not in names:
        raise X.PatchError(f"zip has no member {member_name!r}; members={names[:12]}"
                           f"{'...' if len(names) > 12 else ''}")
    old_src = zin.read(member_name).decode("utf-8")
    new_src = transform(old_src)
    out = io.BytesIO()
    zout = zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED)
    for info in zin.infolist():
        data = zin.read(info.filename)
        if info.filename == member_name:
            data = new_src.encode("utf-8")
        # preserve the original ZipInfo (name, date, external attrs) so unrelated files stay
        # byte-identical in content and metadata; only the one member's bytes change.
        zi = zipfile.ZipInfo(info.filename, date_time=info.date_time)
        zi.external_attr = info.external_attr
        zi.internal_attr = info.internal_attr
        zi.create_system = info.create_system
        zi.compress_type = zipfile.ZIP_DEFLATED
        zout.writestr(zi, data)
    zout.close()
    zin.close()
    return out.getvalue(), old_src, new_src


# ------------------------------------------------------------------ AWS layer (thin)
class Aws:
    """Thin wrapper over boto3 clients, injectable for tests. ONLY the allowed actions."""
    def __init__(self, sfn, lam, s3, region):
        self.sfn, self.lam, self.s3, self.region = sfn, lam, s3, region

    @classmethod
    def real(cls, region):
        import boto3
        return cls(boto3.client("stepfunctions", region_name=region),
                   boto3.client("lambda", region_name=region),
                   boto3.client("s3", region_name=region), region)

    # state machines
    def describe_sm(self, name, account):
        arn = f"arn:aws:states:{self.region}:{account}:stateMachine:{name}" if account else name
        try:
            return self.sfn.describe_state_machine(stateMachineArn=arn)
        except Exception:
            # some deployments key describe by name directly (fakes / older)
            return self.sfn.describe_state_machine(stateMachineArn=name)

    def update_sm(self, arn, definition):
        return self.sfn.update_state_machine(stateMachineArn=arn, definition=definition)

    # lambdas
    def get_function(self, name):
        return self.lam.get_function(FunctionName=name)

    def get_function_configuration(self, name):
        return self.lam.get_function_configuration(FunctionName=name)

    def update_function_code(self, name, zip_bytes):
        return self.lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)

    # s3
    def put_object(self, bucket, key, body):
        return self.s3.put_object(Bucket=bucket, Key=key, Body=body)


def _download_code_zip(aws, fn_name):
    """GetFunction returns a presigned Code.Location URL. Download it with urllib (no extra deps)."""
    meta = aws.get_function(fn_name)
    loc = (meta.get("Code") or {}).get("Location")
    if not loc:
        raise X.PatchError(f"GetFunction for {fn_name!r} returned no Code.Location")
    if loc.startswith("file://") or os.path.exists(loc):
        with open(loc.replace("file://", ""), "rb") as fh:
            return fh.read(), meta
    import urllib.request
    with urllib.request.urlopen(loc) as resp:  # noqa: S310 (AWS presigned URL)
        return resp.read(), meta


# ------------------------------------------------------------------ plan building
LAMBDA_TARGETS = [
    # (suffix-of-function-name, member .py inside the zip, transform fn)
    ("resolve-task", "resolve_task.py", X.patch_resolve_task_src),
    ("preflight-tasks", "preflight_tasks.py", X.patch_preflight_tasks_src),
]


def _sm_names(project):
    return (f"{project}-startup", f"{project}-fleet-startup")


def build_plan(aws, project, account):
    """Read the deployed resources and compute the new bytes. Returns a plan dict. Fails closed
    on any unrecognised shape / missing anchor (the transforms raise X.PatchError)."""
    startup_name, fleet_name = _sm_names(project)
    plan = {"state_machines": [], "lambdas": []}

    for name in (startup_name, fleet_name):
        desc = aws.describe_sm(name, account)
        arn = desc.get("stateMachineArn") or name
        old_def = desc["definition"]
        old_obj = json.loads(old_def)
        if name == startup_name:
            new_obj = X.patch_startup_asl(old_obj)           # fail-closed structural patch
        else:
            new_obj = X.verify_fleet_startup_asl(old_obj)    # recognised + UNCHANGED
        new_def = json.dumps(new_obj, indent=2)
        old_pretty = json.dumps(old_obj, indent=2)
        plan["state_machines"].append({
            "name": name, "arn": arn,
            "old_def": old_def, "old_pretty": old_pretty, "new_def": new_def,
            "diff": _diff(old_pretty, new_def, f"{name}.definition.json"),
            "unchanged": (name == fleet_name),
        })

    for suffix, member, transform in LAMBDA_TARGETS:
        fn_name = f"{project}-{suffix}"
        zip_bytes, _meta = _download_code_zip(aws, fn_name)
        new_zip, old_src, new_src = patch_zip_member(zip_bytes, member, transform)
        plan["lambdas"].append({
            "name": fn_name, "member": member,
            "old_zip": zip_bytes, "new_zip": new_zip,
            "old_src": old_src, "new_src": new_src,
            "diff": _diff(old_src, new_src, f"{fn_name}:{member}"),
        })
    return plan


# ------------------------------------------------------------------ backup
def _timestamp():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def save_backups(aws, bucket, plan, local_root):
    ts = _timestamp()
    s3_prefix = f"scripts/backup/startup_stop_after_load/{ts}/"
    local_dir = os.path.join(local_root, ts)
    manifest = {"timestamp": ts, "s3Prefix": s3_prefix, "stateMachines": [], "lambdas": []}
    for sm in plan["state_machines"]:
        key = s3_prefix + f"{sm['name']}.definition.json"
        aws.put_object(bucket, key, sm["old_def"].encode("utf-8"))
        _write_local(os.path.join(local_dir, f"{sm['name']}.definition.json"),
                     sm["old_def"])
        manifest["stateMachines"].append({"name": sm["name"], "arn": sm["arn"], "s3Key": key})
    for lm in plan["lambdas"]:
        key = s3_prefix + f"{lm['name']}.zip"
        aws.put_object(bucket, key, lm["old_zip"])
        _write_local(os.path.join(local_dir, f"{lm['name']}.zip"), lm["old_zip"])
        manifest["lambdas"].append({"name": lm["name"], "s3Key": key})
    _write_local(os.path.join(local_dir, "MANIFEST.json"), json.dumps(manifest, indent=2))
    aws.put_object(bucket, s3_prefix + "MANIFEST.json",
                   json.dumps(manifest, indent=2).encode("utf-8"))
    return local_dir, s3_prefix


# ------------------------------------------------------------------ apply / verify
def _read_back_sm(aws, name, account, expected_def):
    got = aws.describe_sm(name, account)["definition"]
    return json.loads(got) == json.loads(expected_def)


def apply_plan(aws, project, account, plan):
    """SAFE ORDER: Lambdas first (the ASL calls them), then the startup SM, then the fleet SM.
    After each update, read the resource back and compare. Returns a list of (name, 'updated'|
    'noop'|'verified')."""
    results = []
    # 1) Lambdas
    for lm in plan["lambdas"]:
        if _is_noop(lm["diff"]):
            results.append((lm["name"], "noop"))
            continue
        aws.update_function_code(lm["name"], lm["new_zip"])
        # read-back: re-download and confirm the member now matches new_src
        zb, _ = _download_code_zip(aws, lm["name"])
        got = zipfile.ZipFile(io.BytesIO(zb)).read(lm["member"]).decode("utf-8")
        if got != lm["new_src"]:
            raise X.PatchError(f"read-back mismatch after updating Lambda {lm['name']}")
        results.append((lm["name"], "updated"))
    # 2) startup SM, 3) fleet SM (fleet is a verified no-op)
    order = sorted(plan["state_machines"], key=lambda s: 0 if s["name"].endswith("-startup")
                   and not s["name"].endswith("-fleet-startup") else 1)
    for sm in order:
        if sm["unchanged"] or _is_noop(sm["diff"]):
            results.append((sm["name"], "verified" if sm["unchanged"] else "noop"))
            continue
        aws.update_sm(sm["arn"], sm["new_def"])
        if not _read_back_sm(aws, sm["name"], account, sm["new_def"]):
            raise X.PatchError(f"read-back mismatch after updating state machine {sm['name']}")
        results.append((sm["name"], "updated"))
    return results


def restore(aws, project, account, backup_dir):
    """Put everything back from a local backup dir (definitions + code zips). Lambdas first, then
    SMs, same safe order."""
    man = json.load(open(os.path.join(backup_dir, "MANIFEST.json")))
    results = []
    for lm in man["lambdas"]:
        with open(os.path.join(backup_dir, f"{lm['name']}.zip"), "rb") as fh:
            aws.update_function_code(lm["name"], fh.read())
        results.append((lm["name"], "restored"))
    for sm in man["stateMachines"]:
        with open(os.path.join(backup_dir, f"{sm['name']}.definition.json")) as fh:
            definition = fh.read()
        aws.update_sm(sm["arn"], definition)
        if not _read_back_sm(aws, sm["name"], account, definition):
            raise X.PatchError(f"restore read-back mismatch for {sm['name']}")
        results.append((sm["name"], "restored"))
    return results


# ------------------------------------------------------------------ CLI
def _print_dry_run(plan):
    print("DRY RUN — nothing is changed. Pass --apply to perform the patch.\n")
    for sm in plan["state_machines"]:
        print(f"===== state machine: {sm['name']} "
              f"({'UNCHANGED (verified recognised shape)' if sm['unchanged'] else 'PATCH'}) =====")
        print(sm["diff"] if sm["diff"].strip() else "  (no changes)")
        print()
    for lm in plan["lambdas"]:
        print(f"===== lambda: {lm['name']} :: {lm['member']} =====")
        print(lm["diff"] if lm["diff"].strip() else "  (no changes)")
        print()


def main(argv=None, aws=None):
    ap = argparse.ArgumentParser(description="Apply/dry-run/restore the stopAfterLoad patch.")
    ap.add_argument("--project", required=True, help="pipeline project name (state machine / "
                    "Lambda name prefix); no default (public repo)")
    ap.add_argument("--region", required=True, help="AWS region")
    ap.add_argument("--bucket", required=True, help="pipeline bucket (backups go to "
                    "s3://<bucket>/scripts/backup/startup_stop_after_load/<ts>/)")
    ap.add_argument("--account", default="", help="AWS account id (for building state-machine "
                    "ARNs; optional if describe-by-name works)")
    ap.add_argument("--apply", action="store_true", help="perform the patch (default is dry run)")
    ap.add_argument("--restore", metavar="BACKUP_DIR", default="",
                    help="restore originals from a local backup dir and exit")
    ap.add_argument("--local-backup-dir", default=os.path.join(HERE, "_backups"),
                    help="local dir to also save backups into")
    args = ap.parse_args(argv)

    if aws is None:
        aws = Aws.real(args.region)

    if args.restore:
        res = restore(aws, args.project, args.account, args.restore)
        for name, what in res:
            print(f"[{what}] {name}")
        print("Restore complete.")
        return 0

    plan = build_plan(aws, args.project, args.account)

    if not args.apply:
        _print_dry_run(plan)
        return 0

    local_dir, s3_prefix = save_backups(aws, args.bucket, plan, args.local_backup_dir)
    print(f"Backups saved locally to {local_dir} and to s3://{args.bucket}/{s3_prefix}")
    res = apply_plan(aws, args.project, args.account, plan)
    for name, what in res:
        print(f"[{what}] {name}")
    print("Apply complete (each updated resource was read back and compared).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
