#!/usr/bin/env python3
"""Amendment 12 - validate EVERY `aws <service> <operation>` command tools/deploy_mrl.sh can emit
against the REAL botocore model, so an invalid CLI flag (the --connections-list exit-252 defect)
is caught offline. Also scans the docs' aws command lines with the same validator.

For each command line:
  - map the CLI operation (kebab) to the botocore OperationModel,
  - derive the valid CLI flags = kebab(member) for every input-shape member + the CLI's own
    global/extra flags,
  - assert (a) every --flag used exists, (b) every required member is present, (c) JSON/structure
    flags parse as JSON and their top-level keys exist in the member's shape.
Mutation: re-introducing `--connections-list` makes the test fail.
"""
import json
import os
import re
import shlex
import subprocess
import sys

import pytest

try:
    import botocore.session
    _SESSION = botocore.session.get_session()
except Exception:  # pragma: no cover
    _SESSION = None

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
DEPLOY = os.path.join(PKG, "tools", "deploy_mrl.sh")

# CLI global flags + per-verb extras the CLI accepts beyond the shape members.
_GLOBAL_FLAGS = {"--region", "--output", "--profile", "--cli-input-json", "--no-cli-pager",
                 "--endpoint-url", "--query", "--debug", "--color", "--no-paginate"}
# A few CLI customizations expose flags that are not literal input-shape members.
_CLI_EXTRA = {
    ("lambda", "create-function"): {"--zip-file", "--code"},
    ("lambda", "update-function-code"): {"--zip-file"},
    ("glue", "create-job"): set(),
    ("glue", "update-job"): set(),
    ("s3", "cp"): None,          # s3 (not s3api) is a custom command: skip shape validation
    ("stepfunctions", "create-state-machine"): set(),
    ("stepfunctions", "update-state-machine"): set(),
}
# JSON/structure-valued flags whose top-level keys we check against the member shape.
_SERVICE_OP_RE = re.compile(r"^aws\s+(\S+)\s+(\S+)\s+(.*)$", re.DOTALL)


def _kebab(name):
    # botocore member name (PascalCase) -> CLI flag (kebab). e.g. JobName -> --job-name
    s = re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()
    return "--" + s


def _op_model(service, op_kebab):
    model = _SESSION.get_service_model(service)
    op_name = "".join(p.capitalize() for p in op_kebab.split("-"))
    return model.operation_model(op_name)


def _valid_flags(service, op_kebab):
    om = _op_model(service, op_kebab)
    members = (om.input_shape.members if om.input_shape is not None else {})
    flags = {_kebab(m) for m in members}
    flags |= _GLOBAL_FLAGS
    extra = _CLI_EXTRA.get((service, op_kebab))
    if extra:
        flags |= extra
    return flags, members, om


def _flag_to_member(service, op_kebab, flag):
    _f, members, _om = _valid_flags(service, op_kebab)
    for m in members:
        if _kebab(m) == flag:
            return m, members[m]
    return None, None


def _tokenize(rest):
    """Split a command tail into tokens, keeping a JSON {..} or [..] value (with any quoting)
    as ONE token. shlex cannot be used: the echoed deploy command embeds raw JSON with quotes
    and no shell escaping."""
    toks = []
    i, n = 0, len(rest)
    while i < n:
        while i < n and rest[i].isspace():
            i += 1
        if i >= n:
            break
        if rest[i] in "{[":
            open_c, close_c = rest[i], ("}" if rest[i] == "{" else "]")
            depth, j = 0, i
            while j < n:
                if rest[j] == open_c:
                    depth += 1
                elif rest[j] == close_c:
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
            toks.append(rest[i:j])
            i = j
        else:
            j = i
            while j < n and not rest[j].isspace():
                j += 1
            toks.append(rest[i:j])
            i = j
    return toks


def _parse_cmd(line):
    """Return (service, op, {flag: value}) from an `aws ...` line, or None."""
    m = _SERVICE_OP_RE.match(line.strip())
    if not m:
        return None
    service, op, rest = m.group(1), m.group(2), m.group(3)
    toks = _tokenize(rest)
    flags = {}
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.startswith("--"):
            if i + 1 < len(toks) and not toks[i + 1].startswith("--"):
                flags[t] = toks[i + 1]
                i += 2
            else:
                flags[t] = True
                i += 1
        else:
            i += 1
    return service, op, flags


def _validate(line):
    """Validate one aws command line against botocore. Returns [] or a list of problem strings."""
    parsed = _parse_cmd(line)
    if not parsed:
        return []
    service, op, flags = parsed
    if service in ("sts",):                  # read-only identity call, no shape to police here
        return []
    if service == "s3":                      # custom high-level commands (cp/ls/rm/sync) - skip
        return []
    if _CLI_EXTRA.get((service, op), "x") is None:   # custom command - skip
        return []
    problems = []
    try:
        valid, members, om = _valid_flags(service, op)
    except Exception as e:
        return ["unknown operation %s %s (%s)" % (service, op, e)]
    # (a) every flag used must exist
    for f in flags:
        if f not in valid:
            problems.append("invalid flag %s for `aws %s %s`" % (f, service, op))
    # (b) required members present (honouring CLI shorthand aliases, e.g. lambda create-function
    #     Code is provided via --zip-file / --code / --image-uri).
    _REQ_ALIASES = {
        ("lambda", "create-function"): {"Code": {"--zip-file", "--code", "--image-uri"}},
    }
    aliases = _REQ_ALIASES.get((service, op), {})
    for req in (om.input_shape.required_members if om.input_shape is not None else []):
        if _kebab(req) in flags or "--cli-input-json" in flags:
            continue
        if any(a in flags for a in aliases.get(req, set())):
            continue
        problems.append("missing required %s (%s) for `aws %s %s`"
                        % (_kebab(req), req, service, op))
    # (c) JSON/structure flags parse and their top-level keys exist in the member shape
    for f, v in flags.items():
        if v is True or f in _GLOBAL_FLAGS:
            continue
        member, shape = _flag_to_member(service, op, f)
        if shape is None:
            continue
        if getattr(shape, "type_name", "") in ("structure", "map") and v.strip().startswith("{"):
            try:
                doc = json.loads(v)
            except ValueError:
                problems.append("flag %s value is not valid JSON: %r" % (f, v[:60]))
                continue
            if shape.type_name == "structure" and isinstance(doc, dict):
                allowed = set(shape.members)
                for k in doc:
                    if k not in allowed:
                        problems.append("flag %s has key %r not in %s shape %s"
                                        % (f, k, member, sorted(allowed)[:8]))
    return problems


def _dry_run_lines(with_conn):
    args = ["bash", DEPLOY, "--dry-run", "--bucket", "b", "--account-id", "123456789012",
            "--region", "us-east-1", "--project", "demo",
            "--glue-role-arn", "arn:aws:iam::123456789012:role/demo-glue-exec-role",
            "--sfn-role-arn", "arn:aws:iam::123456789012:role/demo-sfn-exec-role",
            "--lambda-role-arn", "arn:aws:iam::123456789012:role/demo-lambda-exec-role",
            "--dsql-endpoint", "c.dsql.us-east-1.on.aws", "--dsql-user", "admin",
            "--dsql-database", "postgres"]
    if with_conn:
        args += ["--glue-connection", "gconn"]
    out = subprocess.run(args, capture_output=True, text=True)
    lines = []
    for ln in (out.stdout + "\n" + out.stderr).splitlines():
        ln = ln.strip()
        if ln.startswith("+ aws ") or ln.startswith("aws "):
            lines.append(ln[2:].strip() if ln.startswith("+ ") else ln)
    return lines


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_deploy_commands_valid_botocore_flags_both_branches():
    seen_conn_create = False
    for with_conn in (False, True):
        lines = _dry_run_lines(with_conn)
        assert any("glue create-job" in l for l in lines), "dry-run must print glue create-job"
        assert any("glue update-job" in l for l in lines), "dry-run must print glue update-job"
        all_problems = []
        for l in lines:
            all_problems += ["%s :: %s" % (p, l[:90]) for p in _validate(l)]
        assert not all_problems, "invalid aws CLI usage:\n  - " + "\n  - ".join(all_problems)
        if with_conn:
            # the connection flag must be the VALID --connections (NOT --connections-list)
            cj = [l for l in lines if "glue create-job" in l][0]
            assert "--connections-list" not in cj, "the exit-252 defect is back"
            assert "--connections " in cj and '"Connections"' in cj
            seen_conn_create = True
    assert seen_conn_create


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_mutation_connections_list_is_rejected():
    """If any command used the invalid --connections-list, the validator flags it."""
    bad = ('aws glue create-job --region us-east-1 --name demo-mrl-count '
           '--role arn:aws:iam::123456789012:role/r '
           '--command {"Name":"pythonshell","ScriptLocation":"s3://b/x.py"} '
           '--connections-list gconn')
    problems = _validate(bad)
    assert any("--connections-list" in p for p in problems), problems
    # and the VALID form passes
    good = bad.replace("--connections-list gconn", '--connections {"Connections":["gconn"]}')
    assert not [p for p in _validate(good) if "--connections" in p], _validate(good)


@pytest.mark.skipif(_SESSION is None, reason="botocore not available")
def test_docs_aws_commands_valid():
    """Scan RUNBOOK/LIVE_TEST_STEPS/INTEGRATION for `aws ...` lines and validate the service-op
    ones (placeholders substituted with test values)."""
    subs = {"<region>": "us-east-1", "<account-id>": "123456789012", "<bucket>": "b",
            "<project>": "demo", "<task-suffix>": "t", "BUCKET": "b", "REGION": "us-east-1",
            "PROJECT": "demo", "$BUCKET": "b", "$REGION": "us-east-1", "$PROJECT": "demo"}
    problems = []
    for doc in ("RUNBOOK.md", "LIVE_TEST_STEPS.md", "INTEGRATION.md"):
        p = os.path.join(PKG, doc)
        if not os.path.exists(p):
            continue
        # Join backslash-continued shell lines into one logical command first.
        raw_text = open(p).read().replace("\\\n", " ")
        for raw in raw_text.splitlines():
            s = raw.strip().lstrip("$").strip()
            if not s.startswith("aws "):
                continue
            for a, b in subs.items():
                s = s.replace(a, b)
            if "<" in s or "`" in s:        # still has a placeholder/backtick fragment - skip
                continue
            problems += ["%s :: %s (%s)" % (pr, s[:90], doc) for pr in _validate(s)]
    assert not problems, "invalid aws CLI usage in docs:\n  - " + "\n  - ".join(problems)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        try:
            fn()
            print("[PASS]", fn.__name__)
        except Exception as e:
            print("[FAIL]", fn.__name__, e)
