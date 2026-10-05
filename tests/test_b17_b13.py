#!/usr/bin/env python3
"""Offline tests for B17 (DSQL ADD COLUMN ... DEFAULT control-table init) and B13 (invalid
IAM action in iam/*.json). No AWS, no Spark, no network.

B17  scripts/glue_cdc_continuous.py and scripts/glue_cdc_composite.py used
       `ALTER TABLE cdc_control.cdc_validation_failures ADD COLUMN IF NOT EXISTS resolved
       boolean DEFAULT false`. DSQL rejects ADD COLUMN ... DEFAULT at PARSE time (0A000), even
       when the column already exists, so CDC startup failed on BOTH engines. Fix: put
       `resolved boolean` in the CREATE TABLE; for an older table missing it, check
       information_schema.columns and only then ADD COLUMN resolved boolean (NO DEFAULT) +
       backfill NULL->false in batches < 3000 rows; readers use `resolved IS NOT TRUE`.

  B13  iam/lambda.json carried `dsql:GetVpcEndpointServiceName`, which IAM rejects at
       put-role-policy time (MalformedPolicyDocument), failing the whole lambda role policy and
       blocking deploy on the manage_iam=true path. Fix: drop that statement (the runtime
       degrades gracefully; the private-endpoint connect uses dsql:DbConnectAdmin).

Static guards added here:
  - NO `ADD COLUMN` statement anywhere in scripts/ or lambdas/ may contain DEFAULT.
  - EVERY IAM action in iam/*.json must match a known-good <service>:<Action> pattern list.

Fake-DB test covers three control-schema states: fresh (column present via CREATE TABLE),
pre-existing table WITHOUT the column (upgrade path), pre-existing table WITH the column.

Run: python3 tests/test_b17_b13.py   (REPO_DIR overridable)
"""
import ast
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fix6_harness as H  # noqa: E402

REPO = H.REPO

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


def _iter_source_files():
    """Yield (relpath, text) for every .py under scripts/ and lambdas/."""
    for sub in ("scripts", "lambdas"):
        base = os.path.join(REPO, sub)
        for name in sorted(os.listdir(base)):
            if name.endswith(".py"):
                p = os.path.join(base, name)
                with open(p, "r") as fh:
                    yield os.path.join(sub, name), fh.read()


# =============================================================================================
# B17 static guard — no `ADD COLUMN ... DEFAULT` anywhere in scripts/ or lambdas/
# =============================================================================================
# Match an ADD COLUMN clause followed by DEFAULT within the same SQL statement string.
_ADD_COL_DEFAULT = re.compile(r"add\s+column\b[^;]*\bdefault\b", re.IGNORECASE)


def _docstring_nodes(tree):
    """Collect id() of every expression-statement string that is a docstring (module / def /
    class / anywhere a bare string-expression appears), so we can exclude pure prose."""
    doc_ids = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list):
            for stmt in body:
                if (isinstance(stmt, ast.Expr)
                        and isinstance(stmt.value, ast.Constant)
                        and isinstance(stmt.value.value, str)):
                    doc_ids.add(id(stmt.value))
    return doc_ids


def _sql_string_literals(text):
    """Yield the text of every string literal in the file that is NOT a bare docstring/comment,
    reconstructing f-strings (JoinedStr) by concatenating their literal parts (placeholders ->
    a single space). This targets the SQL actually handed to the driver, not prose."""
    tree = ast.parse(text)
    doc_ids = _docstring_nodes(tree)
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in doc_ids:
                continue
            out.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    parts.append(v.value)
                else:
                    parts.append(" ")  # placeholder expression
            out.append("".join(parts))
    return out


def test_b17_no_add_column_default_static():
    violations = []
    for rel, text in _iter_source_files():
        for lit in _sql_string_literals(text):
            flat = " ".join(lit.split())
            if _ADD_COL_DEFAULT.search(flat):
                violations.append(f"{rel}: ...{flat[:90]}...")
    check(not violations,
          "B17 static: no `ADD COLUMN ... DEFAULT` SQL in scripts/ or lambdas/"
          + ("" if not violations else "  VIOLATIONS:\n   " + "\n   ".join(violations)))


def test_b17_sentinel_catches_a_default():
    """The guard regex must actually fire on a known-bad statement (so a future reintroduction
    is caught, not silently missed by an over-narrow pattern)."""
    bad = "ALTER TABLE x ADD COLUMN IF NOT EXISTS resolved boolean DEFAULT false"
    check(bool(_ADD_COL_DEFAULT.search(bad)),
          "B17 static: the guard regex fires on an ADD COLUMN ... DEFAULT sentinel")
    good = "ALTER TABLE x ADD COLUMN resolved boolean"
    check(not _ADD_COL_DEFAULT.search(good),
          "B17 static: the guard regex does NOT fire on ADD COLUMN with no DEFAULT")


def test_b17_create_table_has_resolved():
    """Both CDC scripts must declare `resolved boolean` inside the cdc_validation_failures
    CREATE TABLE (so a fresh schema never needs an ALTER)."""
    pat = re.compile(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\{CONTROL_SCHEMA\}\.cdc_validation_failures\s*\((.*?)\"\"\"",
        re.IGNORECASE | re.DOTALL)
    for rel in ("scripts/glue_cdc_continuous.py", "scripts/glue_cdc_composite.py"):
        text = H.read_source(rel)
        m = pat.search(text)
        check(m is not None, f"B17: {rel} has a cdc_validation_failures CREATE TABLE")
        if m:
            check(re.search(r"\bresolved\s+boolean", m.group(1)) is not None,
                  f"B17: {rel} CREATE TABLE declares `resolved boolean`")


# =============================================================================================
# B17 fake-DB test — three control-schema states, exercising the SHIPPED _ensure_resolved_column
# =============================================================================================
DSQL_ROW_CAP = 3000


class _DsqlTxnTooLarge(Exception):
    def __init__(self, n):
        super().__init__({"C": "54000", "M": f"transaction row limit exceeded ({n})"})


class _FakeCur:
    """A tiny fake DSQL cursor modelling just what _ensure_resolved_column needs:
      - information_schema.columns probe (does 'resolved' exist?)
      - ALTER TABLE ... ADD COLUMN resolved boolean   (adds col; rejects any DEFAULT)
      - UPDATE ... SET resolved=false WHERE id IN (SELECT ... WHERE resolved IS NULL LIMIT n)
        enforcing the ~3000-row/txn cap exactly like real DSQL.
    `conn.rows` is a list of dicts: {"id": i, "resolved": None/False/True}."""

    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self._result = None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        low = s.lower()
        self.conn.executed.append(s)
        if "information_schema.columns" in low:
            present = self.conn.has_resolved_col
            self._result = [(1,)] if present else []
            self.rowcount = len(self._result)
        elif low.startswith("alter table") and "add column" in low:
            if "default" in low:
                # real DSQL rejects this at parse time (0A000) — mirror that so a regression trips
                raise Exception({"C": "0A000",
                                 "M": "ALTER TABLE ADD COLUMN with constraint not supported"})
            self.conn.has_resolved_col = True
            self._result = []
            self.rowcount = 0
        elif low.startswith("update") and "set resolved = false" in low:
            m = re.search(r"limit\s+(\d+)", low)
            lim = int(m.group(1)) if m else len(self.conn.rows)
            null_rows = [r for r in self.conn.rows if r["resolved"] is None]
            doomed = null_rows[:lim]
            if len(doomed) > DSQL_ROW_CAP:
                raise _DsqlTxnTooLarge(len(doomed))
            for r in doomed:
                r["resolved"] = False
            self.rowcount = len(doomed)
            self._result = []
        else:
            self._result = []
            self.rowcount = 0

    def fetchone(self):
        return (self._result or [None])[0]

    def fetchall(self):
        return self._result or []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows, has_resolved_col):
        self.rows = [dict(r) for r in rows]
        self.has_resolved_col = has_resolved_col
        self.executed = []

    def cursor(self):
        return _FakeCur(self)


def _load_ensure(script_rel):
    ns = H.load_defs(script_rel, ["_ensure_resolved_column"],
                     {"CONTROL_SCHEMA": "cdc_control",
                      "print": lambda *a, **k: None})
    return ns["_ensure_resolved_column"]


def _run_three_cases(script_rel):
    fn = _load_ensure(script_rel)

    # Case 1 — FRESH schema: CREATE TABLE already made 'resolved'; no rows. No ALTER expected.
    conn = _FakeConn([], has_resolved_col=True)
    fn(conn.cursor())
    altered = any("add column" in e.lower() for e in conn.executed)
    check(not altered, f"B17 fakedb[{script_rel}]: fresh schema -> no ALTER (column already there)")

    # Case 2 — PRE-EXISTING table WITHOUT the column, with > cap rows that must be backfilled in
    # batches each <= 3000. 7005 rows -> at least 3 batches of 2000; none may exceed the cap.
    rows = [{"id": i, "resolved": None} for i in range(7005)]
    conn = _FakeConn(rows, has_resolved_col=False)
    fn(conn.cursor())
    adds = [e for e in conn.executed if "add column" in e.lower()]
    check(len(adds) == 1 and "default" not in adds[0].lower(),
          f"B17 fakedb[{script_rel}]: missing column -> exactly one ADD COLUMN, NO DEFAULT")
    updates = [e for e in conn.executed if e.lower().startswith("update")]
    check(len(updates) >= 2,
          f"B17 fakedb[{script_rel}]: backfill ran in multiple batches ({len(updates)})")
    # every UPDATE used a LIMIT under the DSQL cap
    lims = [int(re.search(r"limit\s+(\d+)", e, re.I).group(1)) for e in updates]
    check(lims and all(l < DSQL_ROW_CAP for l in lims),
          f"B17 fakedb[{script_rel}]: every backfill batch LIMIT < {DSQL_ROW_CAP} (max {max(lims)})")
    check(all(r["resolved"] is False for r in conn.rows),
          f"B17 fakedb[{script_rel}]: all pre-existing NULL rows backfilled to false")

    # Case 3 — PRE-EXISTING table WITH the column already: idempotent no-op (no ALTER, no UPDATE).
    rows = [{"id": i, "resolved": False} for i in range(10)]
    conn = _FakeConn(rows, has_resolved_col=True)
    fn(conn.cursor())
    touched = any(("add column" in e.lower()) or e.lower().startswith("update")
                  for e in conn.executed)
    check(not touched,
          f"B17 fakedb[{script_rel}]: column already present -> no ALTER and no backfill (idempotent)")


def test_b17_fakedb_continuous():
    _run_three_cases("scripts/glue_cdc_continuous.py")


def test_b17_fakedb_composite():
    _run_three_cases("scripts/glue_cdc_composite.py")


# =============================================================================================
# B13 static guard — every IAM action in iam/*.json matches a known-good <service>:<Action>
# =============================================================================================
# Known-good shape: a service prefix (lowercase alnum, may contain '-'), a colon, then an action
# that is PascalCase/alnum or a single trailing '*' wildcard segment (e.g. "s3:GetObject",
# "logs:PutLogEvents", "dsql:DbConnectAdmin"). A bare "*" is not allowed. This catches an
# unknown/typo'd action name like "dsql:GetVpcEndpointServiceName" ONLY if it is not in the
# explicit allow-list below; to make the guard robust against IAM registry lag we both (a)
# require the lexical shape and (b) pin an allow-list of the exact actions this repo relies on.
_ACTION_SHAPE = re.compile(r"^[a-z][a-z0-9-]*:[A-Za-z0-9]+\*?$")

# Exact actions the pipeline's IAM policies are allowed to use. Kept tight on purpose: adding a
# new action is a deliberate edit here, which forces a human to confirm it is a real action.
_ALLOWED_ACTIONS = {
    # logs
    "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
    # s3
    "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation",
    # dms
    "dms:DescribeReplicationTasks", "dms:DescribeEndpoints", "dms:DescribeTableStatistics",
    "dms:StartReplicationTask", "dms:StopReplicationTask", "dms:ModifyReplicationTask",
    "dms:TestConnection", "dms:DescribeConnections", "dms:DescribeReplicationInstances",
    # dsql (connect only; GetVpcEndpointServiceName intentionally NOT granted — see B13)
    "dsql:DbConnect", "dsql:DbConnectAdmin",
    # glue
    "glue:CreateJob", "glue:UpdateJob", "glue:DeleteJob", "glue:GetJob", "glue:GetJobs",
    "glue:ListJobs", "glue:GetJobRun", "glue:GetJobRuns", "glue:StartJobRun",
    "glue:BatchStopJobRun", "glue:TagResource", "glue:GetTags", "glue:GetConnection",
    "glue:CreateConnection",
    # states (step functions)
    "states:ListExecutions", "states:DescribeExecution", "states:StartExecution",
    "states:DescribeStateMachine",
    # iam
    "iam:PassRole",
    # lambda (step functions invokes task lambdas)
    "lambda:InvokeFunction",
    # cloudwatch metrics (glue jobs emit custom metrics)
    "cloudwatch:PutMetricData",
    # eventbridge (scheduling / rules used by the pipeline)
    "events:PutRule", "events:PutTargets", "events:DescribeRule", "events:DeleteRule",
    "events:RemoveTargets",
    # ec2 (VPC/connection setup)
    "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeVpcs",
    "ec2:AuthorizeSecurityGroupIngress", "ec2:CreateNetworkInterface",
    "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface",
}


def _actions_in_policy(doc):
    out = []
    pol = doc.get("Policy", doc)
    for st in pol.get("Statement", []):
        act = st.get("Action", [])
        if isinstance(act, str):
            act = [act]
        out.extend(act)
    return out


def test_b13_iam_actions_known_good():
    iam_dir = os.path.join(REPO, "iam")
    bad_shape = []
    not_allowed = []
    total = 0
    for name in sorted(os.listdir(iam_dir)):
        if not name.endswith(".json"):
            continue
        doc = json.load(open(os.path.join(iam_dir, name)))
        for act in _actions_in_policy(doc):
            total += 1
            if not _ACTION_SHAPE.match(act):
                bad_shape.append(f"{name}: {act}")
            elif act not in _ALLOWED_ACTIONS:
                not_allowed.append(f"{name}: {act}")
    check(total > 0, "B13 static: found IAM actions to validate")
    check(not bad_shape,
          "B13 static: every IAM action matches the <service>:<Action> shape"
          + ("" if not bad_shape else "  BAD:\n   " + "\n   ".join(bad_shape)))
    check(not not_allowed,
          "B13 static: every IAM action is in the known-good allow-list"
          + ("" if not not_allowed else "  UNKNOWN:\n   " + "\n   ".join(not_allowed)))


def test_b13_invalid_action_removed():
    """The specific action IAM rejected (B13) must not be present in any iam/*.json."""
    iam_dir = os.path.join(REPO, "iam")
    present = []
    for name in sorted(os.listdir(iam_dir)):
        if name.endswith(".json"):
            if "GetVpcEndpointServiceName" in open(os.path.join(iam_dir, name)).read():
                present.append(name)
    check(not present,
          "B13: dsql:GetVpcEndpointServiceName (IAM-rejected) is gone from iam/*.json"
          + ("" if not present else f"  STILL IN: {present}"))


def test_b13_guard_catches_the_regression():
    """The allow-list guard must reject the exact action B13 was about, so re-adding it trips."""
    check("dsql:GetVpcEndpointServiceName" not in _ALLOWED_ACTIONS
          and bool(_ACTION_SHAPE.match("dsql:GetVpcEndpointServiceName")),
          "B13 static: the guard would flag dsql:GetVpcEndpointServiceName as not-allowed")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        globals()[fn]()
    print(f"\n==== b17/b13: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
