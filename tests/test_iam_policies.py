#!/usr/bin/env python3
"""B22 static test: the shipped iam/*.json carry ONLY valid IAM keys, so put-role-policy /
update-assume-role-policy can never fail with MalformedPolicyDocument. Also checks the setup.sh
and docs/MANUAL_SETUP.md fill-in steps strip unknown keys as a backstop.

IAM accepts only:
  document level : Version, Id, Statement
  statement level: Sid, Effect, Action, NotAction, Resource, NotResource, Principal,
                   NotPrincipal, Condition

Offline, no AWS. Run: python3 tests/test_iam_policies.py
"""
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("REPO_DIR") or os.path.dirname(HERE)

IAM_FILES = ["iam/glue.json", "iam/lambda.json", "iam/stepfunctions.json"]
POLICY_KEYS = ("TrustPolicy", "Policy", "VpcPolicy")
WRAPPER_KEYS = {"RoleName", "TrustPolicy", "Policy", "VpcPolicy"}
DOC_OK = {"Version", "Id", "Statement"}
STMT_OK = {"Sid", "Effect", "Action", "NotAction", "Resource", "NotResource",
           "Principal", "NotPrincipal", "Condition"}

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
        # Under pytest the direct-run main()/sys.exit path never runs, so a failed check must
        # surface as a real test failure (otherwise pytest reports PASS even when assertions
        # fail). Direct `python3 tests/<f>.py` runs are unaffected (PYTEST_CURRENT_TEST unset),
        # so the count-and-continue summary still works.
        import os as _os
        if "PYTEST_CURRENT_TEST" in _os.environ:
            raise AssertionError(msg)


def test_each_iam_file_is_valid_and_clean():
    for rel in IAM_FILES:
        p = os.path.join(REPO, rel)
        check(os.path.exists(p), f"{rel}: present")
        with open(p, encoding="utf-8") as fh:
            doc = json.load(fh)                       # must be valid JSON
        base = os.path.basename(rel)
        # Wrapper object: RoleName + one or more policy blocks, nothing else (no _comment).
        extra = [k for k in doc if k not in WRAPPER_KEYS]
        check(not extra, f"{base}: wrapper has no extra keys (got extras {extra})")
        check("_comment" not in doc, f"{base}: no top-level _comment key")
        check("RoleName" in doc and isinstance(doc["RoleName"], str),
              f"{base}: has a RoleName")
        # Every policy document: only Version/Id/Statement; every Statement: only IAM keys.
        present = [k for k in POLICY_KEYS if k in doc]
        check(present, f"{base}: has at least one policy block ({present})")
        for pk in present:
            pol = doc[pk]
            check(isinstance(pol, dict), f"{base}.{pk}: is an object")
            bad_doc = [k for k in pol if k not in DOC_OK]
            check(not bad_doc,
                  f"{base}.{pk}: only Version/Id/Statement at document level (bad {bad_doc})")
            stmts = pol.get("Statement", [])
            if isinstance(stmts, dict):
                stmts = [stmts]
            check(isinstance(stmts, list) and stmts,
                  f"{base}.{pk}: has a non-empty Statement list")
            for i, st in enumerate(stmts):
                check(isinstance(st, dict), f"{base}.{pk}.Statement[{i}]: is an object")
                bad = [k for k in st if k not in STMT_OK]
                check(not bad,
                      f"{base}.{pk}.Statement[{i}] (Sid={st.get('Sid')!r}): "
                      f"only valid IAM keys (bad {bad})")
                check("_comment" not in st,
                      f"{base}.{pk}.Statement[{i}]: no _comment inside the statement")
                check(st.get("Effect") in ("Allow", "Deny"),
                      f"{base}.{pk}.Statement[{i}]: Effect is Allow/Deny")


def test_no_comment_anywhere_in_iam_files():
    """Belt and braces: the raw text of each iam/*.json contains no "_comment" token."""
    for rel in IAM_FILES:
        txt = open(os.path.join(REPO, rel), encoding="utf-8").read()
        check("_comment" not in txt,
              f"{os.path.basename(rel)}: raw file has no _comment token")


def _apply_strip_from(source_text, marker):
    """Extract the strip_policy helper defined in a shipped file's embedded python block and
    return the callable, so we test the SHIPPED backstop (not a copy). Line-based: take from the
    '_DOC_OK = {' marker through the end of the strip_policy def body (first line that dedents to
    column 0 and is not part of the def), then exec it."""
    import textwrap
    lines = source_text.splitlines()
    start = next(i for i, ln in enumerate(lines) if marker in ln)
    block = [lines[start]]
    seen_def = False
    for ln in lines[start + 1:]:
        stripped = ln.strip()
        is_top = ln and not ln[0].isspace()
        if seen_def and is_top and not stripped.startswith(("def strip_policy", "_STMT_OK",
                                                             "_DOC_OK")):
            break
        if stripped.startswith("def strip_policy"):
            seen_def = True
        block.append(ln)
    src = textwrap.dedent("\n".join(block))
    ns = {}
    exec(compile(src, "<strip_policy>", "exec"), ns)
    return ns["strip_policy"]


def test_setup_sh_strips_unknown_keys():
    """The setup.sh split step defines strip_policy; it must drop a stray _comment."""
    txt = open(os.path.join(REPO, "tools/setup.sh"), encoding="utf-8").read()
    check("def strip_policy(" in txt, "setup.sh: defines a strip_policy backstop")
    strip = _apply_strip_from(txt, "_DOC_OK = {")
    dirty = {"Version": "2012-10-17", "_comment": "doc note",
             "Statement": [{"Sid": "A", "Effect": "Allow", "Action": ["s3:GetObject"],
                            "Resource": ["*"], "_comment": "stmt note"}]}
    clean = strip(dirty)
    check("_comment" not in clean, "setup.sh strip_policy: drops a document-level _comment")
    check(all("_comment" not in st for st in clean["Statement"]),
          "setup.sh strip_policy: drops a statement-level _comment")
    check(clean["Statement"][0]["Action"] == ["s3:GetObject"],
          "setup.sh strip_policy: keeps the valid IAM keys intact")


def test_manual_setup_strips_unknown_keys():
    txt = open(os.path.join(REPO, "docs/MANUAL_SETUP.md"), encoding="utf-8").read()
    check("def strip_policy(" in txt, "MANUAL_SETUP.md: fill-in step defines a strip_policy backstop")
    strip = _apply_strip_from(txt, "_DOC_OK = {")
    dirty = {"Version": "2012-10-17",
             "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                            "Principal": {"Service": "glue.amazonaws.com"},
                            "_comment": "x"}]}
    clean = strip(dirty)
    check(all("_comment" not in st for st in clean["Statement"]),
          "MANUAL_SETUP strip_policy: drops a statement-level _comment")
    check(clean["Statement"][0]["Principal"] == {"Service": "glue.amazonaws.com"},
          "MANUAL_SETUP strip_policy: keeps Principal intact")


def main():
    for fn in sorted(g for g in globals() if g.startswith("test_")):
        try:
            globals()[fn]()
        except Exception as e:
            global _failed
            _failed += 1
            print(f"[FAIL] {fn} raised {type(e).__name__}: {e}")
    print(f"\n==== iam-policies: {_passed} passed, {_failed} failed ====")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
