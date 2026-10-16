#!/usr/bin/env python3
"""IAM additions lint (brief section 10): valid JSON; every policy Statement has Effect/Action/
Resource and no stray _comment key (a _comment inside a statement once made IAM reject a
policy); the only keys outside statements are the top-level _README and per-role _attachTo
markers; placeholders are drawn from the allowed set only (no customer names / real ids)."""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
IAM = os.path.join(PKG, "iam", "mrl-additions.json")

_ALLOWED_PLACEHOLDERS = {"<account-id>", "<region>", "<project>", "<bucket>", "<dsql-cluster-id>"}
_POLICY_META = {"Version", "Statement"}
_STATEMENT_KEYS = {"Sid", "Effect", "Action", "Resource", "Condition", "NotAction",
                   "NotResource", "Principal"}


def test_valid_json_and_structure():
    doc = json.load(open(IAM))
    for role_key, role in doc.items():
        if role_key.startswith("_"):
            continue
        assert "_attachTo" in role, "%s missing _attachTo" % role_key
        assert set(role) - {"_attachTo"} <= _POLICY_META, \
            "%s has keys outside _attachTo/Version/Statement" % role_key
        assert role.get("Version") == "2012-10-17"
        for st in role["Statement"]:
            # NO _comment (or any _-prefixed) key inside a statement
            bad = [k for k in st if k.startswith("_")]
            assert not bad, "%s statement has stray key(s) %s" % (role_key, bad)
            assert set(st) <= _STATEMENT_KEYS, \
                "%s statement has unexpected key(s) %s" % (role_key, set(st) - _STATEMENT_KEYS)
            assert st.get("Effect") in ("Allow", "Deny")
            assert "Action" in st and "Resource" in st
            assert st.get("Sid"), "every statement should carry a Sid"


def test_only_allowed_placeholders():
    raw = open(IAM).read()
    found = set(re.findall(r"<[a-z0-9-]+>", raw))
    extra = found - _ALLOWED_PLACEHOLDERS
    assert not extra, "unexpected placeholder(s): %s" % extra


def test_no_customer_names_or_real_ids():
    raw = open(IAM).read()
    # no 12-digit literal account id (only the <account-id> placeholder)
    assert not re.search(r"\b\d{12}\b", raw), "a literal 12-digit account id leaked into IAM"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d IAM lint test(s) passed" % len(fns))
