#!/usr/bin/env python3
"""NULL option parsing (copy-sync): the MRL preflight validates null_values / null_rules with
resolve_task._validate_null_settings - the SAME parser the Glue scripts' shared null-rules block
uses - so a malformed setting FAILS AT PREFLIGHT naming the bad entry (not hours later in the
load). These tests assert the preflight rejects the malformed cases and accepts the valid ones,
and that it is literally resolve_task's parser (so there is no drift)."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import mrl_preflight as pf  # noqa: E402
import resolve_task as rt   # noqa: E402


def _check(null_values="", null_rules=""):
    cfg = {"null_values": null_values, "null_rules": null_rules}
    rt._validate_null_settings(cfg)
    return cfg


def test_preflight_uses_resolve_task_parser():
    # The preflight module imports resolve_task and calls rt._validate_null_settings directly;
    # confirm the symbol is the exact same function object (no fork / copy drift).
    assert pf.rt._validate_null_settings is rt._validate_null_settings


def test_valid_null_options_accepted():
    _check(null_values="NULL|NA", null_rules="s.t.c=none;s.t.d=V1|V2")
    _check()  # blank = today's behaviour


def test_malformed_null_rules_rejected():
    for bad in ("s.t", "s.t.c", "s.t.c=", "s..c=none", "=none", "s.t.c=a||b"):
        try:
            _check(null_rules=bad)
            assert False, "should have rejected %r" % bad
        except rt.SettingsError:
            pass


def test_malformed_null_values_rejected():
    try:
        _check(null_values="A||B")      # empty marker between separators
        assert False
    except rt.SettingsError:
        pass


def test_duplicate_null_rule_column_rejected():
    try:
        _check(null_rules="s.t.c=none;s.t.c=V1")
        assert False
    except rt.SettingsError:
        pass


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d null-option test(s) passed" % len(fns))
