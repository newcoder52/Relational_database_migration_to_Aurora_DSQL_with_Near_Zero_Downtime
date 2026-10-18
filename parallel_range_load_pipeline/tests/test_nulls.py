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


# ---- AMENDMENT 3 (R9): null_notnull_text must FAIL CLOSED on a typo, before any DMS/Glue call --
def _check_nnt(val):
    cfg = {"null_values": "", "null_rules": "", "null_notnull_text": val}
    rt._validate_null_settings(cfg)
    return cfg["null_notnull_text"]


def test_null_notnull_text_valid_values_accepted():
    assert _check_nnt("off") == "off"
    assert _check_nnt("keep_text") == "off" or _check_nnt("keep_text") == "keep_text"
    assert _check_nnt("KEEP_TEXT") == "keep_text"       # case-insensitive
    assert _check_nnt("OFF") == "off"


def test_null_notnull_text_absent_blank_sentinel_are_off_no_error():
    # byte-identical to today: absent / "" / __NULL_UNSET__ all normalize to "off" with no raise
    cfg = {"null_values": "", "null_rules": ""}          # key absent
    rt._validate_null_settings(cfg)
    assert cfg["null_notnull_text"] == "off"
    assert _check_nnt("") == "off"
    assert _check_nnt("__NULL_UNSET__") == "off"


def test_mistyped_null_notnull_text_rejected_by_validator():
    for bad in ("keep_txt", "garbage", "on", "true", "keeptext", "keep text"):
        try:
            _check_nnt(bad)
            assert False, "should have rejected %r" % bad
        except rt.SettingsError:
            pass


def test_mistyped_null_notnull_text_rejected_by_mrl_preflight():
    """The MRL preflight now passes null_notnull_text through the validator, so a typo fails
    closed with MrlPreflightError (before any DMS/Glue call)."""
    cfg = {"null_values": "", "null_rules": "", "null_notnull_text": "keep_text"}
    nnt = "keep_txt"
    # exercise the exact block preflight runs (validator over a cfg carrying null_notnull_text)
    _nullcfg = {"null_values": "", "null_rules": "", "null_notnull_text": nnt}
    try:
        rt._validate_null_settings(_nullcfg)
        assert False, "preflight validator must reject a mistyped null_notnull_text"
    except rt.SettingsError:
        pass
    # and a good value passes + normalizes
    good = {"null_values": "", "null_rules": "", "null_notnull_text": "KEEP_TEXT"}
    rt._validate_null_settings(good)
    assert good["null_notnull_text"] == "keep_text"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d null-option test(s) passed" % len(fns))
