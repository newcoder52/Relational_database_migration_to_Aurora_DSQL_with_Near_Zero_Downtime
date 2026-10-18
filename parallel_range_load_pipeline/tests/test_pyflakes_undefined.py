#!/usr/bin/env python3
"""Amendment 17.3 - fail the build on any `undefined name` (or name used before assignment) in the
MRL tree, using pyflakes. This is a static guard complementing the import-smoke test (which runs
the module top level) and the static call-before-def check. Skips cleanly if pyflakes isn't present.
"""
import glob
import io
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)

pyflakes_api = pytest.importorskip("pyflakes.api")
from pyflakes.reporter import Reporter  # noqa: E402


def _mrl_py_files():
    files = []
    for sub in ("lambdas", "scripts", "tools", "tests"):
        files += glob.glob(os.path.join(PKG, sub, "*.py"))
    return sorted(files)


def test_no_undefined_names_in_mrl_tree():
    out, err = io.StringIO(), io.StringIO()
    rep = Reporter(out, err)
    for fp in _mrl_py_files():
        pyflakes_api.checkPath(fp, rep)
    all_msgs = (out.getvalue() + err.getvalue()).splitlines()
    fatal = [m for m in all_msgs
             if "undefined name" in m or "used before" in m
             or "undefined" in m.lower() and "name" in m.lower()]
    assert not fatal, ("pyflakes found undefined/used-before names in the MRL tree:\n  - "
                       + "\n  - ".join(fatal))


if __name__ == "__main__":
    test_no_undefined_names_in_mrl_tree()
    print("[PASS] no undefined names in the MRL tree")
