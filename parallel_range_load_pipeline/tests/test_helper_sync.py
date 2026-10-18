#!/usr/bin/env python3
"""Copy-sync: the bundled lambdas/mrl_loader_resolver.py (used by mrl_plan for the
resolver-consistency proof) must be byte-equivalent (per function) to the SAME functions in the
patched loader scripts/job2_load_manual_range.py, so the proof can never drift from what the
real Glue load job runs."""
import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
PATCHED = os.path.join(PKG, "scripts", "job2_load_manual_range.py")
SIDECAR = os.path.join(PKG, "lambdas", "mrl_loader_resolver.py")
_NAMES = ["ManualRangeError", "_mr_basename", "_mr_file_number", "_mr_resolve_one",
          "_mr_resolve_slice"]


def _segments(path):
    src = open(path).read()
    tree = ast.parse(src)
    out = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in _NAMES:
            out[n.name] = ast.get_source_segment(src, n)
    return out


def test_resolver_sidecar_matches_patched_loader():
    patched = _segments(PATCHED)
    sidecar = _segments(SIDECAR)
    missing = set(_NAMES) - set(sidecar)
    assert not missing, "sidecar is missing resolver function(s): %s" % missing
    for name in _NAMES:
        assert name in patched, "patched loader is missing %s" % name
        assert sidecar[name] == patched[name], (
            "resolver %s has DRIFTED from the patched loader; regenerate "
            "lambdas/mrl_loader_resolver.py from scripts/job2_load_manual_range.py" % name)


if __name__ == "__main__":
    test_resolver_sidecar_matches_patched_loader()
    print("[PASS] test_resolver_sidecar_matches_patched_loader")
