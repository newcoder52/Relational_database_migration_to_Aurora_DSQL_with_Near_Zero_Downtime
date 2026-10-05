"""Offline test harness for the fix6 code fixes (no AWS, no Spark, no network).

The Glue job scripts call SparkContext()/GlueContext()/getResolvedOptions at import time, so we
cannot import them directly. We parse each script with `ast`, extract the exact source of the
named top-level functions, and exec just those into a namespace seeded with fakes + the module
constants they read. The logic under test is therefore the shipped logic.
"""
import ast
import os

REPO = os.environ.get("REPO_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def extract_defs(path, names):
    with open(path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    want = set(names)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in want:
            out[node.name] = ast.get_source_segment(src, node)
    missing = want - set(out)
    if missing:
        raise AssertionError(f"could not find defs {sorted(missing)} in {path}")
    return out


def load_defs(script_rel, def_names, injected):
    """exec the named defs from repo/<script_rel> into a namespace seeded with `injected`."""
    path = os.path.join(REPO, script_rel)
    defs = extract_defs(path, def_names)
    ns = dict(injected)
    for name in def_names:
        exec(compile(defs[name], f"<{script_rel}:{name}>", "exec"), ns)
    return ns


def read_source(script_rel):
    with open(os.path.join(REPO, script_rel), "r") as fh:
        return fh.read()
