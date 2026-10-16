"""pytest path setup for the MRL offline suite: make the MRL lambdas, the repo lambdas (for
resolve_task / params_csv reuse), the tools dir, and the tests dir importable."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
for p in (HERE, os.path.join(PKG, "lambdas"), os.path.join(PKG, "tools"),
          os.path.join(PKG, "scripts"), os.path.join(REPO, "lambdas")):
    if p not in sys.path:
        sys.path.insert(0, p)
