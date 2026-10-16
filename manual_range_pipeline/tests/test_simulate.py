#!/usr/bin/env python3
"""pytest wrapper: run the whole MRL scenario simulator and assert every scenario passes.
`python3 tools/mrl_simulate.py` prints the full transcript + summary; here we just assert the
exit status is 0 (all scenarios PASS)."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(PKG, "tools"))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(PKG, "lambdas"))
sys.path.insert(0, os.path.join(os.path.dirname(PKG), "lambdas"))

import mrl_simulate as m  # noqa: E402


def test_all_scenarios_pass():
    rc = m.main()
    assert rc == 0, "mrl_simulate reported scenario failures (exit %d)" % rc
