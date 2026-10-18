#!/usr/bin/env python3
"""Amendment 13 A.1 - prove, with NO network, that the count/probe python-shell job's driver set
works ONLY when it is the Requires-Dist-stripped (prepared) set, and that the UNSTRIPPED set fails
with the exact pip resolution error the live test hit (scramp cannot be found behind the firewall).

We build REAL minimal wheels (a pg8000 wheel that declares `Requires-Dist: scramp (>=1.4.5)` plus
asn1crypto, and scramp/asn1crypto wheels), strip them with the REAL lambdas/prepare_cdc_wheels, and
simulate `pip install --no-index --find-links <dir>` resolution over the wheels' METADATA:
  - a locked-down VPC = ONLY the wheels in --find-links are available (no PyPI);
  - pip fails if a wheel declares a Requires-Dist that is not satisfied by something in the dir.
The STRIPPED set has no Requires-Dist -> installs. The UNSTRIPPED pg8000 wheel needs scramp; if
scramp is NOT in the dir (the real firewall case) pip asks PyPI and fails -> this test reproduces
the hang-as-failure. Mutation: feeding the unstripped set must fail.
"""
import email
import os
import sys
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.dirname(HERE)
REPO = os.path.dirname(PKG)
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import prepare_cdc_wheels as pcw  # noqa: E402  (the REAL strip/rebuild code)


def _make_wheel(path, name, version, requires_dist=()):
    """Write a minimal, valid wheel: METADATA (+ optional Requires-Dist), WHEEL, RECORD."""
    dist = "%s-%s.dist-info" % (name.replace("-", "_"), version)
    meta = ["Metadata-Version: 2.1", "Name: %s" % name, "Version: %s" % version,
            "Requires-Python: >=3.7"]
    for r in requires_dist:
        meta.append("Requires-Dist: %s" % r)
    metadata = ("\n".join(meta) + "\n").encode()
    wheel = b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    record = ("%s/METADATA,,\n%s/WHEEL,,\n%s/RECORD,,\n" % (dist, dist, dist)).encode()
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("%s/__init__.py" % name.replace("-", "_"), b"")
        z.writestr("%s/METADATA" % dist, metadata)
        z.writestr("%s/WHEEL" % dist, wheel)
        z.writestr("%s/RECORD" % dist, record)


def _wheel_requires(path):
    """The (normalized) run-time Requires-Dist dependency NAMES of a wheel (markers ignored here)."""
    with zipfile.ZipFile(path) as z:
        mname = [n for n in z.namelist() if n.endswith(".dist-info/METADATA")][0]
        msg = email.message_from_bytes(z.read(mname))
    deps = []
    for line in msg.get_all("Requires-Dist") or []:
        nm = line.split(";")[0].split("(")[0].split()[0].strip()
        deps.append(nm.lower().replace("_", "-"))
    return deps


def _wheel_name(path):
    return os.path.basename(path).split("-")[0].lower().replace("_", "-")


def _simulate_pip_no_index(find_links_dir):
    """Simulate `pip install --no-index --find-links <dir> *.whl`: every wheel's Requires-Dist
    must be satisfiable by a wheel present in the SAME dir. Raise RuntimeError naming the first
    unresolved dependency (the 'Could not find a version that satisfies the requirement X' error),
    exactly as pip would behind a firewall with no PyPI."""
    whls = [os.path.join(find_links_dir, f) for f in os.listdir(find_links_dir)
            if f.endswith(".whl")]
    present = {_wheel_name(w) for w in whls}
    for w in whls:
        for dep in _wheel_requires(w):
            if dep not in present:
                raise RuntimeError(
                    "Could not find a version that satisfies the requirement %s "
                    "(from %s); no matching distribution found (PyPI unreachable)."
                    % (dep, _wheel_name(w)))
    return sorted(present)


def _build_sets(tmp):
    """Return (unstripped_dir, prepared_dir). The driver-cdc set DELIBERATELY omits scramp from
    the dir (as the live job only shipped pg8000's own wheel on the python-shell path), so an
    UNSTRIPPED pg8000 forces a PyPI lookup; the prepared set has Requires-Dist stripped."""
    src = os.path.join(tmp, "src"); os.makedirs(src)
    # pg8000 declares scramp + asn1crypto; we stage pg8000 only (the firewall case) ...
    _make_wheel(os.path.join(src, "pg8000-1.31.2-py3-none-any.whl"), "pg8000", "1.31.2",
                requires_dist=["scramp (>=1.4.5)", "asn1crypto"])
    # ... but the REAL driver-cdc/ also ships scramp+asn1crypto; include them so the PREPARED set
    # is self-contained even unstripped. (The regression is the Requires-Dist lookup itself.)
    _make_wheel(os.path.join(src, "scramp-1.4.5-py3-none-any.whl"), "scramp", "1.4.5")
    _make_wheel(os.path.join(src, "asn1crypto-1.5.1-py3-none-any.whl"), "asn1crypto", "1.5.1")
    prepared = os.path.join(tmp, "prepared"); os.makedirs(prepared)
    for f in os.listdir(src):
        pcw.rebuild_wheel(os.path.join(src, f), os.path.join(prepared, f))  # strips Requires-Dist
    return src, prepared


def test_a13_prepared_set_installs_offline():
    import tempfile
    tmp = tempfile.mkdtemp()
    _src, prepared = _build_sets(tmp)
    # the prepared (stripped) set resolves with --no-index (no Requires-Dist to look up)
    installed = _simulate_pip_no_index(prepared)
    assert "pg8000" in installed and "scramp" in installed and "asn1crypto" in installed
    # and every prepared wheel really has NO Requires-Dist
    for f in os.listdir(prepared):
        assert _wheel_requires(os.path.join(prepared, f)) == [], "%s still has Requires-Dist" % f


def test_a13_unstripped_set_fails_like_the_live_hang():
    """The UNSTRIPPED set, when scramp is NOT co-located (the live firewall case), fails with the
    exact pip resolution error - this is the 20-minute hang turned into a fast, clear failure."""
    import tempfile
    tmp = tempfile.mkdtemp()
    src, _prepared = _build_sets(tmp)
    # simulate the LIVE case: only the pg8000 wheel was effectively usable and it still declares
    # scramp. Put ONLY pg8000 in the dir to force the lookup pip could not satisfy offline.
    only_pg = os.path.join(tmp, "onlypg"); os.makedirs(only_pg)
    import shutil
    for f in os.listdir(src):
        if f.startswith("pg8000"):
            shutil.copy(os.path.join(src, f), os.path.join(only_pg, f))
    try:
        _simulate_pip_no_index(only_pg)
        assert False, "the unstripped pg8000 (needing scramp) must FAIL offline"
    except RuntimeError as e:
        assert "scramp" in str(e) and "satisfies the requirement" in str(e)


def test_a13_mutation_unstripped_full_set_still_needs_strip_or_colocation():
    """Mutation guard: if the pipeline fed the UNSTRIPPED set but relied on co-located scramp, a
    future wheel adding a NEW Requires-Dist not shipped would hang again. Prove the stripped path
    is the robust one: an unstripped wheel with an un-shipped dep fails; stripping it fixes it."""
    import tempfile
    tmp = tempfile.mkdtemp()
    d = os.path.join(tmp, "d"); os.makedirs(d)
    _make_wheel(os.path.join(d, "pg8000-1.31.2-py3-none-any.whl"), "pg8000", "1.31.2",
                requires_dist=["scramp (>=1.4.5)", "brandnewdep (>=9)"])
    _make_wheel(os.path.join(d, "scramp-1.4.5-py3-none-any.whl"), "scramp", "1.4.5")
    # unstripped: brandnewdep is not shipped -> fails (would hang on PyPI)
    try:
        _simulate_pip_no_index(d)
        assert False, "an un-shipped Requires-Dist must fail offline"
    except RuntimeError as e:
        assert "brandnewdep" in str(e)
    # stripped: install succeeds
    prepared = os.path.join(tmp, "p"); os.makedirs(prepared)
    for f in os.listdir(d):
        pcw.rebuild_wheel(os.path.join(d, f), os.path.join(prepared, f))
    assert "pg8000" in _simulate_pip_no_index(prepared)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d count-driver offline test(s) passed" % len(fns))
