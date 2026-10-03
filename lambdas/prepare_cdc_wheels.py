#!/usr/bin/env python3
"""
prepare_cdc_wheels.py -- make the driver-cdc/ wheels installable by Glue Python shell WITHOUT internet.

WHY
  Glue Python shell (Python 3.9) pip-installs every --extra-py-files wheel before the CDC script
  starts, one wheel at a time. When a wheel declares a dependency (Requires-Dist) that is not
  installed yet, pip looks for it on pypi.org. Behind a firewall that times out (~20 min, then
  "...whl installation failed ... CalledProcessError"), even though the dependency's wheel is in
  the same folder. Seen in the field: boto3 -> botocore, python_dateutil -> six.

WHAT THIS DOES
  1. VALIDATES the folder first, using the wheels' own metadata (nothing is written if any check
     fails): pure-Python wheels only; one version per package; every wheel's Requires-Python allows
     the target Python (default 3.9); every dependency that applies on that Python is in the folder
     at a version inside the declared range (e.g. botocore on 3.9 needs urllib3 >=1.25.4,<1.27;
     boto3 needs its matching botocore); boto3, botocore and pg8000 are present; botocore knows
     the 'dsql' service.
     This replaces the check pip would have done, because step 2 removes pip's ability to do it.
  2. STRIPS the Requires-Dist lines from each wheel's METADATA (so pip has nothing to resolve and
     never contacts PyPI), updates the METADATA hash and size in the wheel's RECORD, and writes the
     result to a NEW folder with the SAME file names. Wheels with no Requires-Dist are copied
     byte-for-byte. The input folder is never modified.
  3. VERIFIES every output wheel: zip is readable, every file matches its RECORD hash, METADATA
     has no Requires-Dist, name and version unchanged.
  4. Writes MANIFEST.txt to the output folder: original and new sha256 per wheel and the exact
     lines removed (for security review: modified wheels no longer match PyPI's published hashes).

ONLY for driver-cdc/. Do NOT run it on driver-fullload/ or driver-validation/: the Spark jobs
put --extra-py-files on sys.path without pip, so they never need this.

USAGE
  python3 tools/prepare_cdc_wheels.py <input_folder> <output_folder> [--python 3.9]
  Exit code 0 = PASS (output written), 1 = FAIL (nothing written).

Standard library only. Runs on Python 3.6+.
"""
import argparse
import base64
import csv
import glob
import hashlib
import io
import os
import re
import shutil
import sys
import zipfile

_CDC_REQUIRED_DISTS = ("boto3", "botocore", "pg8000")


def _drv_norm(name):
    """PEP 503 normalized project name: 'python_dateutil' -> 'python-dateutil'."""
    import re
    return re.sub(r"[-_.]+", "-", name).lower()


def _drv_release(v):
    """Release segment of a version as ints: '1.42.97' -> (1, 42, 97), '2.9.0.post0' ->
    (2, 9, 0), '2.0a0' -> (2, 0). Pre/post/dev tags are ignored (enough for these pins)."""
    import re
    m = re.match(r"\s*v?(\d+(?:\.\d+)*)", v)
    if not m:
        raise ValueError(f"unparseable version {v!r}")
    return tuple(int(x) for x in m.group(1).split("."))


def _drv_cmp(a, b):
    n = max(len(a), len(b))
    a, b = a + (0,) * (n - len(a)), b + (0,) * (n - len(b))
    return (a > b) - (a < b)


def _drv_satisfies(version, spec):
    """True if `version` satisfies a specifier like '>=1.25.4,<1.27' or '!=3.0.*,>=2.7'.
    An empty spec is always satisfied."""
    import re
    v = _drv_release(version)
    for clause in [c.strip() for c in (spec or "").split(",") if c.strip()]:
        m = re.match(r"(===|==|!=|~=|>=|<=|>|<)\s*(\S+)$", clause)
        if not m:
            raise ValueError(f"unparseable specifier {clause!r}")
        op, target = m.group(1), m.group(2)
        if target.endswith(".*"):
            if op not in ("==", "!="):
                raise ValueError(f"wildcard only allowed with == or != in {clause!r}")
            pre = _drv_release(target[:-2])
            vv = v + (0,) * max(0, len(pre) - len(v))
            hit = vv[:len(pre)] == pre
            ok = hit if op == "==" else not hit
        else:
            t = _drv_release(target)
            c = _drv_cmp(v, t)
            if op in ("==", "==="):
                ok = c == 0
            elif op == "!=":
                ok = c != 0
            elif op == ">=":
                ok = c >= 0
            elif op == "<=":
                ok = c <= 0
            elif op == ">":
                ok = c > 0
            elif op == "<":
                ok = c < 0
            else:  # ~=  compatible release: >= t and same prefix except the last part
                if len(t) < 2:
                    raise ValueError(f"~= needs at least two version parts in {clause!r}")
                vv = v + (0,) * max(0, len(t) - len(v))
                ok = c >= 0 and vv[:len(t) - 1] == t[:-1]
        if not ok:
            return False
    return True


def _drv_bool_expr(tokens):
    """Evaluate a token list of True/False/and/or/not/(/) without eval()."""
    pos = [0]

    def peek():
        return tokens[pos[0]] if pos[0] < len(tokens) else None

    def take():
        t = peek()
        pos[0] += 1
        return t

    def atom():
        t = take()
        if t == "not":
            return not atom()
        if t == "(":
            val = disj()
            if take() != ")":
                raise ValueError("unbalanced parentheses")
            return val
        if t in ("True", "False"):
            return t == "True"
        raise ValueError(f"unexpected token {t!r}")

    def conj():
        val = atom()
        while peek() == "and":
            take()
            rhs = atom()
            val = val and rhs
        return val

    def disj():
        val = conj()
        while peek() == "or":
            take()
            rhs = conj()
            val = val or rhs
        return val

    val = disj()
    if peek() is not None:
        raise ValueError(f"unexpected token {peek()!r}")
    return val


def _drv_marker_applies(marker, py):
    """Evaluate an environment marker for Python `py` (tuple like (3, 9) or (3, 9, 16)).
    Returns True/False, or None when the marker uses anything other than python_version /
    python_full_version (e.g. extra == "crt", os_name) -- the caller then treats the
    dependency as not required for this check."""
    import re
    if not marker or not marker.strip():
        return True
    short = ".".join(str(x) for x in py[:2])
    full = ".".join(str(x) for x in py)

    def _sub(m):
        ver = full if m.group(1) == "python_full_version" else short
        return " True " if _drv_satisfies(ver, m.group(2) + m.group(4)) else " False "

    expr = re.sub(r"\b(python_full_version|python_version)\s*(===|==|!=|~=|>=|<=|>|<)\s*"
                  r"(['\"])([^'\"]+)\3", _sub, marker)
    tokens = re.findall(r"\(|\)|[A-Za-z_]+|\S", expr)
    if any(t not in ("True", "False", "and", "or", "not", "(", ")") for t in tokens):
        return None
    try:
        return _drv_bool_expr(tokens)
    except ValueError:
        return None


def _drv_parse_req(line):
    """'urllib3 (<1.27,>=1.25.4) ; python_version < "3.10"' ->
    ('urllib3', '<1.27,>=1.25.4', 'python_version < "3.10"'). Returns None if unparseable."""
    import re
    req, _, marker = line.partition(";")
    m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(\[[^\]]*\])?\s*\(?\s*([^()@]*?)\s*\)?\s*$",
                 req)
    if not m:
        return None
    return _drv_norm(m.group(1)), m.group(3).replace(" ", ""), marker.strip()


def _drv_read_wheel(path):
    """Read a wheel's file-name tags and METADATA. Returns a dict or raises ValueError."""
    import os
    import zipfile
    from email.parser import HeaderParser
    fname = os.path.basename(path)
    parts = fname[:-4].split("-")
    if not fname.endswith(".whl") or len(parts) not in (5, 6):
        raise ValueError(f"{fname} is not a valid wheel file name")
    pytag, abitag, plattag = parts[-3], parts[-2], parts[-1]
    with zipfile.ZipFile(path) as z:
        metas = [n for n in z.namelist()
                 if n.count("/") == 1 and n.endswith(".dist-info/METADATA")]
        if len(metas) != 1:
            raise ValueError(f"{fname} has no single *.dist-info/METADATA")
        msg = HeaderParser().parsestr(z.read(metas[0]).decode("utf-8", "replace"))
    return {"file": fname, "path": path, "name": msg.get("Name") or parts[0],
            "norm": _drv_norm(msg.get("Name") or parts[0]),
            "version": msg.get("Version") or parts[1],
            "requires_python": (msg.get("Requires-Python") or "").strip(),
            "requires_dist": msg.get_all("Requires-Dist") or [],
            "pytag": pytag, "abitag": abitag, "plattag": plattag}


def _drv_check(wheel_paths, py):
    """Validate a set of wheel files for Python `py`. Returns (dists, problems): dists maps a
    normalized name to its wheel info; problems is a list of plain-English strings (empty
    means the set is good)."""
    pyname = ".".join(str(x) for x in py[:2])
    problems, dists = [], {}
    if not wheel_paths:
        return dists, ["no .whl files found"]
    for p in wheel_paths:
        try:
            w = _drv_read_wheel(p)
        except Exception as e:
            problems.append(f"{p}: cannot read wheel ({e})")
            continue
        if w["abitag"] != "none" or w["plattag"] != "any":
            problems.append(f"{w['file']} is not a pure-Python wheel ({w['abitag']}-"
                            f"{w['plattag']}); it can only be used with pip on a matching "
                            f"platform. Download a pure-Python ('none-any') release instead.")
        pytags = w["pytag"].split(".")
        if not any(t.startswith("py3") or t == f"cp{py[0]}{py[1]}" for t in pytags):
            problems.append(f"{w['file']} is not built for Python 3 (tag {w['pytag']}).")
        if w["requires_python"]:
            try:
                ok = _drv_satisfies(pyname, w["requires_python"])
            except ValueError:
                ok = True
            if not ok:
                problems.append(f"{w['file']} needs Python {w['requires_python']}, but the "
                                f"CDC job runs Python {pyname}. Download a release that "
                                f"supports {pyname} (RUNBOOK Step 3b).")
        if w["norm"] in dists:
            problems.append(f"two versions of {w['name']} in the folder: "
                            f"{dists[w['norm']]['file']} and {w['file']}. Keep only one.")
            continue
        dists[w["norm"]] = w
    for need in _CDC_REQUIRED_DISTS:
        if need not in dists:
            problems.append(f"{need} is missing from the folder.")
    for w in list(dists.values()):
        for line in w["requires_dist"]:
            parsed = _drv_parse_req(line)
            if not parsed:
                continue
            dep, spec, marker = parsed
            if _drv_marker_applies(marker, py) is not True:
                continue
            have = dists.get(dep)
            if not have:
                problems.append(f"{w['name']} {w['version']} needs {dep}{spec or ''}, but no "
                                f"{dep} wheel is in the folder.")
                continue
            try:
                ok = _drv_satisfies(have["version"], spec)
            except ValueError:
                ok = True
            if not ok:
                problems.append(f"{w['name']} {w['version']} needs {dep}{spec} on Python "
                                f"{pyname}, but the folder has {dep} {have['version']}.")
    return dists, problems


# ---------------------------------------------------------------------------------------------
# strip + RECORD rewrite + verify
# ---------------------------------------------------------------------------------------------
def _b64_sha256(data):
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")


def _file_sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def _dist_info_paths(z):
    metas = [n for n in z.namelist() if n.count("/") == 1 and n.endswith(".dist-info/METADATA")]
    if len(metas) != 1:
        raise ValueError("expected exactly one *.dist-info/METADATA, found %d" % len(metas))
    di = metas[0].rsplit("/", 1)[0]
    rec = di + "/RECORD"
    if rec not in z.namelist():
        raise ValueError("no %s in wheel" % rec)
    return metas[0], rec


def strip_requires_dist(metadata_bytes):
    """Remove Requires-Dist header lines (and their continuation lines) from METADATA.
    Only the header block is touched; the long description after the first blank line is kept
    as-is. Returns (new_bytes, removed_lines)."""
    text = metadata_bytes.decode("utf-8")
    nl = "\r\n" if "\r\n" in text else "\n"
    sep = nl + nl
    if sep in text:
        header, body = text.split(sep, 1)
        has_body = True
    else:
        header, body, has_body = text, "", False
    kept, removed, dropping = [], [], False
    for line in header.split(nl):
        if line[:1] in (" ", "\t") and dropping:      # continuation of a removed header
            removed[-1] += nl + line
            continue
        dropping = line.lower().startswith("requires-dist:")
        if dropping:
            removed.append(line)
        else:
            kept.append(line)
    new = nl.join(kept) + ((sep + body) if has_body else "")
    return new.encode("utf-8"), removed


def rewrite_record(record_bytes, metadata_path, new_metadata):
    rows = list(csv.reader(io.StringIO(record_bytes.decode("utf-8"))))
    hit = 0
    for r in rows:
        if r and r[0] == metadata_path:
            while len(r) < 3:
                r.append("")
            r[1], r[2] = _b64_sha256(new_metadata), str(len(new_metadata))
            hit += 1
    if hit != 1:
        raise ValueError("RECORD has %d entries for %s (expected 1)" % (hit, metadata_path))
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows(rows)
    return out.getvalue().encode("utf-8")


def rebuild_wheel(src, dst):
    """Write dst = src with Requires-Dist stripped. Returns the list of removed lines (empty list
    means the wheel was copied unchanged)."""
    with zipfile.ZipFile(src) as zin:
        meta_path, rec_path = _dist_info_paths(zin)
        new_meta, removed = strip_requires_dist(zin.read(meta_path))
        if not removed:
            shutil.copyfile(src, dst)
            return []
        new_rec = rewrite_record(zin.read(rec_path), meta_path, new_meta)
        with zipfile.ZipFile(dst, "w") as zout:
            for info in zin.infolist():
                data = zin.read(info.filename)
                if info.filename == meta_path:
                    data = new_meta
                elif info.filename == rec_path:
                    data = new_rec
                zi = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                zi.compress_type = zipfile.ZIP_DEFLATED
                zi.external_attr = info.external_attr
                zi.create_system = info.create_system
                zout.writestr(zi, data)
    return removed


def verify_wheel(path, expect_name, expect_version):
    """Return a list of problems with an output wheel (empty = good)."""
    probs = []
    try:
        with zipfile.ZipFile(path) as z:
            bad = z.testzip()
            if bad:
                return ["%s: corrupt member %s" % (os.path.basename(path), bad)]
            meta_path, rec_path = _dist_info_paths(z)
            names = set(z.namelist())
            listed = set()
            for r in csv.reader(io.StringIO(z.read(rec_path).decode("utf-8"))):
                if not r:
                    continue
                listed.add(r[0])
                if len(r) >= 3 and r[1]:
                    if r[0] not in names:
                        probs.append("RECORD lists missing file %s" % r[0])
                        continue
                    data = z.read(r[0])
                    if r[1] != _b64_sha256(data) or (r[2] and int(r[2]) != len(data)):
                        probs.append("hash/size mismatch for %s" % r[0])
            unlisted = [n for n in names - listed if not n.endswith("/")
                        and not n.endswith((".dist-info/RECORD.jws", ".dist-info/RECORD.p7s"))]
            if unlisted:
                probs.append("files not in RECORD: %s" % ", ".join(sorted(unlisted)[:5]))
            md = z.read(meta_path).decode("utf-8")
            header = re.split(r"\r?\n\r?\n", md, 1)[0]
            if re.search(r"(?im)^requires-dist:", header):
                probs.append("METADATA still has Requires-Dist")
    except Exception as e:
        return ["%s: cannot verify (%s)" % (os.path.basename(path), e)]
    w = _drv_read_wheel(path)
    if (w["norm"], w["version"]) != (_drv_norm(expect_name), expect_version):
        probs.append("name/version changed to %s %s" % (w["name"], w["version"]))
    return ["%s: %s" % (os.path.basename(path), p) for p in probs]


def _botocore_has_dsql(path):
    with zipfile.ZipFile(path) as z:
        return any(n.startswith("botocore/data/dsql/") for n in z.namelist())


def main(argv=None):
    ap = argparse.ArgumentParser(description="Validate and strip Requires-Dist from driver-cdc wheels.")
    ap.add_argument("input_folder")
    ap.add_argument("output_folder")
    ap.add_argument("--python", default="3.9", help="target Python of the Glue job (default 3.9)")
    a = ap.parse_args(argv)
    py = tuple(int(x) for x in a.python.split("."))
    src = os.path.abspath(a.input_folder)
    dst = os.path.abspath(a.output_folder)
    if src == dst:
        print("FAIL: output folder must differ from the input folder (originals are kept).")
        return 1
    if os.path.isdir(dst) and glob.glob(os.path.join(dst, "*.whl")):
        print("FAIL: output folder %s already has .whl files. Use an empty folder." % dst)
        return 1
    wheels = sorted(glob.glob(os.path.join(src, "*.whl")))

    # 1. validate (before anything is written)
    print("Step 1/3  Validate %d wheel(s) in %s for Python %s" % (len(wheels), src, a.python))
    dists, problems = _drv_check(wheels, py)
    for w in sorted(dists.values(), key=lambda d: d["norm"]):
        print("    %-16s %-14s Requires-Python: %s" % (w["name"], w["version"], w["requires_python"] or "-"))
    bc = dists.get("botocore")
    if bc and not _botocore_has_dsql(bc["path"]):
        problems.append("botocore %s has no 'dsql' service data; use botocore/boto3 1.35 or later."
                        % bc["version"])
    if problems:
        print("FAIL  (nothing was written)")
        for p in problems:
            print("  - " + p)
        return 1
    print("    PASS")

    # 2. strip + RECORD
    print("Step 2/3  Strip Requires-Dist -> %s" % dst)
    os.makedirs(dst, exist_ok=True)
    manifest = ["# driver-cdc wheels with Requires-Dist removed (see prepare_cdc_wheels.py)",
                "# target Python %s" % a.python, ""]
    done = []
    for w in sorted(dists.values(), key=lambda d: d["norm"]):
        out = os.path.join(dst, w["file"])
        removed = rebuild_wheel(w["path"], out)
        done.append((w, out))
        print("    %-48s %s" % (w["file"], ("removed %d Requires-Dist line(s)" % len(removed))
                                if removed else "no Requires-Dist, copied unchanged"))
        manifest += ["%s" % w["file"],
                     "  original sha256: %s" % _file_sha256(w["path"]),
                     "  new sha256:      %s" % _file_sha256(out)]
        manifest += ["  removed: %s" % r for r in removed] or ["  removed: (none)"]
        manifest.append("")

    # 3. verify outputs
    print("Step 3/3  Verify output wheels")
    vprobs = []
    for w, out in done:
        vprobs += verify_wheel(out, w["name"], w["version"])
    if vprobs:
        print("FAIL")
        for p in vprobs:
            print("  - " + p)
        return 1
    with open(os.path.join(dst, "MANIFEST.txt"), "w") as f:
        f.write("\n".join(manifest))
    print("    PASS  %d wheel(s) written, every file matches its RECORD, no Requires-Dist left." % len(done))
    print("    MANIFEST.txt written (original vs new sha256 and removed lines).")
    print("\nPASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
