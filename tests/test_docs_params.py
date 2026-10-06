#!/usr/bin/env python3
"""Offline consistency check: the docs and example files must document EVERY params.csv key
that lambdas/params_csv.py actually accepts, and no stray key the parser does not know.

Why: params_csv.py (PIPELINE_KEYS / OPTIONAL_DEFAULTS / REQUIRED / SETUP_ONLY) is the single
source of truth for the operator-facing parameters. When a key is added there (e.g. the fork
caps max_composite_forks / max_big_cdc_forks) the operator docs and the copy-me example files
must list it too, or operators never learn it exists. This test fails if:

  * any params_csv key is MISSING from
      - RUNBOOK.md §3 "Fill in params.csv" table            (all ALLOWED keys)
      - config/params.example.csv rows                      (all ALLOWED keys)
      - config/pipeline.example.json keys                   (the PIPELINE_KEYS subset)
      - docs/MANUAL_SETUP.md §3c params.csv -> pipeline.json mapping table (PIPELINE_KEYS)
  * any key DOCUMENTED in those tables/files is NOT a real params_csv key (stale/typo)
  * the RUNBOOK's "<N> keys end up in config/pipeline.json" count is not len(PIPELINE_KEYS)

Static, offline (no AWS, no boto3). Run: python3 tests/test_docs_params.py  (exit 0 = clean).
REPO_DIR overridable.
"""
import json
import os
import re
import sys

REPO = os.environ.get("REPO_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "lambdas"))

import params_csv as P  # noqa: E402  (after sys.path insert; offline, no boto3)

ALLOWED = set(P.ALLOWED)
PIPELINE_KEYS = set(P.PIPELINE_KEYS)
# Every key the parser knows, by category (for the "documented key is real" direction).
KNOWN = set(P.REQUIRED) | set(P.OPTIONAL_DEFAULTS) | set(P.SETUP_ONLY) | set(P._DERIVED_DEFAULT)

# Numbers written out in English that the RUNBOOK may use for the pipeline-key count. Supports
# plain words and hyphenated compounds up to the 20s–90s (e.g. "Twenty-two" -> 22).
_ONES = {"zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
         "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
         "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
         "eighteen": 18, "nineteen": 19}
_TENS = {"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70,
         "eighty": 80, "ninety": 90}


def _word_to_int(word):
    w = word.strip().lower()
    if w.isdigit():
        return int(w)
    if w in _ONES:
        return _ONES[w]
    if w in _TENS:
        return _TENS[w]
    if "-" in w:
        a, _, b = w.partition("-")
        if a in _TENS and b in _ONES and _ONES[b] < 10:
            return _TENS[a] + _ONES[b]
    return None

failures = []


def _read(path):
    with open(os.path.join(REPO, path), encoding="utf-8") as f:
        return f.read()


def _section(text, start_pat, stop_pat=r"\n#{2,6}\s+\S"):
    """Return the slice of `text` from the first match of start_pat up to the next heading
    (stop_pat) or end of file. Used to scope table parsing to one section. stop_pat matches a
    level-2+ ATX heading at line start (two or more '#'), so a single-'#' bash comment inside a
    fenced code block does NOT end the section."""
    m = re.search(start_pat, text)
    if not m:
        return ""
    rest = text[m.end():]
    s = re.search(stop_pat, rest)
    return rest[: s.start()] if s else rest


def _first_col_backtick_keys(table_text):
    """Keys from the first column of a markdown table where cells look like `| \`key\` | ... |`.
    Only rows whose first cell is a single backtick-quoted token are taken (skips the header and
    the |---| separator)."""
    keys = set()
    for line in table_text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if not cells:
            continue
        m = re.fullmatch(r"`([a-z0-9_]+)`", cells[0])
        if m:
            keys.add(m.group(1))
    return keys


def _csv_row_keys(csv_text):
    keys = set()
    for ln in csv_text.splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if s.lower().startswith("parameter,"):
            continue
        keys.add(s.split(",", 1)[0].strip())
    return keys


def _row_cells_by_key(table_text):
    """Map first-column backtick key -> list of all cell strings for rows whose first cell is a
    single backtick-quoted token. Used to assert later columns (Phase(s), Used by) are present
    and non-empty for every documented key."""
    rows = {}
    for line in table_text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if not cells:
            continue
        m = re.fullmatch(r"`([a-z0-9_]+)`", cells[0])
        if m:
            rows[m.group(1)] = cells
    return rows


def _check_phase_usedby(name, table_text, expected_keys, phase_idx, usedby_idx):
    """Every row for a known key must have a non-empty Phase(s) cell (phase_idx) and a non-empty
    Used-by cell (usedby_idx). Fails if a key row is missing, or either cell is blank/'-'."""
    rows = _row_cells_by_key(table_text)
    bad = []
    for k in sorted(expected_keys):
        cells = rows.get(k)
        if cells is None:
            bad.append(f"{k} (row missing)")
            continue
        phase = cells[phase_idx].strip() if len(cells) > phase_idx else ""
        used = cells[usedby_idx].strip() if len(cells) > usedby_idx else ""
        if not phase or phase in ("—", "-"):
            bad.append(f"{k} (empty Phase(s))")
        if not used or used in ("—", "-"):
            bad.append(f"{k} (empty Used by)")
    if bad:
        failures.append(f"{name}: Phase/Used-by cell problem(s): {bad}")
    else:
        print(f"[PASS] {name}: every key row has a non-empty Phase(s) and Used-by cell")


def _check(name, found, expected):
    missing = expected - found
    extra = found - KNOWN  # documented but not a real params_csv key
    if missing:
        failures.append(f"{name}: missing key(s) {sorted(missing)}")
    else:
        print(f"[PASS] {name}: all {len(expected)} expected key(s) present")
    if extra:
        failures.append(f"{name}: documents key(s) not in params_csv.py {sorted(extra)}")
    else:
        print(f"[PASS] {name}: no stray (non-params_csv) keys")


def main():
    # ---- RUNBOOK §3 "Fill in params.csv" table: all ALLOWED keys ----
    runbook = _read("RUNBOOK.md")
    sec3 = _section(runbook, r"#+\s*3\.\s*Fill in params\.csv")
    _check("RUNBOOK §3 table", _first_col_backtick_keys(sec3), ALLOWED)
    # Every key row in §3 must now also name its Phase(s) and the component that uses it.
    # §3 columns: Parameter | Required? | Default | Phase(s) | Used by | Meaning
    _check_phase_usedby("RUNBOOK §3 Phase/Used-by", sec3, ALLOWED, phase_idx=3, usedby_idx=4)

    # ---- RUNBOOK pipeline-key count sentence ("<N> keys end up in config/pipeline.json") ----
    mcount = re.search(r"([A-Za-z]+(?:-[A-Za-z]+)?|\d+)\s+keys end up in\s+`?config/pipeline\.json`?",
                       runbook)
    if not mcount:
        failures.append("RUNBOOK: could not find the '<N> keys end up in config/pipeline.json' "
                        "sentence")
    else:
        n = _word_to_int(mcount.group(1))
        if n != len(PIPELINE_KEYS):
            failures.append(f"RUNBOOK: pipeline-key count says {mcount.group(1)!r} "
                            f"(={n}) but PIPELINE_KEYS has {len(PIPELINE_KEYS)}")
        else:
            print(f"[PASS] RUNBOOK: pipeline-key count sentence == {len(PIPELINE_KEYS)}")

    # ---- config/params.example.csv rows: all ALLOWED keys ----
    _check("params.example.csv", _csv_row_keys(_read("config/params.example.csv")), ALLOWED)

    # ---- config/pipeline.example.json keys: the PIPELINE_KEYS subset (description allowed) ----
    doc = json.loads(_read("config/pipeline.example.json"))
    json_keys = set(doc) - {"description", "settings_version"}
    _check("pipeline.example.json", json_keys, PIPELINE_KEYS)

    # ---- docs/MANUAL_SETUP.md §3c mapping table: the PIPELINE_KEYS ----
    manual = _read("docs/MANUAL_SETUP.md")
    sec3c = _section(manual, r"#+\s*Step 3c")
    _check("MANUAL_SETUP §3c mapping", _first_col_backtick_keys(sec3c), PIPELINE_KEYS)
    # §3c columns: key | from params.csv | default | Phase(s) | Used by
    _check_phase_usedby("MANUAL_SETUP §3c Phase/Used-by", sec3c, PIPELINE_KEYS,
                        phase_idx=3, usedby_idx=4)

    if failures:
        print("\nDOCS/PARAMS CONSISTENCY: FAIL")
        for f in failures:
            print("  -", f)
        print(f"\n{len(failures)} problem(s).")
        return 1
    print("\nDOCS/PARAMS CONSISTENCY: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
