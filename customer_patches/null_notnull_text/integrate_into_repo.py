#!/usr/bin/env python3
"""Apply the --null_notnull_text feature to a REPO working tree (real repo integration), using
the ONE shared transform in nn_transform.py so it cannot drift from the hand-patch deliverable.

    python3 integrate_into_repo.py <repo_root>            # DRY RUN: print sha256 old/new, write nothing
    python3 integrate_into_repo.py <repo_root> --apply    # patch the files in place

Fail-closed: every file is transformed (and Python files compiled) in memory FIRST; if any
anchor is missing/ambiguous or any result fails to compile, nothing is written. Idempotent: a
file that already carries the feature is left unchanged.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nn_transform as T   # noqa: E402


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 2
    repo = os.path.abspath(argv[0])
    apply = "--apply" in argv[1:]
    read = lambda rel: open(os.path.join(repo, rel), encoding="utf-8").read()  # noqa: E731
    # Phase 1: compute everything (raises before any write on any problem).
    plan = T.transform_all(read)
    changed = 0
    for rel, (old, new) in plan.items():
        diff = old != new
        changed += diff
        print("%-62s %s  %s -> %s" % (rel, "CHANGED " if diff else "nochange",
                                      T.sha256(old)[:12], T.sha256(new)[:12]))
    print("--- %d files, %d changed, %s" %
          (len(plan), changed, "APPLIED" if apply else "DRY RUN (nothing written)"))
    if apply:
        for rel, (old, new) in plan.items():
            if old != new:
                with open(os.path.join(repo, rel), "w", encoding="utf-8") as f:
                    f.write(new)
        print("applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
