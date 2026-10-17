#!/usr/bin/env python3
"""Hand-patch a CHECKOUT of the pipeline with the --null_notnull_text feature (for an environment
that is patched file-by-file rather than re-pulled from GitHub). Uses the SAME shared transform
(nn_transform.py) as the repo integration, so the two deliverables cannot drift.

    python3 apply_null_notnull_text_patch.py <repo_root>           # DRY RUN: sha256 old/new, no writes
    python3 apply_null_notnull_text_patch.py <repo_root> --apply   # back up each file (.nnbak), then patch

Guarantees (matching the sibling customer_patches apply scripts):
  * FAIL CLOSED: every file is transformed and (for .py) compiled IN MEMORY first; a missing or
    ambiguous anchor, or a result that will not compile, aborts BEFORE anything is written.
  * BACKUP FIRST: on --apply each changed file is copied to "<file>.nnbak" before it is written.
  * IDEMPOTENT: a file already carrying the feature is a no-op (sha256 unchanged).
  * Prints sha256 old/new for every file.

NOTE on nullability data: with --null_notnull_text=keep_text the jobs need per-column
is_nullable. New Job 1 discovery runs write it into the mapping; for tables whose mapping predates
this change the load/validate/CDC jobs fall back to ONE information_schema query per table.
Re-running Job 1 discovery (or letting it run on the next cycle) refreshes the mappings.
"""
import os
import shutil
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
    missing = [rel for rel, _ in T.FILES if not os.path.exists(os.path.join(repo, rel))]
    if missing:
        print("ERROR: these expected files are missing under %s:" % repo)
        for m in missing:
            print("   " + m)
        return 2
    plan = T.transform_all(read)   # fail-closed: raises before any write
    changed = 0
    for rel, (old, new) in plan.items():
        diff = old != new
        changed += diff
        print("%-62s %s  sha256 %s -> %s" % (rel, "CHANGE" if diff else "same  ",
                                             T.sha256(old)[:16], T.sha256(new)[:16]))
    print("--- %d files, %d to change, %s" %
          (len(plan), changed, "APPLYING" if apply else "DRY RUN (use --apply to write)"))
    if apply:
        for rel, (old, new) in plan.items():
            if old != new:
                p = os.path.join(repo, rel)
                shutil.copy2(p, p + ".nnbak")
                with open(p, "w", encoding="utf-8") as f:
                    f.write(new)
        print("applied; backups written next to each changed file as <file>.nnbak")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
