#!/usr/bin/env python3
"""AMENDMENT 3 (R9 / R2-3): every intra-repo markdown anchor link in README.md must resolve to a
real heading in the target file, using GitHub's heading-slug rules. This catches the class of bug
where README linked RUNBOOK.md#full-load-only-stopafterload but the heading is numbered
"## 13. Full-load-only (stopAfterLoad)" -> GitHub slug "#13-full-load-only-stopafterload".
"""
import os
import re
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)


def _github_slug(heading_text):
    """Approximate GitHub's heading->anchor slug: lowercase, strip non-word/space/hyphen, spaces
    to hyphens. (GitHub also drops a leading '#'; our regex captures text after the #'s.)"""
    s = heading_text.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)      # remove punctuation (incl. '.', '(', ')', '/')
    s = s.replace(" ", "-")
    return s


def _slugs_for(md_path):
    """All heading slugs in a markdown file, with GitHub's duplicate-suffix rule (-1, -2, ...)."""
    slugs = []
    seen = Counter()
    in_fence = False
    for ln in open(md_path, encoding="utf-8"):
        if ln.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = re.match(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$", ln)
        if not m:
            continue
        base = _github_slug(m.group(2))
        n = seen[base]
        slugs.append(base if n == 0 else "%s-%d" % (base, n))
        seen[base] += 1
    return set(slugs)


def _links(md_path):
    """(target_file, anchor) for every markdown link of the form (FILE.md#anchor) or (#anchor)."""
    text = open(md_path, encoding="utf-8").read()
    out = []
    for m in re.finditer(r"\]\(([^)\s]*\.md)?#([\w-]+)\)", text):
        out.append((m.group(1), m.group(2)))
    return out


def test_readme_intra_repo_anchors_resolve():
    readme = os.path.join(REPO, "README.md")
    missing = []
    slug_cache = {}
    for target, anchor in _links(readme):
        target_path = os.path.join(REPO, target) if target else readme
        if not os.path.exists(target_path):
            continue   # external/other-path links are out of scope for this check
        if target_path not in slug_cache:
            slug_cache[target_path] = _slugs_for(target_path)
        if anchor not in slug_cache[target_path]:
            missing.append("%s#%s" % (target or "README.md", anchor))
    assert not missing, "README.md anchor link(s) do not resolve to a heading: %s" % missing


def test_the_three_fixed_runbook_anchors_specifically():
    readme = open(os.path.join(REPO, "README.md"), encoding="utf-8").read()
    runbook_slugs = _slugs_for(os.path.join(REPO, "RUNBOOK.md"))
    for a in ("13-full-load-only-stopafterload", "14-post-load-count-only-check",
              "15-full-load-only-workflow-end-to-end"):
        assert ("RUNBOOK.md#%s" % a) in readme, "README must link the fixed anchor %s" % a
        assert a in runbook_slugs, "RUNBOOK heading slug %s not found" % a


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("[PASS]", fn.__name__)
    print("\n%d anchor test(s) passed" % len(fns))
