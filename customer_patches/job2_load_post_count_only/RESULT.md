# RESULT — job2_load post-load count-only patch

## What this delivers
An additive, optional patch to `scripts/job2_load.py` adding `--post_load_check full|count_only`.
`full` (default / absent / empty / invalid→warn) is byte-identical to today. `count_only`
replaces, at **all four** post-load sites, the per-file range counts + uuid/leak full-table scans
+ probe with **one** `SELECT COUNT(*)` on a fresh autocommit connection under a hard 600s budget
that never hangs and never fails the load on an infrastructure error (mismatch still fails).

Four post-load sites (the task estimated lines ~5203/5444/5555; the actual file differs):
* `validate_whole_table_after_ranges` **def** at line 4680, called at **4946, 5187, 5298**
  (the "3 call sites"). One guard at the top of its body covers all three.
* the single-table (V5 whole-table) post-load block in `load_one_table` at **~4404** (`# ----
  Post-load validation ----`, the bounded `LIMIT`/`COUNT(*)` probe + `_run_uuid_and_leak_checks`).

## Base
* Repo: `newcoder52/Relational_database_migration_to_Aurora_DSQL_with_Near_Zero_Downtime`, `main`
  @ commit `1df6da3aff8ac5313f03758b29a473239bbdce03`.
* `scripts/job2_load.py` git blob `b94e0953d238c2c67c49de2a63901c39592656b7`,
  sha256 `baff63203ea8a28bd93f7399dd7afd06a1f537e5a5a8080564e32e4d039ccfef`.
* Patched file sha256 `8de7a46c60cdf7c17f3be6e081d4a09483f06f73ebf0d476cdc7e5fd4f7398f1`
  (produced by the applier; the diff is additions only — the sole "-" line is the optional-arg
  list's closing `]`, reopened to append `"post_load_check"]`).

## Verified offline (this environment)
* **Applier**: applies cleanly to `main`'s `job2_load.py`; output **py_compiles**; **idempotent**
  (second run is a no-op, identical bytes); **fails closed** on a modified anchor and on an
  unrecognised file (no output written); prints sha256 old/new. Every anchor is asserted present
  **exactly once**.
* **Behaviour (executing, fake cursor/connection/clock, no DB)** — `test_post_count_only_patch.py`,
  **46 passed / 0 failed** (`test_run_output.txt`):
  - match → OK; mismatch → raises with **both** numbers in today's text style;
  - txn-timeout then OK on retry (one 20s sleep, fresh connection per attempt);
  - network/SSL `EOF occurred in violation of protocol` ×3 → **WARNING, not failed**, names
    `ssl.SSLEOFError` (also classified from the message alone, not relying on
    `is_transient_server_error`); XX000 and connection-reset classified as infra;
  - **budget exhaustion** via a fake clock → WARNING, never sleeps past the 600s budget;
  - **full mode unchanged**: `validate_whole_table_after_ranges` still issues one per-file range
    `COUNT(*)` per file and still runs the uuid `!~` regex scan and the auth-token `LIKE` scan;
  - **all 4 sites honour the arg**: site-group A (the 3 call sites, via the function body guard)
    issues no per-file range query and no regex/LIKE scan in `count_only`; site B (single-table
    block) is verified by source-position (the guard is the first statement under
    `# ---- Post-load validation ----`, returning before the bounded probe) **and** by running
    the guard function directly (exactly one plain `COUNT(*)`, no probe/scan);
  - invalid value → warn + `full`; absent/empty/`full` → `full`.
  - **3 mutation checks**, each shown to flip a specific test to FAIL (kill the mutant):
    (1) remove the "skip range counts" guard → the "NO per-file range COUNT(*)" test fails;
    (2) turn a mismatch into a warning → the "mismatch should have raised" test fails;
    (3) re-raise an infra error instead of warning → the ssl-eof suite raises.
* **Existing repo tests still pass on the patched file** (patched in place in a repo copy):
  `test_guardrails` 80/0, `test_ease_guardrails` 86/0, `test_null_rules` 125/0, `test_override`
  87/0, `test_e2e_fixes` 82/0 — identical to the original. Whole `tests/` suite: 23/24 files pass;
  the only failing file, `test_validate_differential.py`, fails **identically on the unpatched
  original** because `pyspark` is not installed here (environment-only, unrelated to this patch).
* **Public-repo cleanliness**: no customer names/ids, no 12-digit account numbers, no emails in
  the added block or the deliverable. ("customer" appears only in pre-existing generic comments
  like "customer-owned step".)

## Design choices worth noting
* `connect_dsql()` in `job2_load.py` takes **no** arguments (unlike `job3_validate`'s
  `connect_dsql(autocommit=...)`); the helper opens it then sets `conn.autocommit = True`,
  matching the two existing post-load blocks.
* For `count_only`, **any** exception from the `COUNT(*)` is treated as "cannot complete" → WARN
  (never fail); only a completed-but-different count fails. SSL EOF is recognised explicitly so
  it does not depend on `is_transient_server_error`.
* The OK/WARNING note is also stored in an in-memory per-table map (`_POST_LOAD_COUNT_NOTES`) so
  it is available to any summary the job builds **without a schema change**; the printed log line
  is the authoritative record.

## NOT verified / limitations (be explicit)
* **Not run on real Aurora DSQL or inside AWS Glue.** All DB/Glue interactions are faked. The
  exact SQLSTATE/text DSQL emits for the 300s txn-age limit, XX000, and SSL EOF is matched by the
  existing classifiers (`is_txn_timeout`, `is_transient_server_error`, `is_broken_pipe_error`)
  plus the explicit SSL-EOF check; if real DSQL emits a fragment none of these recognise, the
  count-only path **still** warns-and-continues (any exception ⇒ cannot complete), so the
  fail-open guarantee holds, but the printed *error class label* could read as the generic
  exception type.
* **`load_one_table`'s full body is not executed offline** (it needs Spark + many runtime
  locals). Site B is verified by source-position of the guard and by exercising the guard
  function directly, not by running the entire function end-to-end.
* **Budget/retry timing** is validated with a fake clock; real wall-clock behaviour (network
  latency inside an attempt, DSQL-side query time) is not exercised. The 600s budget bounds the
  *waiting between* attempts and gates whether another attempt starts; a single `COUNT(*)` call
  that itself blocks for a very long time is bounded only by the underlying connection/driver
  timeout, not by this budget (the budget is checked between attempts). On a healthy DSQL a plain
  `COUNT(*)` returns quickly or raises the txn-age error; a pathological hang inside one call is
  the residual risk and is why the operator should still spot-check the first table's log.
* **`create_glue_jobs` overwrite claim** is read from the committed source
  (`update_job` replaces `DefaultArguments`; comment "UpdateJob REPLACES the whole job
  definition"); not observed against a live Glue API. Treat Option B (template) as the durable
  way to set the arg.
* The applier recognises `main`'s file via a set of required-string/anchor checks; a fork that
  renamed those lines will be **refused** (by design) rather than mis-patched.
