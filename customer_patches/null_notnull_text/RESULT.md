# RESULT — `null_notnull_text` (customer_patches/null_notnull_text)

## What was built and VERIFIED (offline, no AWS/Glue/DSQL/Spark)
* One shared transform (`nn_transform.py`) drives **both** the repo integration and the hand-patch,
  so they cannot drift. Anchor-checked, fail-closed (an anchor missing/not-exactly-once aborts
  before any write), idempotent (already-patched input is a no-op), prints sha256 old/new.
* The feature is integrated into the repo on branch `null-notnull-text` (25 files) and compiles;
  the **four Glue NULL copies + the MRL copy stay byte-identical** (proved by the repo's own
  `tests/test_null_rules.py::test_four_copies_identical`, still green, and by this folder's
  `test_shared_block_identical_after_patch`).
* Both test suites are **identical to baseline**:
  * `tests/` — 200 passed before, **200 passed after**.
  * `manual_range_pipeline/tests/` — 142 passed before, **142 passed after**.
  * `tests/test_null_rules.py` — 125 checks before, 125 after (harness updated for the new
    globals; four-copies test green).
* This folder's `test_null_notnull_text.py` — **163 checks pass** (see `test_run_output.txt`):
  type×nullability×setting matrix for the shared function; the four scripts' REAL call sites via
  AST; byte-identical-when-off; load/validate/CDC parity; old-mapping fallback (returns False ->
  one catalog query; truly-unknown -> fail loud); apply-script fake-boto3 (dry-run no change,
  apply, second apply no-op, restore, read-back, wrong-shape SKIP); and 3 mutation guards
  (nullable-protected, int-protected, ignore-null_rules) — each confirmed to FAIL when the rule
  is mutated.

## CDC data-safety finding (answered from the code + docs)
**Safe.** DMS S3 CSV uses `AddColumnName=true` and sends a FULL after-image on every I/U/D row
(`scripts/glue_cdc_continuous.py` comment "DMS sends EVERY column on EVERY change row";
`docs/NO_PK_CDC_UPDATE_TRACKING.md` §2: no before-image on S3 CSV — Kinesis/Kafka only). An
unchanged column in a CDC UPDATE therefore carries its real value, never the marker `NULL`, so
`keep_text` on a NOT NULL text column cannot overwrite it with the literal `NULL`. The rule is
applied to full loads and all CDC records (I/U/D). Keyless UPDATEs are already skipped-and-logged
(Tier-2), so they are unaffected.

## What was NOT done (no live resources available)
* **No real AWS/Glue/DSQL/Spark.** The Spark `null_marker_expr` is exercised through the same
  offline column simulator the repo's `test_null_rules.py` uses; `_coerce_null` is run directly.
  `enable_glue_job_arg.py` is exercised against a fake boto3 Glue client (GetJob/UpdateJob/list),
  not a real account. No Glue job was created/updated, no DSQL `information_schema` query ran, no
  DMS CDC file was parsed on real infrastructure.
* The old-mapping fallback's **actual** `information_schema` round-trip is covered by registering
  the resulting map in the test; the live query itself was not run.
* Not committed/pushed anywhere except the local branch in `null-auto/repo`.

## Precedence implemented
explicit `null_rules` entry  >  automatic NOT NULL char/text rule  >  `null_values`  >  endpoint
marker.  Empty field is always NULL. Default `off` = byte-identical to today.
