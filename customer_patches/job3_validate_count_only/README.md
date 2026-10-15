# job3_validate.py - count-only validation (drop-in replacement)

`job3_validate.py` in this folder is the full-load validation Glue script with a count-only mode added.
It is the repo's `scripts/job3_validate.py` (git blob `bf4bd737264759b8061fae05d9c28125704f89e6`, sha256 `51fa217fdeff88d15aaa1146569ed71c7a05a637f11ef94c1c05607de46dfa51`)
plus 395 added lines and nothing removed. File name is unchanged so it can be dropped in place.

## What it does
With `--checksum_mode off` set on the validate job, each table is validated by DSQL row count only:
DSQL `COUNT(*)` against the DMS `FullLoadRows` (if DMS has no figure: the row count saved by discovery).
No Spark read of the S3 files, no column checks. With any other `checksum_mode` the script behaves exactly as before.

- Table under 100M rows (or expected count unknown): one plain `SELECT COUNT(*)`. If DSQL says that count is too big
  (statement timeout / SQLSTATE 54000 / 57014 / XX000 "server unavailable"), it falls back to key ranges.
- Table expected at 100M rows or more: counted in key ranges of about 20M rows, 4 at a time. A range that is too big is halved.
  Ranges are built once from the table's lowest and highest key; only the first range is open below and only the last is open above.
  Every run checks that the ranges join with no gap or overlap; if that check ever fails the count is NOT used.
- Hard limit of 15 minutes per table (`COUNT_TABLE_BUDGET_SECONDS = 900`). If a count cannot finish (errors after 3 retries,
  or the limit is reached) the table PASSES WITH A WARNING in the log and `_validation_report.json`, so the run can move on to CDC.
- A count that finishes and differs from the expected count FAILS the table (`COUNT_DIFF`), same as a failed validation.
  The allowed difference is `count_mismatch_tolerance` (default 0). This applies even when `validate_count_check` is `warn`.
- Settings that no longer matter in this mode: `validate_hash`, `validate_rows_per_range`, `validate_parallelism`,
  `validate_target_seconds_per_range`, `conn_budget`. Constants at the top of the added block (`COUNT_*`) can be changed in the file.

## Install
Requirements: the validate job template has `"--checksum_mode": "off"` in `glue-templates/validate.json`
(check: `aws s3 cp s3://<bucket>/glue-templates/validate.json - | grep checksum_mode`).

1. Check your deployed script is the unmodified original:
   ```
   aws s3 cp s3://<bucket>/scripts/job3_validate.py - | sha256sum
   ```
   Expected `51fa217fdeff88d15aaa1146569ed71c7a05a637f11ef94c1c05607de46dfa51`.
   - Same hash: replace it with this folder's `job3_validate.py` (step 2).
   - Different hash: your copy has other changes. Do NOT replace the whole file; run the applier instead:
     `python3 apply_count_only_patch.py <your job3_validate.py> <output.py>` and upload the output.
2. Back up and replace:
   ```
   aws s3 cp s3://<bucket>/scripts/job3_validate.py job3_validate.bak.py
   aws s3 cp job3_validate.py s3://<bucket>/scripts/job3_validate.py
   aws s3 cp s3://<bucket>/scripts/job3_validate.py - | sha256sum     # expect e561fe603b570457a8bd21480d330d2eb415c4dce68fa5c1e538a19e5a457276
   ```
3. Also copy the file over `scripts/job3_validate.py` in your repo clone. `tools/setup.sh` re-uploads the repo copy and would remove the change.
4. Glue reads the script when a run starts. New validate runs use it; runs already started do not.

## Check the first run
The validate log header shows `checksum=off`. Each table logs `count-only [schema.table]: DSQL=... vs DMS FullLoadRows=... -> OK`.
If you see `ranges @ parallelism` lines instead, the job is not running this script (or `checksum_mode` is not `off`).
Read any line containing `COUNT NOT COMPLETED`, `COUNT TIMED OUT` or `RANGE CHECK FAILED`: that table was passed without a count.

Rollback: upload `job3_validate.bak.py` over `scripts/job3_validate.py`.

## Files
| File | Purpose |
|---|---|
| `job3_validate.py` | the complete script, drop-in |
| `apply_count_only_patch.py` | adds the same change to any copy of job3_validate.py; refuses a file it does not recognise or one already patched |
| `job3_validate_count_only.diff` | the change against the repo version (additions only) |
| `test_count_only_patch.py`, `test_run_output.txt` | offline tests (no AWS, no Spark) and the output of the last run |
| `SHA256SUMS` | checksums of the files above |

## Tested / not tested
Tested offline against a fake DSQL (real SQL engine DuckDB plus a pure-python one): 115 checks including 2,000 random tables per engine,
timeouts, XX000 errors, transient errors, boundary keys and the 15-minute limit; the repo's own job3 tests give identical results with
and without the change. Syntax checked for Python 3.7, 3.9 and 3.10 grammar.
NOT tested on real Aurora DSQL or inside Glue. Run one task first and read its validate log before running the rest.
