# job2_load.py — post-load count-only check (optional `--post_load_check`)

> **Included in `main` as of the integration commit** _"job2_load: integrate optional
> --post_load_check full|count_only into scripts/job2_load.py"_. From that commit on, the real
> repo file `scripts/job2_load.py` already contains this change (content sha256
> `8de7a46c60cdf7c17f3be6e081d4a09483f06f73ebf0d476cdc7e5fd4f7398f1`), so on a current `main` the
> apply script `apply_post_count_only_patch.py` is a **no-op** (it detects the change is already
> present and exits cleanly without rewriting the file). This folder is kept for **older,
> hand-patched deployments** whose `job2_load.py` predates that commit.
>
> **Release candidate `release-1`:** this feature ships to `main` in the Sunday release via the
> merge commit _"Merge remote-tracking branch 'integrate/integrate-pc-sal' into release-1"_, which
> carries the integration commit above (feature commit `0730935`). Verified on the merged tree:
> `apply_post_count_only_patch.py scripts/job2_load.py` reports _"already patched (no-op)"_ with the
> input bytes unchanged (merged `scripts/job2_load.py` sha256
> `9d7a7942921083ac27e9c2bb2375057c83cd4130bb268b7aa2121160389cbeee`, which also includes the
> coexisting `--null_notnull_text` edits — the apply script matches on its own anchors only).

A small, additive patch for the **load** job (`scripts/job2_load.py`). It adds one optional
Glue argument, `--post_load_check`, with values **`full`** (default) and **`count_only`**.

## Why
After a big table's files are loaded, the load job's post-load checks can hang for up to an
hour and then **fail the whole load** on a network / DSQL error — even though the rows are
loaded and an operator can confirm the count by hand in seconds. The expensive, failure-prone
steps are:

* `validate_whole_table_after_ranges` (3 call sites): when the files are PK-disjoint (uuid
  keys) it runs **one index-range `COUNT(*)` per file** (hundreds of queries), falls back to a
  whole-table `COUNT(*)`, then runs **two FULL-TABLE regex/LIKE scans** (uuid-format +
  auth-token-leak). A non-timeout error such as a network/SSL EOF is appended to `errors` and
  **fails the load**.
* the single-table post-load block in `load_one_table` (the bounded `LIMIT` probe / `COUNT(*)`
  then the same uuid/leak scans).

## What `count_only` does (at **all four** post-load sites)
1. **No** per-file range counts.
2. **No** uuid-format scan and **no** auth-token-leak scan. One line is printed saying they were
   skipped by `post_load_check=count_only`.
3. Exactly **one** plain `SELECT COUNT(*) FROM schema.table` on a **fresh, autocommit**
   connection.
4. It cannot hang: a hard budget of **`POST_LOAD_COUNT_BUDGET_SECONDS = 600`** seconds for the
   whole count step; a failed **non-mismatch** attempt is retried up to **3** times, **20s**
   apart, only while the budget remains.
5. Outcome:
   * count completes and **equals** the loaded rows → a clear **OK** line;
   * count completes and **differs** → the table **FAILS exactly as today** (a real data
     problem; same exception text style, with **both numbers**);
   * count **cannot complete** (DSQL 300s txn-age, network / SSL `EOF occurred in violation of
     protocol` / connection reset, `XX000`, any other infrastructure error, or the budget is
     exhausted) → a loud **WARNING** naming the error class and
     `row count NOT verified by the load job - verify manually`, and **the load does NOT fail**:
     it continues and the table is marked loaded normally.

No change to checkpointing/resume, chunking, the writers, or any other path. The warning/OK note
is also recorded in an in-memory per-table note map (`_POST_LOAD_COUNT_NOTES`, no schema change);
the authoritative record is the printed log line.

`full` (and absent / empty / an **invalid** value → a warning then `full`) behaves
**byte-identically to today** (the same contract as `job3_validate.py`'s `--checksum_mode`).

## How the operator turns it on
The argument is read with the **same** optional-arg mechanism the script already uses (the
`optional = [...]` list + `getResolvedOptions`, exactly like `--checksum_mode`).

**Option A — Glue console, per job (quick):**
Glue console → the load job (and the load-big job) → **Job details** → **Job parameters** →
add key `--post_load_check` with value `count_only`. Save. New runs pick it up.

**Option B — the job template (durable, recommended):**
Edit `glue-templates/load.json` **and** `glue-templates/load-big.json` and add the arg to
`default_arguments`:
```json
"default_arguments": {
  "--TempDir": "s3://<<BUCKET>>/glue-temp/",
  "--post_load_check": "count_only"
}
```
Re-stage the template(s) to S3 (`s3://<bucket>/glue-templates/`), then let `CreateGlueJobs`
run (startup state machine) or run it once so the jobs are (re)created/updated with the arg.

### ⚠️ A console edit (Option A) is LOST when the jobs are re-created
**Checked in this repo:** the startup state machine state **`CreateGlueJobs`**
(`stepfunctions/startup.asl.json`) invokes `lambdas/create_glue_jobs.py`. For a job that
already exists that lambda calls `glue.update_job(JobName=name, JobUpdate=upd)`, and the code
comment is explicit: *"UpdateJob REPLACES the whole job definition: any field we omit is wiped."*
It builds `DefaultArguments` from the **template** + computed args — it does **not** read the
job's current `DefaultArguments` — so **any `--post_load_check` you added only in the console is
overwritten/removed on the next create_glue_jobs run** (startup re-run / resume, or cutover,
which also calls the same lambda). Only the VPC `Connections` are preserved across an update;
arguments are not.

**Therefore:** for anything other than a one-off manual run, set `--post_load_check` in the
**template `default_arguments`** (Option B) so it survives re-creation. Use the console edit only
if you will not re-run `CreateGlueJobs` before the load finishes.

## How to verify in the log
`count_only` is on when, for each table, you see (exact printed lines):
* the skip notice:
  `post_load_check=count_only [schema.table] @ <site>: skipping per-file range counts, uuid-format scan and auth-token-leak scan ...`
* success: `post_load_check=count_only [schema.table]: OK - COUNT(*) N == loaded N.`
* infra problem (load still succeeds):
  `WARNING post_load_check=count_only [schema.table]: COUNT(*) could not complete (<error class>: ...) within the 600s budget ...; row count NOT verified by the load job - verify manually. The load CONTINUES and the table is marked loaded.`
* a real mismatch (load **fails**, as today):
  `VALIDATION FAILED: ROW COUNT: loaded N, target has M (post_load_check=count_only whole-table COUNT(*)).`

If instead you see per-file range-count lines (`⑃ per-file range-count validation`) or
`INVALID UUID` / `AUTH TOKEN LEAK` scan lines, the job is running in `full` mode (the arg is not
set on the job that actually ran).

**A mismatch still fails the load.** `count_only` only removes the *infrastructure*-driven
failures; a count that completes and disagrees with the loaded rows fails the table exactly as
before, with both numbers in the message.

## What is no longer checked, and why that is safe
`count_only` drops the per-file range counts and the uuid-format / auth-token-leak full-table
scans. That is safe because the load's always-on guarantees already cover them:

* **No lost rows:** every file is gated during load by `rows_read == total_written`
  (per-file). The whole-table `COUNT(*)` (when it completes) is an extra confirmation.
* **No duplicate rows:** the target **PRIMARY KEY** rejects any duplicate at insert (`23505`);
  PK uniqueness is enforced by DSQL, not by a post-load scan.
* **uuid shape:** the per-row uuid guard (`_guard_row` / `flatten_chunk`) validates every uuid
  **before** insert, so a malformed uuid cannot be committed — the post-load uuid-format scan
  was redundant (and is a full-table scan prone to the same XX000/timeout failures).
* **auth-token leak:** this was a full-table `LIKE` scan; it is the main source of the hour-long
  hang-then-fail. It is dropped in `count_only`; run it out-of-band if you need it.

## Files
| File | Purpose |
|---|---|
| `apply_post_count_only_patch.py` | adds the change to any copy of `job2_load.py`; **fails closed** on an unrecognised or already-modified anchor; **idempotent**; prints sha256 old/new; compiles the result before writing |
| `job2_load_post_count_only.diff` | unified diff against `scripts/job2_load.py` on `main` (additions only) |
| `test_post_count_only_patch.py` | offline, executing tests (fake cursor/connection, fake clock; no AWS/DB) incl. the 4-site checks, full-mode-unchanged checks, and 3 mutation checks |
| `test_run_output.txt` | the saved output of the last test run |
| `README.md`, `RESULT.md` | this file and the result/limitations summary |
| `SHA256SUMS` | checksums of the files above |

## Install (operator)
1. Confirm your deployed script matches `main` (then you can apply the patch to it):
   ```
   aws s3 cp s3://<bucket>/scripts/job2_load.py /tmp/job2_load.py
   sha256sum /tmp/job2_load.py        # compare to the git blob on main
   ```
2. Apply the patch (keeps a backup, prints sha256 old/new, compiles before writing):
   ```
   cp /tmp/job2_load.py /tmp/job2_load.bak.py
   python3 apply_post_count_only_patch.py /tmp/job2_load.py /tmp/job2_load.patched.py
   aws s3 cp /tmp/job2_load.patched.py s3://<bucket>/scripts/job2_load.py
   ```
   Also copy it over `scripts/job2_load.py` in your repo clone, or `tools/setup.sh` would
   re-upload the unpatched repo copy.
3. Turn it on with Option B above (template `default_arguments`), then run a single table first
   and read its load log before running the rest.

Rollback: upload `/tmp/job2_load.bak.py` back over `scripts/job2_load.py`, or remove
`--post_load_check` (or set it to `full`) — `full` is byte-identical to today.
