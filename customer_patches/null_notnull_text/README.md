# `--null_notnull_text`: keep text in NOT NULL char/text columns (optional, default OFF)

An opt-in rule for the NULL pipeline: for a target column that is **NOT NULL**
(`information_schema.columns.is_nullable='NO'`) **and** whose type is `character varying` /
`character` / `text`, **no text value is ever converted to SQL NULL** — the DMS endpoint marker
(`CsvNullValue`, default `NULL`) and any `null_values` entry are stored as the literal text.
This lets a customer whose NOT NULL char/text columns legitimately contain the text `NULL`,
`N/A`, `NA`, `null`, ... load that data **as-is** without listing hundreds of columns in
`null_rules`.

* **Empty field stays NULL** in every mode (it cannot occur in a NOT NULL Oracle column anyway).
* **Nullable columns and non-text types** (int, numeric, date, timestamp, uuid, bool, bytea,
  json, ...) behave **exactly as today**.
* **Precedence per column:** an explicit `null_rules` entry **>** this automatic rule **>**
  `null_values` **>** the endpoint marker.
* **Default `off` is byte-identical to today.** Turn it on with one params.csv row
  (`null_notnull_text,keep_text`) or, on a deployed environment, with the sibling script below.

## How it is wired (same path as `null_values`/`null_rules`)
A new optional Glue job argument **`--null_notnull_text`** with values **`off`** (default) |
**`keep_text`**. It travels `params.csv -> pipeline.json -> resolve_task -> create_glue_jobs ->
Step Functions -> the job scripts`, exactly like `null_values`/`null_rules`. Glue cannot pass an
empty value, so a blank setting travels as the `__NULL_UNSET__` sentinel (= off). An invalid
value warns and uses `off`.

The rule lives at the **single choke point** every script already funnels through,
`_effective_null_markers(schema, table, column, category)` in the byte-identical shared
null-rules block. All four Glue scripts and all their call sites (loader/validate Spark
`null_marker_expr`, CDC `_coerce_null`) pick it up automatically, so **load, validate and CDC
always agree** and validation never false-flags a correctly-loaded NOT NULL text value.

Nullability comes from Job 1 discovery, which now writes `is_nullable` onto each mapped
`column_mapping` entry. A table whose mapping predates this change triggers **one**
`information_schema` query per table (via the job's existing DSQL connection), cached; it is
never silently skipped — a truly-unknown column **fails loud**.

## CDC data-safety (the critical question) — SAFE
DMS writes CDC to S3 CSV with `AddColumnName=true`, which sends **every column on every change
row** — i.e. each I/U/D record is a **full after-image** (the repo’s own
`glue_cdc_continuous.py` comment and `docs/NO_PK_CDC_UPDATE_TRACKING.md` confirm there is **no
before-image** on the S3 CSV path — DMS drops before-images when writing S3 CSV; they exist only
for Kinesis/Kafka targets, and there is no partial/minimal-supplemental record). So a column that
was **not** changed in an UPDATE still carries its real current value, **never** the marker text
`NULL`. Therefore `keep_text` on a NOT NULL text column **cannot** overwrite an unchanged column
with the literal `NULL`. The rule is applied to full loads **and all CDC records (I/U/D)**, and
is safe to do so. (Keyless-table UPDATEs are already skipped-and-logged by the Tier-2 model, so
they are unaffected either way.)

## Two deliverables, ONE shared transform (cannot drift)
Everything is produced by the single `nn_transform.py` (anchor-checked, fail-closed, idempotent):

| File | Purpose |
|---|---|
| `nn_transform.py` | the single source of truth: every text edit (shared block + call sites + job1 + plumbing + MRL) as anchor-checked, fail-closed, idempotent functions, plus the `FILES` manifest |
| `integrate_into_repo.py` | applies the transform to a **repo working tree** (real repo integration, for the "pull latest main" path) |
| `apply_null_notnull_text_patch.py` | hand-patches a **checkout** file-by-file (`.nnbak` backup, sha256 old/new, fail-closed, idempotent) for environments patched without a fresh pull |
| `enable_glue_job_arg.py` | **deployed-env** operator tool: sets/removes ONE Glue arg (`--arg-name --arg-value`) on the `<project>-*` load / load-big / validate / cdc / ck-* / bg-* jobs whose script is one of the four. Dry-run default, backup first, GetJob→UpdateJob full-field copy, read-back compare, restore, job-name filter, wrong-shape SKIP. No params.csv plumbing needed |
| `test_null_notnull_text.py` | offline executing tests (no AWS/DB/Spark): type×nullability×setting matrix, the four scripts' real call sites (AST), byte-identical-when-off, load/validate/CDC parity, old-mapping fallback, apply-script fake-boto3 (dry-run/apply/no-op/restore/read-back/wrong-shape), 3 mutation guards |
| `repo_integration.diff` | unified diff of the repo integration vs base `main` |
| `test_run_output.txt`, `RESULT.md`, `SHA256SUMS` | last test output, result/limitations, checksums |

## Turn it on
* **Fresh checkout / latest main:** the feature is already integrated; set `null_notnull_text,keep_text`
  in `params.csv` (or `"null_notnull_text": "keep_text"` in `pipeline.json`) **before the full
  load**, then run as usual.
* **Deployed environment, no checkout:**
  ```
  python3 enable_glue_job_arg.py --project <project> --region <region> \
          --arg-name --null_notnull_text --arg-value keep_text          # DRY RUN
  python3 enable_glue_job_arg.py --project <project> --region <region> \
          --arg-name --null_notnull_text --arg-value keep_text --apply   # apply
  ```
  Then re-run Job 1 discovery (or let it run next cycle) so the mappings carry `is_nullable`;
  until then the jobs fall back to one `information_schema` query per table.

**Rollback:** set the arg back to `off` (or remove it) — `off` is byte-identical to today.
Changing the setting mid-migration mixes rules between loaded and CDC-applied rows, so set it
before the full load.
