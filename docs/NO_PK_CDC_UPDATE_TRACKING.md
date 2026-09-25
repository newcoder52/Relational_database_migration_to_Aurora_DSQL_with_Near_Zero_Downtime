# No-PK CDC UPDATE — Problem Tracking & Solution Search

> ⚠️ **STATUS — SUPERSEDED IN PLACE. Read this banner first.**
> This document is a chronological research log. Its **early sections (§1–§8) and parts of
> §9 (the "accept-duplicates" Tier-2 model and the Tier-3 refuse) are SUPERSEDED** by the
> later **§9 "BUILD-2 DONE"** section, which records what was actually implemented in
> `glue_cdc_continuous.py`. When the top half and the BUILD-2 section disagree, **BUILD-2 is
> authoritative.**
>
> **Final implemented model (as shipped — see §9 BUILD-2 DONE + BUILD-1b):**
> - **Tier 1 — keyed (real PK *or* an operator-declared single-column `metadata.logical_key`):**
>   full synchronous I/U/D via the `ON CONFLICT(key) DO UPDATE` / `DELETE WHERE key=` path.
>   Correct incl. update-heavy. This is the generic no-PK UPDATE solution.
> - **Tier 2 — keyless (no PK, no logical key):** INSERT and DELETE are applied (DELETE by
>   full-row content match under the "no duplicate rows" contract); **UPDATE is SKIPPED, logged
>   to `cdc_control.cdc_skipped_ops`, and the table keeps flowing (never blocked).** A CloudWatch
>   metric is emitted per skipped update. Per-file idempotent reload via the target-only
>   `_cdc_file` tag. The customer must configure DMS to emit **insert/delete-only** for keyless
>   tables.
> - The earlier **"accept cross-file duplicates"** design and the **"Tier-3 fail-closed refuse"**
>   were both **REVERSED** — the final model is skip-and-log (no duplicates created, no block).
>
> The original hard requirement below ("100% UPDATE for no-PK") was proven impossible for a
> truly-keyless table (indistinguishability, see §7/§9) and was resolved by the logical-key
> path (Tier 1) plus skip-and-log for the keyless tail (Tier 2). Historical detail retained
> below for context.

---

## 1. The problem, precisely

Migrate CDC changes into **Aurora DSQL** for **source tables that have NO primary key**.

CDC arrives as **S3 CSV files written by AWS DMS**. Each CDC row = `op` (I/U/D) + `dms_timestamp` + the row's **current (after-image) column values only**.

To apply a `U` (or `D`) to the *right* target row you must identify the **pre-update row**. That needs either the OLD values (before-image) or a stable per-row key. **Neither is in the S3 CSV.**

## 2. Hard constraints (the box we must stay in)

- **Source:** DMS CDC to **S3 target, CSV** format. `op` (I/U/D), `dms_timestamp`, after-image values, `AddColumnName=true`, `Rfc4180=true`.
- **No before-image on S3.** DMS `BeforeImageSettings` is **Kinesis/Kafka only** (verified in DMS docs). S3 CSV never carries old values, regardless of source logging (Postgres REPLICA IDENTITY FULL / Oracle supplemental / SQL Server MS-CDC all capture at source but DMS drops it writing to S3 CSV).
- **Target:** Aurora DSQL. No `TRUNCATE`; no `ctid`; ~3000 row-mods/txn; ~10 MiB/txn; ~300s txn-age; ~60-min connection cap; OCC 40001 retries; `INSERT ... ON CONFLICT` IS supported; `ALTER TABLE ADD/DROP COLUMN` supported; target-side DDL is ours to control.
- **Scale:** e.g. 20M+ rows, ~100 partitions. Reloading the whole table (or a full 200k-row partition) to catch a few changed rows is a **non-starter** (write-amp N/C).
- **Delivery:** at-least-once (SQS/poll); files re-processed on crash → **apply must be idempotent**.
- **Existing code:** `glue_cdc_continuous_v4.py` (CDC), `job2_load_multi_table_v16.py` (full load), `job1_discovery_multi_table_v2.py` (discovery), `job3_validate_multi_table.py` (validate). v4 currently **hard-blocks no-PK tables** (`TableBlocked` in `collapse_net_ops`).
- Ideas explicitly REJECTED by owner: whole-table/partition periodic reload (absurd at scale). Random target-only UUID (can't be referenced by CDC record).

## 3. What's been PROVEN (first 20-agent storm + earlier analysis)

- Target-only key (random UUID / sequence / content-hash-of-new): **cannot target an update** — not in the CDC record, and for a hash, hash(new) ≠ hash(old).
- All-columns match / multiset / append+LWW / async GC / within-file pairing / unique-index-DO-NOTHING: **all collapse on UPDATE** to the same before-image wall. (Within-file pairing works ONLY for insert-then-update *within one window* — near-zero coverage in steady state.)
- `_cdc_file` file-scoped replace: great for **idempotent file re-apply**, but **cannot net a cross-file update** (stale old row tagged by an earlier file is never deleted).
- AR_H_* metadata (change-seq, commit-ts): helps **ordering + idempotency**, NOT targeting (each event has a different value; can't link update to its insert).
- **The 4 exhaustive sources of pre-update row identity** (Agent 20): (a) source-carried stable key in every record; (b) before-image in the record; (c) within-window pairing when prior version in-hand; (d) content-uniqueness + no-update-to-matched-cols. Anything target-generated supplies none of (a)/(b).

## 4. Correct answers found so far (ranked)

1. **Source-carried stable key** (inject identity/UUID column at source; DMS captures it in every I/U/D) → becomes ordinary keyed CDC, v4 upsert path works with ~zero code change, correct incl. updates + duplicates. **Cost: source DDL (often blocked).** Oracle ROWID = no-DDL option but UNSTABLE (rejected).
2. **Refuse-U + insert/delete-only** for no-PK (fail-closed, never corrupts). Doesn't meet the 100%-update requirement — fallback only.
3. **Bounded reconciliation backstop** (reconcile only dirty content-hash buckets, not whole table) — eventually-consistent, cost scales with drift. Candidate if we accept some latency.

## 5. Design assets already spec'd (build regardless)

- **`cdc_file_status` ledger** (per (table, cdc_file): status started/done, file_min_ts, file_max_ts, has_pk, rows_applied) — exact per-file idempotent resume, PK + no-PK. Done-marker commits in the same txn as the file's final chunk.
- **`_cdc_file` tag column** — lets a not-done file's partial rows be purged before re-insert (file idempotency).

## 6. STORM 2 — out-of-the-box search (IN CONSTRAINTS)

Goal: find a way to make no-PK UPDATE **correct** without periodic full/partition reload and without assuming source DDL (unless proven the only way). Agents may collaborate / combine mechanisms.

### Agent angles (storm 2)
- A1: Reconstruct before-image from the TARGET at apply time (read current DSQL state to derive old row) — is there any correlation that works?
- A2: Maintain a Glue/DSQL-side **secondary index / shadow map** (content-hash → current row) updated as we apply, so an update can find the prior version. Does the shadow map itself have the identity problem?
- A3: Exploit DMS **op ordering within a file/stream** + a stateful applier that remembers the last-seen image per (derived) identity across files (persistent state in DSQL control tables).
- A4: Use `dms_timestamp` / AR_H_CHANGE_SEQ as a **version clock** + a target "current version per content-lineage" structure. Can lineage be reconstructed?
- A5: Two-stream approach — run a SECOND DMS task (or full-load refresh) that provides periodic snapshots to anchor, while CDC handles inserts/deletes; net updates by snapshot-anchored diff on changed buckets only.
- A6: Change the DMS S3 output shape WITHIN allowed settings (transformation rules, add-column, hash) to synthesize a source-derivable stable key from an immutable column SUBSET — find whether ANY real table has an update-stable unique subset and how to detect it.
- A7: Treat UPDATE as DELETE-by-new-image + INSERT only when new-image already exists (i.e., detect no-op updates) — bound the damage; characterize residual.
- A8: Content-defined identity using only KNOWN-immutable columns (e.g. created_at + a natural business key that never updates) — operator-declared "logical key" that isn't a DB PK. Is a declared logical key the pragmatic answer?
- A9: Log-structured / bitemporal target: store every version with valid-time; updates append; "current" resolved by a per-logical-key max version — requires a logical key (ties to A8).
- A10: Hybrid: keyed upsert where a logical/derived key exists; refuse/reconcile only where it truly doesn't. Classify per table.
- A11: Reconstruct old image by JOINING the U record against the target on the SUBSET of columns that DIDN'T change — but we don't know which changed. Explore partial-match heuristics and their failure bounds.
- A12: Use DSQL as the "before-image store": before applying, SELECT the candidate row(s) by a declared logical key; if exactly one, update it; if ambiguous, quarantine. Quantify how often ambiguity occurs.
- A13: Second-pass reconciliation scoped by AR_H_CHANGE_SEQ ranges (only lineages touched since last checkpoint), not by partition — minimize blast radius.
- A14: Push identity upstream cheaply: source-side VIEW or materialized column that DMS reads (not a table ALTER) — per engine, what's the least-invasive source change that yields a stable key.
- A15: Accept a declared "no true duplicates" contract per table and use full-row content key with a version column; updates = delete(old-content)+insert(new) ONLY if we can get old-content — tie to A1/A11.
- A16: Idempotency + exactly-once via a per-change unique id (txn-id + seq) ledger, combined with whichever targeting mechanism survives — make replays safe so a heavier apply is acceptable.
- A17: Cost/latency modeling of the surviving candidates at 20M/100-partition, small change rate — which are actually deployable.
- A18: Correctness proofs / break-tests for the surviving candidates (crash, replay, out-of-order, duplicates, DDL).
- A19: Operator-experience / config design — what must the customer declare (logical key? content-unique? update-stable subset?) for the chosen scheme to be safe, and how to fail closed.
- A20 (skeptic/synthesizer): try to break every storm-2 proposal; determine whether ANY in-constraint scheme achieves correct no-PK UPDATE without source DDL; if not, state the minimum unavoidable requirement and the best achievable.

## 7. STORM 2 FINDINGS (17 agents returned; total convergence)

**THE WINNER: operator-declared LOGICAL KEY + version-guard + exactly-once ledger + fail-closed guards + hybrid fallback.** Every independent angle converged on this or collapsed into it.

### The core result (A8, A6, A10, A20 — and every other agent confirms)
A no-PK table almost always has a set of columns that FUNCTION as a key (unique + never updated) — the DBA just never declared a DB PRIMARY KEY. If the **operator declares that logical key**, the after-image carries it (it's unchanged by updates), so:
- `INSERT ... ON CONFLICT (logical_key) DO UPDATE SET col=EXCLUDED.col` handles I and U — **UPDATE works with no before-image**, because the row still exists at that key.
- `DELETE WHERE logical_key = ...` handles D.
- This **reuses v4's existing keyed upsert path** — the logical key just feeds `metadata.primary_key` where v4 already reads it. The applier never cared whether the key was DB-enforced.
- Scales exactly like a PK table (A17): O(changes), write-amp ~1.0, needs only a **target-side UNIQUE index** (our DDL, no source change).

### Every alternative collapsed into "you need an update-stable unique key" (proven dead ends)
- A1 (reconstruct old row from target) — circular; only works if a stable unique subset exists (= logical key).
- A2 (shadow map), A3 (stateful cross-file memory) — inherit the same key requirement; if a key exists the **target table already IS the memory** (redundant).
- A9 (append + latest-per-key) — needs the logical key anyway, and is strictly WORSE than direct upsert (unbounded storage, mandatory compaction, same OCC).
- A11 (minimal-diff / partial match) — fundamentally HEURISTIC, ties/wrong at scale, full-scan cost → REJECT for 100% correctness.
- A15 (no-duplicates contract) — gives uniqueness but NOT update-stability → does not solve U (helps clean DELETE + insert idempotency only).
- A14 (least-invasive source change) — every option that rides in every CDC record requires the source to log a stable value = same cost as adding a real key; views/virtual cols don't get captured. Nothing cheaper than a real logged column.
- Before-image (Postgres REPLICA IDENTITY FULL / Oracle supp log / SQL Server MS-CDC) — source captures it but **DMS drops it writing to S3 CSV** (Kinesis/Kafka only). Dead on S3.

### The companions (must wrap the logical-key path)
- **A16 exactly-once ledger** (per-change `AR_H_CHANGE_SEQ` id, added via a DMS transformation rule → CSV column): makes apply idempotent under at-least-once/replay. Does NOT help targeting (event id ≠ row id) — it's the safety envelope, not the mechanism. Costs ~1 extra row-mod/op (halves net-ops/chunk).
- **A4 version guard** (`_version = AR_H_CHANGE_SEQ`, target column, our DDL): `... DO UPDATE SET ... WHERE target._version < EXCLUDED._version` → correct last-writer-wins under out-of-order/overlapping files. Strict `<` makes replay a no-op.
- **cdc_file_status ledger + `_cdc_file`** (from §5): per-file idempotent resume, PK + no-PK.

### The fail-closed guards (A18, A19, A20 — non-negotiable placements)
1. **U-guard BEFORE collapse** (A20's #1 fix): v4's `collapse_net_ops` folds U→INSERT and discards the op. The guard must see the raw op. A U whose logical key matches **!=1 target row** → quarantine + block (0 = key mutated/row missing; >1 = key not unique).
2. **Load-time UNIQUE gate:** declare the logical key as `UNIQUE NOT NULL` on the DSQL target; the 20M-row full load throws on the first duplicate → validates the declaration loudly before any CDC.
3. **LK-safe DDL gate:** block if a DDL renames/drops a logical-key column or its index.

### The one irreducible hole (A20, A18 — proven)
**100% synchronous correct no-PK UPDATE for ALL tables without (source key OR before-image) is PROVABLY IMPOSSIBLE** (indistinguishability proof: "row X updated to image A" and "new row inserted as A" deliver byte-identical after-image-only input). The logical-key scheme is the **correct maximum**. Two residual cases go **eventual, never silently corrupt**:
- An UPDATE that **mutates a logical-key column** AND the new key collides with another live row → silent overwrite. Mitigated (not eliminated) by a **bounded change-seq-scoped ghost sweep** (A13) that detects+repairs it eventually.
- **Truly keyless tables** (no declarable stable unique key, e.g. junction/staging) → hybrid fallback: append + bounded reconciliation (eventual), or refuse-U (fail-closed). Estimated small tail (~5%); ~70–85% of no-PK tables have a usable logical key (A8/A10 estimate).

### Coverage tiers (report honestly — never flat "100%")
- **Tier 1 (genuinely stable declared key):** fully correct, synchronous, incl. UPDATE. Majority of tables.
- **Tier 1 with occasional key mutation:** fail-closed (quarantine) + eventual ghost-sweep repair.
- **Tier 2 (no declarable key):** eventual (append + bounded reconcile) or refuse-U. Small tail.

## 8. DECISION / NEXT (resume here tomorrow)

### DECISION (to confirm with owner tomorrow)
Build the **logical-key CDC path** in v4 as the primary no-PK UPDATE solution:
1. Config: `metadata.logical_key {columns[], unique:true, update_stable:true}` (separate from `primary_key` so v16 span-recover isn't falsely triggered). Job1 SUGGESTS ranked candidates from the full-load snapshot (uniqueness≈1.0, zero nulls, ~0 update rate, id/uuid/date heuristics).
2. Job2 (v16/v17): create the logical key as a `UNIQUE NOT NULL` index on the DSQL target; fail loud on duplicates at full load.
3. v4 CDC: key the existing `ON CONFLICT`/`DELETE` path on the logical key when no real PK; add `_version` column + version-guarded upsert (A4); add per-change `AR_H_CHANGE_SEQ` ledger for exactly-once (A16); add the **U-guard-before-collapse** cardinality check (A20) — the key code change, since collapse currently folds U→INSERT.
4. DMS: add a transformation rule to emit `AR_H_CHANGE_SEQ` (and optionally commit-ts) as a CSV column.
5. Hybrid fallback + bounded ghost-sweep reconciliation (A13) for the keyless / key-mutation tail; refuse-U as the fail-closed floor.
6. `cdc_file_status` ledger + `_cdc_file` for per-file idempotent resume (both PK + no-PK).

### OPEN QUESTIONS FOR OWNER (answer on resume)
- Q1: For the real no-PK tables in scope — **what's the source engine, and do they have a natural business key** (order_id, external id, (tenant,created_at)) that is unique and never updated? This determines Tier-1 coverage.
- Q2: Do any updates **change that candidate key column**? If never → Tier 1 clean. If sometimes → need the ghost-sweep or a source key.
- Q3: Is adding `AR_H_CHANGE_SEQ` via a DMS transformation rule acceptable (it's a task-mapping change, not source DDL)?
- Q4: For the truly-keyless tail — acceptable to be **eventually consistent** (bounded reconcile) or must it be refuse-U?

### BUILD ORDER (once confirmed)
1. cdc_file_status ledger + no-PK insert/delete file-scoped path (foundation).
2. logical_key config + Job1 suggestions + Job2 UNIQUE index + fail-loud load.
3. v4 logical-key upsert + U-guard-before-collapse + _version guard + change_seq ledger.
4. bounded ghost-sweep reconciliation (backstop) + refuse-U floor.
5. Full multi-agent vet of the whole thing (owner's standard method) before final.

### STATUS: analysis COMPLETE. No code changed yet. v4/v16 remain as left after the prior session's edits (byte budget, batch trigger, token cache, conn pool, CDC upsert). Resume by confirming Q1-Q4, then BUILD ORDER above.

---

## 9. OWNER ANSWERS (resume session) + FINAL GENERIC ARCHITECTURE

**Owner answers:**
- Q1 Source engine: **Oracle**.
- Q2/Q3 Business key: unknown per-table; owner wants to **generate our own key** — but confirmed a **target-only** generated key (UUID/timestamp/seq) CANNOT target updates (not in the source CDC record; indistinguishability wall). Owner accepts this.
- Source is **UNTOUCHABLE** — no source DDL. Can only add/drop columns in the **TARGET**. (So a `SYS_GUID()` source column is OFF the table; Oracle ROWID is NOT emitted to S3 CSV — no `$AR_H_ROWID` transform token exists, confirmed in DMS docs.)
- Q4 `AR_H_CHANGE_SEQ`: DMS CAN add it via a **transformation rule** (task-level, not source DDL) — allowed. It's an ordering/exactly-once clock, NOT a row key.
- Owner's real goal: keep the scripts **GENERIC** for any future no-PK table, not hard-coded to today's tables. Flagged that **update-heavy + no-key** breaks reconciliation (cost scales with drift → toward whole-table reload). Correct concern.

**Confirmed hard fact (final):** with source untouchable + S3 CSV + no before-image, correct **synchronous** per-row UPDATE for a truly-keyless table is **mathematically impossible**. A target-only key does not help (update record can't reference it). Only a source-carried stable key OR a before-image OR eventual consistency can solve it — none available for a genuinely keyless table here.

### FINAL GENERIC ARCHITECTURE — auto-route by table capability (never silently corrupt)

**Tier 1 — logical key exists (declared by operator OR discovered by Job1):**
- Key the existing v4 `ON CONFLICT(logical_key) DO UPDATE` / `DELETE WHERE logical_key=` path on it.
- Correct SYNCHRONOUS I/U/D incl. **update-heavy**; scales like a PK table; target-side UNIQUE index only.
- Job1 discovery auto-suggests candidates (uniqueness≈1.0, non-null, ~0 update rate, id/uuid/date heuristics); operator confirms/declares.
- This is the common + best case. Most "no-PK" Oracle tables have an undeclared unique-stable id column.

**Tier 2 — no key: OWNER-FINALIZED BEHAVIOR (this session) — accept-duplicates, NOT reconcile, NOT refuse:**
- INSERT: supported (full-row content; "no duplicate rows" source contract makes full row a unique id).
- DELETE: supported (D record carries the row's current image → `DELETE WHERE all-cols = image` matches exactly one row).
- UPDATE within the SAME csv file: supported — collapse keeps the final image; file-scoped reload (delete rows tagged to this file + re-insert the file) leaves only the latest.
- UPDATE of a row ACROSS files (row inserted by an earlier, already-committed file; updated in a later file): **ACCEPTED AS A DUPLICATE, BY DESIGN.** The new image is inserted; the stale old row remains. This is a DELIBERATE owner decision (this session) to prefer availability over refusal — it REVERSES the earlier "Tier 3 refuse" for these tables. It is KNOWN-divergent (permanent duplicates, over-counts) — never call it correct/eventually-consistent.
- NO chunking for Tier 2 (owner: don't complicate the keyless path). Whole file applied then reloaded on failure via the file tag.
- Mitigations (cost-free, required): (a) carry `dms_timestamp` (and optionally `AR_H_CHANGE_SEQ`) as a plain target column so a consumer can pick latest-per-row at READ time and dedup later if desired; (b) emit a warning + CloudWatch metric on each accepted cross-file duplicate.
- **DUPLICATE AUDIT (owner ask this session):** whenever a cross-file update (= a U applied that is NOT an in-file collapse) is detected, write a record to a duplicates audit table AND/OR an S3 CSV audit so post-migration it's easy to enumerate exactly which target rows are duplicates created by cross-file updates. Detection signal: with the no-duplicate-rows contract, any U whose targeting can't be resolved in-file is by definition a cross-file update → log (table, cdc_file, dms_timestamp/change_seq, the after-image, reason='cross_file_update_duplicate').
- File-scoped tagging: Glue-generated target-only column(s) — `_cdc_file` (+ optional dms_timestamp+incrementer composite) — for per-file idempotent reload (delete rows tagged to a file, re-insert the file). NOT a targeting key for cross-file U (target-only, not source-carried).
- Rejected for Tier 2: bounded/dirty-bucket reconciliation (owner rejected reconcile entirely); logical key (that would make it Tier 1 — the correct-updates path if a unique+stable col exists).

**Tier 3 — no key AND a CROSS-FILE update appears:**
- **OWNER DECISION (this session): FAIL CLOSED. No reconciliation at all.** Owner evaluated bounded/WHERE-scoped reconciliation and rejected it: it is only correct when an update-stable BUCKET column exists to scope the rescan (and dedup within the bucket still needs a version + an entity identity); without such a column the scope degrades toward whole-table, which is absurd at 20M. Rather than build reconciliation that is only conditionally correct, Tier 3 simply refuses.
- **FAIL CLOSED**: refuse with actionable message ("keyless table with a cross-file UPDATE: declare a metadata.logical_key to become Tier 1, or route this table to a streaming (Kinesis/Kafka) CDC target that carries the before-image"). NEVER silently corrupt, NEVER reconcile.
- Rejected here (owner, this session): bounded/dirty-bucket reconciliation, whole-table reblank, delete+re-insert on full-row content for a cross-file U (the U record has only the after-image, so the DELETE matches 0 rows and the INSERT leaks a stale duplicate — proven again this session against the "no duplicate rows" contract: no-dup makes keyless DELETE + INSERT correct but does NOT solve cross-file UPDATE).

**"No duplicate rows" contract (owner proposal this session) — what it buys Tier 2:**
- If the source guarantees no two rows are byte-identical, the FULL-ROW CONTENT is a unique identifier. This makes keyless **DELETE** correct (`DELETE WHERE col1=v1 AND ... AND colN=vN` matches exactly the one row the D record's after-image describes) and keyless **INSERT** idempotent. This is the mechanism Tier 2 uses.
- It does NOT solve cross-file UPDATE: the U record carries only the NEW image, the target still holds the OLD image, so a full-row DELETE on the new values matches 0 rows → stale row leaks. In-file updates are fine (file-scoped reload replays the final image).
- Glue-generated target-only column (dms_timestamp + incremental int composite) proposed for file-scoped idempotent reload: VALID for I/D/in-file-U tagging (delete rows tagged to a file, re-insert the file). NOT a targeting key for cross-file U (target-only, not carried by the source CDC record — same wall as any target-generated key).

**Always-on wrappers (generic, PK + no-PK):**
- `cdc_file_status` ledger + `_cdc_file` column → exact per-file idempotent resume.
- `AR_H_CHANGE_SEQ` via DMS transform rule → exactly-once dedup + correct out-of-order last-writer-wins.
- Fail-closed guards: load-time UNIQUE index validates a declared key (fail loud on dup); a `U` matching !=1 row → quarantine; U-guard runs BEFORE collapse (v4 currently folds U→INSERT).
- Routing decision recorded per table; op-stream observation can DOWNGRADE a table (e.g. append_only sees a U → block) — observation overrides declaration.

### BUILD-1 DONE (this session): per-file ledger + per-chunk audit (additive tracking)
Implemented in `glue_cdc_continuous_v4.py` (AST+pyflakes clean, exit 0). Purely additive — the existing zero-loss apply path (collapse → chunk → same-txn checkpoint → commit) is unchanged; the blanket no-PK TableBlocked is NOT yet removed (that's BUILD-1b with the key threading).

**Two new control tables (in `ensure_control_tables`, DSQL-safe DDL):**
- `cdc_control.cdc_file_status` — one row per (table_name, cdc_file), composite PRIMARY KEY. Cols: status, has_pk, file_min_ts, file_max_ts, rows_applied, chunks_committed, all_rows_committed, started_time, committed_time, done_time.
- `cdc_control.cdc_chunk_log` — one row per COMMITTED chunk. Cols: id (python uuid4 PK), table_name, cdc_file, chunk_seq, start_offset, end_offset, rows, watermark_ts, committed_time.

**DUAL-MARKER file lifecycle (owner chose BOTH grains — they answer different questions):**
- `started` (Option A, lifecycle): `mark_file_started_cur`, OFF hot path BEFORE the chunk loop, own txn via `run_control_op`. Idempotent on resume (never regresses committed/started_time).
- `all_rows_committed=true` (Option B, GRANULAR TRUTH): `mark_file_committed_cur`, set in the SAME txn as the file's FINAL data chunk (`if new_offset >= n`) — atomic with the last rows, cannot lie. Zero-net-op files (n==0) set it via an own-txn control op after the loop so the ledger stays consistent.
- `status='done'` (Option A, lifecycle): `mark_file_done_cur`, fused into the high-water `_commit_status(_done_file=key)` txn that advances cdc_status.last_done_file + moves the file to processed/.
- The narrow crash window (final chunk committed, high-water not advanced) is now OBSERVABLE: all_rows_committed=true AND status='started' → data safe, high-water lagging, resume re-marks it (idempotent, no loss).

**Per-chunk audit (Option B granularity for chunks, PK-ready now):** `insert_chunk_log_cur` writes one cdc_chunk_log row INSIDE each chunk's data txn (atomic with data + checkpoint). Gives full chunk history (start/end offset, rows, watermark, time) vs cdc_status's single moving offset.

**Row-mod budget:** `_pack_chunk` reserve raised 1 → 3 (cdc_status checkpoint + cdc_chunk_log INSERT + final-chunk cdc_file_status UPDATE). Cost: 2 fewer data net-ops per ~2999-op chunk (negligible). Never exceeds the 3,000 row-mod limit.

**Helpers added (all cursor-based, caller owns commit/retry, mirror upsert_cdc_status pattern):** `mark_file_started_cur`, `insert_chunk_log_cur`, `mark_file_committed_cur`, `mark_file_done_cur`, plus `min_dms_ts` (mirrors max_dms_ts). `_commit_status` gained an optional `_done_file` param.

Note: has_pk is `ctx["pk_col"] is not None` today; the logical-key path (BUILD-2/3) will set it true for no-PK-but-keyed tables. cdc_chunk_log/cdc_file_status are key-agnostic → work as-is for the no-PK path once it lands.

### BUILD-1b DONE (this session): apply_key threading + 3-tier router (blanket TableBlocked removed)
Implemented in `glue_cdc_continuous_v4.py` (AST+pyflakes clean, exit 0).

**apply_key abstraction (build_table_context):** resolves the targeting key once — `pk_col` (single-col DB PK) if present, else `metadata.logical_key.columns[0]` (single-col logical key) for a no-PK table. Adds to ctx: `apply_key`, `logical_key_col`, `key_source` ('pk'|'logical_key'|'none'), `tier_hint` (1 if keyed, None if keyless). A real PK always wins; logical_key honored only when there's no PK. `pk_col` stays in ctx for key_source detail but NOTHING reads it for targeting anymore (grep-verified 0 `ctx["pk_col"]` reads).

**Threaded apply_key through the keyed path** (was `ctx["pk_col"]`, now `ctx["apply_key"]`): `collapse_net_ops` (keys net{} on it), `apply_file` (`pk_col = ctx["apply_key"]` → ON CONFLICT / DELETE WHERE / pk_suffix / _schema_missing exemption all target it), `validate_file_netops` (by-key re-read). So a declared-logical-key table now applies I/U/D end-to-end EXACTLY like a PK table — this is the generic no-PK UPDATE solution, live. Ledger `has_pk` marker now = "applied via keyed path" (apply_key present), honest for logical-key tables.

**3-tier router (collapse_net_ops), replaces the blanket "no PK → block":**
- apply_key present → Tier 1 keyed collapse (existing proven logic). Correct synchronous I/U/D incl. update-heavy. ✅
- apply_key None + any op=U seen (raw-op scan BEFORE the U→INSERT fold) → currently raises Tier-3 TableBlocked. **NOTE: owner has since finalized Tier 2 = ACCEPT-DUPLICATES (see Tier-2 section), which REPLACES this Tier-3 refuse for keyless tables — the accept-duplicates apply path + duplicate audit is the NEXT build (not yet coded).**
- apply_key None + only I/D → currently raises "Tier 2 not yet enabled" TableBlocked. **Also to be replaced by the Tier-2 accept-duplicates apply path.**

STATUS: Tier 1 (PK + logical key) is COMPLETE and verified.

### BUILD-2 DONE (this session): Tier-2 keyless apply — INSERT+DELETE only, UPDATE skipped+logged
Implemented in `glue_cdc_continuous_v4.py` (AST+pyflakes clean, exit 0). FINAL owner model (evolved this session from accept-duplicates → skip-and-log, because keyless in-file update matching is itself unsafe without a key; owner chose insert/delete-only as the customer's DMS contract).

**Behavior:**
- Customer configures DMS to emit INSERT/DELETE-only for no-PK tables (their responsibility; document in RUNBOOK).
- INSERT: applied. Row tagged with the Glue-managed target-only col `_cdc_file` (source file key). (`_dms_ts` and the AR_H_CHANGE_SEQ/unique-sequence idea were DROPPED for Tier 2 this session: with updates SKIPPED there are no duplicates to version/order-resolve at read time, and watermark/ordering already live in cdc_status + cdc_file_status + the CSV-derived max_dms_ts — a per-row target timestamp/sequence added no functional value. The skipped-op audit still records each skipped U's dms_timestamp in cdc_skipped_ops.)
- DELETE: applied via FULL-ROW CONTENT match (`DELETE ... WHERE <every data col> = <val>`, NULLs via IS NULL) — exactly one row under the no-duplicate-rows contract.
- UPDATE: **SKIPPED (not applied), logged to `cdc_control.cdc_skipped_ops`, table NOT blocked, keeps flowing.** Consequence (acknowledged): the target keeps the row's pre-update value; every skip is enumerable in the skip log. CW metric GlueCDC/NonPK SkippedUpdates emitted.
- Idempotent file reload (no PK → no upsert): on (re)apply, `DELETE WHERE _cdc_file=<file>` purges this file's prior inserts, then re-insert — exact crash/replay idempotency. Content DELETEs are naturally idempotent.
- WITHIN-FILE CHUNKING (added this session): chunk a SINGLE file's I/D ops by the DSQL budget (1 row-mod per op, reserve 3 control rows → ≤2997 ops/chunk; byte budget = CDC_CHUNK_BYTE_BUDGET//4 conservative for multi-byte, per-op statements so wire-msg limit never at risk). SMALL file = 1 chunk (whole file); LARGE file = multiple chunks applied IN ORDER. NEVER across files (a chunk is always a slice of one file; files still processed one-at-a-time in timestamp order). The purge (file-scoped reload) runs ONCE, fused into the FIRST chunk's txn. Each chunk: same-txn cdc_status checkpoint + cdc_chunk_log audit; FINAL chunk sets cdc_file_status all_rows_committed + skipped-U log + metric. Full retry set per chunk (OCC/schema-conflict/server/broken-pipe); recycle-check between chunks. Watermark-monotonicity guard kept.
- RESUME = whole-file restart (re-purge + replay), idempotent via `_cdc_file` → keyless ignores start_offset (mid-file seek not needed; reload makes full replay safe). chunks_committed = number of within-file chunks.
- `_cdc_file` is LOAD-BEARING for crash-safe keyless replay (no key → plain INSERT isn't idempotent; the file tag is the only handle to undo a partial file apply). Kept for the ENTIRE life of CDC. `_dms_ts` + unique-sequence were dropped (dead after skip-updates); `_cdc_file` was NOT — it's still required.
- **CLEANUP / DECOMMISSION (owner reminder this session): DROP `_cdc_file` only AFTER CDC is permanently stopped for the table (cutover/decommission), never while CDC could still run (a live apply would re-add it and a crash-replay in that window loses its safety net). One-liner per keyless table: `ALTER TABLE <schema>.<table> DROP COLUMN "_cdc_file";` — target-only, does not touch source/DMS. TODO: add to RUNBOOK as a cutover checklist item; optionally a small `--drop-nonpk-tags` cleanup helper that enumerates keyless tables from the manifest and drops the column from each.**

**Code added:**
- `collapse_net_ops_nonpk` — keyless collapse: I→INSERT netop, D→DELETE netop (full image), U→skipped list. No key folding.
- `apply_file_nonpk` — Tier-2 apply; SAME return shape as apply_file (total_applied, file_watermark, chunks_committed=1) so process_table handling is identical.
- `_ensure_nonpk_tag_columns` — idempotent ALTER ADD `_cdc_file`/`_dms_ts` on the target (target-only; excluded from content match + from the missing-column guard).
- `cdc_control.cdc_skipped_ops` table (repurposed from the earlier cdc_nonpk_duplicates); `insert_skipped_op_cur`; `emit_skipped_update_metric`.
- Constants `NONPK_FILE_TAG_COLUMN='_cdc_file'`, `NONPK_DMS_TS_COLUMN='_dms_ts'`.
- Router: process_table branches `_apply_fn = apply_file if ctx["apply_key"] is not None else apply_file_nonpk`. collapse_net_ops keyed path now has a defensive INTERNAL guard if ever called keyless (routing invariant).

**Superseded:** the earlier "accept-duplicates + cdc_nonpk_duplicates audit + no CSV writer" plan and the Tier-3 fail-closed/two-placeholder-raises. Final = skip-and-log (no duplicates created, no block, no reconcile).

### BUILD ORDER (final, generic)
1. ~~`cdc_file_status` ledger + per-chunk audit~~ ✅ DONE. ~~remove blanket TableBlocked + apply_key threading + router~~ ✅ DONE (BUILD-1b above). Tier 1 (PK + logical key) live. TODO: Tier-2 accept-duplicates apply path (replaces the two keyless placeholder raises).
2. `logical_key` config + Job1 candidate discovery + Job2 UNIQUE-index-on-target + fail-loud-on-dup load.
3. v4 logical-key upsert path: key on logical_key; add `_version`(=AR_H_CHANGE_SEQ) version-guarded upsert; add change_seq exactly-once ledger; add U-guard-BEFORE-collapse cardinality check (!=1 row → quarantine).
4. Auto-router + shape classifier (Tier 1/2/3) + Tier-3 fail-closed refuse + config/discovery UX.
5. Bounded change-seq-scoped reconciliation backstop (Tier 2 updates).
6. DMS transform rule to emit AR_H_CHANGE_SEQ as a CSV column (task mapping, doc'd).
7. Full multi-agent vet of the whole thing before final.

### DECISION: build the generic auto-routing handler. Tier 1 (logical key) is the primary correct path incl. update-heavy; Tier 2 reconcile for insert/delete-dominant keyless; Tier 3 fail-closed for update-heavy keyless (the proven-impossible case, handled honestly). NEXT: start BUILD ORDER step 1.

### DSQL UNIQUE-INDEX FACTS (verified in DSQL docs — shape Job2's target build for Tier 1)
- DSQL **supports secondary UNIQUE indexes** (not just PK): `CREATE UNIQUE INDEX ASYNC name ON tbl (logical_key_cols)`. So the logical key does NOT have to be the table's PRIMARY KEY.
- **`ASYNC` is MANDATORY** — index builds are always asynchronous. Job2 must CREATE the unique index, then POLL until the async build reports ready BEFORE the CDC upsert relies on it (and before declaring the table load-complete). Do not assume immediate availability.
- **NULLs:** DSQL default is `NULLS DISTINCT` → a unique index permits MULTIPLE NULL key values (breaks ON CONFLICT targeting). MUST declare logical-key columns `NOT NULL` (load-time guard already requires non-null key) or use `NULLS NOT DISTINCT`.
- The UNIQUE index is the Tier-1 mechanism (enables ON CONFLICT) AND the validator (full load fails loud on the first duplicate → catches a wrong logical-key declaration before CDC). Target-side only; no source change. Tier 2/3 have no logical key → no unique index.
- Target-generated key idea is DROPPED for Tier 1: we index the table's EXISTING logical-key column(s) that the CDC record already carries — not an invented column.

### INDEX BUILD SEQUENCE (Tier 1) — decided: index AFTER full load (perf), gate CDC on it (correctness)
Owner requirement: do NOT create the index before the full load (per-row index maintenance on 20M inserts is too slow). Correct order:
1. **Full load into the UNINDEXED target** (fast, no per-row index cost).
2. `CREATE UNIQUE INDEX ASYNC <name> ON <tbl>(<logical_key_cols>)` → returns `job_id` immediately.
3. **Wait for the async build:** `sys.wait_for_job('<job_id>')` (blocks until done/failed) OR poll `sys.jobs WHERE job_id=...` for status. Confirm ready via `pg_index.indisvalid = true`.
4. **Gate:**
   - status `completed` + `indisvalid=t` → index READY → START CDC.
   - status `failed`, detail "Found duplicate key while validating index for UCVs" → declared logical key is NOT unique → **BLOCK the table** with that message (name the duplicate values via the doc's GROUP BY...HAVING COUNT(*)>1 query); do NOT start CDC.
5. **CRITICAL failure cleanup (from DSQL docs):** a FAILED unique-index build leaves the index **INVALID**, and *"DML operations are subject to the uniqueness constraint until you drop the index"* — i.e. the table is stuck for writes. So on failure Job2 MUST `DROP INDEX <name>` BEFORE reporting the table blocked, otherwise the table can't be written/reloaded.
6. Removability: the unique index is target-only + config-gated → `DROP INDEX` any time to disable the Tier-1 path for a table. Fully reversible (owner requirement).
- Async-index monitoring facts: CREATE INDEX ASYNC returns job_id; sys.jobs statuses = submitted/processing/failed/completed (sys.jobs auto-purges completed/failed after 30 min, so capture status promptly); sys.wait_for_job blocks; pg_index.indisvalid is the ready flag.


---

## 10. FULL END-TO-END ORCHESTRATION (owner-described target flow) — to build in phases

Owner described the complete intended pipeline (this session). Current CDK only implements part of Phase 1 (StartDMS→poll→Job1→Job2→Succeed, fixed jobs). The rest is TO BUILD.

### Phase 1 — Full load + initial target build (one state machine)
1. Customer sets up DMS (endpoints, task, S3 target CSV: AddColumnName, TimestampColumnName, Rfc4180).
2. Customer fills config + deploys Step Functions.
3. Start SM → start DMS task → poll until full-load 100% + stopped/completed. [EXISTS]
4. DISCOVERY from DMS statistics (table names, schema, row size) → DYNAMICALLY CREATE multiple v15/v16 load Glue jobs (partition by size). [NOT BUILT — today: fixed Job1+Job2]
5. Job1 (target discovery) runs → then the multiple load jobs run. [partially: Job1/Job2 fixed]
6. On success → validation job (job3_validate). [NOT WIRED into SM]
7. Validation success → resume DMS in CDC mode + confirm CDC started. [NOT IN SM]
8. Then start the CDC Glue job(s) → run indefinitely (up to ~1 week). [CDC job not CDK-managed today]

### Phase 2 — Cutover (operator-initiated; SEPARATE state machine — Option 1 chosen)
9. Customer initiates cutover → stop DMS CDC task → wait for FULL stop.
10. Wait for Glue CDC to CATCH UP to the latest S3 CSV (drain) — signal = compare last CDC file in S3 vs cdc_file_status 'done' in DSQL (a drain-check Lambda querying DSQL).
11. Stop the CDC Glue job(s) (glue:BatchStopJobRun).
12. DROP `_cdc_file` from keyless targets (DROP COLUMN IF EXISTS on target tables) — the cleanup we planned. Only safe here (CDC permanently stopped).

### Cross-cutting: intelligent DMS + Glue failure handling + multi-retry throughout (both phases).

### GAPS vs today (all NOT built):
- Dynamic multi-load-job creation from DMS stats (runtime Lambda creating Glue jobs, or CDK-time?).
- CDC Glue job(s) provisioning + naming (so SM can start/stop them; today foundation makes only Job1/Job2).
- job3_validate wired into the SM.
- DMS resume-to-CDC + confirm step.
- Glue-drain "caught up to latest CSV" check (Lambda querying DSQL cdc_file_status vs S3).
- Cutover state machine (stop DMS CDC → wait stop → wait drain → stop CDC Glue → drop _cdc_file).

### BUILD PLAN (phased, to avoid one giant guess):
- PHASE A (self-contained, needed by the _cdc_file cleanup decision): CUTOVER state machine — stop DMS CDC, wait full stop, wait Glue drain (drain-check Lambda), stop CDC Glue job(s), drop _cdc_file Lambda. CDC job name + drain params from config.
- PHASE B: wire validation job + DMS resume-to-CDC + start-CDC into Phase-1 SM.
- PHASE C: dynamic multi-load-job creation from DMS statistics.
- Throughout: DMS/Glue retry + intelligent failure handling (reuse existing _add_transient_retry + catch-to-terminal + DevOps Agent invoker patterns).
OPEN Qs for owner before building: (1) dynamic job creation at CDK-deploy or runtime-Lambda? (2) should the new CDK also CREATE the CDC Glue job(s)? (3) drop _cdc_file on ALL target tables via DROP IF EXISTS (simplest/safe) vs manifest-derived keyless only?
