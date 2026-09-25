# DMS → S3 → Glue → Aurora DSQL Migration Pipeline

A schema-agnostic pipeline that migrates relational data (validated against Oracle sources)
into **Amazon Aurora DSQL** using **AWS DMS** for extraction to S3 (CSV), and **AWS Glue**
for load, validation, and continuous CDC apply.

Flow: **Source DB → DMS (full load + CDC) → S3 (CSV) → Glue (discover → load → validate → CDC) → Aurora DSQL**

## Repository layout

```
manual_kit/                 Operator deployment kit (deploy + run the pipeline by hand)
  scripts/                  The 4 Glue job scripts (stage these to s3://<bucket>/scripts/)
    job1_discovery.py         Discover target schema + build per-table type/PK metadata
    job2_load.py              Full-load apply (Spark, driver-side pg8000) into DSQL — large tables load via per-file parallelism (250MB files, up to 30 in parallel)
    job3_validate.py          Post-load validation (per-range count compare; optional content checksum)
    glue_cdc_continuous.py    Continuous CDC apply loop (long-running Glue job)
  lambdas/                  Orchestration lambdas (resolve_task, plan_split, create_glue_jobs,
                            driver_discovery, drain_check, stop_cdc_run, drop_tags)
  stepfunctions/            startup + cutover state machines (ASL)
  glue-templates/           Glue job definition templates
  iam/                      Role trust + policy documents
  RUNBOOK.md                Step-by-step deploy + operate guide
  USAGE_GUIDE.md            End-to-end operational usage
  ENGINEERING_RECORD.md     Architecture, every bug found + fix, DDL support matrix
  CDC_EDGE_CASE_RESULTS.md  CDC edge-case + data-type limitations (with fixes)

preflight/                  Pre-migration assessment SQL for Oracle sources
  oracle_cdc_readiness.sql            CDC go/no-go prerequisite check
  oracle_migration_assessment.sql     Single-schema assessment
  oracle_migration_assessment_all.sql All non-system schemas
  oracle_table_sizing_csv.sql         CSV sizing export
  README.md

docs/
  NO_PK_CDC_UPDATE_TRACKING.md        Design note: no-PK CDC update tracking
  architecture.png                    Architecture diagram
```

## Getting started

1. Run the **preflight** assessment against the source (see `preflight/README.md`) to
   confirm CDC readiness and size the migration.
2. Follow **`manual_kit/RUNBOOK.md`** to stage scripts/drivers to S3, create the Glue jobs,
   and run the pipeline (full load → validate → CDC → cutover).
3. See **`manual_kit/ENGINEERING_RECORD.md`** for the design, known limitations, and the
   DDL support matrix, and **`manual_kit/CDC_EDGE_CASE_RESULTS.md`** for CDC data-type edge cases.

## Known limitations (see ENGINEERING_RECORD / CDC_EDGE_CASE_RESULTS for detail)

- **DROP COLUMN** during CDC blocks the affected table (resumable after operator remediation).
- **In-place CHANGE DATA TYPE** is not detected by the header-diff schema reconciliation.
- Validation range-checks skip composite / fractional-numeric PK tables (count-only for those).
- CDC requires the source schema to exist in the Oracle LogMiner dictionary before capture
  (a schema created after the dictionary build needs a DBA dictionary rebuild).

## Notes

- Glue scripts are plain Python/PySpark; the CDC job depends on DSQL drivers (+ boto3/botocore)
  staged as per-job driver folders in S3 — see the RUNBOOK Step 0.
- No credentials or account-specific values are committed; supply your own via the config /
  runtime arguments described in the RUNBOOK.
