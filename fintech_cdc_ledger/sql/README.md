# sql/ — every real query, by brief step

One file per step (sub-steps get a dot: `4.1`, `4.2`, `6.1`-`6.3`). Every
statement here actually ran against this account — pulled from the `jobs/`
scripts, `airflow/dags/fintech_cdc_pipeline.py`, and the ad-hoc Redshift/
Athena queries used for Steps 8-10 (those weren't saved as script files
during the live run, so they're reconstructed here using the real table
names, columns, IAM role, and identifiers this project actually used —
not invented ones). Real measured results are noted as comments next to
the query that produced them.

| File | Step | Engine | What it does |
|---|---|---|---|
| `01_bronze_ingest.sql` | 1 | Spark SQL (EMR Serverless) | Create `bronze_cdc_events`, check its latest snapshot |
| `02_silver_staged_changes_dedup.sql` | 2 | Spark SQL (EMR Serverless) | Create `silver_staged_changes`; dedup logic (SQL-equivalent of the job's Window API) |
| `03_silver_ledger_ddl.sql` | 3 | Spark SQL (EMR Serverless) | Create `silver_ledger` with its partitioning/merge-on-read/sort-order choices |
| `04_merge_cdc_upserts.sql` | 4 | Spark SQL (EMR Serverless) | The production CDC `MERGE INTO` |
| `04.1_guard_test_late_arrival.sql` | 4.1 | Spark SQL (EMR Serverless) | Proves the out-of-order guard — ledger must not move |
| `04.2_duplicate_key_test.sql` | 4.2 | Spark SQL (EMR Serverless) | Proves the duplicate-key MERGE abort — real error text included |
| `05_point_in_time_audit_q1.sql` | 5 | Spark SQL (EMR Serverless) | Q1 — balance as-known vs. as-restated, via `TIMESTAMP AS OF` |
| `06.1_gold_daily_account_summary.sql` | 6.1 | Spark SQL (EMR Serverless) | Daily account summary + the reconciliation invariant check |
| `06.2_gold_velocity_alerts_q2.sql` | 6.2 | Spark SQL (EMR Serverless) | Q2 — rolling 15-minute velocity rule |
| `06.3_gold_settlement_drift_q3.sql` | 6.3 | Spark SQL (EMR Serverless) | Q3 — published vs. restated net position by currency/day |
| `07_table_maintenance.sql` | 7 | Spark SQL (EMR Serverless) | Compaction (plain, then the `rewrite-all` fix), manifests, retention, orphans |
| `08_redshift_spectrum_setup.sql` | 8 | Redshift Serverless | `CREATE EXTERNAL SCHEMA` wiring Redshift to the same Glue catalog, zero-copy |
| `09_query_optimization_materialized_view.sql` | 9 | Athena + Redshift Serverless | The 3-way speed test, and the materialized view that wins it |
| `10_executive_reporting_and_idempotency.sql` | 10 | Redshift Serverless + Athena | Q1/Q2/Q3 answered directly in Redshift; the before/after idempotency query |

To actually run any of these: Steps 1-7 need Spark/EMR Serverless with
Iceberg's `glue_catalog` wired in (see `airflow/dags/fintech_config.py`'s
`spark_submit()` for the exact `--conf` flags every one of these was run
with). Steps 8-10 run in Redshift's Query Editor v2 or the Data API,
against database `fintech`, workgroup `ali-fintech-exam-rs-wg`.
