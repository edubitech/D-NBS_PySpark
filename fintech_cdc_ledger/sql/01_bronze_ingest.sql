-- Step 1 (brief) — Bronze ingestion.
-- Engine: Spark SQL, run inside jobs/bronze_ingest.py on EMR Serverless
-- (application fintech-cdc-spark-app / 00g8g90j3dqrmi09).
-- The JSON -> column mapping (op, source.ts_ms, before/after, etc.) happens
-- in PySpark's DataFrame API, not in SQL — see jobs/bronze_ingest.py.

CREATE DATABASE IF NOT EXISTS glue_catalog.fintech_db;

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.bronze_cdc_events (
    cdc_operation   STRING,
    source_ts       TIMESTAMP,
    arrival_ts      TIMESTAMP,
    source_lsn      BIGINT,
    before          STRING,
    after           STRING,
    _src_file       STRING,
    _ingested_at    TIMESTAMP,
    _batch_id       STRING,
    extract_date    DATE
) USING iceberg
PARTITIONED BY (extract_date)
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

-- run after each day's load, to capture the real snapshot id + row count
-- (Evidence item 3: day-5 load 1 snapshot 2476461427939435882,
--  load 2 snapshot 6151183589631536160, both 2,703 rows)
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM glue_catalog.fintech_db.bronze_cdc_events.snapshots
ORDER BY committed_at DESC LIMIT 1;
