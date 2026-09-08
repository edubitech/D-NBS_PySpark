-- Step 2 (brief) — Deduplication & event ordering.
-- Engine: Spark SQL + DataFrame API, jobs/dedup_events.py on EMR Serverless.
-- The dedup itself (row_number() OVER (PARTITION BY transaction_id
-- ORDER BY source_ts DESC, source_lsn DESC)) is expressed with the
-- PySpark Window API, not a SQL string — the equivalent SQL is shown
-- below for reference; it produces the identical result the job measured
-- (Evidence item 5: day 1, 2,341 rows in -> 1,198 out, ratio 0.5117;
--  item 6: 1,143 intra-batch duplicate keys).

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.silver_staged_changes (
    transaction_id  STRING,
    account_id      STRING,
    amount          DECIMAL(18,2),
    currency        STRING,
    tx_type         STRING,
    status          STRING,
    cdc_operation   STRING,
    source_ts       TIMESTAMP,
    source_lsn      BIGINT,
    extract_date    DATE
) USING iceberg
PARTITIONED BY (extract_date)
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

-- equivalent of the PySpark Window dedup, for reference:
WITH extracted AS (
    SELECT
        coalesce(get_json_object(after, '$.transaction_id'),
                 get_json_object(before, '$.transaction_id'))            AS transaction_id,
        get_json_object(after, '$.account_id')                            AS account_id,
        cast(coalesce(get_json_object(after, '$.amount'),
                      get_json_object(before, '$.amount')) AS decimal(18,2)) AS amount,
        upper(get_json_object(after, '$.currency'))                       AS currency,
        get_json_object(after, '$.tx_type')                               AS tx_type,
        coalesce(upper(get_json_object(after, '$.status')),
                 upper(get_json_object(before, '$.status')))              AS status,
        cdc_operation, source_ts, source_lsn, extract_date
    FROM glue_catalog.fintech_db.bronze_cdc_events
    WHERE extract_date = DATE '2026-01-01'   -- one run-date per job invocation
),
ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY transaction_id
               ORDER BY source_ts DESC, source_lsn DESC
           ) AS rn
    FROM extracted
)
SELECT transaction_id, account_id, amount, currency, tx_type, status,
       cdc_operation, source_ts, source_lsn, extract_date
FROM ranked
WHERE rn = 1;

-- intra-batch duplicate key count (Evidence item 6)
SELECT count(*) AS duplicate_keys FROM (
    SELECT transaction_id FROM extracted GROUP BY transaction_id HAVING count(*) > 1
);
