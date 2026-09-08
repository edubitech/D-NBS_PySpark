-- Step 3 (brief) — Silver schema DDL. jobs/silver_ddl.py on EMR Serverless.
-- One row per transaction_id, current state. No data movement here — the
-- Step 4 MERGE populates it. Four defended design choices: hidden day()
-- partitioning, bucket(16, account_id) to cap high-cardinality account_id,
-- merge-on-read (cheap for small CDC updates), hash distribution mode.

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.silver_ledger (
    transaction_id  STRING,
    account_id      STRING,
    amount          DECIMAL(18,2),
    currency        STRING,
    tx_type         STRING,
    status          STRING,
    event_timestamp TIMESTAMP,
    source_lsn      BIGINT,
    is_deleted      BOOLEAN,
    _updated_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (day(event_timestamp), bucket(16, account_id))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.delete.mode'            = 'merge-on-read',
    'write.update.mode'            = 'merge-on-read',
    'write.merge.mode'             = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode'      = 'hash'
);

ALTER TABLE glue_catalog.fintech_db.silver_ledger
WRITE ORDERED BY account_id, event_timestamp DESC;

DESCRIBE TABLE EXTENDED glue_catalog.fintech_db.silver_ledger;
