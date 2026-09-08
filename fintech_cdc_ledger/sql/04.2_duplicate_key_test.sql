-- Step 4.2 (brief deliverable) — the duplicate-key MERGE abort.
-- jobs/duplicate_key_test.py on EMR Serverless. Writes TWO rows for the
-- same transaction_id into one staged_changes batch (extract_date =
-- 2099-01-02, source_lsn 111111 and 222222), then runs the exact same
-- production MERGE. Iceberg must abort rather than guess which row was
-- meant — this is the correctness guarantee working, not a bug.

MERGE INTO glue_catalog.fintech_db.silver_ledger t
USING staged_changes s          -- 2 rows, same transaction_id, one batch
   ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.cdc_operation = 'd'
   THEN UPDATE SET t.is_deleted = true, t._updated_at = current_timestamp()
WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
              AND s.source_ts > t.event_timestamp
   THEN UPDATE SET
        t.amount = s.amount, t.status = s.status, t.currency = s.currency,
        t.event_timestamp = s.source_ts, t.source_lsn = s.source_lsn,
        t._updated_at = current_timestamp()
WHEN NOT MATCHED AND s.cdc_operation <> 'd'
   THEN INSERT (transaction_id, account_id, amount, currency, tx_type,
                status, event_timestamp, source_lsn, is_deleted, _updated_at)
        VALUES (s.transaction_id, s.account_id, s.amount, s.currency, s.tx_type,
                s.status, s.source_ts, s.source_lsn, false, current_timestamp());

-- REAL error text raised by the statement above (Evidence item 7 / break log #1):
--
-- org.apache.spark.SparkRuntimeException: [MERGE_CARDINALITY_VIOLATION] The ON
-- search condition of the MERGE statement matched a single row from the target
-- table with multiple rows of the source table.
--
-- expected result: ledger row for the test transaction_id is UNCHANGED —
-- the MERGE aborted before writing anything.
SELECT transaction_id, amount FROM glue_catalog.fintech_db.silver_ledger
WHERE transaction_id = 'T-000000000';   -- real transaction_id used in this run
