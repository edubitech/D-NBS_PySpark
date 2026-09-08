-- Step 4 (brief) — CDC upserts. jobs/merge_ledger.py on EMR Serverless.
-- Run once per day (or via jobs/process_range.py for a range in one Spark
-- session). Two deliberate safety properties:
--   - s.source_ts > t.event_timestamp guard: blocks a late-arriving stale
--     event from clobbering a newer value already in the ledger.
--   - a duplicate transaction_id in the source aborts the whole MERGE
--     (see 04.2_duplicate_key_test.sql) rather than silently picking one.

MERGE INTO glue_catalog.fintech_db.silver_ledger t
USING staged_changes s          -- = silver_staged_changes for one extract_date
   ON t.transaction_id = s.transaction_id

WHEN MATCHED AND s.cdc_operation = 'd'
   THEN UPDATE SET t.is_deleted = true, t._updated_at = current_timestamp()

WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
              AND s.source_ts > t.event_timestamp
   THEN UPDATE SET
        t.amount          = s.amount,
        t.status          = s.status,
        t.currency        = s.currency,
        t.event_timestamp = s.source_ts,
        t.source_lsn      = s.source_lsn,
        t._updated_at     = current_timestamp()

WHEN NOT MATCHED AND s.cdc_operation <> 'd'
   THEN INSERT (transaction_id, account_id, amount, currency, tx_type,
                status, event_timestamp, source_lsn, is_deleted, _updated_at)
        VALUES (s.transaction_id, s.account_id, s.amount, s.currency, s.tx_type,
                s.status, s.source_ts, s.source_lsn, false, current_timestamp());

-- reconciliation counters printed by the job (real numbers per run-date are
-- in the live report's Step 4 "how to verify" tables)
SELECT count(*) AS after_total FROM glue_catalog.fintech_db.silver_ledger;
SELECT count(*) AS deleted_count FROM glue_catalog.fintech_db.silver_ledger WHERE is_deleted;
