-- Step 4.1 (brief deliverable) — prove the out-of-order guard.
-- jobs/guard_test.py on EMR Serverless. Injects one synthetic late-arriving
-- event for a real, already-settled transaction: source_ts backdated 7 days
-- before the ledger's current event_timestamp, wrong amount ($1.00).
-- Tagged extract_date = 2099-01-01 so it can never be mistaken for a real
-- trading day. The row construction (JSON envelope) happens in PySpark;
-- the guard itself is proven by running the exact same production MERGE:

MERGE INTO glue_catalog.fintech_db.silver_ledger t
USING staged_changes s          -- one synthetic row, extract_date = 2099-01-01
   ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.cdc_operation = 'd'
   THEN UPDATE SET t.is_deleted = true, t._updated_at = current_timestamp()
WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
              AND s.source_ts > t.event_timestamp        -- <- the guard
   THEN UPDATE SET
        t.amount = s.amount, t.status = s.status, t.currency = s.currency,
        t.event_timestamp = s.source_ts, t.source_lsn = s.source_lsn,
        t._updated_at = current_timestamp()
WHEN NOT MATCHED AND s.cdc_operation <> 'd'
   THEN INSERT (transaction_id, account_id, amount, currency, tx_type,
                status, event_timestamp, source_lsn, is_deleted, _updated_at)
        VALUES (s.transaction_id, s.account_id, s.amount, s.currency, s.tx_type,
                s.status, s.source_ts, s.source_lsn, false, current_timestamp());

-- expected result: ledger row for the test transaction_id is UNCHANGED —
-- amount and event_timestamp identical before and after this MERGE, because
-- source_ts (7 days in the past) never satisfies s.source_ts > t.event_timestamp.
SELECT transaction_id, amount, status, event_timestamp
FROM glue_catalog.fintech_db.silver_ledger
WHERE transaction_id = 'T-000000000';   -- real transaction_id used in this run
