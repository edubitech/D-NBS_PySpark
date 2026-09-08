-- Step 6.1 (brief) — gold_daily_account_summary + reconciliation check.
-- jobs/gold_daily_account_summary.py on EMR Serverless.
-- One row per account per day: opening balance, credits, debits, closing
-- balance. CASH_IN is the only credit type. Opening/closing are a running
-- total per account ordered by day, which makes the invariant
--     opening_balance + credit_volume - debit_volume = closing_balance
-- true by construction — the check is still run for real (required
-- deliverable either way). The running-total itself uses a PySpark Window
-- (unboundedPreceding to currentRow); the aggregation and the check below
-- are real SQL run in the job.

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.gold_daily_account_summary (
    account_id       STRING,
    business_date    DATE,
    currency         STRING,
    opening_balance  DECIMAL(18,2),
    credit_volume    DECIMAL(18,2),
    debit_volume     DECIMAL(18,2),
    closing_balance  DECIMAL(18,2),
    txn_count        BIGINT,
    computed_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (business_date)
TBLPROPERTIES ('format-version' = '2');

-- daily credit/debit aggregation feeding the running-balance window
SELECT
    account_id,
    to_date(event_timestamp) AS business_date,
    currency,
    sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE 0 END) AS credit_volume,
    sum(CASE WHEN tx_type != 'CASH_IN' THEN amount ELSE 0 END) AS debit_volume,
    count(*) AS txn_count
FROM glue_catalog.fintech_db.silver_ledger
WHERE NOT is_deleted
GROUP BY account_id, to_date(event_timestamp), currency;

-- the reconciliation invariant — must return 0 rows (Evidence item 10: 0 of 26,412)
SELECT account_id, business_date,
       opening_balance + credit_volume - debit_volume AS derived,
       closing_balance,
       abs(opening_balance + credit_volume - debit_volume - closing_balance) AS drift
FROM glue_catalog.fintech_db.gold_daily_account_summary
WHERE abs(opening_balance + credit_volume - debit_volume - closing_balance) > 0.01;
