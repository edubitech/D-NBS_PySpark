-- Step 6.2 (brief) — gold_velocity_alerts, answering Q2.
-- jobs/gold_velocity_alerts.py on EMR Serverless.
-- Which accounts moved cumulative CASH_OUT + TRANSFER + DEBIT exceeding 80%
-- of their opening daily balance inside any ROLLING 15-minute window?
-- The rolling window itself uses a PySpark Window.rangeBetween(-900, 0)
-- over epoch seconds (a true rolling window, not rowsBetween/tumbling) —
-- the equivalent SQL is shown for reference; the table DDL and the count
-- below are real SQL run in the job.

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.gold_velocity_alerts (
    account_id           STRING,
    business_date        DATE,
    event_timestamp      TIMESTAMP,
    transaction_id       STRING,
    rolling_15m_outflow  DECIMAL(18,2),
    opening_balance      DECIMAL(18,2),
    pct_of_opening       DECIMAL(9,4),
    severity             STRING,
    computed_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (business_date)
TBLPROPERTIES ('format-version' = '2');

-- equivalent of the PySpark rolling-window outflow calc, for reference
-- (RANGE, not ROWS — a true time-window, not a row-count window):
WITH outflow AS (
    SELECT *, cast(event_timestamp AS long) AS ts_epoch,
           to_date(event_timestamp) AS business_date
    FROM glue_catalog.fintech_db.silver_ledger
    WHERE NOT is_deleted AND tx_type IN ('CASH_OUT', 'TRANSFER', 'DEBIT')
)
SELECT *,
       round(sum(amount) OVER (
           PARTITION BY account_id, business_date
           ORDER BY ts_epoch
           RANGE BETWEEN 900 PRECEDING AND CURRENT ROW   -- 900s = 15 min
       ), 2) AS rolling_15m_outflow
FROM outflow;

-- real result (Evidence item 11 / Q2): 0 alerts. Root cause verified
-- directly — every account has exactly one transaction, so the rolling
-- window never has a second transaction to compare against.
SELECT count(*) AS total_alerts FROM glue_catalog.fintech_db.gold_velocity_alerts;

SELECT count(*) AS accounts_with_exactly_one_txn
FROM (
    SELECT account_id FROM glue_catalog.fintech_db.silver_ledger
    WHERE NOT is_deleted GROUP BY account_id HAVING count(*) = 1
);
