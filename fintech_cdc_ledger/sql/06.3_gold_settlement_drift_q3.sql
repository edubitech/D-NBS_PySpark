-- Step 6.3 (brief) — gold_settlement_drift, answering Q3.
-- jobs/gold_settlement_drift.py on EMR Serverless.
-- Per currency per business date: originally-published net position
-- (Iceberg time travel, snapshot pinned just after day 10's close) versus
-- the currently-restated net position, using the same technique as Step 5.
--
-- SNAPSHOT_AT_CLOSE = 2026-09-07 06:44:44 (real snapshot 4685733960018236392,
-- just after day 10's merge — so "published" only knows days 1-10, the way
-- a real close report only covers days that had already happened).

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.gold_settlement_drift (
    currency          STRING,
    business_date     DATE,
    published_net     DECIMAL(18,2),
    restated_net      DECIMAL(18,2),
    drift_amount      DECIMAL(18,2),
    drift_pct         DECIMAL(9,4),
    computed_at       TIMESTAMP
) USING iceberg
PARTITIONED BY (business_date)
TBLPROPERTIES ('format-version' = '2');

WITH published AS (
    SELECT currency, to_date(event_timestamp) AS business_date,
           sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END) AS net
    FROM glue_catalog.fintech_db.silver_ledger TIMESTAMP AS OF '2026-09-07 06:44:44'
    WHERE NOT is_deleted GROUP BY currency, to_date(event_timestamp)
),
restated AS (
    SELECT currency, to_date(event_timestamp) AS business_date,
           sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END) AS net
    FROM glue_catalog.fintech_db.silver_ledger
    WHERE NOT is_deleted GROUP BY currency, to_date(event_timestamp)
)
SELECT
    p.currency, p.business_date,
    round(p.net, 2)                              AS published_net,
    round(r.net, 2)                               AS restated_net,
    round(r.net - p.net, 2)                        AS drift_amount,
    round((r.net - p.net) / abs(p.net), 4)         AS drift_pct
FROM published p
JOIN restated r ON p.currency = r.currency AND p.business_date = r.business_date;

-- real result (Evidence item 12 / Q3): 3 material currency-days of 31
-- checked (threshold 0.1%) — USD 2026-01-10 (+$1,187,154.99, 4.88%),
-- GBP 2026-01-10 (+$1,920,765.10, 4.65%), EUR 2026-01-10 (+$92,656.55, 0.29%)
SELECT currency, business_date, published_net, restated_net, drift_amount, drift_pct
FROM glue_catalog.fintech_db.gold_settlement_drift
WHERE abs(drift_pct) > 0.001
ORDER BY abs(drift_amount) DESC;
