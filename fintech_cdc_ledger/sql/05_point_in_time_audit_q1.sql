-- Step 5 (brief) — Point-in-time audit, answering Q1.
-- jobs/point_in_time_audit.py on EMR Serverless.
-- "As we knew it on day 1's close" vs "as we now know it, including later
-- corrections" — both numbers are legitimate; they differ by exactly the
-- late-adjustment impact.
--
-- AS_OF (business cutoff)   = 2026-01-01 23:59:59
-- SNAPSHOT_PIN (wall-clock) = 2026-09-06 08:08:24
--   (real snapshot 2697762506882925537, committed_at 2026-09-06 08:08:23.285 UTC)
--
-- Gotcha: TIMESTAMP AS OF resolves to the newest snapshot strictly OLDER
-- than the value given — never pass a snapshot's own committed_at.

CREATE TABLE IF NOT EXISTS glue_catalog.fintech_db.gold_pit_balance_compare (
    account_id           STRING,
    balance_as_known     DECIMAL(18,2),
    balance_as_restated  DECIMAL(18,2),
    delta                DECIMAL(18,2),
    as_of_ts             TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

WITH as_known AS (
    SELECT account_id,
           sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END) AS balance_as_known
    FROM glue_catalog.fintech_db.silver_ledger TIMESTAMP AS OF '2026-09-06 08:08:24'
    WHERE NOT is_deleted AND event_timestamp <= TIMESTAMP '2026-01-01 23:59:59'
    GROUP BY account_id
),
as_restated AS (
    SELECT account_id,
           sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END) AS balance_as_restated
    FROM glue_catalog.fintech_db.silver_ledger
    WHERE NOT is_deleted AND event_timestamp <= TIMESTAMP '2026-01-01 23:59:59'
    GROUP BY account_id
)
SELECT
    coalesce(k.account_id, r.account_id)         AS account_id,
    coalesce(k.balance_as_known, 0)               AS balance_as_known,
    coalesce(r.balance_as_restated, 0)            AS balance_as_restated,
    coalesce(r.balance_as_restated, 0) - coalesce(k.balance_as_known, 0) AS delta,
    TIMESTAMP '2026-01-01 23:59:59'               AS as_of_ts
FROM as_known k
FULL OUTER JOIN as_restated r ON k.account_id = r.account_id;

-- Q1 real answer (Evidence item 8-9): top account by |delta| —
-- C197491520: as_known -$3,776,389.09 -> as_restated $0.00 (delta +$3,776,389.09,
-- a full chargeback). Four more accounts with full chargebacks: C458998685
-- (+$1,127,058.69), C883678948 (+$789,419.02), C345290829 (+$668,237.36),
-- C566891420 (+$539,430.95).
SELECT account_id, balance_as_known, balance_as_restated, delta
FROM glue_catalog.fintech_db.gold_pit_balance_compare
ORDER BY abs(delta) DESC LIMIT 5;
