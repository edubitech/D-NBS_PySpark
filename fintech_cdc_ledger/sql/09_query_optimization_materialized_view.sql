-- Step 9 (brief) — query optimisation: three-way speed test.
-- Engine: Athena (path 1) + Redshift Serverless (paths 2 and 3), same
-- question asked three ways: active accounts, total balances,
-- credits/debits/transactions per currency per day.

-- ---- path 1: Athena, direct on the Iceberg table via Glue ---------------
-- (run in the Athena console against fintech_db; check "Run time" / "Data
-- scanned" in the query's details panel)
SELECT
    currency,
    business_date,
    count(DISTINCT account_id)  AS active_accounts,
    sum(closing_balance)        AS total_balance,
    sum(credit_volume)          AS total_credits,
    sum(debit_volume)           AS total_debits,
    sum(txn_count)              AS total_transactions
FROM fintech_db.gold_daily_account_summary
GROUP BY currency, business_date;
-- real result: 1,467 ms / 391 KB scanned

-- ---- path 2: Redshift Spectrum, same aggregation over the external schema
SELECT
    currency,
    business_date,
    count(DISTINCT account_id)  AS active_accounts,
    sum(closing_balance)        AS total_balance,
    sum(credit_volume)          AS total_credits,
    sum(debit_volume)           AS total_debits,
    sum(txn_count)              AS total_transactions
FROM spectrum_fintech.gold_daily_account_summary
GROUP BY currency, business_date;
-- real result: 1,546 ms (bytes-scanned not surfaced by the Data API for
-- Spectrum reads — a noted limitation)

-- ---- path 3: Redshift materialized view — the pre-computed answer -------
CREATE MATERIALIZED VIEW mv_exec_daily_close
AUTO REFRESH NO
AS
SELECT
    currency,
    business_date,
    count(DISTINCT account_id)  AS active_accounts,
    sum(closing_balance)        AS total_balance,
    sum(credit_volume)          AS total_credits,
    sum(debit_volume)           AS total_debits,
    sum(txn_count)              AS total_transactions
FROM spectrum_fintech.gold_daily_account_summary
GROUP BY currency, business_date;
-- real result: 61 rows in the view

SELECT * FROM mv_exec_daily_close;
-- real result: 290 ms — ~5x faster than either path reading the underlying
-- files, because this reads the saved 61-row result, not the source data.

-- AUTO REFRESH NO means the view goes stale after every MERGE — it must be
-- refreshed explicitly (this is the REFRESH task the Step 10 Airflow DAG
-- runs after every pipeline re-run):
REFRESH MATERIALIZED VIEW mv_exec_daily_close;

-- ---- verification queries used in this run -------------------------------
SELECT * FROM svv_mv_info WHERE name = 'mv_exec_daily_close';
SELECT * FROM sys_query_history ORDER BY start_time DESC LIMIT 5;
