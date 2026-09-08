-- Step 10 (brief) — executive reporting & final validation.
-- 10a: answer all 3 exam questions directly in Redshift SQL.
-- 10b: re-run the entire 20-day pipeline (see ../airflow/dags/fintech_cdc_pipeline.py)
-- and prove the gold totals don't move (idempotency).

-- ---- 10a. Q1 — point-in-time balance, in Redshift ------------------------
SELECT account_id, balance_as_known, balance_as_restated, delta
FROM spectrum_fintech.gold_pit_balance_compare
WHERE account_id = 'C197491520'
  AND as_of_ts = TIMESTAMP '2026-01-01 23:59:59';
-- real result: as-known -$3,776,389.09 -> as-restated $0.00 (delta +$3,776,389.09)

-- ---- 10a. Q2 — velocity breaches, in Redshift ----------------------------
SELECT count(*) AS breach_count FROM spectrum_fintech.gold_velocity_alerts;
-- real result: 0 rows — consistent with Step 6.2's structural finding
-- (every account has exactly one transaction)

-- ---- 10a. Q3 — settlement drift by currency, in Redshift -----------------
-- The brief's own SQL uses count(*) FILTER (WHERE ...) — valid in
-- Postgres/Presto/Athena, but Redshift's dialect doesn't support the
-- FILTER clause at all (syntax error at or near "("). Rewritten below as
-- count(CASE WHEN ... THEN 1 END), which every engine accepts.
SELECT
    currency,
    sum(drift_amount) AS total_drift,
    count(CASE WHEN abs(drift_pct) > 0.001 THEN 1 END) AS material_days
FROM spectrum_fintech.gold_settlement_drift
GROUP BY currency
ORDER BY total_drift DESC;
-- real result: GBP +$1,920,765.10 (1 material day) · USD +$1,190,194.48 (1)
-- · EUR +$93,710.34 (1)

-- ---- 10b. idempotency check — before/after gold totals -------------------
-- Run via Athena (AthenaHook) inside the Airflow DAG's capture_before_totals
-- / capture_after_totals tasks — see ../airflow/dags/fintech_cdc_pipeline.py.
-- Same query, run once before the full 20-day re-run and once after; the
-- DAG's assert_idempotent task fails loudly if the two strings differ.
SELECT count(*) || '|' || sum(closing_balance) || '|' || count(DISTINCT account_id)
FROM fintech_db.gold_daily_account_summary;

-- real result, both times (after correcting a stale-baseline mistake on the
-- first attempt — see EVIDENCE.md "Two more real findings from Step 10"):
-- 26,412 | -1941582100.41 | 26,412 — identical to the cent.

-- refresh the materialized view so it reflects the re-run's final state
-- (also a task in the Airflow DAG, run right after table_maintenance):
REFRESH MATERIALIZED VIEW mv_exec_daily_close;
