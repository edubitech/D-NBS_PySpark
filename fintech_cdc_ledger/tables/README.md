# tables/ — real data, exported from the live account

Every file here is a real, full export of that Iceberg table, pulled via
Athena (`SELECT * FROM fintech_db.<table>`) at the point this project's
build finished. Not samples, not mock rows — the actual tables.

| File | Table | Rows | What it is |
|---|---|---|---|
| `bronze_cdc_events.csv` | `bronze_cdc_events` | 53,104 | Every CDC envelope as landed, unfiltered |
| `silver_staged_changes.csv` | `silver_staged_changes` | 27,596 | Deduplicated, one row per transaction per batch |
| `silver_ledger.csv` | `silver_ledger` | 26,418 | The current-state ledger — one row per transaction |
| `gold_daily_account_summary.csv` | `gold_daily_account_summary` | 26,412 | Daily opening/closing balances per account (Step 6.1) |
| `gold_velocity_alerts.csv` | `gold_velocity_alerts` | 0 | Header only — the fraud-speed rule found nothing, for the structural reason documented in the live report (Step 6.2) |
| `gold_settlement_drift.csv` | `gold_settlement_drift` | 31 | Published vs. restated net position, per currency per day (Step 6.3) |
| `gold_pit_balance_compare.csv` | `gold_pit_balance_compare` | 1,194 | Q1's point-in-time balance comparison |

These are a snapshot at export time — re-run the pipeline and these will
change. For the live, current numbers, see the [live report](../README.md#live-report)
or query the tables directly in Athena/Redshift.
