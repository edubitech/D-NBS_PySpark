"""
Step 4 (brief) — CDC upserts. Runs on EMR Serverless (Spark).

MERGE INTO silver_ledger from silver_staged_changes for one day. Two things
that will break if you remove them (documented, not accidental):

  - the s.source_ts > t.event_timestamp guard: without it, a late-arriving
    stale event can clobber a newer value already in the ledger.
  - a duplicate transaction_id in staged_changes: MERGE aborts rather than
    guessing which row you meant. Step 2's dedup is what prevents this from
    ever reaching here.

Usage:
    spark-submit merge_ledger.py --run-date 2026-01-01
"""
import argparse

from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"


def main(run_date: str) -> None:
    spark = SparkSession.builder.appName(f"merge_ledger_{run_date}").getOrCreate()

    staged = spark.table(f"{CATALOG}.{DB}.silver_staged_changes").where(
        F.col("extract_date") == run_date)
    staged.createOrReplaceTempView("staged_changes")

    before_total = spark.table(f"{CATALOG}.{DB}.silver_ledger").count()

    spark.sql(f"""
        MERGE INTO {CATALOG}.{DB}.silver_ledger t
        USING staged_changes s
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
                        s.status, s.source_ts, s.source_lsn, false, current_timestamp())
    """)

    ledger = spark.table(f"{CATALOG}.{DB}.silver_ledger")
    after_total = ledger.count()
    deleted_count = ledger.where(F.col("is_deleted")).count()
    staged_count = staged.count()
    staged_delete_unmatched = staged.where(F.col("cdc_operation") == "d").count()

    print(f"MERGE_LEDGER run_date={run_date} "
          f"before_total={before_total} after_total={after_total} "
          f"staged_rows={staged_count} is_deleted_flagged={deleted_count} "
          f"staged_delete_events={staged_delete_unmatched}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    main(args.run_date)
