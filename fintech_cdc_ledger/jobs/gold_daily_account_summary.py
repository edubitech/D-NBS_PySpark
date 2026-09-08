"""
Step 6a (brief) — gold_daily_account_summary, and the reconciliation check.

One row per account per day: opening balance, money in, money out, closing
balance. CASH_IN is the only credit type (matches the same convention used
throughout this project, incl. point_in_time_audit.py); everything else
(CASH_OUT, PAYMENT, TRANSFER, DEBIT) is a debit.

Opening/closing balances are derived from one running total per account,
ordered by day -- this makes the reconciliation invariant

    opening_balance + credit_volume - debit_volume = closing_balance

true by construction, not by a separate balancing step. The check below is
still run for real and its result is a required deliverable either way.

Usage:
    spark-submit gold_daily_account_summary.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

CATALOG = "glue_catalog"
DB = "fintech_db"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_daily_account_summary").getOrCreate()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.gold_daily_account_summary (
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
        TBLPROPERTIES ('format-version' = '2')
    """)

    ledger = (spark.table(f"{CATALOG}.{DB}.silver_ledger")
              .where(~F.col("is_deleted"))
              .withColumn("business_date", F.to_date("event_timestamp")))

    daily = (ledger.groupBy("account_id", "business_date", "currency")
             .agg(
                 F.sum(F.when(F.col("tx_type") == "CASH_IN", F.col("amount")).otherwise(0))
                  .alias("credit_volume"),
                 F.sum(F.when(F.col("tx_type") != "CASH_IN", F.col("amount")).otherwise(0))
                  .alias("debit_volume"),
                 F.count("*").alias("txn_count")))

    w = Window.partitionBy("account_id").orderBy("business_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary = (daily
        .withColumn("daily_net", F.col("credit_volume") - F.col("debit_volume"))
        .withColumn("closing_balance", F.round(F.sum("daily_net").over(w), 2))
        .withColumn("opening_balance", F.round(F.col("closing_balance") - F.col("daily_net"), 2))
        .withColumn("credit_volume", F.round("credit_volume", 2))
        .withColumn("debit_volume", F.round("debit_volume", 2))
        .withColumn("computed_at", F.current_timestamp())
        .select("account_id", "business_date", "currency", "opening_balance",
                "credit_volume", "debit_volume", "closing_balance", "txn_count", "computed_at"))

    summary.writeTo(f"{CATALOG}.{DB}.gold_daily_account_summary").overwritePartitions()

    row_count = summary.count()
    print(f"GOLD_DAILY_SUMMARY rows={row_count}")

    # the reconciliation invariant -- must return 0 rows
    failing = spark.sql(f"""
        SELECT account_id, business_date,
               opening_balance + credit_volume - debit_volume AS derived,
               closing_balance,
               abs(opening_balance + credit_volume - debit_volume - closing_balance) AS drift
        FROM {CATALOG}.{DB}.gold_daily_account_summary
        WHERE abs(opening_balance + credit_volume - debit_volume - closing_balance) > 0.01
    """)
    fail_count = failing.count()
    print(f"RECONCILIATION rows_failing={fail_count}")
    if fail_count > 0:
        failing.show(20, truncate=False)

    spark.stop()
