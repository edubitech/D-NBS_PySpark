"""
Step 4 deliverable (brief) — prove the out-of-order guard.

Injects one synthetic late-arriving event for a real, already-SETTLED
transaction: source_ts backdated to before the ledger's current
event_timestamp, and a deliberately wrong amount. Writes it to bronze (so
it's on the permanent record) AND to silver_staged_changes (skipping a full
dedup run since it's the only event in this one-row synthetic batch), tagged
with extract_date='2099-01-01' so it's unmistakably a test batch, never a
real trading day.

Then runs the same MERGE used in production. If the guard
(s.source_ts > t.event_timestamp) works, the ledger must not move.

Usage:
    spark-submit guard_test.py --transaction-id T-000000000
"""
import argparse

from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"
TEST_BATCH_DATE = "2099-01-01"
WRONG_AMOUNT = 1.00


def main(transaction_id: str) -> None:
    spark = SparkSession.builder.appName("guard_test").getOrCreate()

    before = spark.table(f"{CATALOG}.{DB}.silver_ledger").where(
        F.col("transaction_id") == transaction_id).collect()[0]
    print(f"GUARD_TEST before: amount={before['amount']} status={before['status']} "
          f"event_timestamp={before['event_timestamp']}")

    late_source_ts = before["event_timestamp"] - F.expr("interval 7 days")
    late_ts_literal = spark.sql(
        f"SELECT (TIMESTAMP '{before['event_timestamp']}' - INTERVAL 7 DAYS) AS ts"
    ).collect()[0]["ts"]
    print(f"GUARD_TEST injecting source_ts={late_ts_literal} "
          f"(7 days before current ledger value), wrong amount={WRONG_AMOUNT}")

    bronze_row = spark.createDataFrame([{
        "cdc_operation": "u",
        "source_ts": late_ts_literal,
        "source_lsn": 999999999,
        "after": (
            f'{{"transaction_id":"{transaction_id}","account_id":"{before["account_id"]}",'
            f'"amount":{WRONG_AMOUNT},"currency":"{before["currency"]}",'
            f'"tx_type":"{before["tx_type"]}","status":"SETTLED"}}'
        ),
        "_src_file": "guard_test_synthetic",
        "_batch_id": "guard-test-late-arrival",
        "extract_date": TEST_BATCH_DATE,
    }])
    bronze_row = bronze_row.withColumn("before", F.lit(None).cast("string")) \
                            .withColumn("arrival_ts", F.current_timestamp()) \
                            .withColumn("_ingested_at", F.current_timestamp()) \
                            .withColumn("extract_date", F.col("extract_date").cast("date"))
    bronze_row.select(
        "cdc_operation", "source_ts", "arrival_ts", "source_lsn", "before", "after",
        "_src_file", "_ingested_at", "_batch_id", "extract_date",
    ).writeTo(f"{CATALOG}.{DB}.bronze_cdc_events").append()
    print("GUARD_TEST synthetic row appended to bronze_cdc_events "
          f"(extract_date={TEST_BATCH_DATE}, _batch_id=guard-test-late-arrival)")

    staged_row = spark.createDataFrame([{
        "transaction_id": transaction_id,
        "account_id": before["account_id"],
        "amount": WRONG_AMOUNT,
        "currency": before["currency"],
        "tx_type": before["tx_type"],
        "status": "SETTLED",
        "cdc_operation": "u",
        "source_ts": late_ts_literal,
        "source_lsn": 999999999,
        "extract_date": TEST_BATCH_DATE,
    }])
    staged_row = staged_row.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
                            .withColumn("extract_date", F.col("extract_date").cast("date"))
    staged_row.writeTo(f"{CATALOG}.{DB}.silver_staged_changes").overwritePartitions()
    print("GUARD_TEST synthetic row written to silver_staged_changes "
          f"(extract_date={TEST_BATCH_DATE})")

    staged_row.createOrReplaceTempView("staged_changes")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{DB}.silver_ledger t
        USING staged_changes s
           ON t.transaction_id = s.transaction_id
        WHEN MATCHED AND s.cdc_operation = 'd'
           THEN UPDATE SET t.is_deleted = true, t._updated_at = current_timestamp()
        WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
                      AND s.source_ts > t.event_timestamp
           THEN UPDATE SET
                t.amount = s.amount, t.status = s.status, t.currency = s.currency,
                t.event_timestamp = s.source_ts, t.source_lsn = s.source_lsn,
                t._updated_at = current_timestamp()
        WHEN NOT MATCHED AND s.cdc_operation <> 'd'
           THEN INSERT (transaction_id, account_id, amount, currency, tx_type,
                        status, event_timestamp, source_lsn, is_deleted, _updated_at)
                VALUES (s.transaction_id, s.account_id, s.amount, s.currency, s.tx_type,
                        s.status, s.source_ts, s.source_lsn, false, current_timestamp())
    """)

    after = spark.table(f"{CATALOG}.{DB}.silver_ledger").where(
        F.col("transaction_id") == transaction_id).collect()[0]
    print(f"GUARD_TEST after: amount={after['amount']} status={after['status']} "
          f"event_timestamp={after['event_timestamp']}")

    unchanged = (before["amount"] == after["amount"]
                 and before["event_timestamp"] == after["event_timestamp"])
    print(f"GUARD_TEST result: ledger_unchanged={unchanged}")

    bronze_count = spark.table(f"{CATALOG}.{DB}.bronze_cdc_events").where(
        F.col("_batch_id") == "guard-test-late-arrival").count()
    print(f"GUARD_TEST synthetic row still present in bronze: count={bronze_count}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transaction-id", required=True)
    args = parser.parse_args()
    main(args.transaction_id)
