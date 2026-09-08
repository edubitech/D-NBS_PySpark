"""
Step 4 deliverable (brief) — the duplicate-key MERGE abort.

Writes TWO rows for the same transaction_id into one staged_changes batch,
then runs the same MERGE used in production. Iceberg's MERGE INTO must abort
rather than guess which row was meant — this is the correctness guarantee
working, not a bug. The exact error text is a required break-log entry.

Usage:
    spark-submit duplicate_key_test.py --transaction-id T-000000000
"""
import argparse
import traceback

from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"
TEST_BATCH_DATE = "2099-01-02"


def main(transaction_id: str) -> None:
    spark = SparkSession.builder.appName("duplicate_key_test").getOrCreate()

    ledger_row = spark.table(f"{CATALOG}.{DB}.silver_ledger").where(
        F.col("transaction_id") == transaction_id).collect()[0]

    ts_a = spark.sql(f"SELECT TIMESTAMP '{ledger_row['event_timestamp']}' + INTERVAL 1 DAY AS ts") \
                .collect()[0]["ts"]
    ts_b = spark.sql(f"SELECT TIMESTAMP '{ledger_row['event_timestamp']}' + INTERVAL 2 DAY AS ts") \
                .collect()[0]["ts"]

    dup_rows = spark.createDataFrame([
        {"transaction_id": transaction_id, "account_id": ledger_row["account_id"],
         "amount": 111.11, "currency": ledger_row["currency"],
         "tx_type": ledger_row["tx_type"], "status": "SETTLED",
         "cdc_operation": "u", "source_ts": ts_a, "source_lsn": 111111,
         "extract_date": TEST_BATCH_DATE},
        {"transaction_id": transaction_id, "account_id": ledger_row["account_id"],
         "amount": 222.22, "currency": ledger_row["currency"],
         "tx_type": ledger_row["tx_type"], "status": "SETTLED",
         "cdc_operation": "u", "source_ts": ts_b, "source_lsn": 222222,
         "extract_date": TEST_BATCH_DATE},
    ])
    dup_rows = dup_rows.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
                       .withColumn("extract_date", F.col("extract_date").cast("date"))

    dup_rows.writeTo(f"{CATALOG}.{DB}.silver_staged_changes").overwritePartitions()
    print(f"DUP_KEY_TEST wrote 2 rows for {transaction_id} into "
          f"extract_date={TEST_BATCH_DATE} (source_lsn 111111 and 222222)")

    dup_rows.createOrReplaceTempView("staged_changes")

    try:
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
        print("DUP_KEY_TEST result: MERGE SUCCEEDED — this should not happen, "
              "duplicate keys should have aborted it")
    except Exception as e:
        print("DUP_KEY_TEST result: MERGE ABORTED (expected)")
        java_ex = getattr(e, "java_exception", None)
        if java_ex is not None:
            print(f"DUP_KEY_TEST exact_error (java): {java_ex.getMessage()}")
        else:
            print(f"DUP_KEY_TEST exact_error (str): {e}")
        print("DUP_KEY_TEST full_traceback_begin")
        print(traceback.format_exc())
        print("DUP_KEY_TEST full_traceback_end")

    after = spark.table(f"{CATALOG}.{DB}.silver_ledger").where(
        F.col("transaction_id") == transaction_id).collect()[0]
    unchanged = (ledger_row["amount"] == after["amount"])
    print(f"DUP_KEY_TEST ledger_unchanged_after_abort={unchanged} "
          f"amount_still={after['amount']}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transaction-id", required=True)
    args = parser.parse_args()
    main(args.transaction_id)
