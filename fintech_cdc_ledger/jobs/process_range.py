"""
Processes a range of days through dedup + MERGE in one Spark session, so
running several more days doesn't cost a separate EMR job startup each time.
Combines the logic of dedup_events.py and merge_ledger.py per day.

Usage:
    spark-submit process_range.py --start-date 2026-01-02 --end-date 2026-01-06
"""
import argparse
import uuid
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

CATALOG = "glue_catalog"
DB = "fintech_db"
RAW_BUCKET = "ali-iceberg-schema-demo-275829498730"
RAW_PREFIX = f"s3://{RAW_BUCKET}/fintech_cdc/raw/cdc"


def ingest_bronze(spark, run_date: str) -> None:
    batch_id = str(uuid.uuid4())
    raw = (spark.read
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt")
           .json(f"{RAW_PREFIX}/dt={run_date}/"))

    bronze = (raw
        .withColumn("cdc_operation", F.col("op"))
        .withColumn("source_ts",     (F.col("source.ts_ms") / 1000).cast("timestamp"))
        .withColumn("arrival_ts",    (F.col("ts_ms") / 1000).cast("timestamp"))
        .withColumn("source_lsn",    F.col("source.lsn"))
        .withColumn("before",        F.to_json(F.col("before")))
        .withColumn("after",         F.to_json(F.col("after")))
        .withColumn("_src_file",     F.input_file_name())
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("extract_date",  F.lit(run_date).cast("date"))
        .select("cdc_operation", "source_ts", "arrival_ts", "source_lsn",
                "before", "after", "_src_file", "_ingested_at", "_batch_id",
                "extract_date"))

    bronze.writeTo(f"{CATALOG}.{DB}.bronze_cdc_events").overwritePartitions()


def process_day(spark, run_date: str) -> None:
    ingest_bronze(spark, run_date)

    bronze = spark.table(f"{CATALOG}.{DB}.bronze_cdc_events").where(
        F.col("extract_date") == run_date)
    rows_in = bronze.count()

    extracted = bronze.select(
        F.coalesce(
            F.get_json_object("after", "$.transaction_id"),
            F.get_json_object("before", "$.transaction_id"),
        ).alias("transaction_id"),
        F.get_json_object("after", "$.account_id").alias("account_id"),
        F.coalesce(
            F.get_json_object("after", "$.amount"),
            F.get_json_object("before", "$.amount"),
        ).cast("decimal(18,2)").alias("amount"),
        F.upper(F.get_json_object("after", "$.currency")).alias("currency"),
        F.get_json_object("after", "$.tx_type").alias("tx_type"),
        F.coalesce(
            F.upper(F.get_json_object("after", "$.status")),
            F.upper(F.get_json_object("before", "$.status")),
        ).alias("status"),
        "cdc_operation", "source_ts", "source_lsn", "extract_date",
    )

    w = (Window.partitionBy("transaction_id")
               .orderBy(F.col("source_ts").desc(), F.col("source_lsn").desc()))
    latest = (extracted.withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1).drop("_rn"))
    rows_out = latest.count()

    latest.select(
        "transaction_id", "account_id", "amount", "currency", "tx_type",
        "status", "cdc_operation", "source_ts", "source_lsn", "extract_date",
    ).writeTo(f"{CATALOG}.{DB}.silver_staged_changes").overwritePartitions()

    latest.createOrReplaceTempView("staged_changes")
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

    ledger_total = spark.table(f"{CATALOG}.{DB}.silver_ledger").count()
    print(f"PROCESS_RANGE run_date={run_date} bronze_rows={rows_in} "
          f"staged_rows={rows_out} ledger_total_after={ledger_total}")


def main(start_date: str, end_date: str) -> None:
    spark = SparkSession.builder.appName(f"process_range_{start_date}_{end_date}").getOrCreate()

    d = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while d <= end:
        process_day(spark, d.isoformat())
        d += timedelta(days=1)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    main(args.start_date, args.end_date)
