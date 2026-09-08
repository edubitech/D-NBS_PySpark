"""
Step 1 (brief) — Bronze ingestion. Runs on EMR Serverless (Spark).

Reads one day of raw Debezium-style CDC envelopes from S3, lands them
append-only into the Iceberg bronze table. Bronze is the record of what
arrived — untouched, unjudged, schema-on-read.

Usage (as an EMR Serverless job entry point):
    spark-submit bronze_ingest.py --run-date 2026-01-01 --batch-id <uuid>

Catalog / warehouse are configured via --conf at job submission time, not
hardcoded here, so the same script works for any run_date / batch_id.
"""
import argparse

from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"
RAW_BUCKET = "ali-iceberg-schema-demo-275829498730"
RAW_PREFIX = f"s3://{RAW_BUCKET}/fintech_cdc/raw/cdc"


def main(run_date: str, batch_id: str) -> None:
    spark = SparkSession.builder.appName(f"bronze_ingest_{run_date}").getOrCreate()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DB}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.bronze_cdc_events (
            cdc_operation   STRING,
            source_ts       TIMESTAMP,
            arrival_ts      TIMESTAMP,
            source_lsn      BIGINT,
            before          STRING,
            after           STRING,
            _src_file       STRING,
            _ingested_at    TIMESTAMP,
            _batch_id       STRING,
            extract_date    DATE
        ) USING iceberg
        PARTITIONED BY (extract_date)
        TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
    """)

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

    row_count = bronze.count()
    print(f"BRONZE_INGEST run_date={run_date} batch_id={batch_id} rows={row_count}")

    (bronze.writeTo(f"{CATALOG}.{DB}.bronze_cdc_events").overwritePartitions())

    snap = spark.sql(f"""
        SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
        FROM {CATALOG}.{DB}.bronze_cdc_events.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).collect()[0]
    print(f"BRONZE_INGEST snapshot_id={snap['snapshot_id']} "
          f"committed_at={snap['committed_at']} added_records={snap['added']}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    main(args.run_date, args.batch_id)
