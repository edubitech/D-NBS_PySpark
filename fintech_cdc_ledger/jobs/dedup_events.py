"""
Step 2 (brief) — Deduplication & event ordering. Runs on EMR Serverless (Spark).

Collapses Bronze's multi-row-per-transaction CDC log to one row per
transaction_id: latest by source_ts, then source_lsn as the tiebreaker.
Never orders by arrival_ts or file order — that is not when the change
happened, it is when the file landed (and the harness deliberately scrambles
file order for ~2% of events to prove this matters).

Gotcha fixed here vs. the brief's reference code: a delete envelope has
after=null, so a transaction_id read only from `after` would come out null
for every delete and silently vanish from the dedup key. Both transaction_id
and amount/status fall back to `before` so deletes survive.

Usage:
    spark-submit dedup_events.py --run-date 2026-01-01

Writes the collapsed rows to glue_catalog.fintech_db.silver_staged_changes,
overwriting that day's partition — safe to re-run.
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

CATALOG = "glue_catalog"
DB = "fintech_db"


def main(run_date: str) -> None:
    spark = SparkSession.builder.appName(f"dedup_events_{run_date}").getOrCreate()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.silver_staged_changes (
            transaction_id  STRING,
            account_id      STRING,
            amount          DECIMAL(18,2),
            currency        STRING,
            tx_type         STRING,
            status          STRING,
            cdc_operation   STRING,
            source_ts       TIMESTAMP,
            source_lsn      BIGINT,
            extract_date    DATE
        ) USING iceberg
        PARTITIONED BY (extract_date)
        TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
    """)

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

    ranked = extracted.withColumn("_rn", F.row_number().over(w))

    dup_keys = (ranked.groupBy("transaction_id").count()
                .where(F.col("count") > 1).count())

    latest = ranked.where(F.col("_rn") == 1).drop("_rn")
    rows_out = latest.count()

    print(f"DEDUP run_date={run_date} rows_in={rows_in} rows_out={rows_out} "
          f"collapse_ratio={rows_out / rows_in:.4f} "
          f"intra_batch_duplicate_keys={dup_keys}")

    latest.select(
        "transaction_id", "account_id", "amount", "currency", "tx_type",
        "status", "cdc_operation", "source_ts", "source_lsn", "extract_date",
    ).writeTo(f"{CATALOG}.{DB}.silver_staged_changes").overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()
    main(args.run_date)
