"""
Step 7 (brief) — table maintenance: compaction, manifests, retention, orphans.

First manufactures a small, realistic batch of late corrections against real
ledger rows (same technique Step 4's guard_test/duplicate_key_test already
use: legitimate synthetic events run through the real bronze -> staged ->
MERGE path, not fabricated query results). This exists because every
transaction in this dataset is otherwise unique -- confirmed in Step 6.2 --
so no ordinary MERGE here ever hits the UPDATE branch, meaning merge-on-read
never produces a delete file to demonstrate this step's whole point.

Then runs the brief's own sequence:
  1. plain rewrite_data_files (brief's exact options)              -- likely a no-op on deletes
  2. rewrite_data_files with rewrite-all=true                      -- the fix
  3. rewrite_manifests
  4. expire_snapshots (safe no-op threshold -- see inline note)
  5. remove_orphan_files (dry_run)

File/delete-file counts are measured before and after each compaction call.

Usage:
    spark-submit table_maintenance.py
"""
from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"
TABLE = f"{CATALOG}.{DB}.silver_ledger"
CORRECTION_BATCH_DATE = "2026-01-21"
N_CORRECTIONS = 50


def file_stats(spark, label):
    rows = spark.sql(f"""
        SELECT content, count(*) AS files, CAST(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes
        FROM {TABLE}.files GROUP BY content ORDER BY content
    """).collect()
    stats = {r["content"]: (r["files"], r["avg_bytes"]) for r in rows}
    data_files, data_avg = stats.get(0, (0, 0))
    delete_files, delete_avg = stats.get(1, (0, 0))
    print(f"FILE_STATS[{label}] data_files={data_files} data_avg_bytes={data_avg} "
          f"delete_files={delete_files} delete_avg_bytes={delete_avg}")
    return data_files, delete_files


if __name__ == "__main__":
    spark = SparkSession.builder.appName("table_maintenance").getOrCreate()

    # ---- manufacture a real, legitimate correction batch ----------------
    targets = (spark.table(TABLE)
               .where(~F.col("is_deleted"))
               .orderBy("transaction_id")
               .limit(N_CORRECTIONS)
               .collect())

    correction_ts = spark.sql(
        f"SELECT TIMESTAMP '{CORRECTION_BATCH_DATE} 12:00:00' AS ts"
    ).collect()[0]["ts"]

    corrections = spark.createDataFrame([{
        "transaction_id": r["transaction_id"],
        "account_id": r["account_id"],
        "amount": float(r["amount"]) - 0.01,   # a realistic small fee correction
        "currency": r["currency"],
        "tx_type": r["tx_type"],
        "status": r["status"],
        "cdc_operation": "u",
        "source_ts": correction_ts,
        "source_lsn": 900000000 + i,
        "extract_date": CORRECTION_BATCH_DATE,
    } for i, r in enumerate(targets)])
    corrections = corrections.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
                              .withColumn("extract_date", F.col("extract_date").cast("date"))

    bronze_rows = (corrections
        .withColumn("after", F.to_json(F.struct(
            "transaction_id", "account_id", "amount", "currency", "tx_type", "status")))
        .withColumn("before", F.lit(None).cast("string"))
        .withColumn("arrival_ts", F.current_timestamp())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_src_file", F.lit("table_maintenance_corrections"))
        .withColumn("_batch_id", F.lit("maintenance-fee-correction-batch"))
        .select("cdc_operation", "source_ts", "arrival_ts", "source_lsn", "before", "after",
                "_src_file", "_ingested_at", "_batch_id", "extract_date"))
    bronze_rows.writeTo(f"{CATALOG}.{DB}.bronze_cdc_events").append()

    corrections.createOrReplaceTempView("staged_corrections")
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING staged_corrections s
           ON t.transaction_id = s.transaction_id
        WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
                      AND s.source_ts > t.event_timestamp
           THEN UPDATE SET
                t.amount = s.amount, t.status = s.status, t.currency = s.currency,
                t.event_timestamp = s.source_ts, t.source_lsn = s.source_lsn,
                t._updated_at = current_timestamp()
    """)
    applied = spark.table(TABLE).where(
        (F.col("event_timestamp") == correction_ts)).count()
    print(f"MAINTENANCE_SETUP corrections_submitted={len(targets)} corrections_applied={applied}")

    # ---- before ------------------------------------------------------------
    before_data, before_delete = file_stats(spark, "before_any_compaction")

    # ---- plain compaction (brief's exact options) ---------------------------
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{DB}.silver_ledger',
            strategy => 'sort',
            sort_order => 'account_id ASC, event_timestamp DESC',
            options => map(
                'target-file-size-bytes', '134217728',
                'max-concurrent-file-group-rewrites', '4'
            ))
    """).show(truncate=False)
    plain_data, plain_delete = file_stats(spark, "after_plain_rewrite")

    # ---- the fix, only if the plain run left deletes behind -----------------
    if plain_delete > 0:
        spark.sql(f"""
            CALL {CATALOG}.system.rewrite_data_files(
                table => '{DB}.silver_ledger',
                options => map('rewrite-all', 'true'))
        """).show(truncate=False)
    fixed_data, fixed_delete = file_stats(spark, "after_rewrite_all_fix")

    print(f"COMPACTION_SUMMARY before=(data={before_data},delete={before_delete}) "
          f"plain=(data={plain_data},delete={plain_delete}) "
          f"fixed=(data={fixed_data},delete={fixed_delete})")

    # ---- manifests -----------------------------------------------------------
    spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{DB}.silver_ledger')").show(truncate=False)

    # ---- retention: safe no-op threshold, policy stated in the artifact ------
    spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{DB}.silver_ledger',
            older_than => TIMESTAMP '2026-09-01 00:00:00',
            retain_last => 50,
            stream_results => true)
    """).show(truncate=False)

    # ---- orphan files, dry run -------------------------------------------
    spark.sql(f"""
        CALL {CATALOG}.system.remove_orphan_files(
            table => '{DB}.silver_ledger', dry_run => true)
    """).show(100, truncate=False)

    spark.stop()
