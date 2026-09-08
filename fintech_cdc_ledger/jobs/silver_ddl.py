"""
Step 3 (brief) — Silver schema DDL. Runs on EMR Serverless (Spark).

Creates silver_ledger: one row per transaction, current state. No data
movement here — the CDC MERGE (brief Step 4) is what populates it.

Four choices this DDL encodes, each defended in the runbook:
  - day(event_timestamp)       hidden partitioning, no derived column needed
  - bucket(16, account_id)     caps high-cardinality account_id
  - merge-on-read              cheap for constant small CDC updates
  - write.distribution-mode=hash   keeps one partition's rows on one task
"""
from pyspark.sql import SparkSession

CATALOG = "glue_catalog"
DB = "fintech_db"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_ddl").getOrCreate()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.silver_ledger (
            transaction_id  STRING,
            account_id      STRING,
            amount          DECIMAL(18,2),
            currency        STRING,
            tx_type         STRING,
            status          STRING,
            event_timestamp TIMESTAMP,
            source_lsn      BIGINT,
            is_deleted      BOOLEAN,
            _updated_at     TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(event_timestamp), bucket(16, account_id))
        TBLPROPERTIES (
            'format-version'               = '2',
            'write.delete.mode'            = 'merge-on-read',
            'write.update.mode'            = 'merge-on-read',
            'write.merge.mode'             = 'merge-on-read',
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode'      = 'hash'
        )
    """)

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{DB}.silver_ledger
        WRITE ORDERED BY account_id, event_timestamp DESC
    """)

    desc = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{DB}.silver_ledger").collect()
    for row in desc:
        print(f"SILVER_DDL {row['col_name']} | {row['data_type']}")

    print("SILVER_DDL table created: silver_ledger")
    spark.stop()
