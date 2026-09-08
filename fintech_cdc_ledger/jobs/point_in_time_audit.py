"""
Step 5 (brief) — Point-in-time audit, answering Q1.

"What was the balance as we knew it on day 1's close?" vs "what do we now
know it to be, including corrections that arrived on later days?" Both
numbers are legitimate; they differ by exactly the late-adjustment impact.

AS_OF (business cutoff) = end of day 1, 2026-01-01 23:59:59.
SNAPSHOT_PIN (wall-clock) = just after day 1's real merge committed
(2026-09-06 08:08:23.285 UTC, snapshot 2697762506882925537, 1193 rows added),
before days 2-20 were processed the next day.

Gotcha (brief's own warning): TIMESTAMP AS OF resolves to the newest
snapshot strictly OLDER than the value given — never pass a snapshot's own
committed_at, it finds nothing older than itself.

Writes results to gold_pit_balance_compare, named to match the brief's own
Step 10 Redshift query so downstream steps read it directly.
"""
from pyspark.sql import SparkSession

CATALOG = "glue_catalog"
DB = "fintech_db"
AS_OF = "2026-01-01 23:59:59"
SNAPSHOT_PIN = "2026-09-06 08:08:24"

BALANCE_EXPR = "sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END)"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("point_in_time_audit").getOrCreate()

    as_known = spark.sql(f"""
        SELECT account_id, {BALANCE_EXPR} AS balance_as_known
        FROM {CATALOG}.{DB}.silver_ledger TIMESTAMP AS OF '{SNAPSHOT_PIN}'
        WHERE NOT is_deleted AND event_timestamp <= TIMESTAMP '{AS_OF}'
        GROUP BY account_id
    """)

    as_restated = spark.sql(f"""
        SELECT account_id, {BALANCE_EXPR} AS balance_as_restated
        FROM {CATALOG}.{DB}.silver_ledger
        WHERE NOT is_deleted AND event_timestamp <= TIMESTAMP '{AS_OF}'
        GROUP BY account_id
    """)

    compare = (as_known.join(as_restated, "account_id", "full_outer")
               .selectExpr(
                   "account_id",
                   "coalesce(balance_as_known, 0) AS balance_as_known",
                   "coalesce(balance_as_restated, 0) AS balance_as_restated",
                   "coalesce(balance_as_restated, 0) - coalesce(balance_as_known, 0) AS delta",
                   f"TIMESTAMP '{AS_OF}' AS as_of_ts"))

    compare.createOrReplaceTempView("pit_compare")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.gold_pit_balance_compare (
            account_id           STRING,
            balance_as_known     DECIMAL(18,2),
            balance_as_restated  DECIMAL(18,2),
            delta                DECIMAL(18,2),
            as_of_ts             TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
    """)
    compare.select(
        "account_id", "balance_as_known", "balance_as_restated", "delta", "as_of_ts",
    ).writeTo(f"{CATALOG}.{DB}.gold_pit_balance_compare").overwritePartitions()

    total_accounts = compare.count()
    changed = compare.where("delta != 0").count()
    print(f"PIT_AUDIT as_of={AS_OF} snapshot_pin={SNAPSHOT_PIN} "
          f"total_accounts={total_accounts} accounts_with_late_adjustment={changed}")

    top5 = compare.selectExpr("*", "abs(delta) AS abs_delta").orderBy(
        "abs_delta", ascending=False).limit(5).collect()
    for row in top5:
        print(f"PIT_AUDIT top5 account={row['account_id']} "
              f"as_known={row['balance_as_known']} "
              f"as_restated={row['balance_as_restated']} "
              f"delta={row['delta']}")

    spark.stop()
