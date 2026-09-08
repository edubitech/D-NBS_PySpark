"""
Step 6c (brief) — gold_settlement_drift, answering Q3.

Per currency per business date: the originally-published net position versus
the restated one, using the same time-travel technique as Step 5.

SNAPSHOT_AT_CLOSE is pinned to just after day 10's real merge -- not day 20 --
so "published" only knows about days 1-10, the same way a real close report
would only ever cover days that had already happened by the time it was
published. That gives up to 10 real currency-day rows to compare against
the current (fully corrected) table, instead of the single day Q1 used.
Documented here as the assumption it is, per the brief's own "decide,
document, defend" instruction.

Usage:
    spark-submit gold_settlement_drift.py
"""
from pyspark.sql import SparkSession, functions as F

CATALOG = "glue_catalog"
DB = "fintech_db"
SNAPSHOT_AT_CLOSE = "2026-09-07 06:44:44"  # just after day 10's merge (snapshot 4685733960018236392)
MATERIALITY_THRESHOLD = 0.001

NET_EXPR = "sum(CASE WHEN tx_type = 'CASH_IN' THEN amount ELSE -amount END)"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_settlement_drift").getOrCreate()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.gold_settlement_drift (
            currency          STRING,
            business_date     DATE,
            published_net     DECIMAL(18,2),
            restated_net      DECIMAL(18,2),
            drift_amount      DECIMAL(18,2),
            drift_pct         DECIMAL(9,4),
            computed_at       TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (business_date)
        TBLPROPERTIES ('format-version' = '2')
    """)

    published = spark.sql(f"""
        SELECT currency, to_date(event_timestamp) AS business_date, {NET_EXPR} AS net
        FROM {CATALOG}.{DB}.silver_ledger TIMESTAMP AS OF '{SNAPSHOT_AT_CLOSE}'
        WHERE NOT is_deleted GROUP BY 1, 2
    """)

    restated = spark.sql(f"""
        SELECT currency, to_date(event_timestamp) AS business_date, {NET_EXPR} AS net
        FROM {CATALOG}.{DB}.silver_ledger
        WHERE NOT is_deleted GROUP BY 1, 2
    """)

    drift = (published.alias("p")
        .join(restated.alias("r"), ["currency", "business_date"], "inner")
        .selectExpr(
            "currency", "business_date",
            "round(p.net, 2) AS published_net",
            "round(r.net, 2) AS restated_net",
            "round(r.net - p.net, 2) AS drift_amount",
            "round((r.net - p.net) / abs(p.net), 4) AS drift_pct")
        .withColumn("computed_at", F.current_timestamp()))

    drift.writeTo(f"{CATALOG}.{DB}.gold_settlement_drift").overwritePartitions()

    total_days = drift.count()
    material = drift.where(F.abs("drift_pct") > MATERIALITY_THRESHOLD).count()
    print(f"SETTLEMENT_DRIFT published_days=10 currency_day_rows={total_days} "
          f"material_days={material} threshold={MATERIALITY_THRESHOLD}")

    top3 = drift.orderBy(F.abs("drift_amount").desc()).limit(3).collect()
    for row in top3:
        print(f"SETTLEMENT_DRIFT top currency={row['currency']} date={row['business_date']} "
              f"published={row['published_net']} restated={row['restated_net']} "
              f"drift_amount={row['drift_amount']} drift_pct={row['drift_pct']}")

    spark.stop()
