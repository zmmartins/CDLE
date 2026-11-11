"""Job 3 — customer segments: how many customers spent more than a limit, and how much they spent, for ten limits.

First the spending of each customer (all the sales of store_sales, about 496,000 customers at SF10),
then one small query for each limit, from 25,000 to 250,000.

Run it with: lab clinic.py job3
"""
from pyspark.sql import functions as F

from common import table


def run(spark, sf):
    sales = spark.read.parquet(table(sf, "store_sales"))

    spent = (sales
             .where(F.col("ss_customer_sk").isNotNull())
             .groupBy("ss_customer_sk")
             .agg(F.sum("ss_net_paid").alias("spent")))

    rows = []
    for limit in range(25_000, 250_001, 25_000):
        r = (spent
             .where(F.col("spent") > limit)
             .agg(F.count("*").alias("customers"), F.sum("spent").alias("revenue"))
             .first())
        rows.append((limit, r["customers"], r["revenue"]))
        print(f"  more than {limit:>7,}: {r['customers']:>7,} customers")
    return rows
