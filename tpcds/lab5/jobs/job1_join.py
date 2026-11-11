"""Job 1 — revenue of each item category, per month.

store_sales (28.8 M rows at SF10) joined with item (102,000 rows) and date_dim (73,049 rows),
then summed per year, month and category: about 700 rows.

Run it with: lab clinic.py job1
"""
from pyspark.sql import functions as F

from common import table


def run(spark, sf):
    # Broadcast joins were turned off after an out-of-memory error, months ago. Nobody remembers which job it was.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    sales = spark.read.parquet(table(sf, "store_sales"))
    item = spark.read.parquet(table(sf, "item"))
    dates = spark.read.parquet(table(sf, "date_dim"))

    return (sales
            .join(item, sales.ss_item_sk == item.i_item_sk)
            .join(dates, sales.ss_sold_date_sk == dates.d_date_sk)
            .groupBy("d_year", "d_moy", "i_category")
            .agg(F.sum("ss_net_paid").alias("revenue"), F.count("*").alias("sales")))
