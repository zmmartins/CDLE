"""Job 2 — loyalty cohorts: the revenue of each year, split by the year of each member's first purchase.

Members of the loyalty programme (c_preferred_cust_flag = 'Y') are followed one by one. Everyone else
(not a member, or an unknown customer) is reported in one line, without a cohort.

Run it with: lab clinic.py job2
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import table


def run(spark, sf):
    sales = (spark.read.parquet(table(sf, "store_sales"))
             .select("ss_customer_sk", "ss_sold_date_sk", "ss_net_paid"))
    customer = spark.read.parquet(table(sf, "customer")).select("c_customer_sk", "c_preferred_cust_flag")
    dates = spark.read.parquet(table(sf, "date_dim")).select("d_date_sk", "d_year")

    s = (sales
         .join(customer, sales.ss_customer_sk == customer.c_customer_sk, "left")
         .join(dates, sales.ss_sold_date_sk == dates.d_date_sk)
         # a member keeps the customer number; everyone else gets -1
         .withColumn("member", F.when(F.col("c_preferred_cust_flag") == "Y", F.col("ss_customer_sk")).otherwise(-1)))

    # the year of each member's first purchase: the smallest year among the member's sales
    s = s.withColumn("first_year", F.min("d_year").over(Window.partitionBy("member")))
    # people who are not members have no cohort
    s = s.withColumn("cohort", F.when(F.col("member") == -1, None).otherwise(F.col("first_year")))

    return (s.groupBy("cohort", "d_year")
            .agg(F.sum("ss_net_paid").alias("revenue"), F.count("*").alias("sales")))
