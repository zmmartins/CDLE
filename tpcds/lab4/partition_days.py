"""Part B.6 (optional, at home): partitions in folders (partitionBy) against row groups in a sorted file.

Turns the SF10 logs into Parquet three times, in /data/views/:
  plain    the product page views in the order of the logs (already in time order: they are logs)
  shuffled the same rows in random order: the order of most tables that nobody sorted
  by_day   one folder per day (day=2002-03-14/), written with partitionBy("day")
Then runs the same query on each: product page views in March 2002. Look at the scan of each plan
(PartitionFilters, PushedFilters) and, in the SQL tab of the UI, at "number of files read" and "size of files read".

Usage: spark-submit partition_days.py [--rewrite]
"""
import argparse
import glob
import os
import re
import time

from pyspark.sql import functions as F

from common import DATA, evidence, logs, spark

LINE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$'
OUT = f"{DATA}/views"


def views(s):
    v = s.read.text(logs(10)).select(
        F.regexp_extract("value", LINE, 2).alias("time"), F.regexp_extract("value", LINE, 3).alias("method"),
        F.regexp_extract("value", LINE, 4).alias("path"), F.regexp_extract("value", LINE, 5).alias("status"))
    v = v.where((F.col("method") == "GET") & (F.col("status") == "200") & F.col("path").startswith("/product/"))
    ts = F.to_timestamp("time", "dd/MMM/yyyy:HH:mm:ss Z")
    return v.select(F.upper(F.regexp_extract("path", r"^/product/([^?]+)", 1)).alias("product"),
                    ts.alias("ts"), F.to_date(ts).alias("day"))


def folder(path):
    files = [f for f in glob.glob(f"{path}/**/*.parquet", recursive=True)]
    return len(files), sum(os.path.getsize(f) for f in files) / 1e6


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rewrite", action="store_true", help="write the three copies again")
    a = ap.parse_args()
    evidence("partition_days")
    s = spark("partition_days")
    s.conf.set("spark.sql.session.timeZone", "UTC")
    if a.rewrite or not os.path.isdir(f"{OUT}/by_day"):
        v = views(s)
        for name, write in [
            ("plain", lambda: v.write.mode("overwrite").parquet(f"file://{OUT}/plain")),
            ("shuffled", lambda: v.orderBy(F.rand(42)).write.mode("overwrite").parquet(f"file://{OUT}/shuffled")),
            ("by_day", lambda: v.repartition("day").write.mode("overwrite").partitionBy("day").parquet(f"file://{OUT}/by_day")),
        ]:
            t = time.time()
            write()
            n, mb = folder(f"{OUT}/{name}")
            print(f"wrote {name:<9} {n:>6,} files {mb:>8,.1f} MB   {time.time() - t:6.1f} s")

    print("\nquery: count(*) WHERE day BETWEEN 2002-03-01 AND 2002-03-31")
    for name in ("plain", "shuffled", "by_day"):
        df = s.read.parquet(f"file://{OUT}/{name}")
        q = df.where(F.col("day").between("2002-03-01", "2002-03-31"))
        t = time.time()
        n = q.count()
        dt = time.time() - t
        scan = [l.strip() for l in q._jdf.queryExecution().executedPlan().toString().split("\n") if "FileScan" in l][0]
        filters = re.findall(r"((?:PartitionFilters|PushedFilters): \[.*?\])(?=, \w+:)", scan)
        print(f"  {name:<9} {n:>8,} rows  {dt:6.2f} s   {' | '.join(filters)}")
    s.stop()


if __name__ == "__main__":
    main()
