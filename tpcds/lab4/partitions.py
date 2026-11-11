"""Part B.5: how many tasks does Spark create to read a table, and why?

Reads store_sales (SF10, the 16 Parquet files of Lab 1, copied to /data/store_sales) and computes
sum(ss_net_paid). Before running, it prints Spark's rule for the size of an input partition, and its
prediction; then the partitions Spark planned, and how many of them actually received rows.

  maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, (bytes of all files + files x openCost) / cores))

Usage: spark-submit partitions.py [--max-partition-mb 128] [--repartition 8]
"""
import argparse
import glob
import os
import time

import duckdb
from pyspark.sql import functions as F

from common import DATA, evidence, spark

MIB = 1024 * 1024
TABLE = f"{DATA}/store_sales"


def predict(sizes, max_bytes, open_cost, cores):
    """Spark 3.5's FilePartition rule: cut files into splits of at most maxSplitBytes, then pack them."""
    total = sum(s + open_cost for s in sizes)
    split = min(max_bytes, max(open_cost, total // cores))
    pieces = sorted((min(split, s - off) for s in sizes for off in range(0, s, split)), reverse=True)
    parts, current = 0, 0
    for p in pieces:
        if current > 0 and current + p > split:
            parts, current = parts + 1, 0
        current += p + open_cost
    return split, parts + (1 if current else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--max-partition-mb", type=int, default=128, help="spark.sql.files.maxPartitionBytes, in MiB")
    ap.add_argument("--repartition", type=int, default=0, help="then repartition(N, ss_store_sk) and show the plan")
    a = ap.parse_args()
    evidence("partitions")
    files = sorted(glob.glob(f"{TABLE}/*.parquet"))
    if not files:
        raise SystemExit(f"no files in {TABLE}: copy them first (section 0.4 of the handout)")
    sizes = [os.path.getsize(f) for f in files]
    rgs = duckdb.sql(f"SELECT count(DISTINCT (file_name, row_group_id)) FROM parquet_metadata('{TABLE}/*.parquet')").fetchone()[0]

    s = spark(f"partitions {a.max_partition_mb}MiB")
    s.conf.set("spark.sql.files.maxPartitionBytes", f"{a.max_partition_mb}m")
    cores = s.sparkContext.defaultParallelism
    open_cost = 4 * MIB
    split, predicted = predict(sizes, a.max_partition_mb * MIB, open_cost, cores)
    print(f"store_sales: {len(files)} files, {sum(sizes) / MIB:,.1f} MiB "
          f"(smallest {min(sizes) / MIB:.1f}, largest {max(sizes) / MIB:.1f} MiB), {rgs} row groups")
    print(f"rule: maxPartitionBytes {a.max_partition_mb} MiB, openCostInBytes 4 MiB, {cores} cores "
          f"-> maxSplitBytes {split / MIB:.1f} MiB -> predicted partitions {predicted}")

    df = s.read.parquet(TABLE)
    planned = df.rdd.getNumPartitions()
    t0 = time.time()
    rows = (df.select(F.spark_partition_id().alias("p"), "ss_net_paid")
              .groupBy("p").agg(F.count("*").alias("rows"), F.sum("ss_net_paid").alias("paid")).collect())
    t1 = time.time()
    total = sum(r.paid for r in rows)
    print(f"planned partitions (tasks of the first stage): {planned}; partitions that received rows: {len(rows)}")
    print(f"rows per partition: min {min(r.rows for r in rows):,}, max {max(r.rows for r in rows):,}")
    print(f"sum(ss_net_paid) = {total}   in {t1 - t0:.2f} s")

    if a.repartition:
        r = df.repartition(a.repartition, "ss_store_sk")
        print(f"\nafter repartition({a.repartition}, ss_store_sk): {r.rdd.getNumPartitions()} partitions; the plan:")
        r.groupBy("ss_store_sk").agg(F.sum("ss_net_paid")).explain()
    s.stop()


if __name__ == "__main__":
    main()
