"""Part C: daily product page views from the access logs. THIS FILE IS A SKELETON: the AI assistant writes
the body of daily_product_counts() from your spec (ai/session-04.md), after you have written the tests.

  daily_product_counts(lines) -> DataFrame[product: string, day: date, n: bigint]

  lines    a DataFrame with one string column, "value": one line of the log per row (spark.read.text)
  product  the product id, in upper case
  day      the day of the request (UTC)
  n        the number of GET requests for that product's page, on that day, with status 200

Usage: spark-submit pipeline.py --sf 1      (writes /data/out/pipeline_sf1, in Parquet)
"""
import argparse
import shutil
import time

from pyspark.sql import DataFrame

from common import evidence, logs, out, spark


def daily_product_counts(lines: DataFrame) -> DataFrame:
    raise NotImplementedError("the body of daily_product_counts comes from the AI session (Part C.2)")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    a = ap.parse_args()
    evidence("pipeline")
    s = spark(f"pipeline sf{a.sf}")
    s.conf.set("spark.sql.session.timeZone", "UTC")
    dest = out(f"pipeline_sf{a.sf}")
    shutil.rmtree(dest, ignore_errors=True)
    t0 = time.time()
    result = daily_product_counts(s.read.text(logs(a.sf)))
    result.write.parquet(f"file://{dest}")
    print(f"pipeline SF{a.sf}: {time.time() - t0:.1f} s, output in {dest}")
    print(f"check it:  lab check.py product_day {dest} --sf {a.sf}")
    s.stop()


if __name__ == "__main__":
    main()
