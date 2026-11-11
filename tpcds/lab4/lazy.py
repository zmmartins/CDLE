"""Part B.3: transformations are only a plan; nothing runs until an action asks for a result.

Five transformations, then one action. After each line, the script prints how long the line took
and how many Spark jobs the application has started so far.

Usage: spark-submit lazy.py --sf 1
"""
import argparse
import time

from pyspark.sql import functions as F

from common import evidence, jobs_so_far, logs, spark

LINE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$'


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    a = ap.parse_args()
    evidence("lazy")
    s = spark(f"lazy sf{a.sf}")

    def step(label, f):
        t = time.time()
        r = f()
        print(f"  {label:<58} {time.time() - t:6.2f} s   jobs so far: {jobs_so_far(s)}")
        return r

    lines = step("1 read.text(logs)", lambda: s.read.text(logs(a.sf)))
    parsed = step("2 select(method, path, status = regexp_extract(...))", lambda: lines.select(
        F.regexp_extract("value", LINE, 3).alias("method"),
        F.regexp_extract("value", LINE, 4).alias("path"),
        F.regexp_extract("value", LINE, 5).alias("status")))
    gets = step("3 where(method = 'GET' and status = '200')",
                lambda: parsed.where((F.col("method") == "GET") & (F.col("status") == "200")))
    prods = step("4 where(path starts with '/product/')", lambda: gets.where(F.col("path").startswith("/product/")))
    ids = step("5 select(upper(product id))",
               lambda: prods.select(F.upper(F.regexp_extract("path", r"^/product/([^?]+)", 1)).alias("product")))
    n = step("6 count()   <- the action", lambda: ids.count())
    print(f"product page views (GET, 200): {n:,}")
    s.stop()


if __name__ == "__main__":
    main()
