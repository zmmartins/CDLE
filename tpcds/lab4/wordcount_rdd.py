"""Part B.1: the word count of Part A, with Spark's RDD API (Python functions, called for every line).

  lines -> flatMap(words of each search) -> map(word -> (word, 1)) -> reduceByKey(+) -> save

Usage: spark-submit wordcount_rdd.py --sf 1 [--slow 3 --partitions 16]
"""
import argparse
import shutil
import time
from operator import add

from common import evidence, executors, logs, out, spark
from logline import parse, search_words


def words(line):
    r = parse(line)
    return search_words(r["path"]) if r else []


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    ap.add_argument("--partitions", type=int, default=0, help="ask for at least this many input partitions")
    ap.add_argument("--slow", type=float, default=0, help="sleep this many seconds in every map task (Part B.4)")
    a = ap.parse_args()
    evidence("wordcount_rdd")
    s = spark(f"wordcount_rdd sf{a.sf}")
    sc = s.sparkContext
    dest = out(f"rdd_words_sf{a.sf}")

    t0 = time.time()
    lines = sc.textFile(logs(a.sf), minPartitions=a.partitions or None)
    if a.slow:
        def slow(it, secs=a.slow):
            time.sleep(secs)
            return it
        lines = lines.mapPartitions(slow)
    counts = lines.flatMap(words).map(lambda w: (w, 1)).reduceByKey(add)
    shutil.rmtree(dest, ignore_errors=True)
    counts.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(f"file://{dest}")     # the action: the job runs here
    t1 = time.time()

    top = counts.takeOrdered(10, key=lambda kv: (-kv[1], kv[0]))
    print(f"wordcount_rdd SF{a.sf}: {t1 - t0:.1f} s, {lines.getNumPartitions()} input partitions, "
          f"{counts.getNumPartitions()} reduce partitions, executors {executors(sc)}")
    print("Top 10: " + ", ".join(f"{k} {v:,}" for k, v in top))
    print(f"Output: {dest}")
    s.stop()


if __name__ == "__main__":
    main()
