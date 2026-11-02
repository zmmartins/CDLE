"""Part B.2: the same word count with Spark's DataFrame API: no Python runs on the executors.

  read.text -> regexp_extract(path of the request) -> split -> explode -> groupBy(word).count() -> write

Usage: spark-submit wordcount_df.py --sf 1 [--explain]
"""
import argparse
import shutil
import time

from pyspark.sql import functions as F

from common import evidence, executors, logs, out, spark

# The same pattern as logline.py: a line that does not match it is not complete, and is skipped.
LINE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$'


def word_counts(lines):
    path = F.regexp_extract("value", LINE, 4)
    return (lines.where(path.startswith("/search?q="))
                 .select(F.explode(F.split(F.substring_index(path, "q=", -1), r"\+")).alias("word"))
                 .where(F.col("word") != "")
                 .groupBy("word").count())


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    ap.add_argument("--explain", action="store_true", help="print the physical plan")
    a = ap.parse_args()
    evidence("wordcount_df")
    s = spark(f"wordcount_df sf{a.sf}")
    dest = out(f"df_words_sf{a.sf}")

    t0 = time.time()
    counts = word_counts(s.read.text(logs(a.sf)))
    if a.explain:
        counts.explain("formatted")
    shutil.rmtree(dest, ignore_errors=True)
    counts.write.csv(f"file://{dest}", sep="\t")                 # the action: the job runs here
    t1 = time.time()

    top = s.read.csv(f"file://{dest}", sep="\t", schema="word string, n long").orderBy(F.desc("n"), "word").limit(10).collect()
    print(f"wordcount_df SF{a.sf}: {t1 - t0:.1f} s, executors {executors(s.sparkContext)}")
    print("Top 10: " + ", ".join(f"{r.word} {r.n:,}" for r in top))
    print(f"Output: {dest}")
    s.stop()


if __name__ == "__main__":
    main()
