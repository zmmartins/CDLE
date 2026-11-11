"""Part C.3: the parts of the audit that a script can check. The rest of the checklist is yours (handout, C.3).

  1. no action or driver-side call inside pipeline.py: collect(), toPandas(), take(), show(), .rdd, count()
  2. no Python UDF (udf, pandas_udf): every step should be visible to the optimizer
  3. the output has the columns product (string), day (date), n (bigint)
  4. the plan aggregates before the shuffle: a partial aggregate below the Exchange (as the combiner of Part A)
  5. the result on sample.log, for you to compare with what you expected when writing the tests

Usage: python audit.py      (in the client container; uses a local Spark, not the cluster)
"""
import os
import re

from pyspark.sql import SparkSession

from common import evidence

FORBIDDEN = [r"\.collect\(", r"\.toPandas\(", r"\.take\(", r"\.show\(", r"\.rdd\b", r"\.count\(\)", r"\.head\(",
             r"\bfirst\(\)"]
UDF = [r"\budf\b", r"pandas_udf", r"@F\.udf", r"\.mapPartitions\(", r"\.map\("]


def code_of_pipeline():
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.py"), encoding="utf-8").read()
    body = src.split("def main(")[0]           # main() may print and time; the function itself may not collect
    return "\n".join(l.split("#")[0] for l in body.splitlines())


def main():
    evidence("audit")
    code = code_of_pipeline()
    results = []
    bad = [p for p in FORBIDDEN if re.search(p, code)]
    results.append(("no collect/toPandas/take/show/.rdd/count in daily_product_counts", not bad, ", ".join(bad)))
    udfs = [p for p in UDF if re.search(p, code)]
    results.append(("no Python UDFs or RDD maps", not udfs, ", ".join(udfs)))

    s = (SparkSession.builder.master("local[2]").appName("audit").config("spark.eventLog.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    from pipeline import daily_product_counts
    df = daily_product_counts(s.read.text(os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests", "sample.log")))
    schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    results.append(("columns product string, day date, n bigint",
                    schema == [("product", "string"), ("day", "date"), ("n", "bigint")], str(schema)))
    plan = df._jdf.queryExecution().executedPlan().toString()
    lines = plan.splitlines()
    ex = next((i for i, l in enumerate(lines) if "Exchange" in l), None)
    partial = ex is not None and any("partial_" in l for l in lines[ex + 1:])
    results.append(("partial aggregation below the Exchange", partial,
                    "no Exchange in the plan" if ex is None else ""))

    for name, ok, why in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"   ({why})" if why and not ok else ""))
    print("\nResult on tests/sample.log:")
    for r in df.orderBy("day", "product").collect():
        print(f"  {r['product']}  {r['day']}  {r['n']}")
    print("\nThe physical plan:")
    df.explain()
    s.stop()


if __name__ == "__main__":
    main()
