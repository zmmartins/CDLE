"""Time a query over the files of Part A, or over Parquet objects in the object store.

  python read_bench.py formats --query sum1 --cold      every configuration of formats.py, page cache emptied before each run
  python read_bench.py formats --query sum1             every configuration, warm (one run first, not counted)
  python read_bench.py obj sf10/store_sales --query month
  python read_bench.py obj sorted/store_sales --query month --runs 3

Each run is a new Python process with a new DuckDB, so nothing is cached inside DuckDB between
runs. "Cold" also asks the kernel to drop the files from its page cache first (posix_fadvise);
see the handout for what that does not cover.

For the object store, the front-end's counters are read before and after every run: GET and
HEAD requests, bytes sent to DuckDB, bytes read from each shard and bytes rebuilt from parity.

The queries (the number of columns each one reads is in its name):
"""
import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
from decimal import Decimal

import obj_client
from formats import CONFIGS

FORMATS = "/data/formats"
MONTH = (2452335, 2452365)    # ss_sold_date_sk of 1 March and 31 March 2002
QUERIES = {
    "sum1":  "SELECT sum(ss_net_paid) FROM {t}",
    "agg5":  "SELECT ss_store_sk, count(*), sum(ss_quantity), avg(ss_sales_price), sum(ss_net_paid), sum(ss_net_profit) "
             "FROM {t} GROUP BY 1 ORDER BY 1",
    "all23": "SELECT count(*), sum(hash(COLUMNS(*))) FROM {t}",
    "month": f"SELECT count(*), sum(ss_net_paid) FROM {{t}} WHERE ss_sold_date_sk BETWEEN {MONTH[0]} AND {MONTH[1]}",
}
__doc__ += "".join(f"  {k:6} {v}\n" for k, v in QUERIES.items())


def table_expr(config, schema):
    """The DuckDB table function that reads one configuration of formats.py."""
    path = f"{FORMATS}/{CONFIGS[config][0]}"
    cols = "{" + ", ".join(f"'{c}': '{t}'" for c, t in schema.items()) + "}"
    if config == "csv":
        return f"read_csv('{path}', header=true, auto_detect=false, columns={cols})"
    if config == "json":
        return f"read_json('{path}', format='newline_delimited', columns={cols})"
    if config == "avro":
        return f"read_avro('{path}')"
    return f"read_parquet('{path}')"


def fingerprint(rows):
    """A short hash of the result, with numbers rounded to cents, to compare results across formats."""
    def norm(v):
        if isinstance(v, (float, Decimal)):
            return f"{float(v):.2f}"
        return str(v)
    return hashlib.md5(repr([[norm(v) for v in r] for r in rows]).encode()).hexdigest()[:8]


def one(sql):
    """Child process: run the query once and print time, CPU time and result as JSON."""
    import resource
    import time
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '2')}")
    con.execute("SET enable_external_file_cache=false")
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    wall = time.perf_counter() - t0
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    cpu = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
    print(json.dumps({"wall": wall, "cpu": cpu, "fp": fingerprint(rows), "first": [str(v) for v in rows[0]][:3]}))


def run(sql):
    p = subprocess.run([sys.executable, __file__, "_one", sql], capture_output=True, text=True)
    if p.returncode:
        raise SystemExit(p.stderr.strip().splitlines()[-1])
    return json.loads(p.stdout)


def evict(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def bench_formats(a):
    try:
        schema = json.load(open(f"{FORMATS}/_schema.json"))["columns"]
    except FileNotFoundError:
        raise SystemExit("no files in /data/formats: run formats.py first (Part A.1)")
    names = [n for n in (a.only.split(",") if a.only else CONFIGS) if os.path.exists(f"{FORMATS}/{CONFIGS[n][0]}")]
    mode = "cold" if a.cold else "warm"
    print(f"query {a.query} ({mode}, {a.runs} runs): {QUERIES[a.query].format(t='<file>')}\n")
    print(f"{'configuration':16} {'MB':>8} {'runs (s)':>24} {'median (s)':>10} {'CPU (s)':>8} {'MB/s':>7}  result")
    fps = {}
    for name in names:
        path = f"{FORMATS}/{CONFIGS[name][0]}"
        sql = QUERIES[a.query].format(t=table_expr(name, schema))
        if not a.cold:
            run(sql)                                   # warm-up, not counted
        res = []
        for _ in range(a.runs):
            if a.cold:
                evict(path)
            res.append(run(sql))
        walls = [r["wall"] for r in res]
        med = statistics.median(walls)
        size = os.path.getsize(path) / 1e6
        fps[name] = res[-1]["fp"]
        print(f"{name:16} {size:>8.1f} {' '.join(f'{w:.3f}' for w in walls):>24} {med:>10.3f} "
              f"{statistics.median(r['cpu'] for r in res):>8.2f} {size / med:>7.0f}  {fps[name]}")
    same = len(set(fps.values())) == 1
    print(f"\nresults: {'SAME in every configuration' if same else 'DIFFERENT  <-- check the formats'}")
    print(f"first values: {res[-1]['first']}")


def bench_obj(a):
    urls = obj_client.urls(a.prefix)
    if not urls:
        raise SystemExit(f"no objects under lake/{a.prefix}: see 'obj status' (and the loading step of the handout)")
    sql = QUERIES[a.query].format(t=f"read_parquet({urls!r})")
    print(f"query {a.query} over {len(urls)} objects under lake/{a.prefix}, {a.runs} runs")
    res, deltas = [], []
    for i in range(a.runs):
        before = obj_client.stats()
        r = run(sql)
        after = obj_client.stats()
        res.append(r)
        deltas.append(obj_client.delta(before, after))
        d = deltas[-1]
        print(f"  run {i + 1}: {r['wall']:.3f} s  ({d['get']} GETs, {obj_client.mb(d['bytes_to_clients'])} MB to DuckDB)")
    med = statistics.median(r["wall"] for r in res)
    print(f"median {med:.3f} s, CPU of DuckDB {statistics.median(r['cpu'] for r in res):.2f} s, result {res[-1]['fp']}: {res[-1]['first']}")
    print("last run:")
    obj_client.print_delta(before, after)


def main():
    if len(sys.argv) > 2 and sys.argv[1] == "_one":
        return one(sys.argv[2])
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="where", required=True)
    p = sub.add_parser("formats", help="the files of formats.py (Part A)")
    p.add_argument("--only", help="comma-separated configurations")
    p.add_argument("--cold", action="store_true", help="drop each file from the page cache before each run")
    p = sub.add_parser("obj", help="Parquet objects in the object store (Parts B and C)")
    p.add_argument("prefix", help="e.g. sf10/store_sales")
    for p in sub.choices.values():
        p.add_argument("--query", choices=QUERIES, default="sum1")
        p.add_argument("--runs", type=int, default=3)
    a = ap.parse_args()
    bench_formats(a) if a.where == "formats" else bench_obj(a)


if __name__ == "__main__":
    main()
