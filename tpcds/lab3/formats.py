"""Part A: write store_sales in five configurations and record the time and the size of each.

  python formats.py --sf 1                 all five, from the Lab 1 Parquet files of SF1
  python formats.py --sf 1 --only json     one of them again

The table is first loaded into memory, so that the time measured is only the time to encode
and write each format, not the time to read the Lab 1 files. The files go to /data/formats,
in the client's Docker volume. The column types are saved in _schema.json, for read_bench.py.
"""
import argparse
import json
import os
import time

import duckdb

CONFIGS = {
    "csv":            ("store_sales.csv",            "FORMAT csv, HEADER"),
    "json":           ("store_sales.json",           "FORMAT json"),
    "avro":           ("store_sales.avro",           "FORMAT avro"),
    "parquet_snappy": ("store_sales_snappy.parquet", "FORMAT parquet, COMPRESSION snappy"),
    "parquet_zstd":   ("store_sales_zstd.parquet",   "FORMAT parquet, COMPRESSION zstd"),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="1", help="scale factor of the Lab 1 files to use (default 1)")
    ap.add_argument("--parquet", default="/parquet", help="the Lab 1 Parquet folder, as the container sees it")
    ap.add_argument("--out", default="/data/formats")
    ap.add_argument("--only", help="comma-separated configurations, e.g. json,avro")
    a = ap.parse_args()

    src = f"{a.parquet}/sf{a.sf}/store_sales"
    if not os.path.isdir(src):
        raise SystemExit(f"folder not found: {src} (are your Lab 1 files in ~/tpcds/parquet/sf{a.sf}?)")
    os.makedirs(a.out, exist_ok=True)
    names = a.only.split(",") if a.only else list(CONFIGS)

    con = duckdb.connect()
    con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '2')}")
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE store_sales AS SELECT * FROM read_parquet('{src}/*.parquet')")
    rows = con.execute("SELECT count(*) FROM store_sales").fetchone()[0]
    schema = {c: t for c, t, *_ in con.execute("DESCRIBE store_sales").fetchall()}
    print(f"loaded {src}: {rows:,} rows, {len(schema)} columns, in {time.perf_counter() - t0:.1f} s")
    with open(f"{a.out}/_schema.json", "w") as f:
        json.dump({"sf": a.sf, "rows": rows, "columns": schema}, f, indent=1)

    sizes = {}
    print(f"\n{'configuration':16} {'file':28} {'write (s)':>9} {'MB':>9}")
    for name in names:
        fname, opts = CONFIGS[name]
        path = f"{a.out}/{fname}"
        t0 = time.perf_counter()
        con.execute(f"COPY store_sales TO '{path}' ({opts})")
        dt = time.perf_counter() - t0
        sizes[name] = os.path.getsize(path)
        print(f"{name:16} {fname:28} {dt:>9.2f} {sizes[name] / 1e6:>9.1f}")

    csv = f"{a.out}/{CONFIGS['csv'][0]}"
    if os.path.exists(csv):
        base = os.path.getsize(csv)
        print(f"\nsize compared with the CSV file ({base / 1e6:.1f} MB):")
        for name in CONFIGS:
            p = f"{a.out}/{CONFIGS[name][0]}"
            if os.path.exists(p):
                print(f"  {name:16} {os.path.getsize(p) / base:6.3f}")


if __name__ == "__main__":
    main()
