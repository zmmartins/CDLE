"""Put a copy of store_sales in the object store, as it is or rewritten (sorted, other row-group size).

  python load.py --name sf10                               the Lab 1 files of SF10, as they are
  python load.py --name sorted --sort ss_sold_date_sk      rewritten, sorted by date
  python load.py --name sorted_rg1m --sort ss_sold_date_sk --row-group-size 1000000

The objects are stored under lake/<name>/store_sales/. A rewritten copy is first written by
DuckDB into /data/tmp (the client's volume), then uploaded, then deleted from /data/tmp.
"""
import argparse
import os
import shutil
import time

import duckdb

import obj_client


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True, help="the copy's name: lake/<name>/store_sales/")
    ap.add_argument("--source", default="/parquet/sf10/store_sales", help="default: the SF10 files of Lab 1")
    ap.add_argument("--sort", help="rewrite the table sorted by this column")
    ap.add_argument("--row-group-size", type=int, help="rewrite with row groups of this many rows")
    a = ap.parse_args()

    if not os.path.isdir(a.source):
        raise SystemExit(f"folder not found: {a.source} (are your Lab 1 files in ~/tpcds/parquet/sf10?)")
    folder = a.source
    if a.sort or a.row_group_size:
        folder = f"/data/tmp/{a.name}"
        shutil.rmtree(folder, ignore_errors=True)
        os.makedirs("/data/tmp/spill", exist_ok=True)
        con = duckdb.connect()
        con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '2')}")
        con.execute("SET memory_limit='1200MB'")
        con.execute("SET temp_directory='/data/tmp/spill'")
        con.execute("SET preserve_insertion_order=true")
        order = f"ORDER BY {a.sort}" if a.sort else ""
        rg = f", ROW_GROUP_SIZE {a.row_group_size}" if a.row_group_size else ""
        t0 = time.perf_counter()
        con.execute(f"""COPY (SELECT * FROM read_parquet('{a.source}/*.parquet') {order})
                        TO '{folder}' (FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES '64MB'{rg})""")
        print(f"rewrote {a.source} {order or ''}{rg.replace(',', '')} in {time.perf_counter() - t0:.0f} s")
    obj_client.cmd_put(argparse.Namespace(path=folder, prefix=f"{a.name}/store_sales"))
    if folder != a.source:
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == "__main__":
    main()
