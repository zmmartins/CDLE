"""Write a partitioned copy of a table: one folder per partition, as on a cluster where each
node stores some of the partitions.

The result is  /data/<name>/<table>/part=<p>/*.parquet  plus a small _layout.json with the
rule and the number of rows and bytes of each partition. Only the columns listed in
layout.py are kept.

Examples (inside the client container, see the handout for the `lab` shortcut):
  python partition.py --parquet /parquet/sf10 --name by_ticket --key ss_ticket_number
  python partition.py --parquet /parquet/sf10 --name by_ticket --table store_returns --key sr_ticket_number
  python partition.py --parquet /parquet/sf10 --name mine --expr "hash(ss_item_sk, ss_store_sk)"
"""
import argparse
import glob
import json
import os
import shutil
import time

import duckdb

import layout


def main():
    ap = argparse.ArgumentParser()
    layout.add_args(ap)
    ap.add_argument("--out", default=os.environ.get("LAB2_DATA", "/data"), help="data folder (default /data)")
    ap.add_argument("--name", required=True, help="name of this layout, e.g. by_ticket")
    args = ap.parse_args()
    layout.check_args(ap, args)

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    base = os.path.join(os.path.expanduser(args.out), args.name)
    dst = os.path.join(base, args.table)
    if os.path.exists(dst):
        shutil.rmtree(dst)
    os.makedirs(base, exist_ok=True)

    t0 = time.perf_counter()
    cols = ", ".join(layout.COLUMNS[args.table])
    con.execute(f"COPY (select {cols}, {layout.part_expr(con, args)} as part from {layout.source(args)}) "
                f"TO '{dst}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (part))")
    secs = time.perf_counter() - t0

    rows, size = [], []
    for p in range(args.parts):
        files = glob.glob(os.path.join(dst, f"part={p}", "*.parquet"))
        rows.append(con.execute(f"select count(*) from read_parquet({files!r})").fetchone()[0] if files else 0)
        size.append(sum(os.path.getsize(f) for f in files))

    meta_path = os.path.join(base, "_layout.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
    if meta.get("parts", args.parts) != args.parts:
        raise SystemExit(f"layout {args.name} already has a table with {meta['parts']} partitions; "
                         "use the same --parts or another --name")
    meta["parts"] = args.parts
    meta.setdefault("tables", {})[args.table] = {"rule": layout.describe(args), "rows": rows, "bytes": size}
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=1)

    mean = sum(rows) / args.parts
    print(f"{args.name}/{args.table}: {layout.describe(args)}, written in {secs:.1f}s")
    print(f"{'part':>5} {'rows':>12} {'MB':>8}")
    for p in range(args.parts):
        print(f"{p:>5} {rows[p]:>12,} {size[p] / 1e6:>8.1f}")
    print(f"total {sum(rows):,} rows, {sum(size) / 1e6:.0f} MB   max/mean rows = {max(rows) / mean:.2f}")


if __name__ == "__main__":
    main()
