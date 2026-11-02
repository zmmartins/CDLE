"""Show how evenly a partitioning rule spreads the rows of a table. Nothing is written to disk.

Examples:
  python skew.py --parquet $T/parquet/sf10 --key ss_ticket_number
  python skew.py --parquet $T/parquet/sf10 --key ss_customer_sk --parts 100
  python skew.py --parquet $T/parquet/sf10 --key ss_sold_date_sk --scheme range
  python skew.py --parquet $T/parquet/sf10 --key ss_sold_date_sk --scheme quantile
  python skew.py --parquet $T/parquet/sf10 \
      --expr "case when ss_customer_sk is null then hash(ss_ticket_number) else hash(ss_customer_sk) end"
"""
import argparse
import statistics

import duckdb

import layout


def main():
    ap = argparse.ArgumentParser()
    layout.add_args(ap)
    ap.add_argument("--top", type=int, default=5, help="how many of the most frequent keys to list (default 5)")
    args = ap.parse_args()
    layout.check_args(ap, args)

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    src = layout.source(args)
    counts = dict(con.execute(f"select {layout.part_expr(con, args)} as part, count(*) from {src} "
                              "group by 1").fetchall())
    rows = [counts.get(p, 0) for p in range(args.parts)]
    total, mean = sum(rows), sum(rows) / args.parts

    print(f"{args.table}: {total:,} rows, {layout.describe(args)}")
    order = sorted(range(args.parts), key=lambda p: -rows[p])
    shown = range(args.parts) if args.parts <= 16 else order[:5] + order[-3:]
    if args.parts > 16:
        print("(the 5 largest and the 3 smallest partitions)")
    print(f"{'part':>5} {'rows':>12} {'share':>7} {'x mean':>7}")
    for p in shown:
        print(f"{p:>5} {rows[p]:>12,} {rows[p] / total:>7.1%} {rows[p] / mean:>7.2f}  {'#' * round(20 * rows[p] / max(rows))}")

    ranked = sorted(rows)
    p99 = ranked[min(args.parts - 1, int(0.99 * args.parts))]
    print(f"min {ranked[0]:,}   p50 {int(statistics.median(rows)):,}   p99 {p99:,}   max {ranked[-1]:,}   "
          f"empty partitions: {rows.count(0)}")
    print(f"max/mean = {ranked[-1] / mean:.2f}   (1.00 is a perfect balance)")

    if args.key and args.top:
        keys = con.execute(f"select {args.key}, count(*) n from {src} group by 1 order by n desc limit {args.top}").fetchall()
        distinct = con.execute(f"select count(distinct {args.key}) from {src}").fetchone()[0]
        print(f"{args.key}: {distinct:,} distinct values; most frequent: "
              + ", ".join(f"{'NULL' if k is None else k} ({n / total:.2%})" for k, n in keys))


if __name__ == "__main__":
    main()
