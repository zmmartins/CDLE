"""Run one SQL query against the TPC-DS data and show the result as a table.

The query can come from a file (for example one of the files in examples/) or from --sql.
Each table is available as a view over the Parquet folders (--parquet) or over the .dat files
(--csv, which also needs --kit for the schema).

Examples:
  python show_query.py --parquet $T/parquet/sf1 $T/starter/examples/02_small_table.sql
  python show_query.py --csv $T/data/sf1 --kit $T/DSGen-software-code-4.0.0/tools \
                       $T/starter/examples/02_small_table.sql
  python show_query.py --parquet $T/parquet/sf1 --sql "select * from reason limit 5"
  python show_query.py --parquet $T/parquet/sf1 --out result.csv $T/starter/examples/07_simple_join.sql
"""
import argparse
import os
import sys
import time

import duckdb

from run_queries import create_views


def split_statements(text):
    sql = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
    return [s.strip() for s in sql.split(";") if s.strip()]


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--parquet", help="Parquet folder (one subfolder per table)")
    src.add_argument("--csv", help="folder with the .dat files (needs --kit)")
    ap.add_argument("--kit", help="the kit's tools folder (contains tpcds.sql); needed with --csv")
    ap.add_argument("file", nargs="?", help="SQL file to run")
    ap.add_argument("--sql", help="SQL text to run instead of a file")
    ap.add_argument("--max-rows", type=int, default=20, help="rows to display (default 20)")
    ap.add_argument("--max-width", type=int, default=160, help="display width in characters (default 160)")
    ap.add_argument("--out", help="also save the full result to this CSV file")
    ap.add_argument("--threads", type=int, help="DuckDB threads (default: all cores)")
    args = ap.parse_args()
    if args.csv and not args.kit:
        ap.error("--csv needs --kit")
    if bool(args.file) == bool(args.sql):
        ap.error("give either a SQL file or --sql")

    if args.file:
        with open(args.file) as f:
            text = f.read()
    else:
        text = args.sql
    stmts = split_statements(text)
    if not stmts:
        sys.exit("no SQL statement found")

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    create_views(con, args)

    for i, s in enumerate(stmts, 1):
        t0 = time.perf_counter()
        rel = con.sql(s)
        if rel is None:  # statement without a result, e.g. SET
            print(f"statement {i}: done in {time.perf_counter() - t0:.3f}s")
            continue
        if args.out:
            out = args.out if len(stmts) == 1 else f"{os.path.splitext(args.out)[0]}_{i}.csv"
            rel.write_csv(out)
            print(f"full result saved to {out}")
        rel.show(max_rows=args.max_rows, max_width=args.max_width)
        print(f"statement {i}: {time.perf_counter() - t0:.3f}s")


if __name__ == "__main__":
    main()
