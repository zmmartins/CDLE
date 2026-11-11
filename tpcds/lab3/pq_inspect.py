"""Read the footers of Parquet files (local, or objects in the object store) and say what a query would read.

  python pq_inspect.py sf10/store_sales                          files, row groups, size of every column
  python pq_inspect.py sf10/store_sales --column ss_sold_date_sk min and max of that column in each row group
  python pq_inspect.py sorted/store_sales --between ss_sold_date_sk 2452335 2452365 --columns ss_net_paid
                                                                 the row groups that may hold matching rows, and
                                                                 the bytes of the columns a query needs in them
  python pq_inspect.py /data/formats/store_sales_zstd.parquet    a local file (a path that starts with /)

Only the footers are read: DuckDB's parquet_metadata() fetches the end of each file.
"""
import argparse
import glob
import statistics

import duckdb

import obj_client


def sources(where):
    if where.startswith("/"):
        files = sorted(glob.glob(where if where.endswith(".parquet") else f"{where}/*.parquet"))
    else:
        files = obj_client.urls(where)
    if not files:
        raise SystemExit(f"no Parquet files at {where}")
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("where", help="an object-store prefix (sf10/store_sales) or a local path (/data/...)")
    ap.add_argument("--column", help="show min/max of this column for each row group")
    ap.add_argument("--show", type=int, default=8, help="row groups to list with --column (default 8)")
    ap.add_argument("--between", nargs=3, metavar=("COLUMN", "LO", "HI"), help="a range filter on one column")
    ap.add_argument("--columns", default="", help="with --between: the other columns the query reads")
    a = ap.parse_args()

    files = sources(a.where)
    con = duckdb.connect()
    con.execute("CREATE TABLE md AS SELECT * FROM parquet_metadata(?)", [files])
    con.execute("CREATE TABLE rg AS SELECT DISTINCT file_name, row_group_id, row_group_num_rows AS n FROM md")
    nf, nrg, rows = con.execute("SELECT count(DISTINCT file_name), count(*), sum(n) FROM rg").fetchone()
    sizes = [r[0] for r in con.execute("SELECT n FROM rg").fetchall()]
    total = con.execute("SELECT sum(total_compressed_size) FROM md").fetchone()[0]
    print(f"{nf} files, {rows:,} rows, {nrg} row groups "
          f"(rows per group: min {min(sizes):,}, median {int(statistics.median(sizes)):,}, max {max(sizes):,})")
    print(f"column data: {total / 1e6:,.1f} MB compressed\n")

    if not a.column and not a.between:
        print(f"{'column':24} {'MB':>8} {'share':>6} {'MB raw':>8} {'ratio':>6}  codec   encodings")
        for col, comp, unc, codec, enc in con.execute("""
                SELECT path_in_schema, sum(total_compressed_size), sum(total_uncompressed_size),
                       any_value(compression), string_agg(DISTINCT encodings, ' | ')
                FROM md GROUP BY 1 ORDER BY min(column_id)""").fetchall():
            print(f"{col:24} {comp / 1e6:>8.1f} {comp / total:>6.1%} {unc / 1e6:>8.1f} {unc / comp:>6.2f}  {codec:7} {enc}")
        return

    if a.column:
        lo_all, hi_all = con.execute("SELECT min(stats_min_value::BIGINT), max(stats_max_value::BIGINT) FROM md "
                                     "WHERE path_in_schema = ?", [a.column]).fetchone()
        print(f"{a.column}: from {lo_all} to {hi_all} in the whole table")
        print(f"{'file':24} {'group':>5} {'rows':>9} {'min':>10} {'max':>10} {'width':>7}")
        q = con.execute("""SELECT regexp_extract(file_name, '[^/]+$'), row_group_id, row_group_num_rows,
                                  stats_min_value::BIGINT, stats_max_value::BIGINT
                           FROM md WHERE path_in_schema = ? ORDER BY file_name, row_group_id""", [a.column]).fetchall()
        widths, nulls = [], 0
        for i, (f, g, n, lo, hi) in enumerate(q):
            if lo is None:                    # a row group where this column is always NULL
                nulls += 1
                if i < a.show:
                    print(f"{f:24} {g:>5} {n:>9,} {'(all NULL)':>21}")
                continue
            w = (hi - lo) / max(hi_all - lo_all, 1)
            widths.append(w)
            if i < a.show:
                print(f"{f:24} {g:>5} {n:>9,} {lo:>10} {hi:>10} {w:>7.1%}")
        print(f"... {len(q)} row groups ({nulls} with only NULLs); "
              f"median width of [min, max]: {statistics.median(widths):.1%} of the whole range")

    if a.between:
        col, lo, hi = a.between[0], int(a.between[1]), int(a.between[2])
        need = [col] + [c for c in a.columns.split(",") if c]
        hit = con.execute("""SELECT count(*) FROM md WHERE path_in_schema = ?
                             AND stats_max_value::BIGINT >= ? AND stats_min_value::BIGINT <= ?""", [col, lo, hi]).fetchone()[0]
        nbytes = con.execute(f"""
            SELECT sum(total_compressed_size) FROM md m
            WHERE path_in_schema IN ({','.join('?' * len(need))})
              AND (file_name, row_group_id) IN (SELECT file_name, row_group_id FROM md WHERE path_in_schema = ?
                   AND stats_max_value::BIGINT >= ? AND stats_min_value::BIGINT <= ?)""", need + [col, lo, hi]).fetchone()[0] or 0
        print(f"\nfilter {col} BETWEEN {lo} AND {hi}, columns read: {', '.join(need)}")
        print(f"row groups that may hold matching rows: {hit} of {nrg}  (the others are skipped using min/max)")
        print(f"bytes of those columns in those row groups: {nbytes / 1e6:,.1f} MB  (plus the footers)")


if __name__ == "__main__":
    main()
