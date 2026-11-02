"""Convert dsdgen .dat files to Parquet with DuckDB, one folder per table.

Column names and types come from the kit's tpcds.sql (the .dat files have no header).
After each table, the Parquet row count is compared with the number of lines in the .dat files.

Usage:
  python to_parquet.py --kit <kit>/tools --src <.dat folder> --dst <parquet folder> [--threads N] [--memory-limit 6GB]
"""
import argparse
import glob
import os
import sys
import time

import duckdb


def dat_files(src, table):
    # split pieces are named <table>_<child>_<parallel>.dat; the [0-9] keeps
    # "customer" from also matching customer_address_*.dat
    return (sorted(glob.glob(os.path.join(src, f"{table}_[0-9]*_[0-9]*.dat")))
            or sorted(glob.glob(os.path.join(src, f"{table}.dat"))))


def line_count(files):
    lines = 0
    for name in files:
        with open(name, "rb") as f:
            while chunk := f.read(1 << 24):
                lines += chunk.count(b"\n")
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kit", required=True, help="the kit's tools folder (contains tpcds.sql)")
    ap.add_argument("--src", required=True, help="folder with the .dat files")
    ap.add_argument("--dst", required=True, help="Parquet output folder (must not exist)")
    ap.add_argument("--threads", type=int)
    ap.add_argument("--memory-limit")
    args = ap.parse_args()

    if os.path.exists(args.dst):
        sys.exit(f"{args.dst} already exists; remove it first")

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}'")
    with open(os.path.join(args.kit, "tpcds.sql")) as f:
        con.execute(f.read())  # empty tables, used only as the schema
    tables = [r[0] for r in con.execute(
        "select table_name from information_schema.tables order by 1").fetchall()]

    os.makedirs(args.dst)
    convert_secs, mismatches = 0.0, 0
    for t in tables:
        files = dat_files(args.src, t)
        if not files:
            print(f"{t:<24} no .dat files found, skipped")
            continue
        cols = con.execute("select column_name, data_type from information_schema.columns "
                           "where table_name = ? order by ordinal_position", [t]).fetchall()
        colspec = "{" + ", ".join(f"'{c}': '{d}'" for c, d in cols) + "}"
        t0 = time.perf_counter()
        con.execute(f"""
            COPY (SELECT * FROM read_csv({files!r}, delim='|', header=false, quote='', escape='',
                                         nullstr='', columns={colspec}))
            TO '{os.path.join(args.dst, t)}' (FORMAT parquet, COMPRESSION zstd, PER_THREAD_OUTPUT true)""")
        secs = time.perf_counter() - t0
        convert_secs += secs
        pq_rows = con.execute(f"select count(*) from '{os.path.join(args.dst, t)}/*.parquet'").fetchone()[0]
        dat_rows = line_count(files)
        ok = pq_rows == dat_rows
        mismatches += not ok
        print(f"{t:<24} files={len(files):<3} rows={pq_rows:<11} dat_lines={dat_rows:<11} "
              f"{'OK' if ok else 'MISMATCH'}  {secs:.1f}s", flush=True)

    print(f"Conversion time (COPY only): {convert_secs:.1f}s; row-count mismatches: {mismatches}")
    sys.exit(1 if mismatches else 0)


if __name__ == "__main__":
    main()
