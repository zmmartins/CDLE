"""Run the TPC-DS queries (one file per query) in DuckDB and record their times.

Each table becomes a view over the Parquet folders (--parquet) or directly over the
.dat files (--csv, which also needs --kit for the schema).

Examples:
  python run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split \
                        --out $T/results/sf1_t4.csv --threads 4 --runs 3
  python run_queries.py --csv $T/data/sf1 --kit $T/DSGen-software-code-4.0.0/tools \
                        --queries $T/queries/sf1/split --out $T/results/sf1_csv.csv --only 3,7,42
  python run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split --explain 67
"""
import argparse
import csv
import glob
import os
import re
import statistics
import sys
import threading
import time

import duckdb


def statements(path):
    with open(path) as f:
        sql = "\n".join(l for l in f.read().splitlines() if not l.lstrip().startswith("--"))
    return [s.strip() for s in sql.split(";") if s.strip()]


def query_number(path):
    return int(re.search(r"query(\d+)\.sql$", path).group(1))


def dat_files(src, table):
    return (sorted(glob.glob(os.path.join(src, f"{table}_[0-9]*_[0-9]*.dat")))
            or sorted(glob.glob(os.path.join(src, f"{table}.dat"))))


def create_views(con, args):
    if args.parquet:
        for t in sorted(os.listdir(args.parquet)):
            if os.path.isdir(os.path.join(args.parquet, t)):
                con.execute(f"CREATE VIEW {t} AS SELECT * FROM '{os.path.join(args.parquet, t)}/*.parquet'")
        return
    schema = duckdb.connect()
    with open(os.path.join(args.kit, "tpcds.sql")) as f:
        schema.execute(f.read())
    columns = {}
    for t, c, d in schema.execute("select table_name, column_name, data_type from information_schema.columns "
                                  "order by table_name, ordinal_position").fetchall():
        columns.setdefault(t, []).append((c, d))
    for t, cols in columns.items():
        files = dat_files(args.csv, t)
        if files:
            colspec = "{" + ", ".join(f"'{c}': '{d}'" for c, d in cols) + "}"
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_csv({files!r}, delim='|', header=false, "
                        f"quote='', escape='', nullstr='', columns={colspec})")


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--parquet", help="Parquet folder (one subfolder per table)")
    src.add_argument("--csv", help="folder with the .dat files (needs --kit)")
    ap.add_argument("--kit", help="the kit's tools folder (contains tpcds.sql); needed with --csv")
    ap.add_argument("--queries", required=True, help="folder with query01.sql ... query99.sql")
    ap.add_argument("--out", help="results CSV to write (overwritten)")
    ap.add_argument("--threads", type=int, help="DuckDB threads (default: all cores)")
    ap.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 4GB (default: 80%% of RAM)")
    ap.add_argument("--runs", type=int, default=1, help="how many times to run the whole set")
    ap.add_argument("--only", default="", help="comma-separated query numbers, e.g. 14,23,67")
    ap.add_argument("--timeout", type=int, default=600, help="seconds per query before it is stopped")
    ap.add_argument("--explain", type=int, metavar="Q", help="print EXPLAIN ANALYZE for query Q and exit")
    args = ap.parse_args()
    if args.csv and not args.kit:
        ap.error("--csv needs --kit")
    if not args.out and args.explain is None:
        ap.error("--out is required (unless --explain is used)")

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}'")
    create_views(con, args)

    paths = sorted(glob.glob(os.path.join(args.queries, "query[0-9][0-9].sql")))
    if args.only:
        wanted = {int(q) for q in args.only.split(",")}
        paths = [p for p in paths if query_number(p) in wanted]
    if not paths:
        sys.exit("no query files found")

    if args.explain is not None:
        for p in paths:
            if query_number(p) == args.explain:
                for s in statements(p):
                    print(con.execute("EXPLAIN ANALYZE " + s).fetchall()[0][1])
                return
        sys.exit(f"query {args.explain} not found")

    threads = con.execute("select current_setting('threads')").fetchone()[0]
    memory = con.execute("select current_setting('memory_limit')").fetchone()[0]
    print(f"DuckDB {duckdb.__version__}, threads={threads}, memory_limit={memory}, "
          f"{len(paths)} queries x {args.runs} run(s)", flush=True)

    times = {query_number(p): [] for p in paths}
    rows, errors = {}, {}
    for run in range(1, args.runs + 1):
        run_total = 0.0
        for p in paths:
            q = query_number(p)
            timer = threading.Timer(args.timeout, con.interrupt)
            timer.start()
            t0 = time.perf_counter()
            try:
                rows[q] = "+".join(str(len(con.execute(s).fetchall())) for s in statements(p))
            except Exception as e:  # duckdb raises several exception types
                errors[q] = ("TIMEOUT " if "INTERRUPT" in str(e).upper() else "") + str(e).splitlines()[0][:200]
            finally:
                timer.cancel()
            secs = time.perf_counter() - t0
            times[q].append(secs)
            run_total += secs
        print(f"run {run}: {run_total:.2f}s", flush=True)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "rows", *[f"run{i}_s" for i in range(1, args.runs + 1)], "median_s", "status", "error"])
        for q in sorted(times):
            w.writerow([q, rows.get(q, ""), *[f"{s:.4f}" for s in times[q]],
                        f"{statistics.median(times[q]):.4f}", "ERROR" if q in errors else "OK", errors.get(q, "")])

    medians = {q: statistics.median(v) for q, v in times.items()}
    slowest = sorted(medians.items(), key=lambda x: -x[1])[:5]
    print(f"OK: {len(paths) - len(errors)}/{len(paths)}   sum of medians: {sum(medians.values()):.2f}s")
    print("slowest (median): " + ", ".join(f"q{q} {s:.3f}s" for q, s in slowest))
    for q, e in sorted(errors.items()):
        print(f"q{q:02d} ERROR {e}")
    print(f"results -> {args.out}")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
