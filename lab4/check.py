"""Compare the output of a job with the right answer that make_logs.py computed from the Parquet files of Lab 1.

  python check.py words       /data/out/mr_words_sf1      --sf 1
  python check.py products    /data/out/df_products_sf1   --sf 1
  python check.py product_day /data/out/pipeline_sf1      --sf 1

The output may be a folder of text files (one "key<TAB>count" per line, as mapreduce.py and the Spark
scripts write it) or of Parquet files. Columns are taken by position: words (word, n), products (product, n),
product_day (product, day, n). Prints SAME or DIFFERENT, and the first differences.
"""
import argparse
import glob
import os

import duckdb

from common import evidence, truth

KEYS = {"words": ["word"], "products": ["product"], "product_day": ["product", "day"]}


def load(con, path, kind):
    keys = KEYS[kind]
    cols = keys + ["n"]
    files = glob.glob(f"{path}/**/*.parquet", recursive=True) if os.path.isdir(path) else [path]
    if os.path.isdir(path) and files:
        rel = f"read_parquet('{path}/**/*.parquet')"
    elif os.path.isdir(path):
        parts = [f for f in glob.glob(f"{path}/**/*", recursive=True)
                 if os.path.isfile(f) and not os.path.basename(f).startswith(("_", ".")) and not f.endswith(".crc")]
        if not parts:
            raise SystemExit(f"no output files in {path}")
        types = ", ".join(f"'c{i}': 'VARCHAR'" for i in range(len(cols)))
        rel = f"read_csv({parts!r}, delim='\\t', header=false, quote='', escape='', columns={{{types}}})"
    elif path.endswith(".parquet"):
        rel = f"read_parquet('{path}')"
    else:
        raise SystemExit(f"not found: {path}")
    got = con.sql(f"SELECT * FROM {rel}")
    if len(got.columns) < len(cols):
        raise SystemExit(f"{path}: {len(got.columns)} columns, expected {len(cols)} ({', '.join(cols)})")
    select = ", ".join(f"CAST(\"{c}\" AS VARCHAR) AS {name}" for c, name in zip(got.columns[:len(keys)], keys))
    con.execute(f"CREATE OR REPLACE TEMP TABLE got AS SELECT {select}, CAST(\"{got.columns[len(keys)]}\" AS BIGINT) AS n FROM {rel}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("kind", choices=sorted(KEYS))
    ap.add_argument("path", help="the job's output: a folder or a file")
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    a = ap.parse_args()
    evidence("check")
    keys = KEYS[a.kind]
    con = duckdb.connect()
    os.makedirs("/data/tmp/check", exist_ok=True)
    con.execute("SET temp_directory = '/data/tmp/check'")      # /lab is read-only
    load(con, a.path, a.kind)
    k = ", ".join(keys)
    con.execute(f"CREATE TEMP TABLE want AS SELECT {', '.join(f'CAST({c} AS VARCHAR) AS {c}' for c in keys)}, n "
                f"FROM read_parquet('{truth(a.sf, a.kind)}')")
    on = " AND ".join(f"g.{c} = w.{c}" for c in keys)
    con.execute(f"""CREATE TEMP TABLE diff AS SELECT {', '.join(f'coalesce(g.{c}, w.{c}) AS {c}' for c in keys)}, w.n AS expected, g.n AS got
                       FROM (SELECT {k}, sum(n) AS n, count(*) AS rows FROM got GROUP BY ALL) g
                       FULL JOIN want w ON {on}
                       WHERE g.n IS DISTINCT FROM w.n""")
    dups = con.sql(f"SELECT count(*) FROM (SELECT {k} FROM got GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    g_rows, g_sum = con.sql("SELECT count(*), coalesce(sum(n), 0) FROM got").fetchone()
    w_rows, w_sum = con.sql("SELECT count(*), sum(n) FROM want").fetchone()
    missing = con.sql(f"SELECT count(*) FROM diff WHERE got IS NULL").fetchone()[0]
    extra = con.sql(f"SELECT count(*) FROM diff WHERE expected IS NULL").fetchone()[0]
    wrong = con.sql(f"SELECT count(*) FROM diff WHERE got IS NOT NULL AND expected IS NOT NULL").fetchone()[0]

    print(f"{a.kind}, SF{a.sf}: {a.path}")
    print(f"  output: {g_rows:,} rows, total {g_sum:,}    right answer: {w_rows:,} rows, total {w_sum:,}")
    if missing + extra + wrong + dups == 0:
        print("SAME")
        return
    print(f"DIFFERENT: {missing:,} keys missing, {extra:,} keys that should not be there, "
          f"{wrong:,} keys with a wrong count, {dups:,} keys on more than one row")
    for row in con.sql("SELECT * FROM diff ORDER BY abs(coalesce(got, 0) - coalesce(expected, 0)) DESC LIMIT 8").fetchall():
        print("   ", " | ".join("—" if v is None else str(v) for v in row), " (key | expected | got)")


if __name__ == "__main__":
    main()
