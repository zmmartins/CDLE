"""Write the web server's access logs of Lab 4, and the right answers, from the TPC-DS tables of Lab 1.

Every row of web_sales becomes one request for a product page, in the Apache "combined" format:

  10.4.17.203 - - [14/Mar/2002:10:22:01 +0000] "GET /product/AAAAAAAAOKJDAAAA?qty=57 HTTP/1.1" 200 1019 "/search?q=electoral+ports" "Mozilla/5.0 (...)"

About half of the sales are preceded by a search (GET /search?q=...), whose words come from the item's
description, and 2% of the lines are health checks of the load balancer. A few lines are dirty, on purpose
and always the same ones: requests that failed (404), HEAD requests, product ids in lower case, a user agent
with bytes that are not UTF-8, and lines cut short.

The right answers go to /data/truth/sfN/ (Parquet):
  words.parquet        word, n       the words of all searches
  products.parquet     product, n    product page views: GET, status 200, id in upper case, complete lines only
  product_day.parquet  product, day, n   the same, per day

Usage: python make_logs.py --sf 1     (writes /data/logs/sf1/access-000.log ...)
"""
import argparse
import glob
import os
import shutil
import time

import duckdb

from common import DATA, PARQUET, evidence, logs

MARK = "@@NOT-UTF8@@"
BAD = "\xe9t\xe9".encode("latin-1")         # "été" written in Latin-1: three bytes that are not valid UTF-8

# What each line is, decided by h = hash(order, item) % 1000, so that every run writes the same file.
BASE = f"""
WITH s AS (
  SELECT ws.ws_order_number AS o, ws.ws_item_sk AS it, ws.ws_quantity AS qty,
         coalesce(ws.ws_bill_customer_sk, 0) AS c,
         d.d_date + to_seconds(coalesce(t.t_time, 0)) AS ts,
         i.i_item_id AS id,
         list_filter(string_split(regexp_replace(lower(coalesce(i.i_item_desc, '')), '[^a-z ]', ' ', 'g'), ' '),
                     x -> x <> '') AS w,
         (hash(ws.ws_order_number, ws.ws_item_sk) % 1000)::BIGINT AS h,
         hash(ws.ws_item_sk, ws.ws_order_number, 7) % 2 = 0 AS searched,
         hash(ws.ws_order_number, ws.ws_item_sk, 3) % 50 = 0 AS probe
  FROM read_parquet('{PARQUET}/sf{{sf}}/web_sales/*.parquet') ws
  JOIN read_parquet('{PARQUET}/sf{{sf}}/item/*.parquet') i ON i.i_item_sk = ws.ws_item_sk
  JOIN read_parquet('{PARQUET}/sf{{sf}}/date_dim/*.parquet') d ON d.d_date_sk = ws.ws_sold_date_sk
  LEFT JOIN read_parquet('{PARQUET}/sf{{sf}}/time_dim/*.parquet') t ON t.t_time_sk = ws.ws_sold_time_sk
), k AS (
  SELECT *,
         len(w) > 0 AND searched AS has_search,
         1 + h % 3 AS nw,
         '10.' || (c % 251) || '.' || ((c // 251) % 251) || '.' || (1 + (c // 63001) % 250) AS ip,
         CASE WHEN h < 10 THEN 404 ELSE 200 END AS status,
         CASE WHEN h BETWEEN 19 AND 21 THEN 'HEAD' ELSE 'GET' END AS method,
         CASE WHEN h BETWEEN 10 AND 14 THEN lower(id) ELSE id END AS shown_id,
         h BETWEEN 17 AND 18 AS truncated,
         CASE WHEN h BETWEEN 15 AND 16 THEN 'Mozilla/5.0 (Linux; Android 14; {MARK})'
              ELSE ['Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
                    'Mozilla/5.0 (X11; Linux x86_64)', 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)'][1 + h % 4]
         END AS ua
  FROM s
), q AS (
  SELECT *, w[1 + (h // 7) % greatest(len(w) - nw + 1, 1) : (h // 7) % greatest(len(w) - nw + 1, 1) + nw] AS qw
  FROM k
)
"""

LINES = """
SELECT line FROM (
  -- the product page
  SELECT ts, o, it, 2 AS kind,
         CASE WHEN truncated THEN left(full_line, 40 + (h * 13) % 50) ELSE full_line END AS line
  FROM (SELECT *, ip || ' - - [' || strftime(ts, '%d/%b/%Y:%H:%M:%S') || ' +0000] "' || method || ' /product/' || shown_id
                  || '?qty=' || coalesce(qty, 1) || ' HTTP/1.1" ' || status || ' ' || (500 + (h * 37) % 9000) || ' "'
                  || CASE WHEN has_search THEN '/search?q=' || array_to_string(qw, '+') ELSE '/' END || '" "' || ua || '"'
                  AS full_line FROM q)
  UNION ALL
  -- the search before it
  SELECT ts - to_seconds(1 + h % 300), o, it, 1,
         ip || ' - - [' || strftime(ts - to_seconds(1 + h % 300), '%d/%b/%Y:%H:%M:%S') || ' +0000] "GET /search?q='
         || array_to_string(qw, '+') || ' HTTP/1.1" 200 ' || (2000 + (h * 53) % 30000) || ' "/" "' || ua || '"'
  FROM q WHERE has_search
  UNION ALL
  -- a health check of the load balancer
  SELECT ts + to_seconds(h % 60), o, it, 3,
         '10.0.0.' || (2 + h % 3) || ' - - [' || strftime(ts + to_seconds(h % 60), '%d/%b/%Y:%H:%M:%S')
         || ' +0000] "GET /health HTTP/1.1" 200 2 "-" "kube-probe/1.29"'
  FROM q WHERE probe
)
ORDER BY ts, o, it, kind
"""

TRUTH = {
    "words": """SELECT word, count(*)::BIGINT AS n FROM (SELECT unnest(qw) AS word FROM q WHERE has_search)
                GROUP BY word ORDER BY n DESC, word""",
    "products": """SELECT id AS product, count(*)::BIGINT AS n FROM q
                   WHERE method = 'GET' AND status = 200 AND NOT truncated GROUP BY id ORDER BY product""",
    "product_day": """SELECT id AS product, CAST(ts AS DATE) AS day, count(*)::BIGINT AS n FROM q
                      WHERE method = 'GET' AND status = 200 AND NOT truncated GROUP BY ALL ORDER BY product, day""",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sf", type=int, choices=[1, 10], required=True)
    ap.add_argument("--files", type=int, help="number of log files (default: 4 for SF1, 16 for SF10)")
    a = ap.parse_args()
    evidence("make_logs")
    nfiles = a.files or (4 if a.sf == 1 else 16)
    for t in ("web_sales", "item", "date_dim", "time_dim"):
        if not glob.glob(f"{PARQUET}/sf{a.sf}/{t}/*.parquet"):
            raise SystemExit(f"folder not found: {PARQUET}/sf{a.sf}/{t} (the Parquet files of Lab 1, in ~/tpcds/parquet)")

    t0 = time.time()
    os.makedirs(f"{DATA}/tmp/make_logs", exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads = {os.environ.get('DUCKDB_THREADS', '2')}")
    con.execute("SET memory_limit = '1GB'")
    con.execute(f"SET temp_directory = '{DATA}/tmp/make_logs'")
    con.execute("SET preserve_insertion_order = false")
    base = BASE.replace("{sf}", str(a.sf))

    # 1. The right answers
    tdir = f"{DATA}/truth/sf{a.sf}"
    os.makedirs(tdir, exist_ok=True)
    for name, sql in TRUTH.items():
        con.execute(f"COPY ({base} {sql}) TO '{tdir}/{name}.parquet' (FORMAT parquet)")
    counts = con.execute(f"""{base}
        SELECT count(*), count(*) FILTER (has_search), count(*) FILTER (probe),
               count(*) FILTER (status = 404), count(*) FILTER (method = 'HEAD'), count(*) FILTER (shown_id <> id),
               count(*) FILTER (ua LIKE '%{MARK}%'), count(*) FILTER (truncated),
               count(*) FILTER (method = 'GET' AND status = 200 AND NOT truncated)
        FROM q""").fetchone()
    t1 = time.time()

    # 2. The log files, in time order; the bytes that are not UTF-8 are put in here, after DuckDB
    ldir = logs(a.sf)
    shutil.rmtree(ldir, ignore_errors=True)
    os.makedirs(ldir)
    total = counts[0] + counts[1] + counts[2]
    per_file = -(-total // nfiles)
    cur = con.execute(f"{base} {LINES}")
    fno, in_file, f = 0, 0, open(f"{ldir}/access-000.log", "wb")
    while rows := cur.fetchmany(200_000):
        lines = [r[0] for r in rows]
        while lines:
            take = lines[: per_file - in_file]
            lines = lines[len(take):]
            f.write(("\n".join(take) + "\n").encode("utf-8").replace(MARK.encode(), BAD))
            in_file += len(take)
            if in_file == per_file and lines:
                f.close()
                fno, in_file = fno + 1, 0
                f = open(f"{ldir}/access-{fno:03d}.log", "wb")
    f.close()
    shutil.rmtree(f"{DATA}/tmp/make_logs", ignore_errors=True)
    t2 = time.time()

    size = sum(os.path.getsize(p) for p in glob.glob(f"{ldir}/*.log"))
    nf = len(glob.glob(f"{ldir}/*.log"))
    print(f"SF{a.sf}: {total:,} lines in {nf} files, {size / 1e6:,.1f} MB, in {ldir}")
    print(f"  product pages      {counts[0]:>12,}   (one per row of web_sales)")
    print(f"  searches           {counts[1]:>12,}")
    print(f"  health checks      {counts[2]:>12,}")
    print(f"  dirty product lines: 404 {counts[3]:,}, HEAD {counts[4]:,}, id in lower case {counts[5]:,}, "
          f"not UTF-8 {counts[6]:,}, cut short {counts[7]:,}")
    print(f"  product page views to count (GET, 200, complete): {counts[8]:,}")
    print(f"Right answers in {tdir}: words, products, product_day")
    print(f"Time: {t2 - t0:.1f} s (answers {t1 - t0:.1f} s, log files {t2 - t1:.1f} s)")


if __name__ == "__main__":
    main()
