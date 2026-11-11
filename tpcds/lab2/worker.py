"""A worker node: a small HTTP server in front of DuckDB.

The coordinator sends a SQL fragment and the list of partitions this worker must read;
the worker runs it and answers with the result as a Parquet file.

  POST /query        JSON {sql, tables, inputs, net}; the answer is a Parquet file
  PUT  /blob/<id>    store a Parquet file sent by the coordinator (a broadcast table)
  GET  /health       name, DuckDB version, limits

In the SQL, {store_sales} or {store_returns} stand for "my partitions of that table" and
{name} for a broadcast table that was sent before.

All workers mount the same data volume, but a worker only reads the partitions named in the
request. On a real cluster each node would keep its partitions on its own disk.
"""
import glob
import json
import os
import socket
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

DATA = os.environ.get("LAB2_DATA", "/data")
TMP = os.environ.get("LAB2_TMP", "/tmp/lab2")
NODE = os.environ.get("NODE", socket.gethostname())
THREADS = os.environ.get("DUCKDB_THREADS", "1")
MEMORY = os.environ.get("DUCKDB_MEMORY", "600MB")

os.makedirs(TMP, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET threads={THREADS}")
con.execute(f"SET memory_limit='{MEMORY}'")
con.execute(f"SET temp_directory='{TMP}/spill'")


def partition_files(layout, table, parts):
    files = []
    for p in parts:
        files += sorted(glob.glob(f"{DATA}/{layout}/{table}/part={p}/*.parquet"))
    if files:
        return f"read_parquet({files!r}, hive_partitioning=false)"
    # all these partitions are empty: an empty table with the right columns
    anyfile = sorted(glob.glob(f"{DATA}/{layout}/{table}/part=*/*.parquet"))[0]
    return f"(select * from read_parquet('{anyfile}', hive_partitioning=false) limit 0)"


def send_slowly(out, data, net):
    """Simulated network: wait delay_ms, then send at no more than mbit megabits per second."""
    time.sleep(net.get("delay_ms", 0) / 1000)
    mbit = net.get("mbit", 0)
    if not mbit:
        out.write(data)
        return
    chunk = 16 * 1024
    for i in range(0, len(data), chunk):
        out.write(data[i:i + chunk])
        time.sleep(chunk * 8 / (mbit * 1e6))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):
        pass

    def reply(self, code, body, headers=None, net=None):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Node", NODE)
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        send_slowly(self.wfile, body, net or {})

    def body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def do_GET(self):
        if self.path == "/health":
            layouts = sorted(os.path.basename(os.path.dirname(p)) for p in glob.glob(f"{DATA}/*/_layout.json"))
            self.reply(200, json.dumps({"node": NODE, "duckdb": duckdb.__version__, "threads": THREADS,
                                        "memory_limit": MEMORY, "layouts": layouts}).encode())
        else:
            self.reply(404, b"{}")

    def do_PUT(self):
        if not self.path.startswith("/blob/"):
            return self.reply(404, b"{}")
        with open(f"{TMP}/blob_{os.path.basename(self.path)}.parquet", "wb") as f:
            f.write(self.body())
        self.reply(200, b"{}")

    def do_POST(self):
        if self.path != "/query":
            return self.reply(404, b"{}")
        req = json.loads(self.body())
        sql = req["sql"]
        for table, where in req.get("tables", {}).items():
            sql = sql.replace("{" + table + "}", partition_files(where["layout"], table, where["parts"]))
        for name, ids in req.get("inputs", {}).items():
            files = [f"{TMP}/blob_{i}.parquet" for i in ids]
            sql = sql.replace("{" + name + "}", f"read_parquet({files!r})")
        out = f"{TMP}/result_{uuid.uuid4().hex}.parquet"
        t0 = time.perf_counter()
        try:
            rows = con.cursor().execute(f"COPY ({sql}) TO '{out}' (FORMAT parquet, COMPRESSION zstd)").fetchone()[0]
            with open(out, "rb") as f:
                data = f.read()
        except Exception as e:  # duckdb raises several exception types
            return self.reply(500, json.dumps({"error": str(e).splitlines()[0][:300]}).encode())
        finally:
            if os.path.exists(out):
                os.remove(out)
        self.reply(200, data, {"X-Rows": rows, "X-Exec-S": f"{time.perf_counter() - t0:.4f}"}, req.get("net"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8100"))
    print(f"worker {NODE}: DuckDB {duckdb.__version__}, threads={THREADS}, memory_limit={MEMORY}, port {port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
