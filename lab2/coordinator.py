"""The coordinator: sends the fragments of a query to the workers, merges their answers and
compares the result with the single-node answer.

Partition p is given to worker number (p mod N), where N is --nodes. If a worker does not
answer within --timeout seconds, or refuses the connection, its partitions are given to the
next worker that is still alive and those fragments are run again.

Examples (inside the client container, see the handout for the `coord` shortcut):
  python coordinator.py status
  python coordinator.py run q1 --plan partial --layout by_ticket --nodes 4 --runs 3
  python coordinator.py run q3 --plan broadcast --layout by_customer --nodes 4 --mbit 100 --delay-ms 5
  python coordinator.py run q2 --plan naive --layout by_store --nodes 8
"""
import argparse
import hashlib
import http.client
import json
import os
import socket
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import duckdb

from plans import PLANS

DATA = os.environ.get("LAB2_DATA", "/data")
TMP = os.environ.get("LAB2_TMP", "/tmp/lab2")
WORKERS = os.environ.get("LAB2_WORKERS", ",".join(f"w{i}:8100" for i in range(1, 9)))


class WorkerDown(Exception):
    pass


def request(addr, method, path, body=b"", timeout=10.0, mbit=0):
    """One HTTP request to a worker. Returns (status, headers, body); raises WorkerDown."""
    host, _, port = addr.partition(":")
    conn = http.client.HTTPConnection(host, int(port or 8100), timeout=timeout)
    try:
        payload = body
        if mbit and body:  # simulated network on the way out: send in small pieces, with pauses
            chunk = 16 * 1024

            def slowly():
                for i in range(0, len(body), chunk):
                    yield body[i:i + chunk]
                    time.sleep(chunk * 8 / (mbit * 1e6))
            payload = slowly()
        conn.request(method, path, body=payload, headers={"Content-Length": str(len(body))})
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    except (OSError, http.client.HTTPException) as e:
        if "timed out" in str(e):
            raise WorkerDown(f"no answer in {timeout:g}s")
        if isinstance(e, socket.gaierror):
            raise WorkerDown("host not found (is the container running?)")
        raise WorkerDown(type(e).__name__)
    finally:
        conn.close()


def name(addr):
    return addr[:-5] if addr.endswith(":8100") else addr


class Run:
    def __init__(self, args, workers, meta):
        self.args = args
        self.net = {"delay_ms": args.delay_ms, "mbit": args.mbit}
        self.ring = workers[:args.nodes]          # the nodes of this run, in order
        self.alive = list(self.ring)
        self.assign = {w: [p for p in range(meta["parts"]) if p % args.nodes == i]
                       for i, w in enumerate(self.ring)}
        self.tables = {"store_sales": args.layout, "store_returns": args.returns_layout}
        self.sent = 0                              # bytes sent by the coordinator to the workers

    def fail(self, worker, why):
        """Give the partitions of a dead worker to the next worker that is alive."""
        parts = self.assign.pop(worker, [])
        if worker in self.alive:
            self.alive.remove(worker)
        if not self.alive:
            sys.exit(f"!! {name(worker)}: {why}. No worker left, giving up.")
        i = self.ring.index(worker)
        heir = next(w for w in self.ring[i + 1:] + self.ring[:i] if w in self.alive)
        self.assign.setdefault(heir, []).extend(parts)
        print(f"!! {name(worker)}: {why} -> its partitions {parts} go to {name(heir)}", flush=True)
        return heir, parts

    def fragment(self, worker, sql, parts, inputs):
        req = {"sql": sql, "net": self.net, "inputs": inputs,
               "tables": {t: {"layout": l, "parts": list(parts)} for t, l in self.tables.items()}}
        t0 = time.perf_counter()
        status, headers, data = request(worker, "POST", "/query", json.dumps(req).encode(), self.args.timeout)
        if status != 200:
            sys.exit(f"{name(worker)} could not run the fragment: {json.loads(data).get('error', data)}")
        return {"worker": worker, "parts": list(parts), "data": data, "rows": int(headers["X-Rows"]),
                "exec_s": float(headers["X-Exec-S"]), "wall_s": time.perf_counter() - t0}

    def scatter(self, sql, inputs=None):
        """Run one fragment per worker, in parallel; re-run the fragments of workers that fail."""
        results, todo = [], {w: list(parts) for w, parts in self.assign.items()}
        while todo:
            with ThreadPoolExecutor(len(todo)) as pool:
                futures = {w: pool.submit(self.fragment, w, sql, parts, inputs or {}) for w, parts in todo.items()}
            failed = {}
            for w, f in futures.items():
                try:
                    results.append(f.result())
                except WorkerDown as e:
                    failed[w] = str(e)
            todo = {}
            for w, why in failed.items():
                if w in self.assign:
                    heir, parts = self.fail(w, why)
                    todo.setdefault(heir, []).extend(parts)
        return results

    def broadcast(self, blobs):
        """Send every blob to every worker that is alive. Returns the blob ids."""
        ids = [uuid.uuid4().hex for _ in blobs]

        def upload(worker):
            for i, blob in zip(ids, blobs):
                time.sleep(self.net["delay_ms"] / 1000)
                request(worker, "PUT", f"/blob/{i}", blob, self.args.timeout, self.net["mbit"])
            return sum(len(b) for b in blobs)

        with ThreadPoolExecutor(len(self.alive)) as pool:
            futures = {w: pool.submit(upload, w) for w in self.alive}
        for w, f in futures.items():
            try:
                self.sent += f.result()
            except WorkerDown as e:
                self.fail(w, str(e))
        return ids


def read_all(table, layout):
    return f"read_parquet('{DATA}/{layout}/{table}/part=*/*.parquet', hive_partitioning=false)"


def reference(con, query, args):
    """The correct answer: the plain SQL over all the partitions, run here. Kept for later runs."""
    sql = PLANS[query]["reference"]
    path = f"{TMP}/reference_{query}_{hashlib.md5(sql.encode()).hexdigest()[:8]}.parquet"
    if not os.path.exists(path):
        print(f"computing the single-node reference answer of {query} (only the first time) ...", flush=True)
        sql = sql.replace("{store_sales}", read_all("store_sales", args.layout))
        sql = sql.replace("{store_returns}", read_all("store_returns", args.returns_layout))
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    return path


def same_result(con, a, b):
    """True if two Parquet files hold the same rows (numbers are compared to 4 decimal places)."""
    def cols(path):
        return con.execute(f"select column_name, column_type from (describe select * from '{path}')").fetchall()
    if [c for c, _ in cols(a)] != [c for c, _ in cols(b)]:
        return False
    sel = ", ".join(f"{c}::varchar as {c}" if t == "VARCHAR" else f"round({c}::double, 4) as {c}" for c, t in cols(a))
    diff = con.execute(f"select count(*) from ((select {sel} from '{a}' except all select {sel} from '{b}') union all "
                       f"(select {sel} from '{b}' except all select {sel} from '{a}'))").fetchone()[0]
    return diff == 0


def cmd_status(args, workers):
    for w in workers:
        t0 = time.perf_counter()
        try:
            _, _, data = request(w, "GET", "/health", timeout=2)
            h = json.loads(data)
            print(f"{name(w):<22} up    {1000 * (time.perf_counter() - t0):6.1f} ms   DuckDB {h['duckdb']}, "
                  f"threads={h['threads']}, memory_limit={h['memory_limit']}")
        except WorkerDown as e:
            print(f"{name(w):<22} DOWN  {e}")
    print("layouts in the data folder:")
    for path in sorted(p for p in os.listdir(DATA) if os.path.exists(f"{DATA}/{p}/_layout.json")):
        with open(f"{DATA}/{path}/_layout.json") as f:
            meta = json.load(f)
        for table, t in meta["tables"].items():
            print(f"  {path:<20} {table:<14} {meta['parts']} partitions, {sum(t['rows']):>11,} rows, "
                  f"max/mean {max(t['rows']) * meta['parts'] / sum(t['rows']):.2f}   {t['rule']}")


def cmd_run(args, workers):
    if args.query not in PLANS:
        sys.exit(f"unknown query {args.query}; choose from {', '.join(PLANS)}")
    query = PLANS[args.query]
    if args.plan not in query["plans"]:
        sys.exit(f"{args.query} has these plans: {', '.join(query['plans'])}")
    plan = query["plans"][args.plan]
    if not plan["merge"]:
        sys.exit(f"the merge step of {args.query}/{args.plan} is not written yet: edit plans.py")
    try:
        with open(f"{DATA}/{args.layout}/_layout.json") as f:
            meta = json.load(f)
    except FileNotFoundError:
        sys.exit(f"layout {args.layout} not found in {DATA}; create it with partition.py")
    if not 1 <= args.nodes <= len(workers):
        sys.exit(f"--nodes must be between 1 and {len(workers)}")
    rows_in = meta["tables"]["store_sales"]["rows"]
    os.makedirs(TMP, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=1")

    net = f"+{args.delay_ms:g} ms per request, " + (f"{args.mbit:g} Mbit/s" if args.mbit else "unlimited speed")
    print(f"{args.query}: {query['about']}")
    print(f"plan {args.plan}, layout {args.layout}, {args.nodes} node(s), simulated network: {net}", flush=True)

    totals = []
    for run_no in range(1, args.runs + 1):
        run = Run(args, workers, meta)
        t_start = time.perf_counter()
        bcast_line, inputs = "", {}
        if "broadcast" in plan:
            collected = run.scatter(plan["broadcast"]["sql"])
            blobs = [r["data"] for r in collected]
            inputs = {plan["broadcast"]["name"]: run.broadcast(blobs)}
            bcast_line = (f"broadcast step {time.perf_counter() - t_start:.2f} s: {sum(r['rows'] for r in collected):,} rows, "
                          f"{sum(map(len, blobs)) / 1e6:.1f} MB collected, {run.sent / 1e6:.1f} MB sent to the workers")
            received = sum(map(len, blobs))
        else:
            received = 0
        t_local = time.perf_counter()
        results = run.scatter(plan["local"], inputs)
        local_s = time.perf_counter() - t_local
        received += sum(len(r["data"]) for r in results)

        t_merge = time.perf_counter()
        files = []
        for i, r in enumerate(results):
            files.append(f"{TMP}/partial_{os.getpid()}_{i}.parquet")
            with open(files[-1], "wb") as f:
                f.write(r["data"])
        out = f"{TMP}/result_{os.getpid()}.parquet"
        con.execute(f"COPY ({plan['merge'].replace('{partials}', f'read_parquet({files!r})')}) TO '{out}' (FORMAT parquet)")
        merge_s = time.perf_counter() - t_merge
        total = time.perf_counter() - t_start
        totals.append(total)
        for f in files:
            os.remove(f)
        if args.runs > 1:
            print(f"run {run_no}: {total:.2f} s", flush=True)

    print(f"{'worker':<22} {'partitions':<12} {'rows read':>11} {'exec s':>7} {'total s':>8} {'rows sent':>10} {'KB sent':>9}")
    for r in sorted(results, key=lambda r: r["worker"]):
        print(f"{name(r['worker']):<22} {','.join(map(str, r['parts'])):<12} {sum(rows_in[p] for p in r['parts']):>11,} "
              f"{r['exec_s']:>7.2f} {r['wall_s']:>8.2f} {r['rows']:>10,} {len(r['data']) / 1e3:>9.1f}")
    if bcast_line:
        print(bcast_line)
    slowest = max(results, key=lambda r: r["wall_s"])
    print(f"local step {local_s:.2f} s (slowest: {name(slowest['worker'])})   merge {merge_s:.2f} s   TOTAL {total:.2f} s"
          + (f"   median of {args.runs} runs: {statistics.median(totals):.2f} s" if args.runs > 1 else ""))
    print(f"network: {received / 1e6:.2f} MB received from the workers, {run.sent / 1e6:.2f} MB sent to them")
    n = con.execute(f"select count(*) from '{out}'").fetchone()[0]
    ok = same_result(con, out, reference(con, args.query, args))
    print(f"result: {n} rows   compared with the single-node answer: {'SAME' if ok else 'DIFFERENT  <-- wrong result'}")
    if args.show:
        con.sql(f"select * from '{out}'").show(max_rows=args.show)
    if not ok and args.show:
        print("the single-node answer:")
        con.sql(f"select * from '{reference(con, args.query, args)}'").show(max_rows=args.show)
    os.remove(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", default=WORKERS, help="comma-separated host:port list (default w1:8100 ... w8:8100)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="which workers answer, and which layouts exist")
    run = sub.add_parser("run", help="run one query with one plan")
    run.add_argument("query", help="q1, q2 or q3")
    run.add_argument("--plan", required=True, help="one of the plans of the query in plans.py")
    run.add_argument("--layout", required=True, help="how store_sales is partitioned, e.g. by_ticket")
    run.add_argument("--returns-layout", default="by_ticket", help="layout of store_returns (default by_ticket)")
    run.add_argument("--nodes", type=int, default=4, help="use the first N workers (default 4)")
    run.add_argument("--runs", type=int, default=1)
    run.add_argument("--timeout", type=float, default=10, help="seconds to wait for a worker (default 10)")
    run.add_argument("--delay-ms", type=float, default=0, help="simulated network: extra delay per request")
    run.add_argument("--mbit", type=float, default=0, help="simulated network: speed limit in Mbit/s (0 = none)")
    run.add_argument("--show", type=int, default=0, metavar="N", help="print the first N rows of the result")
    args = ap.parse_args()
    workers = [w if ":" in w else w + ":8100" for w in args.workers.split(",") if w]
    if args.cmd == "status":
        cmd_status(args, workers)
    else:
        cmd_run(args, workers)


if __name__ == "__main__":
    main()
