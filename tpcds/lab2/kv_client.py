"""Client of the key-value store of Part C. The quorum logic is here, not in the replicas.

A write is sent to all N replicas and succeeds as soon as W of them confirm.
A read is sent to all N replicas (in a random order), waits for the first R answers and
returns the one with the highest version.

Examples (inside the client container, see the handout for the `kv` shortcut):
  python kv_client.py status
  python kv_client.py put colour red --w 2
  python kv_client.py get colour --r 2
  python kv_client.py lag kv3 200            # kv3 now applies every write 200 ms late
  python kv_client.py bench --w 1 --r 1      # 200 writes, each followed at once by a read
"""
import argparse
import http.client
import json
import os
import queue
import random
import socket
import statistics
import sys
import threading
import time

REPLICAS = os.environ.get("LAB2_REPLICAS", "kv1:8200,kv2:8200,kv3:8200")


def call(addr, method, path, obj, timeout):
    host, _, port = addr.partition(":")
    if host == socket.gethostname():   # a replica cut off from the network can still reach itself
        host = "127.0.0.1"
    conn = http.client.HTTPConnection(host, int(port or 8200), timeout=timeout)
    try:
        conn.request(method, path, body=json.dumps(obj).encode() if obj is not None else None)
        return json.loads(conn.getresponse().read())
    finally:
        conn.close()


def why(e):
    if "timed out" in str(e):
        return "no answer in time"
    if isinstance(e, socket.gaierror):
        return "host not found (is the container running?)"
    return type(e).__name__


def ask_all(replicas, method, path, obj, need, timeout):
    """Send the request to every replica in parallel; return when `need` have answered, when all
    have answered or failed, or after `timeout` seconds. Returns (answers, errors, threads)."""
    box = queue.Queue()

    def one(addr):
        try:
            box.put((addr, call(addr, method, path, obj, timeout), None))
        except (OSError, http.client.HTTPException, ValueError) as e:
            box.put((addr, None, why(e)))

    threads = [threading.Thread(target=one, args=(r,), daemon=True) for r in replicas]
    for t in threads:
        t.start()
    answers, errors, deadline = {}, {}, time.monotonic() + timeout
    while len(answers) < need and len(answers) + len(errors) < len(replicas):
        try:
            addr, answer, error = box.get(timeout=max(0.001, deadline - time.monotonic()))
        except queue.Empty:
            break
        if error:
            errors[addr] = error
        else:
            answers[addr] = answer
    return answers, errors, threads


def put(replicas, key, version, value, w, timeout):
    answers, errors, threads = ask_all(replicas, "PUT", f"/kv/{key}", {"version": version, "value": value}, w, timeout)
    return len(answers) >= w, answers, errors, threads


def get(replicas, key, r, timeout):
    order = random.sample(replicas, len(replicas))   # no replica is always asked first
    answers, errors, _ = ask_all(order, "GET", f"/kv/{key}", None, r, timeout)
    best = max(answers.values(), key=lambda a: a["version"]) if len(answers) >= r else None
    return best, answers, errors


def short(addr):
    return addr.split(":")[0]


def cmd_status(args, replicas):
    for r in replicas:
        try:
            h = call(r, "GET", "/health", None, args.timeout)
            print(f"{short(r):<12} up    {h['keys']} key(s), lag {h['lag_ms']} ms")
        except (OSError, http.client.HTTPException) as e:
            print(f"{short(r):<12} DOWN  {why(e)}")


def cmd_lag(args, replicas):
    addr = next((r for r in replicas if short(r) == args.replica), None)
    if not addr:
        sys.exit(f"unknown replica {args.replica}")
    call(addr, "POST", "/config", {"lag_ms": args.ms}, args.timeout)
    print(f"{args.replica}: every write is now applied {args.ms} ms late")


def cmd_put(args, replicas):
    # new version = highest version that any reachable replica holds, plus one
    _, seen, _ = get(replicas, args.key, len(replicas), args.timeout)
    version = max([a["version"] for a in seen.values()] or [0]) + 1
    t0 = time.perf_counter()
    ok, answers, errors, threads = put(replicas, args.key, version, args.value, args.w, args.timeout)
    ms = 1000 * (time.perf_counter() - t0)
    print(f"put {args.key} = {args.value!r} as version {version}, W={args.w}: "
          f"{'OK' if ok else 'FAILED'} after {ms:.0f} ms, confirmed by {sorted(map(short, answers)) or 'nobody'}")
    for r, e in errors.items():
        print(f"   {short(r)}: {e}")
    if not ok:
        print("   a failed write is not undone: the replicas that confirmed keep it")
    for t in threads:  # let the slower replicas receive the write before this program ends
        t.join(args.timeout)
    sys.exit(0 if ok else 1)


def cmd_get(args, replicas):
    t0 = time.perf_counter()
    best, answers, errors = get(replicas, args.key, args.r, args.timeout)
    ms = 1000 * (time.perf_counter() - t0)
    for r in replicas:
        if r in answers:
            print(f"   {short(r)}: version {answers[r]['version']}, value {answers[r]['value']!r}")
        elif r in errors:
            print(f"   {short(r)}: {errors[r]}")
        else:
            print(f"   {short(r)}: (not waited for)")
    if best is None:
        print(f"get {args.key}, R={args.r}: FAILED after {ms:.0f} ms, only {len(answers)} answer(s)")
        sys.exit(1)
    print(f"get {args.key}, R={args.r}: version {best['version']}, value {best['value']!r} ({ms:.0f} ms)")
    if len({a["value"] for a in answers.values() if a["version"] == best["version"]}) > 1:
        print("   CONFLICT: replicas hold different values with the same version")
    elif len({a["version"] for a in answers.values()}) > 1:
        print("   the replicas that answered do not agree: at least one is behind")


def percentiles(ms):
    if not ms:
        return "-"
    ms = sorted(ms)
    return f"p50 {statistics.median(ms):7.1f} ms   p99 {ms[min(len(ms) - 1, int(0.99 * len(ms)))]:7.1f} ms"


def cmd_bench(args, replicas):
    n = len(replicas)
    _, seen, _ = get(replicas, args.key, n, args.timeout)
    version = max([a["version"] for a in seen.values()] or [0])
    acked = version                      # highest version whose write was confirmed to us
    w_ms, r_ms, fail_ms, r_fail, stale = [], [], [], 0, 0
    for _ in range(args.ops):
        version += 1
        t0 = time.perf_counter()
        ok, _, _, _ = put(replicas, args.key, version, f"v{version}", args.w, args.timeout)
        if ok:
            w_ms.append(1000 * (time.perf_counter() - t0))
            acked = version
        else:
            fail_ms.append(1000 * (time.perf_counter() - t0))
        t0 = time.perf_counter()
        best, _, _ = get(replicas, args.key, args.r, args.timeout)
        if best is None:
            r_fail += 1
            continue
        r_ms.append(1000 * (time.perf_counter() - t0))
        if best["version"] < acked:      # we were told the write succeeded, yet we read an older value
            stale += 1
    print(f"N={n}, W={args.w}, R={args.r}: {args.ops} writes, each followed at once by a read   "
          f"W+R>N: {'yes' if args.w + args.r > n else 'no'}")
    print(f"writes: {len(w_ms):>4} ok, {len(fail_ms):>4} failed   {percentiles(w_ms)}"
          + (f"   (a failed write gave up after {statistics.median(fail_ms):.0f} ms)" if fail_ms else ""))
    print(f"reads : {len(r_ms):>4} ok, {r_fail:>4} failed   {percentiles(r_ms)}")
    print(f"stale reads: {stale} of {len(r_ms)}" + (f" ({stale / len(r_ms):.0%})" if r_ms else ""))
    time.sleep(args.timeout)             # let the slower replicas catch up before the next experiment


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replicas", default=REPLICAS, help=f"comma-separated host:port list (default {REPLICAS})")
    ap.add_argument("--timeout", type=float, default=1.0, help="seconds to wait for a replica (default 1)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    p = sub.add_parser("lag", help="make one replica apply its writes late")
    p.add_argument("replica")
    p.add_argument("ms", type=int)
    p = sub.add_parser("put")
    p.add_argument("key")
    p.add_argument("value")
    p.add_argument("--w", type=int, default=2)
    p = sub.add_parser("get")
    p.add_argument("key")
    p.add_argument("--r", type=int, default=2)
    p = sub.add_parser("bench", help="alternate writes and reads; count failures and stale reads")
    p.add_argument("--w", type=int, default=2)
    p.add_argument("--r", type=int, default=2)
    p.add_argument("--ops", type=int, default=200)
    p.add_argument("--key", default="bench")
    args = ap.parse_args()
    replicas = [r if ":" in r else r + ":8200" for r in args.replicas.split(",") if r]
    {"status": cmd_status, "lag": cmd_lag, "put": cmd_put, "get": cmd_get, "bench": cmd_bench}[args.cmd](args, replicas)


if __name__ == "__main__":
    main()
