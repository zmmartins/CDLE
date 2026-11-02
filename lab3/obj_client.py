"""Command-line client of the lab's object store (run it in the client container; see the `obj` shortcut).

  python obj_client.py status                 the shards, the objects, the settings
  python obj_client.py ls [prefix]            objects and their sizes
  python obj_client.py du                     bytes stored on each shard, compared with 3 replicas
  python obj_client.py put <file|folder> <prefix>
  python obj_client.py get <key>              read one object whole and check its MD5
  python obj_client.py verify [prefix]        read every object whole and check its MD5
  python obj_client.py stats                  counters since the front-end started
  python obj_client.py log [-n 20]            the last requests the front-end served
  python obj_client.py repair                 rebuild missing chunks on the shards that are up
  python obj_client.py wipe <shard>           empty a shard's disk (as if the disk were replaced)

Keys look like lake/sf10/store_sales/data_0.parquet; DuckDB reads them as
http://obj:8200/lake/sf10/store_sales/data_0.parquet.
"""
import argparse
import hashlib
import http.client
import json
import os
import sys
import time
import urllib.parse

OBJ = os.environ.get("OBJ_URL", "obj:8200")
BUCKET = "lake"


def call(method, path, body=None, host=OBJ, timeout=120):
    h, _, p = host.partition(":")
    for attempt in range(20):          # just after `dc up`, the front-end may still be starting
        conn = http.client.HTTPConnection(h, int(p or 8200), timeout=timeout)
        try:
            conn.request(method, urllib.parse.quote(path, safe="/?=&_"), body=body)
            resp = conn.getresponse()
            return resp.status, resp.read()
        except ConnectionRefusedError as e:
            err = e
            time.sleep(0.5)
        except OSError as e:
            err = e
            break
        finally:
            conn.close()
    sys.exit(f"cannot reach {host}: {err}. Is the object store running? (dc up -d, then obj status)")


def get_json(path):
    st, body = call("GET", path)
    return json.loads(body)


def stats():
    return get_json("/_stats")


def keys(prefix):
    """The keys under a prefix; the bucket name may be left out."""
    if not prefix.startswith(BUCKET + "/"):
        prefix = f"{BUCKET}/{prefix}"
    return [o["key"] for o in get_json(f"/_list?prefix={prefix}")]


def urls(prefix):
    return [f"http://{OBJ}/{k}" for k in keys(prefix)]


def mb(n):
    return f"{n / 1e6:,.1f}"


def cmd_status(a):
    s = get_json("/_status")
    print(f"code {s['k']}+{s['m']}: {s['k']} data shards (s1-s{s['k']}) and {s['m']} parity shards, "
          f"chunk {s['chunk'] >> 10} KiB; a write needs {s['write_min']} shards; shard timeout {s['timeout_s']:g} s")
    print(f"{'shard':6} {'state':10} {'files':>6} {'MB stored':>10} {'MB served':>10}")
    for x in s["shards"]:
        if x["state"] == "UP":
            print(f"{x['name']:6} {x['state']:10} {x['files']:>6} {mb(x['bytes']):>10} {mb(x['bytes_served']):>10}")
        else:
            print(f"{x['name']:6} {x['state']:10}")
    print(f"objects: {s['objects']}  ({mb(s['logical_bytes'])} MB)   objects with missing chunks: {s['degraded_objects']}")
    groups = {}
    for o in get_json("/_list?prefix="):
        g = o["key"].rsplit("/", 1)[0]
        n, size = groups.get(g, (0, 0))
        groups[g] = (n + 1, size + o["size"])
    for g, (n, size) in sorted(groups.items()):
        print(f"  {g + '/':40} {n:>4} objects {mb(size):>10} MB")


def cmd_ls(a):
    total = 0
    for o in get_json(f"/_list?prefix={a.prefix}"):
        miss = f"   missing on {','.join(o['missing'])}" if o["missing"] else ""
        print(f"{mb(o['size']):>10} MB  {o['key']}{miss}")
        total += o["size"]
    print(f"{mb(total):>10} MB  total")


def cmd_du(a):
    s = get_json("/_status")
    logical = s["logical_bytes"]
    stored = 0
    print(f"{'shard':6} {'role':7} {'MB':>10}")
    for n, x in enumerate(s["shards"]):
        role = "data" if n < s["k"] else "parity"
        if x["state"] == "UP":
            stored += x["bytes"]
            print(f"{x['name']:6} {role:7} {mb(x['bytes']):>10}")
        else:
            print(f"{x['name']:6} {role:7} {'(' + x['state'] + ')':>10}")
    print(f"logical data:        {mb(logical):>10} MB")
    print(f"stored on the shards:{mb(stored):>10} MB   = {stored / max(logical, 1):.3f} x the data")
    print(f"with 3 replicas:     {mb(3 * logical):>10} MB   = 3.000 x the data")


def put_file(path, key):
    with open(path, "rb") as f:
        body = f.read()
    st, resp = call("PUT", "/" + key, body)
    r = json.loads(resp)
    if st != 200:
        sys.exit(f"PUT {key} FAILED: {r['error']}")
    return len(body), r["missing"]


def cmd_put(a):
    if os.path.isdir(a.path):
        files = sorted(f for f in os.listdir(a.path) if not f.startswith("."))
        pairs = [(os.path.join(a.path, f), f) for f in files]
    else:
        pairs = [(a.path, os.path.basename(a.path))]
    prefix = a.prefix.strip("/")
    if not prefix.startswith(BUCKET + "/") and prefix != BUCKET:
        prefix = f"{BUCKET}/{prefix}"
    t0 = time.perf_counter()
    total, degraded = 0, {}
    for path, name in pairs:
        n, missing = put_file(path, f"{prefix}/{name}")
        total += n
        for s in missing:
            degraded[s] = degraded.get(s, 0) + 1
    dt = time.perf_counter() - t0
    print(f"PUT {len(pairs)} objects under {prefix}/, {mb(total)} MB in {dt:.1f} s ({total / 1e6 / dt:.0f} MB/s)")
    for s, n in degraded.items():
        print(f"  {n} objects written without their chunks on {s}")


def read_whole(key):
    st, body = call("GET", "/" + key)
    if st != 200:
        return None, json.loads(body).get("error")
    return body, None


def cmd_get(a):
    key = a.key if a.key.startswith(BUCKET + "/") else f"{BUCKET}/{a.key}"
    before = stats()
    t0 = time.perf_counter()
    body, err = read_whole(key)
    dt = time.perf_counter() - t0
    if body is None:
        sys.exit(f"GET {key} FAILED: {err}")
    after = stats()
    print(f"GET {key}: {mb(len(body))} MB in {dt:.2f} s, MD5 {'OK' if check_md5(key, body) else 'MISMATCH'}")
    print_delta(before, after)


def check_md5(key, body):
    h, _, p = OBJ.partition(":")
    conn = http.client.HTTPConnection(h, int(p), timeout=30)
    conn.request("HEAD", urllib.parse.quote("/" + key))
    etag = conn.getresponse().getheader("ETag", "").strip('"')
    conn.close()
    return etag == hashlib.md5(body).hexdigest()


def delta(before, after):
    d = {k: after[k] - before[k] for k in ("get", "head", "bytes_to_clients", "rebuilt_bytes", "degraded_requests", "errors")}
    d["shard_bytes"] = {s: after["shard_bytes"][s] - before["shard_bytes"][s] for s in after["shard_bytes"]}
    d["shard_failures"] = {s: after["shard_failures"][s] - before["shard_failures"][s] for s in after["shard_failures"]}
    return d


def print_delta(before, after):
    d = delta(before, after)
    sb = d["shard_bytes"]
    print(f"  front-end: {d['get']} GETs, {d['head']} HEADs, {mb(d['bytes_to_clients'])} MB sent to the client, "
          f"{mb(sum(sb.values()))} MB read from shards")
    print("  read from: " + "  ".join(f"{s} {mb(n)}" for s, n in sb.items()) + "  (MB)")
    if d["rebuilt_bytes"] or d["errors"] or any(d["shard_failures"].values()):
        fails = ", ".join(f"{s} x{n}" for s, n in d["shard_failures"].items() if n) or "none"
        print(f"  degraded: {d['degraded_requests']} ranges rebuilt, {mb(d['rebuilt_bytes'])} MB rebuilt; "
              f"failed shard requests: {fails}; errors: {d['errors']}")


def cmd_verify(a):
    ks = keys(a.prefix)
    before = stats()
    t0 = time.perf_counter()
    bad = []
    for k in ks:
        body, err = read_whole(k)
        if body is None or not check_md5(k, body):
            bad.append((k, err or "MD5 MISMATCH"))
    dt = time.perf_counter() - t0
    after = stats()
    print(f"verified {len(ks)} objects in {dt:.1f} s: {len(ks) - len(bad)} OK, {len(bad)} FAILED")
    for k, e in bad[:10]:
        print(f"  {k}: {e}")
    print_delta(before, after)


def cmd_stats(a):
    s = stats()
    print(json.dumps(s, indent=2))


def cmd_log(a):
    for r in get_json(f"/_log?n={a.n}"):
        rng = r["range"] or "-"
        flag = "  DEGRADED" if r["degraded"] else ""
        print(f"{r['op']:5} {r['key']:45} {rng:>22} {r['bytes']:>11,} B {r['ms']:>8} ms{flag}")


def cmd_repair(a):
    st, body = call("POST", "/_repair", b"", timeout=3600)
    r = json.loads(body)
    print(f"repair: {r['repaired']} of {r['objects']} objects repaired, {r['chunks_rebuilt']} chunks rebuilt, "
          f"in {r['seconds']} s")
    print(f"  read {mb(r['bytes_read'])} MB from healthy shards, wrote {mb(r['bytes_written'])} MB"
          + (f"  ({r['bytes_read'] / r['bytes_written']:.1f} MB read per MB written)" if r["bytes_written"] else ""))
    if r["still_degraded"]:
        print(f"  still degraded (a shard is down): {len(r['still_degraded'])} objects")
    if r["lost"]:
        print(f"  LOST (fewer than k shards left): {len(r['lost'])} objects")


def cmd_wipe(a):
    st, _ = call("POST", "/wipe", b"", host=f"{a.shard}:8200")
    print(f"{a.shard}: disk wiped" if st == 200 else f"{a.shard}: wipe failed ({st})")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    p = sub.add_parser("ls")
    p.add_argument("prefix", nargs="?", default="")
    sub.add_parser("du")
    p = sub.add_parser("put")
    p.add_argument("path")
    p.add_argument("prefix")
    p = sub.add_parser("get")
    p.add_argument("key")
    p = sub.add_parser("verify")
    p.add_argument("prefix", nargs="?", default="")
    sub.add_parser("stats")
    p = sub.add_parser("log")
    p.add_argument("-n", type=int, default=20)
    sub.add_parser("repair")
    p = sub.add_parser("wipe")
    p.add_argument("shard")
    a = ap.parse_args()
    globals()["cmd_" + a.cmd](a)


if __name__ == "__main__":
    main()
