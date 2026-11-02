"""The front-end of the object store: the only thing a client ever talks to.

Clients send plain HTTP:
  PUT    /<bucket>/<key>    store an object (the body)
  GET    /<bucket>/<key>    read it; with a Range header, only part of it (DuckDB reads Parquet this way)
  HEAD   /<bucket>/<key>    its size
  DELETE /<bucket>/<key>    remove it
  GET    /_list?prefix=p    the objects whose key starts with p
  GET    /_status           the shards and the objects
  GET    /_stats            counters since start: requests, bytes to clients, bytes read from each shard
  GET    /_log?n=20         the last requests
  POST   /_repair           rebuild the chunks that are missing on shards that are up

How an object is stored (k = 4 data shards, m = 2 parity shards, see ec.py):
  the object is cut into stripes of k chunks of `chunk` bytes (at most 1 MiB, all of the same size);
  data chunk i of every stripe goes to shard s(i+1), and the 2 parity chunks to s5 and s6.
  Shard i keeps, for each object, one file with its chunks one after the other: the chunk of
  stripe t starts at byte t * chunk of that file.

A read with no failures fetches the bytes from the data shards only and decodes nothing.
If a data shard does not answer, the same byte range is fetched from k other shards and the
missing bytes are rebuilt. A shard that does not answer a read within OBJ_TIMEOUT_S seconds
is not asked again for OBJ_SUSPECT_S seconds. Writes and repair, which move whole chunk files,
wait up to OBJ_BULK_TIMEOUT_S.
A write needs OBJ_WRITE_MIN shards to answer; the shards that missed it are recorded.
"""
import hashlib
import http.client
import json
import os
import re
import socket
import threading
import time
import urllib.parse
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np

import ec

K, M = 4, 2
SHARDS = os.environ.get("OBJ_SHARDS", "s1,s2,s3,s4,s5,s6").split(",")
CHUNK = int(os.environ.get("OBJ_CHUNK", str(1 << 20)))
WRITE_MIN = int(os.environ.get("OBJ_WRITE_MIN", "5"))
TIMEOUT = float(os.environ.get("OBJ_TIMEOUT_S", "2"))          # for the ranges a client is waiting for
BULK_TIMEOUT = float(os.environ.get("OBJ_BULK_TIMEOUT_S", "30"))  # for whole chunk files: writes and repair
SUSPECT_S = float(os.environ.get("OBJ_SUSPECT_S", "10"))
META = os.environ.get("OBJ_META", "/meta")
assert len(SHARDS) == K + M

os.makedirs(META, exist_ok=True)
INDEX = os.path.join(META, "index.json")
lock = threading.Lock()
index = json.load(open(INDEX)) if os.path.exists(INDEX) else {}   # key -> object metadata
down_until = {s: 0.0 for s in SHARDS}
recent = deque(maxlen=500)
stats = {"get": 0, "head": 0, "put": 0, "bytes_to_clients": 0, "bytes_from_clients": 0,
         "shard_bytes": {s: 0 for s in SHARDS}, "rebuilt_bytes": 0, "degraded_requests": 0,
         "shard_failures": {s: 0 for s in SHARDS}, "errors": 0}
pool = ThreadPoolExecutor(32)      # one task per shard range of a client request
helpers = ThreadPoolExecutor(32)   # the extra ranges a degraded read needs


class ShardDown(Exception):
    pass


def save_index():
    with open(INDEX + ".tmp", "w") as f:
        json.dump(index, f)
    os.replace(INDEX + ".tmp", INDEX)


def count(name, n=1, shard=None):
    with lock:
        if shard:
            stats[name][shard] += n
        else:
            stats[name] += n


def shard_call(s, method, path, body=None, rng=None, timeout=TIMEOUT):
    """One request to a shard. Returns (status, headers, body). Raises ShardDown."""
    if time.time() < down_until[s]:
        raise ShardDown(f"{s} is marked down")
    conn = http.client.HTTPConnection(s, 8200, timeout=timeout)
    headers = {"Range": f"bytes={rng[0]}-{rng[1] - 1}"} if rng else {}
    try:
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        if method == "GET" and resp.status in (200, 206):
            count("shard_bytes", len(data), s)
        return resp.status, dict(resp.getheaders()), data
    except (OSError, http.client.HTTPException) as e:
        with lock:
            down_until[s] = time.time() + SUSPECT_S
            stats["shard_failures"][s] += 1
        reason = "no answer" if isinstance(e, (socket.timeout, TimeoutError)) else "unreachable"
        raise ShardDown(f"{s}: {reason}")
    finally:
        conn.close()


def layout(size):
    """Chunk size and number of stripes of an object of `size` bytes. The chunks are at most
    CHUNK bytes, and as equal as possible, so that the last stripe is not mostly padding."""
    stripes = max(1, -(-size // (K * CHUNK)))
    return max(1, -(-size // (K * stripes))), stripes


# --- writes -----------------------------------------------------------------------------------

def put_object(key, body):
    size = len(body)
    chunk, stripes = layout(size)
    a = np.zeros(stripes * K * chunk, dtype=np.uint8)
    a[:size] = np.frombuffer(body, dtype=np.uint8)
    a = a.reshape(stripes, K, chunk)
    data = [np.ascontiguousarray(a[:, i, :]) for i in range(K)]
    chunks = data + ec.encode(data, K, M)
    cid = uuid.uuid4().hex

    def send(n):
        try:
            return shard_call(SHARDS[n], "PUT", f"/c/{cid}", chunks[n].tobytes(), timeout=BULK_TIMEOUT)[0] == 200
        except ShardDown:
            return False
    ok = list(pool.map(send, range(K + M)))
    written = [s for s, good in zip(SHARDS, ok) if good]
    if len(written) < WRITE_MIN:
        for s in written:
            pool.submit(shard_call, s, "DELETE", f"/c/{cid}")
        return None, f"only {len(written)} of {K + M} shards answered ({', '.join(written) or 'none'}); a write needs {WRITE_MIN}"
    meta = {"id": cid, "size": size, "chunk": chunk, "stripes": stripes,
            "missing": [s for s in SHARDS if s not in written],
            "md5": hashlib.md5(body).hexdigest(), "mtime": time.time()}
    with lock:
        old = index.get(key)
        index[key] = meta
        save_index()
    if old:
        for s in SHARDS:
            pool.submit(lambda s=s: shard_call(s, "DELETE", f"/c/{old['id']}"))
    return meta, None


# --- reads ------------------------------------------------------------------------------------

def mark_missing(key, meta, s):
    with lock:
        if s not in meta["missing"]:
            meta["missing"].append(s)
            save_index()


def rebuild_span(key, meta, i, a, b):
    """Bytes [a, b) of shard i's file, rebuilt from the same range of k other shards."""
    count("degraded_requests")
    candidates = [j for j in range(K + M) if j != i and SHARDS[j] not in meta["missing"]]
    have = {}
    while len(have) < K and candidates:
        batch, candidates = candidates[:K - len(have)], candidates[K - len(have):]

        def fetch(j):
            try:
                st, _, d = shard_call(SHARDS[j], "GET", f"/c/{meta['id']}", rng=(a, b))
            except ShardDown:
                return j, None
            if st == 404:
                mark_missing(key, meta, SHARDS[j])
            return j, (d if st in (200, 206) and len(d) == b - a else None)
        for j, d in helpers.map(fetch, batch):
            if d is not None:
                have[j] = np.frombuffer(d, dtype=np.uint8)
    if len(have) < K:
        raise ShardDown(f"only {len(have)} of the {K} chunks needed to rebuild the data of {SHARDS[i]} are available")
    count("rebuilt_bytes", b - a)
    return ec.decode(have, K, M, i).tobytes()


def read_span(key, meta, i, a, b):
    """Bytes [a, b) of data shard i's file for this object: from the shard, or rebuilt."""
    s = SHARDS[i]
    if s not in meta["missing"]:
        try:
            st, _, d = shard_call(s, "GET", f"/c/{meta['id']}", rng=(a, b))
            if st in (200, 206) and len(d) == b - a:
                return d, False
            if st == 404:
                mark_missing(key, meta, s)
        except ShardDown:
            pass
    return rebuild_span(key, meta, i, a, b), True


def read_object(key, meta, lo, hi):
    """Bytes [lo, hi) of the object. Returns (data, degraded)."""
    c = meta["chunk"]
    stripe = K * c
    spans = {}                     # data shard -> [first, end, pieces], in that shard's file
    x = lo
    while x < hi:
        t, r = divmod(x, stripe)
        i, o = divmod(r, c)
        n = min(c - o, hi - x)
        pos = t * c + o
        sp = spans.setdefault(i, [pos, pos, []])
        assert sp[1] == pos        # the pieces of one shard are contiguous in its file
        sp[1] = pos + n
        sp[2].append((pos, x - lo, n))
        x += n
    out = bytearray(hi - lo)
    futures = {i: pool.submit(read_span, key, meta, i, a, b) for i, (a, b, _) in spans.items()}
    degraded = False
    for i, f in futures.items():
        d, deg = f.result()
        degraded |= deg
        first = spans[i][0]
        for pos, off, n in spans[i][2]:
            out[off:off + n] = d[pos - first:pos - first + n]
    return bytes(out), degraded


# --- repair -----------------------------------------------------------------------------------

def repair():
    t0 = time.perf_counter()
    res = {"objects": len(index), "repaired": 0, "chunks_rebuilt": 0, "bytes_read": 0,
           "bytes_written": 0, "still_degraded": [], "lost": []}
    for key, meta in sorted(index.items()):
        size = meta["stripes"] * meta["chunk"]
        bad, unreachable = set(meta["missing"]), set()
        for s in SHARDS:
            if s in bad:
                continue
            try:
                st, h, _ = shard_call(s, "HEAD", f"/c/{meta['id']}")
                if st != 200 or int(h.get("Content-Length", -1)) != size:
                    bad.add(s)
            except ShardDown:
                unreachable.add(s)
        targets = [s for s in bad if s not in unreachable and time.time() >= down_until[s]]
        sources = [n for n, s in enumerate(SHARDS) if s not in bad and s not in unreachable][:K]
        if not bad:
            continue
        if len(sources) < K:
            res["lost" if len(SHARDS) - len(bad) < K else "still_degraded"].append(key)
            continue
        have = {}
        for n in sources:
            try:
                st, _, d = shard_call(SHARDS[n], "GET", f"/c/{meta['id']}", timeout=BULK_TIMEOUT)
            except ShardDown:
                break
            have[n] = np.frombuffer(d, dtype=np.uint8)
            res["bytes_read"] += len(d)
        if len(have) < K:
            res["still_degraded"].append(key)
            continue
        fixed = []
        for s in targets:
            n = SHARDS.index(s)
            data = ec.decode(have, K, M, n).tobytes()
            try:
                if shard_call(s, "PUT", f"/c/{meta['id']}", data, timeout=BULK_TIMEOUT)[0] == 200:
                    fixed.append(s)
                    res["bytes_written"] += len(data)
                    res["chunks_rebuilt"] += meta["stripes"]
            except ShardDown:
                pass
        with lock:
            meta["missing"] = sorted(bad - set(fixed))
        if fixed:
            res["repaired"] += 1
        if meta["missing"]:
            res["still_degraded"].append(key)
    with lock:
        save_index()
    res["seconds"] = round(time.perf_counter() - t0, 2)
    return res


def ping(s):
    try:
        conn = http.client.HTTPConnection(s, 8200, timeout=1)
        conn.request("GET", "/health")
        h = json.loads(conn.getresponse().read())
        down_until[s] = 0.0
        return {"name": s, "state": "UP", **{k: h[k] for k in ("files", "bytes", "bytes_served")}}
    except (OSError, http.client.HTTPException, ValueError) as e:
        down_until[s] = time.time() + SUSPECT_S
        state = "NO ANSWER" if isinstance(e, (socket.timeout, TimeoutError)) else "DOWN"
        return {"name": s, "state": state}


# --- HTTP -------------------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, code, body=b"", headers=None, length=None):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body) if length is None else length))
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def json(self, obj, code=200):
        self.reply(code, json.dumps(obj).encode(), {"Content-Type": "application/json"})

    def key(self):
        return urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.lstrip("/"))

    def note(self, rng, n, degraded, t0):
        recent.append({"t": round(time.time(), 3), "op": self.command, "key": self.key(), "range": rng,
                       "bytes": n, "degraded": degraded, "ms": round((time.perf_counter() - t0) * 1000, 1)})

    def do_GET(self):
        t0 = time.perf_counter()
        url = urllib.parse.urlsplit(self.path)
        q = urllib.parse.parse_qs(url.query)
        if url.path == "/_list":
            p = q.get("prefix", [""])[0]
            return self.json([{"key": k, "size": m["size"], "missing": m["missing"]}
                              for k, m in sorted(index.items()) if k.startswith(p)])
        if url.path == "/_status":
            shards = list(pool.map(ping, SHARDS))
            return self.json({"k": K, "m": M, "chunk": CHUNK, "write_min": WRITE_MIN, "timeout_s": TIMEOUT,
                              "shards": shards, "objects": len(index),
                              "logical_bytes": sum(m["size"] for m in index.values()),
                              "degraded_objects": sum(1 for m in index.values() if m["missing"])})
        if url.path == "/_stats":
            with lock:
                return self.json(stats)
        if url.path == "/_log":
            n = int(q.get("n", ["20"])[0])
            return self.json(list(recent)[-n:])
        meta = index.get(self.key())
        if not meta:
            return self.json({"error": f"no object {self.key()}"}, 404)
        size = meta["size"]
        headers = {"Accept-Ranges": "bytes", "ETag": f'"{meta["md5"]}"',
                   "Last-Modified": formatdate(meta["mtime"], usegmt=True)}
        if self.command == "HEAD":
            count("head")
            self.note(None, 0, False, t0)
            return self.reply(200, headers=headers, length=size)
        count("get")
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))
        lo, hi = 0, size
        if m and m[1]:
            lo, hi = int(m[1]), min(int(m[2]) + 1 if m[2] else size, size)
        elif m and m[2]:
            lo = max(0, size - int(m[2]))
        if lo >= hi and size:
            return self.reply(416, headers={"Content-Range": f"bytes */{size}"})
        try:
            data, degraded = read_object(self.key(), meta, lo, hi)
        except ShardDown as e:
            count("errors")
            self.note(f"{lo}-{hi - 1}", 0, True, t0)
            return self.json({"error": str(e)}, 503)
        count("bytes_to_clients", len(data))
        self.note(f"{lo}-{hi - 1}", len(data), degraded, t0)
        if m:
            headers["Content-Range"] = f"bytes {lo}-{hi - 1}/{size}"
        self.reply(206 if m else 200, data, headers)

    do_HEAD = do_GET

    def do_PUT(self):
        t0 = time.perf_counter()
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        count("put")
        count("bytes_from_clients", len(body))
        meta, err = put_object(self.key(), body)
        self.note(None, len(body), bool(meta and meta["missing"]), t0)
        if err:
            return self.json({"error": err}, 503)
        self.json({"key": self.key(), "size": meta["size"], "missing": meta["missing"]})

    def do_DELETE(self):
        with lock:
            meta = index.pop(self.key(), None)
            save_index()
        if meta:
            for s in SHARDS:
                pool.submit(lambda s=s: shard_call(s, "DELETE", f"/c/{meta['id']}"))
        self.json({"deleted": bool(meta)})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if urllib.parse.urlsplit(self.path).path == "/_repair":
            return self.json(repair())
        self.json({"error": "unknown"}, 404)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8200"))
    print(f"object store front-end: {K}+{M} over {','.join(SHARDS)}, chunk {CHUNK} B, "
          f"writes need {WRITE_MIN} shards, shard timeout {TIMEOUT:g} s, {len(index)} objects", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
