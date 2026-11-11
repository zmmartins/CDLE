"""A storage node of the object store: keeps chunks on its own disk and serves byte ranges.

It knows nothing about objects, stripes or parity. The front-end sends it one file per object
(all the chunks this node holds for that object, one after the other) and later asks for byte
ranges of that file.

  PUT    /c/<id>     store the chunks of one object
  GET    /c/<id>     read them (with a Range header: only part of them)
  HEAD   /c/<id>     size, or 404
  DELETE /c/<id>     remove them
  POST   /wipe       remove everything: the disk was replaced by an empty one
  GET    /health     name, number of files and bytes held, bytes served
"""
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STORE = os.environ.get("OBJ_STORE", "/store")
NODE = os.environ.get("NODE", socket.gethostname())
os.makedirs(STORE, exist_ok=True)

lock = threading.Lock()
served = {"gets": 0, "bytes": 0}


def path_of(cid):
    if not re.fullmatch(r"[0-9a-f]{32}", cid):
        raise ValueError(cid)
    return os.path.join(STORE, cid)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, code, body=b"", headers=None):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Node", NODE)
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def target(self):
        if not self.path.startswith("/c/"):
            return None
        try:
            return path_of(self.path[3:])
        except ValueError:
            return None

    def do_GET(self):
        if self.path == "/health":
            files = os.listdir(STORE)
            size = sum(os.path.getsize(os.path.join(STORE, f)) for f in files)
            with lock:
                body = f'{{"node": "{NODE}", "files": {len(files)}, "bytes": {size}, ' \
                       f'"gets": {served["gets"]}, "bytes_served": {served["bytes"]}}}'
            return self.reply(200, body.encode())
        p = self.target()
        if not p or not os.path.exists(p):
            return self.reply(404)
        size = os.path.getsize(p)
        rng = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
        lo, hi = (int(rng[1]), min(int(rng[2]) + 1, size)) if rng else (0, size)
        with open(p, "rb") as f:
            f.seek(lo)
            data = f.read(hi - lo)
        with lock:
            served["gets"] += 1
            served["bytes"] += len(data)
        self.reply(206 if rng else 200, data)

    def do_HEAD(self):
        p = self.target()
        if not p or not os.path.exists(p):
            return self.reply(404)
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(p)))
        self.end_headers()

    def do_PUT(self):
        p = self.target()
        data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if not p:
            return self.reply(400)
        with open(p + ".tmp", "wb") as f:
            f.write(data)
        os.replace(p + ".tmp", p)
        self.reply(200)

    def do_DELETE(self):
        p = self.target()
        if p and os.path.exists(p):
            os.remove(p)
        self.reply(200)

    def do_POST(self):
        if self.path != "/wipe":
            return self.reply(404)
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        for f in os.listdir(STORE):
            os.remove(os.path.join(STORE, f))
        self.reply(200)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8200"))
    print(f"shard {NODE}: storing in {STORE}, port {port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
