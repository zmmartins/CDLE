"""One replica of a tiny key-value store (Part C). Each key holds a (version, value) pair, in memory.

  PUT  /kv/<key>   JSON {version, value}: stored if the version is higher than the one held
  GET  /kv/<key>   JSON {node, version, value}; version 0 means "never written"
  POST /config     JSON {lag_ms}: from now on, wait this long before applying each write
  GET  /health

The replica knows nothing about quorums: kv_client.py decides how many answers are enough.
"""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NODE = os.environ.get("NODE", socket.gethostname())
store = {}
config = {"lag_ms": 0}
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):
        pass

    def reply(self, obj, code=200):
        body = json.dumps(dict(obj, node=NODE)).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")

    def do_GET(self):
        if self.path.startswith("/kv/"):
            with lock:
                version, value = store.get(self.path[4:], (0, None))
            self.reply({"version": version, "value": value})
        elif self.path == "/health":
            self.reply({"keys": len(store), **config})
        else:
            self.reply({}, 404)

    def do_PUT(self):
        if not self.path.startswith("/kv/"):
            return self.reply({}, 404)
        new = self.body()
        time.sleep(config["lag_ms"] / 1000)      # a slow replica: the write is applied late
        with lock:
            if new["version"] > store.get(self.path[4:], (0, None))[0]:
                store[self.path[4:]] = (new["version"], new["value"])
            version = store[self.path[4:]][0]
        self.reply({"version": version})

    def do_POST(self):
        if self.path != "/config":
            return self.reply({}, 404)
        config.update(self.body())
        self.reply(config)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8200"))
    print(f"replica {NODE}: port {port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
