"""Parse one line of the access log (Apache "combined" format) in plain Python. Used by the jobs of Part A.

  10.4.17.203 - - [14/Mar/2002:10:22:01 +0000] "GET /product/AAAAAAAAOKJDAAAA?qty=57 HTTP/1.1" 200 1019 "/search?q=electoral+ports" "Mozilla/5.0 (...)"
"""
import re

LINE = re.compile(r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$')


def parse(line):
    """A dict with the fields of a log line, or None if the line is not complete."""
    m = LINE.match(line.rstrip("\n"))
    if not m:
        return None
    ip, ts, method, path, status, size, referer, agent = m.groups()
    return {"ip": ip, "time": ts, "method": method, "path": path, "status": int(status),
            "bytes": 0 if size == "-" else int(size), "referer": referer, "agent": agent}


def search_words(path):
    """The words of a search request: '/search?q=red+wool+socks' -> ['red', 'wool', 'socks']; [] for other paths."""
    if not path.startswith("/search?q="):
        return []
    return [w for w in path[len("/search?q="):].split("+") if w]


def product_id(path):
    """'/product/AAAAAAAAOKJDAAAA?qty=57' -> 'AAAAAAAAOKJDAAAA' (as written in the line); None for other paths."""
    if not path.startswith("/product/"):
        return None
    return path[len("/product/"):].split("?")[0]
