"""Split a dsqgen query stream into one file per TPC-DS query (query01.sql ... query99.sql).

Files are named after the template number, not the position in the stream.
Queries 14, 23, 24 and 39 contain two statements each; both stay in the same file.

Usage: python split_queries.py <query_0.sql> <output folder>
"""
import os
import re
import sys

START = re.compile(r"-- start query \d+ in stream \d+ using template query(\d+)\.tpl")


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python split_queries.py <query_0.sql> <output folder>")
    src, dst = sys.argv[1], sys.argv[2]
    os.makedirs(dst, exist_ok=True)
    out, count = None, 0
    with open(src) as f:
        for line in f:
            m = START.match(line)
            if m:
                out = open(os.path.join(dst, f"query{int(m.group(1)):02d}.sql"), "w")
                count += 1
            if out:
                out.write(line)
            if out and line.startswith("-- end query"):
                out.close()
                out = None
    print(f"{count} queries written to {dst}")
    if count != 99:
        sys.exit(f"expected 99 queries, found {count}")


if __name__ == "__main__":
    main()
