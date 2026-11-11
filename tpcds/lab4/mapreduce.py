"""A MapReduce framework in about 150 lines, for Part A: the model of the MapReduce paper (§3), on one machine.

  1. split    the input files are cut into M splits of about the same size (a split never cuts a line)
  2. map      M map tasks; each calls map(line) for every line of its split, and writes every (key, value)
              to one of R intermediate files, chosen by hash(key) mod R          <- this is the shuffle
  3. combine  (optional) each map task first runs the reduce on its own pairs, key by key
  4. reduce   R reduce tasks; reduce task r reads file r of every map task, sorts the pairs by key,
              and calls reduce(key, values) once per key

The tasks run in a pool of P processes ("slots"): with M = 8 and P = 2, the maps run in 4 waves.
Usage: python mapreduce.py words --sf 1 --maps 8 --reduces 4 [--combiner]
"""
import argparse
import glob
import os
import shutil
import time
import zlib
from itertools import groupby
from multiprocessing import Pool

from common import DATA, evidence, logs, out
from jobs import JOBS


def make_splits(files, m):
    """Cut the files, seen one after the other, into m pieces of about the same number of bytes.
    A split is a list of (file, start, end): it may span the end of one file and the start of the next."""
    sizes = [os.path.getsize(f) for f in files]
    total = sum(sizes)
    bounds = [total * i // m for i in range(m + 1)]
    splits, offset = [[] for _ in range(m)], 0
    for f, size in zip(files, sizes):
        for i in range(m):
            lo, hi = max(bounds[i], offset), min(bounds[i + 1], offset + size)
            if lo < hi:
                splits[i].append((f, lo - offset, hi - offset))
        offset += size
    return splits


def read_lines(f, start, end):
    """The lines that start inside [start, end): the line that crosses 'end' belongs to this split,
    and a split that starts in the middle of a line skips it (the previous split reads it), as in Hadoop."""
    with open(f, "rb") as fh:
        if start > 0:
            fh.seek(start - 1)
            fh.readline()                       # skip to the first line that starts at or after 'start'
        while fh.tell() < end:
            line = fh.readline()
            if not line:
                break
            yield line.decode("utf-8", errors="replace")


def partition(key, r):
    return zlib.crc32(key.encode()) % r        # the same in every process (Python's hash() is not)


def map_task(args):
    job, m, split, r, combiner, work = args
    mapper, reducer, _ = JOBS[job]
    t0 = time.time()
    lines = emitted = 0
    if combiner:
        groups = {}
        for f, s, e in split:
            for line in read_lines(f, s, e):
                lines += 1
                for k, v in mapper(line):
                    emitted += 1
                    groups.setdefault(k, []).append(v)
        pairs = (kv for k in groups for kv in reducer(k, groups[k]))
    else:
        def gen():
            nonlocal lines, emitted
            for f, s, e in split:
                for line in read_lines(f, s, e):
                    lines += 1
                    for kv in mapper(line):
                        emitted += 1
                        yield kv
        pairs = gen()
    outs = [open(f"{work}/map-{m:02d}-part-{p}.tsv", "w", encoding="utf-8") for p in range(r)]
    written = 0
    for k, v in pairs:
        outs[partition(k, r)].write(f"{k}\t{v}\n")
        written += 1
    for o in outs:
        o.close()
    return {"lines": lines, "emitted": emitted, "written": written, "s": time.time() - t0}


def reduce_task(args):
    job, p, work, dest = args
    _, reducer, _ = JOBS[job]
    t0 = time.time()
    pairs = []
    for f in sorted(glob.glob(f"{work}/map-*-part-{p}.tsv")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                k, v = line.rstrip("\n").split("\t")
                pairs.append((k, int(v)))
    pairs.sort(key=lambda kv: kv[0])
    keys = 0
    with open(f"{dest}/part-r-{p:05d}", "w", encoding="utf-8") as o:
        for k, group in groupby(pairs, key=lambda kv: kv[0]):
            keys += 1
            for key, result in reducer(k, (v for _, v in group)):
                o.write(f"{key}\t{result}\n")
    return {"read": len(pairs), "keys": keys, "s": time.time() - t0}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("job", choices=sorted(JOBS))
    ap.add_argument("--sf", type=int, choices=[1, 10], default=1)
    ap.add_argument("--maps", type=int, default=8, help="M, the number of map tasks (default 8)")
    ap.add_argument("--reduces", type=int, default=4, help="R, the number of reduce tasks (default 4)")
    ap.add_argument("--combiner", action="store_true", help="run the reduce on the output of each map task")
    ap.add_argument("--slots", type=int, default=2, help="P, tasks that run at the same time (default 2: the client's CPUs)")
    a = ap.parse_args()
    evidence("mapreduce")
    _, _, combinable = JOBS[a.job]
    if a.combiner and not combinable:
        raise SystemExit(f"job {a.job}: its reduce cannot be used as a combiner")
    files = sorted(glob.glob(f"{logs(a.sf)}/*.log"))
    if not files:
        raise SystemExit(f"no log files in {logs(a.sf)}: run  lab make_logs.py --sf {a.sf}  first")
    name = f"mr_{a.job}_sf{a.sf}"
    work, dest = f"{DATA}/mr/{name}", out(name)
    for d in (work, dest):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)

    splits = make_splits(files, a.maps)
    t0 = time.time()
    with Pool(a.slots) as pool:
        maps = pool.map(map_task, [(a.job, m, s, a.reduces, a.combiner, work) for m, s in enumerate(splits)], chunksize=1)
        t1 = time.time()
        inter = sorted(glob.glob(f"{work}/*.tsv"))
        inter_mb = sum(os.path.getsize(f) for f in inter) / 1e6
        reds = pool.map(reduce_task, [(a.job, p, work, dest) for p in range(a.reduces)], chunksize=1)
    t2 = time.time()
    shutil.rmtree(work)

    in_mb = sum(os.path.getsize(f) for f in files) / 1e6
    print(f"job {a.job}, SF{a.sf}: {len(files)} files, {in_mb:,.1f} MB; M={a.maps} maps, R={a.reduces} reduces, "
          f"P={a.slots} slots, combiner {'on' if a.combiner else 'off'}")
    print(f"  map     {t1 - t0:7.1f} s   lines read {sum(x['lines'] for x in maps):>12,}   "
          f"pairs emitted {sum(x['emitted'] for x in maps):>12,}   pairs written {sum(x['written'] for x in maps):>12,}")
    print(f"  shuffle            {len(inter)} intermediate files, {inter_mb:,.1f} MB")
    print(f"  reduce  {t2 - t1:7.1f} s   pairs read {sum(x['read'] for x in reds):>12,}   "
          f"keys {sum(x['keys'] for x in reds):>12,}")
    print(f"  total   {t2 - t0:7.1f} s   map tasks: {', '.join(f'{x['s']:.1f}' for x in maps)} s")
    print(f"Output: {dest}/part-r-*")
    rows = []
    for f in glob.glob(f"{dest}/part-r-*"):
        with open(f, encoding="utf-8") as fh:
            rows += [(k, int(v)) for k, v in (line.rstrip("\n").split("\t") for line in fh)]
    rows.sort(key=lambda kv: (-kv[1], kv[0]))
    print("Top 10: " + ", ".join(f"{k} {v:,}" for k, v in rows[:10]))


if __name__ == "__main__":
    main()
