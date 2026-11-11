"""Helpers shared by the lab's scripts: where things are, and a copy of the output for the report."""
import datetime
import os
import sys

DATA = "/data"                  # Docker volume lab5_data, seen by every container
PARQUET = "/parquet"            # the Parquet folders of Lab 1, read-only
EVIDENCE = "/evidence"          # ~/tpcds/report/evidence/lab5 on your machine (client container only)


def table(sf, name):
    """The folder of one TPC-DS table of Lab 1."""
    path = f"{PARQUET}/sf{sf}/{name}"
    if not os.path.isdir(path):
        raise SystemExit(f"folder not found: {path} (your Lab 1 Parquet files must be in ~/tpcds/parquet/sf{sf})")
    return path


def out(name):
    return f"{DATA}/out/{name}"


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)
            st.flush()

    def flush(self):
        for st in self.streams:
            st.flush()


def evidence(name):
    """From now on, also append what this script prints to /evidence/<name>.txt (one file per script)."""
    if not os.path.isdir(EVIDENCE):
        return
    path = os.path.join(EVIDENCE, f"{name}.txt")
    f = open(path, "a", encoding="utf-8")
    try:
        os.chmod(path, 0o666)          # on Linux the container writes as root: let the student edit the file
    except OSError:
        pass
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    f.write(f"\n=== {stamp}  {os.path.basename(sys.argv[0])} {' '.join(sys.argv[1:])}\n")
    sys.stdout = _Tee(sys.__stdout__, f)


def workers():
    """How many workers this profile has: 4, or 2 with the small profile."""
    return int(os.environ.get("LAB5_WORKERS", "4"))


def master_status(timeout=60):
    """The master's status (JSON), waiting until it answers and the workers of this profile are ALIVE:
    just after `dc up -d` the master needs a few seconds."""
    import json
    import time
    import urllib.request
    m, t0 = None, time.time()
    while time.time() - t0 < timeout:
        try:
            m = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=5))
            if sum(w["state"] == "ALIVE" for w in m["workers"]) >= workers():
                break
        except OSError:
            pass
        time.sleep(1)
    if m is None:
        raise SystemExit("the Spark master does not answer: is the cluster up? (dc up -d)")
    return m


def executors(sc):
    """How many executors this application has now (the driver is not counted)."""
    return sc._jsc.sc().getExecutorMemoryStatus().size() - 1


def spark(name):
    """A SparkSession for one of the lab's scripts (the settings are in conf/spark-defaults.conf)."""
    from pyspark.sql import SparkSession
    import time
    master_status()
    s = SparkSession.builder.appName(name).getOrCreate()
    sc = s.sparkContext
    for _ in range(100):                                       # wait (at most 10 s) for the executors to register
        if executors(sc) >= workers():
            break
        time.sleep(0.1)
    return s
