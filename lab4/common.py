"""Helpers shared by the lab's scripts: where things are, and a copy of the output for the report."""
import datetime
import os
import sys

DATA = "/data"                  # Docker volume lab4_data, seen by every container
PARQUET = "/parquet"            # the Parquet folders of Lab 1, read-only
EVIDENCE = "/evidence"          # ~/tpcds/report/evidence/lab4 on your machine (client container only)


def logs(sf):
    """The folder of the generated access logs for one scale factor."""
    return f"{DATA}/logs/sf{sf}"


def truth(sf, name):
    """The right answer, computed by make_logs.py from the Parquet files of Lab 1."""
    return f"{DATA}/truth/sf{sf}/{name}.parquet"


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


def master_status(timeout=60):
    """The master's status (JSON), waiting until it answers and the workers of this profile are ALIVE:
    just after `dc up -d` the master needs a few seconds."""
    import json
    import time
    import urllib.request
    want = int(os.environ.get("LAB4_WORKERS", "4"))
    m, t0 = None, time.time()
    while time.time() - t0 < timeout:
        try:
            m = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=5))
            if sum(w["state"] == "ALIVE" for w in m["workers"]) >= want:
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
    want = int(sc.getConf().get("spark.cores.max", os.environ.get("LAB4_WORKERS", "4")))   # --total-executor-cores N
    for _ in range(100):                                       # wait (at most 10 s) for the executors to register
        if executors(sc) >= want:
            break
        time.sleep(0.1)
    app = sc.applicationId
    print(f"Spark application {app}: while it runs http://localhost:4040, "
          f"afterwards http://localhost:18080/history/{app}/jobs/")
    return s


def jobs_so_far(s):
    """How many Spark jobs this application has started until now."""
    return len(s.sparkContext.statusTracker().getJobIdsForGroup(None))
