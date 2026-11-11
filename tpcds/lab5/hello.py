"""Check the cluster before the lab: the master, the 4 workers, and one small Spark job on all of them.

Usage: spark-submit hello.py
"""
import socket

from common import evidence, executors, master_status, spark, workers


def main():
    evidence("hello")
    m = master_status()
    alive = [w for w in m["workers"] if w["state"] == "ALIVE"]
    print(f"master {m['url']}: {len(alive)} workers alive, {m['cores']} cores, {m['memory']} MB for executors")
    s = spark("hello")
    sc = s.sparkContext
    hosts = sorted(set(sc.parallelize(range(64), 16).map(lambda _: socket.gethostname()).collect()))
    n = executors(sc)
    mem = sc.getConf().get("spark.executor.memory")
    print(f"{n} executors, {sc.defaultParallelism} cores, {mem} per executor; tasks ran on {', '.join(hosts)}")
    s.stop()
    want = workers()
    ok = len(alive) == want and n == want and len(hosts) == want
    print("OK: the cluster is ready" if ok else "NOT OK: see Appendix B of the handout")


if __name__ == "__main__":
    main()
