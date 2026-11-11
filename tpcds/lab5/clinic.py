"""The clinic: run one job, then print what Spark measured, stage by stage, and check the result.

Usage: lab clinic.py job1 [--sf 10] [--explain]

  job1, job2, job3   the job to run (jobs/job1_join.py, jobs/job2_skew.py, jobs/job3_loop.py)
  --sf               the scale factor of the data: 10 (default) or 1
  --explain          also print the plan Spark ran (after the run: the final plan, with the choices of AQE)

The job runs on the cluster, in this process (the driver). While it runs, its UI is at http://localhost:4040;
afterwards, the history server shows it (the link is printed at the end).

Every run also appends a short block to ~/tpcds/report/evidence/lab5/summary.md: the date, the command, the version
of the job file, the plan's joins and exchanges, and the key lines of the output. Keep the blocks your report uses,
and give each one the identifier it backs (see evidence/lab5/INDEX.md).
"""
import argparse
import datetime
import decimal
import os
import sys
import hashlib
import importlib.util
import json
import time
import urllib.request

from common import EVIDENCE, evidence, spark

# The checksum of the right result of each job: the instructor's run of the jobs as they are in the starter,
# and the same queries in DuckDB, which gave the same checksums.
# Your changes must keep it: a faster job with another result is not a fix.
REFERENCE = {
    ("job1", 10): "638a94785c510ea4",
    ("job1", 1): "57b2a1801e05420b",
    ("job2", 10): "50dfe9b9977ca899",
    ("job2", 1): "91ed426ce51da4e2",
    ("job3", 10): "e815d3f4dda47941",
    ("job3", 1): "de47b5e357dac3c3",
}

MB = 1024 * 1024
SHOW = 10                                      # at most this many stages in the table


JOBS = {"job1": "job1_join.py", "job2": "job2_skew.py", "job3": "job3_loop.py"}


def load(job):
    spec = importlib.util.spec_from_file_location(job, f"/lab/jobs/{JOBS[job]}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def canonical(v):
    """One value as text, the same whatever the type Spark gives back (Decimal, float, int, None)."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        v = decimal.Decimal(repr(v))
    if isinstance(v, decimal.Decimal):
        return str(v.quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_EVEN))
    return str(v)


def checksum(rows):
    """A fingerprint of the result: the rows as text, in sorted order, hashed. The order of the rows does not matter."""
    lines = sorted("|".join(canonical(v) for v in row) for row in rows)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16], len(lines)


def rest(ui, path):
    with urllib.request.urlopen(f"{ui}/api/v1/{path}", timeout=10) as r:
        return json.load(r)


def mb(b):
    return f"{b / MB:,.1f}"


def report(ui, app, wall):
    """The stages of the application, from the UI's REST API, as one table."""
    stages = [st for st in rest(ui, f"applications/{app}/stages") if st["status"] == "COMPLETE"]
    stages.sort(key=lambda st: (st["stageId"], st["attemptId"]))
    jobs = rest(ui, f"applications/{app}/jobs")
    rows, total = [], {"tasks": 0, "in": 0, "sr": 0, "sw": 0, "spill": 0}
    for st in stages:
        sec = (_ms(st["completionTime"]) - _ms(st["submissionTime"])) / 1000
        q = rest(ui, f"applications/{app}/stages/{st['stageId']}/{st['attemptId']}/taskSummary?quantiles=0.5,1.0")
        med, mx = (x / 1000 for x in q["executorRunTime"])
        recs = [a + b for a, b in zip(q["inputMetrics"]["recordsRead"], q["shuffleReadMetrics"]["readRecords"])]
        spill = st["diskBytesSpilled"]
        rows.append((st["stageId"], st["numTasks"], sec, med, mx, st["inputBytes"], st["shuffleReadBytes"],
                     st["shuffleWriteBytes"], spill, recs))
        total["tasks"] += st["numTasks"]
        total["in"] += st["inputBytes"]
        total["sr"] += st["shuffleReadBytes"]
        total["sw"] += st["shuffleWriteBytes"]
        total["spill"] += spill
    top = max(rows, key=lambda r: r[2]) if rows else None
    print()
    print(f"{'stage':>5} {'tasks':>6} {'time s':>7} {'task median s':>14} {'task max s':>11} "
          f"{'input MB':>9} {'shuffle read MB':>16} {'shuffle write MB':>17} {'spill MB':>9}")
    shown = rows
    if len(rows) > SHOW:                       # a loop runs many small queries: show only the longest stages
        keep = sorted(rows, key=lambda r: r[2], reverse=True)[:SHOW]
        shown = [r for r in rows if r in keep]
    for r in shown:
        mark = "  <- longest" if r is top else ""
        print(f"{r[0]:>5} {r[1]:>6} {r[2]:>7.1f} {r[3]:>14.2f} {r[4]:>11.2f} {mb(r[5]):>9} {mb(r[6]):>16} "
              f"{mb(r[7]):>17} {mb(r[8]):>9}{mark}")
    if len(shown) < len(rows):
        print(f"{'...':>5} {len(rows) - len(shown)} shorter stages not shown")
    print(f"{'all':>5} {total['tasks']:>6} {'':>7} {'':>14} {'':>11} {mb(total['in']):>9} {mb(total['sr']):>16} "
          f"{mb(total['sw']):>17} {mb(total['spill']):>9}")
    print()
    key = [f"time {wall:.1f} s   jobs {len(jobs)}   stages {len(rows)} (and {sum(j.get('numSkippedStages', 0) for j in jobs)} skipped)"
           f"   tasks {total['tasks']}   input {mb(total['in'])} MB   shuffle write {mb(total['sw'])} MB"
           f"   spill {mb(total['spill'])} MB"]
    if top:
        key.append(f"longest stage: {top[0]}, {top[2]:.1f} s, {top[1]} tasks; task time median {top[3]:.2f} s, max {top[4]:.2f} s; "
                   f"rows read per task median {top[9][0]:,.0f}, max {top[9][1]:,.0f}")
    for line in key:
        print(line)
    cached = rest(ui, f"applications/{app}/storage/rdd")
    for c in cached:
        line = (f"cached DataFrame: {c['numCachedPartitions']} of {c['numPartitions']} partitions, "
                f"{mb(c['memoryUsed'])} MB in memory, {mb(c['diskUsed'])} MB on disk")
        print(line)
        key.append(line)
    return key


def final_plan(df):
    """The plan Spark ran (after the run, AQE's final plan), as text."""
    return df.sparkSession._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")


def plan_counts(plan):
    """The joins and exchanges of the final plan, in one line."""
    final = plan.split("== Final Plan ==")[-1].split("== Initial Plan ==")[0]
    names = ["SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "Window", "Exchange hashpartitioning", "BroadcastExchange"]
    found = [f"{final.count(n)} {n.split()[0]}" for n in names if final.count(n)]
    return "plan (final): " + (", ".join(found) if found else "no join, no exchange")


def summary(job, sf, app, lines):
    """Append one block for this run to /evidence/summary.md (~/tpcds/report/evidence/lab5/summary.md)."""
    if not os.path.isdir(EVIDENCE):
        return
    src = open(f"/lab/jobs/{JOBS[job]}", "rb").read()
    version = hashlib.sha256(src).hexdigest()[:8]
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path = os.path.join(EVIDENCE, "summary.md")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8") as f:
        if new:
            f.write("# clinic.py: one block per run (written by clinic.py)\n\n"
                    "Replace `ID?` in the title of each block your report uses with what it backs (e.g. Job 1 baseline, J2Q1);\n"
                    "delete the blocks of the runs you do not use. Do not edit the lines inside the blocks.\n")
        f.write(f"\n## ID? — {job}, SF{sf}, {stamp}\n\n"
                f"`clinic.py {' '.join(sys.argv[1:])}` · `jobs/{JOBS[job]}` version {version} · {app}\n\n```\n")
        f.write("\n".join(lines) + "\n```\n")
    try:
        os.chmod(path, 0o666)          # on Linux the container writes as root: let the student edit the file
    except OSError:
        pass


def _ms(t):
    """A time of the REST API ('2026-11-06T10:15:02.123GMT') in milliseconds."""
    from datetime import datetime, timezone
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f%Z").replace(tzinfo=timezone.utc).timestamp() * 1000


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("job", choices=sorted(JOBS))
    ap.add_argument("--sf", type=int, default=10, choices=[1, 10])
    ap.add_argument("--explain", action="store_true")
    a = ap.parse_args()
    evidence("clinic")

    module = load(a.job)
    s = spark(f"{a.job}-sf{a.sf}")
    sc = s.sparkContext
    app, ui = sc.applicationId, sc.uiWebUrl.replace("client", "localhost")
    print(f"{a.job} at SF{a.sf}: application {app}, running (UI: http://localhost:4040)")

    t0 = time.time()
    result = module.run(s, a.sf)
    df = None
    if not isinstance(result, list):
        df, result = result, result.collect()
    wall = time.time() - t0
    plan_line = None
    if df is not None:
        plan = final_plan(df)
        plan_line = plan_counts(plan)
        if a.explain:
            print()
            print(plan)
    elif a.explain:
        print("\n(--explain: this job runs many queries in a loop; look at them in the SQL tab of the UI)")

    key = report(ui, app, wall)
    if plan_line:
        print(plan_line)
        key.insert(0, plan_line)
    cs, n = checksum(result)
    ref = REFERENCE.get((a.job, a.sf))
    verdict = "no reference" if ref is None else ("SAME as the reference" if cs == ref else
                                                  f"DIFFERENT from the reference ({ref}): this is not a fix")
    print(f"result: {n} rows, checksum {cs}: {verdict}")
    print(f"afterwards: http://localhost:18080/history/{app}/stages/")
    summary(a.job, a.sf, app, key + [f"result: {n} rows, checksum {cs}: {verdict}"])
    s.stop()


if __name__ == "__main__":
    main()
