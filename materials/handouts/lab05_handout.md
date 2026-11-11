# CDLE — Lab 5: The Performance Clinic

**Measure → hypothesise → change one thing → prove it: three slow Spark jobs, three fixes, the same results**

| | |
|---|---|
| **Duration** | 100 minutes in class: about 75 for the lab, and the rest to catch up on Labs 3 and 4 |
| **Groups** | Two students, one laptop running the containers (the other is the backup) |
| **Data** | The TPC-DS tables of Lab 1, SF10 (`store_sales`, `item`, `date_dim`, `customer` in `$T/parquet/sf10`) |
| **Tools** | The Spark cluster of Lab 4 (the same image), `clinic.py`, the Spark UI, an AI assistant |

## Learning goals

By the end of this lab you should be able to:

- find the stage that dominates a job, and read in it the shuffle, the input, the spill, and the slowest task against the median one;
- recognise in `explain()` a sort-merge join that could be a broadcast join, and fix it;
- recognise skew (one task much longer than the others) and fix it, and say when a hot key slows a whole job down;
- recognise work that a loop repeats, and decide when `cache()` pays;
- use an AI assistant for hypotheses with predictions, and accept only what the numbers confirm.

### The clinic

Deck 5, slides 32–34: every fix follows the same loop, and every step leaves a trace.

```
 1 measure      lab clinic.py jobN               the baseline: time, stages, shuffle, the slowest task
 2 hypothesise  why is it slow? predict a number  "if broadcast is the cause, the shuffle drops to ~0 MB"
 3 change ONE thing in jobs/jobN_*.py, and run clinic.py again: the result must stay SAME
 4 verdict      confirmed / refuted / partly       one entry in your evidence log, either way
```

Every question has an identifier (**J1Q1**, **J2Q3**, **D2**…): start each answer in your report with it.

Rules of the clinic: **one change per run** (two changes at once give zero conclusions); **cite stage and task numbers**, not only the time;
**a faster job with a DIFFERENT result is not a fix**; a refuted hypothesis is a result too.

The cluster is the one of Lab 4: a master, 4 workers with one executor each (1 core, 768 MB), the history server, and the `client`
container, where your driver runs. The data are the Parquet tables of Lab 1: nothing to generate.

---

## 0. Before the lab (at home, about 15 minutes) — required

There is no time for these steps in class. Come with everything below done.

### 0.1 Check what you need from Lab 1 and Lab 4

```bash
export T=~/tpcds
ls -d $T/parquet/sf10/store_sales $T/parquet/sf10/item $T/parquet/sf10/date_dim $T/parquet/sf10/customer
docker image ls cdle-lab4
```

The four folders must exist. `cdle-lab4` is the image of Lab 4 (about 1.3 GB): if it is listed, Lab 5 builds nothing.
If you removed it, `docker compose build` in 0.3 makes it again (about 470 MB to download).

### 0.2 Docker: 4 CPUs and 8 GB; stop Lab 4

Docker needs **4 CPUs and 8 GB of memory**, as in Lab 4 (`docker info | grep -E "CPUs|Total Memory"`).
**A laptop with 8 GB?** Give Docker 4 CPUs and 4 GB, and use the small profile ([Appendix B](#appendix-b--problems-and-fixes)).

Lab 4 and Lab 5 have containers with the same names. Remove the Lab 4 containers; `down` without `-v` keeps their volume
(the logs, until you hand in the Lab 4 report):

```bash
docker compose -f $T/lab4/compose.yaml down
```

Free disk space needed: about 1 GB (the event logs and the outputs of the jobs).

### 0.3 Get the starter pack and the report part, start the cluster

Download `lab5_starter.zip` and `lab5_report_template.zip` from the course page and unzip them in `~/tpcds`:

```bash
cd ~/tpcds
unzip lab5_starter.zip                 # -> ~/tpcds/lab5/
unzip -n lab5_report_template.zip      # -> ~/tpcds/report/ (adds the Lab 5 files; -n never replaces your work)
cd ~/tpcds/lab5
cp -r jobs jobs.orig                   # the original jobs, to go back to
docker compose up -d
docker compose exec -T client python hello.py
docker compose exec -T client python clinic.py job1
```

**Check:** `hello.py` ends with `OK: the cluster is ready`, and `clinic.py` ends with a line `result: 671 rows, checksum 638a94785c510ea4: SAME as the reference`.
That was your first baseline of Job 1 (it took about 20 s, plus the start of Spark).

| File | What it is |
|---|---|
| `jobs/job1_join.py`, `jobs/job2_skew.py`, `jobs/job3_loop.py` | **The three slow jobs.** You change them. |
| `clinic.py` | Runs one job, prints what Spark measured, stage by stage, and checks the result (SAME / DIFFERENT) |
| `hello.py` | Checks the cluster: 4 workers, 4 executors |
| `ai/session-05.md` | The brief for the AI session (Job 3) |
| `compose.yaml`, `compose.small.yaml`, `Dockerfile`, `conf/`, `common.py` | The cluster of Lab 4, and Spark's settings |

The report part adds `report_lab5.md`, `ai-log-lab5.md`, `evidence/lab5/INDEX.md` and `SUBMISSION_lab5.md` to `~/tpcds/report/`.

### 0.4 Read

Slides 22–29 of deck 5 (the Spark UI, AQE, cache, skew, salting) and slides 32–34 (the clinic).

---

## Conventions

Run these lines **in every new terminal** before you start. `dc` and `lab` are the shortcuts of Lab 4, now for `lab5`:

```bash
export T=~/tpcds
dc()  { docker compose -f $T/lab5/compose.yaml "$@"; }
lab() { dc exec -T client python "$@"; }
```

You edit the jobs **on your laptop** (`~/tpcds/lab5/jobs/`), with any editor; the containers see the change at once.
The original jobs stay in `~/tpcds/lab5/jobs.orig/` (0.3): to go back, copy a file from there.

**What `clinic.py` prints.** One line per stage that ran (for a loop, only the 10 longest), then a summary:

```
stage  tasks  time s  task median s  task max s  input MB  shuffle read MB  shuffle write MB  spill MB
    3      8     3.3           1.95        2.19     143.6              0.0             378.7       0.0
    8      7     6.6           3.40        3.52       0.0            379.8             335.4     272.6  <- longest
...
time 17.7 s   jobs 9   stages 9 (and 11 skipped)   tasks 31   input 144.5 MB   shuffle write 716.1 MB   spill 518.3 MB
longest stage: 8, 6.6 s, 7 tasks; task time median 3.40 s, max 3.52 s; rows read per task median 4,401,477, max 4,431,907
result: 671 rows, checksum 638a94785c510ea4: SAME as the reference
```

| Column | Meaning |
|---|---|
| time s | from the start of the stage to the end of its last task |
| task median s / task max s | how long a typical task and the slowest task ran: far apart means one task does much more work (skew) |
| input MB | read from the Parquet files (or from the cache) |
| shuffle write / read MB | written to local disk for the next stage / fetched from the other executors |
| spill MB | data that did not fit in the executor's memory and went to disk |
| checksum | a fingerprint of the result, whatever the order of its rows. It must stay the same. |

Every run also writes the evidence of your report, in `~/tpcds/report/evidence/lab5/`:

- `clinic.txt`: the whole output of every run, with its date and command;
- `summary.md`: one short block per run, written by `clinic.py`: the date, the command, the version of the job file, the plan in one line
  (`plan (final): 2 SortMergeJoin, 5 Exchange`), and the lines `time`, `longest stage`, `cached` and `result`. You do not copy any number by hand:
  give each block your report uses its name (replace `ID?` in its title with the name in the table of `evidence/lab5/INDEX.md`: `Job 1 baseline`,
  `Job 1 after` …) and delete the blocks you do not use. That table is already filled in: it says which block backs which part of the report.

**The Spark UI.** While a job runs: <http://localhost:4040>. Afterwards: the history server, at the link that `clinic.py` prints
(`http://localhost:18080/history/app-…/stages/`; wait a few seconds and reload). Its **SQL / DataFrame** tab shows each query as a graph,
with the joins and the exchanges, and the **Storage** tab shows what is cached.

---

## Class plan

| Time | Part |
|---|---|
| 0–10 min | Introduction: the clinic loop, reading `clinic.py` and the UI |
| 10–30 min | [Job 1](#job-1--the-needless-shuffle-20-min-guided) — the needless shuffle (guided) |
| 30–50 min | [Job 2](#job-2--the-hot-key-20-min) — the hot key |
| 50–70 min | [Job 3](#job-3--the-loop-with-the-ai-20-min) — the loop, with the AI |
| 70–75 min | [Discussion](#discussion-5-min) |
| 75–100 min | **Catch-up time:** finish Lab 3 or Lab 4, with the instructor in the room |

Start the containers now and leave them running:

```bash
dc up -d
lab hello.py
```

Before measuring: plug in your laptop, close other heavy applications, and don't use the machine while a job runs.
Every time is one run: if a number looks odd, run it again.

---

## Job 1 — The needless shuffle (20 min, guided)

`jobs/job1_join.py`: the revenue of each item category per month. It joins `store_sales` (28.8 million rows) with `item` (102,000 rows)
and `date_dim` (73,049 rows), and sums. Read it: 20 lines.

**1. Measure.**

```bash
lab clinic.py job1 --explain
```

**Record** the baseline: time, shuffle write, spill, and the longest stage (tasks, time). In the plan (`== Final Plan ==`), count the joins and the
`Exchange` nodes, and say which join strategy Spark used.

**2. Hypothesise.** The two small tables are a few MB.

- **J1Q1.** What would Spark normally do with them (slides 14–16)? Why didn't it? (Read the job's first line.)

Write your hypothesis and its **prediction** before you change anything: what will happen to the shuffle write, the spill, and the time?

**3. Change one thing.** Either delete the line that sets `spark.sql.autoBroadcastJoinThreshold` (or comment it out with `#`),
or keep it and ask for the broadcast explicitly: `.join(F.broadcast(item), …)` and `.join(F.broadcast(dates), …)`. Then:

```bash
lab clinic.py job1 --explain
```

**4. Record:**

| Run | Time (s) | Joins (strategy) | Exchanges | Shuffle write (MB) | Spill (MB) | Longest stage: tasks, time (s) | Result |
|---|---|---|---|---|---|---|---|
| baseline | | | | | | | |
| your change | | | | | | | |

Questions:

- **J1Q2.** The job moved about 700 MB through the shuffle to join with tables of a few MB. Which rows crossed the shuffle, and how many times?
- **J1Q3.** After the change, what does each executor receive instead (look for `BroadcastExchange`)? When would broadcasting be the wrong choice (slide 36)?
- **J1Q4.** Someone turned broadcasting off "after an out-of-memory error". What would you do instead of a global switch?

**The message of Job 1:** the shuffle is the cost of a join; do not pay it for a table of 5 MB. Read the plan before you pay for it.

---

## Job 2 — The hot key (20 min)

`jobs/job2_skew.py`: loyalty cohorts. Members of the loyalty programme are followed one by one: the year of each member's first purchase
(a window function, `min(d_year)` over the member). Everyone else gets the number `-1` and no cohort. Read it.

**1. Measure.**

```bash
lab clinic.py job2
```

**Record** the time, the longest stage, its tasks, and in it the task time (median, max) and the rows read per task (median, max).
Open the stage in the UI (the link, tab **Stages**, then the longest stage): look at the **Summary Metrics** and the task list.

**2. Hypothesise.** One task reads far more rows than the others. The window is partitioned by `member`, so all the rows of one member
go to the same task.

- **J2Q1.** Which value of `member` has that many rows? (Appendix A: how many sales are not by a member?)

Predict: if that key were spread over many tasks, what would the max task time and the stage time become?

**3. Change one thing.** Choose **one** of these two fixes (slide 29), and apply it in `job2_skew.py`:

**Fix A — salting:** spread the key `-1` over 16 tasks, with a second partitioning column that is 0 for a member. Replace the line of `first_year` with these two lines
(keep the 4 spaces at the start of each line):

```python
    s = s.withColumn("salt", F.when(F.col("member") == -1, F.abs(F.hash("ss_sold_date_sk", "ss_net_paid")) % 16).otherwise(0))
    s = s.withColumn("first_year", F.min("d_year").over(Window.partitionBy("member", "salt")))
```

**Fix B — do not compute what you throw away:** run the window on the members only, and add the others back, without a cohort.
Replace the two lines of `first_year` and `cohort` with:

```python
    members = (s.where(F.col("member") != -1)
               .withColumn("cohort", F.min("d_year").over(Window.partitionBy("member"))))
    others = s.where(F.col("member") == -1).withColumn("cohort", F.lit(None).cast("int"))
    s = members.unionByName(others, allowMissingColumns=True)
```

Then run the job again:

```bash
lab clinic.py job2
```

**4. Record:**

| Run | Time (s) | Longest stage: tasks, time (s) | Task time median / max (s) | Rows per task median / max | Input (MB) | Shuffle write (MB) | Spill (MB) | Result |
|---|---|---|---|---|---|---|---|---|
| baseline | | | | | | | | |
| your fix | | | | | | | | |

Questions:

- **J2Q2.** Your fix gives exactly the same result. Why? (Fix A: what is the cohort of `-1`, whatever its salt? Fix B: what do the others get?)
- **J2Q3.** AQE has skew handling, and it is on (slides 24 and 27). Why did it not split this task? (What kind of operator does it handle?)
- **J2Q4.** Your fix: what did it cost? (Compare the input and the shuffle write with the baseline.) If you chose Fix B, why is the input twice as large?
- **J2Q5.** The key `-1` holds about 54% of the rows. With 4 executors, a stage at its best gives each executor ¼ of the work. In `store_sales`, 4.5% of the sales
  have no customer (NULL): one hot key too. Would a key with 4.5% of the rows slow this stage down on 4 executors? And on 400? (Lab 2: skew; slide 27: the guest user.)

**The message of Job 2:** the time of a stage is the time of its slowest task. A hot key is often a non-key: NULL, unknown, `-1`.

---

## Job 3 — The loop, with the AI (20 min)

`jobs/job3_loop.py`: customer segments. It computes how much each customer spent (about 496,000 customers), then, for ten limits
from 25,000 to 250,000, counts the customers who spent more, in a loop of ten small queries. Read it.

This time the assistant is your consultant (slide 32). Follow `ai/session-05.md`:

1. **Measure:** `lab clinic.py job3`. **Record** the time, the jobs, the tasks, and the input MB, and answer **J3Q1** (below).
2. **Ask:** the brief of `session-05.md`, with the job and the output of `clinic.py`. Three ranked hypotheses, each with a prediction.
3. **Verify:** apply the change of the first hypothesis, **only that one**, and run `lab clinic.py job3`. Confirmed, refuted, or partly?
4. **Again:** if the prediction failed, show the assistant the new output and ask why; make the next change; measure.

**Record:**

| Run | Change | Prediction | Time (s) | Jobs | Tasks | Input (MB) | Shuffle write (MB) | Verdict | Result |
|---|---|---|---|---|---|---|---|---|---|
| baseline | — | — | | | | | | — | |
| 1 | | | | | | | | | |
| 2 | | | | | | | | | |

Questions:

- **J3Q1.** How many times did Spark read `store_sales` in the baseline? (Look at the input MB of the stages.)
- **J3Q2.** If your change used `cache()` (slides 25–26): what does the **Storage** tab show (partitions, size in memory)? How many tasks does each of the ten queries run now, and why that number?
  (`clinic.py` prints the cached DataFrame's partitions. Where do 200 partitions come from: Lab 4, B.2.)
- **J3Q3.** Could the ten queries be **one** query? What would that change for the reads and the tasks?
- **J3Q4.** What did the assistant get right, and what could it not know without your numbers?

**The message of Job 3:** Spark does not remember results between actions: a loop of actions computes its input again each time, unless you cache it.
The cache is memory rent: it pays when you reuse it, and the UI shows whether it fits. And a prediction that fails tells you something too.

---

## Discussion (5 min)

- **D1.** Which fix gave the most, and in which number did you see it before you saw it in the time?
- **D2.** Which of the three problems could you have seen in `explain()` **before** running the job?
- **D3.** Here the jobs take about 15 seconds, and Spark's start takes 5. Which of your three fixes would matter more on 10 TB and 400 machines, and which less?

Discuss them in class; answer them in the report (section Discussion), in a few lines each.

Then use the rest of the class to catch up on Labs 3 and 4. Leave the containers as they are if you continue at home; `dc stop` stops them.

---

## Take-home work

**Deadline:** Sunday 29 November 2026, 23:59 (see the course page on Moodle). Write, check and hand in the report with the report kit
(`report_kit.zip`) and this lab's report part (`lab5_report_template.zip`): follow `SUBMISSION.md` in the kit, with `lab5` in place of `labN`,
and `SUBMISSION_lab5.md` (in `~/tpcds/report/`) for what is particular to Lab 5.

1. **Finish the three tables**, the evidence log and the answers (J1Q1–J3Q4, D1–D3), if you did not finish them in class.
2. **AI session 05:** write it in `ai-log-lab5.md`, entry *Session 05*: the brief as sent, the hypotheses, each change and its verdict.
3. **Optional:** the other fix of Job 2; Job 3 as one query; your fixed jobs at SF1 (`--sf 1`): do the fixes still pay on small data?
4. **Clean up** when your report is handed in: `dc down -v` removes the containers and the volume; `docker rmi cdle-lab4` removes the image (1.3 GB).

### Report (maximum 3 pages, PDF, in the format of the report kit)

This report is shorter than the others: **3 pages at most**, because this lab asks for less.

1. **Machine and Docker:** the CPUs and memory given to Docker, and the profile (4 workers, or the small one). Two lines.
2. **Job 1, Job 2, Job 3:** for each job, its table (baseline and change), one entry of the evidence log (the form is below), and the answers to its questions,
   each one starting with its identifier and repeating the question in one short line (for example: **J2Q3.** *Why did AQE not split the task?* …).
   For Job 3, one evidence-log entry per hypothesis you tested, the refuted ones too.
3. **Discussion:** your answers to D1–D3, in the same way.
4. **Limits:** one result of this lab that you would not trust on a cluster of 400 machines, and why.

One entry of the evidence log:

```
hypothesis:  …                  source: you / the assistant
prediction:  … (a number)
evidence:    … (the numbers of clinic.py, before → after)
verdict:     confirmed / refuted / partly
```

**Evidence.** Every number of your report must appear, as it is, in `evidence/lab5/summary.md` or `clinic.txt`, or in a screenshot of the
Spark UI named after what it backs (`J2Q1_tasks.png`) and listed in `evidence/lab5/INDEX.md`. In `summary.md`, the blocks carry the identifiers.

`pack.py` also puts your three jobs (`jobs/`) in the zip, as they are when you run it: the fixed versions (see `SUBMISSION_lab5.md`).
Be ready to explain any number of your report in class.

### Rules for TPC-DS material

The rules of Lab 1 still apply: do not publish the kit, the generated data or the query files, and do not present your numbers as TPC-DS results.

---

## Appendix A — Expected values

Rows, checksums, bytes, tasks and rows per task come from the instructor's run with the 4 Parquet files that a Lab 1 conversion with `--threads 4` gives,
and should match yours (the data are deterministic). If you converted with another `--threads`, the tasks and the rows per task differ, not the checksums.
Times will not match, and depend on the run.

**Data (SF10):** `store_sales` 28,800,991 rows in 4 files of about 254.5 MiB (1,018.2 MiB), from the Lab 1 conversion with `--threads 4`. Sales that are not by a loyalty member and have a date (`member = -1` in Job 2):
14,803,927 of 27,504,814, that is 53.8%. Sales with no customer at all: 4.5%.

| Job | Result | Checksum, SF10 | Checksum, SF1 |
|---|---:|---|---|
| job1 | 671 rows | `638a94785c510ea4` | `57b2a1801e05420b` |
| job2 | 27 rows | `50dfe9b9977ca899` | `91ed426ce51da4e2` |
| job3 | 10 rows | `e815d3f4dda47941` | `de47b5e357dac3c3` |

The checksums were also computed with DuckDB, from the same tables, without Spark: they are the right answers.

**Job 1** (SF10):

| Run | Time (s) | Joins | Shuffle write (MB) | Spill (MB) | Longest stage |
|---|---:|---|---:|---:|---|
| baseline | 17–19 | 2 SortMergeJoin, 5 Exchanges | 716.1 | 518 | 7 tasks, 6–8 s |
| broadcast (either change) | 6.5–7 | 2 BroadcastHashJoin, 1 Exchange | 0.3 | 0 | the scan, 8 tasks, 2.2 s |

**Job 2** (SF10):

| Run | Time (s) | Longest stage | Task time median / max (s) | Rows per task median / max | Input (MB) | Shuffle write (MB) | Spill (MB) |
|---|---:|---|---|---|---:|---:|---:|
| baseline | 16–16.5 | 4 tasks, 9 s | 3.4–3.7 / 9.0 | 6.4 M / 14.9 M | 109.5 | 220.0 | 309 |
| salting | 12–13 | 5 tasks, 5 s | 4.0 / 4.3 | 6.0 M / 6.8 M | 109.5 | 229.7 | 197 |
| members only | 10.5–11 | 13 tasks, 4 s | 0.7 / 2.4 | 3.4 M / 7.0 M | 217.1 | 108.1 | 69 |

**Job 3** (SF10):

| Run | Time (s) | Jobs | Tasks | Input (MB) | Shuffle write (MB) |
|---|---:|---:|---:|---:|---:|
| baseline (10 queries, each reads `store_sales`) | 14–15 | 31 | 131 | 927.8 | 188.1 |
| `cache()` of `spent` | 12 | 23 | 2,219 | 150.3 | 19.0 |
| `cache()` of `spent.coalesce(4)` | 9 | 23 | 63 | 149.7 | 18.8 |
| `cache()`, and `spark.sql.shuffle.partitions` = 8 | 7–8 | 23 | 107 | 149.7 | 18.7 |
| one query for the ten limits | 6 | 4 | 14 | 92.8 | 18.8 |

With the cache, the input counts the reads from memory too: 92.8 MB from the files once, then 5.7 MB from the cache for each query.
The cached DataFrame kept the 200 partitions of `spark.sql.shuffle.partitions`: AQE did not coalesce them, as it does for the shuffle of a query that is not cached,
so every later query runs 200 small tasks. `coalesce(4)` before the cache, or 8 shuffle partitions, gives few partitions; one query avoids the loop altogether.

**At SF1, small profile (2 workers):** baselines 7 s, 6.5 s and 8 s. Broadcast 5 s (shuffle write 70.8 → 0.1 MB). Salting and members only: 6.5 s, no gain in time
(AQE makes 2 tasks for the 2 executors, and the key `-1`, with 1.6 M rows, fills about half the work: nothing is waiting for it). The `cache()` of Job 3 makes it **slower**: 12 s, 2,213 tasks; one query 5 s.

---

## Appendix B — Problems and fixes

| Problem | Symptom | Fix |
|---|---|---|
| Docker is not running | `Cannot connect to the Docker daemon` | Start Docker Desktop (or `sudo systemctl start docker` on Linux) |
| Lab 4 (or Lab 3) is still there | `container name "/client" is already in use` | `docker compose -f $T/lab4/compose.yaml down` (without `-v`) |
| A port is taken | `port is already allocated` on `dc up` (8080, 18080 or 4040) | Stop the program that uses it, or change the left-hand number in `compose.yaml` |
| The shortcuts are missing, or point to Lab 4 | `command not found: lab`, or `no such service` | Run the lines of [Conventions](#conventions) in this terminal |
| No Lab 1 data | `folder not found: /parquet/sf10/customer` | Your Parquet files must be in `~/tpcds/parquet/sf10` and the starter pack in `~/tpcds/lab5` |
| Your change is not seen | the old numbers again | Edit `~/tpcds/lab5/jobs/…` on your laptop, not in the container (`/lab` is read-only), and save the file |
| `DIFFERENT from the reference` | your change altered the result | Not a fix: compare with your copy (`diff $T/lab5/jobs.orig/job2_skew.py $T/lab5/jobs/job2_skew.py`) and undo or correct it |
| A Python error in your job | `SyntaxError`, `NameError` … with the line | Indentation and names: the code you add must be at the same level as the lines around it |
| A job waits forever: `Initial job has not accepted any resources` | the cores are taken by another application | Only one job at a time: wait for the other, or stop it with Ctrl-C |
| An executor is killed with "Out of memory", or Docker kills a container (exit 137) | Docker has less than 8 GB | Check `docker info`; use the small profile |
| The history server does not show an application | the link gives "Application not found" | Wait 5 seconds and reload |
| You want to start again from nothing | | `dc down -v`, then section 0.3 again (without `unzip`); your jobs are on your laptop and stay |

**The small profile (8 GB laptops).** Start it with both compose files, and use this `dc` in every terminal.
If you had started the 4-worker cluster before, remove its containers first (`docker compose -f $T/lab5/compose.yaml down`).

```bash
dc() { docker compose -f $T/lab5/compose.yaml -f $T/lab5/compose.small.yaml "$@"; }
dc up -d
lab hello.py           # 2 workers, 2 executors: OK
```

Run every job with `--sf 1` (for example `lab clinic.py job1 --sf 1`), and compare with the SF1 checksums of Appendix A. The effects are smaller at SF1,
but they are there: look at the shuffle, the rows per task and the input, more than at the time. Say in the report that you used it.

If you hit a problem that is not in this table, write down the exact error message and ask.

---

## Appendix C — Typical durations

Measured on the instructor's laptop (Apple M3 Max, 16 cores, 48 GB of memory), with Docker limited to 8 GB as in section 0.2, but to 16 CPUs instead of 4
(each container capped as in `compose.yaml`),
and in a simulation of a 4-core laptop (all containers on the same 4 cores). The times are those `clinic.py` prints; every run also takes about 5 s to start Spark.

| Step | Instructor's laptop | Simulated 4-core laptop |
|---|---|---|
| Section 0.3: unzip, `up -d`, `hello.py`, the first run of Job 1 (the image already there) | 46–48 s | |
| Job 1: baseline / broadcast | 17–19 s / 6.5–7 s | 19 s / 7–7.5 s |
| Job 2: baseline / salting / members only | 16–16.5 s / 12–12.5 s / 10.5–11 s | 17 s / 13 s / 12 s |
| Job 3: baseline / cache / cache + 8 partitions / one query | 14–15 s / 12 s / 7–8 s / 6 s | 16 s / 13.5 s / 9 s / 7 s |
| Jobs 1, 2, 3 at SF1 (baselines; 4 workers or the small profile) | 7 s / 7 s / 8 s | |

In the simulation every container shares the same 4 cores. It is not a real 4-core laptop: the disk and the memory are still the Mac's.
