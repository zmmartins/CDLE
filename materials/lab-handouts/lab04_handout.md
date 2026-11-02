# CDLE — Lab 4: First Spark Pipelines on Your Laptop

**MapReduce by hand → the same word count with RDDs and DataFrames → partitions and tasks → a pipeline written by an AI, with tests first and checked against the truth**

| | |
|---|---|
| **Duration** | 100 minutes in class, plus take-home work |
| **Groups** | Two students, one laptop running the containers (the other is the backup) |
| **Data** | Web server logs that you generate from `web_sales` of Lab 1 (SF1 and SF10), and `store_sales` SF10 (`$T/parquet/sf1`, `$T/parquet/sf10`) |
| **Tools** | Docker with Compose, Spark 3.5.9 (PySpark) on a standalone cluster of containers, Python 3, DuckDB, pytest, an AI assistant |

## Learning goals

By the end of this lab you should be able to:

- write the map and reduce functions of a job, say what the framework does for you (split, partition, sort, group), and measure the shuffle with and without a combiner;
- write the same word count with RDDs and with DataFrames, and explain the difference in time (opaque Python functions against Catalyst and Tungsten);
- read an `explain()` (partial and final HashAggregate, Exchange), and find jobs, stages and tasks in the Spark UI;
- predict the number of tasks of a read stage from the sizes of the files, and measure the speedup with 1, 2 and 4 executors (take-home);
- show lazy evaluation and lineage: transformations cost nothing until an action, and the work of a lost executor is computed again;
- write a spec and failing tests before asking an AI assistant for code, audit what it returns, and check it against the right answer.

### The cluster

The "sandbox" of the lecture is a Spark **standalone** cluster made of 7 containers on your laptop, the model of slide 11:

```
   client  ── the driver of every job runs here: your script, the SparkSession, the scheduler (UI: localhost:4040)
     │
     ▼
   spark-master  ── the cluster manager: lends the workers' cores to applications (UI: localhost:8080)
     │
     ├── worker1   one executor: 1 core, 768 MB
     ├── worker2   one executor: 1 core, 768 MB
     ├── worker3   one executor: 1 core, 768 MB
     └── worker4   one executor: 1 core, 768 MB

   history  ── the history server: the jobs, stages and tasks of finished applications (UI: localhost:18080)
```

Every container sees the same Docker volume at `/data` (the logs, the outputs): it plays the part of HDFS or a bucket.
Keep in mind that the four "machines" share one laptop, one disk and one memory, and the network between them has almost no delay.

---

## 0. Before the lab (at home, about 30 minutes) — required

There is no time for these steps in class. Come with everything below done.

### 0.1 Check what you need from Lab 1

```bash
export T=~/tpcds
ls -d $T/parquet/sf1/web_sales $T/parquet/sf10/web_sales $T/parquet/sf10/store_sales $T/parquet/sf10/item $T/parquet/sf10/date_dim $T/parquet/sf10/time_dim
```

All six folders must exist. If you do not have the SF10 files, do the take-home part of Lab 1 first.

### 0.2 Docker: 4 CPUs and 8 GB; stop Lab 3

From this lab on, Docker needs **4 CPUs and 8 GB of memory** (a laptop with 16 GB). In Docker Desktop: *Settings → Resources*.
In WSL 2, the memory is set in `%UserProfile%\.wslconfig` (`[wsl2]`, `memory=8GB`, `processors=4`; then `wsl --shutdown`).
Check it:

```bash
docker info | grep -E "CPUs|Total Memory"
```

**A laptop with 8 GB?** Give Docker 4 CPUs and 4 GB, and use the small profile: 2 workers instead of 4, SF1 only.
Everything in this handout works with it, but your times will not be comparable with Appendix A. See [Appendix B](#appendix-b--problems-and-fixes) (small profile).

Free disk space needed: **at least 6 GB** for Lab 4 (the image 1.3 GB, the logs 1.9 GB, a copy of `store_sales` 1.0 GB, the outputs),
**plus the 8 GB of Lab 3** if you have not handed in its report yet: keep its volumes until then.

Lab 3 and Lab 4 both have a container called `client`. Remove the Lab 3 containers; `down` without `-v` keeps their volumes (the object store):

```bash
docker compose -f $T/lab3/compose.yaml down
```

### 0.3 Get the starter pack and the report part

Download `lab4_starter.zip` and `lab4_report_template.zip` from the course page and unzip them in `~/tpcds`:

```bash
cd ~/tpcds
unzip lab4_starter.zip                 # -> ~/tpcds/lab4/
unzip -n lab4_report_template.zip      # -> ~/tpcds/report/ (adds the Lab 4 files; -n never replaces your work)
```

The report part adds to `~/tpcds/report/` the files of the Lab 4 report: `report_lab4.md`, `ai-log-lab4.md`, `evidence/lab4/INDEX.md`, and
`SUBMISSION_lab4.md`, which says what is particular to handing in this lab (read it before the take-home work).
If you do not have the report kit yet (`report_kit.zip`, folder Labs › Lab reports), unzip it in `~/tpcds` too.

| File | What it does | Part |
|---|---|---|
| `make_logs.py` | Writes the web server logs from `web_sales`, and the right answers | 0.4 |
| `check.py` | Compares the output of a job with the right answer: SAME or DIFFERENT | A, B, C |
| `mapreduce.py` | A MapReduce framework in about 150 lines (split, map, partition, sort, reduce) | A |
| `jobs.py` | The map and reduce functions of Part A; you write one | A |
| `logline.py` | Parses one line of the log in plain Python | A, B |
| `hello.py` | Checks the cluster: 4 workers, 4 executors | 0.4 |
| `wordcount_rdd.py`, `wordcount_df.py` | The word count with RDDs and with DataFrames | B |
| `lazy.py` | Five transformations and one action, timed one by one | B.3 |
| `partitions.py` | How many tasks Spark creates to read `store_sales`, and why | B.5 |
| `partition_days.py` | Partitions in folders against row groups (optional) | B.6 |
| `pipeline.py` | **A skeleton**: the AI assistant writes its body | C |
| `tests/` | `test_pipeline.py` (2 examples; you add yours), `sample.log` (38 lines), `conftest.py` | C |
| `audit.py` | The parts of the audit that a script can check | C |
| `ai/session-04.md` | The brief for the AI session | C |
| `compose.yaml`, `compose.small.yaml`, `Dockerfile`, `conf/` | The cluster, and Spark's settings (`conf/spark-defaults.conf`) | |

### 0.4 Build, start, generate the data

```bash
cd ~/tpcds/lab4
docker compose build                       # Java, Spark, DuckDB, pytest: needs the internet; about 470 MB to download
docker compose up -d
docker compose exec -T client spark-submit hello.py
docker compose exec -T client python make_logs.py --sf 1
docker compose exec -T client python make_logs.py --sf 10
docker compose exec -T client cp -r /parquet/sf10/store_sales /data/
```

**Check:** `hello.py` must end with `OK: the cluster is ready`, after the line
`4 executors, 4 cores, 768m per executor; tasks ran on worker1, worker2, worker3, worker4`.
`make_logs.py` must print, for SF1 and SF10:

```
SF1: 1,092,338 lines in 4 files, 169.4 MB, in /data/logs/sf1
SF10: 10,929,705 lines in 16 files, 1,696.0 MB, in /data/logs/sf10
```

Open the three web pages: <http://localhost:8080> (the master: 4 workers ALIVE), <http://localhost:18080>
(the history server: the `hello` application) and, only while a job runs, <http://localhost:4040>.

You may leave the containers running, or stop them with `docker compose stop`. **Do not** run `docker compose down -v`:
`-v` deletes the volume, and with it the logs.

### 0.5 Read

Slides 28–29 of deck 4 (the spec and the audit) and the MapReduce paper, §2–3 (the reading of week 4).

---

## Conventions

Run these lines **in every new terminal** before you start (they work in bash and in zsh). `dc` and `lab` are the shortcuts of Labs 2–3, now for `lab4`; `sub` is new:

```bash
export T=~/tpcds
dc()  { docker compose -f $T/lab4/compose.yaml "$@"; }
lab() { dc exec -T client python "$@"; }
sub() { dc exec -T client spark-submit "$@"; }
```

| Shortcut | Meaning |
|---|---|
| `dc …` | `docker compose` for the lab's containers, from any folder |
| `lab script.py …` | run one of the lab's scripts with Python, inside the `client` container |
| `sub script.py …` | submit a Spark application to the cluster; its driver runs in the `client` container |

Every script also appends what it prints to `~/tpcds/report/evidence/lab4/<script>.txt`: these files are the evidence of your report.

Where things are:

```
~/tpcds/
├── parquet/sf1, sf10/   from Lab 1 (every container sees them as /parquet/sf1, /parquet/sf10)
├── lab4/                the starter pack (every container sees it as /lab, read-only: edit the files on your laptop)
└── report/              the report kit, and evidence/lab4/ (the client sees it as /evidence)

Docker volume lab4_data  (every container sees it as /data)
  logs/sf1, logs/sf10    the access logs        truth/sf1, truth/sf10   the right answers
  store_sales/           the copy for B.5       out/                    the outputs of the jobs
  events/                the event logs that the history server reads
```

**The Spark UI.** The UI of an application (`localhost:4040`) exists only while it runs, and most jobs of this lab
last a few seconds. Use the history server instead: every script prints the link to its application, for example
`http://localhost:18080/history/app-20261102101500-0003/jobs/`. The history server needs a few seconds to show a
finished application; reload the page.

---

## Class plan

| Time | Part |
|---|---|
| 0–10 min | Introduction; the cluster and its three UIs (slide 11) |
| 10–30 min | [Part A](#part-a--mapreduce-by-hand-20-min) — MapReduce by hand |
| 30–65 min | [Part B](#part-b--the-same-job-on-spark-35-min) — The same job on Spark: RDD, DataFrame, plan, failures, partitions |
| 65–90 min | [Part C](#part-c--a-pipeline-with-the-ai-25-min) — A pipeline with the AI: spec and tests first |
| 90–100 min | [Part D](#part-d--discussion-10-min) — Discussion |

Start the containers now and leave them running until the end:

```bash
dc up -d
sub hello.py
```

Before measuring: plug in your laptop, close other heavy applications, and don't use the machine while a measurement runs.

---

## The data: a web server's access logs

`make_logs.py` turned every row of `web_sales` into one request for a product page, in the format of Apache's logs:

```
10.159.194.1 - - [25/Sep/2000:06:28:16 +0000] "GET /product/AAAAAAAAEMJAAAAA?qty=98 HTTP/1.1" 200 5588 "/search?q=belief" "Mozilla/5.0 (...)"
└─ client ─┘     └──────── time ─────────┘  └──────────── request ─────────────────┘ status bytes  └ referer ┘     └ user agent ┘
```

About half of the sales are preceded by a search (`GET /search?q=say+both`), whose words come from the item's description,
and 2% of the lines are health checks of the load balancer (`GET /health`). Like any real log, it is not clean: a few lines
are requests that failed (404), `HEAD` requests, product ids in lower case, user agents with bytes that are not UTF-8,
and lines cut short. Look at a few:

```bash
dc exec client head -n 5 /data/logs/sf1/access-000.log
```

`make_logs.py` also computed the **right answers** from the Parquet tables, without reading the logs:
the count of every search word, of every product, and of every product per day. `check.py` compares a job's output with them.

---

## Part A — MapReduce by hand (20 min)

`mapreduce.py` is the model of the MapReduce paper (§3), on one machine. You give it two functions; it does the rest:

```
 split ──► map task 0 ──► (key, value) ─┬─► part 0 ─┐
 split ──► map task 1 ──► (key, value) ─┼─► part 1 ─┼──► reduce task r: read part r of every map, sort by key,
   …          …                         └─► …       ┘                     call reduce(key, values) once per key
              (optional) combiner: run the reduce on each map's own pairs first
```

The partition of a pair is `crc32(key) mod R`. The tasks run in a pool of 2 processes (the client has 2 CPUs), so 8 map tasks run in 4 waves.

### A.1 Word count (8 min)

Read `map_words` and `reduce_sum` in `jobs.py` (10 lines). Then:

```bash
lab mapreduce.py words --sf 1 --maps 8 --reduces 4
lab check.py words /data/out/mr_words_sf1 --sf 1
```

**Record:**

| Run | Map (s) | Reduce (s) | Lines read | Pairs emitted | Pairs written to the shuffle | Intermediate files | Shuffle (MB) | Keys |
|---|---|---|---|---|---|---|---|---|
| no combiner | | | | | | | | |
| combiner (A.2) | | | | | | | | |

and the top 10 words, and the result of `check.py`.

Questions:

- How many intermediate files are there, and why that number? What would it be with 1,000 maps and 100 reduces?
- Who decides which reduce task gets the word `not`? Could two reduce tasks both produce a count for `not`?
- The most frequent word (`not`) appears about 450 times more often than the median word (Appendix A). What does that do to the reduce task that gets it, when one key is very frequent? (Lab 2: skew.)

### A.2 The combiner (7 min)

```bash
lab mapreduce.py words --sf 1 --maps 8 --reduces 4 --combiner
lab check.py words /data/out/mr_words_sf1 --sf 1
```

Fill in the second row of the table.

Questions:

- By how much did the shuffle shrink? Why does the combiner help so much for this job? (How many distinct words can one map task see?)
- The combiner runs the reduce on part of the values of a key. Why is that correct for a count? Would it be correct for the **mean** of the quantity per product? How would you fix it? (Lab 2, B.3: the wrong merge.)

### A.3 Your own map function (5 min)

Write `map_product` in `jobs.py` (on your laptop: `~/tpcds/lab4/jobs.py`): it counts the views of each product page, that is
`GET` requests with status 200 for `/product/…`, with the product id in upper case. Use `parse()` and `product_id()` from `logline.py`.
`reduce_sum` is the reduce. Then:

```bash
lab mapreduce.py products --sf 1 --combiner
lab check.py products /data/out/mr_products_sf1 --sf 1
```

**Record:** the result of `check.py` (it must be SAME: 9,000 products), and how many lines your map function has.

**The message of Part A:** you write two pure functions, and the framework does the rest. The shuffle is the cost, and the combiner reduces it.

---

## Part B — The same job on Spark (35 min)

### B.1 Word count with RDDs (7 min)

```bash
sub wordcount_rdd.py --sf 1
lab check.py words /data/out/rdd_words_sf1 --sf 1
```

`wordcount_rdd.py` is Part A's job in 4 lines: `textFile → flatMap(words) → map(w → (w, 1)) → reduceByKey(+)`, then `saveAsTextFile`.
The functions it passes to Spark are Python: Spark ships them, and every line, to a Python process next to each executor.

Open the link the script printed (history server), tab **Jobs**, then the job of `saveAsTextFile`, then **Stages**.

**Record:** the time, the number of jobs, and for each stage of the first job: its tasks, its input and its shuffle read or write (MB).

Questions:

- How many stages, and where does Spark cut the job into stages? Which part of Part A does each stage correspond to?
- The script ran a second job (`takeOrdered`, for the top 10). In its page, one stage is marked **skipped**. Why could Spark skip it?
- Compare the shuffle write with the shuffle of A.1 and A.2. Does `reduceByKey` combine before the shuffle?

### B.2 Word count with DataFrames, and its plan (8 min)

```bash
sub wordcount_df.py --sf 1 --explain
lab check.py words /data/out/df_words_sf1 --sf 1
sub wordcount_df.py --sf 10
sub wordcount_rdd.py --sf 10
```

`wordcount_df.py` does the same with DataFrame operations: `regexp_extract`, `split`, `explode`, `groupBy("word").count()`.
No Python runs on the executors.

**Record:**

| Version | SF1 (s) | SF10 (s) | Stages | Shuffle write, SF1 (MB) |
|---|---|---|---|---|
| RDD | | | | |
| DataFrame | | | | |

Questions:

- In the plan of `--explain`, find the two `HashAggregate` and the `Exchange`. Which is Part A's combiner, which is the reduce, and which is the shuffle?
- The `Exchange` says `hashpartitioning(word, 200)`. Where does 200 come from? How many tasks did the stage after it really have (UI)? (AQE: the plan says `isFinalPlan=false`.)
- Why is the RDD version slower? Name the two costs it pays that the DataFrame version does not.
- The regular expression appears **twice** in the plan. Why? What would you change to evaluate it once?

### B.3 Lazy evaluation (4 min)

```bash
sub lazy.py --sf 1
```

`lazy.py` builds the count of product page views in five transformations, then calls `count()`. It prints the time of each line and the number of jobs Spark has started so far.

**Record:** the time of each of the 6 lines, and the number of jobs after each one.

Questions:

- When does the first job appear? What did the five lines before it do?
- How many jobs did `count()` start, and why more than one? (Look at the jobs in the UI: the first ends at the shuffle. Adaptive query execution waits for it before planning the rest.)
- Check the result against the right answer: it is the total of `products` (`lab check.py products …` prints it).

### B.4 Losing an executor (7 min)

First the reference: the same word count with 16 input partitions and a 3 s pause in every map task, so that the job lasts long enough:

```bash
sub wordcount_rdd.py --sf 1 --slow 3 --partitions 16
```

Now again, and kill `worker2` 12 seconds after the start, when some of its tasks have already finished:

```bash
sub wordcount_rdd.py --sf 1 --slow 3 --partitions 16 &
sleep 12; docker kill worker2
wait
lab check.py words /data/out/rdd_words_sf1 --sf 1
dc up -d worker2
```

**Record:** the time with and without the kill; the result of `check.py`; in the UI (the stages of the first job): which tasks failed, and which ran again, on which executors.

Questions:

- Did the job finish, with the right result? No partition had a copy. How did Spark rebuild what `worker2` had computed? (Lineage: the RDD knows the recipe.)
- Some tasks that had **finished** on `worker2` ran again (attempt 1 in the task list of the first stage). Why? (Where were their map outputs stored?)
  If none ran again, you killed it too early: run the block again with `sleep 15`.
- Compare with Lab 2, C.1 (a worker that does not answer) and Lab 3, C.2 (a node that is down): what did each system keep so that it could survive?

### B.5 Partitions and tasks (9 min)

`partitions.py` reads `store_sales` SF10 (the 16 Parquet files of Lab 1, 240 row groups; you copied them to `/data/store_sales` in 0.4)
and computes `sum(ss_net_paid)`. Spark decides the number of tasks of the read when it plans the job, from the **bytes**:

```
maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, (bytes of all files + files × openCostInBytes) / cores))
```

with `maxPartitionBytes` = 128 MiB and `openCostInBytes` = 4 MiB by default. Files are cut into pieces of at most
`maxSplitBytes`, and pieces are packed into partitions, each counting 4 MiB more for every piece it holds. A row group is never cut:
it is read by the task whose piece holds its middle.

**First predict**, with the file sizes that `partitions.py` prints on its first line: how many tasks will read the table with 128 MiB? With 32 MiB? With 512 MiB?
Then measure:

```bash
sub partitions.py --max-partition-mb 128
sub partitions.py --max-partition-mb 32
sub partitions.py --max-partition-mb 512
sub partitions.py --repartition 8
```

| `maxPartitionBytes` | Your prediction | Spark's rule (printed) | Planned partitions | Partitions with rows | Rows per partition (min / max) | T (s) |
|---|---|---|---|---|---|---|
| 128 MiB | | | | | | |
| 32 MiB | | | | | | |
| 512 MiB | | | | | | |

Questions:

- With 128 MiB there are fewer tasks than files. How can one task read two files?
- With 32 MiB, one partition gets no rows. How is that possible? (Think of the row groups and their middle.)
- With 512 MiB, the rule gives a split of about 270 MiB, not 512. Why does Spark refuse to make fewer tasks than cores?
- The 240 row groups are an upper bound of the parallelism of this table. Lab 3 asked how many tasks could read it on 200 cores: answer again, with the rule.
- `--repartition 8` repartitions by `ss_store_sk`. Find the `Exchange` in the plan. The `groupBy("ss_store_sk")` after it has **no** second Exchange: why? (Lab 2: "the key you partition by decides the plan".)

**The message of Part B:** the same idea as MapReduce, but the DataFrame lets Spark see the whole job and optimise it; the plan and the UI are your debugger.

### B.6 Partitions in folders — optional (at home)

```bash
sub partition_days.py
```

It writes the SF10 product page views three times in Parquet: `plain` (in the order of the logs, which is time order), `shuffled`
(random order) and `by_day` (`partitionBy("day")`: one folder per day), and runs one query for March 2002 on each.

**Record:** files and MB of each copy, and for the query: rows, time, `PartitionFilters` and `PushedFilters`, and in the UI (tab **SQL**, the scan node) "number of files read" and "size of files read".

Questions: Which copy reads the fewest bytes, and how does each one skip data (folders against min/max in footers, Lab 3)? Why is `shuffled` almost four times the size of `plain`? What does `by_day` cost (count its files and their size): what would one folder per hour do?

---

## Part C — A pipeline with the AI (25 min)

The task is the one of slides 28–29: **daily views of each product page**, from the logs, with the DataFrame API.
The order matters, and it is the point: **spec, then tests, then code, then audit**. Follow `ai/session-04.md`.

### C.1 The spec and the tests, before any code (8 min)

1. Complete the brief in `ai/session-04.md`: every `<...>`. Look at `tests/sample.log` (38 lines from the real logs) to find the kinds of dirty lines.
2. Add **at least 4 tests** to `tests/test_pipeline.py`: a line cut short, a line with bytes that are not UTF-8,
   a product id in lower case, a request that failed (404). Two examples are there already.
3. Run them. With the skeleton of `pipeline.py`, **every test must fail**:

```bash
lab -m pytest
```

**Record:** your tests (names, and what each one checks), and the output of pytest.

### C.2 Generate (7 min)

Give the assistant the brief, your tests and `pipeline.py`. Put its `pipeline.py` in `~/tpcds/lab4/pipeline.py`, unchanged, and run:

```bash
lab -m pytest
```

**Record:** the assistant and its version, how many tests pass, and the numbered assumptions it wrote.

### C.3 Audit (10 min)

```bash
lab audit.py
sub pipeline.py --sf 1
lab check.py product_day /data/out/pipeline_sf1 --sf 1
```

The checklist (✓ or ✗, with one line of evidence each):

| # | Check | ✓ / ✗ | Evidence |
|---|---|---|---|
| 1 | All tests pass, and every test checks something (no test that can never fail) | | |
| 2 | No `collect()`, `toPandas()`, `take()`, `.rdd` or Python UDF in the function (`audit.py`) | | |
| 3 | The plan aggregates before the shuffle: `partial_count` below the `Exchange` (`audit.py`) | | |
| 4 | No API that does not exist in Spark 3.5, no parameter it ignores | | |
| 5 | Each numbered assumption of the assistant: true for these logs? (one line each) | | |
| 6 | The day does not depend on the session time zone (what does the code do with `+0000`?) | | |
| 7 | `check.py` at SF1: SAME. If DIFFERENT: which keys, and which kind of line explains them? | | |
| 8 | The two risks the assistant named: real for these logs? | | |

If something fails, fix the spec, the tests or the code (say which, in the AI log) and run the checks again.

**The message of Part C:** the draft takes 30 seconds; the audit takes 10 minutes, and it is the work. Passing tests are not enough:
the right answer catches what the tests did not foresee, and sometimes a test is wrong too.

---

## Part D — Discussion (10 min)

- Is MapReduce dead? (The system is legacy; where does its model live on, in what you ran today?)
- RDDs or DataFrames: when would you write an RDD job today?
- Slide 2: 50 TB of logs on 400 machines. With the rule of B.5, how many tasks would read them, and how many waves on 400 × 8 cores?
- What did your tests catch, and what did only `check.py` catch?

Leave the containers as they are if you continue at home. To stop them:

```bash
dc stop        # stops the containers; the volume (logs, outputs) is kept
```

---

## Take-home work

**Deadline:** Sunday 15 November 2026, 23:59 (see the course page on Moodle). Write, check and hand in the report with the report kit
(`report_kit.zip`) and this lab's report part (`lab4_report_template.zip`): follow `SUBMISSION.md` in the kit, with `lab4` in place of `labN`,
and `SUBMISSION_lab4.md` (in `~/tpcds/report/`) for what is particular to Lab 4: the evidence the scripts write for you, and the code `pack.py` takes.

1. **Finish the tables** of Parts A, B and C, and the audit of C.3 if you did not finish it in class.
2. **Scaling** (median of 3 runs each; about 15 minutes):
   ```bash
   dc up -d                     # if you stopped the cluster
   for c in 1 2 4; do for r in 1 2 3; do
     sub --total-executor-cores $c wordcount_df.py --sf 10
     sub --total-executor-cores $c wordcount_rdd.py --sf 10
   done; done
   lab mapreduce.py words --sf 10
   lab mapreduce.py words --sf 10 --combiner
   ```
   | Version | 1 executor (s) | 2 executors (s) | 4 executors (s) | Speedup 4 | Serial fraction (Amdahl, Lab 1) |
   |---|---|---|---|---|---|
   | RDD, SF10 | | | | | |
   | DataFrame, SF10 | | | | | |

   and, for `mapreduce.py` at SF10 with and without the combiner: time and shuffle MB. Is the speedup of 4 executors close to 4? What part of the job does not scale (look at the stages in the UI)?
3. **B.6** (optional): the table and the questions.
4. **AI session 04:** write it in `ai-log-lab4.md` (template in the report kit), entry *Session 04*: the brief as sent, the answer, the audit table, and what you changed.
   Optional: ask the assistant to "break the pipeline in a subtle way" (slide 31) and see whether your tests or `check.py` catch it.
5. **One figure:** the speedup of RDD and DataFrame at SF10 with 1, 2 and 4 executors, with the ideal line.
6. **Clean up** when your report is handed in: `dc down -v` removes the containers and the volume (about 3 GB); `docker rmi cdle-lab4` removes the image (1.3 GB).

### Report (maximum 4 pages, PDF, in the format of the report kit)

1. **Machine and Docker:** the machine table of Lab 1, the CPUs and memory given to Docker, and the profile (4 workers, or the small one).
2. **Part A:** the table of A.1–A.2 at SF1 and SF10; the combiner and the mean; your `map_product` (only its lines).
3. **Part B:** B.1–B.2 (stages, shuffle, RDD against DataFrame, the plan); B.3; B.4 (what was run again, and why); B.5 (your predictions and the measurements, explained with the rule); the scaling table and the figure.
4. **Part C:** your tests; the assistant's assumptions; the audit table; what the tests caught, and what only `check.py` caught.
5. **Limits:** two results of this lab that you would not trust on a real cluster of 400 machines, and why.

`pack.py` also puts in the zip your `pipeline.py`, your `tests/` and your `jobs.py` (see `SUBMISSION_lab4.md`). Be ready to explain any number of your report in class.

### Rules for TPC-DS material

The rules of Lab 1 still apply: do not publish the kit, the generated data (the logs are generated data too) or the query files,
and do not present your numbers as TPC-DS results.

---

## Appendix A — Expected values

Sizes and counts come from the instructor's run and must match yours exactly (the logs are deterministic). Times will not match.

**Logs:**

| | Lines | Files | MB | Product pages | Searches | Health checks | Views to count (GET, 200, complete) |
|---|---:|---:|---:|---:|---:|---:|---:|
| SF1 | 1,092,338 | 4 | 169.4 | 719,195 | 358,684 | 14,459 | 708,400 |
| SF10 | 10,929,705 | 16 | 1,696.0 | 7,195,778 | 3,589,810 | 144,117 | 7,087,642 |

Dirty product lines at SF1: 404 7,230; HEAD 2,153; id in lower case 3,500; not UTF-8 1,410; cut short 1,412.

**Part A** (SF1): 6,175 distinct words, 699,018 words in all; the top 3 are `not` 12,784, `a` 11,105, `to` 8,624; the median word appears 28 times.
Without combiner, 699,018 pairs and 6.0 MB go through the shuffle; with it, 36,997 pairs and 0.4 MB.
`products`: 9,000 products, 708,400 views (with combiner: 71,998 pairs, 1.4 MB).
At SF10: 9,576 words, 6,998,396 in all.

**Part B:** RDD word count: 2 stages (the map side and `reduceByKey`), 8 tasks each at SF1, 64 at SF10.
`lazy.py`: 0 jobs until `count()`, then 2; the count is 708,400.
`partitions.py`: 16 files, 1,018.6 MiB (61.5 to 65.7 MiB each), 240 row groups; `sum(ss_net_paid)` = 47364095902.84.

| `maxPartitionBytes` | maxSplitBytes | Planned partitions | Partitions with rows |
|---|---:|---:|---:|
| 128 MiB | 128.0 MiB | 14 | 14 |
| 32 MiB | 32.0 MiB | 33 | 32 |
| 512 MiB | 270.7 MiB | 5 | 5 |

B.6: `plain` 16 files, 25.7 MB; `shuffled` 5 files, 95.6 MB; `by_day` 1,823 files, 52.1 MB; March 2002 has 65,486 views.

**Part C:** `tests/sample.log` holds 17 (product, day) pairs and 18 views to count. At SF1, `product_day` has 688,933 rows; at SF10, 6,748,794.

---

## Appendix B — Problems and fixes

| Problem | Symptom | Fix |
|---|---|---|
| Docker is not running | `Cannot connect to the Docker daemon` | Start Docker Desktop (or `sudo systemctl start docker` on Linux) |
| Lab 3 is still there | `container name "/client" is already in use` | `docker compose -f $T/lab3/compose.yaml down` (without `-v`: its volumes stay) |
| A port is taken | `port is already allocated` on `dc up` (8080, 18080 or 4040) | Stop the program that uses it, or change the left-hand number in `compose.yaml` |
| The shortcuts are missing, or point to Lab 3 | `command not found: sub`, or `no such service` | Run the lines of [Conventions](#conventions) in this terminal |
| No Lab 1 data | `folder not found: /parquet/sf10/web_sales` | Your Parquet files must be in `~/tpcds/parquet/sf10` and the starter pack in `~/tpcds/lab4` |
| No logs | `no log files in /data/logs/sf1` | `lab make_logs.py --sf 1` (and `--sf 10`) |
| No copy of `store_sales` | `no files in /data/store_sales` | `dc exec client cp -r /parquet/sf10/store_sales /data/` |
| Fewer than 4 workers | `hello.py` prints NOT OK; the master page shows a worker DEAD | `dc up -d` (after B.4: `dc up -d worker2`) |
| A job waits forever: `Initial job has not accepted any resources` | the cores are taken by another application | Only one Spark job at a time: wait for the other, or stop it with Ctrl-C |
| An executor is killed with "Out of memory", or Docker kills a container (exit 137) | Docker has less than 8 GB | Check `docker info`; use the small profile |
| The history server does not show an application | the link gives "Application not found" | Wait 5 seconds and reload; applications that crashed show under "Show incomplete applications" |
| The links to the executors' logs in the UI do not open | `worker1:8081` cannot be reached | Normal: the workers' pages are not published. Use `dc logs worker1` |
| `pipeline.py` changes are not seen | the old code still runs | Edit `~/tpcds/lab4/pipeline.py` on your laptop, not in the container (`/lab` is read-only) |
| Not enough disk space | `No space left on device` | Free space; remove `/data/views` after B.6 (`dc exec client rm -rf /data/views`) |
| You want to start again from nothing | | `dc down -v`, then section 0.4 again |

**The small profile (8 GB laptops).** Start it with both compose files, and use this `dc` in every terminal.
If you had started the 4-worker cluster before, remove its containers first (`docker compose -f $T/lab4/compose.yaml down`):
the small profile does not see `worker3` and `worker4`, and would leave them running.

```bash
dc() { docker compose -f $T/lab4/compose.yaml -f $T/lab4/compose.small.yaml "$@"; }
dc up -d
sub hello.py           # 2 workers, 2 executors: OK
```

Do everything at SF1 (skip `make_logs.py --sf 10` and the SF10 runs; B.5 still reads the SF10 `store_sales`, which is fine);
in B.4 kill `worker2` all the same: one executor is left, and the job takes about a minute. In the scaling table use 1 and 2 executors at SF1.
Say in the report that you used it.

If you hit a problem that is not in this table, write down the exact error message and ask.

---

## Appendix C — Typical durations

Measured on the instructor's laptop (Apple M3 Max, Docker with 16 CPUs and 8 GB), and in a simulation of a 4-core laptop
(all containers on the same 4 cores, Docker with 8 GB). The containers are capped (each worker 1 CPU, the client 2), so a 4-core laptop should be
only somewhat slower. The times are those the scripts print; every `sub` also takes 4–8 s to start Spark.

| Step | Instructor's laptop | Simulated 4-core laptop |
|---|---|---|
| `docker compose build` (first time, after the download of about 470 MB) | about 30 s | |
| `make_logs.py --sf 1` / `--sf 10` | 3 s / 37 s | 3 s / 41 s |
| `mapreduce.py words --sf 1` (no combiner / combiner) | 1.1 s / 0.9 s | 1.1 s / 0.9 s |
| `wordcount_rdd.py` SF1 / SF10 | 3.0 s / 11.2 s | 3.1 s / 11.5 s |
| `wordcount_df.py` SF1 / SF10 | 4.1 s / 7.9 s | 4.6 s / 8.1 s |
| `lazy.py`: the `count()` | 3.1 s | 3.2 s |
| B.4: reference / with `worker2` killed | 15.2 s / 21.5 s | 15.5 s / 21.7 s |
| `partitions.py`, one run (with Spark's start) | 11 s | 13 s |
| `pytest`, 8 tests | 6 s | 6 s |
| `pipeline.py` SF1 (the instructor's AI version) | 6.2 s | 6.6 s |
| `partition_days.py` (B.6, first time, with Spark's start) | 74 s | 75 s |
| Scaling loop of the take-home work (18 runs) and `mapreduce.py` at SF10 | 7 min | 8 min |

In the simulation every container shares the same 4 cores. It is not a real 4-core laptop: the disk and the memory are still the Mac's.
