# CDLE — Lab 3: Storage for Scale on Your Laptop

**Files and formats → what a query really reads → an erasure-coded object store that loses nodes**

| | |
|---|---|
| **Duration** | 100 minutes in class, plus take-home work |
| **Groups** | Two students, one laptop running the containers (the other is the backup) |
| **Data** | `store_sales` from Lab 1: SF1 for Part A, SF10 for Parts B and C (`$T/parquet/sf1`, `$T/parquet/sf10`) |
| **Tools** | Docker with Compose, Python 3, DuckDB, a small object store written for this lab |

## Learning goals

By the end of this lab you should be able to:

- measure the size and the read time of one table in CSV, JSON, Avro and Parquet (snappy and zstd), and explain the differences;
- read the footer of a Parquet file (row groups, column sizes, encodings, min/max) and predict the bytes a query will read;
- show the effect of reading fewer columns, and explain why the sort order of the data decides which row groups can be skipped;
- explain what a k+m erasure code costs and buys compared with 3 replicas: space, failures survived, read and repair work;
- tell a lost node from a frozen one in an object store, and say what a timeout costs;
- check an AI assistant's claims about Dynamo and Bigtable against the papers (take-home).

### How this object store differs from a real one

The "bucket" of the lecture is an object store made of 7 containers on your laptop: a front-end and six
storage nodes, with the 4+2 erasure code of slides 9–10 (the slides show 6+3; the idea is the same).
It speaks plain HTTP, and DuckDB reads Parquet from it exactly as it would from S3: with `GET`
requests for byte ranges. Its front-end counts every byte it serves. Keep in mind that:

- the nodes share one machine, one disk and one memory, and the network between them has almost no delay;
- it is about 600 lines of Python: its speed is not the speed of S3, but the bytes it reads are the bytes any object store would read.

---

## 0. Before the lab (at home, about 30 minutes) — required

There is no time for these steps in class. Come with everything below done.

### 0.1 Check what you need from Lab 1

```bash
export T=~/tpcds
ls $T/parquet/sf1/store_sales $T/parquet/sf10/store_sales $T/parquet/sf10/store_returns | head
```

All three folders must exist. If you do not have the SF10 files, do the take-home part of Lab 1 first.

Free disk space needed: **at least 12 GB** (the Docker image, 2.4 GB of files in five formats,
about 5 GB in the object store: three copies of `store_sales`, each stored 1.5 times, and up to 2.8 GB
of temporary files while `load.py` sorts).

### 0.2 Docker, and stop Lab 2

You installed Docker in Lab 2. Docker still needs **at least 4 CPUs and 4 GB of memory** (Lab 2, section 0.2).

Lab 2 and Lab 3 both have a container called `client`, so stop the Lab 2 cluster first. Its data stays in its volume:

```bash
docker compose -f $T/lab2/compose.yaml down
```

### 0.3 Get the starter pack

Download `lab3_starter.zip` from the course page and unzip it in `~/tpcds`, so that the files end up in `~/tpcds/lab3/`:

| File | What it does | Runs |
|---|---|---|
| `formats.py` | Writes `store_sales` in the five configurations, records time and size | in the `client` container |
| `read_bench.py` | Times a query over those files, or over Parquet objects in the object store | in the `client` container |
| `pq_inspect.py` | Reads Parquet footers: row groups, column sizes, min/max, what a filter can skip | in the `client` container |
| `load.py` | Puts a copy of `store_sales` (as it is, sorted, or rewritten) in the object store | in the `client` container |
| `obj_client.py` | The object store's command-line client: status, put, get, verify, repair… | in the `client` container |
| `obj_frontend.py` | The object store's front-end: splits objects into chunks, encodes, serves byte ranges | container `obj` |
| `obj_shard.py` | One storage node: keeps chunks on its own disk | containers `s1` … `s6` |
| `ec.py` | The Reed-Solomon 4+2 code over GF(256), about 60 lines | |
| `compose.yaml`, `Dockerfile` | The containers: front-end, 6 storage nodes, 1 client | |
| `ai/session-03.md` | The brief for the AI session (take-home, Lab 3.2) | |

### 0.4 Build, start, and load the object store

```bash
cd ~/tpcds/lab3
docker compose build              # downloads Python, DuckDB and numpy: needs the internet, takes a few minutes
docker compose up -d
docker compose exec client python obj_client.py status
docker compose exec client python load.py --name sf10
docker compose exec client python load.py --name sorted --sort ss_sold_date_sk
docker compose exec client python obj_client.py status
```

The first `load.py` copies the 16 SF10 files of `store_sales` from Lab 1 into the object store. The second
writes a new copy of the same table, sorted by date, and stores it too. Together they take about a minute.

**Check:** the last `status` must show the six shards `UP` and these two lines (the size of the sorted
copy may differ by a few hundred KB from one run to another):

```
  lake/sf10/store_sales/                     16 objects    1,068.1 MB
  lake/sorted/store_sales/                   17 objects    1,153.6 MB
```

You may leave the containers running, or stop them with `docker compose stop`. **Do not** run
`docker compose down -v`: `-v` deletes the volumes, and with them the object store.

If something fails, see [Appendix B](#appendix-b--problems-and-fixes).

### 0.5 Read

The week-3 reading list: Dynamo §4 and Bigtable §5–6. You will need them for the take-home AI session.

---

## Conventions

Run these lines **in every new terminal** before you start (they work in bash and in zsh). The names are
the same as in Lab 2, but they now point to `lab3`:

```bash
export T=~/tpcds
dc()  { docker compose -f $T/lab3/compose.yaml "$@"; }
lab() { dc exec -T client python "$@"; }
obj() { lab obj_client.py "$@"; }
```

| Shortcut | Meaning |
|---|---|
| `dc …` | `docker compose` for the lab's containers, from any folder |
| `lab script.py …` | run one of the lab's scripts inside the `client` container |
| `obj …` | the object store's client (`obj status`, `obj du`, `obj verify`, …) |

Where things are:

```
~/tpcds/
├── parquet/sf1, sf10/   from Lab 1 (the client container sees them as /parquet/sf1, /parquet/sf10)
└── lab3/                the starter pack (every container sees it as /lab)

Docker volume lab3_data       the files of Part A (the client sees it as /data)
Docker volumes lab3_s1 … s6   the "disks" of the six storage nodes
Docker volume lab3_meta       the front-end's index: key → size, chunk size, shards
```

---

## Class plan

| Time | Part |
|---|---|
| 0–10 min | Introduction; start the containers; the AI session of the take-home work |
| 10–40 min | [Part A](#part-a--formats-30-min) — Formats |
| 40–65 min | [Part B](#part-b--what-a-query-reads-25-min) — What a query reads |
| 65–90 min | [Part C](#part-c--erasure-coding-and-failures-25-min) — Erasure coding and failures |
| 90–100 min | [Part D](#part-d--discussion-10-min) — Discussion |

Start the containers now and leave them running until the end:

```bash
dc up -d
obj status
```

Before measuring: plug in your laptop, close other heavy applications, and don't use the machine while a measurement runs.

---

## Part A — Formats (30 min)

All of Part A uses `store_sales` at **SF1**: 2,880,404 rows and 23 columns (integers and decimals).
At SF10 the JSON file alone would be about 15 GB.

### A.1 Write the five configurations (8 min)

```bash
lab formats.py --sf 1
```

`formats.py` loads the table into memory, then writes it five times into `/data/formats`:
CSV with a header line, JSON (one object per line), Avro, and Parquet with two codecs. The time measured
is only the time to encode and write each file.

**Record:**

| Configuration | Write (s) | Size (MB) | Size / CSV |
|---|---|---|---|
| CSV | | | 1.00 |
| JSON | | | |
| Avro | | | |
| Parquet, snappy | | | |
| Parquet, zstd | | | |

Questions:

- Why is JSON about four times the size of CSV, for the same rows? Look at one line: `dc exec client head -c 600 /data/formats/store_sales.json`.
- Avro is binary and writes the schema only once, yet it is more than twice the size of Parquet. What does Parquet do that a row format cannot?
- Which configuration is the slowest to write? Is the cheapest to write also the cheapest to store?

### A.2 One column, cold and warm (10 min)

```bash
lab read_bench.py formats --query sum1 --cold
lab read_bench.py formats --query sum1
```

`sum1` is `SELECT sum(ss_net_paid)`: one column out of 23, every row. Each run is a new process with a
new DuckDB, using 2 threads. `--cold` first asks Linux to drop the file from its page cache. Without it,
one run is made first and not counted, so the file is in memory. The last column, `result`, is a hash of
the answer. It must be the same for the five files, and the script checks it.

**Record** (median of 3 runs):

| Configuration | Cold (s) | Warm (s) | CPU, warm (s) | MB/s, warm |
|---|---|---|---|---|
| CSV | | | | |
| JSON | | | | |
| Avro | | | | |
| Parquet, snappy | | | | |
| Parquet, zstd | | | | |

Questions:

- With 2 threads, CPU time close to 2 × the time means both threads were busy. Which readers use both? Which one uses only one, and what does that do to its time?
- For which files is the difference between cold and warm large, and for which is it small? What dominates the time of a CSV or JSON read: reading the bytes, or turning text into numbers?
- Parquet answers about fifty times faster than CSV. Give two reasons, and check them against the sizes of A.1.
- **What "cold" means here.** On macOS and Windows, Linux runs in a virtual machine, and its disk is a
  file on your real disk. The host may still have that file in its own cache. "Cold" is then "not in
  Linux's memory", which is still very fast. On a Linux laptop it is the SSD. Keep this in mind in your report.

### A.3 More columns (7 min)

```bash
lab read_bench.py formats --query agg5
lab read_bench.py formats --query all23
```

`agg5` groups by store and aggregates 4 more columns (5 in all). `all23` reads every column of every row.

**Record** (warm, median of 3; use your `sum1` value of A.2):

| Configuration | sum1 (s) | agg5 (s) | all23 (s) | all23 / sum1 |
|---|---|---|---|---|
| CSV | | | | |
| JSON | | | | |
| Avro | | | | |
| Parquet, snappy | | | | |
| Parquet, zstd | | | | |

Questions:

- How does the gap between CSV and Parquet change when the query reads 1, 5 and 23 columns? Why?
- CSV, JSON and Avro take almost the same time for 1 and 23 columns. What do they have to do for every row, whatever the query asks?
- `all23` also computes 23 hashes per row. How would you check how much of its time is the hashing and how much is the reading? (You do not need to do it.)

### A.4 snappy or zstd? (5 min)

There are no new commands: use your tables.

- How much smaller is zstd than snappy, and what does it cost to write and to read (time and CPU)?
- Suppose each file must first be downloaded whole from an object store that delivers 100 MB/s. Estimate, for each codec, the download time plus your measured `all23` time. Which codec wins? Which wins on your SSD?
- Which configuration would you choose for the project's data lake, and what measurement would change your mind?

---

## How the object store works

```
            DuckDB (client)          GET lake/sf10/store_sales/data_0.parquet   Range: bytes=1000-2999999
                 │
                 ▼
     ┌──────── obj (front-end) ────────┐   index: key → size, chunk size, which shards miss it
     │  splits, encodes, reassembles   │
     └──┬─────┬─────┬─────┬─────┬─────┬┘
        s1    s2    s3    s4    s5    s6
       data  data  data  data parity parity
```

- An object is cut into **stripes** of 4 **chunks** of at most 1 MiB. For each stripe, `ec.py` computes 2
  parity chunks. The 4 data chunks go to `s1`–`s4` and the 2 parity chunks to `s5`–`s6`. **Any 4 of the 6
  chunks of a stripe rebuild the other 2.**
- A read with all nodes up gets each byte range from the data shards. Nothing needs decoding.
- If a data shard does not answer, the front-end fetches the same range from 4 other shards and rebuilds the missing bytes.
- A write is accepted when **5 of the 6** shards have stored their chunk. The shards that missed it are recorded, for `obj repair`.

The front-end counts every request. `read_bench.py obj …` reads its counters before and after each run,
and `obj log` shows the last requests it served.

---

## Part B — What a query reads (25 min)

You loaded two copies of the SF10 `store_sales` at home (section 0.4):

| Copy | What it is |
|---|---|
| `sf10/store_sales` | the 16 files of Lab 1, as they are |
| `sorted/store_sales` | the same rows, sorted by `ss_sold_date_sk` |

### B.1 Read the footer (7 min)

A Parquet file ends with a footer that describes it: the row groups, and for every column of every row
group its offset, its size, its encodings and the min and max of its values. `pq_inspect.py` reads only the footers.

```bash
lab pq_inspect.py sf10/store_sales
lab pq_inspect.py sf10/store_sales --column ss_sold_date_sk
lab pq_inspect.py sorted/store_sales --column ss_sold_date_sk
```

**Record:**

| | `sf10` | `sorted` |
|---|---|---|
| Files, row groups, median rows per group | | |
| MB of `ss_sold_date_sk` / of `ss_net_paid` (compressed) | | |
| The three largest columns | | |
| Median width of [min, max] of `ss_sold_date_sk`, per row group | | |

Questions:

- `ss_sold_date_sk` and `ss_net_paid` are both one number per row. Why is one about ten times smaller than the other? (Look at the encodings, and think of how many distinct dates there are.)
- In `sf10`, every row group holds dates from the first to the last day. What does that mean for a query on one month?
- Some queries could be answered from the footers alone, without reading any column data. Give two.

### B.2 One column or all of them (5 min)

```bash
lab read_bench.py obj sf10/store_sales --query sum1
lab read_bench.py obj sf10/store_sales --query all23
```

**Record** (median of 3):

| Query | T (s) | GET requests | MB sent to DuckDB | MB of those columns in the footers (B.1) |
|---|---|---|---|---|
| `sum1` | | | | |
| `all23` | | | | |

Questions:

- Compare the MB that DuckDB fetched for `sum1` with the size of `ss_net_paid` in the footers. What is the small difference made of? (`obj log -n 30` shows the last requests.)
- Count: 16 files, 240 row groups. Explain the number of GET requests. Why is it the **same** for 1 and for 23 columns?
- `all23` reads about 12 times more bytes than `sum1`. Is it 12 times slower? Where does its time go? (Compare with A.3.)

### B.3 Sorted or not: predict, then measure — required (10 min)

The query `month` reads two columns for one month (March 2002, 0.9% of the rows):

```sql
SELECT count(*), sum(ss_net_paid) FROM store_sales WHERE ss_sold_date_sk BETWEEN 2452335 AND 2452365
```

**First predict**, from the footers only, how many row groups DuckDB can skip and how many MB it must read:

```bash
lab pq_inspect.py sf10/store_sales   --between ss_sold_date_sk 2452335 2452365 --columns ss_net_paid
lab pq_inspect.py sorted/store_sales --between ss_sold_date_sk 2452335 2452365 --columns ss_net_paid
```

Write your prediction in the table. **Then** measure:

```bash
lab read_bench.py obj sf10/store_sales   --query month
lab read_bench.py obj sorted/store_sales --query month
obj log -n 45
```

| Copy | Row groups that may match | Predicted MB | Measured MB to DuckDB | GETs | T (s) | Result |
|---|---|---|---|---|---|---|
| `sf10` | | | | | | |
| `sorted` | | | | | | |

Questions:

- Was your prediction right? On `sorted`, the measured MB are larger than predicted. Use `obj log`: what are the other requests, and how many bytes does each one fetch?
- The two results must be identical. What would it mean if they were not?
- The sorted copy is about 8% **larger** than the original (B.1 and `obj status`). Which columns grew? (Compare the column tables: `lab pq_inspect.py sorted/store_sales`.) Why does sorting by date make them compress worse?
- A query that filters on `ss_customer_sk` gains nothing from this copy. How many sort orders can one copy have? What does a data lake do when two kinds of query matter? Relate this to Lab 2: "the key you partition by decides the plan".

### B.4 Row-group size — if time allows (required at home) (3 min)

```bash
lab load.py --name sorted_rg1m --sort ss_sold_date_sk --row-group-size 1000000
lab pq_inspect.py sorted_rg1m/store_sales --between ss_sold_date_sk 2452335 2452365 --columns ss_net_paid
lab read_bench.py obj sorted_rg1m/store_sales --query month
lab read_bench.py obj sorted_rg1m/store_sales --query sum1
```

| Copy | Row groups | Rows per group | `month`: MB / GETs / T (s) | `sum1`: MB / GETs / T (s) |
|---|---|---|---|---|
| `sorted` (≈123,000) | | | | |
| `sorted_rg1m` | | | | |

Questions:

- What do larger row groups gain, and what do they lose? For which kind of query?
- Next week, Spark gives each row group (or each file) to a different task. How many tasks could read each copy at the same time? What happens on a cluster with 200 cores?

---

## Part C — Erasure coding and failures (25 min)

### C.1 The space it takes (4 min)

```bash
obj du
```

**Record:** logical MB, MB stored on all shards, ratio, and the MB that 3 replicas would take.

Questions:

- How many lost shards does 4+2 survive? And 3 replicas? Which one keeps more data per byte of disk for the same protection?
- The slides use 6+3 (slide 10). What are its ratio and the number of failures it survives? Why would a cloud provider go further, to 10+4 or more? What does a larger k cost on every read when a shard is missing (see C.2)?

### C.2 Losing nodes — required (9 min)

`obj verify` reads every object whole and compares its MD5 with the one recorded when it was written.
First take the reference, with all nodes up:

```bash
obj verify sf10/store_sales
```

Then kill one data node, then one parity node, then one more. After each `docker kill`, run the two commands again:

```bash
docker kill s2
obj verify sf10/store_sales
lab read_bench.py obj sf10/store_sales --query sum1

docker kill s5
obj verify sf10/store_sales
lab read_bench.py obj sf10/store_sales --query sum1

docker kill s6
obj verify sf10/store_sales
lab read_bench.py obj sf10/store_sales --query sum1 --runs 1
```

Then bring the three nodes back and check again:

```bash
dc up -d s2 s5 s6
obj status
obj verify sf10/store_sales
```

| Nodes down | `verify`: OK / failed | `verify`: T (s) | MB read from shards | MB rebuilt | `sum1`: T (s) | `sum1`: result |
|---|---|---|---|---|---|---|
| none | | | | | (B.2) | |
| `s2` | | | | | | |
| `s2`, `s5` | | | | | | |
| `s2`, `s5`, `s6` | | | | | | |
| none again | | | | | | |

Questions:

- With `s2` down, `verify` delivers the same 1,068 MB but reads far more from the shards. Derive the exact number from the size of what `s2` holds.
- Killing `s5` as well changes almost nothing. Why? Real object stores rotate the parity chunks over all the nodes. What would that change in your table, and why do they do it?
- With three nodes down, are the data lost? What does your last line show? What is the difference between a node that is down and a node whose disk is lost?
- In Lab 2, every partition was available to every worker. Here, what stops one node from being enough to serve a read?

### C.3 Writing with nodes down, and repairing — required (8 min)

```bash
docker kill s2
obj put /parquet/sf10/store_returns sf10/store_returns
docker kill s5
obj put /parquet/sf10/store_returns sf10/store_returns_again
dc up -d s2 s5
obj status
obj repair
```

Now a disk is lost and replaced by an empty one:

```bash
obj wipe s3
obj verify sf10/store_sales
obj repair
obj verify
```

| Step | Result | Numbers |
|---|---|---|
| put with `s2` down | | objects written, shards missing |
| put with `s2`, `s5` down | | |
| `obj status` after restart | | objects with missing chunks |
| repair | | MB read, MB written, s |
| `verify` after wiping `s3` | | OK / failed |
| repair after wiping `s3` | | MB read, MB written, s |
| `verify` of everything | | OK / failed |

Questions:

- A read needs 4 shards, but a write needs 5. Why not accept a write with 4? (What would the object survive just after such a write?)
- For each MB that repair writes, how many MB does it read? Why exactly that number? How many would it read with 3 replicas?
- Repairing the wiped `s3` took some seconds here. Estimate it for a real 16 TB disk at 200 MB/s per disk, with the reads spread over 50 other disks. What limits it, and why does it matter for durability? (Slide 5: a few disks die every year.)

### C.4 A frozen node (4 min)

```bash
docker pause s3
lab read_bench.py obj sf10/store_sales --query sum1
obj status
```

Wait 15 seconds, then:

```bash
lab read_bench.py obj sf10/store_sales --query sum1 --runs 1
docker unpause s3
```

| Case | Run times (s) | What `obj status` showed for `s3` |
|---|---|---|
| `s3` frozen, 3 runs | | |
| `s3` frozen, after 15 s | | |

The front-end waits 2 s for a shard (`OBJ_TIMEOUT_S`). A shard that fails is not asked again for 10 s (`OBJ_SUSPECT_S`).

Questions:

- Why is the first run slow and the next two fast? Why is the run after 15 s slow again?
- Compare with Lab 2, C.1 (the frozen worker). What is the same, and what does erasure coding add?
- The front-end could send each read to 5 shards and use the first 4 answers. What would that cost, and what would it buy?

---

## Part D — Discussion (10 min)

- Which format, sort order and row-group size would you choose for the project's data lake? Which query of yours would lose with that choice?
- Your numbers came from one laptop: page cache, one SSD, containers sharing cores, an object store in Python.
  Which of your conclusions would still hold on S3 and a real cluster, and which would not?
- Erasure coding against replication: when is each the better choice? (Think of HDFS 3, which uses both.)

Leave the containers as they are if you continue at home. To stop them:

```bash
dc stop        # stops the containers; the volumes (Part A files, object store) are kept
```

---

## Take-home work

**Deadline:** see the course page on Moodle. Write, check and hand in the report with the report kit (`report_kit.zip` on Moodle): follow its `SUBMISSION.md`.

1. **Finish the tables** of Parts A, B and C. B.4 is required at home if you did not do it in class.
2. **Two figures.**
   - Part A: for each of the five configurations, the time of `sum1`, `agg5` and `all23` (warm), on a log scale.
   - Part B: for the queries `sum1`, `all23` and `month`, the MB read on each copy (`sf10`, `sorted`, `sorted_rg1m`).
3. **AI session 03: literature triage (Lab 3.2).** Follow `ai/session-03.md`:
   1. send the brief to an AI assistant (it builds a map of Dynamo and Bigtable, with citations);
   2. check every claim against the two PDFs, and give each one a verdict: **confirmed**, **with conditions**, **refuted**, **not verifiable**;
   3. test two of its claims against your own numbers, one from Lab 2 and one from Lab 3;
   4. write both sessions in `ai-log-lab3.md` (the template is in the report kit): *Session 03a — Dynamo* and *Session 03b — Bigtable*.
4. **Clean up** when your report is handed in: `dc down -v` removes the containers and the volumes (about 8 GB).

### Report (maximum 4 pages, PDF, in the format of the report kit)

1. **Machine and Docker:** the machine table of Lab 1, the CPUs and memory given to Docker, and whether "cold" was Linux in a VM or native.
2. **Part A:** the tables, the figure, and your choice of format for the project, with the numbers that justify it.
3. **Part B:** the table of B.1; the predictions and measurements of B.3, with the explanation of the difference; B.4; the figure.
4. **Part C:** the space ratios; the table of C.2 with the derivation of the MB read; the repair numbers and your estimate for a real disk; the frozen node.
5. **AI session:** the two triage tables (at most 12 rows each: the most important ones), the counts of verdicts and of wrong citations, the two claims tested against your numbers, and one paragraph on what the assistant got wrong most often.
6. **Limits:** two results of this lab that you would not trust on S3 or on a real cluster, and why.

Be ready to explain any number of your report in class.

### Rules for TPC-DS material

The rules of Lab 1 still apply: do not publish the kit, the generated data or the query files, and do not
present your numbers as TPC-DS results.

---

## Appendix A — Expected values

These values come from the instructor's run (DuckDB 1.5.6, the version of the lab's image). Sizes and
bytes should match yours, except for the rewritten copies (`sorted`, `sorted_rg1m`): DuckDB writes them with
several threads, and the files differ slightly from one run to another. Times will not match.

**Part A** (SF1, 2,880,404 rows):

| Configuration | Size (MB) | Size / CSV |
|---|---:|---:|
| CSV | 385.6 | 1.000 |
| JSON | 1,512.8 | 3.924 |
| Avro | 242.6 | 0.629 |
| Parquet, snappy | 137.4 | 0.356 |
| Parquet, zstd | 107.5 | 0.279 |

The results of `sum1`, `agg5` and `all23` start with `4741589953.76`, `1, 456896, 22551072` and `2880404`.

**Part B** (SF10, 28,800,991 rows):

| Copy | Files | Row groups | MB in the store |
|---|---:|---:|---:|
| `sf10` | 16 | 240 | 1,068.1 |
| `sorted` | 17 | 235 | about 1,153.5 |
| `sorted_rg1m` | 15 | 29 or 30 | about 1,113 |

| Query | Copy | Row groups that may match | MB to DuckDB | GETs |
|---|---|---:|---:|---:|
| `sum1` | `sf10` | 240 | 82.6 | 256 |
| `all23` | `sf10` | 240 | 1,013.0 | 256 |
| `month` | `sf10` | 240 | 91.0 | 495 |
| `month` | `sorted` | 3 | 3.2 | 23 |
| `month` | `sorted_rg1m` | 2 | about 7.5 | about 19 |

The result of `month` is `259707` rows and `437940742.05`; the result of `sum1` at SF10 is `47364095902.84`.

**Part C:** `obj du` gives a ratio of 1.500. With `s2` down, `obj verify sf10/store_sales` reads 1,869.2 MB
from the shards and rebuilds 267.0 MB. Repair reads 4.0 MB for every MB it writes.

---

## Appendix B — Problems and fixes

| Problem | Symptom | Fix |
|---|---|---|
| Docker is not running | `Cannot connect to the Docker daemon` | Start Docker Desktop (or `sudo systemctl start docker` on Linux) |
| Lab 2 is still running | `container name "/client" is already in use` | `docker compose -f $T/lab2/compose.yaml down` |
| Port 8200 is taken | `port is already allocated` on `dc up` | Stop the program that uses it, or change the left-hand `8200` in `compose.yaml` |
| The shortcuts are missing, or point to Lab 2 | `command not found: obj`, or `no such service: obj` | Run the lines of [Conventions](#conventions) in this terminal |
| No Lab 1 data in the container | `folder not found: /parquet/sf10/store_sales` | Your Parquet files must be in `~/tpcds/parquet/sf10` and the starter pack in `~/tpcds/lab3` |
| No files for Part A | `no files in /data/formats` | Run `lab formats.py --sf 1` (A.1) |
| A copy is missing in the object store | `no objects under lake/sorted/store_sales` | Run the `load.py` command of section 0.4 (or B.4) |
| Nodes left down after Part C | `DOWN` or `NO ANSWER` in `obj status`; HTTP 503 in DuckDB | `dc up -d`, and `docker unpause s3` if you froze it |
| Objects still missing chunks | `objects with missing chunks` is not 0 | `obj repair` (with all six shards up) |
| Not enough disk space | `No space left on device` | Free space, or remove the copy of B.4 when done; Docker Desktop has its own disk limit in *Settings → Resources* |
| You want to start again from nothing | | `dc down -v`, then section 0.4 again |

If you hit a problem that is not in this table, write down the exact error message and ask.

---

## Appendix C — Typical durations

Measured on the instructor's laptop (Apple M3 Max, 16 cores, Docker with 16 CPUs and 8 GB). The lab's
containers are capped (client 2 CPUs, front-end 1 CPU, each shard 0.5 CPU), so a 4-core laptop should be
only somewhat slower; a slower SSD will show most in `load.py` and `obj repair`.

| Step | Instructor's laptop | Simulated 4-core laptop |
|---|---|---|
| `docker compose build` (first time, fast connection; the image is 420 MB) | about 10 s | |
| `load.py --name sf10` / `--name sorted` | 9 s / 27 s | 9 s / 29 s |
| `formats.py --sf 1` | 9 s | 11–13 s |
| `read_bench.py formats`, one query, five configurations | 15–30 s | 15–30 s |
| `read_bench.py obj`, one run of `sum1` / `all23` on `sf10` | 0.29 s / 4.7 s | 0.29 s / 4.8 s |
| `read_bench.py obj`, one run of `month` on `sf10` / `sorted` | 0.47 s / 0.03 s | 0.47 s / 0.07 s |
| `load.py --name sorted_rg1m …` (B.4) | about 30 s | about 31 s |
| `obj verify sf10/store_sales`, all up / `s2` down | 2.5 s / 6.0 s | 2.6 s / 5.8 s |
| `obj repair` after `obj wipe s3` (3.5 GB read) | 16 s | 17.5 s |

In the simulation every container shares the same 4 cores. It is not a real 4-core laptop: the disk and the memory are still the Mac's.
