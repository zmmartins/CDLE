# CDLE — Lab 2: TPC-DS on a Cluster of Containers

**Partition the data → measure the skew → run queries on 1 to 8 nodes → replicate → break things**

| | |
|---|---|
| **Duration** | 100 minutes in class, plus take-home work |
| **Groups** | Two students, one laptop running the cluster (the other is the backup) |
| **Data** | The SF10 Parquet files you prepared in Lab 1 (`$T/parquet/sf10`) |
| **Tools** | Docker with Compose, Python 3, DuckDB |

## Learning goals

By the end of this lab you should be able to:

- compare partitioning rules (hash, range, quantile) and measure the skew each one produces;
- explain why a consistent-hashing ring moves fewer keys than `hash mod N`, and what virtual nodes add;
- run a query on several nodes with a scatter-and-merge plan, and say when such a plan is correct;
- measure speedup when nodes are added, and show how one overloaded node limits it;
- tell a crashed node from a slow one, and explain what a timeout decides;
- choose W and R for a replicated store and predict stale reads, latency and availability;
- describe what each side of a network partition can and cannot do.

### How this cluster differs from a real one

Every "node" is a container on your laptop with at most 1 CPU and 1 GB of RAM. This makes the nodes
identical and lets you stop, freeze and disconnect them with one command. Two things are not real, and you
must keep them in mind when you explain your numbers:

- the nodes share one machine: its cores, its memory and its disk;
- the network between containers has almost no delay, so network cost is **simulated**: the options
  `--delay-ms` and `--mbit` add a delay to every request and limit the transfer speed.

---

## 0. Before the lab (at home, about 30 minutes) — required

There is no time for these steps in class. Come with everything below done.

### 0.1 Check what you need from Lab 1

```bash
export T=~/tpcds
ls $T/parquet/sf10/store_sales $T/parquet/sf10/store_returns | head
$T/venv/bin/python -c "import duckdb; print('DuckDB', duckdb.__version__)"
```

If you do not have the SF10 Parquet files, do the take-home part of Lab 1 first.
(If your machine cannot hold SF10, the lab also runs with `sf1`; say so in your report.)

Free disk space needed: about 3 GB (Docker image, plus 1.5 GB of partitioned data).

### 0.2 Install Docker

- **macOS and Windows:** install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
  On Windows, use the WSL 2 backend, turn on *Settings → Resources → WSL integration* for your Ubuntu,
  and run every command of this lab **inside WSL**, as in Lab 1.
- **Linux:** install Docker Engine and the Compose plugin, and add your user to the `docker` group.

Then check how much of your machine Docker may use:

```bash
docker compose version
docker info --format 'CPUs: {{.NCPU}}   memory: {{.MemTotal}} bytes'
```

Docker needs **at least 4 CPUs and 4 GB of memory**. On macOS, change this in
*Docker Desktop → Settings → Resources*. On Windows, WSL 2 gets half of the machine's RAM by default.

### 0.3 Get the starter pack

Download `lab2_starter.zip` from the course page and unzip it in `~/tpcds`, so that the files end up in `~/tpcds/lab2/`:

| File | What it does | Runs |
|---|---|---|
| `skew.py` | Shows how evenly a partitioning rule spreads the rows | on your machine |
| `ring.py` | Consistent hashing with virtual nodes, compared with `hash mod N` | on your machine |
| `layout.py` | The partitioning rules, shared by `skew.py` and `partition.py` | |
| `partition.py` | Writes a partitioned copy of a table | in the `client` container |
| `worker.py` | A worker node: runs SQL fragments in DuckDB over its partitions | containers `w1` … `w8` |
| `plans.py` | The three queries of the lab and their distributed plans | |
| `coordinator.py` | Sends the fragments to the workers, merges the answers, checks the result | in the `client` container |
| `kv_replica.py` | One replica of a small key-value store | containers `kv1` … `kv3` |
| `kv_client.py` | Reads and writes with quorums (W and R) | in the `client` container |
| `compose.yaml`, `Dockerfile` | The cluster: 8 workers, 3 replicas, 1 client | |
| `ai/session-02.md` | The brief for the AI session (take-home) | |

### 0.4 Build the image and test the cluster

```bash
cd ~/tpcds/lab2
docker compose build          # downloads Python and DuckDB: needs the internet, takes a few minutes
docker compose up -d
docker compose exec client python coordinator.py status
docker compose down
```

**Check:** `status` must print `up` for `w1` to `w8`. The list of layouts is still empty.

If a port is already in use, or a container does not start, see [Appendix C](#appendix-c--problems-and-fixes).

---

## Conventions

Run these lines **in every new terminal** before you start (they work in bash and in zsh):

```bash
export T=~/tpcds
export PY=$T/venv/bin/python
dc()    { docker compose -f $T/lab2/compose.yaml "$@"; }
lab()   { dc exec -T client python "$@"; }
coord() { lab coordinator.py "$@"; }
kv()    { lab kv_client.py "$@"; }
```

| Shortcut | Meaning |
|---|---|
| `dc …` | `docker compose` for the lab's cluster, from any folder |
| `lab script.py …` | run one of the lab's scripts inside the `client` container |
| `coord …` | the query coordinator (Parts B and C) |
| `kv …` | the key-value client (Part C) |

Where things are:

```
~/tpcds/
├── parquet/sf10/        from Lab 1 (the client container sees it as /parquet/sf10)
├── venv/                from Lab 1
└── lab2/                the starter pack (every container sees it as /lab)

Docker volume lab2_data  the partitioned data (every container sees it as /data)
```

The partitioned data is kept in a Docker volume and not in a folder of your machine because, on macOS and
Windows, containers read shared folders slowly and get in each other's way.

---

## Class plan

| Time | Part |
|---|---|
| 0–10 min | Introduction; start the cluster |
| 10–35 min | [Part A](#part-a--partitioning-and-skew-25-min) — Partitioning and skew |
| 35–65 min | [Part B](#part-b--queries-on-several-nodes-30-min) — Queries on several nodes |
| 65–90 min | [Part C](#part-c--replication-and-failures-25-min) — Replication and failures |
| 90–100 min | [Part D](#part-d--discussion-10-min) — Discussion |

Start the cluster now and leave it running until the end:

```bash
dc up -d
coord status
```

Before measuring: plug in your laptop, close other heavy applications, and don't use the machine while a measurement runs.

---

## Part A — Partitioning and skew (25 min)

### A.1 How evenly does each key spread the rows? (10 min)

`skew.py` computes the partition of every row of `store_sales` (28.8 million rows) and counts the rows per
partition. It writes nothing.

```bash
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_ticket_number
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_customer_sk
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_store_sk
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_sold_date_sk --scheme range
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_sold_date_sk --scheme quantile
```

The three schemes:

| `--scheme` | Rule |
|---|---|
| `hash` (default) | partition = `hash(key) mod P` |
| `range` | P ranges of equal width between the smallest and the largest key |
| `quantile` | P ranges that each hold about the same number of rows |

`--parts P` changes the number of partitions (default 8). The summary line `max/mean` is the size of the
largest partition divided by the average size: 1.00 is a perfect balance.

**Record** (8 partitions):

| Rule | max/mean | Distinct keys | Most frequent key and its share |
|---|---|---|---|
| hash(`ss_ticket_number`) | | | |
| hash(`ss_customer_sk`) | | | |
| hash(`ss_store_sk`) | | | |
| range(`ss_sold_date_sk`) | | | |
| quantile(`ss_sold_date_sk`) | | | |

Now run the customer key again with 100 partitions, as in the lecture's 100-node example:

```bash
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 --key ss_customer_sk --parts 100
```

Questions:

- Two different causes of skew appear in your table: one very frequent key, and too few distinct keys.
  Which rule shows which?
- Why does the hot partition of `ss_customer_sk` get worse, relative to the others, when P grows from 8 to 100?
- `range` on the date is uneven. What in the data explains it? (Look at example `06_keys_to_dates.sql` of Lab 1.)
- `quantile` fixes the ranges, yet one partition is still larger. Which rows are in it?

**Salting.** A hot key cannot be split by adding partitions: all its rows have the same hash. The fix is to
give the hot key's rows something else to hash. Here the hot "key" is NULL (sales with no known customer):

```bash
$PY $T/lab2/skew.py --parquet $T/parquet/sf10 \
  --expr "case when ss_customer_sk is null then hash(ss_ticket_number) else hash(ss_customer_sk) end"
```

With `--expr` you write the rule yourself: any SQL expression that gives a non-negative integer.

- What is `max/mean` now?
- After salting, where are the rows of a normal customer? And the rows with no customer? Keep your answer for Part B.4.

### A.2 The ring (7 min)

`ring.py` places 100,000 keys on 4 nodes with a consistent-hashing ring, then adds a fifth node, and
compares with `hash(key) mod N`.

```bash
$PY $T/lab2/ring.py --vnodes 1
$PY $T/lab2/ring.py --vnodes 16
$PY $T/lab2/ring.py --vnodes 256
```

Open `ring.py`: `build_ring` and `owner` are the two functions of the lecture, about ten lines in all.

**Record:**

| Tokens per node | max/mean before | Keys moved by the ring | Taken from | Keys moved by mod N |
|---|---|---|---|---|
| 1 | | | | |
| 16 | | | | |
| 256 | | | | |

Questions:

- With one token per node, how many keys move and which nodes give them up? Why is that a problem for the node that gives them?
- What do virtual nodes change: the number of keys that move, where they come from, or both?
- `mod N` moves about 80% of the keys when N goes from 4 to 5. Derive that number.
- Virtual nodes even out the nodes. Would they help with the hot key of A.1? Why not?

### A.3 Write three layouts (8 min)

`partition.py` writes a partitioned copy of a table into the Docker volume: one folder per partition.
It keeps 9 of the 23 columns of `store_sales`, to save disk space. It runs in the `client` container,
where your Lab 1 folder `$T/parquet` is visible as `/parquet`.

```bash
lab partition.py --parquet /parquet/sf10 --name by_ticket   --key ss_ticket_number
lab partition.py --parquet /parquet/sf10 --name by_customer --key ss_customer_sk
lab partition.py --parquet /parquet/sf10 --name by_store    --key ss_store_sk
lab partition.py --parquet /parquet/sf10 --name by_customer_salted \
  --expr "case when ss_customer_sk is null then hash(ss_ticket_number) else hash(ss_customer_sk) end"
lab partition.py --parquet /parquet/sf10 --name by_ticket --table store_returns --key sr_ticket_number
```

The last command partitions a second table, `store_returns`, by the same rule as `by_ticket`. Part B uses it for a join.

**Check:**

```bash
coord status
```

It must list the five tables above, each with 8 partitions, and the `max/mean` values of [Appendix A](#appendix-a--expected-values-at-sf10).

**Record:** the rows and the MB of the largest and of the smallest partition of each layout. Is skew in rows also skew in bytes?

---

## Part B — Queries on several nodes (30 min)

### How a query runs here

The coordinator gives partition *p* to worker number *p mod N*, where N is `--nodes`. Then:

1. **scatter** — every worker runs the plan's *local* SQL over its own partitions, at the same time;
2. **merge** — the coordinator runs the plan's *merge* SQL over all the answers;
3. **check** — the result is compared with the answer of the plain query on one node.

The queries and their plans are in `plans.py`. Open it now and read query `q1`.

| Query | What it computes | Plans |
|---|---|---|
| `q1` | Top 10 customers by total amount paid | `partial`, `pushdown` |
| `q2` | Per store: sales lines, amount paid, average price | `naive`, `partial` |
| `q3` | Per store: returned lines and amount (join of `store_sales` and `store_returns`) | `colocated`, `broadcast` |

The coordinator runs any plan you ask for and does **not** know whether the plan is valid for the layout.
The last line of its output tells you: `SAME` or `DIFFERENT  <-- wrong result`.

### B.1 More nodes — required (8 min)

```bash
for n in 1 2 4 8; do
  coord run q3 --plan colocated --layout by_ticket --nodes $n --runs 3
done
```

For each run the coordinator prints one line per worker (partitions, rows read, time inside the worker,
time seen by the coordinator, rows and KB sent back) and the totals. Use the **median of 3 runs**.

| Nodes N | T(N) (s) | Speedup T(1)/T(N) | Rows read per worker | KB received by the coordinator |
|---|---|---|---|---|
| 1 | | 1.00 | | |
| 2 | | | | |
| 4 | | | | |
| 8 | | | | |

Questions:

- Is the speedup close to linear? Up to how many nodes?
- Your laptop has a fixed number of cores. What do you expect when N is larger than that number, and what did you measure?
- In Lab 1 you gave DuckDB more threads on one machine. What is different here? What is the same?

### B.2 A merge step that is wrong — required (7 min)

`q2` needs an average. Run the `naive` plan: every worker computes its averages and the coordinator
averages them.

```bash
coord run q2 --plan naive --layout by_ticket --nodes 4 --show 5
```

The result is `DIFFERENT`. `--show 5` prints the first rows of both answers: compare the columns.
Look closely, because the difference is small. An answer that is almost right is the dangerous kind.

**Exercise.** In `plans.py`, the plan `q2` / `partial` has a local step that sends, for each store, the sum
and the count of the prices. Its merge step is missing. Open `$T/lab2/plans.py` in your editor, replace
`None` with your SQL (it reads from `{partials}` and must return the same four columns as the reference), then:

```bash
coord run q2 --plan partial --layout by_ticket --nodes 4
```

Repeat until the result is `SAME`.

Questions:

- Why is the average of the averages wrong? When would it be exactly right? Why is the error so small on `by_ticket`, and when would it be large?
- Run the `naive` plan on the layout `by_store`. It is `SAME`. Why?
- Which of these can be merged from partial results, and how: `count`, `sum`, `min`, `avg`, `count(distinct …)`, `median`?

### B.3 One slow node — required (5 min)

Run the same query on three layouts, with 8 nodes, so that each node has exactly one partition:

```bash
for l in by_ticket by_customer by_store; do
  coord run q2 --plan partial --layout $l --nodes 8 --runs 3
done
```

| Layout | max/mean rows (A.3) | Slowest worker (s) | Fastest worker (s) | T (s) |
|---|---|---|---|---|
| `by_ticket` | | | | |
| `by_customer` | | | | |
| `by_store` | | | | |

With 8 nodes and fewer than 8 free cores, all the workers are slowed down, but the comparison between them remains valid.

Questions:

- Which worker decides the total time? How does `T(by_store) / T(by_ticket)` compare with `max/mean`?
- The same amount of data is read in the three cases. Where did the lost time go?
- Run `by_store` again with 4 nodes (`coord run q2 --plan partial --layout by_store --nodes 4 --runs 3`). The imbalance between workers is smaller. Why? Relate this to virtual nodes.

### B.4 The key you partition by decides the plan — required (5 min)

`q1` has two plans. `partial` sends one row per customer from every worker. `pushdown` sends only each
worker's own top 10.

```bash
coord run q1 --plan partial  --layout by_ticket   --nodes 4
coord run q1 --plan pushdown --layout by_ticket   --nodes 4 --show 5
coord run q1 --plan partial  --layout by_customer --nodes 4
coord run q1 --plan pushdown --layout by_customer --nodes 4
coord run q1 --plan pushdown --layout by_customer_salted --nodes 4 --show 5
```

| Layout | Plan | Result | Rows sent by the workers | MB received | T (s) |
|---|---|---|---|---|---|
| `by_ticket` | `partial` | | | | |
| `by_ticket` | `pushdown` | | | | |
| `by_customer` | `partial` | | | | |
| `by_customer` | `pushdown` | | | | |
| `by_customer_salted` | `pushdown` | | | | |

Questions:

- State the condition on the layout under which `pushdown` is correct.
- With `partial`, the workers send far fewer rows on `by_customer` than on `by_ticket`. Why?
- Salting balanced the partitions in A.1. What did it cost here? Which plan is still correct on the salted layout?

### B.5 A join, and the cost of the network — if time allows (5 min)

`q3` joins two fact tables on `(ticket_number, item_sk)`.

- `colocated` joins, on every worker, its partitions of the two tables.
- `broadcast` first collects the smaller table (`store_returns`, 2.9 million rows) and sends all of it to every worker.

```bash
coord run q3 --plan colocated --layout by_ticket   --nodes 4
coord run q3 --plan colocated --layout by_customer --nodes 4
coord run q3 --plan broadcast --layout by_customer --nodes 4
coord run q3 --plan broadcast --layout by_customer --nodes 4 --mbit 100 --delay-ms 5
coord run q3 --plan broadcast --layout by_customer --nodes 8 --mbit 100 --delay-ms 5
```

`--mbit 100 --delay-ms 5` simulates a 100 Mbit/s network with 5 ms added to every request.

Questions:

- Why is `colocated` wrong on `by_customer`? Are rows missing, or are there too many?
- How many MB does `broadcast` send with 4 nodes and with 8? What is the rule?
- With the simulated network, which step dominates the time?
- Neither plan handles two large tables that are partitioned by different keys. What would have to move? (Spark calls it a shuffle; you will meet it in weeks 4 and 5.)

---

## Part C — Replication and failures (25 min)

### C.1 A worker stops answering — required (7 min)

Every partition is available to every worker (think of it as a replica on each node). When a worker does
not answer, the coordinator gives its partitions to the next worker that is alive and runs those fragments again.

**A frozen node.** `docker pause` freezes a container: it is not dead, it just does nothing.

```bash
docker pause w3
coord run q3 --plan colocated --layout by_ticket --nodes 4
docker unpause w3
```

**A crashed node.** `docker kill` stops it at once.

```bash
docker kill w3
coord run q3 --plan colocated --layout by_ticket --nodes 4
coord status
dc up -d w3
```

**A timeout that is too short.** All workers are healthy again:

```bash
coord run q3 --plan colocated --layout by_ticket --nodes 4 --timeout 0.05
```

| Case | Result | T (s) | What the coordinator printed |
|---|---|---|---|
| `w3` frozen, timeout 10 s | | | |
| `w3` killed | | | |
| all healthy, timeout 0.05 s | | | |

Questions:

- The result is still `SAME` in the first two cases. What made that possible?
- Why does the frozen node cost so much more time than the crashed one? From the coordinator's side, how does a frozen node differ from a node that is just slow?
- What does a timeout that is too short do? And one that is too long? There is no perfect value: why?
- All the partitions of `w3` went to one worker. What does that do to that worker? How would a ring with virtual nodes spread them?

### C.2 Quorums — required (10 min)

Three replicas, `kv1`, `kv2` and `kv3`, hold a small key-value store. Each value has a version number.
The client sends a write to all N = 3 replicas and reports success as soon as **W** of them confirm.
It sends a read to all three, waits for the first **R** answers and returns the one with the highest version.

Try it:

```bash
kv status
kv put colour red --w 2
kv get colour --r 3
```

Now make `kv3` slow: it applies every write 200 ms late. Its reads are still fast, so it serves old values.

```bash
kv lag kv3 200
```

`kv bench` does 200 writes, each followed at once by a read of the same key, and counts the **stale reads**:
reads that return a value older than a write that was already confirmed.

```bash
kv bench --w 1 --r 1
kv bench --w 2 --r 1
kv bench --w 2 --r 2
kv bench --w 3 --r 1
kv bench --w 1 --r 3
```

| W | R | W+R>N? | Stale reads | Write p50 (ms) | Read p50 (ms) |
|---|---|---|---|---|---|
| 1 | 1 | | | | |
| 2 | 1 | | | | |
| 2 | 2 | | | | |
| 3 | 1 | | | | |
| 1 | 3 | | | | |

Now freeze `kv3`, repeat two of the settings, and unfreeze it:

```bash
docker pause kv3
kv bench --w 3 --r 1 --ops 5
kv bench --w 2 --r 2 --ops 50
docker unpause kv3
kv lag kv3 0
```

Questions:

- Which settings give stale reads? Does `W + R > N` predict it?
- Which setting has the slowest writes, and why exactly that one?
- With `kv3` frozen, which setting stops accepting writes? How long does each failed write take, and why?
- `W=2, R=2` survives one frozen replica. What would it not survive?
- The benchmark has a single client. Name one problem that quorums do not solve when two clients write the same key at the same time.

### C.3 A network partition — required (8 min)

Cut `kv1` off from the others. The client container stays on the side of `kv2` and `kv3`.

```bash
kv put colour red --w 3
docker network disconnect lab2_default kv1
kv status
```

**The majority side** (the client, `kv2`, `kv3`):

```bash
kv put colour green --w 2
kv get colour --r 2
```

**The minority side.** Run a client inside `kv1`, which can only reach itself:

```bash
docker exec kv1 python kv_client.py put colour blue --w 2
docker exec kv1 python kv_client.py get colour --r 1
```

**Heal the partition** and look at all three replicas:

```bash
docker network connect lab2_default kv1
kv get colour --r 3
```

| Step | Result (OK or FAILED, value and version) |
|---|---|
| Majority: put `green`, W=2 | |
| Minority: put `blue`, W=2 | |
| Minority: get, R=1 | |
| After healing: get, R=3 | |

Questions:

- On the minority side, the write with W=2 was reported as FAILED. Was it stored anyway? What does the read with R=1 return?
- After healing, what do the three replicas hold? Can the version numbers alone tell which value is right? Which value did the read return, and is there a reason for that choice?
- During the partition, which side behaved as CP and which as AP? Which setting of W and R would make the minority side available for writes, and what would that cost?
- Use PACELC: with no partition (C.2), what did you trade for lower latency?

---

## Part D — Discussion (10 min)

- In your Lab 1 report you predicted what would happen on two machines. What did you get right, and what did you miss?
- Which layout would you choose for these three queries together? What does each query lose with your choice?
- Your cluster shared one disk and one memory, and its network was simulated. Which of your results would change most on eight real machines?

Leave the cluster as it is if you continue at home. To stop it:

```bash
dc down        # stops the containers; the partitioned data is kept
```

---

## Take-home work

**Deadline:** hand in the report (PDF) and `ai-log.md` by the **second class after this lab**.

1. **Finish the tables** of Parts A, B and C, with `--runs 3` where a time is asked. B.5 is required at home if you did not do it in class.
2. **Speedup plot.** Nodes (1, 2, 4, 8) against speedup for `q3`/`colocated` on `by_ticket`, with the ideal line,
   and the Amdahl estimate of the parallel fraction *p* as in Lab 1.
3. **AI session — generate, critique, benchmark.**
   1. Fill in `ai/session-02.md` with your measurements from Part A and give it to an AI assistant.
   2. For each of the 3 schemes it proposes, **before running anything**, write down its predicted `max/mean` and the plan it says is correct for each query.
   3. Measure: `skew.py --expr "…"` for the balance, then `partition.py --name ai_a --expr "…"` (and `ai_b`, `ai_c`) and run `q1`, `q2` and `q3` with 8 nodes and `--runs 3`.
      Same queries, results checked (`SAME`), median of 3 runs.
   4. For each claim of the assistant, mark it **confirmed** or **refuted**, with the number that decides.
   5. Add a *Session 02* entry to your `ai-log.md` (create the file if you do not have one): the brief you sent, the answer, and what you did with it.
4. **Optional: two machines.** See [Appendix B](#appendix-b--optional-two-machines).
5. **Clean up** when your report is handed in: `dc down -v` removes the containers and the 1.5 GB of partitioned data.

### Report (maximum 4 pages, PDF)

1. **Machine and Docker** — the machine table of Lab 1, plus the CPUs and memory given to Docker.
2. **Part A** — the two tables, and your answers on the causes of skew, on salting and on the ring.
3. **Part B** — the speedup plot; the tables of B.3 and B.4; your merge step for `q2` and why the naive one is wrong;
   for each of the three queries, the condition under which its cheapest plan is correct.
4. **Part C** — the three tables; your explanation of frozen against crashed; the quorum settings in terms of
   stale reads, latency and availability; the partition experiment in terms of CAP.
5. **AI session** — the three schemes, the predictions against your measurements, and a one-paragraph verdict:
   which scheme wins for the three queries together, and the evidence.
6. **Limits** — two results of this lab that you would not trust on a real cluster, and why.

Be ready to explain any number of your report in class.

### Rules for TPC-DS material

The rules of Lab 1 still apply: do not publish the kit, the generated data or the query files, and do not
present your numbers as TPC-DS results.

---

## Appendix A — Expected values at SF10

`max/mean` of the rows per partition, 8 partitions, as printed by `coord status`:

| Layout | Table | Rows | max/mean |
|---|---|---:|---:|
| `by_ticket` | `store_sales` | 28,800,991 | 1.00 |
| `by_ticket` | `store_returns` | 2,875,432 | 1.00 |
| `by_customer` | `store_sales` | 28,800,991 | 1.32 |
| `by_customer_salted` | `store_sales` | 28,800,991 | 1.00 |
| `by_store` | `store_sales` | 28,800,991 | 1.95 |

These values assume the DuckDB version of the lab's image (1.4.5). Each layout takes about 0.4 GB in the volume.

---

## Appendix B — Optional: two machines

Each laptop runs its own cluster and has the same layouts (the data of Lab 1 is the same on every machine,
so `partition.py` gives the same partitions). One laptop then uses four of its own workers and four of the other's.

On **both** laptops, connected to the same network (a phone hotspot works; the campus Wi-Fi usually blocks
connections between laptops):

```bash
dc down
LAB2_BIND=0.0.0.0 dc up -d      # publish the workers' ports 8101-8108 to the network
```

On laptop A, with the IP address of laptop B (for example `192.168.1.20`):

```bash
B=192.168.1.20
coord --workers w1,w2,w3,w4,$B:8101,$B:8102,$B:8103,$B:8104 status
coord --workers w1,w2,w3,w4,$B:8101,$B:8102,$B:8103,$B:8104 run q3 --plan colocated --layout by_ticket --nodes 8 --runs 3
coord --workers w1,w2,w3,w4,$B:8101,$B:8102,$B:8103,$B:8104 run q3 --plan broadcast --layout by_customer --nodes 8 --runs 3
```

Compare with the same runs on one laptop: the time of each worker, and the cost of the broadcast over a real network.

While the ports are published, anyone on that network can send SQL to your workers. Go back to the normal
setting when you finish: `dc down`, then `dc up -d`. On Windows, the firewall may ask for permission the first time.

---

## Appendix C — Problems and fixes

| Problem | Symptom | Fix |
|---|---|---|
| Docker is not running | `Cannot connect to the Docker daemon` | Start Docker Desktop (or `sudo systemctl start docker` on Linux) |
| A port is taken | `port is already allocated` on `dc up` | Stop the program that uses ports 8101–8108, or change the left-hand numbers in `compose.yaml` |
| The shortcuts are missing | `command not found: dc` (or `coord`, `kv`, `lab`) | Run the lines of [Conventions](#conventions) in this terminal |
| No Lab 1 data in the container | `folder not found: /parquet/sf10/store_sales` | Your Parquet files must be in `~/tpcds/parquet/sf10` and the starter pack in `~/tpcds/lab2` |
| A layout is missing | `layout … not found in /data` | Run the `partition.py` command of A.3 for that layout |
| A worker was killed and not restarted | `DOWN` in `coord status` | `dc up -d` |
| A container is still frozen | A worker or replica does not answer | `docker unpause w3` (or `kv3`) |
| `kv1` is still cut off | `kv1 DOWN` in `kv status` | `docker network connect lab2_default kv1` |
| A worker runs out of memory | A fragment fails with an out-of-memory error | Give Docker at least 4 GB (section 0.2) and close other applications |
| You want to start again from nothing | | `dc down -v`, then `dc up -d` and repeat A.3 |

If you hit a problem that is not in this table, write down the exact error message and ask.

---

## Appendix D — Typical durations

Measured on the instructor's laptop (Apple M3 Max, 16 cores, Docker with 16 CPUs and 8 GB). On a 4-core laptop
expect clearly longer times, and no gain from 4 to 8 nodes.

| Step | Time |
|---|---|
| `docker compose build` (first time, with a fast connection) | about 10 s |
| `skew.py`, one rule | under 1 s |
| `partition.py`, one layout of `store_sales` | about 5 s |
| `q3` / `colocated` on `by_ticket`, 1 / 2 / 4 / 8 nodes | 0.81 / 0.41 / 0.22 / 0.12 s |
| `q2` / `partial` with 8 nodes: `by_ticket` / `by_customer` / `by_store` | 0.08 / 0.10 / 0.13 s |
| `q3` / `broadcast`, 4 nodes, `--mbit 100 --delay-ms 5` | about 3 s |
| `kv bench`, 200 writes and reads | about 2 s (about 45 s with `--w 3` while `kv3` lags 200 ms) |
