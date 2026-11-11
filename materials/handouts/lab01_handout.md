# CDLE — Lab 1: TPC-DS on a Single Machine

**Generate the data → convert it to Parquet → explore it → generate the queries → run and measure them**

| | |
|---|---|
| **Duration** | 100 minutes in class, plus take-home work |
| **Data size in class** | 1GB (scale factor 1, "SF1") |
| **Take-home** | 10GB (SF10), which you will reuse in Lab 2 |
| **Tools** | TPC-DS kit v4.0.0 (`dsdgen`, `dsqgen`), Python 3, DuckDB |

## Learning goals

By the end of this lab you should be able to:

- build and run the TPC-DS data and query generators;
- convert raw benchmark data to a columnar format (Parquet) and verify the conversion;
- read the TPC-DS data model: fact tables, dimension tables and the keys between them;
- run a 99-query analytical workload and measure it properly;
- explain how run time changes with repeated runs, thread count and data size on one machine.

Lab 2 runs the same workload on two machines, starting from the SF10 data you prepare at home.

---

## 0. Before the lab (at home, about 30 minutes) — required

There is no time for these steps in class. Come with everything below done.

### 0.1 Check your machine

- 4 or more CPU cores and 8 GB of RAM or more.
- Free disk space: 5 GB for the lab, plus 20 GB for the take-home part.
- macOS, Linux, or Windows with **WSL 2** (Ubuntu).
  On Windows, do everything inside WSL and keep all files in the Linux file system (under `~`), **not** under `/mnt/c`, which is much slower.

### 0.2 Download the TPC-DS kit

1. Go to [tpc.org](https://www.tpc.org), open the TPC-DS page and download **TPC-DS Tools v4.0.0**.
   You must register and accept the End User License Agreement; the download link arrives by email.
2. Unzip it and move the folder `DSGen-software-code-4.0.0` (it is inside the zip) to exactly this location:

   ```bash
   mkdir -p ~/tpcds
   mv "<path where you unzipped>/DSGen-software-code-4.0.0" ~/tpcds/
   ls ~/tpcds/DSGen-software-code-4.0.0    # should list: answer_sets query_templates tools ...
   ```

> **Why this exact, short path?** The kit's tools break with long paths or paths containing spaces:
> `dsdgen` crashes when its arguments exceed 200 characters, `dsqgen` truncates file paths at about
> 80 characters, and the build fails if the folder name has a space. See [Appendix B](#appendix-b--known-issues-in-the-kit-and-how-this-lab-handles-them).

### 0.3 Install the build tools and Python packages

**macOS**

```bash
xcode-select --install        # skip if already installed
```

**Linux / WSL (Ubuntu)**

```bash
sudo apt update
sudo apt install -y build-essential flex bison python3-venv
```

**Both** — create a Python environment with DuckDB:

```bash
cd ~/tpcds
python3 -m venv venv
~/tpcds/venv/bin/pip install --upgrade pip duckdb
~/tpcds/venv/bin/python -c "import duckdb; print('DuckDB', duckdb.__version__)"
```

### 0.4 Get the starter pack

Download `lab1_starter.zip` from the course page and unzip it so that the scripts end up in `~/tpcds/starter/`:

| File | What it does |
|---|---|
| `fix_templates.sh` | Copies the kit's query templates and applies the fixes needed for DuckDB |
| `split_queries.py` | Splits a generated query stream into `query01.sql` … `query99.sql` |
| `to_parquet.py` | Converts the generated data to Parquet and checks every table's row count |
| `show_query.py` | Runs one SQL query and shows its result as a table |
| `examples/` | Eight short SQL files for exploring the data with `show_query.py` |
| `run_queries.py` | Runs the benchmark queries in DuckDB and records their times |

### 0.5 Record your machine

You will need this in your report. Fill in:

| Item | Your value |
|---|---|
| Computer model | |
| CPU and number of cores | |
| RAM | |
| Disk (type and free space) | |
| Operating system | |
| DuckDB version | |

Useful commands — macOS: `system_profiler SPHardwareDataType`, `df -h ~`; Linux/WSL: `lscpu`, `free -h`, `df -h ~`.

---

## Conventions

Run these two lines **in every new terminal** before you start:

```bash
export T=~/tpcds
export PY=$T/venv/bin/python
```

At the end of the lab your folder will look like this:

```
~/tpcds/
├── DSGen-software-code-4.0.0/   the kit (tools are built in tools/)
├── starter/                     the starter pack scripts and examples/
├── venv/                        Python environment with DuckDB
├── data/sf1/                    generated data (.dat files)
├── parquet/sf1/                 Parquet version, one folder per table
├── query_templates/             fixed copy of the query templates
├── queries/sf1/                 generated queries (query_0.sql and split/)
└── results/                     your measurements (CSV files)
```

---

## Class plan

| Time | Part |
|---|---|
| 0–10 min | Introduction: the TPC-DS data model and scale factors |
| 10–20 min | [Part 1](#part-1--build-the-tools-10-min) — Build the tools |
| 20–30 min | [Part 2](#part-2--generate-the-1gb-data-10-min) — Generate the 1GB data |
| 30–40 min | [Part 3](#part-3--convert-to-parquet-10-min) — Convert to Parquet |
| 40–45 min | [Part 4](#part-4--explore-the-data-5-min) — Explore the data |
| 45–60 min | [Part 5](#part-5--generate-and-run-the-99-queries-15-min) — Generate and run the queries |
| 60–90 min | [Part 6](#part-6--experiments-30-min) — Experiments |
| 90–100 min | [Part 7](#part-7--discussion-10-min) — Discussion |

---

## Part 1 — Build the tools (10 min)

The kit only supports Linux and a few Unix systems out of the box, and its C code is old enough that
modern compilers reject parts of it. The commands below add the flags (and, on macOS, two small
placeholder headers) needed to build it.

**macOS**

```bash
cd $T/DSGen-software-code-4.0.0/tools
mkdir -p macstub
printf '#include <limits.h>\n#define MAXINT INT_MAX\n' > macstub/values.h
printf '#include <stdlib.h>\n' > macstub/malloc.h
make -f Makefile.suite OS=LINUX CC=clang \
  LINUX_CFLAGS="-O3 -Imacstub -Wno-implicit-function-declaration -Wno-implicit-int -Wno-int-conversion -Wno-incompatible-pointer-types"
```

**Linux / WSL**

```bash
cd $T/DSGen-software-code-4.0.0/tools
make -f Makefile.suite OS=LINUX \
  LINUX_CFLAGS="-O3 -fcommon -Wno-implicit-function-declaration -Wno-implicit-int -Wno-int-conversion -Wno-incompatible-pointer-types"
```

Many compiler warnings are normal. **Check** that the build produced the three files you need:

```bash
ls -l dsdgen dsqgen tpcds.idx
```

---

## Part 2 — Generate the 1GB data (10 min)

`dsdgen` must be run from the `tools` folder, because it reads `tpcds.idx` from the current folder.

```bash
cd $T/DSGen-software-code-4.0.0/tools
mkdir -p $T/data/sf1
N=4
time ( for i in $(seq 1 $N); do
  ./dsdgen -SCALE 1 -DIR $T/data/sf1 -PARALLEL $N -CHILD $i -TERMINATE N -QUIET Y &
done; wait )
```

What the options mean:

| Option | Meaning |
|---|---|
| `-SCALE 1` | Scale factor 1, about 1 GB of raw data |
| `-PARALLEL 4 -CHILD i` | Split the work into 4 parts and generate part *i*; the loop runs the 4 parts at the same time |
| `-TERMINATE N` | Do not end each line with an extra `\|` |
| `-QUIET Y` | No progress output |

**Check** the result:

```bash
ls $T/data/sf1 | head
du -sh $T/data/sf1                         # about 1.2 GB
cat $T/data/sf1/store_sales_*.dat | wc -l  # must be 2880404
```

Large tables are split into pieces such as `store_sales_1_4.dat` … `store_sales_4_4.dat`.
Each file is plain text with `|` between columns.

**Record:** generation time (the `real` value), total size, number of files.

---

## Part 3 — Convert to Parquet (10 min)

```bash
$PY $T/starter/to_parquet.py \
  --kit $T/DSGen-software-code-4.0.0/tools \
  --src $T/data/sf1 --dst $T/parquet/sf1 --threads 4
du -sh $T/parquet/sf1
```

What the script does, for each of the 25 tables:

1. Reads the column names and types from the kit's `tpcds.sql` (the `.dat` files have no header row).
2. Uses DuckDB to read all of the table's `.dat` pieces and write them as zstd-compressed Parquet files, in a folder per table.
3. Counts the rows in the new Parquet files and compares them with the number of lines in the `.dat` files.

**Check:** every table must print `OK`, and the row counts must match [Appendix A](#appendix-a--expected-row-counts).

**Record:** conversion time and Parquet size. How does the size compare with the `.dat` files, and why?

---

## Part 4 — Explore the data (5 min)

Before running the benchmark, look at the data itself. `show_query.py` runs one SQL query and prints the result as a table:

```bash
$PY $T/starter/show_query.py --parquet $T/parquet/sf1 $T/starter/examples/02_small_table.sql
```

The `examples/` folder has eight short queries. Each file starts with a comment explaining what it shows. Run at least **02, 05, 06 and 07**; the others if you have time.

| File | Shows |
|---|---|
| `01_list_tables.sql` | The 25 tables |
| `02_small_table.sql` | All rows of a small dimension table (`ship_mode`, 20 rows) |
| `03_describe_table.sql` | Columns and types of `store_sales` |
| `04_row_counts.sql` | Row count of every table (compare with [Appendix A](#appendix-a--expected-row-counts)) |
| `05_sample_fact_rows.sql` | A few rows of the `store_sales` fact table |
| `06_keys_to_dates.sql` | How a numeric date key turns into a real date through `date_dim` |
| `07_simple_join.sql` | Revenue per item category in 2000: a fact table joined with two dimension tables |
| `08_nulls.sql` | How many values are empty (NULL) in `store_sales` |

You can also type a query directly, or save the full result to a CSV file:

```bash
$PY $T/starter/show_query.py --parquet $T/parquet/sf1 --sql "select * from reason limit 5"
mkdir -p $T/results
$PY $T/starter/show_query.py --parquet $T/parquet/sf1 --out $T/results/categories.csv $T/starter/examples/07_simple_join.sql
```

Other useful options: `--max-rows 50` shows more rows (default 20), and `--csv $T/data/sf1 --kit $T/DSGen-software-code-4.0.0/tools` reads the `.dat` files instead of Parquet.

**Exercise (optional, in class or at home):** write your own file `$T/starter/examples/09_top_states.sql` that returns the **5 US states with the most customers**.
Hint: a customer's current address is `customer.c_current_addr_sk`, which is a key into `customer_address`; the state is `customer_address.ca_state`.

---

## Part 5 — Generate and run the 99 queries (15 min)

### 5.1 Fix the query templates

The v4.0.0 templates need fixes before `dsqgen` can generate the queries and before DuckDB can run all of them.
The script works on a copy; the kit's own templates stay unchanged.

```bash
bash $T/starter/fix_templates.sh $T/DSGen-software-code-4.0.0 $T/query_templates
```

Open `fix_templates.sh` and read what it changes. You will need to explain two of these fixes in your report.

### 5.2 Generate the queries for 1GB

```bash
cd $T/DSGen-software-code-4.0.0/tools
mkdir -p $T/queries/sf1
./dsqgen -DIRECTORY $T/query_templates -INPUT $T/query_templates/templates.lst \
  -DIALECT netezza -SCALE 1 -OUTPUT_DIR $T/queries/sf1 -QUIET Y
```

A warning that the scale factor is not valid for publication is expected.
This writes one file, `query_0.sql`, with all 99 queries in a shuffled order and with filter values chosen at random for this scale.
The `netezza` dialect is used because it writes row limits as `LIMIT n`, which DuckDB understands.

### 5.3 Split them into one file per query

```bash
$PY $T/starter/split_queries.py $T/queries/sf1/query_0.sql $T/queries/sf1/split
ls $T/queries/sf1/split | head -3     # query01.sql query02.sql query03.sql
```

Files are named after the TPC-DS query number. Queries 14, 23, 24 and 39 contain two SQL statements each.

To see what a benchmark query returns, you can run any of these files with `show_query.py`, for example:

```bash
$PY $T/starter/show_query.py --parquet $T/parquet/sf1 $T/queries/sf1/split/query03.sql
```

### 5.4 First run

```bash
$PY $T/starter/run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split \
  --out $T/results/sf1_first.csv --threads 4
```

**Check:** `OK: 99/99`. Several queries return 0 rows at 1GB — that is expected, because the randomly chosen filter values often match nothing in a small data set.
Most queries end with `LIMIT 100`, which is only a maximum: a query can return fewer rows when fewer match.

The results file has one line per query: rows returned, time of each run, median time, and any error.

---

## Part 6 — Experiments (30 min)

Before measuring: plug in your laptop, close other heavy applications, and don't use the machine while a measurement runs.

### A. Repeated runs — required (10 min)

Run the full set three times in the same process:

```bash
$PY $T/starter/run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split \
  --out $T/results/sf1_t4_runs3.csv --threads 4 --runs 3
```

Compare run 1 with runs 2 and 3 (the script prints the total of each run).

- Is the first run slower? By how much?
- What could be cached after the first run, and where (operating system, DuckDB)?
- Why do benchmarks usually report the **median** of several runs rather than a single run?

### B. Thread scaling — required (20 min)

Run the full set with 1, 2 and 4 threads (add your core count if you have more than 4):

```bash
for t in 1 2 4; do
  $PY $T/starter/run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split \
    --out $T/results/sf1_t$t.csv --threads $t --runs 3
done
```

For each thread count *t*, take the **sum of medians** printed by the script as the total time `T(t)`, and compute:

- the speedup `S(t) = T(1) / T(t)` for the whole set;
- the same speedup for queries **14, 23 and 67** alone (use the `median_s` column of the CSV files);
- the parallel fraction *p* from Amdahl's law, `S = 1 / ((1 − p) + p / t)`, which gives `p = (1 − 1/S) / (1 − 1/t)`.

| Threads *t* | T(t), whole set (s) | S(t) | *p* | q14 (s) | q23 (s) | q67 (s) |
|---|---|---|---|---|---|---|
| 1 | | 1.00 | — | | | |
| 2 | | | | | | |
| 4 | | | | | | |

Questions:

- Is the speedup close to linear? Why or why not?
- At 1GB most queries take a few milliseconds. What fraction of that time is not query processing (parsing, planning, opening files, Python)?
- Do the heavy queries (14, 23, 67) scale better than the whole set? Why?

### C. Raw text vs Parquet — optional

Run a few queries directly on the `.dat` files, then on Parquet:

```bash
$PY $T/starter/run_queries.py --csv $T/data/sf1 --kit $T/DSGen-software-code-4.0.0/tools \
  --queries $T/queries/sf1/split --out $T/results/sf1_csv.csv --threads 4 --runs 3 --only 3,7,42,52,55,67
$PY $T/starter/run_queries.py --parquet $T/parquet/sf1 \
  --queries $T/queries/sf1/split --out $T/results/sf1_pq.csv --threads 4 --runs 3 --only 3,7,42,52,55,67
```

Explain the difference in time using what you know about column storage, compression and parsing.

### D. Query plan — optional

```bash
$PY $T/starter/run_queries.py --parquet $T/parquet/sf1 --queries $T/queries/sf1/split --explain 67
```

Where does query 67 spend its time? Which operators are the most expensive, and how many rows flow through them?

---

## Part 7 — Discussion (10 min)

- Which step of the pipeline took the longest, and what limited it (CPU, disk, memory)?
- Your measurements come from one machine. What changes when the same data and queries run on **two** machines? What new costs appear?
- Which of your results do you expect to change most in Lab 2?

---

## Take-home work

**Deadline:** hand in the report (PDF) by the **second class after this lab**.

1. **Generate SF10** — same command as Part 2 with `-SCALE 10` and `-DIR $T/data/sf10`.
   Expect roughly 6–12 minutes and 11 GB.
2. **Convert to Parquet** — `--src $T/data/sf10 --dst $T/parquet/sf10`.
   Expect about a minute and 2.7 GB. Check the counts against [Appendix A](#appendix-a--expected-row-counts)
   (you can use `show_query.py` with `examples/04_row_counts.sql`).
3. **Generate and split the SF10 queries** — Part 5.2 and 5.3 with `-SCALE 10` and `sf10` folders
   (do not run `fix_templates.sh` again; reuse `$T/query_templates`).
4. **Repeat experiments A and B at SF10.**

   > **Memory warning for 8 GB machines.** At SF10, running the whole set several times in one process
   > used up to about 8.3 GB of memory in our tests, even with DuckDB's memory limit set lower.
   > On an 8 GB machine, use `--runs 1` and run the command three times instead (one result file per run),
   > close other applications, and mention this in your report.
5. **Keep `$T/parquet/sf10` and `$T/queries/sf10` for Lab 2.** After checking the row counts you may delete `$T/data/sf10` to free 11 GB, unless told otherwise.

### Report (maximum 4 pages, PDF)

1. **Machine** — the table from section 0.5.
2. **Pipeline** — generation time, conversion time and sizes (`.dat` and Parquet) for SF1 and SF10, and confirmation that all row counts matched.
3. **Data model** — in a few sentences, what a fact table and a dimension table are in TPC-DS, with one example of each from Part 4.
4. **Query fixes** — explain two of the fixes applied by `fix_templates.sh`: what failed and why.
5. **Experiment A** — run times at SF1 and SF10; your explanation of the differences between runs.
6. **Experiment B** — speedup plot (threads vs speedup, one line per scale, plus the ideal line) and the Amdahl estimate of *p* for each scale; explain the differences between SF1 and SF10.
7. **From 1GB to 10GB** — how much longer did the whole set take? Why is the ratio not exactly 10? Note that `dsqgen` picks different filter values for each scale, so a query at SF10 is not exactly the same query on more data.
8. **Optional parts** — the exercise from Part 4, and experiments C and/or D, if you did them.
9. **Prediction for Lab 2** — one paragraph: what do you expect when the workload runs on two machines, and why?

### Rules for TPC-DS material

- Download the kit yourself and follow its license. Do not publish the kit, the generated data or the query files.
- Your numbers are **derived from TPC-DS**: they are not official TPC-DS results and must not be compared with published TPC-DS results.

---

## Appendix A — Expected row counts

| Table | SF1 | SF10 |
|---|---:|---:|
| call_center | 6 | 24 |
| catalog_page | 11,718 | 12,000 |
| catalog_returns | 144,067 | 1,439,749 |
| catalog_sales | 1,441,548 | 14,401,261 |
| customer | 100,000 | 500,000 |
| customer_address | 50,000 | 250,000 |
| customer_demographics | 1,920,800 | 1,920,800 |
| date_dim | 73,049 | 73,049 |
| dbgen_version | 1 | 1 |
| household_demographics | 7,200 | 7,200 |
| income_band | 20 | 20 |
| inventory | 11,745,000 | 133,110,000 |
| item | 18,000 | 102,000 |
| promotion | 300 | 500 |
| reason | 75 | 75 |
| ship_mode | 20 | 20 |
| store | 12 | 102 |
| store_returns | 287,514 | 2,875,432 |
| store_sales | 2,880,404 | 28,800,991 |
| time_dim | 86,400 | 86,400 |
| warehouse | 5 | 10 |
| web_page | 60 | 200 |
| web_returns | 71,763 | 719,217 |
| web_sales | 719,384 | 7,197,566 |
| web_site | 30 | 42 |

---

## Appendix B — Known issues in the kit, and how this lab handles them

| Problem | Symptom | Handled by |
|---|---|---|
| Build supports only Linux-like systems; macOS lacks `values.h` and `malloc.h` | `'values.h' file not found` | Part 1: `OS=LINUX` and placeholder headers in `macstub/` |
| Old C code rejected by modern compilers | Errors about implicit declarations or integer/pointer conversions; on Linux also "multiple definition" link errors | Part 1: the `-Wno-…` flags and, on Linux, `-fcommon` |
| Folder name with spaces | Build fails with `no such file or directory` on part of the path | Section 0.2: short path without spaces |
| `dsdgen` arguments longer than 200 characters | `dsdgen` crashes (segmentation fault) | Short paths |
| `dsqgen` file paths longer than about 80 characters | `ERROR: Open failed on '...'` with a truncated path | Short paths |
| Dialect files don't define `_BEGIN` / `_END` | `Substitution '_END' is used before being initialized` | `fix_templates.sh`, fix 1 |
| `cast(...) + 30 days` is not valid DuckDB SQL | `syntax error at or near "days"` (15 queries) | `fix_templates.sh`, fix 2 |
| `at` and `returns` are reserved words in DuckDB | Syntax errors in queries 90 and 77 | `fix_templates.sh`, fixes 3 and 6 |
| Query 30 uses a column name that `tpcds.sql` spells differently | `Referenced column "c_last_review_date_sk" not found` | `fix_templates.sh`, fix 4 |
| Ambiguous `ORDER BY` columns | `Ambiguous reference to column name` in queries 58 and 72 | `fix_templates.sh`, fix 5 |
| The `ansi` dialect writes `select top n` | Syntax errors on most databases other than SQL Server | Part 5.2: `-DIALECT netezza` (uses `LIMIT n`) |

If you hit a problem that is not in this table, write down the exact error message and ask.

---

## Appendix C — Typical durations

These are estimates for a 4-core, 8 GB laptop. Your machine may be faster or slower.

| Step | SF1 | SF10 |
|---|---|---|
| Data generation (4 processes) | about 1–2 min | about 6–12 min |
| Parquet conversion | under 30 s | about 1 min |
| One run of the 99 queries (4 threads) | about 5–15 s | about 1–2 min |
| Disk: `.dat` + Parquet | 1.2 GB + 0.3 GB | 11 GB + 2.7 GB |
| Peak memory while running queries | about 1–2 GB | 6–8 GB or more |
