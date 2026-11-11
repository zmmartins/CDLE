# A skeleton for evidence/lab4/INDEX.md (added on 6 Nov; optional)

If your `evidence/lab4/INDEX.md` is still the empty one of the template, copy the entries below into it, or replace it with this file:
`cp ~/tpcds/report/evidence_lab4_skeleton.md ~/tpcds/report/evidence/lab4/INDEX.md`.
If you already wrote your INDEX in another way, keep it: this is a model, not a required format.
Under each entry, write the titles of its blocks in `summary.md` (made by `python3 ~/tpcds/lab4/evidence_summary.py`), or paste the lines of output that hold
its numbers, as the script printed them (copied, not retyped). This file is outside evidence/lab4/, so pack.py does not put it in your zip.

## Machine and Docker

Files: `hello.txt`   Commands: `hello.py`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `executors` and `OK` lines

```

```


## The data (0.4) — the logs

Files: `make_logs.txt`   Commands: `make_logs.py --sf 1`, `make_logs.py --sf 10`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `SF1:` and `SF10:` lines, and the views to count

```

```

## A.1–A.2 — word count by hand, with and without the combiner (SF1 and SF10)

Files: `mapreduce.txt`, `check.txt`   Commands: `mapreduce.py words --sf 1 …` (and `--combiner`, and `--sf 10`), `check.py words …`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `map`, `shuffle`, `reduce` lines of each run, and SAME

```

```


## A.3 — your map function

Files: `mapreduce.txt`, `check.txt`   Commands: `mapreduce.py products --sf 1 --combiner`, `check.py products …`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `map`, `shuffle`, `reduce` lines, and SAME (9,000 products)

```

```


## B.1 — word count with RDDs (and its stages in the UI)

Files: `wordcount_rdd.txt`; screenshot `B1_stages.png`   Commands: `wordcount_rdd.py --sf 1`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `wordcount_rdd SF1` line; the stages of the first job (screenshot)

```

```


## B.2 — word count with DataFrames, and its plan

Files: `wordcount_df.txt`, `wordcount_rdd.txt`   Commands: `wordcount_df.py --sf 1 --explain`, `--sf 10`; `wordcount_rdd.py --sf 10`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `wordcount_df`/`wordcount_rdd` lines, and the `plan:` line of summary.md (or the plan)

```

```


## B.3 — lazy evaluation

Files: `lazy.txt`   Commands: `lazy.py --sf 1`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the six numbered lines and the count

```

```


## B.4 — losing an executor

Files: `wordcount_rdd.txt`, `check.txt`; screenshot `B4_tasks.png`   Commands: `wordcount_rdd.py --sf 1 --slow 3 --partitions 16` (twice), `check.py words …`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the two `wordcount_rdd SF1` lines (executors 4, then 3), SAME; the task list of the first stage (screenshot)

```

```


## B.5 — partitions and tasks

Files: `partitions.txt`   Commands: `partitions.py --max-partition-mb 128 / 32 / 512`, `--repartition 8`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `rule`, `planned partitions` and `rows per partition` lines of each run

```

```


## B.6 — partitions in folders (optional)

Files: `partition_days.txt`; screenshot `B6_scan.png`   Commands: `partition_days.py`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the three `wrote` lines and the three query lines

```

```


## Scaling (take-home) — 1, 2 and 4 executors at SF10

Files: `wordcount_rdd.txt`, `wordcount_df.txt`, `mapreduce.txt`   Commands: `--total-executor-cores 1/2/4` runs (3 each), `mapreduce.py words --sf 10` (and `--combiner`)

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the `wordcount_… SF10` lines of the 18 runs; the `map`/`shuffle`/`reduce` lines at SF10

```

```


## C.1 — your tests, failing with the skeleton

Files: `pytest.txt` (saved with `tee`)   Commands: `lab -m pytest | tee -a ~/tpcds/report/evidence/lab4/pytest.txt`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the last line (`N failed`)

```

```


## C.2 — the assistant's code, the tests again

Files: `pytest.txt`; the session in `ai-log-lab4.md`   Commands: `lab -m pytest` after pasting the assistant's `pipeline.py`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the last line (`N passed`, or which failed)

```

```


## C.3 — the audit

Files: `audit.txt`, `pipeline.txt`, `check.txt`   Commands: `audit.py`, `pipeline.py --sf 1`, `check.py product_day … --sf 1`

Blocks of `summary.md` (their titles), or the lines pasted below as printed: the PASS/FAIL lines, the `plan:` line, and SAME (688,933 rows)

```

```

## Other files of this folder

| File | What it backs |
|---|---|
| | |
