# Evidence for the Lab 3 report

<!-- How to use this file:
     - Put in evidence/lab3/ the raw output of your runs (saved with tee, see SUBMISSION.md), and fill in below where each table comes from.
     - Under each entry, paste the two or three lines of output that hold the numbers of that table, as the script printed them
       (copied, not retyped), and the file they come from. Delete the entries you did not do (B.4 if you skipped it, ...).
     - List here every other file of this folder (screenshots, extra outputs), with what it backs. -->

## Machine and Docker

File: …   Command: `docker info | grep -E "CPUs|Total Memory"`

```
(paste here, as printed: the CPUs and the memory of Docker)
```


## A.1 — the five configurations: size and write time

File: …   Command: `lab formats.py --sf 1`

```
(paste here, as printed: the table of configurations, write (s) and MB, and the sizes compared with the CSV)
```


## A.2 — sum1, cold and warm

File: …   Command: `lab read_bench.py formats --query sum1 --cold` and `… --query sum1`

```
(paste here, as printed: the five lines of each table (median, CPU, result) and `results: SAME …`)
```


## A.3 — agg5 and all23

File: …   Command: `lab read_bench.py formats --query agg5` and `… --query all23`

```
(paste here, as printed: the five lines of each table and `results: SAME …`)
```


## B.1 — the footers of sf10 and sorted

File: …   Command: `lab pq_inspect.py sf10/store_sales` (and `--column ss_sold_date_sk`, and `sorted/…`)

```
(paste here, as printed: the line with files, rows and row groups, and the column sizes your table uses)
```


## B.2 — one column or all of them (and B2Q2: the GETs)

File: …   Command: `lab read_bench.py obj sf10/store_sales --query sum1` and `… --query all23`

```
(paste here, as printed: one `run` line, the `median` line and the `front-end` line of each query)
```


## B.3 — month on sf10 and on sorted: prediction and measurement

File: …   Command: `lab pq_inspect.py … --between …` and `lab read_bench.py obj … --query month`

```
(paste here, as printed: `row groups that may hold matching rows`, `bytes of those columns`, and the `median` and `front-end` lines)
```


## B.4 — sorted_rg1m

File: …   Command: `lab load.py --name sorted_rg1m …`, `lab pq_inspect.py sorted_rg1m/…`, `lab read_bench.py obj sorted_rg1m/… --query month` and `--query sum1`

```
(paste here, as printed: the `PUT` line, the row groups, and the `median` and `front-end` lines)
```


## C.1 — the space it takes

File: …   Command: `obj du`

```
(paste here, as printed: the logical MB, the MB stored and the ratio)
```


## C.2 — losing nodes (and C2Q1: the MB read with s2 down)

File: …   Command: `obj verify sf10/store_sales` with all up, with s2, s2+s5, s2+s5+s6 down

```
(paste here, as printed: the `verified`, `front-end`, `read from` and `degraded` lines of each run; what the last run printed)
```


## C.3 — writing with nodes down, and repairing (and C3Q2: MB read per MB written)

File: …   Command: `obj put …` with s2 and with s2+s5 down, `obj status`, `obj repair`, `obj wipe s3`, `obj verify`

```
(paste here, as printed: the `PUT` lines (accepted, refused), `objects with missing chunks`, and the two `repair:` and `read … wrote …` lines)
```


## C.4 — a frozen node

File: …   Command: `lab read_bench.py obj sf10/store_sales --query sum1` with s3 paused, `obj status`, the run after 15 s

```
(paste here, as printed: the three `run` lines, the state of s3 in `obj status`, and the run after 15 s)
```

## Other files of this folder

| File | What it backs |
|---|---|
| | |
