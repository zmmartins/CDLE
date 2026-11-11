# CDLE — How to hand in the Lab 4 report

`SUBMISSION.md` (in the report kit) explains how every lab report is written, turned into a PDF and packed. Follow it, with `lab4`
where it says `labN`. This file adds what is particular to Lab 4.

**Deadline:** Sunday 15 November 2026, 23:59 (Lisbon time). One zip per group on Moodle, by either student.

**Answers:** every question of the handout has an identifier (A1Q1 … B5Q5, S1–S2, D1–D4). Start each answer with it. The section `# Discussion` (D1–D4) goes before `# Limits`;
if you unzipped this report part before 6 November, add that section yourself.

## What goes in the zip

`pack.py` builds it and checks it:

```bash
cd ~/tpcds/report
bash make_pdf.sh lab4                    # writes report_lab4.pdf
python3 pack.py lab4 51234 51987         # your student numbers; one number if you work alone
```

| In the zip | What it is | Required |
|---|---|---|
| `report_lab4.pdf` (and `report_lab4.md`) | The report: at most 4 pages, the sections of the handout's "Report" list, with the answers by identifier (A1Q1 … D4) | yes |
| `ai-log-lab4.md` | The AI log, entry *Session 04*: the brief as sent, the answer, the audit table, what you changed | yes |
| `evidence/lab4/` with `INDEX.md` | The raw outputs your numbers come from, each listed in `INDEX.md` | yes |
| `figures/lab4_*.png` | Your figures (at least the speedup of the take-home work); `pack.py` takes the ones `report_lab4.md` includes | yes |
| `code/pipeline.py`, `code/tests/`, `code/jobs.py` | Your pipeline (Part C), your tests, and your `map_product` (A.3) | yes, added by `pack.py` |

**Code:** you do not copy it anywhere. `pack.py` takes `pipeline.py`, `tests/` and `jobs.py` from your `~/tpcds/lab4/` folder, as they are when you run it.

## Evidence

From Lab 3 on, the evidence is required. In Lab 4 the scripts write most of it for you: every run of a lab script appends its output
to `evidence/lab4/<script>.txt`, with the date and the command.

| File | Comes from | Backs |
|---|---|---|
| `make_logs.txt` | `make_logs.py` | the data (section 0.4) |
| `hello.txt` | `hello.py` | Machine and Docker |
| `mapreduce.txt`, `check.txt` | `mapreduce.py`, `check.py` | Part A; every SAME / DIFFERENT |
| `wordcount_rdd.txt`, `wordcount_df.txt` | the word counts | B.1, B.2, B.4, the scaling table |
| `lazy.txt`, `partitions.txt`, `partition_days.txt` | `lazy.py`, `partitions.py`, `partition_days.py` | B.3, B.5, B.6 |
| `pipeline.txt`, `audit.txt` | `pipeline.py`, `audit.py` | Part C |

Add yourself what the scripts do not write, and list it in `INDEX.md` too:

- the output of `pytest`, before and after the assistant's code (C.1, C.2): `lab -m pytest | tee -a ~/tpcds/report/evidence/lab4/pytest.txt`;
- what you read in the Spark UI (stages, tasks, shuffle, the task list of B.4): a screenshot (PNG) or a short note per table.

Keep only the runs your tables use: if a file holds many old runs, say in `INDEX.md` which ones (the date in each header).

**A summary, without copying anything by hand (optional, recommended; 6 Nov).** `python3 ~/tpcds/lab4/evidence_summary.py` reads the files of `evidence/lab4/`
and writes `evidence/lab4/summary.md`: one short block per run, in date order, with its date, its command and its key lines exactly as the script printed them
(a plan becomes one line that counts its operators). It needs no container. In `summary.md`, replace `ID?` in the title of each block your report uses with what
it backs (`A1Q1`, `B5 table` …) and delete the others; run it again after new runs, and it adds only those. Every number of your report should then appear, as it is,
in `summary.md` (or in the `.txt` files, or in a screenshot listed in `INDEX.md`). If your starter is older than 6 November, take it from the new starter
without touching your work: `cd ~/tpcds && unzip -o lab4_starter.zip lab4/make_logs.py lab4/evidence_summary.py`. The `INDEX.md` of this report part has one entry per table (6 Nov); if you already had one, `unzip -n` adds the same
entries as `evidence_lab4_skeleton.md` in `~/tpcds/report/`, to copy from.

## Before you pack

- `pack.py` takes your code as it is now: if you changed `pipeline.py` after the audit, the audit must describe the version you hand in.
- Your tests must pass with your `pipeline.py` (`lab -m pytest`). If one does not, say why in the report.
- Do not put generated data in the zip: no logs, no Parquet, no `/data` outputs (the logs are generated from TPC-DS data).
