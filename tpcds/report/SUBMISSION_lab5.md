# CDLE — How to hand in the Lab 5 report

`SUBMISSION.md` (in the report kit) explains how every lab report is written, turned into a PDF and packed. Follow it, with `lab5`
where it says `labN`. This file adds what is particular to Lab 5.

**Deadline:** Sunday 29 November 2026, 23:59 (Lisbon time). One zip per group on Moodle, by either student.

**Length:** **3 pages at most** for this lab (`pack.py` checks the limit of 4 that is common to every lab; for Lab 5 the limit is 3). Answer every question of the handout (J1Q1–J3Q4, D1–D3), each answer starting with its identifier.

## What goes in the zip

`pack.py` builds it and checks it:

```bash
cd ~/tpcds/report
bash make_pdf.sh lab5                    # writes report_lab5.pdf
python3 pack.py lab5 51234 51987         # your student numbers; one number if you work alone
```

| In the zip | What it is | Required |
|---|---|---|
| `report_lab5.pdf` (and `report_lab5.md`) | The report: at most 3 pages, the sections of the handout's "Report" list, with the answers by identifier | yes |
| `ai-log-lab5.md` | The AI log, entry *Session 05*: the brief as sent, the hypotheses, each change and its verdict | yes |
| `evidence/lab5/` with `INDEX.md` | The raw outputs your numbers come from, each listed in `INDEX.md` | yes |
| `code/jobs/` | Your three jobs, with your changes | yes, added by `pack.py` |

**Code:** you do not copy it anywhere. `pack.py` takes the folder `jobs/` from `~/tpcds/lab5/`, as it is when you run it: the fixed versions.

## Evidence

`clinic.py` writes it for you, on every run, in `evidence/lab5/`:

- `clinic.txt`: the whole output of every run, with the date and the command (the raw evidence);
- `summary.md`: one short block per run, with the date, the command, the version of the job file, the plan in one line, and the key lines of the output
  (time, longest stage, cache, result). In the title of each block your report uses, replace `ID?` with its name in the table of `INDEX.md` (`Job 1 baseline`, `Job 1 after`, …): the table is already filled in;
  delete the blocks of the runs you do not use. Do not edit the lines inside the blocks.

**The rule:** every number of your report must appear, as it is, in `summary.md` or `clinic.txt`, or in a screenshot listed in `INDEX.md`.

Add yourself at least one screenshot (PNG) of the Spark UI that backs a verdict, named after what it backs (`J2Q1_tasks.png`, `J3Q2_storage.png`),
and list it in `INDEX.md`. `hello.py` writes `hello.txt`.

## Before you pack

- Every job in `jobs/` must give `SAME as the reference` as it is now (`lab clinic.py jobN`). If one does not, say why in the report.
- Do not put generated data in the zip: no Parquet, no `/data` outputs.
