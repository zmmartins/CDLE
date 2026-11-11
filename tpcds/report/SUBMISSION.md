# CDLE — How to hand in a lab report

Every lab report is handed in the same way: **one zip per group on Moodle**, built by `pack.py` from this folder.

| Report | Deadline (Lisbon time) |
|---|---|
| Lab 1 | Sunday 1 November 2026, 23:59 |
| Lab 2 | Sunday 1 November 2026, 23:59 |
| Lab 3 | Sunday 8 November 2026, 23:59 |

## 1. Set up

Two kinds of zip, both unzipped in `~/tpcds` (they all go into `~/tpcds/report/`):

```bash
cd ~/tpcds
unzip report_kit.zip           # once: the tools and the common format
unzip -n lab1_report_template.zip       # one per lab, from that lab's Moodle page: the files of that lab's report
```

Always use `unzip -n` for the report parts: `-n` never replaces a file, so unzipping a part again cannot erase the report you are writing.

**The kit** (`report_kit.zip`, once for the semester):

| File or folder | What it is |
|---|---|
| `make_pdf.sh` | Turns `report_labN.md` into `report_labN.pdf` in the common format |
| `report.yaml`, `report.typ` | The common format (A4, 11 pt, 2 cm margins). Do not change them. |
| `pack.py` | Checks your report and builds the zip |
| `figures/` | Your plots (PNG or SVG), for every lab |

**The report part of each lab** (`labN_report_template.zip`, published with the lab): it only adds new files, so it never overwrites your work.

| File | What it is |
|---|---|
| `report_labN.md` | The report: the sections of the handout's "Report" list, ready to fill |
| `ai-log-labN.md` | The AI log of the lab, if the lab has an AI session (Lab 1 has none) |
| `evidence/labN/INDEX.md` | Where you list the raw outputs your numbers come from |
| `labs/labN.json` | What `pack.py` checks for this lab (sections, AI sessions, code to hand in). Do not change it. |

## 2. Write the report

Write in `report_labN.md`. The comments under each title (`<!-- ... -->`) say what the handout asks for in that section; they are not printed.

- **Header:** replace the two author lines with your names and student numbers, e.g. `"Ana Silva (51234)"`. Delete the second line if you work alone.
- **Sections:** keep the titles and their order. The report is marked section by section, in the handout's order.
- **Length:** at most **4 pages** in the common format. Choose what to show: you do not have to copy every table of the handout, but every
  question must be answered. Lab 2 is the tightest.
- **Figures:** save them in `figures/` with the lab in the name, and include them with
  `![Speedup of q3/colocated](figures/lab2_speedup.png){width=70%}`.
- **Numbers:** every number must come from your own runs (see section 4).

You may write the report in another editor (Word, Google Docs, LaTeX) instead. Then the PDF must follow the same format
(A4, 11 pt body text, 2 cm margins, the same numbered section titles) and be saved here as `report_labN.pdf`; keep `report_labN.md`
with at least the header, or `pack.py` will warn you.

## 3. Make the PDF

```bash
cd ~/tpcds/report
bash make_pdf.sh lab1          # writes report_lab1.pdf
```

It needs Docker (as in Labs 2 and 3); the first run downloads the `pandoc/typst` image, about 370 MB. On Windows, run it inside WSL.
Open the PDF and check it: tables, figures, page count.

## 4. Evidence

Put in `evidence/labN/` the raw outputs your tables come from, and list each file in `evidence/labN/INDEX.md` with the table it backs.
The index is not part of the report and does not count for the 4 pages.

| Lab | What to put there |
|---|---|
| Lab 1 | The CSV files of `run_queries.py` from `$T/results/` (`sf1_t4_runs3.csv`, `sf1_t1.csv` … and the SF10 ones) |
| Lab 2 | The terminal output of the runs in your tables, copied into text files (for example `partB_b1.txt`) |
| Lab 3 | The terminal output of the runs in your tables (see the tip below) |

For **Labs 1 and 2** the evidence is recommended, not required: the scripts of those labs did not save their output, and you do not need
to run the cluster again only to produce it. From Lab 3 on it is required.

Tip for Lab 3 and later: save the output while you run, with `tee`:

```bash
lab read_bench.py formats --query sum1 --cold | tee -a ~/tpcds/report/evidence/lab3/partA.txt
```

## 5. AI log

Each lab with an AI session has its own log: write in it the brief you sent, the answer, and what you did with it.
`pack.py` puts the log of the lab in the zip.

| Lab | File | Entries |
|---|---|---|
| Lab 1 | — | none (Lab 1 has no AI session) |
| Lab 2 | `ai-log-lab2.md` | Session 02 |
| Lab 3 | `ai-log-lab3.md` | Session 03a (Dynamo) and Session 03b (Bigtable) |

## 6. Code

If the lab asks you to hand in code, `pack.py` takes it from the lab's own folder; you do not copy it anywhere.
Lab 2: your `~/tpcds/lab2/plans.py`, with your merge step for `q2`. Labs 1 and 3: no code.

## 7. Check and pack

```bash
cd ~/tpcds/report
python3 pack.py lab1 51234 51987     # your student numbers; one number if you work alone
```

`pack.py` checks the report (sections, page count, header), the lab's AI log, the evidence and the code, and writes `lab1_51234_51987.zip`.
**ERROR** means the zip was not written; **WARNING** means it was written, but you should fix the problem if you can.
Upload the zip to the lab's assignment on Moodle: **one submission per group**, by either student.

For Labs 1 and 2, format warnings (a section out of place, a missing evidence index) are pointed out in the marking but not penalised.

## Rules

- Do not put TPC-DS data, query files or kit files in the zip (`pack.py` refuses `.dat`, `.parquet`, `.tpl` and `queryNN.sql` files).
- Your numbers are derived from TPC-DS: they are not official TPC-DS results.
- Be ready to explain any number of your report in class.
