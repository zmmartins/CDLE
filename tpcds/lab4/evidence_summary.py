"""Build evidence/lab4/summary.md from the files the lab's scripts wrote: one short block per run, nothing to copy by hand.

Usage (on your laptop, no container needed):  python3 ~/tpcds/lab4/evidence_summary.py [evidence folder]

Every script of Lab 4 appends its output to ~/tpcds/report/evidence/lab4/<script>.txt, one run after another, each starting
with "=== <date> <command>". This script reads those files and writes summary.md, in date order: for each run, its date, its
command and its key lines, exactly as the script printed them. Two things are shortened: a plan (--explain) becomes one line
that counts its operators, and the result table of audit.py becomes its number of rows; the whole output stays in the .txt files.
Files without "===" lines (for example the pytest.txt you saved with tee) are left out: list them in INDEX.md.

Then, in summary.md, replace "ID?" in the title of each block your report uses with what it backs (A1Q1, B5 table, ...) and
delete the blocks you do not use (keep the command and the date in each title). Run it again after new runs: it only adds the runs made after the last block of summary.md,
so your titles and your deletions are kept.
"""
import glob
import os
import re
import sys

EVIDENCE = (sys.argv[1] if len(sys.argv) > 1 else
            "/evidence" if os.path.isdir("/evidence") else os.path.expanduser("~/tpcds/report/evidence/lab4"))
HEADER = re.compile(r"^=== (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)  (.*)$")
OPERATORS = ["SortMergeJoin", "BroadcastHashJoin", "HashAggregate", "Exchange", "Window", "Generate"]


def runs(path):
    """The runs of one evidence file: (date, command, lines)."""
    out, cur = [], None
    for line in open(path, encoding="utf-8", errors="replace").read().splitlines():
        m = HEADER.match(line)
        if m:
            cur = (m.group(1), m.group(2).strip(), [])
            out.append(cur)
        elif cur:
            cur[2].append(line)
    return out


def plan_line(plan):
    """One line for a physical plan: how many of each operator, and whether there is a partial aggregate."""
    tree = []
    for line in plan[1:]:
        if not line.strip():
            break
        tree.append(line)
    tree = "\n".join(tree)
    found = [f"{tree.count(op)} {op}" for op in OPERATORS if tree.count(op)]
    partial = "yes" if any("partial_" in line for line in plan) else "no"
    return f"plan: {', '.join(found) or 'no operator found'}; partial aggregate: {partial}   (whole plan in the .txt file)"


def key_lines(lines):
    """The lines of one run worth keeping, as printed; plans and result tables shortened."""
    keep, i = [], 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("Spark application "):
            i += 1
            continue
        if line.startswith("== Physical Plan =="):
            j = i + 1
            while j < len(lines) and not re.match(r"^(wordcount_df |Top 10|Output: |=== )", lines[j]):
                j += 1
            keep.append(plan_line(lines[i:j]))
            i = j
            continue
        if re.match(r"^\s+[A-Z]{16}\s+\d{4}-\d\d-\d\d\s+\d+$", line):
            j = i
            while j < len(lines) and re.match(r"^\s+[A-Z]{16}\s+\d{4}-\d\d-\d\d\s+\d+$", lines[j]):
                j += 1
            keep.append(f"  ({j - i} rows of the result, in the .txt file)")
            i = j
            continue
        keep.append(line)
        i += 1
    while keep and not keep[-1].strip():
        keep.pop()
    return keep


def main():
    target = os.path.join(EVIDENCE, "summary.md")
    every = []
    for path in sorted(glob.glob(os.path.join(EVIDENCE, "*.txt"))):
        for date, command, lines in runs(path):
            every.append((date, command, os.path.basename(path), lines))
    every.sort(key=lambda r: r[0])
    if not every:
        raise SystemExit(f"no runs found in {EVIDENCE}/*.txt (the lab's scripts write them when they run)")

    last = ""
    if os.path.exists(target):
        dates = re.findall(r"^## .*? — .*?, (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)$", open(target, encoding="utf-8").read(), re.M)
        last = max(dates) if dates else ""
    new = [r for r in every if r[0] > last]

    with open(target, "a", encoding="utf-8") as f:
        if not last:
            f.write("# Lab 4: one block per run (written by evidence_summary.py from the files of this folder)\n\n"
                    "Replace `ID?` in the title of each block your report uses with what it backs (A1Q1, B5 table, ...);\n"
                    "delete the blocks of the runs you do not use. Keep the command and the date in each title;\n"
                    "do not edit the lines inside the blocks.\n")
        for date, command, source, lines in new:
            f.write(f"\n## ID? — {command}, {date}\n\nfrom `{source}`\n\n```\n")
            f.write("\n".join(key_lines(lines)) + "\n```\n")
    print(f"{target}: {len(new)} run{'' if len(new) == 1 else 's'} added ({len(every)} in the evidence files)")


if __name__ == "__main__":
    main()
