---
title: "CDLE — Lab 1 — TPC-DS on a Single Machine"
author:
  - "First Last (12345)"
  - "First Last (12345)"
---

<!--
How to use this file (comments like this one do not appear in the PDF):
- Replace the two author lines with your names and student numbers; delete the second line if you work alone.
- Keep the section titles and their order: they are the "Report" list of the handout, and the checker looks for them.
- The hint under each title is a comment too: delete it when you write the section, or leave it (it is not printed).
- Maximum 4 pages in this format (bash make_pdf.sh lab1). Tables count; choose what to show.
- Figures: put PNG files in figures/ and write ![Caption](figures/lab1_speedup.png){width=80%}
- Every number must come from your own runs: the files in evidence/lab1/ back them, listed in evidence/lab1/INDEX.md (not in this report).
-->

# Machine

<!-- The table from section 0.5 of the handout. -->

# Pipeline

<!-- Generation time, conversion time and sizes (.dat and Parquet) for SF1 and SF10, and confirmation that all row counts matched. -->

# Data model

<!-- In a few sentences, what a fact table and a dimension table are in TPC-DS, with one example of each from Part 4. -->

# Query fixes

<!-- Explain two of the fixes applied by fix_templates.sh: what failed and why. -->

# Experiment A

<!-- Run times at SF1 and SF10; your explanation of the differences between runs. -->

# Experiment B

<!-- Speedup plot (threads vs speedup, one line per scale, plus the ideal line) and the Amdahl estimate of p for each scale; explain the differences between SF1 and SF10. -->

# From 1GB to 10GB

<!-- How much longer did the whole set take? Why is the ratio not exactly 10? dsqgen picks different filter values for each scale. -->

# Optional parts

<!-- The exercise from Part 4, and experiments C and/or D, if you did them. Write 'Not done.' otherwise. -->

# Prediction for Lab 2

<!-- One paragraph: what do you expect when the workload runs on two machines, and why? -->
