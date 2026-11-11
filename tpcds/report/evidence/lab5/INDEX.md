# Evidence for the Lab 5 report

<!-- How this folder works:
     - clinic.py writes two files here by itself, on every run:
         clinic.txt  the whole output of every run (with its date and command): the raw evidence
         summary.md  one short block per run: the plan in one line, the key lines of the output, the result
     - In summary.md, replace "ID?" in the title of each block your report uses with the name in the table below
       ("Job 1 baseline", "Job 1 after" ...), and delete the blocks of the runs you do not use.
       Do not edit the lines inside the blocks: they are the output of clinic.py, as it printed them.
     - Every number of your report must appear, as it is, in summary.md or clinic.txt, or in a screenshot listed below.
     - The table is already filled in: change it only if your report uses other runs (a second fix, Job 3 round 3, SF1 ...),
       and delete the rows you did not do. -->

| Report | Evidence |
|---|---|
| Machine and Docker | `hello.txt` |
| Job 1, baseline (and J1Q2) | `summary.md`, block "Job 1 baseline"; the plan in `clinic.txt` (run with `--explain`) |
| Job 1, your change (and J1Q3) | `summary.md`, block "Job 1 after"; the plan in `clinic.txt` |
| Job 2, baseline (and J2Q1) | `summary.md`, block "Job 2 baseline"; `J2Q1_tasks.png` (the task list of the longest stage) |
| Job 2, your fix (and J2Q4) | `summary.md`, block "Job 2 after" |
| Job 3, baseline (and J3Q1) | `summary.md`, block "Job 3 baseline" |
| Job 3, round 1 | `summary.md`, block "Job 3 round 1" |
| Job 3, round 2 (if you made one) | `summary.md`, block "Job 3 round 2" |
| J3Q2, the cache | the `cached DataFrame` line of its block; `J3Q2_storage.png` (the Storage tab) |
| The AI session | `ai-log-lab5.md` (not in this folder) |
| Other files of this folder | |
