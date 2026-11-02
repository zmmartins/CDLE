# CDLE — Computação de Dados em Larga Escala

ISEL · MEIM · 2026/27

A single Git repository for lectures, study notes, reproducible laboratory code, the weekly laboratory portfolio, and the semester project. The layout deliberately preserves the `~/tpcds` paths expected by the instructor's handouts.

## Quick navigation

| Where | Purpose |
|---|---|
| `materials/slides/` | Original lecture decks, consistently named by week |
| `materials/lab-handouts/` | Original Markdown lab instructions |
| `materials/references/` | Background documents from Moodle |
| `docs/study/` | Your lecture notes, concept explanations, and revision questions |
| `docs/labs/` | Personal lab notes and experiment summaries |
| `starter/` | Lab 1 starter scripts (the expected runtime path) |
| `lab2/`, `lab3/`, `lab4/` | Lab source code, tests, Docker and AI session files |
| `report/` | The instructor's root-level lab-report kit, reports, AI logs and evidence |
| `project/` | Semester project, separate from weekly lab work |
| `results/` | Reproducible, small measurement summaries (only when needed) |
| `data/`, `parquet/`, `venv/`, Docker volumes | Local runtime data; do not commit |

## Environment: one canonical copy

Keep the canonical version of the code in this folder. If the original handouts expect `~/tpcds` and that path **does not already exist**, create a symlink:

```sh
ln -s "$(pwd -P)" "$HOME/tpcds"
```

Run that command only when standing in the CDLE root. If `~/tpcds` already exists, **inspect it first** instead of overwriting, deleting, or blindly linking anything.

Then the handouts' expected commands (`export T=~/tpcds`, `docker compose -f "$T/lab4/compose.yaml" ...`) use this repository without changing the course's Docker source paths.

Note: Lab 1 builds a third-party, licensed TPC-DS kit at `$T/DSGen-software-code-4.0.0`; keep that kit outside Git. Lab 1 produces SF1/SF10 Parquet data reused by Labs 2–4. The report kit in `$T/report/` is also part of the expected lab runtime structure.

## Git policy

Commit lecture notes, original handouts (subject to permission to redistribute), code, tests, report sources, small datasets used as test fixtures, AI disclosure logs, and selected report evidence. Never commit generated SF1/SF10 data, private information, credentials, environment folders, or licensed TPC-DS binaries. Keep the remote **private** unless course materials may legally be redistributed.

Example commit cadence:

```sh
git add docs/study/week-02-distributed-principles.md
git commit -m "docs(study): summarize partitioning and replication"

git add lab4/jobs.py lab4/tests/
git commit -m "feat(lab4): add product-view mapper and tests"

git add report/report_lab4.md report/ai-log-lab4.md
git commit -m "docs(lab4): draft report and disclose AI use"
```

## Important

The instructor provides a separate `report_kit.zip` / submission instructions. Do not invent their files or run packaging without following the actual kit. In particular, Lab 4 expects its report support files under `~/tpcds/report/`.

See `docs/course/roadmap.md` and `docs/course/assessment.md` for navigation and assessment weights.
