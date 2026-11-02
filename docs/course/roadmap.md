# CDLE course roadmap — as supplied (Weeks 1–7)

| Week | Slides | Lab | Core topic |
|---|---|---|---|
| 01 | `week-01-landscape.pdf` | Lab 1 | TPC-DS, DuckDB, SF1/SF10, Parquet, benchmark methodology |
| 02 | `week-02-distributed-principles.pdf` | Lab 2 | Partitioning, skew, consistent hashing, replication, quorums and failure |
| 03 | `week-03-distributed-storage-nosql.pdf` | Lab 3 | File formats, Parquet statistics, erasure coding, NoSQL papers |
| 04 | `week-04-mapreduce-spark-core.pdf` | Lab 4 | MapReduce, RDD/DataFrame, Spark, test-first AI pipelines |
| 05 | `week-05-spark-performance.pdf` | Not yet supplied | Shuffles, joins, Spark plans, skew and tuning |
| 06 | `week-06-pipelines-orchestration.pdf` | Not yet supplied | ETL/ELT, Airflow-style DAGs, idempotency, backfills, quality |
| 07 | `week-07-kafka-distributed-logs.pdf` | Not yet supplied | Kafka, topics, partitions, consumers, delivery semantics |

## Lab dependency chain

Lab 1 generates **SF1 and SF10 Parquet datasets**. Labs 2, 3 and 4 consume these exact paths. Labs 2–4 use Docker containers and persist some data in Docker-managed volumes; deleting volumes can destroy data still needed for reports.

## Project

The slides describe a team project assessed through proposal/checkpoints, a running final system, research report, talk and individual defence. The course's chosen project theme and detailed project brief are **not included** in the provided documents; keep `project/` flexible until those are received.

## New week procedure

1. Put the slides in `materials/slides/` and any handout in `materials/lab-handouts/`.
2. Create a study note in `docs/study/` from the existing template.
3. Add a lab note in `docs/labs/` and the instructor's executable folder in the expected runtime location.
4. Track tests, measured results and explanatory evidence, and update the AI log for each assessed deliverable.
5. Commit a coherent, reviewed change with Git.
