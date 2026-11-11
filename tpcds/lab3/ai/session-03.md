# AI session 03 — literature triage: Dynamo and Bigtable

This session is take-home work (Lab 3.2). The assistant builds the map in a minute; checking it is
the part that is assessed.

You need the two PDFs of the reading list (week 3): DeCandia et al., *Dynamo: Amazon's Highly Available
Key-value Store*, SOSP 2007, and Chang et al., *Bigtable: A Distributed Storage System for Structured
Data*, OSDI 2006. Do not give the PDFs to the assistant: the point is to see what it claims without them.

Copy the brief, the answer and your verdicts into `ai-log-lab3.md` (the template is in the report kit): one entry *Session 03a — Dynamo* and
one entry *Session 03b — Bigtable* (entries #5 and #6 of the course).

---

## Step 1 — generate: the brief (send it as it is)

> **Role.** You are a storage-systems researcher writing a short survey for graduate students.
>
> **Task.** Build a map of the design space of distributed storage engines, using exactly two systems:
> Amazon's Dynamo (SOSP 2007) and Google's Bigtable (OSDI 2006).
>
> **Output.** A table with one row per design dimension, and one column per system. Use at least
> these dimensions: data model; partitioning; replication; consistency offered to clients; how
> conflicts are handled; the write path (from the client to durable storage); the read path;
> failure detection and membership; recovery after a node is lost; transactions; secondary indexes;
> what the system depends on (other services).
>
> For every cell: (a) one sentence stating the property; (b) the section and page of the paper where
> it is stated; (c) a number from the paper when there is one (a configuration, a latency, a size).
>
> Then list 6 claims of the form "System X guarantees / does / cannot ..." that you consider the most
> important for someone choosing between the two, each with its citation.

## Step 2 — critique: check every claim against the PDF

For every cell of the table and every one of the 6 claims, find the passage in the paper and give a verdict:

| Verdict | Meaning |
|---|---|
| **confirmed** | the paper says it, at the place cited (or you give the right place) |
| **with conditions** | true only under a condition the assistant left out (say which, and cite it) |
| **refuted** | the paper says something different, or the opposite (quote it) |
| **not verifiable** | the paper does not say it either way (for example, it describes a later version, or another system) |

Use this table (one per paper) in `ai-log-lab3.md` and in your report:

| # | Claim (as the assistant wrote it) | Cited | Where it really is (§, page) | Verdict | Quote or reason (one line) |
|---|---|---|---|---|---|
| 1 | | | | | |

Also count, per paper: claims checked, verdicts of each kind, and **citations that were wrong**
(the property may be right while the section or page is wrong).
Pages are those of the PDF in the reading list; a citation by proceedings page is correct if it points to the same place.

## Step 3 — verify: two claims against your own measurements

Choose two claims of the map that an experiment of ours can test, and confront them with your numbers:

- one with a measurement of **Lab 2** (quorums, stale reads, a partition, the ring and virtual nodes);
- one with a measurement of **Lab 3** (layout and the bytes a query reads, sorted data, replication
  against erasure coding, what happens when nodes are lost, repair).

For each: the claim, what our experiment can and cannot show about it (our systems are toys of the
same ideas, not the real Dynamo or Bigtable), the number that decides, and your conclusion.

## Step 4 — what goes in the report (section 5)

- The two triage tables (at most 12 rows each: the most important ones) and the counts of verdicts and of wrong citations.
- Your two verified claims with their numbers.
- One paragraph: which kinds of claim did the assistant get wrong most often (numbers, citations,
  conditions, mixing the two systems, later versions)? What would you check first next time?
