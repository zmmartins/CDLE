# AI session 02 — partitioning schemes for `store_sales`

Fill in every `<...>` with your own measurements from Part A, then give the whole brief to the assistant.
Copy the brief, the answer and what you did with it into `ai-log-lab2.md` (the template is in the report kit).

---

## Role

You are a distributed-data architect and a sceptical reviewer of your own proposals.

## Task

Propose **3 partitioning schemes** for the table `store_sales` described below, then attack each of them:
say when it fails.

## Context

- TPC-DS `store_sales`, scale factor 10: 28,800,991 rows. Columns available:
  `ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk, ss_quantity, ss_sales_price, ss_net_paid, ss_net_profit`.
- The cluster has 8 nodes with 1 CPU each and one partition per node (8 partitions).
- What I measured (rows per partition, 8 partitions):

  | Rule | max/mean | Note |
  |---|---|---|
  | `hash(ss_ticket_number)` | <...> | |
  | `hash(ss_customer_sk)` | <...> | <share of the most frequent key> |
  | `hash(ss_store_sk)` | <...> | <number of distinct values> |
  | range on `ss_sold_date_sk` | <...> | |

- The most frequent values of each candidate key: <paste the last line of skew.py for each key>
- The queries that matter (all three, equally):
  1. top 10 customers by total amount paid (group by `ss_customer_sk`);
  2. per store: number of lines, amount paid, average price (group by `ss_store_sk`);
  3. join with `store_returns` on `(ticket_number, item_sk)`, which is partitioned by `hash(sr_ticket_number)`.

## Constraints

- A scheme must be one SQL expression (DuckDB syntax) that gives a non-negative integer for each row;
  the partition is that value mod 8.
- No sampling at query time, no extra tables.

## Output

For each scheme:

1. the SQL expression;
2. the predicted `max/mean` of rows per partition, as a **range**, with the assumptions behind it;
3. for each of the three queries: which plan is correct (a local answer per node that is only concatenated,
   partial aggregates that are merged, a co-located join, or a broadcast join) and why;
4. one situation in which the scheme fails.
