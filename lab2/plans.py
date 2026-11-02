"""The queries of Lab 2 and their distributed plans.

A query has a "reference" (plain SQL over the whole table, run on one node to get the correct
answer) and one or more plans. A plan has:

  local      SQL that every worker runs over ITS partitions. {store_sales} and {store_returns}
             stand for the worker's partitions of those tables.
  merge      SQL that the coordinator runs over {partials}: all the workers' answers together.
  broadcast  optional. {"name": ..., "sql": ...}: before the local step, every worker runs this
             SQL, the coordinator collects the answers and sends ALL of them to EVERY worker.
             The local SQL can then read them as {name}.

A plan is only correct under some conditions on how the data is partitioned. The coordinator
does not check those conditions: it runs what you ask and compares the answer with the reference.
"""

PLANS = {
    # ------------------------------------------------------------------------------------------
    "q1": {
        "about": "Top 10 customers by total amount paid",
        "reference": """
            select ss_customer_sk, sum(ss_net_paid) as paid, count(*) as n
            from {store_sales} group by 1 order by paid desc, ss_customer_sk limit 10""",
        "plans": {
            # Every worker sends one row per customer; the coordinator adds them up.
            "partial": {
                "local": """
                    select ss_customer_sk, sum(ss_net_paid) as paid, count(*) as n
                    from {store_sales} group by 1""",
                "merge": """
                    select ss_customer_sk, sum(paid) as paid, sum(n)::bigint as n
                    from {partials} group by 1 order by paid desc, ss_customer_sk limit 10""",
            },
            # Every worker sends only its own top 10.
            "pushdown": {
                "local": """
                    select ss_customer_sk, sum(ss_net_paid) as paid, count(*) as n
                    from {store_sales} group by 1 order by paid desc, ss_customer_sk limit 10""",
                "merge": """
                    select * from {partials} order by paid desc, ss_customer_sk limit 10""",
            },
        },
    },
    # ------------------------------------------------------------------------------------------
    "q2": {
        "about": "Per store: number of sales lines, amount paid and average sales price",
        "reference": """
            select ss_store_sk, count(*) as n, sum(ss_net_paid) as paid, avg(ss_sales_price) as avg_price
            from {store_sales} group by 1 order by 1""",
        "plans": {
            # Every worker computes its own averages; the coordinator averages the averages.
            "naive": {
                "local": """
                    select ss_store_sk, count(*) as n, sum(ss_net_paid) as paid, avg(ss_sales_price) as avg_price
                    from {store_sales} group by 1""",
                "merge": """
                    select ss_store_sk, sum(n)::bigint as n, sum(paid) as paid, avg(avg_price) as avg_price
                    from {partials} group by 1 order by 1""",
            },
            # EXERCISE (Part B.2): the workers send what is needed for a correct average.
            # Write the merge step. It must return the same four columns as the reference.
            "partial": {
                "local": """
                    select ss_store_sk, count(*) as n, sum(ss_net_paid) as paid,
                           sum(ss_sales_price) as price_sum, count(ss_sales_price) as price_cnt
                    from {store_sales} group by 1""",
                "merge": None,  # TODO: replace None with your SQL over {partials}
            },
        },
    },
    # ------------------------------------------------------------------------------------------
    "q3": {
        "about": "Per store: number of returned sales lines and returned amount (a join of two fact tables)",
        "reference": """
            select ss_store_sk, count(*) as returned_lines, sum(sr_return_amt) as returned_amt
            from {store_sales} join {store_returns}
              on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk
            group by 1 order by 1""",
        "plans": {
            # Every worker joins its partitions of the two tables. Nothing but the totals is sent.
            "colocated": {
                "local": """
                    select ss_store_sk, count(*) as returned_lines, sum(sr_return_amt) as returned_amt
                    from {store_sales} join {store_returns}
                      on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk
                    group by 1""",
                "merge": """
                    select ss_store_sk, sum(returned_lines)::bigint as returned_lines, sum(returned_amt) as returned_amt
                    from {partials} group by 1 order by 1""",
            },
            # The smaller table (store_returns) is sent whole to every worker before the join.
            "broadcast": {
                "broadcast": {
                    "name": "all_returns",
                    "sql": "select sr_ticket_number, sr_item_sk, sr_return_amt from {store_returns}",
                },
                "local": """
                    select ss_store_sk, count(*) as returned_lines, sum(sr_return_amt) as returned_amt
                    from {store_sales} join {all_returns}
                      on ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk
                    group by 1""",
                "merge": """
                    select ss_store_sk, sum(returned_lines)::bigint as returned_lines, sum(returned_amt) as returned_amt
                    from {partials} group by 1 order by 1""",
            },
        },
    },
}
