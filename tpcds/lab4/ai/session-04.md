# AI session 04 — a Spark pipeline from a spec and failing tests

The order matters, and it is the point of the session (deck 4, slides 28–29):

1. **Spec first** (C.1). Complete the brief below: every `<...>` is yours to fill in.
2. **Tests before the code** (C.1). Add at least 4 tests to `tests/test_pipeline.py`, and run them: with the skeleton of `pipeline.py`, **every test must fail**.
3. **Generate** (C.2). Give the assistant the whole brief, your tests and the skeleton `pipeline.py`. Put its answer in `pipeline.py`, unchanged, and run the tests.
4. **Audit** (C.3). The checklist of the handout, `lab audit.py`, and the right answer: `lab check.py product_day …`.

Copy the brief, the answer and what you did with it into `ai-log-lab4.md` (the template is in the report kit), entry *Session 04*.
If you change the code yourself, say what you changed and why. If you ask the assistant again, log each round.

---

## Role

You are a senior Spark engineer and a sceptical reviewer of your own code.

## Task

Write the body of `daily_product_counts(lines)` in `pipeline.py` (attached): the number of views of each product page, per day,
from a web server's access log.

## Context

- Input: `lines`, a DataFrame with one string column, `value`: one line of the log per row (what `spark.read.text` gives).
  The real logs are <...> lines (SF1) and <...> lines (SF10), in 4 and 16 files.
- The format is Apache's "combined" log format. Three lines of the real log:
  ```
  <paste a product page line from tests/sample.log>
  <paste a search line>
  <paste a health check line>
  ```
- The log is not clean. I found these kinds of dirty lines in `tests/sample.log`: <list what you found, with one example each>.
- Runs on Spark 3.5.9 (PySpark), on a standalone cluster of 4 executors with 1 core and 768 MB each.
  The tests use a local SparkSession, with `spark.sql.session.timeZone` set to UTC.

## Constraints

- DataFrame API only: no RDDs, no Python UDFs, no `collect()`, `toPandas()`, `take()` or `count()` in the function.
- The function returns a DataFrame and does not write anything; `main()` writes it.
- Output columns, in this order: `product` (string, upper case), `day` (date, UTC), `n` (bigint).
- Count only: <your rule: which method, which status, which paths>. Lines that cannot be parsed are <...>.
- The tests in `tests/test_pipeline.py` (attached) must pass, unchanged.

## Output

1. The complete `pipeline.py`.
2. Your **assumptions**, numbered: everything you decided that the spec did not say.
3. Two risks of your code on the real logs (at SF10), and how I could detect each one.
4. What `explain()` should show for your function, and why.
