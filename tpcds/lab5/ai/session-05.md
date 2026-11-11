# AI session 05 — the assistant as a performance consultant (Job 3)

This week the assistant does not write the job: it proposes **hypotheses**, and the clinic's numbers decide which one is right
(deck 5, slides 32–33). You use it on Job 3.

1. **Measure first.** Run `lab clinic.py job3` and keep its whole output.
2. **Ask** (the brief below): paste `jobs/job3_loop.py` and the output of `clinic.py`. Ask for three ranked hypotheses, each with a **prediction**: a number
   that `clinic.py` will print if the hypothesis is true (the time, the tasks, the input MB…).
3. **Verify.** Take the first hypothesis, make the **one** change it proposes in `jobs/job3_loop.py`, and run `lab clinic.py job3` again.
   The result must stay SAME. Compare the numbers with the prediction: **confirmed**, **refuted**, or **partly confirmed**.
4. **Again, if needed.** If the prediction failed, give the assistant the new output and ask why. One change per run.

Copy the brief, the answers and your verdicts into `ai-log-lab5.md` (entry *Session 05*). A refuted hypothesis is a result too: keep it in the log.

---

## Role

You are a Spark performance consultant. You propose hypotheses; you do not claim anything that the numbers below do not show.

## Task

Job 3 (attached: `job3_loop.py`) is slow. Find out why, and how to make it faster **with exactly the same result**.

## Context

- Spark 3.5.9, standalone cluster on one laptop: 4 executors with 1 core and 768 MB each; adaptive query execution (AQE) on, all other settings at their defaults.
- The input: `store_sales`, <...> rows in <...> Parquet files, <...> MB.
- What one run printed (`clinic.py`: the stages, with their tasks, times, input and shuffle):
  ```
  <paste the whole output of lab clinic.py job3>
  ```

## Output

1. Three hypotheses, ranked from the most to the least likely. For each one:
   - the cause, in one sentence;
   - the evidence in the output above that supports it (quote the numbers);
   - **one** change to the code, as a diff;
   - the prediction: what `clinic.py` will print after the change (time, tasks, input MB), as numbers.
2. What you cannot know from this output, and what I should look at in the Spark UI to find out.
