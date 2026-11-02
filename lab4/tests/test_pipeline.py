"""Tests for pipeline.daily_product_counts. Write yours BEFORE the assistant writes the pipeline (Part C.1):
with the skeleton of pipeline.py, every test must fail.

Run them:  dc exec -T client python -m pytest
"""
import datetime
import os

from pipeline import daily_product_counts

SAMPLE = os.path.join(os.path.dirname(__file__), "sample.log")

CLEAN = ('10.159.194.1 - - [25/Sep/2000:06:28:16 +0000] "GET /product/AAAAAAAAFMFAAAAA?qty=19 HTTP/1.1" 200 7077 '
         '"/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"')
SEARCH = ('10.159.194.1 - - [25/Sep/2000:06:26:32 +0000] "GET /search?q=actually+successful HTTP/1.1" 200 9259 '
          '"/" "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)"')


def rows(df):
    """The result as a set of (product, day, n), to compare without caring about the order."""
    return {(r["product"], r["day"], r["n"]) for r in df.collect()}


# --- two examples --------------------------------------------------------------------------------

def test_one_clean_line(lines):
    assert rows(daily_product_counts(lines([CLEAN]))) == {("AAAAAAAAFMFAAAAA", datetime.date(2000, 9, 25), 1)}


def test_a_search_is_not_a_product_view(lines):
    assert rows(daily_product_counts(lines([SEARCH]))) == set()


# --- your tests (at least 4): a line cut short, a line with bytes that are not UTF-8,
#     a product id in lower case, a request that failed (404). Copy the lines from tests/sample.log.
#     To read sample.log as the pipeline will read the real logs:  spark.read.text(SAMPLE)
