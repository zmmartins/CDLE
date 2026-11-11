"""The jobs of Part A: each one is a map function and a reduce function, nothing else.

  map(line)            -> yields (key, value) pairs; called once for every line of the input
  reduce(key, values)  -> yields (key, result) pairs; called once for every key, with all its values

mapreduce.py does the rest: it splits the input, runs the maps, partitions and sorts the pairs, and runs the reduces.
"""
from logline import parse, product_id, search_words


# --- word count: the words of every search -------------------------------------------------

def map_words(line):
    r = parse(line)
    if r is None:
        return
    for w in search_words(r["path"]):
        yield w, 1


def reduce_sum(key, values):
    yield key, sum(values)


# --- A.3: product page views, per product (write the map function) -------------------------
#
# Count the lines that are a GET of a product page with status 200. Product ids must be counted
# in upper case ('aaaaaaaaokjdaaaa' is the same product as 'AAAAAAAAOKJDAAAA'). Lines that parse()
# cannot read are skipped. Use parse() and product_id() from logline.py, and reduce_sum above.

def map_product(line):
    # TODO (A.3): about 6 lines
    raise NotImplementedError("write map_product in jobs.py (Part A.3)")


JOBS = {
    # name: (map, reduce, combiner: may the reduce also run on the output of each map task?)
    "words": (map_words, reduce_sum, True),
    "products": (map_product, reduce_sum, True),
}
