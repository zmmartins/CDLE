"""Shared by skew.py and partition.py: how a row is assigned to one of P partitions.

Three ready-made schemes work on one column (--key):
  hash      partition = hash(key) mod P
  range     P ranges of equal width between the smallest and the largest key
  quantile  P ranges that each hold about the same number of rows
With --expr you write the rule yourself: any SQL expression that gives a non-negative
integer; the partition is that value mod P.

Rows whose partition would be NULL (NULL key in range and quantile) go to partition 0.
"""
import os

# Columns kept in the partitioned copies (a projection, to save disk space).
COLUMNS = {
    "store_sales": ["ss_ticket_number", "ss_item_sk", "ss_customer_sk", "ss_store_sk", "ss_sold_date_sk",
                    "ss_quantity", "ss_sales_price", "ss_net_paid", "ss_net_profit"],
    "store_returns": ["sr_ticket_number", "sr_item_sk", "sr_customer_sk", "sr_store_sk", "sr_returned_date_sk",
                      "sr_return_quantity", "sr_return_amt"],
}


def add_args(ap):
    ap.add_argument("--parquet", required=True, help="Parquet folder from Lab 1 (one subfolder per table)")
    ap.add_argument("--table", default="store_sales", choices=sorted(COLUMNS))
    ap.add_argument("--parts", type=int, default=8, help="number of partitions P (default 8)")
    ap.add_argument("--key", help="partitioning column, used with --scheme")
    ap.add_argument("--scheme", default="hash", choices=["hash", "range", "quantile"])
    ap.add_argument("--expr", help="your own rule instead of --key/--scheme: a SQL expression giving a "
                                   "non-negative integer")
    ap.add_argument("--threads", type=int, help="DuckDB threads (default: all cores)")


def check_args(ap, args):
    if bool(args.key) == bool(args.expr):
        ap.error("give either --key (with --scheme) or --expr")
    if args.parts < 1:
        ap.error("--parts must be 1 or more")


def source(args):
    folder = os.path.join(os.path.expanduser(args.parquet), args.table)
    if not os.path.isdir(folder):
        raise SystemExit(f"folder not found: {folder}")
    return f"read_parquet('{folder}/*.parquet')"


def describe(args):
    if args.expr:
        return f"({args.expr}) mod {args.parts}"
    return f"{args.scheme}({args.key}), {args.parts} partitions"


def part_expr(con, args):
    """The SQL expression that gives the partition number (0 .. P-1) of a row."""
    p, src = args.parts, source(args)
    if args.expr:
        e = f"({args.expr}) % {p}"
    elif args.scheme == "hash":
        e = f"hash({args.key}) % {p}"
    elif args.scheme == "range":
        lo, hi = con.execute(f"select min({args.key}), max({args.key}) from {src}").fetchone()
        e = f"least({p - 1}, floor(({args.key} - {lo}) * {p} / ({hi} - {lo} + 1)))"
    else:  # quantile: the P-1 boundaries are read from the data
        qs = [i / p for i in range(1, p)]
        bounds = con.execute(f"select quantile_disc({args.key}, {qs}) from {src}").fetchone()[0] if qs else []
        print("quantile boundaries:", bounds)
        e = " + ".join(f"({args.key} > {b})::int" for b in bounds) or "0"
    return f"coalesce({e}, 0)::int"
