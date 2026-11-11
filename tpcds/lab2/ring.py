"""Consistent hashing as in the lecture: the ring is a sorted list of (token, node) pairs.

Compares the ring with "hash(key) mod N" when one node joins: how evenly are the keys spread,
and how many keys change owner?

Examples:
  python ring.py --nodes 4 --vnodes 1
  python ring.py --nodes 4 --vnodes 16
  python ring.py --nodes 4 --vnodes 256
"""
import argparse
import bisect
import hashlib

SPACE = 2 ** 32


def h(text):
    """A hash that is the same on every machine and every run (Python's own hash() is not)."""
    return int(hashlib.md5(str(text).encode()).hexdigest(), 16) % SPACE


def build_ring(nodes, vnodes):
    """Each node gets `vnodes` tokens: the hashes of "node-0", "node-1", ..."""
    return sorted((h(f"{n}-{i}"), n) for n in nodes for i in range(vnodes))


def owner(ring, key):
    """The first token at or after the key's hash, going clockwise; after the last token, wrap to the first."""
    i = bisect.bisect_left(ring, (h(key), ""))
    return ring[i % len(ring)][1]


def owner_mod(nodes, key):
    return nodes[h(key) % len(nodes)]


def shares(owners, nodes):
    return {n: sum(1 for o in owners if o == n) for n in nodes}


def show(title, owners, nodes):
    count = shares(owners, nodes)
    mean = len(owners) / len(nodes)
    print(title)
    for n in nodes:
        print(f"  {n:<4} {count[n]:>8,} keys {count[n] / len(owners):>6.1%}  {'#' * round(40 * count[n] / len(owners))}")
    print(f"  max/mean = {max(count.values()) / mean:.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=4, help="nodes before the new one joins (default 4)")
    ap.add_argument("--vnodes", type=int, default=1, help="tokens per node (default 1)")
    ap.add_argument("--keys", type=int, default=100000, help="number of keys to place (default 100000)")
    args = ap.parse_args()

    keys = range(1, args.keys + 1)
    nodes = [f"n{i}" for i in range(1, args.nodes + 1)]
    bigger = nodes + [f"n{args.nodes + 1}"]

    ring = build_ring(nodes, args.vnodes)
    before = [owner(ring, k) for k in keys]
    show(f"ring, {args.nodes} nodes, {args.vnodes} token(s) per node, {args.keys:,} keys", before, nodes)

    ring_after = build_ring(bigger, args.vnodes)
    after = [owner(ring_after, k) for k in keys]
    moved = [(a, b) for a, b in zip(before, after) if a != b]
    lost = shares([a for a, _ in moved], nodes)
    print(f"\n{bigger[-1]} joins:")
    print(f"  ring : {len(moved) / args.keys:6.1%} of the keys change owner "
          f"(a fair share for the new node is 1/{len(bigger)} = {1 / len(bigger):.1%})")
    print("         taken from: " + ", ".join(f"{n} {lost[n] / max(1, len(moved)):.0%}" for n in nodes))
    moved_mod = sum(1 for k in keys if owner_mod(nodes, k) != owner_mod(bigger, k))
    print(f"  mod N: {moved_mod / args.keys:6.1%} of the keys change owner")


if __name__ == "__main__":
    main()
