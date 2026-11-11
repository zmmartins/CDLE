"""Reed-Solomon erasure code k+m over GF(256), with numpy. Used by the object store's front-end.

An object is cut into stripes of k data chunks. For every stripe the code adds m parity chunks,
and any k of the k+m chunks are enough to rebuild the other m.

The code is systematic: the data chunks are stored as they are, so a read with no failures
needs no decoding at all. Parity chunk j is a fixed linear combination of the data chunks:

    parity_j = c[j][0]*d_0 + c[j][1]*d_1 + ... + c[j][k-1]*d_{k-1}

where + is XOR and * is multiplication in GF(256), byte by byte. The coefficients c form a
Cauchy matrix, which guarantees that every k x k sub-matrix of [identity; c] can be inverted:
that is the "any k rebuild the rest" property.
"""
import numpy as np

# --- arithmetic in GF(256), with the polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11d) ---------------
EXP = [0] * 512
LOG = [0] * 256
_x = 1
for _i in range(255):
    EXP[_i] = _x
    LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    EXP[_i] = EXP[_i - 255]


def mul(a, b):
    return 0 if a == 0 or b == 0 else EXP[LOG[a] + LOG[b]]


def inv(a):
    return EXP[255 - LOG[a]]


# MUL[c] is the table "byte -> c * byte"; multiplying a whole chunk by c is one numpy lookup.
MUL = np.array([[mul(c, b) for b in range(256)] for c in range(256)], dtype=np.uint8)


def generator(k, m):
    """The (k+m) x k matrix that turns k data chunks into k+m stored chunks."""
    rows = [[1 if i == j else 0 for j in range(k)] for i in range(k)]          # data: identity
    rows += [[inv((k + j) ^ i) for i in range(k)] for j in range(m)]           # parity: Cauchy
    return rows


def invert(a):
    """Inverse of a square matrix over GF(256) (Gauss-Jordan)."""
    n = len(a)
    a = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(a)]
    for col in range(n):
        piv = next(r for r in range(col, n) if a[r][col])
        a[col], a[piv] = a[piv], a[col]
        f = inv(a[col][col])
        a[col] = [mul(f, x) for x in a[col]]
        for r in range(n):
            if r != col and a[r][col]:
                g = a[r][col]
                a[r] = [x ^ mul(g, y) for x, y in zip(a[r], a[col])]
    return [row[n:] for row in a]


def combine(coefs, chunks):
    """XOR-sum of coef * chunk, byte by byte. chunks: equal-length uint8 arrays."""
    out = np.zeros_like(chunks[0])
    for c, x in zip(coefs, chunks):
        if c == 1:
            out ^= x
        elif c:
            out ^= MUL[c][x]
    return out


def encode(data_chunks, k, m):
    """The m parity chunks of k data chunks."""
    g = generator(k, m)
    return [combine(g[k + j], data_chunks) for j in range(m)]


def decode(have, k, m, want):
    """Rebuild chunk number `want` (0 .. k+m-1) from `have`, a dict {chunk number: array} with k entries."""
    g = generator(k, m)
    idx = sorted(have)[:k]
    rows = invert([g[i] for i in idx])                 # data chunk t = rows[t] * (the chunks we have)
    # chunk `want` is g[want] * data, so its coefficients on the chunks we have are g[want] * rows
    coefs = [0] * k
    for j, c in enumerate(g[want]):
        if c:
            for t in range(k):
                coefs[t] ^= mul(c, rows[j][t])
    return combine(coefs, [have[i] for i in idx])
