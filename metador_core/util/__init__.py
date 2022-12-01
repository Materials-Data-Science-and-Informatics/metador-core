import sys
from functools import lru_cache
from typing import Iterable

cache = lru_cache(maxsize=None)  # for 3.8 compat


def eprint(*args, **kwargs):
    """Print to error stream."""
    print(*args, file=sys.stderr, **kwargs)


def drop(n: int, it: Iterable):
    """Drop fixed number of elements from iterator."""
    return (x for i, x in enumerate(it) if i >= n)


def is_public_name(n: str):
    """Return whether a name is public (does not start with _)."""
    return n[0] != "_"
