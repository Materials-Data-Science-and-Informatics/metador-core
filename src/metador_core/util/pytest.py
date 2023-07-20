import secrets


def random_hex(length: int) -> str:
    """Return random hex string of given length."""
    return secrets.token_hex(length // 2 + 1)[:length]


def parameters(d, keys=None):
    """Expand parameter combinations from a nested dict into a list of tuples."""
    keys = keys or []
    if isinstance(d, list):
        return sum((parameters(y, keys) for y in d), [])
    elif isinstance(d, dict):
        return sum((parameters(v, keys + [k]) for k, v in d.items()), [])
    else:
        return [tuple(keys + [d])]
