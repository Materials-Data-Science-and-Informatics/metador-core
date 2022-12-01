"""Test hashing helper functions."""
import pytest

from metador_core.util.hashsums import file_hashsum


def test_hashsum(tmp_path):
    file = tmp_path / "test.txt"
    with open(file, "w") as f:
        f.write("hello world!")

    with pytest.raises(ValueError):
        file_hashsum(file, "invalid")

    hsum = file_hashsum(file, "sha256")
    assert (
        hsum
        == "sha256:7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
    )
