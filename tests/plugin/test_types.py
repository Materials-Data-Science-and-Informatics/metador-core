import pytest
from hypothesis import given
from hypothesis import strategies as st

from metador_core.plugin.types import (
    EPName,
    SemVerStr,
    SemVerTuple,
    from_ep_name,
    from_semver_str,
    to_ep_name,
    to_semver_str,
)


@pytest.mark.parametrize("s", ["", "a", "1", "1.0", "1.0.5.1", "1.-1.4"])
def test_semverstr_invalid(s):
    with pytest.raises(TypeError):
        SemVerStr(s)


@given(st.from_type(SemVerTuple))
def test_semverstr_conversion(obj):
    semver_str = to_semver_str(obj)
    assert SemVerStr(semver_str)
    assert from_semver_str(semver_str) == obj


@pytest.mark.parametrize("s", ["", "x_y", "x_1.0", "y_1.0.3.1", "x_1.0.0"])
def test_epname_invalid(s):
    with pytest.raises(TypeError):
        EPName(s)


@given(st.from_regex(EPName.__pattern__, fullmatch=True))
def test_epname_conversion(obj):
    assert EPName(obj)
    name, ver = from_ep_name(obj)

    # sanity check
    assert len(name) >= 2
    assert SemVerStr(to_semver_str(ver))

    # check inverses, but use normalized rep (0.00.01 -> 0.0.1)
    normalized = to_ep_name(name, ver)
    assert to_ep_name(*from_ep_name(normalized)) == normalized
