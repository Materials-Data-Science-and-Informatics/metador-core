import pytest
from hypothesis import given
from hypothesis import strategies as st

from metador_core.plugin.types import (
    EPGroupName,
    EPName,
    SemVerStr,
    SemVerTuple,
    ep_name_has_namespace,
    from_ep_group_name,
    from_ep_name,
    from_semver_str,
    is_pluginlike,
    plugin_args,
    to_ep_group_name,
    to_ep_name,
    to_semver_str,
)


class A:
    pass


class B:
    class Plugin:
        pass


class C:
    class Plugin:
        group = "blub"
        name = "bla"
        version = "bli"


class Info:
    def __init__(self, a="a", b="b", c=(1, 2, 3)):
        self.group = a
        self.name = b
        self.version = c


class D:
    Plugin = Info()


def test_is_pluginlike():
    assert not is_pluginlike(A)  # no inner 'Plugin'
    assert not is_pluginlike(B)  # missing fields
    assert is_pluginlike(C)  # a class
    assert is_pluginlike(D)  # an instance


def test_plugin_args_simple():
    # first argument is string (just name)
    name, ver = plugin_args("")
    assert name == "" and ver is None

    name, ver = plugin_args("test", None)
    assert name == "test" and ver is None

    name, ver = plugin_args("test", (1, 2, 3))
    assert name == "test" and ver == (1, 2, 3)

    # should work
    name, ver = plugin_args("test", (1, 2, 3), require_version=True)
    # should fail (could not infer any version)
    with pytest.raises(ValueError):
        name, ver = plugin_args("test", None, require_version=True)


@pytest.mark.parametrize("obj", [Info(), D()])
def test_plugin_args_complex(obj):
    # name and version come both from first argument
    name, ver = plugin_args(obj)
    assert name == "b" and ver == (1, 2, 3)
    # the implicitly passed version should suffice
    name, ver = plugin_args(obj, require_version=True)
    assert name == "b" and ver == (1, 2, 3)
    # the explicitly passed version should override
    name, ver = plugin_args(obj, (3, 2, 1))
    assert name == "b" and ver == (3, 2, 1)


# ----


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


def test_epname_namespace():
    assert ep_name_has_namespace(EPName("abc.def__0.1.0"))
    assert not ep_name_has_namespace(EPName("abc__1.2.3"))


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


@given(st.from_regex(EPGroupName.__pattern__, fullmatch=True))
def test_ep_group_name_conversion(obj):
    assert to_ep_group_name(from_ep_group_name(obj)) == obj
