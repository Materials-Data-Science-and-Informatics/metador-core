import pytest

from metador_core.plugin.interface import PluginBase, PluginGroup
from metador_core.plugin.util import (
    check_implements_method,
    check_is_subclass,
    is_notebook,
    register_in_group,
)


class A:
    def foo(self):
        pass


class B(A):
    # inherits, but does not implement foo
    pass


class C(B):
    # overrides foo
    def foo(self):
        pass


class D:
    # no foo
    pass


def test_check_is_subclass():
    check_is_subclass("B", B, A)
    check_is_subclass("C", C, A)

    with pytest.raises(TypeError):
        check_is_subclass("D", D, A)
    with pytest.raises(TypeError):
        check_is_subclass("A", A, B)


def test_check_implements_method():
    check_implements_method("C", C, A.foo)
    with pytest.raises(TypeError):
        check_implements_method("B", B, A.foo)
    with pytest.raises(TypeError):
        check_implements_method("D", D, A.foo)


def test_is_notebook():
    # sanity check: tests do not run in a notebook
    assert not is_notebook()


# ----


class DummyBase:
    """Base class for dummy plugins."""

    def perform(self):
        """Perform dummy action."""


class DummyPluginInfo(PluginBase):
    """Model for dummy plugin info."""

    something: int


class PGDummy(PluginGroup):
    """Dummy plugin group."""

    class Plugin:
        name = "dummy"
        version = (0, 1, 0)
        plugin_class = DummyBase
        plugin_info_class = DummyPluginInfo

    # must be overridden
    def check_plugin(self, ep_name, plugin):
        return check_implements_method(ep_name, plugin, DummyBase.perform)

    # can be overridden
    def init_plugin(self, plugin):
        return super().init_plugin(plugin)


def test_register_in_group(plugingroups_test):
    """Test manual ad-hoc plugin registration."""
    pgs = plugingroups_test

    # should work
    register_in_group(pgs, PGDummy, violently=True)

    # should work as a decorator too
    @register_in_group(pgs, violently=True)
    class PGDummy2(PluginGroup):
        class Plugin:
            name = "dummy"
            version = (0, 1, 0)

        # must be overridden
        def check_plugin(self, ep_name, plugin):
            return super().check_plugin(ep_name, plugin)

    # fails:

    with pytest.raises(RuntimeError):
        # not allowed outside notebooks (unless forced)
        register_in_group(pgs, A)

    with pytest.raises(RuntimeError):
        # plugin not suitable (no Plugin inner class)
        register_in_group(pgs, A, violently=True)
