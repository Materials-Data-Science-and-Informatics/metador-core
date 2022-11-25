import pytest

from metador_core.plugin.util import (
    check_implements_method,
    check_is_subclass,
    is_notebook,
    register_in_group,
)

from .dummy_plugins import PGDummy

# some dummy classes for use in tests:


class A:
    def foo(self):
        pass


class B(A):
    # inherits from A, but does not implement foo
    pass


class C(B):
    # overrides foo that was defined in A
    def foo(self):
        pass


class D:
    # no foo method available
    pass


# ----


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


# ----


def test_is_notebook():
    # sanity check: tests do not run in a notebook
    assert not is_notebook()


def test_register_in_group(plugingroups_test):
    """Test manual ad-hoc plugin registration."""
    pgs = plugingroups_test

    # should work
    assert "dummy" not in pgs
    register_in_group(pgs, PGDummy, violently=True)
    assert "dummy" in pgs

    # should work as a decorator too
    @register_in_group(pgs, violently=True)
    class PGDummy2(PGDummy):
        class Plugin:
            # must be defined explicitly, or won't pass as plugin
            name = "dummy"
            version = (0, 1, 0)

        def check_plugin(self, ep_name, plugin):
            ...

    # fails:

    with pytest.raises(RuntimeError):
        # not allowed outside notebooks (unless forced)
        register_in_group(pgs, A)

    with pytest.raises(RuntimeError):
        # not suitable (no Plugin inner class)
        register_in_group(pgs, A, violently=True)
