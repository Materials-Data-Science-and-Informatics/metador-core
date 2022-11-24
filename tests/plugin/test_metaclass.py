import pytest

from metador_core.plugin.metaclass import PluginMetaclassMixin, UndefVersion
from metador_core.plugin.types import to_semver_str


class SomeBasePlugin(metaclass=PluginMetaclassMixin):
    pass


class PluginA(SomeBasePlugin):
    class Plugin:
        name = "plugin_a"
        version = (0, 1, 0)


def test_marker_mixin():
    MarkedA = UndefVersion._mark_class(PluginA)
    with pytest.raises(TypeError):
        UndefVersion._mark_class(MarkedA)  # already marked

    # looks like original plugin wrt. to plugin info
    assert MarkedA.Plugin == PluginA.Plugin

    assert not UndefVersion._is_marked(PluginA)
    assert UndefVersion._is_marked(MarkedA)

    assert UndefVersion._unwrap(MarkedA) is PluginA
    assert UndefVersion._unwrap(PluginA) is None


def test_subclass_plugin():
    MarkedA = UndefVersion._mark_class(PluginA)
    MarkedB = UndefVersion._mark_class(SomeBasePlugin)

    # inherit from plugin -> should work
    class PluginC(PluginA):
        ...

    # but must not inherit Plugin info class
    assert PluginC.Plugin is None

    # must not inherit from marked class...
    with pytest.raises(TypeError):  # with inner Plugin

        class PluginD(MarkedA):
            ...

    with pytest.raises(TypeError):  # without inner Plugin

        class PluginE(MarkedB):
            ...


def test_metaclass_repr():
    # repr of a plugin should show name and version
    repr_str = repr(PluginA)
    assert repr_str.find(PluginA.Plugin.name)
    assert repr_str.find(to_semver_str(PluginA.Plugin.version))

    # repr of marked plugin should show that the version was chosen impicitly
    MarkedA = UndefVersion._mark_class(PluginA)
    assert repr(MarkedA).find("unspecified")
