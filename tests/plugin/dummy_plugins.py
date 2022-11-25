"""Dummy plugin group and plugins for test cases."""
from metador_core.plugin.interface import PluginBase, PluginGroup
from metador_core.plugin.util import check_implements_method

# ----
# the following are invalid plugingroups to be rejected:


class PGInvalid1:  # not subclass of PluginGroup
    class Plugin:
        name = "invalid"
        version = (0, 1, 0)


class PGInvalid2(PluginGroup):
    class Plugin:
        name = "invalid"
        version = (0, 1, 0)

    # no check_plugin method


class PGInvalid3(PluginGroup):
    # incomplete Plugin class (misses plugin_info_class)
    class Plugin:
        name = "invalid"
        version = "0.1"


class InvalidPGI:
    ...  # invalid plugin_info_class


class PGInvalid4(PluginGroup):
    class Plugin:
        name = "invalid"
        version = (0, 1, 0)
        plugin_info_class = InvalidPGI


# ----
# this is a proper plugingroup that should be accepted and work:


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


class DummyPlugin(DummyBase):
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        something = 1

    def perform(self):
        """Perform dummy action."""


class DummyPluginB(DummyPlugin):
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 1)
        something = 1

    def perform(self):
        ...


class DummyPluginC(DummyPlugin):
    class Plugin:
        name = "test.plugin"
        version = (1, 2, 3)
        something = 1

    def perform(self):
        ...


class DummyPlugin2(DummyPlugin):
    class Plugin:
        name = "test.plugin2"
        version = (0, 1, 0)
        something = 1

    def perform(self):
        ...


# ----
# the following are all invalid dummy plugins to be rejected:


class InvalidPlugin1:
    ...  # no Plugin inner class


class InvalidPlugin2:
    class Plugin:
        name = "abc"  # name mismatch with test.plugin ep
        version = (0, 1, 0)


class InvalidPlugin2b:
    class Plugin:
        name = "test"  # lacks prefix
        version = (0, 1, 0)


class InvalidPlugin3:
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        # missing "something"


class InvalidPlugin4:  # not subclass of DummyBase
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        something = 1


class InvalidPlugin5(DummyBase):
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        something = 1

    # missing "perform" method
