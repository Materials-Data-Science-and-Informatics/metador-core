import pytest

from metador_core.plugin.interface import PluginGroup
from metador_core.plugin.metaclass import UndefVersion
from metador_core.plugin.types import is_metador_ep_group, to_ep_group_name, to_ep_name
from metador_core.plugin.util import register_in_group
from metador_core.plugins import plugingroups, schemas

from .test_util import DummyBase, PGDummy

# basic tests


def test_plugingroup_misc():
    # check name
    assert plugingroups.name == "plugingroup"
    assert schemas.name == "schema"

    # check packages
    assert plugingroups.packages == schemas.packages
    assert "metador-core" in plugingroups.packages


def test_plugingroup_str_repr():
    # just make sure it works without error and dummy check output
    rstr = repr(schemas)
    assert rstr.find("core.file")
    pstr = str(schemas)
    assert pstr.find("core.file")
    assert pstr.find("\n")


def test_plugingroup_pluginref():
    """Check that PluginRef subclasses work as expected."""
    # check PluginRef of the plugingroups themselves
    pg_ref = plugingroups.PluginRef(name="plugingroup", version=(0, 1, 0))
    assert pg_ref.group == pg_ref.name
    assert plugingroups.Plugin.ref() == pg_ref

    sg_ref = schemas.Plugin.ref()
    assert sg_ref.group == pg_ref.name
    assert sg_ref.name == "schema"

    # check a plugin PluginRef
    s_ref = next(schemas.keys())
    assert s_ref.group == sg_ref.name


def test_is_plugin():
    assert plugingroups.is_plugin(schemas)
    assert not schemas.is_plugin(plugingroups)

    PCls = next(iter(schemas.values()))

    Marked = schemas.get(PCls.Plugin.name)
    assert UndefVersion._is_marked(Marked)

    class Impostor(PCls):
        ...

    Impostor.Plugin = PCls.Plugin  # metaclass removes it, add back for test

    class Impostor2(PCls):
        class Plugin:
            name = PCls.Plugin.name
            version = PCls.Plugin.version

    class Impostor3(PCls):
        ...

    delattr(Impostor3, "Plugin")

    assert PCls in schemas
    assert Marked in schemas
    # impostor class can pretend to be a plugin in many contexts
    assert Impostor in schemas
    assert Impostor2 in schemas

    assert schemas.is_plugin(PCls)
    assert schemas.is_plugin(Marked)
    # but when we really want to check, we can find out
    assert not schemas.is_plugin(Impostor)
    assert not schemas.is_plugin(Impostor2)
    assert not schemas.is_plugin(Impostor3)


def test_plugingroup_compare():
    # different PluginRef subclasses still comparable!
    from metador_core.schema.plugins import PluginRef

    kwargs = dict(name="xyz", version=(0, 1, 0))

    p_ref = plugingroups.PluginRef(**kwargs)
    s_ref = schemas.PluginRef(**kwargs)
    assert p_ref != s_ref
    assert p_ref == PluginRef(group=plugingroups.name, **kwargs)
    assert s_ref == PluginRef(group=schemas.name, **kwargs)


def test_plugingroup_dictlike_basic():
    """Do some manual sanity checks."""
    # NOTE: important to check that plugingroup itself is a plugingroup
    # because it allows us to do the next rigorous checks easily

    # check __contains__
    assert plugingroups.Plugin.ref() in plugingroups
    assert schemas.Plugin.ref() in plugingroups
    assert "schema" in plugingroups

    # check __getitem__
    assert plugingroups["plugingroup"] is plugingroups
    assert plugingroups["schema"] is schemas
    assert plugingroups[schemas] is schemas  # type: ignore

    # check get
    assert plugingroups.get("plugingroup", (0, 1, 0)) is plugingroups
    assert plugingroups.get(plugingroups, (0, 1, 0)) is plugingroups
    assert plugingroups.get("schema") is schemas


@pytest.mark.parametrize("pg", list(plugingroups.values()))
def test_plugingroup_dictlike_consistency(pg):
    """Check consistency of dict-like operations on all (real) plugingroups."""
    # check keys, values, items, __contains__, __getitem__ and get
    # also check is_plugin
    pg_keys = set(pg.keys())
    pg_values = set(pg.values())
    for k, v in pg.items():
        # minimal invariant
        assert k in pg_keys
        assert v in pg_values
        assert pg.is_plugin(v)

        # lookup and access via name, ref or class
        v_ref = v.Plugin.ref()
        assert k in pg
        assert v_ref in pg
        assert v in pg

        assert pg[k] == v
        assert pg[v] == v
        assert pg[v_ref] == v

        assert pg.get(k) == v

    # now try something that should not exist
    invalid = "_invalid_plugin"
    assert not pg.is_plugin(invalid)
    assert invalid not in pg
    assert invalid not in pg_keys
    pg.get(invalid) is None
    with pytest.raises(KeyError):
        pg[invalid]


# try creating invalid plugin groups
def test_invalid_plugingroups(plugingroups_test):
    class PGInvalid1:
        class Plugin:
            name = "invalid"
            version = (0, 1, 0)

    with pytest.raises(TypeError):
        # not subclass of PluginGroup
        register_in_group(plugingroups_test, PGInvalid1, violently=True)

    class PGInvalid2(PluginGroup):
        class Plugin:
            name = "invalid"
            version = (0, 1, 0)

    with pytest.raises(TypeError):
        # no check_plugin method
        register_in_group(plugingroups_test, PGInvalid2, violently=True)

    class PGInvalid3(PluginGroup):
        class Plugin:
            name = "invalid"
            version = "0.1"

    with pytest.raises(TypeError):
        # invalid regular plugin field
        register_in_group(plugingroups_test, PGInvalid3, violently=True)

    class Foo:
        ...

    class PGInvalid4(PluginGroup):
        class Plugin:
            name = "invalid"
            version = (0, 1, 0)
            plugin_info_class = Foo

    with pytest.raises(TypeError):
        # invalid plugin_info_class
        register_in_group(plugingroups_test, PGInvalid4, violently=True)

    # this should work:
    @register_in_group(plugingroups_test, violently=True)
    class PGValid(PluginGroup):
        class Plugin:
            name = "invalid"
            version = (0, 1, 0)

        def check_plugin(self, ep_name, plugin):
            return super().check_plugin(ep_name, plugin)


# ----
# Test plugingroup behavior (adding, checking, loading plugins, multiple versions, etc)


@pytest.fixture
def pg_dummy(plugingroups_test):
    register_in_group(plugingroups_test, PGDummy, violently=True)

    return plugingroups_test.get("dummy")


@pytest.fixture(scope="session")
def ep_factory():
    """Entry point maker."""
    import importlib_metadata

    def wrapped():
        def maker(obj, ep_name=None, group=None):
            if ep_name is None:
                ep_name = to_ep_name(obj.Plugin.name, obj.Plugin.version)

            assert not is_metador_ep_group(group)
            group = to_ep_group_name(group)
            return importlib_metadata.EntryPoint(
                ep_name, f"{__name__}:{obj.__qualname__}", group
            )

        return maker

    return wrapped


# ----


class InvalidPlugin1:
    ...


class InvalidPlugin2:
    class Plugin:
        name = "abc"
        version = (0, 1, 0)


class InvalidPlugin3:
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)


class InvalidPlugin4:
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        something = 1


class InvalidPlugin5(DummyBase):
    class Plugin:
        name = "test.plugin"
        version = (0, 1, 0)
        something = 1


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


class DummyPluginC(DummyPlugin):
    class Plugin:
        name = "test.plugin"
        version = (1, 2, 3)
        something = 1


class DummyPlugin2(DummyPlugin):
    class Plugin:
        name = "test.plugin2"
        version = (0, 1, 0)
        something = 1


# ----

# try adding invalid plugins
def test_plugingroups(pg_dummy, ep_factory):
    assert not list(pg_dummy.keys())
    make_ep = ep_factory()

    ep1 = make_ep(InvalidPlugin1, group=pg_dummy.name, ep_name="test.plugin__0.1.0")
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep1.name, ep1)
        pg_dummy["test.plugin"]  # no plugin inner class

    ep2 = make_ep(InvalidPlugin2, group=pg_dummy.name, ep_name="xyz__0.1.0")
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep2.name, ep2)
        pg_dummy._get_unsafe("xyz")  # mismatch ep name / stated name
    ep2b = make_ep(InvalidPlugin2, group=pg_dummy.name, ep_name="abc__0.2.0")
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep2b.name, ep2b)
        pg_dummy._get_unsafe("abc")  # mismatch ep version /stated version

    ep3 = make_ep(InvalidPlugin3, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep3.name, ep3)
        pg_dummy._get_unsafe("test.plugin")  # missing 'something'

    ep4 = make_ep(InvalidPlugin4, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep4.name, ep4)
        pg_dummy._get_unsafe("test.plugin")  # not DummyBase subclass

    ep5 = make_ep(InvalidPlugin5, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep5.name, ep5)
        pg_dummy._get_unsafe("test.plugin")  # missing method

    # now this works
    ep6 = make_ep(DummyPlugin, group=pg_dummy.name)
    pg_dummy._add_ep(ep6.name, ep6)
    pg_dummy._get_unsafe("test.plugin")


# TODO: test multi version etc
