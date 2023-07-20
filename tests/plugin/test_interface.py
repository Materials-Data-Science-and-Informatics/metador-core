import importlib_metadata
import pytest

from metador_core.plugin.metaclass import UndefVersion
from metador_core.plugin.types import is_metador_ep_group, to_ep_group_name, to_ep_name
from metador_core.plugin.util import register_in_group
from metador_core.plugins import plugingroups

from . import dummy_plugins as d

# basic tests


def test_plugingroup_misc(plugingroups_test):
    plugingroups = plugingroups_test
    schemas = plugingroups_test["schema"]

    # check name
    assert plugingroups.name == "plugingroup"
    assert schemas.name == "schema"

    # check packages
    assert plugingroups.packages == schemas.packages
    assert "metador-core" in plugingroups.packages

    assert plugingroups.provider(schemas.Plugin.ref()).name == "metador-core"
    assert schemas.provider(schemas["core.file"].Plugin.ref()).name == "metador-core"


def test_plugingroup_str_repr(plugingroups_test):
    # just make sure it works without error and dummy check output
    schemas = plugingroups_test["schema"]
    rstr = repr(schemas)
    assert rstr.find("core.file")
    pstr = str(schemas)
    assert pstr.find("core.file")
    assert pstr.find("\n")


def test_plugingroup_pluginref(plugingroups_test):
    """Check that PluginRef subclasses work as expected."""
    # check PluginRef of the plugingroups themselves
    plugingroups = plugingroups_test
    schemas = plugingroups_test["schema"]

    pg_ref = plugingroups.PluginRef(name="plugingroup", version=(0, 1, 0))
    assert pg_ref.group == pg_ref.name
    assert plugingroups.Plugin.ref() == pg_ref

    sg_ref = schemas.Plugin.ref()
    assert sg_ref.group == pg_ref.name
    assert sg_ref.name == "schema"

    # check a plugin PluginRef
    s_ref = next(schemas.keys())
    assert s_ref.group == sg_ref.name


def test_is_plugin(plugingroups_test):
    plugingroups = plugingroups_test
    schemas = plugingroups_test["schema"]

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


def test_plugingroup_compare(plugingroups_test):
    # different PluginRef subclasses still comparable!
    plugingroups = plugingroups_test
    schemas = plugingroups_test["schema"]

    from metador_core.schema.plugins import PluginRef

    kwargs = dict(name="xyz", version=(0, 1, 0))

    p_ref = plugingroups.PluginRef(**kwargs)
    s_ref = schemas.PluginRef(**kwargs)
    assert p_ref != s_ref
    assert p_ref == PluginRef(group=plugingroups.name, **kwargs)
    assert s_ref == PluginRef(group=schemas.name, **kwargs)


def test_plugingroup_dictlike_basic(plugingroups_test):
    """Do some manual sanity checks."""
    plugingroups = plugingroups_test
    schemas = plugingroups_test["schema"]

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
    assert pg.get(invalid) is None
    with pytest.raises(KeyError):
        pg[invalid]


# try creating invalid plugin groups
@pytest.mark.parametrize("pg", [d.PGInvalid1, d.PGInvalid2, d.PGInvalid3, d.PGInvalid4])
def test_invalid_plugingroups(pg, plugingroups_test):
    with pytest.raises(TypeError):
        register_in_group(plugingroups_test, pg, violently=True)


# ----
# Test plugingroup behavior (adding, checking, loading plugins, multiple versions, etc)


@pytest.fixture
def pg_dummy(plugingroups_test):
    plugingroups_test.__reset__()  # plugingroups_test is module-level, need to reset it
    # add valid dummy plugin group into test plugin environment
    register_in_group(plugingroups_test, d.PGDummy, violently=True)
    # return to tests of plugin system
    return plugingroups_test.get("dummy")


class FakeDist:
    name = "foo"
    version = "1.2.3"


@pytest.fixture(scope="session")
def ep_factory():
    """Entry point maker."""

    def maker(obj, ep_name=None, group=None):
        if ep_name is None:
            ep_name = to_ep_name(obj.Plugin.name, obj.Plugin.version)

        assert not is_metador_ep_group(group)
        group = to_ep_group_name(group)
        return importlib_metadata.EntryPoint(
            ep_name, f"{obj.__module__}:{obj.__qualname__}", group
        )._for(FakeDist)

    return maker


def test_add_ep_invalid(pg_dummy, ep_factory):
    with pytest.raises(ValueError):
        pg_dummy._add_ep("invalid_entrypoint", None)


def test_add_ep_twice(plugingroups_test):
    schemas = plugingroups_test["schema"]

    plugin = schemas.get("core.file")
    ref = plugin.Plugin.ref()
    ep_name = to_ep_name(ref.name, ref.version)
    ep = schemas._ENTRY_POINTS[ep_name]

    ep2 = importlib_metadata.EntryPoint(ep.name, ep.value, ep.group)._for(FakeDist)
    schemas._add_ep(ep_name, ep2)
    assert schemas._ENTRY_POINTS[ep_name] == ep2


# ----


# try adding invalid plugins
def test_plugingroups(pg_dummy, ep_factory):
    assert not list(pg_dummy.keys())
    make_ep = ep_factory

    ep1 = make_ep(d.InvalidPlugin1, group=pg_dummy.name, ep_name="test.plugin__0.1.0")
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep1.name, ep1)
        pg_dummy["test.plugin"]  # no plugin inner class

    ep2 = make_ep(d.InvalidPlugin2, group=pg_dummy.name, ep_name="xyz__0.1.0")
    with pytest.raises(ValueError):
        pg_dummy._add_ep(ep2.name, ep2)
        pg_dummy._get_unsafe("xyz")  # mismatch ep name / stated name
    ep2b = make_ep(d.InvalidPlugin2, group=pg_dummy.name, ep_name="abc__0.2.0")
    with pytest.raises(ValueError):
        pg_dummy._add_ep(ep2b.name, ep2b)
        pg_dummy._get_unsafe("abc")  # mismatch ep version /stated version
    ep2c = make_ep(d.InvalidPlugin2b, group=pg_dummy.name, ep_name="test__0.1.0")
    with pytest.raises(ValueError):
        pg_dummy._add_ep(ep2c.name, ep2c)
        pg_dummy._get_unsafe("test")  # missing prefix

    ep3 = make_ep(d.InvalidPlugin3, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep3.name, ep3)
        pg_dummy._get_unsafe("test.plugin")  # missing 'something'

    ep4 = make_ep(d.InvalidPlugin4, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep4.name, ep4)
        pg_dummy._get_unsafe("test.plugin")  # not DummyBase subclass

    ep5 = make_ep(d.InvalidPlugin5, group=pg_dummy.name)
    with pytest.raises(TypeError):
        pg_dummy._add_ep(ep5.name, ep5)
        pg_dummy._get_unsafe("test.plugin")  # missing method

    # now this works
    ep6 = make_ep(d.DummyPlugin, group=pg_dummy.name)
    pg_dummy._add_ep(ep6.name, ep6)
    pg_dummy._get_unsafe("test.plugin")


# TODO: test multi version etc
def test_versions(pg_dummy, ep_factory):
    register_in_group(pg_dummy, d.DummyPlugin, violently=True)  # 0.1
    register_in_group(pg_dummy, d.DummyPluginB, violently=True)  # 0.1.1
    register_in_group(pg_dummy, d.DummyPluginC, violently=True)  # 1.2.3
    register_in_group(pg_dummy, d.DummyPlugin2, violently=True)

    # both plugins are available
    assert "test.plugin" in pg_dummy
    assert "test.plugin2" in pg_dummy

    # all plugins in all versions are listed
    keys = list(pg_dummy.keys())
    for p in [d.DummyPlugin, d.DummyPlugin2, d.DummyPluginB, d.DummyPluginC]:
        assert p.Plugin.ref() in keys
        assert p in pg_dummy

    # we can get all versions of a plugin
    expected = [(0, 1, 0), (0, 1, 1), (1, 2, 3)]
    vs = pg_dummy.versions("test.plugin")
    assert list(map(lambda x: x.version, vs)) == expected

    # access without version yields latest
    latest = pg_dummy.get("test.plugin")
    assert UndefVersion._is_marked(latest)
    assert UndefVersion._unwrap(latest) is d.DummyPluginC

    # access with version yields newest compatible
    # specifically higher minor is returned
    compat = pg_dummy.get("test.plugin", (0, 1, 0))
    assert not UndefVersion._is_marked(compat)
    assert compat is d.DummyPluginB
    assert pg_dummy.get("test.plugin", (1, 1, 0)) is d.DummyPluginC
    assert pg_dummy.get("test.plugin2", (0, 0, 1)) is d.DummyPlugin2

    # smaller "revision" may be returned (semantically equivalent)
    # TODO: is this actually a good idea?...
    assert pg_dummy.get("test.plugin", (0, 1, 2)) is d.DummyPluginB
    assert pg_dummy.get("test.plugin", (1, 2, 4)) is d.DummyPluginC
    assert pg_dummy.get("test.plugin2", (0, 1, 1)) is d.DummyPlugin2

    # these should not be found
    assert pg_dummy.get("test.plugin", (0, 2, 0)) is None
    assert pg_dummy.get("test.plugin2", (0, 2, 0)) is None
    assert pg_dummy.get("test.plugin", (2, 0, 0)) is None
    assert pg_dummy.get("test.plugin2", (1, 0, 0)) is None


plugingroups.__reset__()  # because we accessed the real plugin groups
