"""Test plugingroup import mechanism."""
import pytest


def test_plugin_import():
    with pytest.raises(ImportError):
        # not a valid plugingroup
        from metador_core.plugins import foo  # noqa: F401
    with pytest.raises(ImportError):
        # not a plural (must have a suffix 's')
        from metador_core.plugins import schema  # noqa: F401

    # should work
    from metador_core.plugins import plugingroups, schemas  # noqa: F401


def test_plugingroup_access():
    from metador_core.plugins import plugingroups, schemas

    # check __contains__
    assert plugingroups.Plugin.ref() in plugingroups
    assert schemas.Plugin.ref() in plugingroups
    assert "schema" in plugingroups

    # check __getitem__
    assert plugingroups["plugingroup"] is plugingroups
    assert plugingroups["schema"] is schemas
    with pytest.raises(KeyError):
        plugingroups["foo"]

    # check get
    assert plugingroups.get("plugingroup", (0, 1, 0)) is plugingroups
    assert plugingroups.get("schema") is schemas
    assert plugingroups.get("foo") is None

    # check name
    assert plugingroups.name == "plugingroup"
    assert schemas.name == "schema"


def test_plugingroup_pluginref():
    """Check that PluginRef subclasses work as expected."""
    from metador_core.plugins import plugingroups, schemas

    pg_ref = plugingroups.PluginRef(name="plugingroup", version=(0, 1, 0))
    assert pg_ref.group == pg_ref.name
    assert plugingroups.Plugin.ref() == pg_ref

    sg_ref = schemas.Plugin.ref()
    assert sg_ref.group == pg_ref.name
    assert sg_ref.name == "schema"

    s_ref = next(schemas.keys())
    assert s_ref.group == sg_ref.name
