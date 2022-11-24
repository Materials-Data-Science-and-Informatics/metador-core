"""Test plugingroup import mechanism."""
import pytest


def test_plugin_module_exports():
    # check what the module lists as available
    from metador_core import plugins

    assert dir(plugins) == plugins.__all__
    assert "plugingroups" in plugins.__all__
    assert "schemas" in plugins.__all__


def test_plugin_import():
    with pytest.raises(ImportError):
        # not a valid plugingroup
        from metador_core.plugins import foos  # noqa: F401
    with pytest.raises(ImportError):
        # not a plural (must have a suffix 's')
        from metador_core.plugins import schema  # noqa: F401

    # should work
    from metador_core.plugins import plugingroups, schemas  # noqa: F401
