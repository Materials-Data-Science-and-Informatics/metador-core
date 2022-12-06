import pytest


@pytest.fixture(scope="module")
def schemas_test(plugingroups_test):
    return plugingroups_test["schema"]
