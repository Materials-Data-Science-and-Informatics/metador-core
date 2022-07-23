"""Plugin groups and plugins."""
from typing import Any, Dict

import lazy_object_proxy
import wrapt

# will be filled by plugins.bootstrap
# dict contains objects of PluginGroup type,
# but we can't state it here due to circular imports
# and type checking dynamic plugins is not so nice anyway
_installed: Dict[str, Any] = {}


# some magic so we don't run into circular import problems so much
class InstalledPlugins(wrapt.ObjectProxy):
    def __getitem__(self, key: str):
        return lazy_object_proxy.Proxy(lambda: _installed[key])


# To be imported for access to installed plugins
mpg = InstalledPlugins(_installed)
"""Installed metador plugin groups. To be imported in code to access plugins."""
