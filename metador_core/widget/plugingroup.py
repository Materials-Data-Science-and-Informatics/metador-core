"""Defines widgets as pluggable entities."""

from typing import List, Set, Type

from ..plugins.interface import PluginGroup
from ..schema.core import FullPluginRef
from .interface import Widget


class PGWidget(PluginGroup):
    """Interface to access installed widget plugins."""

    def check_plugin(self, widget_name: str, widget):
        self.check_is_subclass(widget_name, widget, Widget)

    def supported_schemas(self) -> Set[FullPluginRef]:
        """Returns union of all schemas supported by all widgets."""
        return set.union({w.supports() for w in self.values()})

    def widgets_for(self, schema_ref: FullPluginRef) -> List[Type[Widget]]:
        """Returns widgets that support the given schema."""
        ret = []
        # TODO
        return ret
