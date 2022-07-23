"""Pluggable widgets for Metador."""

from abc import ABC
from typing import List, Set, Type

from panel.viewable import Viewable

from ..container import MetadorNode
from ..plugins.interface import PluginGroup
from ..schema.core import FullPluginRef


class Widget(ABC):
    """Base class for metador widgets."""

    def __init__(self, node: MetadorNode, schema_name: str = ""):
        self._container_node = node

        if schema_name and node.meta.get(schema_name) is None:
            raise ValueError("The node does not contain '{schema_name}' metadata!")

        if not schema_name:
            for schemaref in self.supported():
                if node.meta.get(schemaref.name):
                    schema_name = schemaref.name
                    break

        if not schema_name:
            raise ValueError("The node does not contain any suitable metadata!")

    @classmethod
    def supports(cls, schema_ref: FullPluginRef) -> bool:
        """Return whether a certain schema is supported by the widget."""
        return any(map(lambda sref: sref.supports(schema_ref), cls.supported()))

    @classmethod
    def supported(cls) -> List[FullPluginRef]:
        """Return list of schemas supported by this widget."""
        raise NotImplementedError

    def show(self) -> Viewable:
        """Return a fresh Panel widget representing the node data and metadata."""
        raise NotImplementedError


class PGWidget(PluginGroup[Widget]):
    """Interface to access installed widget plugins."""

    def check_plugin(self, widget_name: str, widget: Type[Widget]):
        self.check_is_subclass(widget_name, widget, Widget)

    def supported_schemas(self) -> Set[FullPluginRef]:
        """Return union of all schemas supported by all widgets."""
        return set.union(*(set(w.supported()) for w in self.values()))

    def widgets_for(self, schema_ref: FullPluginRef) -> List[Type[Widget]]:
        """Return widgets that support the given schema."""
        ret: List[Type[Widget]] = []
        # TODO
        return ret
