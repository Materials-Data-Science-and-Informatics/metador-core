"""Pluggable widgets for Metador."""

from abc import ABC
from typing import List, Set, Type

from panel.viewable import Viewable

from ..container import MetadorNode
from ..plugins.interface import PluginGroup
from ..schema.core import MetadataSchema, PluginRef
from .server import WidgetServer
from .server.standalone import widget_server


class Widget(ABC):
    """Base class for metador widgets."""

    _node: MetadorNode
    _meta: MetadataSchema
    _server: WidgetServer

    def __init__(
        self, node: MetadorNode, schema_name: str = "", server: WidgetServer = None
    ):
        self._node = node

        srv = server or widget_server()
        if srv is None:
            raise ValueError("No widget server passed and standalone launch failed!")
        self._server = srv

        if not schema_name:
            for schemaref in self.supported():
                if node.meta.get(schemaref.name):
                    schema_name = schemaref.name
                    break
        if not schema_name:
            raise ValueError("The node does not contain any suitable metadata!")

        if metadata := node.meta.get(schema_name):
            self._meta = metadata
        else:
            raise ValueError("The node does not contain '{schema_name}' metadata!")

        self.setup()

    def file_url_for(self, node: MetadorNode):
        return self._server.file_url_for(node)

    @classmethod
    def supports(cls, schema_ref: PluginRef) -> bool:
        """Return whether a certain schema is supported by the widget."""
        return any(map(lambda sref: sref.supports(schema_ref), cls.supported()))

    @classmethod
    def supported(cls) -> List[PluginRef]:
        """Return list of schemas supported by this widget."""
        raise NotImplementedError

    def setup(self):
        """Check that passed node and parsed metadata is valid and do preparations.

        In case the widget is not able to work with the given node and metadata,
        it will raise a `ValueError`.

        Otherwise, it will prepare everything that can be done once here.
        """

    def show(self) -> Viewable:
        """Return a fresh Panel widget representing the node data and metadata.

        This method assumes that the widget is fully initialized and setup is completed.
        """
        raise NotImplementedError


class PGWidget(PluginGroup[Widget]):
    """Widget plugin group interface."""

    def check_plugin(self, widget_name: str, widget: Type[Widget]):
        self.check_is_subclass(widget_name, widget, Widget)
        if not widget.supported():
            raise TypeError("Widget must support at least one schema!")

    def supported_schemas(self) -> Set[PluginRef]:
        """Return union of all schemas supported by all installed widgets."""
        return set.union(*(set(w.supported()) for w in self.values()))

    def widgets_for(self, schema_ref: PluginRef) -> List[Type[Widget]]:
        """Return widgets that support the given schema."""
        return [wclass for wclass in self.values() if wclass.supports(schema_ref)]
