"""Pluggable widgets for Metador."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Literal, Optional, Set, Type

from overrides import EnforceOverrides, overrides
from panel.viewable import Viewable
from pydantic import Field
from typing_extensions import Annotated

from ..container import MetadorNode
from ..plugin import interface as pg
from ..plugin import plugingroups
from ..schema import MetadataSchema
from ..schema.pg import PG_NAME as SCHEMA_GROUP_NAME
from ..schema.pg import PGSchema
from .server import WidgetServer


class Widget(ABC, EnforceOverrides):
    """Base class for metador widgets."""

    _node: MetadorNode
    _meta: MetadataSchema
    _server: WidgetServer

    Plugin: WidgetPlugin

    def __init__(
        self,
        node: MetadorNode,
        schema_name: str = "",
        server: Optional[WidgetServer] = None,
    ):
        self._node = node

        srv: WidgetServer
        if server is not None:
            srv = server
        else:
            from .server.standalone import widget_server

            srv = widget_server()

        if srv is None:
            raise ValueError("No widget server passed and standalone launch failed!")
        self._server = srv

        if not schema_name:
            for schemaref in self.Plugin.supports:
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
    def supports(cls, schema: PGSchema.PluginRef) -> bool:
        """Return whether a certain schema is supported by the widget."""
        return any(map(lambda sref: sref.supports(schema), cls.Plugin.supports))

    def setup(self):
        """Check that passed node and parsed metadata is valid and do preparations.

        In case the widget is not able to work with the given node and metadata,
        it will raise a `ValueError`.

        Otherwise, it will prepare everything that can be done once here.
        """

    @abstractmethod
    def show(self) -> Viewable:
        """Return a fresh Panel widget representing the node data and metadata.

        This method assumes that the widget is fully initialized and setup is completed.
        """
        raise NotImplementedError


WIDGET_GROUP_NAME = "widget"


class WidgetPlugin(pg.PluginBase):
    group = WIDGET_GROUP_NAME
    supports: List[PGSchema.PluginRef]

    class Fields(pg.PluginBase.Fields):
        supports: Annotated[List[PGSchema.PluginRef], Field(min_items=1)]
        """Return list of schemas supported by this widget."""


class PGWidget(pg.PluginGroup[Widget]):
    """Widget plugin group interface."""

    class PluginRef(pg.PluginRef):
        group: Literal["widget"]

    class Plugin(pg.PGPlugin):
        name = WIDGET_GROUP_NAME
        version = (0, 1, 0)
        required_plugin_groups = [SCHEMA_GROUP_NAME]
        plugin_class = Widget
        plugin_info_class = WidgetPlugin

    @overrides
    def check_plugin(self, name: str, plugin: Type[Widget]):
        pg.check_implements_method(name, plugin, Widget.show)

    def supported_schemas(self) -> Set[PGSchema.PluginRef]:
        """Return union of all schemas supported by all installed widgets."""
        return set.union(*(set(w.Plugin.supports) for w in self.values()))

    def widgets_for(self, schema: PGSchema.PluginRef) -> List[Type[Widget]]:
        """Return widgets that support the given schema."""
        return [wclass for wclass in self.values() if wclass.supports(schema)]


widgets: PGWidget
widgets = plugingroups.get(WIDGET_GROUP_NAME, PGWidget)
