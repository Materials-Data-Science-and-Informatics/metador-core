"""Pluggable widgets for Metador."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Set, Type

from overrides import overrides
from panel.viewable import Viewable
from pydantic import Field
from typing_extensions import Annotated, TypeAlias

from ..container import MetadorNode
from ..plugin import interface as pg
from ..plugins import schemas
from ..schema import MetadataSchema
from ..schema.plugins import PluginRef
from .server import WidgetServer


class Widget(ABC):
    """Base class for metador widgets."""

    _args: Dict[str, Any]
    _node: MetadorNode
    _meta: MetadataSchema
    _server: WidgetServer

    Plugin: ClassVar[WidgetPlugin]

    def __init__(
        self,
        node: MetadorNode,
        schema_name: str = "",
        *,
        server: Optional[WidgetServer] = None,
        metadata: Optional[MetadataSchema] = None,
        max_width: Optional[int] = None,
        max_height: Optional[int] = None,
    ):
        """Instantiate a widget for a node.

        If no schema name is provided, the widget will try to pick the first metadata object
        from the node that is an instance of a supported schema, in the listed order.

        If no server is provided, a stand-alone server is started (e.g. for use in a notebook).

        If a metadata object is passed explicitly, it will be used instead of trying to
        retrieve one from the node.
        """
        self._node = node
        self._w = max_width
        self._h = max_height

        srv: WidgetServer
        if server is not None:
            srv = server
        else:
            from .server.standalone import widget_server

            srv = widget_server()

        if srv is None:
            raise ValueError("No widget server passed and standalone launch failed!")
        self._server = srv

        if metadata is not None:
            if not self.supports_meta(metadata):
                raise ValueError(
                    "Passed metadata is not instance of a supported schema!"
                )
            self._meta = metadata
        else:
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

    def file_data(self, node: Optional[MetadorNode] = None) -> bytes:
        """Return data at node as bytes."""
        node = node or self._node
        return node[()].tolist()

    def file_url(self, node: Optional[MetadorNode] = None) -> str:
        """Return URL resolving to the data at node."""
        node = node or self._node
        return self._server.file_url_for(node)

    @classmethod
    def supports(cls, *schemas: PluginRef) -> bool:
        """Return whether any (exact) schema is supported (version-compatible) by widget."""
        for schema in schemas:
            if any(map(lambda sref: sref.supports(schema), cls.Plugin.supports)):
                return True
        return False

    @classmethod
    def supports_meta(cls, obj: MetadataSchema) -> bool:
        """Return whether widget supports the specific metadata object.

        The passed object is assumed to be of one of the supported schema types.

        Default implementation will just check that the object is of a supported schema.

        Override to constrain further (e.g. check field values).

        This method affects the dashboard widget selection process and is used
        to check a metadata object if directly passed to `__init__`.
        """
        return cls.supports(type(obj).Plugin.ref())

    def setup(self):
        """Check that passed node is valid and do preparations.

        If multiple supported schemas are listed, case splitting based on the
        schema type should be done here to minimize logic in the rendering.

        Everything that instances can reuse, especially if it is computationally
        expensive, should also be done here.

        In case the widget is not able to work with the given node and metadata,
        it will raise a `ValueError`.
        """

    @abstractmethod
    def show(self) -> Viewable:
        """Return a fresh Panel widget representing the node data and/or metadata.

        If width and height were provided during initialization, the widget is supposed
        to fit within these dimensions, not exceed them and if possible, usefully
        fill up the space.

        This method assumes that the widget is fully initialized and setup is completed.
        """
        raise NotImplementedError


WIDGET_GROUP_NAME = "widget"

if TYPE_CHECKING:
    SchemaPluginRef: TypeAlias = PluginRef
else:
    SchemaPluginRef = schemas.PluginRef


class WidgetPlugin(pg.PluginBase):
    supports: Annotated[List[SchemaPluginRef], Field(min_items=1)]  # type: ignore
    """Return list of schemas supported by this widget."""

    primary: bool = True
    """Return whether the widget is a primary choice.

    If False, will not be used automatically by dashboard.
    """


class PGWidget(pg.PluginGroup[Widget]):
    """Widget plugin group interface."""

    class Plugin:
        name = WIDGET_GROUP_NAME
        version = (0, 1, 0)

        requires = [schemas.name]

        plugin_class = Widget
        plugin_info_class = WidgetPlugin

    @overrides
    def check_plugin(self, name: str, plugin: Type[Widget]):
        pg.check_implements_method(name, plugin, Widget.show)

    def plugin_deps(self, plugin):
        return set(map(lambda r: (schemas.name, r.name), plugin.Plugin.supports))

    def supported_schemas(self) -> Set[PluginRef]:
        """Return union of all schemas supported by all installed widgets.

        This includes registered child schemas (subclasses of supported schemas).
        """
        supported = set()
        for w in self.values():
            for sref in w.Plugin.supports:
                supported.add(sref)
                for cs_name in schemas.children(sref.name):
                    supported.add(schemas.fullname(cs_name))

        return supported

    def widgets_for(self, schema: PluginRef) -> List[str]:
        """Return widgets that support (are version-compatible) with the (exact) given schema."""
        return [w_name for w_name, w_cls in self.items() if w_cls.supports(schema)]
