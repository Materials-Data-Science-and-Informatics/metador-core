"""Common widgets."""

import json
from typing import Set

import panel as pn
from bokeh.plotting import figure
from overrides import overrides
from panel.viewable import Viewable

from metador_core.schema import MetadataSchema

from ..plugins import schemas
from . import Widget

FileMeta = schemas.get("core.file", (0, 1, 0))
ImageFileMeta = schemas.get("core.imagefile", (0, 1, 0))


class FileWidget(Widget):
    """Simple widget based on (a subschema of) 'core.file'.

    Allows to state supported MIME types with less boilerplate.
    """

    class Plugin:
        # name and version must be overridden in subclasses
        supports = [FileMeta.Plugin.ref()]

    MIME_TYPES: Set[str] = set()

    @classmethod
    @overrides
    def supports_meta(cls, obj: MetadataSchema) -> bool:
        return obj.encodingFormat in cls.MIME_TYPES

    @overrides
    def setup(self):
        self._name: str = self._meta.name or self._node.name
        self._data: bytes = self._node[()].tolist()


# wrap simple text-based widgets from panel:


class MarkdownWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.markdown"
        version = (0, 1, 0)

    MIME_TYPES = {"text/plain"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.Markdown(self._data.decode("utf-8"))


class HTMLWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.html"
        version = (0, 1, 0)

    MIME_TYPES = {"text/html"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.HTML(self._data.decode("utf-8"))


class JSONWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.json"
        version = (0, 1, 0)

    MIME_TYPES = {"application/json"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.JSON(json.loads(self._data), name=self._name)


# ----


class ImageWidget(Widget):
    class Plugin:
        name = "core.imagefile"
        version = (0, 1, 0)
        supports = [ImageFileMeta.Plugin.ref()]

    @overrides
    def setup(self):
        # If multiple supported schemas are listed,
        # case splitting based on the schema type must be done here.
        # Everything that instances can reuse should also be done here.
        self._w = self._meta.width.value
        self._h = self._meta.height.value
        self._aspect_ratio = self._w / self._h
        self._image_url = self.file_url_for(self._node)

    @overrides
    def show(self) -> Viewable:
        # Construct the actual widget instance. Every call returns a fresh one.
        self.plot = figure(
            aspect_ratio=self._aspect_ratio,
            max_width=640,
            max_height=360,
            sizing_mode="scale_both",
        )
        self.plot.image_url(
            url=[self._image_url],
            x=0,
            y=0,
            w=self._w,
            h=self._h,
            anchor="top_left",
        )
        self.plot.grid.grid_line_color = None
        self.plot.xaxis.visible = False
        self.plot.yaxis.visible = False
        # Hide the Bokeh logo, in the future there could be a metador logo instead
        self.plot.toolbar.logo = None
        return pn.panel(self.plot)
