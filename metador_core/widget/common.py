"""Common widgets."""

import json
from typing import List, Set, Type

import panel as pn
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
    """If non-empty, metadata objects must have a MIME type from this set."""

    FILE_EXTS: Set[str] = set()
    """If non-empty, filename must have an extension from this set."""

    @property
    def title(self) -> str:
        return self._meta.name or self._meta.filename or self._node.name

    @classmethod
    @overrides
    def supports_meta(cls, obj: MetadataSchema) -> bool:
        supported_mime = True
        if cls.MIME_TYPES:
            supported_mime = obj.encodingFormat in cls.MIME_TYPES
        supported_ext = False
        if cls.FILE_EXTS:
            supported_ext = obj.filename.endswith(tuple(cls.FILE_EXTS))
        # either supported mime or supported ext is enough
        return supported_mime or supported_ext


# wrap simple generic widgets from panel:

# pass content:


class MarkdownWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.text.md"
        version = (0, 1, 0)

    MIME_TYPES = {"text/markdown", "text/x-markdown"}
    FILE_EXTS = {".md", ".markdown"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.Markdown(
            self.file_data().decode("utf-8"), max_width=self._w, max_height=self._h
        )


class HTMLWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.text.html"
        version = (0, 1, 0)

    MIME_TYPES = {"text/html"}
    FILE_EXTS = {".htm", ".html"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.HTML(
            self.file_data().decode("utf-8"), max_width=self._w, max_height=self._h
        )


class CodeWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.text.code"
        version = (0, 1, 0)

    @classmethod
    @overrides
    def supports_meta(cls, obj: MetadataSchema) -> bool:
        return obj.encodingFormat.startswith("text")

    def show(self) -> Viewable:
        return pn.widgets.Ace(
            filename=self._meta.filename,
            readonly=True,
            value=self.file_data().decode("utf-8"),
            width=self._w,
            height=self._h,
        )


class DispatcherWidget(Widget):
    """Meta-widget to dispatch a node+metadata object to a more specific widget.

    Make sure that the dispatcher widget is probed before the widgets it can
    dispatch to.

    This works if and only if for each widget in listed in `WIDGETS`:
    * the plugin name of the dispatcher is a prefix of the widget name, or
    * the widget has `primary = False` and thus is not considered by the dashboard.
    """

    WIDGETS: List[Type[Widget]]
    """Widgets in the order they should be tested."""

    def dispatch(self, w_cls: Type[Widget]) -> Widget:
        """Dispatch to another widget (used by meta-widgets)."""
        return w_cls(
            self._node,
            "",
            server=self._server,
            metadata=self._meta,
            max_width=self._w,
            max_height=self._h,
        )

    @classmethod
    @overrides
    def supports_meta(cls, obj: MetadataSchema) -> bool:
        return any(map(lambda w: w.supports_meta(obj), cls.WIDGETS))

    @overrides
    def setup(self):
        for w_cls in self.WIDGETS:
            if w_cls.supports_meta(self._meta):
                self._widget = self.dispatch(w_cls)
                break

    @overrides
    def show(self) -> Viewable:
        return self._widget.show()


class TextWidget(DispatcherWidget, FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.text"
        version = (0, 1, 0)

    WIDGETS = [MarkdownWidget, HTMLWidget, CodeWidget]

    def show(self) -> Viewable:
        return super().show()


class JSONWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.json"
        version = (0, 1, 0)

    MIME_TYPES = {"application/json"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.JSON(
            json.loads(self.file_data()),
            name=self.title,
            max_width=self._w,
            max_height=self._h,
        )


# pass URL:


class PDFWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.pdf"
        version = (0, 1, 0)

    MIME_TYPES = {"application/pdf"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.PDF(self.file_url(), width=self._w, height=self._h)


class ImageWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.image"
        version = (0, 1, 0)

    MIME_TYPES = {"image/jpeg", "image/png", "image/gif", "image/svg+xml"}
    PANEL_WIDGET = {
        "image/jpeg": pn.pane.JPG,
        "image/png": pn.pane.PNG,
        "image/gif": pn.pane.GIF,
        "image/svg+xml": pn.pane.SVG,
    }

    @overrides
    def show(self) -> Viewable:
        return self.PANEL_WIDGET[self._meta.encodingFormat](
            self.file_url(), width=self._w, height=self._h
        )


class AudioWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.audio"
        version = (0, 1, 0)

    MIME_TYPES = {"audio/mpeg", "audio/ogg", "audio/webm"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.Audio(self.file_url(), name=self.title)


class VideoWidget(FileWidget):
    class Plugin(FileWidget.Plugin):
        name = "core.file.video"
        version = (0, 1, 0)

    MIME_TYPES = {"video/mp4", "video/ogg", "video/webm"}

    @overrides
    def show(self) -> Viewable:
        return pn.pane.Video(
            self.file_url(), name=self.title, max_width=self._w, max_height=self._h
        )
