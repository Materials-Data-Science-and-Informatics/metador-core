"""Common widgets."""


import panel as pn
from bokeh.plotting import figure
from overrides import overrides
from panel.viewable import Viewable

from ..schema.common import ImageFileMeta
from . import Widget, WidgetPlugin


class ImageWidget(Widget):
    class Plugin(WidgetPlugin):
        name = "core.imagefile"
        version = (0, 1, 0)
        supports = [ImageFileMeta.Plugin.ref(version=(0, 1, 0))]

    # Declared type should be Union of schema classes that are listed as `supports`
    _meta: ImageFileMeta

    @overrides
    def setup(self):
        # If multiple supported schemas are listed,
        # case splitting based on the schema type must be done here.
        # Everything that instances can reuse should also be done here.
        self._w = self._meta.width.value  # type: ignore
        self._h = self._meta.height.value  # type: ignore
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
        return pn.panel(self.plot)
