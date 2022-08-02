"""Common widgets."""

import panel as pn
from bokeh.plotting import figure
from panel.viewable import Viewable

from ..schema import schema_ref
from ..schema.common import ImageMeta
from . import Widget


class ImageWidget(Widget):
    # Declared type should be Union of schema classes that are listed as `supported`
    _meta: ImageMeta

    @classmethod
    def supported(cls):
        # this will add the currently installed common_image schema as supported
        return [schema_ref("common_image")]

    def setup(self):
        # If multiple supported schemas are listed,
        # case splitting based on the schema type must be done here.
        # Everything that instances can reuse should also be done here.
        self._aspect_ratio = self._meta.width / self._meta.height
        self._image_url = self.file_url_for(self._node)

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
            w=self._meta.width,
            h=self._meta.height,
            anchor="top_left",
        )
        self.plot.grid.grid_line_color = None
        self.plot.xaxis.visible = False
        self.plot.yaxis.visible = False
        return pn.panel(self.plot)
