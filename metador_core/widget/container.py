import panel as pn
import wrapt

from ..container import MetadorContainer
from ..ih5.container import IH5Record
from .server import standalone


class Previewable(wrapt.ObjectProxy):
    """Wrapper to be used around MetadorContainer inside Jupyter.

    Will ensure that widgets can work.
    """

    def __init__(self, container: MetadorContainer):
        super().__init__(container)

        if not standalone.running():
            pn.extension()  # required for panel in jupyter
            standalone.run()  # required for our widgets to work

        if idx := standalone.container_index():
            if isinstance(container, IH5Record):
                idx.update(container.container_uuid, container.ih5_files)
            else:
                idx.update(container.container_uuid, [container.filename])

    def close(self, *args, **kwargs):
        if idx := standalone.container_index():
            idx.update(self.__wrapped__.container_uuid, [])
        self.__wrapped__.close(*args, **kwargs)
