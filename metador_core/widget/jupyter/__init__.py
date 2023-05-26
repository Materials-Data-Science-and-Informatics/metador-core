"""Functionality to make widgets and dashboard work stand-alone in Jupyter."""

import wrapt

from ...container import MetadorContainer
from . import standalone


class Previewable(wrapt.ObjectProxy):
    """Wrapper to be used around MetadorContainer inside Jupyter.

    Will ensure that widgets can work in the notebook.
    """

    def __init__(self, container: MetadorContainer):
        super().__init__(container)

        if not standalone.running():
            standalone.run()

        if provider := standalone.container_provider():
            provider[str(self.metador.container_uuid)] = self

    def close(self, *args, **kwargs):
        if provider := standalone.container_provider():
            del provider[str(self.metador.container_uuid)]

        self.__wrapped__.close(*args, **kwargs)


__all__ = ["Previewable"]
