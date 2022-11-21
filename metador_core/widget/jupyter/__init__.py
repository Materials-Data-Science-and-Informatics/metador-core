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

        if prv := standalone.container_provider():
            prv[self.metador.container_uuid] = (
                self.metador.driver,
                self.metador.source,
            )

    def close(self, *args, **kwargs):
        if prv := standalone.container_provider():
            del prv[self.metador.container_uuid]

        self.__wrapped__.close(*args, **kwargs)


__all__ = ["Previewable"]
