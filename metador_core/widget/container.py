import wrapt

from ..container import MetadorContainer
from . import flask


class Previewable(wrapt.ObjectProxy):
    def __init__(self, container: MetadorContainer):
        super().__init__(container)
        if not flask.running():
            flask.start()
            print(f"http://{flask.host}:{flask.port}/")
        flask.register(container.uuid, container.ih5_files)

    def close(self, *args, **kwargs):
        flask.unregister(self.__wrapped__.uuid)
        super().close(*args, **kwargs)
