"""Ad-hoc standalone widget server.

Provided for convenience use in local Jupyter notebooks.

Not to be used in any other settings.
"""
import socket
from threading import Thread

from flask import Flask

host: str = "127.0.0.1"
port: int = -1

_widget_server = None
_known_containers = None


def get_free_port():
    # get a free port and use it (no way to retrieve it when letting flask choose)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


# ----


def running() -> bool:
    return port > 0


def run():
    global _widget_server, _known_containers, port

    from . import WidgetServer
    from .util import ContainerIndex

    _known_containers = ContainerIndex()
    _widget_server = WidgetServer(_known_containers)

    port = get_free_port()
    flask_base = f"http://{host}:{port}"

    # prepare bokeh server
    bokeh_port = get_free_port()
    bokeh_base = f"http://{host}:{bokeh_port}"

    def run_bokeh():
        _widget_server.run(
            host=host, port=bokeh_port, allowed_websocket_origin=[f"{host}:{port}"]
        )

    # prepare flask server
    flask_app = Flask(__name__)
    _widget_server.set_bokeh_endpoint(bokeh_base)
    _widget_server.set_flask_endpoint(flask_base)
    flask_bokeh = _widget_server.get_flask_blueprint("widget-api", __name__)
    flask_app.register_blueprint(flask_bokeh)

    def run_flask():
        flask_app.run(host=host, port=port)

    # launch
    t_flask = Thread(target=run_flask)
    t_bokeh = Thread(target=run_bokeh)
    t_flask.start()
    t_bokeh.start()
    # t_flask.join()
    # t_bokeh.join()


def widget_server():
    if not running():
        run()
    return _widget_server


def container_index():
    if not running():
        return None
    return _known_containers
