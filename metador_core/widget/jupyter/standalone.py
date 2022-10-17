"""
Ad-hoc standalone dashboard/widget server for use within Jupyter notebooks.

Runs everything needed to see a dashboard or widget in threads.

This is mostly intended for convenient local use (e.g. by a researcher),
or could be adapted for a containerized (in the Docker-sense) environment, e.g.
where the user has metador libraries available and can inspect containers.
"""
import logging
import socket
from threading import Thread
from typing import List, Optional
from uuid import UUID

import panel as pn
from flask import Flask

from ...container.provider import SimpleContainerProvider
from ..server import WidgetServer


def get_free_port():
    # get a free port and use it (no way to retrieve it, when letting flask choose)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def silence_flask():
    """Disable HTTP request log (for use inside jupyter)."""
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)


# ----
# Use this Python module as singleton instance for server.
# As this is only used ad-hoc e.g. by a researcher playing in a notebook,
# this should be not a problem ("serious" servers are implemented elsewhere!).

DEFAULT_PANEL_EXTS = ["ace"]

host: str = "127.0.0.1"
port: int = -1

_known_containers: SimpleContainerProvider[UUID] = SimpleContainerProvider[UUID]()
_widget_server: WidgetServer = WidgetServer(_known_containers)


def widget_server() -> WidgetServer:
    return _widget_server


def container_provider() -> SimpleContainerProvider[UUID]:
    return _known_containers


def running() -> bool:
    return port > 0


def run(*, debug: bool = False, pn_exts: Optional[List[str]] = None):
    """Run ad-hoc standalone server to use widgets and dashboards in a Jupyter notebook."""
    global _widget_server, _known_containers, port

    if not debug:
        silence_flask()
    pn.extension(*(pn_exts or DEFAULT_PANEL_EXTS))  # required for panel within jupyter

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
