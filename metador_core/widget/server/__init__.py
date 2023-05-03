"""The Metador widget server."""
import io
from typing import Dict, Optional, Union

import numpy as np
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.embed import server_document
from bokeh.server.server import Server
from flask import Blueprint, request, send_file
from tornado.ioloop import IOLoop
from typing_extensions import Literal
from werkzeug.exceptions import BadRequest, NotFound

from metador_core.container import ContainerProxy, MetadorContainer, MetadorNode


def get_arg(args, name) -> Optional[str]:
    """Extract argument from bokeh server request argument dict."""
    if name in args and args[name]:
        return args[name][0].decode("utf-8")
    return None


class WidgetServer:
    """Server backing the instances of Metador widgets (and dashboard).

    Metador widgets depend on a `WidgetServer` to:
    * get data from Metador containers (via special flask API, provided as a mountable blueprint)
    * wire up the information flow with a bokeh server instance (requirement for interactive bokeh widgets)

    For information on running a bokeh server see:
    https://docs.bokeh.org/en/latest/docs/user_guide/server.html#embedding-bokeh-server-as-a-library
    """

    def __init__(self, containers: ContainerProxy[str], populate: bool = True):
        """Widget server to serve widget- and dashboard-like bokeh entities.

        Requires a `ContainerProxy` that is used to retrieve containers by some container id string.

        If populate is True (default), will load and serve all installed widgets
        and also add the generic panel dashboard.
        """
        self._containers = containers
        self._bokeh_apps: Dict[str, Application] = {}
        self._reg_widgets: Dict[str, str] = {}
        self._reg_dashboards: Dict[str, str] = {}
        if populate:
            self.register_installed()

        # these are to be set after launching the server threads
        self._flask_endpoint = ""
        self._bokeh_endpoint = ""

    def parse_and_retrieve(self, doc) -> Optional[Union[MetadorContainer, MetadorNode]]:
        """Parse query parameters, lookup container and possibly node at path.

        If `path` is provided in the query parameters,
        will return the container node, otherwise returns the full container.
        """
        args = doc.session_context.request.arguments
        container_id = get_arg(args, "id")
        path = get_arg(args, "path")

        try:
            container = self._containers.get(container_id)
        except TypeError:
            container = None
        if path is None or container is None:
            return container

        if node := container.get(path):
            return node.restrict(read_only=True, local_only=True)
        return None

    def make_bokeh_app(self, viewable_class) -> Application:
        def handler(doc):
            """Return bokeh app for Metador widget.

            In this context, a suitable class must satisfy the interface
            of being initialized with a metador node or container,
            and having a `show()` method returning a panel `Viewable`.

            The app will understand take `id` and optionally a `path` as query params.
            These are parsed and used to look up the correct container (node).
            """
            if obj := self.parse_and_retrieve(doc):
                # if we retrieved container / node, instantiate a widget and show it
                doc.add_root(viewable_class(obj, server=self).show().get_root(doc))

        return Application(FunctionHandler(handler))

    def register_widget(self, name: str, bokeh_app: Application):
        mapped_name = f"w-{name}"
        self._bokeh_apps[f"/{mapped_name}"] = bokeh_app
        self._reg_widgets[name] = mapped_name

    def register_dashboard(self, name: str, bokeh_app: Application):
        mapped_name = f"d-{name}"
        self._bokeh_apps[f"/{mapped_name}"] = bokeh_app
        self._reg_dashboards[name] = mapped_name

    def register_installed(self):
        # register installed widgets and the generic dashboard.
        # do imports here, otherwise circular imports.
        from metador_core.plugins import widgets

        from ..dashboard import Dashboard

        self.register_dashboard("generic", self.make_bokeh_app(Dashboard))
        for wclass in widgets.values():
            self.register_widget(
                wclass.Plugin.plugin_string(), self.make_bokeh_app(wclass)
            )

    def run(self, **kwargs):
        """Run bokeh server with the registered apps (will block the current process)."""
        # kwargs["io_loop"] = kwargs.get("io_loop") or IOLoop()
        # server = pn.io.server.get_server(self._bokeh_apps, **kwargs)

        kwargs["loop"] = kwargs.get("io_loop") or IOLoop()
        server = Server(self._bokeh_apps, **kwargs)

        server.start()
        server.io_loop.start()

    def _expect_container(self, container_id: str) -> MetadorContainer:
        if c := self._containers.get(container_id):
            return c
        raise NotFound(f"Container not found: '{container_id}'")

    def file_url_for(self, node: MetadorNode) -> str:
        # TODO: this won't work for other container proxies
        # must add method to container proxy to do reverse lookup of container id
        # based on the metador container uuid (which the node knows)
        return (
            f"{self._flask_endpoint}/file/{str(node.metador.container_uuid)}{node.name}"
        )

    def set_flask_endpoint(self, uri: str):
        """Set URI where the blueprint from `get_flask_blueprint` is mounted."""
        self._flask_endpoint = uri.rstrip("/")

    def set_bokeh_endpoint(self, uri: str):
        """Set URI where the bokeh server is running."""
        self._bokeh_endpoint = uri.rstrip("/")

    def get_script(
        self,
        viewable_type: Literal["widget", "dashboard"],
        name: str,
        container_id: str,
        container_path: Optional[str] = None,
    ):
        assert self._bokeh_endpoint
        if viewable_type not in {"widget", "dashboard"}:
            msg = f"Invalid type: {viewable_type}. Must be widget or dashboard!"
            raise NotFound(msg)
        if viewable_type == "dashboard" and container_path:
            raise BadRequest("Dashboards do not accept a path!")
        known = self._reg_widgets if viewable_type == "widget" else self._reg_dashboards
        if name not in known:
            raise NotFound(f"Bokeh {viewable_type} not found: '{name}'")
        req_args = {"id": container_id}
        if container_path:
            req_args["path"] = container_path
        return server_document(
            f"{self._bokeh_endpoint}/{known[name]}", arguments=req_args
        )

    def get_flask_blueprint(self, *args, **kwargs):
        """Return the internal widget API Flask blueprint.

        Widgets require a flask app with this API mounted to work.
        """
        assert self._bokeh_endpoint
        api = Blueprint(*args, **kwargs)

        @api.route("/")
        def index():
            """Return list of registered widgets (node-based) and dashboards (container-based)."""
            return {
                "widgets": list(self._reg_widgets),
                "dashboards": list(self._reg_dashboards),
            }

        @api.route("/file/<container_id>/<path:container_path>")
        def download(container_id: str, container_path: str):
            """Return a file download of an embedded file in the container.

            Needed for widgets that need the raw data resolvable via an URL in the frontend.
            """
            container = self._containers.get(container_id)
            if container is None:
                return "no such container"
            if container_path not in container:
                raise NotFound(f"Path not in record: /{container_path}")

            obj = container[container_path][()]
            if isinstance(obj, np.void):
                bs = obj.tolist()
            else:
                bs = obj
            if not isinstance(bs, bytes):
                raise BadRequest(f"Path not a binary object: /{container_path}")

            dl = bool(request.args.get("download", False))  # as explicit file download?
            # if object has attached file metadata, use it to serve:
            filemeta = container[container_path].meta.get("core.file")
            def_name = f"{container_id}_{container_path.replace('/', '__')}"
            name = filemeta.id_ if filemeta else def_name
            mime = filemeta.encodingFormat if filemeta else None
            return send_file(
                io.BytesIO(bs), download_name=name, mimetype=mime, as_attachment=dl
            )

        @api.route("/<viewable_type>/<name>/<container_id>/")
        @api.route("/<viewable_type>/<name>/<container_id>/<path:container_path>")
        def get_script(*args, **kwargs):
            """Return script tag that auto-loads the desired widget/dashboard."""
            return self.get_script(*args, **kwargs)

        return api


# NOTE: snippet to make script tag not evaluate by default
# it can be used to prevent the auto-loading during DOM injection, if needed for some reason
#     # disable self-evaluation (save call in variable, call when requested)
#     sc_id = re.match(r"\s*<script id=\"(.*)\">", script).group(1)
#     script = script.replace("(function", f"w{sc_id} = (function").replace(
#         "})();", "});"
#     )
#     return f'{script}<button type="button" onclick="w{sc_id}()">Load widget</button>'
