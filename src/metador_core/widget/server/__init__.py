"""The Metador widget server."""
from __future__ import annotations

import io
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

import numpy as np
import panel as pn
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.document import Document
from bokeh.embed import server_document
from bokeh.server.server import Server
from flask import Blueprint, request, send_file
from tornado.ioloop import IOLoop
from werkzeug.exceptions import BadRequest, NotFound

from metador_core.container import ContainerProxy, MetadorContainer, MetadorNode

if TYPE_CHECKING:
    from panel.viewable import Viewable


class WidgetServer:
    """Server backing the instances of Metador widgets (and dashboard).

    Metador widgets depend on a `WidgetServer` to:
    * get data from Metador containers (via special flask API, provided as a mountable blueprint)
    * wire up the information flow with a bokeh server instance (requirement for interactive bokeh widgets)

    For information on running a bokeh server see:
    https://docs.bokeh.org/en/latest/docs/user_guide/server.html#embedding-bokeh-server-as-a-library
    """

    @classmethod
    def _get_widget_arg(cls, args: Dict[str, List[bytes]], name: str) -> Optional[str]:
        """Extract argument from bokeh server request argument dict."""
        return args[name][0].decode("utf-8") if name in args and args[name] else None

    @classmethod
    def _get_widget_args(cls, doc: Document):
        """Extract arguments from bokeh server request parameters."""
        args = doc.session_context.request.arguments
        return dict(
            container_id=cls._get_widget_arg(args, "id"),
            container_path=cls._get_widget_arg(args, "path"),
        )

    @classmethod
    def _make_widget_args(
        cls, container_id: str, container_path: Optional[str]
    ) -> Dict[str, str]:
        """Construct dict to be passed through bokeh request into widget."""
        req_args = {"id": container_id}
        if container_path:
            req_args["path"] = container_path
        return req_args

    def _get_bokeh_widget_name(
        self,
        viewable_type: Literal["widget", "dashboard"],
        name: str,
    ) -> str:
        """Return mapped name of a registered widget or dashboard (bokeh server endpoint).

        Raises NotFound exception if widget has not been found.
        """
        if viewable_type not in {"widget", "dashboard"}:
            msg = f"Invalid type: {viewable_type}. Must be widget or dashboard!"
            raise NotFound(msg)
        known = self._reg_widgets if viewable_type == "widget" else self._reg_dashboards
        if name not in known:
            raise NotFound(f"Bokeh {viewable_type} not found: '{name}'")
        return known[name]

    def _get_container_node(
        self, container_id: str, container_path: Optional[str] = None
    ) -> Optional[Union[MetadorContainer, MetadorNode]]:
        """Retrieve desired container (and target path, if provided).

        If `path` is provided in the query parameters,
        will return the container node, otherwise returns the full container.

        Raises NotFound exception if container or path in container do not exist.
        """
        try:
            container = self._containers.get(container_id)
        except KeyError as e:
            raise NotFound(f"Container not found: '{container_id}'") from e

        if container_path is None:
            return container
        if node := container.get(container_path):
            return node.restrict(read_only=True, local_only=True)

        raise NotFound(f"Path not found in container: {container_path}")

    # ----

    def __init__(
        self,
        containers: ContainerProxy[str],
        *,
        bokeh_endpoint: Optional[str] = None,
        flask_endpoint: Optional[str] = None,
        populate: bool = True,
    ):
        """Widget server to serve widget- and dashboard-like bokeh entities.

        Args:
            containers: `ContainerProxy` to retrieve containers by some container id string.
            bokeh_endpoint: Endpoint where the bokeh server will run (`WidgetServer.run()`)
            flask_endpoint: Endpoint where Widget API is mounted (`WidgetServer.get_flask_blueprint()`)
            populate: If true (default), load and serve all installed widgets and generic dashboard
        """
        self._containers = containers
        self._bokeh_apps: Dict[str, Application] = {}
        self._reg_widgets: Dict[str, str] = {}
        self._reg_dashboards: Dict[str, str] = {}

        # these can be set after launching the server threads
        # (e.g. in case of dynamic port selection)
        self._flask_endpoint = flask_endpoint or ""
        self._bokeh_endpoint = bokeh_endpoint or ""

        if populate:
            self.register_installed()

    def register_installed(self) -> None:
        """Register installed widgets and the generic dashboard."""
        # NOTE: do imports here, otherwise circular imports.
        from metador_core.plugins import widgets

        from ..dashboard import Dashboard

        self.register_dashboard("generic", self.make_bokeh_app(Dashboard))
        for wclass in widgets.values():
            self.register_widget(
                wclass.Plugin.plugin_string(), self.make_bokeh_app(wclass)
            )

    def register_widget(self, name: str, bokeh_app: Application) -> None:
        """Register a new widget application."""
        mapped_name = f"w-{name}"
        self._bokeh_apps[f"/{mapped_name}"] = bokeh_app
        self._reg_widgets[name] = mapped_name

    def register_dashboard(self, name: str, bokeh_app: Application) -> None:
        """Register a new dashboard application."""
        mapped_name = f"d-{name}"
        self._bokeh_apps[f"/{mapped_name}"] = bokeh_app
        self._reg_dashboards[name] = mapped_name

    def make_bokeh_app(self, viewable_class: Viewable) -> Application:
        def handler(doc: Document) -> None:
            """Return bokeh app for Metador widget.

            In this context, a suitable class must satisfy the interface
            of being initialized with a metador node or container,
            and having a `show()` method returning a panel `Viewable`.

            The app will understand take `id` and optionally a `path` as query params.
            These are parsed and used to look up the correct container (node).
            """
            w_args = self._get_widget_args(doc)
            if c_obj := self._get_container_node(**w_args):
                # if we retrieved container / node, instantiate a widget and show it
                widget = viewable_class(
                    c_obj, server=self, container_id=w_args["container_id"]
                ).show()
                doc.add_root(widget.get_root(doc))

        return Application(FunctionHandler(handler, trap_exceptions=True))

    @property
    def flask_endpoint(self) -> str:
        """Get configured endpoint where WidgetServer API is mounted."""
        return self._flask_endpoint

    @flask_endpoint.setter
    def flask_endpoint(self, uri: str):
        """Set URI where the blueprint from `get_flask_blueprint` is mounted."""
        self._flask_endpoint = uri.rstrip("/")

    @property
    def bokeh_endpoint(self) -> str:
        """Get URI where the bokeh server is running."""
        return self._bokeh_endpoint

    @bokeh_endpoint.setter
    def bokeh_endpoint(self, uri: str):
        """Set URI where the bokeh server is running."""
        self._bokeh_endpoint = uri.rstrip("/")

    def run(self, **kwargs):
        """Run bokeh server with the registered apps (will block the current process)."""
        # kwargs["io_loop"] = kwargs.get("io_loop") or IOLoop()
        # server = pn.io.server.get_server(self._bokeh_apps, **kwargs)

        # NOTE: this loads unused extensions (e.g. ace) that are not even listed?!
        # pn.extension(inline=True)
        # This seems to work ok:
        pn.config.inline = True

        kwargs["loop"] = kwargs.get("io_loop") or IOLoop()
        server = Server(self._bokeh_apps, **kwargs)

        server.start()
        server.io_loop.start()

    # ----
    # Helper functions exposed to widgets

    def file_url_for(self, container_id: str, node: MetadorNode) -> str:
        """Return URL for given container ID and file at Metador Container node.

        To be used by widgets that need direct access to files in the container.
        """
        if not self._flask_endpoint:
            raise RuntimeError("missing flask endpoint!")
        return f"{self._flask_endpoint}/file/{container_id}{node.name}"

    # ----
    # Functions making up the WidgetServer API

    def index(self):
        """Return information about current Metador environment.

        Response includes an overview of metador-related Python packages,
        Metador plugins, and the known widgets (nodes) and dashboards (containers).
        """
        from metador_core.plugin.types import to_ep_name
        from metador_core.plugins import plugingroups

        # build dict with all available metador plugins
        pgs = {to_ep_name(x.name, x.version): x.dict() for x in plugingroups.keys()}
        groups = {plugingroups.Plugin.name: pgs}
        for pg in plugingroups.values():
            groups[pg.Plugin.name] = {
                to_ep_name(x.name, x.version): x.dict() for x in pg.keys()
            }

        return {
            "widgets": list(self._reg_widgets),
            "dashboards": list(self._reg_dashboards),
            "plugins": groups,
        }

    def download(self, container_id: str, container_path: str):
        """Return file download stream of a file embedded in the container."""
        node = self._get_container_node(container_id, container_path)
        # get data out of container
        obj = node[()]
        bs = obj.tolist() if isinstance(obj, np.void) else obj
        if not isinstance(bs, bytes):
            raise BadRequest(f"Path not a bytes object: /{container_path}")

        # construct a default file name based on path in container
        def_name = f"{container_id}_{container_path.replace('/', '__')}"
        # if object has attached file metadata, use it to serve data:
        filemeta = node.meta.get("core.file")
        name = filemeta.id_ if filemeta else def_name
        mime = filemeta.encodingFormat if filemeta else None

        # requested as explicit file download?
        dl = bool(request.args.get("download", False))
        # return file download stream with download metadata
        return send_file(
            io.BytesIO(bs), download_name=name, mimetype=mime, as_attachment=dl
        )

    def get_script(
        self,
        viewable_type: Literal["widget", "dashboard"],
        name: str,
        container_id: str,
        container_path: Optional[str] = None,
    ) -> str:
        """Return a script tag that will auto-load the desired widget for selected container."""
        if not self._bokeh_endpoint:
            raise RuntimeError("missing bokeh endpoint!")
        if viewable_type == "dashboard" and container_path:
            raise BadRequest("Dashboards do not accept a container path!")

        return server_document(
            f"{self._bokeh_endpoint}/{self._get_bokeh_widget_name(viewable_type, name)}",
            arguments=self._make_widget_args(container_id, container_path),
        )

    def get_flask_blueprint(self, *args):
        """Return a Flask blueprint with the Metador container and widget API."""
        api = Blueprint(*args)

        api.route("/")(self.index)
        api.route("/file/<container_id>/<path:container_path>")(self.download)
        api.route("/<viewable_type>/<name>/<container_id>/")(
            api.route("/<viewable_type>/<name>/<container_id>/<path:container_path>")(
                self.get_script
            )
        )

        return api


# NOTE: snippet to make script tag not evaluate by default
# it can be used to prevent the auto-loading during DOM injection, if needed for some reason
#     # disable self-evaluation (save call in variable, call when requested)
#     sc_id = re.match(r"\s*<script id=\"(.*)\">", script).group(1)
#     script = script.replace("(function", f"w{sc_id} = (function").replace(
#         "})();", "});"
#     )
#     return f'{script}<button type="button" onclick="w{sc_id}()">Load widget</button>'
