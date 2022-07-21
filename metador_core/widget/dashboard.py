from __future__ import annotations

from typing import Dict, Optional, Tuple, Type, cast

import panel as pn
from panel.viewable import Viewable

from ..container import MetadorContainer, MetadorNode
from ..plugins import installed
from ..schema.core import MetadataSchema
from .interface import Widget

_SCHEMAS = installed["schema"]
_WIDGETS = installed["widget"]


class DashboardMeta(MetadataSchema):
    """Schema describing dashboard configuration for a node in a container."""

    show: bool
    """If true, the dashboard will try to instantiate a widget for this node."""

    schema: Optional[str]
    """Name of schema of an metadata object at the current node to be visualized.

    If not given, any suitable presentable object will be used.
    """

    widget: Optional[str]
    """Name of widget to be used to present the (meta)data.

    If not given, any suitable will be used.
    """


class Dashboard:
    """The dashboard presents a view of all marked nodes in a container.

    To be included in the dashboard, a node must be marked by a DashboardMeta
    object that has show=True and contains possibly additional directives.
    """

    def __init__(self, container: MetadorContainer):
        self._container: MetadorContainer = container

        # get nodes that are marked to be shown in dashboard
        self._to_show: Dict[MetadorNode, DashboardMeta] = {
            k: cast(DashboardMeta, v)
            for k, v in self._container.toc.find("dashboard_meta").items()
            if cast(DashboardMeta, v).show
        }
        # figure out what schemas to show and what widgets to use
        self._resolved: Dict[MetadorNode, Tuple[str, Type[Widget]]] = {}
        for node, dbmeta in self._to_show.items():
            self._resolved[node] = self._resolve_node(node, dbmeta)

    def _resolve_node(
        self, node: MetadorNode, dbmeta: DashboardMeta
    ) -> Tuple[str, Type[Widget]]:
        """Try to instantiate a widget for a node based on its dashboard metadata."""
        assert dbmeta.show
        if dbmeta.schema:
            schema_name = dbmeta.schema
            if _SCHEMAS.get(schema_name) is None:
                msg = f"Dashboard metadata contains unknown schema: {schema_name}"
                raise ValueError(msg)

            installed_schema = _SCHEMAS.fullname(schema_name)
            container_schema = self._container.toc.fullname(schema_name)
            if not installed_schema.supports(container_schema):
                msg = f"Dashboard metadata contains incompatible schema: {schema_name}"
                raise ValueError(msg)
        else:
            schema_name = ""
            for attached_obj_schema in node.find():
                container_schema = self._container.toc.fullname(attached_obj_schema)
                if container_schema in _WIDGETS.supported_schemas():
                    schema_name = attached_obj_schema
                    break

        if schema_name is None:
            msg = f"Cannot find schema suitable for known widgets for node: {node.name}"
            raise ValueError(msg)

        container_schema_ref = self._container.toc.fullname(schema_name)
        if dbmeta.widget:
            widget_class = _WIDGETS.get(dbmeta.widget)
            if not widget_class.supports(container_schema_ref):
                msg = f"Desired widget {dbmeta.widget} does not "
                msg += f"support {container_schema_ref}"
                raise ValueError(msg)
        else:
            widget_class = next(iter(_WIDGETS.widgets_for(schema_name)), None)
            if widget_class is None:
                raise ValueError("Could not find suitable widget for {schema_name}")

        return (schema_name, widget_class)

    def show(self) -> Viewable:
        """Instantiate widgets for container and return resulting dashboard."""
        db = pn.FlexBox(
            align_content="center", align_items="center", justify_content="center"
        )
        for node, tup in self._resolved.items():
            schema_name, widget_class = tup
            db.append(widget_class(node, schema_name))
        return db
