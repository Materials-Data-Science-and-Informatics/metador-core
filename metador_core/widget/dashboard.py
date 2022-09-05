from __future__ import annotations

from itertools import chain, groupby
from typing import Iterable, List, Optional, Tuple

import panel as pn
from panel.viewable import Viewable
from phantom.interval import Open

from ..container import MetadorContainer, MetadorNode
from ..plugins import schemas, widgets
from ..schema import MetadataSchema


class DashboardPriority(int, Open, low=1, high=10):
    ...


class DashboardGroup(int, Open, low=1):
    ...


class DashboardWidgetMeta(MetadataSchema):
    """Configuration for a widget in the dashboard."""

    priority: Optional[DashboardPriority] = DashboardPriority(1)
    """Priority of the widget (1-10), higher priority nodes are shown first."""

    group: Optional[DashboardGroup] = None
    """Dashboard group of the widget.

    Groups are presented in ascending order.
    Widgets are ordered by priority within a group.

    Widgets without an assigned group come last.
    """

    metador_schema: Optional[str] = None
    """Name of schema of an metadata object at the current node to be visualized.

    If not given, any suitable presentable object will be used.
    """

    metador_widget: Optional[str] = None
    """Name of widget to be used to present the (meta)data.

    If not given, any suitable will be used.
    """


class DashboardMeta(MetadataSchema):
    """Schema describing dashboard configuration for a node in a container."""

    class Plugin:
        name = "core.dashboard"
        version = (0, 1, 0)

    widgets: List[DashboardWidgetMeta] = [DashboardWidgetMeta()]
    """Widgets to present for this node in the dashboard.

    If empty, will try present any widget usable for this node.
    """


NodeWidgetPair = Tuple[MetadorNode, DashboardWidgetMeta]
"""A container node paired up with a widget configuration."""


class Dashboard:
    """The dashboard presents a view of all marked nodes in a container.

    To be included in the dashboard, a node must be marked by a DashboardMeta
    object that has show=True and contains possibly additional directives.
    """

    def __init__(self, container: MetadorContainer, *, server=None):
        self._container: MetadorContainer = container
        self._server = server

        # figure out what schemas to show and what widgets to use and collect
        self._widgets: List[NodeWidgetPair] = []
        for node, dbmeta in self._container.toc.query(DashboardMeta).items():
            localized_node = node.restrict(local_only=True)
            for wmeta in dbmeta.widgets:
                self._widgets.append((localized_node, self._resolve_node(node, wmeta)))

        # order widgets by group, priority and node

        def group(tup: NodeWidgetPair) -> int:
            return tup[1].group or 0

        def prio(tup: NodeWidgetPair) -> int:
            return -tup[1].priority or 0  # in descending order of priority

        def sorted_widgets(ws: Iterable[NodeWidgetPair]) -> List[NodeWidgetPair]:
            """Sort first on priority, and for same priority on container node."""
            return list(sorted(sorted(ws, key=lambda x: x[0].name), key=prio))

        # dict, sorted in ascending group order
        self._groups = dict(
            sorted(
                {
                    k: sorted_widgets(v)
                    for k, v in groupby(sorted(self._widgets, key=group), key=group)
                }.items()
            )
        )
        # separate out the ungrouped widgets
        self._ungrouped = self._groups[0]
        del self._groups[0]

    def _resolve_node(
        self, node: MetadorNode, wmeta: DashboardWidgetMeta
    ) -> DashboardWidgetMeta:
        """Try to instantiate a widget for a node based on its dashboard metadata."""
        ret = wmeta.copy()
        if ret.metador_schema is not None:
            if schemas.get(ret.metador_schema) is None:
                msg = (
                    f"Dashboard metadata contains unknown schema: {ret.metador_schema}"
                )
                raise ValueError(msg)

            installed_schema = schemas.fullname(ret.metador_schema)
            container_schema = self._container.toc.fullname(ret.metador_schema)
            if not installed_schema.supports(container_schema):
                msg = f"Dashboard metadata contains incompatible schema: {container_schema}"
                raise ValueError(msg)
        else:
            for attached_obj_schema in node.meta.find():
                container_schema = self._container.toc.fullname(attached_obj_schema)
                if container_schema in widgets.supported_schemas():
                    ret.metador_schema = attached_obj_schema
                    break

        if ret.metador_schema is None:
            msg = f"Cannot find schema suitable for known widgets for node: {node.name}"
            raise ValueError(msg)

        container_schema_ref = self._container.toc.fullname(ret.metador_schema)
        if ret.metador_widget is not None:
            widget_class = widgets.get(ret.metador_widget)
            if widget_class is None:
                raise ValueError(f"Could not find widget: {ret.metador_widget}")
            if not widget_class.supports(container_schema_ref):
                msg = f"Desired widget {ret.metador_widget} does not "
                msg += f"support {container_schema_ref}"
                raise ValueError(msg)
        else:
            cand_widgets = widgets.widgets_for(container_schema_ref)
            ret.metador_widget = next(iter(cand_widgets), None)

        if ret.metador_widget is None:
            msg = f"Could not find suitable widget for {ret.metador_schema} at node {node.name}"
            raise ValueError(msg)

        return ret

    def show(self) -> Viewable:
        """Instantiate widgets for container and return resulting dashboard."""
        flexbox_conf = dict(
            align_content="center", align_items="center", justify_content="center"
        )
        db = pn.FlexBox(**flexbox_conf)
        for widget_group in chain(self._groups.values(), (self._ungrouped,)):
            grp = pn.FlexBox(**flexbox_conf)
            for node, wmeta in widget_group:
                w_cls = widgets[wmeta.metador_widget]
                grp.append(w_cls(node, wmeta.metador_schema, self._server).show())
            db.append(grp)
        return db
