from __future__ import annotations

from functools import partial
from itertools import groupby
from typing import Dict, Iterable, List, Optional, Tuple

import panel as pn
from panel.viewable import Viewable
from phantom.interval import Open

from ..container import MetadorContainer, MetadorNode
from ..plugins import schemas, widgets
from ..schema import MetadataSchema
from ..schema.plugins import PluginRef
from ..schema.types import NonEmptyStr, SemVerTuple


class DashboardPriority(int, Open, low=1, high=10):
    ...


class DashboardGroup(int, Open, low=1):
    ...


class DashboardWidgetMeta(MetadataSchema):
    """Configuration for a widget in the dashboard."""

    priority: Optional[DashboardPriority] = DashboardPriority(1)
    """Priority of the widget (1-10), higher priority nodes are shown first."""

    group: Optional[DashboardGroup]
    """Dashboard group of the widget.

    Groups are presented in ascending order.
    Widgets are ordered by priority within a group.
    All widgets in a group are shown in a single row.

    Widgets without an assigned group come last.
    """

    schema_name: Optional[NonEmptyStr]
    """Name of schema of an metadata object at the current node to be visualized.

    If not given, any suitable presentable object will be used.
    """

    schema_version: Optional[SemVerTuple]

    widget_name: Optional[str]
    """Name of widget to be used to present the (meta)data.

    If not given, any suitable will be used.
    """

    widget_version: Optional[SemVerTuple]


class DashboardMeta(MetadataSchema):
    """Schema describing dashboard configuration for a node in a container.

    Instantiating without passing a list of widget configurations will
    return an instance that will show an arbitrary suitable widget, i.e.
    is equivalent to `DashboardMeta.show()`
    """

    class Plugin:
        name = "core.dashboard"
        version = (0, 1, 0)

    widgets: List[DashboardWidgetMeta] = [DashboardWidgetMeta()]
    """Widgets to present for this node in the dashboard.

    If left empty, will try present any widget usable for this node.
    """

    @classmethod
    def widget(cls, **kwargs) -> DashboardWidgetMeta:
        # for convenience
        return DashboardWidgetMeta(**kwargs)

    @classmethod
    def show(cls, _arg: List[DashboardWidgetMeta] = None, **kwargs):
        """Return an instance of dashboard configuration metadata.

        Convenience method to construct a configuration for the node
        for one or multiple widgets.

        For one widget, pass the widget config (if any) as keyword arguments,
        e.g.  `DashboardMeta.show(group=1)`.

        For multiple widgets, instantiate widget configurations with `widget(...)`,
        and pass them to `show`, e.g.:
        `DashboardMeta.show([DashboardMeta.widget(), DashboardMeta.widget(group=2)])`.
        """
        if _arg and kwargs:
            msg = "Pass widget config arguments or list of widget configs - not both!"
            raise ValueError(msg)

        if _arg is None:
            # kwargs have config for a singleton widget
            widgets = [cls.widget(**kwargs)]
        else:
            # multiple widgets, preconfigured
            widgets = list(_arg)
        return cls(widgets=widgets)


# ----

NodeWidgetPair = Tuple[MetadorNode, DashboardWidgetMeta]
"""A container node paired up with a widget configuration."""

NodeWidgetRow = List[NodeWidgetPair]
"""Sorted list of NodeWidgetPairs.

Ordered first by descending priority, then by ascending node path.
"""


def sorted_widgets(
    widgets: Iterable[NodeWidgetPair],
) -> Tuple[Dict[int, NodeWidgetRow], NodeWidgetRow]:
    """Return widgets in groups, ordered by priority and node path.

    Returns tuple with dict of groups and a remainder of ungrouped widgets.
    """

    def nwp_group(tup: NodeWidgetPair) -> int:
        return tup[1].group or 0

    def nwp_prio(tup: NodeWidgetPair) -> int:
        return -tup[1].priority or 0  # in descending order of priority

    def sorted_group(ws: Iterable[NodeWidgetPair]) -> NodeWidgetRow:
        """Sort first on priority, and for same priority on container node."""
        return list(sorted(sorted(ws, key=lambda x: x[0].name), key=nwp_prio))

    # dict, sorted in ascending group order (but ungrouped are 0)
    ret = dict(
        sorted(
            {
                k: sorted_group(v)
                for k, v in groupby(sorted(widgets, key=nwp_group), key=nwp_group)
            }.items()
        )
    )
    ungrp = ret.pop(0, [])  # separate out the ungrouped (were mapped to 0)
    return ret, ungrp


# ----


def _resolve_schema(node: MetadorNode, wmeta: DashboardWidgetMeta) -> PluginRef:
    """Return usable schema+version pair for the node based on widget metadata.

    If a schema name or schema version is missing, will complete these values.

    Usable schema means that:
    * there exists a compatible installed schema
    * there exists a compatible metadata object at given node

    Raises ValueError on failure to find a suitable schema.
    """
    if wmeta.schema_name is None:
        # if no schema selected -> pick any schema for which we have:
        #   * a schema instance at the current node
        #   * installed widget(s) that support it
        for obj_schema in node.meta.query():
            if next(widgets.widgets_for(obj_schema), None):
                return obj_schema

        msg = f"Cannot find suitable schema for a widget at node: {node.name}"
        raise ValueError(msg)

    # check that a node object is compatible with the one requested
    req_ver = wmeta.schema_version if wmeta.schema_version else "any"
    req_schema = f"{wmeta.schema_name} ({req_ver})"
    s_ref = next(node.meta.query(wmeta.schema_name, wmeta.schema_version), None)
    if s_ref is None:
        msg = f"Dashboard wants metadata compatible with {req_schema}, but node"
        if nrf := next(node.meta.query(wmeta.schema_name), None):
            nobj_schema = f"{nrf.name} {nrf.version}"
            msg += f"only has incompatible object: {nobj_schema}"
        else:
            msg += "has no suitable object"
        raise ValueError(msg)

    # if no version is specified, pick the one actually present at the node
    version = wmeta.schema_version or s_ref.version
    s_ref = schemas.PluginRef(name=wmeta.schema_name, version=version)

    # ensure there is an installed schema compatible with the one requested
    # (NOTE: if child schemas exist, the parents do too - no need to check)
    installed_schema = schemas.get(s_ref.name, s_ref.version)
    if installed_schema is None:
        msg = f"No installed schema is compatible with {req_schema}"
        raise ValueError(msg)

    return s_ref


def _widget_suitable_for(m_obj: MetadataSchema, w_ref: PluginRef) -> bool:
    """Granular check whether the widget actually works with the metadata object.

    Assumes that the passed object is known to be one of the supported schemas.
    """
    w_cls = widgets._get_unsafe(w_ref.name, w_ref.version)
    return w_cls.Plugin.primary and w_cls.supports_meta(m_obj)


def _resolve_widget(
    node: MetadorNode,
    s_ref: PluginRef,
    w_name: Optional[str],
    w_version: Optional[SemVerTuple],
) -> PluginRef:
    """Return suitable widget for the node based on given dashboard metadata."""
    if w_name is None:
        # get candidate widgets in alphabetic order (all that claim to work with schema)
        cand_widgets = sorted(widgets.widgets_for(s_ref))
        is_suitable = partial(_widget_suitable_for, node.meta[s_ref.name])
        # filter out the ones that ACTUALLY can handle the object and are eligible
        if w_ref := next(filter(is_suitable, cand_widgets), None):
            return w_ref
        else:
            msg = f"Could not find suitable widget for {w_name} at node {node.name}"
            raise ValueError(msg)

    # now we have a widget name (and possibly version) - check it
    widget_class = widgets._get_unsafe(w_name, w_version)
    if widget_class is None:
        raise ValueError(f"Could not find compatible widget: {w_name} {w_version}")
    if not widget_class.supports(*schemas.parent_path(s_ref.name, s_ref.version)):
        msg = f"Widget {widget_class.Plugin.ref()} does not support {s_ref}"
        raise ValueError(msg)

    w_ref = widget_class.Plugin.ref()
    return w_ref


# ----


class Dashboard:
    """The dashboard presents a view of all marked nodes in a container.

    To be included in the dashboard, a node must be marked by a DashboardMeta
    object that has show=True and contains possibly additional directives.
    """

    def __init__(self, container: MetadorContainer, *, server=None):
        self._container: MetadorContainer = container
        self._server = server

        # figure out what schemas to show and what widgets to use and collect
        ws: List[NodeWidgetPair] = []
        for node, dbmeta in self._container.metador.query(DashboardMeta).items():
            restr_node = node.restrict(read_only=True, local_only=True)
            for wmeta in dbmeta.widgets:
                ws.append((restr_node, self._resolve_node(node, wmeta)))

        grps, ungrp = sorted_widgets(ws)
        self._groups = grps
        self._ungrouped = ungrp

    def _resolve_node(
        self, node: MetadorNode, wmeta: DashboardWidgetMeta
    ) -> DashboardWidgetMeta:
        """Check and resolve widget dashboard metadata for a node."""
        wmeta = wmeta.copy()  # use copy, abandon original

        s_ref: PluginRef = _resolve_schema(node, wmeta)
        wmeta.schema_name = s_ref.name
        wmeta.schema_version = s_ref.version

        w_ref: PluginRef = _resolve_widget(
            node, s_ref, wmeta.widget_name, wmeta.widget_version
        )
        wmeta.widget_name = w_ref.name
        wmeta.widget_version = w_ref.version

        return wmeta

    def show(self) -> Viewable:
        """Instantiate widgets for container and return resulting dashboard."""
        w_width, w_height = 640, 480  # max size of a widget tile
        db_height = int(3.5 * w_height)  # max size of the dashboard
        db_width = int(2.5 * w_width)

        # Outermost element: The Dashboard is a column of widget groups
        db = pn.Column(scroll=True, height=db_height, width=db_width)

        # helper, to fill widget instances into row or flexbox
        def add_widgets(w_grp, ui_row):
            for node, wmeta in w_grp:
                w_cls = widgets.get(wmeta.widget_name, wmeta.widget_version)
                label = pn.pane.Str(f"{node.name}:")
                w_obj = w_cls(
                    node,
                    wmeta.schema_name,
                    wmeta.schema_version,
                    server=self._server,
                    max_width=w_width,
                    max_height=w_height,
                )
                ui_row.append(pn.Column(label, w_obj.show()))
            return ui_row

        # instantiate each widget group as row (those are non-wrapping)
        for widget_group in self._groups.values():
            db.append(
                add_widgets(
                    widget_group,
                    pn.Row(width=db_width, height=int(1.2 * w_height), scroll=True),
                )
            )
        # dump remaining ungrouped widgets into flexbox (auto-wrapping)
        db.append(add_widgets(self._ungrouped, pn.FlexBox()))
        return db
