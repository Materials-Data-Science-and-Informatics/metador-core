"""Generic container dashboard.

To **configure** a container dashboard: attach `DashboardConf` metadata to `MetadorContainer` nodes.

To **show** a container dashboard: create a `Dashboard` instance.
"""
from __future__ import annotations

from functools import partial
from itertools import groupby
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

import panel as pn
from panel.viewable import Viewable
from phantom.interval import Inclusive

from ..container import MetadorContainer, MetadorNode
from ..plugins import schemas, widgets
from ..schema import MetadataSchema
from ..schema.plugins import PluginRef
from ..schema.types import NonEmptyStr, SemVerTuple

if TYPE_CHECKING:
    from . import WidgetServer


class DashboardPriority(int, Inclusive, low=1, high=10):
    """Dashboard priority of a widget."""


class DashboardGroup(int, Inclusive, low=1):
    """Dashboard group of a widget."""


class WidgetConf(MetadataSchema):
    """Configuration of a widget in the dashboard."""

    priority: Optional[DashboardPriority] = DashboardPriority(1)
    """Priority of the widget (1-10), higher priority nodes are shown first."""

    group: Optional[DashboardGroup]
    """Dashboard group of the widget.

    Groups are presented in ascending order.
    Widgets are ordered by priority within a group.
    All widgets in a group are shown in a single row.

    Widgets without an assigned group come last.
    """

    # ----

    schema_name: Optional[NonEmptyStr]
    """Name of schema of an metadata object at the current node that is to be visualized.

    If not given, any suitable will be selected if possible.
    """

    schema_version: Optional[SemVerTuple]
    """Version of schema to be used.

    If not given, any suitable will be selected if possible.
    """

    widget_name: Optional[str]
    """Name of widget to be used.

    If not given, any suitable will be selected if possible.
    """

    widget_version: Optional[SemVerTuple]
    """Version of widget to be used.

    If not given, any suitable will be selected if possible.
    """


class DashboardConf(MetadataSchema):
    """Schema describing dashboard configuration for a node in a container.

    Instantiating without passing a list of widget configurations will
    return an instance that will show an arbitrary suitable widget, i.e.
    is equivalent to `DashboardConf.show()`
    """

    class Plugin:
        name = "core.dashboard"
        version = (0, 1, 0)

    widgets: List[WidgetConf] = [WidgetConf()]
    """Widgets to present for this node in the dashboard.

    If left empty, will try present any widget usable for this node.
    """

    @staticmethod
    def widget(**kwargs) -> WidgetConf:
        """Construct a dashboard widget configuration (see `WidgetConf`)."""
        # for convenience
        return WidgetConf(**kwargs)

    @classmethod
    def show(cls, _arg: List[WidgetConf] = None, **kwargs):
        """Construct a dashboard configuration for the widget(s) of one container node.

        For one widget, pass the widget config (if any) as keyword arguments,
        e.g.  `DashboardConf.show(group=1)`.

        For multiple widgets, create widget configurations with `widget(...)`,
        and pass them to `show`, e.g.:
        `DashboardConf.show([DashboardConf.widget(), DashboardConf.widget(group=2)])`.
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

NodeWidgetPair = Tuple[MetadorNode, WidgetConf]
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


def _resolve_schema(node: MetadorNode, wmeta: WidgetConf) -> PluginRef:
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


def get_grp_label(idx):
    """Create and return a styled group label."""
    return pn.pane.Str(
        f"Group {idx+1}" if idx is not None else "Ungrouped resources",
        style={
            "font-size": "15px",
            "font-weight": "bold",
            "text-decoration": "underline",
        },
    )


def add_widgets(w_grp, ui_grp, *, server=None, container_id: Optional[str] = None):
    """Instantiate and add widget to the flexibly wrapping row that handles the entire group."""
    w_width, w_height = 500, 500  # max size of a widget tile, arbitrarily set
    for node, wmeta in w_grp:
        w_cls = widgets.get(wmeta.widget_name, wmeta.widget_version)
        label = pn.pane.Str(f"{node.name}:")

        # instantiating the appropriate widget
        w_obj = w_cls(
            node,
            wmeta.schema_name,
            wmeta.schema_version,
            server=server,
            container_id=container_id,
            # reset max widget of a widget tile,  only if it is for a pdf, text or video file
            max_width=700
            if "pdf" in wmeta.widget_name
            or "text" in wmeta.widget_name
            or "video" in wmeta.widget_name
            else w_width,
            # reset max height of a widget tile, only if it is for a text file
            max_height=700 if "text" in wmeta.widget_name else w_height,
        )

        # adding the new widget to the given row
        ui_grp.append(
            pn.Column(
                label,
                w_obj.show(),
                sizing_mode="scale_both",
                scroll=False
                if "image" in wmeta.widget_name or "pdf" in wmeta.widget_name
                else True,
            )
        )
    return ui_grp


def get_grp_row(
    *,
    idx=None,
    widget_group=None,
    divider=False,
    server=None,
    container_id: Optional[str] = None,
):
    """Create a flexible and wrapping row for all widgets within a single group."""
    return pn.FlexBox(
        get_grp_label(idx=idx),
        add_widgets(
            widget_group,
            pn.FlexBox(
                flex_direction="row",
                justify_content="space-evenly",
                align_content="space-evenly",
                align_items="center",
                sizing_mode="scale_both",
            ),
            server=server,
            container_id=container_id,
        ),
        pn.layout.Divider(margin=(100, 0, 20, 0)) if divider else None,
        flex_direction="column",
        justify_content="space-evenly",
        align_content="space-evenly",
        align_items="center",
        sizing_mode="scale_both",
    )


class Dashboard:
    """The dashboard presents a view of all marked nodes in a container.

    To be included in the dashboard, a node must be marked by a `DashboardConf`
    object configuring at least one widget for that node.


    Note that the `Dashboard` needs
    * either a widget server to be passed (embedding in a website),
    * or the container is wrapped by `metador_core.widget.jupyter.Previewable` (notebook mode)
    """

    def __init__(
        self,
        container: MetadorContainer,
        *,
        server: WidgetServer = None,
        container_id: Optional[str] = None,
    ):
        """Return instance of a dashboard.

        Args:
            container: Actual Metador container that is open and readable
            server: `WidgetServer` to use for the widgets (default: standalone server / Jupyter mode)
            container_id: Container id usable with the server to get this container (default: container UUID)
        """
        self._container: MetadorContainer = container
        self._server = server
        self._container_id: str = container_id

        # figure out what schemas to show and what widgets to use and collect
        ws: List[NodeWidgetPair] = []
        for node in self._container.metador.query(DashboardConf):
            dbmeta = node.meta.get(DashboardConf)
            restr_node = node.restrict(read_only=True, local_only=True)
            for wmeta in dbmeta.widgets:
                ws.append((restr_node, self._resolve_node(node, wmeta)))

        grps, ungrp = sorted_widgets(ws)
        self._groups = grps
        self._ungrouped = ungrp

    def _resolve_node(self, node: MetadorNode, wmeta: WidgetConf) -> WidgetConf:
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
        # Outermost element: The Dashboard is a column of widget groups
        db = pn.FlexBox(
            flex_direction="column",
            justify_content="space-evenly",
            align_content="space-evenly",
            align_items="center",
            sizing_mode="scale_both",
        )

        # add each widget group within individual, flexibly-wrapping rows
        for idx, widget_group in enumerate(self._groups.values()):
            db.append(
                get_grp_row(
                    idx=idx,
                    widget_group=widget_group,
                    divider=False,  # does not work offline with panel >= 1.0?
                    server=self._server,
                    container_id=self._container_id,
                )
            )

        # dump remaining ungrouped widgets into a separate flexibly-wrapping row
        ungrp_exist = len(self._ungrouped) != 0
        if ungrp_exist:
            db.append(
                get_grp_row(
                    widget_group=self._ungrouped,
                    server=self._server,
                    container_id=self._container_id,
                )
            )
        return db
