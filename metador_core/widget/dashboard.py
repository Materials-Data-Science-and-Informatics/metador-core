from __future__ import annotations

from functools import partial
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

    group: Optional[DashboardGroup]
    """Dashboard group of the widget.

    Groups are presented in ascending order.
    Widgets are ordered by priority within a group.
    All widgets in a group are shown in a single row.

    Widgets without an assigned group come last.
    """

    metador_schema: Optional[str]
    """Name of schema of an metadata object at the current node to be visualized.

    If not given, any suitable presentable object will be used.
    """

    metador_widget: Optional[str]
    """Name of widget to be used to present the (meta)data.

    If not given, any suitable will be used.
    """


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


NodeWidgetPair = Tuple[MetadorNode, DashboardWidgetMeta]
"""A container node paired up with a widget configuration."""


def widget_suitable_for(m_obj: MetadataSchema, w_name: str) -> bool:
    """Granular check whether the widget actually works with the metadata object.

    Assumes that the passed object is known to be one of the supported schemas.
    """
    w_cls = widgets._get_unsafe(w_name)
    return w_cls.Plugin.primary and w_cls.supports_meta(m_obj)


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
            # no desired schema selected -> pick any schema for which we have:
            #   * a schema instance at the current node
            #   * installed widget(s) that support it
            for attached_obj_schema in node.meta.find():
                container_schema = self._container.toc.fullname(attached_obj_schema)
                if container_schema in widgets.supported_schemas():
                    ret.metador_schema = attached_obj_schema
                    break

        if ret.metador_schema is None:
            msg = f"Cannot find schema suitable for known widgets for node: {node.name}"
            raise ValueError(msg)

        # TODO: use parent_path of the CONTAINER schema (toc.schemas should be PluginRefs)
        # this is not correct yet!
        cand_schemas = list(
            map(lambda n: schemas.fullname(n), schemas.parent_path(ret.metador_schema))
        )

        container_schema_ref = self._container.toc.fullname(ret.metador_schema)
        if ret.metador_widget is not None:
            widget_class = widgets._get_unsafe(ret.metador_widget)
            if widget_class is None:
                raise ValueError(f"Could not find widget: {ret.metador_widget}")
            if not widget_class.supports(*cand_schemas):
                msg = f"Desired widget {ret.metador_widget} does not "
                msg += f"support {container_schema_ref}"
                raise ValueError(msg)
        else:
            # get candidate widgets in alphabetic order (all that claim to work with schema)
            cand_widgets = sorted(
                list(chain(*(iter(widgets.widgets_for(sref)) for sref in cand_schemas)))
            )

            # filter out the ones that ACTUALLY can handle the object and are eligible
            is_suitable = partial(widget_suitable_for, node.meta[ret.metador_schema])
            ret.metador_widget = next(filter(is_suitable, cand_widgets), None)

        if ret.metador_widget is None:
            msg = f"Could not find suitable widget for {ret.metador_schema} at node {node.name}"
            raise ValueError(msg)

        return ret

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
                w_cls = widgets[wmeta.metador_widget]
                label = pn.pane.Str(f"{node.name}:")
                w_obj = w_cls(
                    node,
                    wmeta.metador_schema,
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
