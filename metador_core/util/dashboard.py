import panel as pn

from ..plugins import widgets


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


def add_widgets(w_grp, ui_grp, server=None):
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


def get_grp_row(idx=None, widget_group=None, server=None, divider=False):
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
        ),
        pn.layout.Divider(margin=(100, 0, 20, 0)) if divider == True else None,
        flex_direction="column",
        justify_content="space-evenly",
        align_content="space-evenly",
        align_items="center",
        sizing_mode="scale_both",
    )
