"""Collection of entry-points defining previewable metadata pydantic models."""

import sys
from typing import Union, get_type_hints

from typing_extensions import Final

if sys.version_info[:2] >= (3, 9):
    from typing import _UnionGenericAlias  # type: ignore
elif sys.version_info[:2] == (3, 8):
    from typing import _GenericAlias  # type: ignore

    _UnionGenericAlias = _GenericAlias

import entrypoints
from typing_extensions import Literal

from .base import ArdiemBaseModel, create_union_model

PREVIEWABLE_GROUP = "ardiem_previewable"

known_previewables = {
    ep.name: ep.load() for ep in entrypoints.get_group_all(group=PREVIEWABLE_GROUP)
}
"""
Dict mapping from registered previewable models to the corresponding classes.
"""

# check that the previewables are valid
for k, v in known_previewables.items():
    if not issubclass(v, ArdiemBaseModel):
        msg = f"ArdiemBaseModel must be parent class of previewable: '{k}'"
        raise RuntimeError(msg)
    ann = get_type_hints(v).get("type")
    if ann is None:
        msg = f"Missing 'type' field in previewable: '{k}'"
        raise RuntimeError(msg)
    if ann is not Literal[k]:
        msg = f"Field 'type' must be typed as Literal['{k}'] in previewable: '{k}'"
        raise RuntimeError(msg)


# dynamically initialize the union type hint to configure Pydantic.
# (disgusting hack... but currently yields best ergonomics for parsing down the line)
# problem: initialization order!!! would require this to be loaded last
NodeMetaUnion = _UnionGenericAlias(Union, tuple(known_previewables.values()))  # type: ignore

NODE_META_ATTR: Final[str] = "node_meta"
"""Name of the attribute used to store node-local (group or dataset) metadata."""


NodeMeta = create_union_model("NodeMeta", NodeMetaUnion, "type", ArdiemBaseModel)
"""Metadata attached to a node (group or HDF5 record).

Use this class for parsing an unknown node metadata entity.
It acts as a wrapper for the embedded Union of types.
"""
