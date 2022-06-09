"""Metadata models for entries in the header or Ardiem records."""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Set, Tuple

from pydantic import AnyHttpUrl, NonNegativeInt, validator
from typing_extensions import Final

from ..ih5 import IH5Record
from .base import ArdiemBaseModel
from .previewable import NODE_META_ATTR, NodeMeta, known_previewables
from .types import nonempty_str

_previewable_types: Set[str] = set(known_previewables.keys())
"""Known previewable types loaded from entrypoints."""


PACKER_META_PATH: Final[str] = "/head/packer"
"""Path in container where PackerMeta is to be stored."""


class PackerMeta(ArdiemBaseModel):
    """Metadata of the packer that created some record."""

    id: nonempty_str
    """Unique identifier of the packer (the same as the entry-point name)."""

    version: Tuple[NonNegativeInt, NonNegativeInt, NonNegativeInt]
    """Version of the packer (can be different from version of the Python package)."""

    docs: Optional[AnyHttpUrl] = None
    """Documentation of the packer (possibly location within more general package docs)."""

    python_package: Optional[nonempty_str] = None
    """Python package name (i.e. what to `pip install`, if published on PyPI)."""

    python_source: Optional[AnyHttpUrl] = None
    """Python package source location (e.g. online-hosted git repository)."""

    uname: List[str] = []
    """Environment of packer during execution (os.uname() without the hostname)."""

    @staticmethod
    def get_uname() -> List[str]:
        """Get minimal information about the system to be put in the uname attribute."""
        un = os.uname()
        # remove nodename (too private)
        return [un.sysname, un.release, un.version, un.machine]


TOC_META_PATH: Final[str] = "/head/toc"


class TOCMeta(ArdiemBaseModel):
    previewable: Dict[str, List[str]] = {}

    @validator("previewable")
    def valid_previewable_dict(cls, v):
        unknown = set(v.keys()) - _previewable_types
        if unknown:
            m = f"Unknown previewables: {unknown} (known: {_previewable_types})"
            raise ValueError(m)

        return v

    @classmethod
    def for_record_body(cls, rec: IH5Record):
        if "/body" not in rec:
            return cls()

        ret = {}

        def collect(name, obj):
            if NODE_META_ATTR not in obj.attrs:
                return
            print(obj.attrs[NODE_META_ATTR])
            meta = NodeMeta.parse_raw(obj.attrs[NODE_META_ATTR])
            typ: str = meta.type  # type: ignore
            if typ not in _previewable_types:
                raise ValueError("Unknown previewable type '{meta.type}' at {name}")
            # add node to the list
            if typ not in ret:
                ret[typ] = []
            ret[typ].append(name)

        rec["/body"].visititems(collect)
        return cls(previewable=ret)
