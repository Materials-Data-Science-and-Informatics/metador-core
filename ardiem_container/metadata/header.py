"""Metadata models for entries in the header or Ardiem records."""

from __future__ import annotations

import os
from typing import List, Optional, Tuple

from pydantic import AnyHttpUrl, NonNegativeInt

from .base import ArdiemBaseModel
from .types import nonempty_str


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
