"""Processing of entry points for Metador plugins."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from importlib_metadata import Distribution, entry_points

from ..schema.plugins import PluginPkgMeta, SemVerTuple
from .types import from_ep_group_name, is_metador_ep_group, to_ep_group_name

_eps = entry_points()
"""All entry points."""

pkg_meta = {}
"""Collected infos about packages that provide plugins (filled by get_group)."""


def get_group(group_name: str) -> Dict[str, Any]:
    """Get a dict of all available entrypoints for a Metador plugin group."""
    ep_grp = to_ep_group_name(group_name)
    plugins: Dict[str, Any] = {}

    for ep in _eps.select(group=ep_grp):

        if ep.name in plugins:
            # TODO: will importlib_metadata even return colliding packages?
            # should be figured out (quite important to know)
            msg = f"{group_name}: a plugin named '{ep.name}' is already registered!"
            raise TypeError(msg)

        plugins[ep.name] = ep
        if ep.dist.name not in pkg_meta:
            pkg_meta[ep.dist.name] = PluginPkgMeta.for_package(ep.dist.name)

    return plugins


# ----


@dataclass
class DistMeta:
    name: str
    version: Tuple[int, int, int]
    plugins: Dict[str, List[str]]
    repository_url: Optional[str]


def distmeta_for(dist: Distribution) -> DistMeta:
    """Extract required metadata from importlib_metadata distribution object."""
    ver = dist.version
    if not re.fullmatch("[0-9]+\\.[0-9]+\\.[0-9]+", ver):
        msg = f"Invalid version string of {dist.name}: {ver}"
        raise TypeError(msg)
    parsed_ver: SemVerTuple = tuple(map(int, ver.split(".")))  # type: ignore

    # parse entry point groups
    epgs = filter(
        is_metador_ep_group,
        dist.entry_points.groups,
    )
    eps = {
        from_ep_group_name(epg): list(
            map(lambda x: x.name, dist.entry_points.select(group=epg))
        )
        for epg in epgs
    }

    def is_repo_url(kv):
        return kv[0] == "Project-URL" and kv[1].startswith("Repository,")

    repo_url: Optional[str] = None
    try:
        url = next(filter(is_repo_url, dist.metadata.items()))
        url = url[1].split()[1]  # from "Repository, http://..."
        repo_url = url
    except StopIteration:
        pass
    return DistMeta(
        name=dist.name, version=parsed_ver, plugins=eps, repository_url=repo_url
    )
