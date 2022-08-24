import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..schema.core import SemVerTuple
from .entrypoints import PG_PREFIX


@dataclass
class DistMeta:
    name: str
    version: Tuple[int, int, int]
    plugins: Dict[str, List[str]]
    repository_url: Optional[str]


def distmeta_for(dist) -> DistMeta:
    """Extract required metadata from importlib_metadata distribution object."""
    ver = dist.version
    if not re.fullmatch("[0-9]+\\.[0-9]+\\.[0-9]+", ver):
        msg = f"Invalid version string of {dist.name}: {ver}"
        raise TypeError(msg)
    parsed_ver: SemVerTuple = tuple(map(int, ver.split(".")))  # type: ignore

    # parse entry point groups
    epgs = filter(
        lambda x: x.startswith(PG_PREFIX),
        dist.entry_points.groups,
    )
    eps = {
        epg.lstrip(PG_PREFIX): list(
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
