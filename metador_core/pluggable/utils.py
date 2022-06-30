import re
from typing import Optional
from .interface import PGB_GROUP_PREFIX
from ..schema.core import PluginPkgMeta, SemVerTuple, AnyHttpUrl

# from importlib_metadata import distribution
# def this_package():
#     """Return name of the current package (usage: `this_package()()`).

#     Infers it by looking up distribution of top level module."""
#     def get_package_name() -> str:
#         return distribution(globals()["__main__"].split('.')[0]).name
#     return get_package_name


def pkgmeta_from_dist(dist) -> PluginPkgMeta:
    """Extract required metadata from importlib_metadata distribution object."""
    ver = dist.version
    if not re.fullmatch("[0-9]+\\.[0-9]+\\.[0-9]+", ver):
        msg = f"Invalid version string of {dist.name}: {ver}"
        raise TypeError(msg)
    parsed_ver: SemVerTuple = tuple(map(int, ver.split(".")))  # type: ignore

    # parse entry point groups
    epgs = filter(
        lambda x: x.startswith(PGB_GROUP_PREFIX),
        dist.entry_points.groups,
    )
    eps = {
        epg.lstrip(PGB_GROUP_PREFIX): list(
            map(lambda x: x.name, dist.entry_points.select(group=epg))
        )
        for epg in epgs
    }

    def is_repo_url(kv):
        return kv[0] == "Project-URL" and kv[1].startswith("Repository,")

    repo_url: Optional[AnyHttpUrl] = None
    try:
        url = next(filter(is_repo_url, dist.metadata.items()))
        url = url[1].split()[1]  # from "Repository, http://..."
        repo_url = url  # type: ignore
    except StopIteration:
        pass
    return PluginPkgMeta(
        name=dist.name, version=parsed_ver, plugins=eps, repository_url=repo_url
    )
