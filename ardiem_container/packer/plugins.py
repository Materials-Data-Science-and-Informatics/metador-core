import entrypoints

from .base import ArdiemPacker

PACKER_GROUP = "ardiem_packer"
"""
Group in which packer plugin entry-points are registered.
"""

ardiem_packers = {
    ep.name: ep.load() for ep in entrypoints.get_group_all(group=PACKER_GROUP)
}
"""
Dict mapping from registered packer names to the corresponding classes.
"""
for k, v in ardiem_packers.items():
    if not issubclass(v, ArdiemPacker):
        msg = f"Registered packer not subclass of ArdiemPacker: '{k}'"
        raise RuntimeError(msg)
