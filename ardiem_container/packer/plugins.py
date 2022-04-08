import entrypoints

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
