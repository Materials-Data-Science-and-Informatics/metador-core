import pkg_resources

PACKER_GROUP = "ardiem_packer"
"""
Group in which packer plugin entry-points are registered.
"""

ardiem_packers = {
    ep.name: ep.load() for ep in pkg_resources.iter_entry_points(group=PACKER_GROUP)
}
"""
Dict mapping from registered packer names to the corresponding classes.
"""
