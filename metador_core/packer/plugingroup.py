"""Define packers as pluggable entity."""

from ..plugins.interface import PluginGroup
from .interface import Packer, PackerInfo


class PGPacker(PluginGroup[Packer]):
    def packer_info(self, packer_name):
        """Return a PackerInfo object for given packer name."""
        pkg = self.provider(packer_name)
        return PackerInfo(
            python_pkg=pkg.name,
            python_pkg_version=pkg.version,
            plugin_group=self.name,
            plugin_name=packer_name,
        )

    @classmethod
    def check_plugin(cls, ep_name, ep):
        if not issubclass(ep, Packer):
            msg = f"Registered packer not subclass of Packer: '{ep_name}'"
            raise TypeError(msg)
