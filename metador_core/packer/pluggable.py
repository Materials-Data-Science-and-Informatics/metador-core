"""Define packers as pluggable entity."""

from ..pluggable.interface import Pluggable
from .interface import Packer, PackerInfo


class PluggablePacker(Pluggable):
    @classmethod
    def packer_info(cls, packer_name):
        """Return a PackerInfo object for given packer name."""
        pkg = cls.provider(packer_name)
        return PackerInfo(
            python_pkg=pkg.name,
            python_pkg_version=pkg.version,
            plugin_group=cls.name,
            plugin_name=packer_name,
        )

    @classmethod
    def check_plugin(cls, ep_name, ep):
        if not issubclass(ep, Packer):
            msg = f"Registered packer not subclass of Packer: '{ep_name}'"
            raise TypeError(msg)
