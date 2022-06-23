"""Define schemas as pluggable entity."""

from ..pluggable.interface import Pluggable
from .interface import MetadataSchema


class PluggableSchema(Pluggable):
    @classmethod
    def check_plugin(cls, ep_name, ep):
        if not issubclass(ep, MetadataSchema):
            msg = f"Registered schema not subclass of MetadataSchema: '{ep_name}'"
            raise TypeError(msg)
