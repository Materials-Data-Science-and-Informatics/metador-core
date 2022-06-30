"""Define schemas as pluggable entity."""

from ..pluggable.interface import Pluggable
from .interface import MetadataSchema
from typing import List


class PluggableSchema(Pluggable):
    @classmethod
    def check_plugin(cls, schema_name: str, ep):
        if not issubclass(ep, MetadataSchema):
            msg = f"Registered schema not subclass of MetadataSchema: '{schema_name}'"
            raise TypeError(msg)

    @classmethod
    def parent_path(cls, schema_name: str) -> List[str]:
        """Get sequence of registered parent schema names leading to the given schema."""
        ret = [schema_name]

        curr = cls[schema_name]  # type: ignore
        parent = curr.parent_schema()
        while parent is not None:
            ret.append(parent.name)
            curr = cls[parent.name]  # type: ignore
            parent = curr.parent_schema()

        ret.reverse()
        return ret
