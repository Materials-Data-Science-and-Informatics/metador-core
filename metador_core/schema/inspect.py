from collections import ChainMap
from dataclasses import dataclass
from io import UnsupportedOperation
from typing import Any, Dict, List, Mapping, Optional, Set, Type

from simple_parsing.docstring import get_attribute_docstring

from .core import MetadataSchema
from .utils import field_model_types, field_origins


class LiftedRODict(type):
    """Metaclass for classes providing dict keys as attributes.

    Mostly for aesthetic reasons and to be used for things
    where the dict is actually a fixed lookup table.
    """

    _keys: Optional[List[str]] = None
    _dict: Dict[str, Any]
    _repr: str = ""

    def __repr__(self):
        if self._repr:
            if isinstance(self._repr, str):
                return self._repr
            else:
                return self._repr(self)

        elif self._keys:
            return repr(self._keys)
        return repr(list(self._dict.keys()))

    def __dir__(self):
        return list(self._dict.keys())

    def __setattr__(self, key, value):
        raise UnsupportedOperation

    def __getattr__(self, key):
        if s := self._dict.get(key):
            return s
        raise AttributeError(key)

    def __getitem__(self, key):
        return self._dict[key]

    def keys(self):
        if self._keys:
            return list(self._keys)
        return self._dict.keys()

    def values(self):
        if self._keys:
            return (self._dict[k] for k in self.keys())
        return self._dict.values()

    def items(self):
        if self._keys:
            return ((k, self._dict[k]) for k in self.keys())
        return self._dict.items()

    def __iter__(self):
        if self._keys:
            return iter(self._keys)
        return iter(self._dict)

    def __bool__(self):
        return bool(self._dict)


def _get_docs(cls, name) -> str:
    docs = get_attribute_docstring(cls, name)
    return docs.docstring_below


def get_annotations(cls, *, all: bool = False) -> Mapping[str, Any]:
    """Return (non-inherited) annotations (unparsed) of given class."""
    if not all:
        return cls.__dict__.get("__annotations__", {})
    return ChainMap(*(c.__dict__.get("__annotations__", {}) for c in cls.__mro__))


def indent_text(txt, prefix="\t"):
    return "\n".join(map(lambda l: f"{prefix}{l}", txt.split("\n")))


@dataclass
class FieldInspector:
    origin: Type

    name: str
    description: str
    type: str

    schemas: Type

    def __init__(self, model, name, hint, subschemas):
        origin = next(field_origins(model, name))

        self.origin = origin
        self._origin_name = f"{origin.__module__}.{origin.__qualname__}"
        if origin.is_plugin:
            self._origin_name += f" (plugin: {origin.Plugin.name} {'.'.join(map(str, origin.Plugin.version))})"

        self.name = name
        self.type = hint
        self.description = _get_docs(self.origin, self.name)

        class Schemas(metaclass=LiftedRODict):
            _dict = {s.__name__: s for s in subschemas}

        self.schemas = Schemas

    def __repr__(self):
        desc_str = (
            f"description:\n{indent_text(self.description)}\n"
            if self.description
            else ""
        )
        schemas_str = f"schemas: {', '.join(self.schemas)}\n" if self.schemas else ""
        info = f"type: {str(self.type)}\norigin: {self._origin_name}\n{schemas_str}{desc_str}"
        return f"{self.name}\n{indent_text(info)}"


def add_field_inspector(model: MetadataSchema):
    """Attach inner class to a model for sub-model lookup.

    This enables users to access subschemas without extra imports,
    improving decoupling of plugins and packages.

    Also can be used for introspection about fields.
    """
    # get hints corresponding to fields that are not inherited
    anns = get_annotations(model)
    field_hints = {
        k: v
        for k, v in get_annotations(model, all=True).items()
        if k in model.__fields__ and k in anns and k not in model.__constants__
    }
    field_schemas = {
        k: set(field_model_types(model.__fields__[k], bound=MetadataSchema))
        for k in field_hints.keys()
    }
    new_inspectors = {
        k: FieldInspector(model, k, v, field_schemas[k]) for k, v in field_hints.items()
    }

    # make sure base classes have inspectors
    inspector_chain = [new_inspectors]
    for b in model.__bases__:
        if issubclass(b, MetadataSchema):
            add_field_inspector(b)
            inspector_chain.append(b.Fields)

    # compute traversal order (from newest overwritten to oldest inherited)
    covered_keys: Set[str] = set()
    ordered_keys: List[str] = []
    for d in inspector_chain:
        rem_keys = set(d.keys()) - covered_keys
        covered_keys.update(rem_keys)
        ordered_keys += [k for k in d.keys() if k in rem_keys]

    # construct a class
    class FieldInspectors(metaclass=LiftedRODict):
        _keys = ordered_keys
        _dict = ChainMap(*inspector_chain)

        def _repr(self):
            return "\n".join(map(str, self.values()))

    # attach
    setattr(model, "Fields", FieldInspectors)
