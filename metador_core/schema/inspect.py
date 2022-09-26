from collections import ChainMap
from dataclasses import dataclass
from io import UnsupportedOperation
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

from pydantic import BaseModel
from simple_parsing.docstring import get_attribute_docstring

from .utils import field_origins, get_annotations


class LiftedRODict(type):
    """Metaclass for classes providing dict keys as attributes.

    Mostly for aesthetic reasons and to be used for things
    where the dict is actually a fixed lookup table.

    We don't provide explicit `keys`/`values`/`items`, because
    these could be key names in the dict.

    You can use `iter` to go through the keys and use dict-like
    access, if dynamic iteration is needed.
    """

    _dict: Dict[str, Any]
    """The underlying dict."""

    _keys: Optional[List[str]] = None
    """Optionally, list of keys in desired order."""

    _repr: Union[str, Callable] = ""
    """Optional custom repr string or function."""

    def __repr__(self):
        # choose best representation based on configuration
        if self._repr:
            if isinstance(self._repr, str):
                return self._repr
            else:
                return self._repr(self)
        if self._keys:
            return repr(self._keys)
        return repr(list(self._dict.keys()))

    def __dir__(self):
        # helpful for tab completion
        return list(self._dict.keys())

    def __bool__(self):
        return bool(self._dict)

    def __contains__(self, key):
        return key in self._dict

    def __iter__(self):
        if self._keys:
            return iter(self._keys)
        return iter(self._dict)

    def __getitem__(self, key):
        return self._dict[key]

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(str(e))

    def __setattr__(self, key, value):
        # this is supposed to be read-only
        raise UnsupportedOperation


@dataclass
class FieldInspector:
    """Basic field inspector carrying type and description of a field."""

    origin: Type
    name: str
    type: str

    description: str  # declared for proper repr generation

    def _get_description(self):
        desc = get_attribute_docstring(self.origin, self.name).docstring_below
        if not desc:
            # if none set, try getting docstring from field type
            if ths := getattr(self.origin, "_typehints", None):
                th = ths[self.name]
                if isinstance(th, type):  # it's a class-like thing?
                    desc = th.__doc__

        return desc

    @property  # type: ignore
    def description(self):
        # look up on-demand and cache, could be expensive (parses source)
        if not hasattr(self, "_description"):
            self._description = self._get_description()
        return self._description

    def __init__(self, model: Type[BaseModel], name: str, hint: str):
        origin = next(field_origins(model, name))
        self.origin = origin
        self.name = name
        self.type = hint


def make_field_inspector(
    model: Type[BaseModel],
    prop_name: str,
    *,
    bound: Optional[Type[BaseModel]] = BaseModel,
    key_filter: Optional[Callable[[str], bool]],
    i_cls: Optional[Type[FieldInspector]] = FieldInspector,
):
    """Create a field inspector class for the given model (see `get_field_inspector`)."""
    # get hints corresponding to fields that are not inherited
    field_hints = {
        k: v
        for k, v in get_annotations(model).items()
        if not key_filter or key_filter(k)
    }

    # inspectors for fields declared in the given model (for inherited, will reuse/create parent inspectors)
    new_inspectors = {k: i_cls(model, k, v) for k, v in field_hints.items()}
    # manually compute desired traversal order (from newest overwritten to oldest inherited fields)
    # as the default chain map order semantically is not suitable.
    inspectors = [new_inspectors] + [
        getattr(b, prop_name) for b in model.__bases__ if issubclass(b, bound)
    ]
    covered_keys: Set[str] = set()
    ordered_keys: List[str] = []
    for d in inspectors:
        rem_keys = set(iter(d)) - covered_keys
        covered_keys.update(rem_keys)
        ordered_keys += [k for k in d if k in rem_keys]

    # construct and return the class
    return LiftedRODict(
        f"{model.__name__}.{prop_name}",
        (),
        dict(
            _keys=ordered_keys,
            _dict=ChainMap(*inspectors),
            _repr=lambda self: "\n".join(map(str, (self[k] for k in self))),
        ),
    )


def get_field_inspector(
    model: Type[BaseModel],
    prop_name: str,
    attr_name: str,
    *,
    bound: Optional[Type[BaseModel]] = BaseModel,
    key_filter: Optional[Callable[[str], bool]],
    i_cls: Optional[Type[FieldInspector]] = FieldInspector,
):
    """Return a field inspector for the given model.

    This can be used for introspection about fields and also
    enables users to access subschemas without extra imports,
    improving decoupling of plugins and packages.

    To be used in a metaclass for a custom top level model.

    Args:
        model: Class for which to return the inspector
        prop_name: Name of the metaclass property that wraps this function
        attr_name: Name of the class attribute that caches the returned class
        i_cls: Optional subclass of FieldInspector to customize it
        bound: Top level class using the custom metaclass that uses this function
        key_filter: Predicate used to filter the annotations that are to be inspectable
    Returns:
        If an inspector was already created, will return it.
        Otherwise will create a fresh inspector class first and cache it.
    """
    if inspector := model.__dict__.get(attr_name):
        return inspector
    setattr(
        model,
        attr_name,
        make_field_inspector(
            model, prop_name, key_filter=key_filter, bound=bound, i_cls=i_cls
        ),
    )
    return model.__dict__.get(attr_name)
