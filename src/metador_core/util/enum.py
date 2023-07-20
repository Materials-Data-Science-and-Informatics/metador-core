from __future__ import annotations

import itertools
from enum import Enum, EnumMeta
from typing import Any, Dict, Optional, Set, Union


class NestedEnumMeta(EnumMeta):
    """Enum subclass for Enums generated from a semantic taxonomy.

    Approach is to collect values and treat them as sets.
    """

    __self__: Optional[Union[int, str]] = None
    __values__: Set[Any] = set()
    __nested__: Dict[str, NestedEnumMeta] = {}

    name: str
    value: Union[int, str]

    def __call__(metacls, clsname, dct, *args, **kwargs):
        self = kwargs.pop("self", None)
        mod_dct = {}
        nested = {}
        for k, v in dct.items():
            if isinstance(v, NestedEnumMeta):
                if v.__self__ is None:
                    raise ValueError(f"Nested enum {v} must have a self value!")
                nested[k] = v
            else:
                mod_dct[k] = v

        ret = super().__call__(clsname, mod_dct, *args, **kwargs)

        ret.__self__ = self
        nested_values = set.union(
            set(), *map(lambda x: {x.__self__}.union(x.__values__), nested.values())
        )
        ret.__values__ = set(map(lambda x: x.value, iter(ret))).union(nested_values)
        ret.__nested__ = nested
        for name, nst in nested.items():
            setattr(ret, name, nst)

        return ret

    def __contains__(self, other):
        print(other, "in", self, "?")
        if isinstance(other, NestedEnum):
            return self.__contains__(other.value)
        if isinstance(other, type) and issubclass(other.__class__, NestedEnumMeta):
            return self.__contains__(other.__self__)
        # lookup plain value
        return other in self.__values__

    def __iter__(self):
        return itertools.chain(
            super().__iter__(),
            *(itertools.chain(iter((x,)), iter(x)) for x in self.__nested__.values()),
        )

    def __dir__(self):
        return itertools.chain(super().__dir__(), self.__nested__.keys())

    def __repr__(self):
        if self.__self__ is not None:
            return f"<enum {self.__name__}: {self.__self__}>"
        else:
            return super().__repr__()


class NestedEnum(Enum, metaclass=NestedEnumMeta):
    """Base class for hierarchical enumerations."""
