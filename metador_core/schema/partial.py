"""Partial pydantic models.

Partial models replicate another model with all fields made optional.
However, they cannot be part of the inheritance chain of the original
models, because semantically it does not make sense or leads to
a diamond problem.

Therefore they are independent models, unrelated to the original
models, but convertable through methods.

Partial models are implemented as a mixin class to be combined
with the top level base model that you are using in your model
hierarchy, e.g. if you use plain `BaseModel`, you can e.g. define:

```
class MyPartial(DeepPartialModel, BaseModel): ...
```

And use `MyPartial.get_partial` on your models.

If different compatible model instances are merged,
the merge will produce an instance of the left type.

Some theory - partial schemas form a monoid with:
* the empty partial schema as neutral element
* merge of the fields as the binary operation
* associativity follows from associativity of used merge operations
"""

from __future__ import annotations

from functools import reduce
from typing import (
    Any,
    ClassVar,
    Dict,
    ForwardRef,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from pydantic import BaseModel, ValidationError, create_model, validate_model
from pydantic.fields import FieldInfo
from typing_extensions import Annotated

from ..util import is_public_name
from ..util import typing as t


def _is_list_or_set(hint):
    return t.is_list(hint) or t.is_set(hint)


def _check_type_mergeable(hint, *, allow_none: bool = False) -> bool:
    """Check whether a type is mergeable.

    An atomic type is:
    * not None
    * not a List, Set, Union or Optional
    * not a model

    (i.e. usually a primitive value e.g. int/bool/etc.)

    A singular type is:
    * an atomic type, or
    * a model, or
    * a Union of multiple singular types

    A complex type is:
    * a singular type
    * it is a List/Set of a singular type

    A mergeable type is:
    * a complex type, or
    * an Optional complex type

    merge(x,y):
        merge(None, None) = None
        merge(None, x) = merge(x, None) = x
        merge(x: model1, y: model2)
        | model1 < model2 or model2 < model1 = recursive_merge(x, y)
        | otherwise = y
        merge(x: singular, y: singular) = y
        merge(x: list, y: list) = x ++ y
        merge(x: set, y: set) =  x.union(y)
    """
    # print("check ", hint)
    args = t.get_args(hint)

    if _is_list_or_set(hint):  # list or set -> dig deeper
        return all(map(_check_type_mergeable, args))

    # not union, list or set?
    if not t.is_union(hint):
        # will be either primitive or recursively merged -> ok
        return True

    # Union case:
    if not allow_none and t.is_optional(hint):
        return False  # allows none, but should not!

    # If a Union contains a set or list, it must be only combined with None
    is_prim_union = not any(map(_is_list_or_set, args))
    is_opt_set_or_list = len(args) == 2 and t.NoneType in args
    if not (is_prim_union or is_opt_set_or_list):
        return False

    # everything looks fine on this level -> recurse down
    return all(map(_check_type_mergeable, args))


def is_mergeable_type(hint) -> bool:
    """Return whether given type can be deeply merged.

    This imposes some constraints on the shape of valid hints.
    """
    return _check_type_mergeable(hint, allow_none=True)


def val_from_partial(val):
    """Recursively convert back from a partial model if val is one."""
    if isinstance(val, PartialModel):
        return val.from_partial()
    if isinstance(val, list):
        return [val_from_partial(x) for x in val]
    if isinstance(val, set):
        return {val_from_partial(x) for x in val}
    return val


class PartialModel:
    """Base partial metadata model mixin.

    In this variant merging is done by simply overwriting old field values
    with new values (if the new value is not `None`) in a shallow way.

    For more fancy merging, consider `DeepPartialModel`.
    """

    __partial_src__: ClassVar[Type[BaseModel]]
    """Original model class this partial class is based on."""

    __partial_fac__: ClassVar[Type[PartialFactory]]
    """Factory class that created this partial."""

    def from_partial(self):
        """Return a new non-partial model instance (will run full validation).

        Raises ValidationError on failure (e.g. if the partial is missing fields).
        """
        fields = {
            k: val_from_partial(v)
            for k, v in self.__partial_fac__._get_field_vals(self)
        }
        return self.__partial_src__.parse_obj(fields)

    @classmethod
    def to_partial(cls, obj, *, ignore_invalid: bool = False):
        """Transform `obj` into a new instance of this partial model.

        If passed object is instance of (a subclass of) this or the original model,
        no validation is performed.

        Returns partial instance with the successfully parsed fields.

        Raises ValidationError if parsing fails.
        (usless ignore_invalid is set by default or passed).
        """
        if isinstance(obj, (cls, cls.__partial_src__)):
            # safe, because subclasses are "stricter"
            return cls.construct(**obj.__dict__)  # type: ignore

        if ignore_invalid:
            # validate data and keep only valid fields
            data, fields, _ = validate_model(cls, obj)  # type: ignore
            return cls.construct(_fields_set=fields, **data)  # type: ignore

        # parse a dict or another pydantic model
        if isinstance(obj, BaseModel):
            obj = obj.dict(exclude_none=True)  # type: ignore
        return cls.parse_obj(obj)  # type: ignore

    @classmethod
    def cast(
        cls,
        obj: Union[BaseModel, PartialModel],
        *,
        ignore_invalid: bool = False,
    ):
        """Cast given object into this partial model if needed.

        If it already is an instance, will do nothing.
        Otherwise, will call `to_partial`.
        """
        if isinstance(obj, cls):
            return obj
        return cls.to_partial(obj, ignore_invalid=ignore_invalid)

    def _update_field(
        cls,
        v_old,
        v_new,
        *,
        path: List[str] = [],
        allow_overwrite: bool = False,
    ):
        """Return merged result of the two passed arguments.

        None is always overwritten by a non-None value,
        lists are concatenated, sets are unionized,
        partial models are recursively merged,
        otherwise the new value overwrites the old one.
        """
        # None -> missing value -> just use new value (shortcut)
        if v_old is None or v_new is None:
            return v_new or v_old

        # list -> new one must also be a list -> concatenate
        if isinstance(v_old, list):
            return v_old + v_new

        # set -> new one must also be a set -> set union
        if isinstance(v_old, set):
            # NOTE: we could try being smarter for sets of partial models
            # https://github.com/Materials-Data-Science-and-Informatics/metador-core/issues/20
            return v_old.union(v_new)  # set union

        # another model -> recursive merge of partials, if compatible
        old_is_model = isinstance(v_old, cls.__partial_fac__.base_model)
        new_is_model = isinstance(v_new, cls.__partial_fac__.base_model)
        if old_is_model and new_is_model:
            v_old_p = cls.__partial_fac__.get_partial(type(v_old)).cast(v_old)
            v_new_p = cls.__partial_fac__.get_partial(type(v_new)).cast(v_new)
            new_subclass_old = issubclass(type(v_new_p), type(v_old_p))
            old_subclass_new = issubclass(type(v_old_p), type(v_new_p))
            if new_subclass_old or old_subclass_new:
                try:
                    return v_old_p.merge_with(
                        v_new_p, allow_overwrite=allow_overwrite, _path=path
                    )
                except ValidationError:
                    # casting failed -> proceed to next merge variant
                    # TODO: maybe raise unless "ignore invalid"?
                    pass

        # if we're here, treat it as an opaque value
        if not allow_overwrite:
            msg_title = (
                f"Can't overwrite (allow_overwrite=False) at {' -> '.join(path)}:"
            )
            msg = f"{msg_title}\n\t{repr(v_old)}\n\twith\n\t{repr(v_new)}"
            raise ValueError(msg)
        return v_new

    def merge_with(
        self,
        obj,
        *,
        ignore_invalid: bool = False,
        allow_overwrite: bool = False,
        _path: List[str] = [],
    ):
        """Return a new partial model with updated fields (without validation).

        Raises `ValidationError` if passed `obj` is not suitable,
        unless `ignore_invalid` is set to `True`.

        Raises `ValueError` if `allow_overwrite=False` and a value would be overwritten.
        """
        obj = self.cast(obj, ignore_invalid=ignore_invalid)  # raises on failure

        ret = self.copy()  # type: ignore
        for f_name, v_new in self.__partial_fac__._get_field_vals(obj):
            v_old = ret.__dict__.get(f_name)
            v_merged = self._update_field(
                v_old, v_new, path=_path + [f_name], allow_overwrite=allow_overwrite
            )
            ret.__dict__[f_name] = v_merged
        return ret

    @classmethod
    def merge(cls, *objs: PartialModel, **kwargs) -> PartialModel:
        """Merge all passed partial models in given order using `merge_with`."""
        # sadly it looks like *args and named kwargs (,*,) syntax cannot be mixed
        ignore_invalid = kwargs.get("ignore_invalid", False)
        allow_overwrite = kwargs.get("allow_overwrite", False)

        if not objs:
            return cls()

        def merge_two(x, y):
            return cls.cast(x).merge_with(
                y, ignore_invalid=ignore_invalid, allow_overwrite=allow_overwrite
            )

        return cls.cast(reduce(merge_two, objs))


# ----
# Partial factory
#
# PartialModel-specific lookup dicts
_partials: Dict[Type[PartialFactory], Dict[Type[BaseModel], Type[PartialModel]]] = {}
_forwardrefs: Dict[Type[PartialFactory], Dict[str, Type[PartialModel]]] = {}


class PartialFactory:
    """Factory class to create and manage partial models."""

    base_model: Type[BaseModel] = BaseModel
    partial_mixin: Type[PartialModel] = PartialModel

    # TODO: how to configure the whole partial family
    # with default parameters for merge() efficiently?
    # ----
    # default arguments for merge (if not explicitly passed)
    # allow_overwrite: bool = False
    # """Default argument for merge() of partials."""

    # ignore_invalid: bool = False
    # """Default argument for merge() of partials."""

    @classmethod
    def _is_base_subclass(cls, obj: Any) -> bool:
        if not isinstance(obj, type):
            return False  # not a class (probably just a hint)
        if not issubclass(obj, cls.base_model):
            return False  # not a suitable model
        return True

    @classmethod
    def _partial_name(cls, mcls: Type[BaseModel]) -> str:
        """Return class name for partial of model `mcls`."""
        return f"{mcls.__qualname__}.{cls.partial_mixin.__name__}"

    @classmethod
    def _partial_forwardref_name(cls, mcls: Type[BaseModel]) -> str:
        """Return ForwardRef string for partial of model `mcls`."""
        return f"__{mcls.__module__}_{cls._partial_name(mcls)}".replace(".", "_")

    @classmethod
    def _get_field_vals(cls, obj: BaseModel) -> Iterator[Tuple[str, Any]]:
        """Return field values, excluding None and private fields.

        This is different from `BaseModel.dict` as it ignores the defined alias
        and is used here only for "internal representation".
        """
        return (
            (k, v)
            for k, v in obj.__dict__.items()
            if is_public_name(k) and v is not None
        )

    @classmethod
    def _nested_models(cls, field_types: Dict[str, t.TypeHint]) -> Set[Type[BaseModel]]:
        """Collect all compatible nested model classes (for which we need partials)."""
        return {
            cast(Type[BaseModel], h)
            for th in field_types.values()
            for h in t.traverse_typehint(th)
            if cls._is_base_subclass(h)
        }

    @classmethod
    def _model_to_partial_fref(cls, orig_type: t.TypeHint) -> t.TypeHint:
        """Substitute type hint with forward reference to partial models.

        Will return unchanged type if passed argument is not suitable.
        """
        if not cls._is_base_subclass(orig_type):
            return orig_type
        return ForwardRef(cls._partial_forwardref_name(orig_type))

    @classmethod
    def _model_to_partial(
        cls, mcls: Type[BaseModel]
    ) -> Type[Union[BaseModel, PartialModel]]:
        """Substitute a model class with partial model.

        Will return unchanged type if argument not suitable.
        """
        return cls.get_partial(mcls) if cls._is_base_subclass(mcls) else mcls

    @classmethod
    def _partial_type(cls, orig_type: t.TypeHint) -> t.TypeHint:
        """Convert a field type hint into a type hint for the partial.

        This will make the field optional and also replace all nested models
        derived from the configured base_model with the respective partial model.
        """
        return Optional[t.map_typehint(orig_type, cls._model_to_partial_fref)]

    @classmethod
    def _partial_field(cls, orig_type: t.TypeHint) -> Tuple[Type, Optional[FieldInfo]]:
        """Return a field declaration tuple for dynamic model creation."""
        th, fi = orig_type, None

        # if pydantic Field is added (in an Annotated[...]) - unwrap
        if t.get_origin(orig_type) is Annotated:
            args = t.get_args(orig_type)
            th = args[0]
            fi = next(filter(lambda ann: isinstance(ann, FieldInfo), args[1:]), None)

        pth = cls._partial_type(th)  # map the (unwrapped) type to optional
        return (pth, fi)

    @classmethod
    def _create_base_partial(cls):
        class PartialBaseModel(cls.partial_mixin, cls.base_model):
            class Config:
                frozen = True  # make sure it's hashable

        return PartialBaseModel

    @classmethod
    def _create_partial(cls, mcls: Type[BaseModel], *, typehints=None):
        """Create a new partial model class based on `mcls`."""
        if not cls._is_base_subclass(mcls):
            raise TypeError(f"{mcls} is not subclass of {cls.base_model.__name__}!")
        if mcls is cls.base_model:
            return (cls._create_base_partial(), [])
        # ----
        # get field type annotations (or use the passed ones / for performance)
        hints = typehints or t.get_type_hints(mcls)
        field_types = {k: v for k, v in hints.items() if k in mcls.__fields__}
        # get dependencies that must be substituted
        missing_partials = cls._nested_models(field_types)
        # compute new field types
        new_fields = {k: cls._partial_field(v) for k, v in field_types.items()}
        # replace base classes with corresponding partial bases
        new_bases = tuple(map(cls._model_to_partial, mcls.__bases__))
        # create partial model
        ret: Type[PartialModel] = create_model(
            cls._partial_name(mcls),
            __base__=new_bases,
            __module__=mcls.__module__,
            __validators__=mcls.__validators__,  # type: ignore
            **new_fields,
        )
        ret.__partial_src__ = mcls  # connect to original model
        ret.__partial_fac__ = cls  # connect to this class
        # ----
        return ret, missing_partials

    @classmethod
    def get_partial(cls, mcls: Type[BaseModel], *, typehints=None):
        """Return a partial schema with all fields of the given schema optional.

        Original default values are not respected and are set to `None`.

        The use of the returned class is for validating partial data before
        zipping together partial results into a completed one.

        This is a much more fancy version of e.g.
        https://github.com/pydantic/pydantic/issues/1799

        because it recursively substitutes with partial models.
        This allows us to implement smart deep merge for partials.
        """
        if cls not in _partials:  # first use of this partial factory
            _partials[cls] = {}
            _forwardrefs[cls] = {}

        if partial := _partials[cls].get(mcls):
            return partial  # already have a partial
        else:  # block the spot (to break recursion)
            _partials[cls][mcls] = None

        # ----
        # create a partial for a model:
        mcls.update_forward_refs()  # to be sure
        partial, nested = cls._create_partial(mcls, typehints=typehints)
        partial_ref = cls._partial_forwardref_name(mcls)
        # store result
        _forwardrefs[cls][partial_ref] = partial
        _partials[cls][mcls] = partial
        # create partials for nested models
        for model in nested:
            cls.get_partial(model)
        # resolve possible circular references
        partial.update_forward_refs(**_forwardrefs[cls])  # type: ignore
        # ----
        return partial
