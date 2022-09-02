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

And use `MyPartial._create_partial` on your models.
"""

from functools import reduce
from typing import ClassVar, ForwardRef, List, Optional, Type

from overrides import overrides
from pydantic import BaseModel, ValidationError, create_model, validate_model
from pydantic.fields import FieldInfo
from typing_extensions import Annotated

from . import utils


class PartialModel:
    """Base partial metadata model mixin.

    In this variant merging is done by simply overwriting old field values
    with new values (if the new value is not `None`) in a shallow way.

    For more fancy merging, consider `DeepPartialModel`.
    """

    _partial_of: ClassVar[Type[BaseModel]]  # original model class, auto-generated

    # default arguments for merge
    _def_allow_overwrite: bool = False
    _def_ignore_invalid: bool = False

    @classmethod
    def _partial_name(cls, mcls):
        """Return class name for partial of model `mcls`."""
        return f"{mcls.__qualname__}.{cls.__name__}"

    @classmethod
    def _partial_forwardref_name(cls, mcls):
        """Return ForwardRef string for partial of model `mcls`."""
        return f"__{mcls.__module__}_{cls._partial_name(mcls)}".replace(".", "_")

    @classmethod
    def _get_fields(cls, obj):
        """Return dict with fields excluding None and private fields.

        This is different from `BaseModel.dict` as it ignores the defined alias
        and is used here only for "internal representation".
        """
        return (
            (k, v) for k, v in obj.__dict__.items() if k[0] != "_" and v is not None
        )

    # ----

    def from_partial(self):
        """Return a fresh non-partial model (will run full validation)."""
        return self._partial_of.parse_obj(dict(self._get_fields(self)))

    @classmethod
    def to_partial(cls, obj, *, ignore_invalid: bool = _def_ignore_invalid):
        """Transform `obj` into a new instance of this partial model.

        If passed object is instance of (a subclass of) this or the original model,
        no validation is performed. Otherwise, will try to parse the object.
        """
        if isinstance(obj, (cls, cls._partial_of)):
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
    def cast(cls, obj, *, ignore_invalid: bool = _def_ignore_invalid):
        """Cast given object into this partial model if needed.

        If it already is an instance, will do nothing.
        Otherwise, will call `to_partial`.
        """
        if isinstance(obj, cls):
            return obj
        return cls.to_partial(obj, ignore_invalid=ignore_invalid)

    @classmethod
    def _update_field(
        cls,
        v_old,
        v_new,
        *,
        path: List[str] = [],
        allow_overwrite: bool = _def_allow_overwrite,
    ):
        """Return updated field value based on old and new field value.

        Will prefer anything over `None` and newer over older values.
        """
        if not allow_overwrite and v_new and v_old:
            msg_title = (
                f"Cannot overwrite (allow_overwrite=False) at {' -> '.join(path)}:"
            )
            msg = f"{msg_title}\n\t{repr(v_old)}\n\twith\n\t{repr(v_new)}"
            raise ValueError(msg)
        return v_new or v_old

    def merge_with(
        self,
        obj,
        *,
        ignore_invalid: bool = _def_ignore_invalid,
        allow_overwrite: bool = _def_allow_overwrite,
        _path: List[str] = [],
    ):
        """Return a new partial model with updated fields (without validation).

        Raises `ValidationError` if passed `obj` is not suitable,
        unless `ignore_invalid` is set to `True`.

        Raises `ValueError` if `allow_overwrite=False` and a value would be overwritten.
        """
        obj = self.cast(obj, ignore_invalid=ignore_invalid)  # raises on failure

        ret = self.copy()  # type: ignore
        for f_name, v_new in self._get_fields(obj):
            v_old = ret.__dict__.get(f_name)
            v_merged = self._update_field(
                v_old, v_new, path=_path + [f_name], allow_overwrite=allow_overwrite
            )
            setattr(
                ret, f_name, v_merged
            )  # validates if used BaseModel configured for it
        return ret

    @classmethod
    def merge(cls, *objs, **kwargs):
        """Merge all passed partial models in given order using `merge_with`."""
        ignore_invalid = kwargs.get("ignore_invalid", cls._def_ignore_invalid)
        allow_overwrite = kwargs.get("allow_overwrite", cls._def_allow_overwrite)

        def merge_two(x, y):
            return x.merge_with(
                y, ignore_invalid=ignore_invalid, allow_overwrite=allow_overwrite
            )

        return reduce(merge_two, objs)

    # ----

    @classmethod
    def _partial_type(cls, orig_type):
        return Optional[orig_type]

    @classmethod
    def _partial_field(cls, orig_field):
        th, fi = orig_field, None
        # if pydantic Field is added (in an Annotated[...]) - unwrap
        if utils.get_origin(orig_field) is Annotated:
            args = utils.get_args(orig_field)
            if not isinstance(args[1], FieldInfo):
                raise RuntimeError(f"Unexpected annotation: {args}")
            th, fi = args[0], args[1]

        # map to partial type
        return (cls._partial_type(th), fi)

    @classmethod
    def _create_partial(cls, mcls, *, partials={}):
        """Return a new schema with all fields of the given schema optional.

        Original default values are not respected and are set to `None`.

        The use of the returned class is for validating partial data before
        zipping together partial results into a completed one.

        This is a much more fancy version of e.g.
        https://github.com/pydantic/pydantic/issues/1799

        because it recursively substitutes with partial models.
        This allows us to implement smart deep merge for partials.
        """
        if not issubclass(mcls, cls.__base__):
            raise ValueError(f"{mcls} is not a {cls.__base__.__name__}!")
        for b in mcls.__bases__:
            if not issubclass(b, cls.__base__) or b is cls.__base__:
                continue
            if b not in partials:
                raise ValueError(f"No partial provided for {mcls} base {b}!")

        # get all annotations (we define fields only using them)
        field_types = utils.get_type_hints(mcls, include_inherited=True)
        fields = {
            k: cls._partial_field(v)
            for k, v in field_types.items()
            if k[0] != "_" and utils.get_origin(v) is not ClassVar
        }

        # replace base classes with corresponding partial bases
        def partial_base(b_cls):
            if b_cls is cls.__base__:
                return cls  # top base
            if not issubclass(b_cls, cls.__base__):
                return b_cls  # not child of top base
            return partials[b_cls]  # replace with existing partials

        # create partial model
        ret: Type[PartialModel] = create_model(
            cls._partial_name(mcls),
            __base__=tuple(map(partial_base, mcls.__bases__)),
            __module__=mcls.__module__,
            __validators__=mcls.__validators__,  # type: ignore
            **fields,
        )
        ret._partial_of = mcls  # connect to original model

        return ret


# ----
# Deep merging model


def is_list_or_set(hint):
    return utils.is_list(hint) or utils.is_set(hint)


def check_type_mergeable(hint, *, allow_none: bool = False):
    args = utils.get_args(hint)

    if is_list_or_set(hint):  # list or set -> dig deeper
        return all(map(check_type_mergeable, args))

    # not union, list or set?
    if not utils.is_union(hint):
        # will be either primitive or recursively merged -> ok
        return True

    # Union case:
    if not allow_none and utils.is_optional(hint):
        return False  # allows none, but should not!

    # If a Union contains a set or list, it must be only combined with None
    is_prim_union = not any(map(is_list_or_set, args))
    is_opt_set_or_list = len(args) == 2 and utils.NoneType in args
    if not (is_prim_union or is_opt_set_or_list):
        return False

    # everything looks fine on this level -> recurse down
    return all(map(check_type_mergeable, args))


class DeepPartialModel(PartialModel):
    """Recursive partial model with smart updates."""

    _def_allow_overwrite = PartialModel._def_allow_overwrite

    @classmethod
    def _is_mergeable_type(cls, hint) -> bool:
        """Return whether given type can be deeply merged.

        This imposes some constraints on the shape of valid hints.
        """
        return check_type_mergeable(hint, allow_none=True)

    # make partial transformation recursive for the smart update to be useful:

    @classmethod
    def _substitute_partial(cls, orig_type):
        if not isinstance(orig_type, type):
            return orig_type  # not a class (probably just a hint)
        if not issubclass(orig_type, cls.__base__):
            return orig_type  # not a suitable model
        return ForwardRef(cls._partial_forwardref_name(orig_type))

    @classmethod
    @overrides
    def _partial_type(cls, orig_type):
        return Optional[utils.map_typehint(orig_type, cls._substitute_partial)]

    # smart update combining values recursively:

    @classmethod
    @overrides
    def _update_field(
        cls,
        v_old,
        v_new,
        *,
        path: List[str] = [],
        allow_overwrite: bool = _def_allow_overwrite,
    ):
        """Return merged result of the two passed arguments.

        None is always overwritten by a non-None value,
        lists are concatenated, sets are unionized,
        partial models are recursively merged,
        otherwise the new value overwrites the old one.
        """
        # None -> missing value -> just use new value (shortcut)
        if v_old is None:
            return v_new

        # list -> new one must also be a list -> concatenate
        if isinstance(v_old, list):
            return v_old + v_new

        # set -> new one must also be a set -> set union
        if isinstance(v_old, set):
            # NOTE: we could try being smarter for sets of partial models
            # https://github.com/Materials-Data-Science-and-Informatics/metador-core/issues/20
            return v_old.union(v_new)  # set union

        # another partial -> recursive merge of partials, if compatible
        if isinstance(v_old, PartialModel):
            new_subclass_old = issubclass(type(v_new), type(v_old))
            old_subclass_new = issubclass(type(v_old), type(v_new))
            if new_subclass_old or old_subclass_new:
                try:
                    return v_old.merge_with(
                        v_new, allow_overwrite=allow_overwrite, _path=path
                    )
                except ValidationError:  # casting failed -> proceed to next merge variant
                    pass

        # if we're here, treat it as an opaque value
        return super()._update_field(
            v_old, v_new, allow_overwrite=allow_overwrite, path=path
        )

    # this also requires to recursively transform back:

    @classmethod
    def _val_from_partial(cls, val):
        if isinstance(val, PartialModel):
            return val.from_partial()
        if isinstance(val, list):
            return [cls._val_from_partial(x) for x in val]
        if isinstance(val, set):
            return {cls._val_from_partial(x) for x in val}
        return val

    @overrides
    def from_partial(self):
        """Return a fresh finalized model (will run full validation).

        Will recursively transform nested partial models as needed.
        """
        fs = {k: self._val_from_partial(v) for k, v in self._get_fields(self)}
        return self._partial_of.parse_obj(fs)
