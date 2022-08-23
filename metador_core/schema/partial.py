from functools import reduce
from typing import ClassVar, ForwardRef, Optional, Type

from pydantic import create_model, validate_model
from pydantic.fields import FieldInfo
from typing_extensions import Annotated

from . import utils
from .core import MetadataSchema


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


def is_mergeable_type(hint) -> bool:
    """Return whether provided type hint is valid for automatic deep merging."""
    return check_type_mergeable(hint, allow_none=True)


class PartialSchema(MetadataSchema):
    """MPartial metadata schema."""

    _partial_of: ClassVar[MetadataSchema]  # points to original model class
    _forwardref_name: ClassVar[str]  # unique name used in forwardref context

    @classmethod
    def _get_fields(cls, obj):
        """Return dict with fields that are neither private nor set to None.

        This is different from `dict()` as it ignores the defined alias.
        """
        constants = obj.__dict__.get("__constants__", set())
        return {
            k: v
            for k, v in obj.__dict__.items()
            if k[0] != "_" and k not in constants and v is not None
        }

    # ----

    @classmethod
    def to_partial(cls, obj, *, ignore_invalid: bool = False):
        """Transform object into a new partial instance.

        If passed object is instance of a (subclass of) the original schema,
        no validation is performed. Otherwise, will try to parse the object.
        """
        if isinstance(obj, cls._partial_of):
            return cls.construct(**obj.__dict__)
        if ignore_invalid:
            data, fields, errs = validate_model(cls, obj)
            return cls.construct(_fields_set=fields, **data)
        if isinstance(obj, MetadataSchema):
            obj = obj.dict(exclude_none=True)
        return cls.parse_obj(obj)

    @classmethod
    def cast(cls, obj, *, ignore_invalid: bool = False):
        """Cast given object into this partial model.

        If it already is an instance, will do nothing.
        Otherwise, will call `to_partial`.
        """
        if isinstance(obj, cls):
            return obj
        else:
            return cls.to_partial(obj, ignore_invalid=ignore_invalid)

    @classmethod
    def _val_from_partial(cls, val):
        if isinstance(val, PartialSchema):
            return val.from_partial()
        if isinstance(val, list):
            return [cls._val_from_partial(x) for x in val]
        if isinstance(val, set):
            return {cls._val_from_partial(x) for x in val}
        return val

    def from_partial(self):
        """Return a fresh finalized model (will run full validation).

        Will recursively transform nested partial models as needed.
        """
        unpartialed_fields = {
            k: self._val_from_partial(v) for k, v in self._get_fields(self).items()
        }
        return self._partial_of.parse_obj(unpartialed_fields)

    @classmethod
    def merge_field(cls, v_old, v_new):
        """Return merged result of the two passed arguments.

        None is always overwritten by a non-None value,
        lists are concatenated, sets are unionized,
        partial models are recursively merged,
        otherwise the new value overwrites the old one.
        """
        # None -> missing value -> just use new value
        if v_old is None:
            return v_new
        # list -> new one must also be a list -> concatenate
        if isinstance(v_old, list):
            return v_old + v_new
        # set -> new one must also be a set -> set union
        if isinstance(v_old, set):
            # NOTE: we could try being smarter for sets of partial models
            # e.g. two models with same @id/id_ could be merged! (TODO)
            return v_old.union(v_new)  # set union
        # another partial -> recursive merge of partial models
        if isinstance(v_old, PartialSchema) and isinstance(v_new, PartialSchema):
            return v_old.merge(v_new)
        # if we're here, treat it as an opaque value
        return v_new  # simply substitute

    def merge_with(self, obj, *, ignore_invalid: bool = False):
        """Return a new partial model with updated fields (without validation).

        Raises `ValidationError` if passed `obj` is not suitable.
        """
        obj = self.cast(obj, ignore_invalid=ignore_invalid)  # raises on failure
        ret = self.copy()
        for f_name, v_new in self._get_fields(obj).items():
            v_old = ret.__dict__.get(f_name)
            v_merged = self.merge_field(v_old, v_new)
            setattr(ret, f_name, v_merged)
        return ret

    @classmethod
    def merge(cls, *objs, **kwargs):
        """Merge all passed partial models in given order."""
        ignore_invalid = kwargs.get("ignore_invalid", False)

        def merge_partials(x, y):
            return x.merge_with(y, ignore_invalid=ignore_invalid)

        return reduce(merge_partials, objs, cls())


def partial_name(mcls):
    return f"__Partial_{mcls.__name__}"


def partial_forwardref_name(mcls):
    return f"__{mcls.__module__.replace('.', '_')}_{partial_name(mcls)}"


def substitute_partial(orig_type):
    if not isinstance(orig_type, type):
        return orig_type  # not a class
    if not issubclass(orig_type, MetadataSchema):
        return orig_type  # not a suitable model
    return ForwardRef(partial_forwardref_name(orig_type))


def partial_type(orig_type):
    # assumes this contains no "Annotated" with pydantic stuff
    # Annotated is simply "passed through" like a normal type!
    return Optional[utils.map_typehint(orig_type, substitute_partial)]


def partial_field(orig_field):
    if utils.get_origin(orig_field) is ClassVar:
        return orig_field  # ignored by pydantic anyway
    th, fi = orig_field, None
    # if pydantic Field is added in an Annotated[...] - unwrap
    if utils.get_origin(orig_field) is Annotated:
        args = utils.get_args(orig_field)
        if not isinstance(args[1], FieldInfo):
            raise RuntimeError(f"Unexpected annotation: {args}")
        th, fi = args[0], args[1]
    # map recursively to partial schemas
    return (partial_type(th), fi)


def create_partial_schema(mcls: MetadataSchema):
    """Return a new schema with all fields of the given schema optional.

    Original default values are not respected and are kept as `None`.

    The use of the returned class is for validating partial data before
    zipping together partial results into a completed one.

    This is more fancy than e.g.
    https://github.com/pydantic/pydantic/issues/1799

    because it is recursively replacing with partial models.
    This allows us to implement smart deep merge for partials.
    """
    if not issubclass(mcls, MetadataSchema):
        raise ValueError(f"{mcls} is not subclass of MetadataSchema!")

    # get all annotations (we define fields only using them)
    field_types = utils.get_type_hints(mcls, include_inherited=True)
    fields = {
        k: partial_field(v)
        for k, v in field_types.items()
        if k[0] != "_"  # ignore private ones
    }

    # NOTE: partial MUST NOT be subclass of real model!
    # this semantically does not make sense and would break things!
    ret: Type[PartialSchema] = create_model(
        partial_name(mcls),
        __base__=PartialSchema,
        __module__=mcls.__module__,
        __validators__=mcls.__validators__,  # type: ignore
        **fields,
    )

    # make sure to disable root validators
    # (they can require combinations of fields to be present!)
    ret.__pre_root_validators__ = []
    ret.__post_root_validators__ = []

    # add required info and return
    ret._partial_of = mcls
    ret._forwardref_name = partial_forwardref_name(mcls)
    return ret
