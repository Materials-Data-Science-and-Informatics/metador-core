from typing import ClassVar, ForwardRef, Optional

from pydantic import Field, create_model
from pydantic.fields import ModelField

from .core import MetadataSchema
from .utils import map_typehint


class PartialModel:
    """Mixin for partial metadata models."""

    _partial_of: ClassVar[MetadataSchema]

    @classmethod
    def to_partial(cls, obj):
        """Transform instance into partial instance.

        If passed object is instance of a (subclass of) the original schema,
        no validation is performed. Otherwise, will try to parse the object.
        """
        if isinstance(obj, cls._partial_of):
            print("buh")
            return cls.construct(**obj.__dict__)
        else:
            return cls.parse_obj(obj.dict(exclude_none=True))

    def from_partial(self):
        """Return a finalized model (will run full validation)."""
        return self._partial_of.parse_obj(self.__dict__)

    def update(self, obj):
        """Return copy with updated fields (without validation)."""
        if not isinstance(obj, type(self)):
            msg = f"Argument of type {type(obj)} is not instance of {type(self)}!"
            raise ValueError(msg)
        return self.copy(update=obj.dict(exclude_none=True))


def partial_name_for(orig_cls):
    return f"__Partial_{orig_cls.__name__}"


def substitute_partial(orig_type):
    if not isinstance(orig_type, type):
        return orig_type  # not a class
    if not issubclass(orig_type, MetadataSchema):
        return orig_type  # not a suitable model
    ref = partial_name_for(orig_type)
    return ForwardRef(ref)


def partial_type(orig_type):
    return Optional[map_typehint(orig_type, substitute_partial)]


def partial_field(mf: ModelField):
    arg = None if mf.alias == mf.name else Field(alias=mf.alias)
    return (partial_type(mf.type_), arg)


def partial_model2(mcls):
    """Return partial class using create_model.

    This version could be more fancy due to replacing inner field types
    with the corresponding partial models as well,
    but this is not so easy to achieve in a robust way.

    Then we could provide an automatic and recursive "smart merge"
    of partial instances (see issue #18).
    """
    fields = {k: partial_field(v) for k, v in mcls.__fields__.items()}
    ret = create_model(  # type: ignore
        partial_name_for(mcls),
        __base__=(PartialModel, mcls),
        __module__=mcls.__module__,
        __validators__=mcls.__validators__,
        **fields,
    )
    ret._partial_of = mcls
    return ret


def partial_model(mcls):
    """Create partial class by directly subclassing.

    Thus, for now we will use the simpler approach based on:
    https://github.com/pydantic/pydantic/issues/1799
    """

    class PartialSchema(PartialModel, mcls):
        ...

    ret = PartialSchema
    for field in ret.__fields__.values():
        field.required = False
        field.default = None

    ret.__name__ = ret.__qualname__ = partial_name_for(mcls)
    ret.__module__ = mcls.__module__
    ret._partial_of = mcls
    return ret


def create_partial_model(mcls: MetadataSchema):
    """Return a new model with all fields of the given model optional.

    Original default values are not respected and are kept as `None`.

    The use of the returned class is for validating partial data before
    zipping together partial results into a completed one.
    """
    if not issubclass(mcls, MetadataSchema):
        raise ValueError(f"{mcls} is not subclass of MetadataSchema!")
    return partial_model(mcls)
