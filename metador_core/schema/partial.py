from typing import ClassVar, ForwardRef, Optional

from pydantic import create_model

from .core import MetadataSchema
from .utils import map_typehint


class PartialModel(MetadataSchema):

    _partial_of: ClassVar[MetadataSchema]

    def complete(self):
        """Return a completed model (will run full validation)."""
        return self._partial_of.parse_obj(self.dict())

    def update(self, **kwargs):
        """Update fields inplace (validates only the new fields)."""
        # TODO: recursive "zipping", see issue #18
        for k, v in kwargs.items():
            # as validate_assignment=True in MetadatSchema.Config,
            # this will trigger validation
            setattr(self, k, v)
        return self


def partial_name_for(orig_cls):
    return f"__Partial_{orig_cls.__name__}"


def create_partial_model(mcls: MetadataSchema):
    """Return a new model with all fields of the given model optional.

    Default values are not respected and are kept as `None`.

    The use of the returned class is for validating partial data before
    zipping together partial results into a completed one.
    """
    if not issubclass(mcls, MetadataSchema):
        raise ValueError(f"{mcls} is not subclass of MetadataSchema!")

    def to_partial(orig_type):
        if not isinstance(orig_type, type):
            return orig_type  # not a class
        if not issubclass(orig_type, MetadataSchema):
            return orig_type  # not a suitable model
        ref = partial_name_for(orig_type)
        return ForwardRef(ref)

    def partialize_model_field(orig_type):
        return Optional[map_typehint(orig_type, to_partial)]

    fields = {
        name: (partialize_model_field(f.type_), None)
        for name, f in mcls.__fields__.items()
    }

    ret = create_model(  # type: ignore
        partial_name_for(mcls),
        __base__=PartialModel,
        __module__=mcls.__module__,
        __validators__=mcls.__validators__,
        **fields,
    )
    ret._partial_of = mcls

    return ret
