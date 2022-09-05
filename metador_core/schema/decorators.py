from typing import Any, Dict, Optional

from pydantic.fields import FieldInfo, ModelField

from .core import MetadataSchema
from .utils import (
    get_annotations,
    is_public_name,
    make_literal,
    parent_field_type,
    unoptional,
)


def _expect_schema_class(mcls):
    if not issubclass(mcls, MetadataSchema):
        raise ValueError("This decorator is for MetadataSchema subclasses!")


def _check_names_public(names):
    if priv_overrides := set(filter(lambda x: not is_public_name(x), names)):
        raise ValueError(f"Illegal private overrides: {priv_overrides}")


def override(*names: str):
    """Declare fields that are overridden.

    These are checked during plugin loading, in order to catch accidental
    overridden fields in schemas.
    """
    _check_names_public(names)

    def add_overrides(mcls):
        _expect_schema_class(mcls)
        mcls.__overrides__.update(set(names))
        return mcls

    return add_overrides


def specialize(*names: str):
    """Declare fields that are overridden with a narrowed type.

    Like `override`, but for these fields it is also checked that the
    new field allows a subset of values allowed in the original field.
    """
    _check_names_public(names)

    def add_specialized(mcls):
        _expect_schema_class(mcls)
        mcls.__specialized__.update(set(names))
        return override(*names)(mcls)

    return add_specialized


def make_mandatory(*names: str):
    """Make a field inherited from a base class mandatory if it is optional.

    The field must exist in a base class and must not be defined in the
    decorated class.

    Use this decorator instead of manually declaring an annotation,
    if all you need to do is making an existing field mandatory.
    """
    _check_names_public(names)

    def make_fields_mandatory(mcls):
        _expect_schema_class(mcls)

        for name in names:

            if name not in mcls.__fields__:
                raise ValueError(f"{mcls} has no field named '{name}'!")
            if name in get_annotations(mcls):
                raise ValueError(
                    f"{mcls} manually defines '{name}', cannot use decorator!"
                )

            hint = unoptional(parent_field_type(mcls, name))
            # update model and type hint
            mcls.__fields__[name].required = True
            mcls.__annotations__[name] = hint

        return specialize(*names)(mcls)

    return make_fields_mandatory


def const(consts: Dict[str, Any], *, override: bool = False):
    """Add constant fields to pydantic models.

    Must be passed a dict of field names mapped to the default values (only default JSON types).

    Annotated fields are optional during parsing and are added to a parsed instance.
    If is present during parsing, they must have exactly the passed annotated value.

    Annotation fields are included in serialization, unless exclude_defaults is set.

    This can be used e.g. to make JSON data models semantic by attaching JSON-LD annotations.
    """
    _check_names_public(consts.keys())

    def add_fields(mcls):
        _expect_schema_class(mcls)

        # hacking it in-place approach:
        overridden = set()
        for name, value in consts.items():

            if name in mcls.__fields__:
                if not override:
                    raise ValueError(
                        f"{mcls} already has a field '{name}'! (override={override})"
                    )
                else:
                    overridden.add(name)

            val = value.default if isinstance(value, FieldInfo) else value
            ctype = Optional[make_literal(val)]  # type: ignore
            field = ModelField.infer(
                name=name,
                value=value,
                annotation=ctype,
                class_validators=None,
                config=mcls.__config__,
            )
            mcls.__fields__[name] = field
            mcls.__annotations__[name] = field.type_
        ret = mcls

        # dynamic subclass approach:
        # ret = create_model(mcls.__name__, __base__=mcls, __module__=mcls.__module__, **consts)
        # if hasattr(mcls, "Plugin"):
        #     ret.Plugin = mcls.Plugin

        # to later distinguish "const" fields from normal fields:
        parent_consts = mcls.__dict__.get("__constants__", set())
        ret.__constants__ = parent_consts.union(set(consts.keys()))
        ret.__overrides__.update(overridden)
        return ret

    return add_fields
