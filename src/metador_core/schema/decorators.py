from typing import Any, Dict, Literal, Optional

from pydantic.fields import ModelField

from ..util import is_public_name
from ..util.models import field_parent_type
from ..util.typing import get_annotations, is_enum, is_literal, is_subtype, unoptional
from .core import MetadataSchema


def _expect_schema_class(mcls):
    if not issubclass(mcls, MetadataSchema):
        raise TypeError("This decorator is for MetadataSchema subclasses!")


def _check_names_public(names):
    if priv_overrides := set(filter(lambda x: not is_public_name(x), names)):
        raise ValueError(f"Illegal private overrides: {priv_overrides}")


def make_mandatory(*names: str):
    """Make a field inherited from a base class mandatory if it is optional.

    The field must exist in a base class and must not be defined in the
    decorated class.

    Use this decorator instead of manually declaring an annotation,
    if all you need to do is making an existing field mandatory.
    """
    # NOTE: idea: could take a dict, then values are the new default for non-optional
    # but is this good? defaults are implicit optionality -> we discourage it, so no.
    _check_names_public(names)

    def make_fields_mandatory(mcls):
        _expect_schema_class(mcls)

        for name in names:
            if name not in mcls.__fields__:
                raise ValueError(f"{mcls.__name__} has no field named '{name}'!")
            if name in get_annotations(mcls):
                raise ValueError(
                    f"{mcls.__name__} already defines '{name}', cannot use decorator!"
                )

            hint = unoptional(field_parent_type(mcls, name))
            # update model and type hint (important for type analysis)
            mcls.__fields__[name].required = True
            mcls.__annotations__[name] = hint

        return mcls

    return make_fields_mandatory


def add_const_fields(consts: Dict[str, Any], *, override: bool = False):
    """Add constant fields to pydantic models.

    Must be passed a dict of field names and the constant values (only JSON-like types).

    Constant fields are optional during input.
    If present during parsing, they are be ignored and overriden with the constant.
    Constant fields are included in serialization, unless `exclude_defaults` is set.

    This can be used e.g. to attach JSON-LD annotations to schemas.

    Constant fields are inherited and may only be overridden by other constant fields
    using this decorator, they cannot become normal fields again.
    """
    _check_names_public(consts.keys())

    def add_fields(mcls):
        _expect_schema_class(mcls)

        # hacking it in-place approach:
        overridden = set()
        for name, value in consts.items():
            if field_def := mcls.__fields__.get(name):  # overriding a field
                # we allow to silently override of enum/literal types with suitable values
                # to support a schema design pattern of marked subclasses
                # but check that it is actually used correctly.
                enum_specialization = is_enum(field_def.type_)
                literal_specialization = is_literal(field_def.type_)

                valid_specialization = False
                if enum_specialization:
                    valid_specialization = isinstance(value, field_def.type_)
                elif literal_specialization:
                    lit_const = Literal[value]  # type: ignore
                    valid_specialization = is_subtype(lit_const, field_def.type_)

                if (
                    enum_specialization or literal_specialization
                ) and not valid_specialization:
                    msg = f"{mcls.__name__}.{name} cannot be overriden with '{value}', "
                    msg += f"because it is not a valid value of {field_def.type_}!"
                    raise TypeError(msg)

                # reject if not force override or allowed special cases
                if not (override or enum_specialization or literal_specialization):
                    msg = f"{mcls.__name__} already has a field '{name}'!"
                    msg += f" (override={override})"
                    raise ValueError(msg)

                else:  # new field
                    overridden.add(name)

            # this would force the exact constant on load
            # but this breaks parent compatibility if consts overridden!
            # ----
            # val = value.default if isinstance(value, FieldInfo) else value
            # ctype = Optional[make_literal(val)]  # type: ignore
            # ----
            # we simply ignore the constants as opaque somethings
            ctype = Optional[Any]  # type: ignore

            # configure pydantic field
            field = ModelField.infer(
                name=name,
                value=value,
                annotation=ctype,
                class_validators=None,
                config=mcls.__config__,
            )
            mcls.__fields__[name] = field
            # add type hint (important for our field analysis!)
            mcls.__annotations__[name] = field.type_
        ret = mcls

        # dynamic subclass approach:
        # ret = create_model(mcls.__name__, __base__=mcls, __module__=mcls.__module__, **consts)
        # if hasattr(mcls, "Plugin"):
        #     ret.Plugin = mcls.Plugin

        # to later distinguish "const" fields from normal fields:
        ret.__constants__.update(consts)
        return ret

    return add_fields


def override(*names: str):
    """Declare fields that are overridden (and not valid as subtypes).

    These are checked during plugin loading, in order to catch accidental
    overridden fields in schemas.
    """
    _check_names_public(names)

    def add_overrides(mcls):
        _expect_schema_class(mcls)
        mcls.__overrides__.update(set(names))
        return mcls

    return add_overrides
