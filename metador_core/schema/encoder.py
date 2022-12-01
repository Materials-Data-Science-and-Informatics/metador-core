"""Support for dynamically registered JSON encoders in pydantic.

Preparation:

For your top level BaseModel, set `DynEncoderModelMeta` as metaclass, e.g.

```
class MyBaseModel(BaseModel, metaclass=DynEncoderModelMeta):
    ...
```

(If you already use a custom metaclass for your base model,
add the `DynJsonEncoder` metaclass mixin)

Usage:

Decorate any class with `@json_encoder(ENCODER_FUNCTION)`.

To add an encoder for some existing class you cannot decorate,
use `add_json_encoder(ClassName, func)`.

Note that `json_encoders` declared as intended by Pydantic in the `Config` section
will always be prioritized over the dynamic encoder. This means, that the dynamic
encoders are only triggered for classes that are not models themselves
(because pydantic handles them already).

Also note that to prevent bugs, you cannot override encoders for a class that already
has a registered dynamic encoder. Use the normal pydantic mechanisms for
cases where this is really needed.

Ideally, design your classes in a way that there is a 1-to-1 relationship between
class and desired JSON encoder (e.g. you can have different subclasses with
different encoders).
"""

from typing import Any, Callable, Dict, Type

from pydantic.main import ModelMetaclass

_reg_json_encoders: Dict[Type, Callable[[Any], str]] = {}
"""Global registry of declared JSON encoders."""


def json_encoder(func):
    """Decorate a class to register a new JSON encoder for it."""

    def reg_encoder(cls):
        if issubclass(cls.__class__, ModelMetaclass):
            raise TypeError("This decorator does not work for pydantic models!")
        if hasattr(cls, "__dataclass_fields__"):
            raise TypeError("This decorator does not work for dataclasses!")

        if cls in _reg_json_encoders:
            raise ValueError(f"A JSON encoder function for {cls} already exists!")

        _reg_json_encoders[cls] = func
        return cls

    return reg_encoder


def add_json_encoder(cls, func):
    """Register a JSON encoder function for a class."""
    return json_encoder(func)(cls)


# ----


def _dynamize_encoder(encoder_func):
    """Wrap the JSON encoder pydantic generates from the Config to support the dynamic registry."""

    def wrapped_encoder(obj):
        try:
            # try the default lookup
            return encoder_func(obj)
        except TypeError as e:
            if enc := _reg_json_encoders.get(type(obj)):
                return enc(obj)  # try dynamic lookup
            raise e

    return wrapped_encoder


class DynJsonEncoderMetaMixin(type):
    """Metaclass mixin to first look in dynamic encoder registry.

    Combine this with (a subclass of) `ModelMetaClass` and use it for your custom base model.
    """

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        self.__json_encoder__ = staticmethod(_dynamize_encoder(self.__json_encoder__))


class DynEncoderModelMetaclass(DynJsonEncoderMetaMixin, ModelMetaclass):
    """Set this metaclass for your custom base model to enable dynamic encoders."""
