import json
from typing import Optional, cast

from pydantic import BaseModel, Extra
from pydantic_yaml import YamlModelMixin
from pydantic_yaml.mixin import YamlModelMixinConfig, YamlStyle

from .encoder import DynEncoderModelMetaclass
from .parser import ParserMixin


def _mod_def_dump_args(kwargs):
    """Set `by_alias=True` in given kwargs dict, if not set explicitly."""
    if "by_alias" not in kwargs:
        kwargs["by_alias"] = True  # e.g. so we get correct @id, etc fields
    if "exclude_none" not in kwargs:
        kwargs["exclude_none"] = True  # we treat None as "missing" so leave it out
    return kwargs


class BaseModelPlus(
    ParserMixin, YamlModelMixin, BaseModel, metaclass=DynEncoderModelMetaclass
):
    """Extended pydantic BaseModel with some good defaults.

    Used as basis for various entities, including:
    * Metadata schemas
    * Harvester arguments
    """

    class Config:
        # keep extra fields by default
        extra = Extra.allow
        # make PrivateAttr wrappers not always needed
        underscore_attrs_are_private = True
        # serialize enums properly
        use_enum_values = True
        # when alias is set, still allow using field name
        # (we use aliases for invalid attribute names in Python)
        allow_population_by_field_name = True
        # users should jump through hoops to add invalid stuff
        validate_assignment = True
        # defaults should also be validated
        validate_all = True
        # for JSON compat
        allow_inf_nan = False
        # pydantic anystr config: non-empty, non-whitespace
        # (but we prefer NonEmptyStr anyway for inheritance)
        anystr_strip_whitespace = True
        min_anystr_length = 1

    def dict(self, *args, **kwargs):
        """Return a dict.

        Nota that this will eliminate all pydantic models,
        but might still contain complex value types.
        """
        return super().dict(*args, **_mod_def_dump_args(kwargs))

    def json(self, *args, **kwargs) -> str:
        """Return serialized JSON as string."""
        return super().json(*args, **_mod_def_dump_args(kwargs))

    def json_dict(self, **kwargs):
        """Return a JSON-compatible dict.

        Uses round-trip through JSON serialization.
        """
        return json.loads(self.json(**kwargs))

    def yaml(
        self,
        *,
        # sort_keys: bool = False,
        default_flow_style: bool = False,
        default_style: Optional[YamlStyle] = None,
        indent: Optional[bool] = None,
        encoding: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Return serialized YAML as string."""
        # Current way: use round trip through JSON to kick out non-JSON entities
        # (more elegant: allow ruamel yaml to reuse defined custom JSON dumpers)
        tmp = self.json_dict(**_mod_def_dump_args(kwargs))
        # NOTE: yaml_dumps is defined by the used yaml mixin in the config
        cfg = cast(YamlModelMixinConfig, self.__config__)
        return cfg.yaml_dumps(
            tmp,
            # sort_keys=sort_keys, # does not work for some weird arg passing reason
            default_flow_style=default_flow_style,
            default_style=default_style,
            encoding=encoding,
            indent=indent,
        )

    def __bytes__(self) -> bytes:
        """Serialize to JSON and return UTF-8 encoded bytes to be written in a file."""
        # add a newline, as otherwise behaviour with text editors will be confusing
        # (e.g. vim automatically adds a trailing newline that it hides)
        # https://stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
        return (self.json() + "\n").encode(encoding="utf-8")

    def __str__(self) -> str:
        return self.json(indent=2)
