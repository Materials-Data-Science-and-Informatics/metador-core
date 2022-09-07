"""Simplify creation of custom parsers in pydantic models."""
from typing import Any, Dict


class ParserMixin:
    """Mixin class to simplify creation of custom pydantic field types.

    Can also be mixed into arbitrary classes, not just pydantic models,
    that is why it is kept separately from the top level base model we use.

    Also, we avoid using a custom metaclass for the mixin itself,
    to increase compatibility with various classes.
    """

    class Parser:
        schema_info: Dict[str, Any] = {}

        @classmethod
        def parse(pcls, cls, v):
            """Override and implement this method for custom parsing."""
            raise NotImplementedError

    @classmethod
    def validate(cls, v):
        """Parse and validate passed value."""
        try:
            return cls.Parser.parse(cls, v)
        except ValueError as e:
            msg = f"Could not parse {cls.__name__} value {v}: {str(e)}"
            raise ValueError(msg)

    @classmethod
    def __get_validators__(cls):
        if cls.__dict__.get(cls.Parser.__name__):
            yield cls.validate

    @classmethod
    def __modify_schema__(cls, schema):
        if parser := cls.__dict__.get(cls.Parser.__name__):
            if schema_info := parser.schema_info:
                schema.update(**schema_info)
