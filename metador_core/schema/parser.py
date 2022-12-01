"""Simplify creation of custom parsers for pydantic models."""
from functools import partial
from typing import Any, ClassVar, Dict, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class BaseParser:
    """Parsers that work with the ParserMixin must inherit from this class."""

    schema_info: Dict[str, Any] = {}
    strict: bool = True

    @classmethod
    def parse(cls, tcls: Type[T], v: Any) -> T:
        """Override and implement this method for custom parsing.

        The default implementation will simply pass through
        any instances of `tcls` unchanged and fail on anything else.

        Make sure that the parser can also handle any object that itself
        produces as an input.

        By default, parsers are expected to normalize the input,
        i.e. produce an instance of `tcls`, any other returned type
        will lead to an exception.

        If you know what you are doing, set `strict=False` to
        disable this behavior.

        Args:
            tcls: target class whose instance should be returned
            v: value to be parsed
        """
        if not isinstance(v, tcls):
            raise TypeError(f"Expected {tcls.__name__}, but got {type(v).__name__}!")
        return v


def run_parser(cls: Type[BaseParser], tcls: Type, v: Any):
    """Parse and validate passed value."""
    # print("call parser", cls, "into", tcls, "on", v, ":", type(v))
    ret = cls.parse(tcls, v)
    if cls.strict and not isinstance(ret, tcls):
        msg = f"Parser returned:  {type(ret)}, expected: {tcls} (strict=True)"
        raise RuntimeError(msg)
    return ret


def get_parser(cls):
    """Return inner Parser class, or None.

    If the inner Parser class is not a subclass of `BaseParser`,
    will raise an exception, as this is most likely an error.
    """
    if parser := cls.__dict__.get("Parser"):
        if not issubclass(parser, BaseParser):
            msg = f"{cls}: {cls.Parser.__name__} must be a subclass of {BaseParser.__name__}!"
            raise TypeError(msg)
        return parser


class NoParserDefined:
    ...


class ParserMixin:
    """Mixin class to simplify creation of custom pydantic field types.

    Can also be mixed into arbitrary classes, not just pydantic models,
    that is why it is kept separately from the top level base model we use.

    Also, we avoid using a custom metaclass for the mixin itself,
    to increase compatibility with various classes.
    """

    Parser: ClassVar[Type[BaseParser]]

    @classmethod
    def __get_validators__(cls):
        pfunc = cls.__dict__.get("__parser_func__")

        if pfunc is None:
            if parser := get_parser(cls):
                pfunc = partial(run_parser, parser, cls)
            else:
                pfunc = NoParserDefined
            cls.__parser_func__ = pfunc  # cache it

        if pfunc is not NoParserDefined:  # return cached parser function
            # print("yield custom parser", pfunc)
            yield pfunc

        # important: if no parser is given, return the default validate function of the model!
        if issubclass(cls, BaseModel):
            yield cls.validate

    @classmethod
    def __modify_schema__(cls, schema):
        if parser := get_parser(cls):
            if schema_info := parser.schema_info:
                schema.update(**schema_info)


__all__ = ["BaseParser", "ParserMixin"]
