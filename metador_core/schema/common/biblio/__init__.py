"""Pydantic model for citation metadata."""

from typing import List

from ... import MetadataSchema
from ...types import nonempty_str


class BibDummyMeta(MetadataSchema):
    """Minimal bibliographic information about a package.

    Provided only for testing purposes until datacite or something similar is implemented.
    """

    title: nonempty_str
    description: nonempty_str
    creators: List[nonempty_str]


# TODO: we should define and provide a DataCite compatible model
# should be valid according to official schema:
# https://github.com/datacite/schema/blob/master/source/json/kernel-4.3/datacite_4.3_schema.json
