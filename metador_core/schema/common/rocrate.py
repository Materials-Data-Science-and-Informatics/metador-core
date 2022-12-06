"""RO Crate compatible Metadata.

Here we do impose certain constraints (make fields mandatory).

See https://www.researchobject.org/ro-crate/1.1/
"""
from __future__ import annotations

from typing import Set

from pydantic import parse_obj_as, root_validator, validator

from ..decorators import make_mandatory
from ..ld import LDIdRef, ld_decorator
from ..types import MimeTypeStr, NonEmptyStr
from . import schemaorg

CTX_URL_ROCRATE = "https://w3id.org/ro/crate/1.1/context"


rocrate = ld_decorator(context=CTX_URL_ROCRATE)


@make_mandatory("contentSize", "sha256")
@rocrate(type="File")
class FileMeta(schemaorg.MediaObject):
    class Plugin:
        name = "core.file"
        version = (0, 1, 0)

    # NOTE: We do not use `name` here because `name` is used semantically
    # like a title in schema.org, which could also make sense for a file to have.
    filename: NonEmptyStr
    """Original name of the file in source directory."""

    encodingFormat: MimeTypeStr
    """MIME type of the file."""


@rocrate(type="Dataset")
class DirMeta(schemaorg.Dataset):
    class Plugin:
        name = "core.dir"
        version = (0, 1, 0)

    hasPart: Set[LDIdRef] = set()
    """References to (a subset of) contained files and subdirectories."""


@rocrate(type="Organization")
class Organization(schemaorg.Organization):
    @validator("id_")
    def check_id(cls, v):
        if not v:
            return None
        if not v.startswith("https://ror.org/"):
            raise ValueError("Person @id must be a valid ROR URL!")
        return parse_obj_as(schemaorg.URL, v)


@rocrate(type="Person")
class Person(schemaorg.Person):
    @validator("id_")
    def check_id(cls, v):
        if not v:
            return None
        if not v.startswith("https://orcid.org/"):
            raise ValueError("Person @id must be a valid ORCID URL!")
        return parse_obj_as(schemaorg.URL, v)

    @root_validator(pre=True)
    def check_name(cls, values):
        has_given_or_additional = values.get("givenName") or values.get(
            "additionalName"
        )
        if has_given_or_additional and not values.get("familyName"):
            msg = "givenName and additionalName require also familyName to be provided!"
            raise ValueError(msg)
        return values

    @root_validator
    def infer_name(cls, values):
        missing_name = values.get("name") is None
        if missing_name:
            parts = []
            for k in ["givenName", "additionalName", "familyName"]:
                if v := values.get(k):
                    parts.append(v)
            values["name"] = " ".join(parts)
        return values


# required self-description of RO-Crate file
rocrate_self_meta = {
    "@type": "CreativeWork",
    "@id": "ro-crate-metadata.json",
    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
    "about": {"@id": "./"},
}
