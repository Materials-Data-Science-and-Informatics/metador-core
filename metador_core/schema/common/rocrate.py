"""RO Crate compatible Metadata.

Here we do impose certain constraints (make fields mandatory).

See https://www.researchobject.org/ro-crate/1.1/
"""
from typing import Set, Union

from pydantic import parse_obj_as, root_validator, validator

from ..ld import LDIdRef, add_const, ld_type
from . import schemaorg
from .schemaorg import URL, Text

CTX_URL_ROCRATE = "https://w3id.org/ro/crate/1.1/context"


def annotate_rocrate_type(name: str):
    return add_const(ld_type(name, context=CTX_URL_ROCRATE))


@annotate_rocrate_type("File")
class FileMeta(schemaorg.MediaObject):
    class Plugin:
        name = "core.file"
        version = (0, 1, 0)

    # required by RO-Crate to be relative to RO Crate root and be URL-encoded
    # we keep just the actual filename in "filename"
    # and set @id when assembling the crate.
    # cannot use "name" because "name" is used for stuff like "title" as well
    filename: str

    contentSize: int
    sha256: Text
    # NOTE: preferably MIME type or PRONOM or contextual entity
    encodingFormat: Union[URL, Text]


@annotate_rocrate_type("Dataset")
class DirMeta(schemaorg.Dataset):
    class Plugin:
        name = "core.dir"
        version = (0, 1, 0)

    # required by RO-Crate to be relative to RO Crate root, URL-encoded and end with slash
    # we keep just the actual dir name in "name"
    # and set @id when assembling the crate.

    # to list (subset of) files and subdirectories:
    hasPart: Set[LDIdRef] = set()


@annotate_rocrate_type("Organization")
class Organization(schemaorg.Organization):
    @validator("id_")
    def check_id(cls, v):
        if not v:
            return None
        if not v.startswith("https://ror.org/"):
            raise ValueError("Person @id must be a valid ROR URL!")
        return parse_obj_as(schemaorg.URL, v)


@annotate_rocrate_type("Person")
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
