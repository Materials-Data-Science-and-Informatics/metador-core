"""RO Crate compatible Metadata.

Here we do impose certain constraints (make fields mandatory).

See https://www.researchobject.org/ro-crate/1.1/
"""
from typing import List

from pydantic import Field, parse_obj_as, root_validator, validator
from typing_extensions import Annotated

from .. import SchemaPlugin
from ..ld import LDIdRef, add_annotations, ld_type
from . import schemaorg
from .schemaorg import Text


def annotate_rocrate_type(name: str):
    return add_annotations(
        ld_type(name, context="https://w3id.org/ro/crate/1.1/context")
    )


@annotate_rocrate_type("File")
class FileMeta(schemaorg.MediaObject):
    class Plugin(SchemaPlugin):
        name = "core.file"
        version = (0, 1, 0)

    # required by RO-Crate to be relative to RO Crate root and be URL-encoded
    # we keep just the actual filename in "name"
    # and set @id when assembling the crate.

    # NOTE: preferably size in bytes
    contentSize: Annotated[str, Field(regex="^[0-9]+$")]
    # NOTE: preferably MIME type or PRONOM or contextual entity
    encodingFormat: List[str]
    # we require hashsums
    sha256: Text


@annotate_rocrate_type("Dataset")
class DirMeta(schemaorg.Dataset):
    class Plugin(SchemaPlugin):
        name = "core.dir"
        version = (0, 1, 0)

    # required by RO-Crate to be relative to RO Crate root, URL-encoded and end with slash
    # we keep just the actual dir name in "name"
    # and set @id when assembling the crate.

    # to list (subset of) files and subdirectories:
    hasPart: List[LDIdRef] = []


@annotate_rocrate_type("Organization")
class Organization(schemaorg.Organization):
    @validator("id_")
    def check_id(cls, v):
        if not v.startswith("https://ror.org/"):
            raise ValueError("Person @id must be a valid ROR URL!")
        return parse_obj_as(schemaorg.URL, v)


@annotate_rocrate_type("Person")
class Person(schemaorg.Person):
    @validator("id_")
    def check_id(cls, v):
        if not v.startswith("https://orcid.org/"):
            raise ValueError("Person @id must be a valid ORCID URL!")
        return parse_obj_as(schemaorg.URL, v)

    @root_validator(pre=True)
    def check_name(cls, values):
        if (values.get("givenName") or values.get("additionalName")) and not values.get(
            "familyName"
        ):
            raise ValueError(
                "givenName and additionalName require also familyName to be provided!"
            )
        return values

    @root_validator
    def assemble_name(cls, values):
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

# TODO: add somewhere helper to assemble RO Crate metadata file
