"""Schema.org-compatible common metadata schemas.

Supports a subset of commonly useful fields.

Adds no constraints beyond the spec, except for fixing a multiplicity for fields.

Can serve as the basis for more specific schemas.
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import List, Optional, Set, Union

from pydantic import AnyHttpUrl, NonNegativeInt

from ..ld import LDOrRef, LDSchema, ld_type_decorator
from ..types import Duration, NonEmptyStr, Number

URL = AnyHttpUrl
Text = NonEmptyStr
DateOrDatetime = Union[date, datetime]
TimeOrDatetime = Union[time, datetime]

CTX_URL_SCHEMAORG = "https://schema.org"

schemaorg_type = ld_type_decorator(CTX_URL_SCHEMAORG)


@schemaorg_type("Thing")
class Thing(LDSchema):
    """See http://schema.org/Thing for field documentation."""

    name: Optional[Text]
    """Name, title or caption of the entity."""

    identifier: Optional[Union[URL, Text]]
    """Arbitrary identifier of the entity.

    Prefer @id if the identifier is web-resolvable, or use more
    specific fields if available."""

    url: Optional[URL]
    """URL of the entity."""

    description: Optional[Text]
    """Description of the entity."""


@schemaorg_type("QuantitativeValue")
class QuantitativeValue(Thing):
    """See http://schema.org/QuantitativeValue for field documentation."""

    value: Optional[Union[bool, Number, Text]]

    minValue: Optional[Number]
    """Minimal value of property this value corresponds to."""

    maxValue: Optional[Number]
    """Maximal value of property this value corresponds to."""

    unitCode: Optional[Union[URL, Text]]
    """UN/CEFACT Common Code (3 characters) or URL.

    Other codes may be used with a prefix followed by a colon."""

    unitText: Optional[Text]
    """String indicating the unit of measurement.

    Useful if no standard unitCode can be provided."""


@schemaorg_type("Organization")
class Organization(Thing):
    """See http://schema.org/Organization for field documentation."""

    address: Optional[Text]
    """Address of the organization."""


@schemaorg_type("Person")
class Person(Thing):
    """See http://schema.org/Person for field documentation."""

    givenName: Optional[Text]
    """Given name, typically the first name of a Person."""

    familyName: Optional[Text]
    """Family name of a Person."""

    additionalName: Optional[Text]
    """Additional name for a Person, e.g. for a middle name."""

    email: Optional[Text]
    """E-mail address."""

    affiliation: Optional[LDOrRef[Organization]]
    """An organization this person is affiliated with."""


OrgOrPerson = Union[Person, Organization]


@schemaorg_type("CreativeWork")
class CreativeWork(Thing):
    """See http://schema.org/CreativeWork for field documentation."""

    version: Optional[Union[NonNegativeInt, Text]]
    """Version of this work.

    Either an integer, or a version string, e.g. "1.0.5".

    When using version strings, follow https://semver.org
    whenever applicable.
    """

    citation: Optional[Set[Union[LDOrRef[CreativeWork], Text]]]
    """Citation or reference to another creative work, e.g.
    another publication, scholarly article, etc."""

    # search

    abstract: Optional[Text]
    """A short description that summarizes the creative work."""

    keywords: Optional[Set[Text]]
    """Keywords or tags to describe this creative work."""

    # people

    author: Optional[List[LDOrRef[OrgOrPerson]]]
    """People responsible for the work, e.g. in research,
    the people who would be authors on the relevant paper."""

    contributor: Optional[List[LDOrRef[OrgOrPerson]]]
    """Additional people who contributed to the work, e.g.
    in research, the people who would be in the acknowledgements
    section of the relevant paper."""

    maintainer: Optional[List[LDOrRef[OrgOrPerson]]]
    producer: Optional[List[LDOrRef[OrgOrPerson]]]
    provider: Optional[List[LDOrRef[OrgOrPerson]]]
    publisher: Optional[List[LDOrRef[OrgOrPerson]]]
    sponsor: Optional[List[LDOrRef[OrgOrPerson]]]
    editor: Optional[List[LDOrRef[Person]]]

    # date

    dateCreated: Optional[DateOrDatetime]
    dateModified: Optional[DateOrDatetime]
    datePublished: Optional[DateOrDatetime]

    # legal

    copyrightHolder: Optional[LDOrRef[OrgOrPerson]]
    copyrightYear: Optional[int]
    copyrightNotice: Optional[Text]
    license: Optional[Union[URL, LDOrRef[CreativeWork]]]

    # provenance

    about: Optional[Set[LDOrRef[Thing]]]
    subjectOf: Optional[Set[LDOrRef[CreativeWork]]]
    hasPart: Optional[Set[LDOrRef[CreativeWork]]]
    isPartOf: Optional[Set[Union[URL, LDOrRef[CreativeWork]]]]
    isBasedOn: Optional[Set[Union[URL, LDOrRef[CreativeWork]]]]


@schemaorg_type("MediaObject")
class MediaObject(CreativeWork):
    """See http://schema.org/MediaObject for field documentation."""

    contentSize: Optional[int]
    """Size of the object in bytes."""

    sha256: Optional[Text]
    """Sha256 hashsum string of the object."""

    encodingFormat: Optional[Union[URL, Text]]
    """MIME type, or if the format is too niche or no standard MIME type is
    defined, an URL pointing to a description of the format."""

    width: Optional[QuantitativeValue]
    """Width of the entity."""

    height: Optional[QuantitativeValue]
    """Height of the entity."""

    bitrate: Optional[Text]
    """Bitrate of the entity (e.g. for audio or video)."""

    duration: Optional[Duration]
    """Duration of the entity (e.g. for audio or video)."""

    startTime: Optional[TimeOrDatetime]
    """Physical starting time, e.g. of a recording or measurement."""

    endTime: Optional[TimeOrDatetime]
    """Physical ending time, e.g. of a recording or measurement."""


@schemaorg_type("Dataset")
class Dataset(CreativeWork):
    """See http://schema.org/Dataset for field documentation."""

    distribution: Optional[URL]  # NOTE: for top level description could link to repo
    """Downloadable form of this dataset, at a specific location, in a specific format."""
