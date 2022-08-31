"""Schema.org-compatible common metadata schemas.

Supports a subset of commonly useful fields.

Adds no constraints beyond the spec, except for fixing a multiplicity for fields.

Can serve as the basis for more specific schemas.
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import List, Optional, Set, Union

from pydantic import AnyHttpUrl, NonNegativeInt

from ..ld import LDSchema, add_const, ld_type
from ..types import Duration, NonEmptyStr, Number

URL = AnyHttpUrl
Text = NonEmptyStr
DateOrDatetime = Union[date, datetime]
TimeOrDatetime = Union[time, datetime]

CTX_URL_SCHEMAORG = "https://schema.org"


def annotate_schemaorg_type(name: str):
    # return add_annotations(ld_type(name, context=))
    return add_const(ld_type(name, context=CTX_URL_SCHEMAORG))


@annotate_schemaorg_type("Thing")
class Thing(LDSchema):
    """See http://schema.org/Thing for field documentation."""

    # can be used as "title/caption"
    name: Optional[Text]
    # should be same as @id, if @id is identifier
    identifier: Optional[Union[URL, Text]]  # or PropertyValue
    # should be same as @id, if @id is URL
    url: Optional[URL]

    description: Optional[Text]
    disambiguatingDescription: Optional[Text]

    # linking:
    additionalType: Optional[Set[URL]]
    sameAs: Optional[Set[URL]]
    alternateName: Optional[Set[Text]]


@annotate_schemaorg_type("QuantitativeValue")
class QuantitativeValue(Thing):
    """See http://schema.org/QuantitativeValue for field documentation."""

    value: Optional[Union[bool, Number, Text]]
    minValue: Optional[Number]
    maxValue: Optional[Number]
    unitCode: Optional[Union[URL, Text]]
    unitText: Optional[Text]


@annotate_schemaorg_type("Organization")
class Organization(Thing):
    """See http://schema.org/Organization for field documentation."""

    address: Optional[Text]


@annotate_schemaorg_type("Person")
class Person(Thing):
    """See http://schema.org/Person for field documentation."""

    givenName: Optional[Text]
    familyName: Optional[Text]
    additionalName: Optional[Text]

    email: Optional[Text]
    affiliation: Optional[Organization]


OrgOrPerson = Union[Person, Organization]


@annotate_schemaorg_type("CreativeWork")
class CreativeWork(Thing):
    """See http://schema.org/CreativeWork for field documentation."""

    version: Optional[Union[NonNegativeInt, Text]]
    citation: Optional[Set[Union[CreativeWork, Text]]]

    # search
    abstract: Optional[Text]
    keywords: Optional[Set[Text]]

    # people
    author: Optional[List[OrgOrPerson]]
    contributor: Optional[List[OrgOrPerson]]
    maintainer: Optional[List[OrgOrPerson]]
    producer: Optional[List[OrgOrPerson]]
    provider: Optional[List[OrgOrPerson]]
    publisher: Optional[List[OrgOrPerson]]
    sponsor: Optional[List[OrgOrPerson]]
    editor: Optional[List[Person]]

    # date
    dateCreated: Optional[DateOrDatetime]
    dateModified: Optional[DateOrDatetime]
    datePublished: Optional[DateOrDatetime]

    # legal
    copyrightHolder: Optional[OrgOrPerson]
    copyrightYear: Optional[int]
    copyrightNotice: Optional[Text]
    license: Optional[Union[URL, CreativeWork]]

    # provenance
    about: Optional[Set[Thing]]
    subjectOf: Optional[Set[CreativeWork]]
    hasPart: Optional[Set[CreativeWork]]
    isPartOf: Optional[Set[Union[URL, CreativeWork]]]
    isBasedOn: Optional[Set[Union[URL, CreativeWork]]]


@annotate_schemaorg_type("MediaObject")
class MediaObject(CreativeWork):
    """See http://schema.org/MediaObject for field documentation."""

    # we deviate to accept only bytes and as a number,
    # storing it as string if we don't allow units is plain stupid
    # google jsonld tester did not complain.
    #
    # alternatively, we could try using a variant of
    # the quantity parser, but it should serialize to string, like pint
    # but this requires to support the K(i)/M(i)/G(i)B units
    contentSize: Optional[int]

    sha256: Optional[Text]
    encodingFormat: Optional[Union[URL, Text]]

    width: Optional[QuantitativeValue]
    height: Optional[QuantitativeValue]

    bitrate: Optional[Text]
    duration: Optional[Duration]
    startTime: Optional[TimeOrDatetime]
    endTime: Optional[TimeOrDatetime]


@annotate_schemaorg_type("Dataset")
class Dataset(CreativeWork):
    """See http://schema.org/Dataset for field documentation."""

    distribution: Optional[URL]  # NOTE: for top level description could link to repo
