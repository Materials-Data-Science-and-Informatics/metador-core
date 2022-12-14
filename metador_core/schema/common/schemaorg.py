"""Schema.org-compatible common metadata schemas.

Supports a subset of commonly useful fields.

Adds almost no constraints beyond the spec, except for fixing a multiplicity for fields.

Intended to serve as the basis for more specific schemas.

Note that this schemas ARE NOT able to parse arbitrary schema.org-aligned metadata,
their purpose is to ensure that successfully parsed input is semantically enriched.

See schema.org official documentation for full explanation and list of all fields.
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import List, Optional, Set, Union

from pydantic import AnyHttpUrl, NonNegativeInt
from typing_extensions import TypeAlias

from ..ld import LDIdRef, LDOrRef, LDSchema, ld_decorator
from ..types import Bool, Duration, Float, Int, NonEmptyStr

CTX_URL_SCHEMAORG = "https://schema.org"

schemaorg = ld_decorator(context=CTX_URL_SCHEMAORG)

# ----

URL: TypeAlias = AnyHttpUrl
Text: TypeAlias = NonEmptyStr
Number: TypeAlias = Union[Int, Float]

DateOrDatetime = Union[date, datetime]
TimeOrDatetime = Union[time, datetime]

# ----


@schemaorg(type="Thing")
class Thing(LDSchema):
    """See https://schema.org/Thing for field documentation."""

    name: Optional[Text]
    """Name, title or caption of the entity."""

    identifier: Optional[Union[URL, Text]]  # can't put PropertyValue here, weird bug
    """Arbitrary identifier of the entity.

    Prefer @id if the identifier is web-resolvable, or use more
    specific fields if available."""

    url: Optional[URL]
    """URL of the entity."""

    description: Optional[Text]
    """Description of the entity."""

    # ----

    alternateName: Optional[List[Text]]
    """Known aliases of the entity."""

    sameAs: Optional[List[URL]]


class ValueCommon(Thing):
    """Common properties of multiple *Value classes.

    For some reason these have no common ancestor in schema.org.
    """

    value: Optional[Union[Bool, Number, Text, StructuredValue]]

    # valueReference: Optional[]

    minValue: Optional[Number]
    """Minimal value of property this value corresponds to."""

    maxValue: Optional[Number]
    """Maximal value of property this value corresponds to."""

    unitCode: Optional[Union[URL, Text]]
    """UN/CEFACT Common Code (3 characters) or URL.

    Other codes may be used with a prefix followed by a colon."""

    unitText: Optional[Text]
    """String indicating the unit of measurement.

    Useful if no standard unitCode can be provided.
    """


@schemaorg(type="StructuredValue")
class StructuredValue(ValueCommon):
    """See https://schema.org/StructuredValue for field documentation."""


@schemaorg(type="QuantitativeValue")
class QuantitativeValue(StructuredValue):
    """See https://schema.org/QuantitativeValue for field documentation."""


@schemaorg(type="PropertyValue")
class PropertyValue(StructuredValue):
    """Use 'name' for the property name and 'description' for alternative human-readable value.

    See https://schema.org/PropertyValue for field documentation.
    """

    propertyID: Optional[Union[URL, Text]]
    """A commonly used identifier for the characteristic represented by the property,
    e.g. a manufacturer or a standard code for a property."""

    measurementTechnique: Optional[Union[URL, Text]]
    """A technique or technology used in a Dataset (or DataDownload, DataCatalog),
    corresponding to the method used for measuring the corresponding variable(s)
    (described using variableMeasured).

    This is oriented towards scientific and scholarly dataset publication but
    may have broader applicability; it is not intended as a full representation
    of measurement, but rather as a high level summary for dataset discovery.
    """


# ----


@schemaorg(type="Organization")
class Organization(Thing):
    """See https://schema.org/Organization for field documentation."""

    address: Optional[Text]
    """Address of the organization."""


@schemaorg(type="Person")
class Person(Thing):
    """See https://schema.org/Person for field documentation."""

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

# ----


@schemaorg(type="CreativeWork")
class CreativeWork(Thing):
    """See https://schema.org/CreativeWork for field documentation."""

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
    copyrightYear: Optional[Int]
    copyrightNotice: Optional[Text]
    license: Optional[Union[URL, LDOrRef[CreativeWork]]]

    # provenance

    about: Optional[Set[LDOrRef[Thing]]]
    subjectOf: Optional[Set[LDOrRef[CreativeWork]]]
    hasPart: Optional[Set[LDOrRef[CreativeWork]]]
    isPartOf: Optional[Set[Union[URL, LDOrRef[CreativeWork]]]]
    isBasedOn: Optional[Set[Union[URL, LDOrRef[CreativeWork]]]]


# ----


@schemaorg(type="DefinedTermSet")
class DefinedTermSet(CreativeWork):
    """See https://schema.org/DefinedTermSet for field documentation."""

    hasDefinedTerm: List[LDOrRef[DefinedTerm]]


@schemaorg(type="DefinedTerm")
class DefinedTerm(Thing):
    """See https://schema.org/DefinedTerm for field documentation."""

    # NOTE: also use name and description

    termCode: Text
    """A code that identifies this DefinedTerm within a DefinedTermSet."""

    inDefinedTermSet: Optional[Union[URL, LDIdRef]]  # ref to a DefinedTermSet
    """A DefinedTermSet that contains this term."""


@schemaorg(type="CategoryCodeSet")
class CategoryCodeSet(DefinedTermSet):
    """See https://schema.org/CategoryCodeSet for field documentation."""

    hasCategoryCode: List[LDOrRef[CategoryCode]]


@schemaorg(type="CategoryCode")
class CategoryCode(DefinedTerm):
    """See https://schema.org/CategoryCode for field documentation."""

    codeValue: Text
    """A short textual code that uniquely identifies the value."""

    inCodeSet: Optional[Union[URL, LDIdRef]]  # ref to a CategoryCodeSet
    """A CategoryCodeSet that contains this category code."""


# ----


@schemaorg(type="MediaObject")
class MediaObject(CreativeWork):
    """See https://schema.org/MediaObject for field documentation."""

    contentSize: Optional[Int]
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


@schemaorg(type="Dataset")
class Dataset(CreativeWork):
    """See https://schema.org/Dataset for field documentation."""

    distribution: Optional[URL]  # NOTE: for top level description could link to repo
    """Downloadable form of this dataset, at a specific location, in a specific format."""

    variableMeasured: Optional[List[Union[Text, PropertyValue]]]
    """Variables that are measured in the dataset."""


# ----


@schemaorg(type="Product")
class Product(Thing):
    """See https://schema.org/Product for field documentation."""

    productID: Optional[Text]
    """The product identifier, such as ISBN."""

    # properties

    category: Optional[Union[Text, URL, CategoryCode, Thing]]
    """A category for the item.

    Greater signs or slashes can be used to informally indicate a category hierarchy.
    """

    material: Optional[Union[URL, Text, Product]]
    """A material that something is made from, e.g. leather, wool, cotton, paper."""

    pattern: Optional[Union[DefinedTerm, Text]]
    """A pattern that something has, for example 'polka dot', 'striped', 'Canadian flag'.

    Values are typically expressed as text, although links to controlled value schemes are also supported.
    """

    width: Optional[QuantitativeValue]
    height: Optional[QuantitativeValue]
    depth: Optional[QuantitativeValue]

    weight: Optional[QuantitativeValue]
    color: Optional[Text]

    additionalProperty: Optional[List[PropertyValue]]
    """A property-value pair representing an additional characteristic of the entity, e.g. a product feature or another characteristic for which there is no matching property in schema.org."""

    # meta

    productionDate: Optional[DateOrDatetime]
    releaseDate: Optional[DateOrDatetime]

    isRelatedTo: Optional[LDOrRef[Product]]
    isSimilarTo: Optional[LDOrRef[Product]]
