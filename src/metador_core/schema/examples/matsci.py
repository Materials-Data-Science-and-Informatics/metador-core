"""Example schema for Metador in Materials Science tutorial.

The metadata modelling here is intentionally left simple and minimal.
"""

from typing import List, Optional

from pydantic import Field, PositiveFloat
from typing_extensions import Annotated, Literal

from ..common import rocrate, schemaorg
from ..core import MetadataSchema
from ..decorators import make_mandatory


class Material(MetadataSchema):
    """A material that was used in an experiment or simulation."""

    class Plugin:
        name = "example.matsci.material"
        version = (0, 1, 0)

    materialName: schemaorg.Text
    """The name of material."""

    chemicalComposition: Optional[schemaorg.Text]
    """Chemical formula reflecting distribution of elements in the material."""

    density: Optional[PositiveFloat]
    """The density of the material (in g/cm3)."""

    crystalGrainType: Optional[Literal["single_crystal", "bi_crystal", "poly_crystal"]]
    """Type of a grain of a crystalline material."""


class Instrument(MetadataSchema):
    """Metadata of the instrument used to generate some data."""

    class Plugin:
        name = "example.matsci.instrument"
        version = (0, 1, 0)

    instrumentName: schemaorg.Text
    instrumentModel: schemaorg.Text
    instrumentManufacturer: Optional[rocrate.Organization]


class Specimen(MetadataSchema):
    """Metadata of the specimen tested in an experiment."""

    class Plugin:
        name = "example.matsci.specimen"
        version = (0, 1, 0)

    diameter: PositiveFloat
    """The diameter of the specimen (in mm)."""

    gaugeLength: PositiveFloat
    """The gauge length of the specimen (in mm)."""


class Method(MetadataSchema):
    """A method used to conduct a materials science experiment or simulation."""

    class Plugin:
        name = "example.matsci.method"
        version = (0, 1, 0)

    methodType: Optional[Literal["tensile_test", "other"]]
    """Type of method used to obtain the resulting data."""

    instrument: Instrument
    specimen: Specimen


@make_mandatory("abstract", "dateCreated", "author")
class MatsciFileInfo(schemaorg.CreativeWork):
    """Metadata for a file with data obtained from research in Materials Science.

    **Note:** To state authors or contributors, use the `core.person` schema.
    """

    class Plugin:
        name = "example.matsci.info"
        version = (0, 1, 0)

    material: Annotated[List[Material], Field(default_factory=lambda: [], min_items=1)]
    """Physical material associated with the file."""

    method: Annotated[List[Method], Field(default_factory=lambda: [])]
    """Materials Science method associated with the file."""
