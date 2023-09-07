import platform
import tempfile
from pathlib import Path

import typer
from rich import print

from metador_core import __version__

app = typer.Typer()


@app.command("info")
def info():
    """Show information about the system and Python environment."""
    un = platform.uname()
    print(f"[b]System:[/b] {un.system} {un.release} {un.version}")
    print(
        f"[b]Python:[/b] {platform.python_version()} ({platform .python_implementation()})"
    )
    print("[b]Env:[/b]")
    # TODO: print versions of all relevant packages / starting with metador-*
    print("metador-core", __version__)


@app.command("check")
def check():
    """Run a self-test to ensure that central metador subsystems work correctly."""
    from datetime import datetime

    from metador_core.container import MetadorContainer
    from metador_core.packer.utils import pack_file
    from metador_core.plugins import schemas
    from metador_core.widget.dashboard import Dashboard
    from metador_core.widget.jupyter import Previewable

    today = datetime.today().isoformat()

    print("Loading schema plugins...")

    BibMeta = schemas["core.bib"]
    Person = schemas["core.person"]
    DBMeta = schemas["core.dashboard"]

    Material = schemas["example.matsci.material"]
    Method = schemas["example.matsci.method"]
    Instrument = schemas["example.matsci.instrument"]
    Specimen = schemas["example.matsci.specimen"]
    MSInfo = schemas["example.matsci.info"]

    print("Constructing metadata objects...")

    author = Person(
        id_="https://orcid.org/0000-0002-1825-0097",
        givenName="Josiah",
        familyName="Carberry",
    )
    my_bibmeta = BibMeta(
        name="Title for my container",
        abstract="This is a Metador-compliant container",
        author=[author],
        dateCreated=today,
    )

    msi = MSInfo(
        abstract="hello",
        author=[Person(name="Anton Pirogov")],
        dateCreated=today,
        material=[
            Material(
                materialName="bla",
                chemicalComposition="bla",
                density=1,
                crystalGrainType="single_crystal",
            )
        ],
        method=[
            Method(
                instrument=Instrument(
                    instrumentName="microscope", instrumentModel="micro2000"
                ),
                specimen=Specimen(diameter=2.5, gaugeLength=123),
            )
        ],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        csvfile = Path(tmpdir) / "testfile.csv"
        with open(csvfile, "w") as f:
            f.write(
                """AtomID,Time,PosX,PosY,PosZ
AtomA,0,0,0,0
AtomB,0,0,1,0
AtomC,0,0,0,1
AtomA,1,1,0,0
AtomB,1,0,0.5,0
AtomC,1,0,0,0.25
"""
            )

        print("Creating Metador container with test (meta)data...")
        container_path = Path(tmpdir) / "test_container.h5"
        with MetadorContainer(container_path, "w") as mc:
            # Attach the bibliographic metadata to the very top
            mc["/"].meta["core.bib"] = my_bibmeta

            # add a file
            node = pack_file(mc, csvfile)
            # add more specific metadata
            node.meta[MSInfo] = msi
            # make it visible in the dashboard with high prio
            node.meta[DBMeta] = DBMeta.show(group=1, priority=10)

        print("Opening Metador container...")
        with Previewable(MetadorContainer(container_path)) as mc:
            print("Try to access metadata...")
            mc[csvfile.name].meta[MSInfo]
            print("Try instantiating dashboard...")
            Dashboard(mc).show()

    print("[b][green]Self-check successfully completed![/green][/b]")
