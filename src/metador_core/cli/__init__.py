"""Metador CLI for system introspection."""
import typer

from . import general

app = typer.Typer()
app.add_typer(general.app, name="self")
