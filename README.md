# metador-core

![Project status](https://img.shields.io/badge/project%20status-alpha-%23ff8000)
[
![Test](https://img.shields.io/github/workflow/status/Materials-Data-Science-and-Informatics/metador-core/test?label=test)
](https://github.com/Materials-Data-Science-and-Informatics/metador-core/actions?query=workflow:test)
[
![Coverage](https://img.shields.io/codecov/c/gh/Materials-Data-Science-and-Informatics/metador-core?token=4JU2SZFZDZ)
](https://app.codecov.io/gh/Materials-Data-Science-and-Informatics/metador-core)
[
![Docs](https://img.shields.io/badge/read-docs-success)
](https://materials-data-science-and-informatics.github.io/metador-core/)

Core library of the Metador platform. It provides:

* an interface for managing structured and validated metadata (`MetadorContainer`)
* an API to manage immutable (but still "patchable") HDF5 files (`IH5Record`)
* an extensible entry-points based plugin system defining plugin groups and plugins
* core plugin group interfaces (schemas, packers, widgets, ...)
* general semantically aligned schemas that should be used and extended
* visualization widgets for common data types based on Bokeh and Panel
* generic dashboard presenting (meta)data for which suitable widgets are installed

## Developer Tutorial

Tutorials on various topics are provided [here](./tutorial)

## Getting Started

As a user, you can install this package just as any other package into your current
Python environment using:
```
$ pip install metador-core@git+https://github.com/Materials-Data-Science-and-Informatics/metador-core.git
```

As usual, it is highly recommended that you use a
[virtual environment](https://stackoverflow.com/questions/41573587/what-is-the-difference-between-venv-pyvenv-pyenv-virtualenv-virtualenvwrappe)
to ensure isolation of dependencies between unrelated projects
(or use `poetry` as described further below, which automatically takes care of this).

## Compatibility and Known Issues

This package supports Python `>=3.8`.

There was a mysterious bug when using inside Jupyter `6.4.6`,
but there are no known problems when upgrading to Jupyter `6.4.10`.

If you encounter any problems, ensure that your bug is reproducible in a simple and
minimal standalone Python script that is runnable in a venv with this package installed
and can demonstrate your issue.

## Development

This project uses [Poetry](https://python-poetry.org/) for dependency
management, so you will need to have poetry
[installed](https://python-poetry.org/docs/master/#installing-with-the-official-installer)
in order to contribute.

Then you can run the following lines to setup the project and install the package:
```
$ git clone https://github.com/Materials-Data-Science-and-Informatics/metador-core.git
$ cd metador-core
$ poetry install
```

Run `pre-commit install` (see [https://pre-commit.com](https://pre-commit.com))
after cloning. This enables pre-commit to enforce the required linting hooks.

Run `pytest` (see [https://docs.pytest.org](https://docs.pytest.org)) before
merging your changes to make sure you did not break anything. To check
coverage, use `pytest --cov`.

To generate local documentation (as the one linked above), run
`pdoc -o docs metador_core` (see [https://pdoc.dev](https://pdoc.dev)).

## Acknowledgements

<div>
<img style="vertical-align: middle;" alt="HMC Logo" src="https://github.com/Materials-Data-Science-and-Informatics/Logos/raw/main/HMC/HMC_Logo_M.png" width=50% height=50% />
&nbsp;&nbsp;
<img style="vertical-align: middle;" alt="FZJ Logo" src="https://github.com/Materials-Data-Science-and-Informatics/Logos/raw/main/FZJ/FZJ.png" width=30% height=30% />
</div>
<br />

This project was developed at the Institute for Materials Data Science and Informatics
(IAS-9) of the Jülich Research Center and funded by the Helmholtz Metadata Collaboration
(HMC), an incubator-platform of the Helmholtz Association within the framework of the
Information and Data Science strategic initiative.
