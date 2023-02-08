# metador-core

![Project status](https://img.shields.io/badge/project%20status-alpha-%23ff8000)
[
![Test](https://img.shields.io/github/actions/workflow/status/Materials-Data-Science-and-Informatics/metador-core/ci.yml?branch=dev&label=test)
](https://github.com/Materials-Data-Science-and-Informatics/metador-core/actions?query=workflow:test)
[
![Coverage](https://img.shields.io/codecov/c/gh/Materials-Data-Science-and-Informatics/metador-core?token=4JU2SZFZDZ)
](https://app.codecov.io/gh/Materials-Data-Science-and-Informatics/metador-core)
[
![Docs](https://img.shields.io/badge/read-docs-success)
](https://materials-data-science-and-informatics.github.io/metador-core/)

The core library of the Metador framework. It provides:

* an interface for managing structured and validated metadata (`MetadorContainer`)
* an API to manage immutable (but still "patchable") HDF5 files (`IH5Record`)
* an extensible entry-points based plugin system for defining plugin groups and plugins
* core plugin group types and interfaces (schemas, packers, widgets, ...)
* general semantically aligned schemas that can be used and extended
* visualization widgets for common data types based on Bokeh and Panel
* generic dashboard presenting (meta)data for which suitable widgets are installed

## Getting Started

This library is not a batteries-included solution, it is intended for people interested in
using and extending the Metador ecosystem and who are willing to write their own plugins
to adapt Metador to their use-case and provide tools and services based on it.

For a first taste, you can install this package just as any other package into your
current Python environment using:

```
$ pip install git+ssh://git@github.com:Materials-Data-Science-and-Informatics/metador-core.git
```

or, if you are adding it as a dependency into a poetry project:

```
$ poetry add git+ssh://git@github.com:Materials-Data-Science-and-Informatics/metador-core.git
```

As usual, it is highly recommended that you use a
[virtual environment](https://stackoverflow.com/questions/41573587/what-is-the-difference-between-venv-pyvenv-pyenv-virtualenv-virtualenvwrappe)
to ensure isolation of dependencies between unrelated projects.

If you want to write or extend plugins, such as metadata schemas or widgets,
the [tutorial notebooks](./tutorial) will get you started. They explain general concepts,
interfaces and specific plugin development topics. To launch the notebooks you can run:

```
pip install notebook
jupyter notebook ./tutorial
```

If you are interested in contributing to the actual core, see further below.

## Compatibility and Known Issues

This package supports Python `>=3.8`.

If you encounter any problems, ensure that your bug is reproducible in a simple and
minimal standalone Python script that is runnable in a venv with this package installed
and can demonstrate your issue.

## Development

This project uses [Poetry](https://python-poetry.org/) for dependency management,
so you will need to have it
[installed](https://python-poetry.org/docs/master/#installing-with-the-official-installer)
for a development setup for working on this package.

Then you can run the following lines to setup the project:

```
$ git clone git@github.com:Materials-Data-Science-and-Informatics/metador-core.git
$ cd metador-core
$ poetry install
```

Common tasks are accessible via [poethepoet](https://github.com/nat-n/poethepoet),
which can be installed by running `poetry self add 'poethepoet[poetry_plugin]'`.

* Use `poetry poe init-dev` after cloning to enable automatic linting before each commit.

* Use `poetry poe lint` to run the same linters manually.

* Use `poetry poe test` to run tests, add `--cov` to also show test coverage.

* Use `poetry poe docs` to generate local documentation.

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
