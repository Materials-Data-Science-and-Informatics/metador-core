[
![Docs](https://img.shields.io/badge/read-docs-success)
](https://materials-data-science-and-informatics.github.io/metador-core)
[
![CI](https://img.shields.io/github/actions/workflow/status/Materials-Data-Science-and-Informatics/metador-core/ci.yml?branch=main&label=ci)
](https://github.com/Materials-Data-Science-and-Informatics/metador-core/actions/workflows/ci.yml)
[
![Test Coverage](https://materials-data-science-and-informatics.github.io/metador-core/main/coverage_badge.svg)
](https://materials-data-science-and-informatics.github.io/metador-core/main/coverage)
[
![PyPIPkgVersion](https://img.shields.io/pypi/v/metador-core)
](https://pypi.org/project/metador-core/)
[
![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7174/badge)
](https://bestpractices.coreinfrastructure.org/projects/7174)
[
![fair-software.eu](https://img.shields.io/badge/fair--software.eu-%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F-green)
](https://fair-software.eu)

<!-- --8<-- [start:abstract] -->
# metador-core

The core library of the Metador framework. It provides:

* an interface for managing structured and validated metadata (`MetadorContainer`)
* an API to manage immutable (but still "patchable") HDF5 files (`IH5Record`)
* an extensible entry-points based plugin system for defining plugin groups and plugins
* core plugin group types and interfaces (schemas, packers, widgets, ...)
* general semantically aligned schemas that can be used and extended
* visualization widgets for common data types based on Bokeh and Panel
* generic dashboard presenting (meta)data for which suitable widgets are installed

<!-- --8<-- [end:abstract] -->
<!-- --8<-- [start:quickstart] -->

## Installation

You can install the current stable version of Metador from PyPI:

```
pip install metador-core
```

## Getting Started

If you successfully installed the package, check out the
[tutorial notebooks](https://materials-data-science-and-informatics.github.io/metador-core/main/notebooks/about/).

These are intended to showcase what Metador has to offer and get you started with usage
and development of your own schemas, widgets or other plugins.

To explore the notebooks interactively, clone this repo, install it, and then run:

```
pip install notebook
jupyter notebook ./docs/notebooks
```

You can use the
[metador-extension-cookiecutter](https://github.com/Materials-Data-Science-and-Informatics/metador-extension-cookiecutter)
template to generate a well-structured Python package repository that you can use to
quickly get started with Metador plugin development.

## Compatibility and Known Issues

Currently this package supports Python `>=3.8`.

We will try to support all still officially updated versions of Python,
unless forced to drop it for technical reasons.

<!-- --8<-- [end:quickstart] -->

**You can find more information on using and contributing to this repository in the
[documentation](https://materials-data-science-and-informatics.github.io/metador-core/main).**

<!-- --8<-- [start:citation] -->

## How to Cite

If you want to cite this project in your scientific work,
please use the [citation file](https://citation-file-format.github.io/)
in the [repository](https://github.com/Materials-Data-Science-and-Informatics/metador-core/blob/main/CITATION.cff).

<!-- --8<-- [end:citation] -->
<!-- --8<-- [start:acknowledgements] -->

## Acknowledgements

We kindly thank all
[authors and contributors](https://materials-data-science-and-informatics.github.io/metador-core/latest/credits).

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

<!-- --8<-- [end:acknowledgements] -->
