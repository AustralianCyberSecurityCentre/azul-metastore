#!/usr/bin/env python3
"""Setup script."""
import os

from setuptools import setup


def open_file(fname):
    """Open and return a file-like object for the relative filename."""
    return open(os.path.join(os.path.dirname(__file__), fname))


setup(
    name="azul-metastore",
    description="Common library for Azul 3 data & metadata manipulation.",
    author="Azul",
    author_email="azul@asd.gov.au",
    url="https://www.asd.gov.au/",
    packages=["azul_metastore"],
    include_package_data=True,
    python_requires=">=3.12",
    classifiers=[],
    entry_points={
        # shortcut to run ingestor
        "console_scripts": ["azul-metastore = azul_metastore.entry:cli"],
        # plugins for restapi
        "azul_restapi.plugin": [
            "binaries_submit = azul_metastore.restapi.binaries_submit:router",
            "binaries_data = azul_metastore.restapi.binaries_data:router",
            "binaries = azul_metastore.restapi.binaries:router",
            "features = azul_metastore.restapi.features:router",
            "users = azul_metastore.restapi.me:router",
            "plugins = azul_metastore.restapi.plugins:router",
            "purge = azul_metastore.restapi.purge:router",
            "sources = azul_metastore.restapi.sources:router",
            "statistics = azul_metastore.restapi.statistics:router",
        ],
    },
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    install_requires=[r.strip() for r in open_file("requirements.txt") if not r.startswith("#")],
)
