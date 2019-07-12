#!/usr/bin/env python
import os
import logging
from codecs import open
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from subprocess import check_call
import shlex

logger = logging.getLogger(__name__)

try:
    from pypandoc import convert_text
except ImportError:
    convert_text = lambda string, *args, **kwargs: string

here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", encoding="utf-8") as readme_file:
    readme = convert_text(readme_file.read(), "rst", format="md")

with open(os.path.join(here, "R2PD", "version.py"), encoding="utf-8") as f:
    version = f.read()

version = version.split('=')[-1].strip().strip('"').strip("'")

test_requires = [
    "backports.tempfile",
    "pytest",
    "pytest-cov",
    "sphinx-rtd-theme",
    "nbsphinx",
    "sphinxcontrib-napoleon",
    "ghp-import",
]


class PostDevelopCommand(develop):

    def run(self):
        try:
            check_call(shlex.split("pre-commit install"))
        except Exception:
            logger.warning("Unable to run 'pre-commit install'")
        develop.run(self)


setup(
    name="R2PD",
    version=version,
    description="Renewable Resource and Power Data tool",
    long_description=readme,
    author="Michael Rossol, Elaine Hale",
    author_email="michael.rossol@nrel.gov",
    url="https://github.com/Smart-DS/R2PD",
    packages=find_packages(),
    package_dir={"R2PD": "R2PD"},
    entry_points={
        "console_scripts": ["R2PD=R2PD.cli:main",
                            "R2PD-lite=R2PD.r2pd_lite:cli"],
        "shapers": [
            "timeseries=R2PD.library.shapers:DefaultTimeseriesShaper",
            "forecast=R2PD.library.shapers:DefaultForecastShaper",
        ],
        # "formatters": [
        #     "csv=R2PD.library.formatters:ToCSV",
        #     "hdf=ditto.writers.opendss:ToHDF",
        # ],
    },
    package_data={
        "R2PD.library": ["solar_site_meta.csv", "wind_site_meta.csv"]
    },
    include_package_data=True,
    license="MIT license",
    zip_safe=False,
    keywords="R2PD",
    classifiers=[
        "Development Status :: Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    test_suite="tests",
    install_requires=["click", "future", "pandas", "numpy", "h5py", "scipy"],
    extras_require={
        "test": test_requires,
        "dev": test_requires + ["pypandoc", "pre-commit"],
    },
    cmdclass={"develop": PostDevelopCommand},
)
