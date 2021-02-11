"""
This file defines the ``extras_require`` argument used in setup.py -- i.e., the
set of available Bionic subpackages (like bionic[standard] or bionic[gcp]).
It's in its own file because Bionic uses the information here when importing
optional dependencies.
"""

from collections import OrderedDict


def combine(*dep_lists):
    """Combines multiple lists into a single sorted list of distinct items."""
    return list(sorted(set(dep for dep_list in dep_lists for dep in dep_list)))


# Construct the mapping from "extra name" to package descriptor.
# We use an OrderedDict because the optdep module will want to know which
# extras were added first.
extras = OrderedDict()

extras["image"] = ["Pillow"]
# We don't support versions of matplotlib below 3.1 because the default backend has
# problems on OS X; and we don't support 3.2.x because of this bug:
# https://github.com/matplotlib/matplotlib/issues/15410
extras["matplotlib"] = combine(["matplotlib>=3.1,!=3.2.*"], extras["image"])
extras["viz"] = combine(["hsluv", "networkx", "pydot"], extras["image"])

extras["standard"] = combine(extras["matplotlib"], extras["viz"])

extras["dill"] = ["dill"]
extras["dask"] = ["dask[dataframe]"]
extras["gcp"] = ["fsspec", "gcsfs"]
extras["parallel"] = ["cloudpickle", "loky"]
extras["geopandas"] = ["geopandas"]
extras["aip"] = combine(
    [
        "google-auth",
        "google-api-python-client",
        "google-cloud-logging",
        "cloudpickle",
        "docker",
    ],
    extras["gcp"],
)

extras["examples"] = combine(extras["standard"], ["scikit-learn"])
extras["full"] = combine(*extras.values())

extras["dev"] = combine(
    [
        "pytest",
        "pytest-shard",
        "black",
        "flake8",
        "flake8-print",
        "flake8-fixme",
        "sphinx!=3.2.0",
        "sphinx_rtd_theme",
        "sphinx-autobuild",
        "nbsphinx",
        "jupyter",
        "bumpversion",
        "GitPython",
    ],
    *extras.values()
)

# This will be imported by setup.py.
extras_require = extras
