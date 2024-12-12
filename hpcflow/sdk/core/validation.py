"""
Schema management.
"""

from importlib import resources

from valida import Schema


def get_schema(filename):
    """
    Get a valida `Schema` object from the embedded data directory.

    Parameter
    ---------
    schema: str
        The name of the schema file within the resources package
        (:py:mod:`hpcflow.sdk.data`).
    """
    package = "hpcflow.sdk.data"
    fh = resources.files(package).joinpath(filename).open("rt")
    schema_dat = fh.read()
    fh.close()
    schema = Schema.from_yaml(schema_dat)
    return schema
