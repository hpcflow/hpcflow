from pathlib import Path


def resolve_path(path):
    """On Windows Python 3.8, 3.9, and 3.10, `Pathlib.resolve` does
    not return an absolute path for non-existant paths, when it should.

    See: https://github.com/python/cpython/issues/82852

    """
    # TODO: this only seems to be used in a test; remove?
    return Path.cwd() / Path(path).resolve()  # cwd is ignored if already absolute
