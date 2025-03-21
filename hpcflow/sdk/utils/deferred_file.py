from os import PathLike
from typing import Literal, Union


class DeferredFileWriter:
    """A class that provides a context manager for deferring writing or appending to a
    file until a write method is called.

    Attributes
    ----------
    filename
        The file path to open
    mode
        The mode to use.

    Examples
    --------
    >>> with DeferredFileWrite("new_file.txt", "w") as f:
    ...     # file is not yet created
    ...     f.write("contents")
    ...     # file is now created, but not closed
    ... # file is now closed

    """

    def __init__(self, filename: Union[str, PathLike], mode: Literal["w", "a"], **kwargs):
        self.filename = filename
        self.mode = mode
        self.file = None
        self.kwargs = kwargs
        self._is_open = False

    def _ensure_open(self):
        if not self._is_open:
            self.file = open(self.filename, self.mode, **self.kwargs)
            self._is_open = True

    def write(self, data):
        self._ensure_open()
        self.file.write(data)

    def writelines(self, lines):
        self._ensure_open()
        self.file.writelines(lines)

    def close(self):
        if self._is_open:
            self.file.close()
            self._is_open = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
