from os import PathLike
from pathlib import Path
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

    def __init__(
        self,
        filename: Union[str, PathLike],
        mode: Literal["w", "a"],
        preamble=None,
        **kwargs
    ):
        self.filename = Path(filename)
        self.mode = mode
        self.preamble = preamble
        self.file = None
        self.kwargs = kwargs
        self._is_open = False

    def _ensure_open(self):
        if self._is_open:
            return

        self.filename.parent.mkdir(parents=True, exist_ok=True)

        write_preamble = self.preamble and (
            self.mode == "w"
            or not self.filename.exists()
            or self.filename.stat().st_size == 0
        )

        self.file = open(
            self.filename,
            self.mode,
            encoding="utf-8",
            **self.kwargs,
        )
        self._is_open = True

        if write_preamble:
            self.file.write(self.preamble)

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

    def flush(self):
        if self._is_open:
            self.file.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
