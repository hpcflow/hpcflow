"""Code to enable user-friendly exceptions and warnings that do not print the Python
tracebacks, but may include additional information such as links to relevant
documentation, and how to solve the problem."""

from __future__ import annotations
import sys
import traceback
import warnings
from typing import Any, Type, TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.traceback import Traceback

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.errors import CompactException, format_problem
from hpcflow.sdk.core.warnings import CompactWarning


if TYPE_CHECKING:
    from typing import ClassVar, TextIO
    from types import TracebackType
    from rich.text import Text


class CompactProblemFormatter(AppAware):
    """Compact exception/warning formatter. Multiple instances are allowed, but only
    one may be enabled at any time."""

    #: The currently active instance, if there is one.
    _active_instance: ClassVar[CompactProblemFormatter | None] = None
    #: The default width within which to warp error and warning messages.
    _DEFAULT_WIDTH: ClassVar[int] = 90
    #: The Rich colour used for exceptions.
    ERROR_COLOUR: ClassVar[str] = "red3"
    #: The Rich colour used for warnings.
    WARNING_COLOUR: ClassVar[str] = "yellow"

    def __init__(
        self,
        width: int = _DEFAULT_WIDTH,
        console_kwargs: dict[str, Any] | None = None,
        output_stream: TextIO | None = None,
    ):
        self.enabled = False
        self.width = width
        self.output_stream = output_stream
        self.console_kwargs = console_kwargs or {}

        # original Python hooks
        self._orig_excepthook = sys.excepthook
        self._orig_showwarning = warnings.showwarning

        # IPython state
        self._ipython_hook_patched = False
        self._ipython_orig_showtraceback = None

        #: assigned on first access via the Config (note: if we change these default
        #: values, we must update `BaseApp._load_config` accordingly).
        self._show_tracebacks = False
        self._use_rich_tracebacks = False

    @property
    def show_tracebacks(self) -> bool:
        """Whether to show the traceback for `CompactException`s, in addition to the
        custom formatted string. Tracebacks for other exceptions are always shown."""
        return self._show_tracebacks

    @show_tracebacks.setter
    def show_tracebacks(self, value):
        self._show_tracebacks = bool(value)
        self._re_patch_ipython_hook()

    @property
    def use_rich_tracebacks(self) -> bool:
        """Whether to use Rich to display tracebacks."""
        return self._use_rich_tracebacks

    @use_rich_tracebacks.setter
    def use_rich_tracebacks(self, value):
        self._use_rich_tracebacks = bool(value)
        self._re_patch_ipython_hook()

    @property
    def ipython_shell(self) -> Any | None:
        """The IPython shell, if present."""
        return self._app.run_time_info.ipython_shell

    def get_console(self) -> Console:
        """Rich console for printing messages.

        For testing, the output stream will be set to something provided by a test
        fixture; for normal use the output stream will be `sys.stderr`.

        """
        return Console(
            file=self.output_stream or sys.stderr, width=self.width, **self.console_kwargs
        )

    def enable(self):
        """Enable compact formatting of `CompactException`s and `CompactWarning`s."""
        if self.__class__._active_instance is not None:
            raise RuntimeError(f"Another {self.__class__.__name__!r} is already active.")
        self.__class__._active_instance = self
        self.enabled = True
        sys.excepthook = self._excepthook
        warnings.showwarning = self._showwarning
        self._patch_ipython_hook()

    def disable(self):
        """Disable compact formatting of `CompactException`s and `CompactWarning`s."""
        if self.__class__._active_instance is self:
            self.__class__._active_instance = None
        self.enabled = False
        sys.excepthook = self._orig_excepthook
        if warnings.showwarning is self._showwarning:
            warnings.showwarning = self._orig_showwarning
        self._unpatch_ipython_hook()

    def __show_rich_traceback(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ):
        """Show the exception traceback using Rich."""
        rich_tb = Traceback.from_exception(exc_type, exc_value, exc_tb, show_locals=True)
        self.get_console().print(rich_tb)

    def __show_traceback(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ):
        """Show the exception traceback, using Rich if configured to do so."""
        if self.use_rich_tracebacks:
            self.__show_rich_traceback(exc_type, exc_value, exc_tb)
        else:
            self._orig_excepthook(exc_type, exc_value, exc_tb)

    def _excepthook(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ):
        """Custom except-hook that overrides `sys.excepthook` when enabled."""
        if self.enabled and isinstance(exc_value, CompactException):
            if self.show_tracebacks:
                self.__show_traceback(
                    exc_type,
                    exc_value,
                    exc_tb,
                )

            self._show_compact_exception(type(exc_value), exc_value, exc_tb)
        else:
            self.__show_traceback(exc_type, exc_value, exc_tb)

    def _showwarning(
        self,
        message: Warning,
        category,
        filename,
        lineno,
        file=None,
        line=None,
    ):
        """Function that overrides the standard `warnings.showwarning` function."""
        if self.enabled and issubclass(category, CompactWarning):
            self._show_compact_warning(message, category, filename, lineno, file, line)
        else:
            self._orig_showwarning(message, category, filename, lineno, file, line)

    @staticmethod
    def __format_obj(obj, **kwargs) -> tuple[Text, Text]:
        """Run the function for formatting the exception or warning object.

        If a method `format` is defined, then use that. Otherwise use the default
        formatter.
        """
        if hasattr(obj, "format"):
            return obj.format(**kwargs)
        else:
            return format_problem(obj=obj, **kwargs)

    def __print_stderr(
        self,
        title,
        subtitle,
        obj: CompactException | CompactWarning,
        colour,
        filename: str,
        lineno: int,
        cause: dict | None = None,
    ):
        """Print to stderr the compact exception or warning class."""
        console = self.get_console()
        title, txt = self.__format_obj(
            title=title,
            subtitle=subtitle,
            obj=obj,
            colour=colour,
            filename=filename,
            lineno=lineno,
            console=console,
        )
        if cause:
            cause_title, cause_txt = self.__format_obj(**cause)
            txt = txt + "\n\n" + cause_title + "\n" + cause_txt

        console.print(Panel(txt, title=title, title_align="left", border_style=colour))

    @staticmethod
    def __extract_location(
        tb: TracebackType | None,
        default_filename: str = "<not raised>",
        default_lineno: int = 0,
    ):
        """Try to extract the file name and line number of the initial traceback
        frame."""
        try:
            last_frame = traceback.extract_tb(tb)[-1]
            filename = last_frame.filename
            lineno = last_frame.lineno
        except IndexError:
            # no frames in the traceback (e.g. if exception never raised, as in
            # testing):
            filename = default_filename
            lineno = default_lineno
        return (filename, lineno)

    def _show_compact_exception(
        self,
        exc_type: Type[CompactException],
        exc_value: CompactException,
        exc_tb: TracebackType | None,
    ):
        """Print to stderr the formatted `CompactException`."""
        filename, lineno = self.__extract_location(exc_tb)
        cause = None
        if cause_exc := getattr(exc_value, "__cause__", None):
            cs_filename, cs_lineno = self.__extract_location(
                cause_exc.__traceback__,
                default_filename=filename,
                default_lineno=lineno,
            )
            cause = dict(
                title="Caused by",
                subtitle=cause_exc.__class__.__name__,
                obj=cause_exc,
                colour=self.ERROR_COLOUR,
                filename=cs_filename,
                lineno=cs_lineno,
            )
        self.__print_stderr(
            title="Error",
            subtitle=exc_type.__name__,
            obj=exc_value,
            colour=self.ERROR_COLOUR,
            filename=filename,
            lineno=lineno,
            cause=cause,
        )

    def _show_compact_warning(
        self,
        message,
        category,
        filename,
        lineno,
        file=None,
        line=None,
    ):
        """Print to stderr the formatted `CompactWarning`."""
        # note: our custom warning classes may include a "_" suffix (when subclassing
        # built-in warning classes of the same name), so remove:
        subtitle = category.__name__.removesuffix("_")
        self.__print_stderr(
            title="Warning",
            subtitle=subtitle,
            obj=message,
            colour=self.WARNING_COLOUR,
            filename=filename,
            lineno=lineno,
        )

    def _patch_ipython_hook(self):
        """Patch the IPython exception hook to use our custom hook."""
        if not (ip := self.ipython_shell):
            return

        # restore first to avoid stacking wrappers:
        self._unpatch_ipython_hook()

        orig = getattr(ip.showtraceback, "__wrapped__", ip.showtraceback)
        self._ipython_orig_showtraceback = orig

        def __show_traceback(exc_tuple, *args, **kwargs):
            if self.use_rich_tracebacks:
                self.__show_rich_traceback(*exc_tuple)
            else:
                self._ipython_orig_showtraceback(exc_tuple, *args, **kwargs)

        def _ip_showtraceback(*args: Any, **kwargs: Any):
            exc_type, _, _ = exc_tuple = ip._get_exc_info()
            if self.enabled and issubclass(exc_type, CompactException):
                if self.show_tracebacks:
                    __show_traceback(exc_tuple, *args, **kwargs)
                self._show_compact_exception(*exc_tuple)
            else:
                __show_traceback(exc_tuple, *args, **kwargs)

        ip.showtraceback = _ip_showtraceback
        self._ipython_hook_patched = True

    def _unpatch_ipython_hook(self):
        """Unpatch the IPython exception hook to use the original hook."""
        if not self._ipython_hook_patched:
            return

        if (ip := self.ipython_shell) and self._ipython_orig_showtraceback:
            ip.showtraceback = self._ipython_orig_showtraceback

        self._ipython_hook_patched = False
        self._ipython_orig_showtraceback = None

    def _re_patch_ipython_hook(self):
        """Unpatch, and then patch the IPython exception hook to use our custom hook."""
        self._unpatch_ipython_hook()
        self._patch_ipython_hook()
