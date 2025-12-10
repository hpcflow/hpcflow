from __future__ import annotations
import warnings
from functools import wraps

from ..utils.web_docs import get_docs_url_of_class


def batch_warnings(func):
    """Decorator to deduplicate and defer warnings until the function has returned."""

    @wraps(func)
    def inner(*args, **kwargs):
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            result = func(*args, **kwargs)
        seen = set()
        for warning in warning_list:
            # cannot deduplicate with a filter, because filters are ignored by
            # `showwarning`, so keep track of re-issued:
            if (
                key := (
                    str(warning.message),
                    warning.category,
                    warning.filename,
                    warning.lineno,
                )
            ) in seen:
                continue
            else:
                seen.add(key)
            # use `showwarning` (which will be patched), instead of `warnings.warn`, so we
            # maintain the file name and line number:
            warnings.showwarning(
                message=warning.message,
                category=warning.category,
                filename=warning.filename,
                lineno=warning.lineno,
                file=warning.file,
                line=warning.line,
            )
        return result

    return inner


class CompactWarning(Warning):
    """A base class for warnings that might include additional descriptive information
    that can be printed when the warning is issued."""

    def __init__(self, message, solution=None, docs=None):
        super().__init__(message)
        self.solution = solution
        self.docs = docs or {}


class UserWarning_(CompactWarning, UserWarning):
    """UserWarning that inherits from the `CompactWarning` base class which
    facilitates a more descriptive warning message."""

    def __init__(self, message, solution=None, docs=None):
        super().__init__(message, solution, docs)


class DeprecationWarning_(CompactWarning, DeprecationWarning):
    """DeprecationWarning that inherits from the `CompactWarning` base class which
    facilitates a more descriptive warning message."""

    def __init__(self, message, solution=None, docs=None):
        super().__init__(message, solution, docs)


# `DeprecationWarning` is not shown by default unless it originates from `__main__`, but
# our `DeprecationWarning_`s are important user-facing deprecations which should always be
# visible:
warnings.simplefilter("always", DeprecationWarning_)


def warn_obj_sched_options_deprecated(app, cls_name: str):
    link = get_docs_url_of_class(app, cls_name)
    return DeprecationWarning_(
        f"The scheduler attribute 'options' is deprecated and will be "
        f"removed in a future release.",
        solution=(
            f"Please use 'directives' instead of 'options' when "
            f"parametrising the scheduler. See the scheduler class documentation for "
            f"more details: [link={link}]{cls_name}[/link]."
        ),
    )
