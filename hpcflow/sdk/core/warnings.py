from __future__ import annotations
import warnings
from functools import wraps

from ..compact_errors import FormatMixin
from ..utils.web_docs import get_docs_url_of_class, get_docs_url_of_class_method


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


class CompactWarning(Warning, FormatMixin):
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


def warn_scheduler_options_deprecated(app, cls_name: str):
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


def warn_script_data_files_use_opt_deprecated():
    return DeprecationWarning_(
        f"The action attribute 'script_data_files_use_opt' is deprecated "
        f"and will be removed in a future release.",
        solution=(
            f"Please use the attribute 'data_files_use_opt' instead of "
            f"'script_data_file_use_opt' when constructing actions."
        ),
    )


def warn_from_random_uniform_deprecated(app, cls_name):
    new_method = "from_uniform"
    link = get_docs_url_of_class_method(app, cls_name, new_method)

    if cls_name == "ValueSequence":
        soln_info = "You will need to "
    elif cls_name == "InputValue":
        soln_info = "If required, you can "

    return DeprecationWarning_(
        f"The {cls_name!r} class method 'from_random_uniform' is deprecated "
        f"and will be removed in a future release.",
        solution=(
            f"Please use {new_method!r} instead of 'from_random_uniform'. {soln_info}"
            f"specify the number of required values with the 'shape' argument, instead "
            f"of the 'num' argument. See the method documentation for details: "
            f"[link={link}]{cls_name}.{new_method}[/link]."
        ),
    )
