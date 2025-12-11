"""Utilities associated with the online documentation."""

from __future__ import annotations
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from ..app import BaseApp


def get_docs_url_of_class(app: BaseApp, cls_name: str) -> str:
    """Get the URL to the docs page that documents the specified App class."""
    cls_name_low = cls_name.lower()
    return (
        f"{app.docs_url}/reference/_autosummary/"
        f"{app.module}.{cls_name}.html#{app.module}-"
        f"{cls_name_low}"
    )


def get_docs_url_of_class_method(app: BaseApp, cls_name: str, method_name: str) -> str:
    """Get the URL to the docs page that documents the specified App class method."""
    return (
        f"{app.docs_url}/reference/_autosummary/"
        f"{app.module}.{cls_name}.html#{app.module}."
        f"{cls_name}.{method_name}"
    )


def get_docs_url_how_to(app: BaseApp, topic: str) -> str:
    """Get the URL to the docs page that documents the specified How-To topic.

    Examples
    --------
    >>> get_docs_url_how_to(app, "task_schemas")
    https://hpcflow.github.io/docs/stable/user/how_to/task_schemas.html
    """
    return f"{app.docs_url}/user/how_to/{topic}.html"
