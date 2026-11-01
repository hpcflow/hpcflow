"""Application-facing monitoring methods and their lazy callable interfaces.

Collection, registry persistence, filtering, and rendering remain in their dedicated
modules. This mixin supplies the public app properties and dispatches their calls
through the host application's existing binding and function-cache machinery.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, TYPE_CHECKING, cast

from rich.console import Group
from rich.panel import Panel
from rich.table import Table, box
from rich.text import Text
from rich import print as rich_print

from ..core.warnings import batch_warnings
from ..log import TimeIt
from ..submission.enums import JobscriptElementState

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import Protocol

    from rich.status import Status

    from ..app import BaseApp

    class _GetKnownSubmissions(Protocol):
        """Type of :py:meth:`BaseApp.get_known_submissions`"""

        # Should be overloaded on as_json, but not bothering
        def __call__(
            self,
            max_recent: int | None = None,
            no_update: bool = False,
            as_json: bool = False,
            status: Status | None = None,
        ) -> list[dict[str, Any]]: ...

    class _Show(Protocol):
        """Type of :py:meth:`BaseApp.show`"""

        def __call__(
            self,
            max_recent: int | None = None,
            full: bool = False,
            no_update: bool = False,
            workflow_ref: int | str | Path | None = None,
            ref_type: str | None = None,
            include_loop_skipped: bool = False,
            list_only: bool = False,
            columns: Sequence[str] | None = None,
            template_names: Sequence[str] | str | None = None,
        ) -> None: ...


class MonitoringMixin:
    """Connect submission monitoring to a BaseApp host.

    The host provides configuration, workflow construction, scheduler adapters,
    logging, data directories, and the protected ``_get_app_func`` binding hook.
    It owns initialization and callable caching; this mixin has no initializer or
    separate mutable state. Workflow-reference resolution and cancellation remain
    the host's responsibility.
    """

    @property
    def _monitoring_app(self) -> BaseApp:
        """Expose the BaseApp host contract without a runtime circular import.

        This mixin is intended for BaseApp and its derived applications. The
        type-only dependency documents the complete app interface required by
        collection helpers without redeclaring its properties or dynamic classes.
        """
        return cast("BaseApp", self)

    @property
    def get_known_submissions(self) -> _GetKnownSubmissions:
        """Retrieve JSON-compatible snapshots from the monitoring registry.

        Active and uncertain submissions precede recent inactive history. ``max_recent``
        defaults to ``show_max_recent``. ``no_update`` prevents registry/cache writes.
        ``as_json`` is retained for compatibility; results always contain plain data.
        """
        return self._monitoring_app._get_app_func("get_known_submissions")

    @property
    def show(self) -> _Show:
        """Show active submissions and recent inactive history.

        Parameters
        ----------
        workflow_ref : int, str, Path, optional
            Select all retained parts of one workflow by local ID, exact registered
            workflow name, or path. An explicit path can recover pruned history.
        ref_type : str, optional
            Interpret the reference as ``id``, ``name``, or ``path``. Automatic
            interpretation prefers registered local IDs, then names, then paths.
        template_names : str or sequence of str, optional
            Filter by original, case-sensitive template names, independently of
            custom workflow names. Accept comma-separated text or a sequence.
            Filter before scheduler queries and recent-history selection.
        max_recent : int, optional
            Maximum inactive submissions to display; active and unknown submissions
            are always included. Defaults to ``show_max_recent`` for live monitoring
            or all retained inactive history with ``list_only=True``.
        full : bool, default False
            Show aligned task/element ranges and grouped iteration action patterns
            instead of compact counts. Elements share one state across iterations.
        no_update : bool, default False
            Observe live states without writing the registry or terminal-state caches.
        include_loop_skipped : bool, default False
            Include runs skipped because a loop terminated. Task-condition skips
            remain distinct from loop skips and upstream-failure skips.
        list_only : bool, default False
            Use registered/cached observations without constructing workflows or
            querying schedulers. Template filters may read metadata headers for
            older records whose template names have not been saved.
        columns : sequence of str, optional
            Override the ordered ``show_columns`` or ``show_list_columns`` setting
            for this call. See ``monitoring.columns.SHOW_COLUMNS`` for identifiers.

        Notes
        -----
        Datetime columns use ``show_timezone`` (local by default). Suffixes are
        omitted only when abbreviation and offset match the machine's current
        local time. The combined Times column aligns clock times and omits dates
        matching the submit date. Stored timestamps remain UTC.

        Examples
        --------
        >>> app.show(template_names=["simulation", "analysis"])
        >>> app.show(workflow_ref=42, full=True, columns=["name", "elements", "times"])
        >>> app.show(list_only=True, template_names="simulation,analysis")
        """
        return self._monitoring_app._get_app_func("show")

    @property
    def show_legend(self) -> Callable[[], None]:
        """
        Output a legend for the jobscript-element and EAR states that are displayed
        by the `show` command.
        """
        return self._monitoring_app._get_app_func("show_legend")

    @TimeIt.decorator
    def _get_known_submissions(
        self,
        max_recent: int | None = None,
        no_update: bool = False,
        as_json: bool = False,
        status: Status | None = None,
    ) -> list[dict[str, Any]]:
        """Return JSON-compatible monitoring snapshots from the v2 registry.

        ``as_json`` is retained for callers; snapshots always contain plain data.
        """
        from hpcflow.sdk.monitoring.status import collect
        from hpcflow.sdk.monitoring.models import to_json

        if status:
            status.update("Inspecting submissions...")

        return to_json(
            collect(self._monitoring_app, max_recent=max_recent, no_update=no_update)
        )

    def _show_legend(self) -> None:
        """
        Output a legend for the jobscript-element and EAR states that are displayed
        by the `show` command.
        """
        js_notes = Panel(
            "The [i]Status[/i] column shows scheduler activity (active, inactive, or "
            "unknown), workflow accessibility, and observed jobscript states. "
            "Workflow readability and scheduler activity are independent. Jobscript element "
            "state meanings are shown below.",
            width=80,
            box=box.SIMPLE,
        )

        js_tab = Table(box=box.SQUARE, title="Jobscript element states")
        js_tab.add_column("Symbol")
        js_tab.add_column("State")
        js_tab.add_column("Description")
        for jse_state in JobscriptElementState.__members__.values():
            js_tab.add_row(jse_state.rich_repr, jse_state.name, jse_state.doc)

        act_notes = Panel(
            "\nThe [i]Actions[/i] column of the `show` command output displays either the "
            "counts of action states for that submission, or (with the `--full` "
            "option) identical action patterns grouped by element ranges. Each pattern "
            "applies separately to every listed element. Element and iteration ranges "
            "precede the icons in aligned columns. Iterations with identical action "
            "patterns share a row; nonconsecutive ranges are comma-separated. Each "
            "pattern applies to every listed iteration. Runs skipped due to loop "
            "termination are hidden by default; use --include-loop-skipped to show "
            "them. Failure and task-condition skips remain visible. The optional "
            "Elements column combines the latest action attempts into one state per "
            "element across all iterations, retaining failures and active progress. Action state "
            "meanings are shown below.",
            width=80,
            box=box.SIMPLE,
        )

        act_tab = Table(box=box.SQUARE, title="Action states")
        act_tab.add_column("Symbol")
        act_tab.add_column("State")
        act_tab.add_column("Description")
        from hpcflow.sdk.monitoring.states import ACTION_STATES

        for state in sorted(ACTION_STATES.values(), key=lambda state: state.order):
            act_tab.add_row(
                Text(state.symbol, style=state.colour), state.name, state.description
            )

        group = Group(
            js_notes,
            js_tab,
            act_notes,
            act_tab,
        )
        rich_print(group)

    @TimeIt.decorator
    @batch_warnings
    def _show(
        self,
        max_recent: int | None = None,
        full: bool = False,
        no_update: bool = False,
        workflow_ref: int | str | Path | None = None,
        ref_type: str | None = None,
        include_loop_skipped: bool = False,
        list_only: bool = False,
        columns: Sequence[str] | None = None,
        template_names: Sequence[str] | str | None = None,
    ) -> None:
        """Resolve display settings, collect selected submissions, and render the table.

        Validate columns and timezone before querying monitoring sources. Dispatch
        registry-only listings separately so they cannot trigger live state collection.
        See ``show`` for the callable's public parameters and configuration rules.
        """
        from hpcflow.sdk.monitoring.columns import SHOW_COLUMNS, configured_columns
        from hpcflow.sdk.monitoring.timezones import configured_timezone

        selected_columns = configured_columns(
            self._monitoring_app.config, list_only, columns
        )
        display_timezone = configured_timezone(self._monitoring_app.config)
        if list_only:
            from hpcflow.sdk.monitoring.listing import list_registered_submissions
            from hpcflow.sdk.monitoring.render import render_registered

            render_registered(
                list_registered_submissions(
                    self._monitoring_app,
                    workflow_ref,
                    ref_type,
                    max_recent,
                    template_names,
                ),
                columns=selected_columns,
                display_timezone=display_timezone,
                full=full,
                include_loop_skipped=include_loop_skipped,
            )
            return
        from hpcflow.sdk.monitoring.render import render
        from hpcflow.sdk.monitoring.status import collect

        render(
            collect(
                self._monitoring_app,
                workflow_ref,
                ref_type,
                max_recent,
                no_update,
                template_names=template_names,
                include_summary=any(
                    SHOW_COLUMNS[name].requires_summary for name in selected_columns
                ),
            ),
            full=full,
            include_loop_skipped=include_loop_skipped,
            columns=selected_columns,
            display_timezone=display_timezone,
        )
