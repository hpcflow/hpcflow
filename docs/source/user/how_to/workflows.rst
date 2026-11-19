Workflow how-tos
----------------

Generating a persistent workflow from a workflow template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. jinja:: first_ctx

    If you have a workflow template object in Python :code:`wkt`, you can generate a new persistent workflow via the :func:`{{ app_package_name }}.app.Workflow.from_template` function:

    .. code-block:: python

        import {{ app_module }} as {{ app_docs_import_conv }}

        wk = {{ app_docs_import_conv }}.Workflow.from_template(wkt)

    In the CLI, if you have a workflow template YAML file, you can generate a new persistent workflow like this:

    .. code-block:: console

        {{ app_package_name }} make /path/to/workflow/template.yaml

    There are several options for telling |app_name| about the format of the template, and controlling how the workflow is generated. For instance, to specify the workflow name, use the :code:`--name` option.  See the CLI reference documentation for more details.
    


Loading workflows
~~~~~~~~~~~~~~~~~

We support paths like:

* `/path/to/workflow` for local zarr or json
* `/path/to/workflow.zip` for local zarr-zip
* `/path/to/workflow.json` for local json-single # TODO
* `ssh://user@host/path/to/workflow` for remote zarr
* `ssh://user@host/path/to/workflow.zip` for remote zarr-zip
* `ssh://user@host/path/to/workflow.json` for remote json
* `https://sandbox.zenodo.org/record/1210144/files/workflow.zip` for zenodo zarr-zip

You can convert a zarr store to an archived zarr-zip store using `Workflow.zip()`.


Monitor submissions
~~~~~~~~~~~~~~~~~~~

.. jinja:: first_ctx

    In an interactive terminal, ``show`` displays the table while it collects data.
    Scheduler status starts as ``Checking...`` and run-state columns as ``Loading...``;
    each row fills in when ready. Rows can reorder once activity is known, keeping
    active submissions first. Scheduler queries remain batched across submissions.
    Redirected output prints the completed table once. Use ``--no-status`` to disable
    progressive display.

    Use ``{{ app_package_name }} show`` to list active submissions first, followed by
    recent inactive submissions. Submissions whose scheduler cannot be queried remain
    visible with unknown activity. Missing or incompatible workflow data does not imply
    that its jobs have stopped. Workflows with deleted or missing paths are hidden
    by default, before the recent-history limit is applied. Include them with
    ``--include-missing`` (also available with ``--list``). Filtering does not remove
    their registry records or saved cancellation references:

    .. code-block:: console

        {{ app_package_name }} show --include-missing
        {{ app_package_name }} show --list --include-missing

    .. code-block:: console

        {{ app_package_name }} show
        {{ app_package_name }} show --max-recent 10
        {{ app_package_name }} show 42 --full
        {{ app_package_name }} show ./my-workflow --ref-type path

    The Status column places compact coloured scheduler icons to the left of
    activity text. Space is reserved across submissions so the text stays aligned,
    including rows with no icons.
    Add ``availability`` when you need store readability and scheduler/cache
    diagnostics; it is hidden by default:

    .. code-block:: console

        {{ app_package_name }} show --columns id,name,status,availability,actions

    Workflows copied from other machines remain inspectable. Foreign schedulers
    are marked remote in Availability rather than making workflow data incompatible.
    Live queries are made only for local jobs or queued jobs whose saved machine
    matches the configured machine. Foreign process IDs are never checked locally.
    Completed action snapshots can establish inactivity; otherwise inaccessible
    schedulers leave activity unknown.

    Local ZIP workflows can be inspected by path or registry reference without
    extraction or changing the archive. Embedded terminal caches are reused when
    present; missing states are read from the archived workflow. Archive status
    reflects saved action data, and archive scheduler references are not queried.

    .. code-block:: console

        {{ app_package_name }} show ./workflow.zip

    Each invocation queries a compatible scheduler target once across all selected
    submissions, even when their resource directives or submission/cancel commands
    differ. Duplicate references share the same observation. Distinct query
    commands and cluster targets remain separate. Slurm reads one queue snapshot
    and filters job IDs locally, so finished jobs do not trigger additional queries.
    Snapshots are scoped to one invocation; the next ``show`` refreshes them.

    Rich progress displays workflow loading and scheduler queries. Suppress it
    with ``--no-status`` (or ``app.show(status=False)`` in Python).

    Use ``{{ app_package_name }} show --list`` to list all registered submission
    parts, including their local ID, workflow name, submission index, submission
    time, last recorded activity, and path. It reads the registry and checks path
    existence without loading workflows, querying schedulers, updating states, or
    pruning history. Incompatible workflows remain listed; missing workflows are
    included only with ``--include-missing``. Activity in this view is the last
    recorded observation, rather than a current scheduler query.

    The list includes all retained inactive submissions by default. An explicit
    ``--max-recent N`` limits inactive history; ``WORKFLOW_REF`` filters the list
    to registered submissions of one workflow, for example
    ``{{ app_package_name }} show --list 42``.

    The optional reference selects one workflow by a displayed local ID, an exact
    registered workflow name, or a path, including
    all retained submission parts for that workflow. Selecting a workflow disables
    the default recent-submission limit; supply ``--max-recent N`` explicitly to
    limit its inactive history. This applies to ZIP paths too. An explicit path also supports
    inspecting a workflow whose submission history is no longer retained. Local IDs are
    not reused after automatic pruning. Explicit unregistering frees an ID for reuse.

    Remove a specific registered submission part with ``unregister``:

    .. code-block:: console

        {{ app_package_name }} unregister 42

    This removes only the record identified by the displayed submission ID and frees
    that ID for reuse. Other parts of the same workflow remain registered. Running
    jobs continue and workflow files are preserved. Missing workflows can also be
    unregistered. The command is rejected while a submission command is still
    registering jobs, to protect in-progress writes.

    Unregister all inactive submissions with ``--all``. Include active and unknown
    records with ``--include-active``; this flag requires ``--all``:

    .. code-block:: console

        {{ app_package_name }} unregister --all
        {{ app_package_name }} unregister --all --include-active

    Activity is the last registered observation; no scheduler queries are made.
    Bulk removal is atomic: if any selected submission is still registering jobs,
    the whole operation fails without removing records. Other activity states are
    preserved when removing inactive entries, even if retention limits were lowered.
    Every explicitly removed ID becomes reusable. In Python, use
    ``app.unregister(all_submissions=True, include_active=False)``; bulk calls
    return the removed IDs.

    Future submissions reuse the lowest explicitly freed ID before allocating a new
    ID. Automatic retention pruning does not free IDs. After unregistering, an old
    command using that numeric ID may therefore refer to a newly registered workflow.
    The SDK equivalent is ``app.unregister(42)``.

    Filter by exact, case-sensitive workflow template names with ``--tname``:

    .. code-block:: console

        {{ app_package_name }} show --tname my_template
        {{ app_package_name }} show --tname my_template,another_template
        {{ app_package_name }} show --list --tname my_template

    The filter combines with ``WORKFLOW_REF`` and is applied before the recent
    submission limit and scheduler queries. Names refer to the original template,
    independently of a custom workflow name or directory name. New submissions
    save the template name in the registry. For older entries without a saved name,
    filtering reads only the workflow metadata header; unreadable or missing names
    do not match. ``--list`` never writes this recovered metadata or queries schedulers.
    In Python, use ``app.show(template_names=["my_template", "another_template"])``.

    Choose which columns appear, and their order, in the configuration file:

    .. code-block:: yaml

        show_timezone: local  # or UTC, Europe/London, America/New_York, etc.
        show_columns: [id, name, status, elements, times]
        show_list_columns: [id, name, submission, submit, status, path]

    Omitted columns are hidden. The available identifiers are ``id``, ``name``,
    ``status``, ``availability``, ``actions``, ``elements``, ``submit``, ``start``, ``end``, ``times``,
    ``submission``, and ``path``. Lists must be nonempty and contain no duplicates.
    The default columns are ``id,name,status,actions``. An explicit
    ``show_columns`` setting takes precedence. Override the configuration for one
    invocation with ``--columns``, for example:

    .. code-block:: console

        {{ app_package_name }} show --columns id,name,status,elements,times
        {{ app_package_name }} show --full --columns name,elements,actions

    ``Elements`` counts one overall state per task element in each submission,
    across all its iterations, using the latest attempt of each action. Errors, aborts, and upstream-failure
    skips take precedence; otherwise running actions and then waiting actions
    determine the state. Fully successful elements show completed; terminal
    task-condition skips remain distinguishable as ``C``. Loop-skipped runs are
    omitted by default. When included, terminated future iterations do not change
    an otherwise completed element to ``L``; wholly loop-skipped elements show ``L``.
    With ``--full``, elements sharing an overall state are grouped into aligned
    element ranges, using the same layout as action states.

    For confirmed inactive submissions, runs last recorded as running show as
    ``interrupted`` (an orange ``I``) in both Actions and Elements. This is a terminal
    display state with an unknown exit outcome; it does not change the stored run
    status or invent a finish time. For unknown submissions, recorded running states
    show as ``unknown`` (a grey ``?``); confirmed terminal states remain visible.
    Active submissions retain running states. Use ``show --legend`` to see all state icons.

    ``Submit``, ``Start``, and ``End`` show their respective datetimes in the
    displaying machine's local timezone, to seconds. Set ``show_timezone`` to
    ``UTC`` or an IANA timezone name to override this. Each timestamp is converted
    independently. All datetime columns (including ``Times`` and cached observation
    times) omit timezone suffixes when the timestamp's timezone abbreviation and
    UTC offset match the displaying machine's current local time. Other timestamps
    retain their abbreviation and offset, including timestamps on the other side
    of a daylight-saving change. This also applies when ``show_timezone`` selects
    a different display zone. The current local time is captured once per table.
    Stored timestamps, ordering, and duration calculations remain in UTC. Missing
    times show an em dash. ``Times`` combines all three in one cell, labelled
    ``submit``, ``start``, and ``end`` on separate lines. Within ``Times``, start/end
    dates are omitted when
    they match the submit date in the display timezone; the separate ``Start`` and
    ``End`` columns always retain the full date. Omitted dates leave blank space
    so the clock times stay aligned. Metadata-only selections such as
    ``id,name,status,submit`` skip action-state loading.
    In ``--list`` mode, all columns use registered/cached data without refreshing
    workflows or querying schedulers.

    For example, ``{{ app_package_name }} show my_workflow_2026-11-01_120000``
    selects the workflow with that registered name. Names also work with ``show --list``,
    ``cancel``, other commands accepting ``WORKFLOW_REF``, and the Python ``Workflow``
    constructor. Multiple submission parts of the same workflow are selected together.
    If several distinct workflows share a name, use a local ID or path instead.
    Names are matched exactly, including case; numeric local IDs take precedence.
    Use ``--ref-type name`` or ``--ref-type path`` to disambiguate a name from a
    relative path (and ``--ref-type name`` for a numeric workflow name). Explicit
    paths such as ``./my-workflow`` retain path semantics.

    Displayed workflow names come from workflow metadata, so enabling
    ``workflow_name_use_dir`` keeps the workflow name visible alongside its timestamp.
    New submissions save this name in the registry, keeping it available even when
    the workflow data later becomes unavailable.

    Configure history retention and the default inactive display count independently:

    .. code-block:: console

        {{ app_package_name }} config set known_submissions_max_inactive 100
        {{ app_package_name }} config set known_submissions_max_unknown 10
        {{ app_package_name }} config set show_max_recent 3

    All values are non-negative integers; zero disables retention for the respective
    activity state or disables inactive display. Unknown records are limited to the
    ten most recently submitted records by default, with local IDs breaking timestamp
    ties. Rechecking an older record does not make it more recent. Active submissions
    are always retained, so the total stored count can exceed these limits.
    Retention is applied during registry writes; ``--list`` and ``--no-update`` do
    not prune records. ``--max-recent`` overrides the inactive display setting for
    one invocation.

    With ``--full``, elements with identical ordered action states share one row. For
    example, the element range ``[0–99]`` before the action icons applies separately
    to each of those 100 elements. Task names, element ranges, iteration ranges, and
    action icons use separate columns, keeping the icons aligned across tasks.
    Iterations with identical ordered action states also share a row. For example:

    .. code-block:: text

        system_analysis  [0–9]  iters {0–22}   ■
                                iters {23}     ●
                                iters {24–63}  .

    This shows completed iterations 0–22, running iteration 23, and pending iterations 24–63.
    Each action pattern applies separately to every iteration in its range and every
    listed element. Nonconsecutive element and iteration ranges are comma-separated,
    using square brackets for elements (``[0–9,20–29]``) and curly braces for
    iterations (``{0–9,20–29}``). Different action orders and retry attempts remain separate.
    Element and iteration numbering are unchanged.

    Choose the Actions and Elements views explicitly with ``--view``:

    * ``counts`` shows overall run-state and element-state counts (the default for plain ``show``).
    * ``tasks`` shows per-task iteration progress and run totals in Actions, and
      per-task overall element-state totals in Elements, even for small workflows.
    * ``detail`` shows exact grouped element/iteration patterns in Actions and
      element ranges grouped by overall state in Elements, without automatic compaction.
    * ``auto`` chooses detailed patterns or task summaries independently for each
      column based on its layout size.
      ``--full`` is an alias for this behavior when no explicit view is supplied.

    For example, ``{{ app_package_name }} show 42 --view tasks`` requests the task
    summary, and ``{{ app_package_name }} show 42 --view detail`` retains the exact
    grouped patterns. Explicit views override ``--full``. The same views work with
    ``--list`` using saved state data. The SDK equivalent is ``show(view="tasks")``.

    Large ``--view auto`` (or ``--full``) Actions layouts automatically switch to a compact task summary.
    This happens above 40 detail rows, or above eight rows when long labels would
    crowd the icons. Each task shows its element count, completed and visible
    iteration counts per element (a minimum–maximum range when elements differ),
    and coloured run-state totals. For example, ``216–256 done / 486 visible`` means
    each element has completed between 216 and 256 of its 486 visible iterations;
    these are counts, not iteration indices. An iteration counts as done only when
    the latest attempt of every known action succeeded. Entirely condition-skipped
    iterations are reported separately as ``C``. Run totals include retry attempts.

    Tasks remain in workflow order, regardless of their states. Up to three problem groups per task include
    element and iteration references; an explicit count reports further omitted
    groups. Names wrap above the icons rather than taking their space. This view
    uses the same collected data and does not make additional scheduler queries.
    Use ``--view detail`` to inspect exact ranges while retaining grouping.

    Disable grouping independently with ``--no-group-elements`` or
    ``--no-group-iterations``, or use ``--no-group`` to disable both. These options
    imply ``--full``, preserve icon alignment, and work with ``show --list`` when
    cached state data is available:

    .. code-block:: console

        {{ app_package_name }} show 42 --no-group
        {{ app_package_name }} show 42 --no-group-iterations

    The SDK equivalents are ``group_elements=False`` and ``group_iterations=False``.
    Explicit ``counts`` and ``tasks`` views cannot be combined with disabled grouping;
    use ``detail`` or ``auto`` with those options.

    The Elements task view always counts each element once across all iterations,
    using the same latest-attempt rollup and state precedence as the other views.
    It shows no iteration progress or run-attempt counts. Tasks remain in workflow
    order regardless of their states. In ``auto``, large element
    layouts and long, fragmented element ranges use this task summary so range
    labels cannot crowd out the icons. For example:

    .. code-block:: console

        {{ app_package_name }} show 42 --view tasks --columns name,elements
        {{ app_package_name }} show 42 --view tasks --columns actions,elements


    Runs skipped because a loop terminated are hidden by default in both compact and
    full output. Skips due to upstream failures or unmet task conditions remain visible.
    To include loop-skipped runs, use
    ``{{ app_package_name }} show --full --include-loop-skipped``.
    All terminal states, including hidden skips and their reasons, remain cached;
    filtering does not change submission activity or completion tracking.
    The action symbols distinguish ``L`` (loop termination), ``C`` (task condition
    not met), and ``s`` (upstream failure or an unspecified skip reason). ``L`` and
    ``C`` use the same grey as pending actions. The *iters* label is italicized;
    element and iteration ranges retain their regular style.

    The monitoring registry is stored separately as ``known_submissions_v2.json`` in the
    host's application data directory. Legacy metadata and local IDs are imported when
    the registry is first created; the legacy text file is left untouched. Use
    ``{{ app_package_name }} open known-subs --path`` to locate the registry.

    Terminal action states are cached in
    ``artifacts/submissions/<index>/tmp/terminal_states.json.zst`` within each workflow,
    using zstd-compressed JSON. Legacy ``terminal_states.json`` files remain readable
    and are migrated on a writable observation, including already-complete caches.
    ``--no-update`` prevents migration. ZIP workflows can reuse either embedded
    format and are never modified by cache reads.
    Subsequent queries read valid cached terminal states and refresh unresolved runs.
    Missing or corrupt caches are rebuilt when the workflow is readable. ``--no-update``
    queries current information without writing monitoring history or action caches.

    For active workflows, ``show`` also saves run coordinates and each run's parent
    jobscript element in ``artifacts/submissions/<index>/tmp/run_index.json.zst``.
    The first observation builds this index from the jobscript block arrays and
    inspected runs. Later observations reuse confirmed terminal states and skip
    store lookups for indexed runs whose parent jobscript element is queued or
    waiting. Running items use active-run markers to inspect only current and
    preceding uncached runs, showing their actual individual states and
    progress within sequential jobscripts without loading every future action.
    Runs whose parent is no longer reported are also read to capture their actual
    results, including skip reasons. If every unresolved run is queued or waiting,
    no workflow load is needed. Scheduler queries remain batched across submissions.

    Grey ``Q`` identifies a scheduler-derived observation: the parent jobscript
    element is queued or waiting. It does not assert the individual action's stored
    state. Confirmed terminal states take precedence. ``Q`` observations never
    become terminal-cache entries or invent finish times. Running elements use
    ordinary action-state icons based on store reads; there is no ``R`` state.

    Executing jobs maintain one small marker per jobscript array item in
    ``artifacts/submissions/<index>/tmp/active_runs/jobs/<job-tree>/items/<shard-tree>/markers/js_<jobscript>_item_<item>.json``.
    Array items are grouped into shards of 500. Plans, locks, and temporary files
    live in separate sibling directories, so each directory has at most 500 files,
    including during concurrent marker updates. Parent trees use radix-256 branches
    to keep directory fanout bounded for very large workflows. Existing flat-layout
    cursors remain supported without relocating files used by running jobs.
    Each item executes sequentially and records its current run ID and last-started
    cursor position. Combined scripts advance the cursor one run at a time too.
    Serial/direct jobs use item zero. Markers are replaced atomically; finishing a
    run clears its active ID but retains the cursor, including on exceptions.

    Immutable execution plans in each shard's ``plans`` directory record the actual run order
    once when the submission is created. ``show`` reads uncached runs at or before
    the cursor and adds confirmed results to the terminal cache; runs after it
    remain submitted without store lookups. Numeric run IDs are never sorted to
    infer execution order. The marker remains small and no visited-ID history is
    maintained or appended to. A fingerprint binds each cursor to its plan.
    Cross-item loop, task-condition, and upstream-failure skip notifications ensure
    affected future runs are inspected even if their parent is still queued.

    Marker reads are used alongside a fresh running scheduler observation or a
    confirmed inactive submission. Inactive tracked items also avoid repeatedly
    reading future actions that never started; preceding unfinished runs are still
    inspected and retain the interrupted-state behavior.
    Missing, corrupt, mismatched, or incomplete markers fall back to the existing
    store-read path. New submissions receive an ``active_runs/enabled.json``
    capability record before execution can start. Older submissions keep the
    existing fallback; tracking is never enabled halfway through an older job,
    because its execution position cannot be established safely. The older history
    protocol also falls back rather than being reinterpreted as a cursor. Submit a new workflow submission
    to use these hooks, including regenerated combined scripts. This changes
    execution telemetry, not the store format.

    Missing mappings, unavailable schedulers, foreign machines without shared
    scheduler configuration, and ZIP workflows
    fall back to ordinary store reads for unresolved runs. Scheduler observations
    are valid only for the current command and are never persisted as evidence for
    the next command. Corrupt or mismatched indices are rebuilt when writable;
    ``--no-update`` writes neither index nor terminal cache. Embedded ZIP indices
    remain readable, and archives remain unchanged. Existing terminal caches are
    not rewritten when no new terminal states were observed.

    New submissions save scheduler references after each successful job submission.
    ``{{ app_package_name }} cancel WORKFLOW_REF`` can therefore request cancellation
    even when workflow data is missing or incompatible, provided scheduler access is
    available. Cancellation requests remain unconfirmed until a subsequent scheduler
    query establishes that jobs have stopped. Legacy records can acquire these
    references only while their workflows remain readable.

    The Python API's ``get_known_submissions()`` returns JSON-compatible monitoring
    snapshots, including activity, accessibility, saved job references, and the last
    action summary. It no longer returns live submission objects.


Inspect individual action runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. jinja:: first_ctx

    Use ``show-runs`` to inspect individual action attempts in a workflow:

    .. code-block:: console

        {{ app_package_name }} workflow WORKFLOW_REF show-runs --state error
        {{ app_package_name }} workflow WORKFLOW_REF show-runs --state running
        {{ app_package_name }} workflow WORKFLOW_REF show-runs

    ``WORKFLOW_REF`` accepts a registered local ID, an exact registered workflow
    name, or a path. Omit ``--state`` to list all runs. State names are
    case-insensitive: ``pending``, ``prepared``, ``submitted``, ``running``,
    ``skipped``, ``aborted``, ``success``, and ``error``.

    The table shows run IDs, task names and insertion IDs, element/iteration/action
    indices, attempt/submission/jobscript indices, coloured states, and exit codes.
    The Action column includes the action index and its ``short_name()``. Jobscript
    indices are local to the indicated submission; unassigned runs show an em dash.
    Indices are zero-based. Every attempt is included, so an earlier error remains
    visible after a successful retry. Missing values show an em dash, and an empty
    selection prints a message. This command reads current workflow data without
    querying schedulers or changing the monitoring registry or caches.

    The same inspection is available in Python:

    .. code-block:: python

        workflow.show_runs(state="error")
