.. _persistent-script-workers:

.. jinja:: first_ctx

    Persistent Python script workers
    --------------------------------

    Python script workers are an experimental, opt-in execution mode. They reuse
    an interpreter for compatible script runs within a jobscript instead of
    starting a new interpreter for each run. The ordinary command execution path
    remains the default.

    Enable workers
    ~~~~~~~~~~~~~~

    To enable workers when submitting an existing workflow:

    .. code-block:: console

        {{ app_package_name }} workflow WORKFLOW_REF submit --enable-script-workers

    Alternatively, prepare a submission with workers enabled and submit it later:

    .. code-block:: console

        {{ app_package_name }} workflow WORKFLOW_REF add-submission --enable-script-workers
        {{ app_package_name }} workflow WORKFLOW_REF submit

    In Python, the option is available on both ``Workflow.submit`` and
    ``Workflow.add_submission``:

    .. code-block:: python

        import {{ app_module }} as {{ app_docs_import_conv }}

        wk = {{ app_docs_import_conv }}.Workflow("/path/to/workflow")
        wk.submit(enable_script_workers=True)

    The setting is stored when a submission is created. If a submission is
    already pending, submitting it uses its saved setting; passing the option to
    ``submit`` does not change that pending submission.

    Workers are used for Python snippet scripts with a configured ``script_exe``
    when the jobscript shell is ``bash``, ``wsl+bash``, ``wsl``, or
    ``powershell``. Other actions continue to use the ordinary command path.
    The retired ``combine_scripts=True`` execution mode is rejected; set it to
    ``False`` independently of whether workers are enabled.

    State between runs
    ~~~~~~~~~~~~~~~~~~

    A worker can survive an intervening external action and handle a later
    compatible script. Different jobscripts, interpreter commands, environment
    specifications, or shells may use different workers.

    .. warning::

        The interpreter is not reset between requests. Explicit changes to the
        generated worker module's globals, or to imported modules, can be
        visible to later scripts using the same worker. An ordinary script
        failure does not roll back such changes. This differs from the isolation
        provided by a fresh process for each run.

    Each invocation runs its snippet inside a generated wrapper and recreates
    its local definitions; ordinary local variables do not automatically
    persist. |app_name| restores the working directory, environment variables,
    redirected standard streams, and its temporary file logger after an
    invocation. ``ContextVar`` bindings are isolated in a copied context. Other
    mutable process state is not generally reset, including objects referenced
    by a ``ContextVar``.

    Worker state is not shared across worker processes and is lost when a worker
    stops or is aborted. Do not assume that separate jobscripts or runs with
    different worker identities will share state.

    For implementation details and regression guardrails, see
    :ref:`script-worker-implementation`.
