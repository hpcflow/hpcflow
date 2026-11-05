.. jinja:: first_ctx

    Resources
    ---------

    Scratch working directories
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Set the ``scratch`` resource to a parent directory for execution working directories.
    For example, to run simulations on node-local storage:

    .. code-block:: yaml

      resources:
        any:
          scratch: /tmp

    The default, ``null`` (Python ``None``), keeps working directories in the workflow's
    ``execute`` directory. When specified, working directories are placed under
    ``<scratch>/{{ app_name }}_<workflow-ID>/execute``, retaining the usual task, element,
    iteration and run subdirectories. Relative scratch paths are resolved against the
    workflow directory; ``~`` is expanded on the execution machine. The scratch path must
    be non-empty. As with other resources, it can be specified at workflow or task level,
    or for a particular action scope. Different ``scratch`` values do not force separate
    jobscripts.

    Scratch directories and staged input files are created when a run starts, not when
    its submission is prepared. Saved files and parsed outputs still go into the workflow
    store. The usual ``save_files`` and ``delete_execute_dir`` policies apply: successful
    iterations are cleaned up by default, while failed iterations retain their working
    directories. After cleanup, empty ancestors are pruned up to and including the
    workflow-specific scratch directory. Pruning stops at non-empty directories, so other
    runs' files are preserved. The supplied scratch parent is never removed. When
    ``delete_execute_dir`` is false, working directories are retained.

    By default, ``move_scratch_to_workflow: true`` moves retained custom-scratch working
    directories back into the workflow's normal ``execute`` tree. This includes failed
    iterations and successful iterations retained by ``delete_execute_dir: false``.
    Moving waits until every action in the iteration has finished or been skipped, so
    intermediate files remain available to actions that still need them. All remaining
    contents are moved, including undeclared files and subdirectories; directory lookup
    and temporary file references are updated. Empty scratch ancestors are then pruned.
    Existing non-empty destinations are reported as errors rather than overwritten.

    Set ``move_scratch_to_workflow: false`` in the application configuration or the
    workflow template's ``config`` mapping to leave retained directories on scratch.
    Recovery requires the execution process to reach its normal completion handling:
    node loss, forcible termination, or scheduler wall-time termination may prevent it.

    Node-local scratch is not shared between nodes. Actions that share intermediate
    working files must use the same scratch path and execute on the same node, for example
    within one job. No automatic transfer of intermediate working files between jobs or
    nodes is performed. Files needed by downstream tasks should be saved into the workflow
    store. Node-local directories may be removed by the cluster when a job finishes,
    regardless of the workflow's cleanup policy.

    Shell and scheduler arguments
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    When submitting a workflow, {{ app_name }} generates jobscripts that are submitted to
    the scheduler (if using one), or invoked directly (if not). Depending on how the
    scheduler is configured by your HPC administrators, you may need to add extra
    arguments to the shebang line of the jobscript. A shebang line usually looks something
    like this:

    .. code-block:: bash

        #!/bin/bash

    For example, on an HPC system, you might need to execute the job submission script via
    a bash *login* shell, meaning the first line in your jobscript should look like this:

    .. code-block:: bash
    
        #!/bin/bash --login

    To achieve this in {{ app_name }}, we can edit the configuration's `shells` block to
    look like this (note this excerpt is not a valid configuration on its own!):

    .. code-block:: yaml

      config:        
        shells:
          bash:
            defaults:
              executable_args: [--login]

    In this way, we ensure that wherever a ``bash`` shell command is constructed (such as
    when constructing the shebang line for a jobscript), ``--login`` will be appended to
    the shell executable command.

    We can also modify the shell executable path like this:

    .. code-block:: yaml

      config:
        shells:
          bash:
            defaults:
              executable: /bin/bash # /bin/bash is the default value
              executable_args: [--login]

    Additionally, there is one other place where the shell command is constructed, which
    is when {{ app_name }} invokes a commands file to execute a run. Typically, the shell
    command that you set in the above configuration change is sufficient. However, if you
    need these two scenarios to use different shell executables or executable arguments,
    you can additionally modify the scheduler's ``shebang_executable`` default value in
    the configuration (which overrides the ``shell`` configuration) like this:

    .. code-block:: yaml

      config:
        shells:
          bash:
            defaults:
              executable_args: [--login] # applied when invoking command files
        schedulers:
          sge:
            defaults:
              shebang_executable: [/path/to/bash/executable, arg_1, arg_2] # applied to scheduler shebang only

    Note that in this case (for ``shebang_exectuable``), the shell executable path must
    also be specified, in addition to the shell arguments.

    Random seeds
    ~~~~~~~~~~~~

    By default, |app_name| sets a workflow-level resource item ``random_seed`` (using
    Numpy's ``np.random.SeedSequence.generate_state(1)``). This integer is then exposed
    as a (string) environment variable called ``{{ app_caps }}_RUN_RANDOM_SEED``. The 
    value of this seed can be overriden in the usual way of setting resource items 
    (e.g. at the workflow level or at the task level, for a given action scope).

    If specifying a ``repeats`` value in a task (or element set), a value sequence will be 
    auto-generated by default to assign distinct ``random_seed`` values for each individual
    repeat (using the path ``resources.any.random_seed``). By default a master seed will be
    generated and stored in the ``repeats`` descriptor, from which the sequence's seed
    values are derived. For reproducibility, the master seed can be specified manually in
    the repeats descriptor:
    
    .. code-block:: yaml

      tasks:
        - schema: my_schema
          inputs:
            p1: 100
          repeats:
            number: 2
            master_seed: 1234

    Resource references in inputs
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    An input value can reference an element's resources using
    ``<<resource:random_seed>>``. This is shorthand for
    ``<<resource:any.random_seed>>``. To select a different action scope, use a
    reference such as ``<<resource:main.num_cores>>``.

    References can appear inside nested dictionaries and lists, including the
    arguments to a parameter constructor. For example, a constructor accepting a
    ``seed`` argument can use the automatically generated seed for each repeat:

    .. code-block:: yaml

      tasks:
        - schema: my_schema
          repeats:
            number: 2
            master_seed: 1234
          inputs:
            p1::my_random_constructor:
              seed: <<resource:random_seed>>

    The reference must occupy the entire string; it is not interpolated into
    surrounding text. References are resolved when elements are created, and the
    resolved input values are stored. Reloading the workflow or sourcing an input
    in a downstream task therefore preserves the defining element's resource
    value. An unavailable or unset resource raises an error. Literal seeds and
    omitted seed arguments are unchanged.

    To prevent a ``random_seed`` sequence being generated, you 
    can set the repeats attribute ``generate_seed_sequence`` to ``False``. For example, in
    YAML, instead of:

    .. code-block:: yaml

      tasks:
        - schema: my_schema
          inputs:
            p1: 100
          repeats: 2

    you can write:

    .. code-block:: yaml

      tasks:
        - schema: my_schema
          inputs:
            p1: 100
          repeats:
            number: 2
            generate_seed_sequence: false
    
    Random number generator spawn keys
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    An RNG spawn key (as used by Numpy's ``SeedSequence``) can be specified via the ``rng_spawn_key`` resources key. If specified, this should be an integer or list of integers. This list is exposed as a comma-separated string environment variable called ``{{ app_caps }}_RUN_RNG_SPAWN_KEY`` during run execution.
