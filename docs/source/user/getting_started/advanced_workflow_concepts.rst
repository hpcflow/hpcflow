.. jinja:: first_ctx

  Advanced workflow concepts
  ###########################

  Resources
  ----------

  Requesting resources can be done using a ``resources`` block, either for the whole workflow at the top level,

  .. code-block:: yaml

      resources:
        any:
          scheduler_args:
            directives:
              --time: 1:00:00
              --partition: multicore

  or at the task level

  .. code-block:: yaml

    - schema: simulate_VE_loading_damask
      resources:
        any:
          # This will use two cores for input file generators and output file parsers
          num_cores: 2
        main:
          # Use 16 cores for the "main" part of the task (the simulation in this case)
          num_cores: 16
      inputs:
          ...


  We can see from above that it is possible to request resources for subsets of the actions
  in a task schema. A full list of the different options you can select resources for is given below.

  - ``input_file_generator``
  - ``output_file_parser``
  - ``processing`` (a shortcut for ``input_file_generator`` +  ``output_file_parser``)
  - ``main`` (the main part of the action i.e. not ``processing``)
  - ``any`` (anything not already specified with any of the above options)

  These are used to choose resources (done at the workflow/task level),
  and also the same values can be used within the schema to select an ``environment``
  by ``scope`` e.g.

  .. code-block:: yaml

      actions:
        - environments:
        - scope:
            type: processing
            environment: damask_parse_env
        - scope:
            type: main
            environment: damask_env

  {{ app_name }} is then looking for a match within your environment definitions for the requested
  resources, and will run the command which matches those resources.

  There are lots of :ref:`resource options <reference/_autosummary/{{ app_module }}.ResourceSpec:{{ app_module }}.ResourceSpec>`
  available that can be requested.

  Scheduler arguments can be passed like this e.g. to set a time limit of 1 hour

  .. code-block:: yaml

      resources:
      any:
        scheduler_args:
          directives:
            --time: 1:00:00
        num_cores: 10

  Anything specified under `directives` is passed directly to the scheduler as a jobscript command (i.e. isn't processed by {{ app_name }} at all).

  Scheduler directives are merged by key: scheduler configuration defaults are overridden
  by workflow-level resources, then by task-level resources at the same scope.
  More specific action scopes override less specific scopes. Unchanged directives, such
  as a default ``--account``, are retained when overriding another directive such as
  ``--time``. List-valued directives are replaced as a whole, not concatenated.

  ``inherit_directives`` defaults to ``true``. Set it to ``false`` to discard raw
  directives inherited from configuration or broader resources, then apply only the
  local directives. This also applies to the deprecated ``scheduler_args.options``.
  More-specific scopes can inherit this new baseline; discarded configuration defaults
  are not restored. Resource-derived directives such as partition and core requests
  are unaffected.

  An empty ``directives: {}`` now adds no overrides; it no longer clears inherited
  directives. To clear them for a particular task, use:

  .. code-block:: yaml

    - schema: simulate_VE_loading_damask
      resources:
        main:
          num_cores: 16
          inherit_directives: false
          scheduler_args:
            directives: {}


  Task sequences
  ----------------

  {{ app_name }} can run tasks over a set of independent input values.
  For this, you use a ``sequence``, and a ``nesting_order`` to control the nesting of the loops
  but you can also "zip" two or more lists of inputs by using the same level of nesting.
  Lower values of ``nesting_order`` act like the "outer" loop.

  .. code-block:: yaml

      tasks:
        - schema: my_schema
          sequences:
            - path: inputs.conductance_value
          values:
            - 0
            - 100
            - 200
          nesting_order: 0

  Groups
  -------

  To combine outputs from multiple elements, you can use a ``group`` in a task schema:

  .. code-block:: yaml

    - objective: my_task_schema
      inputs:
        - parameter: p2
          group: my_group

  combined with a ``groups`` entry in the task itself.

  .. code-block:: yaml

    - schema: my_task_schema
      groups:
        - name: my_group

  Then whichever parameters are linked with the group in the task schema will be received by the task as a list.

  Here is an example workflow using sequences and groups that you might wish to run to solidify your understanding


  .. literalinclude:: groups_workflow.yaml
        :language: YAML


  Execution directories
  ---------------------

  Execution paths are assigned during submission, but directories are created only
  when an element action run (EAR) starts. Input files are staged at that point;
  files already present in the shared working directory are preserved so that
  later actions can use changes made by earlier actions. Skipped EARs do not
  create execution directories.

  Actions within an element iteration share their working directory. By default,
  after the final action/run completes successfully, its execution directories are
  deleted, provided all runs in that iteration succeeded and none were skipped.
  Set the ``delete_execute_dir`` configuration item to ``False`` to retain them.
  Cleanup happens after output parameters, saved files, and directory snapshots
  have been persisted, including when the final action does not itself require a
  directory. Failed or skipped iterations retain their directories for inspection.

  Files that must remain available after successful completion must be selected
  for retention with ``save_files`` (see below). Unsaved working files are deleted
  when the execution directory is removed.

  Retaining files as artifacts
  ----------------------------

  Deciding which files to keep is split into two independent steps:

  1. **Declare** the files that actions produce. Files written by an action's
     commands or script are declared in the action's ``output_files``; files
     written by an output file parser script are declared in the parser's
     ``generates_files``. The files an input file generator writes and the files a
     parser reads (``from_files``) are already declared. Every declared file is
     available as an ``output_files.<label>`` (or ``input_files.<label>``)
     parameter, so downstream tasks can depend on it. An action that declares
     ``input_files`` or ``output_files`` is given its own working directory
     (``requires_dir`` defaults to ``true``), so there is no need to set it
     explicitly.
  2. **Select** which declared files to retain as workflow artifacts with the task
     schema's ``save_files`` policy. A task may set its own ``save_files`` to replace
     the schema's policy.

  By default no files are retained: a task schema with no ``save_files`` policy keeps
  nothing (apart from files required by runs in other element iterations or
  downstream tasks, which are always retained). The policy may be:

  * ``true`` -- retain every declared file;
  * ``false`` or ``[]`` -- retain nothing;
  * a list of declared file labels, optionally together with ``path:``-prefixed
    entries that select arbitrary files within the working directory by relative
    path (glob patterns such as ``path:logs/*.log`` are allowed).

  Label entries are validated against the files declared by the schema. ``path:``
  entries must be relative to the working directory and must not contain ``..``.
  Files matched by ``path:`` entries are copied to the artifacts directory once, when
  the element iteration's final run completes (whatever its outcome), so files that
  several actions modify are saved in their final state. Unlike declared files they
  are not associated with a parameter, so they cannot be consumed by downstream
  tasks; a non-glob ``path:`` entry that does not exist is logged rather than
  treated as an error.

  .. code-block:: yaml

      task_schemas:
        - objective: simulate
          save_files: [stdout_file, path:restart/*.dat]
          actions:
            - commands: ...
              output_files: [stdout_file, result_file]
              output_file_parsers:
                summary:
                  from_files: [result_file]
                  generates_files: [summary_plot]
                  script: <<script:summarise.py>>

      tasks:
        - schema: simulate
          # Replaces the schema policy rather than adding to it:
          save_files: [stdout_file, result_file, summary_plot]

  ``clean_up`` (available on actions and on output file parsers) lists declared files
  that should be removed from the working directory once they have been saved. It only
  applies to files selected by the ``save_files`` policy, and is mainly useful when
  ``delete_execute_dir`` is ``False``.

  Task schema shortcuts
  ---------------------

  Input file generators
  ~~~~~~~~~~~~~~~~~~~~~

  ``input_file_generators`` is a convenience shortcut for a python script which generates an input file
  for a subsequent action within a task. It's more compact, easier to reference, and has more interaction options.
  The first parameter in the input generator (python) function definition must be "path",
  which is the file path to ``input_file``, the file you want to create.
  Given this is a {{ app_name }} input file, the path is just the file name which will be created in the
  execute directory.
  The ``input_file`` must point to the label of a file in ``command_files``.
  ``from_inputs`` defines which of the task schema inputs are required for each of the ``input_file_generators``.

  .. code-block:: yaml

    task_schemas:
      - objective: my_task_schema
    actions:
      - input_file_generators:
          - input_file: my_command_file
            from_inputs:
              - my_input_1
              - my_input_2
            script: <<script:/full/path/to/generate_input_file.py>>

  Output file parsers
  ~~~~~~~~~~~~~~~~~~~

  ``output_file_parsers`` is a shortcut for a python script which processes output files
  from previous steps.
  The function in the python script must have parameters for each of the files listed
  in ``from_files``, and this function should return data in a dictionary.
  The output file parser script can also have parameters for any of the task schema inputs,
  and these are listed under an ``inputs`` key.
  If you want to save results to a file, this can be done in the python function too,
  but the function should return a dict. This can be hard-coded in the function,
  or via an ``inputs: [path_to_output_file]`` line in the output file parser,
  and it will come after the output files in the function signature.

  The "name" of the ``output_file_parsers`` is the parameter returned i.e.

  .. code-block:: yaml

      output_file_parsers:
        return_parameter: # This should be listed as an output parameter for the task schema
          from_files:
            - command_file1
            - command_file2
          script: <<script:your_processing_script.py>>
          generates_files: # Files written by the parser script itself (if any)
            - command_file_written_by_parser
          inputs:
            - input1
            - input2

  Files read by the parser (``from_files``) and files it writes (``generates_files``)
  are retained as artifacts only if selected by the task schema's ``save_files``
  policy.

  The output_file_parser script that is run as the action should return one variable,
  rather than a dictionary. This is different behaviour to
  a "main" action script.
  i.e. ``return the_data`` rather than ``return {"return_parameter": the_data}``.
  This is because an output file parser only has one named output parameter,
  so a dictionary isn't needed to distinguish different output parameters.

  The :ref:`previous example <command_files_example_workflow>` has been reworked and
  expanded below to demonstrate ``input_file_generators`` and ``output_file_parsers``,
  along with the alternative code which would be needed to achieve the same result
  as the input file generator:

  .. literalinclude:: advanced_workflow.yaml
        :language: yaml


  This workflow uses the same python scripts as before, with the addition of ``parse_output.py``:

  .. literalinclude:: parse_output.py
        :language: python
