.. jinja:: first_ctx

  Command-line interface
  ======================

  CLI reference documentation.

  The ``go``, ``demo-workflow go``, and ``workflow WORKFLOW_REF submit`` commands
  accept ``--ret-id`` to print the local workflow ID after submission. Use
  ``--quiet --ret-id`` to suppress submission messages and print only the ID.
  If multiple submissions submit jobscripts, each local ID is printed on its own
  line. This option requires ``--add-to-known`` (the default).

  Debugging an action run
  -----------------------

  Use ``workflow REF debug-execute-run --run-id ID`` to replay one run, or
  ``--first-failed`` to select the lowest-ID errored/interrupted attempt.
  A run last recorded as running is considered interrupted when its jobscript
  element is no longer active or activity cannot be ascertained.

  The command writes a new jobscript, commands file, and composed action script
  under a unique ``artifacts/submissions/SUB/tmp/debug_run_ID_*`` directory.
  It prints their paths and pauses so you can edit them, for example to print
  ``func_kwargs`` immediately before the script function is called.
  Confirm to submit a single diagnostic job using the original resources and
  environment, without dependencies or array execution. ``--yes`` skips the pause.
  ``--direct`` instead executes the commands file synchronously without submitting
  a scheduler job; its exit code becomes the command's exit code.

  Working files are copied into the debug directory's ``work`` subdirectory.
  Debug execution does not save output parameters/files to the workflow, record
  run status, or advance loops/dependencies. Diagnostic logs and script-created
  files remain in the debug directory. This is not a security sandbox: scripts
  that explicitly write external paths, call services, or modify a supplied
  workflow object can still cause side effects. Use an unpacked workflow on the
  execution machine with its original software and input files available.

  .. click:: {{ app_package_name }}.cli:cli
    :prog: {{ app_package_name }}
    :nested: full
