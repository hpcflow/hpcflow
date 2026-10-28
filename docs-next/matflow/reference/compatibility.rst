Compatibility and execution
===========================

The local project metadata declares Python ``>=3.10,<3.14``.

.. list-table::
   :header-rows: 1

   * - Layer
     - What to verify
   * - Python package
     - Python version and the project's dependency constraints.
   * - Downstream application
     - The hpcflow version required by the MatFlow release or checkout.
   * - Shell
     - Whether the action uses Bash, PowerShell or another supported shell.
   * - Scheduler
     - Site-specific resource limits and jobscript directives.
   * - Solver
     - The version and executable expected by the chosen task schema.

This is a checklist, not a tested solver-version matrix. Record verified combinations
before publishing a compatibility claim.

For environment variables set during execution, consult the source and generated
reference for the installed version; a complete variable inventory still needs
to be migrated.
