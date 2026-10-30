.. jinja:: first_ctx

  Command-line interface
  ======================

  CLI reference documentation.

  The ``go``, ``demo-workflow go``, and ``workflow WORKFLOW_REF submit`` commands
  accept ``--ret-id`` to print the local workflow ID after submission. Use
  ``--quiet --ret-id`` to suppress submission messages and print only the ID.
  If multiple submissions submit jobscripts, each local ID is printed on its own
  line. This option requires ``--add-to-known`` (the default).

  .. click:: {{ app_package_name }}.cli:cli
    :prog: {{ app_package_name }}
    :nested: full
