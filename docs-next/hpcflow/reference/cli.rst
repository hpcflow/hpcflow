Command line interface
======================

The installed CLI is the authority for the version you are running:

.. code-block:: console

   hpcflow --help
   hpcflow go --help
   hpcflow workflow --help
   hpcflow demo-workflow --help
   hpcflow manage --help

.. list-table:: Common tasks
   :header-rows: 1
   :widths: 45 55

   * - Task
     - Command
   * - List built-in examples
     - ``hpcflow demo-workflow --list``
   * - Submit a template
     - ``hpcflow go WORKFLOW.yaml``
   * - Show workflow status
     - ``hpcflow show -f``
   * - Cancel execution
     - ``hpcflow cancel WORKFLOW_REF``
   * - Find configuration
     - ``hpcflow manage get-config-path``

For the full generated command listing, see the
`existing CLI reference <https://hpcflow.github.io/docs/stable/reference/cli.html>`_.
