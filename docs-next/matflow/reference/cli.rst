Command line interface
======================

The installed CLI is the authority for the version you are running:

.. code-block:: console

   matflow --help
   matflow go --help
   matflow workflow --help
   matflow demo-workflow --help
   matflow manage --help

.. list-table:: Common tasks
   :header-rows: 1
   :widths: 45 55

   * - Task
     - Command
   * - List built-in examples
     - ``matflow demo-workflow --list``
   * - Submit a template
     - ``matflow go WORKFLOW.yaml``
   * - Show workflow status
     - ``matflow show -f``
   * - Cancel execution
     - ``matflow cancel WORKFLOW_REF``
   * - Find configuration
     - ``matflow manage get-config-path``

For the full generated command listing, see the
`existing CLI reference <https://docs.matflow.io/stable/reference/cli.html>`_.
