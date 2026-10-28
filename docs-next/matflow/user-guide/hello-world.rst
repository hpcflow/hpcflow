Write a task schema
===================

This example keeps both a reusable task schema and its use in one file.
The command ``echo`` works in the usual Bash and PowerShell setups; choose a
local shell configuration for this first example.

Save this as ``hello.yaml``:

.. literalinclude:: ../examples/hello.yaml
   :language: yaml

The ``objective`` names the schema. Its action declares a command, and the
task under ``tasks`` selects that schema. There are no external solver dependencies.

Run it and check the workflow status:

.. code-block:: console

   matflow go hello.yaml
   matflow show -f

Look in the generated workflow's submission artifacts for standard output containing
``Hello from a workflow``.

Next, add typed inputs and outputs when the task needs to pass data.
Use :doc:`features/actions` for scripts, :doc:`features/parameters` for data,
and :doc:`../reference/syntax` for substitution syntax.
