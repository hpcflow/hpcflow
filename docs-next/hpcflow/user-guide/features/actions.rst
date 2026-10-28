Actions and scripts
===================

Actions describe commands, scripts, input-file generation and output parsing. They connect a task's parameter data to the external software.

For a Python script action, match the function name to the script filename. Select data exchange options deliberately; direct output commonly returns a dictionary keyed by output parameter names. Keep command-file paths and parameter values in their supported substitution contexts.

See :doc:`../hello-world` for a small task schema and :doc:`../../reference/catalogue`
for built-in component definitions.

The `existing how-to guides <https://hpcflow.github.io/docs/stable/user/how_to/index.html>`_
contain the detailed syntax while it is being migrated.
