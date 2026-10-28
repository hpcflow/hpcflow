Command files and artifacts
===========================

Command files name files that an action reads or produces. A parser can turn file contents into output parameters; an artifact policy controls which files remain available after execution.

Declare output files and set save_files for files you need to keep. Test matching rules carefully when a tool creates several files. A name in the working directory is not by itself a guarantee of retention.

See :doc:`../hello-world` for a small task schema and :doc:`../../reference/catalogue`
for built-in component definitions.

The `existing how-to guides <https://hpcflow.github.io/docs/stable/user/how_to/index.html>`_
contain the detailed syntax while it is being migrated.
