Integrate another tool
======================

First make the software run outside |app| with a small, reproducible input.
Record the command, required files, software version and expected outputs.

Then describe the executable in an environment. Write a task schema that declares
its inputs, outputs and actions. Use command files for named input/output artifacts,
and parsers when results need to become typed parameters for subsequent tasks.

Test one task locally before adding sequences, parallel execution or a scheduler.
Decide which files must be retained and set a ``save_files`` policy explicitly.
Include a small example that another person can run without your working directory.

See :doc:`../development/contributing` to contribute the integration.
