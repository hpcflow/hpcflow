Python API
==========

The Python API supports creating and inspecting workflows from Python.
Import the package from the same environment used to submit your workflows:

.. code-block:: python

   import hpcflow.app as app

Keep templates and small input files alongside your analysis so that another user
can reproduce how the workflow was created. Prefer public API objects over editing
the workflow's metadata or parameter store directly.

The full signature-level reference remains in the
`existing Python API documentation <https://hpcflow.github.io/docs/stable/reference/api.html>`_.
This prototype intentionally builds without importing the application or executing
notebooks. Moving generated API pages is tracked in the migration notes.
