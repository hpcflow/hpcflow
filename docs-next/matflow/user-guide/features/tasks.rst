Tasks and task schemas
======================

A task schema defines a reusable operation; a task selects it and supplies values. Keep machine-specific launch details in environments, and put scientific defaults in schemas only when they are valid for every use.

Start with one task and inspect its inputs and outputs before chaining it to another task. An output parameter can supply an input to a later task. Use explicit input sources when automatic selection would be ambiguous.

See :doc:`../hello-world` for a small task schema and :doc:`../../reference/catalogue`
for built-in component definitions.

The `existing how-to guides <https://docs.matflow.io/stable/user/how_to/index.html>`_
contain the detailed syntax while it is being migrated.
