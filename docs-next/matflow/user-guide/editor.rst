Edit YAML in VS Code
====================

Install the `YAML extension by Red Hat <https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml>`_
for syntax highlighting and YAML diagnostics. Use spaces for indentation.
The command **Toggle Render Whitespace** helps expose accidental tabs.

Keep a small workflow open while editing a task schema so you can check both
the reusable definition and the values passed to it. Quoting a value is useful
when a colon, hash, or substitution marker might otherwise be ambiguous.

.. tip::

   YAML syntax checking does not prove that a workflow can execute. Environments,
   solver installations and task-schema validation still need to be checked.
   A published workflow JSON Schema and its VS Code association remain migration
   work; this prototype does not point the editor at an invented schema URL.
