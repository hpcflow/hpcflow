Contribute a change
===================

For hpcflow work, clone the engine repository. For MatFlow work, keep the two
repositories side by side; the current MatFlow checkout uses a local editable
dependency on ``../hpcflow``.

Install the development environment using the repository's Poetry configuration.
Run the checks relevant to your change before opening a pull request.
Include a small workflow when adding a task schema, and describe required software.

Use the commit conventions in ``pyproject.toml``: for example,
``docs: clarify environment setup``, ``fix: correct input selection``, or
``build(deps): update a dependency``.

Documentation changes
---------------------

The new sources are under ``docs-next/``; the original ``docs/`` tree remains
in place. Follow the build instructions in the prototype README. Build with warnings
treated as errors, and inspect both sites when changing shared styles or navigation.
