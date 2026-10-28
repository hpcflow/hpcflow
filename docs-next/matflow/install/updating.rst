Update and alternative installs
===============================

Update within the virtual environment:

.. code-block:: console

   python -m pip install --upgrade --pre matflow

Read the release notes before changing the version used by a research project.
Record package and solver versions alongside the workflow, and keep matching |app|
versions in submission and execution environments.

Binary distributions, when available, are listed on the project's
`GitHub releases <https://github.com/hpcflow/matflow/releases>`_.
Choose the correct platform and read the release-specific instructions.
The Python package is the recommended route in the existing documentation.

For an editable checkout, see :doc:`../development/contributing`.
