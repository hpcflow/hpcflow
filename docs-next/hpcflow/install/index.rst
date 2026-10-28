Install
=======

Install the Python package to get both the command line interface and the Python API.
Use a separate virtual environment so dependencies stay local to this installation.

Python 3.10–3.13 is supported by the checked-out projects.

.. code-block:: console

   python -m venv .venv

Activate it on your platform:

.. tab-set::

   .. tab-item:: Linux / macOS

      .. code-block:: console

         source .venv/bin/activate

   .. tab-item:: Windows PowerShell

      .. code-block:: powershell

         .venv\Scripts\Activate.ps1

Then install the prerelease package (these projects currently use alpha versions):

.. code-block:: console

   python -m pip install --pre hpcflow
   hpcflow --help

**On a cluster:** follow your site's policy for Python environments and install from a
login node with network access. Configure the scheduler before submitting production work.

.. toctree::
   :maxdepth: 2

   configure
   environments
   updating
