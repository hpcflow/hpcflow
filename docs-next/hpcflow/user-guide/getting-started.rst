Run a demo workflow
===================

List the workflows shipped with your installed version:

.. code-block:: console

   hpcflow demo-workflow --list

Choose a name from that list. In the commands below, replace ``WORKFLOW.yaml``
with that exact name.

.. code-block:: console

   hpcflow demo-workflow copy WORKFLOW.yaml .
   hpcflow go WORKFLOW.yaml
   hpcflow show -f

Copying the template first gives you a file to inspect and edit.
Check the required environments before submitting; a materials simulation can
require software beyond the Python package.

To see the status legend:

.. code-block:: console

   hpcflow show --legend

To cancel a running workflow:

.. code-block:: console

   hpcflow cancel WORKFLOW_REF

Replace ``WORKFLOW_REF`` with the workflow path or the ID shown by ``hpcflow show``.
Inspect the jobscript output and error files if execution fails; see
:doc:`workflow-structure`. Continue with :doc:`hello-world` to define your own task.
