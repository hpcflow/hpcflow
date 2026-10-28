A first local workflow
======================

**Theme:** Getting started

A small, OS-agnostic task using a built-in test schema. Use this to check basic submission before integrating another tool.

Run the example
---------------

Check the environments required by each schema in the YAML. Confirm the installed
version lists this demo before using its name:

.. code-block:: console

   hpcflow demo-workflow --list
   hpcflow demo-workflow copy workflow_1.yaml .
   hpcflow go workflow_1.yaml
   hpcflow show -f

The gallery is a snapshot of the local source tree, so a released package may have
different demos or schema requirements. A source snapshot is not an execution test.
Review input paths and workload size before submitting it.

Inspect the template
--------------------

:download:`Download YAML <../examples/workflow_1.yaml>` · `Upstream source <https://github.com/hpcflow/hpcflow/blob/7aa0b37c444a3850c1d30e7d1fccc3f2696aeb66/hpcflow/data/workflows/workflow_1.yaml>`_

The downloadable file preserves the local checkout, including any uncommitted edits;
the upstream link points to the recorded commit.

.. literalinclude:: ../examples/workflow_1.yaml
   :language: yaml

Continue with :doc:`../user-guide/workflow-structure` to locate results.
