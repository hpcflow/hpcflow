Crystal plasticity with DAMASK
==============================

**Theme:** Crystal plasticity

Inspect an aluminium loading example built from DAMASK-related task schemas. Configure the matching DAMASK and supporting Python environments before submission.

Run the example
---------------

Check the environments required by each schema in the YAML. Confirm the installed
version lists this demo before using its name:

.. code-block:: console

   matflow demo-workflow --list
   matflow demo-workflow copy tension_DAMASK_Al.yaml .
   matflow go tension_DAMASK_Al.yaml
   matflow show -f

The gallery is a snapshot of the local source tree, so a released package may have
different demos or schema requirements. A source snapshot is not an execution test.
Review input paths and workload size before submitting it.

Inspect the template
--------------------

:download:`Download YAML <../examples/tension_DAMASK_Al.yaml>` · `Upstream source <https://github.com/hpcflow/matflow/blob/1eec25503f153e6779d9253b660816bd7204d8d0/matflow/data/workflows/tension_DAMASK_Al.yaml>`_

The downloadable file preserves the local checkout, including any uncommitted edits;
the upstream link points to the recorded commit.

.. literalinclude:: ../examples/tension_DAMASK_Al.yaml
   :language: yaml

Continue with :doc:`../user-guide/workflow-structure` to locate results.
