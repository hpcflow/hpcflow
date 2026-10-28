Sample a random texture
=======================

**Theme:** Microstructure

Start from the shipped random-texture example and inspect its orientation inputs and task schema requirements before running it.

Run the example
---------------

Check the environments required by each schema in the YAML. Confirm the installed
version lists this demo before using its name:

.. code-block:: console

   matflow demo-workflow --list
   matflow demo-workflow copy sample_texture_random.yaml .
   matflow go sample_texture_random.yaml
   matflow show -f

The gallery is a snapshot of the local source tree, so a released package may have
different demos or schema requirements. A source snapshot is not an execution test.
Review input paths and workload size before submitting it.

Inspect the template
--------------------

:download:`Download YAML <../examples/sample_texture_random.yaml>` · `Upstream source <https://github.com/hpcflow/matflow/blob/1eec25503f153e6779d9253b660816bd7204d8d0/matflow/data/workflows/sample_texture_random.yaml>`_

The downloadable file preserves the local checkout, including any uncommitted edits;
the upstream link points to the recorded commit.

.. literalinclude:: ../examples/sample_texture_random.yaml
   :language: yaml

Continue with :doc:`../user-guide/workflow-structure` to locate results.
