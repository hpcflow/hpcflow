Generate a volume element
=========================

**Theme:** Microstructure

Create a Dream.3D pipeline from phase statistics and generate a volume element. This template also illustrates texture inputs. It requires the environments for Dream.3D and the selected visualisation tasks.

Run the example
---------------

Check the environments required by each schema in the YAML. Confirm the installed
version lists this demo before using its name:

.. code-block:: console

   matflow demo-workflow --list
   matflow demo-workflow copy generate_volume_element_from_statistics.yaml .
   matflow go generate_volume_element_from_statistics.yaml
   matflow show -f

The gallery is a snapshot of the local source tree, so a released package may have
different demos or schema requirements. A source snapshot is not an execution test.
Review input paths and workload size before submitting it.

Inspect the template
--------------------

:download:`Download YAML <../examples/generate_volume_element_from_statistics.yaml>` · `Upstream source <https://github.com/hpcflow/matflow/blob/1eec25503f153e6779d9253b660816bd7204d8d0/matflow/data/workflows/generate_volume_element_from_statistics.yaml>`_

The downloadable file preserves the local checkout, including any uncommitted edits;
the upstream link points to the recorded commit.

.. literalinclude:: ../examples/generate_volume_element_from_statistics.yaml
   :language: yaml

Continue with :doc:`../user-guide/workflow-structure` to locate results.
