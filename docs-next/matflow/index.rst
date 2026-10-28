MatFlow
=======

.. rst-class:: hero-kicker

   Computational workflows for materials science

Build materials-science workflows from reusable tasks. Connect microstructure generation, simulation and analysis, with hpcflow managing execution and data dependencies.

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Install
      :link: install/index
      :link-type: doc

      Set up the package and execution environments.

   .. grid-item-card:: Run your first workflow
      :link: user-guide/getting-started
      :link-type: doc

      Copy a demo, submit it, and find the results.

   .. grid-item-card:: Explore workflows
      :link: workflows/index
      :link-type: doc

      Start from a real template and inspect its requirements.

   .. grid-item-card:: Design a workflow
      :link: user-guide/workflow-design
      :link-type: doc

      Bring a command or script into a reusable task schema.


.. raw:: html

   <div class="workflow-map" role="img" aria-label="Define tasks, connect their data, then execute locally or on a cluster">
     <span>Define tasks</span> &rarr; <span>Connect data</span> &rarr; <span>Run &amp; inspect</span>
   </div>

MatFlow adds domain-specific components to the hpcflow engine. For scientific context, community links and citation information, visit the :doc:`project overview <about>`.

.. note::

   This is a separate documentation prototype. The existing published reference
   remains available; pages that still need migration link to it explicitly.

.. toctree::
   :maxdepth: 2

   install/index
   user-guide/index
   workflows/index
   reference/index
   development/index
   about
