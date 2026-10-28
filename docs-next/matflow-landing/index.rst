Make your materials workflows reproducible
==========================================

.. rst-class:: hero-kicker

   MatFlow · Materials science

Connect the steps of your research in a workflow you can inspect, rerun and share.
MatFlow brings materials-science components to the hpcflow workflow engine.

.. raw:: html

   <p><a class="sd-btn sd-btn-primary" href="../matflow/install/index.html">Install MatFlow</a>
   <a class="sd-btn sd-btn-outline-primary" href="../matflow/index.html">Read the documentation</a></p>

From microstructure to analysis
-------------------------------

Describe tasks in YAML, reuse domain-specific schemas, and let the engine manage
data dependencies and execution. Keep machine setup separate so that a workflow
can be configured for local or cluster execution.

.. raw:: html

   <div class="workflow-map" role="img" aria-label="Generate a microstructure, simulate its response, and analyse the results">
     <span>Microstructure</span> &rarr; <span>Simulation</span> &rarr; <span>Analysis</span>
   </div>

Explore the built-in examples
-----------------------------

The workflow library includes volume-element generation, DAMASK loading examples
and uncertainty-quantification templates. Each requires its corresponding software
environment; installing MatFlow alone does not install the solvers.

.. raw:: html

   <p><a href="../matflow/workflows/index.html">Explore the workflow library &rarr;</a></p>

Community
---------

Ask questions, report problems or propose a new integration through
`GitHub issues <https://github.com/hpcflow/matflow/issues>`_.
Office hours and a community chat are ideas for the future; no meeting or server
is advertised until one is established.

About and acknowledgements
--------------------------

MatFlow builds on `hpcflow <https://github.com/hpcflow/hpcflow>`_.
The engine handles workflow state and execution; MatFlow contributes the
materials-science layer. Contributor and publication metadata is maintained in
the `repository <https://github.com/hpcflow/matflow>`_.

How to cite
-----------

Use the version-specific citation metadata in
`CITATION.cff <https://github.com/hpcflow/matflow/blob/develop/CITATION.cff>`_.
Record the MatFlow version and cite the scientific tools used by your workflow.

Published case studies and a browser-based trial are planned additions. They are
not presented here as available features.
