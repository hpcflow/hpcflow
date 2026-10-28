How the projects fit together
=============================

hpcflow owns the workflow engine: tasks, parameters, execution, schedulers and
configuration. MatFlow is a downstream application that adds materials-science
parameters, task schemas, scripts and workflow templates.

.. raw:: html

   <div class="workflow-map" role="img" aria-label="MatFlow uses hpcflow, which invokes shells, schedulers and scientific software">
     <span>MatFlow<br>Materials workflows</span> &rarr;
     <span>hpcflow<br>Workflow engine</span> &rarr;
     <span>Shells &amp; schedulers<br>Scientific software</span>
   </div>

The application layer connects the shared engine to application-specific component
sources and parameter value classes. Keeping solver commands in environments allows
the same scientific schema to run on different machines.

A new downstream application should define its domain components and application
configuration while reusing engine behavior. Use MatFlow's application setup as
a working example, rather than copying the scheduler implementation.

Container execution is an engine concern. Host integration and isolated execution
need distinct documentation covering filesystem visibility and launch commands;
that detail remains in the existing engineering documentation until migrated.
