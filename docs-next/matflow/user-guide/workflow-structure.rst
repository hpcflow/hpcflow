Find your results
=================

A submitted workflow has its own directory. The template describes what to run;
the workflow directory stores execution state and results.

.. code-block:: text

   workflow_name_YYYY-MM-DD_timestamp/
   |-- artifacts/
   |   |-- workflow.yaml
   |   |-- submissions/
   |   `-- tasks/
   |-- execute/
   |-- metadata/
   `-- parameters/

**artifacts/workflow.yaml** records the submitted template.
**artifacts/submissions/** contains generated jobscripts and their output/error
streams. Start here when a job fails.
**artifacts/tasks/** holds retained task files.
**execute/** contains working directories used by executing tasks.
**metadata/** and **parameters/** store workflow structure and parameter data.

Do not rely on temporary execution files being retained after success. Declare
action ``output_files`` and use the schema or task ``save_files`` policy to
keep files as artifacts. Use the Python API to explore stored parameter values.
