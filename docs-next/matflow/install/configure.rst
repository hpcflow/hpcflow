Configure your machine
======================

Find the configuration file:

.. code-block:: console

   matflow manage get-config-path
   matflow open config

The default configuration is intended for local execution. On a cluster, describe
the scheduler and the available resources before running a workflow. Keep machine
details in configuration, so workflow templates can be reused on another system.

Add paths to your environment definitions under ``environment_sources``.
See :doc:`environments` for the difference between installing software and describing it.

Continue with :doc:`../user-guide/configuration` for cluster setup and debugging.
