Choose execution environments
=============================

An |app| environment describes how to invoke software: setup commands, executable
labels, and supported core counts. Installing |app| does not install every solver
used by a workflow.

Start by examining the environments required by the task schemas in your chosen
workflow. Use an available built-in definition when it matches your machine.
Otherwise, define the environment in a YAML file and register it through
``environment_sources`` in your configuration.

Here is a minimal environment for scripts run by the same Python installation:

.. code-block:: yaml

   - name: python_env
     executables:
       - label: python_script
         instances:
           - command: python "<<script_path>>" <<args>>
             num_cores: 1
             parallel_mode: null

If setup activates another Python environment, install the same |app| version
there as the one used to submit the workflow.

.. note::

   Automatic provisioning is a proposed documentation topic. This prototype
   does not assume that external solvers are provisioned automatically.

See :doc:`../user-guide/features/environments` for environment specifiers.
