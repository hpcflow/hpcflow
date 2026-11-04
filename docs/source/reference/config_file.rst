Configuration file
==================

Configurable settings that can be set within the configuration file are listed here.

.. _default_config:

Default config file
~~~~~~~~~~~~~~~~~~~

.. literalinclude:: _generated/default_config.yaml
   :language: yaml

The ``delete_execute_dir`` setting defaults to ``True``. Set it to ``False`` to
retain successful execution directories for inspection. This setting can also be
overridden in a workflow template's ``config`` mapping.

The ``move_scratch_to_workflow`` setting also defaults to ``True``. When an element
iteration finishes (including failure with remaining actions skipped), retained
custom-scratch working directories are moved into the workflow's normal ``execute``
tree. Set it to ``False`` to leave them on scratch. It can also be overridden in a
workflow template's ``config`` mapping. Moving cannot be guaranteed after node loss
or forcible termination.

Validation schema
~~~~~~~~~~~~~~~~~

The configuration file must conform to the following validation schema. 

.. raw:: html
   :file: _generated/config_validation.html
