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

Validation schema
~~~~~~~~~~~~~~~~~

The configuration file must conform to the following validation schema. 

.. raw:: html
   :file: _generated/config_validation.html
