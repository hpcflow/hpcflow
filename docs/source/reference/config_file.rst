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

Applications can select settings that resolve to their ``DefaultConfiguration``
value when omitted from an existing config file. This is configured with
``ConfigOptions.defaulted_config_keys`` and requires no config-file rewrite. Explicit
values still take precedence. Other unset settings retain the existing unset behavior.
For caller-specific fallbacks, ``config.get("setting", default=value)`` handles an
absent setting or nested path without changing the configuration file.

The long-term intention is for all settings to use their ``DefaultConfiguration``
values when absent. Extending this behavior to every setting would change the
semantics of existing unset settings, so it is a breaking change and is not being
done implicitly.

The ``move_scratch_to_workflow`` setting also defaults to ``True``, including when
it is absent from an existing configuration file; no config rewrite is required.
When an element iteration finishes (including failure with remaining actions
skipped), retained custom-scratch working directories are moved into the workflow's
normal ``execute`` tree. Set it to ``False`` to leave them on scratch. It can also
be overridden in a workflow template's ``config`` mapping. Moving cannot be
guaranteed after node loss or forcible termination.

Validation schema
~~~~~~~~~~~~~~~~~

The configuration file must conform to the following validation schema. 

.. raw:: html
   :file: _generated/config_validation.html
