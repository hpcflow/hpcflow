Workflow templates how-tos
--------------------------

.. jinja:: first_ctx

    Sharing inputs across tasks
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Define ``inputs`` at the workflow-template level to make values available to any
    task requiring the corresponding parameter:

    .. code-block:: yaml

        name: shared_inputs
        inputs:
          p1: 10
        tasks:
          - schema: my_first_schema
          - schema: my_second_schema
            inputs:
              p1: 20

    The first task can use the shared value ``10``; the second uses its local value
    ``20``. Local task inputs (including sequences) and sources from preceding tasks
    take precedence over workflow inputs. Workflow inputs take precedence over schema
    defaults. Inputs that a task does not require are ignored.

    As with task inputs, workflow inputs accept either a parameter-to-value mapping
    or a list of input-value specifications. Parameter labels and sub-parameter paths
    are supported. A list value remains a single input value, not a value sequence.

    To explicitly choose a workflow input instead of another available source, use:

    .. code-block:: yaml

        input_sources:
          p1: workflow

    In Python, supply ``inputs`` to
    :class:`{{ app_package_name }}.app.WorkflowTemplate` or
    :meth:`{{ app_package_name }}.app.Workflow.from_template_data`, and use
    ``InputSource.workflow()`` to explicitly select a shared input. Shared inputs
    remain available after reloading a workflow, including for tasks added later.

    Referencing workflow inputs in template values
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    YAML and JSON templates can reference workflow-level inputs using
    ``<<input:NAME>>``. References are resolved after template variables and
    ``updates`` have been applied, but before tasks and their descriptors are
    constructed. They always refer to workflow-level values, even if a task
    overrides an input of the same name.

    Use an explicit ``<<calc(EXPRESSION)>>`` marker for arithmetic:

    .. code-block:: yaml

        name: shared_calculations
        inputs:
          p1: 2
          p2: 3
          p3: "<<calc(<<input:p1>> * <<input:p2>>)>>"
        tasks:
          - schema: my_schema
            inputs:
              p4: "<<input:p3>>"
            repeats:
              number: "<<input:p3>>"
              nesting_order: 2
            resources:
              any:
                num_cores: "<<calc(<<input:p1>> + 1)>>"

    A reference or calculation occupying an entire value preserves its type:
    the example above supplies the integer ``6`` for both ``p4`` and the repeat
    count. Lists, mappings, booleans and null values can also be referenced.
    Within surrounding text, references and calculations are interpolated as
    strings, for example ``"run_<<input:p1>>"`` becomes ``"run_2"``.
    Bare arithmetic is not evaluated: ``"<<input:p1>> * <<input:p2>>"`` becomes
    the string ``"2 * 3"``.

    Calculations accept numeric literals, input references, parentheses,
    unary ``+`` and ``-``, and the operators ``+``, ``-``, ``*``, ``/``, ``//``,
    ``%`` and ``**``. Division with ``/`` produces a float; use ``//`` when a
    descriptor requires an integer. Boolean and non-numeric operands are rejected.
    Functions, attribute access and arbitrary Python code are not supported.
    Expression size, integer size and exponent limits guard against excessive
    calculations.

    References also support labels and nested data paths, such as
    ``<<input:p1[label].settings.count>>`` or ``<<input:p1.0>>`` for a list member.
    Workflow inputs may reference other workflow inputs, including inputs declared
    later. Input-value factories that run during template loading (for example
    ``p1::from_linear_space``) can also supply references; their values are generated
    once and reused by the workflow input and its references.
    Demo-data file markers (``<<demo_data_file:NAME>>``) in workflow inputs are
    resolved to local file paths before substitution, including in nested values.
    For example, a workflow input ``material_parameters: <<demo_data_file:posterior.txt>>``
    can supply ``file_path: <<input:material_parameters>>`` in a sequence's
    ``values::from_load_txt`` arguments.
    Missing inputs, cycles, invalid calculations and references to deferred
    values produce errors when loading the template. Task inputs and outputs,
    resource references, and deferred value-class methods cannot supply values for
    this build-time substitution.

    Plain ``<<input:NAME>>`` references in schema-output ``multiplicity`` retain
    their existing meaning: they refer to each element's task input, including
    any task-level override. An explicit ``<<calc(...)>>`` there instead evaluates
    workflow-level inputs when loading the template.

    Recording creation information
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    A workflow's creation information (``Workflow.creation_info``) also records
    the string variables passed on creation (including any ``[default=...]`` values
    used) under ``"variables"``, the result of each ``<<calc(...)>>`` expression
    (keyed by expression text) under ``"calculations"``, and, if the workflow was
    created via the CLI, the command line used under ``"command_line"`` (otherwise
    ``None``). For workflows created before these were recorded, ``"variables"`` and
    ``"calculations"`` are empty and ``"command_line"`` is ``None``.

    Loading workflow templates
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    Workflow template YAML files can be loaded from remote sources by providing :func:`{{ app_package_name }}.app.WorkflowTemplate.from_YAML_file` with an fsspec URL. For example, to load a workflow template YAML file from a GitHub repository, use the following format:

    .. code-block:: python

        import {{ app_module }} as {{ app_docs_import_conv }}

        wkt = {{ app_docs_import_conv }}.WorkflowTemplate.from_YAML_file()

    Updating values in a workflow template file
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    When constructing a workflow template from a (YAML or JSON) file or string, we can provide updates to the data of the template which are applied before the template is loaded. In particular, we can provide updates to the template-level resources block (for the ``any`` scope) like this:

    .. tab-set::

        .. tab-item:: CLI

            For making the a demo workflow with modified resources:

            .. code-block:: console
                
                {{ app_package_name }} demo-workflow make DEMO_WORKFLOW_NAME --resource random_seed 1234

            For making and submitting a demo workflow with modified resources:

            .. code-block:: console
                
                {{ app_package_name }} demo-workflow go DEMO_WORKFLOW_NAME --resource random_seed 1234

        .. tab-item:: Python API

            .. code-block:: python
                
                import {{ app_module }} as {{ app_docs_import_conv }}

                # for making a demo workflow with modified resources:
                wk = {{ app_docs_import_conv }}.make_and_submit_demo_workflow(
                    "DEMO_WORKFLOW_NAME", 
                    resources={"random_seed": 1234},
                )

                # for making and submitting a demo workflow with modified resources:
                wk = {{ app_docs_import_conv }}.make_and_submit_demo_workflow(
                    "DEMO_WORKFLOW_NAME", 
                    resources={"random_seed": 1234},
                )

    This also works for non-demo workflow templates as well. Workflow-specific configuration items ``config`` can also be updates in a similar way.

    We can also update arbitrary data within the template in the following way, using a mapping of "paths" within the template data to the new values that should be applied at those "paths":

    .. code-block:: python
        
        import {{ app_module }} as {{ app_docs_import_conv }}

        # for making a demo workflow with a modified input value for the first task
        wkt = {{ app_docs_import_conv }}.make_and_submit_demo_workflow(
            "DEMO_WORKFLOW_NAME", 
            updates={("tasks", 0, "inputs"): 99},
        )
