Parameters and propagation
==========================

Parameters are typed input and output values. Schema inputs and outputs describe the values a task consumes and produces; propagation controls how values are made available to later tasks.

Declare the parameter rather than passing an undocumented file path whenever later tasks need structured data. Add a parameter value class when the domain data needs constructors, validation or specialized behavior.

See :doc:`../hello-world` for a small task schema and :doc:`../../reference/catalogue`
for built-in component definitions.

The `existing how-to guides <https://docs.matflow.io/stable/user/how_to/index.html>`_
contain the detailed syntax while it is being migrated.
