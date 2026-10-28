Variables and qualified names
=============================

Different parts of a template interpret different markers. Use a marker only in
a field that supports it.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Form
     - Purpose
   * - ``<<parameter:name>>``
     - Refer to a parameter in a supported action field.
   * - ``<<executable:label>>``
     - Use an executable from the selected environment.
   * - ``<<file:label>>``
     - Refer to a declared command file.
   * - ``<<script:path/to/script.py>>``
     - Locate a script for a script action.

A script action is different from a shell command: its data exchange options
control how inputs and outputs are passed.

Build-time expressions
----------------------

The current hpcflow source also supports ``<<input:name>>`` references and
``<<calc(...)>>`` arithmetic in supported template input values.
These are resolved while constructing the workflow. Check the detailed
`workflow template guide <https://hpcflow.github.io/docs/stable/user/how_to/workflow_templates.html>`_
for supported contexts and restrictions.

Double-colon names
------------------

In supported input definitions, a double colon selects a constructor method:
for example, ``p1::from_linear_space``. Sequence definitions also support
factory keys such as ``values::from_range``; the mapping under that key supplies
the method's arguments. Parameter value classes can provide domain-specific
constructors in these contexts.

Use the constructor supported by the particular parameter or sequence type.
The double colon is not a general-purpose separator for arbitrary template fields.
