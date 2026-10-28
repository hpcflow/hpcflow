Find template components
========================

Template components are reusable definitions: parameters, task schemas, command
files and environments. A workflow selects schemas, supplies inputs and requests
resources; a schema describes the computation.

For each component, check:

* Which task consumes or produces it?
* Which software and executable labels are required?
* Which files need to exist, and which are retained?
* Which parameter types and constructors does it expect?

The :doc:`../reference/catalogue` provides source links for the built-in definitions.
For now the full parameter constructor and API documentation remains on the existing
reference site. The migration plan calls for generated input/output relationships
and constructor examples, so those details have a single source of truth.
