Task schemas
------------

Action command strings
======================

An action command string, ``COMMAND_STRING``, determines what the action should actually run, and is specified within a task schema definition like this (where schema inputs/outputs have been omitted for brevity):

.. tab-set::

    .. tab-item:: YAML

        .. code-block:: yaml

            objective: t1
            inputs: ...
            outputs: ...
            actions:
              - environments:
                  any: python_env
                commands:
                  - command: COMMAND_STRING

    .. tab-item:: Python

        .. code-block:: python

            ts1 = hf.TaskSchema(
                objective="t1",
                inputs=...,
                outputs=...,
                actions=[
                    hf.Action(
                        environments=[hf.ActionEnvironment("python_env", scope="any")],
                        commands=[hf.Command("COMMAND_STRING")],
                    )
                ]
            )

Action command strings may contain the following variables:

- ``<<executable:EXE_NAME>>`` where ``EXE_NAME`` is the label of an environment executable that is defined within the action's environment.
- ``<<parameter:PARAM_NAME>>`` where ``PARAM_NAME`` is an input or output parameter type of the task schema that contains the action.
- ``<<args>>``, which corresponds to the list of items in the ``args`` attribute of the command, and will be formatted as a space-separated list.
- ``<<VAR_NAME:VAR_VALUE>>`` where ``VAR_NAME`` and ``VAR_VALUE`` correspond to items in the ``variables`` attribute of the command. This is used internally (when running scripts, for example, to pass information about the script name and path), but is typically not used when defining commands.

Parameter flow within task schemas
==================================

In addition to an actual implementation of the task, task schemas also define a mapping between input and output types. Within a task schema, multiple actions can use those inputs and outputs in different ways. For a simple example, a task schema with one input, ``p1``, and one output, ``p2``, might include a single action that consumes the schema input and produces the schema output:

.. tab-set::

    .. tab-item:: YAML

        .. code-block:: yaml
        
            objective: t1
            inputs:
              - parameter: p1
            outputs:
              - parameter: p2
            actions:
             - commands:
                 - command: echo $((<<parameter:p1>> + 1))
                   stdout: <<parameter:p2>>

    .. tab-item:: Python

        .. code-block:: python

            ts1 = hf.TaskSchema(
                objective="t1",
                inputs=[hf.SchemaInput("p1")],
                outputs=[hf.SchemaInput("p2")],
                actions=[
                    hf.Action(
                        commands=[
                            hf.Command(
                                command="echo $((<<parameter:p1>> + 1))",
                                stdout="<<parameter:p2>>"
                            )
                        ],
                    ),
                ],
            )

It is useful to understand the difference between task schema inputs and outputs, and action-level inputs and outputs. Task schema inputs and outputs are those parameters defined in the ``inputs`` and ``outputs`` attributes of the task schema. In particular, these are represented by the ``SchemaInput`` and ``SchemaOutput`` objects. ``SchemaInput`` objects, in their most basic form, simply point to a parameter type, e.g. ``p1``. However, they may also include other information, such as a default value, and whether the parameter can be used in multiple labelled forms, which is a useful facility when inputs of the same type that originate from different tasks (or element sets) should be distinguished.

As checked during task schema validation, it's a requirement that the schema-level inputs and outputs are compatible with the actions of the that task schema. This means that the constituent actions of the task schema consume and produce parameters in a way that is commensurate with their parent task schema. For example, in the simple case of a task schema that has a single action with a command string ``echo <<parameter:p1>>``, the task schema must include an schema input of the ``p1`` parameter type. Conversely, if the task schema includes an input ``p1``, then it must be consumed by one of the task schema's actions.

But what constitutes action-level inputs (or consumers) and outputs (or producers)? The simplest examples to demonstrate this are those seen above, where we define a command string that includes references to an input parameter (e.g. ``command="echo $((<<parameter:p1>> + 1))"``), and we use the ``stdout`` command attribute to map the shell standard output from the command string back to an output parameter (e.g. ``stdout="<<parameter:p2>>"``). So in this case the action level inputs and outputs are ``p1`` and ``p2`` respectively. However, action level inputs and outputs can also be defined from scripts. Currently, the action-level inputs and outputs for script actions are simply set to those from the task schema, but this will be refined in future to improve validation (since a script may not require all inputs of a task schema, and/or it might not generate all task schema outputs).

.. note::

   Don't worry about getting this wrong! |app_name| will tell you if you are trying to define a task schema whose actions are not compatible with the schema's inputs and outputs.

It's important to note that in the examples above, we use the substitution syntax ``<<parameter:INPUT_OR_OUTPUT_TYPE>>`` within the command string and the standard output string (``stdout``). We use the term ``parameter`` because we can reference both inputs and outputs within actions. We do this in the following example, where the task schema input ``p1`` is used in the first action to generate the task schema output ``p2``, but we additionally include a second action which uses (as an action-level input) the task schema output ``p2``:

.. tab-set::

    .. tab-item:: YAML

        .. code-block:: yaml

            objective: t1
            inputs:
            - parameter: p1
            outputs:
            - parameter: p2
            actions:
            - commands:
                - command: echo $((<<parameter:p1>> + 1))
                  stdout: <<parameter:p2>>
            - commands:
                - command: echo <<parameter:p2>> # note: schema output `p2` used as an action input

    .. tab-item:: Python

        .. code-block:: python

            ts1 = hf.TaskSchema(
                objective="t1",
                inputs=[hf.SchemaInput("p1")],
                outputs=[hf.SchemaInput("p2")],
                actions=[
                    hf.Action(
                        commands=[
                            hf.Command(
                                command="echo <<parameter:p1>>",
                                stdout="<<parameter:p2>>",
                            )
                        ]
                    ),
                    hf.Action(  # note: schema output `p2` used as an action input
                        commands=[
                            hf.Command(command="echo <<parameter:p2>>")
                        ]
                    ),
                ],
            )

Additionally, an action may output a parameter that is *not* a task schema output, as long as it is used as an input in a downstream action within the same task schema, as in this example, where the task schema converts its input ``p1`` to its output ``p3`` via an intermediate parameter ``p2``, which is an *output* of the first action, and an *input* of the second action:

.. tab-set::

    .. tab-item:: YAML

        .. code-block:: yaml

            objective: t1
            inputs:
            - parameter: p1
            outputs:
            - parameter: p3
            actions:
            - commands:
                - command: echo $((<<parameter:p1>> + 1))
                  stdout: <<parameter:p2>> # note: action output `p2` is not a schema-level output
            - commands:
                - command: echo $((<<parameter:p2>> + 1))
                  stdout: <<parameter:p3>>

    .. tab-item:: Python

        .. code-block:: python

            ts1 = hf.TaskSchema(
                objective="t1",
                inputs=[hf.SchemaInput("p1")],
                outputs=[hf.SchemaInput("p3")],
                actions=[
                    hf.Action(
                        commands=[
                            hf.Command(
                                command="echo $((<<parameter:p1>> + 1))",
                                stdout="<<parameter:p2>>",  # note: action output `p2` is not a schema-level output
                            )
                        ]
                    ),
                    hf.Action(
                        commands=[
                            hf.Command(
                                command="echo $((<<parameter:p2>> + 1))",
                                stdout="<<parameter:p3>>",
                            )
                        ]
                    ),
                ],
            )

Output multiplicity
~~~~~~~~~~~~~~~~~~~

``SchemaOutput.multiplicity`` defaults to ``None``, which preserves the whole
output value as one candidate input per upstream element, even when the value
itself is a list. A positive integer instead declares how many output members
**each upstream element** supplies as possible input values to downstream tasks.
For example:

.. code-block:: python

    hf.SchemaOutput("p2", multiplicity=3)

The equivalent YAML task-schema output is:

.. code-block:: yaml

    outputs:
      - parameter: p2
        multiplicity: 3

Each producing element must write a list, tuple, or NumPy array of length three
as its whole ``p2`` output. Downstream inputs then reference individual members,
in element order followed by member order. Thus two upstream elements producing
``[10, 11, 12]`` and ``[20, 21, 22]`` supply six candidate input values, not
three upstream elements or repeated copies of either whole list. Existing input
source selection, labels, grouping and nesting still apply; selecting one
upstream element selects all its members, and grouping collects the selected
members together.

Every positive multiplicity extracts members, including ``1``: an output
``[value]`` supplies ``value`` to a downstream task, not the singleton list.
To supply a list as a single member with ``multiplicity=1``, write a nested list
such as ``[[10, 11, 12]]``. To propagate a list unchanged without member extraction,
omit ``multiplicity`` or set it to ``None`` (``null`` in YAML).
Zero, negative integers, booleans, and non-integer values are not supported.
The declared length is checked when a downstream member is read.

The multiplicity can also be taken from a schema input, resolved separately for
each producing element:

.. code-block:: yaml

    inputs:
      - parameter: member_count
    outputs:
      - parameter: p2
        multiplicity: <<input:member_count>>

The referenced input must be set to ``None`` (``null`` in YAML) or a positive integer
when workflow elements are built. These values have the same meaning as a directly
declared multiplicity: ``None`` preserves the whole output, and a positive integer
extracts that many members. This allows locally supplied values and sequences to
mix whole outputs and member counts for different elements. An unset input or a
value that will only become available after workflow execution cannot be used to
determine multiplicity.
For a positive resolved multiplicity, the producer must write a sequence of that
length, including a singleton sequence when the input is ``1``.

With the Zarr store, all arrays from one output parameter are stored in indexed
run records, whether or not the output has positive multiplicity. A record with
member index ``-1`` represents an ordinary whole-value output; a positive
multiplicity output has one record per member. Each record contains the enabled
array sub-parameters for that value, including masked-array data and masks. For
example, the stress arrays for every step of one ``load_case`` are indexed
together. Members with different numbers of steps remain sparse: a member without
a second step has no ``steps.1.stress`` entry. Array shapes are preserved without
padding. This also applies to an explicit multiplicity of ``1``. If the whole
output value is a single array, rather than a sequence of members, it is stored as
an ordinary whole-value record and members are selected from it when read.

During execution, records are bundled by output parameter and execution shard.
Sequential runs assigned to the same shard share its archive, while concurrently
writable runs use separate shards. Shards are grouped by submission index, so
separate submissions that run at the same time also write separate archives. The
number of live files therefore follows writable execution shards rather than
multiplying by the number of runs, array sub-parameters, steps, or members.
Consolidation merges the shard archives into one indexed archive per output
parameter. Reads from a zipped workflow use the same partial-read path: only the
index and selected array entries are read from the outer zip.

Reading a downstream member from a sequence-valued Zarr output decodes only that
member, rather than reconstructing every member of the producing output first.
The reader reuses the producing action-run context across array sub-parameters.
Bulk type decoders run once per parameter or selected member, not once per array
in the type lookup table. Whole-output reads still return the complete value.
These read optimizations also apply to existing saved workflows. Existing per-path
shards and indexed per-path archives remain readable through the legacy per-path
reader and are not converted to run records during consolidation. Existing
version-1 member archives are migrated to the current run-record format when
consolidated.

Each run-record archive contains a versioned ``format.json``, a binary
``index.npz`` sorted by run ID and member index, and a ``paths.json`` table of
logical array paths. Arrays larger than 64 KiB have their own independently
Zstandard-compressed ``.npy`` entry, so reading one path does not decompress its
sibling arrays. Smaller arrays in one record share a compressed NPZ group to avoid
the ZIP-entry overhead of many tiny arrays. Consolidation copies these compressed
entries without recompressing them. Reads locate records by run ID and member
index, then use the path-presence mask to open only the requested array entries.
Different member shapes and ragged steps remain independent.

Workflow ZIPs store nested array archives without outer compression, allowing
direct range reads, including on Python 3.10--3.12. Externally recompressing the
outer ZIP can make seeking slower.

Existing padded and shape-group ``consolidated.npz.zst`` archives remain readable
through the legacy reader. Consolidation migrates them to the indexed per-path
archive format, even without new shards, trimming their padding. Older hpcflow
versions cannot read the new indexed archives.
Array archives store task, element, iteration, action, and run IDs and indices as
``uint32`` (0 to 4,294,967,295). Writes reject values outside this range rather
than wrapping them. Existing signed metadata remains readable and is converted
when archives are merged. Member indices remain signed to preserve the ``-1``
whole-value marker.
Consolidation makes two passes over its sources. The first collects metadata,
original shapes, and dtypes; the second copies entries one source at a time into
shape buffers with a shared 32 MiB budget. Full buffers and evicted partial
buffers are written directly as chunks. An entry exceeding 16 MiB is written on
its own after flushing the shape buffers. Metadata stays in RAM, but complete
source collections and destination groups are never materialized there.
Source metadata, shapes, and dtypes are checked again in the second pass to
detect changes during consolidation.

For live and legacy ``.npz.zst`` sources, the decompressed NPZ spills to a unique subdirectory of
``WORKFLOW/parameters/array_outputs/.tmp/`` above 8 MiB. Entries in standard uncompressed C-order NPY
members are then read individually, trimming existing padding. This needs
temporary disk space up to the largest decompressed source NPZ and decompresses
each source twice. Externally compressed or Fortran-order legacy NPZs use a
compatibility fallback that can load one source array into RAM. Indexed sources
use their existing partial-read path. Source ZIP checksums and output chunks are
verified without loading the complete standard array into RAM.
To use an existing scratch directory instead of the workflow-adjacent default,
pass ``temp_dir="/scratch/my-job"`` to ``Workflow.consolidate_array_outputs`` or
``Workflow.zip``. The corresponding CLI commands accept
``--temp-dir /scratch/my-job``. A unique subdirectory is created under that
directory for each output and removed afterward, including on failure. The
default ``.tmp`` parent may remain empty so concurrent operations can share it
safely. This controls decompressed source buffers in both passes; small buffers
remain in RAM. Temporary replacement archives stay beside
the destination archive to preserve atomic replacement across filesystems.
Consolidation writes a temporary replacement archive and verifies every chunk
before replacing the previous archive and removing the source shards.
Indexed reads need no full-size temporary decompressed file. Legacy reads retain
the temporary-file fallback above 8 MiB of decompressed NPZ data. Passing
``consolidate=False`` to ``Workflow.zip`` (or ``--no-consolidate`` on the CLI)
preserves existing archives instead.

``Workflow.get_array_output`` and ``WorkflowTask.get_array_output`` expose
``member_idx`` alongside the producing run metadata. A scalar run ID returns all
its saved members, stacked when shapes match or as a list when shapes vary.
Ordinary non-member outputs retain their existing scalar-run behavior and have
``member_idx=None``. In an archive shared by member and whole-value outputs,
``member_idx=-1`` identifies a whole-value entry. Sorting, export, consolidation,
and zipped-workflow reads preserve member metadata and ordering.

Show information about a task schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the Python API, use the `info` property of a task schema object to print information such as the input and output parameters of the schema: :code:`TaskSchema.info`.
