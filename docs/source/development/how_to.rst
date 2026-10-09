Contributor how to guides
=========================

Preparing jobscripts for host submission
---------------------------------------

The ``--containerised`` option on ``go``, ``demo-workflow go``, and
``workflow ... submit`` writes jobscripts without launching them. Standard output
contains a single JSON submission plan; progress and other messages go to standard
error. ``--wait`` and ``--cancel`` are not supported in this mode.
With ``--modify-js``, all jobscripts are generated first, then their paths and a
confirmation prompt are written to standard error. Edit the files and answer
``y`` to return the JSON plan. Interactive use requires standard input to be
available to the container; jobscripts are never launched by this prompt.

The plan contains ``schema_version`` (currently ``1``), ``workflow_id``,
``workflow_path`` (the absolute workflow directory as seen by hpcflow), and a
``jobscripts`` list in
submission order. Each entry includes ``submission_index``, ``jobscript_index``,
``path`` (relative to the workflow directory, with slash separators), ``scheduler``,
``shell``, ``is_array``, ``dependencies``, and ``submit_command`` (argument words,
not shell source). Non-array direct jobs also include ``stdout_path`` and
``stderr_path``, relative to the workflow directory.

Each dependency identifies its submission and jobscript indices and whether it is
an array dependency. ``reference`` contains the scheduler job ID or process ID for
an already submitted dependency, or ``null`` for a dependency in this plan.
``placeholder`` identifies the corresponding token in the submission command.
The host must replace unresolved dependency tokens with the actual IDs obtained
from submitting earlier jobscripts. It must also replace
``__HPCFLOW_JOBSCRIPT_PATH__`` with the host-visible jobscript path and submit from
the host-visible workflow directory. Container and host mount paths may differ.
Preserve argument boundaries; do not evaluate the command as shell source.
Temporary tokens also use the application's upper-case package name: MatFlow
plans use ``__MATFLOW_JOB_0_0__`` and ``__MATFLOW_JOBSCRIPT_PATH__`` rather than
the hpcflow-prefixed examples above. The wrapper template's generic app prefix
and temporary container-image token are replaced when the wrapper is exported.

Preparation does not set job IDs, process IDs, submission timestamps, or
known-submission records. Repeating preparation regenerates the pending jobscripts
without marking them submitted. In Python, ``Workflow.submit(containerised=True)``
returns the plan regardless of ``return_idx``; the app's ``make_and_submit_*``
functions return ``(workflow, plan)``.

Recording host submission results
#################################

The host wrapper acknowledges each successful host-side launch before launching
the next jobscript, using the internal CLI (not an end-user command):

.. code-block:: console

    hpcflow internal workflow WORKFLOW_PATH record-host-submission result.json

Use ``-`` instead of a filename to read JSON from standard input. The command
returns ``{"recorded": true}`` for a new acknowledgement and
``{"recorded": false}`` for an identical retry. Retrying does not launch jobs or
append another submission part. Conflicting acknowledgements fail without
overwriting the recorded job. Keep the result until acknowledgement succeeds;
if recording fails, retry recording, not the host launch.

Each result is a JSON object, for example for a queued job:

.. code-block:: json

    {
      "schema_version": 1,
      "workflow_id": "ID_FROM_THE_PLAN",
      "submission_index": 0,
      "jobscript_index": 0,
      "scheduler_job_ID": "12345",
      "submit_command": ["sbatch", "--parsable", "/host/workflow/jobscripts/js.sh"],
      "submit_time": "2026-10-09T13:30:00.123456+00:00",
      "submit_hostname": "login-node",
      "submit_machine": "my-cluster"
    }

For direct execution, omit ``scheduler_job_ID`` and supply ``process_ID`` as a
positive integer. This must be the host process ID used by the direct scheduler
(not the container process ID). Queued job IDs must be parsed from the scheduler's
successful submission output. Never acknowledge a failed launch.
``submit_command`` is the actual host command's argument list, with paths and
dependency placeholders already resolved. ``submit_time`` must include a timezone;
it is stored in UTC. ``submit_hostname`` and ``submit_machine`` describe the host,
not the container. The machine name should match the hpcflow ``machine``
configuration used to monitor or cancel these jobs. Optional ``version_info``
maps strings to strings or lists of strings collected on the host; acknowledgement
does not probe host-only schedulers or shells inside the container.

The corresponding private method is
``workflow._record_host_submission(result, add_to_known=True)``.
This uses the same jobscript metadata and submission-part storage as normal
submission and commits the record before returning. The default also updates the
known-submissions file; use ``--no-add-to-known`` (or ``add_to_known=False``) to
disable that update. If this update fails after the workflow record is committed,
an identical retry repairs it.

Only acknowledge jobscripts already prepared by hpcflow. Acknowledging some of
the jobscripts leaves the rest pending; subsequent containerised preparation
excludes acknowledged jobs and uses their real IDs for dependencies. Host
acknowledgements must be serialised, not made concurrently against the same
workflow. This protocol does not coordinate concurrent host submitters.

PowerShell host submission wrapper
##################################

The initial wrapper requires PowerShell 7.3 or newer and Docker on the host, but
not a host hpcflow installation for orchestration. It supports Slurm and SGE
submission, and direct execution on Windows using PowerShell jobscripts. The host must have
the submission executable available (``sbatch`` or ``qsub``). These schedulers
normally require a POSIX host, where PowerShell can also be installed.

Build the runtime image from the repository root:

.. code-block:: powershell

    docker build --build-arg HPCFLOW_IMAGE=hpcflow:dev -t hpcflow:dev .

The Dockerfile uses Python 3.13 slim and a separate build stage, installing only
runtime dependencies and the packaged application. Test/development dependencies,
the source checkout, Git history and build caches are not included in the final
image. Installation skips bytecode compilation and runtime bytecode writes are
disabled. Required scientific and notebook dependencies are retained; this image
does not remove supported application features to save space.
``--build-arg PYTHON_VERSION=3.12`` selects another supported Python version.
Dependencies are resolved from ``pyproject.toml`` at build time, not from the
Poetry lock file.

Declare the image name explicitly at build time. Docker does not automatically
make its eventual image tag available inside an image. The Dockerfile includes:

.. code-block:: dockerfile

    ARG HPCFLOW_IMAGE=hpcflow:dev
    ENV HPCFLOW_CONTAINER=${HPCFLOW_IMAGE}

Build using the same value for the build argument and image tag.
``RunTimeInfo.container_image`` reads ``HPCFLOW_CONTAINER`` at app initialisation,
and ``RunTimeInfo.in_container`` indicates whether that value is present.
The variable prefix is the application's upper-case package name, following the
existing app-specific environment-variable convention. For downstream MatFlow,
use ``MATFLOW_CONTAINER`` (and an appropriately named build argument/image tag)
instead. Each app reads only its own container variable.
Both fields are included in runtime information. This marker does not yet
automatically change submission behaviour.

With an image whose entrypoint invokes hpcflow, export the wrapper to the host:

.. code-block:: powershell

    docker run --rm hpcflow:dev install > hpcflow.ps1

This prints only the wrapper to stdout; shell redirection saves it on the host
without a bind mount or host Python installation. Use PowerShell 7.3 or newer.
``install --image IMAGE`` overrides the baked image name.
The low-level ``internal get-container-wrapper`` command remains available.
Name the exported script after the application's package name: ``hpcflow.ps1``
or, for MatFlow, ``matflow.ps1``. Only the PowerShell wrapper is currently
implemented; a future Bash wrapper will use the corresponding ``.sh`` name.

Run the wrapper from the directory containing your templates and workflows:

.. code-block:: powershell

    .\hpcflow.ps1 -Machine my-cluster go template.yaml --path .

Pass the hpcflow command and arguments directly after any wrapper options.
Quote values containing spaces as usual in PowerShell:

.. code-block:: powershell

    .\hpcflow.ps1 demo-workflow go workflow_1 `
        --path . --name "direct demo" --name-no-timestamp

    .\hpcflow.ps1 workflow "direct demo" show-all-status

The explicit ``-HpcflowArgs @('go', 'template.yaml', '--path', '.')`` form
remains supported for programmatic callers. Wrapper options such as ``-Image``,
``-Machine`` and ``-ConfigArgs`` should precede the hpcflow command.
``-Machine`` defaults to the host hostname, so local direct execution does not
require it. Override it when using a different logical machine name, such as a
named cluster configuration. The wrapper uses this name during preparation and
records it in host submission acknowledgements.

The current directory is bind-mounted at ``/work``, which is also the container's
working directory. Workflow paths must stay within this mount; use relative paths
or container-visible ``/work/...`` paths in hpcflow arguments. The mount directory
must not contain a comma. Windows direct execution maps workflow arguments,
command-file paths, working directories, and app environment paths between the
host mount and ``/work``. Arbitrary paths embedded in user command source are
not rewritten; use the app's path environment variables or host-visible paths.

Container configuration is shared across working directories, but kept separate
from ordinary host configuration. The wrapper appends ``-container`` to the host
``HPCFLOW_CONFIG_DIR`` (``MATFLOW_CONFIG_DIR`` for MatFlow), or uses
``~/.hpcflow-container`` (``~/.matflow-container``) when unset. For example,
``D:\.hpcflow`` becomes ``D:\.hpcflow-container``. This directory is created on
the host and bind-mounted at ``/config/.hpcflow-container`` inside the container.
The configuration directory must not contain a comma.
Explicit container-visible ``/work/...`` configuration paths remain supported
on Windows. The wrapper does not copy existing host or working-directory
configuration into the new directory. Direct-job contexts retain the selected
host configuration directory for subsequent container invocations.

The same mount persists caches and application data. The wrapper sets
``XDG_CACHE_HOME`` and ``XDG_DATA_HOME`` to ``cache`` and ``data`` beneath the
container configuration directory. Platformdirs adds the application name:
for hpcflow these become ``cache/hpcflow`` and ``data/hpcflow`` on the host.
Downloaded demonstration data, cached programs, and application state therefore
survive container removal and are shared across working directories. Container
caches are separate from native host caches, since cached programs may target a
different operating system. Persistent file-path arguments and environment values
are mapped back to the host during direct execution; this does not make Linux
executables runnable on Windows. Existing ephemeral caches are not migrated.

The wrapper recognises ``go``, ``demo-workflow go``, and
``workflow WORKFLOW_PATH submit`` at the start of ``HpcflowArgs``. Supply global
hpcflow options separately via ``-ConfigArgs @('--config-key', 'KEY')``.
Other commands pass through, preserving argument boundaries and the exit code.
Submission adds ``--containerised`` and the host's ``-Machine`` configuration
override; ``--wait`` and ``--cancel`` are rejected. ``--modify-js`` requires
interactive stdin, which Docker receives through ``-i``.

The wrapper validates the plan, launches commands from the host workflow
directory, parses Slurm ``--parsable`` and SGE ``-terse`` IDs, substitutes dependency
tokens, and immediately acknowledges each launch. It stops on the first error.
A ``.hpcflow-host-submission.json`` journal in the workflow directory preserves
the command, output and result. If acknowledgement fails after a known successful
launch, retry only the acknowledgement:

.. code-block:: powershell

    .\hpcflow.ps1 -ResumeResult (
        '.\workflow\.hpcflow-host-submission.json'
    )

Run recovery from the same mount root and with the same image and configuration.
Successful recovery removes the journal without launching anything. Run submission
again to prepare the remaining pending jobscripts. A journal with state
``launching`` means the launch outcome is uncertain (including scheduler output
that cannot be parsed). Inspect the scheduler and saved record manually; recovery
will not relaunch or silently mark such a job submitted. Do not delete an uncertain
journal until the outcome has been reconciled. Serialise all wrapper invocations
against a workflow.

Windows direct execution with a Linux container
##############################################

On Windows the wrapper supplies ``HPCFLOW_CONTAINER``,
``HPCFLOW_CONTAINER_HOST_OS=nt``, ``HPCFLOW_CONTAINER_HOST_HOSTNAME`` and
``HPCFLOW_CONTAINER_HOST_CPU_ARCH`` to the
container. These variables use the application's upper-case package name
(``MATFLOW_`` for MatFlow). Runtime information retains the actual container
platform and adds ``execution_os``, ``execution_platform`` and
``execution_CPU_arch`` for host resource selection, plus ``execution_hostname``
for run metadata. New configurations default
to the host shell; the wrapper selects the host PowerShell 7 executable.
An existing configuration must include a ``powershell`` shell entry.

Direct jobs start as detached host processes with separate jobscript stdout and
stderr logs. The recorded process ID and command line describe the host launcher,
not a container process. A durable acknowledgement gate prevents execution before
the launch is recorded. If acknowledgement fails, the process waits; use
``-ResumeResult`` to acknowledge the existing process and open its gate without
launching another job. Dependencies are waited on by the host launcher before
invoking the jobscript, not by hpcflow inside the container. Process creation
times in the saved context protect against reused PIDs. Running dependencies
launched outside this wrapper cannot be verified and are rejected.

The wrapper saves a copy of itself and per-job execution context in the workflow
directory. Generated PowerShell functions route hpcflow operations (including
parameter saving) back through this copy. Keep these files and the original
mount directory available while jobs are running.

For each run, ``internal workflow ... execute-run --containerised`` prepares
inputs and a command file and marks the run started. It returns a JSON command,
working directory and execution environment, or ``null`` if no host command is
needed and the run was finalised in the container. The host runs the command
synchronously, then calls ``internal workflow ... complete-host-run`` with the
same five indices and the actual exit code. Completion processes generated
files, loop termination and downstream failures using the ordinary run lifecycle.
Identical completion retries are idempotent; conflicting exit codes fail.
Prepared runs are never automatically executed again.

A ``.HPCFLOW-run-*.json`` journal (``.MATFLOW-run-*.json`` for MatFlow) is retained
if execution or completion is interrupted. State ``executed`` contains
``complete_args`` and the exit code: retry those arguments through the wrapper,
then remove the reconciled journal. Do not rerun the command file. State
``launching`` is uncertain and requires manual reconciliation. Journals omit
execution environment values, including secrets.

Host executables and their environment setup must be available on Windows;
container-installed executables are not automatically host executables. Python
scripts that import the app still require a suitable host Python environment,
or an explicitly configured container-based executable.

Combined scripts, abortable actions, direct job arrays, and host process
monitoring/cancellation through the wrapper are explicitly unsupported for now.
The submission command still does not support ``--wait`` or ``--cancel``.
POSIX queued execution still needs its execution bridge; ``manage
install-container`` and Bash wrapper support are also not yet implemented.

The optional integration test for this path requires a local Linux image with
an hpcflow entrypoint. Set ``HPCFLOW_TEST_CONTAINER_IMAGE`` to its tag and run
``hpcflow test --integration --configure-python-env -k
test_linux_container_windows_direct_workflow``.

Adding class methods to the ``ValueSequence`` and ``MultiPathSequence`` classes
-------------------------------------------------------------------------------

Adding class methods to :py:class:`hpcflow.app.ValueSequence`
#############################################################

``ValueSequence`` exposes class methods that can be used to generate sequences of values (i.e. multiple elements) within a task. Within a YAML workflow template, the requirement to generate a sequence via a class method is written using a double-colon syntax as follows:

.. code-block:: yaml

    tasks:
      - objective: t1
        sequences:
          - path: inputs.p1
            values::from_range:
              start: 0
              stop: 10
              step: 1

In the case above, we are telling |app_name| to use the method :py:meth:`hpcflow.app.ValueSequence.from_range` to generate multiple elements for the task. The inner block containing ``start``, ``stop``, and ``step`` is then passed as a ``dict`` of keyword arguments to that class method.

Follow these steps to add a new sequence-generating class method to :py:class:`hpcflow.app.ValueSequence`:

1. Consider how to name your new method. For consistency with existing methods, please consider a name that is prefixed by ``from...``. For this example, let's consider a method named ``from_my_new_method``.

2. Add a new method that generates a list of values using your new approach, named ``_values_from_my_new_method``` (i.e. your new method name, prefixed by ``_values_``). If your new technique is parametrised by two arguments, ``arg_1`` (an integer), and ``arg_2`` (a list of strings), the signature of this method should look like this (the ``**kwargs`` is optional—consider if it is necessary to include):

   .. code-block:: python
 
       @classmethod
       def _values_from_my_new_method(
           cls,
           arg_1: int,
           arg_2: list[str],
           **kwargs,
       ) -> Self:
           pass # implementation here

3. Add a new method that can be used within the API:

   .. code-block:: python
 
       @classmethod
       def from_my_new_method(
           cls,
           path: str,
           arg_1: int,
           arg_2: list[str],
           nesting_order: float = 0,
           label: str | int | None = None,          
           **kwargs,
       ) -> Self:
           """
           Build a sequence from ...
           """
           args = {"arg_1": arg_1, "arg_2": arg_2, **kwargs}
           values = cls._values_from_my_new_method(**args)
           obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
           obj._values_method = "from_my_new_method"
           obj._values_method_args = args
           return obj
           
   Note that in addition to ``arg_1`` and ``arg_2``, this method signature must include some positional and keyword arguments: ``path``, ``nesting_order``, and ``label``, which  should be passed directly to the constructor. We use the method added previously (``_values_from_my_new_method``, in this case) to generate the values, and then pass those values  on to the constructor. Note also that after object construction, we must assign two attributes:
 
   - ``_values_method`` which should be the name of this method
   - ``_values_method_args``, which should be a mapping of argument names and values used to parametrise the value-generating method (``_values_from_my_new_method``, in this case).

4. Write some tests. Please include somewhere within ``hpcflow/tests/unit`` at least one test to convince yourself that your new method generates the correct sequence of values:

   .. code-block:: python

    from hpcflow.app import app as hf

    def test_sequence_from_my_new_method():
        seq = hf.ValueSequence.from_my_new_method(path="inputs.p1", arg_1=9, arg_2=['a', 'b'])
        
        # check the expected number of values generated:
        assert len(seq.values) == 2
        
        # check the expected values, if possible:
        assert seq.values == ["val_a", "val_b"]

        # check the correct attributes are set:
        assert seq._values_method == "from_my_new_method"
        assert seq._values_method_args == {"arg_1": 9, "arg_2": ['a', 'b']}


Adding class methods to :py:class:`hpcflow.app.MultiPathSequence`
#################################################################

``MultiPathSequence`` exposes class methods that can be used to generate multiple sequences of values (i.e. multiple elements) within a task, corresponding to multiple paths (i.e. inputs or resources). Within a YAML workflow template, the requirement to generate a multi-path sequence via a class method is written using a double-colon syntax as follows:

.. code-block:: yaml

    tasks:
      - objective: t1
        multi_path_sequences:
          - paths: [inputs.p1, inputs.p2]
            values::from_latin_hypercube:
              num_samples: 5

In the case above, we are telling |app_name| to use the method :py:meth:`hpcflow.app.MultiPathSequence.from_latin_hypercube` to generate five elements for the task, by combining five values for the input ``p1`` with five values for the input ``p2``, where all ten values are generated at the same time via a Latin hypercube sampling. The inner block containing ``num_samples``, is passed as a ``dict`` of keyword arguments to that class method.

The same process as above can be used for adding new class methods to ``MultiPathSequence``, with two exceptions. Firstly, a ``paths`` postitional argument (note: plural) must be specified in the values-generating method as defined in step 2. above. Thus the method should look like this:

.. code-block:: python

    @classmethod
    def _values_from_my_new_method(
        cls,
        paths: Seqeuence[str], # <- note additional `paths` argument
        arg_1: int,
        arg_2: list[str],
        **kwargs,
    ) -> Self:
        pass # implementation here

The reason for including the ``paths`` argument is so we can know, for example, for how many paths the ``MultiPathSequence`` should generate values for.

Secondly, the ``path`` (note: singular) argument in the public-facing method (``from_my_new_method`` in this case) should be replaced by ``paths`` (note: plural), which should have the type annotation: ``Sequence[str]``.
