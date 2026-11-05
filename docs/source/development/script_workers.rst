.. _script-worker-implementation:

Persistent Python script-worker execution
=========================================

Script workers provide an experimental, opt-in alternative to launching a new
interpreter for every eligible Python snippet. The user-facing option and its
state-isolation consequences are described in :ref:`persistent-script-workers`.
The legacy command path remains the default.

Routing and generated artifacts
-------------------------------

The submission stores ``enable_script_workers``. The jobscript executor uses a
worker only when that option is enabled, the action is a Python snippet with a
configured executable, and its shell is supported. Other actions use the
ordinary run-command path. ``combine_scripts=True`` is a retired execution mode
and is rejected; it is not the worker opt-in.

During submission preparation, ``PythonScriptWriter`` generates a Python module
containing action wrappers and a registry keyed by jobscript block and action
coordinates. It also generates a manifest and a shell command file that performs
environment setup and launches the module. The bootstrap loads the persisted
workflow and binds the registered callables to their assigned runs. Requests
identify the run, block, action, and element; Python source is not sent over
ZeroMQ for each execution.

The ``ScriptWorkerArgs`` identity hashes the jobscript index, language,
environment specification, executable command, and shell name.
``JobscriptExecutor.script_workers`` is a plural mapping of these worker IDs to
``ScriptWorkerClient`` instances. A jobscript can therefore have separate
workers for different interpreter or environment identities. Each jobscript
execution owns its own workers.

Ownership and lifecycle
-----------------------

* ``JobscriptExecutor`` owns jobscript orchestration, run preparation and
  finalization, persistent run state, timing aggregation, the jobscript server,
  and worker cleanup. It starts workers lazily and stops them in jobscript
  cleanup, including on exceptional paths.
* ``RunExecutor`` continues to execute ordinary external commands.
  ``ScriptWorkerRun`` provides per-run control of one invocation, including
  abort and cancellation.
* ``ScriptWorkerExecutor`` owns the worker process. ``ScriptWorkerClient`` owns
  its private loopback ZeroMQ connection and serializes exchanges with that
  worker. The service dispatches one request to a registered callable and
  returns a structured result.
* ``ScriptWorkerInvocation`` supplies per-run inputs and context and persists
  direct outputs. The parent still validates required files and owns run
  finalization; the worker does not implement a separate workflow orchestration
  loop.

The worker can remain alive across non-script actions, for example::

    Python A -> Python B -> external command C -> Python D -> Python E

The same compatible worker handles A and B, waits while C runs, then handles D
and E. An abort or cancellation of an active invocation terminates and reaps its
worker. The client does not automatically retry failed requests. Child work and
orchestration timings are reported to the parent for aggregation by ``TimeIt``.

Invocation context and persistent state
---------------------------------------

The generated module and imported modules remain loaded for the life of a
worker. Explicit module-global mutations and imported-module state can therefore
be shared across its requests, including after an ordinary script failure.
Each generated wrapper executes the snippet and recreates its local definitions
on each invocation; this is not a general reset of the interpreter.

The adapter restores the working directory, ``os.environ``, redirected
stdout/stderr, and the temporary application file logger. It executes each
invocation in a copied ``ContextVar`` context, including its timing activation.
This isolates bindings, not arbitrary mutable objects or other process-global
state. If setup or restoration fails, the service treats the worker as unsafe
to reuse.

Run state and waiting
---------------------

``JobscriptExecutor`` prepares run files, handles conditions and skips, and
finalizes results. It persists run ends before updating run-wait state and
notifying waiters. Jobscript completion markers on the shared filesystem are
authoritative; ZeroMQ notifications are wake-ups rather than proof of
completion. Preserve completion-marker and notification cleanup when execution
or worker cleanup fails. The synchronous wait API also needs to work when the
caller already has an event loop, such as in a notebook.

``EARStatus.get_terminal_states()`` includes skipped, aborted, success, and
error. Skipped runs also appear in ``get_executable_states()`` because they may
still need jobscript processing and finalization.

A successful script exit is not necessarily a successful run. In particular,
an input-file generator can exit with status zero without creating its promised
file. Parent-side finalization must detect the missing file and preserve
downstream failure behavior when ``skip_downstream_on_failure=False``.
An earlier regression left a downstream run submitted when its commands-file
path was uninitialized after preparation failed. Keep that path initialized
before preparation and handle unset-input errors from both file preparation and
commands-file generation.

Regression and platform guardrails
----------------------------------

The worker adapter tests cover context restoration, real transport, failures,
interpreter/environment isolation, and worker reuse across an intervening
external action. The executor, transport, and bootstrap test files cover their
respective lifecycle and protocol boundaries. The integration test
``test_run_status_fail_when_missing_IFG_input_file`` exercises the missing-file
regression on both the legacy and worker paths. Keep run-finalization,
jobscript-completion, and workflow-wait tests when changing this code.

When invoking pytest directly, load ``hpcflow.pytest_plugin``. Integration runs
also require ``--configure-python-env``; ``--force-script-workers`` can exercise
new submissions through the opt-in path. Run focused tests before broader
integration tests.

Keep Python 3.10 compatibility; for example, do not use ``asyncio.timeout()``
or ``StrEnum`` in production code while 3.10 is supported. Do not assume
``sys.executable`` is a Python interpreter in a frozen application. Generated
Bash and PowerShell scripts have different quoting and line-continuation rules;
validate each on its target platform. Historical macOS runner failures have
also involved hostname resolution, for which a reachable loopback address can
be useful. When debugging test startup, check malformed pytest options (for
example, ``-o`` without a ``key=value`` argument) and empty positional arguments
in frozen test runners before attributing a collection failure to execution.

Future changes should preserve the opt-in boundary, parent-owned finalization,
multiple worker identities, and the state contract above. Changes to interpreter
grouping or namespace semantics require explicit compatibility decisions and
tests; the old combined-script ``run_all()`` orchestration is not the worker
design.
