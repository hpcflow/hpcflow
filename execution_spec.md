# Execution specification

Here is a specification for the execution model of hpcflow.

## Current executor integration

`JobscriptExecutor` owns run orchestration, persistence, finalization, timing, and
completion notifications. `RunExecutor` owns each ordinary command subprocess.
`ScriptWorkerExecutor` remains a scaffold, and `ElementActionRun.use_script_worker`
remains disabled.

The restored worker scaffold can generate Python wrappers for isolated execution, but its
entry point explicitly rejects execution until a production worker adapter is implemented.
Production submissions neither generate nor launch these workers. The existing
interpreter/environment-specific worker mapping remains provisional.

Worker lifecycle operations are serialized per worker. Without a protocol client,
the lifecycle owner's `stop()` terminates the direct child, waits up to five
seconds, then kills it if necessary; `abort()` kills immediately. Reaping after kill is bounded to
five seconds. A timeout or OS error is surfaced and the process handle is retained
for inspection or retry. This does not yet terminate descendant processes.
Cancellation during spawn or shutdown waits for the owned child to be reaped
before propagating; repeated cancellation does not interrupt cleanup.
Jobscript cleanup attempts every worker, logs each failure, and raises the first
failure after all attempts. Successfully stopped workers are removed from the
mapping. Worker cleanup failures do not skip server shutdown, completion markers,
or waiter notification.

Run directories are prepared lazily for non-skipped runs. Finalization uses the
workflow's declared-file validation and task/schema file-retention policies.
An input-file generator that exits successfully without producing its required
file still fails, independently of whether that file is selected for retention.
Policy-selected files are saved before execution directories are removed.
Successful iteration directories are cleaned up only after run-end persistence
commits, subject to `delete_execute_dir`; failed or skipped iterations are retained.
Waiters are notified after final state is persisted, with filesystem completion
state authoritative and ZeroMQ notifications used as wake-ups.

## Script-worker protocol contract (version 1)

The message contract and an isolated client/service loop are implemented.
Production worker routing and generated-worker service startup remain disabled.
These messages are separate from the existing child-to-jobscript control protocol.
The script-worker protocol, process owner, client, service, adapter, and bootstrap are
grouped under `hpcflow/sdk/execution/script_worker/`; script source generation remains
under `hpcflow/sdk/scripting/`.

Requests and responses are JSON objects with an integer `version` of `1`.
Requests have a `command`; responses have a `response` and a boolean `ok`.
Missing, unknown, incorrectly typed, and unsupported-version fields are rejected.

| Request command | Fields | Response |
| --- | --- | --- |
| `ping` | None | `ready`, `ok: true` |
| `execute` | `run_id`, `block_idx`, `action_idx`, `element_idx` | `result`, `ok: true`, echoed identity, `exit_code`, `work_time`, `orchestration_time` |
| `shutdown` | None | `shutdown`, `ok: true` |

Identity fields are nonnegative integers (not booleans). `element_idx` is local to
the jobscript block. `(block_idx, action_idx)` selects a precomposed callable in a
jobscript-specific worker; a future dispatcher must also validate that the run and
element match that selection. Action indices alone are not unique across tasks.
No source code, arbitrary callables, or parameter/output objects are sent.

`ok: true` on a result means the invocation completed and returned a result, not
that the script or run succeeded. `exit_code` is a signed or unsigned integer
without wire-level normalization. Timings are required, finite, nonnegative
numbers in seconds. Work time measures callable execution; orchestration time
measures worker-side per-run setup, serialization, and context cleanup, excluding
idle time and transport. Finalization and promised-file validation remain the
executor's responsibility.

Protocol rejection uses `response: "error"`, `ok: false`, and a nonempty `error`
description; it is not a script exit code. A rejected execution request must not
invoke a callable. Unexpected service failures must be logged explicitly and must
not produce a success-shaped result.

The isolated transport is private loopback ZeroMQ REQ/REP, one connection per
worker and one request in flight. The executor binds an ephemeral endpoint before
spawn; the worker connects. Startup and shutdown are bounded; execution must not
inherit a short RPC timeout. Awaiting a reply must also monitor process exit.
There are no automatic execution retries: after a lost response, execution may
already have caused side effects. Abort/cancellation terminates and reaps the
worker rather than sending an abort over an occupied REQ/REP exchange.

The future production adapter must release per-run working directory, environment, logging,
and app context before replying, including on script exceptions. Shutdown
acknowledgement precedes service exit and is not a substitute for reaping the
process. Interpreter/environment compatibility, module-global persistence, and
real-script input/output integration are deferred.

### Isolated client and service

`ScriptWorkerClient(cmd, env)` composes the existing process lifecycle owner rather
than changing `JobscriptExecutor`. It binds the REQ socket on `127.0.0.1` before
spawn and supplies the endpoint in `HPCFLOW_SCRIPT_WORKER_ENDPOINT`. The launched
program must construct and run a `ScriptWorkerService` using that endpoint.
Startup (including readiness) defaults to ten seconds. Idle shutdown uses a
five-second reply budget and a five-second process-exit budget. Failures or
cancellation close the socket and kill/reap the child through the existing bounded
lifecycle implementation. A busy `stop()` aborts instead of waiting indefinitely
for execution to complete. An idle `stop()` requires acknowledgement and exit
code zero; acknowledgement alone does not count as successful shutdown.

Requests are serialized by an async lock. Execution has no implicit RPC deadline;
callers may cancel it, including through `asyncio.wait_for`. Cancellation while
queued does not abort another invocation. A malformed, unexpected, or
identity-mismatched reply invalidates the connection and aborts the worker.
A protocol rejection raises `ScriptWorkerRejectedError` but leaves the connection
usable. Process death raises `ScriptWorkerExitedError`, not a script result.
After process exit, a final in-transit reply may drain for at most 0.1 seconds.
No execution is retried; an explicit `start()` is required to restart a client
after transport failure.

The service runs a synchronous REP loop so registered callables execute on the
worker's main thread. Its explicit registry maps a complete `ExecuteScriptRequest`
identity to a zero-argument callable returning an integer exit code or a measured
`ScriptExecutionResult` from an adapter. Result identity must match the registered
request. This is an isolated testing seam, not the generated action registry:
unknown identities are
rejected without invocation. Ordinary callable exceptions are logged with their
traceback and produce exit code one; invalid handler return values are fatal
service errors. `SystemExit`, `KeyboardInterrupt`, and abrupt process death are
not converted into ordinary exception results. Malformed requests are rejected
and the service continues accepting requests. Unexpected service failures are
logged and raised.

No handler is discovered or imported from a wire message. Registry construction
is explicit; per-run context restoration, serialization, and output persistence
belong to the adapter described below, not the transport service. The service does
not claim to isolate arbitrary Python global state or orchestrate workflow runs.

### Off-production precomposed-script adapter

`PythonScriptWriter` now exposes an `ACTION_SCRIPTS` callable registry when its
generated module is imported. Explicit `(block_idx, action_idx)` keys preserve
actions with the same local index in different blocks. Submission registry assembly
now derives those keys from jobscript block rows and groups them by worker
environment/executable arguments; runs sharing a registry key must resolve to the
same action. The generated main entry point loads a versioned manifest, reloads the
persistent workflow and submission, checks the worker identity against each assigned
run, and registers only the declared block/action callables before starting the
existing service. Manifest configuration identity is explicit; malformed, stale, or
mismatched registry entries fail startup.

Persistent workers are experimental and remain disabled by default. Opt in when
creating a submission with `add-submission --enable-script-workers`; eligible Python
snippet actions then receive worker artifacts, and generated jobscripts pass the
enable flag to the executor. `JobscriptExecutor` owns one client per stable
interpreter/environment identity, sends each run's environment with its identity,
and passes the returned exit code through normal parent-side finalization. Supported
shells are Bash, WSL Bash, and PowerShell. Submissions without the opt-in continue to
use the legacy path and do not generate worker artifacts.

The opt-in is persisted with the submission for both JSON and Zarr stores and
retained when jobscripts are regenerated after reload. Submissions created before
the option was introduced default to disabled. Reload tests launch the generated
jobscript in a fresh process and verify that successive runs use the same worker
interpreter. A script failure returns a nonzero result without poisoning the
worker: a later invocation can succeed in the same process.

Worker command-file generation substitutes the generated script path using the
target shell's quoting rules and removes the empty argument placeholder. Legacy
integer keys remain accepted by the writer scaffold, but the bootstrap manifest
requires explicit tuple keys. Wrapper names use unique generation indices, so
identical snippets in different blocks do not overwrite one another.

`ScriptWorkerInvocation` binds a request, locally resolved EAR, selected callable,
and explicit environment/stdout/log paths. It validates the request's complete
identity against the EAR's assigned submission/jobscript block before execution.
It neither accepts arbitrary wire arguments nor selects an interpreter. The parent
executor supplies each run's environment and prepares directories and input files,
just as it does on the legacy path.

Inputs use `get_py_script_func_kwargs`, existing failure thresholds, and parameter
metadata/array cache contexts. Direct output mappings and scalar output-parser
results use `Workflow.save_parameter` with that EAR's ID, retaining run-specific
array-output policy. File-format outputs stay on the existing file path and are
saved/validated by parent finalization, not by the adapter. Input-file-generator
exit zero alone does not establish success: promised-file validation remains
authoritative in the parent.

The shared input helper converts the internal `EnvironmentSpec` to a plain
dictionary at the script boundary, retaining the existing `script_pass_env_spec`
API for direct arguments, file-format input serialization, and IFGs. The internal
hashable environment identity and matching rules are unchanged.

Each invocation runs in a copied ContextVar context and its own TimeIt session.
The adapter temporarily installs its environment, execution cwd, stdout/stderr
redirection, and app file logger, restoring them before returning a measured result.
Work time covers only callable invocation; orchestration time includes argument
resolution, output persistence, and context setup/teardown. Script and serialization
exceptions are logged, written to run stderr, and returned as exit one. Context
setup/teardown failures are fatal service errors, so an unsafe interpreter is not
reused. `SystemExit` and other BaseException exits propagate after cleanup rather
than becoming successful replies.

Wrapper-local definitions, including mutable function defaults, are rebuilt per
invocation as before. Imported modules retain Python's normal process-wide cache.
Arbitrary module globals, user-created threads, external handles, global app
reconfiguration, and random-library state are not isolated by this adapter.
It is a synchronous, single-flight worker facility, not a thread-safe in-process
execution sandbox. No run starts/ends, skips, completion notifications, directory
preparation, or cleanup policy decisions move out of `JobscriptExecutor`.

Factors that affect the method of execution of these commands:

- Scheduler variant (e.g. SLURM, SGE, or "direct" execution)
- Job array vs single job  
- Host OS: Windows or posix

Assume elements are resolved for each task, and we have a function that can retrieve a given input for a given element.

We can consider parameter data to be stored in the following form:
```
[
  {'is_set': True, 'data': {'parameter1': 101}},
  {'is_set': True, 'data': {'parameter1': 102}},
  {'is_set': True, 'data': {'parameter2': 201}},
  {'is_set': True, 'data': {'parameter2': 202}},
  {'is_set': False, 'data': None},
  {'is_set': False, 'data': None},
  {'is_set': False, 'data': None},
]
```

Elements are associated with tasks with the following index map (i.e each sublist corresponds to the elements associated with that task):

```
elements_indices = [
  [0, 1],         # e.g. task 1 elements
  [2, 3],         #      task 2 elements
  [4, 5, 6, 7],   #      task 3 elements
]
```

Parameter data is mapped to elements in the following way (i.e. each dict represents an element):

```
[
  { # element 0
    'inputs': [
      {
        'path': ('parameter1',),
        'data_index': 0
      },
    ],
    'outputs': [
      {
        'path': ('parameter2',),
        'data_index': 0
      }
    ]
  },
  ...
]
```

For now, we can assume each task is associated with a list of string commands to be executed for each element in that task. We can also assume that commands accept string inputs which correspond to the input parameters of that task, and that commands produce string outputs, which correspond to the output parameter of that task:

```
commands = [
  [ # commands for task 1
    "doSomething parameter1 parameter2",
    "doSomethingElse parameter1"
  ],
  [
    # commands for task 2
  ],
  ...
]
```


```python
def run_elements(command, scheduler='SGE', host_os='posix'):
    if scheduler == 'direct':
        subprocess.run(command, shell=True)
    else:
        pass

```
