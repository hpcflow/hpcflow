# Execution specification

Here is a specification for the execution model of hpcflow.

## Current executor integration

`JobscriptExecutor` owns run orchestration, persistence, finalization, timing, and
completion notifications. `RunExecutor` owns each ordinary command subprocess.
`ScriptWorkerExecutor` remains a scaffold, and `ElementActionRun.use_script_worker`
remains disabled.

Run directories are prepared lazily for non-skipped runs. Finalization uses the
workflow's declared-file validation and task/schema file-retention policies.
An input-file generator that exits successfully without producing its required
file still fails, independently of whether that file is selected for retention.
Policy-selected files are saved before execution directories are removed.
Successful iteration directories are cleaned up only after run-end persistence
commits, subject to `delete_execute_dir`; failed or skipped iterations are retained.
Waiters are notified after final state is persisted, with filesystem completion
state authoritative and ZeroMQ notifications used as wake-ups.

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
