from typing import List, Type, Union

from hpcflow.parameters import Input, InputValue, InputValueSequence, Parameter


class DuplicateTaskError(Exception):
    pass


class TaskSchema:

    def __init__(self, name, inputs, outputs):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs

    def __repr__(self):
        return f'<TaskSchema(inputs={self.inputs}, outputs={self.outputs})>'

    def __eq__(self, other):
        """Two TaskSchemas are considered equal if their names, inputs and outputs are
        equal."""
        if isinstance(other, TaskSchema):
            if (
                self.name == other.name and
                self.inputs == other.inputs and
                self.outputs == other.outputs
            ):
                return True

        return False


class Task:

    def __init__(self, schema, input_values: Union[InputValue, InputValueSequence], nesting_order, context=None):

        self.schema = schema
        self.context = context
        self.input_values = input_values
        self.nesting_order = nesting_order

    def __repr__(self):
        return f'<Task(schema={self.schema}, input_values={self.input_values})>'

    def __add__(self, other):
        if isinstance(other, Task):
            return TaskSet(self, other)
        elif isinstance(other, TaskSet):
            return TaskSet(
                self,
                *other.tasks,
                imported_parameters=other.imported_parameters,
            )
        else:
            raise TypeError(f'Cannot add object "{other}" to Task.')

    def __eq__(self, other):
        """Two tasks are considered equal if their schemas and contexts are equal. 
        Equality of parameter values is not considered. This is used to check that all
        tasks in a workflow or (TaskSet) are unique."""

        if isinstance(other, Task):
            if self.schema == other.schema and self.context == other.context:
                return True

        return False

    @property
    def name_and_context(self) -> str:
        ctx_str = ''
        if self.context:
            ctx_str = f' and context "{self.context}"'
        return f'"{self.schema.name}"' + ctx_str


class TaskSet:
    """A set of tasks."""

    def __init__(self, *tasks, imported_parameters: Parameter = None):
        self._tasks = tasks
        self._imported_parameters = imported_parameters

        self._validate()

    def __repr__(self):
        return f'<TaskSet({", ".join([f"{task.schema.name!r}" for task in self.tasks])})>'

    def __add__(self, other):
        if isinstance(other, TaskSet):
            return TaskSet(
                *self.tasks,
                *other.tasks,
                imported_parameters=(self.imported_parameters +
                                     other.imported_parameters),
            )
        elif isinstance(other, Task):
            return TaskSet(
                *self.tasks,
                other,
                imported_parameters=self.imported_parameters,
            )
        else:
            raise TypeError(f'Cannot add object "{other}" to Task.')

    def _validate(self):

        for i_idx, task_i in enumerate(self.tasks):
            for j_idx, task_j in enumerate(self.tasks[i_idx + 1:], start=i_idx + 1):
                if task_i == task_j:
                    msg = (
                        f'TaskSet must not include equivalent tasks; task {i_idx} with '
                        f'name {task_i.name_and_context} is equivalent to task {j_idx} '
                        f'with name {task_j.name_and_context}.'
                    )
                    raise DuplicateTaskError(msg)

    @property
    def imported_parameters(self):
        return self._imported_parameters or []

    @imported_parameters.setter
    def imported_parameters(self, value):
        self._imported_parameters = value

    @property
    def tasks(self):
        return self._tasks


class WorkflowTask:
    """A concrete task within a workflow."""
    pass
