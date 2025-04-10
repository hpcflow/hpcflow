"""
A looping construct for a workflow.
There are multiple types of loop,
notably looping over a set of values or until a condition holds.
"""

from __future__ import annotations

from collections import defaultdict
import copy
from pprint import pp
import pprint
from typing import Dict, List, Optional, Tuple, Union, Any
from warnings import warn
from collections import defaultdict
from itertools import chain
from typing import cast, TYPE_CHECKING
from typing_extensions import override

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.actions import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.errors import LoopTaskSubsetError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.loop_cache import LoopCache, LoopIndex
from hpcflow.sdk.core.enums import InputSourceType, TaskSourceType
from hpcflow.sdk.core.utils import check_valid_py_identifier, nth_key, nth_value
from hpcflow.sdk.utils.strings import shorten_list_str
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar
    from typing_extensions import Self, TypeIs
    from rich.status import Status
    from ..typing import DataIndex, ParamSource
    from .parameters import SchemaInput, InputSource
    from .rule import Rule
    from .task import WorkflowTask
    from .types import IterableParam
    from .workflow import Workflow, WorkflowTemplate


# @dataclass
# class StoppingCriterion:
#     parameter: Parameter
#     condition: ConditionLike


# @dataclass
# class Loop:
#     parameter: Parameter
#     stopping_criteria: StoppingCriterion
#     # TODO: should be a logical combination of these (maybe provide a superclass in valida to re-use some logic there?)
#     maximum_iterations: int


class Loop(JSONLike):
    """
    A loop in a workflow template.

    Parameters
    ----------
    tasks: list[int | ~hpcflow.app.WorkflowTask]
        List of task insert IDs or workflow tasks.
    num_iterations:
        Number of iterations to perform.
    name: str
        Loop name.
    non_iterable_parameters: list[str]
        Specify input parameters that should not iterate.
    termination: v~hpcflow.app.Rule
        Stopping criterion, expressed as a rule.
    termination_task: int | ~hpcflow.app.WorkflowTask
        Task at which to evaluate the termination condition.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="termination", class_name="Rule"),
    )

    @classmethod
    def __is_WorkflowTask(cls, value) -> TypeIs[WorkflowTask]:
        return isinstance(value, cls._app.WorkflowTask)

    def __init__(
        self,
        tasks: Iterable[int | WorkflowTask],
        num_iterations: int,
        name: str | None = None,
        non_iterable_parameters: list[str] | None = None,
        termination: Rule | None = None,
        termination_task: int | WorkflowTask | None = None,
    ) -> None:
        _task_insert_IDs: list[int] = []
        for task in tasks:
            if self.__is_WorkflowTask(task):
                _task_insert_IDs.append(task.insert_ID)
            elif isinstance(task, int):
                _task_insert_IDs.append(task)
            else:
                raise TypeError(
                    f"`tasks` must be a list whose elements are either task insert IDs "
                    f"or `WorkflowTask` objects, but received the following: {tasks!r}."
                )

        if termination_task is None:
            _term_task_iID = _task_insert_IDs[-1]  # terminate on final task by default
        elif self.__is_WorkflowTask(termination_task):
            _term_task_iID = termination_task.insert_ID
        elif isinstance(task, int):
            _term_task_iID = termination_task
        else:
            raise TypeError(
                f"`termination_task` must be a task insert ID or a `WorkflowTask` "
                f"object, but received the following: {termination_task!r}."
            )

        if _term_task_iID not in _task_insert_IDs:
            raise ValueError(
                f"If specified, `termination_task` (provided: {termination_task!r}) must "
                f"refer to a task that is part of the loop. Available task insert IDs "
                f"are: {_task_insert_IDs!r}."
            )

        self._task_insert_IDs = _task_insert_IDs
        self._num_iterations = num_iterations
        self._name = check_valid_py_identifier(name) if name else name
        self._non_iterable_parameters = non_iterable_parameters or []
        self._termination = termination
        self._termination_task_insert_ID = _term_task_iID

        self._workflow_template: WorkflowTemplate | None = (
            None  # assigned by parent WorkflowTemplate
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        return {k.lstrip("_"): v for k, v in out.items()}

    @classmethod
    def _json_like_constructor(cls, json_like: dict) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        if "task_insert_IDs" in json_like:
            insert_IDs = json_like.pop("task_insert_IDs")
        else:
            insert_IDs = json_like.pop("tasks")

        if "termination_task_insert_ID" in json_like:
            tt_iID = json_like.pop("termination_task_insert_ID")
        elif "termination_task" in json_like:
            tt_iID = json_like.pop("termination_task")
        else:
            tt_iID = None

        return cls(tasks=insert_IDs, termination_task=tt_iID, **json_like)

    @property
    def task_insert_IDs(self) -> tuple[int, ...]:
        """Get the list of task insert_IDs that define the extent of the loop."""
        return tuple(self._task_insert_IDs)

    @property
    def name(self) -> str | None:
        """
        The name of the loop, if one was provided.
        """
        return self._name

    @property
    def num_iterations(self) -> int:
        """
        The number of loop iterations to do.
        """
        return self._num_iterations

    @property
    def non_iterable_parameters(self) -> Sequence[str]:
        """
        Which parameters are not iterable.
        """
        return self._non_iterable_parameters

    @property
    def termination(self) -> Rule | None:
        """
        A termination rule for the loop, if one is provided.
        """
        return self._termination

    @property
    def termination_task_insert_ID(self) -> int:
        """
        The insert ID of the task at which the loop will terminate.
        """
        return self._termination_task_insert_ID

    @property
    def termination_task(self) -> WorkflowTask:
        """
        The task at which the loop will terminate.
        """
        if (wt := self.workflow_template) is None:
            raise RuntimeError(
                "Workflow template must be assigned to retrieve task objects of the loop."
            )
        assert wt.workflow
        return wt.workflow.tasks.get(insert_ID=self.termination_task_insert_ID)

    @property
    def workflow_template(self) -> WorkflowTemplate | None:
        """
        The workflow template that contains this loop.
        """
        return self._workflow_template

    @workflow_template.setter
    def workflow_template(self, template: WorkflowTemplate):
        self._workflow_template = template
        self.__validate_against_template()

    def __workflow(self) -> None | Workflow:
        if (wt := self.workflow_template) is None:
            return None
        return wt.workflow

    @property
    def task_objects(self) -> tuple[WorkflowTask, ...]:
        """
        The tasks in the loop.
        """
        if not (wf := self.__workflow()):
            raise RuntimeError(
                "Workflow template must be assigned to retrieve task objects of the loop."
            )
        return tuple(wf.tasks.get(insert_ID=t_id) for t_id in self.task_insert_IDs)

    def __validate_against_template(self) -> None:
        """Validate the loop parameters against the associated workflow."""

        # insert IDs must exist:
        if not (wf := self.__workflow()):
            raise RuntimeError(
                "workflow cannot be validated against as it is not assigned"
            )
        for insert_ID in self.task_insert_IDs:
            try:
                wf.tasks.get(insert_ID=insert_ID)
            except ValueError:
                raise ValueError(
                    f"Loop {self.name!r} has an invalid task insert ID {insert_ID!r}. "
                    f"Such as task does not exist in the associated workflow."
                )

    def __repr__(self) -> str:
        num_iterations_str = ""
        if self.num_iterations is not None:
            num_iterations_str = f", num_iterations={self.num_iterations!r}"

        name_str = ""
        if self.name:
            name_str = f", name={self.name!r}"

        return (
            f"{self.__class__.__name__}("
            f"task_insert_IDs={self.task_insert_IDs!r}{num_iterations_str}{name_str}"
            f")"
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = self.to_dict()
        kwargs["tasks"] = kwargs.pop("task_insert_IDs")
        kwargs["termination_task"] = kwargs.pop("termination_task_insert_ID")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._workflow_template = self._workflow_template
        return obj


class WorkflowLoop(AppAware):
    """
    Class to represent a :py:class:`.Loop` that is bound to a
    :py:class:`~hpcflow.app.Workflow`.

    Parameters
    ----------
    index: int
        The index of this loop in the workflow.
    workflow: ~hpcflow.app.Workflow
        The workflow containing this loop.
    template: Loop
        The loop that this was generated from.
    num_added_iterations:
        Description of what iterations have been added.
    iterable_parameters:
        Description of what parameters are being iterated over.
    output_parameters:
        Description of what parameter are output from this loop, and the final task insert
        ID from which they are output.
    parents: list[str]
        The paths to the parent entities of this loop.
    """

    def __init__(
        self,
        index: int,
        workflow: Workflow,
        template: Loop,
        num_added_iterations: dict[tuple[int, ...], int],
        iterable_parameters: dict[str, IterableParam],
        output_parameters: dict[str, int],
        parents: list[str],
    ) -> None:
        self._index = index
        self._workflow = workflow
        self._template = template
        self._num_added_iterations = num_added_iterations
        self._iterable_parameters = iterable_parameters
        self._output_parameters = output_parameters
        self._parents = parents

        # appended to when adding an empty loop to the workflow that is a parent of this
        # loop; reset and added to `self._parents` on dump to disk:
        self._pending_parents: list[str] = []

        # used for `num_added_iterations` when a new loop iteration is added, or when
        # parents are append to; reset to None on dump to disk. Each key is a tuple of
        # parent loop indices and each value is the number of pending new iterations:
        self._pending_num_added_iterations: dict[tuple[int, ...], int] | None = None

        self._validate()

    @TimeIt.decorator
    def _validate(self) -> None:
        # task subset must be a contiguous range of task indices:
        task_indices = self.task_indices
        task_min, task_max = task_indices[0], task_indices[-1]
        if task_indices != tuple(range(task_min, task_max + 1)):
            raise LoopTaskSubsetError(self.name, self.task_indices)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(template={self.template!r}, "
            f"num_added_iterations={self.num_added_iterations!r})"
        )

    @property
    def num_added_iterations(self) -> Mapping[tuple[int, ...], int]:
        """
        The number of added iterations.
        """
        if self._pending_num_added_iterations:
            return self._pending_num_added_iterations
        else:
            return self._num_added_iterations

    @property
    def __pending(self) -> dict[tuple[int, ...], int]:
        if not self._pending_num_added_iterations:
            self._pending_num_added_iterations = dict(self._num_added_iterations)
        return self._pending_num_added_iterations

    def _initialise_pending_added_iters(self, added_iters: Iterable[int]):
        if not self._pending_num_added_iterations:
            self._pending_num_added_iterations = dict(self._num_added_iterations)
        if (added_iters_key := tuple(added_iters)) not in (pending := self.__pending):
            pending[added_iters_key] = 1

    def _increment_pending_added_iters(self, added_iters_key: Iterable[int]):
        self.__pending[tuple(added_iters_key)] += 1

    def _update_parents(self, parent: WorkflowLoop):
        assert parent.name
        self._pending_parents.append(parent.name)

        self._pending_num_added_iterations = {
            (*k, 0): v
            for k, v in (
                self._pending_num_added_iterations or self._num_added_iterations
            ).items()
        }

        self.workflow._store.update_loop_parents(
            index=self.index,
            num_added_iters=self.num_added_iterations,
            parents=self.parents,
        )

    def _reset_pending_num_added_iters(self) -> None:
        self._pending_num_added_iterations = None

    def _accept_pending_num_added_iters(self) -> None:
        if self._pending_num_added_iterations:
            self._num_added_iterations = dict(self._pending_num_added_iterations)
            self._reset_pending_num_added_iters()

    def _reset_pending_parents(self) -> None:
        self._pending_parents = []

    def _accept_pending_parents(self) -> None:
        self._parents += self._pending_parents
        self._reset_pending_parents()

    @property
    def index(self) -> int:
        """
        The index of this loop within its workflow.
        """
        return self._index

    @property
    def task_insert_IDs(self) -> tuple[int, ...]:
        """
        The insertion IDs of the tasks inside this loop.
        """
        return self.template.task_insert_IDs

    @property
    def task_objects(self) -> tuple[WorkflowTask, ...]:
        """
        The tasks in this loop.
        """
        return self.template.task_objects

    @property
    def task_indices(self) -> tuple[int, ...]:
        """
        The list of task indices that define the extent of the loop.
        """
        return tuple(task.index for task in self.task_objects)

    @property
    def workflow(self) -> Workflow:
        """
        The workflow containing this loop.
        """
        return self._workflow

    @property
    def template(self) -> Loop:
        """
        The loop template for this loop.
        """
        return self._template

    @property
    def parents(self) -> Sequence[str]:
        """
        The parents of this loop.
        """
        return self._parents + self._pending_parents

    @property
    def name(self) -> str:
        """
        The name of this loop, if one is defined.
        """
        assert self.template.name
        return self.template.name

    @property
    def iterable_parameters(self) -> dict[str, IterableParam]:
        """
        The parameters that are being iterated over.
        """
        return self._iterable_parameters

    @property
    def output_parameters(self) -> dict[str, int]:
        """
        The parameters that are outputs of this loop, and the final task insert ID from
        which each parameter is output.
        """
        return self._output_parameters

    @property
    def num_iterations(self) -> int:
        """
        The number of iterations.
        """
        return self.template.num_iterations

    @property
    def downstream_tasks(self) -> Iterator[WorkflowTask]:
        """Tasks that are not part of the loop, and downstream from this loop."""
        tasks = self.workflow.tasks
        for idx in range(self.task_objects[-1].index + 1, len(tasks)):
            yield tasks[idx]

    @property
    def upstream_tasks(self) -> Iterator[WorkflowTask]:
        """Tasks that are not part of the loop, and upstream from this loop."""
        tasks = self.workflow.tasks
        for idx in range(0, self.task_objects[0].index):
            yield tasks[idx]

    @staticmethod
    @TimeIt.decorator
    def _find_iterable_and_output_parameters(
        loop_template: Loop,
    ) -> tuple[dict[str, IterableParam], dict[str, int]]:
        all_inputs_first_idx: dict[str, int] = {}
        all_outputs_idx: dict[str, list[int]] = defaultdict(list)
        for task in loop_template.task_objects:
            for typ in task.template.all_schema_input_types:
                all_inputs_first_idx.setdefault(typ, task.insert_ID)
            for typ in task.template.all_schema_output_types:
                all_outputs_idx[typ].append(task.insert_ID)

        # find input parameters that are also output parameters at a later/same task:
        iterable_params: dict[str, IterableParam] = {}
        for typ, first_idx in all_inputs_first_idx.items():
            if typ in all_outputs_idx and first_idx <= all_outputs_idx[typ][0]:
                iterable_params[typ] = {
                    "input_task": first_idx,
                    "output_tasks": all_outputs_idx[typ],
                }

        for non_iter in loop_template.non_iterable_parameters:
            iterable_params.pop(non_iter, None)

        final_out_tasks = {k: v[-1] for k, v in all_outputs_idx.items()}

        return iterable_params, final_out_tasks

    @classmethod
    @TimeIt.decorator
    def new_empty_loop(
        cls,
        index: int,
        workflow: Workflow,
        template: Loop,
        iter_loop_idx: Sequence[Mapping[str, int]],
    ) -> WorkflowLoop:
        """
        Make a new empty loop.

        Parameters
        ----------
        index: int
            The index of the loop to create.
        workflow: ~hpcflow.app.Workflow
            The workflow that will contain the loop.
        template: Loop
            The template for the loop.
        iter_loop_idx: list[dict]
            Iteration information from parent loops.
        """
        parent_loops = cls._get_parent_loops(index, workflow, template)
        parent_names = [i.name for i in parent_loops]
        num_added_iters: dict[tuple[int, ...], int] = {}
        for i in iter_loop_idx:
            num_added_iters[tuple([i[j] for j in parent_names])] = 1

        iter_params, out_params = cls._find_iterable_and_output_parameters(template)
        return cls(
            index=index,
            workflow=workflow,
            template=template,
            num_added_iterations=num_added_iters,
            iterable_parameters=iter_params,
            output_parameters=out_params,
            parents=parent_names,
        )

    @classmethod
    @TimeIt.decorator
    def _get_parent_loops(
        cls,
        index: int,
        workflow: Workflow,
        template: Loop,
    ) -> list[WorkflowLoop]:
        parents: list[WorkflowLoop] = []
        passed_self = False
        self_tasks = set(template.task_insert_IDs)
        for loop_i in workflow.loops:
            if loop_i.index == index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issubset(other_tasks):
                if (self_tasks == other_tasks) and not passed_self:
                    continue
                parents.append(loop_i)
        return parents

    @TimeIt.decorator
    def get_parent_loops(self) -> list[WorkflowLoop]:
        """Get loops whose task subset is a superset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop list is
        considered the child."""
        return self._get_parent_loops(self.index, self.workflow, self.template)

    @TimeIt.decorator
    def get_child_loops(self) -> list[WorkflowLoop]:
        """Get loops whose task subset is a subset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop list is
        considered the child."""
        children: list[WorkflowLoop] = []
        passed_self = False
        self_tasks = set(self.task_insert_IDs)
        for loop_i in self.workflow.loops:
            if loop_i.index == self.index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issuperset(other_tasks):
                if (self_tasks == other_tasks) and passed_self:
                    continue
                children.append(loop_i)

        # order by depth, so direct child is first:
        return sorted(children, key=lambda x: len(next(iter(x.num_added_iterations))))

    @TimeIt.decorator
    def add_iteration(
        self,
        parent_loop_indices: Mapping[str, int] | None = None,
        cache: LoopCache | None = None,
        status: Status | None = None,
    ) -> None:
        """
        Add an iteration to this loop.

        Parameters
        ----------
        parent_loop_indices:
            Where have any parent loops got up to?
        cache:
            A cache used to make adding the iteration more efficient.
            One will be created if it is not supplied.
        """
        if not cache:
            cache = LoopCache.build(self.workflow)
        assert cache is not None
        parent_loops = self.get_parent_loops()
        child_loops = self.get_child_loops()
        parent_loop_indices_ = parent_loop_indices or {
            loop.name: 0 for loop in parent_loops
        }

        iters_key = tuple(parent_loop_indices_[p_nm] for p_nm in self.parents)
        cur_loop_idx = self.num_added_iterations[iters_key] - 1

        # keys are (task.insert_ID and element.index)
        all_new_data_idx: dict[tuple[int, int], DataIndex] = {}

        # initialise a new `num_added_iterations` key on each child loop:
        iters_key_dct = {
            **parent_loop_indices_,
            self.name: cur_loop_idx + 1,
        }
        for child in child_loops:
            child._initialise_pending_added_iters(
                iters_key_dct.get(j, 0) for j in child.parents
            )

            # needed for the case where an inner loop has only one iteration, meaning
            # `add_iteration` will not be called recursively on it:
            self.workflow._store.update_loop_num_iters(
                index=child.index,
                num_added_iters=child.num_added_iterations,
            )

        for task in self.task_objects:
            new_loop_idx = LoopIndex(iters_key_dct) + {
                child.name: 0
                for child in child_loops
                if task.insert_ID in child.task_insert_IDs
            }
            added_iter_IDs: list[int] = []
            for elem_idx in range(task.num_elements):
                elem_ID = task.element_IDs[elem_idx]

                new_data_idx: DataIndex = {}

                # copy resources from zeroth iteration:
                zeroth_iter_ID, zi_iter_data_idx = cache.zeroth_iters[elem_ID]
                zi_elem_ID, zi_idx = cache.iterations[zeroth_iter_ID]
                zi_data_idx = nth_value(cache.data_idx[zi_elem_ID], zi_idx)

                for key, val in zi_data_idx.items():
                    if key.startswith("resources."):
                        new_data_idx[key] = val

                for inp in task.template.all_schema_inputs:
                    is_inp_task = False
                    if iter_dat := self.iterable_parameters.get(inp.typ):
                        is_inp_task = task.insert_ID == iter_dat["input_task"]

                    inp_key = f"inputs.{inp.typ}"

                    if is_inp_task:
                        assert iter_dat is not None
                        inp_dat_idx = self.__get_looped_index(
                            task,
                            elem_ID,
                            cache,
                            iter_dat,
                            inp,
                            parent_loops,
                            parent_loop_indices_,
                            child_loops,
                            cur_loop_idx,
                        )
                        new_data_idx[inp_key] = inp_dat_idx
                    else:
                        orig_inp_src = cache.elements[elem_ID]["input_sources"][inp_key]
                        inp_dat_idx = None

                        if orig_inp_src.source_type is InputSourceType.LOCAL:
                            # keep locally defined inputs from original element
                            inp_dat_idx = zi_data_idx[inp_key]

                        elif orig_inp_src.source_type is InputSourceType.DEFAULT:
                            # keep default value from original element
                            try:
                                inp_dat_idx = zi_data_idx[inp_key]
                            except KeyError:
                                # if this input is required by a conditional action, and
                                # that condition is not met, then this input will not
                                # exist in the action-run data index, so use the initial
                                # iteration data index:
                                inp_dat_idx = zi_iter_data_idx[inp_key]

                        elif orig_inp_src.source_type is InputSourceType.TASK:
                            inp_dat_idx = self.__get_task_index(
                                task,
                                orig_inp_src,
                                cache,
                                elem_ID,
                                inp,
                                inp_key,
                                parent_loop_indices_,
                                all_new_data_idx,
                            )

                        if inp_dat_idx is None:
                            raise RuntimeError(
                                f"Could not find a source for parameter {inp.typ} "
                                f"when adding a new iteration for task {task!r}."
                            )

                        new_data_idx[inp_key] = inp_dat_idx

                # add any locally defined sub-parameters:
                inp_statuses = cache.elements[elem_ID]["input_statuses"]
                inp_status_inps = set(f"inputs.{inp}" for inp in inp_statuses)
                for sub_param_i in inp_status_inps.difference(new_data_idx):
                    sub_param_data_idx_iter_0 = zi_data_idx
                    try:
                        sub_param_data_idx = sub_param_data_idx_iter_0[sub_param_i]
                    except KeyError:
                        # as before, if this input is required by a conditional action,
                        # and that condition is not met, then this input will not exist in
                        # the action-run data index, so use the initial iteration data
                        # index:
                        sub_param_data_idx = zi_data_idx[sub_param_i]

                    new_data_idx[sub_param_i] = sub_param_data_idx

                for out in task.template.all_schema_outputs:
                    path_i = f"outputs.{out.typ}"
                    p_src: ParamSource = {"type": "EAR_output"}
                    new_data_idx[path_i] = self.workflow._add_unset_parameter_data(p_src)

                schema_params = set(i for i in new_data_idx if len(i.split(".")) == 2)
                all_new_data_idx[task.insert_ID, elem_idx] = new_data_idx

                iter_ID_i = self.workflow._store.add_element_iteration(
                    element_ID=elem_ID,
                    data_idx=new_data_idx,
                    schema_parameters=list(schema_params),
                    loop_idx=new_loop_idx,
                )
                if cache:
                    cache.add_iteration(
                        iter_ID=iter_ID_i,
                        task_insert_ID=task.insert_ID,
                        element_ID=elem_ID,
                        loop_idx=new_loop_idx,
                        data_idx=new_data_idx,
                    )

                added_iter_IDs.append(iter_ID_i)

            task.initialise_EARs(iter_IDs=added_iter_IDs)

        self._increment_pending_added_iters(
            parent_loop_indices_[p_nm] for p_nm in self.parents
        )
        self.workflow._store.update_loop_num_iters(
            index=self.index,
            num_added_iters=self.num_added_iterations,
        )

        # add iterations to fixed-number-iteration children only:
        for child in child_loops[::-1]:
            if child.num_iterations is not None:
                if status:
                    status_prev = str(status.status).rstrip(".")
                for iter_idx in range(child.num_iterations - 1):
                    if status:
                        status.update(
                            f"{status_prev} --> ({child.name!r}): iteration "
                            f"{iter_idx + 2}/{child.num_iterations}."
                        )
                    par_idx = {parent_name: 0 for parent_name in child.parents}
                    if parent_loop_indices:
                        par_idx.update(parent_loop_indices)
                    par_idx[self.name] = cur_loop_idx + 1
                    child.add_iteration(parent_loop_indices=par_idx, cache=cache)

        self.__update_loop_downstream_data_idx(parent_loop_indices_)

    def __get_src_ID_and_groups(
        self,
        elem_ID: int,
        iter_dat: IterableParam,
        inp: SchemaInput,
        cache: LoopCache,
        task: WorkflowTask,
    ) -> tuple[int, Sequence[int]]:
        # `cache.elements` contains only elements that are part of the
        # loop, so indexing a dependent element may raise:
        src_elem_IDs = {}
        for k, v in cache.element_dependents[elem_ID].items():
            try:
                if cache.elements[k]["task_insert_ID"] == iter_dat["output_tasks"][-1]:
                    src_elem_IDs[k] = v
            except KeyError:
                continue

        # consider groups
        single_data = inp.single_labelled_data
        assert single_data is not None
        inp_group_name = single_data.get("group")
        grouped_elems = [
            src_elem_j_ID
            for src_elem_j_ID, src_elem_j_dat in src_elem_IDs.items()
            if any(nm == inp_group_name for nm in src_elem_j_dat["group_names"])
        ]

        if not grouped_elems and len(src_elem_IDs) > 1:
            raise NotImplementedError(
                f"Multiple elements found in the iterable parameter "
                f"{inp!r}'s latest output task (insert ID: "
                f"{iter_dat['output_tasks'][-1]}) that can be used "
                f"to parametrise the next iteration of task "
                f"{task.unique_name!r}: "
                f"{list(src_elem_IDs)!r}."
            )

        elif not src_elem_IDs:
            # TODO: maybe OK?
            raise NotImplementedError(
                f"No elements found in the iterable parameter "
                f"{inp!r}'s latest output task (insert ID: "
                f"{iter_dat['output_tasks'][-1]}) that can be used "
                f"to parametrise the next iteration."
            )

        return nth_key(src_elem_IDs, 0), grouped_elems

    def __get_looped_index(
        self,
        task: WorkflowTask,
        elem_ID: int,
        cache: LoopCache,
        iter_dat: IterableParam,
        inp: SchemaInput,
        parent_loops: list[WorkflowLoop],
        parent_loop_indices: Mapping[str, int],
        child_loops: list[WorkflowLoop],
        cur_loop_idx: int,
    ):
        # source from final output task of previous iteration, with all parent
        # loop indices the same as previous iteration, and all child loop indices
        # maximised:

        # identify element(s) from which this iterable input should be
        # parametrised:
        if task.insert_ID == iter_dat["output_tasks"][-1]:
            # single-task loop
            src_elem_ID = elem_ID
            grouped_elems: Sequence[int] = []
        else:
            # multi-task loop
            src_elem_ID, grouped_elems = self.__get_src_ID_and_groups(
                elem_ID, iter_dat, inp, cache, task
            )

        child_loop_max_iters: dict[str, int] = {}
        parent_loop_same_iters = {
            loop.name: parent_loop_indices[loop.name] for loop in parent_loops
        }
        child_iter_parents = {
            **parent_loop_same_iters,
            self.name: cur_loop_idx,
        }
        for loop in child_loops:
            if iter_dat["output_tasks"][-1] in loop.task_insert_IDs:
                i_num_iters = loop.num_added_iterations[
                    tuple(child_iter_parents[j] for j in loop.parents)
                ]
                i_max = i_num_iters - 1
                child_iter_parents[loop.name] = i_max
                child_loop_max_iters[loop.name] = i_max

        loop_idx_key = LoopIndex(child_loop_max_iters)
        loop_idx_key.update(parent_loop_same_iters)
        loop_idx_key[self.name] = cur_loop_idx

        # identify the ElementIteration from which this input should be
        # parametrised:
        if grouped_elems:
            src_data_idx = [
                cache.data_idx[src_elem_ID][loop_idx_key] for src_elem_ID in grouped_elems
            ]
            if not src_data_idx:
                raise RuntimeError(
                    f"Could not find a source iteration with loop_idx: "
                    f"{loop_idx_key!r}."
                )
            return [i[f"outputs.{inp.typ}"] for i in src_data_idx]
        else:
            return cache.data_idx[src_elem_ID][loop_idx_key][f"outputs.{inp.typ}"]

    def __get_task_index(
        self,
        task: WorkflowTask,
        orig_inp_src: InputSource,
        cache: LoopCache,
        elem_ID: int,
        inp: SchemaInput,
        inp_key: str,
        parent_loop_indices: Mapping[str, int],
        all_new_data_idx: Mapping[tuple[int, int], DataIndex],
    ) -> int | list[int]:
        if orig_inp_src.task_ref not in self.task_insert_IDs:
            # source the data_idx from the iteration with same parent
            # loop indices as the new iteration to add:
            src_data_idx = next(
                di_k
                for li_k, di_k in cache.data_idx[elem_ID].items()
                if all(li_k.get(p_k) == p_v for p_k, p_v in parent_loop_indices.items())
            )

            # could be multiple, but they should all have the same
            # data index for this parameter:
            return src_data_idx[inp_key]

        is_group = (
            inp.single_labelled_data is not None
            and "group" in inp.single_labelled_data
            # this input is a group, assume for now all elements
        )

        # same task/element, but update iteration to the just-added
        # iteration:
        assert orig_inp_src.task_source_type is not None
        key_prefix = orig_inp_src.task_source_type.name.lower()
        prev_dat_idx_key = f"{key_prefix}s.{inp.typ}"
        new_sources: list[tuple[int, int]] = []
        for (tiID, e_idx), _ in all_new_data_idx.items():
            if tiID == orig_inp_src.task_ref:
                # find which element in that task `element`
                # depends on:
                src_elem_IDs = cache.element_dependents[
                    self.workflow.tasks.get(insert_ID=tiID).element_IDs[e_idx]
                ]
                # `cache.elements` contains only elements that are part of the loop, so
                # indexing a dependent element may raise:
                src_elem_IDs_i = []
                for k, _v in src_elem_IDs.items():
                    try:
                        if (
                            cache.elements[k]["task_insert_ID"] == task.insert_ID
                            and k == elem_ID
                            # filter src_elem_IDs_i for matching element IDs
                        ):

                            src_elem_IDs_i.append(k)
                    except KeyError:
                        continue

                if len(src_elem_IDs_i) == 1:
                    new_sources.append((tiID, e_idx))

        if is_group:
            # Convert into simple list of indices
            return list(
                chain.from_iterable(
                    self.__as_sequence(all_new_data_idx[src][prev_dat_idx_key])
                    for src in new_sources
                )
            )
        else:
            assert len(new_sources) == 1
            return all_new_data_idx[new_sources[0]][prev_dat_idx_key]

    @staticmethod
    def __as_sequence(seq: int | Iterable[int]) -> Iterable[int]:
        if isinstance(seq, int):
            yield seq
        else:
            yield from seq

    def __update_loop_downstream_data_idx(
        self,
        parent_loop_indices: Mapping[str, int],
    ):
        # update data indices of loop-downstream tasks that depend on task outputs from
        # this loop:

        # keys: iter or run ID, values: dict of param type and new parameter index
        iter_new_data_idx: dict[int, DataIndex] = defaultdict(dict)
        run_new_data_idx: dict[int, DataIndex] = defaultdict(dict)

        param_sources = self.workflow.get_all_parameter_sources()

        # keys are parameter type, then task insert ID, then data index keys mapping to
        # their updated values:
        all_updates: dict[str, dict[int, dict[int, int]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        for task in self.downstream_tasks:
            for elem in task.elements:
                for param_typ, param_out_task_iID in self.output_parameters.items():
                    if param_typ in task.template.all_schema_input_types:
                        # this element's input *might* need updating, only if it has a
                        # task input source type that is this loop's output task for this
                        # parameter:
                        elem_src = elem.input_sources[f"inputs.{param_typ}"]
                        if (
                            elem_src.source_type is InputSourceType.TASK
                            and elem_src.task_source_type is TaskSourceType.OUTPUT
                            and elem_src.task_ref == param_out_task_iID
                        ):
                            for iter_i in elem.iterations:

                                # do not modify element-iterations of previous iterations
                                # of the current loop:
                                skip_iter = False
                                for k, v in parent_loop_indices.items():
                                    if iter_i.loop_idx.get(k) != v:
                                        skip_iter = True
                                        break

                                if skip_iter:
                                    continue

                                # update the iteration data index and any pending runs:
                                iter_old_di = iter_i.data_idx[f"inputs.{param_typ}"]

                                is_group = True
                                if not isinstance(iter_old_di, list):
                                    is_group = False
                                    iter_old_di = [iter_old_di]

                                iter_old_run_source = [
                                    param_sources[i]["EAR_ID"] for i in iter_old_di
                                ]
                                iter_old_run_objs = self.workflow.get_EARs_from_IDs(
                                    iter_old_run_source
                                )  # TODO: use cache

                                # need to check the run source is actually from the loop
                                # output task (it could be from a previous iteration of a
                                # separate loop in this task):
                                if any(
                                    i.task.insert_ID != param_out_task_iID
                                    for i in iter_old_run_objs
                                ):
                                    continue

                                iter_new_iters = [
                                    i.element.iterations[-1] for i in iter_old_run_objs
                                ]

                                # note: we can cast to int, because output keys never
                                # have multiple data indices (unlike input keys):
                                iter_new_dis = [
                                    cast("int", i.get_data_idx()[f"outputs.{param_typ}"])
                                    for i in iter_new_iters
                                ]

                                # keep track of updates so we can also update task-input
                                # type sources:
                                all_updates[param_typ][task.insert_ID].update(
                                    dict(zip(iter_old_di, iter_new_dis))
                                )

                                iter_new_data_idx[iter_i.id_][f"inputs.{param_typ}"] = (
                                    iter_new_dis if is_group else iter_new_dis[0]
                                )

                                for run_j in iter_i.action_runs:
                                    if run_j.status is EARStatus.pending:
                                        try:
                                            old_di = run_j.data_idx[f"inputs.{param_typ}"]
                                        except KeyError:
                                            # not all actions will include this input
                                            continue

                                        is_group = True
                                        if not isinstance(old_di, list):
                                            is_group = False
                                            old_di = [old_di]

                                        old_run_source = [
                                            param_sources[i]["EAR_ID"] for i in old_di
                                        ]
                                        old_run_objs = self.workflow.get_EARs_from_IDs(
                                            old_run_source
                                        )  # TODO: use cache

                                        # need to check the run source is actually from the loop
                                        # output task (it could be from a previous action in this
                                        # element-iteration):
                                        if any(
                                            i.task.insert_ID != param_out_task_iID
                                            for i in old_run_objs
                                        ):
                                            continue

                                        new_iters = [
                                            i.element.iterations[-1] for i in old_run_objs
                                        ]

                                        # note: we can cast to int, because output keys
                                        # never have multiple data indices (unlike input
                                        # keys):
                                        new_dis = [
                                            cast(
                                                "int",
                                                i.get_data_idx()[f"outputs.{param_typ}"],
                                            )
                                            for i in new_iters
                                        ]

                                        run_new_data_idx[run_j.id_][
                                            f"inputs.{param_typ}"
                                        ] = (new_dis if is_group else new_dis[0])

                        elif (
                            elem_src.source_type is InputSourceType.TASK
                            and elem_src.task_source_type is TaskSourceType.INPUT
                        ):
                            # parameters are that sourced from inputs of other tasks,
                            # might need to be updated if those other tasks have
                            # themselves had their data indices updated:
                            assert elem_src.task_ref
                            ups_i = all_updates.get(param_typ, {}).get(elem_src.task_ref)

                            if ups_i:
                                # if a further-downstream task has a task-input source
                                # that points to this task, this will also need updating:
                                all_updates[param_typ][task.insert_ID].update(ups_i)

                            else:
                                continue

                            for iter_i in elem.iterations:

                                # update the iteration data index and any pending runs:
                                iter_old_di = iter_i.data_idx[f"inputs.{param_typ}"]

                                is_group = True
                                if not isinstance(iter_old_di, list):
                                    is_group = False
                                    iter_old_di = [iter_old_di]

                                iter_new_dis = [ups_i.get(i, i) for i in iter_old_di]

                                if iter_new_dis != iter_old_di:
                                    iter_new_data_idx[iter_i.id_][
                                        f"inputs.{param_typ}"
                                    ] = (iter_new_dis if is_group else iter_new_dis[0])

                                for run_j in iter_i.action_runs:
                                    if run_j.status is EARStatus.pending:
                                        try:
                                            old_di = run_j.data_idx[f"inputs.{param_typ}"]
                                        except KeyError:
                                            # not all actions will include this input
                                            continue

                                        is_group = True
                                        if not isinstance(old_di, list):
                                            is_group = False
                                            old_di = [old_di]

                                        new_dis = [ups_i.get(i, i) for i in old_di]

                                        if new_dis != old_di:
                                            run_new_data_idx[run_j.id_][
                                                f"inputs.{param_typ}"
                                            ] = (new_dis if is_group else new_dis[0])

        # now update data indices (TODO: including in cache!)
        if iter_new_data_idx:
            self.workflow._store.update_iter_data_indices(iter_new_data_idx)

        if run_new_data_idx:
            self.workflow._store.update_run_data_indices(run_new_data_idx)

    def test_termination(self, element_iter) -> bool:
        """Check if a loop should terminate, given the specified completed element
        iteration."""
        if self.template.termination:
            return self.template.termination.test(element_iter)
        return False

    @TimeIt.decorator
    def get_element_IDs(self):
        elem_IDs = [
            j
            for i in self.task_insert_IDs
            for j in self.workflow.tasks.get(insert_ID=i).element_IDs
        ]
        return elem_IDs

    @TimeIt.decorator
    def get_elements(self):
        return self.workflow.get_elements_from_IDs(self.get_element_IDs())

    @TimeIt.decorator
    def skip_downstream_iterations(self, elem_iter) -> list[int]:
        """
        Parameters
        ----------
        elem_iter
            The element iteration whose subsequent iterations should be skipped.
        dep_element_IDs
            List of elements that are dependent (recursively) on the element
            of `elem_iter`.
        """
        current_iter_idx = elem_iter.loop_idx[self.name]
        current_task_iID = elem_iter.task.insert_ID
        self._app.logger.info(
            f"setting loop {self.name!r} iterations downstream of current iteration "
            f"index {current_iter_idx} to skip"
        )
        elements = self.get_elements()

        # TODO: fix for multiple loop cycles
        warn(
            "skip downstream iterations does not work correctly for multiple loop cycles!"
        )

        to_skip = []
        for elem in elements:
            for iter_i in elem.iterations:
                if iter_i.loop_idx[self.name] > current_iter_idx or (
                    iter_i.loop_idx[self.name] == current_iter_idx
                    and iter_i.task.insert_ID > current_task_iID
                ):
                    to_skip.extend(iter_i.EAR_IDs_flat)

        self._app.logger.info(
            f"{len(to_skip)} runs will be set to skip: {shorten_list_str(to_skip)}"
        )
        self.workflow.set_EAR_skip({k: SkipReason.LOOP_TERMINATION for k in to_skip})

        return to_skip
