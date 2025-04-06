"""
Errors from the workflow system.
"""

from __future__ import annotations
import os
from collections.abc import Iterable, Mapping, Sequence
from textwrap import indent
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from logging import Logger
    from .enums import ParallelMode
    from .object_list import WorkflowLoopList
    from .parameters import InputSource, ValueSequence, SchemaInput
    from .types import ScriptData
    from .task import WorkflowTask, Task


class InputValueDuplicateSequenceAddress(ValueError):
    """
    An InputValue has the same sequence address twice.
    """

    # FIXME: never used


class TaskTemplateMultipleSchemaObjectives(ValueError):
    """
    A TaskTemplate has multiple objectives.
    """

    def __init__(self, names: set[str]) -> None:
        super().__init__(
            f"All task schemas used within a task must have the same "
            f"objective, but found multiple objectives: {sorted(names)!r}"
        )


class TaskTemplateUnexpectedInput(ValueError):
    """
    A TaskTemplate was given unexpected input.
    """

    def __init__(self, unexpected_types: set[str]) -> None:
        super().__init__(
            f"The following input parameters are unexpected: {sorted(unexpected_types)!r}"
        )


class TaskTemplateUnexpectedSequenceInput(ValueError):
    """
    A TaskTemplate was given an unexpected sequence.
    """

    def __init__(
        self, inp_type: str, expected_types: set[str], seq: ValueSequence
    ) -> None:
        allowed_str = ", ".join(f'"{in_typ}"' for in_typ in expected_types)
        super().__init__(
            f"The input type {inp_type!r} specified in the following sequence"
            f" path is unexpected: {seq.path!r}. Available input types are: "
            f"{allowed_str}."
        )


class TaskTemplateMultipleInputValues(ValueError):
    """
    A TaskTemplate had multiple input values bound over each other.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class InvalidIdentifier(ValueError):
    """
    A bad identifier name was given.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"Invalid string for identifier: {name!r}")


class MissingInputs(Exception):
    """
    Inputs were missing.

    Parameters
    ----------
    missing_inputs:
        The missing inputs.
    """

    # TODO: add links to doc pages for common user-exceptions?

    def __init__(self, task: Task, missing_inputs: Iterable[str]) -> None:
        self.missing_inputs = tuple(missing_inputs)
        missing_str = ", ".join(map(repr, missing_inputs))
        super().__init__(
            f"Task {task.name}: the following inputs have no sources: {missing_str}."
        )


class UnrequiredInputSources(ValueError):
    """
    Input sources were provided that were not required.

    Parameters
    ----------
    unrequired_sources:
        The input sources that were not required.
    """

    def __init__(self, unrequired_sources: Iterable[str]) -> None:
        self.unrequired_sources = frozenset(unrequired_sources)
        message = (
            f"The following input sources are not required but have been specified: "
            f'{", ".join(map(repr, sorted(self.unrequired_sources)))}.'
        )
        if any((bad := src).startswith("inputs.") for src in self.unrequired_sources):
            # reminder about how to specify input sources:
            message += (
                f" Note that input source keys should not be specified with the "
                f"'inputs.' prefix. Did you mean to specify "
                f"{bad.removeprefix('inputs.')!r} instead of {bad!r}?"
            )
        super().__init__(message)


class ExtraInputs(Exception):
    """
    Extra inputs were provided.

    Parameters
    ----------
    extra_inputs:
        The extra inputs.
    """

    def __init__(self, extra_inputs: set[str]) -> None:
        self.extra_inputs = frozenset(extra_inputs)
        super().__init__(
            f"The following inputs are not required, but have been passed: "
            f'{", ".join(f"{typ!r}" for typ in extra_inputs)}.'
        )


class UnavailableInputSource(ValueError):
    """
    An input source was not available.
    """

    def __init__(
        self, source: InputSource, path: str, avail: Sequence[InputSource]
    ) -> None:
        super().__init__(
            f"The input source {source.to_string()!r} is not "
            f"available for input path {path!r}. Available "
            f"input sources are: {[src.to_string() for src in avail]}."
        )


class InapplicableInputSourceElementIters(ValueError):
    """
    An input source element iteration was inapplicable."""

    def __init__(self, source: InputSource, elem_iters_IDs: Sequence[int] | None) -> None:
        super().__init__(
            f"The specified `element_iters` for input source "
            f"{source.to_string()!r} are not all applicable. "
            f"Applicable element iteration IDs for this input source "
            f"are: {elem_iters_IDs!r}."
        )


class NoCoincidentInputSources(ValueError):
    """
    Could not line up input sources to make an actual valid execution.
    """

    def __init__(self, name: str, task_ref: int) -> None:
        super().__init__(
            f"Task {name!r}: input sources from task {task_ref!r} have "
            f"no coincident applicable element iterations. Consider setting "
            f"the element set (or task) argument "
            f"`allow_non_coincident_task_sources` to `True`, which will "
            f"allow for input sources from the same task to use different "
            f"(non-coinciding) subsets of element iterations from the "
            f"source task."
        )


class TaskTemplateInvalidNesting(ValueError):
    """
    Invalid nesting in a task template.
    """

    def __init__(self, key: str, value: float) -> None:
        super().__init__(
            f"`nesting_order` must be >=0 for all keys, but for key {key!r}, value "
            f"of {value!r} was specified."
        )


class TaskSchemaSpecValidationError(Exception):
    """
    A task schema failed to validate.
    """

    # FIXME: never used


class WorkflowSpecValidationError(Exception):
    """
    A workflow failed to validate.
    """

    # FIXME: never used


class InputSourceValidationError(Exception):
    """
    An input source failed to validate.
    """

    # FIXME: never used


class EnvironmentSpecValidationError(Exception):
    """
    An environment specification failed to validate.
    """

    # FIXME: never used


class ParameterSpecValidationError(Exception):
    """
    A parameter specification failed to validate.
    """

    # FIXME: never used


class FileSpecValidationError(Exception):
    """
    A file specification failed to validate.
    """

    # FIXME: never used


class DuplicateExecutableError(ValueError):
    """
    The same executable was present twice in an executable environment.
    """

    def __init__(self, duplicate_labels: list) -> None:
        super().__init__(
            f"Executables must have unique `label`s within each environment, but "
            f"found label(s) multiple times: {duplicate_labels!r}"
        )


class MissingCompatibleActionEnvironment(Exception):
    """
    Could not find a compatible action environment.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(f"No compatible environment is specified for the {msg}.")


class MissingActionEnvironment(Exception):
    """
    Could not find an action environment.
    """

    # FIXME: never used


class ActionEnvironmentMissingNameError(Exception):
    """
    An action environment was missing its name.
    """

    def __init__(self, environment: Mapping[str, Any]) -> None:
        super().__init__(
            "The action-environment environment specification must include a string "
            "`name` key, or be specified as string that is that name. Provided "
            f"environment key was {environment!r}."
        )


class FromSpecMissingObjectError(Exception):
    """
    Missing object when deserialising from specification.
    """

    # FIXME: never used


class TaskSchemaMissingParameterError(Exception):
    """
    Parameter was missing from task schema.
    """

    # FIXME: never used


class ToJSONLikeChildReferenceError(Exception):
    """
    Failed to generate or reference a child object when converting to JSON.
    """

    # FIXME: never thrown


class InvalidInputSourceTaskReference(Exception):
    """
    Invalid input source in task reference.
    """

    def __init__(self, input_source: InputSource, task_ref: int | None = None) -> None:
        super().__init__(
            f"Input source {input_source.to_string()!r} cannot refer to the "
            f"outputs of its own task!"
            if task_ref is None
            else f"Input source {input_source.to_string()!r} refers to a missing "
            f"or inaccessible task: {task_ref!r}."
        )


class WorkflowNotFoundError(Exception):
    """
    Could not find the workflow.
    """

    def __init__(self, path, fs) -> None:
        super().__init__(
            f"Cannot infer a store format at path {path!r} with file system {fs!r}."
        )


class MalformedWorkflowError(Exception):
    """
    Workflow was a malformed document.
    """

    # TODO: use this class


class ValuesAlreadyPersistentError(Exception):
    """
    Trying to make a value persistent that already is so.
    """

    # FIXME: never used


class MalformedParameterPathError(ValueError):
    """
    The path to a parameter was ill-formed.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class MalformedNestingOrderPath(ValueError):
    """
    A nesting order path was ill-formed.
    """

    def __init__(self, k: str, allowed_nesting_paths: Iterable[str]) -> None:
        super().__init__(
            f"Element set: nesting order path {k!r} not understood. Each key in "
            f"`nesting_order` must be start with one of "
            f"{sorted(allowed_nesting_paths)!r}."
        )


class UnknownResourceSpecItemError(ValueError):
    """
    A resource specification item was not found.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class WorkflowParameterMissingError(AttributeError):
    """
    A parameter to a workflow was missing.
    """

    # FIXME: never thrown


class WorkflowBatchUpdateFailedError(Exception):
    """
    An update to a workflow failed.
    """

    # FIXME: only throw is commented out?


class WorkflowLimitsError(ValueError):
    """
    Workflow hit limits.
    """

    # FIXME: never used


class UnsetParameterDataErrorBase(Exception):
    """
    Exceptions related to attempts to retrieve unset parameters.
    """


class UnsetParameterDataError(UnsetParameterDataErrorBase):
    """
    Tried to read from an unset parameter.
    """

    def __init__(self, path: str | None, path_i: str) -> None:
        super().__init__(
            f"Element data path {path!r} resolves to unset data for "
            f"(at least) data-index path: {path_i!r}."
        )


class UnsetParameterFractionLimitExceededError(UnsetParameterDataErrorBase):
    """
    Given the specified `allow_failed_dependencies`, the fraction of failed dependencies
    (unset parameter data) is too high."""

    def __init__(
        self,
        schema_inp: SchemaInput,
        task: WorkflowTask,
        unset_fraction: float,
        log: Logger | None = None,
    ):
        msg = (
            f"Input {schema_inp.parameter.typ!r} of task {task.name!r}: higher "
            f"proportion of dependencies failed ({unset_fraction!r}) than allowed "
            f"({schema_inp.allow_failed_dependencies!r})."
        )
        if log:
            log.info(msg)
        super().__init__(msg)


class UnsetParameterNumberLimitExceededError(UnsetParameterDataErrorBase):
    """
    Given the specified `allow_failed_dependencies`, the number of failed dependencies
    (unset parameter data) is too high."""

    def __init__(
        self,
        schema_inp: SchemaInput,
        task: WorkflowTask,
        unset_num: int,
        log: Logger | None = None,
    ):
        msg = (
            f"Input {schema_inp.parameter.typ!r} of task {task.name!r}: higher number of "
            f"dependencies failed ({unset_num!r}) than allowed "
            f"({schema_inp.allow_failed_dependencies!r})."
        )
        if log:
            log.info(msg)
        super().__init__(msg)


class LoopAlreadyExistsError(Exception):
    """
    A particular loop (or its name) already exists.
    """

    def __init__(self, loop_name: str, loops: WorkflowLoopList) -> None:
        super().__init__(
            f"A loop with the name {loop_name!r} already exists in the workflow: "
            f"{getattr(loops, loop_name)!r}."
        )


class LoopTaskSubsetError(ValueError):
    """
    Problem constructing a subset of a task for a loop.
    """

    def __init__(self, loop_name: str, task_indices: Sequence[int]) -> None:
        super().__init__(
            f"Loop {loop_name!r}: task subset must be an ascending contiguous range, "
            f"but specified task indices were: {task_indices!r}."
        )


class SchedulerVersionsFailure(RuntimeError):
    """We couldn't get the scheduler and or shell versions."""

    # FIXME: unused

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class JobscriptSubmissionFailure(RuntimeError):
    """
    A job script could not be submitted to the scheduler.
    """

    def __init__(
        self,
        message: str,
        *,
        submit_cmd: list[str],
        js_idx: int,
        js_path: str,
        stdout: str | None = None,
        stderr: str | None = None,
        subprocess_exc: Exception | None = None,
        job_ID_parse_exc: Exception | None = None,
    ):
        self.message = message
        self.submit_cmd = submit_cmd
        self.js_idx = js_idx
        self.js_path = js_path
        self.stdout = stdout
        self.stderr = stderr
        self.subprocess_exc = subprocess_exc
        self.job_ID_parse_exc = job_ID_parse_exc
        super().__init__(message)


class SubmissionFailure(RuntimeError):
    """
    A job submission failed.
    """

    def __init__(
        self,
        sub_idx: int,
        submitted_js_idx: Sequence[int],
        exceptions: Iterable[JobscriptSubmissionFailure],
    ) -> None:
        msg = f"Some jobscripts in submission index {sub_idx} could not be submitted"
        if submitted_js_idx:
            msg += f" (but jobscripts {submitted_js_idx} were submitted successfully):"
        else:
            msg += ":"

        msg += "\n"
        for sub_err in exceptions:
            msg += (
                f"Jobscript {sub_err.js_idx} at path: {str(sub_err.js_path)!r}\n"
                f"Submit command: {sub_err.submit_cmd!r}.\n"
                f"Reason: {sub_err.message!r}\n"
            )
            if sub_err.subprocess_exc is not None:
                msg += f"Subprocess exception: {sub_err.subprocess_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Subprocess job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.stdout:
                msg += f"Submission stdout:\n{indent(sub_err.stdout, '  ')}\n"
            if sub_err.stderr:
                msg += f"Submission stderr:\n{indent(sub_err.stderr, '  ')}\n"
        self.message = msg
        super().__init__(msg)


class WorkflowSubmissionFailure(RuntimeError):
    """
    A workflow submission failed.
    """

    def __init__(self, exceptions: Sequence[SubmissionFailure]) -> None:
        super().__init__("\n" + "\n\n".join(exn.message for exn in exceptions))


class ResourceValidationError(ValueError):
    """An incompatible resource requested by the user."""


class UnsupportedOSError(ResourceValidationError):
    """This machine is not of the requested OS."""

    def __init__(self, os_name: str) -> None:
        message = (
            f"OS {os_name!r} is not compatible with this machine/instance with OS: "
            f"{os.name!r}."
        )
        super().__init__(message)
        self.os_name = os_name


class UnsupportedShellError(ResourceValidationError):
    """We don't support this shell on this OS."""

    def __init__(self, shell: str, supported: Iterable[str]) -> None:
        sup = set(supported)
        message = (
            f"Shell {shell!r} is not supported on this machine/instance. Supported "
            f"shells are: {sup!r}."
        )
        super().__init__(message)
        self.shell = shell
        self.supported = frozenset(sup)


class UnsupportedSchedulerError(ResourceValidationError):
    """This scheduler is not supported on this machine according to the config.

    This is also raised in config validation when attempting to add a scheduler that is
    not known for this OS.

    """

    def __init__(
        self,
        scheduler: str,
        supported: Iterable[str] | None = None,
        available: Iterable[str] | None = None,
    ) -> None:
        if supported is not None:
            message = (
                f"Scheduler {scheduler!r} is not supported on this machine/instance. "
                f"Supported schedulers according to the app configuration are: "
                f"{supported!r}."
            )
        elif available is not None:
            message = (
                f"Scheduler {scheduler!r} is not supported on this OS. Schedulers "
                f"compatible with this OS are: {available!r}."
            )
        super().__init__(message)
        self.scheduler = scheduler
        self.supported = None if supported is None else tuple(supported)
        self.available = None if available is None else tuple(available)


class UnknownSGEPEError(ResourceValidationError):
    """
    Miscellaneous error from SGE parallel environment.
    """

    def __init__(self, env_name: str, all_env_names: Iterable[str]) -> None:
        super().__init__(
            f"The SGE parallel environment {env_name!r} is not "
            f"specified in the configuration. Specified parallel environments "
            f"are {sorted(all_env_names)!r}."
        )


class IncompatibleSGEPEError(ResourceValidationError):
    """
    The SGE parallel environment selected is incompatible.
    """

    def __init__(self, env_name: str, num_cores: int | None) -> None:
        super().__init__(
            f"The SGE parallel environment {env_name!r} is not "
            f"compatible with the number of cores requested: "
            f"{num_cores!r}."
        )


class NoCompatibleSGEPEError(ResourceValidationError):
    """
    No SGE parallel environment is compatible with request.
    """

    def __init__(self, num_cores: int | None) -> None:
        super().__init__(
            f"No compatible SGE parallel environment could be found for the "
            f"specified `num_cores` ({num_cores!r})."
        )


class IncompatibleParallelModeError(ResourceValidationError):
    """
    The parallel mode is incompatible.
    """

    def __init__(self, parallel_mode: ParallelMode) -> None:
        super().__init__(
            f"For the {parallel_mode.name.lower()} parallel mode, "
            f"only a single node may be requested."
        )


class UnknownSLURMPartitionError(ResourceValidationError):
    """
    The requested SLURM partition isn't known.
    """

    def __init__(self, part_name: str, all_parts: Iterable[str]) -> None:
        super().__init__(
            f"The SLURM partition {part_name!r} is not "
            f"specified in the configuration. Specified partitions are "
            f"{sorted(all_parts)!r}."
        )


class IncompatibleSLURMPartitionError(ResourceValidationError):
    """
    The requested SLURM partition is incompatible.
    """

    def __init__(self, part_name: str, attr_kind: str, value) -> None:
        super().__init__(
            f"The SLURM partition {part_name!r} is not "
            f"compatible with the {attr_kind} requested: {value!r}."
        )


class IncompatibleSLURMArgumentsError(ResourceValidationError):
    """
    The SLURM arguments are incompatible with each other.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class _MissingStoreItemError(ValueError):
    def __init__(self, id_lst: Iterable[int], item_type: str) -> None:
        message = (
            f"Store {item_type}s with the following IDs do not all exist: {id_lst!r}"
        )
        super().__init__(message)
        self.id_lst = id_lst


class MissingStoreTaskError(_MissingStoreItemError):
    """Some task IDs do not exist."""

    _item_type = "task"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreElementError(_MissingStoreItemError):
    """Some element IDs do not exist."""

    _item_type = "element"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreElementIterationError(_MissingStoreItemError):
    """Some element iteration IDs do not exist."""

    _item_type = "element iteration"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreEARError(_MissingStoreItemError):
    """Some EAR IDs do not exist."""

    _item_type = "EAR"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingParameterData(_MissingStoreItemError):
    """Some parameter IDs do not exist"""

    _item_type = "parameter"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class ParametersMetadataReadOnlyError(RuntimeError):
    pass


class NotSubmitMachineError(RuntimeError):
    """
    The requested machine can't be submitted to.
    """

    def __init__(self) -> None:
        super().__init__(
            "Cannot get active state of the jobscript because the current machine "
            "is not the machine on which the jobscript was submitted."
        )


class RunNotAbortableError(ValueError):
    """
    Cannot abort the run.
    """

    def __init__(self) -> None:
        super().__init__(
            "The run is not defined as abortable in the task schema, so it cannot "
            "be aborted."
        )


class NoCLIFormatMethodError(AttributeError):
    """
    Some CLI class lacks a format method
    """

    def __init__(self, method: str, inp_val: object) -> None:
        super().__init__(
            f"No CLI format method {method!r} exists for the object {inp_val!r}."
        )


class ContainerKeyError(KeyError):
    """
    A key could not be mapped in a container.

    Parameters
    ----------
    path:
        The path whose resolution failed.
    """

    def __init__(self, path: list[str]) -> None:
        self.path = path
        super().__init__()


class MayNeedObjectError(Exception):
    """
    An object is needed but not present.

    Parameters
    ----------
    path:
        The path whose resolution failed.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__()


class NoAvailableElementSetsError(Exception):
    """
    No element set is available.
    """

    def __init__(self) -> None:
        super().__init__()


class OutputFileParserNoOutputError(ValueError):
    """
    There was no output for the output file parser to parse.
    """

    def __init__(self) -> None:
        super().__init__()


class SubmissionEnvironmentError(ValueError):
    """
    Raised when submitting a workflow on a machine without a compatible environment.
    """


def _spec_to_ref(env_spec: Mapping[str, Any]):
    non_name_spec = {k: v for k, v in env_spec.items() if k != "name"}
    spec_str = f" with specifiers {non_name_spec!r}" if non_name_spec else ""
    return f"{env_spec['name']!r}{spec_str}"


class MissingEnvironmentExecutableError(SubmissionEnvironmentError):
    """
    The environment does not have the requested executable at all.
    """

    def __init__(self, env_spec: Mapping[str, Any], exec_label: str) -> None:
        super().__init__(
            f"The environment {_spec_to_ref(env_spec)} as defined on this machine has no "
            f"executable labelled {exec_label!r}, which is required for this "
            f"submission, so the submission cannot be created."
        )


class MissingEnvironmentExecutableInstanceError(SubmissionEnvironmentError):
    """
    The environment does not have a suitable instance of the requested executable.
    """

    def __init__(
        self, env_spec: Mapping[str, Any], exec_label: str, js_idx: int, res: dict
    ) -> None:
        super().__init__(
            f"No matching executable instances found for executable "
            f"{exec_label!r} of environment {_spec_to_ref(env_spec)} for jobscript "
            f"index {js_idx!r} with requested resources {res!r}."
        )


class MissingEnvironmentError(SubmissionEnvironmentError):
    """
    There is no environment with that name.
    """

    def __init__(self, env_spec: Mapping[str, Any]) -> None:
        super().__init__(
            f"The environment {_spec_to_ref(env_spec)} is not defined on this machine, so the "
            f"submission cannot be created."
        )


class UnsupportedScriptDataFormat(ValueError):
    """
    That format of script data is not supported.
    """

    def __init__(
        self, data: ScriptData, kind: str, name: str, formats: tuple[str, ...]
    ) -> None:
        super().__init__(
            f"Script data format {data!r} for {kind} parameter {name!r} is not "
            f"understood. Available script data formats are: "
            f"{formats!r}."
        )


class UnknownScriptDataParameter(ValueError):
    """
    Unknown parameter in script data.
    """

    def __init__(self, name: str, kind: str, param_names: Sequence[str]) -> None:
        super().__init__(
            f"Script data parameter {name!r} is not a known parameter of the "
            f"action. Parameters ({kind}) are: {param_names!r}."
        )


class UnknownScriptDataKey(ValueError):
    """
    Unknown key in script data.
    """

    def __init__(self, key: str, allowed_keys: Sequence[str]) -> None:
        super().__init__(
            f"Script data key {key!r} is not understood. Allowed keys are: "
            f"{allowed_keys!r}."
        )


class MissingVariableSubstitutionError(KeyError):
    """
    No definition available of a variable being substituted.
    """

    def __init__(self, var_name: str, variables: Iterable[str]) -> None:
        super().__init__(
            f"The variable {var_name!r} referenced in the string does not match "
            f"any of the provided variables: {sorted(variables)!r}."
        )


class EnvironmentPresetUnknownEnvironmentError(ValueError):
    """
    An environment preset could not be resolved to an execution environment.
    """

    def __init__(self, name: str, bad_envs: Iterable[str]) -> None:
        super().__init__(
            f"Task schema {name} has environment presets that refer to one "
            f"or more environments that are not referenced in any of the task "
            f"schema's actions: {', '.join(f'{env!r}' for env in sorted(bad_envs))}."
        )


class UnknownEnvironmentPresetError(ValueError):
    """
    An execution environment was unknown.
    """

    def __init__(self, preset_name: str, schema_name: str) -> None:
        super().__init__(
            f"There is no environment preset named {preset_name!r} defined "
            f"in the task schema {schema_name}."
        )


class MultipleEnvironmentsError(ValueError):
    """
    Multiple applicable execution environments exist.
    """

    def __init__(self, env_spec: Mapping[str, Any]) -> None:
        super().__init__(
            f"Multiple environments {_spec_to_ref(env_spec)} are defined on this machine."
        )


class MissingElementGroup(ValueError):
    """
    An element group should exist but doesn't.
    """

    def __init__(self, task_name: str, group_name: str, input_path: str) -> None:
        super().__init__(
            f"Adding elements to task {task_name!r}: "
            f"no element group named {group_name!r} found for input {input_path!r}."
        )
