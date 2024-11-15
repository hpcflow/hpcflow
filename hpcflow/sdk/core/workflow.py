"""
Main workflow model.
"""

from __future__ import annotations
from collections import defaultdict
from contextlib import contextmanager, nullcontext
import copy
from dataclasses import dataclass, field

from pathlib import Path
import random
import string
from threading import Thread
import time
from typing import overload, cast, TYPE_CHECKING
from uuid import uuid4
from warnings import warn
from fsspec.implementations.local import LocalFileSystem  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
import numpy as np
from fsspec.core import url_to_fs  # type: ignore
import rich.console

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS, ABORT_EXIT_CODE
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.loop_cache import LoopCache, LoopIndex
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.persistence import store_cls_from_str
from hpcflow.sdk.persistence.defaults import DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, infer_store
from hpcflow.sdk.submission.jobscript import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
    jobscripts_to_list,
    merge_jobscripts_across_tasks,
    resolve_jobscript_dependencies,
)
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import (
    read_JSON_file,
    read_JSON_string,
    read_YAML_str,
    read_YAML_file,
    replace_items,
    current_timestamp,
    normalise_timestamp,
    parse_timestamp,
)
from hpcflow.sdk.core.errors import (
    InvalidInputSourceTaskReference,
    LoopAlreadyExistsError,
    OutputFileParserNoOutputError,
    RunNotAbortableError,
    SubmissionFailure,
    WorkflowSubmissionFailure,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from contextlib import AbstractContextManager
    from typing import Any, ClassVar, Literal
    from typing_extensions import Self, TypeAlias
    from numpy.typing import NDArray
    import psutil
    from rich.status import Status
    from ..typing import DataIndex, ParamSource, PathLike, TemplateComponents
    from .actions import ElementActionRun
    from .element import Element, ElementIteration
    from .loop import Loop, WorkflowLoop
    from .object_list import ObjectList, ResourceList, WorkflowLoopList, WorkflowTaskList
    from .parameters import InputSource, ResourceSpec
    from .task import Task, WorkflowTask
    from .types import (
        AbstractFileSystem,
        CreationInfo,
        Pending,
        Resources,
        WorkflowTemplateTaskData,
    )
    from ..submission.submission import Submission
    from ..submission.jobscript import (
        Jobscript,
        JobScriptDescriptor,
        JobScriptCreationArguments,
    )
    from ..persistence.base import (
        StoreElement,
        StoreElementIter,
        StoreTask,
        StoreParameter,
        StoreEAR,
    )
    from ..persistence.types import TemplateMeta

    #: Convenience alias
    _TemplateComponents: TypeAlias = "dict[str, ObjectList[JSONLike]]"


@dataclass
class _Pathway:
    id_: int
    names: LoopIndex[str, int] = field(default_factory=LoopIndex)
    iter_ids: list[int] = field(default_factory=list)
    data_idx: list[DataIndex] = field(default_factory=list)

    def as_tuple(
        self, *, ret_iter_IDs: bool = False, ret_data_idx: bool = False
    ) -> tuple:
        if ret_iter_IDs:
            if ret_data_idx:
                return (self.id_, self.names, tuple(self.iter_ids), tuple(self.data_idx))
            else:
                return (self.id_, self.names, tuple(self.iter_ids))
        else:
            if ret_data_idx:
                return (self.id_, self.names, tuple(self.data_idx))
            else:
                return (self.id_, self.names)

    def __deepcopy__(self, memo) -> Self:
        return self.__class__(
            self.id_,
            self.names,
            copy.deepcopy(self.iter_ids, memo),
            copy.deepcopy(self.data_idx, memo),
        )


@dataclass
@hydrate
class WorkflowTemplate(JSONLike):
    """Class to represent initial parametrisation of a {app_name} workflow, with limited
    validation logic.

    Parameters
    ----------
    name:
        A string name for the workflow. By default this name will be used in combination
        with a date-time stamp when generating a persistent workflow from the template.
    tasks: list[~hpcflow.app.Task]
        A list of Task objects to include in the workflow.
    loops: list[~hpcflow.app.Loop]
        A list of Loop objects to include in the workflow.
    workflow:
        The associated concrete workflow.
    resources: dict[str, dict] | list[~hpcflow.app.ResourceSpec] | ~hpcflow.app.ResourceList
        Template-level resources to apply to all tasks as default values. This can be a
        dict that maps action scopes to resources (e.g. `{{"any": {{"num_cores": 2}}}}`)
        or a list of `ResourceSpec` objects, or a `ResourceList` object.
    environments:
        The execution environments to use.
    env_presets:
        The environment presets to use.
    source_file:
        The file this was derived from.
    store_kwargs:
        Additional arguments to pass to the persistent data store constructor.
    merge_resources:
        If True, merge template-level `resources` into element set resources. If False,
        template-level resources are ignored.
    merge_envs:
        Whether to merge the environemtns into task resources.
    """

    _validation_schema: ClassVar[str] = "workflow_spec_schema.yaml"

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="tasks",
            class_name="Task",
            is_multiple=True,
            parent_ref="workflow_template",
        ),
        ChildObjectSpec(
            name="loops",
            class_name="Loop",
            is_multiple=True,
            parent_ref="_workflow_template",
        ),
        ChildObjectSpec(
            name="resources",
            class_name="ResourceList",
            parent_ref="_workflow_template",
        ),
    )

    #: A string name for the workflow.
    name: str
    #: Documentation information.
    doc: list[str] | str | None = field(repr=False, default=None)
    #: A list of Task objects to include in the workflow.
    tasks: list[Task] = field(default_factory=list)
    #: A list of Loop objects to include in the workflow.
    loops: list[Loop] = field(default_factory=list)
    #: The associated concrete workflow.
    workflow: Workflow | None = None
    #: Template-level resources to apply to all tasks as default values.
    resources: Resources = None
    #: The execution environments to use.
    environments: Mapping[str, Mapping[str, Any]] | None = None
    #: The environment presets to use.
    env_presets: str | list[str] | None = None
    #: The file this was derived from.
    source_file: str | None = field(default=None, compare=False)
    #: Additional arguments to pass to the persistent data store constructor.
    store_kwargs: dict[str, Any] = field(default_factory=dict)
    #: Whether to merge template-level `resources` into element set resources.
    merge_resources: bool = True
    #: Whether to merge the environemtns into task resources.
    merge_envs: bool = True

    def __post_init__(self) -> None:
        resources = self._app.ResourceList.normalise(self.resources)
        self.resources = resources
        self._set_parent_refs()

        # merge template-level `resources` into task element set resources (this mutates
        # `tasks`, and should only happen on creation of the workflow template, not on
        # re-initialisation from a persistent workflow):
        if self.merge_resources:
            for task in self.tasks:
                for element_set in task.element_sets:
                    element_set.resources.merge_other(resources)
            self.merge_resources = False

        if self.merge_envs:
            self._merge_envs_into_task_resources()

        if self.doc and not isinstance(self.doc, list):
            self.doc = [self.doc]

    @property
    def _resources(self) -> ResourceList:
        res = self.resources
        assert isinstance(res, self._app.ResourceList)
        return res

    def _get_resources_copy(self) -> Iterator[ResourceSpec]:
        """
        Get a deep copy of the list of resources.
        """
        memo: dict[int, Any] = {}
        for spec in self._resources:
            yield copy.deepcopy(spec, memo)

    def _merge_envs_into_task_resources(self) -> None:
        self.merge_envs = False

        # disallow both `env_presets` and `environments` specifications:
        if self.env_presets and self.environments:
            raise ValueError(
                "Workflow template: specify at most one of `env_presets` and "
                "`environments`."
            )

        if not isinstance(self.env_presets, list):
            self.env_presets = [self.env_presets] if self.env_presets else []

        for task in self.tasks:
            # get applicable environments and environment preset names:
            try:
                schema = task.schema
            except ValueError:
                # TODO: consider multiple schemas
                raise NotImplementedError(
                    "Cannot merge environment presets into a task without multiple "
                    "schemas."
                )
            schema_presets = schema.environment_presets
            app_envs = {act.get_environment_name() for act in schema.actions}
            for es in task.element_sets:
                app_env_specs_i: Mapping[str, Mapping[str, Any]] | None = None
                if not es.environments and not es.env_preset:
                    # no task level envs/presets specified, so merge template-level:
                    if self.environments:
                        app_env_specs_i = {
                            k: v for k, v in self.environments.items() if k in app_envs
                        }
                        if app_env_specs_i:
                            self._app.logger.info(
                                f"(task {task.name!r}, element set {es.index}): using "
                                f"template-level requested `environment` specifiers: "
                                f"{app_env_specs_i!r}."
                            )
                            es.environments = app_env_specs_i

                    elif self.env_presets and schema_presets:
                        # take only the first applicable preset:
                        for app_preset in self.env_presets:
                            if app_preset in schema_presets:
                                es.env_preset = app_preset
                                app_env_specs_i = schema_presets[app_preset]
                                self._app.logger.info(
                                    f"(task {task.name!r}, element set {es.index}): using "
                                    f"template-level requested {app_preset!r} "
                                    f"`env_preset`: {app_env_specs_i!r}."
                                )
                                break

                    else:
                        # no env/preset applicable here (and no env/preset at task level),
                        # so apply a default preset if available:
                        if app_env_specs_i := (schema_presets or {}).get("", None):
                            self._app.logger.info(
                                f"(task {task.name!r}, element set {es.index}): setting "
                                f"to default (empty-string named) `env_preset`: "
                                f"{app_env_specs_i}."
                            )
                            es.env_preset = ""

                    if app_env_specs_i:
                        es.resources.merge_one(
                            self._app.ResourceSpec(
                                scope="any", environments=app_env_specs_i
                            )
                        )

    @classmethod
    @TimeIt.decorator
    def _from_data(cls, data: dict[str, Any]) -> WorkflowTemplate:
        task_dat: WorkflowTemplateTaskData
        # use element_sets if not already:
        for task_idx, task_dat in enumerate(data["tasks"]):
            schema = task_dat.pop("schema")
            schema_list: list = schema if isinstance(schema, list) else [schema]
            if "element_sets" in task_dat:
                # just update the schema to a list:
                data["tasks"][task_idx]["schema"] = schema_list
            else:
                # add a single element set, and update the schema to a list:
                out_labels = task_dat.pop("output_labels", [])
                data["tasks"][task_idx] = {
                    "schema": schema_list,
                    "element_sets": [task_dat],
                    "output_labels": out_labels,
                }

        # extract out any template components:
        # TODO: TypedDict for data
        tcs: dict[str, list] = data.pop("template_components", {})
        if params_dat := tcs.pop("parameters", []):
            parameters = cls._app.ParametersList.from_json_like(
                params_dat, shared_data=cls._app._shared_data
            )
            cls._app.parameters.add_objects(parameters, skip_duplicates=True)

        if cmd_files_dat := tcs.pop("command_files", []):
            cmd_files = cls._app.CommandFilesList.from_json_like(
                cmd_files_dat, shared_data=cls._app._shared_data
            )
            cls._app.command_files.add_objects(cmd_files, skip_duplicates=True)

        if envs_dat := tcs.pop("environments", []):
            envs = cls._app.EnvironmentsList.from_json_like(
                envs_dat, shared_data=cls._app._shared_data
            )
            cls._app.envs.add_objects(envs, skip_duplicates=True)

        if ts_dat := tcs.pop("task_schemas", []):
            task_schemas = cls._app.TaskSchemasList.from_json_like(
                ts_dat, shared_data=cls._app._shared_data
            )
            cls._app.task_schemas.add_objects(task_schemas, skip_duplicates=True)

        return cls.from_json_like(data, shared_data=cls._app._shared_data)

    @classmethod
    @TimeIt.decorator
    def from_YAML_string(
        cls,
        string: str,
        variables: dict[str, str] | None = None,
    ) -> WorkflowTemplate:
        """Load from a YAML string.

        Parameters
        ----------
        string
            The YAML string containing the workflow template parametrisation.
        variables
            String variables to substitute in `string`.
        """
        return cls._from_data(read_YAML_str(string, variables=variables))

    @classmethod
    def _check_name(cls, data: dict[str, Any], path: PathLike) -> None:
        """Check the workflow template data has a "name" key. If not, add a "name" key,
        using the file path stem.

        Note: this method mutates `data`.

        """
        if "name" not in data and path is not None:
            name = Path(path).stem
            cls._app.logger.info(
                f"using file name stem ({name!r}) as the workflow template name."
            )
            data["name"] = name

    @classmethod
    @TimeIt.decorator
    def from_YAML_file(
        cls,
        path: PathLike,
        variables: dict[str, str] | None = None,
    ) -> WorkflowTemplate:
        """Load from a YAML file.

        Parameters
        ----------
        path
            The path to the YAML file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`.

        """
        cls._app.logger.debug("parsing workflow template from a YAML file")
        data = read_YAML_file(path, variables=variables)
        cls._check_name(data, path)
        data["source_file"] = str(path)
        return cls._from_data(data)

    @classmethod
    @TimeIt.decorator
    def from_JSON_string(
        cls,
        string: str,
        variables: dict[str, str] | None = None,
    ) -> WorkflowTemplate:
        """Load from a JSON string.

        Parameters
        ----------
        string
            The JSON string containing the workflow template parametrisation.
        variables
            String variables to substitute in `string`.
        """
        return cls._from_data(read_JSON_string(string, variables=variables))

    @classmethod
    @TimeIt.decorator
    def from_JSON_file(
        cls,
        path: PathLike,
        variables: dict[str, str] | None = None,
    ) -> WorkflowTemplate:
        """Load from a JSON file.

        Parameters
        ----------
        path
            The path to the JSON file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`.
        """
        cls._app.logger.debug("parsing workflow template from a JSON file")
        data = read_JSON_file(path, variables=variables)
        cls._check_name(data, path)
        data["source_file"] = str(path)
        return cls._from_data(data)

    @classmethod
    @TimeIt.decorator
    def from_file(
        cls,
        path: PathLike,
        template_format: Literal["yaml", "json"] | None = None,
        variables: dict[str, str] | None = None,
    ) -> WorkflowTemplate:
        """Load from either a YAML or JSON file, depending on the file extension.

        Parameters
        ----------
        path
            The path to the file containing the workflow template parametrisation.
        template_format
            The file format to expect at `path`. One of "json" or "yaml", if specified. By
            default, "yaml".
        variables
            String variables to substitute in the file given by `path`.

        """
        path_ = Path(path or ".")
        fmt = template_format.lower() if template_format else None
        if fmt == "yaml" or path_.suffix in (".yaml", ".yml"):
            return cls.from_YAML_file(path_, variables=variables)
        elif fmt == "json" or path_.suffix in (".json", ".jsonc"):
            return cls.from_JSON_file(path_, variables=variables)
        else:
            raise ValueError(
                f"Unknown workflow template file extension {path_.suffix!r}. Supported "
                f"template formats are {ALL_TEMPLATE_FORMATS!r}."
            )

    def _add_empty_task(self, task: Task, new_index: int, insert_ID: int) -> None:
        """Called by `Workflow._add_empty_task`."""
        assert self.workflow
        new_task_name = self.workflow._get_new_task_unique_name(task, new_index)

        task._insert_ID = insert_ID
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"
        task._element_sets = []  # element sets are added to the Task during add_elements

        task.workflow_template = self
        self.tasks.insert(new_index, task)

    def _add_empty_loop(self, loop: Loop) -> None:
        """Called by `Workflow._add_empty_loop`."""

        assert self.workflow
        if not loop.name:
            existing = {loop.name for loop in self.loops}
            new_idx = len(self.loops)
            while (name := f"loop_{new_idx}") in existing:
                new_idx += 1
            loop._name = name
        elif loop.name in self.workflow.loops.list_attrs():
            raise LoopAlreadyExistsError(loop.name, self.workflow.loops)

        loop._workflow_template = self
        self.loops.append(loop)


def resolve_fsspec(
    path: PathLike, **kwargs
) -> tuple[AbstractFileSystem, str, str | None]:
    """
    Decide how to handle a particular virtual path.

    Parameters
    ----------
    kwargs
        This can include a `password` key, for connections via SSH.

    """

    path_s = str(path)
    fs: AbstractFileSystem
    if path_s.endswith(".zip"):
        # `url_to_fs` does not seem to work for zip combos e.g. `zip::ssh://`, so we
        # construct a `ZipFileSystem` ourselves and assume it is signified only by the
        # file extension:
        fs, pw = ask_pw_on_auth_exc(
            ZipFileSystem,
            fo=path_s,
            mode="r",
            target_options=kwargs or {},
            add_pw_to="target_options",
        )
        path_s = ""

    else:
        (fs, path_s), pw = ask_pw_on_auth_exc(url_to_fs, path_s, **kwargs)
        path_s = str(Path(path_s).as_posix())
        if isinstance(fs, LocalFileSystem):
            path_s = str(Path(path_s).resolve())

    return fs, path_s, pw


@dataclass(frozen=True)
class _IterationData:
    id_: int
    idx: int


class Workflow(AppAware):
    """
    A concrete workflow.

    Parameters
    ----------
    workflow_ref:
        Either the path to a persistent workflow, or an integer that will interpreted
        as the local ID of a workflow submission, as reported by the app `show`
        command.
    store_fmt:
        The format of persistent store to use. Used to select the store manager class.
    fs_kwargs:
        Additional arguments to pass when resolving a virtual workflow reference.
    kwargs:
        For compatibility during pre-stable development phase.
    """

    _default_ts_fmt: ClassVar[str] = r"%Y-%m-%d %H:%M:%S.%f"
    _default_ts_name_fmt: ClassVar[str] = r"%Y-%m-%d_%H%M%S"
    _input_files_dir_name: ClassVar[str] = "input_files"
    _exec_dir_name: ClassVar[str] = "execute"

    def __init__(
        self,
        workflow_ref: str | Path | int,
        store_fmt: str | None = None,
        fs_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        if isinstance(workflow_ref, int):
            path = self._app._get_workflow_path_from_local_ID(workflow_ref)
        elif isinstance(workflow_ref, str):
            path = Path(workflow_ref)
        else:
            path = workflow_ref

        self._app.logger.info(f"loading workflow from path: {path}")
        fs_path = str(path)
        fs, path_s, _ = resolve_fsspec(path, **(fs_kwargs or {}))
        store_fmt = store_fmt or infer_store(fs_path, fs)
        store_cls = store_cls_from_str(store_fmt)

        self.path = path_s

        # assigned on first access:
        self._ts_fmt: str | None = None
        self._ts_name_fmt: str | None = None
        self._creation_info: CreationInfo | None = None
        self._name: str | None = None
        self._template: WorkflowTemplate | None = None
        self._template_components: TemplateComponents | None = None
        self._tasks: WorkflowTaskList | None = None
        self._loops: WorkflowLoopList | None = None
        self._submissions: list[Submission] | None = None

        self._store = store_cls(self._app, self, self.path, fs)
        self._in_batch_mode = False  # flag to track when processing batch updates

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    def reload(self) -> Self:
        """Reload the workflow from disk."""
        return self.__class__(self.url)

    @property
    def name(self) -> str:
        """
        The name of the workflow.

        The workflow name may be different from the template name, as it includes the
        creation date-timestamp if generated.
        """
        if not self._name:
            self._name = self._store.get_name()
        return self._name

    @property
    def url(self) -> str:
        """An fsspec URL for this workflow."""
        if self._store.fs:
            if self._store.fs.protocol == "zip":
                return self._store.fs.of.path
            elif self._store.fs.protocol == "file":
                return self.path
        raise NotImplementedError("Only (local) zip and local URLs provided for now.")

    @property
    def store_format(self) -> str:
        """
        The format of the workflow's persistent store.
        """
        return self._store._name

    @classmethod
    @TimeIt.decorator
    def from_template(
        cls,
        template: WorkflowTemplate,
        path: PathLike = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        status: Status | None = None,
    ) -> Workflow:
        """Generate from a `WorkflowTemplate` object.

        Parameters
        ----------
        template:
            The WorkflowTemplate object to make persistent.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        """
        if status:
            status.update("Generating empty workflow...")
        try:
            wk = cls._write_empty_workflow(
                template=template,
                path=path,
                name=name,
                overwrite=overwrite,
                store=store,
                ts_fmt=ts_fmt,
                ts_name_fmt=ts_name_fmt,
                store_kwargs=store_kwargs,
            )
            with wk._store.cached_load(), wk.batch_update(
                is_workflow_creation=True
            ), wk._store.cache_ctx():
                for idx, task in enumerate(template.tasks):
                    if status:
                        status.update(
                            f"Adding task {idx + 1}/{len(template.tasks)} "
                            f"({task.name!r})..."
                        )
                    wk._add_task(task)
                if status:
                    status.update(f"Preparing to add {len(template.loops)} loops...")
                if template.loops:
                    # TODO: if loop with non-initialisable actions, will fail
                    cache = LoopCache.build(workflow=wk, loops=template.loops)
                    for idx, loop in enumerate(template.loops):
                        if status:
                            status.update(
                                f"Adding loop {idx + 1}/"
                                f"{len(template.loops)} ({loop.name!r})"
                            )
                        wk._add_loop(loop, cache=cache, status=status)
        except Exception:
            if status:
                status.stop()
            raise
        return wk

    @classmethod
    @TimeIt.decorator
    def from_YAML_file(
        cls,
        YAML_path: PathLike,
        path: PathLike = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
    ) -> Workflow:
        """Generate from a YAML file.

        Parameters
        ----------
        YAML_path:
            The path to a workflow template in the YAML file format.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables:
            String variables to substitute in the file given by `YAML_path`.
        """
        template = cls._app.WorkflowTemplate.from_YAML_file(
            path=YAML_path,
            variables=variables,
        )
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
        )

    @classmethod
    def from_YAML_string(
        cls,
        YAML_str: str,
        path: PathLike = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
    ) -> Workflow:
        """Generate from a YAML string.

        Parameters
        ----------
        YAML_str:
            The YAML string containing a workflow template parametrisation.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables:
            String variables to substitute in the string `YAML_str`.
        """
        template = cls._app.WorkflowTemplate.from_YAML_string(
            string=YAML_str,
            variables=variables,
        )
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
        )

    @classmethod
    def from_JSON_file(
        cls,
        JSON_path: PathLike,
        path: PathLike = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        status: Status | None = None,
    ) -> Workflow:
        """Generate from a JSON file.

        Parameters
        ----------
        JSON_path:
            The path to a workflow template in the JSON file format.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables:
            String variables to substitute in the file given by `JSON_path`.
        """
        template = cls._app.WorkflowTemplate.from_JSON_file(
            path=JSON_path,
            variables=variables,
        )
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
            status,
        )

    @classmethod
    def from_JSON_string(
        cls,
        JSON_str: str,
        path: PathLike = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        status: Status | None = None,
    ) -> Workflow:
        """Generate from a JSON string.

        Parameters
        ----------
        JSON_str:
            The JSON string containing a workflow template parametrisation.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables:
            String variables to substitute in the string `JSON_str`.
        """
        template = cls._app.WorkflowTemplate.from_JSON_string(
            string=JSON_str,
            variables=variables,
        )
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
            status,
        )

    @classmethod
    @TimeIt.decorator
    def from_file(
        cls,
        template_path: PathLike,
        template_format: Literal["json", "yaml"] | None = None,
        path: str | None = None,
        name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        status: Status | None = None,
    ) -> Workflow:
        """Generate from either a YAML or JSON file, depending on the file extension.

        Parameters
        ----------
        template_path:
            The path to a template file in YAML or JSON format, and with a ".yml",
            ".yaml", or ".json" extension.
        template_format:
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format regardless of the file extension.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables:
            String variables to substitute in the file given by `template_path`.
        """
        try:
            template = cls._app.WorkflowTemplate.from_file(
                template_path,
                template_format,
                variables=variables,
            )
        except Exception:
            if status:
                status.stop()
            raise
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
            status,
        )

    @classmethod
    @TimeIt.decorator
    def from_template_data(
        cls,
        template_name: str,
        tasks: list[Task] | None = None,
        loops: list[Loop] | None = None,
        resources: Resources = None,
        path: PathLike | None = None,
        workflow_name: str | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
    ) -> Workflow:
        """Generate from the data associated with a WorkflowTemplate object.

        Parameters
        ----------
        template_name:
            Name of the new workflow template, from which the new workflow will be
            generated.
        tasks:
            List of Task objects to add to the new workflow.
        loops:
            List of Loop objects to add to the new workflow.
        resources:
            Mapping of action scopes to resource requirements, to be applied to all
            element sets in the workflow. `resources` specified in an element set take
            precedence of those defined here for the whole workflow.
        path:
            The directory in which the workflow will be generated. The current directory
            if not specified.
        workflow_name:
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified `template_name` will be used, in
            combination with a date-timestamp.
        overwrite:
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store:
            The persistent store to use for this workflow.
        ts_fmt:
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt:
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs:
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        """
        template = cls._app.WorkflowTemplate(
            template_name,
            tasks=tasks or [],
            loops=loops or [],
            resources=resources,
        )
        return cls.from_template(
            template,
            path,
            workflow_name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
            store_kwargs,
        )

    @TimeIt.decorator
    def _add_empty_task(
        self,
        task: Task,
        new_index: int | None = None,
    ) -> WorkflowTask:
        if new_index is None:
            new_index = self.num_tasks

        insert_ID = self.num_added_tasks

        # make a copy with persistent schema inputs:
        task_c, _ = task.to_persistent(self, insert_ID)

        # add to the WorkflowTemplate:
        self.template._add_empty_task(task_c, new_index, insert_ID)

        # create and insert a new WorkflowTask:
        self.tasks.add_object(
            self._app.WorkflowTask.new_empty_task(self, task_c, new_index),
            index=new_index,
        )

        # update persistent store:
        task_js, temp_comps_js = task_c.to_json_like()
        assert temp_comps_js is not None
        self._store.add_template_components(temp_comps_js)
        self._store.add_task(new_index, cast("Mapping", task_js))

        # update in-memory workflow template components:
        temp_comps = cast(
            "_TemplateComponents",
            self._app.template_components_from_json_like(temp_comps_js),
        )
        for comp_type, comps in temp_comps.items():
            ol = self.__template_components[comp_type]
            for comp in comps:
                comp._set_hash()
                if comp not in ol:
                    self._pending["template_components"][comp_type].append(
                        ol.add_object(comp, skip_duplicates=False)
                    )

        self._pending["tasks"].append(new_index)
        return self.tasks[new_index]

    @TimeIt.decorator
    def _add_task(self, task: Task, new_index: int | None = None) -> None:
        new_wk_task = self._add_empty_task(task=task, new_index=new_index)
        new_wk_task._add_elements(element_sets=task.element_sets, propagate_to={})

    def add_task(self, task: Task, new_index: int | None = None) -> None:
        """
        Add a task to this workflow.
        """
        with self._store.cached_load(), self.batch_update():
            self._add_task(task, new_index=new_index)

    def add_task_after(self, new_task: Task, task_ref: Task | None = None) -> None:
        """Add a new task after the specified task.

        Parameters
        ----------
        task_ref
            If not given, the new task will be added at the end of the workflow.
        """
        new_index = (
            task_ref.index + 1 if task_ref and task_ref.index is not None else None
        )
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def add_task_before(self, new_task: Task, task_ref: Task | None = None) -> None:
        """Add a new task before the specified task.

        Parameters
        ----------
        task_ref
            If not given, the new task will be added at the beginning of the workflow.
        """
        new_index = task_ref.index if task_ref else 0
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    @TimeIt.decorator
    def _add_empty_loop(self, loop: Loop, cache: LoopCache) -> WorkflowLoop:
        """Add a new loop (zeroth iterations only) to the workflow."""

        new_index = self.num_loops

        # don't modify passed object:
        loop_c = copy.deepcopy(loop)

        # add to the WorkflowTemplate:
        self.template._add_empty_loop(loop_c)

        # all these element iterations will be initialised for the new loop:
        iter_IDs = cache.get_iter_IDs(loop_c)
        iter_loop_idx = cache.get_iter_loop_indices(iter_IDs)

        # create and insert a new WorkflowLoop:
        new_loop = self._app.WorkflowLoop.new_empty_loop(
            index=new_index,
            workflow=self,
            template=loop_c,
            iter_loop_idx=iter_loop_idx,
        )
        self.loops.add_object(new_loop)
        wk_loop = self.loops[new_index]

        # update any child loops of the new loop to include their new parent:
        for chd_loop in wk_loop.get_child_loops():
            chd_loop._update_parents(wk_loop)

        loop_js, _ = loop_c.to_json_like()

        # update persistent store:
        self._store.add_loop(
            loop_template=cast("Mapping", loop_js),
            iterable_parameters=wk_loop.iterable_parameters,
            parents=wk_loop.parents,
            num_added_iterations=wk_loop.num_added_iterations,
            iter_IDs=iter_IDs,
        )

        self._pending["loops"].append(new_index)

        # update cache loop indices:
        cache.update_loop_indices(new_loop_name=loop_c.name or "", iter_IDs=iter_IDs)

        return wk_loop

    @TimeIt.decorator
    def _add_loop(
        self, loop: Loop, cache: LoopCache | None = None, status: Status | None = None
    ) -> None:
        cache_ = cache or LoopCache.build(workflow=self, loops=[loop])
        new_wk_loop = self._add_empty_loop(loop, cache_)
        if loop.num_iterations is not None:
            # fixed number of iterations, so add remaining N > 0 iterations:
            if status:
                status_prev = status.status
            for iter_idx in range(loop.num_iterations - 1):
                if status:
                    status.update(
                        f"{status_prev}: iteration {iter_idx + 2}/{loop.num_iterations}."
                    )
                new_wk_loop.add_iteration(cache=cache_)

    def add_loop(self, loop: Loop) -> None:
        """Add a loop to a subset of workflow tasks."""
        with self._store.cached_load(), self.batch_update():
            self._add_loop(loop)

    @property
    def creation_info(self) -> CreationInfo:
        """
        The creation descriptor for the workflow.
        """
        if not self._creation_info:
            info = self._store.get_creation_info()
            self._creation_info = {
                "app_info": info["app_info"],
                "create_time": parse_timestamp(info["create_time"], self.ts_fmt),
                "id": info["id"],
            }
        return self._creation_info

    @property
    def id_(self) -> str:
        """
        The ID of this workflow.
        """
        return self.creation_info["id"]

    @property
    def ts_fmt(self) -> str:
        """
        The timestamp format.
        """
        if not self._ts_fmt:
            self._ts_fmt = self._store.get_ts_fmt()
        return self._ts_fmt

    @property
    def ts_name_fmt(self) -> str:
        """
        The timestamp format for names.
        """
        if not self._ts_name_fmt:
            self._ts_name_fmt = self._store.get_ts_name_fmt()
        return self._ts_name_fmt

    @property
    def template_components(self) -> TemplateComponents:
        """
        The template components used for this workflow.
        """
        if self._template_components is None:
            with self._store.cached_load():
                tc_js = self._store.get_template_components()
            self._template_components = self._app.template_components_from_json_like(
                tc_js
            )
        return self._template_components

    @property
    def __template_components(self) -> _TemplateComponents:
        return cast("_TemplateComponents", self.template_components)

    @property
    def template(self) -> WorkflowTemplate:
        """
        The template that this workflow was made from.
        """
        if self._template is None:
            with self._store.cached_load():
                temp_js = self._store.get_template()

                # TODO: insert_ID and id_ are the same thing:
                for task in cast("list[dict]", temp_js["tasks"]):
                    task.pop("id_", None)

                template = self._app.WorkflowTemplate.from_json_like(
                    temp_js, cast("dict", self.template_components)
                )
                template.workflow = self
            self._template = template

        return self._template

    @property
    def tasks(self) -> WorkflowTaskList:
        """
        The tasks in this workflow.
        """
        if self._tasks is None:
            with self._store.cached_load():
                all_tasks: Iterable[StoreTask] = self._store.get_tasks()
                self._tasks = self._app.WorkflowTaskList(
                    self._app.WorkflowTask(
                        workflow=self,
                        template=self.template.tasks[task.index],
                        index=task.index,
                        element_IDs=task.element_IDs,
                    )
                    for task in all_tasks
                )

        return self._tasks

    @property
    def loops(self) -> WorkflowLoopList:
        """
        The loops in this workflow.
        """

        def repack_iteration_tuples(
            num_added_iterations: list[list[list[int] | int]],
        ) -> Iterator[tuple[tuple[int, ...], int]]:
            """
            Unpacks a very ugly type from the persistence layer, turning it into
            something we can process into a dict more easily. This in turn is caused
            by JSON and Zarr not really understanding tuples as such.
            """
            for item in num_added_iterations:
                # Convert the outside to a tuple and narrow the inner types
                key_vec, count = item
                yield tuple(cast("list[int]", key_vec)), cast("int", count)

        if self._loops is None:
            with self._store.cached_load():
                self._loops = self._app.WorkflowLoopList(
                    self._app.WorkflowLoop(
                        index=idx,
                        workflow=self,
                        template=self.template.loops[idx],
                        parents=loop_dat["parents"],
                        num_added_iterations=dict(
                            repack_iteration_tuples(loop_dat["num_added_iterations"])
                        ),
                        iterable_parameters=loop_dat["iterable_parameters"],
                    )
                    for idx, loop_dat in self._store.get_loops().items()
                )
        return self._loops

    @property
    def submissions(self) -> list[Submission]:
        """
        The job submissions done by this workflow.
        """
        if self._submissions is None:
            self._app.persistence_logger.debug("loading workflow submissions")
            with self._store.cached_load():
                subs: list[Submission] = []
                for idx, sub_dat in self._store.get_submissions().items():
                    sub = self._app.Submission.from_json_like(
                        {"index": idx, **cast("dict", sub_dat)}
                    )
                    sub.workflow = self
                    subs.append(sub)
                self._submissions = subs
        return self._submissions

    @property
    def num_added_tasks(self) -> int:
        """
        The total number of added tasks.
        """
        return self._store._get_num_total_added_tasks()

    @TimeIt.decorator
    def get_store_EARs(self, id_lst: Iterable[int]) -> Sequence[StoreEAR]:
        """
        Get the persistent element action runs.
        """
        return self._store.get_EARs(id_lst)

    @TimeIt.decorator
    def get_store_element_iterations(
        self, id_lst: Iterable[int]
    ) -> Sequence[StoreElementIter]:
        """
        Get the persistent element iterations.
        """
        return self._store.get_element_iterations(id_lst)

    @TimeIt.decorator
    def get_store_elements(self, id_lst: Iterable[int]) -> Sequence[StoreElement]:
        """
        Get the persistent elements.
        """
        return self._store.get_elements(id_lst)

    @TimeIt.decorator
    def get_store_tasks(self, id_lst: Iterable[int]) -> Sequence[StoreTask]:
        """
        Get the persistent tasks.
        """
        return self._store.get_tasks_by_IDs(id_lst)

    def get_element_iteration_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> list[int]:
        """
        Get the element iteration IDs of EARs.
        """
        return [ear.elem_iter_ID for ear in self.get_store_EARs(id_lst)]

    def get_element_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> list[int]:
        """
        Get the element IDs of EARs.
        """
        iter_IDs = self.get_element_iteration_IDs_from_EAR_IDs(id_lst)
        return [itr.element_ID for itr in self.get_store_element_iterations(iter_IDs)]

    def get_task_IDs_from_element_IDs(self, id_lst: Iterable[int]) -> list[int]:
        """
        Get the task IDs of elements.
        """
        return [elem.task_ID for elem in self.get_store_elements(id_lst)]

    def get_EAR_IDs_of_tasks(self, id_lst: Iterable[int]) -> list[int]:
        """Get EAR IDs belonging to multiple tasks."""
        return [ear.id_ for ear in self.get_EARs_of_tasks(id_lst)]

    def get_EARs_of_tasks(self, id_lst: Iterable[int]) -> Iterator[ElementActionRun]:
        """Get EARs belonging to multiple tasks."""
        for id_ in id_lst:
            for elem in self.tasks.get(insert_ID=id_).elements[:]:
                for iter_ in elem.iterations:
                    yield from iter_.action_runs

    def get_element_iterations_of_tasks(
        self, id_lst: Iterable[int]
    ) -> Iterator[ElementIteration]:
        """Get element iterations belonging to multiple tasks."""
        for id_ in id_lst:
            for elem in self.tasks.get(insert_ID=id_).elements[:]:
                yield from elem.iterations

    @dataclass
    class _IndexPath1:
        elem: int
        task: int

    @TimeIt.decorator
    def get_elements_from_IDs(self, id_lst: Iterable[int]) -> list[Element]:
        """Return element objects from a list of IDs."""

        store_elems = self.get_store_elements(id_lst)
        store_tasks = self.get_store_tasks(el.task_ID for el in store_elems)

        element_idx_by_task: dict[int, set[int]] = defaultdict(set)
        index_paths: list[Workflow._IndexPath1] = []
        for elem, task in zip(store_elems, store_tasks):
            elem_idx = task.element_IDs.index(elem.id_)
            index_paths.append(Workflow._IndexPath1(elem_idx, task.index))
            element_idx_by_task[task.index].add(elem_idx)

        elements_by_task = {
            task_idx: {idx: self.tasks[task_idx].elements[idx] for idx in elem_idxes}
            for task_idx, elem_idxes in element_idx_by_task.items()
        }

        return [elements_by_task[path.task][path.elem] for path in index_paths]

    @dataclass
    class _IndexPath2:
        iter: int
        elem: int
        task: int

    @TimeIt.decorator
    def get_element_iterations_from_IDs(
        self, id_lst: Iterable[int]
    ) -> list[ElementIteration]:
        """Return element iteration objects from a list of IDs."""

        store_iters = self.get_store_element_iterations(id_lst)
        store_elems = self.get_store_elements(it.element_ID for it in store_iters)
        store_tasks = self.get_store_tasks(el.task_ID for el in store_elems)

        element_idx_by_task: dict[int, set[int]] = defaultdict(set)

        index_paths: list[Workflow._IndexPath2] = []
        for itr, elem, task in zip(store_iters, store_elems, store_tasks):
            iter_idx = elem.iteration_IDs.index(itr.id_)
            elem_idx = task.element_IDs.index(elem.id_)
            index_paths.append(Workflow._IndexPath2(iter_idx, elem_idx, task.index))
            element_idx_by_task[task.index].add(elem_idx)

        elements_by_task = {
            task_idx: {idx: self.tasks[task_idx].elements[idx] for idx in elem_idx}
            for task_idx, elem_idx in element_idx_by_task.items()
        }

        return [
            elements_by_task[path.task][path.elem].iterations[path.iter]
            for path in index_paths
        ]

    @dataclass
    class _IndexPath3:
        run: int
        act: int
        iter: int
        elem: int
        task: int

    @overload
    def get_EARs_from_IDs(self, ids: Iterable[int]) -> list[ElementActionRun]:
        ...

    @overload
    def get_EARs_from_IDs(self, ids: int) -> ElementActionRun:
        ...

    @TimeIt.decorator
    def get_EARs_from_IDs(
        self, ids: Iterable[int] | int
    ) -> list[ElementActionRun] | ElementActionRun:
        """Get element action run objects from a list of IDs."""
        id_lst = [ids] if isinstance(ids, int) else list(ids)
        self._app.persistence_logger.debug(f"get_EARs_from_IDs: id_lst={id_lst!r}")

        store_EARs = self.get_store_EARs(id_lst)
        store_iters = self.get_store_element_iterations(
            ear.elem_iter_ID for ear in store_EARs
        )
        store_elems = self.get_store_elements(it.element_ID for it in store_iters)
        store_tasks = self.get_store_tasks(el.task_ID for el in store_elems)

        # to allow for bulk retrieval of elements/iterations
        element_idx_by_task: dict[int, set[int]] = defaultdict(set)
        iter_idx_by_task_elem: dict[int, dict[int, set[int]]] = defaultdict(
            lambda: defaultdict(set)
        )

        index_paths: list[Workflow._IndexPath3] = []
        for rn, it, el, tk in zip(store_EARs, store_iters, store_elems, store_tasks):
            act_idx = rn.action_idx
            run_idx = it.EAR_IDs[act_idx].index(rn.id_) if it.EAR_IDs is not None else -1
            iter_idx = el.iteration_IDs.index(it.id_)
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                Workflow._IndexPath3(run_idx, act_idx, iter_idx, elem_idx, tk.index)
            )
            element_idx_by_task[tk.index].add(elem_idx)
            iter_idx_by_task_elem[tk.index][elem_idx].add(iter_idx)

        # retrieve elements/iterations:
        iters = {
            task_idx: {
                elem_i.index: {
                    iter_idx: elem_i.iterations[iter_idx]
                    for iter_idx in iter_idx_by_task_elem[task_idx][elem_i.index]
                }
                for elem_i in self.tasks[task_idx].elements[list(elem_idxes)]
            }
            for task_idx, elem_idxes in element_idx_by_task.items()
        }

        result = [
            iters[path.task][path.elem][path.iter].actions[path.act].runs[path.run]
            for path in index_paths
        ]
        if isinstance(ids, int):
            return result[0]
        return result

    @TimeIt.decorator
    def get_all_elements(self) -> list[Element]:
        """
        Get all elements in the workflow.
        """
        return self.get_elements_from_IDs(range(self.num_elements))

    @TimeIt.decorator
    def get_all_element_iterations(self) -> list[ElementIteration]:
        """
        Get all iterations in the workflow.
        """
        return self.get_element_iterations_from_IDs(range(self.num_element_iterations))

    @TimeIt.decorator
    def get_all_EARs(self) -> list[ElementActionRun]:
        """
        Get all runs in the workflow.
        """
        return self.get_EARs_from_IDs(range(self.num_EARs))

    @contextmanager
    def batch_update(self, is_workflow_creation: bool = False) -> Iterator[None]:
        """A context manager that batches up structural changes to the workflow and
        commits them to disk all together when the context manager exits."""

        if self._in_batch_mode:
            yield
        else:
            try:
                self._app.persistence_logger.info(
                    f"entering batch update (is_workflow_creation={is_workflow_creation!r})"
                )
                self._in_batch_mode = True
                yield

            except Exception:
                self._app.persistence_logger.error("batch update exception!")
                self._in_batch_mode = False
                self._store._pending.reset()

                for task in self.tasks:
                    task._reset_pending_element_IDs()
                    task.template._reset_pending_element_sets()

                for loop in self.loops:
                    loop._reset_pending_num_added_iters()
                    loop._reset_pending_parents()

                self._reject_pending()

                if is_workflow_creation:
                    # creation failed, so no need to keep the newly generated workflow:
                    self._store.delete_no_confirm()
                    self._store.reinstate_replaced_dir()

                raise

            else:
                if self._store._pending:
                    # is_diff = self._store.is_modified_on_disk()
                    # if is_diff:
                    #     raise WorkflowBatchUpdateFailedError(
                    #         f"Workflow modified on disk since it was loaded!"
                    #     )

                    for task in self.tasks:
                        task._accept_pending_element_IDs()
                        task.template._accept_pending_element_sets()

                    for loop in self.loops:
                        loop._accept_pending_num_added_iters()
                        loop._accept_pending_parents()

                    # TODO: handle errors in commit pending?
                    self._store._pending.commit_all()
                    self._accept_pending()

                if is_workflow_creation:
                    self._store.remove_replaced_dir()

                self._app.persistence_logger.info("exiting batch update")
                self._in_batch_mode = False

    @classmethod
    def temporary_rename(cls, path: str, fs: AbstractFileSystem) -> str:
        """Rename an existing same-path workflow (directory) so we can restore it if
        workflow creation fails.

        Renaming will occur until the successfully completed. This means multiple new
        paths may be created, where only the final path should be considered the
        successfully renamed workflow. Other paths will be deleted."""

        all_replaced: list[str] = []

        @cls._app.perm_error_retry()
        def _temp_rename(path: str, fs: AbstractFileSystem) -> str:
            temp_ext = "".join(random.choices(string.ascii_letters, k=10))
            replaced = str(Path(f"{path}.{temp_ext}").as_posix())
            cls._app.persistence_logger.debug(
                f"temporary_rename: _temp_rename: {path!r} --> {replaced!r}."
            )
            all_replaced.append(replaced)
            try:
                fs.rename(path, replaced, recursive=True)
            except TypeError:
                # `SFTPFileSystem.rename` has no `recursive` argument:
                fs.rename(path, replaced)
            return replaced

        @cls._app.perm_error_retry()
        def _remove_path(path: str, fs: AbstractFileSystem) -> None:
            cls._app.persistence_logger.debug(
                f"temporary_rename: _remove_path: {path!r}."
            )
            while fs.exists(path):
                fs.rm(path, recursive=True)
                time.sleep(0.5)

        _temp_rename(path, fs)

        for path in all_replaced[:-1]:
            _remove_path(path, fs)

        return all_replaced[-1]

    @classmethod
    @TimeIt.decorator
    def _write_empty_workflow(
        cls,
        template: WorkflowTemplate,
        *,
        path: PathLike | None = None,
        name: str | None = None,
        overwrite: bool | None = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        fs_kwargs: dict[str, Any] | None = None,
        store_kwargs: dict[str, Any] | None = None,
    ) -> Workflow:
        """
        Parameters
        ----------
        template
            The workflow description to instantiate.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        """

        # store all times in UTC, since NumPy doesn't support time zone info:
        ts_utc = current_timestamp()
        ts = normalise_timestamp(ts_utc)

        ts_name_fmt = ts_name_fmt or cls._default_ts_name_fmt
        ts_fmt = ts_fmt or cls._default_ts_fmt

        name = name or f"{template.name}_{ts.strftime(ts_name_fmt)}"

        fs_path = f"{path or '.'}/{name}"
        fs_kwargs = fs_kwargs or {}
        fs, path, pw = resolve_fsspec(path or "", **fs_kwargs)
        wk_path = f"{path}/{name}"

        replaced_wk = None
        if fs.exists(wk_path):
            cls._app.logger.debug("workflow path exists")
            if overwrite:
                cls._app.logger.debug("renaming existing workflow path")
                replaced_wk = cls.temporary_rename(wk_path, fs)
            else:
                raise ValueError(
                    f"Path already exists: {wk_path} on file system " f"{fs!r}."
                )

        class PersistenceGrabber:
            """An object to pass to ResourceSpec.make_persistent that pretends to be a
            Workflow object, so we can pretend to make template-level inputs/resources
            persistent before the workflow exists."""

            def __init__(self) -> None:
                self.__ps: list[tuple[Any, ParamSource]] = []

            def _add_parameter_data(self, data: Any, source: ParamSource) -> int:
                ref = len(self.__ps)
                self.__ps.append((data, source))
                return ref

            def get_parameter_data(self, data_idx: int) -> Any:
                return self.__ps[data_idx - 1][0]

            def check_parameters_exist(self, id_lst: int | list[int]) -> bool:
                r = range(len(self.__ps))
                if isinstance(id_lst, int):
                    return id_lst in r
                else:
                    return all(id_ in r for id_ in id_lst)

            def write_persistence_data_to_workflow(self, workflow: Workflow) -> None:
                for dat_i, source_i in self.__ps:
                    workflow._add_parameter_data(dat_i, source_i)

        # make template-level inputs/resources think they are persistent:
        grabber = PersistenceGrabber()
        param_src: ParamSource = {"type": "workflow_resources"}
        for res_i_copy in template._get_resources_copy():
            res_i_copy.make_persistent(grabber, param_src)

        template_js_, template_sh = template.to_json_like(exclude={"tasks", "loops"})
        template_js: TemplateMeta = {
            **cast("TemplateMeta", template_js_),  # Trust me, bro!
            "tasks": [],
            "loops": [],
        }

        store_kwargs = store_kwargs if store_kwargs else template.store_kwargs
        store_cls = store_cls_from_str(store)
        store_cls.write_empty_workflow(
            app=cls._app,
            template_js=template_js,
            template_components_js=template_sh or {},
            wk_path=wk_path,
            fs=fs,
            name=name,
            replaced_wk=replaced_wk,
            creation_info={
                "app_info": cls._app.get_info(),
                "create_time": ts_utc.strftime(ts_fmt),
                "id": str(uuid4()),
            },
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            **store_kwargs,
        )

        fs_kwargs = {"password": pw, **fs_kwargs}
        wk = cls(fs_path, store_fmt=store, fs_kwargs=fs_kwargs)

        # actually make template inputs/resources persistent, now the workflow exists:
        grabber.write_persistence_data_to_workflow(wk)

        if template.source_file:
            wk.artifacts_path.mkdir(exist_ok=False)
            src = Path(template.source_file)
            wk.artifacts_path.joinpath(src.name).write_text(src.read_text())

        return wk

    def zip(
        self,
        path: str = ".",
        *,
        log: str | None = None,
        overwrite: bool = False,
        include_execute: bool = False,
        include_rechunk_backups: bool = False,
    ) -> str:
        """
        Convert the workflow to a zipped form.

        Parameters
        ----------
        path:
            Path at which to create the new zipped workflow. If this is an existing
            directory, the zip file will be created within this directory. Otherwise,
            this path is assumed to be the full file path to the new zip file.
        """
        return self._store.zip(
            path=path,
            log=log,
            overwrite=overwrite,
            include_execute=include_execute,
            include_rechunk_backups=include_rechunk_backups,
        )

    def unzip(self, path: str = ".", *, log: str | None = None) -> str:
        """
        Convert the workflow to an unzipped form.

        Parameters
        ----------
        path:
            Path at which to create the new unzipped workflow. If this is an existing
            directory, the new workflow directory will be created within this directory.
            Otherwise, this path will represent the new workflow directory path.
        """
        return self._store.unzip(path=path, log=log)

    def copy(self, path: str | Path = ".") -> Path:
        """Copy the workflow to a new path and return the copied workflow path."""
        return self._store.copy(path)

    def delete(self) -> None:
        """
        Delete the persistent data.
        """
        self._store.delete()

    def _delete_no_confirm(self) -> None:
        self._store.delete_no_confirm()

    def get_parameters(self, id_lst: Iterable[int], **kwargs) -> Sequence[StoreParameter]:
        """
        Get parameters known to the workflow.

        Parameter
        ---------
        id_lst:
            The indices of the parameters to retrieve.

        Keyword Arguments
        -----------------
        dataset_copy: bool
            For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        return self._store.get_parameters(id_lst, **kwargs)

    @TimeIt.decorator
    def get_parameter_sources(self, id_lst: Iterable[int]) -> list[ParamSource]:
        """
        Get parameter sources known to the workflow.
        """
        return self._store.get_parameter_sources(id_lst)

    @TimeIt.decorator
    def get_parameter_set_statuses(self, id_lst: Iterable[int]) -> list[bool]:
        """
        Get whether some parameters are set.
        """
        return self._store.get_parameter_set_statuses(id_lst)

    @TimeIt.decorator
    def get_parameter(self, index: int, **kwargs) -> StoreParameter:
        """
        Get a single parameter.

        Parameter
        ---------
        index:
            The index of the parameter to retrieve.

        Keyword Arguments
        -----------------
        dataset_copy: bool
            For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        return self.get_parameters((index,), **kwargs)[0]

    @TimeIt.decorator
    def get_parameter_data(self, index: int, **kwargs) -> Any:
        """
        Get the data relating to a parameter.
        """
        param = self.get_parameter(index, **kwargs)
        if param.data is not None:
            return param.data
        else:
            return param.file

    @TimeIt.decorator
    def get_parameter_source(self, index: int) -> ParamSource:
        """
        Get the source of a particular parameter.
        """
        return self.get_parameter_sources((index,))[0]

    @TimeIt.decorator
    def is_parameter_set(self, index: int) -> bool:
        """
        Test if a particular parameter is set.
        """
        return self.get_parameter_set_statuses((index,))[0]

    @TimeIt.decorator
    def get_all_parameters(self, **kwargs) -> list[StoreParameter]:
        """
        Retrieve all persistent parameters.

        Keyword Arguments
        -----------------
        dataset_copy: bool
            For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        num_params = self._store._get_num_total_parameters()
        return self._store.get_parameters(range(num_params), **kwargs)

    @TimeIt.decorator
    def get_all_parameter_sources(self, **kwargs) -> list[ParamSource]:
        """Retrieve all persistent parameters sources."""
        num_params = self._store._get_num_total_parameters()
        return self._store.get_parameter_sources(range(num_params), **kwargs)

    @TimeIt.decorator
    def get_all_parameter_data(self, **kwargs) -> dict[int, Any]:
        """
        Retrieve all workflow parameter data.

        Keyword Arguments
        -----------------
        dataset_copy: bool
            For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        return {
            param.id_: (param.data if param.data is not None else param.file)
            for param in self.get_all_parameters(**kwargs)
        }

    def check_parameters_exist(self, id_lst: int | list[int]) -> bool:
        """
        Check if all the parameters exist.
        """
        if isinstance(id_lst, int):
            return next(iter(self._store.check_parameters_exist((id_lst,))))
        return all(self._store.check_parameters_exist(id_lst))

    def _add_unset_parameter_data(self, source: ParamSource) -> int:
        # TODO: use this for unset files as well
        return self._store.add_unset_parameter(source)

    def _add_parameter_data(self, data, source: ParamSource) -> int:
        return self._store.add_set_parameter(data, source)

    def _add_file(
        self,
        *,
        store_contents: bool,
        is_input: bool,
        source: ParamSource,
        path=None,
        contents=None,
        filename: str,
    ) -> int:
        return self._store.add_file(
            store_contents=store_contents,
            is_input=is_input,
            source=source,
            path=path,
            contents=contents,
            filename=filename,
        )

    def _set_file(
        self,
        param_id: int | list[int] | None,
        store_contents: bool,
        is_input: bool,
        path: Path | str,
        contents=None,
        filename: str | None = None,
        clean_up: bool = False,
    ) -> None:
        self._store.set_file(
            param_id=cast("int", param_id),
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
            clean_up=clean_up,
        )

    @overload
    def get_task_unique_names(
        self, map_to_insert_ID: Literal[False] = False
    ) -> Sequence[str]:
        ...

    @overload
    def get_task_unique_names(self, map_to_insert_ID: Literal[True]) -> Mapping[str, int]:
        ...

    def get_task_unique_names(
        self, map_to_insert_ID: bool = False
    ) -> Sequence[str] | Mapping[str, int]:
        """Return the unique names of all workflow tasks.

        Parameters
        ----------
        map_to_insert_ID : bool
            If True, return a dict whose values are task insert IDs, otherwise return a
            list.

        """
        names = self._app.Task.get_task_unique_names(self.template.tasks)
        if map_to_insert_ID:
            return dict(zip(names, (task.insert_ID for task in self.template.tasks)))
        else:
            return names

    def _get_new_task_unique_name(self, new_task: Task, new_index: int) -> str:
        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = self._app.Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _get_empty_pending(self) -> Pending:
        return {
            "template_components": {k: [] for k in TEMPLATE_COMP_TYPES},
            "tasks": [],  # list of int
            "loops": [],  # list of int
            "submissions": [],  # list of int
        }

    def _accept_pending(self) -> None:
        self._reset_pending()

    def _reset_pending(self) -> None:
        self._pending = self._get_empty_pending()

    def _reject_pending(self) -> None:
        """Revert pending changes to the in-memory representation of the workflow.

        This deletes new tasks, new template component data, new loops, and new
        submissions. Element additions to existing (non-pending) tasks are separately
        rejected/accepted by the WorkflowTask object.

        """
        for task_idx in self._pending["tasks"][::-1]:
            # iterate in reverse so the index references are correct
            self.tasks._remove_object(task_idx)
            self.template.tasks.pop(task_idx)

        for comp_type, comp_indices in self._pending["template_components"].items():
            for comp_idx in comp_indices[::-1]:
                # iterate in reverse so the index references are correct
                tc = self.__template_components[comp_type]
                assert hasattr(tc, "_remove_object")
                tc._remove_object(comp_idx)

        for loop_idx in self._pending["loops"][::-1]:
            # iterate in reverse so the index references are correct
            self.loops._remove_object(loop_idx)
            self.template.loops.pop(loop_idx)

        for sub_idx in self._pending["submissions"][::-1]:
            # iterate in reverse so the index references are correct
            assert self._submissions is not None
            self._submissions.pop(sub_idx)

        self._reset_pending()

    @property
    def num_tasks(self) -> int:
        """
        The total number of tasks.
        """
        return self._store._get_num_total_tasks()

    @property
    def num_submissions(self) -> int:
        """
        The total number of job submissions.
        """
        return self._store._get_num_total_submissions()

    @property
    def num_elements(self) -> int:
        """
        The total number of elements.
        """
        return self._store._get_num_total_elements()

    @property
    def num_element_iterations(self) -> int:
        """
        The total number of element iterations.
        """
        return self._store._get_num_total_elem_iters()

    @property
    @TimeIt.decorator
    def num_EARs(self) -> int:
        """
        The total number of element action runs.
        """
        return self._store._get_num_total_EARs()

    @property
    def num_loops(self) -> int:
        """
        The total number of loops.
        """
        return self._store._get_num_total_loops()

    @property
    def artifacts_path(self) -> Path:
        """
        Path to artifacts of the workflow (temporary files, etc).
        """
        # TODO: allow customisation of artifacts path at submission and resources level
        return Path(self.path) / "artifacts"

    @property
    def input_files_path(self) -> Path:
        """
        Path to input files for the workflow.
        """
        return self.artifacts_path / self._input_files_dir_name

    @property
    def submissions_path(self) -> Path:
        """
        Path to submission data for ths workflow.
        """
        return self.artifacts_path / "submissions"

    @property
    def task_artifacts_path(self) -> Path:
        """
        Path to artifacts of tasks.
        """
        return self.artifacts_path / "tasks"

    @property
    def execution_path(self) -> Path:
        """
        Path to working directory path for executing.
        """
        return Path(self.path) / self._exec_dir_name

    @TimeIt.decorator
    def get_task_elements(
        self,
        task: WorkflowTask,
        idx_lst: list[int] | None = None,
    ) -> list[Element]:
        """
        Get the elements of a task.
        """
        return [
            self._app.Element(
                task=task, **{k: v for k, v in te.items() if k != "task_ID"}
            )
            for te in self._store.get_task_elements(task.insert_ID, idx_lst)
        ]

    def set_EAR_submission_index(self, EAR_ID: int, sub_idx: int) -> None:
        """Set the submission index of an EAR."""
        with self._store.cached_load(), self.batch_update():
            self._store.set_EAR_submission_index(EAR_ID, sub_idx)

    def set_EAR_start(self, EAR_ID: int) -> None:
        """Set the start time on an EAR."""
        self._app.logger.debug(f"Setting start for EAR ID {EAR_ID!r}")
        with self._store.cached_load(), self.batch_update():
            self._store.set_EAR_start(EAR_ID)

    def set_EAR_end(
        self,
        js_idx: int,
        js_act_idx: int,
        EAR_ID: int,
        exit_code: int,
    ) -> None:
        """Set the end time and exit code on an EAR.

        If the exit code is non-zero, also set all downstream dependent EARs to be
        skipped. Also save any generated input/output files.

        """
        self._app.logger.debug(
            f"Setting end for EAR ID {EAR_ID!r} with exit code {exit_code!r}."
        )
        with self._store.cached_load():
            EAR = self.get_EARs_from_IDs(EAR_ID)
            with self.batch_update():
                success = exit_code == 0  # TODO  more sophisticated success heuristics
                if EAR.action.abortable and exit_code == ABORT_EXIT_CODE:
                    # the point of aborting an EAR is to continue with the workflow:
                    success = True

                for IFG_i in EAR.action.input_file_generators:
                    inp_file = IFG_i.input_file
                    self._app.logger.debug(
                        f"Saving EAR input file: {inp_file.label!r} for EAR ID "
                        f"{EAR_ID!r}."
                    )
                    param_id = EAR.data_idx[f"input_files.{inp_file.label}"]

                    file_paths = inp_file.value()
                    for path_i in (
                        file_paths if isinstance(file_paths, list) else [file_paths]
                    ):
                        self._set_file(
                            param_id=param_id,
                            store_contents=True,  # TODO: make optional according to IFG
                            is_input=False,
                            path=Path(path_i).resolve(),
                        )

                if EAR.action.script_data_out_has_files:
                    EAR._param_save(js_idx=js_idx, js_act_idx=js_act_idx)

                # Save action-level files: (TODO: refactor with below for OFPs)
                for save_file_j in EAR.action.save_files:
                    self._app.logger.debug(
                        f"Saving file: {save_file_j.label!r} for EAR ID " f"{EAR_ID!r}."
                    )
                    # We might be saving a file that is not a defined
                    # "output file"; this will avoid saving a reference in the
                    # parameter data in that case
                    param_id_j = EAR.data_idx.get(f"output_files.{save_file_j.label}")

                    file_paths = save_file_j.value()
                    self._app.logger.debug(f"Saving output file paths: {file_paths!r}")
                    for path_i in (
                        file_paths if isinstance(file_paths, list) else [file_paths]
                    ):
                        self._set_file(
                            param_id=param_id_j,
                            store_contents=True,
                            is_input=False,
                            path=Path(path_i).resolve(),
                            clean_up=(save_file_j in EAR.action.clean_up),
                        )

                for OFP_i in EAR.action.output_file_parsers:
                    for save_file_j in OFP_i._save_files:
                        self._app.logger.debug(
                            f"Saving EAR output file: {save_file_j.label!r} for EAR ID "
                            f"{EAR_ID!r}."
                        )
                        # We might be saving a file that is not a defined
                        # "output file"; this will avoid saving a reference in the
                        # parameter data in that case
                        param_id_j = EAR.data_idx.get(f"output_files.{save_file_j.label}")

                        file_paths = save_file_j.value()
                        self._app.logger.debug(
                            f"Saving EAR output file paths: {file_paths!r}"
                        )
                        for path_i in (
                            file_paths if isinstance(file_paths, list) else [file_paths]
                        ):
                            self._set_file(
                                param_id=param_id_j,
                                store_contents=True,  # TODO: make optional according to OFP
                                is_input=False,
                                path=Path(path_i).resolve(),
                                clean_up=(save_file_j in OFP_i.clean_up),
                            )

                if not success:
                    for EAR_dep_ID in EAR.get_dependent_EARs():
                        # TODO: this needs to be recursive?
                        self._app.logger.debug(
                            f"Setting EAR ID {EAR_dep_ID!r} to skip because it depends on"
                            f" EAR ID {EAR_ID!r}, which exited with a non-zero exit code:"
                            f" {exit_code!r}."
                        )
                        self._store.set_EAR_skip(EAR_dep_ID)

                self._store.set_EAR_end(EAR_ID, exit_code, success)

    def set_EAR_skip(self, EAR_ID: int) -> None:
        """
        Record that an EAR is to be skipped due to an upstream failure or loop
        termination condition being met.
        """
        with self._store.cached_load(), self.batch_update():
            self._store.set_EAR_skip(EAR_ID)

    def get_EAR_skipped(self, EAR_ID: int) -> bool:
        """Check if an EAR is to be skipped."""
        with self._store.cached_load():
            return self._store.get_EAR_skipped(EAR_ID)

    @TimeIt.decorator
    def set_parameter_value(
        self, param_id: int | list[int], value: Any, commit: bool = False
    ) -> None:
        """
        Set the value of a parameter.
        """
        with self._store.cached_load(), self.batch_update():
            self._store.set_parameter_value(cast("int", param_id), value)

        if commit:
            # force commit now:
            self._store._pending.commit_all()

    def set_EARs_initialised(self, iter_ID: int) -> None:
        """
        Set :py:attr:`~hpcflow.app.ElementIteration.EARs_initialised` to True for the
        specified iteration.
        """
        with self._store.cached_load(), self.batch_update():
            self._store.set_EARs_initialised(iter_ID)

    def elements(self) -> Iterator[Element]:
        """
        Get the elements of the workflow's tasks.
        """
        for task in self.tasks:
            for element in task.elements[:]:
                yield element

    @overload
    def get_iteration_task_pathway(
        self,
        *,
        ret_iter_IDs: Literal[False] = False,
        ret_data_idx: Literal[False] = False,
    ) -> Sequence[tuple[int, LoopIndex[str, int]]]:
        ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[False] = False, ret_data_idx: Literal[True]
    ) -> Sequence[tuple[int, LoopIndex[str, int], tuple[Mapping[str, int], ...]]]:
        ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[True], ret_data_idx: Literal[False] = False
    ) -> Sequence[tuple[int, LoopIndex[str, int], tuple[int, ...]]]:
        ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[True], ret_data_idx: Literal[True]
    ) -> Sequence[
        tuple[int, LoopIndex[str, int], tuple[int, ...], tuple[Mapping[str, int], ...]]
    ]:
        ...

    @TimeIt.decorator
    def get_iteration_task_pathway(
        self, ret_iter_IDs: bool = False, ret_data_idx: bool = False
    ) -> Sequence[tuple]:
        """
        Get the iteration task pathway.
        """
        pathway: list[_Pathway] = []
        for task in self.tasks:
            pathway.append(_Pathway(task.insert_ID))

        added_loop_names: set[str] = set()
        for _ in range(self.num_loops):
            for loop in self.loops:
                if loop.name in added_loop_names:
                    continue
                elif set(loop.parents).issubset(added_loop_names):
                    # add a loop only once their parents have been added:
                    to_add = loop
                    break
            else:
                raise RuntimeError(
                    "Failed to find a loop whose parents have already been added to the "
                    "iteration task pathway."
                )

            iIDs = to_add.task_insert_IDs
            relevant_idx = (
                idx for idx, path_i in enumerate(pathway) if path_i.id_ in iIDs
            )

            for num_add_k, num_add in to_add.num_added_iterations.items():
                parent_loop_idx = list(zip(to_add.parents, num_add_k))
                replacement: list[_Pathway] = []
                repl_idx: list[int] = []
                for i in range(num_add):
                    for p_idx, path in enumerate(pathway):
                        if path.id_ not in iIDs:
                            continue
                        if all(path.names[k] == v for k, v in parent_loop_idx):
                            new_path = copy.deepcopy(path)
                            new_path.names += {to_add.name: i}
                            repl_idx.append(p_idx)
                            replacement.append(new_path)

                if replacement:
                    pathway = replace_items(
                        pathway, min(repl_idx), max(repl_idx) + 1, replacement
                    )

            added_loop_names.add(to_add.name)

        if added_loop_names != set(loop.name for loop in self.loops):
            raise RuntimeError(
                "Not all loops have been considered in the iteration task pathway."
            )

        if ret_iter_IDs or ret_data_idx:
            all_iters = self.get_all_element_iterations()
            for path_i in pathway:
                i_iters = [
                    iter_j
                    for iter_j in all_iters
                    if (
                        iter_j.task.insert_ID == path_i.id_
                        and iter_j.loop_idx == path_i.names
                    )
                ]
                if ret_iter_IDs:
                    path_i.iter_ids.extend(elit.id_ for elit in i_iters)
                if ret_data_idx:
                    path_i.data_idx.extend(elit.get_data_idx() for elit in i_iters)

        return [
            path.as_tuple(ret_iter_IDs=ret_iter_IDs, ret_data_idx=ret_data_idx)
            for path in pathway
        ]

    @TimeIt.decorator
    def _submit(
        self,
        status: Status | None = None,
        ignore_errors: bool = False,
        JS_parallelism: bool | None = None,
        print_stdout: bool = False,
        add_to_known: bool = True,
        tasks: Sequence[int] | None = None,
    ) -> tuple[Sequence[SubmissionFailure], Mapping[int, Sequence[int]]]:
        """Submit outstanding EARs for execution."""

        # generate a new submission if there are no pending submissions:
        if not (pending := [sub for sub in self.submissions if sub.needs_submit]):
            if status:
                status.update("Adding new submission...")
            if not (new_sub := self._add_submission(tasks, JS_parallelism)):
                raise ValueError("No pending element action runs to submit!")
            pending = [new_sub]

        self.submissions_path.mkdir(exist_ok=True, parents=True)
        self.execution_path.mkdir(exist_ok=True, parents=True)
        self.task_artifacts_path.mkdir(exist_ok=True, parents=True)

        # for direct execution the submission must be persistent at submit-time, because
        # it will be read by a new instance of the app:
        if status:
            status.update("Committing to the store...")
        self._store._pending.commit_all()

        # submit all pending submissions:
        exceptions: list[SubmissionFailure] = []
        submitted_js: dict[int, list[int]] = {}
        for sub in pending:
            try:
                if status:
                    status.update(f"Preparing submission {sub.index}...")
                sub_js_idx = sub.submit(
                    status=status,
                    ignore_errors=ignore_errors,
                    print_stdout=print_stdout,
                    add_to_known=add_to_known,
                )
                submitted_js[sub.index] = sub_js_idx
            except SubmissionFailure as exc:
                exceptions.append(exc)

        return exceptions, submitted_js

    @overload
    def submit(
        self,
        *,
        ignore_errors: bool = False,
        JS_parallelism: bool | None = None,
        print_stdout: bool = False,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: Literal[True],
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ) -> Mapping[int, Sequence[int]]:
        ...

    @overload
    def submit(
        self,
        *,
        ignore_errors: bool = False,
        JS_parallelism: bool | None = None,
        print_stdout: bool = False,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: Literal[False] = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ) -> None:
        ...

    def submit(
        self,
        *,
        ignore_errors: bool = False,
        JS_parallelism: bool | None = None,
        print_stdout: bool = False,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ) -> Mapping[int, Sequence[int]] | None:
        """Submit the workflow for execution.

        Parameters
        ----------
        ignore_errors
            If True, ignore jobscript submission errors. If False (the default) jobscript
            submission will halt when a jobscript fails to submit.
        JS_parallelism
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature.
            If not set, jobscript parallelism will be used if the store type supports it.
        print_stdout
            If True, print any jobscript submission standard output, otherwise hide it.
        wait
            If True, this command will block until the workflow execution is complete.
        add_to_known
            If True, add the submitted submissions to the known-submissions file, which is
            used by the `show` command to monitor current and recent submissions.
        return_idx
            If True, return a dict representing the jobscript indices submitted for each
            submission.
        tasks
            List of task indices to include in the new submission if no submissions
            already exist. By default all tasks are included if a new submission is
            created.
        cancel
            Immediately cancel the submission. Useful for testing and benchmarking.
        status
            If True, display a live status to track submission progress.
        """

        # Type hint for mypy
        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            rich.console.Console().status("Submitting workflow...")
            if status
            else nullcontext()
        )
        with status_context as status_, self._store.cached_load():
            if not self._store.is_submittable:
                raise NotImplementedError("The workflow is not submittable.")
            # commit updates before raising exception:
            with self.batch_update(), self._store.cache_ctx():
                exceptions, submitted_js = self._submit(
                    ignore_errors=ignore_errors,
                    JS_parallelism=JS_parallelism,
                    print_stdout=print_stdout,
                    status=status_,
                    add_to_known=add_to_known,
                    tasks=tasks,
                )

        if exceptions:
            raise WorkflowSubmissionFailure(exceptions)

        if cancel:
            self.cancel()

        elif wait:
            self.wait(submitted_js)

        if return_idx:
            return submitted_js
        return None

    @staticmethod
    def __wait_for_direct_jobscripts(jobscripts: list[Jobscript]):
        """Wait for the passed direct (i.e. non-scheduled) jobscripts to finish."""

        def callback(proc: psutil.Process) -> None:
            js = js_pids[proc.pid]
            assert hasattr(proc, "returncode")
            # TODO sometimes proc.returncode is None; maybe because multiple wait
            # calls?
            print(
                f"Jobscript {js.index} from submission {js.submission.index} "
                f"finished with exit code {proc.returncode}."
            )

        js_pids = {js.process_ID: js for js in jobscripts}
        process_refs = [
            (js.process_ID, js.submit_cmdline)
            for js in jobscripts
            if js.process_ID and js.submit_cmdline
        ]
        DirectScheduler.wait_for_jobscripts(process_refs, callback=callback)

    def __wait_for_scheduled_jobscripts(self, jobscripts: list[Jobscript]):
        """Wait for the passed scheduled jobscripts to finish."""
        schedulers = self._app.Submission.get_unique_schedulers_of_jobscripts(jobscripts)
        threads: list[Thread] = []
        for js_indices, sched in schedulers:
            jobscripts_gen = (
                self.submissions[sub_idx].jobscripts[js_idx]
                for sub_idx, js_idx in js_indices
            )
            job_IDs = [
                js.scheduler_job_ID
                for js in jobscripts_gen
                if js.scheduler_job_ID is not None
            ]
            threads.append(Thread(target=sched.wait_for_jobscripts, args=(job_IDs,)))

        for thr in threads:
            thr.start()

        for thr in threads:
            thr.join()

    def wait(self, sub_js: Mapping[int, Sequence[int]] | None = None):
        """Wait for the completion of specified/all submitted jobscripts."""

        # TODO: think about how this might work with remote workflow submission (via SSH)

        # TODO: add a log file to the submission dir where we can log stuff (e.g starting
        # a thread...)

        if not sub_js:
            # find any active jobscripts first:
            sub_js_: dict[int, list[int]] = defaultdict(list)
            for sub in self.submissions:
                sub_js_[sub.index].extend(sub.get_active_jobscripts())
            sub_js = sub_js_

        js_direct: list[Jobscript] = []
        js_sched: list[Jobscript] = []
        for sub_idx, all_js_idx in sub_js.items():
            for js_idx in all_js_idx:
                try:
                    js = self.submissions[sub_idx].jobscripts[js_idx]
                except IndexError:
                    raise ValueError(
                        f"No jobscript with submission index {sub_idx!r} and/or "
                        f"jobscript index {js_idx!r}."
                    )
                if js.process_ID is not None:
                    js_direct.append(js)
                elif js.scheduler_job_ID is not None:
                    js_sched.append(js)
                else:
                    raise RuntimeError(
                        f"Process ID nor scheduler job ID is set for {js!r}."
                    )

        if js_direct or js_sched:
            # TODO: use a rich console status? how would that appear in stdout though?
            print("Waiting for workflow submissions to finish...")
        else:
            print("No running jobscripts.")
            return

        try:
            t_direct = Thread(target=self.__wait_for_direct_jobscripts, args=(js_direct,))
            t_sched = Thread(
                target=self.__wait_for_scheduled_jobscripts, args=(js_sched,)
            )
            t_direct.start()
            t_sched.start()

            # without these, KeyboardInterrupt seems to not be caught:
            while t_direct.is_alive():
                t_direct.join(timeout=1)

            while t_sched.is_alive():
                t_sched.join(timeout=1)

        except KeyboardInterrupt:
            print("No longer waiting (workflow execution will continue).")
        else:
            print("Specified submissions have finished.")

    def get_running_elements(
        self,
        submission_idx: int = -1,
        task_idx: int | None = None,
        task_insert_ID: int | None = None,
    ) -> list[Element]:
        """Retrieve elements that are running according to the scheduler."""

        if task_idx is not None and task_insert_ID is not None:
            raise ValueError("Specify at most one of `task_insert_ID` and `task_idx`.")

        # keys are task_insert_IDs, values are element indices:
        active_elems: dict[int, set[int]] = defaultdict(set)
        sub = self.submissions[submission_idx]
        for js_idx, states in sub.get_active_jobscripts().items():
            js = sub.jobscripts[js_idx]
            for js_elem_idx, state in states.items():
                if state is JobscriptElementState.running:
                    for task_iID, elem_idx in zip(
                        js.task_insert_IDs, js.task_elements[js_elem_idx]
                    ):
                        active_elems[task_iID].add(elem_idx)

        # retrieve Element objects:
        out: list[Element] = []
        for task_iID, elem_idxes in active_elems.items():
            if task_insert_ID is not None and task_iID != task_insert_ID:
                continue
            task = self.tasks.get(insert_ID=task_iID)
            if task_idx is not None and task_idx != task.index:
                continue
            for idx_i in elem_idxes:
                out.append(task.elements[idx_i])

        return out

    def get_running_runs(
        self,
        submission_idx: int = -1,
        task_idx: int | None = None,
        task_insert_ID: int | None = None,
        element_idx: int | None = None,
    ) -> list[ElementActionRun]:
        """Retrieve runs that are running according to the scheduler."""

        elems = self.get_running_elements(
            submission_idx=submission_idx,
            task_idx=task_idx,
            task_insert_ID=task_insert_ID,
        )
        out = []
        for elem in elems:
            if element_idx is not None and elem.index != element_idx:
                continue
            # for a given element, only one iteration will be running (assume for now the
            # this is the latest iteration, as provided by `action_runs`):
            for act_run in elem.action_runs:
                if act_run.status is EARStatus.running:
                    out.append(act_run)
                    break  # only one element action may be running at a time
        return out

    def _abort_run_ID(self, submission_idx: int, run_ID: int):
        """Modify the submission abort runs text file to signal that a run should be
        aborted."""
        self.submissions[submission_idx]._set_run_abort(run_ID)

    def abort_run(
        self,
        submission_idx: int = -1,
        task_idx: int | None = None,
        task_insert_ID: int | None = None,
        element_idx: int | None = None,
    ):
        """Abort the currently running action-run of the specified task/element.

        Parameters
        ----------
        task_idx
            The parent task of the run to abort.
        element_idx
            For multi-element tasks, the parent element of the run to abort.
        submission_idx
            Defaults to the most-recent submission.

        """
        running = self.get_running_runs(
            submission_idx=submission_idx,
            task_idx=task_idx,
            task_insert_ID=task_insert_ID,
            element_idx=element_idx,
        )
        if not running:
            raise ValueError("Specified run is not running.")

        elif len(running) > 1:
            if element_idx is None:
                elem_idx = tuple(ear.element.index for ear in running)
                raise ValueError(
                    f"Multiple elements are running (indices: {elem_idx!r}). Specify "
                    "which element index you want to abort."
                )
            else:
                raise RuntimeError("Multiple running runs.")

        run = running[0]
        if not run.action.abortable:
            raise RunNotAbortableError()
        self._abort_run_ID(submission_idx, run.id_)

    @TimeIt.decorator
    def cancel(self, hard: bool = False):
        """Cancel any running jobscripts."""
        for sub in self.submissions:
            sub.cancel()

    def add_submission(
        self, tasks: list[int] | None = None, JS_parallelism: bool | None = None
    ) -> Submission | None:
        """
        Add a job submission to this workflow.
        """
        # JS_parallelism=None means guess
        with self._store.cached_load(), self.batch_update():
            return self._add_submission(tasks, JS_parallelism)

    @TimeIt.decorator
    def _add_submission(
        self, tasks: Sequence[int] | None = None, JS_parallelism: bool | None = None
    ) -> Submission | None:
        new_idx = self.num_submissions
        _ = self.submissions  # TODO: just to ensure `submissions` is loaded
        sub_obj: Submission = self._app.Submission(
            index=new_idx,
            workflow=self,
            jobscripts=self.resolve_jobscripts(tasks),
            JS_parallelism=JS_parallelism,
        )
        sub_obj._set_environments()
        all_EAR_ID = [i for js in sub_obj.jobscripts for i in js.EAR_ID.flatten()]
        if not all_EAR_ID:
            print(
                "There are no pending element action runs, so a new submission was not "
                "added."
            )
            return None

        with self._store.cached_load(), self.batch_update():
            for id_ in all_EAR_ID:
                self._store.set_EAR_submission_index(EAR_ID=id_, sub_idx=new_idx)

        sub_obj_js, _ = sub_obj.to_json_like()
        assert self._submissions is not None
        self._submissions.append(sub_obj)
        self._pending["submissions"].append(new_idx)
        with self._store.cached_load(), self.batch_update():
            self._store.add_submission(new_idx, sub_obj_js)

        return self.submissions[new_idx]

    @TimeIt.decorator
    def resolve_jobscripts(self, tasks: Sequence[int] | None = None) -> list[Jobscript]:
        """
        Resolve this workflow to a set of job scripts to run.
        """
        js, element_deps = self._resolve_singular_jobscripts(tasks)
        js_deps = resolve_jobscript_dependencies(js, element_deps)

        for js_idx, jsca in js.items():
            if js_idx in js_deps:
                jsca["dependencies"] = js_deps[js_idx]

        js = merge_jobscripts_across_tasks(js)
        return [self._app.Jobscript(**jsca) for jsca in jobscripts_to_list(js)]

    def __EAR_obj_map(
        self,
        js_desc: JobScriptDescriptor,
        jsca: JobScriptCreationArguments,
        task: WorkflowTask,
        task_actions: Sequence[tuple[int, int, int]],
        EAR_map: NDArray,
    ) -> Mapping[int, ElementActionRun]:
        all_EAR_IDs: list[int] = []
        for js_elem_idx, (elem_idx, act_indices) in enumerate(
            js_desc["elements"].items()
        ):
            for act_idx in act_indices:
                EAR_ID_i: int = EAR_map[act_idx, elem_idx].item()
                all_EAR_IDs.append(EAR_ID_i)
                js_act_idx = task_actions.index((task.insert_ID, act_idx, 0))
                jsca["EAR_ID"][js_act_idx][js_elem_idx] = EAR_ID_i
        return dict(zip(all_EAR_IDs, self.get_EARs_from_IDs(all_EAR_IDs)))

    @TimeIt.decorator
    def _resolve_singular_jobscripts(
        self, tasks: Sequence[int] | None = None
    ) -> tuple[
        Mapping[int, JobScriptCreationArguments],
        Mapping[int, Mapping[int, Sequence[int]]],
    ]:
        """
        We arrange EARs into `EARs` and `elements` so we can quickly look up membership
        by EAR idx in the `EARs` dict.

        Returns
        -------
        submission_jobscripts
            Information for making each jobscript.
        all_element_deps
            For a given jobscript index, for a given jobscript element index within that
            jobscript, this is a list of EAR IDs dependencies of that element.
        """
        task_set = frozenset(tasks if tasks else range(self.num_tasks))

        if self._store.use_cache:
            # pre-cache parameter sources (used in `EAR.get_EAR_dependencies`):
            self.get_all_parameter_sources()

        submission_jobscripts: dict[int, JobScriptCreationArguments] = {}
        all_element_deps: dict[int, dict[int, list[int]]] = {}

        for task_iID, loop_idx_i in self.get_iteration_task_pathway():
            task = self.tasks.get(insert_ID=task_iID)
            if task.index not in task_set:
                continue
            res, res_hash, res_map, EAR_map = generate_EAR_resource_map(task, loop_idx_i)
            jobscripts, _ = group_resource_map_into_jobscripts(res_map)

            for js_dat in jobscripts:
                # (insert ID, action_idx, index into task_loop_idx):
                task_actions = sorted(
                    set(
                        (task.insert_ID, act_idx_i, 0)
                        for act_idx in js_dat["elements"].values()
                        for act_idx_i in act_idx
                    ),
                    key=lambda x: x[1],
                )
                # Invert the mapping
                task_actions_inv = {k: idx for idx, k in enumerate(task_actions)}
                # task_elements: { JS_ELEM_IDX: [TASK_ELEM_IDX for each task insert ID]}
                task_elements = {
                    js_elem_idx: [task_elem_idx]
                    for js_elem_idx, task_elem_idx in enumerate(js_dat["elements"])
                }
                EAR_idx_arr_shape = (
                    len(task_actions),
                    len(js_dat["elements"]),
                )
                EAR_ID_arr = np.empty(EAR_idx_arr_shape, dtype=np.int32)
                EAR_ID_arr[:] = -1

                new_js_idx = len(submission_jobscripts)

                js_i: JobScriptCreationArguments = {
                    "task_insert_IDs": [task.insert_ID],
                    "task_loop_idx": [loop_idx_i],
                    "task_actions": task_actions,  # map jobscript actions to task actions
                    "task_elements": task_elements,  # map jobscript elements to task elements
                    "EAR_ID": EAR_ID_arr,
                    "resources": res[js_dat["resources"]],
                    "resource_hash": res_hash[js_dat["resources"]],
                    "dependencies": {},
                }

                all_EAR_objs = self.__EAR_obj_map(
                    js_dat, js_i, task, task_actions, EAR_map
                )

                for js_elem_idx, (elem_idx, act_indices) in enumerate(
                    js_dat["elements"].items()
                ):
                    all_EAR_IDs: list[int] = []
                    for act_idx in act_indices:
                        EAR_ID_i: int = EAR_map[act_idx, elem_idx].item()
                        all_EAR_IDs.append(EAR_ID_i)
                        js_act_idx = task_actions_inv[task.insert_ID, act_idx, 0]
                        EAR_ID_arr[js_act_idx][js_elem_idx] = EAR_ID_i

                    # get indices of EARs that this element depends on:
                    EAR_deps_EAR_idx = [
                        dep_ear_id
                        for main_ear_id in all_EAR_IDs
                        for dep_ear_id in all_EAR_objs[main_ear_id].get_EAR_dependencies()
                        if dep_ear_id not in EAR_ID_arr
                    ]
                    if EAR_deps_EAR_idx:
                        all_element_deps.setdefault(new_js_idx, {})[
                            js_elem_idx
                        ] = EAR_deps_EAR_idx

                submission_jobscripts[new_js_idx] = js_i

        return submission_jobscripts, all_element_deps

    def __get_commands(
        self, jobscript: Jobscript, JS_action_idx: int, ear: ElementActionRun
    ):
        try:
            commands, shell_vars = ear.compose_commands(jobscript, JS_action_idx)
        except OutputFileParserNoOutputError:
            # no commands to write but still need to write the file,
            # the jobscript is expecting it.
            return ""

        self._app.persistence_logger.debug("need to write commands")
        pieces = [commands]
        for cmd_idx, var_dat in shell_vars.items():
            for param_name, shell_var_name, st_typ in var_dat:
                pieces.append(
                    jobscript.shell.format_save_parameter(
                        workflow_app_alias=jobscript.workflow_app_alias,
                        param_name=param_name,
                        shell_var_name=shell_var_name,
                        EAR_ID=ear.id_,
                        cmd_idx=cmd_idx,
                        stderr=(st_typ == "stderr"),
                    )
                )
        commands = jobscript.shell.wrap_in_subshell("".join(pieces), ear.action.abortable)

        # add loop-check command if this is the last action of this loop iteration
        # for this element:
        if self.loops:
            final_runs = (
                # TODO: excessive reads here
                self.get_iteration_final_run_IDs(id_lst=jobscript.all_EAR_IDs)
            )
            self._app.persistence_logger.debug(f"final_runs: {final_runs!r}")
            pieces = []
            for loop_name, run_IDs in final_runs.items():
                if ear.id_ in run_IDs:
                    loop_cmd = jobscript.shell.format_loop_check(
                        workflow_app_alias=jobscript.workflow_app_alias,
                        loop_name=loop_name,
                        run_ID=ear.id_,
                    )
                    pieces.append(jobscript.shell.wrap_in_subshell(loop_cmd, False))
            commands += "".join(pieces)
        return commands

    def write_commands(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_action_idx: int,
        EAR_ID: int,
    ) -> None:
        """Write run-time commands for a given EAR."""
        with self._store.cached_load():
            self._app.persistence_logger.debug("Workflow.write_commands")
            self._app.persistence_logger.debug(
                f"loading jobscript (submission index: {submission_idx}; jobscript "
                f"index: {jobscript_idx})"
            )
            jobscript = self.submissions[submission_idx].jobscripts[jobscript_idx]
            self._app.persistence_logger.debug(f"loading run {EAR_ID!r}")
            EAR = self.get_EARs_from_IDs(EAR_ID)
            self._app.persistence_logger.debug(f"run {EAR_ID!r} loaded: {EAR!r}")
            commands = self.__get_commands(jobscript, JS_action_idx, EAR)
            self._app.persistence_logger.debug(f"commands to write: {commands!r}")
            cmd_file_name = jobscript.get_commands_file_name(JS_action_idx)
            with Path(cmd_file_name).open("wt", newline="\n") as fp:
                # (assuming we have CD'd correctly to the element run directory)
                fp.write(commands)

    def process_shell_parameter_output(
        self, name: str, value: str, EAR_ID: int, cmd_idx: int, stderr: bool = False
    ) -> Any:
        """Process the shell stdout/stderr stream according to the associated Command
        object."""
        with self._store.cached_load(), self.batch_update():
            EAR = self.get_EARs_from_IDs(EAR_ID)
            command = EAR.action.commands[cmd_idx]
            return command.process_std_stream(name, value, stderr)

    def save_parameter(
        self,
        name: str,
        value: Any,
        EAR_ID: int,
    ):
        """
        Save a parameter where an EAR can find it.
        """
        self._app.logger.info(f"save parameter {name!r} for EAR_ID {EAR_ID}.")
        self._app.logger.debug(f"save parameter {name!r} value is {value!r}.")
        with self._store.cached_load(), self.batch_update():
            EAR = self.get_EARs_from_IDs(EAR_ID)
            param_id = EAR.data_idx[name]
            self.set_parameter_value(param_id, value)

    def show_all_EAR_statuses(self) -> None:
        """
        Print a description of the status of every element action run in
        the workflow.
        """
        print(
            f"{'task':8s} {'element':8s} {'iteration':8s} {'action':8s} "
            f"{'run':8s} {'sub.':8s} {'exitcode':8s} {'success':8s} {'skip':8s}"
        )
        for task in self.tasks:
            for element in task.elements[:]:
                for iter_idx, iteration in enumerate(element.iterations):
                    for act_idx, action_runs in iteration.actions.items():
                        for run_idx, EAR in enumerate(action_runs.runs):
                            suc = EAR.success if EAR.success is not None else "-"
                            if EAR.exit_code is not None:
                                exc = f"{EAR.exit_code:^8d}"
                            else:
                                exc = f"{'-':^8}"
                            print(
                                f"{task.insert_ID:^8d} {element.index:^8d} "
                                f"{iter_idx:^8d} {act_idx:^8d} {run_idx:^8d} "
                                f"{EAR.status.name.lower():^8s}"
                                f"{exc}"
                                f"{suc:^8}"
                                f"{EAR.skip:^8}"
                            )

    def _resolve_input_source_task_reference(
        self, input_source: InputSource, new_task_name: str
    ) -> None:
        """Normalise the input source task reference and convert a source to a local type
        if required."""

        # TODO: test thoroughly!

        if isinstance(input_source.task_ref, str):
            if input_source.task_ref == new_task_name:
                if input_source.task_source_type is self._app.TaskSourceType.OUTPUT:
                    raise InvalidInputSourceTaskReference(input_source)
                warn(
                    f"Changing input source {input_source.to_string()!r} to a local "
                    f"type, since the input source task reference refers to its own "
                    f"task."
                )
                # TODO: add an InputSource source_type setter to reset
                # task_ref/source_type?
                input_source.source_type = self._app.InputSourceType.LOCAL
                input_source.task_ref = None
                input_source.task_source_type = None
            else:
                try:
                    uniq_names_cur = self.get_task_unique_names(map_to_insert_ID=True)
                    input_source.task_ref = uniq_names_cur[input_source.task_ref]
                except KeyError:
                    raise InvalidInputSourceTaskReference(
                        input_source, input_source.task_ref
                    )

    def get_all_submission_run_IDs(self) -> Iterable[int]:
        """
        Get the run IDs of all submissions.
        """
        self._app.persistence_logger.debug("Workflow.get_all_submission_run_IDs")
        for sub in self.submissions:
            yield from sub.all_EAR_IDs

    def check_loop_termination(self, loop_name: str, run_ID: int) -> None:
        """Check if a loop should terminate, given the specified completed run, and if so,
        set downstream iteration runs to be skipped."""
        loop = self.loops.get(loop_name)
        elem_iter = self.get_EARs_from_IDs(run_ID).element_iteration
        if loop.test_termination(elem_iter):
            # run IDs of downstream iterations that can be skipped
            to_skip: set[int] = set()
            elem_id = elem_iter.element.id_
            loop_map = self.get_loop_map()  # over all jobscripts
            for iter_idx, iter_dat in loop_map[loop_name][elem_id].items():
                if iter_idx > elem_iter.index:
                    to_skip.update(itr_d.id_ for itr_d in iter_dat)
            self._app.logger.info(
                f"Loop {loop_name!r} termination condition met for run_ID {run_ID!r}."
            )
            for run_ID in to_skip:
                self.set_EAR_skip(run_ID)

    def get_loop_map(
        self, id_lst: Iterable[int] | None = None
    ) -> Mapping[str, Mapping[int, Mapping[int, Sequence[_IterationData]]]]:
        """
        Get a description of what is going on with looping.
        """
        # TODO: test this works across multiple jobscripts
        self._app.persistence_logger.debug("Workflow.get_loop_map")
        if id_lst is None:
            id_lst = self.get_all_submission_run_IDs()
        loop_map: dict[str, dict[int, dict[int, list[_IterationData]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for EAR in self.get_EARs_from_IDs(id_lst):
            for loop_name, iter_idx in EAR.element_iteration.loop_idx.items():
                act_idx = EAR.element_action.action_idx
                loop_map[loop_name][EAR.element.id_][iter_idx].append(
                    _IterationData(EAR.id_, act_idx)
                )
        return loop_map

    def get_iteration_final_run_IDs(
        self,
        id_lst: Iterable[int] | None = None,
    ) -> Mapping[str, Sequence[int]]:
        """Retrieve the run IDs of those runs that correspond to the final action within
        a named loop iteration.

        These runs represent the final action of a given element-iteration; this is used to
        identify which commands file to append a loop-termination check to.
        """
        self._app.persistence_logger.debug("Workflow.get_iteration_final_run_IDs")

        loop_map = self.get_loop_map(id_lst)

        # find final EARs for each loop:
        final_runs: dict[str, list[int]] = defaultdict(list)
        for loop_name, dat in loop_map.items():
            for elem_dat in dat.values():
                for iter_dat in elem_dat.values():
                    final_runs[loop_name].append(max(iter_dat, key=lambda x: x.idx).id_)
        return final_runs

    def rechunk_runs(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ):
        """
        Reorganise the stored data chunks for EARs to be more efficient.
        """
        self._store.rechunk_runs(chunk_size=chunk_size, backup=backup, status=status)

    def rechunk_parameter_base(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ):
        """
        Reorganise the stored data chunks for parameterss to be more efficient.
        """
        self._store.rechunk_parameter_base(
            chunk_size=chunk_size, backup=backup, status=status
        )

    def rechunk(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ):
        """
        Rechunk metadata/runs and parameters/base arrays, making them more efficient.
        """
        self.rechunk_runs(chunk_size=chunk_size, backup=backup, status=status)
        self.rechunk_parameter_base(chunk_size=chunk_size, backup=backup, status=status)


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parameterise.
    (For example, fitting workflows.)"""

    #: The template inside this blueprint.
    workflow_template: WorkflowTemplate
