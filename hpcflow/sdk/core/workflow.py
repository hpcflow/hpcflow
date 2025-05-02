"""
Main workflow model.
"""

from __future__ import annotations
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager, nullcontext
import copy
from dataclasses import dataclass, field

from functools import wraps
import os
from pathlib import Path
import random
import shutil
import string
from threading import Thread
import time
from typing import overload, cast, TYPE_CHECKING, TypeVar
from typing_extensions import ParamSpec, Concatenate

from uuid import uuid4
from warnings import warn
from fsspec.implementations.local import LocalFileSystem  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
import numpy as np
from fsspec.core import url_to_fs  # type: ignore
from rich import print as rich_print
import rich.console
import rich.panel
import rich.table
import rich.text
import rich.box


from hpcflow.sdk import app
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.config.errors import (
    ConfigNonConfigurableError,
    UnknownMetaTaskConstitutiveSchema,
)
from hpcflow.sdk.core import (
    ALL_TEMPLATE_FORMATS,
    ABORT_EXIT_CODE,
    RUN_DIR_ARR_FILL,
    SKIPPED_EXIT_CODE,
    NO_COMMANDS_EXIT_CODE,
)
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.cache import ObjectCache
from hpcflow.sdk.core.loop_cache import LoopCache, LoopIndex
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.persistence import store_cls_from_str
from hpcflow.sdk.persistence.defaults import DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, infer_store
from hpcflow.sdk.submission.jobscript import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
    is_jobscript_array,
    merge_jobscripts_across_tasks,
    resolve_jobscript_blocks,
    resolve_jobscript_dependencies,
)
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.submission.submission import Submission
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.utils.strings import shorten_list_str
from hpcflow.sdk.core.utils import (
    read_JSON_file,
    read_JSON_string,
    read_YAML_str,
    read_YAML_file,
    redirect_std_to_file,
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
    UnsetParameterDataErrorBase,
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
    from .actions import ElementActionRun, UnsetParamTracker
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
        WorkflowTemplateElementSetData,
        BlockActionKey,
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
    from .json_like import JSONed

    #: Convenience alias
    _TemplateComponents: TypeAlias = "dict[str, ObjectList[JSONLike]]"

P = ParamSpec("P")
T = TypeVar("T")
S = TypeVar("S", bound="Workflow")


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
        Whether to merge the environments into task resources.
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
    config: dict = field(default_factory=lambda: {})
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
    #: Whether to merge the environments into task resources.
    merge_envs: bool = True

    def __post_init__(self) -> None:

        # TODO: in what scenario is the reindex required? are loops initialised?

        # replace metatasks with tasks
        new_tasks: list[Task] = []
        do_reindex = False
        reindex = {}
        for task_idx, i in enumerate(self.tasks):
            if isinstance(i, app.MetaTask):
                do_reindex = True
                tasks_from_meta = copy.deepcopy(i.tasks)
                reindex[task_idx] = [
                    len(new_tasks) + i for i in range(len(tasks_from_meta))
                ]
                new_tasks.extend(tasks_from_meta)
            else:
                reindex[task_idx] = [len(new_tasks)]
                new_tasks.append(i)
        if do_reindex:
            if self.loops:
                for loop_idx, loop in enumerate(cast("list[dict[str, Any]]", self.loops)):
                    loop["tasks"] = [j for i in loop["tasks"] for j in reindex[i]]
                    term_task = loop.get("termination_task")
                    if term_task is not None:
                        loop["termination_task"] = reindex[term_task][0]

        self.tasks = new_tasks

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

        if self.config:
            # don't do a full validation (which would require loading the config file),
            # just check all specified keys are configurable:
            bad_keys = set(self.config) - set(self._app.config_options._configurable_keys)
            if bad_keys:
                raise ConfigNonConfigurableError(name=bad_keys)

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
        def _normalise_task_parametrisation(task_lst: list[WorkflowTemplateTaskData]):
            """
            For each dict in a list of task parametrisations, ensure the `schema` key is
            a list of values, and ensure `element_sets` are defined.

            This mutates `task_lst`.

            """
            # use element_sets if not already:
            task_dat: WorkflowTemplateTaskData
            for task_idx, task_dat in enumerate(task_lst):
                schema = task_dat.pop("schema")
                schema_list: list = schema if isinstance(schema, list) else [schema]
                if "element_sets" in task_dat:
                    # just update the schema to a list:
                    task_lst[task_idx]["schema"] = schema_list
                else:
                    # add a single element set, and update the schema to a list:
                    out_labels = task_dat.pop("output_labels", [])
                    es_dat = cast("WorkflowTemplateElementSetData", task_dat)
                    new_task_dat: WorkflowTemplateTaskData = {
                        "schema": schema_list,
                        "element_sets": [es_dat],
                        "output_labels": out_labels,
                    }
                    task_lst[task_idx] = new_task_dat
                # move sequences with `paths` (note: plural) to multi_path_sequences:
                for elem_set in task_lst[task_idx]["element_sets"]:
                    new_mps = []
                    seqs = elem_set.get("sequences", [])
                    seqs = list(seqs)  # copy
                    # loop in reverse so indices for pop are valid:
                    for seq_idx, seq_dat in zip(range(len(seqs) - 1, -1, -1), seqs[::-1]):
                        if "paths" in seq_dat:  # (note: plural)
                            # move to a multi-path sequence:
                            new_mps.append(elem_set["sequences"].pop(seq_idx))
                    elem_set.setdefault("multi_path_sequences", []).extend(new_mps[::-1])

        meta_tasks = data.pop("meta_tasks", {})
        if meta_tasks:
            for i in list(meta_tasks):
                _normalise_task_parametrisation(meta_tasks[i])
            new_task_dat: list[WorkflowTemplateTaskData] = []
            reindex = {}
            for task_idx, task_dat in enumerate(data["tasks"]):
                if meta_task_dat := meta_tasks.get(task_dat["schema"]):
                    reindex[task_idx] = [
                        len(new_task_dat) + i for i in range(len(meta_task_dat))
                    ]

                    all_schema_names = [j for i in meta_task_dat for j in i["schema"]]

                    # update any parametrisation provided in the task list:
                    base_data = copy.deepcopy(meta_task_dat)

                    # any other keys in `task_dat` should be mappings whose keys are
                    # the schema name (within the meta task) optionally suffixed by
                    # a period and the element set index to which the updates should be
                    # copied (no integer suffix indicates the zeroth element set):
                    for k, v in task_dat.items():
                        if k == "schema":
                            continue

                        for elem_set_id, dat in v.items():

                            elem_set_id_split = elem_set_id.split(".")
                            try:
                                es_idx = int(elem_set_id_split[-1])
                                schema_name = ".".join(elem_set_id_split[:-1])
                            except ValueError:
                                es_idx = 0
                                schema_name = ".".join(elem_set_id_split)
                            schema_name = schema_name.strip(".")

                            # check valid schema name:
                            if schema_name not in all_schema_names:
                                raise UnknownMetaTaskConstitutiveSchema(
                                    f"Task schema with objective {schema_name!r} is not "
                                    f"part of the meta-task with objective "
                                    f"{task_dat['schema']!r}. The constitutive schemas of"
                                    f" this meta-task have objectives: "
                                    f"{all_schema_names!r}."
                                )

                            # copy `dat` to the correct schema and element set in the
                            # meta-task:
                            for s_idx, s in enumerate(base_data):
                                if s["schema"] == [schema_name]:
                                    if k == "inputs":
                                        # special case; merge inputs
                                        base_data[s_idx]["element_sets"][es_idx][
                                            k
                                        ].update(dat)
                                    else:
                                        # just overwrite
                                        base_data[s_idx]["element_sets"][es_idx][k] = dat

                    new_task_dat.extend(base_data)

                else:
                    reindex[task_idx] = [len(new_task_dat)]
                    new_task_dat.append(task_dat)

            data["tasks"] = new_task_dat

            if loops := data.get("loops"):
                for loop_idx, loop in enumerate(loops):
                    loops[loop_idx]["tasks"] = [
                        j for i in loop["tasks"] for j in reindex[i]
                    ]
                    term_task = loop.get("termination_task")
                    if term_task is not None:
                        loops[loop_idx]["termination_task"] = reindex[term_task][0]

        _normalise_task_parametrisation(data["tasks"])

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

        if mts_dat := tcs.pop("meta_task_schemas", []):
            meta_ts = [
                cls._app.MetaTaskSchema.from_json_like(
                    i, shared_data=cls._app.template_components
                )
                for i in mts_dat
            ]
            cls._app.task_schemas.add_objects(meta_ts, skip_duplicates=True)

        wkt = cls.from_json_like(data, shared_data=cls._app._shared_data)

        # print(f"WorkflowTemplate._from_data: {wkt=!r}")
        # TODO: what is this for!?
        # for idx, task in enumerate(wkt.tasks):
        #     if isinstance(task.schema, cls._app.MetaTaskSchema):
        #         print(f"{task=!r}")
        #         wkt.tasks[idx] = cls._app.MetaTask(schema=task.schema, tasks=task.tasks)
        return wkt

    @classmethod
    @TimeIt.decorator
    def from_YAML_string(
        cls,
        string: str,
        variables: dict[str, str] | Literal[False] | None = None,
    ) -> WorkflowTemplate:
        """Load from a YAML string.

        Parameters
        ----------
        string
            The YAML string containing the workflow template parametrisation.
        variables
            String variables to substitute in `string`. Substitutions will be attempted if
            the YAML string looks to contain variable references (like "<<var:name>>"). If
            set to `False`, no substitutions will occur, which may result in an invalid
            workflow template!
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
        variables: dict[str, str] | Literal[False] | None = None,
    ) -> WorkflowTemplate:
        """Load from a YAML file.

        Parameters
        ----------
        path
            The path to the YAML file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`. Substitutions will
            be attempted if the YAML file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!

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
        variables: dict[str, str] | Literal[False] | None = None,
    ) -> WorkflowTemplate:
        """Load from a JSON string.

        Parameters
        ----------
        string
            The JSON string containing the workflow template parametrisation.
        variables
            String variables to substitute in `string`. Substitutions will be attempted if
            the JSON string looks to contain variable references (like "<<var:name>>"). If
            set to `False`, no substitutions will occur, which may result in an invalid
            workflow template!
        """
        return cls._from_data(read_JSON_string(string, variables=variables))

    @classmethod
    @TimeIt.decorator
    def from_JSON_file(
        cls,
        path: PathLike,
        variables: dict[str, str] | Literal[False] | None = None,
    ) -> WorkflowTemplate:
        """Load from a JSON file.

        Parameters
        ----------
        path
            The path to the JSON file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`. Substitutions will
            be attempted if the JSON file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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
        variables: dict[str, str] | Literal[False] | None = None,
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
            String variables to substitute in the file given by `path`. Substitutions will
            be attempted if the file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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


def load_workflow_config(
    func: Callable[Concatenate[S, P], T],
) -> Callable[Concatenate[S, P], T]:
    """Decorator to apply workflow-level config items during execution of a Workflow
    method."""

    @wraps(func)
    def wrapped(self: S, *args: P.args, **kwargs: P.kwargs) -> T:

        updates = self.template.config
        if updates:
            with self._app.config._with_updates(updates):
                return func(self, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    return wrapped


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

        self._use_merged_parameters_cache = False
        self._merged_parameters_cache: dict[
            tuple[str | None, tuple[tuple[str, tuple[int, ...] | int], ...]], Any
        ] = {}

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

        # reassigned within `ElementActionRun.raise_on_failure_threshold` context manager:
        self._is_tracking_unset: bool = False
        self._tracked_unset: dict[str, UnsetParamTracker] | None = None

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
            elif self._store.fs.protocol == ("file", "local"):
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
                    if status:
                        status.update(
                            f"Added {len(template.loops)} loops. "
                            f"Committing to store..."
                        )
        except (Exception, NotImplementedError):
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
        variables: dict[str, str] | Literal[False] | None = None,
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
            String variables to substitute in the file given by `YAML_path`. Substitutions
            will be attempted if the YAML file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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
        variables: dict[str, str] | Literal[False] | None = None,
        status: Status | None = None,
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
            String variables to substitute in the string `YAML_str`. Substitutions will be
            attempted if the YAML string looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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
            status,
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
        variables: dict[str, str] | Literal[False] | None = None,
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
            String variables to substitute in the file given by `JSON_path`. Substitutions
            will be attempted if the JSON file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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
        variables: dict[str, str] | Literal[False] | None = None,
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
            String variables to substitute in the string `JSON_str`. Substitutions will be
            attempted if the JSON string looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
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
        variables: dict[str, str] | Literal[False] | None = None,
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
            Substitutions will be attempted if the file looks to contain variable
            references (like "<<var:name>>"). If set to `False`, no substitutions will
            occur, which may result in an invalid workflow template!
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
        config: dict | None = None,
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
        config:
            Configuration items that should be set whenever the resulting workflow is
            loaded. This includes config items that apply during workflow execution.
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
            config=config or {},
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
            output_parameters=wk_loop.output_parameters,
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
                new_wk_loop.add_iteration(cache=cache_, status=status)

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
    @TimeIt.decorator
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
                        output_parameters=loop_dat["output_parameters"],
                    )
                    for idx, loop_dat in self._store.get_loops().items()
                )
        return self._loops

    @property
    @TimeIt.decorator
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
    def get_EARs_from_IDs(self, ids: Iterable[int]) -> list[ElementActionRun]: ...

    @overload
    def get_EARs_from_IDs(self, ids: int) -> ElementActionRun: ...

    @TimeIt.decorator
    def get_EARs_from_IDs(
        self, ids: Iterable[int] | int, as_dict: bool = False
    ) -> list[ElementActionRun] | dict[int, ElementActionRun] | ElementActionRun:
        """Get element action run objects from a list of IDs."""
        id_lst = [ids] if isinstance(ids, int) else list(ids)

        with self._store.cached_load(), self._store.cache_ctx():

            self._app.persistence_logger.debug(
                f"get_EARs_from_IDs: {len(id_lst)} EARs: {shorten_list_str(id_lst)}."
            )

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
                run_idx = (
                    it.EAR_IDs[act_idx].index(rn.id_) if it.EAR_IDs is not None else -1
                )
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

            result = {}
            for path in index_paths:
                run = (
                    iters[path.task][path.elem][path.iter]
                    .actions[path.act]
                    .runs[path.run]
                )
                result[run.id_] = run

            if not as_dict:
                res_lst = list(result.values())
                return res_lst[0] if isinstance(ids, int) else res_lst

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

    @contextmanager
    def cached_merged_parameters(self):
        if self._use_merged_parameters_cache:
            yield
        else:
            try:
                self._app.logger.debug("entering merged-parameters cache.")
                self._use_merged_parameters_cache = True
                yield
            finally:
                self._app.logger.debug("exiting merged-parameters cache.")
                self._use_merged_parameters_cache = False
                self._merged_parameters_cache = {}  # reset the cache

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
            shutil.copy(src, wk.artifacts_path.joinpath(src.name))

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
    ) -> Sequence[str]: ...

    @overload
    def get_task_unique_names(
        self, map_to_insert_ID: Literal[True]
    ) -> Mapping[str, int]: ...

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
        return (
            len(self._submissions)
            if self._submissions is not None
            else self._store._get_num_total_submissions()
        )

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

    def set_EAR_start(
        self, run_id: int, run_dir: Path | None, port_number: int | None
    ) -> None:
        """Set the start time on an EAR."""
        self._app.logger.debug(f"Setting start for EAR ID {run_id!r}")
        with self._store.cached_load(), self.batch_update():
            self._store.set_EAR_start(run_id, run_dir, port_number)

    def set_multi_run_starts(
        self, run_ids: list[int], run_dirs: list[Path | None], port_number: int
    ) -> None:
        """Set the start time on multiple runs."""
        self._app.logger.debug(f"Setting start for multiple run IDs {run_ids!r}")
        with self._store.cached_load(), self.batch_update():
            self._store.set_multi_run_starts(run_ids, run_dirs, port_number)

    def set_EAR_end(
        self,
        block_act_key: BlockActionKey,
        run: ElementActionRun,
        exit_code: int,
    ) -> None:
        """Set the end time and exit code on an EAR.

        If the exit code is non-zero, also set all downstream dependent EARs to be
        skipped. Also save any generated input/output files.

        """
        self._app.logger.debug(
            f"Setting end for run ID {run.id_!r} with exit code {exit_code!r}."
        )
        param_id: int | list[int] | None
        with self._store.cached_load(), self.batch_update():
            success = exit_code == 0  # TODO  more sophisticated success heuristics
            if not run.skip:

                is_aborted = False
                if run.action.abortable and exit_code == ABORT_EXIT_CODE:
                    # the point of aborting an EAR is to continue with the workflow:
                    is_aborted = True
                    success = True

                run_dir = run.get_directory()
                if run_dir:
                    assert isinstance(run_dir, Path)
                    for IFG_i in run.action.input_file_generators:
                        inp_file = IFG_i.input_file
                        self._app.logger.debug(
                            f"Saving EAR input file: {inp_file.label!r} for EAR ID "
                            f"{run.id_!r}."
                        )
                        param_id = run.data_idx[f"input_files.{inp_file.label}"]

                        file_paths = inp_file.value(directory=run_dir)
                        for path_i in (
                            file_paths if isinstance(file_paths, list) else [file_paths]
                        ):
                            full_path = run_dir.joinpath(path_i)
                            if not full_path.exists():
                                self._app.logger.debug(
                                    f"expected input file {path_i!r} does not "
                                    f"exist, so setting run to an error state "
                                    f"(if not aborted)."
                                )
                                if not is_aborted and success is True:
                                    # this is unlikely to happen, but could happen
                                    # if the input file is deleted in between
                                    # the input file generator completing and this
                                    # code being run
                                    success = False
                                    exit_code = 1  # TODO more custom exit codes?
                            else:
                                self._set_file(
                                    param_id=param_id,
                                    store_contents=True,  # TODO: make optional according to IFG
                                    is_input=False,
                                    path=full_path,
                                )

                    if run.action.script_data_out_has_files:
                        try:
                            run._param_save(block_act_key, run_dir)
                        except FileNotFoundError:
                            self._app.logger.debug(
                                f"script did not generate an expected output parameter "
                                f"file (block_act_key={block_act_key!r}), so setting run "
                                f"to an error state (if not aborted)."
                            )
                            if not is_aborted and success is True:
                                success = False
                                exit_code = 1  # TODO more custom exit codes?

                    # Save action-level files: (TODO: refactor with below for OFPs)
                    for save_file_j in run.action.save_files:
                        self._app.logger.debug(
                            f"Saving file: {save_file_j.label!r} for EAR ID "
                            f"{run.id_!r}."
                        )
                        try:
                            param_id = run.data_idx[f"output_files.{save_file_j.label}"]
                        except KeyError:
                            # We might be saving a file that is not a defined
                            # "output file"; this will avoid saving a reference in the
                            # parameter data:
                            param_id = None

                        file_paths = save_file_j.value(directory=run_dir)
                        self._app.logger.debug(
                            f"Saving output file paths: {file_paths!r}"
                        )

                        for path_i in (
                            file_paths if isinstance(file_paths, list) else [file_paths]
                        ):
                            full_path = run_dir.joinpath(path_i)
                            if not full_path.exists():
                                self._app.logger.debug(
                                    f"expected file to save {path_i!r} does not "
                                    f"exist, so setting run to an error state "
                                    f"(if not aborted)."
                                )
                                if not is_aborted and success is True:
                                    # this is unlikely to happen, but could happen
                                    # if the input file is deleted in between
                                    # the input file generator completing and this
                                    # code being run
                                    success = False
                                    exit_code = 1  # TODO more custom exit codes?
                            else:
                                self._set_file(
                                    param_id=param_id,
                                    store_contents=True,
                                    is_input=False,
                                    path=full_path,
                                    clean_up=(save_file_j in run.action.clean_up),
                                )

                    for OFP_i in run.action.output_file_parsers:
                        for save_file_j in OFP_i._save_files:
                            self._app.logger.debug(
                                f"Saving EAR output file: {save_file_j.label!r} for EAR ID "
                                f"{run.id_!r}."
                            )
                            try:
                                param_id = run.data_idx[
                                    f"output_files.{save_file_j.label}"
                                ]
                            except KeyError:
                                # We might be saving a file that is not a defined
                                # "output file"; this will avoid saving a reference in the
                                # parameter data:
                                param_id = None

                            file_paths = save_file_j.value(directory=run_dir)
                            self._app.logger.debug(
                                f"Saving EAR output file paths: {file_paths!r}"
                            )

                            for path_i in (
                                file_paths
                                if isinstance(file_paths, list)
                                else [file_paths]
                            ):
                                full_path = run_dir.joinpath(path_i)
                                if not full_path.exists():
                                    self._app.logger.debug(
                                        f"expected output file parser `save_files` file "
                                        f"{path_i!r} does not exist, so setting run "
                                        f"to an error state (if not aborted)."
                                    )
                                    if not is_aborted and success is True:
                                        success = False
                                        exit_code = 1  # TODO more custom exit codes?
                                else:
                                    self._set_file(
                                        param_id=param_id,
                                        store_contents=True,  # TODO: make optional according to OFP
                                        is_input=False,
                                        path=full_path,
                                        clean_up=(save_file_j in OFP_i.clean_up),
                                    )

            if (
                run.resources.skip_downstream_on_failure
                and not success
                and run.skip_reason is not SkipReason.LOOP_TERMINATION
            ):
                # loop termination skips are already propagated
                for EAR_dep_ID in run.get_dependent_EARs(as_objects=False):
                    self._app.logger.debug(
                        f"Setting EAR ID {EAR_dep_ID!r} to skip because it depends on"
                        f" EAR ID {run.id_!r}, which exited with a non-zero exit code:"
                        f" {exit_code!r}."
                    )
                    self._store.set_EAR_skip(
                        {EAR_dep_ID: SkipReason.UPSTREAM_FAILURE.value}
                    )

            self._store.set_EAR_end(run.id_, exit_code, success, run.action.requires_dir)

    def set_multi_run_ends(
        self,
        runs: dict[
            BlockActionKey,
            list[tuple[ElementActionRun, int, Path | None]],
        ],
    ) -> None:
        """Set end times and exit codes on multiple runs.

        If the exit code is non-zero, also set all downstream dependent runs to be
        skipped. Also save any generated input/output files."""

        self._app.logger.debug(f"Setting end for multiple run IDs.")
        param_id: int | list[int] | None
        with self._store.cached_load(), self.batch_update():
            run_ids = []
            run_dirs = []
            exit_codes = []
            successes = []
            for block_act_key, run_dat in runs.items():
                for run, exit_code, run_dir in run_dat:

                    success = (
                        exit_code == 0
                    )  # TODO  more sophisticated success heuristics
                    self._app.logger.info(
                        f"setting end for run {run.id_} with exit_code={exit_code}, "
                        f"success={success}, skip={run.skip!r}, and skip_reason="
                        f"{run.skip_reason!r}."
                    )
                    if not run.skip:
                        self._app.logger.info(f"run was not skipped.")
                        is_aborted = False
                        if run.action.abortable and exit_code == ABORT_EXIT_CODE:
                            # the point of aborting an EAR is to continue with the
                            # workflow:
                            self._app.logger.info(
                                "run was abortable and exit code was ABORT_EXIT_CODE,"
                                " so setting success to True."
                            )
                            is_aborted = True
                            success = True

                        run_dir = run.get_directory()
                        if run_dir:
                            assert isinstance(run_dir, Path)
                            for IFG_i in run.action.input_file_generators:
                                self._app.logger.info(f"setting IFG file {IFG_i!r}")
                                inp_file = IFG_i.input_file
                                self._app.logger.debug(
                                    f"Saving EAR input file: {inp_file.label!r} for EAR "
                                    f"ID {run.id_!r}."
                                )
                                param_id = run.data_idx[f"input_files.{inp_file.label}"]

                                file_paths = inp_file.value(directory=run_dir)
                                for path_i in (
                                    file_paths
                                    if isinstance(file_paths, list)
                                    else [file_paths]
                                ):
                                    full_path = run_dir.joinpath(path_i)
                                    if not full_path.exists():
                                        self._app.logger.debug(
                                            f"expected input file {path_i!r} does not "
                                            f"exist, so setting run to an error state "
                                            f"(if not aborted)."
                                        )
                                        if not is_aborted and success is True:
                                            # this is unlikely to happen, but could happen
                                            # if the input file is deleted in between
                                            # the input file generator completing and this
                                            # code being run
                                            success = False
                                            exit_code = 1  # TODO more custom exit codes?
                                    else:
                                        self._set_file(
                                            param_id=param_id,
                                            store_contents=True,  # TODO: make optional according to IFG
                                            is_input=False,
                                            path=full_path,
                                        )

                            if run.action.script_data_out_has_files:
                                self._app.logger.info(
                                    f"saving script-generated parameters."
                                )
                                try:
                                    run._param_save(block_act_key, run_dir)
                                except FileNotFoundError:
                                    # script did not generate the output parameter file, so
                                    # set a failed exit code (if we did not abort the run):
                                    self._app.logger.debug(
                                        f"script did not generate an expected output "
                                        f"parameter file (block_act_key="
                                        f"{block_act_key!r}), so setting run to an error "
                                        f"state (if not aborted)."
                                    )
                                    if not is_aborted and success is True:
                                        success = False
                                        exit_code = 1  # TODO more custom exit codes?

                            # Save action-level files: (TODO: refactor with below for OFPs)
                            for save_file_j in run.action.save_files:
                                self._app.logger.info(
                                    f"saving action-level file {save_file_j!r}."
                                )
                                self._app.logger.debug(
                                    f"Saving file: {save_file_j.label!r} for EAR ID "
                                    f"{run.id_!r}."
                                )
                                try:
                                    param_id = run.data_idx[
                                        f"output_files.{save_file_j.label}"
                                    ]
                                except KeyError:
                                    # We might be saving a file that is not a defined
                                    # "output file"; this will avoid saving a reference in
                                    # the parameter data:
                                    param_id = None

                                file_paths = save_file_j.value(directory=run_dir)
                                self._app.logger.debug(
                                    f"Saving output file paths: {file_paths!r}"
                                )
                                for path_i in (
                                    file_paths
                                    if isinstance(file_paths, list)
                                    else [file_paths]
                                ):
                                    full_path = run_dir.joinpath(path_i)
                                    if not full_path.exists():
                                        self._app.logger.debug(
                                            f"expected file to save {path_i!r} does not "
                                            f"exist, so setting run to an error state "
                                            f"(if not aborted)."
                                        )
                                        if not is_aborted and success is True:
                                            # this is unlikely to happen, but could happen
                                            # if the input file is deleted in between
                                            # the input file generator completing and this
                                            # code being run
                                            success = False
                                            exit_code = 1  # TODO more custom exit codes?
                                    else:
                                        self._set_file(
                                            param_id=param_id,
                                            store_contents=True,
                                            is_input=False,
                                            path=full_path,
                                            clean_up=(save_file_j in run.action.clean_up),
                                        )

                            for OFP_i in run.action.output_file_parsers:
                                self._app.logger.info(
                                    f"saving files from OFP: {OFP_i!r}."
                                )
                                for save_file_j in OFP_i._save_files:
                                    self._app.logger.debug(
                                        f"Saving EAR output file: {save_file_j.label!r} "
                                        f"for EAR ID {run.id_!r}."
                                    )
                                    try:
                                        param_id = run.data_idx[
                                            f"output_files.{save_file_j.label}"
                                        ]
                                    except KeyError:
                                        # We might be saving a file that is not a defined
                                        # "output file"; this will avoid saving a
                                        # reference in the parameter data:
                                        param_id = None

                                    file_paths = save_file_j.value(directory=run_dir)
                                    self._app.logger.debug(
                                        f"Saving EAR output file paths: {file_paths!r}"
                                    )

                                    for path_i in (
                                        file_paths
                                        if isinstance(file_paths, list)
                                        else [file_paths]
                                    ):
                                        full_path = run_dir.joinpath(path_i)
                                        if not full_path.exists():
                                            self._app.logger.debug(
                                                f"expected output file parser `save_files` file "
                                                f"{path_i!r} does not exist, so setting run "
                                                f"to an error state (if not aborted)."
                                            )
                                            if not is_aborted and success is True:
                                                success = False
                                                exit_code = (
                                                    1  # TODO more custom exit codes?
                                                )
                                        else:
                                            self._set_file(
                                                param_id=param_id,
                                                store_contents=True,  # TODO: make optional according to OFP
                                                is_input=False,
                                                path=full_path,
                                                clean_up=(save_file_j in OFP_i.clean_up),
                                            )

                    else:
                        self._app.logger.info(
                            f"run was skipped: reason: {run.skip_reason!r}."
                        )

                    if (
                        run.resources.skip_downstream_on_failure
                        and not success
                        and run.skip_reason is not SkipReason.LOOP_TERMINATION
                    ):
                        # run failed
                        self._app.logger.info(
                            "run was not succcess and skip reason was not "
                            "LOOP_TERMINATION."
                        )
                        # loop termination skips are already propagated
                        for EAR_dep_ID in run.get_dependent_EARs(as_objects=False):
                            # TODO: `get_dependent_EARs` seems to be stuck in a
                            # recursion for some workflows
                            # TODO: this needs to be recursive?
                            self._app.logger.info(
                                f"Setting EAR ID {EAR_dep_ID!r} to skip because it "
                                f"depends on EAR ID {run.id_!r}, which exited with a "
                                f"non-zero exit code: {exit_code!r}."
                            )
                            self._store.set_EAR_skip(
                                {EAR_dep_ID: SkipReason.UPSTREAM_FAILURE.value}
                            )
                    else:
                        self._app.logger.info(
                            "`skip_downstream_on_failure` is False, run was "
                            "succcess, or skip reason was LOOP_TERMINATION."
                        )

                    run_ids.append(run.id_)
                    run_dirs.append(run_dir)
                    exit_codes.append(exit_code)
                    successes.append(success)

            self._store.set_multi_run_ends(run_ids, run_dirs, exit_codes, successes)

    def set_EAR_skip(self, skip_reasons: dict[int, SkipReason]) -> None:
        """
        Record that an EAR is to be skipped due to an upstream failure or loop
        termination condition being met.
        """
        with self._store.cached_load(), self.batch_update():
            self._store.set_EAR_skip({k: v.value for k, v in skip_reasons.items()})

    def get_EAR_skipped(self, EAR_ID: int) -> int:
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

    @TimeIt.decorator
    def set_parameter_values(self, values: dict[int, Any], commit: bool = False) -> None:
        with self._store.cached_load(), self.batch_update(), self._store.cache_ctx():
            self._store.set_parameter_values(values)

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
    ) -> Sequence[tuple[int, LoopIndex[str, int]]]: ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[False] = False, ret_data_idx: Literal[True]
    ) -> Sequence[tuple[int, LoopIndex[str, int], tuple[Mapping[str, int], ...]]]: ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[True], ret_data_idx: Literal[False] = False
    ) -> Sequence[tuple[int, LoopIndex[str, int], tuple[int, ...]]]: ...

    @overload
    def get_iteration_task_pathway(
        self, *, ret_iter_IDs: Literal[True], ret_data_idx: Literal[True]
    ) -> Sequence[
        tuple[int, LoopIndex[str, int], tuple[int, ...], tuple[Mapping[str, int], ...]]
    ]: ...

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
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        print_stdout: bool = False,
        add_to_known: bool = True,
        tasks: Sequence[int] | None = None,
    ) -> tuple[Sequence[SubmissionFailure], Mapping[int, Sequence[int]]]:
        """Submit outstanding EARs for execution."""

        # generate a new submission if there are no pending submissions:
        if not (pending := [sub for sub in self.submissions if sub.needs_submit]):
            if status:
                status.update("Adding new submission...")
            if not (
                new_sub := self._add_submission(
                    tasks=tasks,
                    JS_parallelism=JS_parallelism,
                    status=status,
                )
            ):
                if status:
                    status.stop()
                raise ValueError("No pending element action runs to submit!")
            pending = [new_sub]

        self.execution_path.mkdir(exist_ok=True, parents=True)
        self.task_artifacts_path.mkdir(exist_ok=True, parents=True)

        # the submission must be persistent at submit-time, because it will be read by a
        # new instance of the app:
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
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        print_stdout: bool = False,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: Literal[True],
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ) -> Mapping[int, Sequence[int]]: ...

    @overload
    def submit(
        self,
        *,
        ignore_errors: bool = False,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        print_stdout: bool = False,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: Literal[False] = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ) -> None: ...

    def submit(
        self,
        *,
        ignore_errors: bool = False,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
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
            If True, allow multiple jobscripts to execute simultaneously. If
            'scheduled'/'direct', only allow simultaneous execution of scheduled/direct
            jobscripts. Raises if set to True, 'scheduled', or 'direct', but the store
            type does not support the `jobscript_parallelism` feature. If not set,
            jobscript parallelism will be used if the store type supports it, for
            scheduled jobscripts only.
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
            with (
                self.batch_update(),
                self._store.parameters_metadata_cache(),
                self._store.cache_ctx(),
            ):
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
            self.cancel(status=status)

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
        for js_idx, block_states in sub.get_active_jobscripts().items():
            js = sub.jobscripts[js_idx]
            for block_idx, block in enumerate(js.blocks):
                states = block_states[block_idx]
                for js_elem_idx, state in states.items():
                    if state is JobscriptElementState.running:
                        for task_iID, elem_idx in zip(
                            block.task_insert_IDs, block.task_elements[js_elem_idx]
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
            for iter_i in elem.iterations:
                for elem_acts in iter_i.actions.values():
                    for run in elem_acts.runs:
                        if run.status is EARStatus.running:
                            out.append(run)
                            # for a given element and submission, only one run
                            # may be running at a time:
                            break
        return out

    def _abort_run(self, run: ElementActionRun):
        # connect to the ZeroMQ server on the worker node:
        self._app.logger.info(f"abort run: {run!r}")
        self._app.Executor.send_abort(
            hostname=run.run_hostname, port_number=run.port_number
        )

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
        self._abort_run(run)

    @TimeIt.decorator
    def cancel(self, status: bool = True):
        """Cancel any running jobscripts."""
        status_msg = f"Cancelling jobscripts of workflow {self.path!r}"
        # Type hint for mypy
        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            rich.console.Console().status(status_msg) if status else nullcontext()
        )
        with status_context as status_, self._store.cached_load():
            for sub in self.submissions:
                sub.cancel()

    def add_submission(
        self,
        tasks: list[int] | None = None,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        force_array: bool = False,
        status: bool = True,
    ) -> Submission | None:
        """Add a new submission.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.
        """
        # JS_parallelism=None means guess
        # Type hint for mypy
        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            rich.console.Console().status("") if status else nullcontext()
        )
        with status_context as status_, self._store.cached_load(), self.batch_update():
            return self._add_submission(tasks, JS_parallelism, force_array, status_)

    @TimeIt.decorator
    @load_workflow_config
    def _add_submission(
        self,
        tasks: Sequence[int] | None = None,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        force_array: bool = False,
        status: Status | None = None,
    ) -> Submission | None:
        """Add a new submission.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.
        """
        new_idx = self.num_submissions
        _ = self.submissions  # TODO: just to ensure `submissions` is loaded
        if status:
            status.update("Adding new submission: resolving jobscripts...")

        cache = ObjectCache.build(self, elements=True, iterations=True, runs=True)

        sub_obj: Submission = self._app.Submission(
            index=new_idx,
            workflow=self,
            jobscripts=self.resolve_jobscripts(cache, tasks, force_array),
            JS_parallelism=JS_parallelism,
        )
        if status:
            status.update("Adding new submission: setting environments...")
        sub_obj._set_environments()
        all_EAR_ID = sub_obj.all_EAR_IDs
        if not all_EAR_ID:
            print(
                "There are no pending element action runs, so a new submission was not "
                "added."
            )
            return None

        if status:
            status.update("Adding new submission: making artifact directories...")

        # TODO: a submission should only be "submitted" once shouldn't it?
        # no; there could be an IO error (e.g. internet connectivity), so might
        # need to be able to reattempt submission of outstanding jobscripts.
        self.submissions_path.mkdir(exist_ok=True, parents=True)
        sub_obj.path.mkdir(exist_ok=True)
        sub_obj.tmp_path.mkdir(exist_ok=True)
        sub_obj.app_std_path.mkdir(exist_ok=True)
        sub_obj.js_path.mkdir(exist_ok=True)  # for jobscripts
        sub_obj.js_std_path.mkdir(exist_ok=True)  # for stdout/err stream files
        sub_obj.js_funcs_path.mkdir(exist_ok=True)
        sub_obj.js_run_ids_path.mkdir(exist_ok=True)
        sub_obj.scripts_path.mkdir(exist_ok=True)
        sub_obj.commands_path.mkdir(exist_ok=True)

        if sub_obj.needs_app_log_dir:
            sub_obj.app_log_path.mkdir(exist_ok=True)

        if sub_obj.needs_win_pids_dir:
            sub_obj.js_win_pids_path.mkdir(exist_ok=True)

        if sub_obj.needs_script_indices_dir:
            sub_obj.js_script_indices_path.mkdir(exist_ok=True)

        if status:
            status.update("Adding new submission: writing scripts and command files...")

        # write scripts and command files where possible to the submission directory:
        cmd_file_IDs, run_indices, run_inp_files = sub_obj._write_scripts(cache, status)

        sub_obj._write_execute_dirs(run_indices, run_inp_files, cache, status)

        if status:
            status.update("Adding new submission: updating the store...")

        with self._store.cached_load(), self.batch_update():
            for id_ in all_EAR_ID:
                self._store.set_run_submission_data(
                    EAR_ID=id_,
                    cmds_ID=cmd_file_IDs[id_],
                    sub_idx=new_idx,
                )

        sub_obj._ensure_JS_parallelism_set()
        sub_obj_js, _ = sub_obj.to_json_like()
        assert self._submissions is not None
        self._submissions.append(sub_obj)
        self._pending["submissions"].append(new_idx)
        with self._store.cached_load(), self.batch_update():
            self._store.add_submission(new_idx, cast("Mapping[str, JSONed]", sub_obj_js))

        return self.submissions[new_idx]

    @TimeIt.decorator
    def resolve_jobscripts(
        self,
        cache: ObjectCache,
        tasks: Sequence[int] | None = None,
        force_array: bool = False,
    ) -> list[Jobscript]:
        """
        Resolve this workflow to a set of jobscripts to run for a new submission.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.

        """
        with self._app.config.cached_config():
            with self.cached_merged_parameters():
                js, element_deps = self._resolve_singular_jobscripts(
                    cache, tasks, force_array
                )

            js_deps = resolve_jobscript_dependencies(js, element_deps)

            for js_idx, jsca in js.items():
                if js_idx in js_deps:
                    jsca["dependencies"] = js_deps[js_idx]  # type: ignore

            js = merge_jobscripts_across_tasks(js)

            # for direct or (non-array scheduled), combine into jobscripts of multiple
            # blocks for dependent jobscripts that have the same resource hashes
            js_ = resolve_jobscript_blocks(js)

            return [self._app.Jobscript(**i, index=idx) for idx, i in enumerate(js_)]

    def __EAR_obj_map(
        self,
        js_desc: JobScriptDescriptor,
        jsca: JobScriptCreationArguments,
        task: WorkflowTask,
        task_actions: Sequence[tuple[int, int, int]],
        EAR_map: NDArray,
        cache: ObjectCache,
    ) -> Mapping[int, ElementActionRun]:
        assert cache.runs is not None
        all_EAR_IDs: list[int] = []
        for js_elem_idx, (elem_idx, act_indices) in enumerate(
            js_desc["elements"].items()
        ):
            for act_idx in act_indices:
                EAR_ID_i: int = EAR_map[act_idx, elem_idx].item()
                all_EAR_IDs.append(EAR_ID_i)
                js_act_idx = task_actions.index((task.insert_ID, act_idx, 0))
                jsca["EAR_ID"][js_act_idx][js_elem_idx] = EAR_ID_i
        return dict(zip(all_EAR_IDs, (cache.runs[i] for i in all_EAR_IDs)))

    @TimeIt.decorator
    def _resolve_singular_jobscripts(
        self,
        cache: ObjectCache,
        tasks: Sequence[int] | None = None,
        force_array: bool = False,
    ) -> tuple[
        Mapping[int, JobScriptCreationArguments],
        Mapping[int, Mapping[int, Sequence[int]]],
    ]:
        """
        We arrange EARs into `EARs` and `elements` so we can quickly look up membership
        by EAR idx in the `EARs` dict.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.

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
            # note: this cache is unrelated to the `cache` argument
            self.get_all_parameter_sources()

        submission_jobscripts: dict[int, JobScriptCreationArguments] = {}
        all_element_deps: dict[int, dict[int, list[int]]] = {}

        for task_iID, loop_idx_i in self.get_iteration_task_pathway():
            task = self.tasks.get(insert_ID=task_iID)
            if task.index not in task_set:
                continue
            res, res_hash, res_map, EAR_map = generate_EAR_resource_map(
                task, loop_idx_i, cache
            )
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

                is_array = force_array or is_jobscript_array(
                    res[js_dat["resources"]],
                    EAR_ID_arr.shape[1],
                    self._store,
                )
                js_i: JobScriptCreationArguments = {
                    "task_insert_IDs": [task.insert_ID],
                    "task_loop_idx": [loop_idx_i],
                    "task_actions": task_actions,  # map jobscript actions to task actions
                    "task_elements": task_elements,  # map jobscript elements to task elements
                    "EAR_ID": EAR_ID_arr,
                    "resources": res[js_dat["resources"]],
                    "resource_hash": res_hash[js_dat["resources"]],
                    "dependencies": {},
                    "is_array": is_array,
                }

                all_EAR_objs = self.__EAR_obj_map(
                    js_dat, js_i, task, task_actions, EAR_map, cache
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

    @load_workflow_config
    def execute_run(
        self,
        submission_idx: int,
        block_act_key: BlockActionKey,
        run_ID: int,
    ) -> None:
        """Execute commands of a run via a subprocess."""

        # CD to submission tmp dir to ensure std streams and exceptions have somewhere
        # sensible to go:
        os.chdir(Submission.get_tmp_path(self.submissions_path, submission_idx))

        sub_str_path = Submission.get_app_std_path(self.submissions_path, submission_idx)
        run_std_path = sub_str_path / f"{str(run_ID)}.txt"  # TODO: refactor
        has_commands = False

        # redirect (as much as possible) app-generated stdout/err to a dedicated file:
        with redirect_std_to_file(run_std_path):
            with self._store.cached_load():
                js_idx = cast("int", block_act_key[0])
                run = self.get_EARs_from_IDs([run_ID])[0]
                run_dir = None
                if run.action.requires_dir:
                    run_dir = run.get_directory()
                    assert run_dir
                    self._app.submission_logger.debug(
                        f"changing directory to run execution directory: {run_dir}."
                    )
                    os.chdir(run_dir)
                self._app.submission_logger.debug(f"{run.skip=}; {run.skip_reason=}")

                # check if we should skip:
                if not run.skip:

                    try:
                        with run.raise_on_failure_threshold() as unset_params:
                            if run.action.script:
                                run.write_script_input_files(block_act_key)

                            # write the command file that will be executed:
                            cmd_file_path = self.ensure_commands_file(
                                submission_idx, js_idx, run
                            )

                    except UnsetParameterDataErrorBase:
                        # not all required parameter data is set, so fail this run:
                        self._app.submission_logger.debug(
                            f"unset parameter threshold satisfied (or any unset "
                            f"parameters found when trying to write commands file), so "
                            f"not attempting run. unset_params={unset_params!r}."
                        )
                        self.set_EAR_start(run_ID, run_dir, port_number=None)
                        self._check_loop_termination(run)  # not sure if this is required
                        self.set_EAR_end(
                            block_act_key=block_act_key,
                            run=run,
                            exit_code=1,
                        )
                        return

                    # sufficient parameter data is set so far, but need to pass `unset_params`
                    # on as an environment variable so it can be appended to and failure
                    # thresholds can be rechecked if necessary (i.e. in a Python script
                    # where we also load input parameters "directly")
                    if unset_params:
                        self._app.submission_logger.debug(
                            f"some unset parameters found, but no unset-thresholds met: "
                            f"unset_params={unset_params!r}."
                        )

                    # TODO: pass on unset_params to script as environment variable

                    if has_commands := bool(cmd_file_path):

                        assert isinstance(cmd_file_path, Path)
                        if not cmd_file_path.is_file():
                            raise RuntimeError(
                                f"Command file {cmd_file_path!r} does not exist."
                            )
                        # prepare subprocess command:
                        jobscript = self.submissions[submission_idx].jobscripts[js_idx]
                        cmd = jobscript.shell.get_command_file_launch_command(
                            str(cmd_file_path)
                        )
                        loop_idx_str = ";".join(
                            f"{k}={v}" for k, v in run.element_iteration.loop_idx.items()
                        )
                        app_caps = self._app.package_name.upper()

                        # TODO: make these optionally set (more difficult to set in combine_script,
                        # so have the option to turn off) [default ON]
                        add_env = {
                            f"{app_caps}_RUN_ID": str(run_ID),
                            f"{app_caps}_RUN_IDX": str(run.index),
                            f"{app_caps}_ELEMENT_IDX": str(run.element.index),
                            f"{app_caps}_ELEMENT_ID": str(run.element.id_),
                            f"{app_caps}_ELEMENT_ITER_IDX": str(
                                run.element_iteration.index
                            ),
                            f"{app_caps}_ELEMENT_ITER_ID": str(run.element_iteration.id_),
                            f"{app_caps}_ELEMENT_ITER_LOOP_IDX": loop_idx_str,
                        }

                        if run.action.script:
                            if run.is_snippet_script:
                                script_artifact_name = run.get_script_artifact_name()
                                script_dir = Path(
                                    os.environ[f"{app_caps}_SUB_SCRIPTS_DIR"]
                                )
                                script_name = script_artifact_name
                            else:
                                # not a snippet script; expect the script in the run execute
                                # directory (i.e. created by a previous action)
                                script_dir = Path.cwd()
                                script_name = run.action.script
                            script_name_no_ext = Path(script_name).stem
                            add_env.update(
                                {
                                    f"{app_caps}_RUN_SCRIPT_NAME": script_name,
                                    f"{app_caps}_RUN_SCRIPT_NAME_NO_EXT": script_name_no_ext,
                                    f"{app_caps}_RUN_SCRIPT_DIR": str(script_dir),
                                    f"{app_caps}_RUN_SCRIPT_PATH": str(
                                        script_dir / script_name
                                    ),
                                }
                            )

                        env = {**dict(os.environ), **add_env}

                        self._app.submission_logger.debug(
                            f"Executing run commands via subprocess with command {cmd!r}, and "
                            f"environment variables as below."
                        )
                        for k, v in env.items():
                            if k.startswith(app_caps):
                                self._app.submission_logger.debug(f"{k} = {v!r}")
                        exe = self._app.Executor(cmd, env, self._app.package_name)
                        port = (
                            exe.start_zmq_server()
                        )  # start the server so we know the port

                        try:
                            self.set_EAR_start(run_ID, run_dir, port)
                        except:
                            self._app.submission_logger.error(f"Failed to set run start.")
                            exe.stop_zmq_server()
                            raise

        # this subprocess may include commands that redirect to the std_stream file (e.g.
        # calling the app to save a parameter from a shell command output):
        if not run.skip and has_commands:
            ret_code = exe.run()  # this also shuts down the server

        # redirect (as much as possible) app-generated stdout/err to a dedicated file:
        with redirect_std_to_file(run_std_path):
            if run.skip:
                ret_code = SKIPPED_EXIT_CODE
            elif not has_commands:
                ret_code = NO_COMMANDS_EXIT_CODE
            else:
                self._check_loop_termination(run)

            # set run end:
            self.set_EAR_end(
                block_act_key=block_act_key,
                run=run,
                exit_code=ret_code,
            )

    def _check_loop_termination(self, run: ElementActionRun) -> set[int]:
        """Check if we need to terminate a loop if this is the last action of the loop
        iteration for this element, and set downstream iteration runs to skip."""

        elem_iter = run.element_iteration
        task = elem_iter.task
        check_loops = []
        to_skip = set()
        for loop_name in elem_iter.loop_idx:
            self._app.logger.info(f"checking loop termination of loop {loop_name!r}.")
            loop = self.loops.get(loop_name)
            if (
                loop.template.termination
                and task.insert_ID == loop.template.termination_task_insert_ID
                and run.element_action.action_idx == max(elem_iter.actions)
            ):
                check_loops.append(loop_name)
                # TODO: test with condition actions
                if loop.test_termination(elem_iter):
                    self._app.logger.info(
                        f"loop {loop_name!r} termination condition met for run "
                        f"ID {run.id_!r}."
                    )
                    to_skip.update(loop.skip_downstream_iterations(elem_iter))
        return to_skip

    @load_workflow_config
    def execute_combined_runs(self, submission_idx: int, jobscript_idx: int) -> None:
        """Execute a combined script (multiple runs) via a subprocess."""

        # CD to submission tmp dir to ensure std streams and exceptions have somewhere
        # sensible to go:
        os.chdir(Submission.get_tmp_path(self.submissions_path, submission_idx))

        sub = self.submissions[submission_idx]
        js = sub.jobscripts[jobscript_idx]

        app_caps = self._app.package_name.upper()
        script_dir = Path(os.environ[f"{app_caps}_SUB_SCRIPTS_DIR"])
        script_name = f"js_{jobscript_idx}.py"  # TODO: refactor script name
        script_path = script_dir / script_name

        add_env = {
            f"{app_caps}_RUN_SCRIPT_NAME": script_name,
            f"{app_caps}_RUN_SCRIPT_NAME_NO_EXT": script_path.stem,
            f"{app_caps}_RUN_SCRIPT_DIR": str(script_dir),
            f"{app_caps}_RUN_SCRIPT_PATH": str(script_path),
            f"{app_caps}_SCRIPT_INDICES_FILE": str(js.combined_script_indices_file_path),
        }
        env = {**dict(os.environ), **add_env}

        # note: unlike in `Workflow.execute_run`, here we can be reasonably sure the
        # commands file already exists, because we call `Action.try_write_commands` with
        # `raise_on_unset=True` in `Workflow._add_submission` during submission.

        # TODO: refactor cmd file name:
        cmd_file_path = sub.commands_path / f"js_{jobscript_idx}{js.shell.JS_EXT}"
        cmd = js.shell.get_command_file_launch_command(str(cmd_file_path))

        self._app.submission_logger.debug(
            f"Executing combined runs via subprocess with command {cmd!r}, and "
            f"environment variables as below."
        )
        for k, v in env.items():
            if k.startswith(app_caps):
                self._app.submission_logger.debug(f"{k} = {v}")

        exe = self._app.Executor(cmd, env, self._app.package_name)
        exe.start_zmq_server()  # start the server
        exe.run()  # this also shuts down the server

    def ensure_commands_file(
        self,
        submission_idx: int,
        js_idx: int,
        run: ElementActionRun,
    ) -> Path | bool:
        """Ensure a commands file exists for the specified run."""
        self._app.persistence_logger.debug("Workflow.ensure_commands_file")

        if run.commands_file_ID is None:
            # no commands to write
            return False

        with self._store.cached_load():
            sub = self.submissions[submission_idx]
            jobscript = sub.jobscripts[js_idx]

            # check if a commands file already exists, first checking using the run ID:
            cmd_file_name = f"{run.id_}{jobscript.shell.JS_EXT}"  # TODO: refactor
            cmd_file_path = jobscript.submission.commands_path / cmd_file_name

            if not cmd_file_path.is_file():
                # then check for a file from the "root" run ID (the run ID of a run that
                # shares the same commands file):

                cmd_file_name = (
                    f"{run.commands_file_ID}{jobscript.shell.JS_EXT}"  # TODO: refactor
                )
                cmd_file_path = jobscript.submission.commands_path / cmd_file_name

            if not cmd_file_path.is_file():
                # no file available, so write (using the run ID):
                try:
                    cmd_file_path = run.try_write_commands(
                        jobscript=jobscript,
                        environments=sub.environments,
                        raise_on_unset=True,
                    )
                except OutputFileParserNoOutputError:
                    # no commands to write, might be used just for saving files
                    return False

        return cmd_file_path

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
                        f"Input source {input_source.to_string()!r} refers to a missing "
                        f"or inaccessible task: {input_source.task_ref!r}."
                    )

    @TimeIt.decorator
    def get_all_submission_run_IDs(self) -> Iterable[int]:
        """
        Get the run IDs of all submissions.
        """
        self._app.persistence_logger.debug("Workflow.get_all_submission_run_IDs")
        for sub in self.submissions:
            yield from sub.all_EAR_IDs

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
        Reorganise the stored data chunks for parameters to be more efficient.
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

    @TimeIt.decorator
    def get_run_directories(
        self,
        run_ids: list[int] | None = None,
        dir_indices_arr: np.ndarray | None = None,
    ) -> list[Path | None]:
        """"""

        @TimeIt.decorator
        def _get_depth_dirs(
            item_idx: int,
            max_per_dir: int,
            max_depth: int,
            depth_idx_cache: dict[tuple[int, int], NDArray],
            prefix: str,
        ) -> list[str]:
            dirs = []
            max_avail_items = max_per_dir**max_depth
            for depth_i in range(1, max_depth):
                tot_items_per_level = int(max_avail_items / max_per_dir**depth_i)
                key = (max_avail_items, tot_items_per_level)
                if (depth_idx := depth_idx_cache.get(key)) is None:
                    depth_idx = np.repeat(
                        np.arange(max_avail_items / tot_items_per_level, dtype=int),
                        tot_items_per_level,
                    )
                    depth_idx_cache[key] = depth_idx
                idx_i = cast("NDArray", depth_idx)[item_idx]
                start_idx = idx_i * tot_items_per_level
                end_idx = start_idx + tot_items_per_level - 1
                dirs.append(f"{prefix}_{start_idx}-{end_idx}")
            return dirs

        if dir_indices_arr is None:  # TODO: document behaviour!
            dir_indices_arr = self._store.get_dirs_array()
            if run_ids is not None:
                dir_indices_arr = dir_indices_arr[run_ids]

        # TODO: make these configurable so easier to test!
        MAX_ELEMS_PER_DIR = 1000  # TODO: configurable (add `workflow_defaults` to Config)
        MAX_ITERS_PER_DIR = 1000

        exec_path = self.execution_path

        # a fill value means no sub directory should be created
        T_FILL, E_FILL, I_FILL, A_FILL, R_FILL, _, _ = RUN_DIR_ARR_FILL

        depth_idx_cache: dict[tuple[int, int], NDArray] = (
            {}
        )  # keys are (max_avail, tot_elems_per_dir_level)

        # format run directories:
        dirs = []
        for dir_data in dir_indices_arr:

            # TODO: retrieve task,element,iteration,action,run dir formats from
            # (t_iID, act_idx) combo (cached)?

            t_iID, e_idx, i_idx, _, r_idx, e_depth, i_depth = dir_data
            path_args = []

            if t_iID != T_FILL:
                path_args.append(f"t_{t_iID}")

            if e_idx != E_FILL:
                if e_depth > 1:
                    path_args.extend(
                        _get_depth_dirs(
                            item_idx=e_idx,
                            max_per_dir=MAX_ELEMS_PER_DIR,
                            max_depth=e_depth,
                            depth_idx_cache=depth_idx_cache,
                            prefix="e",
                        )
                    )
                path_args.append(f"e_{e_idx}")

            if i_idx != I_FILL:
                if i_depth > 1:
                    path_args.extend(
                        _get_depth_dirs(
                            item_idx=i_idx,
                            max_per_dir=MAX_ITERS_PER_DIR,
                            max_depth=i_depth,
                            depth_idx_cache=depth_idx_cache,
                            prefix="i",
                        )
                    )
                path_args.append(f"i_{i_idx}")

            if r_idx != R_FILL:
                path_args.append(f"r_{r_idx}")

            if path_args:
                run_dir = exec_path.joinpath(*path_args)
            elif e_depth == 1:
                run_dir = exec_path
            else:
                run_dir = None

            dirs.append(run_dir)

        return dirs

    @TimeIt.decorator
    def get_scheduler_job_IDs(self) -> tuple[str, ...]:
        """Return jobscript scheduler job IDs from all submissions of this workflow."""
        return tuple(
            IDs_j for sub_i in self.submissions for IDs_j in sub_i.get_scheduler_job_IDs()
        )

    @TimeIt.decorator
    def get_process_IDs(self) -> tuple[int, ...]:
        """Return jobscript process IDs from all submissions of this workflow."""
        return tuple(
            IDs_j for sub_i in self.submissions for IDs_j in sub_i.get_process_IDs()
        )

    @TimeIt.decorator
    def list_jobscripts(
        self,
        sub_idx: int = 0,
        max_js: int | None = None,
        jobscripts: list[int] | None = None,
        width: int | None = None,
    ) -> None:
        """Print a table listing jobscripts and associated information from the specified
        submission.

        Parameters
        ----------
        sub_idx
            The submission index whose jobscripts are to be displayed.
        max_js
            Maximum jobscript index to display. This cannot be specified with `jobscripts`.
        jobscripts
            A list of jobscripts to display. This cannot be specified with `max_js`.
        width
            Width in characters of the printed table.
        """

        with self._store.cached_load():

            if max_js is not None and jobscripts is not None:
                raise ValueError("Do not specify both `max_js` and `jobscripts`.")

            loop_names = [i.name for i in self.loops][::-1]
            loop_names_panel: rich.panel.Panel | str = ""
            if loop_names:
                loop_names_panel = rich.panel.Panel(
                    "\n".join(f"{idx}: {i}" for idx, i in enumerate(loop_names)),
                    title="[b]Loops[/b]",
                    title_align="left",
                    box=rich.box.SIMPLE,
                )

            table = rich.table.Table(width=width)

            table.add_column("Jobscript", justify="right", style="cyan", no_wrap=True)
            table.add_column("Acts, Elms", justify="right", style="green")
            table.add_column("Deps.", style="orange3")
            table.add_column("Tasks", overflow="fold")
            table.add_column("Loops")

            sub_js = self.submissions[sub_idx].jobscripts
            max_js = max_js if max_js is not None else len(sub_js)
            for js in sub_js:
                if jobscripts is not None and js.index not in jobscripts:
                    continue
                if js.index > max_js:
                    break
                for blk in js.blocks:
                    blk_task_actions = blk.task_actions
                    num_actions = blk_task_actions.shape[0]

                    if blk.index == 0:
                        c1 = f"{js.index} - {blk.index}"
                    else:
                        c1 = f"{blk.index}"
                    c3 = f"{num_actions}, {blk.num_elements}"

                    deps = "; ".join(f"{i[0],i[1]}" for i in blk.dependencies)

                    for blk_t_idx, t_iID in enumerate(blk.task_insert_IDs):

                        # loop indices are the same for all actions within a task, so get the
                        # first `task_action` for this task insert ID:
                        for i in blk_task_actions:
                            if i[0] == t_iID:
                                loop_idx = [
                                    blk.task_loop_idx[i[2]].get(loop_name_i, "-")
                                    for loop_name_i in loop_names
                                ]
                                break

                        c2 = self.tasks.get(insert_ID=t_iID).unique_name

                        if blk_t_idx > 0:
                            c1 = ""
                            c3 = ""
                            deps = ""

                        table.add_row(
                            c1, c3, deps, c2, (" | ".join(f"{i}" for i in loop_idx))
                        )

                table.add_section()

        group = rich.console.Group(
            rich.text.Text(f"Workflow: {self.name}"),
            rich.text.Text(f"Submission: {sub_idx}" + ("\n" if loop_names_panel else "")),
            loop_names_panel,
            table,
        )
        rich_print(group)

    def list_task_jobscripts(
        self,
        sub_idx: int = 0,
        task_names: list[str] | None = None,
        max_js: int | None = None,
        width: int | None = None,
    ):
        """Print a table listing the jobscripts associated with the specified (or all)
        tasks for the specified submission.

        Parameters
        ----------
        sub_idx
            The submission index whose jobscripts are to be displayed.
        task_names
            List of sub-strings to match to task names. Only matching task names will be
            included.
        max_js
            Maximum jobscript index to display.
        width
            Width in characters of the printed table.
        """

        with self._store.cached_load():
            loop_names = [i.name for i in self.loops][::-1]
            loop_names_panel: rich.panel.Panel | str = ""
            if loop_names:
                loop_names_panel = rich.panel.Panel(
                    "\n".join(f"{idx}: {i}" for idx, i in enumerate(loop_names)),
                    title="[b]Loops[/b]",
                    title_align="left",
                    box=rich.box.SIMPLE,
                )

            sub_js = self.submissions[sub_idx].jobscripts
            all_task_names = {i.insert_ID: i.unique_name for i in self.tasks}

            # filter task names by those matching the specified names
            matched = all_task_names
            if task_names:
                matched = {
                    k: v
                    for k, v in all_task_names.items()
                    if any(i in v for i in task_names)
                }

            task_jobscripts = defaultdict(list)
            for js in sub_js:
                if max_js is not None and js.index > max_js:
                    break
                for blk in js.blocks:
                    blk_task_actions = blk.task_actions
                    for i in blk.task_insert_IDs:
                        if i in matched:
                            for j in blk_task_actions:
                                if j[0] == i:
                                    loop_idx = [
                                        blk.task_loop_idx[j[2]].get(loop_name_i, "-")
                                        for loop_name_i in loop_names
                                    ]
                                    break
                            task_jobscripts[i].append((js.index, blk.index, loop_idx))

            table = rich.table.Table(width=width)
            table.add_column("Task")
            table.add_column("Jobscripts", style="cyan", no_wrap=True)
            table.add_column("Loops")
            for insert_ID_i, jobscripts_i in task_jobscripts.items():
                for idx, js_j in enumerate(jobscripts_i):
                    js_idx, blk_idx, loop_idx = js_j
                    table.add_row(
                        matched[insert_ID_i] if idx == 0 else "",
                        f"({js_idx}, {blk_idx})",
                        (" | ".join(f"{i}" for i in loop_idx)),
                    )
                table.add_section()

        group = rich.console.Group(
            rich.text.Text(f"Workflow: {self.name}"),
            rich.text.Text(f"Submission: {sub_idx}" + ("\n" if loop_names_panel else "")),
            loop_names_panel,
            table,
        )
        rich_print(group)

    def get_text_file(self, path: str | Path) -> str:
        """Retrieve the contents of a text file stored within the workflow."""
        return self._store.get_text_file(path)


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parameterise.
    (For example, fitting workflows.)"""

    #: The template inside this blueprint.
    workflow_template: WorkflowTemplate
