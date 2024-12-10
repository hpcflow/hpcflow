from __future__ import annotations
from collections import defaultdict
from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone

import os
from pathlib import Path
import random
import shutil
import string
from threading import Thread
import time
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional, Tuple, Union
from functools import wraps
from uuid import uuid4
from warnings import warn
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.zip import ZipFileSystem
import numpy as np
from fsspec.core import url_to_fs
import rich.console

from hpcflow.sdk import app
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
from hpcflow.sdk.core.actions import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.cache import ObjectCache
from hpcflow.sdk.core.loop_cache import LoopCache
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.persistence import store_cls_from_str, DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES, AnySEAR
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, infer_store
from hpcflow.sdk.submission.jobscript import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
    is_jobscript_array,
    merge_jobscripts_across_tasks,
    resolve_jobscript_blocks,
    resolve_jobscript_dependencies,
)
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.submission.submission import Submission
from hpcflow.sdk.typing import PathLike
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.utils.patches import resolve_path
from .utils import (
    nth_key,
    read_JSON_file,
    read_JSON_string,
    read_YAML_str,
    read_YAML_file,
    redirect_std_to_file,
    replace_items,
)
from hpcflow.sdk.core.errors import (
    InvalidInputSourceTaskReference,
    LoopAlreadyExistsError,
    OutputFileParserNoOutputError,
    RunNotAbortableError,
    SubmissionFailure,
    WorkflowSubmissionFailure,
)


class _DummyPersistentWorkflow:
    """An object to pass to ResourceSpec.make_persistent that pretends to be a
    Workflow object, so we can pretend to make template-level inputs/resources
    persistent before the workflow exists."""

    def __init__(self):
        self._parameters = []
        self._sources = []
        self._data_ref = []

    def _add_parameter_data(self, data, source: Dict) -> int:
        self._parameters.append(data)
        self._sources.append(source)
        self._data_ref.append(len(self._data_ref))
        return self._data_ref[-1]

    def get_parameter_data(self, data_idx):
        return self._parameters[self._data_ref.index(data_idx)]

    def make_persistent(self, workflow: app.Workflow):
        for dat_i, source_i in zip(self._parameters, self._sources):
            workflow._add_parameter_data(dat_i, source_i)


@dataclass
class WorkflowTemplate(JSONLike):
    """Class to represent initial parametrisation of a {app_name} workflow, with limited
    validation logic.

    Parameters
    ----------
    name
        A string name for the workflow. By default this name will be used in combination
        with a date-time stamp when generating a persistent workflow from the template.
    tasks
        A list of Task objects to include in the workflow.
    loops
        A list of Loop objects to include in the workflow.
    resources
        Template-level resources to apply to all tasks as default values. This can be a
        dict that maps action scopes to resources (e.g. `{{"any": {{"num_cores": 2}}}}`)
        or a list of `ResourceSpec` objects, or a `ResourceList` object.
    merge_resources
        If True, merge template-level `resources` into element set resources. If False,
        template-level resources are ignored.
    """

    _app_attr = "app"
    _validation_schema = "workflow_spec_schema.yaml"

    _child_objects = (
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

    name: str
    doc: Optional[Union[List[str], str]] = field(repr=False, default=None)
    tasks: Optional[List[app.Task]] = field(default_factory=lambda: [])
    loops: Optional[List[app.Loop]] = field(default_factory=lambda: [])
    workflow: Optional[app.Workflow] = None
    resources: Optional[Dict[str, Dict]] = None
    config: Optional[Dict] = field(default_factory=lambda: {})
    environments: Optional[Dict[str, Dict[str, Any]]] = None
    env_presets: Optional[Union[str, List[str]]] = None
    source_file: Optional[str] = field(default=None, compare=False)
    store_kwargs: Optional[Dict] = field(default_factory=lambda: {})
    merge_resources: Optional[bool] = True
    merge_envs: Optional[bool] = True

    def __post_init__(self):

        # replace metatasks with tasks
        new_tasks = []
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
                for loop_idx, loop in enumerate(self.loops):
                    self.loops[loop_idx]["tasks"] = [
                        j for i in loop["tasks"] for j in reindex.get(i)
                    ]
                    term_task = loop.get("termination_task")
                    if term_task is not None:
                        self.loops[loop_idx]["termination_task"] = reindex.get(term_task)[
                            0
                        ]

        self.tasks = new_tasks

        self.resources = self.app.ResourceList.normalise(self.resources)
        self._set_parent_refs()

        # merge template-level `resources` into task element set resources (this mutates
        # `tasks`, and should only happen on creation of the workflow template, not on
        # re-initialisation from a persistent workflow):
        if self.merge_resources:
            for task in self.tasks:
                for element_set in task.element_sets:
                    element_set.resources.merge_other(self.resources)
            self.merge_resources = False

        if self.merge_envs:
            self._merge_envs_into_task_resources()

        if self.doc and not isinstance(self.doc, list):
            self.doc = [self.doc]

        if self.config:
            # don't do a full validation (which would require loading the config file),
            # just check all specified keys are configurable:
            bad_keys = set(self.config) - set(self.app.config_options._configurable_keys)
            if bad_keys:
                raise ConfigNonConfigurableError(name=bad_keys)

    def _merge_envs_into_task_resources(self):

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
                app_env_specs_i = None
                if not es.environments and not es.env_preset:
                    # no task level envs/presets specified, so merge template-level:
                    if self.environments:
                        app_env_specs_i = {
                            k: v for k, v in self.environments.items() if k in app_envs
                        }
                        if app_env_specs_i:
                            self.app.logger.info(
                                f"(task {task.name!r}, element set {es.index}): using "
                                f"template-level requested `environment` specifiers: "
                                f"{app_env_specs_i!r}."
                            )
                            es.environments = app_env_specs_i

                    elif self.env_presets:
                        # take only the first applicable preset:
                        app_presets_i = [
                            k for k in self.env_presets if k in schema_presets
                        ]
                        if app_presets_i:
                            app_env_specs_i = schema_presets[app_presets_i[0]]
                            self.app.logger.info(
                                f"(task {task.name!r}, element set {es.index}): using "
                                f"template-level requested {app_presets_i[0]!r} "
                                f"`env_preset`: {app_env_specs_i!r}."
                            )
                            es.env_preset = app_presets_i[0]

                    else:
                        # no env/preset applicable here (and no env/preset at task level),
                        # so apply a default preset if available:
                        app_env_specs_i = (schema_presets or {}).get("", None)
                        if app_env_specs_i:
                            self.app.logger.info(
                                f"(task {task.name!r}, element set {es.index}): setting "
                                f"to default (empty-string named) `env_preset`: "
                                f"{app_env_specs_i}."
                            )
                            es.env_preset = ""

                    if app_env_specs_i:
                        es.resources.merge_other(
                            self.app.ResourceList(
                                [
                                    self.app.ResourceSpec(
                                        scope="any", environments=app_env_specs_i
                                    )
                                ]
                            )
                        )

    @classmethod
    @TimeIt.decorator
    def _from_data(cls, data: Dict) -> app.WorkflowTemplate:
        def _normalise_task_parametrisation(task_lst):
            """
            For each dict in a list of task parametrisations, ensure the `schema` key is
            a list of values, and ensure `element_sets` are defined.

            This mutates `task_lst`.

            """

            # use element_sets if not already:
            for task_idx, task_dat in enumerate(task_lst):
                schema = task_dat.pop("schema")
                schema = schema if isinstance(schema, list) else [schema]
                if "element_sets" in task_dat:
                    # just update the schema to a list:
                    task_lst[task_idx]["schema"] = schema
                else:
                    # add a single element set, and update the schema to a list:
                    out_labels = task_dat.pop("output_labels", [])
                    task_lst[task_idx] = {
                        "schema": schema,
                        "element_sets": [task_dat],
                        "output_labels": out_labels,
                    }

        meta_tasks = data.pop("meta_tasks", {})
        if meta_tasks:
            for i in list(meta_tasks):
                _normalise_task_parametrisation(meta_tasks[i])
            new_task_dat = []
            reindex = {}
            for task_idx, task_dat in enumerate(data["tasks"]):
                meta_task_dat = meta_tasks.get(task_dat["schema"])
                if meta_task_dat:
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

            loops = data.get("loops")
            if loops:
                for loop_idx, loop in enumerate(loops):
                    loops[loop_idx]["tasks"] = [
                        j for i in loop["tasks"] for j in reindex.get(i)
                    ]
                    term_task = loop.get("termination_task")
                    if term_task is not None:
                        loops[loop_idx]["termination_task"] = reindex.get(term_task)[0]

        _normalise_task_parametrisation(data["tasks"])

        # extract out any template components:
        tcs = data.pop("template_components", {})
        params_dat = tcs.pop("parameters", [])
        if params_dat:
            parameters = cls.app.ParametersList.from_json_like(
                params_dat, shared_data=cls.app.template_components
            )
            cls.app.parameters.add_objects(parameters, skip_duplicates=True)

        cmd_files_dat = tcs.pop("command_files", [])
        if cmd_files_dat:
            cmd_files = cls.app.CommandFilesList.from_json_like(
                cmd_files_dat, shared_data=cls.app.template_components
            )
            cls.app.command_files.add_objects(cmd_files, skip_duplicates=True)

        envs_dat = tcs.pop("environments", [])
        if envs_dat:
            envs = cls.app.EnvironmentsList.from_json_like(
                envs_dat, shared_data=cls.app.template_components
            )
            cls.app.envs.add_objects(envs, skip_duplicates=True)

        ts_dat = tcs.pop("task_schemas", [])
        if ts_dat:
            task_schemas = cls.app.TaskSchemasList.from_json_like(
                ts_dat, shared_data=cls.app.template_components
            )
            cls.app.task_schemas.add_objects(task_schemas, skip_duplicates=True)

        mts_dat = tcs.pop("meta_task_schemas", [])
        if mts_dat:
            meta_ts = [
                cls.app.MetaTaskSchema.from_json_like(
                    i, shared_data=cls.app.template_components
                )
                for i in mts_dat
            ]
            cls.app.task_schemas.add_objects(meta_ts, skip_duplicates=True)

        wkt = cls.from_json_like(data, shared_data=cls.app.template_components)
        for idx, task in enumerate(wkt.tasks):
            if isinstance(task.schema, cls.app.MetaTaskSchema):
                wkt.tasks[idx] = cls.app.MetaTask(schema=task.schema, tasks=task.tasks)
        return wkt

    @classmethod
    @TimeIt.decorator
    def from_YAML_string(
        cls,
        string: str,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.WorkflowTemplate:
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
    def _check_name(cls, data: Dict, path: PathLike) -> str:
        """Check the workflow template data has a "name" key. If not, add a "name" key,
        using the file path stem.

        Note: this method mutates `data`.

        """
        if "name" not in data:
            name = Path(path).stem
            cls.app.logger.info(
                f"using file name stem ({name!r}) as the workflow template name."
            )
            data["name"] = name

    @classmethod
    @TimeIt.decorator
    def from_YAML_file(
        cls,
        path: PathLike,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.WorkflowTemplate:
        """Load from a YAML file.

        Parameters
        ----------
        path
            The path to the YAML file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`.

        """
        cls.app.logger.debug("parsing workflow template from a YAML file")
        data = read_YAML_file(path, variables=variables)
        cls._check_name(data, path)
        data["source_file"] = str(path)
        return cls._from_data(data)

    @classmethod
    @TimeIt.decorator
    def from_JSON_string(
        cls,
        string: str,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.WorkflowTemplate:
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
        variables: Optional[Dict[str, str]] = None,
    ) -> app.WorkflowTemplate:
        """Load from a JSON file.

        Parameters
        ----------
        path
            The path to the JSON file containing the workflow template parametrisation.
        variables
            String variables to substitute in the file given by `path`.
        """
        cls.app.logger.debug("parsing workflow template from a JSON file")
        data = read_JSON_file(path, variables=variables)
        cls._check_name(data, path)
        data["source_file"] = str(path)
        return cls._from_data(data)

    @classmethod
    @TimeIt.decorator
    def from_file(
        cls,
        path: PathLike,
        template_format: Optional[str] = None,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.WorkflowTemplate:
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
        path = Path(path)
        fmt = template_format.lower() if template_format else None
        if fmt == "yaml" or path.suffix in (".yaml", ".yml"):
            return cls.from_YAML_file(path, variables=variables)
        elif fmt == "json" or path.suffix in (".json", ".jsonc"):
            return cls.from_JSON_file(path, variables=variables)
        else:
            raise ValueError(
                f"Unknown workflow template file extension {path.suffix!r}. Supported "
                f"template formats are {ALL_TEMPLATE_FORMATS!r}."
            )

    def _add_empty_task(self, task: app.Task, new_index: int, insert_ID: int) -> None:
        """Called by `Workflow._add_empty_task`."""
        new_task_name = self.workflow._get_new_task_unique_name(task, new_index)

        task._insert_ID = insert_ID
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"
        task._element_sets = []  # element sets are added to the Task during add_elements

        task.workflow_template = self
        self.tasks.insert(new_index, task)

    def _add_empty_loop(self, loop: app.Loop) -> None:
        """Called by `Workflow._add_empty_loop`."""

        if not loop.name:
            existing = [i.name for i in self.loops]
            new_idx = len(self.loops)
            name = f"loop_{new_idx}"
            while name in existing:
                new_idx += 1
                name = f"loop_{new_idx}"
            loop._name = name
        elif loop.name in self.workflow.loops.list_attrs():
            raise LoopAlreadyExistsError(
                f"A loop with the name {loop.name!r} already exists in the workflow: "
                f"{getattr(self.workflow.loops, loop.name)!r}."
            )

        loop._workflow_template = self
        self.loops.append(loop)


def resolve_fsspec(path: PathLike, **kwargs) -> Tuple[Any, str, str]:
    """
    Parameters
    ----------
    kwargs
        This can include a `password` key, for connections via SSH.

    """

    path = str(path)
    if path.endswith(".zip"):
        # `url_to_fs` does not seem to work for zip combos e.g. `zip::ssh://`, so we
        # construct a `ZipFileSystem` ourselves and assume it is signified only by the
        # file extension:
        fs, pw = ask_pw_on_auth_exc(
            ZipFileSystem,
            fo=path,
            mode="r",
            target_options=kwargs or {},
            add_pw_to="target_options",
        )
        path = ""

    else:
        (fs, path), pw = ask_pw_on_auth_exc(url_to_fs, str(path), **kwargs)
        path = str(Path(path).as_posix())
        if isinstance(fs, LocalFileSystem):
            path = str(Path(path).resolve())

    return fs, path, pw


def load_workflow_config(func):
    """Decorator to apply workflow-level config items during execution of a Workflow
    method."""

    @wraps(func)
    def wrapped(self, *args, **kwargs):

        updates = self.template.config
        if updates:
            with self.app.config._with_updates(updates):
                return func(self, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    return wrapped


class Workflow:
    _app_attr = "app"
    _default_ts_fmt = r"%Y-%m-%d %H:%M:%S.%f"
    _default_ts_name_fmt = r"%Y-%m-%d_%H%M%S"
    _input_files_dir_name = "input_files"
    _exec_dir_name = "execute"

    def __init__(
        self,
        workflow_ref: Union[str, Path, int],
        store_fmt: Optional[str] = None,
        fs_kwargs: Optional[Dict] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        workflow_ref
            Either the path to a persistent workflow, or an integer that will interpreted
            as the local ID of a workflow submission, as reported by the app `show`
            command.
        kwargs
            For compatibility during pre-stable development phase.
        """

        if isinstance(workflow_ref, int):
            path = self.app._get_workflow_path_from_local_ID(workflow_ref)
        else:
            path = workflow_ref

        self.app.logger.info(f"loading workflow from path: {path}")
        fs_path = str(path)
        fs, path, _ = resolve_fsspec(fs_path or "", **(fs_kwargs or {}))
        store_fmt = store_fmt or infer_store(fs_path, fs)
        store_cls = store_cls_from_str(store_fmt)

        self.path = path

        # assigned on first access:
        self._ts_fmt = None
        self._ts_name_fmt = None
        self._creation_info = None
        self._name = None
        self._template = None
        self._template_components = None
        self._tasks = None
        self._loops = None
        self._submissions = None

        self._store = store_cls(self.app, self, self.path, fs)
        self._in_batch_mode = False  # flag to track when processing batch updates

        self._use_merged_parameters_cache = False
        self._merged_parameters_cache = {}

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    def reload(self):
        """Reload the workflow from disk."""
        return self.__class__(self.url)

    @property
    def name(self):
        """The workflow name may be different from the template name, as it includes the
        creation date-timestamp if generated."""
        if not self._name:
            self._name = self._store.get_name()
        return self._name

    @property
    def url(self):
        """Get an fsspec URL for this workflow."""
        if self._store.fs.protocol == "zip":
            return self._store.fs.of.path
        elif self._store.fs.protocol == "file":
            return self.path
        else:
            raise NotImplementedError("Only (local) zip and local URLs provided for now.")

    @property
    def store_format(self):
        return self._store._name

    @property
    def num_tasks(self) -> int:
        return len(self.tasks)

    @classmethod
    @TimeIt.decorator
    def from_template(
        cls,
        template: WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        status: Optional[Any] = None,
    ) -> app.Workflow:
        """Generate from a `WorkflowTemplate` object.

        Parameters
        ----------
        template
            The WorkflowTemplate object to make persistent.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
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
            with wk._store.cached_load():
                with wk.batch_update(is_workflow_creation=True):
                    with wk._store.cache_ctx():
                        for idx, task in enumerate(template.tasks):
                            if status:
                                status.update(
                                    f"Adding task {idx + 1}/{len(template.tasks)} "
                                    f"({task.name!r})..."
                                )
                            wk._add_task(task)
                        if status:
                            status.update(
                                f"Preparing to add {len(template.loops)} loops..."
                            )
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
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.Workflow:
        """Generate from a YAML file.

        Parameters
        ----------
        YAML_path
            The path to a workflow template in the YAML file format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables
            String variables to substitute in the file given by `YAML_path`.
        """
        template = cls.app.WorkflowTemplate.from_YAML_file(
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
        YAML_str: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
    ) -> app.Workflow:
        """Generate from a YAML string.

        Parameters
        ----------
        YAML_str
            The YAML string containing a workflow template parametrisation.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables
            String variables to substitute in the string `YAML_str`.
        """
        template = cls.app.WorkflowTemplate.from_YAML_string(
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
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
        status: Optional[Any] = None,
    ) -> app.Workflow:
        """Generate from a JSON file.

        Parameters
        ----------
        JSON_path
            The path to a workflow template in the JSON file format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables
            String variables to substitute in the file given by `JSON_path`.
        """
        template = cls.app.WorkflowTemplate.from_JSON_file(
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
        JSON_str: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
        status: Optional[Any] = None,
    ) -> app.Workflow:
        """Generate from a JSON string.

        Parameters
        ----------
        JSON_str
            The JSON string containing a workflow template parametrisation.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables
            String variables to substitute in the string `JSON_str`.
        """
        template = cls.app.WorkflowTemplate.from_JSON_string(
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
        template_format: Optional[str] = None,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
        status: Optional[Any] = None,
    ) -> app.Workflow:
        """Generate from either a YAML or JSON file, depending on the file extension.

        Parameters
        ----------
        template_path
            The path to a template file in YAML or JSON format, and with a ".yml",
            ".yaml", or ".json" extension.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format regardless of the file extension.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables
            String variables to substitute in the file given by `template_path`.
        """
        try:
            template = cls.app.WorkflowTemplate.from_file(
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
        tasks: Optional[List[app.Task]] = None,
        loops: Optional[List[app.Loop]] = None,
        resources: Optional[Dict[str, Dict]] = None,
        config: Optional[Dict] = None,
        path: Optional[PathLike] = None,
        workflow_name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        store_kwargs: Optional[Dict] = None,
    ) -> app.Workflow:
        """Generate from the data associated with a WorkflowTemplate object.

        Parameters
        ----------
        template_name
            Name of the new workflow template, from which the new workflow will be
            generated.
        tasks
            List of Task objects to add to the new workflow.
        loops
            List of Loop objects to add to the new workflow.
        resources
            Mapping of action scopes to resource requirements, to be applied to all
            element sets in the workflow. `resources` specified in an element set take
            precedence of those defined here for the whole workflow.
        config
            Configuration items that should be set whenever the resulting workflow is
            loaded. This includes config items that apply during workflow execution.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        workflow_name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified `template_name` will be used, in
            combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        """
        template = cls.app.WorkflowTemplate(
            template_name,
            tasks=tasks or [],
            loops=loops or [],
            resources=resources,
            config=config,
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
        task: app.Task,
        new_index: Optional[int] = None,
    ) -> app.WorkflowTask:
        if new_index is None:
            new_index = self.num_tasks

        insert_ID = self.num_added_tasks

        # make a copy with persistent schema inputs:
        task_c, _ = task.to_persistent(self, insert_ID)

        # add to the WorkflowTemplate:
        self.template._add_empty_task(task_c, new_index, insert_ID)

        # create and insert a new WorkflowTask:
        self.tasks.add_object(
            self.app.WorkflowTask.new_empty_task(self, task_c, new_index),
            index=new_index,
        )

        # update persistent store:
        task_js, temp_comps_js = task_c.to_json_like()
        self._store.add_template_components(temp_comps_js)
        self._store.add_task(new_index, task_js)

        # update in-memory workflow template components:
        temp_comps = self.app.template_components_from_json_like(temp_comps_js)
        for comp_type, comps in temp_comps.items():
            for comp in comps:
                comp._set_hash()
                if comp not in self.template_components[comp_type]:
                    idx = self.template_components[comp_type].add_object(comp)
                    self._pending["template_components"][comp_type].append(idx)

        self._pending["tasks"].append(new_index)
        return self.tasks[new_index]

    @TimeIt.decorator
    def _add_task(self, task: app.Task, new_index: Optional[int] = None) -> None:
        new_wk_task = self._add_empty_task(task=task, new_index=new_index)
        new_wk_task._add_elements(element_sets=task.element_sets)

    def add_task(self, task: app.Task, new_index: Optional[int] = None) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._add_task(task, new_index=new_index)

    def add_task_after(self, new_task: app.Task, task_ref: app.Task = None) -> None:
        """Add a new task after the specified task.

        Parameters
        ----------
        task_ref
            If not given, the new task will be added at the end of the workflow.

        """
        new_index = task_ref.index + 1 if task_ref else None
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def add_task_before(self, new_task: app.Task, task_ref: app.Task = None) -> None:
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
    def _add_empty_loop(
        self, loop: app.Loop, cache: LoopCache
    ) -> Tuple[app.WorkflowLoop, List[app.ElementIteration]]:
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
        new_loop = self.app.WorkflowLoop.new_empty_loop(
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
            loop_template=loop_js,
            iterable_parameters=wk_loop.iterable_parameters,
            output_parameters=wk_loop.output_parameters,
            parents=wk_loop.parents,
            num_added_iterations=wk_loop.num_added_iterations,
            iter_IDs=iter_IDs,
        )

        self._pending["loops"].append(new_index)

        # update cache loop indices:
        cache.update_loop_indices(new_loop_name=loop_c.name, iter_IDs=iter_IDs)

        return wk_loop

    @TimeIt.decorator
    def _add_loop(
        self, loop: app.Loop, cache: Optional[Dict] = None, status: Optional[Any] = None
    ) -> None:
        if not cache:
            cache = LoopCache.build(workflow=self, loops=[loop])
        new_wk_loop = self._add_empty_loop(loop, cache)
        if loop.num_iterations is not None:
            # fixed number of iterations, so add remaining N > 0 iterations:
            if status:
                status_prev = status.status
            for iter_idx in range(loop.num_iterations - 1):
                if status:
                    status.update(
                        f"{status_prev}: iteration {iter_idx + 2}/{loop.num_iterations}."
                    )
                new_wk_loop.add_iteration(cache=cache, status=status)

    def add_loop(self, loop: app.Loop) -> None:
        """Add a loop to a subset of workflow tasks."""
        with self._store.cached_load():
            with self.batch_update():
                self._add_loop(loop)

    @property
    def creation_info(self):
        if not self._creation_info:
            info = self._store.get_creation_info()
            info["create_time"] = (
                datetime.strptime(info["create_time"], self.ts_fmt)
                .replace(tzinfo=timezone.utc)
                .astimezone()
            )
            self._creation_info = info
        return self._creation_info

    @property
    def id_(self):
        return self.creation_info["id"]

    @property
    def ts_fmt(self):
        if not self._ts_fmt:
            self._ts_fmt = self._store.get_ts_fmt()
        return self._ts_fmt

    @property
    def ts_name_fmt(self):
        if not self._ts_name_fmt:
            self._ts_name_fmt = self._store.get_ts_name_fmt()
        return self._ts_name_fmt

    @property
    def template_components(self) -> Dict:
        if self._template_components is None:
            with self._store.cached_load():
                tc_js = self._store.get_template_components()
            self._template_components = self.app.template_components_from_json_like(tc_js)
        return self._template_components

    @property
    def template(self) -> app.WorkflowTemplate:
        if self._template is None:
            with self._store.cached_load():
                temp_js = self._store.get_template()

                # TODO: insert_ID and id_ are the same thing:
                for task in temp_js["tasks"]:
                    task.pop("id_", None)

                template = self.app.WorkflowTemplate.from_json_like(
                    temp_js, self.template_components
                )
                template.workflow = self
            self._template = template

        return self._template

    @property
    def tasks(self) -> app.WorkflowTaskList:
        if self._tasks is None:
            with self._store.cached_load():
                all_tasks = self._store.get_tasks()
                wk_tasks = []
                for i in all_tasks:
                    wk_task = self.app.WorkflowTask(
                        workflow=self,
                        template=self.template.tasks[i.index],
                        index=i.index,
                        element_IDs=i.element_IDs,
                    )
                    wk_tasks.append(wk_task)
                self._tasks = self.app.WorkflowTaskList(wk_tasks)

        return self._tasks

    @property
    def loops(self) -> app.WorkflowLoopList:
        if self._loops is None:
            with self._store.cached_load():
                wk_loops = []
                for idx, loop_dat in self._store.get_loops().items():
                    num_add_iters = {
                        tuple(i[0]): i[1] for i in loop_dat["num_added_iterations"]
                    }
                    wk_loop = self.app.WorkflowLoop(
                        index=idx,
                        workflow=self,
                        template=self.template.loops[idx],
                        parents=loop_dat["parents"],
                        num_added_iterations=num_add_iters,
                        iterable_parameters=loop_dat["iterable_parameters"],
                        output_parameters=loop_dat["output_parameters"],
                    )
                    wk_loops.append(wk_loop)
                self._loops = self.app.WorkflowLoopList(wk_loops)
        return self._loops

    @property
    def submissions(self) -> List[app.Submission]:
        if self._submissions is None:
            self.app.persistence_logger.debug("loading workflow submissions")
            with self._store.cached_load():
                subs = []
                for idx, sub_dat in self._store.get_submissions().items():
                    sub_js = {"index": idx, **sub_dat}
                    sub = self.app.Submission.from_json_like(sub_js)
                    sub.workflow = self
                    subs.append(sub)
                self._submissions = subs
        return self._submissions

    @property
    def num_added_tasks(self) -> int:
        return self._store._get_num_total_added_tasks()

    @TimeIt.decorator
    def get_store_EARs(self, id_lst: Iterable[int]) -> List[AnySEAR]:
        return self._store.get_EARs(id_lst)

    @TimeIt.decorator
    def get_store_element_iterations(
        self, id_lst: Iterable[int]
    ) -> List[AnySElementIter]:
        return self._store.get_element_iterations(id_lst)

    @TimeIt.decorator
    def get_store_elements(self, id_lst: Iterable[int]) -> List[AnySElement]:
        return self._store.get_elements(id_lst)

    @TimeIt.decorator
    def get_store_tasks(self, id_lst: Iterable[int]) -> List[AnySTask]:
        return self._store.get_tasks_by_IDs(id_lst)

    def get_element_iteration_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> List[int]:
        return [i.elem_iter_ID for i in self.get_store_EARs(id_lst)]

    def get_element_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> List[int]:
        iter_IDs = self.get_element_iteration_IDs_from_EAR_IDs(id_lst)
        return [i.element_ID for i in self.get_store_element_iterations(iter_IDs)]

    def get_task_IDs_from_element_IDs(self, id_lst: Iterable[int]) -> List[int]:
        return [i.task_ID for i in self.get_store_elements(id_lst)]

    def get_EAR_IDs_of_tasks(self, id_lst: int) -> List[int]:
        """Get EAR IDs belonging to multiple tasks"""
        return [i.id_ for i in self.get_EARs_of_tasks(id_lst)]

    def get_EARs_of_tasks(self, id_lst: Iterable[int]) -> List[app.ElementActionRun]:
        """Get EARs belonging to multiple tasks"""
        EARs = []
        for i in id_lst:
            task = self.tasks.get(insert_ID=i)
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    for run in iter_.action_runs:
                        EARs.append(run)
        return EARs

    def get_element_iterations_of_tasks(
        self, id_lst: Iterable[int]
    ) -> List[app.ElementIteration]:
        """Get element iterations belonging to multiple tasks"""
        iters = []
        for i in id_lst:
            task = self.tasks.get(insert_ID=i)
            for elem in task.elements[:]:
                for iter_i in elem.iterations:
                    iters.append(iter_i)
        return iters

    @TimeIt.decorator
    def get_elements_from_IDs(self, id_lst: Iterable[int]) -> List[app.Element]:
        """Return element objects from a list of IDs."""

        store_elems = self._store.get_elements(id_lst)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        element_idx_by_task = defaultdict(set)
        index_paths = []
        for el, tk in zip(store_elems, store_tasks):
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )
            element_idx_by_task[tk.index].add(elem_idx)

        elements_by_task = {}
        for task_idx, elem_idx in element_idx_by_task.items():
            task = self.tasks[task_idx]
            elements_by_task[task_idx] = dict(
                zip(elem_idx, task.elements[list(elem_idx)])
            )

        objs = []
        for idx_dat in index_paths:
            elem = elements_by_task[idx_dat["task_idx"]][idx_dat["elem_idx"]]
            objs.append(elem)

        return objs

    @TimeIt.decorator
    def get_element_iterations_from_IDs(
        self, id_lst: Iterable[int]
    ) -> List[app.ElementIteration]:
        """Return element iteration objects from a list of IDs."""

        store_iters = self._store.get_element_iterations(id_lst)

        elem_IDs = [i.element_ID for i in store_iters]
        store_elems = self._store.get_elements(elem_IDs)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        element_idx_by_task = defaultdict(set)

        index_paths = []
        for it, el, tk in zip(store_iters, store_elems, store_tasks):
            iter_idx = el.iteration_IDs.index(it.id_)
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "iter_idx": iter_idx,
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )
            element_idx_by_task[tk.index].add(elem_idx)

        elements_by_task = {}
        for task_idx, elem_idx in element_idx_by_task.items():
            task = self.tasks[task_idx]
            elements_by_task[task_idx] = dict(
                zip(elem_idx, task.elements[list(elem_idx)])
            )

        objs = []
        for idx_dat in index_paths:
            elem = elements_by_task[idx_dat["task_idx"]][idx_dat["elem_idx"]]
            iter_ = elem.iterations[idx_dat["iter_idx"]]
            objs.append(iter_)

        return objs

    @TimeIt.decorator
    def get_EARs_from_IDs(
        self, id_lst: Iterable[int], as_dict: Optional[bool] = False
    ) -> List[app.ElementActionRun]:
        """Return element action run objects from a list of IDs."""
        self.app.persistence_logger.debug(f"get_EARs_from_IDs: id_lst={id_lst!r}")

        store_EARs = self._store.get_EARs(id_lst)

        elem_iter_IDs = [i.elem_iter_ID for i in store_EARs]
        store_iters = self._store.get_element_iterations(elem_iter_IDs)

        elem_IDs = [i.element_ID for i in store_iters]
        store_elems = self._store.get_elements(elem_IDs)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        # to allow for bulk retrieval of elements/iterations
        element_idx_by_task = defaultdict(set)
        iter_idx_by_task_elem = defaultdict(lambda: defaultdict(set))

        index_paths = []
        for rn, it, el, tk in zip(store_EARs, store_iters, store_elems, store_tasks):
            act_idx = rn.action_idx
            run_idx = it.EAR_IDs[act_idx].index(rn.id_)
            iter_idx = el.iteration_IDs.index(it.id_)
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "run_idx": run_idx,
                    "action_idx": act_idx,
                    "iter_idx": iter_idx,
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )
            element_idx_by_task[tk.index].add(elem_idx)
            iter_idx_by_task_elem[tk.index][elem_idx].add(iter_idx)

        # retrieve elements/iterations:
        iters_by_task_elem = defaultdict(lambda: defaultdict(dict))
        for task_idx, elem_idx in element_idx_by_task.items():
            elements = self.tasks[task_idx].elements[list(elem_idx)]
            for elem_i in elements:
                elem_i_iters_idx = iter_idx_by_task_elem[task_idx][elem_i.index]
                elem_iters = [elem_i.iterations[j] for j in elem_i_iters_idx]
                iters_by_task_elem[task_idx][elem_i.index].update(
                    dict(zip(elem_i_iters_idx, elem_iters))
                )

        objs = {} if as_dict else []
        for idx_dat in index_paths:
            iter_ = iters_by_task_elem[idx_dat["task_idx"]][idx_dat["elem_idx"]][
                idx_dat["iter_idx"]
            ]
            run = iter_.actions[idx_dat["action_idx"]].runs[idx_dat["run_idx"]]
            if as_dict:
                objs[run.id_] = run
            else:
                objs.append(run)

        return objs

    @TimeIt.decorator
    def get_all_elements(self) -> List[app.Element]:
        return self.get_elements_from_IDs(range(self.num_elements))

    @TimeIt.decorator
    def get_all_element_iterations(self) -> List[app.ElementIteration]:
        return self.get_element_iterations_from_IDs(range(self.num_element_iterations))

    @TimeIt.decorator
    def get_all_EARs(self) -> List[app.ElementActionRun]:
        return self.get_EARs_from_IDs(range(self.num_EARs))

    @contextmanager
    def batch_update(self, is_workflow_creation: bool = False) -> Iterator[None]:
        """A context manager that batches up structural changes to the workflow and
        commits them to disk all together when the context manager exits."""

        if self._in_batch_mode:
            yield
        else:
            try:
                self.app.persistence_logger.info(
                    f"entering batch update (is_workflow_creation={is_workflow_creation!r})"
                )
                self._in_batch_mode = True
                yield

            except Exception as err:
                self.app.persistence_logger.error("batch update exception!")
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

                raise err

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

                self.app.persistence_logger.info("exiting batch update")
                self._in_batch_mode = False

    @contextmanager
    def cached_merged_parameters(self):
        if self._use_merged_parameters_cache:
            yield
        else:
            try:
                self.app.logger.debug("entering merged-parameters cache.")
                self._use_merged_parameters_cache = True
                yield
            finally:
                self.app.logger.debug("exiting merged-parameters cache.")
                self._use_merged_parameters_cache = False
                self._merged_parameters_cache = {}  # reset the cache

    @classmethod
    def temporary_rename(cls, path: str, fs) -> List[str]:
        """Rename an existing same-path workflow (directory) so we can restore it if
        workflow creation fails.

        Renaming will occur until the successfully completed. This means multiple new
        paths may be created, where only the final path should be considered the
        successfully renamed workflow. Other paths will be deleted."""

        all_replaced = []

        @cls.app.perm_error_retry()
        def _temp_rename(path: str, fs) -> str:
            temp_ext = "".join(random.choices(string.ascii_letters, k=10))
            replaced = str(Path(f"{path}.{temp_ext}").as_posix())
            cls.app.persistence_logger.debug(
                f"temporary_rename: _temp_rename: {path!r} --> {replaced!r}."
            )
            all_replaced.append(replaced)
            try:
                fs.rename(path, replaced, recursive=True)
            except TypeError:
                # `SFTPFileSystem.rename` has no `recursive` argument:
                fs.rename(path, replaced)
            return replaced

        @cls.app.perm_error_retry()
        def _remove_path(path: str, fs) -> None:
            cls.app.persistence_logger.debug(f"temporary_rename: _remove_path: {path!r}.")
            while fs.exists(path):
                fs.rm(path, recursive=True)
                time.sleep(0.5)

        _temp_rename(path, fs)

        for i in all_replaced[:-1]:
            _remove_path(i, fs)

        return all_replaced[-1]

    @classmethod
    @TimeIt.decorator
    def _write_empty_workflow(
        cls,
        template: app.WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        fs_kwargs: Optional[Dict] = None,
        store_kwargs: Optional[Dict] = None,
    ) -> app.Workflow:
        """
        Parameters
        ----------
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.

        """
        ts = datetime.now()

        # store all times in UTC, since NumPy doesn't support time zone info:
        ts_utc = ts.astimezone(tz=timezone.utc)

        ts_name_fmt = ts_name_fmt or cls._default_ts_name_fmt
        ts_fmt = ts_fmt or cls._default_ts_fmt

        name = name or f"{template.name}_{ts.strftime(ts_name_fmt)}"

        fs_path = f"{path or '.'}/{name}"
        fs_kwargs = fs_kwargs or {}
        fs, path, pw = resolve_fsspec(path or "", **fs_kwargs)
        wk_path = f"{path}/{name}"

        replaced_wk = None
        if fs.exists(wk_path):
            cls.app.logger.debug("workflow path exists")
            if overwrite:
                cls.app.logger.debug("renaming existing workflow path")
                replaced_wk = cls.temporary_rename(wk_path, fs)
            else:
                raise ValueError(
                    f"Path already exists: {wk_path} on file system " f"{fs!r}."
                )

        # make template-level inputs/resources think they are persistent:
        wk_dummy = _DummyPersistentWorkflow()
        param_src = {"type": "workflow_resources"}
        for res_i in copy.deepcopy(template.resources):
            res_i.make_persistent(wk_dummy, param_src)

        template_js, template_sh = template.to_json_like(exclude=["tasks", "loops"])
        template_js["tasks"] = []
        template_js["loops"] = []

        creation_info = {
            "app_info": cls.app.get_info(),
            "create_time": ts_utc.strftime(ts_fmt),
            "id": str(uuid4()),
        }

        store_kwargs = store_kwargs if store_kwargs else template.store_kwargs
        store_cls = store_cls_from_str(store)
        store_cls.write_empty_workflow(
            app=cls.app,
            template_js=template_js,
            template_components_js=template_sh,
            wk_path=wk_path,
            fs=fs,
            name=name,
            replaced_wk=replaced_wk,
            creation_info=creation_info,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            **store_kwargs,
        )

        fs_kwargs = {"password": pw, **fs_kwargs}
        wk = cls(fs_path, store_fmt=store, fs_kwargs=fs_kwargs)

        # actually make template inputs/resources persistent, now the workflow exists:
        wk_dummy.make_persistent(wk)

        if template.source_file:
            wk.artifacts_path.mkdir(exist_ok=False)
            src = Path(template.source_file)
            shutil.copy(src, wk.artifacts_path.joinpath(src.name))

        return wk

    def zip(
        self,
        path=".",
        log=None,
        overwrite=False,
        include_execute=False,
        include_rechunk_backups=False,
    ) -> str:
        """
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

    def unzip(self, path=".", log=None) -> str:
        """
        Parameters
        ----------
        path:
            Path at which to create the new unzipped workflow. If this is an existing
            directory, the new workflow directory will be created within this directory.
            Otherwise, this path will represent the new workflow directory path.
        """
        return self._store.unzip(path=path, log=log)

    def copy(self, path=None) -> str:
        """Copy the workflow to a new path and return the copied workflow path."""
        return self._store.copy(path)

    def delete(self):
        self._store.delete()

    def _delete_no_confirm(self):
        self._store.delete_no_confirm()

    def get_parameters(
        self, id_lst: Iterable[int], **kwargs: Dict
    ) -> List[AnySParameter]:
        return self._store.get_parameters(id_lst, **kwargs)

    @TimeIt.decorator
    def get_parameter_sources(self, id_lst: Iterable[int]) -> List[Dict]:
        return self._store.get_parameter_sources(id_lst)

    @TimeIt.decorator
    def get_parameter_set_statuses(self, id_lst: Iterable[int]) -> List[bool]:
        return self._store.get_parameter_set_statuses(id_lst)

    @TimeIt.decorator
    def get_parameter(self, index: int, **kwargs: Dict) -> AnySParameter:
        return self.get_parameters([index], **kwargs)[0]

    @TimeIt.decorator
    def get_parameter_data(self, index: int, **kwargs: Dict) -> Any:
        param = self.get_parameter(index, **kwargs)
        if param.data is not None:
            return param.data
        else:
            return param.file

    @TimeIt.decorator
    def get_parameter_source(self, index: int) -> Dict:
        return self.get_parameter_sources([index])[0]

    @TimeIt.decorator
    def is_parameter_set(self, index: int) -> bool:
        return self.get_parameter_set_statuses([index])[0]

    @TimeIt.decorator
    def get_all_parameters(self, **kwargs: Dict) -> List[AnySParameter]:
        """Retrieve all store parameters."""
        num_params = self._store._get_num_total_parameters()
        id_lst = list(range(num_params))
        return self._store.get_parameters(id_lst, **kwargs)

    @TimeIt.decorator
    def get_all_parameter_sources(self, **kwargs: Dict) -> List[Dict]:
        """Retrieve all store parameters."""
        num_params = self._store._get_num_total_parameters()
        id_lst = list(range(num_params))
        return self._store.get_parameter_sources(id_lst, **kwargs)

    @TimeIt.decorator
    def get_all_parameter_data(self, **kwargs: Dict) -> Dict[int, Any]:
        """Retrieve all workflow parameter data."""
        params = self.get_all_parameters(**kwargs)
        return {i.id_: (i.data if i.data is not None else i.file) for i in params}

    def check_parameters_exist(
        self, id_lst: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        is_multi = True
        if isinstance(id_lst, int):
            is_multi = False
            id_lst = [id_lst]
        exists = self._store.check_parameters_exist(id_lst)
        if not is_multi:
            exists = exists[0]
        return exists

    def _add_unset_parameter_data(self, source: Dict) -> int:
        # TODO: use this for unset files as well
        return self._store.add_unset_parameter(source)

    def _add_parameter_data(self, data, source: Dict) -> int:
        return self._store.add_set_parameter(data, source)

    def _add_file(
        self,
        store_contents: bool,
        is_input: bool,
        source: Dict,
        path=None,
        contents=None,
        filename: str = None,
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
        param_id: int,
        store_contents: bool,
        is_input: bool,
        path=None,
        contents=None,
        filename: str = None,
        clean_up: bool = False,
    ) -> int:
        self._store.set_file(
            param_id=param_id,
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
            clean_up=clean_up,
        )

    def get_task_unique_names(
        self, map_to_insert_ID: bool = False
    ) -> Union[List[str], Dict[str, int]]:
        """Return the unique names of all workflow tasks.

        Parameters
        ----------
        map_to_insert_ID : bool, optional
            If True, return a dict whose values are task insert IDs, otherwise return a
            list.

        """
        names = self.app.Task.get_task_unique_names(self.template.tasks)
        if map_to_insert_ID:
            insert_IDs = (i.insert_ID for i in self.template.tasks)
            return dict(zip(names, insert_IDs))
        else:
            return names

    def _get_new_task_unique_name(self, new_task: app.Task, new_index: int) -> str:
        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = self.app.Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _get_empty_pending(self) -> Dict:
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
                self.template_components[comp_type]._remove_object(comp_idx)

        for loop_idx in self._pending["loops"][::-1]:
            # iterate in reverse so the index references are correct
            self.loops._remove_object(loop_idx)
            self.template.loops.pop(loop_idx)

        for sub_idx in self._pending["submissions"][::-1]:
            # iterate in reverse so the index references are correct
            self._submissions.pop(sub_idx)

        self._reset_pending()

    @property
    def num_tasks(self):
        return self._store._get_num_total_tasks()

    @property
    def num_submissions(self):
        return self._store._get_num_total_submissions()

    @property
    def num_elements(self):
        return self._store._get_num_total_elements()

    @property
    def num_element_iterations(self):
        return self._store._get_num_total_elem_iters()

    @property
    @TimeIt.decorator
    def num_EARs(self):
        return self._store._get_num_total_EARs()

    @property
    def num_loops(self) -> int:
        return self._store._get_num_total_loops()

    @property
    def artifacts_path(self):
        # TODO: allow customisation of artifacts path at submission and resources level
        return Path(self.path) / "artifacts"

    @property
    def input_files_path(self):
        return self.artifacts_path / self._input_files_dir_name

    @property
    def submissions_path(self):
        return self.artifacts_path / "submissions"

    @property
    def task_artifacts_path(self):
        return self.artifacts_path / "tasks"

    @property
    def execution_path(self):
        return Path(self.path) / self._exec_dir_name

    @TimeIt.decorator
    def get_task_elements(
        self,
        task: app.Task,
        idx_lst: Optional[List[int]] = None,
    ) -> List[app.Element]:
        return [
            self.app.Element(task=task, **{k: v for k, v in i.items() if k != "task_ID"})
            for i in self._store.get_task_elements(task.insert_ID, idx_lst)
        ]

    def set_EAR_start(
        self, run_id: int, run_dir: Union[Path, None], port_number: int
    ) -> None:
        """Set the start time on an EAR."""
        self.app.logger.debug(f"Setting start for EAR ID {run_id!r}")
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_start(run_id, run_dir, port_number)

    def set_multi_run_starts(
        self, run_ids: List[int], run_dirs: List[Union[Path, None]], port_number: int
    ) -> None:
        """Set the start time on multiple runs."""
        self.app.logger.debug(f"Setting start for multiple run IDs {run_ids!r}")
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_multi_run_starts(run_ids, run_dirs, port_number)

    def set_EAR_end(
        self,
        block_act_key: Tuple[int, int, int],
        run: app.ElementActionRun,
        exit_code: int,
    ) -> None:
        """Set the end time and exit code on an EAR.

        If the exit code is non-zero, also set all downstream dependent EARs to be
        skipped. Also save any generated input/output files.

        """
        self.app.logger.debug(
            f"Setting end for run ID {run.id_!r} with exit code {exit_code!r}."
        )
        with self._store.cached_load():
            with self.batch_update():
                success = exit_code == 0  # TODO  more sophisticated success heuristics
                if not run.skip:
                    run_dir = run.get_directory()
                    if run.action.abortable and exit_code == ABORT_EXIT_CODE:
                        # the point of aborting an EAR is to continue with the workflow:
                        success = True

                    for IFG_i in run.action.input_file_generators:
                        inp_file = IFG_i.input_file
                        self.app.logger.debug(
                            f"Saving EAR input file: {inp_file.label!r} for EAR ID "
                            f"{run.id_!r}."
                        )
                        param_id = run.data_idx[f"input_files.{inp_file.label}"]

                        file_paths = inp_file.value(directory=run_dir)
                        if not isinstance(file_paths, list):
                            file_paths = [file_paths]

                        for path_i in file_paths:
                            self._set_file(
                                param_id=param_id,
                                store_contents=True,  # TODO: make optional according to IFG
                                is_input=False,
                                path=run_dir.joinpath(path_i),
                            )

                    if run.action.script_data_out_has_files:
                        run._param_save(block_act_key)

                    # Save action-level files: (TODO: refactor with below for OFPs)
                    for save_file_j in run.action.save_files:
                        self.app.logger.debug(
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
                        self.app.logger.debug(f"Saving output file paths: {file_paths!r}")
                        if not isinstance(file_paths, list):
                            file_paths = [file_paths]

                        for path_i in file_paths:
                            self._set_file(
                                param_id=param_id,
                                store_contents=True,
                                is_input=False,
                                path=run_dir.joinpath(path_i),
                                clean_up=(save_file_j in run.action.clean_up),
                            )

                    for OFP_i in run.action.output_file_parsers:
                        for save_file_j in OFP_i.save_files:
                            self.app.logger.debug(
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
                            self.app.logger.debug(
                                f"Saving EAR output file paths: {file_paths!r}"
                            )
                            if not isinstance(file_paths, list):
                                file_paths = [file_paths]

                            for path_i in file_paths:
                                self._set_file(
                                    param_id=param_id,
                                    store_contents=True,  # TODO: make optional according to OFP
                                    is_input=False,
                                    path=run_dir.joinpath(path_i),
                                    clean_up=(save_file_j in OFP_i.clean_up),
                                )

                if not success and run.skip_reason is not SkipReason.LOOP_TERMINATION:
                    # loop termination skips are already propagated
                    for EAR_dep_ID in run.get_dependent_EARs(as_objects=False):
                        self.app.logger.debug(
                            f"Setting EAR ID {EAR_dep_ID!r} to skip because it depends on"
                            f" EAR ID {run.id_!r}, which exited with a non-zero exit code:"
                            f" {exit_code!r}."
                        )
                        self._store.set_EAR_skip(
                            {EAR_dep_ID: SkipReason.UPSTREAM_FAILURE.value}
                        )

                self._store.set_EAR_end(
                    run.id_, exit_code, success, run.action.requires_dir
                )

    def set_multi_run_ends(
        self,
        runs: Dict[Tuple[int, int, int], List[Tuple[app.ElementActionRun, int]]],
        run_dirs: List[Union[Path, None]],
    ) -> None:
        """Set end times and exit codes on multiple runs.

        If the exit code is non-zero, also set all downstream dependent runs to be
        skipped. Also save any generated input/output files."""

        self.app.logger.debug(f"Setting end for multiple run IDs.")
        with self._store.cached_load():
            with self.batch_update():
                run_ids = []
                exit_codes = []
                successes = []
                for block_act_key, run_dat in runs.items():
                    for run, exit_code in run_dat:
                        success = (
                            exit_code == 0
                        )  # TODO  more sophisticated success heuristics
                        if not run.skip:
                            run_dir = run.get_directory()
                            if run.action.abortable and exit_code == ABORT_EXIT_CODE:
                                # the point of aborting an EAR is to continue with the
                                # workflow:
                                success = True

                            for IFG_i in run.action.input_file_generators:
                                inp_file = IFG_i.input_file
                                self.app.logger.debug(
                                    f"Saving EAR input file: {inp_file.label!r} for EAR "
                                    f"ID {run.id_!r}."
                                )
                                param_id = run.data_idx[f"input_files.{inp_file.label}"]

                                file_paths = inp_file.value(directory=run_dir)
                                if not isinstance(file_paths, list):
                                    file_paths = [file_paths]

                                for path_i in file_paths:
                                    self._set_file(
                                        param_id=param_id,
                                        store_contents=True,  # TODO: make optional according to IFG
                                        is_input=False,
                                        path=run_dir.joinpath(path_i),
                                    )

                            if run.action.script_data_out_has_files:
                                run._param_save(block_act_key)

                            # Save action-level files: (TODO: refactor with below for OFPs)
                            for save_file_j in run.action.save_files:
                                self.app.logger.debug(
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
                                self.app.logger.debug(
                                    f"Saving output file paths: {file_paths!r}"
                                )
                                if not isinstance(file_paths, list):
                                    file_paths = [file_paths]

                                for path_i in file_paths:
                                    self._set_file(
                                        param_id=param_id,
                                        store_contents=True,
                                        is_input=False,
                                        path=run_dir.joinpath(path_i),
                                        clean_up=(save_file_j in run.action.clean_up),
                                    )

                            for OFP_i in run.action.output_file_parsers:
                                for save_file_j in OFP_i.save_files:
                                    self.app.logger.debug(
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
                                    self.app.logger.debug(
                                        f"Saving EAR output file paths: {file_paths!r}"
                                    )
                                    if not isinstance(file_paths, list):
                                        file_paths = [file_paths]

                                    for path_i in file_paths:
                                        self._set_file(
                                            param_id=param_id,
                                            store_contents=True,  # TODO: make optional according to OFP
                                            is_input=False,
                                            path=run_dir.joinpath(path_i),
                                            clean_up=(save_file_j in OFP_i.clean_up),
                                        )

                        if (
                            not success
                            and run.skip_reason is not SkipReason.LOOP_TERMINATION
                        ):
                            # loop termination skips are already propagated
                            for EAR_dep_ID in run.get_dependent_EARs(as_objects=False):
                                # TODO: this needs to be recursive?
                                self.app.logger.debug(
                                    f"Setting EAR ID {EAR_dep_ID!r} to skip because it "
                                    f"depends on EAR ID {run.id_!r}, which exited with a "
                                    f"non-zero exit code: {exit_code!r}."
                                )
                                self._store.set_EAR_skip(
                                    {EAR_dep_ID: SkipReason.UPSTREAM_FAILURE.value}
                                )

                        run_ids.append(run.id_)
                        exit_codes.append(exit_code)
                        successes.append(success)

                self._store.set_multi_run_ends(run_ids, run_dirs, exit_codes, successes)

    def set_EAR_skip(self, skip_reasons: Dict[int, SkipReason]) -> None:
        """Record that an EAR is to be skipped due to an upstream failure or loop
        termination condition being met."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_skip({k: v.value for k, v in skip_reasons.items()})

    def get_EAR_skipped(self, EAR_ID: int) -> None:
        """Check if an EAR is to be skipped."""
        with self._store.cached_load():
            return self._store.get_EAR_skipped(EAR_ID)

    @TimeIt.decorator
    def set_parameter_value(
        self, param_id: int, value: Any, commit: bool = False
    ) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_parameter_value(param_id, value)

        if commit:
            # force commit now:
            self._store._pending.commit_all()

    @TimeIt.decorator
    def set_parameter_values(self, values: Dict[int, Any], commit: bool = False) -> None:
        with self._store.cached_load():
            with self.batch_update():
                with self._store.cache_ctx():
                    self._store.set_parameter_values(values)

        if commit:
            # force commit now:
            self._store._pending.commit_all()

    def set_EARs_initialised(self, iter_ID: int):
        """Set `ElementIteration.EARs_initialised` to True for the specified iteration."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EARs_initialised(iter_ID)

    def elements(self) -> Iterator[app.Element]:
        for task in self.tasks:
            for element in task.elements[:]:
                yield element

    @TimeIt.decorator
    def get_iteration_task_pathway(self, ret_iter_IDs=False, ret_data_idx=False):
        pathway = []
        for task in self.tasks:
            pathway.append((task.insert_ID, {}))

        added_loop_names = set()
        for _ in range(self.num_loops):
            to_add = None
            for loop in self.loops:
                if loop.name in added_loop_names:
                    continue
                elif set(loop.parents).issubset(added_loop_names):
                    # add a loop only once their parents have been added:
                    to_add = loop
                    break

            if to_add is None:
                raise RuntimeError(
                    "Failed to find a loop whose parents have already been added to the "
                    "iteration task pathway."
                )

            iIDs = to_add.task_insert_IDs
            relevant_idx = [idx for idx, i in enumerate(pathway) if i[0] in iIDs]

            for num_add_k, num_add in to_add.num_added_iterations.items():
                parent_loop_idx = {
                    to_add.parents[idx]: i for idx, i in enumerate(num_add_k)
                }

                repl = []
                repl_idx = []
                for i in range(num_add):
                    for p_idx, p in enumerate(pathway):
                        skip = False
                        if p[0] not in iIDs:
                            continue
                        for k, v in parent_loop_idx.items():
                            if p[1][k] != v:
                                skip = True
                                break
                        if skip:
                            continue
                        p = copy.deepcopy(p)
                        p[1].update({to_add.name: i})
                        repl_idx.append(p_idx)
                        repl.append(p)

                if repl:
                    repl_start, repl_stop = min(repl_idx), max(repl_idx)
                    pathway = replace_items(pathway, repl_start, repl_stop + 1, repl)

            added_loop_names.add(to_add.name)

        if added_loop_names != set(i.name for i in self.loops):
            raise RuntimeError(
                "Not all loops have been considered in the iteration task pathway."
            )

        if ret_iter_IDs or ret_data_idx:
            all_iters = self.get_all_element_iterations()
            for idx, i in enumerate(pathway):
                i_iters = []
                for iter_j in all_iters:
                    if iter_j.task.insert_ID == i[0] and iter_j.loop_idx == i[1]:
                        i_iters.append(iter_j)
                new = list(i)
                if ret_iter_IDs:
                    new += [tuple([j.id_ for j in i_iters])]
                if ret_data_idx:
                    new += [tuple(j.get_data_idx() for j in i_iters)]
                pathway[idx] = tuple(new)

        return pathway

    @TimeIt.decorator
    def _submit(
        self,
        status: Optional[Any] = None,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[Union[bool, Literal["direct", "scheduled"]]] = None,
        print_stdout: Optional[bool] = False,
        add_to_known: Optional[bool] = True,
        tasks: Optional[List[int]] = None,
    ) -> Tuple[List[Exception], Dict[int, int]]:
        """Submit outstanding EARs for execution."""

        # generate a new submission if there are no pending submissions:
        pending = [i for i in self.submissions if i.needs_submit]
        if not pending:
            if status:
                status.update("Adding new submission...")
            new_sub = self._add_submission(
                tasks=tasks,
                JS_parallelism=JS_parallelism,
                status=status,
            )
            if not new_sub:
                if status:
                    status.stop()
                raise ValueError("No pending element action runs to submit!")
            pending = [new_sub]

        self.execution_path.mkdir(exist_ok=True, parents=True)
        self.task_artifacts_path.mkdir(exist_ok=True, parents=True)

        # for direct execution the submission must be persistent at submit-time, because
        # it will be read by a new instance of the app:
        if status:
            status.update("Committing to the store...")
        self._store._pending.commit_all()

        # submit all pending submissions:
        exceptions = []
        submitted_js = {}
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

    def submit(
        self,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[Union[bool, Literal["direct", "scheduled"]]] = None,
        print_stdout: Optional[bool] = False,
        wait: Optional[bool] = False,
        add_to_known: Optional[bool] = True,
        return_idx: Optional[bool] = False,
        tasks: Optional[List[int]] = None,
        cancel: Optional[bool] = False,
        status: Optional[bool] = True,
    ) -> Dict[int, int]:
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

        if status:
            console = rich.console.Console()
            status = console.status("Submitting workflow...")
            status.start()

        with self._store.cached_load():
            if not self._store.is_submittable:
                if status:
                    status.stop()
                raise NotImplementedError("The workflow is not submittable.")
            with self.batch_update():
                # commit updates before raising exception:
                try:
                    with self._store.cache_ctx():
                        exceptions, submitted_js = self._submit(
                            ignore_errors=ignore_errors,
                            JS_parallelism=JS_parallelism,
                            print_stdout=print_stdout,
                            status=status,
                            add_to_known=add_to_known,
                            tasks=tasks,
                        )
                except Exception:
                    if status:
                        status.stop()
                    raise

        if exceptions:
            msg = "\n" + "\n\n".join([i.message for i in exceptions])
            if status:
                status.stop()
            raise WorkflowSubmissionFailure(msg)

        if status:
            status.stop()

        if cancel:
            self.cancel()

        elif wait:
            self.wait(submitted_js)

        if return_idx:
            return submitted_js

    def wait(self, sub_js: Optional[Dict] = None):
        """Wait for the completion of specified/all submitted jobscripts."""

        # TODO: think about how this might work with remote workflow submission (via SSH)

        def wait_for_direct_jobscripts(jobscripts: List[app.Jobscript]):
            """Wait for the passed direct (i.e. non-scheduled) jobscripts to finish."""

            def callback(proc):
                js = js_pids[proc.pid]
                # TODO sometimes proc.returncode is None; maybe because multiple wait
                # calls?
                print(
                    f"Jobscript {js.index} from submission {js.submission.index} "
                    f"finished with exit code {proc.returncode}."
                )

            js_pids = {i.process_ID: i for i in jobscripts}
            process_refs = [(i.process_ID, i.submit_cmdline) for i in jobscripts]
            DirectScheduler.wait_for_jobscripts(js_refs=process_refs, callback=callback)

        def wait_for_scheduled_jobscripts(jobscripts: List[app.Jobscript]):
            """Wait for the passed scheduled jobscripts to finish."""
            schedulers = app.Submission.get_unique_schedulers_of_jobscripts(jobscripts)
            threads = []
            for js_indices, sched in schedulers.items():
                jobscripts = [
                    self.submissions[sub_idx].jobscripts[js_idx]
                    for sub_idx, js_idx in js_indices
                ]
                job_IDs = [i.scheduler_job_ID for i in jobscripts]
                threads.append(Thread(target=sched.wait_for_jobscripts, args=(job_IDs,)))

            for i in threads:
                i.start()

            for i in threads:
                i.join()

        # TODO: add a log file to the submission dir where we can log stuff (e.g starting
        # a thread...)

        if not sub_js:
            # find any active jobscripts first:
            sub_js = defaultdict(list)
            for sub in self.submissions:
                for js_idx in sub.get_active_jobscripts():
                    sub_js[sub.index].append(js_idx)

        js_direct = []
        js_sched = []
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
            t_direct = Thread(target=wait_for_direct_jobscripts, args=(js_direct,))
            t_sched = Thread(target=wait_for_scheduled_jobscripts, args=(js_sched,))
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
        task_idx: Optional[int] = None,
        task_insert_ID: Optional[int] = None,
    ) -> List[app.Element]:
        """Retrieve elements that are running according to the scheduler."""

        if task_idx is not None and task_insert_ID is not None:
            raise ValueError("Specify at most one of `task_insert_ID` and `task_idx`.")

        # keys are task_insert_IDs, values are element indices:
        active_elems = defaultdict(set)
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
        out = []
        for task_iID, elem_idx in active_elems.items():
            if task_insert_ID is not None and task_iID != task_insert_ID:
                continue
            task = self.tasks.get(insert_ID=task_iID)
            if task_idx is not None and task_idx != task.index:
                continue
            for idx_i in elem_idx:
                out.append(task.elements[idx_i])

        return out

    def get_running_runs(
        self,
        submission_idx: int = -1,
        task_idx: Optional[int] = None,
        task_insert_ID: Optional[int] = None,
        element_idx: int = None,
    ) -> List[app.ElementActionRun]:
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

    def _abort_run(self, run):
        # connect to the ZeroMQ server on the worker node:
        self.app.logger.info(f"abort run: {run!r}")
        self.app.Executor.send_abort(
            hostname=run.run_hostname, port_number=run.port_number
        )

    def abort_run(
        self,
        submission_idx: int = -1,
        task_idx: Optional[int] = None,
        task_insert_ID: Optional[int] = None,
        element_idx: int = None,
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
                elem_idx = tuple(i.element.index for i in running)
                raise ValueError(
                    f"Multiple elements are running (indices: {elem_idx!r}). Specify "
                    f"which element index you want to abort."
                )
            else:
                raise RuntimeError(f"Multiple running runs.")

        run = running[0]
        if not run.action.abortable:
            raise RunNotAbortableError(
                "The run is not defined as abortable in the task schema, so it cannot "
                "be aborted."
            )
        self._abort_run(run)

    @TimeIt.decorator
    def cancel(self, hard=False):
        """Cancel any running jobscripts."""
        for sub in self.submissions:
            sub.cancel()

    def add_submission(
        self,
        tasks: Optional[List[int]] = None,
        JS_parallelism: Optional[Union[bool, Literal["direct", "scheduled"]]] = None,
        force_array: Optional[bool] = False,
        status: Optional[bool] = True,
    ) -> app.Submission:
        """Add a new submission.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.
        """
        if status:
            console = rich.console.Console()
            status = console.status("")
            status.start()
        try:
            with self._store.cached_load():
                with self.batch_update():
                    return self._add_submission(
                        tasks, JS_parallelism, force_array, status
                    )
        finally:
            if status:
                status.stop()

    @TimeIt.decorator
    @load_workflow_config
    def _add_submission(
        self,
        tasks: Optional[List[int]] = None,
        JS_parallelism: Optional[bool] = None,
        force_array: Optional[bool] = False,
        status: Optional[Any] = None,
    ) -> app.Submission:
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

        sub_obj = self.app.Submission(
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
                f"There are no pending element action runs, so a new submission was not "
                f"added."
            )
            return

        if status:
            status.update("Adding new submission: making artifact directories...")

        # TODO: a submission should only be "submitted" once shouldn't it?
        # no; there could be an IO error (e.g. internet connectivity), so might
        # need to be able to reattempt submission of outstanding jobscripts.
        self.submissions_path.mkdir(exist_ok=True, parents=True)
        sub_obj.path.mkdir(exist_ok=True)
        sub_obj.tmp_path.mkdir(exist_ok=True)
        sub_obj.app_std_path.mkdir(exist_ok=True)
        sub_obj.js_std_path.mkdir(exist_ok=True)
        sub_obj.scripts_path.mkdir(exist_ok=True)
        sub_obj.commands_path.mkdir(exist_ok=True)

        if sub_obj.needs_app_log_dir:
            sub_obj.app_log_path.mkdir(exist_ok=True)

        if status:
            status.update("Adding new submission: writing scripts and command files...")

        # write scripts and command files where possible to the submission directory:
        cmd_file_IDs, run_indices, run_inp_files = sub_obj._write_scripts(cache, status)

        sub_obj._write_execute_dirs(run_indices, run_inp_files, cache, status)

        if status:
            status.update("Adding new submission: updating the store...")

        with self._store.cached_load():
            with self.batch_update():
                for i in all_EAR_ID:
                    self._store.set_run_submission_data(
                        EAR_ID=i,
                        cmds_ID=cmd_file_IDs[i],
                        sub_idx=new_idx,
                    )

        sub_obj_js, _ = sub_obj.to_json_like()
        self._submissions.append(sub_obj)
        self._pending["submissions"].append(new_idx)
        with self._store.cached_load():
            with self.batch_update():
                self._store.add_submission(new_idx, sub_obj_js)

        return self.submissions[new_idx]

    @TimeIt.decorator
    def resolve_jobscripts(
        self,
        cache,
        tasks: Optional[List[int]] = None,
        force_array: Optional[bool] = False,
    ) -> List[app.Jobscript]:
        """Generate jobscripts for a new submission.

        Parameters
        ----------
        force_array
            Used to force the use of job arrays, even if the scheduler does not support
            it. This is provided for testing purposes only.

        """
        with self.app.config.cached_config():
            with self.cached_merged_parameters():
                js, element_deps = self._resolve_singular_jobscripts(
                    cache, tasks, force_array
                )

            js_deps = resolve_jobscript_dependencies(js, element_deps)

            for js_idx in js:
                if js_idx in js_deps:
                    js[js_idx]["dependencies"] = js_deps[js_idx]

            js = merge_jobscripts_across_tasks(js)

            # for direct or (non-array scheduled), combine into jobscripts of multiple
            # blocks for dependent jobscripts that have the same resource hashes
            js = resolve_jobscript_blocks(js)

            js_objs = [self.app.Jobscript(**i, index=idx) for idx, i in enumerate(js)]

        return js_objs

    @TimeIt.decorator
    def _resolve_singular_jobscripts(
        self,
        cache,
        tasks: Optional[List[int]] = None,
        force_array: Optional[bool] = False,
    ) -> Tuple[Dict[int, Dict], Dict]:
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
        all_element_deps
            For a given jobscript index, for a given jobscript element index within that
            jobscript, this is a list of EAR IDs dependencies of that element.

        """
        if not tasks:
            tasks = list(range(self.num_tasks))

        if self._store.use_cache:
            # pre-cache parameter sources (used in `EAR.get_EAR_dependencies`):
            # note: this cache is unrelated to the `cache` argument
            self.get_all_parameter_sources()

        submission_jobscripts = {}
        all_element_deps = {}

        for task_iID, loop_idx_i in self.get_iteration_task_pathway():
            task = self.tasks.get(insert_ID=task_iID)
            if task.index not in tasks:
                continue
            res, res_hash, res_map, EAR_map = generate_EAR_resource_map(
                task, loop_idx_i, cache
            )
            jobscripts, _ = group_resource_map_into_jobscripts(res_map)

            for js_dat in jobscripts:
                # (insert ID, action_idx, index into task_loop_idx):
                task_actions = [
                    [task.insert_ID, i, 0]
                    for i in sorted(
                        set(
                            act_idx_i
                            for act_idx in js_dat["elements"].values()
                            for act_idx_i in act_idx
                        )
                    )
                ]
                # task_elements: { JS_ELEM_IDX: [TASK_ELEM_IDX for each task insert ID]}
                task_elements = {
                    js_elem_idx: [task_elem_idx]
                    for js_elem_idx, task_elem_idx in enumerate(js_dat["elements"].keys())
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
                js_i = {
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

                all_EAR_IDs = []
                for js_elem_idx, (elem_idx, act_indices) in enumerate(
                    js_dat["elements"].items()
                ):
                    for act_idx in act_indices:
                        EAR_ID_i = EAR_map[act_idx, elem_idx].item()
                        all_EAR_IDs.append(EAR_ID_i)
                        js_act_idx = task_actions.index([task.insert_ID, act_idx, 0])
                        js_i["EAR_ID"][js_act_idx][js_elem_idx] = EAR_ID_i

                all_EAR_objs = {k: cache.runs[k] for k in all_EAR_IDs}

                for js_elem_idx, (elem_idx, act_indices) in enumerate(
                    js_dat["elements"].items()
                ):
                    all_EAR_IDs = []
                    for act_idx in act_indices:
                        EAR_ID_i = EAR_map[act_idx, elem_idx].item()
                        all_EAR_IDs.append(EAR_ID_i)
                        js_act_idx = task_actions.index([task.insert_ID, act_idx, 0])
                        js_i["EAR_ID"][js_act_idx][js_elem_idx] = EAR_ID_i

                    # get indices of EARs that this element depends on:
                    EAR_objs = [all_EAR_objs[k] for k in all_EAR_IDs]
                    EAR_deps = [i.get_EAR_dependencies() for i in EAR_objs]
                    EAR_deps_flat = [j for i in EAR_deps for j in i]
                    EAR_deps_EAR_idx = [
                        i for i in EAR_deps_flat if i not in js_i["EAR_ID"]
                    ]
                    if EAR_deps_EAR_idx:
                        if new_js_idx not in all_element_deps:
                            all_element_deps[new_js_idx] = {}

                        all_element_deps[new_js_idx][js_elem_idx] = EAR_deps_EAR_idx

                submission_jobscripts[new_js_idx] = js_i

        return submission_jobscripts, all_element_deps

    @load_workflow_config
    def execute_run(
        self,
        submission_idx: int,
        block_act_key: Tuple[int, int, int],
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

            js_idx = block_act_key[0]
            run = self.get_EARs_from_IDs([run_ID])[0]
            run_dir = None
            if run.action.requires_dir:
                run_dir = run.get_directory()
                self.app.submission_logger.debug(
                    f"changing directory to run execution directory: {run_dir}."
                )
                os.chdir(run_dir)
            self.app.submission_logger.debug(f"{run.skip=}; {run.skip_reason=}")

            # check if we should skip:
            if not run.skip:

                if run.action.script:
                    run.write_script_input_files(block_act_key)

                # write the command file that will be executed:
                cmd_file_path = self.ensure_commands_file(submission_idx, js_idx, run)
                has_commands = bool(cmd_file_path)
                if has_commands:

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
                    app_caps = self.app.package_name.upper()

                    # TODO: make these optionally set (more difficult to set in combine_script,
                    # so have the option to turn off) [default ON]
                    add_env = {
                        f"{app_caps}_RUN_ID": str(run_ID),
                        f"{app_caps}_RUN_IDX": str(run.index),
                        f"{app_caps}_ELEMENT_IDX": str(run.element.index),
                        f"{app_caps}_ELEMENT_ID": str(run.element.id_),
                        f"{app_caps}_ELEMENT_ITER_IDX": str(run.element_iteration.index),
                        f"{app_caps}_ELEMENT_ITER_ID": str(run.element_iteration.id_),
                        f"{app_caps}_ELEMENT_ITER_LOOP_IDX": loop_idx_str,
                    }

                    if run.action.script:
                        if run.is_snippet_script:
                            script_artifact_name = run.get_script_artifact_name()
                            script_dir = Path(os.environ[f"{app_caps}_SUB_SCRIPTS_DIR"])
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

                    self.app.submission_logger.debug(
                        f"Executing run commands via subprocess with command {cmd!r}, and "
                        f"environment variables as below."
                    )
                    for k, v in env.items():
                        if k.startswith(app_caps):
                            self.app.submission_logger.debug(f"{k} = {v!r}")

                    exe = self.app.Executor(cmd, env, self.app.package_name)
                    port = exe.start_zmq_server()  # start the server so we know the port

                    try:
                        self.set_EAR_start(run_ID, run_dir, port)
                    except:
                        self.app.submission_logger.error(f"Failed to set run start.")
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

    def _check_loop_termination(self, run) -> set[int]:
        """Check if we need to terminate a loop if this is the last action of the loop
        iteration for this element."""

        elem_iter = run.element_iteration
        task = elem_iter.task
        check_loops = []
        to_skip = set()
        for loop_name in elem_iter.loop_idx:
            self.app.logger.info(f"checking loop termination of loop {loop_name!r}.")
            loop = self.loops.get(loop_name)
            if (
                loop.template.termination
                and task.insert_ID == loop.template.termination_task_insert_ID
                and run.element_action.action_idx == max(elem_iter.actions)
            ):
                check_loops.append(loop_name)
                # TODO: test with condition actions
                if loop.test_termination(elem_iter):
                    self.app.logger.info(
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

        app_caps = self.app.package_name.upper()
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

        # TODO: refactor cmd file name:
        cmd_file_path = sub.commands_path / f"js_{jobscript_idx}{js.shell.JS_EXT}"
        cmd = js.shell.get_command_file_launch_command(str(cmd_file_path))

        self.app.submission_logger.debug(
            f"Executing combined runs via subprocess with command {cmd!r}, and "
            f"environment variables as below."
        )
        for k, v in env.items():
            if k.startswith(app_caps):
                self.app.submission_logger.debug(f"{k} = {v}")

        exe = self.app.Executor(cmd, env, self.app.package_name)
        exe.start_zmq_server()  # start the server
        exe.run()  # this also shuts down the server

    def ensure_commands_file(
        self,
        submission_idx: int,
        js_idx: int,
        run: app.ElementActionRun,
    ) -> Union[Path, bool]:
        """Ensure a commands file exists for the specified run."""
        self.app.persistence_logger.debug("Workflow.ensure_commands_file")

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
        with self._store.cached_load():
            with self.batch_update():
                EAR = self.get_EARs_from_IDs([EAR_ID])[0]
                command = EAR.action.commands[cmd_idx]
                return command.process_std_stream(name, value, stderr)

    def save_parameter(
        self,
        name: str,
        value: Any,
        EAR_ID: int,
    ):
        self.app.logger.info(f"save parameter {name!r} for EAR_ID {EAR_ID}.")
        self.app.logger.debug(f"save parameter {name!r} value is {value!r}.")
        with self._store.cached_load():
            with self.batch_update():
                EAR = self.get_EARs_from_IDs([EAR_ID])[0]
                param_id = EAR.data_idx[name]
                self.set_parameter_value(param_id, value)

    def show_all_EAR_statuses(self):
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
        self, input_source: app.InputSource, new_task_name: str
    ) -> None:
        """Normalise the input source task reference and convert a source to a local type
        if required."""

        # TODO: test thoroughly!

        if isinstance(input_source.task_ref, str):
            if input_source.task_ref == new_task_name:
                if input_source.task_source_type is self.app.TaskSourceType.OUTPUT:
                    raise InvalidInputSourceTaskReference(
                        f"Input source {input_source.to_string()!r} cannot refer to the "
                        f"outputs of its own task!"
                    )
                else:
                    warn(
                        f"Changing input source {input_source.to_string()!r} to a local "
                        f"type, since the input source task reference refers to its own "
                        f"task."
                    )
                    # TODO: add an InputSource source_type setter to reset
                    # task_ref/source_type?
                    input_source.source_type = self.app.InputSourceType.LOCAL
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
    def get_all_submission_run_IDs(self) -> List[int]:
        self.app.persistence_logger.debug("Workflow.get_all_submission_run_IDs")
        id_lst = []
        for sub in self.submissions:
            id_lst.extend(list(sub.all_EAR_IDs))
        return id_lst

    def rechunk_runs(
        self,
        chunk_size: Optional[int] = None,
        backup: Optional[bool] = True,
        status: Optional[bool] = True,
    ):
        self._store.rechunk_runs(chunk_size=chunk_size, backup=backup, status=status)

    def rechunk_parameter_base(
        self,
        chunk_size: Optional[int] = None,
        backup: Optional[bool] = True,
        status: Optional[bool] = True,
    ):
        self._store.rechunk_parameter_base(
            chunk_size=chunk_size, backup=backup, status=status
        )

    def rechunk(
        self,
        chunk_size: Optional[int] = None,
        backup: Optional[bool] = True,
        status: Optional[bool] = True,
    ):
        """Rechunk metadata/runs and parameters/base arrays."""
        self.rechunk_runs(chunk_size=chunk_size, backup=backup, status=status)
        self.rechunk_parameter_base(chunk_size=chunk_size, backup=backup, status=status)

    @TimeIt.decorator
    def get_run_directories(
        self,
        run_ids: Optional[List[int]] = None,
        dir_indices_arr: Optional[np.ndarray] = None,
    ) -> List[Union[Path, None]]:
        """"""

        @TimeIt.decorator
        def _get_depth_dirs(
            item_idx, max_per_dir, max_depth, depth_idx_cache, prefix
        ) -> List[str]:
            dirs = []
            max_avail_items = max_per_dir**max_depth
            for depth_i in range(1, max_depth):
                tot_items_per_level = int(max_avail_items / max_per_dir**depth_i)
                key = (max_avail_items, tot_items_per_level)
                depth_idx = depth_idx_cache.get(key)
                if depth_idx is None:
                    depth_idx = np.repeat(
                        np.arange(max_avail_items / tot_items_per_level, dtype=int),
                        tot_items_per_level,
                    )
                    depth_idx_cache[key] = depth_idx
                idx_i = depth_idx[item_idx]
                start_idx = idx_i * tot_items_per_level
                end_idx = start_idx + tot_items_per_level - 1
                dirs.append(f"{prefix}_{start_idx}-{end_idx}")
            return dirs

        if dir_indices_arr is None:  # TODO: document behaviour!
            dir_indices_arr = self._store.get_dirs_arr()
            if run_ids is not None:
                dir_indices_arr = dir_indices_arr[run_ids]

        MAX_ELEMS_PER_DIR = 1000  # TODO: configurable (add `workflow_defaults` to Config)
        MAX_ITERS_PER_DIR = 1000

        exec_path = self.execution_path

        # a fill value means no sub directory should be created
        T_FILL, E_FILL, I_FILL, A_FILL, R_FILL, _, _ = RUN_DIR_ARR_FILL

        depth_idx_cache = {}  # keys are (max_avail, tot_elems_per_dir_level)

        # format run directories:
        dirs = []
        for dir_data in dir_indices_arr:

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


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
