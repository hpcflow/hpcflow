"""An hpcflow application."""

from __future__ import annotations

from collections import Counter, namedtuple, defaultdict
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime, timezone
import stat
import copy
import enum
import json
import shutil
from functools import wraps
from importlib import resources, import_module
import os
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypeVar, Generic, cast, TYPE_CHECKING, Literal
import warnings
import zipfile
from platformdirs import user_cache_path, user_data_dir
import requests
from reretry import retry  # type: ignore
from rich.console import Console, Group
from rich.syntax import Syntax
from rich.table import Table, box
from rich.text import Text
from rich.padding import Padding
from rich.panel import Panel
from rich.pretty import Pretty
from rich import print as rich_print
from fsspec.core import url_to_fs  # type: ignore
from fsspec.implementations.local import LocalFileSystem  # type: ignore

from hpcflow import __version__
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.utils import (
    read_YAML_str,
    read_YAML_file,
    read_JSON_file,
    write_YAML_file,
    write_JSON_file,
    redirect_std_to_file as redirect_std_to_file_hpcflow,
    parse_timestamp,
    get_file_context,
    open_text_resource,
)
from hpcflow.sdk import sdk_classes, sdk_funcs, get_SDK_logger
from hpcflow.sdk.config import Config, ConfigFile
from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS
from hpcflow.sdk.utils.envs import get_env_py_exe, norm_env_setup
from hpcflow.sdk.utils.files import (
    copy_file_or_dir,
    delete_file_or_dir,
    overwrite_YAML_file,
    download_github_repo,
)
from hpcflow.sdk.utils.errors import get_with_index, StoredIndexError
from .core.workflow import Workflow as _Workflow
from .core.environment import Environment as Environment_cls
from hpcflow.sdk.log import AppLog, TimeIt
from hpcflow.sdk.persistence.defaults import DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES
from hpcflow.sdk.runtime import RunTimeInfo
from hpcflow.sdk.cli import make_cli
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.shells import DEFAULT_SHELL_NAMES, get_shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)
from hpcflow.sdk.core.errors import (
    EnvironmentAlreadyExists,
    EnvironmentNotFound,
    CannotRemoveBuiltinEnvironment,
)
from hpcflow.sdk.core.warnings import batch_warnings

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from logging import Logger
    from types import ModuleType
    from typing import ClassVar, Literal, Protocol
    from typing_extensions import Final
    from rich.status import Status
    from .typing import (
        BasicTemplateComponents,
        KnownSubmission,
        KnownSubmissionItem,
        PathLike,
        TemplateComponents,
        MakeWorkflowCommonArgs,
    )
    from .config.config import ConfigOptions
    from .core.actions import (
        ElementActionRun,
        ElementAction,
        ActionEnvironment,
        Action,
        ActionScope,
        ActionRule,
    )
    from .core.command_files import (
        FileSpec,
        FileNameSpec,
        InputFileGenerator,
        FileNameStem,
        FileNameExt,
        OutputFileParser,
    )
    from .core.commands import Command
    from .core.element import (
        ElementInputs,
        ElementOutputs,
        ElementInputFiles,
        ElementOutputFiles,
        ElementIteration,
        Element,
        ElementParameter,
        ElementResources,
        ElementFilter,
        ElementGroup,
    )
    from .core.enums import ActionScopeType, InputSourceType, TaskSourceType
    from .core.environment import (
        NumCores,
        Environment as _Environment,
        Executable as _Executable,
        ExecutableInstance,
    )
    from .core.loop import Loop, WorkflowLoop
    from .core.object_list import (
        CommandFilesList as _CommandFilesList,
        EnvironmentsList as _EnvironmentsList,
        ExecutablesList,
        GroupList,
        ParametersList as _ParametersList,
        ResourceList,
        TaskList,
        TaskSchemasList as _TaskSchemasList,
        TaskTemplateList,
        WorkflowLoopList,
        WorkflowTaskList,
    )
    from .core.parameters import (
        SchemaParameter,
        InputValue,
        Parameter,
        ParameterValue,
        InputSource,
        ResourceSpec,
        SchemaOutput,
        ValueSequence,
        MultiPathSequence,
        SchemaInput,
    )
    from .core.rule import Rule
    from .core.run_dir_files import RunDirAppFiles
    from .core.task import (
        Task,
        WorkflowTask,
        Parameters,
        TaskInputParameters,
        TaskOutputParameters,
        ElementPropagation,
        ElementSet,
    )
    from .core.task_schema import TaskSchema, TaskObjective
    from .core.workflow import WorkflowTemplate as _WorkflowTemplate
    from .submission.jobscript import Jobscript
    from .submission.submission import Submission as _Submission  # TODO: why?
    from .submission.schedulers import Scheduler, QueuedScheduler
    from .submission.schedulers.direct import DirectPosix, DirectWindows
    from .submission.schedulers.sge import SGEPosix
    from .submission.schedulers.slurm import SlurmPosix
    from .submission.shells.base import VersionInfo
    from .core.json_like import JSONDocument
    from .compact_errors import CompactProblemFormatter

    # Complex types for SDK functions
    class _MakeWorkflow(Protocol):
        """Type of :py:meth:`BaseApp.make_workflow`"""

        def __call__(
            self,
            template_file_or_str: PathLike | str,
            is_string: bool = False,
            template_format: Literal["json", "yaml"] | None = None,
            path: PathLike = None,
            name: str | None = None,
            name_add_timestamp: bool | None = None,
            name_use_dir: bool | None = None,
            overwrite: bool = False,
            store: str = DEFAULT_STORE_FORMAT,
            ts_fmt: str | None = None,
            ts_name_fmt: str | None = None,
            store_kwargs: dict[str, Any] | None = None,
            variables: dict[str, str] | None = None,
            status: bool = True,
            add_submission: bool = False,
        ) -> _Workflow | _Submission | None: ...

    class _MakeDemoWorkflow(Protocol):
        """Type of :py:meth:`BaseApp.make_demo_workflow`"""

        def __call__(
            self,
            workflow_name: str,
            template_format: Literal["json", "yaml"] | None = None,
            path: PathLike | None = None,
            name: str | None = None,
            name_add_timestamp: bool | None = None,
            name_use_dir: bool | None = None,
            overwrite: bool = False,
            store: str = DEFAULT_STORE_FORMAT,
            ts_fmt: str | None = None,
            ts_name_fmt: str | None = None,
            store_kwargs: dict[str, Any] | None = None,
            variables: dict[str, str] | None = None,
            status: bool = True,
            add_submission: bool = False,
        ) -> _Workflow | _Submission | None: ...

    class _MakeAndSubmitWorkflow(Protocol):
        """Type of :py:meth:`BaseApp.make_and_submit_workflow`"""

        # Should be overloaded on return_idx, but not bothering
        def __call__(
            self,
            template_file_or_str: PathLike | str,
            is_string: bool = False,
            template_format: Literal["json", "yaml"] | None = None,
            path: PathLike | None = None,
            name: str | None = None,
            name_add_timestamp: bool | None = None,
            name_use_dir: bool | None = None,
            overwrite: bool = False,
            store: str = DEFAULT_STORE_FORMAT,
            ts_fmt: str | None = None,
            ts_name_fmt: str | None = None,
            store_kwargs: dict[str, Any] | None = None,
            variables: dict[str, str] | None = None,
            JS_parallelism: bool | None = None,
            wait: bool = False,
            add_to_known: bool = True,
            return_idx: bool = False,
            tasks: list[int] | None = None,
            cancel: bool = False,
            status: bool = True,
            quiet: bool = False,
        ) -> tuple[_Workflow, Mapping[int, Sequence[int]]] | _Workflow: ...

    class _MakeAndSubmitDemoWorkflow(Protocol):
        """Type of :py:meth:`BaseApp.make_and_submit_demo_workflow`"""

        # Should be overloaded on return_idx, but not bothering
        def __call__(
            self,
            workflow_name: str,
            template_format: Literal["json", "yaml"] | None = None,
            path: PathLike | None = None,
            name: str | None = None,
            name_add_timestamp: bool | None = None,
            name_use_dir: bool | None = None,
            overwrite: bool = False,
            store: str = DEFAULT_STORE_FORMAT,
            ts_fmt: str | None = None,
            ts_name_fmt: str | None = None,
            store_kwargs: dict[str, Any] | None = None,
            variables: dict[str, str] | None = None,
            JS_parallelism: bool | None = None,
            wait: bool = False,
            add_to_known: bool = True,
            return_idx: bool = False,
            tasks: list[int] | None = None,
            cancel: bool = False,
            status: bool = True,
            quiet: bool = False,
        ) -> tuple[_Workflow, Mapping[int, Sequence[int]]] | _Workflow: ...

    class _SubmitWorkflow(Protocol):
        """Type of :py:meth:`BaseApp.submit_workflow`"""

        # Should be overloaded on return_idx, but not bothering
        def __call__(
            self,
            workflow_path: PathLike,
            JS_parallelism: bool | None = None,
            wait: bool = False,
            return_idx: bool = False,
            tasks: list[int] | None = None,
            quiet: bool = False,
        ) -> Mapping[int, Sequence[int]] | None: ...

    class _GetKnownSubmissions(Protocol):
        """Type of :py:meth:`BaseApp.get_known_submissions`"""

        # Should be overloaded on as_json, but not bothering
        def __call__(
            self,
            max_recent: int = 3,
            no_update: bool = False,
            as_json: bool = False,
            status: Status | None = None,
        ) -> Sequence[KnownSubmissionItem]: ...

    class _Show(Protocol):
        """Type of :py:meth:`BaseApp.show`"""

        def __call__(
            self,
            max_recent: int = 3,
            full: bool = False,
            no_update: bool = False,
        ) -> None: ...

    class _Cancel(Protocol):
        """Type of :py:meth:`BaseApp.cancel`"""

        def __call__(
            self,
            workflow_ref: int | str | Path,
            ref_is_path: str | None = None,
            status: bool = False,
            quiet: bool = False,
        ) -> None: ...

    class _RunTests(Protocol):
        """Type of :py:meth:`BaseApp.run_tests and run_hpcflow_tests`"""

        def __call__(
            self,
            test_dirs: Sequence[str | Path] | None = None,
            pytest_args: Sequence[str] | None = None,
        ) -> int: ...


SDK_logger = get_SDK_logger(__name__)
DEMO_WK_FORMATS = {".yaml": "yaml", ".yml": "yaml", ".json": "json", ".jsonc": "json"}

T = TypeVar("T")

EnvInfo = namedtuple("EnvInfo", ["manager", "exe", "prefix"])


def rate_limit_safe_url_to_fs(
    app: BaseApp, *args, logger: Logger | None = None, **kwargs
):
    R"""
    Call fsspec's ``url_to_fs`` but retry on ``requests.exceptions.HTTPError``\ s.

    References
    ----------
    [1]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?
            apiVersion=2022-11-28#about-secondary-rate-limits
    """
    auth = {}
    if app.run_time_info.in_pytest:
        gh_token = os.environ.get("GH_TOKEN")
        if gh_token:
            # using the GitHub actions built in token increases the number of API
            # requests allowed per hour to 1000 [1]. fsspec requires "username" to be
            # set if using "token":
            auth = {"username": "", "token": gh_token}
            if logger:
                logger.info(
                    "calling fsspec's `url_to_fs` with a token from the env variable "
                    "`GH_TOKEN`."
                )

    # GitHub actions testing is potentially highly concurrent, with multiple
    # Python versions and OSes being tested at the same time; so we might hit
    # GitHub's secondary rate limit:
    @retry(
        requests.exceptions.HTTPError,
        tries=3,
        delay=5,
        backoff=1.5,
        jitter=(0, 20),
        logger=logger,
    )
    def _inner(*args, **kwargs):
        kwargs.update(auth)
        return url_to_fs(*args, **kwargs)

    return _inner(*args, **kwargs)


def __getattr__(name: str):
    """Allow access to core classes and API functions."""
    try:
        return get_app_attribute(name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")


def get_app_attribute(name: str):
    """
    A function to assign to an app module `__getattr__` to access app attributes.
    """
    app_obj: BaseApp
    try:
        app_obj = cast("App", App.get_instance())
    except RuntimeError:
        app_obj = cast("BaseApp", BaseApp.get_instance())
    try:
        return getattr(app_obj, name)
    except AttributeError:
        raise AttributeError(f"module {app_obj.module!r} has no attribute {name!r}.")


def get_app_module_all() -> list[str]:
    """
    The list of all symbols exported by this module.
    """
    return ["app", *sdk_classes, *sdk_funcs]


def get_app_module_dir() -> Callable[[], list[str]]:
    """
    The sorted list of all symbols exported by this module.
    """
    return lambda: sorted(get_app_module_all())


class Singleton(type, Generic[T]):
    """
    Metaclass that enforces that only one instance of a class can be made.

    Type Parameters
    ---------------
    T
        The type of the class that is a singleton.
    """

    _instances: ClassVar[dict[Singleton, Any]] = {}

    def __call__(cls: Singleton[T], *args, **kwargs) -> T:
        """
        Get the current instance or make it if it doesn't already exist.

        Return
        ------
        T:
            The unique instance of the class.
        """
        SDK_logger.info(
            f"App metaclass __call__: "
            f"name={kwargs['name']!r}, version={kwargs['version']!r}."
        )
        if cls not in cls._instances:
            SDK_logger.info(f"App metaclass initialising new object {kwargs['name']!r}.")
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def get_instance(cls: Singleton[T]) -> T:
        """
        Retrieve the instance of the singleton class if initialised.

        Raises
        ------
        RuntimeError
            If there is no instance already.
        """
        try:
            return cls._instances[cls]
        except KeyError:
            raise RuntimeError(f"{cls.__name__!r} object has not be instantiated!")


class BaseApp(metaclass=Singleton):
    """
    Class to generate the hpcflow application.

    Parameters
    ----------
    name:
        The name of the application.
    version:
        The version of the application.
    module:
        The module name in which the app object is defined.
    description:
        Description of the application.
    gh_org:
        Name of Github organisation responsible for the application.
    gh_repo:
        Github repository containing the application source.
    config_options:
        Configuration options.
    scripts_dir:
        Directory for scripts.
    jinja_templates_dir:
        Directory for Jinja templates.
    workflows_dir:
        Directory for workflows.
    data_manifest_dir:
        Directory that contains JSON manifest files for the demonstration data
        ("data.json") and built-in program data ("programs.json").
    data_dir:
        fsspec-compatible URL pointing to a directory that contains the app's built-in
        demonstration data, as referenced in the data manifest.
    program_dir:
        fsspec-compatible URL pointing to a directory that contains the app's built-in
        programs, as referenced in the data manifest.
    template_components:
        Template components.
    pytest_args:
        Arguments for pytest.
    package_name:
        Name of package if not the application name.
    docs_import_conv:
        The convention for the app alias used in import statements in the documentation.
        E.g. for the `hpcflow` base app, this is `hf`. This is combined with `module` to
        form the complete import statement. E.g. for the `hpcflow` base app, the complete
        import statement is: `import hpcflow.app as hf`, where `hpcflow.app` is the
        `module` argument and `hf` is the `docs_import_conv` argument.
    docs_url:
        URL to documentation.
    encoders:
        callable that takes no arguments and returns a mapping between string store types
        (e.g. "zarr", "json") and a dictionary of additional parameter encoders.
    decoders:
        callable that takes no arguments and returns a mapping between string store types
        (e.g. "zarr", "json") and a dictionary of additional parameter decoders.
    """

    _known_subs_file_name: ClassVar = "known_submissions.txt"
    _known_subs_file_sep: ClassVar = "::"
    _submission_ts_fmt: ClassVar = r"%Y-%m-%d %H:%M:%S.%f"
    __load_pending: ClassVar = False

    def __init__(
        self,
        name: str,
        version: str,
        module: str,
        description: str,
        gh_org: str,
        gh_repo: str,
        config_options: ConfigOptions,
        scripts_dir: str,
        jinja_templates_dir: str | None = None,
        workflows_dir: str | None = None,
        data_manifest_dir: str | None = None,
        data_dir: str | None = None,
        program_dir: str | None = None,
        template_components: dict[str, list[dict]] | None = None,
        pytest_args: list[str] | None = None,
        package_name: str | None = None,
        docs_import_conv: str | None = None,
        docs_url: str | None = None,
        encoders: Callable | None = None,
        decoders: Callable | None = None,
    ):
        SDK_logger.info(f"Generating {self.__class__.__name__} {name!r}.")

        #: The name of the application.
        self.name = name
        #: Name of package.
        self.package_name = package_name or name.lower()
        #: The version of the application.
        self.version = version
        #: The module name in which the app object is defined.
        self.module = module
        #: Description of the application.
        self.description = description
        #: Name of Github organisation responsible for the application.
        self.gh_org = gh_org
        #: Github repository containing the application source.
        self.gh_repo = gh_repo
        #: Configuration options.
        self.config_options = config_options
        #: Arguments for pytest.
        self.pytest_args = pytest_args
        #: Directory for scripts.
        self.scripts_dir = scripts_dir
        #: Directory for Jinja templates.
        self.jinja_templates_dir = jinja_templates_dir
        #: Directory for workflows.
        self.workflows_dir = workflows_dir
        #: Directory for JSON manifest files.
        self.data_manifest_dir = data_manifest_dir
        #: Directory for demonstration data.
        self.data_dir = data_dir
        #: Directory for built-in program data.
        self.program_dir = program_dir
        #: The convention for the app alias used in import statements in the documentation.
        self.docs_import_conv = docs_import_conv
        #: URL to documentation.
        self.docs_url = docs_url
        #: Callable that returns additional parameter encoders.
        self.encoders = encoders or (lambda: {})
        #: Callable that returns additional parameter decoders.
        self.decoders = decoders or (lambda: {})

        main_CLI, env_CLI = make_cli(self)

        #: Command line interface subsystem.
        self.cli = main_CLI
        #: Environment setup CLI, to which downstream apps can add their own commands
        self.env_setup_CLI = env_CLI

        self._log = AppLog(self)
        self._run_time_info = RunTimeInfo(
            self.name,
            self.package_name,
            self.version,
            self.runtime_info_logger,
        )

        self._builtin_template_components = template_components or {}

        self._config: Config | None = (
            None  # assigned on first access to `config` property
        )
        self._config_files: dict[str, ConfigFile] = (
            {}
        )  # assigned on config load, keys are string absolute paths

        # Set by `_load_template_components`:
        self._template_components: TemplateComponents = {}
        self._parameters: _ParametersList | None = None
        self._command_files: _CommandFilesList | None = None
        self._environments: _EnvironmentsList | None = None
        self._task_schemas: _TaskSchemasList | None = None
        self._scripts: dict[str, Path] | None = None
        self._jinja_templates: dict[str, Path] | None = None
        self._programs: dict[str, Path] | None = None

        self.__app_type_cache: dict[str, type] = {}
        self.__app_func_cache: dict[str, Callable[..., Any]] = {}

        # assigned on first access to respective properties
        self._user_data_dir: Path | None = None
        self._user_cache_dir: Path | None = None
        self._user_runtime_dir: Path | None = None
        self._user_data_hostname_dir: Path | None = None
        self._user_cache_hostname_dir: Path | None = None

        self._data_cache_dir: Path | None = None
        self._program_cache_dir: Path | None = None

        # enable compact errors by default (if set to False in config, we disable when the
        # config is loaded):
        self._compact_formatter = self.CompactProblemFormatter()
        self._compact_formatter.enable()

    @property
    def ElementActionRun(self) -> type[ElementActionRun]:
        """
        The :class:`ElementActionRun` class.

        :meta private:
        """
        return self._get_app_core_class("ElementActionRun")

    @property
    def ElementAction(self) -> type[ElementAction]:
        """
        The :class:`ElementAction` class.

        :meta private:
        """
        return self._get_app_core_class("ElementAction")

    @property
    def ElementFilter(self) -> type[ElementFilter]:
        """
        The :class:`ElementFilter` class.

        :meta private:
        """
        return self._get_app_core_class("ElementFilter")

    @property
    def ElementGroup(self) -> type[ElementGroup]:
        """
        The :class:`ElementGroup` class.

        :meta private:
        """
        return self._get_app_core_class("ElementGroup")

    @property
    def Environment(self) -> type[_Environment]:
        """
        The :class:`Environment` class.

        :meta private:
        """
        return self._get_app_core_class("Environment")

    @property
    def Executable(self) -> type[_Executable]:
        """
        The :class:`Executable` class.

        :meta private:
        """
        return self._get_app_core_class("Executable")

    @property
    def ExecutableInstance(self) -> type[ExecutableInstance]:
        """
        The :class:`ExecutableInstance` class.

        :meta private:
        """
        return self._get_app_core_class("ExecutableInstance")

    @property
    def NumCores(self) -> type[NumCores]:
        """
        The :class:`NumCores` class.

        :meta private:
        """
        return self._get_app_core_class("NumCores")

    @property
    def ActionEnvironment(self) -> type[ActionEnvironment]:
        """
        The :class:`ActionEnvironment` class.

        :meta private:
        """
        return self._get_app_core_class("ActionEnvironment")

    @property
    def Action(self) -> type[Action]:
        """
        The :class:`Action` class.

        :meta private:
        """
        return self._get_app_core_class("Action")

    @property
    def ActionRule(self) -> type[ActionRule]:
        """
        The :class:`ActionRule` class.

        :meta private:
        """
        return self._get_app_core_class("ActionRule")

    @property
    def ActionScope(self) -> type[ActionScope]:
        """
        The :class:`ActionScope` class.

        :meta private:
        """
        return self._get_app_core_class("ActionScope")

    @property
    def ActionScopeType(self) -> type[ActionScopeType]:
        """
        The :class:`ActionScopeType` class.

        :meta private:
        """
        return self._get_app_core_class("ActionScopeType")

    @property
    def FileSpec(self) -> type[FileSpec]:
        """
        The :class:`FileSpec` class.

        :meta private:
        """
        return self._get_app_core_class("FileSpec")

    @property
    def FileNameSpec(self) -> type[FileNameSpec]:
        """
        The :class:`FileNameSpec` class.

        :meta private:
        """
        return self._get_app_core_class("FileNameSpec")

    @property
    def FileNameStem(self) -> type[FileNameStem]:
        """
        The :class:`FileNameStem` class.

        :meta private:
        """
        return self._get_app_core_class("FileNameStem")

    @property
    def FileNameExt(self) -> type[FileNameExt]:
        """
        The :class:`FileNameExt` class.

        :meta private:
        """
        return self._get_app_core_class("FileNameExt")

    @property
    def OutputFileParser(self) -> type[OutputFileParser]:
        """
        The :class:`OutputFileParser` class.

        :meta private:
        """
        return self._get_app_core_class("OutputFileParser")

    @property
    def InputSource(self) -> type[InputSource]:
        """
        The :class:`InputSource` class.

        :meta private:
        """
        return self._get_app_core_class("InputSource")

    @property
    def InputSourceType(self) -> type[InputSourceType]:
        """
        The :class:`InputSourceType` class.

        :meta private:
        """
        return self._get_app_core_class("InputSourceType")

    @property
    def ValueSequence(self) -> type[ValueSequence]:
        """
        The :class:`ValueSequence` class.

        :meta private:
        """
        return self._get_app_core_class("ValueSequence")

    @property
    def MultiPathSequence(self) -> type[MultiPathSequence]:
        """
        The :class:`MultiPathSequence` class.

        :meta private:
        """
        return self._get_app_core_class("MultiPathSequence")

    @property
    def SchemaInput(self) -> type[SchemaInput]:
        """
        The :class:`SchemaInput` class.

        :meta private:
        """
        return self._get_app_core_class("SchemaInput")

    @property
    def InputFileGenerator(self) -> type[InputFileGenerator]:
        """
        The :class:`InputFileGenerator` class.

        :meta private:
        """
        return self._get_app_core_class("InputFileGenerator")

    @property
    def Command(self) -> type[Command]:
        """
        The :class:`Command` class.

        :meta private:
        """
        return self._get_app_core_class("Command")

    @property
    def ElementInputs(self) -> type[ElementInputs]:
        """
        The :class:`ElementInputs` class.

        :meta private:
        """
        return self._get_app_core_class("ElementInputs")

    @property
    def ElementOutputs(self) -> type[ElementOutputs]:
        """
        The :class:`ElementOutputs` class.

        :meta private:
        """
        return self._get_app_core_class("ElementOutputs")

    @property
    def ElementInputFiles(self) -> type[ElementInputFiles]:
        """
        The :class:`ElementInputFiles` class.

        :meta private:
        """
        return self._get_app_core_class("ElementInputFiles")

    @property
    def ElementOutputFiles(self) -> type[ElementOutputFiles]:
        """
        The :class:`ElementOutputFiles` class.

        :meta private:
        """
        return self._get_app_core_class("ElementOutputFiles")

    @property
    def ElementResources(self) -> type[ElementResources]:
        """
        The :class:`ElementResources` class.

        :meta private:
        """
        return self._get_app_core_class("ElementResources")

    @property
    def ElementIteration(self) -> type[ElementIteration]:
        """
        The :class:`ElementIteration` class.

        :meta private:
        """
        return self._get_app_core_class("ElementIteration")

    @property
    def ElementSet(self) -> type[ElementSet]:
        """
        The :class:`ElementSet` class.

        :meta private:
        """
        return self._get_app_core_class("ElementSet")

    @property
    def Element(self) -> type[Element]:
        """
        The :class:`Element` class.

        :meta private:
        """
        return self._get_app_core_class("Element")

    @property
    def ElementParameter(self) -> type[ElementParameter]:
        """
        The :class:`ElementParameter` class.

        :meta private:
        """
        return self._get_app_core_class("ElementParameter")

    @property
    def Loop(self) -> type[Loop]:
        """
        The :class:`Loop` class.

        :meta private:
        """
        return self._get_app_core_class("Loop")

    @property
    def WorkflowLoop(self) -> type[WorkflowLoop]:
        """
        The :class:`WorkflowLoop` class.

        :meta private:
        """
        return self._get_app_core_class("WorkflowLoop")

    @property
    def CommandFilesList(self) -> type[_CommandFilesList]:
        """
        The :class:`CommandFilesList` class.

        :meta private:
        """
        return self._get_app_core_class("CommandFilesList")

    @property
    def EnvironmentsList(self) -> type[_EnvironmentsList]:
        """
        The :class:`EnvironmentsList` class.

        :meta private:
        """
        return self._get_app_core_class("EnvironmentsList")

    @property
    def ExecutablesList(self) -> type[ExecutablesList]:
        """
        The :class:`ExecutablesList` class.

        :meta private:
        """
        return self._get_app_core_class("ExecutablesList")

    @property
    def GroupList(self) -> type[GroupList]:
        """
        The :class:`GroupList` class.

        :meta private:
        """
        return self._get_app_core_class("GroupList")

    @property
    def ParametersList(self) -> type[_ParametersList]:
        """
        The :class:`ParametersList` class.

        :meta private:
        """
        return self._get_app_core_class("ParametersList")

    @property
    def ResourceList(self) -> type[ResourceList]:
        """
        The :class:`ResourceList` class.

        :meta private:
        """
        return self._get_app_core_class("ResourceList")

    @property
    def ResourceSpec(self) -> type[ResourceSpec]:
        """
        The :class:`ResourceSpec` class.

        :meta private:
        """
        return self._get_app_core_class("ResourceSpec")

    @property
    def TaskList(self) -> type[TaskList]:
        """
        The :class:`TaskList` class.

        :meta private:
        """
        return self._get_app_core_class("TaskList")

    @property
    def TaskSchemasList(self) -> type[_TaskSchemasList]:
        """
        The :class:`TaskSchemasList` class.

        :meta private:
        """
        return self._get_app_core_class("TaskSchemasList")

    @property
    def TaskTemplateList(self) -> type[TaskTemplateList]:
        """
        The :class:`TaskTemplateList` class.

        :meta private:
        """
        return self._get_app_core_class("TaskTemplateList")

    @property
    def WorkflowLoopList(self) -> type[WorkflowLoopList]:
        """
        The :class:`WorkflowLoopList` class.

        :meta private:
        """
        return self._get_app_core_class("WorkflowLoopList")

    @property
    def WorkflowTaskList(self) -> type[WorkflowTaskList]:
        """
        The :class:`WorkflowTaskList` class.

        :meta private:
        """
        return self._get_app_core_class("WorkflowTaskList")

    @property
    def SchemaParameter(self) -> type[SchemaParameter]:
        """
        The :class:`SchemaParameter` class.

        :meta private:
        """
        return self._get_app_core_class("SchemaParameter")

    @property
    def SchemaOutput(self) -> type[SchemaOutput]:
        """
        The :class:`SchemaOutput` class.

        :meta private:
        """
        return self._get_app_core_class("SchemaOutput")

    @property
    def Rule(self) -> type[Rule]:
        """
        The :class:`Rule` class.

        :meta private:
        """
        return self._get_app_core_class("Rule")

    @property
    def RunDirAppFiles(self) -> type[RunDirAppFiles]:
        """
        The :class:`RunDirAppFiles` class.

        :meta private:
        """
        return self._get_app_core_class("RunDirAppFiles")

    @property
    def WorkflowTask(self) -> type[WorkflowTask]:
        """
        The :class:`WorkflowTask` class.

        :meta private:
        """
        return self._get_app_core_class("WorkflowTask")

    @property
    def Parameters(self) -> type[Parameters]:
        """
        The :class:`Parameters` class.

        :meta private:
        """
        return self._get_app_core_class("Parameters")

    @property
    def Parameter(self) -> type[Parameter]:
        """
        The :class:`Parameter` class.

        :meta private:
        """
        return self._get_app_core_class("Parameter")

    @property
    def ParameterValue(self) -> type[ParameterValue]:
        """
        The :class:`ParameterValue` class.

        :meta private:
        """
        return self._get_app_core_class("ParameterValue")

    @property
    def InputValue(self) -> type[InputValue]:
        """
        The :class:`InputValue` class.

        :meta private:
        """
        return self._get_app_core_class("InputValue")

    @property
    def Task(self) -> type[Task]:
        """
        The :class:`Task` class.

        :meta private:
        """
        return self._get_app_core_class("Task")

    @property
    def TaskSchema(self) -> type[TaskSchema]:
        """
        The :class:`TaskSchema` class.

        :meta private:
        """
        return self._get_app_core_class("TaskSchema")

    @property
    def TaskSourceType(self) -> type[TaskSourceType]:
        """
        The :class:`TaskSourceType` class.

        :meta private:
        """
        return self._get_app_core_class("TaskSourceType")

    @property
    def TaskObjective(self) -> type[TaskObjective]:
        """
        The :class:`TaskObjective` class.

        :meta private:
        """
        return self._get_app_core_class("TaskObjective")

    @property
    def TaskInputParameters(self) -> type[TaskInputParameters]:
        """
        The :class:`TaskInputParameters` class.

        :meta private:
        """
        return self._get_app_core_class("TaskInputParameters")

    @property
    def TaskOutputParameters(self) -> type[TaskOutputParameters]:
        """
        The :class:`TaskOutputParameters` class.

        :meta private:
        """
        return self._get_app_core_class("TaskOutputParameters")

    @property
    def ElementPropagation(self) -> type[ElementPropagation]:
        """
        The :class:`ElementPropagation` class.

        :meta private:
        """
        return self._get_app_core_class("ElementPropagation")

    @property
    def WorkflowTemplate(self) -> type[_WorkflowTemplate]:
        """
        The :class:`WorkflowTemplate` class.

        :meta private:
        """
        return self._get_app_core_class("WorkflowTemplate")

    @property
    def Workflow(self) -> type[_Workflow]:
        """
        The :class:`Workflow` class.

        :meta private:
        """
        return self._get_app_core_class("Workflow")

    @property
    def Jobscript(self) -> type[Jobscript]:
        """
        The :class:`Jobscript` class.

        :meta private:
        """
        return self._get_app_core_class("Jobscript")

    @property
    def Submission(self) -> type[_Submission]:
        """
        The :class:`Submission` class.

        :meta private:
        """
        return self._get_app_core_class("Submission")

    @property
    def DirectPosix(self) -> type[DirectPosix]:
        """
        The :class:`DirectPosix` class.

        :meta private:
        """
        return self._get_app_core_class("DirectPosix")

    @property
    def DirectWindows(self) -> type[DirectWindows]:
        """
        The :class:`DirectWindows` class.

        :meta private:
        """
        return self._get_app_core_class("DirectWindows")

    @property
    def SGEPosix(self) -> type[SGEPosix]:
        """
        The :class:`SGEPosix` class.

        :meta private:
        """
        return self._get_app_core_class("SGEPosix")

    @property
    def SlurmPosix(self) -> type[SlurmPosix]:
        """
        The :class:`SlurmPosix` class.

        :meta private:
        """
        return self._get_app_core_class("SlurmPosix")

    @property
    def QueuedScheduler(self) -> type[QueuedScheduler]:
        """
        The :class:`QueuedScheduler` class.

        :meta private:
        """
        return self._get_app_core_class("QueuedScheduler")

    @property
    def CompactProblemFormatter(self) -> type[CompactProblemFormatter]:
        """
        The :class:`CompactProblemFormatter` class.

        :meta private:
        """
        return self._get_app_core_class("CompactProblemFormatter")

    @property
    def make_workflow(self) -> _MakeWorkflow:
        """
        Generate a new workflow from a file or string containing a workflow
        template parametrisation.

        Parameters
        ----------
        template_path_or_str: str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string: bool
            Determines if passing a file path or a string.
        template_format: str
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path: str | Path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name: str
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite: bool
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store: str
            The persistent store type to use.
        ts_fmt: str
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt: str
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs: dict[str, object]
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables: dict[str, str]
            String variables to substitute in `template_file_or_str`.
        status: bool
            If True, display a live status to track workflow creation progress.
        add_submission
            If True, add a submission to the workflow (but do not submit).

        Returns
        -------
        Workflow
            The created workflow, if `add_submission` is `False`.
        Submission
            The created submission object, if `add_submission` is `True`.
        """
        return self.__get_app_func("make_workflow")

    @property
    def make_demo_workflow(self) -> _MakeDemoWorkflow:
        """
        Generate a new workflow from a builtin demo workflow template.

        Parameters
        ----------
        workflow_name: str
            Name of the demo workflow to make.
        template_format: str
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path: str | Path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name: str
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite: bool
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store: str
            The persistent store type to use.
        ts_fmt: str
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt: str
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs: dict[str, object]
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables: dict[str, str]
            String variables to substitute in the demo workflow template file.
        status: bool
            If True, display a live status to track workflow creation progress.
        add_submission
            If True, add a submission to the workflow (but do not submit).

        Returns
        -------
        Workflow
            The created workflow, if `add_submission` is `False`.
        Submission
            The created submission object, if `add_submission` is `True`.
        """
        return self.__get_app_func("make_demo_workflow")

    @property
    def make_and_submit_workflow(self) -> _MakeAndSubmitWorkflow:
        """
        Generate and submit a new workflow from a file or string containing a
        workflow template parametrisation.

        Parameters
        ----------

        template_path_or_str: str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string: str
            Determines whether `template_path_or_str` is a string or a file.
        template_format: str
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path: str | Path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name: str
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite: bool
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store: str
            The persistent store to use for this workflow.
        ts_fmt: str
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt: str
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs: dict[str, object]
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables: dict[str, str]
            String variables to substitute in `template_file_or_str`.
        JS_parallelism: bool
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature. If
            not set, jobscript parallelism will be used if the store type supports it.
        wait: bool
            If True, this command will block until the workflow execution is complete.
        add_to_known: bool
            If True, add the new submission to the known-submissions file, which is
            used by the `show` command to monitor current and recent submissions.
        return_idx: bool
            If True, return a dict representing the jobscript indices submitted for each
            submission.
        tasks: list[int]
            List of task indices to include in this submission. By default all tasks are
            included.
        cancel: bool
            Immediately cancel the submission. Useful for testing and benchmarking.
        status: bool
            If True, display a live status to track workflow creation and submission
            progress.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        Workflow
            The created workflow.
        dict[int, list[int]]
            Mapping of submission handles. If requested by ``return_idx`` parameter.
        """
        return self.__get_app_func("make_and_submit_workflow")

    @property
    def make_and_submit_demo_workflow(self) -> _MakeAndSubmitDemoWorkflow:
        """
        Generate and submit a new demo workflow from a file or string containing a
        workflow template parametrisation.

        Parameters
        ----------
        workflow_name: str
            Name of the demo workflow to make. **Required.**
        template_format: str
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path: str | Path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name: str
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite: bool
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store: str
            The persistent store to use for this workflow.
        ts_fmt: str
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt: str
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        store_kwargs: dict[str, object]
            Keyword arguments to pass to the store's `write_empty_workflow` method.
        variables: dict[str, str]
            String variables to substitute in the demo workflow template file.
        JS_parallelism: bool
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature. If
            not set, jobscript parallelism will be used if the store type supports it.
        wait: bool
            If True, this command will block until the workflow execution is complete.
        add_to_known: bool
            If True, add the new submission to the known-submissions file, which is
            used by the `show` command to monitor current and recent submissions.
        return_idx: bool
            If True, return a dict representing the jobscript indices submitted for each
            submission.
        tasks: list[int]
            List of task indices to include in this submission. By default all tasks are
            included.
        cancel: bool
            Immediately cancel the submission. Useful for testing and benchmarking.
        status: bool
            If True, display a live status to track submission progress.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        Workflow
            The created workflow.
        dict[int, list[int]]
            Mapping of submission handles. If requested by ``return_idx`` parameter.
        """
        return self.__get_app_func("make_and_submit_demo_workflow")

    @property
    def submit_workflow(self) -> _SubmitWorkflow:
        """
        Submit an existing workflow.

        Parameters
        ----------
        workflow_path: str
            Path to an existing workflow
        JS_parallelism: bool
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature. If
            not set, jobscript parallelism will be used if the store type supports it.
        tasks: list[int]
            List of task indices to include in this submission. By default all tasks are
            included.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        dict[int, list[int]]
            Mapping of submission handles. If requested by ``return_idx`` parameter.
        """
        return self.__get_app_func("submit_workflow")

    @property
    def run_hpcflow_tests(self) -> _RunTests:
        """Run hpcflow test suite. This function is only available from derived apps."""
        return self.__get_app_func("run_hpcflow_tests")

    @property
    def run_tests(self) -> _RunTests:
        """Run the test suite."""
        return self.__get_app_func("run_tests")

    @property
    def get_OS_info(self) -> Callable[[], Mapping[str, str]]:
        """
        Get information about the operating system.

        Returns
        -------
        dict[str, str]
            Key-value mapping containing system version information.
        """
        return self.__get_app_func("get_OS_info")

    @property
    def get_shell_info(self) -> Callable[[str, bool], VersionInfo]:
        """
        Get information about a given shell and the operating system.

        Parameters
        ----------
        shell_name: str
            One of the supported shell names.
        exclude_os: bool
            If True, exclude operating system information.

        Returns
        -------
        VersionInfo
            The shell version information descriptor.
        """
        return self.__get_app_func("get_shell_info")

    @property
    def get_known_submissions(self) -> _GetKnownSubmissions:
        """
        Retrieve information about active and recently inactive finished workflows.

        This method removes workflows from the known-submissions file that are found to be
        inactive on this machine (according to the scheduler/process ID).

        Parameters
        ----------
        max_recent: int
            Maximum number of inactive workflows to retrieve.
        no_update: bool
            If True, do not update the known-submissions file to set submissions that are
            now inactive.
        as_json: bool
            If True, only include JSON-compatible information. This will exclude the
            `submission` key, for instance.

        Returns
        -------
        list[KnownSubmissionItem]
            List of descriptions of known items.
        """
        return self.__get_app_func("get_known_submissions")

    @property
    def show(self) -> _Show:
        """
        Show information about running workflows.

        Parameters
        ----------
        max_recent: int
            Maximum number of inactive workflows to show.
        full: bool
            If True, provide more information; output may spans multiple lines for each
            workflow submission.
        no_update: bool
            If True, do not update the known-submissions file to remove workflows that are
            no longer running.
        """
        return self.__get_app_func("show")

    @property
    def show_legend(self) -> Callable[[], None]:
        """
        Output a legend for the jobscript-element and EAR states that are displayed
        by the `show` command.
        """
        return self.__get_app_func("show_legend")

    @property
    def cancel(self) -> _Cancel:
        """
        Cancel the execution of a workflow submission.

        Parameters
        ----------
        workflow_ref: int | str | Path
            Which workflow to cancel, by ID or path.
        ref_is_path: str
            One of "``id``", "``path``" or "``assume-id``" (the default)
        status: bool
            Whether to show a live status during cancel.
        """
        return self.__get_app_func("cancel")

    def __getattr__(self, name: str):
        if name in sdk_classes:
            return self._get_app_core_class(name)
        elif name in sdk_funcs:
            return self.__get_app_func(name)
        else:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r}, version={self.version!r})"

    def _get_app_core_class(self, name: str) -> type:
        if name in self.__app_type_cache:
            return self.__app_type_cache[name]
        obj_mod = import_module(sdk_classes[name])
        cls = getattr(obj_mod, name)
        if issubclass(cls, enum.Enum):
            sub_cls = cls
        else:
            dct: dict[str, Any] = {}
            if hasattr(cls, "_app_attr"):
                dct = {getattr(cls, "_app_attr"): self}
            sub_cls = type(cls.__name__, (cls,), dct)
            if cls.__doc__:
                sub_cls.__doc__ = cls.__doc__.format(app_name=self.name)
        sub_cls.__module__ = self.module
        self.__app_type_cache[name] = sub_cls
        return sub_cls

    def __get_app_func(self, name: str) -> Callable[..., Any]:
        if name in self.__app_func_cache:
            return self.__app_func_cache[name]

        def wrap_func(func) -> Callable[..., Any]:
            # this function avoids scope issues
            return lambda *args, **kwargs: func(*args, **kwargs)

        # retrieve the "private" function:
        sdk_func = getattr(self, f"_{name}")

        func = wrap_func(sdk_func)
        func = wraps(sdk_func)(func)
        if func.__doc__:
            func.__doc__ = func.__doc__.format(app_name=self.name)
        func.__module__ = self.module
        self.__app_func_cache[name] = func
        return func

    @property
    def run_time_info(self) -> RunTimeInfo:
        """
        Information about the runtime.
        """
        return self._run_time_info

    @property
    def log(self) -> AppLog:
        """
        The application log.
        """
        return self._log

    @property
    def timeit(self) -> bool:
        """
        Whether the timing analysis system is active.
        """
        return TimeIt.active

    @timeit.setter
    def timeit(self, value: bool):
        TimeIt.active = bool(value)

    @property
    def template_components(self) -> TemplateComponents:
        """
        The template component data.
        """
        if not self.is_template_components_loaded:
            if BaseApp.__load_pending:
                return {}
            BaseApp.__load_pending = True
            self._load_template_components()
            BaseApp.__load_pending = False
        return self._template_components

    @property
    def _shared_data(self) -> Mapping[str, Any]:
        return cast("Mapping[str, Any]", self.template_components)

    def _ensure_template_component(self, name: str) -> None:
        """Invoked by access to individual template components (e.g. parameters)"""
        if not getattr(self, f"_{name}"):
            self._load_template_components(name)
        else:
            self.logger.debug(f"Template component {name!r} already loaded")

    def load_template_components(self, warn: bool = True) -> None:
        """Load all template component data, warning by default if already loaded."""
        if warn and self.is_template_components_loaded:
            warnings.warn("Template components already loaded; reloading now.")
        self._load_template_components()

    def reload_template_components(self, warn: bool = True) -> None:
        """
        Reload all template component data, warning by default if not already
        loaded.
        """
        if warn and not self.is_template_components_loaded:
            warnings.warn("Template components not loaded; loading now.")
        self._load_template_components()

    @TimeIt.decorator
    def _load_template_components(self, *include: str) -> None:
        """
        Combine any builtin template components with user-defined template components
        and initialise list objects.
        """
        # we mutate builtins (e.g. replace a builtin environment with a user defined one):
        builtins = copy.deepcopy(self._builtin_template_components)

        if not include or "task_schemas" in include:
            # task schemas require all other template components to be loaded first
            include = (
                "parameters",
                "command_files",
                "environments",
                "task_schemas",
                "scripts",
                "jinja_templates",
            )

        self.logger.debug(f"Loading template components: {include!r}.")

        self_tc: Any = self._template_components

        if "parameters" in include:
            params: list[Any] = builtins.get("parameters", [])
            for path in self.config.parameter_sources:
                params.extend(read_YAML_file(path))
            param_list = self.ParametersList.from_json_like(params, shared_data=self_tc)
            self._template_components["parameters"] = param_list
            self._parameters = param_list

        if "command_files" in include:
            cmd_files: list[Any] = builtins.get("command_files", [])
            for path in self.config.command_file_sources:
                cmd_files.extend(read_YAML_file(path))
            cf_list = self.CommandFilesList.from_json_like(cmd_files, shared_data=self_tc)
            self._template_components["command_files"] = cf_list
            self._command_files = cf_list

        if "environments" in include:
            envs = []
            builtin_envs: list[Any] = builtins.get("environments", [])
            env_file_paths: dict[int, Literal["<builtin>"] | Path] = {
                self.Environment.get_id(
                    env["name"], env.get("specifiers")
                ): self.Environment.BUILTIN_ENV_SOURCE
                for env in builtin_envs
            }
            for e_path in self.config.environment_sources:
                for env_j in read_YAML_file(e_path):
                    for b_idx, builtin_env in enumerate(list(builtin_envs)):
                        # overwrite builtin envs with user-supplied:
                        if builtin_env["name"] == env_j["name"]:
                            builtin_envs.pop(b_idx)
                    envs.append(env_j)
                    env_file_paths[
                        self.Environment.get_id(env_j["name"], env_j.get("specifiers"))
                    ] = e_path
            envs = builtin_envs + envs
            env_list = self.EnvironmentsList.from_json_like(envs, shared_data=self_tc)
            env_list._set_source_file_paths(env_file_paths)
            self._template_components["environments"] = env_list
            self._environments = env_list

        if "task_schemas" in include:
            schemas: list[Any] = builtins.get("task_schemas", [])
            for path in self.config.task_schema_sources:
                schemas.extend(read_YAML_file(path))
            ts_list = self.TaskSchemasList.from_json_like(schemas, shared_data=self_tc)
            self._template_components["task_schemas"] = ts_list
            self._task_schemas = ts_list

        if "scripts" in include:
            scripts = self._load_scripts()
            self._template_components["scripts"] = scripts
            self._scripts = scripts

        if "jinja_templates" in include:
            jinja_templates = self._load_jinja_templates()
            self._template_components["jinja_templates"] = jinja_templates
            self._jinja_templates = jinja_templates

        self.logger.info(f"Template components loaded ({include!r}).")

    @classmethod
    def load_builtin_template_component_data(
        cls, package: ModuleType | str
    ) -> BasicTemplateComponents:
        """
        Load the template component data built into the package.
        This is as opposed to the template components defined by users.
        """
        SDK_logger.info(
            f"Loading built-in template component data for package: {package!r}."
        )
        components: BasicTemplateComponents = {}
        for comp_type in TEMPLATE_COMP_TYPES:
            with open_text_resource(package, f"{comp_type}.yaml") as fh:
                SDK_logger.info(f"Parsing file as YAML: {fh.name!r}")
                source = f"from {Path(fh.name)!r}"
                components[comp_type] = read_YAML_str(fh.read(), source=source)

        return components

    @property
    def parameters(self) -> _ParametersList:
        """
        The known template parameters.
        """
        self._ensure_template_component("parameters")
        assert self._parameters is not None
        return self._parameters

    @property
    def command_files(self) -> _CommandFilesList:
        """
        The known template command files.
        """
        self._ensure_template_component("command_files")
        assert self._command_files is not None
        return self._command_files

    @property
    def envs(self) -> _EnvironmentsList:
        """
        The known template execution environments.
        """
        self._ensure_template_component("environments")
        assert self._environments is not None
        return self._environments

    @property
    def scripts(self) -> dict[str, Path]:
        """
        The known template scripts.
        """
        self._ensure_template_component("scripts")
        assert self._scripts is not None
        return self._scripts

    @property
    def jinja_templates(self) -> dict[str, Path]:
        """
        The known Jinja template files.
        """
        self._ensure_template_component("jinja_templates")
        assert self._jinja_templates is not None
        return self._jinja_templates

    @property
    def programs(self) -> dict[str, Path]:
        """
        The known programs.
        """
        self._ensure_template_component("programs")
        assert self._programs is not None
        return self._programs

    @property
    def task_schemas(self) -> _TaskSchemasList:
        """
        The known template task schemas.
        """
        self._ensure_template_component("task_schemas")
        assert self._task_schemas is not None
        return self._task_schemas

    @property
    def logger(self) -> Logger:
        """
        The main underlying logger.
        """
        return self.log.logger

    @property
    def API_logger(self) -> Logger:
        """
        The logger for API messages.
        """
        return self.logger.getChild("api")

    @property
    def CLI_logger(self) -> Logger:
        """
        The logger for CLI messages.
        """
        return self.logger.getChild("cli")

    @property
    def config_logger(self) -> Logger:
        """
        The logger for configuration messages.
        """
        return self.logger.getChild("config")

    @property
    def persistence_logger(self) -> Logger:
        """
        The logger for persistence engine messages.
        """
        return self.logger.getChild("persistence")

    @property
    def submission_logger(self) -> Logger:
        """
        The logger for job submission messages.
        """
        return self.logger.getChild("submission")

    @property
    def runtime_info_logger(self) -> Logger:
        """
        The logger for runtime messages.
        """
        return self.logger.getChild("runtime")

    @property
    def is_config_loaded(self) -> bool:
        """
        Whether the configuration is loaded.
        """
        return bool(self._config)

    @property
    def is_template_components_loaded(self) -> bool:
        """Whether any template component (e.g. parameters) has been loaded."""
        return bool(self._template_components)

    @property
    def config(self) -> Config:
        """
        The configuration.
        """
        if not self.is_config_loaded:
            self.load_config()
        assert self._config
        return self._config

    @property
    def scheduler_lookup(self) -> dict[tuple[str, str], type[Scheduler]]:
        """
        The scheduler mapping.
        """
        return {
            ("direct", "posix"): self.DirectPosix,
            ("direct", "nt"): self.DirectWindows,
            ("sge", "posix"): self.SGEPosix,
            ("slurm", "posix"): self.SlurmPosix,
        }

    def get_scheduler(
        self,
        scheduler_name: str,
        os_name: str,
        scheduler_args: dict[str, Any] | None = None,
    ) -> Scheduler:
        """Get an arbitrary scheduler object."""
        scheduler_kwargs = scheduler_args or {}

        os_name = os_name.lower()
        if os_name == "nt" and "_" in scheduler_name:
            # e.g. WSL on windows uses *_posix
            key = tuple(scheduler_name.split("_"))
            assert len(key) == 2
        else:
            key = (scheduler_name.lower(), os_name)

        try:
            scheduler_cls = self.scheduler_lookup[key]
        except KeyError:
            raise ValueError(
                f"Unsupported combination of scheduler and operation system: {key!r}"
            )
        return scheduler_cls(**scheduler_kwargs)

    def get_OS_supported_schedulers(self) -> Iterator[str]:
        """
        Retrieve a list of schedulers that are supported in principle by this operating
        system.

        This does not necessarily mean all the returned schedulers are available on this
        system.
        """
        for k in self.scheduler_lookup:
            if os.name == "nt" and k == ("direct", "posix"):
                # this is valid for WSL on Windows
                yield "_".join(k)
            elif k[1] == os.name:
                yield k[0]

    def perm_error_retry(self):
        """
        Return a decorator for retrying functions on permission and OS errors that
        might be associated with cloud-storage desktop sync. engine operations.
        """
        return retry(
            (PermissionError, OSError),
            tries=10,
            delay=1,
            backoff=2,
            logger=self.persistence_logger,
        )

    @property
    def user_data_dir(self) -> Path:
        """
        The user's data directory.
        """
        if self._user_data_dir is None:
            self._user_data_dir = Path(user_data_dir(appname=self.package_name))
        return self._user_data_dir

    @property
    def user_cache_dir(self) -> Path:
        """The user's cache directory."""
        if self._user_cache_dir is None:
            self._user_cache_dir = Path(user_cache_path(appname=self.package_name))
        return self._user_cache_dir

    @property
    def user_runtime_dir(self) -> Path:
        """The user's temporary runtime directory."""
        if self._user_runtime_dir is None:
            self._user_runtime_dir = self.user_data_dir.joinpath("temp")
        return self._user_runtime_dir

    @property
    def data_cache_dir(self) -> Path:
        """A directory for demonstration data caching."""
        if self._data_cache_dir is None:
            self._data_cache_dir = self.user_cache_dir.joinpath("data")
        return self._data_cache_dir

    @property
    def program_cache_dir(self) -> Path:
        """A directory for built-in program caching."""
        if self._program_cache_dir is None:
            self._program_cache_dir = self.user_cache_dir.joinpath("programs")
        return self._program_cache_dir

    @property
    def user_data_hostname_dir(self) -> Path:
        """
        The directory for holding user data.

        We segregate by hostname to account for the case where multiple machines might
        use the same shared file system.
        """
        # This might need to cover e.g. multiple login nodes, as described in the
        # config file:
        if self._user_data_hostname_dir is None:
            machine_name = self.config.get("machine")
            self._user_data_hostname_dir = self.user_data_dir.joinpath(machine_name)
        return self._user_data_hostname_dir

    @property
    def user_cache_hostname_dir(self) -> Path:
        """The hostname-scoped app cache directory."""
        if self._user_cache_hostname_dir is None:
            machine_name = self.config.get("machine")
            self._user_cache_hostname_dir = self.user_cache_dir.joinpath(machine_name)
        return self._user_cache_hostname_dir

    def _ensure_user_data_dir(self) -> Path:
        """Ensure a user data directory exists."""
        if not self.user_data_dir.exists():
            self.user_data_dir.mkdir(parents=True)
            self.logger.info(f"Created user data directory: {self.user_data_dir!r}.")
        return self.user_data_dir

    def _ensure_user_runtime_dir(self) -> Path:
        """
        Generate a user runtime directory for this machine in which we can create
        semi-persistent temporary files.

        Note
        ----
        Unlike `_ensure_user_data_dir`, and `_ensure_user_data_hostname_dir`, this
        method is not invoked on config load, because it might need to be created after
        each reboot, and it is not routinely used.
        """
        if not self.user_runtime_dir.exists():
            self.user_runtime_dir.mkdir(parents=True)
            self.logger.info(
                f"Created user runtime directory: {self.user_runtime_dir!r}."
            )
        return self.user_runtime_dir

    def _ensure_user_cache_dir(self) -> Path:
        """Ensure a cache directory exists."""
        if not self.user_cache_dir.exists():
            self.user_cache_dir.mkdir(parents=True)
            self.logger.info(f"Created user cache directory: {self.user_cache_dir!r}.")
        return self.user_cache_dir

    def _ensure_data_cache_dir(self) -> Path:
        """Ensure a cache directory for demonstration data files exists."""
        if not self.data_cache_dir.exists():
            self.data_cache_dir.mkdir(parents=True)
            self.logger.info(
                f"Created demonstration data cache directory: {self.data_cache_dir!r}."
            )
        return self.data_cache_dir

    def _ensure_program_cache_dir(self) -> Path:
        """Ensure a cache directory for built-in programs exists."""
        if not self.program_cache_dir.exists():
            self.program_cache_dir.mkdir(parents=True)
            self.logger.info(
                f"Created built-in program cache directory: "
                f"{self.program_cache_dir!r}."
            )
        return self.program_cache_dir

    def _ensure_user_data_hostname_dir(self) -> Path:
        """
        Ensure a user data directory for this machine exists (used by the helper
        process and the known-submissions file).
        """
        if not self.user_data_hostname_dir.exists():
            self.user_data_hostname_dir.mkdir(parents=True)
            self.logger.info(
                f"Created user data hostname directory: {self.user_data_hostname_dir!r}."
            )
        return self.user_data_hostname_dir

    def _ensure_user_cache_hostname_dir(self) -> Path:
        """Ensure a cache directory exists."""
        if not self.user_cache_hostname_dir.exists():
            self.user_cache_hostname_dir.mkdir(parents=True)
            self.logger.info(
                f"Created hostname-scoped user cache directory: "
                f"{self.user_cache_hostname_dir!r}."
            )
        return self.user_cache_hostname_dir

    def clear_user_runtime_dir(self) -> None:
        """Delete the contents of the user runtime directory."""
        if self.user_runtime_dir.exists():
            shutil.rmtree(self.user_runtime_dir)
            self._ensure_user_runtime_dir()

    def clear_user_cache_dir(self) -> None:
        """Delete the contents of the cache directory."""
        if self.user_cache_dir.exists():
            shutil.rmtree(self.user_cache_dir)
            self._ensure_user_cache_dir()

    def clear_data_cache_dir(self) -> None:
        """Delete the contents of the demonstration data files cache directory."""
        if self.data_cache_dir.exists():
            shutil.rmtree(self.data_cache_dir)
            self._ensure_data_cache_dir()

    def clear_program_cache_dir(self) -> None:
        """Delete the contents of the built-in program cache directory."""
        if self.program_cache_dir.exists():
            shutil.rmtree(self.program_cache_dir)
            self._ensure_program_cache_dir()

    def clear_user_cache_hostname_dir(self) -> None:
        """Delete the contents of the hostname-scoped cache directory."""
        if self.user_cache_hostname_dir.exists():
            shutil.rmtree(self.user_cache_hostname_dir)
            self._ensure_user_cache_hostname_dir()

    @TimeIt.decorator
    def _load_config(
        self, config_dir: PathLike, config_key: str | None, **overrides
    ) -> None:
        self.logger.info("Loading configuration.")
        self._ensure_user_data_dir()
        resolved_config_dir = ConfigFile._resolve_config_dir(
            config_opt=self.config_options,
            logger=self.config_logger,
            directory=config_dir,
        )
        if str(resolved_config_dir) not in self._config_files:
            self._config_files[str(resolved_config_dir)] = ConfigFile(
                directory=resolved_config_dir,
                logger=self.config_logger,
                config_options=self.config_options,
            )
        file = self._config_files[str(resolved_config_dir)]
        self._config = Config(
            app=self,
            config_file=file,
            options=self.config_options,
            config_key=config_key,
            logger=self.config_logger,
            variables={"app_name": self.name, "app_version": self.version},
            **overrides,
        )
        self.log.update_console_level(self.config.get("log_console_level"))
        log_file_path = self.config.get("log_file_path")
        if log_file_path:
            self.log.add_file_logger(
                path=log_file_path,
                level=self.config.get("log_file_level"),
            )
        self.logger.info(f"Configuration loaded from: {self.config.config_file_path}")
        self._ensure_user_data_hostname_dir()

        if self._config.show_tracebacks:
            self.enable_show_tracebacks()
        if self._config.use_rich_tracebacks:
            self.enable_use_rich_tracebacks()

    def load_config(
        self,
        config_dir: PathLike = None,
        config_key: str | None = None,
        warn: bool = True,
        **overrides,
    ) -> None:
        """
        Load the user's configuration.

        Parameters
        ----------
        config_dir:
            Directory containing the configuration, if not default.
        config_key:
            Key to the configuration within the config file.
        warn:
            Whether to warn if a configuration is already loaded.
        """
        if warn and self.is_config_loaded:
            warnings.warn("Configuration is already loaded; reloading.")
        self._load_config(config_dir, config_key, **overrides)

    def unload_config(self) -> None:
        """
        Discard any loaded configuration.
        """
        self._config_files = {}
        self._config = None

    def get_config_path(self, config_dir: PathLike = None) -> Path:
        """Return the full path to the config file, without loading the config."""
        config_dir = ConfigFile._resolve_config_dir(
            config_opt=self.config_options,
            logger=self.logger,
            directory=config_dir,
        )
        return ConfigFile.get_config_file_path(config_dir)

    def _delete_config_file(self, config_dir: PathLike = None) -> None:
        """Delete the config file."""
        config_path = self.get_config_path(config_dir=config_dir)
        self.logger.info(f"deleting config file: {str(config_path)!r}.")
        config_path.unlink()

    def reset_config(
        self,
        config_dir: PathLike = None,
        config_key: str | None = None,
        warn: bool = True,
        **overrides,
    ) -> None:
        """Reset the config file to defaults, and reload the config."""
        self.logger.info("resetting config")
        self._delete_config_file(config_dir=config_dir)
        self._config = None
        self._config_files = {}
        self.load_config(config_dir, config_key, warn=warn, **overrides)

    def reload_config(
        self,
        config_dir: PathLike = None,
        config_key: str | None = None,
        warn: bool = True,
        **overrides,
    ) -> None:
        """
        Reload the configuration. Use if a user has updated the configuration file
        outside the scope of this application.
        """
        if warn and not self.is_config_loaded:
            warnings.warn("Configuration is not loaded; loading.")
        self.log.remove_file_handler()
        self._config_files = {}
        self._load_config(config_dir, config_key, **overrides)

    @TimeIt.decorator
    def __load_builtin_files_from_nested_package(
        self, directory: str | None
    ) -> dict[str, Path]:
        """Discover where the built-in files are (scripts or jinja templates)."""
        # TODO: load custom directories / custom functions (via decorator)

        # must include an `__init__.py` file:
        package = f"{self.package_name}.{directory}"

        out: dict[str, Path] = {}
        if not directory:
            return out
        try:
            with get_file_context(package) as path:
                for dirpath, _, filenames in os.walk(path):
                    dirpath_ = Path(dirpath)
                    if dirpath_.name == "__pycache__":
                        continue
                    for filename in filenames:
                        if filename == "__init__.py":
                            continue
                        val = dirpath_.joinpath(filename)
                        out[val.relative_to(path).as_posix()] = Path(val)
        except ModuleNotFoundError:
            self.logger.exception(f"failed to find built-in files at {package}.")
        SDK_logger.info(f"loaded {len(out)} files from {package}.")
        return out

    @TimeIt.decorator
    def _load_scripts(self) -> dict[str, Path]:
        """
        Discover where the built-in scripts are.
        """
        return self.__load_builtin_files_from_nested_package(self.scripts_dir)

    @TimeIt.decorator
    def _load_jinja_templates(self) -> dict[str, Path]:
        """
        Discover where the built-in Jinja templates are.
        """
        return self.__load_builtin_files_from_nested_package(self.jinja_templates_dir)

    def _get_demo_workflows(self) -> dict[str, Path]:
        """Get all builtin demo workflow template file paths."""
        templates: dict[str, Path] = {}
        pkg = f"{self.package_name}.{self.workflows_dir}"
        for file in resources.files(pkg).iterdir():
            p = Path(str(file))
            if p.exists() and p.suffix in (".yaml", ".yml", ".json", ".jsonc"):
                templates[p.stem] = p
        return templates

    def list_demo_workflows(self) -> tuple[str, ...]:
        """Return a list of demo workflow templates included in the app."""
        return tuple(sorted(self._get_demo_workflows()))

    @contextmanager
    def get_demo_workflow_template_file(
        self, name: str, doc: bool = True, delete: bool = True
    ) -> Iterator[Path]:
        """
        Context manager to get a (temporary) file path to an included demo workflow
        template.

        Parameters
        ----------
        name:
            Name of the builtin demo workflow template whose file path is to be retrieved.
        doc:
            If False, the yielded path will be to a file without the `doc` attribute (if
            originally present).
        delete:
            If True, remove the temporary file on exit.
        """
        tmp_dir = self._ensure_user_runtime_dir()
        builtin_path = self._get_demo_workflows()[name]
        path = tmp_dir / builtin_path.name

        if doc:
            # copy the file to the temp location:
            path.write_text(builtin_path.read_text())
        else:
            # load the file, modify, then dump to temp location:
            if builtin_path.suffix in (".yaml", ".yml"):
                # use round-trip loader to preserve comments:
                data = read_YAML_file(builtin_path, typ="rt", variables={})
                data.pop("doc", None)
                write_YAML_file(data, path, typ="rt")

            elif builtin_path.suffix in (".json", ".jsonc"):
                data = read_JSON_file(builtin_path, variables={})
                data.pop("doc", None)
                write_JSON_file(data, path)

        yield path

        if delete:
            path.unlink()

    def copy_demo_workflow(
        self, name: str, dst: PathLike | None = None, doc: bool = True
    ) -> str:
        """
        Copy a builtin demo workflow to the specified location.

        Parameters
        ----------
        name
            The name of the demo workflow to copy
        dst
            Directory or full file path to copy the demo workflow to. If not specified,
            the current working directory will be used.
        doc
            If False, the copied workflow template file will not include the `doc`
            attribute (if originally present).
        """
        dst = dst or Path(".")
        with self.get_demo_workflow_template_file(name, doc=doc) as src:
            shutil.copy2(src, dst)  # copies metadata, and `dst` can be a dir

        return src.name

    def show_demo_workflow(self, name: str, syntax: bool = True, doc: bool = False):
        """
        Print the contents of a builtin demo workflow template file.

        Parameters
        ----------
        name:
            The name of the demo workflow file to print.
        syntax:
            If True, use rich to syntax-highlight the output.
        doc:
            If False, the printed workflow template file contents will not include the
            `doc` attribute (if originally present).
        """
        with self.get_demo_workflow_template_file(name, doc=doc) as path:
            with path.open("rt") as fp:
                contents = fp.read()

            if syntax:
                fmt = DEMO_WK_FORMATS[path.suffix]
                Console().print(Syntax(contents, fmt))
            else:
                print(contents)

    def load_demo_workflow(
        self, name: str, variables: dict[str, str] | Literal[False] | None = None
    ) -> _WorkflowTemplate:
        """Load a WorkflowTemplate object from a builtin demo template file.

        Parameters
        ----------
        name:
            Name of the demo workflow to load.
        variables:
            String variables to substitute in the demo workflow. Substitutions will be
            attempted if the file looks to contain variable references (like
            "<<var:name>>"). If set to `False`, no substitutions will occur, which may
            result in an invalid workflow template!
        """
        with self.get_demo_workflow_template_file(name) as path:
            return self.WorkflowTemplate.from_file(path, variables=variables)

    def template_components_from_json_like(
        self, json_like: dict[str, dict]
    ) -> TemplateComponents:
        """
        Get template components from a (simply parsed) JSON document.
        """
        tc: TemplateComponents = {}
        sd: Mapping[str, Any] = tc
        tc["parameters"] = self.ParametersList.from_json_like(
            json_like.get("parameters", {}), shared_data=sd, is_hashed=True
        )
        tc["command_files"] = self.CommandFilesList.from_json_like(
            json_like.get("command_files", {}), shared_data=sd, is_hashed=True
        )
        tc["environments"] = self.EnvironmentsList.from_json_like(
            json_like.get("environments", {}), shared_data=sd, is_hashed=True
        )
        tc["task_schemas"] = self.TaskSchemasList.from_json_like(
            json_like.get("task_schemas", {}), shared_data=sd, is_hashed=True
        )
        return tc

    def get_parameter_task_schema_map(self) -> dict[str, list[list[str]]]:
        """
        Get a dict mapping parameter types to task schemas that input/output each
        parameter.
        """
        param_map: dict[str, list[list[str]]] = {}
        for ts in self.task_schemas:
            for inp in ts.inputs:
                if inp.parameter.typ not in param_map:
                    param_map[inp.parameter.typ] = [[], []]
                param_map[inp.parameter.typ][0].append(ts.objective.name)
            for out in ts.outputs:
                if out.parameter.typ not in param_map:
                    param_map[out.parameter.typ] = [[], []]
                param_map[out.parameter.typ][1].append(ts.objective.name)

        return param_map

    def get_info(self) -> dict[str, Any]:
        """
        Get miscellaneous runtime system information.
        """
        return {
            "name": self.name,
            "version": self.version,
            "python_version": self.run_time_info.python_version,
            "is_frozen": self.run_time_info.is_frozen,
        }

    @property
    def known_subs_file_path(self) -> Path:
        """
        The path to the file describing known submissions.
        """
        return self.user_data_hostname_dir / self._known_subs_file_name

    def _format_known_submissions_line(
        self,
        local_id,
        workflow_id,
        submit_time,
        sub_idx,
        is_active,
        wk_path,
        start_time,
        end_time,
    ) -> str:
        line = [
            str(local_id),
            workflow_id,
            str(int(is_active)),
            str(sub_idx),
            submit_time,
            str(wk_path),
            start_time,
            end_time,
        ]
        return self._known_subs_file_sep.join(line) + "\n"

    def _parse_known_submissions_line(self, line: str) -> KnownSubmission:
        (
            local_id,
            workflow_id,
            is_active,
            sub_idx,
            submit_time,
            path_i,
            start_time,
            end_time,
        ) = line.split(self._known_subs_file_sep, maxsplit=7)
        return {
            "local_id": int(local_id),
            "workflow_id": workflow_id,
            "is_active": bool(int(is_active)),
            "sub_idx": int(sub_idx),
            "submit_time": submit_time,
            "path": path_i,
            "start_time": start_time,
            "end_time": end_time.strip(),
        }

    @TimeIt.decorator
    def read_known_submissions_file(self) -> list[KnownSubmission]:
        """Retrieve existing workflows that *might* be running."""
        with self.known_subs_file_path.open("rt", newline="\n") as fh:
            return [self._parse_known_submissions_line(ln) for ln in fh.readlines()]

    def _add_to_known_submissions(
        self,
        wk_path: PathLike,
        wk_id: str,
        sub_idx: int,
        sub_time: str,
    ) -> int:
        """
        Ensure a the specified workflow submission is in the known-submissions file and
        return the associated local ID.
        """
        try:
            known = self.read_known_submissions_file()
        except FileNotFoundError:
            known = []

        wk_path = str(wk_path)
        all_ids = []
        for known_sub in known:
            all_ids.append(known_sub["local_id"])
            if (
                wk_path == known_sub["path"]
                and sub_idx == known_sub["sub_idx"]
                and sub_time == known_sub["submit_time"]
            ):
                # workflow submission part already present
                return known_sub["local_id"]

        # get the next available local ID:
        if all_ids:
            avail = set(range(0, max(all_ids) + 1)).difference(all_ids)
            next_id = min(avail) if avail else max(all_ids) + 1
        else:
            next_id = 0

        run_line = self._format_known_submissions_line(
            local_id=next_id,
            workflow_id=wk_id,
            is_active=True,
            submit_time=sub_time,
            sub_idx=sub_idx,
            wk_path=wk_path,
            start_time="",
            end_time="",
        )
        with self.known_subs_file_path.open("at", newline="\n") as fh:
            # TODO: check wk_path is an absolute path? what about if a remote fsspec path?
            self.submission_logger.info(
                f"adding to known-submissions file workflow path: {wk_path}"
            )
            fh.write(run_line)

        return next_id

    @TimeIt.decorator
    def update_known_subs_file(
        self,
        inactive_IDs: list[int],
        start_times: dict[int, str],
        end_times: dict[int, str],
    ) -> list[int]:
        """
        Update submission records in the known-submission file.

        Note
        ----
        We aim for atomicity to help with the scenario where a new workflow
        submission is adding itself to the file at the same time as we have decided an
        existing workflow should no longer be part of this file. Ideally, such a scenario
        should not arise because both operations should only ever be interactively
        initiated by the single user (`Workflow.submit` and `App.get_known_submissions`).
        If this operation is atomic, then at least the known-submissions file should be
        left in a usable (but inaccurate) state.

        Returns
        -------
        list[int]
            List of local IDs removed from the known-submissions file due to the maximum
            number of recent workflows to store being exceeded.
        """
        self.submission_logger.info(
            f"setting these local IDs to inactive in known-submissions file: "
            f"{inactive_IDs}"
        )

        max_inactive = 10

        # keys are line indices of non-running submissions, values are submission
        # date-times:
        line_date: dict[int, str] = {}

        removed_IDs: list[int] = (
            []
        )  # which submissions we completely remove from the file

        new_lines: list[str] = []
        line_IDs: list[int] = []
        for ln_idx, line in enumerate(self.known_subs_file_path.read_text().split("\n")):
            if not line.strip():
                continue
            item = self._parse_known_submissions_line(line)
            line_IDs.append(item["local_id"])
            shows_as_active = item["is_active"]
            is_inactive = item["local_id"] in inactive_IDs
            start_time = item["start_time"] or start_times.get(item["local_id"], "")
            end_time = item["end_time"] or end_times.get(item["local_id"], "")

            update_inactive = is_inactive and shows_as_active
            update_start = item["local_id"] in start_times
            update_end = item["local_id"] in end_times

            if update_inactive or update_start or update_end:
                updated = self._format_known_submissions_line(
                    local_id=item["local_id"],
                    workflow_id=item["workflow_id"],
                    is_active=not is_inactive,
                    submit_time=item["submit_time"],
                    sub_idx=item["sub_idx"],
                    wk_path=item["path"],
                    start_time=start_time,
                    end_time=end_time,
                )
                new_lines.append(updated)

                self.submission_logger.debug(
                    f"Updating (workflow, submission) from the known-submissions file: "
                    f"{'set to inactive; ' if update_inactive else ''}"
                    f"{f'set start_time: {start_time!r}; ' if update_start else ''}"
                    f"{f'set end_time: {end_time!r}; ' if update_end else ''}"
                    f"({item['path']}, {item['sub_idx']})"
                )
            else:
                # leave this one alone:
                new_lines.append(line + "\n")

            if is_inactive:
                line_date[ln_idx] = item["submit_time"]

        ld_srt_idx = sorted(line_date, key=lambda x: line_date[x])

        if len(line_date) > max_inactive:
            # remove oldest inactive submissions:
            num_remove = len(line_date) - max_inactive
            self.submission_logger.debug(
                f"will remove {num_remove} inactive workflow submissions from the "
                f"known-submissions file because the maximum number of stored inactive "
                f"workflows ({max_inactive}) has been exceeded."
            )

            # sort in reverse so we can remove indices from new_lines:
            for i in sorted(ld_srt_idx[:num_remove], reverse=True):
                new_lines.pop(i)
                removed_IDs.append(line_IDs.pop(i))

        # write the temp file:
        tmp_file = self.known_subs_file_path.with_suffix(
            self.known_subs_file_path.suffix + ".tmp"
        )
        with tmp_file.open("wt", newline="\n") as fh:
            fh.writelines(new_lines + [])

        # hopefully atomic rename:
        os.replace(src=tmp_file, dst=self.known_subs_file_path)
        self.submission_logger.debug("known-submissions file updated")

        return removed_IDs

    def clear_known_submissions_file(self) -> None:
        """
        Clear the known-submissions file of all submissions. This shouldn't be needed
        normally.
        """
        self.submission_logger.warning(
            f"clearing the known-submissions file at {self.known_subs_file_path}"
        )
        with self.known_subs_file_path.open("wt", newline="\n"):
            pass

    @batch_warnings
    def _make_workflow(
        self,
        template_file_or_str: PathLike | str,
        is_string: bool = False,
        template_format: Literal["json", "yaml"] | None = None,
        path: PathLike = None,
        name: str | None = None,
        name_add_timestamp: bool | None = None,
        name_use_dir: bool | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        status: bool = True,
        add_submission: bool = False,
    ) -> _Workflow | _Submission | None:
        """
        Generate a new {app_name} workflow from a file or string containing a workflow
        template parametrisation.

        Parameters
        ----------
        template_path_or_str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string
            Determines if passing a file path or a string.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store type to use.
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
            String variables to substitute in `template_file_or_str`.
        status
            If True, display a live status to track workflow creation progress.
        add_submission
            If True, add a submission to the workflow (but do not submit).

        Returns
        -------
        Workflow
            The created workflow, if `add_submission` is `False`.
        Submission
            The created submission object, if `add_submission` is `True`.
        """
        self.API_logger.info("make_workflow called")

        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            Console().status("Making persistent workflow...") if status else nullcontext()
        )

        with status_context as status_:

            common: MakeWorkflowCommonArgs = {
                "path": str(path) if path else None,
                "name": name,
                "name_add_timestamp": name_add_timestamp,
                "name_use_dir": name_use_dir,
                "overwrite": overwrite,
                "store": store,
                "ts_fmt": ts_fmt,
                "ts_name_fmt": ts_name_fmt,
                "store_kwargs": store_kwargs,
                "variables": variables,
                "status": status_,
            }
            if not is_string:
                wk = self.Workflow.from_file(
                    template_path=template_file_or_str,
                    template_format=template_format,
                    **common,
                )
            elif template_format == "json":
                wk = self.Workflow.from_JSON_string(
                    JSON_str=str(template_file_or_str), **common
                )
            elif template_format == "yaml":
                wk = self.Workflow.from_YAML_string(
                    YAML_str=str(template_file_or_str), **common
                )
            elif not template_format:
                raise ValueError(
                    f"Must specify `template_format` if parsing a workflow template from a "
                    f"string; available options are: {ALL_TEMPLATE_FORMATS!r}."
                )
            else:
                raise ValueError(
                    f"Template format {template_format!r} not understood. Available template "
                    f"formats are {ALL_TEMPLATE_FORMATS!r}."
                )
            if add_submission:
                with wk._store.cached_load(), wk.batch_update():
                    return wk._add_submission(status=status_)

        return wk

    @batch_warnings
    def _make_and_submit_workflow(
        self,
        template_file_or_str: PathLike | str,
        is_string: bool = False,
        template_format: Literal["json", "yaml"] | None = None,
        path: PathLike | None = None,
        name: str | None = None,
        name_add_timestamp: bool | None = None,
        name_use_dir: bool | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
        quiet: bool = False,
    ) -> tuple[_Workflow, Mapping[int, Sequence[int]]] | _Workflow:
        """
        Generate and submit a new {app_name} workflow from a file or string containing a
        workflow template parametrisation.

        Parameters
        ----------

        template_path_or_str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string
            Determines whether `template_path_or_str` is a string or a file.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
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
            String variables to substitute in `template_file_or_str`.
        JS_parallelism
            If True, allow multiple jobscripts to execute simultaneously. If
            'scheduled'/'direct', only allow simultaneous execution of scheduled/direct
            jobscripts. Raises if set to True, 'scheduled', or 'direct', but the store
            type does not support the `jobscript_parallelism` feature. If not set,
            jobscript parallelism will be used if the store type supports it, for
            scheduled jobscripts only.
        wait
            If True, this command will block until the workflow execution is complete.
        add_to_known
            If True, add the new submission to the known-submissions file, which is
            used by the `show` command to monitor current and recent submissions.
        return_idx
            If True, return a dict representing the jobscript indices submitted for each
            submission.
        tasks
            List of task indices to include in this submission. By default all tasks are
            included.
        cancel
            Immediately cancel the submission. Useful for testing and benchmarking.
        status
            If True, display a live status to track workflow creation and submission
            progress.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        Workflow
            The created workflow.
        dict[int, list[int]]
            Mapping of submission handles. If requested by ``return_idx`` parameter.
        """
        self.API_logger.info("make_and_submit_workflow called")

        wk = self._make_workflow(
            template_file_or_str=template_file_or_str,
            is_string=is_string,
            template_format=template_format,
            path=path,
            name=name,
            name_add_timestamp=name_add_timestamp,
            name_use_dir=name_use_dir,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            store_kwargs=store_kwargs,
            variables=variables,
            status=status,
        )
        assert isinstance(wk, _Workflow)
        submitted_js = wk.submit(
            JS_parallelism=JS_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=True,
            tasks=tasks,
            cancel=cancel,
            status=status,
            quiet=quiet,
        )
        if return_idx:
            return (wk, submitted_js)
        else:
            return wk

    def _make_demo_workflow(
        self,
        workflow_name: str,
        template_format: Literal["json", "yaml"] | None = None,
        path: PathLike | None = None,
        name: str | None = None,
        name_add_timestamp: bool | None = None,
        name_use_dir: bool | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        status: bool = True,
        add_submission: bool = False,
    ) -> _Workflow | _Submission | None:
        """
        Generate a new {app_name} workflow from a builtin demo workflow template.

        Parameters
        ----------
        workflow_name
            Name of the demo workflow to make.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store type to use.
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
            String variables to substitute in the demo workflow template file.
        status
            If True, display a live status to track workflow creation progress.
        add_submission
            If True, add a submission to the workflow (but do not submit).

        Returns
        -------
        Workflow
            The created workflow, if `add_submission` is `False`.
        Submission
            The created submission object, if `add_submission` is `True`.
        """
        self.API_logger.info("make_demo_workflow called")

        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            Console().status("Making persistent workflow...") if status else nullcontext()
        )

        with status_context as status_, self.get_demo_workflow_template_file(
            workflow_name
        ) as template_path:
            wk = self.Workflow.from_file(
                template_path=template_path,
                template_format=template_format,
                path=str(path) if path else None,
                name=name,
                name_add_timestamp=name_add_timestamp,
                name_use_dir=name_use_dir,
                overwrite=overwrite,
                store=store,
                ts_fmt=ts_fmt,
                ts_name_fmt=ts_name_fmt,
                store_kwargs=store_kwargs,
                variables=variables,
                status=status_,
            )
            if add_submission:
                with wk._store.cached_load():
                    with wk.batch_update():
                        return wk._add_submission(status=status_)
            return wk

    @batch_warnings
    def _make_and_submit_demo_workflow(
        self,
        workflow_name: str,
        template_format: Literal["json", "yaml"] | None = None,
        path: PathLike | None = None,
        name: str | None = None,
        name_add_timestamp: bool | None = None,
        name_use_dir: bool | None = None,
        overwrite: bool = False,
        store: str = DEFAULT_STORE_FORMAT,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        store_kwargs: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        wait: bool = False,
        add_to_known: bool = True,
        return_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
        quiet: bool = False,
    ) -> tuple[_Workflow, Mapping[int, Sequence[int]]] | _Workflow:
        """
        Generate and submit a new {app_name} workflow from a file or string containing a
        workflow template parametrisation.

        Parameters
        ----------
        workflow_name
            Name of the demo workflow to make.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. If not specified, the
            config item `default_workflow_path` will be used; if that is not set, the
            current directory is used.
        name
            The name to use for the workflow. If not provided, the name will be set to
            that of the template (optionally suffixed by a date-timestamp if
            `name_add_timestamp` is True).
        name_add_timestamp: bool
            If True, suffix the name with a date-timestamp. A default value can be set
            with the config item `workflow_name_add_timestamp`; otherwise set to `True`.
        name_use_dir: bool
            If True, and `name_add_timestamp` is also True, the workflow directory name
            will be just the date-timestamp, and will be contained within a parent
            directory corresponding to the workflow name. A default value can be set
            with the config item `workflow_name_use_dir`; otherwise set to `False`.
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
            String variables to substitute in the demo workflow template file.
        JS_parallelism
            If True, allow multiple jobscripts to execute simultaneously. If
            'scheduled'/'direct', only allow simultaneous execution of scheduled/direct
            jobscripts. Raises if set to True, 'scheduled', or 'direct', but the store
            type does not support the `jobscript_parallelism` feature. If not set,
            jobscript parallelism will be used if the store type supports it, for
            scheduled jobscripts only.
        wait
            If True, this command will block until the workflow execution is complete.
        add_to_known
            If True, add the new submission to the known-submissions file, which is
            used by the `show` command to monitor current and recent submissions.
        return_idx
            If True, return a dict representing the jobscript indices submitted for each
            submission.
        tasks
            List of task indices to include in this submission. By default all tasks are
            included.
        cancel
            Immediately cancel the submission. Useful for testing and benchmarking.
        status
            If True, display a live status to track submission progress.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        Workflow
            The created workflow.
        dict[int, list[int]]
            Mapping of submission handles. If requested by ``return_idx`` parameter.
        """
        self.API_logger.info("make_and_submit_demo_workflow called")

        wk = self._make_demo_workflow(
            workflow_name=workflow_name,
            template_format=template_format,
            path=path,
            name=name,
            name_add_timestamp=name_add_timestamp,
            name_use_dir=name_use_dir,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            store_kwargs=store_kwargs,
            variables=variables,
            status=status,
        )
        assert isinstance(wk, _Workflow)
        submitted_js = wk.submit(
            JS_parallelism=JS_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=True,
            tasks=tasks,
            cancel=cancel,
            status=status,
            quiet=quiet,
        )
        if return_idx:
            return (wk, submitted_js)
        else:
            return wk

    def _submit_workflow(
        self,
        workflow_path: PathLike,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        wait: bool = False,
        return_idx: bool = False,
        tasks: list[int] | None = None,
        quiet: bool = False,
    ) -> Mapping[int, Sequence[int]] | None:
        """
        Submit an existing {app_name} workflow.

        Parameters
        ----------
        workflow_path:
            Path to an existing workflow.
        JS_parallelism:
            If True, allow multiple jobscripts to execute simultaneously. If
            'scheduled'/'direct', only allow simultaneous execution of scheduled/direct
            jobscripts. Raises if set to True, 'scheduled', or 'direct', but the store
            type does not support the `jobscript_parallelism` feature. If not set,
            jobscript parallelism will be used if the store type supports it, for
            scheduled jobscripts only.
        wait:
            Whether to wait for the submission to complete.
        return_idx:
            Whether to return the index information.
        tasks:
            List of task indices to include in this submission. By default all tasks are
            included.
        quiet: bool
            If True, do not print anything about submission.

        Returns
        -------
        dict[int, list[int]]
            Mapping of submission handles, if requested by ``return_idx`` parameter.
        """
        self.API_logger.info("submit_workflow called")
        assert workflow_path is not None
        wk = self.Workflow(workflow_path)
        if return_idx:
            return wk.submit(
                JS_parallelism=JS_parallelism,
                wait=wait,
                return_idx=True,
                tasks=tasks,
                quiet=quiet,
            )
        wk.submit(JS_parallelism=JS_parallelism, wait=wait, tasks=tasks)
        return None

    def _run_hpcflow_tests(
        self,
        test_dirs: Sequence[str | Path] | None = None,
        pytest_args: Sequence[str] | None = None,
    ) -> int:
        """Run hpcflow test suite. This function is only available from derived apps."""
        from hpcflow import app as hf

        return hf.app.run_tests(test_dirs=test_dirs, pytest_args=pytest_args)

    def _run_tests(
        self,
        test_dirs: Sequence[str | Path] | None = None,
        pytest_args: Sequence[str] | None = None,
    ) -> int:
        """Run {app_name} test suite."""
        try:
            import pytest
        except ModuleNotFoundError:
            raise RuntimeError(
                f"{self.name} has not been built with testing dependencies."
            )
        with get_file_context(self.package_name, "tests") as root_test_dir:
            root_dir = str(root_test_dir)
            test_dirs_ = []
            if not test_dirs:
                test_dirs_.append(str(root_test_dir))
            else:
                for dir_i in test_dirs:
                    if not (dir_i_path := Path(dir_i)).is_absolute():
                        # assume relative to the root test dir; this makes it easier to
                        # specify test_dirs when running the Pyinstaller-built executable:
                        test_dirs_.append(str(root_test_dir / dir_i_path))
                    else:
                        test_dirs_.append(str(dir_i_path))

        # note: the `--ignore` is required for Pyinstaller-built "one-file" executables on
        # Windows, where Pytest will try to collect tests from C:\ for some reason.
        # `C:\Documents and Settings` is a hidden/protected compatibility link which
        # we don't have permission to traverse; so without this ignore, Pytest will raise
        # a PermissionError on test collection.
        cmd = [
            "--rootdir",
            root_dir,
            "-p",
            f"{self.package_name}.pytest_plugin",
            "--ignore",
            "C:\\Documents and Settings",
            *(self.pytest_args or ()),
            *(pytest_args or ()),
            *test_dirs_,
        ]
        self.logger.info(f"running Pytest with args: {cmd!r}.")
        return pytest.main(cmd)

    def _get_OS_info(self) -> Mapping[str, str]:
        """Get information about the operating system."""
        os_name = os.name
        if os_name == "posix":
            return get_OS_info_POSIX(
                linux_release_file=self.config.get("linux_release_file")
            )
        elif os_name == "nt":
            return get_OS_info_windows()
        else:
            raise Exception(f"unsupported OS '{os_name}'")

    def _get_shell_info(
        self,
        shell_name: str,
        exclude_os: bool = False,
    ) -> VersionInfo:
        """
        Get information about a given shell and the operating system.

        Parameters
        ----------
        shell_name:
            One of the supported shell names.
        exclude_os:
            If True, exclude operating system information.
        """
        shell = get_shell(
            shell_name=shell_name,
            os_args={"linux_release_file": self.config.linux_release_file},
        )
        return shell.get_version_info(exclude_os)

    @TimeIt.decorator
    def _get_known_submissions(
        self,
        max_recent: int = 3,
        no_update: bool = False,
        as_json: bool = False,
        status: Status | None = None,
    ) -> Sequence[KnownSubmissionItem]:
        """
        Retrieve information about active and recently inactive finished {app_name}
        workflows.

        This method removes workflows from the known-submissions file that are found to be
        inactive on this machine (according to the scheduler/process ID).

        Parameters
        ----------
        max_recent:
            Maximum number of inactive workflows to retrieve.
        no_update:
            If True, do not update the known-submissions file to set submissions that are
            now inactive.
        as_json:
            If True, only include JSON-compatible information. This will exclude the
            `submission` key, for instance.
        """
        out: list[KnownSubmissionItem] = []
        inactive_IDs: list[int] = []
        start_times: dict[int, str] = {}
        end_times: dict[int, str] = {}

        ts_fmt = self._submission_ts_fmt

        try:
            if status:
                status.update("Reading known submissions file...")
            known_subs = self.read_known_submissions_file()
        except FileNotFoundError:
            known_subs = []

        # keys are (workflow path, submission index)
        active_jobscripts: dict[
            tuple[str, int],
            Mapping[int, Mapping[int, Mapping[int, JobscriptElementState]]],
        ] = {}
        loaded_workflows: dict[str, _Workflow] = {}  # keys are workflow path

        # loop in reverse so we process more-recent submissions first:
        for file_dat_i in known_subs[::-1]:
            submit_time_str = file_dat_i["submit_time"]
            submit_time_obj = parse_timestamp(submit_time_str, ts_fmt)

            start_time_str = file_dat_i["start_time"]
            start_time_obj = None
            if start_time_str:
                start_time_obj = parse_timestamp(start_time_str, ts_fmt)

            end_time_str = file_dat_i["end_time"]
            end_time_obj = None
            if end_time_str:
                end_time_obj = parse_timestamp(end_time_str, ts_fmt)

            out_item: KnownSubmissionItem = {
                "local_id": file_dat_i["local_id"],
                "workflow_id": file_dat_i["workflow_id"],
                "workflow_path": file_dat_i["path"],
                "submit_time": submit_time_str,
                "submit_time_obj": submit_time_obj,
                "start_time": start_time_str,
                "start_time_obj": start_time_obj,
                "end_time": end_time_str,
                "end_time_obj": end_time_obj,
                "sub_idx": file_dat_i["sub_idx"],
                "jobscripts": [],
                "active_jobscripts": {},
                "deleted": False,
                "unloadable": False,
            }
            if file_dat_i["path"] in loaded_workflows:
                wk_i = loaded_workflows[file_dat_i["path"]]
            else:
                # might have been moved/archived/deleted:
                path_exists = Path(file_dat_i["path"]).exists()
                out_item["deleted"] = not path_exists
                if path_exists:
                    try:
                        if status:
                            status.update(f"Inspecting workflow {file_dat_i['path']!r}.")
                        wk_i = self.Workflow(file_dat_i["path"])
                    except KeyboardInterrupt:
                        raise
                    except Exception:
                        wk_i = None
                        self.submission_logger.info(
                            f"cannot load workflow from known-submissions file: "
                            f"{file_dat_i['path']!r}!"
                        )
                        out_item["unloadable"] = True
                        if file_dat_i["is_active"]:
                            inactive_IDs.append(file_dat_i["local_id"])
                            file_dat_i["is_active"] = False

                    else:
                        # cache:
                        loaded_workflows[file_dat_i["path"]] = wk_i
                else:
                    wk_i = None

            if wk_i:
                if wk_i.id_ != file_dat_i["workflow_id"]:
                    # overwritten with a new workflow
                    if file_dat_i["is_active"]:
                        inactive_IDs.append(file_dat_i["local_id"])
                    out_item["deleted"] = True

                else:
                    with wk_i._store.cache_ctx():
                        sub = wk_i.submissions[file_dat_i["sub_idx"]]
                        if status:
                            status.update(
                                f"Loading workflow {file_dat_i['path']!r} run metadata..."
                            )
                        sub.use_EARs_cache = True  # pre-cache EARs of this submission

                        all_jobscripts = sub._submission_parts[submit_time_str]
                        out_item["jobscripts"] = all_jobscripts
                        out_item["submission"] = sub
                        if not out_item["start_time"]:
                            start_time_obj = sub.start_time
                            if start_time_obj:
                                start_time = datetime.strftime(start_time_obj, ts_fmt)
                                out_item["start_time"] = start_time
                                start_times[file_dat_i["local_id"]] = start_time
                            out_item["start_time_obj"] = start_time_obj

                        if not out_item["end_time"]:
                            end_time_obj = sub.end_time
                            if end_time_obj:
                                end_time = datetime.strftime(end_time_obj, ts_fmt)
                                out_item["end_time"] = end_time
                                end_times[file_dat_i["local_id"]] = end_time
                            out_item["end_time_obj"] = end_time_obj

                    if file_dat_i["is_active"]:
                        # check it really is active:
                        run_key = (file_dat_i["path"], file_dat_i["sub_idx"])
                        act_i_js: Mapping[
                            int, Mapping[int, Mapping[int, JobscriptElementState]]
                        ]
                        if run_key in active_jobscripts:
                            act_i_js = active_jobscripts[run_key]
                        else:
                            try:
                                if as_json:
                                    act_i_js = cast(  # not actually used?
                                        Any, sub.get_active_jobscripts(as_json=True)
                                    )
                                else:
                                    act_i_js = sub.get_active_jobscripts()
                            except KeyboardInterrupt:
                                raise
                            except Exception:
                                self.submission_logger.info(
                                    f"failed to retrieve active jobscripts from workflow "
                                    f"at: {file_dat_i['path']!r}!"
                                )
                                out_item["unloadable"] = True
                                act_i_js = {}
                            else:
                                active_jobscripts[run_key] = act_i_js

                        out_item["active_jobscripts"] = {
                            k: v for k, v in act_i_js.items() if k in all_jobscripts
                        }
                        if (
                            not out_item["unloadable"]
                            and not act_i_js
                            and file_dat_i["is_active"]
                        ):
                            inactive_IDs.append(file_dat_i["local_id"])

            out.append(out_item)

        if (inactive_IDs or start_times or end_times) and not no_update:
            removed_IDs = self.update_known_subs_file(
                inactive_IDs, start_times, end_times
            )
            # remove these from the output, to avoid confusion (if kept, they would not
            # appear in the next invocation of this method):
            out = [item for item in out if item["local_id"] not in removed_IDs]

        out_active, out_inactive = self.__partition(
            out, lambda item: item["active_jobscripts"]
        )
        # sort inactive by most-recently finished, then deleted:
        out_no_access, out_access = self.__partition(
            out_inactive, lambda item: item["deleted"] or item["unloadable"]
        )

        # sort loadable inactive by end time or start time or submit time:
        out_access = sorted(
            out_access,
            key=lambda i: (
                i["end_time_obj"]
                or i["start_time_obj"]
                or i.get("submit_time_obj")
                or self.__DEF_TIMESTAMP
            ),
            reverse=True,
        )
        out_inactive = (out_access + out_no_access)[:max_recent]

        # show active submissions first:
        out = out_active + out_inactive

        if as_json:
            for item in out:
                item.pop("submission", None)
                item.pop("submit_time_obj")
        return out

    __DEF_TIMESTAMP: Final[datetime] = datetime.fromtimestamp(0, tz=timezone.utc)

    @staticmethod
    def __partition(
        lst: Iterable[T], cond: Callable[[T], Any]
    ) -> tuple[list[T], list[T]]:
        """
        Split a list into two by whether the condition holds for each item.

        Returns
        -------
        true_items
            List of items for which the condition is true (or at least truthy).
        false_items
            List of items for which the condition is false.
        """
        lists: tuple[list[T], list[T]] = [], []
        for item in lst:
            lists[not cond(item)].append(item)
        return lists

    def _show_legend(self) -> None:
        """
        Output a legend for the jobscript-element and EAR states that are displayed
        by the `show` command.
        """
        js_notes = Panel(
            "The [i]Status[/i] column of the `show` command output displays the set of "
            "unique jobscript-element states for that submission. Jobscript element "
            "state meanings are shown below.",
            width=80,
            box=box.SIMPLE,
        )

        js_tab = Table(box=box.SQUARE, title="Jobscript element states")
        js_tab.add_column("Symbol")
        js_tab.add_column("State")
        js_tab.add_column("Description")
        for jse_state in JobscriptElementState.__members__.values():
            js_tab.add_row(jse_state.rich_repr, jse_state.name, jse_state.doc)

        act_notes = Panel(
            "\nThe [i]Actions[/i] column of the `show` command output displays either the "
            "set of unique action states for that submission, or (with the `--full` "
            "option) an action state for each action of the submission. Action state "
            "meanings are shown below.",
            width=80,
            box=box.SIMPLE,
        )

        act_tab = Table(box=box.SQUARE, title="Action states")
        act_tab.add_column("Symbol")
        act_tab.add_column("State")
        act_tab.add_column("Description")
        for ear_state in EARStatus.__members__.values():
            act_tab.add_row(ear_state.rich_repr, ear_state.name, ear_state.doc)

        group = Group(
            js_notes,
            js_tab,
            act_notes,
            act_tab,
        )
        rich_print(group)

    @TimeIt.decorator
    def _show(
        self,
        max_recent: int = 3,
        full: bool = False,
        no_update: bool = False,
    ) -> None:
        """
        Show information about running {app_name} workflows.

        Parameters
        ----------
        max_recent:
            Maximum number of inactive workflows to show.
        full:
            If True, provide more information; output may spans multiple lines for each
            workflow submission.
        no_update:
            If True, do not update the known-submissions file to remove workflows that are
            no longer running.
        """
        # TODO: add --json to show, just returning this but without submissions?

        allowed_cols = {
            "id": "ID",
            "name": "Name",
            "status": "Status",
            "submit_time": "Submit",
            "start_time": "Start",
            "end_time": "End",
            "times": "Times",
            "actions": "Actions",
            "actions_compact": "Actions",
        }

        columns: tuple[str, ...]
        if full:
            columns = ("id", "name", "status", "actions")
        else:
            columns = (
                "id",
                "name",
                "status",
                # "submit_time",
                # "start_time",
                # "end_time",
                "actions_compact",
            )

        unknown_cols = set(columns).difference(allowed_cols)
        if unknown_cols:
            raise ValueError(
                f"Unknown column names: {unknown_cols!r}. Allowed columns are "
                f"{list(allowed_cols)!r}."
            )

        # TODO: add --filter option to filter by ID or name
        # TODO: add --sort option to sort by ID/name/start/end

        ts_fmt = r"%Y-%m-%d %H:%M:%S"
        ts_fmt_part = r"%H:%M:%S"

        console = Console()
        with console.status("Retrieving data...") as status:
            run_dat = self._get_known_submissions(
                max_recent=max_recent,
                no_update=no_update,
                status=status,
            )
            if not run_dat:
                return

            status.update("Formatting...")
            table = Table(box=box.SQUARE, expand=False)
            for col_name in columns:
                table.add_column(allowed_cols[col_name])

            row_pad = 1 if full else 0

            for dat_i in run_dat:
                deleted = dat_i["deleted"]
                unloadable = dat_i["unloadable"]
                no_access = deleted or unloadable
                act_js = dat_i["active_jobscripts"]
                style = "grey42" if (no_access or not act_js) else ""
                style_wk_name = "grey42 strike" if deleted else style
                style_it = "italic grey42" if (no_access or not act_js) else "italic"

                all_cells: dict[str, str | Text | Padding] = {}
                if "status" in columns:
                    if act_js:
                        act_js_states = set(
                            state_i
                            for js_dat in act_js.values()
                            for block_dat in js_dat.values()
                            for state_i in block_dat.values()
                        )
                        all_cells["status"] = "/".join(
                            js_state.rich_repr
                            for js_state in sorted(act_js_states, key=lambda x: x.id)
                        )
                    else:
                        if deleted:
                            txt = "deleted"
                        elif unloadable:
                            txt = "unloadable"
                        else:
                            txt = "inactive"
                        all_cells["status"] = Text(txt, style=style_it)

                if "id" in columns:
                    all_cells["id"] = Text(str(dat_i["local_id"]), style=style)

                if "name" in columns:
                    all_cells["name"] = Text(
                        Path(dat_i["workflow_path"]).name, style=style_wk_name
                    )

                start_time, end_time = None, None
                if not no_access:
                    start_time = cast("datetime", dat_i["start_time_obj"])
                    end_time = cast("datetime", dat_i["end_time_obj"])

                if "actions" in columns:
                    task_tab: str | Table
                    if not no_access:
                        task_tab = Table(box=None, show_header=False)
                        task_tab.add_column()
                        task_tab.add_column()

                        sub = dat_i["submission"]
                        for task_idx, elements in sub.EARs_by_elements.items():
                            task = sub.workflow.tasks[task_idx]

                            # inner table for elements/actions:
                            elem_tab_i = Table(box=None, show_header=False)
                            elem_tab_i.add_column()
                            for elem_idx, EARs in elements.items():
                                elem_status = Text(f"{elem_idx} | ", style=style)
                                iter_idx = 0
                                for ear in EARs:
                                    if ear.element_iteration.index >= iter_idx:
                                        if iter_idx > 0:
                                            elem_status.append("|")
                                        iter_idx += 1
                                    elem_status.append(
                                        ear.status.symbol, style=ear.status.colour
                                    )
                                elem_tab_i.add_row(elem_status)
                            task_tab.add_row(task.unique_name, elem_tab_i, style=style)
                    else:
                        task_tab = ""

                    all_cells["actions"] = Padding(task_tab, (0, 0, row_pad, 0))

                if "actions_compact" in columns:
                    if not no_access:
                        EAR_stat_count = Counter(
                            ear.status
                            for elements in dat_i["submission"].EARs_by_elements.values()
                            for EARs in elements.values()
                            for ear in EARs
                        )
                        all_cells["actions_compact"] = " | ".join(
                            f"[{k.colour}]{k.symbol}[/{k.colour}]:{v}"  # type: ignore
                            for k, v in dict(
                                sorted(EAR_stat_count.items(), key=lambda x: x[0].id)
                            ).items()
                        )
                    else:
                        all_cells["actions_compact"] = ""

                if "submit_time" in columns or "times" in columns:
                    submit_time = parse_timestamp(
                        dat_i["submit_time"], self._submission_ts_fmt
                    )
                    submit_time_full = submit_time.strftime(ts_fmt)

                if "start_time" in columns or "times" in columns:
                    start_time_full = start_time.strftime(ts_fmt) if start_time else "-"
                    start_time_part = start_time_full
                    if start_time and start_time.date() == submit_time.date():
                        start_time_part = start_time.strftime(ts_fmt_part)

                if "end_time" in columns or "times" in columns:
                    end_time_full = end_time.strftime(ts_fmt) if end_time else "-"
                    end_time_part = end_time_full
                    if end_time and start_time and end_time.date() == start_time.date():
                        end_time_part = end_time.strftime(ts_fmt_part)

                if "submit_time" in columns:
                    all_cells["submit_time"] = Padding(
                        Text(submit_time_full, style=style), (0, 0, row_pad, 0)
                    )

                if "start_time" in columns:
                    all_cells["start_time"] = Padding(
                        Text(start_time_part, style=style), (0, 0, row_pad, 0)
                    )

                if "end_time" in columns:
                    all_cells["end_time"] = Padding(
                        Text(end_time_part, style=style), (0, 0, row_pad, 0)
                    )

                if "times" in columns:
                    # submit/start/end on separate lines:
                    times_tab = Table(box=None, show_header=False)
                    times_tab.add_column()
                    times_tab.add_column(justify="right")

                    times_tab.add_row(
                        Text("sb.", style=style_it), Text(submit_time_full, style=style)
                    )

                    if start_time:
                        times_tab.add_row(
                            Text("st.", style=style_it),
                            Text(start_time_part, style=style),
                        )
                    if end_time:
                        times_tab.add_row(
                            Text("en.", style=style_it), Text(end_time_part, style=style)
                        )

                    all_cells["times"] = Padding(times_tab, (0, 0, row_pad, 0))

                table.add_row(*(all_cells[col_name] for col_name in columns))

        if table.row_count:
            console.print(table)

    def _get_workflow_path_from_local_ID(self, local_ID: int) -> Path:
        try:
            known_subs = self.read_known_submissions_file()
        except FileNotFoundError:
            known_subs = []

        if any((witness := sub)["local_id"] == local_ID for sub in known_subs):
            return Path(witness["path"])
        raise ValueError(f"Specified local ID is not valid: {local_ID}.")

    def _resolve_workflow_reference(
        self, workflow_ref: str, ref_type: str | None
    ) -> Path:
        path = None
        if ref_type == "path":
            path = Path(workflow_ref)

        elif ref_type == "id":
            local_ID = int(workflow_ref)
            path = self._get_workflow_path_from_local_ID(local_ID)

        elif ref_type in ("assume-id", None):
            # see if reference is a valid local ID:
            is_local_ID = True
            try:
                local_ID = int(workflow_ref)
            except ValueError:
                is_local_ID = False
            else:
                try:
                    path = self._get_workflow_path_from_local_ID(local_ID)
                except ValueError:
                    is_local_ID = False

        if path is None:
            # see if reference is a valid path:
            is_path = True
            path = Path(workflow_ref)
            if not path.exists():
                is_path = False

            if is_path and is_local_ID:
                raise ValueError(
                    "Workflow reference appears to be both a valid path and a valid "
                    "local ID; set `ref_is_path` to True or False to disambiguate: "
                    f"{workflow_ref}."
                )
            elif not is_path and not is_local_ID:
                raise ValueError(
                    "Workflow reference appears to be neither a valid path or a valid "
                    f"local ID: {workflow_ref}."
                )
        return path.resolve()

    def _cancel(
        self,
        workflow_ref: int | str | Path,
        ref_is_path: str | None = None,
        status: bool = True,
    ) -> None:
        """
        Cancel the execution of a workflow submission.

        Parameters
        ----------
        workflow_ref: int | str | Path
            Which workflow to cancel, by ID or path.
        ref_is_path: str
            One of "``id``", "``path``" or "``assume-id``" (the default)
        status: bool
            Whether to show a live status during cancel.
        """
        path = self._resolve_workflow_reference(str(workflow_ref), ref_is_path)
        self.Workflow(path).cancel(status=status)

    @staticmethod
    def redirect_std_to_file(*args, **kwargs):
        return redirect_std_to_file_hpcflow(*args, **kwargs)

    def __update_env_source_file(
        self, env_file: Path, new_contents: list[JSONDocument], typ: str = "safe"
    ):
        """
        Update the contents of the specified environments sources file (e.g. when adding
        or removing environments).
        """
        overwrite_YAML_file(
            env_file,
            new_contents,
            description="environment sources",
            logger=self.logger,
            typ=typ,
        )

    @staticmethod
    def detect_env_manager() -> EnvInfo:
        mamba_exe = os.environ.get("MAMBA_EXE")
        conda_exe = os.environ.get("CONDA_EXE")

        manager = None

        if mamba_exe and conda_exe:
            manager = "mamba"
            exe = mamba_exe
        elif mamba_exe:
            # micromamba sets only MAMBA_EXE
            manager = "micromamba"
            exe = mamba_exe
        elif conda_exe:
            manager = "conda"
            exe = conda_exe
        else:
            raise ValueError("Cannot detect environment manager.")

        prefix = os.environ.get("CONDA_PREFIX")
        return EnvInfo(manager=manager, exe=exe, prefix=prefix)

    def get_shell_hook(self, shell: str, env_info: EnvInfo) -> str:
        """Get the shell hook string that, when executed, will initialise micromamba, mamba,
        or conda using the executable by which an environment is currently activated.

        """
        exe = env_info.exe
        SHELL_HOOKS = {
            "micromamba": {
                "bash": f'eval "$({exe} shell hook -s posix)"',
                "powershell": f"& '{exe}' shell hook -s powershell | Out-String | Invoke-Expression",
            },
            "conda": {
                "bash": f'eval "$({exe} shell.bash hook)"',
                "powershell": f"& '{exe}' shell.powershell hook | Out-String | Invoke-Expression",
            },
            "mamba": {
                "bash": f'eval "$({exe} shell.bash hook)"',
                "powershell": f"& '{exe}' shell.powershell hook | Out-String | Invoke-Expression",
            },
        }
        return SHELL_HOOKS[env_info.manager][shell]

    def get_env_setup(self, shell: str) -> list[str]:
        """Generate shell commands to activate the current conda-like or Python venv
        environment."""

        if (rti := self.run_time_info).is_venv:
            ENV_ACTIVATE = {
                "bash": f"source {rti.venv_path}/bin/activate",
                "powershell": f"{rti.venv_path}\\Scripts\\activate.ps1",
            }
            return [ENV_ACTIVATE[shell]]

        elif rti.is_conda_venv:
            env_info = self.detect_env_manager()
            shell_hook = self.get_shell_hook(shell, env_info)
            # note: the shell hook defines a shell function named e.g. `micromamba`, so
            # the shell hook won't work if we just try to use the executable to e.g.
            # activate an environment; it must be this function!
            ENV_ACTIVATE = {
                "bash": f"{env_info.manager} activate {env_info.prefix}",
                "powershell": f"{env_info.manager} activate {env_info.prefix}",
            }
            return [shell_hook, ENV_ACTIVATE[shell]]
        else:
            raise ValueError("Not in a venv or conda-like environment!")

    def add_env(
        self,
        name: str,
        setup: str | Sequence[str] | None = None,
        executables: list[_Executable] | None = None,
        use_current: bool = False,
        env_source_file: Path | None = None,
        file_name: str = Environment_cls.DEFAULT_CONFIGURED_ENVS_FILE,
        replace: bool = False,
    ) -> Path:
        """
        Generate and save a new environment.


        Parameters
        ----------
        name:
            The name of the new environment.
        setup:
            Setup commands to be invoked when using the environment.
        executables:
            Executables that the environment provides.
        use_current:
            Use the currently activate Python environment to provide a `python_script`
            executable within the environment. False by default.
        env_source_file:
            The environment source file to save the environment to, if specified.
        file_name:
            If `env_source_file` is not specified, the file name of the environment source
            file to use within the app configuration directory.
        replace
            If True, replace an existing environment with the same name and specifiers
            with the new one. If False and an existing environment exists, an exception
            will be raised.

        Returns
        -------
        path:
            The path to the file in which the new environment definition was saved.

        """
        shell = DEFAULT_SHELL_NAMES[os.name]
        setup = self.get_env_setup(shell) if use_current else setup
        env = self.Environment(name=name, setup=setup, executables=executables)
        return self.save_env(
            env, env_source_file=env_source_file, file_name=file_name, replace=replace
        )

    def __get_envs(
        self,
        id: int | list[int] | None = None,
        *,
        name: str | None = None,
        specifiers: Mapping[str, Any] | None = None,
        label: str | None = None,
    ) -> list[_Environment]:
        """
        Retrieve Environment objects using either a local ID (index within the app `envs`
        list, sorted by name and then specifiers), a name, or a label (and potentially,
        specifiers).

        Multiple environments may be returned only if `label` is specified.
        """
        specifiers = specifiers or {}
        if sum(arg is not None for arg in (id, name, label)) != 1:
            raise ValueError(
                "Specify either `id`, `label` or `name` (and optionally, `specifiers`)."
            )
        if id is not None and specifiers:
            raise ValueError(
                "Cannot use `specifiers` to filter environments if an ID was provided."
            )
        if id is not None:
            if isinstance(id, int):
                id = [id]
            env_srt = sorted(self.envs)
            try:
                return [get_with_index(env_srt, id_i) for id_i in id]
            except StoredIndexError as exc:
                raise EnvironmentNotFound(self, id=exc.index)
        if name is not None:
            try:
                return [self.envs.get(name=name, **specifiers)]
            except ValueError:
                raise EnvironmentNotFound(self, name=name, specifiers=specifiers)
        return [
            env
            for env in self.envs
            if env.setup_label == label and env.specifiers == specifiers
        ]

    def _remove_envs_from_files(self, envs: list[_Environment]) -> list[Path]:
        """
        Remove the specified environments from their source files.
        """
        # group by source file and store ID hashes:
        env_IDs_by_file = defaultdict(list)
        for env in envs:
            if isinstance(source_file := env.source_file, Path):
                env_IDs_by_file[source_file].append(env.id)
                self.logger.debug(
                    f"Will remove environment {env.name!r} with hash ID {env.id!r} from "
                    f"file {source_file!r}."
                )
            elif source_file == self.Environment.BUILTIN_ENV_SOURCE:
                raise CannotRemoveBuiltinEnvironment(env)

        # rewrite source files
        for file, remove_IDs in env_IDs_by_file.items():
            env_data = read_YAML_file(file, typ="rt")
            env_list = self.EnvironmentsList.from_json_like(
                env_data, shared_data=self._shared_data
            )
            new_env_data = [
                env_i.to_json_like(exclude={"_hash_value"})[0]
                for env_i in env_list
                if env_i.id not in remove_IDs
            ]
            self.__update_env_source_file(file, new_env_data, typ="rt")
            self.logger.debug(
                f"Removed environments with hash IDs {remove_IDs!r} from file "
                f"{source_file!r}."
            )

        return list(env_IDs_by_file)

    def remove_env(
        self,
        id: int | list[int] | None = None,
        name: str | None = None,
        label: str | None = None,
        specifiers: Mapping[str, Any] | None = None,
    ):
        """
        Remove an environment identified by its name and specifiers, or remove all
        environments with a particular setup label and specifiers.

        """
        envs = self.__get_envs(id=id, name=name, label=label, specifiers=specifiers)
        updated = self._remove_envs_from_files(envs)
        num_envs = len(envs)
        print(
            f"Removed {num_envs} environment definition{'s' if num_envs > 1 else ''} "
            f"from file{'s' if len(updated) > 1 else ''}: "
            f"{', '.join(str(path) for path in updated)}."
        )

    def save_env(
        self,
        env: _Environment,
        env_source_file: Path | None = None,
        file_name: str = Environment_cls.DEFAULT_CONFIGURED_ENVS_FILE,
        replace: bool = False,
    ) -> Path:
        """
        Save an environment to the environment definitions file.

        Parameters
        ----------
        env:
            The new environment to save to the app environments list.
        env_source_file:
            The environment source file to save the environment to, if specified.
        file_name:
            If `env_source_file` is not specified, the file name of the environment source
            file to use within the app configuration directory.
        replace
            If True, replace an existing environment with the same name and specifiers
            with the new one. If False and an existing environment exists, an exception
            will be raised.

        Returns
        -------
        env_source:
            The file path the environment was added to.

        """
        env_source = env_source_file or self.config.get("config_directory").joinpath(
            file_name
        )
        if env in self.envs:
            if replace:
                print(f"Replacing existing environment: {env.name} {env.specs_fmt}.")
                self.remove_env(name=env.name, specifiers=env.specifiers)
            else:
                raise EnvironmentAlreadyExists(env)
        assert isinstance(env_source, Path)
        new_env_dat = env.to_json_like(exclude={"_hash_value"})[0]
        if env_source.exists():
            existing_env_dat: list[dict] = read_YAML_file(env_source, typ="rt")
            all_env_dat = [*existing_env_dat, new_env_dat]
            self.__update_env_source_file(env_source, all_env_dat, typ="rt")
        else:
            all_env_dat = [new_env_dat]
            write_YAML_file(all_env_dat, env_source, typ="rt")

        cur_env_source_files = self.config.get("environment_sources")
        if env_source not in cur_env_source_files:
            self.config.append("environment_sources", str(env_source))
            self.config.save()

        print(f"Saved a new environment: {env.name!r} to file: {env_source}.")
        return env_source

    def env_configure_python(
        self,
        shell: Literal["bash", "powershell"] | None = None,
        setup: str | list[str] | None = None,
        names: list[str] | None = None,
        use_current: bool = True,
        save: bool = False,
        env_source_file: Path | None = None,
        file_name: str = Environment_cls.DEFAULT_CONFIGURED_ENVS_FILE,
        replace: bool = False,
    ) -> list[_Environment]:
        """Configure app environments that use Python.

        Parameters
        ----------
        names:
            If specified, also set up these named environments using the same Python
            executable and setup, otherwise just set up the `python_env` environment. This
            should be a list of strings without the "_env" prefix, which will be added.
        use_current:
            Use the currently activate Python environment to provide a `python_script`
            executable within the environment. True by default.
        save:
            If True, save the environment to a persistent environment definitions file.
        env_source_file:
            Applicable only if `save` is True. The environment source file to save the
            environment to, if specified.
        file_name:
            Applicable only if `save` is True. If `env_source_file` is not specified, the
            file name of the environment source file to use within the app configuration
            directory.
        replace
            Applicable only if `save` is True. If True, replace an existing environment
            with the same name and specifiers with the new one. If False and an existing
            environment exists, an exception will be raised.
        """
        shell = shell or DEFAULT_SHELL_NAMES[os.name]
        setup = norm_env_setup(setup)
        executables = [
            self.Executable(
                label="python_script",
                instances=[
                    self.ExecutableInstance(
                        command=(f'{get_env_py_exe(shell)} "<<script_path>>" <<args>>'),
                        num_cores=1,
                        parallel_mode=None,
                    ),
                ],
            ),
        ]
        setup = self.get_env_setup(shell) if use_current else setup
        environments = []
        for name in sorted(set(["python", *(names if names else [])])):
            environments.append(
                self.Environment(
                    name=f"{name}_env",
                    setup=setup,
                    executables=executables,
                    setup_label="python",
                )
            )
        if save:
            for env in environments:
                self.save_env(
                    env,
                    env_source_file=env_source_file,
                    file_name=file_name,
                    replace=replace,
                )
        return environments

    def _get_envs_table(self, include_source: bool = True) -> Table:
        """Generate a Rich table that shows details of all environments."""
        headers = ["ID", "Name", "Specifiers", "Label"]
        if include_source:
            headers.append("Source")
        tab = Table(*headers, box=box.SIMPLE)
        for env_idx, env in enumerate(sorted(self.envs)):
            for row_idx, (key, val) in enumerate((env.specifiers or {"": None}).items()):
                spec_col = f"{key}: {val}" if key != "" else "-"
                lab_col = (env.setup_label or "-") if row_idx == 0 else ""
                src_col = str(env.source_file) or "-" if row_idx == 0 else ""
                cols = [
                    str(env_idx),
                    env.name if row_idx == 0 else "",
                    spec_col,
                    lab_col,
                ]
                if include_source:
                    cols.append(src_col)
                tab.add_row(*cols)
        return tab

    def print_envs(self) -> None:
        """
        Print a table of available app environments.
        """
        Console().print(self._get_envs_table(include_source=True))

    def show_env(
        self,
        id: int | list[int] | None = None,
        name: str | None = None,
        label: str | None = None,
        specifiers: dict[str, Any] | None = None,
    ):
        """
        Print one or more environment definitions.
        """
        panels = []
        for env in self.__get_envs(id=id, name=name, label=label, specifiers=specifiers):
            execs = env.executables
            tab = Table(show_header=False, box=None)
            tab.add_column()
            tab.add_column()
            tab.add_row("name", Pretty(env.name))
            tab.add_row("specifiers", Pretty(env.specifiers))
            tab.add_row("label", Pretty(env.setup_label))
            tab.add_row("source", str(env.source_file))
            tab.add_row("setup", "\n".join(env.setup or []) or "-")
            tab.add_row("executables:", "-" if not execs else "")
            if execs:
                tab.add_row("", "")
                exec_i_tab = Table(show_header=False, box=None, padding=(0, 0, 0, 1))
                exec_i_tab.add_column()
                for exec_i in execs:
                    inst_tab_j = Table(show_header=False, box=None, padding=(0, 0, 0, 2))
                    inst_tab_j.add_column()
                    inst_tab_j.add_column()
                    for inst in exec_i.instances:
                        inst_tab_j.add_row("parallel_mode", Pretty(inst.parallel_mode))
                        inst_tab_j.add_row("num_cores", Pretty(inst.num_cores.to_dict()))
                        inst_tab_j.add_row("command", inst.command)
                        inst_tab_j.add_row("", "")

                    exec_i_tab.add_row(f"[u]{exec_i.label}[/u]")
                    exec_i_tab.add_row(inst_tab_j)

            panels.append(
                Panel(
                    title=f"Environment: {env.name!r}",
                    renderable=Group(tab, exec_i_tab) if execs else tab,
                )
            )
        Console().print(Group(*panels))

    def get_env_info(
        self,
        attribute: str,
        id: int | list[int] | None = None,
        name: str | None = None,
        label: str | None = None,
        specifiers: dict[str, Any] | None = None,
    ) -> tuple[Any]:
        """
        Retrieve the value of a particular attribute for one or more environments.
        """
        out = []
        for env in self.__get_envs(id=id, name=name, label=label, specifiers=specifiers):
            out.append(getattr(env, attribute))
        return tuple(out)

    def get_data_manifest(
        self, data_type: Literal["data", "program"]
    ) -> dict[str, dict[str, str]]:
        """
        Get a key-sorted dict whose keys are example data file or program names and whose
        values are the source files if the source file required unzipping or `None`
        otherwise.

        If the config items `data_manifest_file`/`program_manifest_file` is set, this is
        used as the manifest file path. Otherwise, the app attribute `data_manifest_dir`
        is used, and is expected to be a (fsspec-compatible) URL to a directory that
        contains `data.json` and `programs.json` manifest files.

        """
        CONFIG_LOOKUP = {
            "data": "data_manifest_file",
            "program": "program_manifest_file",
        }
        try:
            config_key = CONFIG_LOOKUP[data_type]
        except KeyError:
            raise ValueError(
                f"`data_type` must be 'data' or 'program', but received {data_type}."
            )

        if config_attr := self.config.get(config_key):
            self.logger.debug(
                f"loading {data_type} files manifest from the config item `{config_key}` "
                f"with value: {config_attr!r}."
            )
            fs, url_path = rate_limit_safe_url_to_fs(
                self, str(config_attr), logger=self.logger
            )
            with fs.open(url_path) as fh:
                return dict(sorted(json.load(fh).items()))
        else:
            self.logger.debug(
                f"loading {data_type} files manifest from the directory defined by the app "
                f"attribute `data_manifest_dir` with value: {self.data_manifest_dir!r}."
            )
        if (package := self.data_manifest_dir) is None:
            self.logger.warning("`data_manifest_dir` is not defined.")
            return {}

        MANIFEST_LOOKUP = {
            "data": "data.json",
            "program": "programs.json",
        }
        with open_text_resource(package, MANIFEST_LOOKUP[data_type]) as fh:
            return dict(sorted(json.load(fh).items()))

    def get_data_files_manifest(self) -> dict[str, dict[str, str]]:
        """
        Get the demonstration data files manifest.
        """
        return self.get_data_manifest("data")

    def get_programs_manifest(self) -> dict[str, dict[str, str]]:
        """
        Get the built-in programs manifest.
        """
        return self.get_data_manifest("program")

    def __validate_cacheable_file_spec(
        self, data_type: Literal["data", "program"], file_key: str, spec: dict[str, str]
    ) -> dict[str, str]:
        """
        Validate the spec dictionary of a cacheable file (demo data or program), as found
        within the respective manifest JSON file.

        Note the distinction between the "zip_file" and "zip_contents" keys, both of which
        point to the location of a zip file within the data or program directory. The use
        of the "zip_file" key means the uncompressed zip file will be associated with the
        spec's key directly (meaning it must contain only a single file/directory). On the
        other hand, the "zip_contents" key indicates that the contents of the uncompressed
        zip file will be placed under the key (meaning it can contain one or more items).

        """
        ALLOWED = {
            "data": {"zip_file", "zip_contents"},
            "program": {"zip_file", "zip_contents", "executable", "set_executable"},
        }
        REQ = {"data": set(), "program": {"executable"}}

        allowed = ALLOWED[data_type]
        required = REQ[data_type]
        spec_keys = set(spec)

        if missing := required - spec_keys:
            raise ValueError(
                f"Manifest spec for {data_type} {file_key!r} is missing keys: "
                f"{missing!r}."
            )
        if extra := spec_keys - allowed:
            raise ValueError(
                f"Manifest spec for {data_type} {file_key!r} has unknown keys: "
                f"{extra!r}. Allowed keys are {allowed!r}."
            )
        return spec

    def __set_executable(
        self, data_type: Literal["data", "program"], spec: dict[str, str], path: Path
    ):
        """Set the executable bit on the specified glob-matched paths, if required."""

        if data_type == "program":
            self.logger.debug(f"set executable called on path: {path!r}.")
            for pattern in spec.get("set_executable", []):
                self.logger.debug(f"pattern is: {pattern!r}.")
                for match in list(path.glob(pattern)):
                    if os.name == "posix":
                        self.logger.debug(f"setting the executable bit on: {match!r}.")
                        match.chmod(match.stat().st_mode | stat.S_IEXEC)

    def __get_fsspec_filesystem_and_url_path(
        self, data_type: Literal["data", "program"]
    ) -> tuple[Any, Any]:
        """Retrieve the fsspec filesystem object and URL path from an fsspec-compatible
        URL."""

        DIR_ATTR_LOOKUP = {
            "data": "data_dir",
            "program": "program_dir",
        }
        try:
            dir_key = DIR_ATTR_LOOKUP[data_type]
        except KeyError:
            raise ValueError(
                f"`data_type` must be 'data' or 'program', but received {data_type}."
            )
        # first try the config attribute; if not set, use the app defined attribute:
        msg_attr = "config"
        if not (url := self.config.get(dir_key)):
            url = getattr(self, dir_key)
            msg_attr = "app"

        self.logger.debug(
            f"using {msg_attr} attribute {dir_key!r} to locate file(s) to cache."
        )
        self.logger.debug(f"retrieving fsspec filesystem instance from URL: {url!r} ")
        return rate_limit_safe_url_to_fs(self, url, logger=self.logger)

    def cache_file(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        manifest: dict[str, dict[str, str]] | None = None,
        exist_ok: bool = True,
        fs_and_url_path: tuple[Any, Any] | None = None,
    ) -> Path:
        """
        Cache and retrieve the path to a data file (demo data or built-in program),
        according to the config or app attributes `data_dir` or `program_dir` (which
        are fsspec-compatible URLs), and a file key that represents the relative path of
        the file within the respective data directory.

        For remote file systems, this will involve downloading the file to a
        temporary directory. For files that live within a zip file, as specified in the
        manifest, this method will first unzip the files before moving to the cache
        directory.
        """

        ENSURE_LOOKUP = {
            "data": self._ensure_data_cache_dir,
            "program": self._ensure_program_cache_dir,
        }
        manifest = manifest or self.get_data_manifest(data_type)
        if file_key not in manifest:
            raise ValueError(f"No such {data_type} file {file_key!r}.")

        spec = self.__validate_cacheable_file_spec(
            data_type, file_key, manifest[file_key]
        )
        req_unpack = bool(zip_path := (spec.get("zip_file") or spec.get("zip_contents")))
        src_fn = zip_path or file_key

        cache_base = ENSURE_LOOKUP[data_type]()
        cache_file_path = cache_base.joinpath(file_key)
        cache_file_path.parent.mkdir(parents=True, exist_ok=True)
        if cache_file_path.exists():
            if not exist_ok:
                raise ValueError(
                    f"Cache already exists for {data_type} file key: {file_key!r}"
                )
            self.logger.debug(f"Cache of {file_key!r} already exists.")
            return cache_file_path

        # get the fsspec `FileSystem` object and URL path, if not provided:
        if fs_and_url_path is None:
            fs, url_path = self.__get_fsspec_filesystem_and_url_path(data_type)
        else:
            fs, url_path = fs_and_url_path

        if isinstance(fs, LocalFileSystem):
            src_path = Path(f"{url_path}/{src_fn}")
            delete = False
        else:
            # download to a temporary directory:
            self._ensure_user_runtime_dir()
            temp_path = self.user_runtime_dir.joinpath(src_fn)
            if add_exec := data_type == "program" and not zip_path:
                temp_path = temp_path.joinpath(spec["executable"])

            self.logger.debug(
                f"downloading {data_type} file source {src_fn!r} from remote file "
                f"system {fs!r} at remote path {url_path!r} to a temporary "
                f"directory file {temp_path!r}."
            )
            if temp_path.is_file():
                # overwrite if it already exists:
                temp_path.unlink()

            local_path = str(temp_path)
            remote_path = f"{url_path}/{src_fn}"
            if add_exec:
                remote_path += f"/{spec['executable']}"

            fs.get(rpath=remote_path, lpath=local_path)

            delete = True
            src_path = temp_path
            if add_exec:
                src_path = src_path.parent

        if req_unpack:
            self.logger.debug(f"unzipping {data_type} file path {src_path!r}")
            with TemporaryDirectory() as zip_temp_dir:
                with zipfile.ZipFile(src_path, "r") as zip_ref:
                    zip_ref.extractall(zip_temp_dir)

                unzipped_paths = list(Path(zip_temp_dir).glob("*"))

                if num_paths := len(unzipped_paths) == 0:
                    raise FileNotFoundError(f"Empty zip file: {src_path!r}.")
                elif spec.get("zip_file"):
                    if num_paths > 1:
                        raise ValueError(
                            f"When using the 'zip_file' key, the referenced zip file "
                            f"must contain exactly one item (file or directory), but the "
                            f"zip file contains {num_paths} items: {unzipped_paths!r}."
                        )
                    ext_src = unzipped_paths[0]
                    if data_type == "program":
                        # add on the executable:
                        cache_file_path.mkdir(exist_ok=True)
                        cache_file_path = cache_file_path.joinpath(spec["executable"])
                    self.logger.debug(
                        f"copying unzipped {data_type} file {ext_src.name!r} to "
                        f"cache location: {cache_file_path!r}."
                    )
                    copy_file_or_dir(ext_src, cache_file_path)

                else:
                    # copy contents of zip file to directory under `file_key`
                    cache_file_path.mkdir(exist_ok=True)
                    for ext_src_i in unzipped_paths:
                        dst_i = cache_file_path / ext_src_i.name
                        self.logger.debug(
                            f"copying unzipped {data_type} file {ext_src_i.name!r} to "
                            f"cache location: {dst_i!r}."
                        )
                        copy_file_or_dir(ext_src_i, dst_i)
                    if data_type == "program":
                        # add on the executable:
                        cache_file_path = cache_file_path.joinpath(spec["executable"])

        else:
            # copy to cache dir:
            if data_type == "program":
                # add on the executable:
                cache_file_path.mkdir(exist_ok=True)
                cache_file_path = cache_file_path.joinpath(spec["executable"])
                src_path = Path(src_path).joinpath(spec["executable"])
            self.logger.debug(
                f"copying {data_type} file path {src_path!r} to cache location: "
                f"{cache_file_path!r}."
            )
            shutil.copy(src_path, cache_file_path)

        self.__set_executable(data_type, spec, cache_file_path.parent)

        if delete:
            self.logger.debug(f"deleting file {file_key!r} source file {src_path!r}.")
            src_path.unlink()

        return cache_file_path

    def purge_file(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        manifest: dict[str, dict[str, str]] | None = None,
        not_exist_ok: bool = False,
    ):
        """Delete a built-in data or program file from the cache."""
        manifest = manifest or self.get_data_manifest(data_type)
        if path := self._get_data_file_cached_path(data_type, file_key, manifest):
            delete_file_or_dir(path)
        elif not not_exist_ok:
            raise ValueError(
                f"Built-in {data_type} {file_key!r} does not exist and so cannot be "
                f"purged, when `not_exist_ok` is set to False."
            )

    def recache_file(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        not_exist_ok: bool = True,
        manifest: dict[str, dict[str, str]] | None = None,
        fs_and_url_path: tuple[Any, Any] | None = None,
    ) -> Path:
        """
        Delete and then re-cache a built-in data or program file, and return its path.
        """
        self.purge_file(data_type, file_key, manifest=manifest, not_exist_ok=not_exist_ok)
        return self.cache_file(
            data_type,
            file_key,
            manifest=manifest,
            exist_ok=False,
            fs_and_url_path=fs_and_url_path,
        )

    def is_data_file_cached(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        manifest: dict[str, dict[str, str]] | None = None,
    ) -> bool:
        """
        Return True if the specified built-in data or program is cached.
        """
        return bool(self._get_data_file_cached_path(data_type, file_key, manifest))

    def __get_cache_dir(self, data_type: Literal["data", "program"]):
        """Get the cache directory for either built-in data or programs."""
        CACHE_LOOKUP = {
            "data": self.data_cache_dir,
            "program": self.program_cache_dir,
        }
        try:
            return CACHE_LOOKUP[data_type]
        except KeyError:
            raise ValueError(
                f"`data_type` must be 'data' or 'program', but received {data_type}."
            )

    def _get_data_file_cached_path(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        manifest: dict[str, dict[str, str]] | None = None,
    ) -> Path | None:
        """
        Return the path to a built-in data or program, if it is cached, or None otherwise.

        If the data/program is cached, this returns the cached directory joined
        `file_key`, and so for programs, this does not return the path to the executable.

        """
        manifest = manifest or self.get_data_manifest(data_type)
        if file_key not in manifest:
            raise ValueError(f"No such {data_type} file {file_key!r}.")
        cache_dir = self.__get_cache_dir(data_type)
        if (cache_file_path := cache_dir.joinpath(file_key)).exists():
            return cache_file_path
        return None

    def get_data_file_path(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        manifest: dict[str, dict[str, str]] | None = None,
    ) -> Path:
        """
        Retrieve the local path to a cached data or builtin program file.
        """
        manifest = manifest or self.get_data_manifest(data_type)
        if cache_file_path := self._get_data_file_cached_path(
            data_type, file_key, manifest
        ):
            self.logger.info(
                f"{data_type} file {file_key!r} is already in the cache: "
                f"{cache_file_path!r}."
            )
            if data_type == "program":
                cached_path = cache_file_path / manifest[file_key]["executable"]
            else:
                cached_path = cache_file_path
        else:
            cache_file_path = self.__get_cache_dir(data_type) / file_key
            self.logger.info(
                f"{data_type} file {file_key!r} is not in the cache, so copying to the "
                f"cache: {cache_file_path!r}."
            )
            cached_path = self.cache_file(data_type, file_key, manifest)
            if data_type == "program":
                # program cached path has the executable on the end:
                assert cached_path.parent == cache_file_path
            else:
                assert cached_path == cache_file_path

        return cached_path

    def _get_data_file_key_from_ID(
        self, data_type: Literal["data", "program"], id: int
    ) -> str:
        """
        Get the file key of a built-in data or program file (as used in the
        respective manifest) from an integer ID as displayed by `print_data_files` or
        `print_programs`.
        """
        try:
            return list(self.get_data_manifest(data_type))[id]
        except IndexError:
            raise ValueError(
                f"No built-in {data_type} file exists with ID {id!r}."
            ) from None

    def get_data_files(self) -> tuple[str, ...]:
        """Get a tuple of available demonstration data files."""
        return tuple(self.get_data_manifest("data"))

    def get_programs(self) -> tuple[str, ...]:
        """List available built-in program files."""
        return tuple(self.get_data_manifest("program"))

    def __print_data_files_rich_table(
        self, data_type: Literal["data", "program"]
    ) -> None:
        """
        Print a table of available demonstration data files and whether they are
        cached.
        """
        tab = Table("ID", data_type.title(), "Cached", box=box.SIMPLE)
        manifest = self.get_data_manifest(data_type)
        EMOJI_IS_CACHED = {
            True: "✅",
            False: "❌",
        }
        for idx, file_i in enumerate(manifest):
            tab.add_row(
                str(idx),
                file_i,
                EMOJI_IS_CACHED[self.is_data_file_cached(data_type, file_i, manifest)],
            )
        Console().print(tab)

    def print_data_files(self) -> None:
        """Print a table of available demonstration data files and whether they are
        cached."""
        self.__print_data_files_rich_table("data")

    def print_programs(self) -> None:
        """Print a table of available built-in program files and whether they are
        cached."""
        self.__print_data_files_rich_table("program")

    def get_demo_data_file_path(self, file_name: str) -> Path:
        """
        Get the full path to an example data file in the app cache directory.

        If the file does not already exist in the app cache directory, it will be added
        (and unzipped if required). The file may first be downloaded from a remote file
        system such as GitHub (see `get_data_file_path` for details).
        """
        return self.get_data_file_path("data", file_key=file_name)

    def get_program_path(self, file_name: str) -> Path:
        """
        Get the full path to a built-in program in the app cache directory.

        If the file does not already exist in the app cache directory, it will be added
        (and unzipped if required). The file may first be downloaded from a remote file
        system such as GitHub (see `get_data_file_path` for details).
        """
        return self.get_data_file_path("program", file_key=file_name)

    def cache_data_file(self, file_key: str, exist_ok: bool = True) -> Path:
        """
        Cache a data file and return its cached path.
        """
        return self.cache_file("data", file_key, exist_ok=exist_ok)

    def cache_program(self, file_key: str, exist_ok: bool = True) -> Path:
        """
        Cache a built-in program and return its cached path.
        """
        return self.cache_file("program", file_key, exist_ok=exist_ok)

    def purge_data_file(self, file_key: str, not_exist_ok: bool = True):
        """
        Delete a built-in data file from the cache.
        """
        return self.purge_file("data", file_key, not_exist_ok=not_exist_ok)

    def purge_program(self, file_key: str, not_exist_ok: bool = True):
        """
        Delete a built-in program from the cache.
        """
        return self.purge_file("program", file_key, not_exist_ok=not_exist_ok)

    def recache_data_file(self, file_key: str, not_exist_ok: bool = True):
        """
        Delete and then re-cache a built-in data file.
        """
        return self.recache_file("data", file_key, not_exist_ok=not_exist_ok)

    def recache_program(self, file_key: str, not_exist_ok: bool = True):
        """
        Delete and then re-cache a built-in program.
        """
        return self.recache_file("program", file_key, not_exist_ok=not_exist_ok)

    @contextmanager
    def __check_cache_all_via_github(
        self,
        fs_and_url_path: tuple[Any, Any],
        data_type: Literal["data", "program"],
        tmp_dir: Path | None = None,
    ):
        """For the special case where the data/program directory points to a GitHub
        repository, download the whole repository as an archive, instead of making many
        individual API requests, which can cause rate-limit errors.

        Parameters
        ----------
        fs_and_url_path
            A tuple of the fsspec FileSystem and the URL path within that file system, as
            returned by the fsspec `url_to_fs` function when provided with an
            fsspec-compatible URL.
        tmp_dir
            If provided, and the file system is a GithubFileSystem, the directory into
            which an archive of that repository will be downloaded. It will be created if
            it does not exists, and it is required. If not provided, a temporary directory
            will created, used, and then deleted.
        """
        from fsspec.implementations.github import GithubFileSystem  # type: ignore

        DIR_ATTR_LOOKUP = {
            "data": "data_dir",
            "program": "program_dir",
        }
        fs, url_path = fs_and_url_path
        if isinstance(fs, GithubFileSystem):
            self.logger.debug(
                "file system is `GitHubFileSystem`; downloading the repo archive to "
                "cache all."
            )
            org = fs.org
            repo = fs.repo
            sha = fs.storage_options["sha"]
            repo_dir_name = f"{repo}-{sha}"
            tmp_provided = bool(tmp_dir)
            tmp_dir = tmp_dir if tmp_provided else self._ensure_user_runtime_dir()
            assert tmp_dir
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_repo = tmp_dir / repo_dir_name

            if tmp_repo.exists():
                self.logger.debug(f"GitHub repo already downloaded: {tmp_repo}.")
            else:
                self.logger.debug(f"need to download GitHub repo to: {tmp_repo}.")
                download_github_repo(org=org, repo=repo, sha=sha, local_path=tmp_dir)

            updates = {DIR_ATTR_LOOKUP[data_type]: tmp_repo / url_path}
            with self.config._with_updates(updates):
                yield self.__get_fsspec_filesystem_and_url_path(data_type)

            if not tmp_provided:
                self.logger.debug(f"deleting GitHub repo at: {tmp_repo}.")
                shutil.rmtree(tmp_repo)
        else:
            self.logger.debug("file system is not `GitHubFileSystem`.")
            yield fs_and_url_path

    def __get_github_repo_tmp_dir(self):
        """Get a directory for downloading GitHub repo archives, so that if both data
        and programs point to the same repo, we don't need to download it twice.
        """
        return self._ensure_user_runtime_dir() / "github_repos"

    def cache_all(self, exist_ok: bool = True) -> tuple[list[Path], list[Path]]:
        """
        Cache all cacheable files: data files and programs, and return their paths.
        """
        tmp_repos_dir = self.__get_github_repo_tmp_dir()
        data_paths = self.cache_all_data_files(exist_ok, tmp_repos_dir)
        program_paths = self.cache_all_programs(exist_ok, tmp_repos_dir)
        if tmp_repos_dir.exists():
            self.logger.debug(
                f"deleting temp directory for GitHub repos at: {tmp_repos_dir}."
            )
            shutil.rmtree(tmp_repos_dir)

        return (data_paths, program_paths)

    def recache_all(self, not_exist_ok: bool = True) -> tuple[list[Path], list[Path]]:
        """
        Purge and then re-cache all cacheable files: data files and programs, and return
        their paths.
        """
        tmp_repos_dir = self.__get_github_repo_tmp_dir()
        data_paths = self.recache_all_data_files(not_exist_ok, tmp_repos_dir)
        program_paths = self.recache_all_programs(not_exist_ok, tmp_repos_dir)
        if tmp_repos_dir.exists():
            self.logger.debug(
                f"deleting temp directory for GitHub repos at: {tmp_repos_dir}."
            )
            shutil.rmtree(tmp_repos_dir)

        return (data_paths, program_paths)

    def purge_all(self, not_exist_ok: bool = True):
        """
        Delete all cacheable files from the cache: data files and programs.
        """
        self.purge_all_data_files(not_exist_ok)
        self.purge_all_programs(not_exist_ok)

    def cache_all_data_files(
        self, exist_ok: bool = True, tmp_dir: Path | None = None
    ) -> list[Path]:
        """
        Cache all data files, and return their paths.

        Parameters
        ----------
        tmp_dir
            If provided, and the `data_dir` file system is a GithubFileSystem, the
            directory into which an archive of that repository will be downloaded. It will
            be created if it does not exists, and it is required. If not provided, a
            temporary directory will created, used, and then deleted.
        """
        manifest = self.get_data_manifest("data")
        with self.__check_cache_all_via_github(
            fs_and_url_path=self.__get_fsspec_filesystem_and_url_path("data"),
            data_type="data",
            tmp_dir=tmp_dir,
        ) as data_fs_url:
            return [
                self.cache_file(
                    "data",
                    key,
                    manifest=manifest,
                    exist_ok=exist_ok,
                    fs_and_url_path=data_fs_url,
                )
                for key in self.get_data_files()
            ]

    def purge_all_data_files(self, not_exist_ok: bool = True):
        """
        Delete all built-in data files from the cache.
        """
        manifest = self.get_data_manifest("data")
        for key in self.get_data_files():
            self.purge_file("data", key, manifest=manifest, not_exist_ok=not_exist_ok)

    def recache_all_data_files(
        self, not_exist_ok: bool = True, tmp_dir: Path | None = None
    ):
        """
        Delete and then re-cache all built-in data files, and return their paths.

        Parameters
        ----------
        tmp_dir
            If provided, and the `data_dir` file system is a GithubFileSystem, the
            directory into which an archive of that repository will be downloaded. It will
            be created if it does not exists, and it is required. If not provided, a
            temporary directory will created, used, and then deleted.
        """
        manifest = self.get_data_manifest("data")
        with self.__check_cache_all_via_github(
            fs_and_url_path=self.__get_fsspec_filesystem_and_url_path("data"),
            data_type="data",
            tmp_dir=tmp_dir,
        ) as data_fs_url:
            return [
                self.recache_file(
                    "data",
                    key,
                    manifest=manifest,
                    not_exist_ok=not_exist_ok,
                    fs_and_url_path=data_fs_url,
                )
                for key in self.get_data_files()
            ]

    def cache_all_programs(
        self, exist_ok: bool = True, tmp_dir: Path | None = None
    ) -> list[Path]:
        """
        Cache all built-in programs, and return their paths.

        Parameters
        ----------
        tmp_dir
            If provided, and the `program_dir` file system is a GithubFileSystem, the
            directory into which an archive of that repository will be downloaded. It will
            be created if it does not exists, and it is required. If not provided, a
            temporary directory will created, used, and then deleted.
        """
        manifest = self.get_data_manifest("program")
        with self.__check_cache_all_via_github(
            fs_and_url_path=self.__get_fsspec_filesystem_and_url_path("program"),
            data_type="program",
            tmp_dir=tmp_dir,
        ) as program_fs_url:
            return [
                self.cache_file(
                    "program",
                    key,
                    manifest=manifest,
                    exist_ok=exist_ok,
                    fs_and_url_path=program_fs_url,
                )
                for key in self.get_programs()
            ]

    def purge_all_programs(self, not_exist_ok: bool = True):
        """
        Delete all built-in program from the cache.
        """
        manifest = self.get_data_manifest("program")
        for key in self.get_programs():
            self.purge_file("program", key, manifest=manifest, not_exist_ok=not_exist_ok)

    def recache_all_programs(
        self, not_exist_ok: bool = True, tmp_dir: Path | None = None
    ):
        """
        Delete and then re-cache all built-in programs, and return their paths.

        Parameters
        ----------
        tmp_dir
            If provided, and the `program_dir` file system is a GithubFileSystem, the
            directory into which an archive of that repository will be downloaded. It will
            be created if it does not exists, and it is required. If not provided, a
            temporary directory will created, used, and then deleted.
        """
        manifest = self.get_data_manifest("program")
        with self.__check_cache_all_via_github(
            fs_and_url_path=self.__get_fsspec_filesystem_and_url_path("program"),
            data_type="program",
            tmp_dir=tmp_dir,
        ) as program_fs_url:
            return [
                self.recache_file(
                    "program",
                    key,
                    manifest=manifest,
                    not_exist_ok=not_exist_ok,
                    fs_and_url_path=program_fs_url,
                )
                for key in self.get_programs()
            ]

    def copy_cacheable_file(
        self,
        data_type: Literal["data", "program"],
        file_key: str,
        dst: PathLike | None = None,
    ) -> Path:
        """
        Copy a builtin demo data or program file to the specified location, or the current
        directory.

        Parameters
        ----------
        data_type
            Type of the data: "data" or "program".
        file_key
            The file key to copy.
        dst
            File path to copy the file to, or, if an existing directory, the parent
            directory to copy the source file or directory to. If not specified, the
            current working directory will be used. Note that if the source is a
            directory, this must be specified as the parent directory to copy into.

        Returns
        -------
        new_path
            The new path created at or in the destination.

        Raises
        ------
        FileExistsError
            If the source is a directory, and the destination directory already contains a
            directory of the same name, FileExistsError will be raised. However, if the
            source is a file, and the destination directory already contains a file of the
            same name, it will be over-written.

        """
        dst = dst or Path(".")
        dst = Path(dst).resolve()
        src = self.get_data_file_path(data_type, file_key)

        if src.is_file():
            new_path = dst.joinpath(src.name) if dst.is_dir() else dst
            shutil.copy2(src, new_path)

        elif src.is_dir():
            if not dst.is_dir():
                raise ValueError(
                    "Destination must exist as a directory, if the source is a directory."
                )
            new_path = dst.joinpath(src.name)
            shutil.copytree(src, new_path)  # uses copy2 by default

        return new_path

    def copy_data_file(self, file_key: str, dst: PathLike | None = None) -> Path:
        """
        Copy a demonstration data file to the specified location, or the current
        directory.

        Parameters
        ----------
        file_key
            The file key to copy.
        dst
            File path to copy the file to, or, if an existing directory, the parent
            directory to copy the source file or directory to. If not specified, the
            current working directory will be used. Note that if the source is a
            directory, this must be specified as the parent directory to copy into.

        Returns
        -------
        new_path
            The new path created at or in the destination.

        Raises
        ------
        FileExistsError
            If the source is a directory, and the destination directory already contains a
            directory of the same name, FileExistsError will be raised. However, if the
            source is a file, and the destination directory already contains a file of the
            same name, it will be over-written.

        """
        return self.copy_cacheable_file("data", file_key, dst)

    def copy_program(self, file_key: str, dst: PathLike | None = None) -> Path:
        """
        Copy a built-in program to the specified location, or the current directory.

        Parameters
        ----------
        file_key
            The file key to copy.
        dst
            File path to copy the file to, or, if an existing directory, the parent
            directory to copy the source file or directory to. If not specified, the
            current working directory will be used. Note that if the source is a
            directory, this must be specified as the parent directory to copy into.

        Returns
        -------
        new_path
            The new path created at or in the destination.

        Raises
        ------
        FileExistsError
            If the source is a directory, and the destination directory already contains a
            directory of the same name, FileExistsError will be raised. However, if the
            source is a file, and the destination directory already contains a file of the
            same name, it will be over-written.

        """
        return self.copy_cacheable_file("program", file_key, dst)

    def _get_github_url(self, sha: str, path: str):
        """
        Return a fsspec URL for retrieving a file or directory on the app's GitHub
        repository.
        """
        return f"github://{self.gh_org}:{self.gh_repo}@{sha}/{path}"

    def enable_show_tracebacks(self):
        """Enable showing tracebacks of `CompactException` objects."""
        self._compact_formatter.show_tracebacks = True

    def disable_show_tracebacks(self):
        """Disable showing tracebacks of `CompactException` objects."""
        self._compact_formatter.show_tracebacks = False

    def enable_use_rich_tracebacks(self):
        """Enable using the Rich library to format exception tracebacks."""
        self._compact_formatter.use_rich_tracebacks = True

    def disable_use_rich_tracebacks(self):
        """Disable using the Rich library to format exception tracebacks."""
        self._compact_formatter.use_rich_tracebacks = False

    def __install_data_or_program_cache(
        self,
        data_type: Literal["data", "program"],
        path: str | Path,
        overwrite: bool = False,
    ):
        """Copy pre-existing cached data or programs to the correct locations.

        Note: this does not unzip data or programs that are compressed; it expects data or
        programs files in their final, uncompressed form.
        """
        assert (path_ := Path(path)).is_dir()

        ENSURE_LOOKUP = {
            "data": self._ensure_data_cache_dir,
            "program": self._ensure_program_cache_dir,
        }
        cache_dir = ENSURE_LOOKUP[data_type]()
        manifest = self.get_data_manifest(data_type)
        for key in manifest:
            spec = self.__validate_cacheable_file_spec(data_type, key, manifest[key])
            src = path_ / key
            dst = cache_dir / key
            if not src.exists():
                raise ValueError(
                    f"{data_type.title()} cache item {key!r} does not exist within the "
                    f"provided path: {path_!r}."
                )
            overwrote = False
            if dst.exists():
                if overwrite:
                    # delete existing first:
                    delete_file_or_dir(dst)
                    overwrote = True
                else:
                    raise FileExistsError(
                        f"{data_type.title()} cache item {key!r} already exists; set "
                        f"`overwrite` to True to replace it."
                    )

            # copy into the app cache directory:
            copy_file_or_dir(src, dst)

            # set executable bits, if required:
            if data_type == "program":
                self.__set_executable(data_type, spec, dst)

            ow = f" (by overwriting existing item)" if overwrote else ""
            print(f"Installed {data_type} cache item {key!r}{ow} from path {path_!r}")

    def install_data_cache(self, path: str | Path, overwrite: bool = False):
        """Copy pre-existing cached data to the correct location.

        This can be used in CI workflows to load the cache from a single source for all
        tests, thus mitigating, for example, GitHub actions rate-limits. In principle,
        this could also be used for "offline" installations, where built-in programs can
        be included in a distributed installation package.

        Parameters
        ----------
        path
            Path to an existing directory containing files/folders to be copied to the
            app's data cache directory.
        overwrite
            If True, overwrite (by first removing) any items that are already within the
            data cache directory. If False (the default), a `FileExistsError` exception
            will be raised if an item already exists.
        """
        self.__install_data_or_program_cache("data", path, overwrite)

    def install_program_cache(self, path: str | Path, overwrite: bool = False):
        """Copy pre-existing cached programs to the correct location.

        This can be used in CI workflows to load the cache from a single source for all
        tests, thus mitigating, for example, GitHub actions rate-limits. In principle,
        this could also be used for "offline" installations, where built-in data can be
        included in a distributed installation package.

        Parameters
        ----------
        path
            Path to an existing directory containing files/folders to be copied to the
            app's program cache directory.
        overwrite
            If True, overwrite (by first removing) any items that are already within the
            data cache directory. If False (the default), a `FileExistsError` exception
            will be raised if an item already exists.
        """
        self.__install_data_or_program_cache("program", path, overwrite)


class App(BaseApp):
    """Class from which to instantiate downstream app objects (e.g. MatFlow)."""

    pass
