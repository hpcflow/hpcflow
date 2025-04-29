"""
Model of files that hold commands.
"""

from __future__ import annotations
import copy
from dataclasses import dataclass, field, InitVar
from pathlib import Path
from textwrap import dedent
from typing import Protocol, cast, overload, TYPE_CHECKING
from typing_extensions import Final, override

from hpcflow.sdk.typing import PathLike, hydrate, ParamSource
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import search_dir_files_by_regex
from hpcflow.sdk.core.zarr_io import zarr_decode
from hpcflow.sdk.core.parameters import _process_demo_data_strings

if TYPE_CHECKING:
    import os
    from collections.abc import Mapping
    from typing import Any, ClassVar
    from typing_extensions import Self
    from .actions import Action, ActionRule
    from .environment import Environment
    from .object_list import CommandFilesList
    from .parameters import Parameter
    from .task import ElementSet
    from .workflow import Workflow


class FileNamePart(Protocol):
    """
    A filename or piece of filename that can be expanded.
    """

    def value(self, directory: str | os.PathLike = ".") -> str | list[str]:
        """
        Get the part of the file, possibly with directory specified.
        Implementations of this may ignore the directory.
        If a pattern, the expanded value may be a list of strings.
        """


@dataclass(init=False)
@hydrate
class FileSpec(JSONLike):
    """
    A specification of a file handled by a workflow.
    """

    _validation_schema: ClassVar[str] = "files_spec_schema.yaml"
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="name", class_name="FileNameSpec"),
    )

    #: Label for this file specification.
    label: Final[str]
    #: The name of the file.
    name: Final[FileNameSpec]
    #: Documentation for the file specification.
    doc: Final[str]
    _hash_value: str | None = field(default=None, repr=False)

    def __init__(
        self,
        label: str,
        name: str | FileNameSpec,
        doc: str = "",
        _hash_value: str | None = None,
    ) -> None:
        self.label = label
        self.name = self._app.FileNameSpec(name) if isinstance(name, str) else name
        self.doc = doc
        self._hash_value = _hash_value
        self.__hash = hash((label, self.name))

    def value(self, directory: str | os.PathLike = ".") -> str:
        """
        The path to a file, optionally resolved with respect to a particular directory.
        """
        return cast("str", self.name.value(directory))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.label == other.label and self.name == other.name

    def __hash__(self) -> int:
        return self.__hash

    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        d.pop("_FileSpec__hash")
        return d

    @property
    def stem(self) -> FileNameStem:
        """
        The stem of the file name.
        """
        return self.name.stem

    @property
    def ext(self) -> FileNameExt:
        """
        The extension of the file name.
        """
        return self.name.ext

    @property
    def documentation(self) -> str:
        """
        Documentation for rendering via Jinja.
        """
        if self.doc:
            import markupsafe

            return markupsafe.Markup(self.doc)
        return repr(self)


@hydrate
class FileNameSpec(JSONLike):
    """
    The name of a file handled by a workflow, or a pattern that matches multiple files.

    Parameters
    ----------
    name: str
        The name or pattern.
    args: list
        Positional arguments to use when formatting the name.
        Can be omitted if the name does not contain a Python formatting pattern.
    is_regex: bool
        If true, the name is used as a regex to search for actual files.
    """

    def __init__(
        self,
        name: str,
        args: list[FileNamePart] | None = None,
        is_regex: bool = False,
    ) -> None:
        #: The name or pattern.
        self.name: Final[str] = name
        #: Positional arguments to use when formatting the name.
        self.args: Final[tuple[FileNamePart, ...]] = tuple(args or [])
        #: Whether the name is used as a regex to search for actual files.
        self.is_regex: Final[bool] = is_regex
        self.__hash = hash((name, self.args, is_regex))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.name == other.name
            and self.args == other.args
            and self.is_regex == other.is_regex
        )

    def __hash__(self) -> int:
        return self.__hash

    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        d.pop("_FileNameSpec__hash")
        return d

    @property
    def stem(self) -> FileNameStem:
        """
        The stem of the name or pattern.
        """
        return self._app.FileNameStem(self)

    @property
    def ext(self) -> FileNameExt:
        """
        The extension of the name or pattern.
        """
        return self._app.FileNameExt(self)

    def value(self, directory: str | os.PathLike = ".") -> list[str] | str:
        """
        Get the template-resolved name of the file
        (or files matched if the name is a regex pattern).

        Parameters
        ----------
        directory: PathLike
            Where to resolve values with respect to.
        """
        format_args = [arg.value(Path(directory)) for arg in self.args]
        value = self.name.format(*format_args)
        if self.is_regex:
            return search_dir_files_by_regex(value, directory=directory)
        return value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


@dataclass
class FileNameStem(JSONLike):
    """
    The stem of a file name.
    """

    #: The file specification this is derived from.
    file_name: FileNameSpec

    def value(self, directory: str | os.PathLike = ".") -> str:
        """
        Get the stem, possibly with directory specified.
        """
        d = self.file_name.value(directory)
        if self.file_name.is_regex:
            raise ValueError("cannot get the stem of a regex match")
        assert not isinstance(d, list)
        return Path(d).stem


@dataclass
class FileNameExt(JSONLike):
    """
    The extension of a file name.
    """

    #: The file specification this is derived from.
    file_name: FileNameSpec

    def value(self, directory: str | os.PathLike = ".") -> str:
        """
        Get the extension.
        """
        d = self.file_name.value(directory)
        if self.file_name.is_regex:
            raise ValueError("cannot get the extension of a regex match")
        assert not isinstance(d, list)
        return Path(d).suffix


@dataclass
@hydrate
class InputFileGenerator(JSONLike):
    """
    Represents a script that is run to generate input files for an action.

    Parameters
    ----------
    input_file:
        The file to generate.
    inputs: list[~hpcflow.app.Parameter]
        The input parameters to the generator.
    script:
        The script that generates the input.
    environment:
        The environment in which to run the generator.
    script_pass_env_spec:
        Whether to pass in the environment.
    abortable:
        Whether the generator can be stopped early.
        Quick-running scripts tend to not need this.
    rules: list[~hpcflow.app.ActionRule]
        User-specified rules for whether to run the generator.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="input_file",
            class_name="FileSpec",
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="inputs",
            class_name="Parameter",
            is_multiple=True,
            json_like_name="from_inputs",
            shared_data_primary_key="typ",
            shared_data_name="parameters",
        ),
        ChildObjectSpec(
            name="rules",
            class_name="ActionRule",
            is_multiple=True,
            parent_ref="input_file_generator",
        ),
    )

    #: The file to generate.
    input_file: FileSpec
    #: The input parameters to the generator.
    inputs: list[Parameter]
    #: The script that generates the inputs.
    script: str | None = None
    #: The environment in which to run the generator.
    environment: Environment | None = None
    #: Whether to pass in the environment.
    script_pass_env_spec: bool = False
    #: Whether the generator can be stopped early.
    #: Quick-running scripts tend to not need this.
    abortable: bool = False
    #: User-specified rules for whether to run the generator.
    rules: list[ActionRule] = field(default_factory=list)
    #: Whether the generator requires a working directory.
    requires_dir: bool = True

    def get_action_rules(self) -> list[ActionRule]:
        """
        Get the rules that allow testing if this input file generator must be run or
        not for a given element.
        """
        return [
            self._app.ActionRule.check_missing(f"input_files.{self.input_file.label}")
        ] + self.rules


@dataclass
@hydrate
class OutputFileParser(JSONLike):
    """
    Represents a script that is run to parse output files from an action and create outputs.

    Parameters
    ----------
    output_files: list[FileSpec]
        The output files that this parser will parse.
    output: ~hpcflow.app.Parameter
        The singular output parsed by this parser. Not to be confused with `outputs` (plural).
    script: str
        The name of the file containing the output file parser source.
    environment: ~hpcflow.app.Environment
        The environment to use to run the parser.
    inputs: list[str]
        The other inputs to the parser.
    outputs: list[str]
        Optional multiple outputs from the upstream actions of the schema that are
        required to parametrise this parser.
    options: dict
        Miscellaneous options.
    script_pass_env_spec: bool
        Whether to pass the environment specifier to the script.
    abortable: bool
        Whether this script can be aborted.
    save_files: list[str]
        The files that should be saved to the persistent store for the workflow.
    clean_up: list[str]
        The files that should be immediately removed.
    rules: list[~hpcflow.app.ActionRule]
        Rules for whether to enable this parser.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="output",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
        ChildObjectSpec(
            name="output_files",
            json_like_name="from_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="save_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="clean_up",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="rules",
            class_name="ActionRule",
            is_multiple=True,
            parent_ref="output_file_parser",
        ),
    )

    #: The output files that this parser will parse.
    output_files: list[FileSpec]
    #: The singular output parsed by this parser.
    #: Not to be confused with :py:attr:`outputs` (plural).
    output: Parameter | None = None
    #: The name of the file containing the output file parser source.
    script: str | None = None
    #: The environment to use to run the parser.
    environment: Environment | None = None
    #: The other inputs to the parser.
    inputs: list[str] | None = None
    #: Optional multiple outputs from the upstream actions of the schema that are
    #: required to parametrise this parser.
    #: Not to be confused with :py:attr:`output` (singular).
    outputs: list[str] | None = None
    #: Miscellaneous options.
    options: dict[str, Any] | None = None
    #: Whether to pass the environment specifier to the script.
    script_pass_env_spec: bool = False
    #: Whether this script can be aborted.
    abortable: bool = False
    #: The files that should be saved to the persistent store for the workflow.
    save_files: InitVar[list[FileSpec] | bool] = True
    _save_files: list[FileSpec] = field(init=False)
    #: The files that should be immediately removed.
    clean_up: list[str] = field(default_factory=list)
    #: Rules for whether to enable this parser.
    rules: list[ActionRule] = field(default_factory=list)
    #: Whether the parser requires a working directory.
    requires_dir: bool = True

    def __post_init__(self, save_files: list[FileSpec] | bool) -> None:
        if not save_files:
            # save no files
            self._save_files = []
        elif save_files is True:
            # save all output files
            self._save_files = [out_f for out_f in self.output_files]
        else:
            self._save_files = save_files

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        d = super()._postprocess_to_dict(d)
        if "_save_files" in d:
            d["save_files"] = d.pop("_save_files")
        return d

    @classmethod
    def from_json_like(  # type: ignore[override]
        cls, json_like: dict[str, Any], shared_data: Mapping | None = None
    ) -> Self:
        if "save_files" in json_like:
            if not json_like["save_files"]:
                json_like["save_files"] = []
            elif json_like["save_files"] is True:
                json_like["save_files"] = [i for i in json_like["output_files"]]
        return super().from_json_like(json_like, shared_data)

    def get_action_rules(self) -> list[ActionRule]:
        """Get the rules that allow testing if this output file parser must be run or not
        for a given element."""
        return [
            self._app.ActionRule.check_missing(f"output_files.{out_f.label}")
            for out_f in self.output_files
        ] + self.rules


@hydrate
class _FileContentsSpecifier(JSONLike):
    """Class to represent the contents of a file, either via a file-system path or
    directly."""

    #: What file is this? Only if known.
    file: FileSpec

    def __init__(
        self,
        path: Path | str | None = None,
        contents: str | None = None,
        extension: str = "",
        store_contents: bool = True,
    ) -> None:
        if path is not None and contents is not None:
            raise ValueError("Specify exactly one of `path` and `contents`.")

        if contents is not None and not store_contents:
            raise ValueError(
                "`store_contents` cannot be set to False if `contents` was specified."
            )

        self._path = _process_demo_data_strings(self._app, path)
        self._contents = contents
        self._extension = extension
        self._store_contents = store_contents

        # assigned by `make_persistent`
        self._workflow: Workflow | None = None
        self._value_group_idx: int | None = None

        # assigned by parent `ElementSet`
        self._element_set: ElementSet | None = None

    def __deepcopy__(self, memo: dict | None) -> Self:
        kwargs = self.to_dict()
        value_group_idx = kwargs.pop("value_group_idx")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._value_group_idx = value_group_idx
        obj._workflow = self._workflow
        obj._element_set = self._element_set
        return obj

    @property
    def normalised_path(self) -> str:
        """
        Full workflow value path to the file.

        Note
        ----
        This is not the same as the path in the filesystem, but is closely
        related.
        """
        return str(self._path) if self._path else "."

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        if "_workflow" in out:
            del out["_workflow"]
        return {k.lstrip("_"): v for k, v in out.items()}

    @classmethod
    def _json_like_constructor(cls, json_like: dict[str, Any]) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("value_group_idx", None)
        obj = cls(**json_like)
        obj._value_group_idx = _value_group_idx

        return obj

    def _get_members(self, ensure_contents: bool = False) -> dict[str, Any]:
        out = self.to_dict()
        del out["value_group_idx"]

        if ensure_contents and self._store_contents and self._contents is None:
            out["contents"] = self.read_contents()

        return out

    def make_persistent(
        self,
        workflow: Workflow,
        source: ParamSource,
    ) -> tuple[str, list[int], bool]:
        """Save to a persistent workflow.

        Returns
        -------
        String is the data path for this task input and integer list
        contains the indices of the parameter data Zarr groups where the data is
        stored.

        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameters_exist(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a data reference "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_file(
                store_contents=self.store_contents,
                is_input=True,
                source=source,
                path=self.path,
                contents=self.contents,
                filename=self.file.name.name,
            )
            # data_ref = workflow._add_parameter_data(
            #     data=self._get_members(ensure_contents=True, use_file_label=True),
            #     source=source,
            # )
            is_new = True
            self._value_group_idx = data_ref
            self._workflow = workflow
            self._path = None
            self._contents = None
            self._extension = ""
            self._store_contents = True

        return (self.normalised_path, [data_ref], is_new)

    @overload
    def _get_value(self, value_name: None = None) -> dict[str, Any]: ...

    @overload
    def _get_value(self, value_name: str) -> Any: ...

    def _get_value(self, value_name: str | None = None) -> Any:
        # TODO: fix
        assert self._value_group_idx is None
        if self._value_group_idx is not None:
            from ..persistence.zarr import ZarrPersistentStore

            assert isinstance(self.workflow._store, ZarrPersistentStore)
            # FIXME: Next two lines are both thoroughly broken, but at least resolve to something
            grp = self.workflow._store._get_parameter_group(self._value_group_idx)
            val = zarr_decode(grp)
        else:
            val = self._get_members(ensure_contents=(value_name == "contents"))
        if value_name:
            return val.get(value_name)

        return val

    def read_contents(self) -> str:
        """
        Get the actual contents of the file.
        """
        with self.__path.open("r") as fh:
            return fh.read()

    @property
    def __path(self) -> Path:
        path = self._get_value("path")
        assert path is not None
        return Path(path)

    @property
    def path(self) -> Path | None:
        """
        The path to the file.
        """
        path = self._get_value("path")
        return Path(path) if path else None

    @property
    def store_contents(self) -> Any:
        """
        Whether the file's contents are stored in the workflow's persistent store.
        """
        return self._get_value("store_contents")

    @property
    def contents(self) -> str:
        """
        The contents of the file.
        """
        if self.store_contents:
            return self._get_value("contents")
        else:
            return self.read_contents()

    @property
    def extension(self) -> str:
        """
        The extension of the file.
        """
        return self._get_value("extension")

    @property
    def workflow(self) -> Workflow:
        """
        The owning workflow.
        """
        if self._workflow:
            return self._workflow
        elif self._element_set:
            w_tmpl = self._element_set.task_template.workflow_template
            if w_tmpl and w_tmpl.workflow:
                return w_tmpl.workflow
        raise NotImplementedError


@hydrate
class InputFile(_FileContentsSpecifier):
    """
    An input file.

    Parameters
    ----------
    file:
        What file is this?
    path: Path
        Where is the (original) file?
    contents: str
        What is the contents of the file (if already known)?
    extension: str
        What is the extension of the file?
    store_contents: bool
        Are the file's contents to be cached in the workflow persistent store?
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="file",
            class_name="FileSpec",
            shared_data_name="command_files",
            shared_data_primary_key="label",
        ),
    )

    def __init__(
        self,
        file: FileSpec | str,
        path: Path | str | None = None,
        contents: str | None = None,
        extension: str = "",
        store_contents: bool = True,
    ) -> None:
        if not isinstance(file, FileSpec):
            files: CommandFilesList = self._app.command_files
            self.file = files.get(file)
        else:
            self.file = file

        super().__init__(path, contents, extension, store_contents)

    def _get_members(
        self, ensure_contents: bool = False, use_file_label: bool = False
    ) -> dict[str, Any]:
        out = super()._get_members(ensure_contents)
        if use_file_label:
            out["file"] = self.file.label
        return out

    def __repr__(self) -> str:
        val_grp_idx = ""
        if self._value_group_idx is not None:
            val_grp_idx = f", value_group_idx={self._value_group_idx}"

        path_str = ""
        if self.path is not None:
            path_str = f", path={self.path!r}"

        return (
            f"{self.__class__.__name__}("
            f"file={self.file.label!r}"
            f"{path_str}"
            f"{val_grp_idx}"
            f")"
        )

    @property
    def normalised_files_path(self) -> str:
        """
        Standard name for the file within the workflow.
        """
        return self.file.label

    @property
    def normalised_path(self) -> str:
        return f"input_files.{self.normalised_files_path}"


@hydrate
class InputFileGeneratorSource(_FileContentsSpecifier):
    """
    The source of code for use in an input file generator.

    Parameters
    ----------
    generator:
        How to generate the file.
    path:
        Path to the file to generate.
    contents:
        Contents of the file. Only used when recreating this object.
    extension:
        File name extension.
    """

    def __init__(
        self,
        generator: InputFileGenerator,
        path: Path | str | None = None,
        contents: str | None = None,
        extension: str = "",
    ):
        #: How to generate the file.
        self.generator = generator
        super().__init__(path, contents, extension)


@hydrate
class OutputFileParserSource(_FileContentsSpecifier):
    """
    The source of code for use in an output file parser.

    Parameters
    ----------
    parser:
        How to parse the file.
    path: Path
        Path to the file to parse.
    contents:
        Contents of the file. Only used when recreating this object.
    extension:
        File name extension.
    """

    def __init__(
        self,
        parser: OutputFileParser,
        path: Path | str | None = None,
        contents: str | None = None,
        extension: str = "",
    ):
        #: How to parse the file.
        self.parser = parser
        super().__init__(path, contents, extension)
