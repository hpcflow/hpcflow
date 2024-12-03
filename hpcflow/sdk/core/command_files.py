"""
Model of files that hold commands.
"""

from __future__ import annotations
import copy
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union

from hpcflow.sdk import app
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.environment import Environment
from hpcflow.sdk.core.utils import search_dir_files_by_regex
from hpcflow.sdk.core.zarr_io import zarr_decode
from hpcflow.sdk.core.parameters import _process_demo_data_strings


@dataclass
class FileSpec(JSONLike):
    """
    A specification of a file handled by a workflow.
    """

    _app_attr = "app"

    _validation_schema = "files_spec_schema.yaml"
    _child_objects = (ChildObjectSpec(name="name", class_name="FileNameSpec"),)

    #: Label for this file specification.
    label: str
    #: The name of the file.
    name: str
    #: Documentation for the file specification.
    doc: str = ""
    _hash_value: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.name = (
            self.app.FileNameSpec(self.name) if isinstance(self.name, str) else self.name
        )

    def value(self, directory="."):
        """
        The path to a file, optionally resolved with respect to a particular directory.
        """
        return self.name.value(directory)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.label == other.label and self.name == other.name:
            return True
        return False

    @property
    def stem(self):
        """
        The stem of the file name.
        """
        return self.name.stem

    @property
    def ext(self):
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

    _app_attr = "app"

    def __init__(self, name, args=None, is_regex=False):
        #: The name or pattern.
        self.name = name
        #: Positional arguments to use when formatting the name.
        self.args = args
        #: Whether the name is used as a regex to search for actual files.
        self.is_regex = is_regex

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.name == other.name
            and self.args == other.args
            and self.is_regex == other.is_regex
        )

    @property
    def stem(self):
        """
        The stem of the name or pattern.
        """
        return self.app.FileNameStem(self)

    @property
    def ext(self):
        """
        The extension of the name or pattern.
        """
        return self.app.FileNameExt(self)

    def value(self, directory="."):
        """
        Get the template-resolved name of the file
        (or files matched if the name is a regex pattern).

        Parameters
        ----------
        directory: str
            Where to resolve values with respect to.
        """
        format_args = [i.value(directory) for i in self.args or []]
        value = self.name.format(*format_args)
        if self.is_regex:
            value = search_dir_files_by_regex(value, group=0, directory=directory)
        return value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


@dataclass
class FileNameStem(JSONLike):
    """
    The stem of a file name.
    """

    #: The file specification this is derived from.
    file_name: app.FileNameSpec

    def value(self, directory=None):
        """
        Get the stem, possibly with directory specified.
        """
        return Path(self.file_name.value(directory)).stem


@dataclass
class FileNameExt(JSONLike):
    """
    The extension of a file name.
    """

    #: The file specification this is derived from.
    file_name: app.FileNameSpec

    def value(self, directory=None):
        """
        Get the extension.
        """
        return Path(self.file_name.value(directory)).suffix


@dataclass
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

    _app_attr = "app"

    _child_objects = (
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
    input_file: app.FileSpec
    #: The input parameters to the generator.
    inputs: List[app.Parameter]
    #: The script that generates the inputs.
    script: str = None
    #: The environment in which to run the generator.
    environment: app.Environment = None
    #: Whether to pass in the environment.
    script_pass_env_spec: Optional[bool] = False
    #: Whether the generator can be stopped early.
    #: Quick-running scripts tend to not need this.
    abortable: Optional[bool] = False
    #: User-specified rules for whether to run the generator.
    rules: Optional[List[app.ActionRule]] = None

    def __post_init__(self):
        self.rules = self.rules or []

    def get_action_rules(self):
        """Get the rules that allow testing if this input file generator must be run or
        not for a given element."""
        return [
            self.app.ActionRule.check_missing(f"input_files.{self.input_file.label}")
        ] + self.rules

    def compose_source(self, snip_path) -> str:
        """Generate the file contents of this input file generator source."""

        script_main_func = snip_path.stem
        with snip_path.open("rt") as fp:
            script_str = fp.read()

        main_block = dedent(
            """\
            if __name__ == "__main__":
                import sys
                from pathlib import Path
                import {app_module} as app
                app.load_config(
                    log_file_path=Path("{run_log_file}").resolve(),
                    config_dir=r"{cfg_dir}",
                    config_key=r"{cfg_invoc_key}",
                )
                wk_path, EAR_ID = sys.argv[1:]
                EAR_ID = int(EAR_ID)
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                {script_main_func}(path=Path({file_path!r}), **EAR.get_IFG_input_values())
        """
        )
        main_block = main_block.format(
            run_log_file=self.app.RunDirAppFiles.get_log_file_name(),
            app_module=self.app.module,
            cfg_dir=self.app.config.config_directory,
            cfg_invoc_key=self.app.config.config_key,
            script_main_func=script_main_func,
            file_path=self.input_file.name.value(),
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        )

        out = out.format(script_str=script_str, main_block=main_block)
        return out

    def write_source(self, action, env_spec: Dict[str, Any]):
        """
        Write the script if it is specified as a snippet script, otherwise we assume
        the script already exists in the working directory.
        """
        snip_path = action.get_snippet_script_path(self.script, env_spec)
        if snip_path:
            source_str = self.compose_source(snip_path)
            with Path(snip_path.name).open("wt", newline="\n") as fp:
                fp.write(source_str)


@dataclass
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
    clean_files: list[str]
        The files that should be immediately removed.
    rules: list[~hpcflow.app.ActionRule]
        Rules for whether to enable this parser.
    """

    _child_objects = (
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
    output_files: List[app.FileSpec]
    #: The singular output parsed by this parser.
    #: Not to be confused with :py:attr:`outputs` (plural).
    output: Optional[app.Parameter] = None
    #: The name of the file containing the output file parser source.
    script: str = None
    #: The environment to use to run the parser.
    environment: Environment = None
    #: The other inputs to the parser.
    inputs: List[str] = None
    #: Optional multiple outputs from the upstream actions of the schema that are
    #: required to parametrise this parser.
    #: Not to be confused with :py:attr:`output` (plural).
    outputs: List[str] = None
    #: Miscellaneous options.
    options: Dict = None
    #: Whether to pass the environment specifier to the script.
    script_pass_env_spec: Optional[bool] = False
    #: Whether this script can be aborted.
    abortable: Optional[bool] = False
    #: The files that should be saved to the persistent store for the workflow.
    save_files: Union[List[str], bool] = True
    #: The files that should be immediately removed.
    clean_up: Optional[List[str]] = None
    #: Rules for whether to enable this parser.
    rules: Optional[List[app.ActionRule]] = None

    def __post_init__(self):
        if not self.save_files:
            # save no files
            self.save_files = []
        elif self.save_files is True:
            # save all output files
            self.save_files = [i for i in self.output_files]
        if self.clean_up is None:
            self.clean_up = []
        self.rules = self.rules or []

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "save_files" in json_like:
            if not json_like["save_files"]:
                json_like["save_files"] = []
            elif json_like["save_files"] is True:
                json_like["save_files"] = [i for i in json_like["output_files"]]
        return super().from_json_like(json_like, shared_data)

    def get_action_rules(self):
        """Get the rules that allow testing if this output file parser must be run or not
        for a given element."""
        return [
            self.app.ActionRule.check_missing(f"output_files.{i.label}")
            for i in self.output_files
        ] + self.rules

    def compose_source(self, snip_path) -> str:
        """Generate the file contents of this output file parser source."""

        if self.output is None:
            # might be used just for saving files:
            return

        script_main_func = snip_path.stem
        with snip_path.open("rt") as fp:
            script_str = fp.read()

        main_block = dedent(
            """\
            if __name__ == "__main__":
                import sys
                from pathlib import Path
                import {app_module} as app
                app.load_config(
                    log_file_path=Path("{run_log_file}").resolve(),
                    config_dir=r"{cfg_dir}",
                    config_key=r"{cfg_invoc_key}",
                )
                wk_path, EAR_ID = sys.argv[1:]
                EAR_ID = int(EAR_ID)
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                value = {script_main_func}(
                    **EAR.get_OFP_output_files(),
                    **EAR.get_OFP_inputs(),
                    **EAR.get_OFP_outputs(),
                )
                wk.save_parameter(name="{param_name}", value=value, EAR_ID=EAR_ID)

        """
        )
        main_block = main_block.format(
            run_log_file=self.app.RunDirAppFiles.get_log_file_name(),
            app_module=self.app.module,
            cfg_dir=self.app.config.config_directory,
            cfg_invoc_key=self.app.config.config_key,
            script_main_func=script_main_func,
            param_name=f"outputs.{self.output.typ}",
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        )

        out = out.format(script_str=script_str, main_block=main_block)
        return out

    def write_source(self, action, env_spec: Dict[str, Any]):
        """
        Write the actual output parser to a file so it can be enacted.
        """
        if self.output is None:
            # might be used just for saving files:
            return

        # write the script if it is specified as a snippet script, otherwise we assume
        # the script already exists in the working directory:
        snip_path = action.get_snippet_script_path(self.script, env_spec)
        if snip_path:
            source_str = self.compose_source(snip_path)
            with Path(snip_path.name).open("wt", newline="\n") as fp:
                fp.write(source_str)


class _FileContentsSpecifier(JSONLike):
    """Class to represent the contents of a file, either via a file-system path or
    directly."""

    def __init__(
        self,
        path: Union[Path, str] = None,
        contents: Optional[str] = None,
        extension: Optional[str] = "",
        store_contents: Optional[bool] = True,
    ):
        if path is not None and contents is not None:
            raise ValueError("Specify exactly one of `path` and `contents`.")

        if contents is not None and not store_contents:
            raise ValueError(
                "`store_contents` cannot be set to False if `contents` was specified."
            )

        self._path = _process_demo_data_strings(self.app, path)
        self._contents = contents
        self._extension = extension
        self._store_contents = store_contents

        # assigned by `make_persistent`
        self._workflow = None
        self._value_group_idx = None

        # assigned by parent `ElementSet`
        self._element_set = None

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        value_group_idx = kwargs.pop("value_group_idx")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._value_group_idx = value_group_idx
        obj._workflow = self._workflow
        obj._element_set = self._element_set
        return obj

    def to_dict(self):
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]

        out = {k.lstrip("_"): v for k, v in out.items()}
        return out

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("value_group_idx", None)
        obj = cls(**json_like)
        obj._value_group_idx = _value_group_idx

        return obj

    def _get_members(self, ensure_contents=False):
        out = self.to_dict()
        del out["value_group_idx"]

        if ensure_contents and self._store_contents and self._contents is None:
            out["contents"] = self.read_contents()

        return out

    def make_persistent(
        self,
        workflow: app.Workflow,
        source: Dict,
    ) -> Tuple[str, List[int], bool]:
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
            if not workflow.check_parameter_group_exists(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
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
            self._extension = None
            self._store_contents = None

        return (self.normalised_path, [data_ref], is_new)

    def _get_value(self, value_name=None):
        # TODO: fix
        if self._value_group_idx is not None:
            grp = self.workflow.get_zarr_parameter_group(self._value_group_idx)
            val = zarr_decode(grp)
        else:
            val = self._get_members(ensure_contents=(value_name == "contents"))
        if value_name:
            val = val.get(value_name)

        return val

    def read_contents(self):
        """
        Get the actual contents of the file.
        """
        with self.path.open("r") as fh:
            return fh.read()

    @property
    def path(self):
        """
        The path to the file.
        """
        path = self._get_value("path")
        return Path(path) if path else None

    @property
    def store_contents(self):
        """
        Whether the file's contents are stored in the workflow's persistent store.
        """
        return self._get_value("store_contents")

    @property
    def contents(self):
        """
        The contents of the file.
        """
        if self.store_contents:
            contents = self._get_value("contents")
        else:
            contents = self.read_contents()

        return contents

    @property
    def extension(self):
        """
        The extension of the file.
        """
        return self._get_value("extension")

    @property
    def workflow(self) -> app.Workflow:
        """
        The owning workflow.
        """
        if self._workflow:
            return self._workflow
        elif self._element_set:
            return self._element_set.task_template.workflow_template.workflow


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

    _child_objects = (
        ChildObjectSpec(
            name="file",
            class_name="FileSpec",
            shared_data_name="command_files",
            shared_data_primary_key="label",
        ),
    )

    def __init__(
        self,
        file: Union[app.FileSpec, str],
        path: Optional[Union[Path, str]] = None,
        contents: Optional[str] = None,
        extension: Optional[str] = "",
        store_contents: Optional[bool] = True,
    ):
        #: What file is this?
        self.file = file
        if not isinstance(self.file, FileSpec):
            self.file = self.app.command_files.get(self.file.label)

        super().__init__(path, contents, extension, store_contents)

    def to_dict(self):
        dct = super().to_dict()
        return dct

    def _get_members(self, ensure_contents=False, use_file_label=False):
        out = super()._get_members(ensure_contents)
        if use_file_label:
            out["file"] = self.file.label
        return out

    def __repr__(self):
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
    def normalised_files_path(self):
        """
        Standard name for the file within the workflow.
        """
        return self.file.label

    @property
    def normalised_path(self):
        """
        Full workflow value path to the file.

        Note
        ----
        This is not the same as the path in the filesystem.
        """
        return f"input_files.{self.normalised_files_path}"


class InputFileGeneratorSource(_FileContentsSpecifier):
    """
    The source of code for use in an input file generator.

    Parameters
    ----------
    generator:
        How to generate the file.
    path:
        Path to the file.
    contents:
        Contents of the file. Only used when recreating this object.
    extension:
        File name extension.
    """

    def __init__(
        self,
        generator: app.InputFileGenerator,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        #: How to generate the file.
        self.generator = generator
        super().__init__(path, contents, extension)


class OutputFileParserSource(_FileContentsSpecifier):
    """
    The source of code for use in an output file parser.

    Parameters
    ----------
    parser:
        How to parse the file.
    path: Path
        Path to the file.
    contents:
        Contents of the file. Only used when recreating this object.
    extension:
        File name extension.
    """

    def __init__(
        self,
        parser: app.OutputFileParser,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        #: How to parse the file.
        self.parser = parser
        super().__init__(path, contents, extension)
