"""Sub-package to define an extensible hpcflow application."""

from __future__ import annotations
from collections.abc import Mapping
import logging
import os
import sys
from typing import Final

#: Classes used in the construction of a workflow.
#: :meta hide-value:
sdk_classes: Final[Mapping[str, str]] = {
    "Workflow": "hpcflow.sdk.core.workflow",
    "Task": "hpcflow.sdk.core.task",
    "MetaTask": "hpcflow.sdk.core.task",
    "ActionScopeType": "hpcflow.sdk.core.enums",
    "ActionScope": "hpcflow.sdk.core.actions",
    "ActionRule": "hpcflow.sdk.core.actions",
    "Rule": "hpcflow.sdk.core.rule",
    "Action": "hpcflow.sdk.core.actions",
    "ActionEnvironment": "hpcflow.sdk.core.actions",
    "ElementActionRun": "hpcflow.sdk.core.actions",
    "ElementAction": "hpcflow.sdk.core.actions",
    "FileSpec": "hpcflow.sdk.core.command_files",
    "FileNameSpec": "hpcflow.sdk.core.command_files",
    "FileNameStem": "hpcflow.sdk.core.command_files",
    "FileNameExt": "hpcflow.sdk.core.command_files",
    "FileNameExt": "hpcflow.sdk.core.command_files",
    "InputFileGenerator": "hpcflow.sdk.core.command_files",
    "OutputFileParser": "hpcflow.sdk.core.command_files",
    "InputFile": "hpcflow.sdk.core.command_files",
    "InputFile": "hpcflow.sdk.core.command_files",
    "InputFileGeneratorSource": "hpcflow.sdk.core.command_files",
    "OutputFileParserSource": "hpcflow.sdk.core.command_files",
    "Command": "hpcflow.sdk.core.commands",
    "ElementInputs": "hpcflow.sdk.core.element",
    "ElementOutputs": "hpcflow.sdk.core.element",
    "ElementInputFiles": "hpcflow.sdk.core.element",
    "ElementOutputFiles": "hpcflow.sdk.core.element",
    "ElementResources": "hpcflow.sdk.core.element",
    "ElementIteration": "hpcflow.sdk.core.element",
    "Element": "hpcflow.sdk.core.element",
    "ElementParameter": "hpcflow.sdk.core.element",
    "ElementFilter": "hpcflow.sdk.core.element",
    "ElementGroup": "hpcflow.sdk.core.element",
    "ElementRepeats": "hpcflow.sdk.core.element",
    "NumCores": "hpcflow.sdk.core.environment",
    "ExecutableInstance": "hpcflow.sdk.core.environment",
    "Executable": "hpcflow.sdk.core.environment",
    "Executor": "hpcflow.sdk.core.execute",
    "Environment": "hpcflow.sdk.core.environment",
    "Loop": "hpcflow.sdk.core.loop",
    "WorkflowLoop": "hpcflow.sdk.core.loop",
    "TaskList": "hpcflow.sdk.core.object_list",
    "TaskTemplateList": "hpcflow.sdk.core.object_list",
    "TaskSchemasList": "hpcflow.sdk.core.object_list",
    "GroupList": "hpcflow.sdk.core.object_list",
    "EnvironmentsList": "hpcflow.sdk.core.object_list",
    "ExecutablesList": "hpcflow.sdk.core.object_list",
    "ParametersList": "hpcflow.sdk.core.object_list",
    "CommandFilesList": "hpcflow.sdk.core.object_list",
    "WorkflowTaskList": "hpcflow.sdk.core.object_list",
    "WorkflowLoopList": "hpcflow.sdk.core.object_list",
    "ResourceList": "hpcflow.sdk.core.object_list",
    "ParameterValue": "hpcflow.sdk.core.parameters",
    "ParameterPath": "hpcflow.sdk.core.parameters",
    "Parameter": "hpcflow.sdk.core.parameters",
    "SubParameter": "hpcflow.sdk.core.parameters",
    "SchemaParameter": "hpcflow.sdk.core.parameters",
    "SchemaInput": "hpcflow.sdk.core.parameters",
    "SchemaOutput": "hpcflow.sdk.core.parameters",
    "ValueSequence": "hpcflow.sdk.core.parameters",
    "MultiPathSequence": "hpcflow.sdk.core.parameters",
    "ValuePerturbation": "hpcflow.sdk.core.parameters",
    "InputValue": "hpcflow.sdk.core.parameters",
    "ResourceSpec": "hpcflow.sdk.core.parameters",
    "TaskSourceType": "hpcflow.sdk.core.enums",
    "InputSourceType": "hpcflow.sdk.core.enums",
    "ParameterPropagationMode": "hpcflow.sdk.core.enums",
    "InputSource": "hpcflow.sdk.core.parameters",
    "TaskObjective": "hpcflow.sdk.core.task_schema",
    "TaskSchema": "hpcflow.sdk.core.task_schema",
    "MetaTaskSchema": "hpcflow.sdk.core.task_schema",
    "ElementSet": "hpcflow.sdk.core.task",
    "Task": "hpcflow.sdk.core.task",
    "WorkflowTask": "hpcflow.sdk.core.task",
    "Elements": "hpcflow.sdk.core.task",
    "Parameters": "hpcflow.sdk.core.task",
    "TaskInputParameters": "hpcflow.sdk.core.task",
    "TaskOutputParameters": "hpcflow.sdk.core.task",
    "ElementPropagation": "hpcflow.sdk.core.task",
    "WorkflowTemplate": "hpcflow.sdk.core.workflow",
    "Workflow": "hpcflow.sdk.core.workflow",
    "WorkflowBlueprint": "hpcflow.sdk.core.workflow",
    "Jobscript": "hpcflow.sdk.submission.jobscript",
    "JobscriptBlock": "hpcflow.sdk.submission.jobscript",
    "Submission": "hpcflow.sdk.submission.submission",
    "QueuedScheduler": "hpcflow.sdk.submission.schedulers",
    "DirectWindows": "hpcflow.sdk.submission.schedulers.direct",
    "DirectPosix": "hpcflow.sdk.submission.schedulers.direct",
    "SlurmPosix": "hpcflow.sdk.submission.schedulers.slurm",
    "SGEPosix": "hpcflow.sdk.submission.schedulers.sge",
    "OutputLabel": "hpcflow.sdk.core.task",
    "RunDirAppFiles": "hpcflow.sdk.core.run_dir_files",
}

# these are defined as `BaseApp` methods with an underscore prefix:
#: Functions exported by the application.
#: :meta hide-value:
sdk_funcs: Final[tuple[str, ...]] = (
    "make_workflow",
    "make_demo_workflow",
    "make_and_submit_workflow",
    "make_and_submit_demo_workflow",
    "submit_workflow",
    "run_hpcflow_tests",
    "run_tests",
    "get_OS_info",
    "get_shell_info",
    "get_known_submissions",
    "show",
    "show_legend",
    "cancel",
)


def get_SDK_logger(name: str | None = None) -> logging.Logger:
    """Get a logger with prefix of "hpcflow_sdk" instead of "hpcflow.sdk" to ensure the
    handlers of the SDK logger and app logger are distinct."""
    name = ".".join(["hpcflow_sdk"] + (name or __name__).split(".")[2:])
    return logging.getLogger(name)


def _init_logger() -> None:
    level = os.environ.get("HPCFLOW_SDK_CONSOLE_LOG_LEVEL", "ERROR")
    SDK_logger = get_SDK_logger()
    SDK_logger.setLevel("DEBUG")

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    sh.setLevel(level)
    SDK_logger.addHandler(sh)


_init_logger()
if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    import multiprocessing

    multiprocessing.freeze_support()
