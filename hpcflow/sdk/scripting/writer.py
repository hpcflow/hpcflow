from __future__ import annotations


from dataclasses import dataclass
from collections.abc import Mapping
import hashlib
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pathlib import Path
    from hpcflow.sdk.core.actions import Action
    from hpcflow.sdk.core.environment import Environment, EnvironmentSpec
    from hpcflow.sdk.submission.shells.base import Shell

SCRIPT_WORKER_HASH_LENGTH = 10
SCRIPT_WORKER_SHELL_NAMES = frozenset(("bash", "wsl+bash", "wsl", "powershell"))


@dataclass(frozen=True)
class ScriptWorkerArgs:
    """Interpreter, environment, and jobscript identity for one worker."""

    jobscript_idx: int
    env: Environment
    exec_cmd: str
    language: str
    shell_name: str = "powershell"

    @property
    def stable_hash(self) -> str:
        """Return a deterministic short identity for worker grouping."""
        to_encode = (
            self.jobscript_idx,
            self.language,
            self.env.spec.as_tuple(),
            self.exec_cmd,
            self.shell_name,
        )
        return hashlib.sha256(repr(to_encode).encode()).hexdigest()[
            :SCRIPT_WORKER_HASH_LENGTH
        ]

    def __hash__(self) -> int:
        """Hash the stable worker identity for use as a mapping key."""
        return int(self.stable_hash, 16)

    @property
    def id(self) -> str:
        """Return the short stable worker identifier."""
        return self.stable_hash

    @property
    def base_file_name(self) -> str:
        """Return the common basename for this worker's artifacts."""
        return f"js_{self.jobscript_idx}_{self.id}"

    def get_script_path(self, path: Path):
        """Return the generated worker script path inside ``path``."""
        return path / f"{self.base_file_name}.py"

    def get_commands_path(self, path: Path):
        """Return the generated worker command-file path inside ``path``."""
        if self.shell_name in ("bash", "wsl+bash", "wsl"):
            suffix = ".sh"
        elif self.shell_name == "powershell":
            suffix = ".ps1"
        else:
            raise ValueError(f"Unsupported script-worker shell: {self.shell_name!r}.")
        return path / f"{self.base_file_name}{suffix}"

    def get_manifest_path(self, path: Path):
        """Return the generated worker manifest path inside ``path``."""
        return path / f"{self.base_file_name}.json"


@dataclass
class ScriptWriter:
    """Base class for generating interpreter-specific script workers."""

    worker_args: ScriptWorkerArgs
    actions: Mapping[int, Action] | Mapping[tuple[int, int], Action]

    _language: ClassVar[str]
    _registry: ClassVar[dict[str, type["ScriptWriter"]]] = {}

    def __init_subclass__(cls, **kwargs):
        """Register subclasses that declare a worker language."""
        super().__init_subclass__(**kwargs)

        language = getattr(cls, "_language", None)
        if language is not None:
            ScriptWriter._registry[language] = cls

    @property
    def jobscript_idx(self):
        """Return the parent jobscript index."""
        return self.worker_args.jobscript_idx

    @property
    def env(self):
        """Return the worker's configured environment."""
        return self.worker_args.env

    @property
    def env_spec(self) -> EnvironmentSpec:
        """Return the hashable environment specification."""
        return self.env.spec

    @property
    def exec_cmd(self):
        """Return the configured interpreter command."""
        return self.worker_args.exec_cmd

    @property
    def language(self):
        """Return the script language implemented by this writer."""
        return self.worker_args.language

    @classmethod
    def create(
        cls,
        worker_args: ScriptWorkerArgs,
        actions: Mapping[int, Action] | Mapping[tuple[int, int], Action],
    ):
        """Create the registered writer for the worker language."""
        try:
            writer_cls = cls._registry[worker_args.language]
        except KeyError:
            raise ValueError(f"Unknown script language: {worker_args.language!r}")

        return writer_cls(worker_args=worker_args, actions=actions)

    def compose_worker(self) -> str:
        """Compose source code for the worker module."""
        raise NotImplementedError("Script writers must implement worker generation.")

    def write_worker(self, path: Path):
        """Write the generated worker source into the scripts directory."""
        script_str = self.compose_worker()
        script_path = self.worker_args.get_script_path(path)
        with script_path.open("wt", encoding="utf-8", newline="\n") as fp:
            fp.write(script_str)

    def write_worker_commands_file(
        self,
        path: Path,
        *,
        script_path: Path | None = None,
        shell: Shell | None = None,
    ) -> None:
        """Write environment setup and a shell-quoted worker invocation."""
        command_lns = []
        if self.env.setup:
            command_lns.extend(self.env.setup)

        command = self.exec_cmd
        if script_path is not None:
            if command.count("<<script_path>>") != 1:
                raise ValueError(
                    "Worker executable command must contain exactly one "
                    "'<<script_path>>' placeholder."
                )
            if shell is None:
                raise ValueError("A shell is required to quote the worker script path.")
            quoted_script_path = shell.quote_arg(shell.prepare_JS_path(script_path))
            placeholder = "<<script_path>>"
            for quote in ("'", '"'):
                quoted_placeholder = f"{quote}{placeholder}{quote}"
                if quoted_placeholder in command:
                    command = command.replace(quoted_placeholder, quoted_script_path)
                    break
            else:
                command = command.replace(placeholder, quoted_script_path)
            command = command.replace("<<args>>", "")
            command = command.rstrip()
        elif "<<script_path>>" in command:
            raise ValueError(
                "A script_path is required to resolve the worker command template."
            )

        command_lns.append(command)

        cmds_path = self.worker_args.get_commands_path(path)
        with cmds_path.open("wt", encoding="utf-8", newline="\n") as fp:
            fp.write("\n".join(command_lns) + "\n")

    def write_worker_manifest(
        self,
        path: Path,
        *,
        workflow_path: str | Path,
        submission_idx: int,
        config_directory: str | Path,
        config_key: str,
    ) -> None:
        """Write the persisted workflow identity needed by a generated worker."""
        if self.language != "python":
            raise ValueError("Script-worker bootstrap is only implemented for Python.")

        from hpcflow.sdk.execution.script_worker.bootstrap import (
            ScriptWorkerManifest,
        )

        action_keys: list[tuple[int, int]] = []
        for key in self.actions:
            if not isinstance(key, tuple) or len(key) != 2:
                raise ValueError(
                    "Worker manifests require nonempty (block_idx, action_idx) keys."
                )
            block_idx, action_idx = key
            if (
                type(block_idx) is not int
                or block_idx < 0
                or type(action_idx) is not int
                or action_idx < 0
            ):
                raise ValueError(
                    "Worker manifests require nonempty (block_idx, action_idx) keys."
                )
            action_keys.append((block_idx, action_idx))
        if not action_keys:
            raise ValueError(
                "Worker manifests require nonempty (block_idx, action_idx) keys."
            )
        manifest = ScriptWorkerManifest(
            workflow_path=str(workflow_path),
            submission_idx=submission_idx,
            jobscript_idx=self.jobscript_idx,
            worker_id=self.worker_args.stable_hash,
            action_keys=tuple(action_keys),
            config_directory=str(config_directory),
            config_key=config_key,
        )
        manifest.write(self.worker_args.get_manifest_path(path))
