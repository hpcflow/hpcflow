from __future__ import annotations


from dataclasses import dataclass
import hashlib
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pathlib import Path
    from hpcflow.sdk.core.actions import Action
    from hpcflow.sdk.core.environment import Environment, EnvironmentSpec

SCRIPT_WORKER_HASH_LENGTH = 10


@dataclass(frozen=True)
class ScriptWorkerArgs:

    jobscript_idx: int
    env: Environment
    exec_cmd: str
    language: str

    @property
    def stable_hash(self) -> str:
        to_encode = (
            self.jobscript_idx,
            self.language,
            self.env.spec.as_tuple(),
            self.exec_cmd,
        )
        return hashlib.sha256(repr(to_encode).encode()).hexdigest()[
            :SCRIPT_WORKER_HASH_LENGTH
        ]

    def __hash__(self) -> int:
        return int(self.stable_hash, 16)

    @property
    def id(self) -> str:
        return self.stable_hash

    @property
    def base_file_name(self) -> str:
        return f"js_{self.jobscript_idx}_{self.id}"

    def get_script_path(self, path: Path):
        return path / f"{self.base_file_name}.py"

    def get_commands_path(self, path: Path):
        return path / f"{self.base_file_name}.ps1"


@dataclass
class ScriptWriter:

    worker_args: ScriptWorkerArgs
    actions: dict[int, Action]

    _language: ClassVar[str]
    _registry: ClassVar[dict[str, type["ScriptWriter"]]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        language = getattr(cls, "_language", None)
        if language is not None:
            ScriptWriter._registry[language] = cls

    @property
    def jobscript_idx(self):
        return self.worker_args.jobscript_idx

    @property
    def env(self):
        return self.worker_args.env

    @property
    def env_spec(self) -> EnvironmentSpec:
        return self.env.spec

    @property
    def exec_cmd(self):
        return self.worker_args.exec_cmd

    @property
    def language(self):
        return self.worker_args.language

    @classmethod
    def create(
        cls,
        worker_args: ScriptWorkerArgs,
        actions: dict[int, Action],
    ):
        """Initialise the correct script writer subclass according to the environment
        specification name."""
        try:
            writer_cls = cls._registry[worker_args.language]
        except KeyError:
            raise ValueError(f"Unknown script language: {worker_args.language!r}")

        return writer_cls(worker_args=worker_args, actions=actions)

    def compose_worker(self) -> str:
        raise NotImplementedError("Script writers must implement worker generation.")

    def write_worker(self, path: Path):
        script_str = self.compose_worker()
        script_path = self.worker_args.get_script_path(path)
        with script_path.open("wt", encoding="utf-8", newline="\n") as fp:
            fp.write(script_str)

    def write_worker_commands_file(self, path: Path):

        command_lns = []
        if self.env.setup:
            command_lns.extend(self.env.setup)

        command_lns.append(self.exec_cmd)

        cmds_path = self.worker_args.get_commands_path(path)
        with cmds_path.open("wt", encoding="utf-8", newline="\n") as fp:
            fp.write("\n".join(command_lns) + "\n")
