"""
An interface to SGE.
"""

from __future__ import annotations
from collections.abc import Sequence
import re
from typing import cast, TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import (
    IncompatibleSGEPEError,
    NoCompatibleSGEPEError,
    UnknownSGEPEError,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers import QueuedScheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import Any, ClassVar
    from ...config.types import SchedulerConfigDescriptor
    from ...core.element import ElementResources
    from ..jobscript import Jobscript
    from ..types import VersionInfo
    from ..shells.base import Shell


@hydrate
class SGEPosix(QueuedScheduler):
    """
    A scheduler that uses SGE.

    Keyword Args
    ------------
    cwd_switch: str
        Override of default switch to use to set the current working directory.
    directives: dict
        Scheduler directives. Each item is written verbatim in the jobscript as a
        scheduler directive, and is not processed in any way. If a value is `None`, the
        key is considered a flag-like directive. If a value is a list, multiple directives
        will be printed to the jobscript with the same key, but different values.

    Notes
    -----
    - runs in serial by default

    References
    ----------
    [1] https://gridscheduler.sourceforge.net/htmlman/htmlman1/qsub.html
    [2] https://softpanorama.org/HPC/Grid_engine/Queues/queue_states.shtml

    """

    #: Default submission command.
    DEFAULT_SUBMIT_CMD: ClassVar[str] = "qsub"
    #: Default command to show the queue state.
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]] = ("qstat",)
    #: Default cancel command.
    DEFAULT_DEL_CMD: ClassVar[str] = "qdel"
    #: Default job control directive prefix.
    DEFAULT_JS_CMD: ClassVar[str] = "#$"
    #: Default prefix to enable array processing.
    DEFAULT_ARRAY_SWITCH: ClassVar[str] = "-t"
    #: Default shell variable with array ID.
    DEFAULT_ARRAY_ITEM_VAR: ClassVar[str] = "SGE_TASK_ID"
    #: Default switch to control CWD.
    DEFAULT_CWD_SWITCH: ClassVar[str] = "-cwd"
    #: Default command to get the login nodes.
    DEFAULT_LOGIN_NODES_CMD: ClassVar[Sequence[str]] = ("qconf", "-sh")

    #: Maps scheduler state codes to :py:class:`JobscriptElementState` values.
    state_lookup: ClassVar[Mapping[str, JobscriptElementState]] = {
        "qw": JobscriptElementState.pending,
        "hq": JobscriptElementState.waiting,
        "hR": JobscriptElementState.waiting,
        "r": JobscriptElementState.running,
        "t": JobscriptElementState.running,
        "Rr": JobscriptElementState.running,
        # "Rt": JobscriptElementState.running,
        "s": JobscriptElementState.errored,
        "ts": JobscriptElementState.errored,
        "S": JobscriptElementState.errored,
        "tS": JobscriptElementState.errored,
        "T": JobscriptElementState.errored,
        "tT": JobscriptElementState.errored,
        "Rs": JobscriptElementState.errored,
        "Rt": JobscriptElementState.errored,
        "RS": JobscriptElementState.errored,
        "RT": JobscriptElementState.errored,
        "Eq": JobscriptElementState.errored,
        "Eh": JobscriptElementState.errored,
        "dr": JobscriptElementState.cancelled,
        "dt": JobscriptElementState.cancelled,
        "dR": JobscriptElementState.cancelled,
        "ds": JobscriptElementState.cancelled,
        "dS": JobscriptElementState.cancelled,
        "dT": JobscriptElementState.cancelled,
    }

    def __init__(self, cwd_switch: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cwd_switch = cwd_switch or self.DEFAULT_CWD_SWITCH

    @classmethod
    @override
    @TimeIt.decorator
    def process_resources(
        cls, resources: ElementResources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """
        Perform scheduler-specific processing to the element resources.

        Note
        ----
        This mutates `resources`.
        """
        if resources.num_nodes is not None:
            raise ValueError(
                f"Specifying `num_nodes` for the {cls.__name__!r} scheduler is not "
                f"supported."
            )

        para_envs = scheduler_config.get("parallel_environments", {})

        if resources.SGE_parallel_env is not None:
            # check user-specified `parallel_env` is valid and compatible with
            # `num_cores`:
            if resources.num_cores and resources.num_cores == 1:
                raise ValueError(
                    f"An SGE parallel environment should not be specified if `num_cores` "
                    f"is 1 (`SGE_parallel_env` was specified as "
                    f"{resources.SGE_parallel_env!r})."
                )

            try:
                env = para_envs[resources.SGE_parallel_env]
            except KeyError:
                raise UnknownSGEPEError(resources.SGE_parallel_env, para_envs)
            if not cls.is_num_cores_supported(resources.num_cores, env["num_cores"]):
                raise IncompatibleSGEPEError(
                    resources.SGE_parallel_env, resources.num_cores
                )
        else:
            # find the first compatible PE:
            for pe_name, pe_info in para_envs.items():
                if cls.is_num_cores_supported(resources.num_cores, pe_info["num_cores"]):
                    resources.SGE_parallel_env = pe_name
                    break
            else:
                raise NoCompatibleSGEPEError(resources.num_cores)

    def get_login_nodes(self) -> list[str]:
        """Return a list of hostnames of login/administrative nodes as reported by the
        scheduler."""
        get_login = self.login_nodes_cmd
        assert get_login is not None and len(get_login) >= 1
        stdout, stderr = run_cmd(get_login)
        if stderr:
            print(stderr)
        return stdout.strip().split("\n")

    def __format_core_request_lines(self, resources: ElementResources) -> Iterator[str]:
        if resources.num_cores and resources.num_cores > 1:
            yield f"{self.js_cmd} -pe {resources.SGE_parallel_env} {resources.num_cores}"
        if resources.max_array_items:
            yield f"{self.js_cmd} -tc {resources.max_array_items}"

    def __format_array_request(self, num_elements: int) -> str:
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}"

    def get_stdout_filename(
        self, js_idx: int, job_ID: str, array_idx: int | None = None
    ) -> str:
        """File name of the standard output stream file."""
        # TODO: untested, might not work!
        array_idx_str = f".{array_idx}" if array_idx is not None else ""
        return f"js_{js_idx}.sh.o{job_ID}{array_idx_str}"

    def get_stderr_filename(
        self, js_idx: int, job_ID: str, array_idx: int | None = None
    ) -> str:
        """File name of the standard error stream file."""
        # TODO: untested, might not work!
        array_idx_str = f".{array_idx}" if array_idx is not None else ""
        return f"js_{js_idx}.sh.e{job_ID}{array_idx_str}"

    def __format_std_stream_file_option_lines(
        self, is_array: bool, sub_idx: int, js_idx: int, combine_std: bool
    ) -> Iterator[str]:
        # note: if we modify the file names, there is, I believe, no way to include the
        # job ID; so we don't modify the file names:
        base = f"./artifacts/submissions/{sub_idx}/js_std/{js_idx}"
        yield f"{self.js_cmd} -o {base}"
        if combine_std:
            yield f"{self.js_cmd} -j y"  # redirect stderr to stdout
        else:
            yield f"{self.js_cmd} -e {base}"

    @override
    def format_directives(
        self,
        resources: ElementResources,
        num_elements: int,
        is_array: bool,
        sub_idx: int,
        js_idx: int,
    ) -> str:
        """
        Format the directives to the jobscript command.
        """
        opts: list[str] = []
        opts.append(self.format_switch(self.cwd_switch))
        opts.extend(self.__format_core_request_lines(resources))
        if is_array:
            opts.append(self.__format_array_request(num_elements))

        opts.extend(
            self.__format_std_stream_file_option_lines(
                is_array, sub_idx, js_idx, resources.combine_jobscript_std
            )
        )

        for opt_k, opt_v in self.directives.items():
            if opt_v is None:
                opts.append(f"{self.js_cmd} {opt_k}")
            elif isinstance(opt_v, list):
                opts.extend(f"{self.js_cmd} {opt_k} {i}" for i in opt_v)
            elif opt_v:
                opts.append(f"{self.js_cmd} {opt_k} {opt_v}")

        return "\n".join(opts) + "\n"

    @override
    @TimeIt.decorator
    def get_version_info(self) -> VersionInfo:
        stdout, stderr = run_cmd([*self.show_cmd, "-help"])
        if stderr:
            print(stderr)
        first_line, *_ = stdout.split("\n")
        name, version, *_ = first_line.strip().split()
        return {
            "scheduler_name": name,
            "scheduler_version": version,
        }

    @override
    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: dict[Any, tuple[Any, ...]],
    ) -> list[str]:
        """
        Get the command to use to submit a job to the scheduler.

        Returns
        -------
        List of argument words.
        """
        cmd = [self.submit_cmd, "-terse"]

        dep_job_IDs: list[str] = []
        dep_job_IDs_arr: list[str] = []
        for job_ID, is_array_dep in deps.values():
            if is_array_dep:  # array dependency
                dep_job_IDs_arr.append(str(job_ID))
            else:
                dep_job_IDs.append(str(job_ID))

        if dep_job_IDs:
            cmd.append("-hold_jid")
            cmd.append(",".join(dep_job_IDs))

        if dep_job_IDs_arr:
            cmd.append("-hold_jid_ad")
            cmd.append(",".join(dep_job_IDs_arr))

        cmd.append(js_path)
        return cmd

    __SGE_JOB_ID_RE: ClassVar[re.Pattern] = re.compile(r"^\d+")

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        if not (match := self.__SGE_JOB_ID_RE.search(stdout)):
            raise RuntimeError(f"Could not parse Job ID from scheduler output {stdout!r}")
        return match.group()

    def get_job_statuses(
        self,
    ) -> Mapping[str, JobscriptElementState | Mapping[int, JobscriptElementState]]:
        """Get information about all of this user's jobscripts that are currently listed
        by the scheduler."""
        cmd = [*self.show_cmd, "-u", "$USER", "-g", "d"]  # "-g d": separate arrays items
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query SGE jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        elif not stdout:
            return {}

        info: dict[str, dict[int, JobscriptElementState] | JobscriptElementState] = {}
        lines = stdout.split("\n")
        # assuming a job name with spaces means we can't split on spaces to get
        # anywhere beyond the job name, so get the column index of the state heading
        # and assume the state is always left-aligned with the heading:
        state_idx = lines[0].index("state")
        task_id_idx = lines[0].index("ja-task-ID")
        for ln in lines[2:]:
            if not ln:
                continue
            base_job_ID, *_ = ln.split()

            # states can be one or two chars (for our limited purposes):
            state_str = ln[state_idx : state_idx + 2].strip()
            state = self.state_lookup[state_str]

            arr_idx_s = ln[task_id_idx:].strip()
            arr_idx = (
                int(arr_idx_s) - 1  # We are using zero-indexed info
                if arr_idx_s
                else None
            )

            if arr_idx is not None:
                entry = cast(
                    dict[int, JobscriptElementState], info.setdefault(base_job_ID, {})
                )
                entry[arr_idx] = state
            else:
                info[base_job_ID] = state
        return info

    @override
    def get_job_state_info(
        self, *, js_refs: Sequence[str] | None = None
    ) -> Mapping[str, JobscriptElementState | Mapping[int, JobscriptElementState]]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method.

        """
        info = self.get_job_statuses()
        if js_refs:
            return {k: v for k, v in info.items() if k in js_refs}
        return info

    @override
    def cancel_jobs(
        self,
        js_refs: list[str],
        jobscripts: list[Jobscript] | None = None,
    ):
        """
        Cancel submitted jobs.
        """
        cmd = [self.del_cmd] + js_refs
        self._app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query SGE {self.__class__.__name__}. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self._app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
