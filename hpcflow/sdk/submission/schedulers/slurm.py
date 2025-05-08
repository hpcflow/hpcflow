"""
An interface to SLURM.
"""

from __future__ import annotations
import subprocess
import time
from typing import cast, TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.enums import ParallelMode
from hpcflow.sdk.core.errors import (
    IncompatibleParallelModeError,
    IncompatibleSLURMArgumentsError,
    IncompatibleSLURMPartitionError,
    UnknownSLURMPartitionError,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers import QueuedScheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar
    from ...config.types import SchedulerConfigDescriptor, SLURMPartitionsDescriptor
    from ...core.element import ElementResources
    from ..jobscript import Jobscript
    from ..types import VersionInfo
    from ..shells.base import Shell


@hydrate
class SlurmPosix(QueuedScheduler):
    """
    A scheduler that uses SLURM.

    Keyword Args
    ------------
    directives: dict
        Scheduler directives. Each item is written verbatim in the jobscript as a
        scheduler directive, and is not processed in any way. If a value is `None`, the
        key is considered a flag-like directive. If a value is a list, multiple directives
        will be printed to the jobscript with the same key, but different values.

    Notes
    -----
    - runs in current working directory by default [2]

    Todo
    ----
    - consider getting memory usage like: https://stackoverflow.com/a/44143229/5042280

    References
    ----------
    [1] https://manpages.org/sbatch
    [2] https://ri.itservices.manchester.ac.uk/csf4/batch/sge-to-slurm/

    """

    #: Default submission command.
    DEFAULT_SUBMIT_CMD: ClassVar[str] = "sbatch"
    #: Default command to show the queue state.
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]] = ("squeue", "--me")
    #: Default cancel command.
    DEFAULT_DEL_CMD: ClassVar[str] = "scancel"
    #: Default job control directive prefix.
    DEFAULT_JS_CMD: ClassVar[str] = "#SBATCH"
    #: Default prefix to enable array processing.
    DEFAULT_ARRAY_SWITCH: ClassVar[str] = "--array"
    #: Default shell variable with array ID.
    DEFAULT_ARRAY_ITEM_VAR: ClassVar[str] = "SLURM_ARRAY_TASK_ID"
    #: Number of times to try when querying the state.
    NUM_STATE_QUERY_TRIES: ClassVar[int] = 5
    #: Delay (in seconds) between attempts to query the state.
    INTER_STATE_QUERY_DELAY: ClassVar[float] = 0.5

    #: Maps scheduler state codes to :py:class:`JobscriptElementState` values.
    state_lookup: ClassVar[Mapping[str, JobscriptElementState]] = {
        "PENDING": JobscriptElementState.pending,
        "RUNNING": JobscriptElementState.running,
        "COMPLETING": JobscriptElementState.running,
        "CANCELLED": JobscriptElementState.cancelled,
        "COMPLETED": JobscriptElementState.finished,
        "FAILED": JobscriptElementState.errored,
        "OUT_OF_MEMORY": JobscriptElementState.errored,
        "TIMEOUT": JobscriptElementState.errored,
    }

    @classmethod
    @override
    @TimeIt.decorator
    def process_resources(
        cls, resources: ElementResources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """Perform scheduler-specific processing to the element resources.

        Note
        ----
        This mutates `resources`.
        """
        if resources.is_parallel:
            if resources.parallel_mode is None:
                # set default parallel mode:
                resources.parallel_mode = ParallelMode.DISTRIBUTED

            if resources.parallel_mode is ParallelMode.SHARED:
                if (resources.num_nodes and resources.num_nodes > 1) or (
                    resources.SLURM_num_nodes and resources.SLURM_num_nodes > 1
                ):
                    raise IncompatibleParallelModeError(resources.parallel_mode)
                # consider `num_cores` and `num_threads` synonyms in this case:
                if resources.SLURM_num_tasks and resources.SLURM_num_tasks != 1:
                    raise IncompatibleSLURMArgumentsError(
                        f"For the {resources.parallel_mode.name.lower()} parallel mode, "
                        f"`SLURM_num_tasks` must be set to 1 (to ensure all requested "
                        f"cores reside on the same node)."
                    )
                resources.SLURM_num_tasks = 1

                if resources.SLURM_num_cpus_per_task == 1:
                    raise IncompatibleSLURMArgumentsError(
                        f"For the {resources.parallel_mode.name.lower()} parallel mode, "
                        f"if `SLURM_num_cpus_per_task` is set, it must be set to the "
                        f"number of threads/cores to use, and so must be greater than 1, "
                        f"but {resources.SLURM_num_cpus_per_task!r} was specified."
                    )
                resources.num_threads = resources.num_threads or resources.num_cores
                if not resources.num_threads and not resources.SLURM_num_cpus_per_task:
                    raise ValueError(
                        f"For the {resources.parallel_mode.name.lower()} parallel "
                        f"mode, specify `num_threads` (or its synonym for this "
                        f"parallel mode: `num_cores`), or the SLURM-specific "
                        f"parameter `SLURM_num_cpus_per_task`."
                    )
                elif (resources.num_threads and resources.SLURM_num_cpus_per_task) and (
                    resources.num_threads != resources.SLURM_num_cpus_per_task
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores`/`num_threads` "
                        f"({resources.num_threads}) and `SLURM_num_cpus_per_task` "
                        f"({resources.SLURM_num_cpus_per_task}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                resources.SLURM_num_cpus_per_task = resources.num_threads

            elif resources.parallel_mode is ParallelMode.DISTRIBUTED:
                if resources.num_threads:
                    raise ValueError(
                        f"For the {resources.parallel_mode.name.lower()} parallel "
                        f"mode, specifying `num_threads` is not permitted."
                    )
                if (
                    resources.SLURM_num_tasks
                    and resources.num_cores
                    and resources.SLURM_num_tasks != resources.num_cores
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores` ({resources.num_cores})"
                        f" and `SLURM_num_tasks` ({resources.SLURM_num_tasks}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif not resources.SLURM_num_tasks and resources.num_cores:
                    resources.SLURM_num_tasks = resources.num_cores
                elif (
                    resources.SLURM_num_tasks_per_node
                    and resources.num_cores_per_node
                    and resources.SLURM_num_tasks_per_node != resources.num_cores_per_node
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores_per_node` "
                        f"({resources.num_cores_per_node}) and `SLURM_num_tasks_per_node`"
                        f" ({resources.SLURM_num_tasks_per_node}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif (
                    not resources.SLURM_num_tasks_per_node
                    and resources.num_cores_per_node
                ):
                    resources.SLURM_num_tasks_per_node = resources.num_cores_per_node

                if (
                    resources.SLURM_num_nodes
                    and resources.num_nodes
                    and resources.SLURM_num_nodes != resources.num_nodes
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_nodes` ({resources.num_nodes})"
                        f" and `SLURM_num_nodes` ({resources.SLURM_num_nodes}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif not resources.SLURM_num_nodes and resources.num_nodes:
                    resources.SLURM_num_nodes = resources.num_nodes

            elif resources.parallel_mode is ParallelMode.HYBRID:
                raise NotImplementedError("hybrid parallel mode not yet supported.")

        else:
            if resources.SLURM_is_parallel:
                raise IncompatibleSLURMArgumentsError(
                    "Some specified SLURM-specific arguments (which indicate a parallel "
                    "job) conflict with the scheduler-agnostic arguments (which "
                    "indicate a serial job)."
                )
            if not resources.SLURM_num_tasks:
                resources.SLURM_num_tasks = 1

            if resources.SLURM_num_tasks_per_node:
                resources.SLURM_num_tasks_per_node = None

            if not resources.SLURM_num_nodes:
                resources.SLURM_num_nodes = 1

            if not resources.SLURM_num_cpus_per_task:
                resources.SLURM_num_cpus_per_task = 1

        num_cores = resources.num_cores or resources.SLURM_num_tasks
        num_cores_per_node = (
            resources.num_cores_per_node or resources.SLURM_num_tasks_per_node
        )
        num_nodes = resources.num_nodes or resources.SLURM_num_nodes
        para_mode = resources.parallel_mode

        # select matching partition if possible:
        all_parts = scheduler_config.get("partitions", {})
        if resources.SLURM_partition is not None:
            # check user-specified partition is valid and compatible with requested
            # cores/nodes:
            try:
                part = all_parts[resources.SLURM_partition]
            except KeyError:
                raise UnknownSLURMPartitionError(resources.SLURM_partition, all_parts)
            # TODO: we when we support ParallelMode.HYBRID, these checks will have to
            # consider the total number of cores requested per node
            # (num_cores_per_node * num_threads)?
            part_num_cores = part.get("num_cores", ())
            part_num_cores_per_node = part.get("num_cores_per_node", ())
            part_num_nodes = part.get("num_nodes", ())
            part_para_modes = part.get("parallel_modes", ())
            if cls.__is_present_unsupported(num_cores, part_num_cores):
                raise IncompatibleSLURMPartitionError(
                    resources.SLURM_partition, "number of cores", num_cores
                )
            if cls.__is_present_unsupported(num_cores_per_node, part_num_cores_per_node):
                raise IncompatibleSLURMPartitionError(
                    resources.SLURM_partition,
                    "number of cores per node",
                    num_cores_per_node,
                )
            if cls.__is_present_unsupported(num_nodes, part_num_nodes):
                raise IncompatibleSLURMPartitionError(
                    resources.SLURM_partition, "number of nodes", num_nodes
                )
            if para_mode and para_mode.name.lower() not in part_para_modes:
                raise IncompatibleSLURMPartitionError(
                    resources.SLURM_partition, "parallel mode", para_mode
                )
        else:
            # find the first compatible partition if one exists:
            # TODO: bug here? not finding correct partition?
            for part_name, part_info in all_parts.items():
                if cls.__partition_matches(
                    num_cores, num_cores_per_node, num_nodes, para_mode, part_info
                ):
                    resources.SLURM_partition = str(part_name)
                    break

    @classmethod
    def __is_present_unsupported(
        cls, num_req: int | None, part_have: Sequence[int] | None
    ) -> bool:
        """
        Test if information is present on both sides, but doesn't match.
        """
        return bool(
            num_req and part_have and not cls.is_num_cores_supported(num_req, part_have)
        )

    @classmethod
    def __is_present_supported(
        cls, num_req: int | None, part_have: Sequence[int] | None
    ) -> bool:
        """
        Test if information is present on both sides, and also matches.
        """
        return bool(
            num_req and part_have and cls.is_num_cores_supported(num_req, part_have)
        )

    @classmethod
    def __partition_matches(
        cls,
        num_cores: int | None,
        num_cores_per_node: int | None,
        num_nodes: int | None,
        para_mode: ParallelMode | None,
        part_info: SLURMPartitionsDescriptor,
    ) -> bool:
        """
        Check whether a partition (part_name, part_info) matches the requested number
        of cores and nodes.
        """
        part_num_cores = part_info.get("num_cores", [])
        part_num_cores_per_node = part_info.get("num_cores_per_node", [])
        part_num_nodes = part_info.get("num_nodes", [])
        part_para_modes = part_info.get("parallel_modes", [])
        if (
            not cls.__is_present_supported(num_cores, part_num_cores)
            or not cls.__is_present_supported(num_cores_per_node, part_num_cores_per_node)
            or not cls.__is_present_supported(num_nodes, part_num_nodes)
        ):
            return False
        # FIXME: Does the next check come above or below the check below?
        # Surely not both!
        part_match = True
        if part_match:
            return True
        if para_mode and para_mode.name.lower() not in part_para_modes:
            return False
        if part_match:
            return True
        return False

    def __format_core_request_lines(self, resources: ElementResources) -> Iterator[str]:
        if resources.SLURM_partition:
            yield f"{self.js_cmd} --partition {resources.SLURM_partition}"
        if resources.SLURM_num_nodes:  # TODO: option for --exclusive ?
            yield f"{self.js_cmd} --nodes {resources.SLURM_num_nodes}"
        if resources.SLURM_num_tasks:
            yield f"{self.js_cmd} --ntasks {resources.SLURM_num_tasks}"
        if resources.SLURM_num_tasks_per_node:
            yield f"{self.js_cmd} --ntasks-per-node {resources.SLURM_num_tasks_per_node}"
        if resources.SLURM_num_cpus_per_task:
            yield f"{self.js_cmd} --cpus-per-task {resources.SLURM_num_cpus_per_task}"

    def __format_array_request(self, num_elements: int, resources: ElementResources):
        # TODO: Slurm docs start indices at zero, why are we starting at one?
        #   https://slurm.schedmd.com/sbatch.html#OPT_array
        max_str = f"%{resources.max_array_items}" if resources.max_array_items else ""
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}{max_str}"

    def get_stdout_filename(
        self, js_idx: int, job_ID: str, array_idx: int | None = None
    ) -> str:
        """File name of the standard output stream file."""
        array_idx_str = f".{array_idx}" if array_idx is not None else ""
        return f"js_{js_idx}.sh_{job_ID}{array_idx_str}.out"

    def get_stderr_filename(
        self, js_idx: int, job_ID: str, array_idx: int | None = None
    ) -> str:
        """File name of the standard error stream file."""
        array_idx_str = f".{array_idx}" if array_idx is not None else ""
        return f"js_{js_idx}.sh_{job_ID}{array_idx_str}.err"

    def __format_std_stream_file_option_lines(
        self, is_array: bool, sub_idx: int, js_idx: int, combine_std: bool
    ) -> Iterator[str]:
        pattern = R"%x_%A.%a" if is_array else R"%x_%j"
        base = f"./artifacts/submissions/{sub_idx}/js_std/{js_idx}/{pattern}"
        yield f"{self.js_cmd} --output {base}.out"
        if not combine_std:
            yield f"{self.js_cmd} --error {base}.err"

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
        Format the directives to the scheduler.
        """
        opts: list[str] = []
        opts.extend(self.__format_core_request_lines(resources))

        if is_array:
            opts.append(self.__format_array_request(num_elements, resources))

        opts.extend(
            self.__format_std_stream_file_option_lines(
                is_array, sub_idx, js_idx, resources.combine_jobscript_std
            )
        )

        for opt_k, opt_v in self.directives.items():
            if isinstance(opt_v, list):
                for i in opt_v:
                    opts.append(f"{self.js_cmd} {opt_k} {i}")
            elif opt_v:
                opts.append(f"{self.js_cmd} {opt_k} {opt_v}")
            elif opt_v is None:
                opts.append(f"{self.js_cmd} {opt_k}")

        return "\n".join(opts) + "\n"

    @override
    @TimeIt.decorator
    def get_version_info(self) -> VersionInfo:
        vers_cmd = [self.submit_cmd, "--version"]
        proc = subprocess.run(
            args=vers_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode().strip()
        stderr = proc.stderr.decode().strip()
        if stderr:
            print(stderr)
        name, version = stdout.split()
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
        cmd = [self.submit_cmd, "--parsable"]
        if deps:
            cmd.append("--dependency")
            cmd.append(",".join(self.__dependency_args(deps)))
        cmd.append(js_path)
        return cmd

    @staticmethod
    def __dependency_args(deps: dict[Any, tuple[Any, ...]]) -> Iterator[str]:
        for job_ID, is_array_dep in deps.values():
            if is_array_dep:  # array dependency
                yield f"aftercorr:{job_ID}"
            else:
                yield f"afterany:{job_ID}"

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        if ";" in stdout:
            return stdout.split(";")[0]  # since we submit with "--parsable"
        # Try using the whole thing
        return stdout

    @staticmethod
    def _parse_job_IDs(job_ID_str: str) -> tuple[str, None | list[int]]:
        """
        Parse the job ID column from the `squeue` command (the `%i` format option).

        Returns
        -------
        job_id
            The job identifier.
        array_indices
            The indices into the job array.
        """
        base_job_ID, *arr_idx_data = job_ID_str.split("_")
        if not arr_idx_data:
            return base_job_ID, None
        arr_idx = arr_idx_data[0]
        try:
            return base_job_ID, [int(arr_idx) - 1]  # zero-index
        except ValueError:
            pass
        # split on commas (e.g. "[5,8-40]")
        _arr_idx: list[int] = []
        for i_range_str in arr_idx.strip("[]").split(","):
            if "-" in i_range_str:
                _from, _to = i_range_str.split("-")
                if "%" in _to:
                    # indicates max concurrent array items; not needed
                    _to = _to.split("%")[0]
                _arr_idx.extend(range(int(_from) - 1, int(_to)))
            else:
                _arr_idx.append(int(i_range_str) - 1)
        return base_job_ID, _arr_idx

    def __parse_job_states(
        self, stdout: str
    ) -> dict[str, JobscriptElementState | dict[int, JobscriptElementState]]:
        """Parse output from Slurm `squeue` command with a simple format."""
        info: dict[str, JobscriptElementState | dict[int, JobscriptElementState]] = {}
        for ln in stdout.split("\n"):
            if not ln:
                continue
            job_id, job_state, *_ = ln.split()
            base_job_ID, arr_idx = self._parse_job_IDs(job_id)
            state = self.state_lookup.get(job_state, JobscriptElementState.errored)

            if arr_idx is not None:
                entry = cast(
                    dict[int, JobscriptElementState], info.setdefault(base_job_ID, {})
                )
                for arr_idx_i in arr_idx:
                    entry[arr_idx_i] = state
            else:
                info[base_job_ID] = state

        return info

    def __query_job_states(self, job_IDs: Iterable[str]) -> tuple[str, str]:
        """Query the state of the specified jobs."""
        cmd = [
            *self.show_cmd,
            "--noheader",
            "--format",
            R"%200i %30T",  # job ID (<base_job_id>_<index> for array job) and job state
            "--jobs",
            ",".join(job_IDs),
        ]
        return run_cmd(cmd, logger=self._app.submission_logger)

    def __get_job_valid_IDs(self, job_IDs: Collection[str] | None = None) -> set[str]:
        """Get a list of job IDs that are known by the scheduler, optionally filtered by
        specified job IDs."""

        cmd = [*self.show_cmd, "--noheader", "--format", r"%F"]
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query Slurm jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        else:
            known_jobs = set(i.strip() for i in stdout.split("\n") if i.strip())
        if job_IDs is None:
            return known_jobs
        return known_jobs.intersection(job_IDs)

    @override
    def get_job_state_info(
        self, *, js_refs: Sequence[str] | None = None
    ) -> Mapping[str, JobscriptElementState | Mapping[int, JobscriptElementState]]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method.
        """

        # if job_IDs are passed, then assume they are existant, otherwise retrieve valid
        # jobs:
        refs: Collection[str] = js_refs or self.__get_job_valid_IDs()

        count = 0
        while refs:
            stdout, stderr = self.__query_job_states(refs)
            if not stderr:
                return self.__parse_job_states(stdout)
            if (
                "Invalid job id specified" not in stderr
                or count >= self.NUM_STATE_QUERY_TRIES
            ):
                raise ValueError(f"Could not get Slurm job states. Stderr was: {stderr}")

            # the job might have finished; this only seems to happen if a single
            # non-existant job ID is specified; for multiple non-existant jobs, no
            # error is produced;
            self._app.submission_logger.info(
                "A specified job ID is non-existant; refreshing known job IDs..."
            )
            time.sleep(self.INTER_STATE_QUERY_DELAY)
            refs = self.__get_job_valid_IDs(refs)
            count += 1
        return {}

    @override
    def cancel_jobs(
        self,
        js_refs: list[str],
        jobscripts: list[Jobscript] | None = None,
    ):
        """
        Cancel submitted jobs.
        """
        cmd = [self.del_cmd, *js_refs]
        self._app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query {self.__class__.__name__} jobs. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self._app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
