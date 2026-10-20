"""Module containing code for generating numerical input and sequence values from various
class methods."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import TypeVar, Type, TYPE_CHECKING, overload, ClassVar, Any, cast
from typing_extensions import Self
import re
import numpy as np

from hpcflow.sdk.core.utils import linspace_rect, process_string_nodes

if TYPE_CHECKING:
    from numpy.typing import NDArray
    from ..app import BaseApp
    from hpcflow.sdk.core.parameters import SchemaInput, Parameter

T = TypeVar("T", bound="ValuesMixin")
U = TypeVar("U")


def process_demo_data_strings(app: BaseApp, value: Any) -> Any:
    demo_pattern = re.compile(r"\<\<demo_data_file:(.*)\>\>")

    def string_processor(str_in: str) -> str:
        str_out = demo_pattern.sub(
            repl=lambda x: str(app.get_demo_data_file_path(x[1])),
            string=str_in,
        )
        return str_out

    return process_string_nodes(value, string_processor)


def ensure_seed(seed: int | list[int] | None) -> int | list[int]:
    """For methods that use a random seed, if the seed is not set, set it randomly so it
    can be recorded within the method args for reproducibility."""
    return (
        int(cast("int", np.random.SeedSequence().generate_state(1)[0]))
        if seed is None
        else seed
    )


@overload
def sample_rows(
    values: NDArray,
    num_samples: int,
    seed: int | list[int] | None = None,
    replace: bool = False,
) -> NDArray: ...


@overload
def sample_rows(
    values: list[U],
    num_samples: int,
    seed: int | list[int] | None = None,
    replace: bool = False,
) -> list[U]: ...


def sample_rows(
    values: NDArray | list[U],
    num_samples: int,
    seed: int | list[int] | None = None,
    replace: bool = False,
) -> NDArray | list[U]:
    """Randomly sample rows (items along the first axis) from `values`.

    Parameters
    ----------
    values
        A list, or a Numpy array whose first axis indexes the rows.
    num_samples
        The number of rows to sample.
    seed
        Seed for the random number generator.
    replace
        Whether to sample with replacement. If `False`, `num_samples` must not exceed
        the number of rows available.
    """
    num_rows = len(values)
    if not replace and num_samples > num_rows:
        raise ValueError(
            f"Cannot sample {num_samples} rows without replacement from {num_rows} "
            f"available rows; set `replace=True` or reduce `num_samples`."
        )
    rng = np.random.default_rng(seed)
    indices = rng.choice(num_rows, size=num_samples, replace=replace)
    if isinstance(values, np.ndarray):
        return values[indices]
    return [values[int(idx)] for idx in indices]


def load_txt_values(
    fname: str | Path,
    num_samples: int | None = None,
    seed: int | list[int] | None = None,
    replace: bool = False,
    **kwargs,
) -> NDArray:
    """Load an array from a text file with Numpy's `loadtxt`, optionally randomly
    sampling rows (items along the first axis) from it.

    Parameters
    ----------
    fname
        Path to the text file.
    num_samples
        If specified, the number of rows to randomly sample from the loaded array.
    seed
        Seed for the random number generator used to sample rows.
    replace
        Whether to sample rows with replacement.
    kwargs
        Additional keyword arguments passed to `numpy.loadtxt`.
    """
    arr = np.loadtxt(fname, **kwargs)
    if num_samples is not None:
        arr = sample_rows(arr, num_samples, seed=seed, replace=replace)
    return arr


class ValuesMixin(ABC):

    _app: ClassVar[BaseApp]

    @classmethod
    @abstractmethod
    def _process_mixin_args(cls, *args: Any, **kwargs: Any) -> dict[str, Any]: ...

    @abstractmethod
    def _remember_values_method_args(self, name: str, args: dict[str, Any]) -> Self: ...

    @classmethod
    def _values_from_linear_space(
        cls: Type[T], start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.linspace(start, stop, num=num, **kwargs).tolist()

    @classmethod
    def _values_from_geometric_space(
        cls, start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.geomspace(start, stop, num=num, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_log_space(
        cls, start: float, stop: float, num: int, base: float = 10.0, **kwargs
    ) -> list[float]:
        return np.logspace(start, stop, num=num, base=base, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_range(
        cls, start: int | float, stop: int | float, step: int | float, **kwargs
    ) -> list[float]:
        return np.arange(start, stop, step, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_repeat(cls, arr, repeats, axis=None) -> list[float]:
        return np.repeat(arr, repeats, axis=axis).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_tile(cls, arr, reps) -> list[float]:
        return np.tile(arr, reps).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_file(
        cls,
        file_path: str | Path,
        num_samples: int | None = None,
        seed: int | list[int] | None = None,
        replace: bool = False,
    ) -> list[str]:
        with Path(file_path).open("rt") as fh:
            lines = [line.strip() for line in fh.readlines()]
        if num_samples is not None:
            lines = sample_rows(lines, num_samples, seed=seed, replace=replace)
        return lines

    @classmethod
    def _values_from_load_txt(
        cls,
        fname: str | Path,
        num_samples: int | None = None,
        seed: int | list[int] | None = None,
        replace: bool = False,
        **kwargs,
    ) -> NDArray:
        return load_txt_values(
            fname, num_samples=num_samples, seed=seed, replace=replace, **kwargs
        )

    @classmethod
    def _values_from_rectangle(
        cls,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | tuple[int, int] | None = None,
        include: Sequence[str] | None = None,
        **kwargs,
    ) -> list[float]:
        vals = linspace_rect(start=start, stop=stop, num=num, include=include, **kwargs)
        if coord is not None:
            return vals[coord].tolist()
        else:
            return (vals.T).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_numpy_distribution(
        cls,
        method_name: str,
        shape: int | Sequence[int] | None,
        seed: int | list[int],
        **kwargs,
    ) -> list[float] | float:
        kwargs["size"] = shape
        rng = np.random.default_rng(seed)
        method = getattr(rng, method_name)
        out = method(**kwargs)
        if shape is None:
            return out
        else:
            return out.tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @overload
    @classmethod
    def _values_from_uniform(
        cls, shape: int | Sequence[int], **kwargs
    ) -> list[float]: ...

    @overload
    @classmethod
    def _values_from_uniform(cls, shape: None, **kwargs) -> float: ...

    @classmethod
    def _values_from_uniform(
        cls, shape: int | Sequence[int] | None, **kwargs
    ) -> float | list[float]:
        return cls._values_from_numpy_distribution("uniform", **kwargs)

    @classmethod
    def _from_linear_space(
        cls: Type[T],
        start: float,
        stop: float,
        num: int,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a NumPy linear space.
        """
        args = {"start": start, "stop": stop, "num": num, **kwargs}
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_linear_space(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_linear_space", args)

    @classmethod
    def _from_geometric_space(
        cls: Type[T],
        start: float,
        stop: float,
        num: int,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        endpoint=True,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a NumPy geometric space.
        """
        args = {"start": start, "stop": stop, "num": num, "endpoint": endpoint, **kwargs}
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_geometric_space(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_geometric_space", args)

    @classmethod
    def _from_log_space(
        cls: Type[T],
        start: float,
        stop: float,
        num: int,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        base=10.0,
        endpoint=True,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a NumPy logarithmic space.
        """
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "endpoint": endpoint,
            "base": base,
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_log_space(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_log_space", args)

    @classmethod
    def _from_range(
        cls: Type[T],
        start: float,
        stop: float,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        step: int | float = 1,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a range.
        """
        args = {"start": start, "stop": stop, "step": step, **kwargs}
        if isinstance(step, int):
            values = cls._values_from_range(**args)
        else:
            # Use linspace for non-integer step, as recommended by Numpy:
            values = cls._values_from_linear_space(
                start=start,
                stop=stop,
                num=int((stop - start) / step),
                endpoint=False,
                **kwargs,
            )

        obj = cls(
            **cls._process_mixin_args(
                values,
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_range", args)

    @classmethod
    def _from_repeat(
        cls: Type[T],
        arr,
        repeats,
        axis=None,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from the return of Numpy's ``repeat`` function.
        """
        args = {"arr": arr, "repeats": repeats, "axis": axis, **kwargs}
        values = cls._values_from_repeat(**args)
        obj = cls(
            **cls._process_mixin_args(
                values,
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_repeat", args)

    @classmethod
    def _from_tile(
        cls: Type[T],
        arr,
        reps,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from the return of Numpy's ``tile`` function.
        """
        args = {"arr": arr, "reps": reps, **kwargs}
        values = cls._values_from_tile(**args)
        obj = cls(
            **cls._process_mixin_args(
                values,
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_tile", args)

    @staticmethod
    def _sampling_args(
        num_samples: int | None, seed: int | list[int] | None, replace: bool
    ) -> dict[str, Any]:
        """Row-sampling arguments to record alongside other value-method arguments.

        Only recorded when sampling is requested, in which case the seed is ensured so
        the sampled values are reproducible."""
        if num_samples is None:
            return {}
        return {"num_samples": num_samples, "seed": ensure_seed(seed), "replace": replace}

    @classmethod
    def _from_file(
        cls: Type[T],
        file_path: str | Path,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        num_samples: int | None = None,
        seed: int | list[int] | None = None,
        replace: bool = False,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a simple file, optionally randomly sampling lines from it.
        """
        args = {
            "file_path": process_demo_data_strings(cls._app, file_path),
            **cls._sampling_args(num_samples, seed, replace),
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_file(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_file", args)

    @classmethod
    def _from_load_txt(
        cls: Type[T],
        file_path: str | Path,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        num_samples: int | None = None,
        seed: int | list[int] | None = None,
        replace: bool = False,
        **kwargs,
    ) -> T:
        """
        Load an array from a text file with Numpy, optionally randomly sampling rows
        from it.
        """
        args = {
            "fname": process_demo_data_strings(cls._app, file_path),
            **cls._sampling_args(num_samples, seed, replace),
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_load_txt(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_load_txt", args)

    @classmethod
    def _from_rectangle(
        cls: Type[T],
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        coord: int | None = None,
        include: list[str] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from coordinates to cover the perimeter of a rectangle.

        Parameters
        ----------
        coord:
            Which coordinate to use. Either 0, 1, or `None`, meaning each value will be
            both coordinates.
        include
            If specified, include only the specified edges. Choose from "top", "right",
            "bottom", "left".
        """
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "coord": coord,
            "include": include,
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_rectangle(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_rectangle", args)

    @classmethod
    def _from_uniform(
        cls: Type[T],
        shape: int | Sequence[int] | None,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a uniform random number generator.
        """
        args = {
            "low": low,
            "high": high,
            "shape": shape,
            "seed": ensure_seed(seed),
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_numpy_distribution("uniform", **args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_uniform", args)

    @classmethod
    def _from_normal(
        cls: Type[T],
        shape: int | Sequence[int] | None,
        loc: float = 0.0,
        scale: float = 1.0,
        seed: int | list[int] | None = None,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a uniform random number generator.
        """
        args = {
            "loc": loc,
            "scale": scale,
            "shape": shape,
            "seed": ensure_seed(seed),
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_numpy_distribution("normal", **args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_normal", args)

    @classmethod
    def _from_log_normal(
        cls: Type[T],
        shape: int | Sequence[int] | None,
        mean: float = 0.0,
        sigma: float = 1.0,
        seed: int | list[int] | None = None,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a log-normal random number generator.
        """
        args = {
            "mean": mean,
            "sigma": sigma,
            "shape": shape,
            "seed": ensure_seed(seed),
            **kwargs,
        }
        obj = cls(
            **cls._process_mixin_args(
                cls._values_from_numpy_distribution("lognormal", **args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("from_log_normal", args)

    @classmethod
    def _random_seeds(
        cls: Type[T],
        number: int | None,
        master_seed: int | None = None,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = "resources.any.random_seed",
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ):
        args = {
            "number": number,
            "master_seed": master_seed,
            **kwargs,
        }
        values = (
            np.random.SeedSequence(master_seed)
            .generate_state(1 if number is None else number)
            .tolist()
        )
        if number is None:
            values = values[0]  # default for an InputValue is to return one value
        obj = cls(
            **cls._process_mixin_args(
                values=values,
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("random_seeds", args)
