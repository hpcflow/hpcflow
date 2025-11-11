"""Module containing code for generating numerical input and sequence values from various
class methods."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import TypeVar, Type, TYPE_CHECKING, overload, Protocol, Any, cast
from typing_extensions import Self
import numpy as np

from hpcflow.sdk.core.utils import linspace_rect


if TYPE_CHECKING:
    from hpcflow.sdk.core.parameters import SchemaInput, Parameter

T = TypeVar("T", bound="ValuesMixin")


def _get_seed(seed: int | list[int] | None) -> int | list[int]:
    """For methods that use a random seed, if the seed is not set, set it randomly so it
    can be recorded within the method args for reproducibility."""
    return int(cast("int", np.random.SeedSequence().entropy)) if seed is None else seed


class ValuesMixin(ABC):

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
    def _values_from_file(cls, file_path: str | Path) -> list[str]:
        with Path(file_path).open("rt") as fh:
            return [line.strip() for line in fh.readlines()]

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
    def _from_file(
        cls: Type[T],
        file_path: str | Path,
        parameter: Parameter | SchemaInput | str | None = None,
        path: str | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        value_class_method: str | None = None,
        **kwargs,
    ) -> T:
        """
        Build a sequence from a simple file.
        """
        args = {"file_path": file_path, **kwargs}
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
        **kwargs,
    ) -> T:
        """
        Load an array from a text file with Numpy.
        """
        args = {"fname": file_path, **kwargs}
        obj = cls(
            **cls._process_mixin_args(
                np.loadtxt(**args),
                parameter=parameter,
                path=path,
                nesting_order=nesting_order,
                label=label,
                value_class_method=value_class_method,
            )
        )
        return obj._remember_values_method_args("_from_load_txt", args)

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
            "seed": _get_seed(seed),
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
            "seed": _get_seed(seed),
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
            "seed": _get_seed(seed),
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
