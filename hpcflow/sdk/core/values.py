"""Module containing code for generating numerical input and sequence values from various
class methods."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
import numpy as np

from hpcflow.sdk.core.utils import linspace_rect

if TYPE_CHECKING:
    from typing_extensions import Self


class ValuesMixin:
    @classmethod
    def _values_from_linear_space(
        cls, start: float, stop: float, num: int, **kwargs
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
    def _values_from_random_uniform(
        cls,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
    ) -> list[float]:
        rng = np.random.default_rng(seed)
        return rng.uniform(low=low, high=high, size=num).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def from_linear_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a NumPy linear space.
        """
        # TODO: save persistently as an array?
        args = {"start": start, "stop": stop, "num": num, **kwargs}
        values = cls._values_from_linear_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_linear_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_geometric_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a NumPy geometric space.
        """
        args = {"start": start, "stop": stop, "num": num, "endpoint": endpoint, **kwargs}
        values = cls._values_from_geometric_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_geometric_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_log_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        base=10.0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
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
        values = cls._values_from_log_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_log_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_range(
        cls,
        path: str,
        start: float,
        stop: float,
        nesting_order: float = 0,
        step: int | float = 1,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a range.
        """
        # TODO: save persistently as an array?
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
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )
        obj._values_method = "from_range"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_file(
        cls,
        path: str,
        file_path: str | Path,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a simple file.
        """
        args = {"file_path": file_path, **kwargs}
        values = cls._values_from_file(**args)
        obj = cls(
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )

        obj._values_method = "from_file"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_rectangle(
        cls,
        path: str,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | None = None,
        include: list[str] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence to cover a rectangle.

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
        values = cls._values_from_rectangle(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_rectangle"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_random_uniform(
        cls,
        path,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a uniform random number generator.
        """
        args = {"low": low, "high": high, "num": num, "seed": seed, **kwargs}
        values = cls._values_from_random_uniform(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_random_uniform"
        obj._values_method_args = args
        return obj
