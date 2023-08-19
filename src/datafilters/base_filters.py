from __future__ import annotations

import inspect
import sys
from abc import abstractmethod
from collections.abc import Collection
from dataclasses import dataclass
from enum import Enum, EnumType
from pathlib import Path
from typing import Any, Self

from polars import DataFrame, Expr, PolarsDataType


class Comparable:
    @abstractmethod
    def compare(self, lst: Collection[Path], cols: DataFrame) -> list:
        """Uses all collected data to return a new list of only valid images, depending on what the filter does."""
        raise NotImplementedError


class FastComparable:
    @abstractmethod
    def fast_comp(self) -> Expr | bool:
        """Returns an Expr that can be used to filter more efficiently"""
        raise NotImplementedError


@dataclass(frozen=True)
class Column:
    """A class defining what is in a column which a filter may use to apply a"""

    source: DataFilter | None
    name: str
    dtype: PolarsDataType | type
    build_method: Expr | None = None


class DataFilter:
    """An abstract DataFilter format, for use in DatasetBuilder."""

    config_keyword: str

    def __init__(self) -> None:
        """

        filedict: dict[str, Path]
            This is filled from the dataset builder, and contains a dictionary going from the resolved versions of
            the files to the ones given from the user.
        """
        self.schema: tuple[Column, ...] = tuple()
        self.filedict: dict[str, Path] = {}  # used for certain filters, like Existing

    @classmethod
    def from_cfg(cls, *args, **kwargs) -> Self:
        return cls(*args, **kwargs)  # type: ignore

    @classmethod
    def get_cfg(cls) -> dict:
        cfg: dict[str, Any] = {}
        module = sys.modules[cls.__module__]
        for key, val in list(inspect.signature(cls.__init__).parameters.items())[1:]:
            if issubclass(type(val.default), Enum):
                cfg[key] = val.default.value
            else:
                cfg[key] = val.default
            if val.annotation is not inspect._empty:
                annotation = eval(val.annotation, module.__dict__)
                comment = DataFilter._obj_to_comment(annotation)
                if comment:
                    cfg[f"!#{key}"] = comment

        return cfg

    @staticmethod
    def _obj_to_comment(obj) -> str:
        if type(obj) is EnumType:
            return " | ".join(obj._member_map_.values())  # type: ignore
        elif hasattr(obj, "__metadata__"):
            return str(obj.__metadata__[0])

        return ""

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in self.__dict__.items()
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__
