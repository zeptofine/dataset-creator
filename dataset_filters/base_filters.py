from abc import abstractmethod
from collections.abc import Collection
from pathlib import Path

from polars import DataFrame, Expr, PolarsDataType


class Comparable:
    @abstractmethod
    def compare(self, lst: Collection[Path], cols: DataFrame) -> list:
        """Uses collected data to return a new list of only valid images, depending on what the filter does."""
        raise NotImplementedError


class FastComparable:
    @abstractmethod
    def fast_comp(self) -> Expr | bool:
        """Returns an Expr that can be used to filter more efficiently, in Rust"""
        raise NotImplementedError


class DataFilter:
    """An abstract DataFilter format, for use in DatasetBuilder."""

    def __init__(self) -> None:
        """

        filedict: dict[str, Path]
            This is filled from the dataset builder, and contains a dictionary going from the resolved versions of
            the files to the ones given from the user.

        column_schema: dict[str, PolarsDataType | type]
            This is used to add a column using names and types to the file database.

        build_schema: dict[str, Expr]
            This is used to build the data given in the column_schema.

        """
        self.filedict: dict[str, Path] = {}  # used for certain filters, like Existing
        self.column_schema: dict[str, PolarsDataType | type] = {}
        self.build_schema: dict[str, Expr] | None = None

    def __repr__(self):
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in self.__dict__.items()
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__
