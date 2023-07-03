from abc import abstractmethod
from collections.abc import Collection
from pathlib import Path
from typing import Any

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
             These *must* be filled by the build_schema.

        build_schema: dict[str, Expr]
            This is used to build the data given in the column_schema.

        config: tuple[str | None, dict[str, Any]]
            This is used to populate self attributes from the database's config file.
            The string represents the section of the config that belongs to this filter.
            If none, it's disabled.
            the dictionary __must__ include an "enabled" flag. It represents whether the filter is enabled by default.

        """
        self.filedict: dict[str, Path] = {}  # used for certain filters, like Existing
        self.column_schema: dict[str, PolarsDataType | type] = {}
        self.build_schema: dict[str, Expr] | None = None
        self.config: tuple[str | None, dict[str, Any]] = (None, {"enabled": False})
        self.__enabled = False

    def enable(self):
        self.__enabled = True

    def get_config(self) -> tuple[str | None, dict[str, Any]]:
        return self.config

    def populate_from_cfg(self, dct: dict[str, Any]):
        for key, val in dct.items():
            if key not in ("filedict", "column_schema", "build_schema", "config", "config_triggers"):
                setattr(self, key, val)

    def __bool__(self):
        return self.__enabled

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in self.__dict__.items()
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__
