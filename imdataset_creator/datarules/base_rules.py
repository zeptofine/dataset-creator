from __future__ import annotations

import textwrap
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from types import MappingProxyType
from typing import Any, ClassVar

import numpy as np
import wcmatch.glob as wglob
from polars import DataFrame, DataType, Expr, PolarsDataType

from ..configs import FilterData, Keyworded
from ..configs.configtypes import InputData, OutputData
from ..file import File

PartialDataFrame = DataFrame
FullDataFrame = DataFrame
DataTypeSchema = dict[str, DataType | type]
ExprDict = dict[str, Expr]
ProducerSchema = list[ExprDict]


def indent(t):
    return textwrap.indent(t, "    ")


class DataFrameMatcher:
    """A class that filters sections of a dataframe based on a function"""

    func: Callable[[PartialDataFrame, FullDataFrame], DataFrame]

    def __init__(self, func: Callable[[PartialDataFrame, FullDataFrame], DataFrame]):
        """
        Args:
            func (Callable[[PartialDataFrame, FullDataFrame], DataFrame]): A function that takes a DataFrame and a Dataframe with more information as input, and returns a filtered
            DataFrame as an output.
        """
        self.func = func

    def __call__(self, *args, **kwargs) -> DataFrame:
        return self.func(*args, **kwargs)

    def __repr__(self):
        return f"DataFrameMatcher({self.func.__name__})"


class ExprMatcher:
    """A class that filters files based on an expression"""

    expr: Expr

    def __init__(self, *exprs: Expr):
        self.expr = combine_expr_conds(exprs)

    def __call__(self) -> Expr:
        return self.expr

    def __repr__(self) -> str:
        return f"ExprMatcher({self.expr})"


@dataclass(frozen=True)
class DataColumn:
    """A class defining a column that a filter may need"""

    name: str
    dtype: PolarsDataType | type | None = None


class Producer(Keyworded):
    """A class that produces a certain column in a dataframe"""

    produces: MappingProxyType[str, PolarsDataType | type]
    all_producers: ClassVar[dict[str, type[Producer]]] = {}

    def __init__(self):
        ...

    def __init_subclass__(cls) -> None:
        Producer.all_producers[cls.cfg_kwd()] = cls

    def __call__(self) -> ProducerSchema:
        raise NotImplementedError


class ProducerSet(set[Producer]):
    @staticmethod
    def _combine_schema(exprs: ProducerSchema) -> ExprDict:
        return {col: expr for expression in exprs for col, expr in expression.items()}

    @property
    def schema(self) -> ProducerSchema:
        dct = defaultdict(list)
        for producer in self:
            exprs: ProducerSchema = producer()
            for idx, expr_dict in enumerate(exprs):
                dct[idx].append(expr_dict)
        return [self._combine_schema(sequence) for sequence in dct.values()]

    @property
    def type_schema(self) -> DataTypeSchema:
        return {col: dtype for producer in self for col, dtype in producer.produces.items()}


class Rule(Keyworded):
    """An abstract DataFilter format, for use in DatasetBuilder."""

    requires: DataColumn | tuple[DataColumn, ...]
    matcher: DataFrameMatcher | ExprMatcher

    all_rules: ClassVar[dict[str, type[Rule]]] = {}

    def __init__(self) -> None:
        self.requires = ()

    def __init_subclass__(cls) -> None:
        Rule.all_rules[cls.cfg_kwd()] = cls

    def __str__(self) -> str:
        return self.__class__.__name__


@dataclass(frozen=True, repr=False)
class Filter(Keyworded):
    """EVERY FILTER MUST BE A FROZEN DATACLASS.
    Define your arguments just like how you'd define a dataclass, and run() defines the Filter action.
    """

    all_filters: ClassVar[dict[str, type[Filter]]] = {}

    def __init_subclass__(cls):
        Filter.all_filters[cls.cfg_kwd()] = cls

    @abstractmethod
    def run(self, img: np.ndarray) -> np.ndarray:
        raise NotImplementedError


flags: int = wglob.BRACE | wglob.SPLIT | wglob.EXTMATCH | wglob.IGNORECASE | wglob.GLOBSTAR


PathGenerator = Generator[Path, None, None]


@dataclass(repr=False)
class Input(Keyworded):
    """
    A dataclass representing the input configuration.

    Attributes
    ----------
    folder : Path
        The path to the input folder.
    expressions : list[str]
        A list of glob patterns to match files in the input folder.
    """

    folder: Path
    expressions: list[str]

    @classmethod
    def from_cfg(cls, cfg: InputData):
        return cls(Path(cfg["folder"]), cfg["expressions"])

    def run(self) -> PathGenerator:
        """
        Yield the paths of all files in the input folder that match the glob patterns.

        Returns
        -------
        Iterator[Path]
            An iterator over all paths of files in the input folder that match the glob patterns.
        """
        for file in wglob.iglob(self.expressions, flags=flags, root_dir=self.folder):  # type: ignore
            yield self.folder / file


class InvalidFormatError(Exception):
    def __init__(self, disallowed: str):
        super().__init__(f"invalid format string. '{disallowed}' is not allowed.")


class SafeFormatter(Formatter):
    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        # the goal is to make sure `property`s and indexing is still available, while dunders and things are not
        if "__" in field_name:
            raise InvalidFormatError("__")

        return super().get_field(field_name, args, kwargs)


output_formatter = SafeFormatter()


DEFAULT_OUTPUT_FORMAT = "{relative_path}/{file}.{ext}"
PLACEHOLDER_FORMAT_FILE = File.from_src(Path("/folder"), Path("/folder/subfolder/to/file.png"))
PLACEHOLDER_FORMAT_KWARGS = PLACEHOLDER_FORMAT_FILE.to_dict()


@dataclass(repr=False)
class Output(Keyworded):
    """
    A dataclass representing the output configuration.

    Attributes
    ----------
    folder : Path
        The path to the output folder.
    filters : list[Filter]
        A list of `Filter` objects to be applied to the output.
    output_format : str
        The format of the output files.
    overwrite : bool
        Whether to overwrite existing files.

    Raises
    ------
    InvalidFormatException
        If the `output_format` is invalid.
    """

    folder: Path
    filters: list[Filter]
    output_format: str
    overwrite: bool

    def __init__(
        self,
        path: Path,
        filters: list[Filter],
        overwrite: bool = False,
        output_format: str = DEFAULT_OUTPUT_FORMAT,
    ):
        self.folder = path
        # try to format. If it fails, it will raise InvalidFormatException
        output_formatter.format(output_format, **PLACEHOLDER_FORMAT_KWARGS)
        self.output_format = output_format
        self.overwrite = overwrite
        self.filters = filters

    def format_file(self, file: File):
        """
        Format a `File` object according to the `output_format`.

        Parameters
        ----------
        file : File
            The `File` object to be formatted.

        Returns
        -------
        str
            The formatted string.
        """
        return output_formatter.format(self.output_format, **file.to_dict())

    @classmethod
    def from_cfg(cls, cfg: OutputData):
        return cls(
            Path(cfg["folder"]),
            [Filter.all_filters[filter_["name"]].from_cfg(filter_["data"]) for filter_ in cfg["lst"]],
            cfg["overwrite"],
            cfg["output_format"],
        )


def combine_expr_conds(exprs: Iterable[Expr]) -> Expr:
    """
    Combine a list of `Expr` objects using the `&` operator.

    Parameters
    ----------
    exprs : list[Expr]
        A list of `Expr` objects to be combined.

    Returns
    -------
    Expr
        A single `Expr` object representing the combined expression.
    """
    comp: Expr | None = None
    for e in exprs:
        comp = comp & e if comp is None else e
    assert comp is not None

    return comp
