from __future__ import annotations

from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from types import MappingProxyType
from typing import Any, ClassVar, Generic, Set, TypedDict, TypeVar

from polars import DataFrame, DataType, Expr, PolarsDataType

from ..configs import Keyworded
from ..file import File

PartialDataFrame = FullDataFrame = DataFrame
DataTypeSchema = dict[str, DataType | type]
ExprDict = dict[str, Expr | bool]
ProducerResult = list[ExprDict]

repr_blacklist = ("requires", "comparer")


class Comparable:
    func: Callable[[PartialDataFrame, FullDataFrame], DataFrame]

    def __init__(self, func: Callable[[PartialDataFrame, FullDataFrame], DataFrame]):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return f"<Comparable {self.func.__name__}>"


class FastComparable:
    expr: Expr | bool

    def __init__(self, expr: Expr | bool):
        self.expr = expr

    def __call__(self) -> Expr | bool:
        return self.expr


@dataclass(frozen=True)
class Column:
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

    def __call__(self) -> ProducerResult:
        raise NotImplementedError

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in vars(self).items()
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"


class ProducerSet(Set[Producer]):
    @staticmethod
    def _combine_schema(exprs: ProducerResult) -> ExprDict:
        return {col: expr for expression in exprs for col, expr in expression.items()}

    @property
    def schema(self) -> list[ExprDict]:
        dct = defaultdict(list)
        for producer in self:
            exprs: ProducerResult = producer()
            for idx, exprdct in enumerate(exprs):
                dct[idx].append(exprdct)
        return [self._combine_schema(sequence) for sequence in dct.values()]

    @property
    def type_schema(self) -> DataTypeSchema:
        return {col: dtype for producer in self for col, dtype in producer.produces.items()}


class Rule(Keyworded):
    """An abstract DataFilter format, for use in DatasetBuilder."""

    requires: Column | tuple[Column, ...]
    comparer: Comparable | FastComparable

    all_rules: ClassVar[dict[str, type[Rule]]] = {}

    def __init__(self) -> None:
        self.requires = ()

    def __init_subclass__(cls) -> None:
        Rule.all_rules[cls.cfg_kwd()] = cls

    def __repr__(self) -> str:
        attrlist: list[str] = [f"{key}={val}" for key, val in vars(self).items() if key not in repr_blacklist]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__


class Filter(Keyworded):
    all_filters: ClassVar[dict[str, Callable]] = {}

    def __init_subclass__(cls):
        Filter.all_filters[cls.cfg_kwd()] = cls.run

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError
