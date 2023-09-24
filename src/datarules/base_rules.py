from __future__ import annotations

import inspect
import sys
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, EnumType
from types import MappingProxyType
from typing import ClassVar, Self, Set

from polars import DataFrame, DataType, Expr, PolarsDataType

PartialDataFrame = FullDataFrame = DataFrame
DataTypeSchema = dict[str, DataType | type]
ExprDict = dict[str, Expr | bool]
ProducerResult = list[ExprDict]


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


class Producer:
    """A class that produces a certain column in a dataframe"""

    produces: MappingProxyType[str, PolarsDataType | type]
    all_producers: ClassVar[list[type[Producer]]] = []

    def __init__(self):
        ...

    def __init_subclass__(cls) -> None:
        Producer.all_producers.append(cls)

    def __call__(self) -> ProducerResult:
        raise NotImplementedError

    def __repr__(self) -> str:
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in self.__dict__.items()
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


class Rule:
    """An abstract DataFilter format, for use in DatasetBuilder."""

    config_keyword: str
    requires: Column | tuple[Column, ...]
    comparer: Comparable | FastComparable

    def __init__(self) -> None:
        self.requires = ()

    @classmethod
    def from_cfg(cls, **kwargs) -> Self:
        return cls(**kwargs)

    @classmethod
    def get_cfg(cls) -> dict:
        cfg = {}
        for key, val in list(inspect.signature(cls.__init__).parameters.items())[1:]:
            if issubclass(type(val.default), Enum):
                cfg[key] = val.default.value
            else:
                cfg[key] = val.default
            if val.annotation is not inspect._empty:
                annotation = eval(val.annotation, sys.modules[cls.__module__].__dict__)
                comment = Rule._obj_to_comment(annotation)
                if comment:
                    cfg[f"!#{key}"] = comment
        return cfg

    @staticmethod
    def _obj_to_comment(obj) -> str:
        if type(obj) is EnumType:
            return " | ".join(obj._member_map_.values())  # type: ignore
        if hasattr(obj, "__metadata__"):
            return str(obj.__metadata__[0])

        return ""

    def __repr__(self) -> str:
        attrlist: list[str] = [f"{key}={val}" for key, val in self.__dict__.items() if key not in repr_blacklist]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__


repr_blacklist = ("requires", "comparer", "config_keyword")
