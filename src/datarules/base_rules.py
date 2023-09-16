from __future__ import annotations

import inspect
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum, EnumType
from types import MappingProxyType
from typing import ClassVar, Self

from polars import DataFrame, Expr, PolarsDataType

PartialDataFrame = FullDataFrame = DataFrame


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


ExprDict = dict[str, Expr | bool]
ProducerResult = list[ExprDict]


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


def _combine_schema(exprs: ProducerResult) -> ExprDict:
    return {col: expr for expression in exprs for col, expr in expression.items()}


def combine_schema(producers: Iterable[Producer]) -> ProducerResult:
    dct = defaultdict(list)
    for producer in producers:
        exprs: ProducerResult = producer()
        for idx, exprdct in enumerate(exprs):
            dct[idx].append(exprdct)
    return [_combine_schema(sequence) for sequence in dct.values()]


class Rule:
    """An abstract DataFilter format, for use in DatasetBuilder."""

    config_keyword: str
    requires: Column | tuple[Column, ...]
    comparer: Comparable | FastComparable

    def __init__(self) -> None:
        self.requires = ()

    @classmethod
    def from_cfg(cls, *args, **kwargs) -> Self:
        return cls(*args, **kwargs)

    @classmethod
    def get_cfg(cls) -> dict:
        cfg = {}
        module = sys.modules[cls.__module__]
        for key, val in list(inspect.signature(cls.__init__).parameters.items())[1:]:
            if issubclass(type(val.default), Enum):
                cfg[key] = val.default.value
            else:
                cfg[key] = val.default
            if val.annotation is not inspect._empty:
                annotation = eval(val.annotation, module.__dict__)
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
        attrlist: list[str] = [
            f"{key}=..." if hasattr(val, "__iter__") and not isinstance(val, str) else f"{key}={val}"
            for key, val in self.__dict__.items()
        ]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__
