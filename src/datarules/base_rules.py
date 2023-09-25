from __future__ import annotations

import inspect
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum, EnumType
from pathlib import Path
from string import Formatter
from types import MappingProxyType
from typing import Any, ClassVar, Generic, Self, Set, TypedDict, TypeVar

from polars import DataFrame, DataType, Expr, PolarsDataType

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


class Keyworded:
    @classmethod
    def cfg_kwd(cls):
        return cls.__name__

    @classmethod
    def from_cfg(cls, cfg) -> Self:
        return cls(**cfg)

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
        attrlist: list[str] = [f"{key}={val}" for key, val in self.__dict__.items() if key not in repr_blacklist]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"

    def __str__(self) -> str:
        return self.__class__.__name__


class FilterT(Keyworded):
    all_filters: ClassVar[dict[str, Callable]] = {}

    def __init_subclass__(cls) -> None:
        FilterT.all_filters[cls.cfg_kwd()] = cls

    @classmethod
    def register_filter(cls, *f: Callable):
        for f_ in f:
            cls.all_filters[f_.__name__] = f_


def Filter(f: Callable):
    return type(f.__name__, (FilterT,), {"f": f})


class InvalidFormatException(Exception):
    def __init__(self, disallowed: str):
        super().__init__(f"invalid format string. '{disallowed}' is not allowed.")


class SafeFormatter(Formatter):
    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        # the goal is to make sure `property`s and indexing is still available, while dunders and things are not
        if "__" in field_name:
            raise InvalidFormatException("__")

        return super().get_field(field_name, args, kwargs)


outputformatter = SafeFormatter()


@dataclass
class Input(Keyworded):
    path: Path
    expressions: list[str]


@dataclass
class File:
    absolute_pth: str
    src: str
    relative_path: str
    file: str
    ext: str

    def to_dict(self):
        return {
            "absolute_pth": str(self.absolute_pth),
            "src": str(self.src),
            "relative_path": str(self.relative_path),
            "file": self.file,
            "ext": self.ext,
        }


DEFAULT_OUTPUT_FORMAT = "{relative_path}/{file}.{ext}"
PLACEHOLDER_FORMAT_FILE = File("/folder/subfolder/to/file.png", "/folder", "subfolder/to", "file", ".png")
PLACEHOLDER_FORMAT_KWARGS = PLACEHOLDER_FORMAT_FILE.to_dict()


@dataclass
class Output(Keyworded):
    path: Path
    filters: dict[Callable, FilterData]
    output_format: str

    def __init__(self, path, filters, output_format=DEFAULT_OUTPUT_FORMAT):
        self.path = path
        # try to format. If it fails, it will raise InvalidFormatException
        outputformatter.format(output_format, **PLACEHOLDER_FORMAT_KWARGS)
        self.output_format = output_format
        self.filters = filters

    def format_file(self, file: File):
        return outputformatter.format(self.output_format, **file.to_dict())


T = TypeVar("T")


class ItemConfig(TypedDict, Generic[T]):
    data: T
    enabled: bool
    name: str
    opened: bool


ItemData = dict


class SpecialItemData(TypedDict):
    ...


class InputData(SpecialItemData):
    expressions: list[str]
    folder: str
    ...


class FilterData(SpecialItemData):
    pass


class OutputData(SpecialItemData):
    folder: str
    lst: list[ItemConfig[FilterData]]
    output_format: str


class ProducerData(ItemData):
    ...


class RuleData(ItemData):
    ...


class MainConfig(TypedDict):
    inputs: list[ItemConfig[InputData]]
    producers: list[ItemConfig[ProducerData]]
    rules: list[ItemConfig[RuleData]]
    output: list[ItemConfig[OutputData]]
