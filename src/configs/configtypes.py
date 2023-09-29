from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from string import Formatter
from typing import Any, Generic, TypedDict, TypeVar

from src.datarules.base_rules import Filter

from ..file import File
from .keyworded import Keyworded

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


@dataclass
class Input(Keyworded):
    path: Path
    expressions: list[str]


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


DEFAULT_OUTPUT_FORMAT = "{relative_path}/{file}.{ext}"
PLACEHOLDER_FORMAT_FILE = File("/folder/subfolder/to/file.png", "/folder", "subfolder/to", "file", ".png")
PLACEHOLDER_FORMAT_KWARGS = PLACEHOLDER_FORMAT_FILE.to_dict()


@dataclass
class Output(Keyworded):
    path: Path
    filters: dict[Filter, FilterData]
    output_format: str

    def __init__(self, path, filters, output_format=DEFAULT_OUTPUT_FORMAT):
        self.path = path
        # try to format. If it fails, it will raise InvalidFormatException
        outputformatter.format(output_format, **PLACEHOLDER_FORMAT_KWARGS)
        self.output_format = output_format
        self.filters = filters

    def format_file(self, file: File):
        return outputformatter.format(self.output_format, **file.to_dict())
