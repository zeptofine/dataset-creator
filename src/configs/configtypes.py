from typing import Generic, TypedDict, TypeVar

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


class FilterData(SpecialItemData):
    ...


class OutputData(SpecialItemData):
    path: str
    lst: list[ItemConfig[FilterData]]
    output_format: str
    overwrite: bool


class ProducerData(ItemData):
    ...


class RuleData(ItemData):
    ...


class MainConfig(TypedDict):
    inputs: list[ItemConfig[InputData]]
    producers: list[ItemConfig[ProducerData]]
    rules: list[ItemConfig[RuleData]]
    output: list[ItemConfig[OutputData]]
