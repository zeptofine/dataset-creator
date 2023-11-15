import threading
from collections.abc import Callable, Generator
from pathlib import Path

import numpy as np
import qtpynodeeditor as ne
from qtpynodeeditor import (
    NodeData,
    NodeDataType,
)
from qtpynodeeditor.type_converter import TypeConverter


class PathGeneratorData(NodeData):
    """Node data holding a generator"""

    data_type = NodeDataType("path_generator", "Path Generator")

    def __init__(self, generator: Generator):
        self._generator: Generator = generator
        self._lock = threading.RLock()

    @property
    def lock(self):
        return self._lock

    @property
    def generator(self) -> Generator:
        return self._generator


class RandomNumberGeneratorData(NodeData):
    """Node data holding a generator"""

    data_type = NodeDataType("random_number_generator", "Random Number Generator")

    def __init__(self, generator: Callable[[], float]):
        self._generator: Callable[[], float] = generator
        self._lock = threading.RLock()

    @property
    def lock(self):
        return self._lock

    @property
    def generator(self) -> Callable[[], float]:
        return self._generator


class IntegerData(NodeData):
    data_type = NodeDataType("integer", "Integer")

    def __init__(self, value: int):
        self._value = value

    @property
    def value(self) -> int:
        return self._value


class FloatData(NodeData):
    data_type = NodeDataType("float", "Float")

    def __init__(self, value: float):
        self._value = value

    @property
    def value(self) -> float:
        return self._value


class SignalData(NodeData):
    data_type = NodeDataType("signal", "Signal")


class BoolData(NodeData):
    data_type = NodeDataType("bool", "Boolean")

    def __init__(self, v: bool) -> None:
        super().__init__()
        self._value = v

    @property
    def value(self) -> bool:
        return self._value


class ListData(NodeData):
    """Node data holding a list"""

    data_type = NodeDataType("list", "List")

    def __init__(self, lst: list):
        self._list = lst

    @property
    def list(self):  # noqa: A003
        return self._list


class AnyData(NodeData):
    data_type = NodeDataType("any", "Any")

    def __init__(self, item) -> None:
        self._item = item

    @property
    def item(self):
        return self._item


class PathData(NodeData):
    data_type = NodeDataType("path", "Path")

    def __init__(self, p: Path) -> None:
        super().__init__()
        self._path = p

    @property
    def path(self):
        return self._path


class StringData(NodeData):
    data_type = NodeDataType("str", "String")

    def __init__(self, s: str) -> None:
        super().__init__()
        self._str = s

    @property
    def string(self):
        return self._str


class ImageData(NodeData):
    data_type = NodeDataType("image", "Image")

    def __init__(self, im: np.ndarray) -> None:
        self._image = im

    @property
    def image(self):
        return self._image


def generator_to_list_converter(data: PathGeneratorData) -> ListData:
    return ListData(list(data.generator))


def list_to_generator_converter(data: ListData) -> PathGeneratorData:
    return PathGeneratorData(x for x in data.list)


def any_to_generator_converter(data: AnyData):
    return PathGeneratorData(Path(p) for p in data.item)


def anything_to_signal_converter(_: NodeData) -> SignalData:
    return SignalData()


def register_type(registry: ne.DataModelRegistry, from_: NodeDataType, to_: NodeDataType, converter: Callable):
    registry.register_type_converter(from_, to_, TypeConverter(from_, to_, converter))


def bool_to_signal_generator(item: BoolData) -> SignalData | None:
    return SignalData() if item.value else None


def register_types(registry: ne.DataModelRegistry):
    register_type(registry, PathGeneratorData.data_type, ListData.data_type, generator_to_list_converter)
    register_type(registry, ListData.data_type, PathGeneratorData.data_type, list_to_generator_converter)
    register_type(registry, AnyData.data_type, PathGeneratorData.data_type, any_to_generator_converter)

    register_type(registry, PathData.data_type, StringData.data_type, lambda item: StringData(str(item.path)))
    register_type(registry, StringData.data_type, PathData.data_type, lambda item: PathData(Path(item.string)))
    register_type(registry, StringData.data_type, AnyData.data_type, lambda item: AnyData(item.string))
    register_type(registry, AnyData.data_type, StringData.data_type, lambda item: StringData(item.item))
    # I hate Any
    register_type(registry, AnyData.data_type, ListData.data_type, lambda item: ListData(list(item.item)))
    register_type(registry, AnyData.data_type, ImageData.data_type, lambda item: ImageData(item.item))
    register_type(registry, AnyData.data_type, PathData.data_type, lambda item: PathData(item.item))
    register_type(registry, ListData.data_type, AnyData.data_type, lambda item: AnyData(item.list))
    register_type(registry, PathData.data_type, AnyData.data_type, lambda item: AnyData(item.path))
    register_type(registry, ImageData.data_type, AnyData.data_type, lambda item: AnyData(item.image))
    register_type(registry, SignalData.data_type, AnyData.data_type, lambda item: AnyData(True))
    register_type(registry, SignalData.data_type, BoolData.data_type, lambda item: BoolData(True))
    register_type(registry, AnyData.data_type, BoolData.data_type, lambda item: BoolData(item.item))
    register_type(registry, IntegerData.data_type, FloatData.data_type, lambda item: FloatData(float(item.value)))
    register_type(registry, FloatData.data_type, IntegerData.data_type, lambda item: IntegerData(int(item.value)))
    register_type(registry, IntegerData.data_type, AnyData.data_type, lambda item: AnyData(item.value))
    register_type(registry, FloatData.data_type, AnyData.data_type, lambda item: AnyData(item.value))
    register_type(registry, AnyData.data_type, IntegerData.data_type, lambda item: IntegerData(int(item.item)))
    register_type(registry, AnyData.data_type, FloatData.data_type, lambda item: FloatData(float(item.item)))
    register_type(
        registry,
        IntegerData.data_type,
        RandomNumberGeneratorData.data_type,
        lambda item: RandomNumberGeneratorData(lambda: float(item.value)),
    )
    register_type(
        registry,
        FloatData.data_type,
        RandomNumberGeneratorData.data_type,
        lambda item: RandomNumberGeneratorData(lambda: item.value),
    )
    register_type(registry, BoolData.data_type, SignalData.data_type, bool_to_signal_generator)
    register_type(registry, BoolData.data_type, IntegerData.data_type, lambda item: IntegerData(int(item.value)))
    register_type(registry, BoolData.data_type, FloatData.data_type, lambda item: FloatData(float(item.value)))
    register_type(registry, BoolData.data_type, AnyData.data_type, lambda item: AnyData(item.value))
    for data in (
        PathGeneratorData,
        ListData,
        AnyData,
        ImageData,
        PathData,
        StringData,
        FloatData,
        IntegerData,
    ):
        register_type(registry, data.data_type, SignalData.data_type, anything_to_signal_converter)
