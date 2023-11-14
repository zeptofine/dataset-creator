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


class SignalData(NodeData):
    data_type = NodeDataType("signal", "Signal")


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


def register_types(registry: ne.DataModelRegistry):
    register_type(registry, PathGeneratorData.data_type, ListData.data_type, generator_to_list_converter)
    register_type(registry, ListData.data_type, PathGeneratorData.data_type, list_to_generator_converter)
    register_type(registry, AnyData.data_type, ListData.data_type, lambda item: ListData(list(item.item)))
    register_type(registry, AnyData.data_type, ImageData.data_type, lambda item: ImageData(item.item))
    register_type(registry, AnyData.data_type, PathData.data_type, lambda item: PathData(item.item))
    register_type(registry, ListData.data_type, AnyData.data_type, lambda item: AnyData(item.list))
    register_type(registry, PathData.data_type, AnyData.data_type, lambda item: AnyData(str(item.path)))
    register_type(registry, ImageData.data_type, AnyData.data_type, lambda item: AnyData(item.image))
    register_type(registry, SignalData.data_type, AnyData.data_type, lambda item: AnyData(True))
    register_type(registry, AnyData.data_type, PathGeneratorData.data_type, any_to_generator_converter)

    for data in (PathGeneratorData, ListData, AnyData, ImageData, PathData):
        register_type(registry, data.data_type, SignalData.data_type, anything_to_signal_converter)
