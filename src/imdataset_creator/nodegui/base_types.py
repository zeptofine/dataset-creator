import threading
from collections.abc import Generator
from pathlib import Path

import numpy as np
from qtpynodeeditor import (
    NodeData,
    NodeDataType,
)


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
