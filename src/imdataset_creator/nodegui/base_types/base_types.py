from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import numpy as np
from qtpynodeeditor import (
    NodeData,
    NodeDataType,
)


class ValuedNodeData(NodeData):
    value: property

    @staticmethod
    def check(s: ValuedNodeData | None) -> Any | None:
        return s.value if s is not None else None


class PathGeneratorData(ValuedNodeData):
    """Node data holding a generator"""

    data_type = NodeDataType("path_generator", "Path Generator")

    def __init__(self, generator: Generator):
        self._generator: Generator = generator
        self._lock = threading.RLock()

    @property
    def lock(self):
        return self._lock

    @property
    def value(self) -> Generator:
        return self._generator


class RandomNumberGeneratorData(ValuedNodeData):
    """Node data holding a generator"""

    data_type = NodeDataType("random_number_generator", "Random Number Generator")

    def __init__(self, generator: Callable[[], float]):
        self._generator: Callable[[], float] = generator
        self._lock = threading.RLock()

    @property
    def lock(self):
        return self._lock

    @property
    def value(self) -> Callable[[], float]:
        return self._generator


class IntegerData(ValuedNodeData):
    data_type = NodeDataType("integer", "Integer")

    def __init__(self, value: int):
        self._value = value

    @property
    def value(self) -> int:
        return self._value


class FloatData(ValuedNodeData):
    data_type = NodeDataType("float", "Float")

    def __init__(self, value: float):
        self._value = value

    @property
    def value(self) -> float:
        return self._value


class SignalData(NodeData):
    data_type = NodeDataType("signal", "Signal")


class BoolData(ValuedNodeData):
    data_type = NodeDataType("bool", "Boolean")

    def __init__(self, v: bool) -> None:
        super().__init__()
        self._value = v

    @property
    def value(self) -> bool:
        return self._value


class ListData(ValuedNodeData):
    """Node data holding a list"""

    data_type = NodeDataType("list", "List")

    def __init__(self, lst: list):
        self._list = lst

    @property
    def value(self):
        return self._list


class AnyData(ValuedNodeData):
    data_type = NodeDataType("any", "Any")

    def __init__(self, item) -> None:
        self._item = item

    @property
    def value(self):
        return self._item


class PathData(ValuedNodeData):
    data_type = NodeDataType("path", "Path")

    def __init__(self, p: Path) -> None:
        super().__init__()
        self._path = p

    @property
    def value(self):
        return self._path


class StringData(ValuedNodeData):
    data_type = NodeDataType("str", "String")

    def __init__(self, s: str) -> None:
        super().__init__()
        self._str = s

    @property
    def value(self):
        return self._str


class ImageData(ValuedNodeData):
    data_type = NodeDataType("image", "Image")

    def __init__(self, im: np.ndarray) -> None:
        self._image = im

    @property
    def value(self):
        return self._image
