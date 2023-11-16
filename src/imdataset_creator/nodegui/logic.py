import operator
import random
import threading
from collections.abc import Callable
from decimal import Decimal
from time import sleep

import cv2
import numpy as np
import PySide6.QtCore
from PIL import Image
from qtpy.QtCore import QEvent, QObject, Qt, QThread
from qtpy.QtGui import QImage, QPixmap
from qtpy.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QLabel,
    QSpinBox,
    QWidget,
)
from qtpynodeeditor import (
    CaptionOverride,
    ConnectionPolicy,
    DataTypes,
    NodeData,
    NodeDataModel,
    NodeDataType,
    NodeValidationState,
    Port,
    PortCount,
)

from .base_types.base_types import (
    AnyData,
    BoolData,
    FloatData,
    IntegerData,
    RandomNumberGeneratorData,
    SignalData,
)


class NotNode(NodeDataModel):
    caption = "NOT"
    all_data_types = BoolData.data_type
    num_ports = PortCount(1, 1)
    caption_override = CaptionOverride({0: "In"}, {0: "Out"})

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result: bool | None = None

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return BoolData(self._result)

    def set_in_data(self, node_data: BoolData | None, port: Port):
        if node_data is not None:
            self._result = not node_data.value
            self.data_updated.emit(0)


class BufferNode(NodeDataModel):
    caption = "Buffer"
    num_ports = PortCount(2, 2)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: SignalData.data_type,
        },
        {
            0: AnyData.data_type,
            1: SignalData.data_type,
        },
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._item = None

    def out_data(self, port: int) -> AnyData | None:
        if self._item is None:
            return None
        return AnyData(self._item)

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            self._item = None
            return
        if port.index == 0:
            self._item = node_data.item
        elif port.index == 1:
            self.data_updated.emit(0)
            self.data_updated.emit(1)


class SwitchCaseNode(NodeDataModel):
    num_ports = PortCount(3, 2)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: BoolData.data_type,
            2: SignalData.data_type,
        },
        {
            0: AnyData.data_type,
            1: AnyData.data_type,
        },
    )
    caption_override = CaptionOverride(
        inputs={
            0: "Data",
            1: "Condition",
            2: "Execute",
        },
        outputs={
            0: "Then",
            1: "Else",
        },
    )
    caption_visible = True

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._bool = None
        self._item = None

    def out_data(self, port: int) -> AnyData | None:
        if self._item is None:
            return None
        return AnyData(self._item)

    def set_in_data(self, node_data: AnyData | BoolData | None, port: Port):
        if node_data is None:
            self._item = None
            return
        if port.index == 0 and isinstance(node_data, AnyData):
            self._item = node_data.item
        elif port.index == 1 and isinstance(node_data, BoolData):
            self._bool = node_data.value
        elif port.index == 2 and self._bool is not None and self._item is not None:  # reset
            self.data_updated.emit(int(not self._bool))


class DistributorNode(NodeDataModel):
    num_ports = PortCount(1, 2)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._n = 0
        self._item = None
        self._released = False

    def out_data(self, port: int) -> NodeData | None:
        if self._item is None:
            return None
        if self._released:
            self._released = False
            return AnyData(self._item)
        return None

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return

        self._item = node_data.item
        self._released = True
        self.data_updated.emit(self._n)
        self._n = (self._n + 1) % 2


class BooleanComparisonNode(NodeDataModel):
    caption_visible = False

    data_types = DataTypes(
        {
            0: BoolData.data_type,
            1: BoolData.data_type,
        },
        {0: BoolData.data_type},
    )
    caption_override = CaptionOverride(
        {0: "A", 1: "B"},
        {0: "Out"},
    )

    num_ports = PortCount(2, 1)

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._widget = QComboBox(parent)
        self.operators: dict[str, Callable[[float, float], bool]] = {
            ">": operator.gt,
            ">=": operator.ge,
            "<": operator.lt,
            "<=": operator.le,
            "==": operator.eq,
            "!=": operator.ne,
        }
        self._widget.addItems(list(self.operators.keys()))
        self.first: float | None = None
        self.second: float | None = None
        self._result: bool | None = None

    def out_data(self, port: int) -> NodeData | None:
        if self._result is None:
            return None
        return BoolData(self._result)
        # return FloatData(self._widget.value())

    def set_in_data(self, node_data: FloatData | None, port: Port):
        if port.index == 0:
            self.first = node_data.value if node_data is not None else None
        elif port.index == 1:
            self.second = node_data.value if node_data is not None else None

        if self.first is not None and self.second is not None:
            self._result = self.operators[self._widget.currentText()](self.first, self.second)
            self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["operator"] = self._widget.currentText()
        return dct

    def restore(self, doc: dict):
        if "operator" in doc:
            self._widget.setCurrentText(doc["operator"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class DelayThread(QThread):
    def run(self):
        sleep(0.01)


class DelayNode(NodeDataModel):
    num_ports = PortCount(1, 1)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._thread = DelayThread(parent)
        self._thread.finished.connect(lambda: self.data_updated.emit(0))
        self._in = None

    def out_data(self, port: int) -> NodeData | None:
        return self._in

    def set_in_data(self, node_data: NodeData | None, port: Port):
        if node_data is None:
            return
        self._in = node_data
        self._thread.start()


class MergeLaneNode(NodeDataModel):
    num_ports = PortCount(2, 1)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result = None

    def out_data(self, port: int) -> NodeData | None:
        return AnyData(self._result)

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return
        self._result = node_data.item
        self.data_updated.emit(0)


class SplitLaneNode(NodeDataModel):
    num_ports = PortCount(1, 4)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result = None

    def out_data(self, port: int) -> NodeData | None:
        return self._result

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return
        self._result = AnyData(node_data.item)
        self.data_updated.emit(0)
        self.data_updated.emit(1)
        self.data_updated.emit(2)
        self.data_updated.emit(3)


ALL_MODELS = [NotNode, DelayNode, BufferNode, SwitchCaseNode, DistributorNode, MergeLaneNode, SplitLaneNode]
