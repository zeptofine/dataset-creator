import operator
import random
import threading
from collections.abc import Callable
from decimal import Decimal

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

from .base_types import (
    AnyData,
    BoolData,
    FloatData,
    IntegerData,
    RandomNumberGeneratorData,
    SignalData,
)


class BufferNode(NodeDataModel):
    caption = "Buffer"
    num_ports = PortCount(2, 1)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: SignalData.data_type,
        },
        {
            0: AnyData.data_type,
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


class SwitchCaseNode(NodeDataModel):
    num_ports = PortCount(2, 2)
    data_types = DataTypes(
        {
            0: AnyData.data_type,
            1: BoolData.data_type,
        },
        {
            0: AnyData.data_type,
            1: AnyData.data_type,
        },
    )
    caption_override = CaptionOverride(
        outputs={
            0: "Then",
            1: "Else",
        }
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
            if self._bool is not None:
                self.data_updated.emit(int(not self._bool))
                self._item = None
        elif port.index == 1 and isinstance(node_data, BoolData):
            self._bool = node_data.value


ALL_MODELS = [
    BufferNode,
    SwitchCaseNode,
]
