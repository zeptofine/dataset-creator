import contextlib
import logging
import random
import threading
from collections.abc import Callable, Generator
from itertools import chain
from pathlib import Path
from pprint import pformat
from queue import Empty, Queue

import cv2
import numpy as np
import qtpynodeeditor as ne
import wcmatch.glob as wglob
from qtpy.QtGui import QDoubleValidator, QFont, QFontMetrics, Qt, QTextOption
from qtpy.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDoubleSpinBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
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
    PortType,
)
from qtpynodeeditor.type_converter import TypeConverter

from ..gui.settings_inputs import DirectoryInput, DirectoryInputSettings, MultilineInput, SettingsBox, SettingsRow
from .base_types import (
    AnyData,
    register_types,
)
from .image_models import ALL_MODELS as IMAGE_MODELS
from .lists_and_generators import (
    ALL_GENERATORS,
    ALL_LIST_MODELS,
    get_text_bounds,
)
from .signals import ALL_MODELS as SIGNAL_MODELS
from .signals import SignalHandler


class OrDataModel(NodeDataModel):
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


class DistributorDataModel(NodeDataModel):
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


class PrinterDataModel(NodeDataModel):
    num_ports = PortCount(1, 0)
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._label = QLabel()

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            return

        txt = pformat(node_data.item)
        self._label.setText(txt)
        self._label.setFixedSize(get_text_bounds(txt, self._label.font()))

    def embedded_widget(self) -> QWidget:
        return self._label


class DebugPrinterDataModel(NodeDataModel):
    name = "Debug print"
    num_ports = PortCount(1, 0)
    all_data_types = AnyData.data_type

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is not None:
            print(node_data.item)


class BufferDataModel(NodeDataModel):
    caption = "Buffer"
    all_data_types = AnyData.data_type

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._item = None
        self._buffer_button = QToolButton()
        self._buffer_button.setText("release")
        self._buffer_button.clicked.connect(lambda: self.data_updated.emit(0))

    def out_data(self, port: int) -> AnyData | None:
        if self._item is None:
            return None
        return AnyData(self._item)

    def set_in_data(self, node_data: AnyData | None, port: Port):
        if node_data is None:
            self._item = None
            return
        self._item = node_data.item

    def embedded_widget(self) -> QWidget:
        return self._buffer_button


def register_type(registry: ne.DataModelRegistry, from_: NodeDataType, to_: NodeDataType, converter: Callable):
    registry.register_type_converter(from_, to_, TypeConverter(from_, to_, converter))


def main(app):
    registry = ne.DataModelRegistry()

    model_dct = {
        "generators": ALL_GENERATORS,
        "lists": [
            BufferDataModel,
            *ALL_LIST_MODELS,
        ],
        "images": IMAGE_MODELS,
        "misc": [
            DistributorDataModel,
            PrinterDataModel,
            DebugPrinterDataModel,
            BufferDataModel,
            OrDataModel,
        ],
    }
    for category, models in model_dct.items():
        for model in models:
            registry.register_model(model, category)

    signal_handler = SignalHandler()
    for model in SIGNAL_MODELS:
        registry.register_model(model, "signals", signal_handler=signal_handler)

    register_types(registry)

    scene = ne.FlowScene(registry=registry)
    view = ne.FlowView(scene)
    view.setWindowTitle("Generator ui")
    view.resize(2560, 600)
    view.show()
    signal_handler.start()
    scene.load("text.flow")
    return scene, view, signal_handler


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    app = QApplication([])
    scene, view, sh = main(app)
    scene.load("text.flow")
    app.exec_()
    scene.save("text.flow")
    sh.signal_queue.put(None)
