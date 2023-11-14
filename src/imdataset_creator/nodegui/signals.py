import contextlib
import random
from collections.abc import Generator
from pathlib import Path
from queue import Empty, Queue
from time import sleep

from PySide6.QtCore import QThread, Signal, Slot
from qtpy.QtCore import QObject
from qtpy.QtWidgets import QSpinBox, QToolButton, QVBoxLayout, QWidget
from qtpynodeeditor import (
    ConnectionPolicy,
    DataModelRegistry,
    DataTypes,
    NodeData,
    NodeDataModel,
    NodeValidationState,
    Port,
    PortCount,
)

from .base_types import (
    ListData,
    PathData,
    PathGeneratorData,
    SignalData,
)


class SignalHandler(QThread):
    signal_queue: Queue[int | None] = Queue()
    signalled = Signal(int)

    def run(self):
        while True:
            signal = self.signal_queue.get()
            if signal is None:
                break
            self.signalled.emit(signal)


SIGNALS_HANDLER = SignalHandler()


class SignalSourceModel(NodeDataModel):
    all_data_types = SignalData.data_type
    num_ports = PortCount(0, 1)

    def __init__(self, style=None, parent=None, signal_handler: SignalHandler | None = None):
        assert signal_handler is not None
        super().__init__(style, parent)
        self._signal_handler = signal_handler
        self._signal_handler.signalled.connect(self.signal_received)
        self.clicked = False
        self._signal_button = QToolButton()
        self._signal_button.clicked.connect(self.button_clicked)
        self._signal_button.setText("Release")
        self._slider = QSpinBox(parent)
        self._widget = QWidget()
        layout = QVBoxLayout()
        self._widget.setLayout(layout)
        layout.addWidget(self._signal_button)
        layout.addWidget(self._slider)

    def out_data(self, port: int) -> NodeData | None:
        if self.clicked:
            self.clicked = False
            return SignalData()
        return None

    @Slot(int)
    def signal_received(self, val: int):
        if self._slider.value() == val:
            self.button_clicked()

    def button_clicked(self):
        self.clicked = True
        self.data_updated.emit(0)

    def save(self) -> dict:
        dct = super().save()
        dct["slot"] = self._slider.value()
        return dct

    def restore(self, doc: dict):
        if "slot" in doc:
            self._slider.setValue(doc["slot"])

    def embedded_widget(self) -> QWidget:
        return self._widget


class SignalSinkModel(NodeDataModel):
    all_data_types = SignalData.data_type
    num_ports = PortCount(1, 0)

    def __init__(self, style=None, parent=None, signal_handler: SignalHandler | None = None):
        assert signal_handler is not None
        super().__init__(style, parent)
        self._signal_handler = signal_handler
        self._slider = QSpinBox(parent)
        self._widget = QWidget()
        layout = QVBoxLayout()
        self._widget.setLayout(layout)
        layout.addWidget(self._slider)

    def set_in_data(self, node_data: NodeData | None, port: Port):
        if node_data is None:
            return
        self._signal_handler.signal_queue.put(self._slider.value())

    def embedded_widget(self) -> QWidget:
        return self._widget

    def save(self) -> dict:
        dct = super().save()
        dct["slot"] = self._slider.value()
        return dct

    def restore(self, doc: dict):
        if "slot" in doc:
            self._slider.setValue(doc["slot"])


ALL_MODELS = [SignalSourceModel, SignalSinkModel]
