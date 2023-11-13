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
    ImageData,
    ListData,
    PathData,
    PathGeneratorData,
    generator_to_list_converter,
    list_to_generator_converter,
)
from .image_models import (
    ImageReaderDataModel,
)
from .lists_and_generators import (
    FileGlobber,
    GeneratorResolverDataModel,
    GeneratorSplitterDataModel,
    GeneratorStepper,
    ListBufferDataModel,
    ListHeadDataModel,
    ListShufflerDataModel,
    get_text_bounds,
)


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


def register_type(registry: ne.DataModelRegistry, from_: NodeDataType, to_: NodeDataType, converter: Callable):
    registry.register_type_converter(from_, to_, TypeConverter(from_, to_, converter))


def main(app):
    registry = ne.DataModelRegistry()

    model_dct = {
        "generators": [
            FileGlobber,
            GeneratorStepper,
            GeneratorSplitterDataModel,
            GeneratorResolverDataModel,
        ],
        "lists": [
            ListBufferDataModel,
            ListHeadDataModel,
            ListShufflerDataModel,
        ],
        "images": [
            ImageReaderDataModel,
        ],
        "misc": [
            PrinterDataModel,
        ],
    }
    for category, models in model_dct.items():
        for model in models:
            registry.register_model(model, category)

    register_type(registry, PathGeneratorData.data_type, ListData.data_type, generator_to_list_converter)
    register_type(registry, ListData.data_type, PathGeneratorData.data_type, list_to_generator_converter)
    register_type(registry, AnyData.data_type, ListData.data_type, lambda item: ListData(list(item.item)))
    register_type(registry, ListData.data_type, AnyData.data_type, lambda item: AnyData(item.list))
    register_type(registry, PathData.data_type, AnyData.data_type, lambda item: AnyData(str(item.path)))
    register_type(registry, ImageData.data_type, AnyData.data_type, lambda item: AnyData(item.image))

    scene = ne.FlowScene(registry=registry)
    view = ne.FlowView(scene)
    view.setWindowTitle("Generator ui")
    view.resize(1920, 600)
    view.show()

    scene.load("text.flow")
    return scene, view


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    app = QApplication([])
    scene, view = main(app)
    scene.load("text.flow")
    app.exec_()
    scene.save("text.flow")
