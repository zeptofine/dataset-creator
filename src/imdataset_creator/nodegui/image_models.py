import random
import threading
from pathlib import Path
from queue import Empty, Queue

import cv2
import numpy as np
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


class ImageReaderDataModel(NodeDataModel):
    data_types = DataTypes(
        {0: PathData.data_type},
        {0: ImageData.data_type},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._img = None

    def out_data(self, port: int) -> ImageData | None:
        if self._img is None:
            return None
        return ImageData(self._img)

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            return

        self._img = cv2.imread(str(node_data.path))
        if self._img is None:
            self._img = np.ndarray((0, 0, 3))
        self.data_updated.emit(0)
