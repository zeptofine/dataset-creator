import contextlib
from abc import abstractmethod
from pathlib import Path

import cv2
import numpy as np
from PySide6.QtCore import Signal, Slot
from qtpy.QtCore import QEvent, QObject, Qt, QThread
from qtpy.QtGui import QImage, QPixmap
from qtpy.QtWidgets import (
    QLabel,
    QWidget,
)
from qtpynodeeditor import (
    CaptionOverride,
    DataTypes,
    NodeData,
    NodeDataModel,
    Port,
    PortCount,
)

from ..datarules.base_rules import Filter
from ..datarules.image_rules import _get_hwc
from ..gui.config_inputs import ItemDeclaration
from ..gui.output_view import (
    BlurFilterView_,
    CompressionFilterView_,
    CropFilterView_,
    FilterDeclaration,
    NoiseFilterView_,
    RandomFlipFilterView_,
    RandomRotateFilterView_,
    ResizeFilterView_,
)
from ..gui.settings_inputs import SettingsBox
from .base_types.base_types import (
    ImageData,
    IntegerData,
    PathData,
)


class ImageReaderThread(QThread):
    pth: Path
    image_read = Signal(np.ndarray)

    def run(self):
        img = cv2.imread(str(self.pth))
        if img is not None:
            self.image_read.emit(img)


class ImageReaderNode(NodeDataModel):
    name = "Read Image"
    data_types = DataTypes(
        {0: PathData.data_type},
        {0: ImageData.data_type},
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._img = None
        self._reader_thread = ImageReaderThread()
        self._reader_thread.image_read.connect(self.set_image)

    def out_data(self, port: int) -> ImageData | None:
        if self._img is None:
            return None
        return ImageData(self._img)

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            return

        self._reader_thread.pth = node_data.path
        self._reader_thread.start()
        self._reader_thread.wait()

    def set_image(self, img):
        self._img = img
        self.data_updated.emit(0)


class ImageViewerNode(NodeDataModel):
    name = "View Image"
    num_ports = PortCount(1, 0)
    all_data_types = ImageData.data_type

    def resizable(self) -> bool:
        return True

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._node_data = None
        self._pixmap = None
        self._pixmap_label = QLabel()
        self._pixmap_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._pixmap_label.setFixedSize(200, 200)
        self._pixmap_label.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if watched is self._pixmap_label and event.type() == QEvent.Type.Resize and self._pixmap is not None:
            self.update_pixmap()
        return False

    def update_pixmap(self):
        if self._pixmap is not None:
            pixmap = self._pixmap

            w, h = self._pixmap_label.width(), self._pixmap_label.height()
            if w < self._pixmap.width() or h < self._pixmap.height():
                pixmap = pixmap.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatio)

            self._pixmap_label.setPixmap(pixmap)

    def set_in_data(self, node_data: ImageData | None, port: Port):
        self._node_data = node_data
        if node_data is not None:
            im = node_data.image
            if im is None:
                return
            im: np.ndarray = cv2.cvtColor(im, cv2.COLOR_BGR2RGB)
            im = im.astype(np.uint32)
            shape = im.shape
            im = (255 << 24 | im[:, :, 0] << 16 | im[:, :, 1] << 8 | im[:, :, 2]).flatten()  # pack RGB values
            image = QImage(  # type: ignore
                im.tobytes(),
                shape[1],
                shape[0],
                QImage.Format.Format_RGB32,
            )
            self._pixmap = QPixmap(image)
            self.update_pixmap()

    def embedded_widget(self) -> QWidget:
        return self._pixmap_label


def get_h_w_c(image: np.ndarray) -> tuple[int, int, int]:
    """Returns the height, width, and number of channels."""
    h, w = image.shape[:2]
    c = 1 if image.ndim == 2 else image.shape[2]
    return h, w, c


class ImageShapeNode(NodeDataModel):
    num_ports = PortCount(1, 3)
    data_types = DataTypes(
        {
            0: ImageData.data_type,
        },
        {
            0: IntegerData.data_type,
            1: IntegerData.data_type,
            2: IntegerData.data_type,
        },
    )
    caption_override = CaptionOverride(
        outputs={
            0: "Height",
            1: "Width",
            2: "Channels",
        }
    )

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._result: tuple[int, int, int] | None = None

    def out_data(self, port: int) -> IntegerData | None:
        if self._result is None:
            return None
        return IntegerData(self._result[port])

    def set_in_data(self, node_data: ImageData | None, port: Port):
        if node_data is None:
            self._result = None
            return

        self._result = get_h_w_c(node_data.image)
        self.data_updated.emit(0)
        self.data_updated.emit(1)
        self.data_updated.emit(2)


class FastImageShapeNode(ImageShapeNode):
    name = "Fast Image Shape"
    caption = "Fast Image Shape"
    data_types = DataTypes(
        {
            0: PathData.data_type,
        },
        {
            0: IntegerData.data_type,
            1: IntegerData.data_type,
            2: IntegerData.data_type,
        },
    )

    def set_in_data(self, node_data: PathData | None, port: Port):
        if node_data is None:
            self._result = None
            return

        self._result = _get_hwc(node_data.path)
        self.data_updated.emit(0)
        self.data_updated.emit(1)
        self.data_updated.emit(2)


class ImageConverterThread(QThread):
    f: Filter
    img: np.ndarray

    completed_data = Signal(np.ndarray)

    def run(self):
        self.completed_data.emit(self.f.run(self.img))


class BasicImageConverterModel(NodeDataModel):
    all_data_types = ImageData.data_type
    num_ports = PortCount(1, 1)

    item: FilterDeclaration

    def __init__(self, style=None, parent=None):
        super().__init__(style, parent)
        self._output: ImageData | None = None
        self._thread = ImageConverterThread(parent)
        self._thread.completed_data.connect(self.set_output)
        self._settings = self.get_widget()

    def out_data(self, port: int) -> NodeData | None:
        if self._output is None:
            return None
        return self._output

    def set_in_data(self, node_data: ImageData | None, port: Port):
        if node_data is None:
            return
        f: Filter = self.item.get(self._settings)
        # self._thread.wait()
        self._thread.f = f
        self._thread.img = node_data.image

        self._thread.start()
        self._thread.wait()

        # self._output = ImageData(obj.run(node_data.image))
        # self.data_updated.emit(0)

    @Slot(np.ndarray)
    def set_output(self, img: np.ndarray):
        self._output = ImageData(img)
        self.data_updated.emit(0)

    def compute(self, img: ImageData):
        self._output = img

    def embedded_widget(self) -> QWidget:
        return self._settings

    @abstractmethod
    def get_widget(self) -> SettingsBox:
        ...

    def save(self) -> dict:
        doc = super().save()
        if self._settings is not None:
            doc["settings"] = self._settings.get_cfg()
        return doc

    def restore(self, doc: dict):
        with contextlib.suppress(KeyError):
            self._settings.from_cfg(doc["settings"])


def new_converter_model(filter_view: ItemDeclaration):
    return type(
        filter_view.title,
        (BasicImageConverterModel,),
        {
            "name": filter_view.title,
            "caption": filter_view.title,
            "get_widget": filter_view.create_settings_widget,
            "item": filter_view,
        },
    )


ALL_MODELS = [
    ImageReaderNode,
    ImageViewerNode,
    ImageShapeNode,
    FastImageShapeNode,
] + [
    new_converter_model(item)
    for item in (
        BlurFilterView_,
        CompressionFilterView_,
        CropFilterView_,
        NoiseFilterView_,
        RandomFlipFilterView_,
        RandomRotateFilterView_,
        ResizeFilterView_,
    )
]
