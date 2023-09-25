from __future__ import annotations

from abc import abstractmethod

# from PySide6.QtCore import QDate, QDateTime, QRect, Qt, QThread, QTime, Signal, Slot
# from PySide6.QtGui import QMouseEvent
from PySide6.QtWidgets import (
    # QCheckBox,
    # QDateTimeEdit,
    # QFrame,
    # QGridLayout,
    # QGroupBox,
    # QLineEdit,
    # QListWidget,
    # QListWidgetItem,
    # QMenu,
    # QPushButton,
    # QScrollArea,
    # QSizePolicy,
    # QSlider,
    # QSpinBox,
    # QSplitter,
    # QTextEdit,
    # QToolButton,
    # QVBoxLayout,
    # QWidget,
    QComboBox,
    QLabel,
)

from ..datarules import base_rules, data_rules, image_rules
from .frames import FlowItem


class ProducerView(FlowItem):
    title = "Producer"
    movable = False

    bound_item: type[base_rules.Producer]

    def setup_widget(self):
        super().setup_widget()
        if self.desc:
            self.desc += "\n"
        self.desc += f"Produces: {set(self.bound_item.produces)}"
        self.descriptionwidget.setText(self.desc)


class FileInfoProducerView(ProducerView):
    title = "File Info Producer"

    bound_item = data_rules.FileInfoProducer

    def get(self):
        super().get()
        return self.bound_item()


class ImShapeProducerView(ProducerView):
    title = "Image shape"
    bound_item = image_rules.ImShapeProducer

    def get(self):
        super().get()
        return self.bound_item()


class HashProducerView(ProducerView):
    title = "Hash Producer"
    desc = "gets a hash for the contents of an image"
    bound_item: type[image_rules.HashProducer] = image_rules.HashProducer
    needs_settings = True

    def configure_settings_group(self):
        self.hash_type = QComboBox()
        self.hash_type.addItems([*image_rules.HASHERS])
        self.groupgrid.addWidget(QLabel("Hash type: ", self), 0, 0)
        self.groupgrid.addWidget(self.hash_type, 0, 1)

    def reset_settings_group(self):
        self.hash_type.setCurrentIndex(0)

    def get_config(self):
        return {"hash_type": self.hash_type.currentText()}

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.hash_type.setCurrentText(cfg["hash_type"])
        return self

    def get(self):
        super().get()
        return self.bound_item(image_rules.HASHERS(self.hash_type.currentText()))
