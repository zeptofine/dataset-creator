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

    bound_producer: type[base_rules.Producer]

    def setup_widget(self):
        super().setup_widget()
        if self.desc:
            self.desc += "\n"
        self.desc += f"Produces: {set(self.bound_producer.produces)}"
        self.descriptionwidget.setText(self.desc)
        # return super().setup_widget()

    @abstractmethod
    def get_producer(self):
        """Evaluates the settings and returns a Producer instance"""


class FileInfoProducerView(ProducerView):
    title = "File Info Producer"
    bound_producer = data_rules.FileInfoProducer


class ImShapeProducerView(ProducerView):
    title = "Image shape"
    bound_producer = image_rules.ImShapeProducer


class HashProducerView(ProducerView):
    title = "Hash Producer"
    desc = "gets a hash for the contents of an image"
    bound_producer = image_rules.HashProducer
    needs_settings = True

    def configure_settings_group(self):
        hash_type = QComboBox()
        hash_type.addItems([*image_rules.HASHERS])
        self.groupgrid.addWidget(QLabel("Hash type: "), 0, 0)
        self.groupgrid.addWidget(hash_type, 0, 1)
