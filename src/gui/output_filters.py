from abc import abstractmethod
from pathlib import Path
from pprint import pprint

import cv2
import numpy as np
from PySide6.QtCore import QDate, QDateTime, QRect, Qt, QThread, QTime, Signal, Slot
from PySide6.QtGui import QIcon, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QSpinBox,
    QSplitter,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ..datarules import base_rules, data_rules, image_rules
from .frames import FlowItem, FlowList


class Filter(FlowItem):
    title = "Filter"

    @abstractmethod
    def __call__(self, img: np.ndarray):
        return img


class ResizeFilter(Filter):
    title = "Resize"
    needs_settings = True

    def configure_settings_group(self):
        self.scale = QSpinBox()
        self.scale.setPrefix("1/")
        self.scale.setMinimum(1)
        self.groupgrid.addWidget(QLabel("Scale:", self), 0, 0)
        self.groupgrid.addWidget(self.scale, 0, 1)

    def get_config(self):
        return {"scale": self.scale.value()}

    @classmethod
    def from_config(cls, cfg: dict, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"])
        return self
