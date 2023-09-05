from __future__ import annotations

import os
import sys
import textwrap
from abc import abstractmethod
from pathlib import Path
from pprint import pprint

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


class OutputView(FlowItem):
    title = "Output"

    needs_settings = True

    def setup_widget(self):
        super().setup_widget()
        self.text = QLineEdit(self)
        self.fileselect = QToolButton(self)
        self.fileselect.setText("...")
        self.fileselect.setIcon(QIcon.fromTheme("folder-open"))
        self.fileselect.clicked.connect(self.select_folder)
        self.list = FlowList(self)
        self.list.setMinimumHeight(400)
        self.list.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)  # type: ignore

        self.list.register_items()
        self.groupgrid.setGeometry(QRect(0, 0, 800, 800))
        self.groupgrid.addWidget(QLabel("Folder: "), 0, 0)
        self.groupgrid.addWidget(self.text, 1, 0)
        self.groupgrid.addWidget(self.fileselect, 1, 1)
        self.groupgrid.addWidget(QLabel("Filters: "), 2, 0, 1, 2)
        self.groupgrid.addWidget(self.list, 3, 0, 1, 2)

    @Slot()
    def select_folder(self):
        filename = QFileDialog.getExistingDirectory(
            self,
            "Select output folder",
            str(Path.home()),
        )
        self.text.setText(filename)
