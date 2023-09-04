#!/usr/bin/python
from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from pprint import pprint

from PySide6 import QtGui, QtWidgets
from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QSplitter,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from src.gui.rule_frames import (
    BlacklistWhitelistView,
    ChannelRuleView,
    ExistingRuleView,
    FileInfoProducerView,
    # FlowItem,
    FlowList,
    HashProducerView,
    HashRuleView,
    ImShapeProducerView,
    ResRuleView,
    StatRuleView,
    TotalLimitRuleView,
)

CPU_COUNT = os.cpu_count()
PROGRAM_ORIGIN = Path(__file__).parent


class Window(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("dataset-creator")
        self.resize(800, 500)
        self.setMinimumSize(400, 300)

        self._layout = QtWidgets.QGridLayout(self)
        self.setLayout(self._layout)

        self.producerlist = FlowList()

        self.producerlist.register_items(FileInfoProducerView, ImShapeProducerView, HashProducerView)

        self._layout.addWidget(self.producerlist, 0, 0)

        self.rulelist = FlowList()
        self.rulelist.register_items(
            StatRuleView,
            BlacklistWhitelistView,
            ExistingRuleView,
            TotalLimitRuleView,
            ResRuleView,
            ChannelRuleView,
            HashRuleView,
        )
        self._layout.addWidget(self.rulelist, 0, 1)


if __name__ == "__main__":
    app = QtWidgets.QApplication([])
    central_window = Window()
    central_window.show()
    code = app.exec()
    sys.exit(code)
