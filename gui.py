#!/usr/bin/python
from __future__ import annotations

import os
import sys

# import time
# from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypedDict

from PySide6 import QtWidgets
from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtWidgets import (
    # QCheckBox,
    # QComboBox,
    # QDialog,
    # QGridLayout,
    # QGroupBox,
    # QLabel,
    # QProgressBar,
    # QPushButton,
    # QScrollArea,
    # QSlider,
    QSplitter,
    # QTextEdit,
    QToolButton,
    QWidget,
)
from rich import print as rprint

from src.gui.frames import FlowList, ItemConfig
from src.gui.output_view import OutputView
from src.gui.producer_views import (
    FileInfoProducerView,
    HashProducerView,
    ImShapeProducerView,
)
from src.gui.rule_views import (
    BlacklistWhitelistView,
    ChannelRuleView,
    ExistingRuleView,
    HashRuleView,
    ResRuleView,
    StatRuleView,
    TotalLimitRuleView,
)

CPU_COUNT = os.cpu_count()
PROGRAM_ORIGIN = Path(__file__).parent


class Config(TypedDict):
    producers: list[ItemConfig]
    rules: list[ItemConfig]
    output: list[ItemConfig]


class Window(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("dataset-creator")
        self.resize(1200, 500)
        self.setMinimumSize(400, 300)
        self._layout = QtWidgets.QGridLayout(self)
        self.setLayout(self._layout)

        self.lists = QSplitter(self)
        self.producerlist = FlowList(self)
        self.producerlist.register_items(
            FileInfoProducerView,
            ImShapeProducerView,
            HashProducerView,
        )

        self.rulelist = FlowList(self)
        self.rulelist.register_items(
            StatRuleView,
            BlacklistWhitelistView,
            ExistingRuleView,
            TotalLimitRuleView,
            ResRuleView,
            ChannelRuleView,
            HashRuleView,
        )

        self.outputlist = FlowList(self)
        self.outputlist.register_items(OutputView)

        self._layout.addWidget(self.lists, 0, 0)
        self.lists.addWidget(self.producerlist)
        self.lists.addWidget(self.rulelist)
        self.lists.addWidget(self.outputlist)

        self.get_cfg_button = QToolButton(self)
        self.get_cfg_button.setText("get cfg")
        self.get_cfg_button.clicked.connect(self.save_cfg)
        self._layout.addWidget(self.get_cfg_button, 1, 0)

        cfg = {
            "producers": [],
            "rules": [
                {"name": "Time Range", "data": {"after": "01/01/1970 12:00 AM", "before": "23/04/2024 8:24 PM"}},
                {
                    "name": "Blacklist and whitelist",
                    "data": {
                        "whitelist": ["me", "and", "you"],
                        "whitelist_exclusive": False,
                        "blacklist": ["us", "too"],
                    },
                },
                {"name": "Existing", "data": {"list": [], "exists_in": "all"}},
                {"name": "Total count", "data": {"limit": 0}},
                {"name": "Resolution", "data": {"min": 0, "max": 2048, "crop": True, "scale": 4}},
                {"name": "Channels", "data": {"min": 0, "max": 3}},
                {"name": "Hash", "data": {"resolver": "mtime", "ignore_all": False}},
            ],
            "output": [],
        }
        self.load_cfg(cfg)

    @Slot()
    def save_cfg(self):
        out = Config(
            {
                "producers": self.producerlist.get_cfg(),
                "rules": self.rulelist.get_cfg(),
                "output": self.outputlist.get_cfg(),
            }
        )
        rprint(out)

    @Slot(dict)
    def load_cfg(self, cfg):
        self.rulelist.add_from_cfg(cfg["rules"])
        pass


if __name__ == "__main__":
    app = QtWidgets.QApplication([])
    central_window = Window()
    central_window.show()
    code = app.exec()
    sys.exit(code)
