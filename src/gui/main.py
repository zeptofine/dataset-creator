#!/usr/bin/python
from __future__ import annotations

import json
import os
import sys

# import time
# from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypedDict

from PySide6 import QtWidgets
from PySide6.QtCore import Qt, Signal, Slot  # QThread,,
from PySide6.QtGui import QAction, QIcon, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    # QGridLayout,
    # QGroupBox,
    # QProgressBar,
    # QPushButton,
    QDialog,
    QDialogButtonBox,
    # QCheckBox,
    # QComboBox,
    # QDialog,
    QFileDialog,
    QLabel,
    QLineEdit,
    # QScrollArea,
    # QSlider,
    QSplitter,
    # QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)
from rich import print as rprint

from ..datarules.dataset_builder import DatasetBuilder
from .err_dialog import catch_errors
from .frames import FlowList, ItemConfig
from .input_view import InputView
from .output_view import OutputView
from .producer_views import (
    FileInfoProducerView,
    HashProducerView,
    ImShapeProducerView,
    ProducerView,
)
from .rule_views import (
    BlacklistWhitelistView,
    ChannelRuleView,
    ExistingRuleView,
    HashRuleView,
    ResRuleView,
    RuleView,
    StatRuleView,
    TotalLimitRuleView,
)

CPU_COUNT = os.cpu_count()
PROGRAM_ORIGIN = Path(__file__).parent


class Config(TypedDict):
    inputs: list[ItemConfig]
    producers: list[ItemConfig]
    rules: list[ItemConfig]
    output: list[ItemConfig]


class InputList(FlowList):
    items: list[InputView]

    gathered = Signal(dict)

    def add_item(self, item: InputView, *args, **kwargs):
        item.gathered.connect(self.gathered.emit)
        return super().add_item(item, *args, **kwargs)


class ProducerList(FlowList):
    items: list[ProducerView]


class RuleList(FlowList):
    items: list[RuleView]


class OutputList(FlowList):
    items: list[OutputView]


catch_loading = catch_errors("loading failed")
catch_gathering = catch_errors("gathering failed")
catch_building = catch_errors("building failed")


class Window(QWidget):
    def __init__(self, cfg_path=Path("config.json")):
        super().__init__()
        self.setWindowTitle("dataset-creator")
        self.resize(1200, 500)
        self.setMinimumSize(400, 300)
        self._layout = QtWidgets.QGridLayout(self)
        self.setLayout(self._layout)
        self.cfg_path = cfg_path
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)

        self.lists = QSplitter(self)
        self.producers_rules = QSplitter(self)
        self.producers_rules.setOrientation(Qt.Orientation.Vertical)

        self.inputlist = InputList(self)
        self.inputlist.register_items(InputView)
        self.inputlist.gathered.connect(self.collect_files)
        self.filedict = {}

        self.producerlist = ProducerList(self)
        self.producerlist.register_items(
            FileInfoProducerView,
            ImShapeProducerView,
            HashProducerView,
        )

        self.rulelist = RuleList(self)
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

        self.lists.addWidget(self.inputlist)
        self.producers_rules.addWidget(self.producerlist)
        self.producers_rules.addWidget(self.rulelist)
        # self.producers_rules.setStretchFactor(1, 10)
        # self.producers_rules.setStretchFactor(1, 10)
        self.lists.addWidget(self.producers_rules)
        self.lists.addWidget(self.outputlist)

        self.save_shortcut = QShortcut(QKeySequence("Ctrl+S"), self, self.save_cfg)
        (get_producers := QAction("get producers", self)).triggered.connect(self.gather_producers)
        (get_rules := QAction("get rules", self)).triggered.connect(self.gather_rules)
        (get_builder := QAction("get builder", self)).triggered.connect(self.create_builder)
        (get_files := QAction("get files", self)).triggered.connect(self.gather_files)
        (print_files := QAction("print files", self)).triggered.connect(lambda: rprint(self.filedict))
        self.addActions([get_producers, get_rules, get_builder, get_files, print_files])

        self.file_select_txt = QLineEdit(self)
        self.fileselect = QToolButton(self)
        self.fileselect.setText("...")
        self.fileselect.setIcon(QIcon.fromTheme("folder-open"))
        self.fileselect.clicked.connect(self.select_folders)

        self._layout.addWidget(self.lists, 0, 0, 1, 10)
        self._layout.addWidget(self.file_select_txt, 2, 0, 1, 3)
        self._layout.addWidget(self.fileselect, 2, 3)

        if not cfg_path.exists():
            self.save_cfg()
        with self.cfg_path.open("r") as f:
            self.load_cfg(Config(json.load(f)))

    def get_cfg(self):
        return Config(
            {
                "inputs": self.inputlist.get_cfg(),
                "output": self.outputlist.get_cfg(),
                "producers": self.producerlist.get_cfg(),
                "rules": self.rulelist.get_cfg(),
            }
        )

    @catch_errors("Error saving")
    @Slot()
    def save_cfg(self):
        with self.cfg_path.open("w") as f:
            json.dump(self.get_cfg(), f, indent=4)

    @catch_loading
    @Slot(dict)
    def load_cfg(self, cfg):
        self.inputlist.add_from_cfg(cfg["inputs"])
        self.producerlist.add_from_cfg(cfg["producers"])
        self.rulelist.add_from_cfg(cfg["rules"])
        self.outputlist.add_from_cfg(cfg["output"])
        pass

    @catch_gathering
    @Slot()
    def gather_producers(self):
        rprint(self.producerlist.get())

    @catch_gathering
    @Slot()
    def gather_rules(self):
        rprint(self.rulelist.get())

    @catch_gathering
    @Slot()
    def gather_files(self):
        self.filedict.clear()
        self.inputlist.get()

    @Slot(dict)
    def collect_files(self, dct):
        # print(dct)
        self.filedict.update(dct)

    @catch_building
    @Slot()
    def create_builder(self):
        print("building...")
        producers = self.producerlist.get()
        rules = self.rulelist.get()
        rprint(producers, rules)

        builder = DatasetBuilder(Path("filedb.feather"))

        builder.add_producers(producers)
        builder.add_rules(rules)
        rprint(builder)
        pass

    @Slot()
    def select_folders(self):
        filename: str = QFileDialog.getExistingDirectory(
            self,
            "Select output folder",
            str(Path.home()),
            QFileDialog.Option.ShowDirsOnly | QFileDialog.Option.DontResolveSymlinks,
        )
        if filename:
            self.file_select_txt.setText(filename)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cfg_path")

    args = parser.parse_args()
    app = QtWidgets.QApplication([])
    central_window = Window(Path(args.cfg_path)) if args.cfg_path else Window()
    if sys.platform == "win32":
        import pywinstyles

        pywinstyles.apply_style(central_window, "acrylic")

    central_window.show()
    code = app.exec()
    sys.exit(code)
