#!/usr/bin/python

import json
import os
import sys
from pathlib import Path

from PySide6 import QtWidgets
from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtGui import QAction, QKeySequence, QShortcut
from PySide6.QtWidgets import QFileDialog, QMainWindow, QSplitter, QStatusBar, QToolBar, QWidget, QMenu
from rich import print as rprint

from ..configs import MainConfig
from ..datarules.dataset_builder import DatasetBuilder
from .err_dialog import catch_errors
from .frames import FlowList
from .input_view import InputView
from .output_view import OutputView
from .producer_views import FileInfoProducerView, HashProducerView, ImShapeProducerView, ProducerView
from .rule_views import (
    BlacklistWhitelistView,
    ChannelRuleView,
    HashRuleView,
    ResRuleView,
    RuleView,
    StatRuleView,
    TotalLimitRuleView,
)

CPU_COUNT = os.cpu_count()
PROGRAM_ORIGIN = Path(__file__).parent
RECENT_FILES_PATH = PROGRAM_ORIGIN / ".recent_files"


def get_recent_files():
    if RECENT_FILES_PATH.exists():
        return RECENT_FILES_PATH.read_text().splitlines()
    return []


def save_recent_files(files):
    RECENT_FILES_PATH.write_text("\n".join(files))


class InputList(FlowList):
    items: list[InputView]

    gathered = Signal(dict)

    def add_item(self, item: InputView, *args, **kwargs):
        item.gathered.connect(self.gathered.emit)
        return super().add_item(item, *args, **kwargs)


class ProducerList(FlowList):
    items: list[ProducerView]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__registered_by: dict[str, type[ProducerView]] = {}

    def additemtomenu(self, item: type[ProducerView]):
        self.addmenu.addAction(f"{item.title}: {set(item.bound_item.produces)}", lambda: self.initialize_item(item))

    def _register_item(self, item: type[ProducerView]):
        super()._register_item(item)
        for produces in item.bound_item.produces:
            self.__registered_by[produces] = item

    def registered_by(self, s: str):
        return self.__registered_by.get(s)


class RuleList(FlowList):
    items: list[RuleView]


class OutputList(FlowList):
    items: list[OutputView]


catch_loading = catch_errors("loading failed")
catch_gathering = catch_errors("gathering failed")
catch_building = catch_errors("building failed")


class Window(QMainWindow):
    def __init__(self, cfg_path=Path("config.json")):
        super().__init__()
        self.setWindowTitle("dataset-creator")
        self.resize(1200, 500)
        self.setMinimumSize(400, 300)
        self.cfg_path = cfg_path
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)

        self.lists = QSplitter(self)
        self.setCentralWidget(self.lists)
        self.producers_rules = QSplitter(self)
        self.producers_rules.setOrientation(Qt.Orientation.Vertical)

        self.inputlist = InputList(self)
        self.inputlist.set_text("Inputs")
        self.inputlist.register_item(InputView)
        self.inputlist.gathered.connect(self.collect_files)
        self.filedict = {}

        self.producerlist = ProducerList(self)
        self.producerlist.set_text("Producers")
        self.producerlist.register_item(
            FileInfoProducerView,
            ImShapeProducerView,
            HashProducerView,
        )

        self.rulelist = RuleList(self)
        self.rulelist.set_text("Rules")
        self.rulelist.register_item(
            StatRuleView,
            BlacklistWhitelistView,
            TotalLimitRuleView,
            ResRuleView,
            ChannelRuleView,
            HashRuleView,
        )

        self.outputlist = FlowList(self)
        self.outputlist.set_text("Outputs")
        self.outputlist.register_item(OutputView)

        self.lists.addWidget(self.inputlist)
        self.producers_rules.addWidget(self.producerlist)
        self.producers_rules.addWidget(self.rulelist)
        self.lists.addWidget(self.producers_rules)
        self.lists.addWidget(self.outputlist)

        (save_action := QAction("Save", self)).triggered.connect(self.save_config)
        (save_as_action := QAction("Save As...", self)).triggered.connect(self.save_config_as)
        (open_action := QAction("Open...", self)).triggered.connect(self.open_config)
        (reload_action := QAction("Reload", self)).triggered.connect(self.load_config)
        (clear_action := QAction("clear", self)).triggered.connect(self.clear)
        save_action.setShortcut(QKeySequence("Ctrl+S"))
        save_as_action.setShortcut(QKeySequence("Ctrl+Shift+S"))
        open_action.setShortcut(QKeySequence("Ctrl+O"))
        reload_action.setShortcut(QKeySequence("Ctrl+R"))
        menu = self.menuBar()

        filemenu = menu.addMenu("File")
        filemenu.addAction(open_action)
        filemenu.addAction(save_action)
        filemenu.addAction(save_as_action)
        filemenu.addAction(reload_action)

        self.recents_menu = QMenu("Open Recent", self)
        self.recent_files = []
        filemenu.addMenu(self.recents_menu)

        editmenu = menu.addMenu("Edit")
        editmenu.addAction(clear_action)

        # (get_builder := QAction("get builder", self)).triggered.connect(self.create_builder)
        # (run_builder := QAction("run builder", self)).triggered.connect(self.run_builder)

        if self.cfg_path.exists():
            self.load_config()

    def get_config(self) -> MainConfig:
        return {
            "inputs": self.inputlist.get_config(),
            "output": self.outputlist.get_config(),
            "producers": self.producerlist.get_config(),
            "rules": self.rulelist.get_config(),
        }

    @catch_errors("Error saving")
    @Slot()
    def save_config(self):
        cfg = self.get_config()
        with self.cfg_path.open("w") as f:
            json.dump(cfg, f, indent=4)
            print("saved", cfg)

    @catch_errors("Error saving")
    @Slot()
    def save_config_as(self):
        file = QFileDialog.getSaveFileName(
            self,
            "Select cfg path",
            str(self.cfg_path),
            "JSON files (*.json)",
        )[0]
        if file:
            self.cfg_path = Path(file)
            self.save_config()

    @Slot()
    def load_config(self):
        with self.cfg_path.open("r") as f:
            self.from_cfg(MainConfig(json.load(f)))
            self.update_recents()

    @Slot()
    def open_config(self, s: str = ""):
        if not s:
            file = QFileDialog.getOpenFileName(
                self,
                "Select cfg path",
                str(self.cfg_path.parent),
            )[0]
        else:
            file = s
        if file:
            print(f"Opening {file}")
            self.cfg_path = Path(file)
            self.load_config()

    @Slot()
    def clear(self):
        self.inputlist.empty()
        self.producerlist.empty()
        self.rulelist.empty()
        self.outputlist.empty()

    @Slot()
    def update_recents(self):
        recents = get_recent_files()
        if (txt := str(self.cfg_path.resolve())) not in recents:
            recents.insert(0, txt)
        else:
            recents.remove(txt)
            recents.insert(0, txt)
        save_recent_files(recents[:10])

        self.recents_menu.clear()
        for file in recents:
            self.recents_menu.addAction(file).triggered.connect(lambda: self.open_config(file))

    @catch_loading
    @Slot(dict)
    def from_cfg(self, cfg: MainConfig):
        self.clear()
        self.inputlist.add_from_cfg(cfg["inputs"])
        self.producerlist.add_from_cfg(cfg["producers"])
        self.rulelist.add_from_cfg(cfg["rules"])
        self.outputlist.add_from_cfg(cfg["output"])

    @Slot(dict)
    def collect_files(self, dct):
        self.filedict.update(dct)

    @catch_building
    @Slot()
    def create_builder(self):
        print("building builder...")
        producers = self.producerlist.get()
        rules = self.rulelist.get()

        self.builder = DatasetBuilder(Path("filedb.arrow"))

        self.builder.add_producers(*producers)
        self.builder.add_rules(*rules)
        rprint(self.builder)
        print("built builder.")
        return self.builder

    @Slot()
    def run_builder(self):
        pathdict: dict[Path, list[Path]] = {Path(src): list(map(Path, dst)) for src, dst in self.filedict.items()}
        all_files = {str(src / file) for src, lst in pathdict.items() for file in lst}

        self.builder.add_new_paths(all_files)

        print("gathered resources")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cfg_path")
    args = parser.parse_args()

    # check all recent files exist

    save_recent_files([file for file in get_recent_files() if os.path.exists(file)])

    app = QtWidgets.QApplication([])
    central_window = Window(Path(args.cfg_path)) if args.cfg_path else Window()
    central_window.show()
    code = app.exec()
    sys.exit(code)
