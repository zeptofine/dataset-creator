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

    def add_item_to_menu(self, item: type[ProducerView]):
        self.add_menu.addAction(f"{item.title}: {set(item.bound_item.produces)}", lambda: self.initialize_item(item))

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

        self.input_list = InputList(self)
        self.input_list.set_text("Inputs")
        self.input_list.register_item(InputView)
        self.input_list.gathered.connect(self.collect_files)
        self.file_dict = {}

        self.producer_list = ProducerList(self)
        self.producer_list.set_text("Producers")
        self.producer_list.register_item(
            FileInfoProducerView,
            ImShapeProducerView,
            HashProducerView,
        )

        self.rule_list = RuleList(self)
        self.rule_list.set_text("Rules")
        self.rule_list.register_item(
            StatRuleView,
            BlacklistWhitelistView,
            TotalLimitRuleView,
            ResRuleView,
            ChannelRuleView,
            HashRuleView,
        )

        self.output_list = FlowList(self)
        self.output_list.set_text("Outputs")
        self.output_list.register_item(OutputView)

        self.lists.addWidget(self.input_list)
        self.producers_rules.addWidget(self.producer_list)
        self.producers_rules.addWidget(self.rule_list)
        self.lists.addWidget(self.producers_rules)
        self.lists.addWidget(self.output_list)

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

        file_menu = menu.addMenu("File")
        file_menu.addAction(open_action)
        file_menu.addAction(save_action)
        file_menu.addAction(save_as_action)
        file_menu.addAction(reload_action)

        self.recent_menu = QMenu("Open Recent", self)
        self.recent_files = []
        file_menu.addMenu(self.recent_menu)

        edit_menu = menu.addMenu("Edit")
        edit_menu.addAction(clear_action)

        # (get_builder := QAction("get builder", self)).triggered.connect(self.create_builder)
        # (run_builder := QAction("run builder", self)).triggered.connect(self.run_builder)

        if self.cfg_path.exists():
            self.load_config()

    def get_config(self) -> MainConfig:
        return {
            "inputs": self.input_list.get_config(),
            "output": self.output_list.get_config(),
            "producers": self.producer_list.get_config(),
            "rules": self.rule_list.get_config(),
        }

    @catch_errors("Error saving")
    @Slot()
    def save_config(self):
        cfg = self.get_config()
        with self.cfg_path.open("w") as f:
            json.dump(cfg, f, indent=4)
            rprint("saved", cfg)

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
            self.update_recent()

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
        self.input_list.empty()
        self.producer_list.empty()
        self.rule_list.empty()
        self.output_list.empty()

    @Slot()
    def update_recent(self):
        recent = get_recent_files()
        if (txt := str(self.cfg_path.resolve())) not in recent:
            recent.insert(0, txt)
        else:
            recent.remove(txt)
            recent.insert(0, txt)
        save_recent_files(recent[:10])

        self.recent_menu.clear()
        for file in recent:
            self.recent_menu.addAction(file).triggered.connect(lambda: self.open_config(file))

    @catch_loading
    @Slot(dict)
    def from_cfg(self, cfg: MainConfig):
        self.clear()
        self.input_list.add_from_cfg(cfg["inputs"])
        self.producer_list.add_from_cfg(cfg["producers"])
        self.rule_list.add_from_cfg(cfg["rules"])
        self.output_list.add_from_cfg(cfg["output"])

    @Slot(dict)
    def collect_files(self, dct):
        self.file_dict.update(dct)

    @catch_building
    @Slot()
    def create_builder(self):
        print("building builder...")
        producers = self.producer_list.get()
        rules = self.rule_list.get()

        self.builder = DatasetBuilder(Path("filedb.arrow"))

        self.builder.add_producers(*producers)
        self.builder.add_rules(*rules)
        rprint(self.builder)
        print("built builder.")
        return self.builder

    @Slot()
    def run_builder(self):
        path_dict: dict[Path, list[Path]] = {Path(src): list(map(Path, dst)) for src, dst in self.file_dict.items()}
        all_files = {str(src / file) for src, lst in path_dict.items() for file in lst}

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
