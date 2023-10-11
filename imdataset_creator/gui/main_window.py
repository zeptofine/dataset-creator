#!/usr/bin/python

import json
import os
import sys
from pathlib import Path

from PySide6 import QtWidgets
from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtGui import QAction, QKeySequence, QShortcut
from PySide6.QtWidgets import QFileDialog, QMainWindow, QSplitter, QStatusBar, QToolBar, QWidget, QMenu, QGridLayout
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
    RECENT_FILES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RECENT_FILES_PATH.open("w") as f:
        f.writelines(files)


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


class MainWidget(QWidget):
    status = Signal(str)

    def __init__(self, parent, cfg_path=Path("config.json")):
        super().__init__(parent)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        self._cfg_path: Path
        self.cfg_path = cfg_path

        self.file_dict = {}

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

        self.producers_rules = QSplitter(self)
        self.producers_rules.addWidget(self.producer_list)
        self.producers_rules.addWidget(self.rule_list)
        self.producers_rules.setOrientation(Qt.Orientation.Vertical)

        self.output_list = FlowList(self)
        self.output_list.set_text("Outputs")
        self.output_list.register_item(OutputView)

        self.lists = QSplitter(self)
        self.lists.addWidget(self.input_list)
        self.lists.addWidget(self.producers_rules)
        self.lists.addWidget(self.output_list)

        self.save_action = QAction("Save", self)
        self.save_as_action = QAction("Save as...", self)
        self.open_action = QAction("Open...", self)
        self.reload_action = QAction("Reload", self)
        self.clear_action = QAction("clear", self)
        self.save_action.triggered.connect(self.save_config)
        self.save_as_action.triggered.connect(self.save_config_as)
        self.open_action.triggered.connect(self.open_config)
        self.reload_action.triggered.connect(self.load_config)
        self.clear_action.triggered.connect(self.clear)

        self.save_action.setShortcut(QKeySequence("Ctrl+S"))
        self.save_as_action.setShortcut(QKeySequence("Ctrl+Shift+S"))
        self.open_action.setShortcut(QKeySequence("Ctrl+O"))
        self.reload_action.setShortcut(QKeySequence("Ctrl+R"))

        self.recent_menu = QMenu("Open Recent", self)
        self.recent_files = []

        # (get_builder := QAction("get builder", self)).triggered.connect(self.create_builder)
        # (run_builder := QAction("run builder", self)).triggered.connect(self.run_builder)

        if self.cfg_path.exists():
            self.load_config()

        self._layout = QGridLayout(self)
        self.setLayout(self._layout)

        self._layout.addWidget(self.lists, 0, 0)

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
        cfg: MainConfig = self.get_config()
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

    @property
    def cfg_path(self) -> Path:
        return self._cfg_path

    @cfg_path.setter
    def cfg_path(self, s):
        self._cfg_path = s
        self.status.emit(str(self.cfg_path))
        self.setWindowTitle(f"{self.cfg_path} | dataset-creator")

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
        if (txt := str(self._cfg_path.resolve())) not in recent:
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


class MainWindow(QMainWindow):
    def __init__(self, cfg_path=Path("config.json")):
        super().__init__()
        self.resize(1200, 500)
        self.setMinimumSize(400, 300)

        self.widget = MainWidget(self, cfg_path)
        self.widget.status.connect(self.status_changed)
        self.status_changed(str(self.widget.cfg_path))

        self.setCentralWidget(self.widget)
        file_menu: QMenu = self.menuBar().addMenu("File")
        file_menu.addAction(self.widget.save_action)
        file_menu.addAction(self.widget.save_as_action)
        file_menu.addAction(self.widget.open_action)
        file_menu.addMenu(self.widget.recent_menu)
        file_menu.addAction(self.widget.reload_action)

        edit_menu = self.menuBar().addMenu("Edit")
        edit_menu.addAction(self.widget.clear_action)

    @Slot(str)
    def status_changed(self, s: str):
        self.setWindowTitle(f"{s} | dataset-creator")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cfg_path")
    args = parser.parse_args()

    # check all recent files exist

    save_recent_files([file for file in get_recent_files() if os.path.exists(file)])

    app = QtWidgets.QApplication([])
    central_window = MainWindow(Path(args.cfg_path)) if args.cfg_path else MainWindow()
    central_window.show()
    code = app.exec()
    sys.exit(code)
