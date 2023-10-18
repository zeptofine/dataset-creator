#!/usr/bin/python

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from pprint import pprint

from polars import DataFrame, concat
from PySide6.QtCore import QRect, Qt, QThread, Signal, Slot
from PySide6.QtGui import QAction, QIcon, QKeySequence
from PySide6.QtWidgets import (
    QFileDialog,
    QGridLayout,
    QMainWindow,
    QMenu,
    QMenuBar,
    QProgressBar,
    QPushButton,
    QSplitter,
    QWidget,
)
from rich import print as rprint

from imdataset_creator.datarules.dataset_builder import chunk_split

from .. import DatasetBuilder, File, Input
from ..configs import MainConfig
from .err_dialog import catch_errors
from .input_view import InputList, InputView
from .output_filters import FilterView
from .output_view import OutputList, OutputView
from .producer_views import FileInfoProducerView, HashProducerView, ImShapeProducerView, ProducerList, ProducerView
from .rule_views import (
    BlacklistWhitelistView,
    ChannelRuleView,
    HashRuleView,
    ResRuleView,
    RuleList,
    RuleView,
    StatRuleView,
    TotalLimitRuleView,
)

log = logging.getLogger()

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

        self.locks = 0
        self.__builder: DatasetBuilder | None = None
        # self.set_builder_button = QPushButton("Create builder", self)
        # self.set_builder_button.clicked.connect(self.set_builder)

        self.input_list = InputList(self)
        self.input_list.gathered.connect(self.collect_files)
        self.file_dict: dict[str, list[File]] = {}
        # self.run_all_inputs_button = QPushButton("Gather all inputs", self)
        # self.run_all_inputs_button.clicked.connect(self.input_list.gather_all)

        self.producer_list = ProducerList(self)

        self.rule_list = RuleList(self)

        self.producers_rules = QSplitter(self)
        self.producers_rules.addWidget(self.producer_list)
        self.producers_rules.addWidget(self.rule_list)
        self.producers_rules.setOrientation(Qt.Orientation.Vertical)
        # self.run_population_button = QPushButton("Run population", self)
        # self.population_pbar = QProgressBar(self)
        # self.population_pbar.setFormat("%p%  %v/%m")
        # self.populator_thread = PopulatorThread(self)
        # self.populator_thread.started.connect(self.add_lock)
        # self.populator_thread.finished.connect(self.remove_lock)

        # self.populator_thread.saved.connect(lambda dt: log.info(f"Saved at {dt}"))
        # self.populator_thread.completed_count.connect(self.on_population_pbar)
        # self.populator_thread.population_interval = 30
        # self.populator_thread.population_chunksize = 100
        # self.run_population_button.clicked.connect(self.run_population)

        self.output_list = OutputList(self)
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

        # (get_builder := QAction("get builder", self)).triggered.connect(self.create_builder)
        # (run_builder := QAction("run builder", self)).triggered.connect(self.run_builder)

        if self.cfg_path.exists():
            self.load_config()

        self._layout = QGridLayout(self)
        self.setLayout(self._layout)

        self._layout.addWidget(self.lists, 0, 0, 1, 3)
        # self._layout.addWidget(self.run_all_inputs_button, 1, 0, 1, 1)
        # self._layout.addWidget(self.set_builder_button, 1, 1, 1, 1)
        # self._layout.addWidget(self.run_population_button, 1, 2, 1, 1)
        # self._layout.addWidget(self.population_pbar, 2, 0, 1, 3)

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
        file = (
            s
            if s
            else QFileDialog.getOpenFileName(
                self,
                "Select cfg path",
                str(self.cfg_path.parent),
            )[0]
        )
        if file:
            log.info(f"Opening {file}")
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
            self.recent_menu.addAction(file).triggered.connect(lambda file=file: self.open_config(file))

    @catch_loading
    @Slot(dict)
    def from_cfg(self, cfg: MainConfig):
        self.clear()
        self.input_list.add_from_cfg(cfg["inputs"])
        self.producer_list.add_from_cfg(cfg["producers"])
        self.rule_list.add_from_cfg(cfg["rules"])
        self.output_list.add_from_cfg(cfg["output"])

    @Slot()
    def add_lock(self):
        """Locks the UI"""
        self.locks += 1
        if self.locks == 1:
            log.info("locked")
        ...

    @Slot()
    def remove_lock(self):
        """Unlocks the UI"""
        self.locks -= 1
        if self.locks == 0:
            log.info("unlocked")
        ...

    @Slot(dict)
    def collect_files(self, dct):
        for pth, files in dct.items():
            self.file_dict[pth] = files

    @catch_building
    @Slot()
    def create_builder(self) -> DatasetBuilder:
        producers = self.producer_list.get()
        rules = self.rule_list.get()

        builder = DatasetBuilder(Path("filedb.arrow"))

        builder.add_producers(*producers)
        builder.add_rules(*rules)
        log.info(f"built builder: {builder}")
        return builder

    @property
    def builder(self) -> DatasetBuilder:
        if self.__builder is None:
            self.__builder = self.create_builder()
            # self.populator_thread.db = self.__builder
        return self.__builder

    def set_builder(self):
        self.__builder = self.create_builder()

    # @Slot()
    # def run_population(self):
    #     if self.builder is None:
    #         self.create_builder()
    #         assert self.builder is not None

    #     self.builder.add_new_paths({file.absolute_pth for lst in self.file_dict.values() for file in lst})
    #     self.builder.comply_to_schema(self.builder.type_schema, in_place=True)
    #     unfinished: DataFrame = self.builder.get_unfinished()
    #     if unfinished.is_empty():
    #         log.info("No files are unfinished")
    #         return

    #     self.populator_thread.unfinished = unfinished
    #     self.population_pbar.setMaximum(len(unfinished))
    #     self.populator_thread.start()

    #     log.info("gathered resources")

    # @Slot()
    # def on_population_pbar(self, n: int):
    #     self.population_pbar.setValue(n)


class PopulatorThread(QThread):
    unfinished: DataFrame
    db: DatasetBuilder
    population_interval: int
    population_chunksize: int

    completed_count = Signal(int)
    saved = Signal(datetime)

    def run(self):
        log.info("started")
        if finished := self.db.remove_finished_producers():
            log.warning(f"Skipping finished producers: {finished}")
        collected: list[DataFrame] = []
        save_timer: datetime = datetime.now()
        chunk: DataFrame
        cnt = 0
        for schemas, df in self.db.split_files_via_nulls(self.unfinished):
            for chunk in self.db.populate_chunks(chunk_split(df, chunksize=self.population_chunksize), schemas):
                collected.append(chunk)
                cnt += len(chunk)
                self.completed_count.emit(cnt)
                old_c: list[DataFrame] = collected
                save_timer, collected = self.db.trigger_save_via_time(save_timer, collected, self.population_interval)
                if collected is not old_c:
                    self.saved.emit(save_timer)

        concatenated: DataFrame = concat(collected, how="diagonal")
        # This breaks with datatypes like Array(3, pl.UInt32). Not sure why.
        # `pyo3_runtime.PanicException: implementation error, cannot get ref Array(Null, 0) from Array(UInt32, 3)`
        self.db.update(concatenated)
        self.db.save_df()


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
