from __future__ import annotations

import logging
import time
from pathlib import Path

from PySide6.QtCore import QRect, QThread, Signal, Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QLabel,
    QLineEdit,
    QListView,
    QTextEdit,
    QToolButton,
    QTreeView,
    QWidget,
)

from .. import File, Input
from ..configs.configtypes import InputData
from .err_dialog import catch_errors
from .frames import FlowItem, FlowList
from .output_filters import FilterView

log = logging.getLogger()


class FilterList(FlowList):
    items: list[FilterView]


DEFAULT_IMAGE_FORMATS = (
    ".webp",
    ".bmp",
    ".jpeg",
    ".jpg",
    ".png",
    ".tiff",
    ".tif",
)


class GathererThread(QThread):
    inputobj: Input

    total = Signal(int)
    files = Signal(list)

    def run(self):
        log.info(f"Starting search in: '{self.inputobj.folder}' with expressions: {self.inputobj.expressions}")
        filelist = []

        count = 0
        self.total.emit(0)
        emit_timer = time.time()
        for file in self.inputobj.run():
            count += 1
            if (new_time := time.time()) > emit_timer + 0.2:
                self.total.emit(count)
                emit_timer = new_time
            filelist.append(file)

        log.info(f"Gathered {count} files from '{self.inputobj.folder}'")
        self.total.emit(count)
        self.files.emit(filelist)


class InputView(FlowItem):
    needs_settings = True
    movable = False

    text: QLineEdit

    gathered = Signal(dict)

    bound_item = Input

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_widget(self):
        super().setup_widget()

        self.text = QLineEdit(self)
        self.text.textChanged.connect(self.top_text.setText)

        self.file_select = QToolButton(self)
        self.file_select.setText("...")
        self.file_select.setIcon(QIcon.fromTheme("folder-open"))
        self.file_select.clicked.connect(self.select_folder)
        self.group_grid.setGeometry(QRect(0, 0, 800, 800))
        self.group_grid.addWidget(QLabel("Folder: ", self), 0, 0)
        self.group_grid.addWidget(self.text, 0, 1)
        self.group_grid.addWidget(self.file_select, 0, 2)

        self.filedialog = QFileDialog(self)
        self.filedialog.setFileMode(QFileDialog.FileMode.Directory)
        self.filedialog.setOption(QFileDialog.Option.ShowDirsOnly, True)
        self.filedialog.setOption(QFileDialog.Option.DontResolveSymlinks, True)

        file_view: QListView = self.filedialog.findChild(QListView, "listView")  # type: ignore
        if file_view:
            file_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        f_tree_view: QTreeView = self.filedialog.findChild(QTreeView)  # type: ignore
        if f_tree_view:
            f_tree_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

    def configure_settings_group(self):
        self.gather_button = QToolButton(self)
        self.glob_exprs = QTextEdit(self)
        self.gatherer = GathererThread(self)

        self.gatherer.total.connect(self.file_count.setNum)
        self.gatherer.total.connect(self.on_total)
        self.gatherer.files.connect(self.on_gathered)
        self.gatherer.started.connect(self.on_started)
        self.gatherer.finished.connect(self.on_finished)

        self.gather_button.setText("gather")
        self.gather_button.clicked.connect(self.get)

        self.group_grid.addWidget(QLabel("Search patterns:", self), 1, 0, 1, 3)
        self.group_grid.addWidget(self.glob_exprs, 2, 0, 1, 3)
        self.group_grid.addWidget(self.gather_button, 3, 0, 1, 1)

    def _top_bar(self) -> list[QWidget]:
        top: list[QWidget] = super()._top_bar()
        self.top_text = QLabel(self)
        self.file_count = QLabel(self)

        top[:1] += [self.top_text, self.file_count]
        return top

    @Slot(int)
    def on_total(self, val):
        self.total = val

    @Slot()
    def on_started(self):
        self.n = 0
        self.setEnabled(False)

    @Slot()
    def on_finished(self):
        self.n = self.total
        self.setEnabled(True)

    @Slot()
    def reset_settings_group(self):
        self.text.clear()
        self.glob_exprs.setText("\n".join(f"**/*{ext}" for ext in DEFAULT_IMAGE_FORMATS))

    @catch_errors("gathering failed")
    @Slot()
    def get(self):
        self.total = 0
        self.n = 0
        if not self.text.text():
            raise NotADirectoryError(self.text.text())

        self.gatherer.inputobj = Input(Path(self.text.text()), self.glob_exprs.toPlainText().splitlines())
        self.gatherer.start()

    @Slot(dict)
    def on_gathered(self, lst):
        self.gathered.emit({self.text.text(): [File.from_src(Path(self.text.text()), file) for file in lst]})

    def get_config(self) -> InputData:
        return {
            "folder": self.text.text(),
            "expressions": self.glob_exprs.toPlainText().splitlines(),
        }

    @classmethod
    def from_config(cls, cfg: InputData, parent=None):
        self = cls(parent)
        self.text.setText(cfg["folder"])
        self.glob_exprs.setText("\n".join(cfg["expressions"]))

        return self

    @Slot()
    def select_folder(self):
        # ! this as a whole is very fucky

        files = self._select_folder()
        if files:
            while len(files) > 1:
                self.text.setText(files.pop(0))
                self.duplicate.emit()
            self.text.setText(files.pop(0))

    def _select_folder(self):
        self.filedialog.setDirectory(self.text.text() or str(Path.home()))
        if self.filedialog.exec():
            return self.filedialog.selectedFiles()
        return []


class InputList(FlowList):
    items: list[InputView]

    gathered = Signal(dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_text("Inputs")
        self.register_item(InputView)

    def add_item(self, item: InputView, *args, **kwargs):
        item.gathered.connect(self.gathered.emit)
        return super().add_item(item, *args, **kwargs)

    @Slot()
    def gather_all(self):
        for item in self.items:
            item.get()
