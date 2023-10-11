from __future__ import annotations

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

from ..configs.configtypes import InputData
from ..datarules.base_rules import Input
from .err_dialog import catch_errors
from .frames import FlowItem, FlowList
from .output_filters import FilterView


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
    input: Input

    count = Signal(int)
    total = Signal(int)
    files = Signal(list)

    def run(self):
        print(f"Starting search in: '{self.input.folder}' with expressions: {self.input.expressions}")
        filelist = []

        count = 0
        self.count.emit(0)
        emit_timer = time.time()
        for file in self.input.run():
            count += 1
            if (newtime := time.time()) > emit_timer + 0.2:
                self.count.emit(count)
                emit_timer = newtime
            filelist.append(file)

        print(f"Gathered {count} files from '{self.input.folder}'")
        self.count.emit(count)
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
        self.text.textChanged.connect(self.toptext.setText)

        self.fileselect = QToolButton(self)
        self.fileselect.setText("...")
        self.fileselect.setIcon(QIcon.fromTheme("folder-open"))
        self.fileselect.clicked.connect(self.select_folder)
        self.group_grid.setGeometry(QRect(0, 0, 800, 800))
        self.group_grid.addWidget(QLabel("Folder: ", self), 0, 0)
        self.group_grid.addWidget(self.text, 0, 1)
        self.group_grid.addWidget(self.fileselect, 0, 2)

        self.filedialog = QFileDialog(self)
        self.filedialog.setFileMode(QFileDialog.FileMode.Directory)
        self.filedialog.setOption(QFileDialog.Option.ShowDirsOnly, True)
        self.filedialog.setOption(QFileDialog.Option.DontResolveSymlinks, True)

        fileview: QListView = self.filedialog.findChild(QListView, "listView")  # type: ignore
        if fileview:
            fileview.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        f_tree_view: QTreeView = self.filedialog.findChild(QTreeView)  # type: ignore
        if f_tree_view:
            f_tree_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

    def configure_settings_group(self):
        self.gather_button = QToolButton(self)
        self.globexprs = QTextEdit(self)
        self.gatherer = GathererThread(self)

        self.gatherer.count.connect(self.filecount.setNum)
        self.gatherer.files.connect(self.on_gathered)
        self.gatherer.started.connect(self.on_started)
        self.gatherer.finished.connect(self.on_finished)
        self.gatherer.finished.connect(self.increment.emit)

        self.gather_button.setText("gather")
        self.gather_button.clicked.connect(self.get)

        self.group_grid.addWidget(QLabel("Search patterns:", self), 1, 0, 1, 3)
        self.group_grid.addWidget(self.globexprs, 2, 0, 1, 3)
        self.group_grid.addWidget(self.gather_button, 3, 0, 1, 1)

    def _top_bar(self) -> list[QWidget]:
        top: list[QWidget] = super()._top_bar()
        self.toptext = QLabel(self)
        self.filecount = QLabel(self)

        top[:1] += [self.toptext, self.filecount]
        return top

    @Slot()
    def on_started(self):
        self.setEnabled(False)

    @Slot()
    def on_finished(self):
        self.setEnabled(True)

    @Slot()
    def reset_settings_group(self):
        self.text.clear()
        self.globexprs.setText("\n".join(f"**/*{ext}" for ext in DEFAULT_IMAGE_FORMATS))

    @catch_errors("gathering failed")
    @Slot()
    def get(self):
        if not self.text.text():
            raise NotADirectoryError(self.text.text())

        self.gatherer.input = Input(Path(self.text.text()), self.globexprs.toPlainText().splitlines())
        self.gatherer.start()

    @Slot(dict)
    def on_gathered(self, lst):
        self.gathered.emit({self.text.text(): lst})

    def get_config(self) -> InputData:
        return {
            "folder": self.text.text(),
            "expressions": self.globexprs.toPlainText().splitlines(),
        }

    @classmethod
    def from_config(cls, cfg: InputData, parent=None):
        self = cls(parent)
        self.text.setText(cfg["folder"])
        self.globexprs.setText("\n".join(cfg["expressions"]))

        return self

    @Slot()
    def select_folder(self):
        # ! this as a whole is very fucky

        self.filedialog.setDirectory(self.text.text() or str(Path.home()))
        if self.filedialog.exec():
            print(self.filedialog.selectedFiles())
            files = self.filedialog.selectedFiles()

            while len(files) > 1:
                self.text.setText(files.pop(0))
                self.duplicate.emit()

            self.text.setText(files.pop(0))
