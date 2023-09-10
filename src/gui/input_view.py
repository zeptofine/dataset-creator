from __future__ import annotations

import time
from pathlib import Path

import cv2
import wcmatch.glob as wglob
from PySide6.QtCore import QRect, QThread, Signal, Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QSpinBox,
    QTextEdit,
    QToolButton,
    QWidget,
)

from src.gui.err_dialog import catch_errors

from .frames import FlowItem, FlowList
from .output_filters import Filter


class FilterList(FlowList):
    items: list[Filter]


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
    source: Path
    expressions: list[str]
    flags: int

    count = Signal(int)
    total = Signal(int)
    files = Signal(list)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        print(f"Starting search in: '{self.source}' with expressions: {self.expressions}")
        filelist = []

        count = 0
        self.count.emit(0)
        emit_timer = time.time()
        for file in wglob.iglob(self.expressions, flags=self.flags, root_dir=self.source):
            count += 1
            if (newtime := time.time()) > emit_timer + 0.2:
                self.count.emit(count)
                emit_timer = newtime
            filelist.append(file)

        print(f"Gathered {count} files from '{self.source}'")
        self.count.emit(count)
        self.files.emit(filelist)


class InputView(FlowItem):
    cfg_name = "input"
    needs_settings = True
    movable = False

    text: QLineEdit

    flags = wglob.BRACE | wglob.SPLIT | wglob.EXTMATCH | wglob.IGNORECASE | wglob.GLOBSTAR

    gathered = Signal(dict)

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
        self.groupgrid.setGeometry(QRect(0, 0, 800, 800))
        self.groupgrid.addWidget(QLabel("Folder: ", self), 0, 0)
        self.groupgrid.addWidget(self.text, 0, 1)
        self.groupgrid.addWidget(self.fileselect, 0, 2)

    def configure_settings_group(self):
        self.gather_button = QToolButton(self)
        self.globexprs = QTextEdit(self)
        self.gatherer = GathererThread(self)

        self.gatherer.count.connect(self.filecount.setNum)
        self.gatherer.files.connect(self.on_gathered)
        self.gatherer.started.connect(self.on_started)
        self.gatherer.finished.connect(self.on_finished)

        self.gatherer.flags = self.flags

        self.gather_button.setText("gather")
        self.gather_button.clicked.connect(self.get)

        self.groupgrid.addWidget(QLabel("Search patterns:", self), 1, 0, 1, 3)
        self.groupgrid.addWidget(self.globexprs, 2, 0, 1, 3)
        self.groupgrid.addWidget(self.gather_button, 3, 0, 1, 1)

    def _top_bar(self) -> list[QWidget]:
        top: list[QWidget] = super()._top_bar()
        self.toptext = QLabel(self)
        self.toptext.setDisabled(True)
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

        self.gatherer.source = Path(self.text.text())
        self.gatherer.expressions = self.globexprs.toPlainText().splitlines()
        self.gatherer.start()

    @Slot(dict)
    def on_gathered(self, lst):
        self.gathered.emit({self.text.text(): lst})

    def get_json(self):
        return {
            "file": self.text.text(),
            "expressions": self.globexprs.toPlainText().splitlines(),
        }

    @classmethod
    def from_json(cls, cfg: dict, parent=None):
        self = cls(parent)
        self.text.setText(cfg["file"])
        self.globexprs.setText("\n".join(cfg["expressions"]))

        return self

    @Slot()
    def select_folder(self):
        filename = QFileDialog.getExistingDirectory(
            self,
            "Select output folder",
            self.text.text() or str(Path.home()),
        )
        self.text.setText(filename)
