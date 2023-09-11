from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from string import Formatter
from typing import Any

from PySide6.QtCore import QRect, Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QGridLayout,
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
)

from .err_dialog import catch_errors
from .frames import FlowItem, FlowList
from .input_view import InputView
from .output_filters import Filter, ResizeFilter


class FilterList(FlowList):
    items: list[Filter]


class InvalidFormatException(Exception):
    def __init__(self, disallowed: str):
        super().__init__(f"invalid format string. '{disallowed}' is not allowed.")


class SafeFormatter(Formatter):
    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        # the goal is to make sure `property`s and indexing is still available, while dunders and things are not
        if "__" in field_name:
            raise InvalidFormatException("__")

        return super().get_field(field_name, args, kwargs)


output_formatter = SafeFormatter()


class OutputView(InputView):
    cfg_name = "output"

    def configure_settings_group(self):
        self.list = FilterList(self)
        self.list.register_items(ResizeFilter)
        self.list.setMinimumHeight(400)
        self.list.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)

        self.list.register_items()
        self.groupgrid.addWidget(QLabel("Filters: ", self), 1, 0, 1, 3)
        self.groupgrid.addWidget(self.list, 2, 0, 1, 3)

    def reset_settings_group(self):
        self.list.items.clear()

    def get(self) -> str:
        return self.text.text()

    def get_config(self):
        return {
            "file": self.text.text(),
            "list": self.list.get_config(),
        }

    @classmethod
    def from_config(cls, cfg: dict, parent=None):
        self = cls(parent)
        self.text.setText(cfg["file"])
        self.list.add_from_cfg(cfg["list"])
        return self

    @Slot()
    def select_folder(self):
        filename = QFileDialog.getExistingDirectory(
            self,
            "Select output folder",
            str(Path.home()),
        )
        self.text.setText(filename)
