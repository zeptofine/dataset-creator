from __future__ import annotations

from collections.abc import Mapping, Sequence
from string import Formatter
from typing import Any

from PySide6.QtWidgets import QCheckBox, QFileDialog, QLabel, QLineEdit

from ..configs import OutputData
from ..datarules.base_rules import Output
from .frames import FlowList
from .input_view import InputView
from .output_filters import FilterList


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
    bound_item = Output

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setMouseTracking(True)
        self.setMinimumHeight(400)
        self.filedialog.setFileMode(QFileDialog.FileMode.AnyFile)
        self.previous_position = None

    def configure_settings_group(self):
        self.format_str = QLineEdit(self)

        self.overwrite = QCheckBox(self)
        self.overwrite.setText("overwrite existing files")

        self.list = FilterList(self)

        self.list.register_item()
        self.group_grid.addWidget(self.overwrite, 1, 0, 1, 3)
        self.group_grid.addWidget(QLabel("format text: ", self), 2, 0, 1, 3)
        self.group_grid.addWidget(self.format_str, 3, 0, 1, 3)
        self.group_grid.addWidget(self.list, 4, 0, 1, 3)

    def reset_settings_group(self):
        self.format_str.setText("{relative_path}/{file}.{ext}")
        self.overwrite.setChecked(False)
        self.list.items.clear()

    def get(self) -> Output:
        return Output.from_cfg(self.get_config())

    def get_config(self) -> OutputData:
        return {
            "folder": self.text.text(),
            "output_format": self.format_str.text() or self.format_str.placeholderText(),
            "lst": self.list.get_config(),
            "overwrite": self.overwrite.isChecked(),
        }

    @classmethod
    def from_config(cls, cfg: OutputData, parent=None):
        self = cls(parent)
        self.text.setText(cfg["folder"])
        self.format_str.setText(cfg["output_format"])
        self.list.add_from_cfg(cfg["lst"])
        self.overwrite.setChecked(cfg["overwrite"])
        return self


class OutputList(FlowList):
    items: list[OutputView]
