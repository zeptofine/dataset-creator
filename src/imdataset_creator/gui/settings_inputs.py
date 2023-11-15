# from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from PySide6.QtCore import QDate, QDateTime, QObject, QRect, QSize, Qt, QTime, Signal, Slot
from PySide6.QtGui import QAction, QFont, QIcon, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDoubleSpinBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)
from rich import print as rprint

from .minichecklist import MiniCheckList

TooltipFont = QFont()
TooltipFont.setUnderline(True)


def apply_tooltip(widget: QWidget, txt: str):
    widget.setToolTip(txt)
    widget.setFont(TooltipFont)


class SettingsItem(ABC):
    widget: QWidget

    @abstractmethod
    def create(self) -> list[QWidget]:
        raise NotImplementedError

    @abstractmethod
    def from_cfg(self, val) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_cfg(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def reset(self):
        raise NotImplementedError


class SettingsRow(QHBoxLayout):
    def __init__(
        self,
        item: SettingsItem,
        label: str | None,
        tooltip: str | None,
        from_config_mod: Callable | None = None,
        to_config_mod: Callable | None = None,
        parent: QWidget | None = None,
    ):
        if parent is not None:  # why is this inconsistent to every other widget
            super().__init__(parent)
        else:
            super().__init__()
        self.setContentsMargins(0, 0, 0, 0)
        self.setSpacing(2)

        self.label = label
        self.tooltip = tooltip
        self.from_config_mod = from_config_mod
        self.to_config_mod = to_config_mod
        self.item: SettingsItem = item
        self.widgets = self.create_widgets()
        for widget in self.widgets:
            self.addWidget(widget)

    @abstractmethod
    def create_widgets(self) -> list[QWidget]:
        lst: list[QWidget] = []
        if self.label:
            label = QLabel(self.label)
            if self.tooltip:
                apply_tooltip(label, self.tooltip)
            lst.append(label)
        lst.extend(self.item.create())
        return lst

    def from_cfg(self, val):
        if self.from_config_mod is not None:
            val = self.from_config_mod(val)
        self.item.from_cfg(val)

    @abstractmethod
    def get_cfg(self):
        val = self.item.get_cfg()
        if self.to_config_mod is not None:
            val = self.to_config_mod(val)
        return val

    @abstractmethod
    @Slot()
    def reset(self):
        self.item.reset()


class BaseInput(ABC):
    _label: str | None
    _tooltip: str | None
    optional: bool = False
    _from_config_mod: Callable | None = None
    _to_config_mod: Callable | None = None

    def __init__(self):
        self._label = None
        self._tooltip = None

    def create_layout(self) -> SettingsRow:
        return SettingsRow(
            self.get_settings(),
            self._label,
            self._tooltip,
            self._from_config_mod,
            self._to_config_mod,
        )

    @abstractmethod
    def get_settings(self) -> SettingsItem:
        raise NotImplementedError

    def label(self, text: str):
        self._label = text
        return self

    def tooltip(self, text: str):
        self._tooltip = text
        return self

    def set_optional(self):
        self.optional = True
        return self

    def from_config_modification(self, func: Callable):
        self._from_config_mod = func
        return self

    def to_config_modification(self, func: Callable):
        self._to_config_mod = func
        return self


class NumberInputSettings(SettingsItem):
    def __init__(
        self, bounds: tuple[int, int], default: float = 0, step: float = 1, slider: Qt.Orientation | None = None
    ):
        super().__init__()
        self.bounds: tuple[int, int] = bounds
        self.default: float = default
        self.step: float = step
        self.slider: Qt.Orientation | None = slider

    def create(self):
        self.widget: QSpinBox = QSpinBox()
        self.widget.setRange(*self.bounds)
        self.reset()
        return [self.widget]

    def from_cfg(self, val):
        self.widget.setValue(val)

    def get_cfg(self):
        return self.widget.value()

    @Slot()
    def reset(self):
        self.widget.setValue(int(self.default))


class NumberInput(BaseInput):
    def __init__(
        self,
        bounds: tuple[int, int],
        default: float = 0,
        step: float = 1,
        slider: Qt.Orientation | None = None,
    ) -> None:
        super().__init__()
        self.bounds: tuple[int, int] = bounds
        self.default: float = default
        self.step: float = step
        self.slider: Qt.Orientation | None = slider

    def get_settings(self):
        return NumberInputSettings(self.bounds, self.default, self.step, self.slider)


class DoubleInputSettings(NumberInputSettings):
    def create(self):
        self.widget: QDoubleSpinBox = QDoubleSpinBox()
        self.widget.setRange(*self.bounds)
        self.reset()
        return [self.widget]

    def from_cfg(self, val):
        self.widget.setValue(val)

    def get_cfg(self):
        return self.widget.value()

    @Slot()
    def reset(self):
        self.widget.setValue(self.default)


class DoubleInput(NumberInput):
    def get_settings(self) -> SettingsItem:
        return DoubleInputSettings(self.bounds, self.default, self.step, self.slider)


def small_widget(parent=None):
    widget = QWidget(parent)
    layout = QHBoxLayout(widget)
    layout.setSpacing(2)
    layout.setContentsMargins(0, 0, 0, 0)
    return widget


class RangeInputSettings(SettingsItem):
    def __init__(
        self,
        bounds: tuple[int, int],
        default: float | tuple[float, float] = 0,
        step: float = 1,
        min_and_max_correlate=True,
    ):
        super().__init__()
        self.bounds: tuple[int, int] = bounds
        self.default: float | tuple[float, float] = default
        self.step: float = step
        self.min_and_max_correlate = min_and_max_correlate

    def create(self):
        self.min_widget: QSpinBox = QSpinBox()
        self.max_widget: QSpinBox = QSpinBox()
        if self.min_and_max_correlate:
            self.min_widget.valueChanged.connect(self.max_widget.setMinimum)
            self.max_widget.valueChanged.connect(self.min_widget.setMaximum)
        self.min_widget.setMinimum(self.bounds[0])
        self.max_widget.setMaximum(self.bounds[1])

        return [self.min_widget, self.max_widget]

    def from_cfg(self, val: tuple[int, int]):
        self.max_widget.setValue(val[1])
        self.min_widget.setValue(val[0])

    def get_cfg(self) -> tuple[int, int]:
        return (self.min_widget.value(), self.max_widget.value())

    @Slot()
    def reset(self):
        mi: float
        ma: float
        if isinstance(self.default, tuple):
            mi, ma = self.default
        else:
            mi = ma = self.default
        self.max_widget.setValue(int(ma))
        self.min_widget.setValue(int(mi))


class RangeInput(BaseInput):
    def __init__(
        self,
        bounds: tuple[int, int] = (0, 9_999_999),
        default: float | tuple[float, float] = 0,
        step: float = 1,
        min_and_max_correlate=True,
    ):
        super().__init__()
        self.bounds: tuple[int, int] = bounds
        self.default: float | tuple[float, float] = default
        self.step: float = step
        self.min_and_max_correlate: bool = min_and_max_correlate

    def get_settings(self) -> SettingsItem:
        return RangeInputSettings(self.bounds, self.default, self.step, self.min_and_max_correlate)


class BoolInputSettings(SettingsItem):
    widget: QCheckBox

    def __init__(self, default: bool):
        self.default: bool = default

    def create(self):
        self.widget = QCheckBox()
        self.widget.setChecked(self.default)
        return [self.widget]

    def from_cfg(self, val):
        self.widget.setChecked(val)

    def get_cfg(self):
        return self.widget.isChecked()

    def reset(self):
        self.widget.setChecked(self.default)


class BoolInput(BaseInput):
    widget: QCheckBox

    def __init__(self, default=False):
        super().__init__()
        self.default: bool = default

    def get_settings(self) -> SettingsItem:
        return BoolInputSettings(self.default)


class TextInputSettings(SettingsItem):
    def __init__(self, default="", placeholder=""):
        super().__init__()
        self.default = default
        self.placeholder = placeholder

    def create(self):
        self.widget: QLineEdit = QLineEdit()
        if self.default:
            self.widget.setText(str(self.default))
        if self.placeholder:
            self.widget.setPlaceholderText(self.placeholder)
        return [self.widget]

    def from_cfg(self, val) -> None:
        self.widget.setText(val)

    def get_cfg(self):
        return self.widget.text()

    @Slot()
    def reset(self):
        self.widget.setText(self.default)


class TextInput(BaseInput):
    def __init__(self, default="", placeholder=""):
        super().__init__()
        self.default = default
        self.placeholder = placeholder

    def get_settings(self) -> SettingsItem:
        return TextInputSettings(self.default, self.placeholder)


class MultilineInputSettings(SettingsItem):
    def __init__(self, default="", is_list=False):
        super().__init__()
        self.default = default
        self.is_list = is_list

    def create(self):
        self.widget: QTextEdit = QTextEdit()
        if self.default:
            self.widget.setText(str(self.default))
        return [self.widget]

    def from_cfg(self, val) -> None:
        if isinstance(val, list):
            val = "\n".join(val)
        self.widget.setText(val)

    def get_cfg(self):
        val = self.widget.toPlainText()
        if self.is_list:
            return val.splitlines()
        return val

    @Slot()
    def reset(self):
        self.widget.setText(self.default)


class MultilineInput(BaseInput):
    widget: QTextEdit

    def __init__(self, default="", is_list=False):
        super().__init__()
        self.default = default
        self.is_list = is_list

    def get_settings(self):
        return MultilineInputSettings(self.default, self.is_list)


class DropdownInputSettings(SettingsItem):
    def __init__(self, choices: list[str], default_idx=0):
        super().__init__()
        self.choices = choices
        self.default_idx = default_idx

    def create(self):
        self.widget: QComboBox = QComboBox()
        self.widget.addItems(self.choices)
        self.widget.setCurrentIndex(self.default_idx)
        return [self.widget]

    def from_cfg(self, val):
        self.widget.setCurrentIndex(self.choices.index(val))

    def get_cfg(self):
        return self.choices[self.widget.currentIndex()]

    def reset(self):
        self.widget.setCurrentIndex(self.default_idx)


class DropdownInput(BaseInput):
    choices: list[str]

    def __init__(self, choices: list[str], default_idx=0):
        super().__init__()
        self.choices = choices
        self.default_idx = default_idx

    def get_settings(self) -> SettingsItem:
        return DropdownInputSettings(self.choices, self.default_idx)


class EnumChecklistInputSettings(SettingsItem):
    def __init__(self, enum: type[Enum]):
        super().__init__()
        self.enum: type[Enum] = enum

    def create(self):
        self.widget: MiniCheckList = MiniCheckList(self.enum.__members__.keys())
        return [self.widget]

    def from_cfg(self, val) -> None:
        newval: dict[str, bool] = {k: True for k in val}
        self.widget.update_items(newval)

    def get_cfg(self):
        return self.widget.get_enabled()

    def reset(self):
        self.widget.disable_all()


class EnumChecklistInput(BaseInput):
    checklist: MiniCheckList

    def __init__(self, enum: type[Enum]):
        super().__init__()
        self.enum: type[Enum] = enum

    def get_settings(self):
        return EnumChecklistInputSettings(self.enum)


StartOfDateTime = QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0))


class DateTimeInputSettings(SettingsItem):
    def __init__(
        self,
        dt_format: str = "dd/MM/yyyy h:mm AP",
        default: QDateTime = StartOfDateTime,
        calendar_popup=False,
    ):
        super().__init__()
        self.format = dt_format
        self.default = default
        self.calendar_popup = calendar_popup

    def create(self):
        self.widget: QDateTimeEdit = QDateTimeEdit()
        self.widget.setCalendarPopup(self.calendar_popup)
        if self.default:
            self.widget.setDateTime(self.default)
        self.widget.setDisplayFormat(self.format)
        return [self.widget]

    def from_cfg(self, val) -> None:
        self.widget.setDateTime(QDateTime.fromString(val, self.format))

    def get_cfg(self) -> Any:
        return self.widget.dateTime().toString(self.format)

    def reset(self):
        self.widget.setDateTime(self.default)


class DateTimeInput(BaseInput):
    widget: QDateTimeEdit

    def __init__(
        self,
        dt_format: str = "dd/MM/yyyy h:mm AP",
        default: QDateTime = StartOfDateTime,
        calendar_popup=True,
    ):
        super().__init__()
        self.format = dt_format
        self.default = default
        self.calendar_popup = calendar_popup

    def get_settings(self):
        return DateTimeInputSettings(self.format, self.default, self.calendar_popup)


class FileInputSettings(SettingsItem):
    def __init__(self, default: str = "", mode: Literal["directory", "file"] = "directory"):
        super().__init__()
        self.default = default
        self.filemode = {
            "directory": QFileDialog.FileMode.Directory,
            "file": QFileDialog.FileMode.ExistingFile,
        }.get(
            mode,
            QFileDialog.FileMode.Directory,
        )

    def create(self):
        self.text_widget: QLineEdit = QLineEdit()
        if self.default:
            self.text_widget.setText(self.default)
        self.folder_select = QToolButton()
        self.folder_select.setText("...")
        self.folder_select.setIcon(QIcon.fromTheme("folder-open"))
        self.folder_select.clicked.connect(self.select_folder)
        self.filedialog = QFileDialog()
        self.filedialog.setFileMode(self.filemode)
        if self.filemode is QFileDialog.FileMode.Directory:
            self.filedialog.setOption(QFileDialog.Option.ShowDirsOnly, True)

        return [self.text_widget, self.folder_select]

    @Slot()
    def select_folder(self):
        # ! this as a whole is very fucky

        files = self._select_folder()
        if files:
            while len(files) > 1:
                self.text_widget.setText(files.pop(0))
                # self.duplicate.emit()
            self.text_widget.setText(files.pop(0))

    def _select_folder(self):
        self.filedialog.setDirectory(self.text_widget.text() or str(Path.home()))
        if self.filedialog.exec():
            return self.filedialog.selectedFiles()
        return []

    def from_cfg(self, val):
        self.text_widget.setText(val)

    def get_cfg(self):
        return self.text_widget.text()

    def reset(self):
        self.text_widget.setText(self.default)


class DirectoryInput(BaseInput):
    def __init__(self, default: str = ""):
        super().__init__()
        self.default = default

    def get_settings(self):
        return FileInputSettings(self.default, mode="directory")


class FileInput(DirectoryInput):
    def get_settings(self):
        return FileInputSettings(self.default, mode="file")


ItemSettings = dict[str | tuple[str, ...], BaseInput]


class SettingsBox(QFrame):
    def __init__(self, settings: ItemSettings, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(2)
        layout.setSizeConstraint(QVBoxLayout.SizeConstraint.SetMinimumSize)
        self.setFrameStyle(QFrame.Shape.Panel | QFrame.Shadow.Sunken)
        self.settings = settings
        self.rows: dict[str | tuple[str, ...], SettingsRow] = {}

        for key, inpt in settings.items():
            box = QWidget(self)
            self.rows[key] = (sl := inpt.create_layout())
            box.setLayout(sl)
            layout.addWidget(box)

    def get_cfg(self):
        dct = {}
        for key, row in self.rows.items():
            val = row.get_cfg()
            if isinstance(key, tuple):
                dct.update(dict(zip(key, val)))
            else:
                dct[key] = val

        return dct

    def from_cfg(self, cfg: dict):
        for key, row in self.rows.items():
            if isinstance(key, tuple):
                vals = [cfg[k] for k in key]
                row.from_cfg(vals)
            else:
                if key in cfg:
                    row.from_cfg(cfg[key])

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        if event.buttons() == Qt.MouseButton.LeftButton and self.previous_position is not None:
            pos_change = event.position() - self.previous_position
            new_height = int(self.size().height() + pos_change.y())
            self.setMinimumHeight(max(new_height, 0))
        self.previous_position = event.position()
        return super().mouseMoveEvent(event)

    def mousePressEvent(self, event: QMouseEvent) -> None:
        self.previous_position = event.position()
        return super().mousePressEvent(event)
