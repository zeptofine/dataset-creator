from __future__ import annotations

from abc import abstractmethod
from collections.abc import Collection
from typing import TypeVar

from PySide6.QtCore import QRect, QSize, Qt, Signal, Slot
from PySide6.QtGui import QAction, QCursor, QIcon, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
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

from ..configs.configtypes import ItemConfig, ItemData
from ..configs.keyworded import Keyworded


class FlowItem(QFrame):  # TODO: Better name lmao
    title: str = ""
    desc: str = ""
    needs_settings: bool = False
    movable: bool = True

    move_down = Signal()
    move_up = Signal()
    position_changed = Signal()
    closed = Signal()
    duplicate = Signal()

    increment = Signal()

    bound_item: type[Keyworded] | Keyworded

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
        self.setLineWidth(2)
        self.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)

        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        (collapse_action := QAction("collapse", self)).triggered.connect(self.toggle_group)
        (duplicate_action := QAction("duplicate", self)).triggered.connect(self.duplicate.emit)
        (revert_action := QAction("revert to defaults", self)).triggered.connect(self.reset_settings_group)
        self.addActions([collapse_action, duplicate_action, revert_action])

        self._minimum_size = self.size()
        self.previous_position = None

        self.setup_widget()
        self.configure_settings_group()
        self.reset_settings_group()
        self.opened = True

    def setup_widget(self):
        self._layout = QGridLayout()
        self.setLayout(self._layout)

        top_bar: list[QWidget] = self._top_bar()
        for idx, widget in enumerate(top_bar):
            self._layout.addWidget(widget, 0, idx)

        self.group = QGroupBox()
        self.description_widget = QLabel(self.desc, self)

        self.description_widget.hide()
        self.description_widget.setWordWrap(True)

        self.group_grid = QGridLayout()
        self.group.hide()
        self.group.setLayout(self.group_grid)
        self._layout.addWidget(self.description_widget, 1, 0, 1, len(top_bar))
        self._layout.addWidget(self.group, 2, 0, 1, len(top_bar))

    def _top_bar(self) -> list[QWidget]:
        widgets: list[QWidget] = []
        self.checkbox = QCheckBox()
        self.checkbox.setChecked(True)
        if self.title:
            self.checkbox.setText(self.title)
        widgets.append(self.checkbox)

        if self.movable:
            self.up_arrow = QToolButton()
            self.down_arrow = QToolButton()
            self.up_arrow.setText("↑")
            self.down_arrow.setText("↓")
            self.up_arrow.clicked.connect(self.position_changed)
            self.up_arrow.clicked.connect(self.move_up)
            self.down_arrow.clicked.connect(self.position_changed)
            self.down_arrow.clicked.connect(self.move_down)
            widgets.append(self.up_arrow)
            widgets.append(self.down_arrow)

        self.close_button = QToolButton()
        self.close_button.setText("X")
        self.close_button.clicked.connect(self.closed)
        widgets.append(self.close_button)

        return widgets

    @abstractmethod
    def get(self):
        """produces something the item represents"""
        self.increment.emit()

    @abstractmethod
    def configure_settings_group(self) -> None:
        ...

    @abstractmethod
    def reset_settings_group(self):
        ...

    @Slot()
    def toggle_group(self):
        self.opened = not self.opened

    def mouseDoubleClickEvent(self, event: QMouseEvent) -> None:
        self.toggle_group()
        event.accept()

    @property
    def enabled(self):
        return self.checkbox.isChecked()

    @enabled.setter
    def enabled(self, b: bool):
        self.checkbox.setChecked(b)

    @property
    def opened(self):
        return self.__opened

    @opened.setter
    def opened(self, b: bool):
        if self.needs_settings:
            self.group.setVisible(b)
        if self.desc:
            self.description_widget.setVisible(b)
        if not b:
            self._minimum_size = self.minimumSize()
            self.setMinimumSize(0, 0)
        else:
            self.setMinimumSize(self._minimum_size)
        self.__opened = b

    @classmethod
    def cfg_name(cls):
        return cls.bound_item.cfg_kwd()

    # Saving and creating methods

    @abstractmethod
    def get_config(self) -> ItemData:
        return {}

    @classmethod
    @abstractmethod
    def from_config(cls, cfg: ItemData, parent=None):
        return cls(parent=parent)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        if event.buttons() == Qt.MouseButton.LeftButton and self.opened:
            if self.previous_position is not None:
                pos_change = event.position() - self.previous_position
                new_size = QSize(self.size().width(), int(self.size().height() + pos_change.y()))
                self.setMinimumHeight(new_size.height())
        self.previous_position = event.position()
        return super().mouseMoveEvent(event)

    def mousePressEvent(self, event: QMouseEvent) -> None:
        self.previous_position = event.position()
        return super().mousePressEvent(event)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.cfg_name()})"


class FlowList(QGroupBox):  # TODO: Better name lmao
    n = Signal(int)
    total = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._layout = QGridLayout()
        self.items: list[FlowItem] = []
        self.registered_items: dict[str, type[FlowItem]] = {}
        self.setLayout(self._layout)
        self.scroll_area = QScrollArea(self)
        self.scroll_area.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
        self.scroll_widget = QWidget(self)

        self.name_text = QLabel(self)
        self.box = QVBoxLayout(self.scroll_widget)
        self.scroll_widget.setLayout(self.box)

        self.scroll_widget.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.scroll_widget)
        self.add_box = QToolButton(self)
        self.add_box.setText("+")
        self.add_box.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.add_box.hide()
        self.add_button = QToolButton(self)
        self.add_button.setText("+")
        self.add_button.hide()
        self.progressbar = QProgressBar(self)
        self.progressbar.setFormat("%p%  %v/%m")

        self.total.connect(self.progressbar.setMaximum)
        self.n.connect(self.progressbar.setValue)

        self.add_menu = QMenu(self)
        self.add_box.setMenu(self.add_menu)

        self._layout.addWidget(self.add_box, 0, 0)
        self._layout.addWidget(self.add_button, 0, 0)
        self._layout.addWidget(self.name_text, 0, 1)
        self._layout.addWidget(self.progressbar, 0, 2)
        self._layout.addWidget(self.scroll_area, 1, 0, 1, 3)

    def set_text(self, s: str):
        self.name_text.setText(s)

    def _register_item(self, item: type[FlowItem]):
        self.add_item_to_menu(item)
        self.registered_items[item.cfg_name()] = item
        if len(self.add_menu.actions()) == 1:
            self.add_button.show()
            self.add_button.clicked.connect(self.add_menu.actions()[0].trigger)
        elif not self.add_box.isVisible():
            self.add_box.show()
            self.add_button.hide()

    def add_item_to_menu(self, item: type[FlowItem]):
        self.add_menu.addAction(item.title, lambda: self.initialize_item(item))

    def initialize_item(self, item: type[FlowItem]):
        self.add_item(item(self))

    def register_item(self, *items: type[FlowItem]):
        for item in items:
            self._register_item(item)

    def add_item(self, item: FlowItem, idx=None):
        if idx is None:
            self.items.append(item)
            self.box.addWidget(item)
        else:
            self.items.insert(idx, item)
            self.box.insertWidget(idx, item)
        item.move_up.connect(lambda: self.move_item(item, -1))
        item.move_down.connect(lambda: self.move_item(item, 1))
        item.closed.connect(lambda: self.remove_item(item))
        item.duplicate.connect(lambda: self.duplicate_item(item))
        item.increment.connect(self.increment_pbar)

    def remove_item(self, item: FlowItem):
        item.setGeometry(QRect(0, 0, 0, 0))
        self.box.removeWidget(item)
        item.hide()
        self.items.remove(item)

    def move_item(self, item: FlowItem, direction: int):
        """moves items up (-) and down (+) the list

        Parameters
        ----------
        item : FlowItem
            the item to move
        direction : int
            how much to move, and what direction
        """
        index: int = self.box.indexOf(item)
        if index == -1:
            return
        new_index = min(max(index + direction, 0), self.box.count())

        self.box.removeWidget(item)
        self.box.insertWidget(new_index, item)
        self.items.insert(new_index, self.items.pop(index))

    def duplicate_item(self, item: FlowItem):
        """duplicates an item"""
        self.add_item(
            item.from_config(item.get_config(), parent=self),
            self.box.indexOf(item) + 1,
        )

    def empty(self):
        for item in self.items.copy():
            self.remove_item(item)

    @Slot()
    def increment_pbar(self):
        self.n.emit(self.progressbar.value() + 1)

    @Slot()
    def get_config(self) -> list[ItemConfig]:
        return [
            ItemConfig[ItemData](
                data=item.get_config(),
                enabled=item.enabled,
                name=item.cfg_name(),
                open=item.opened,
            )  # type: ignore
            for item in self.items
        ]

    def add_from_cfg(self, lst: list[ItemConfig]):
        for new_item in lst:
            item: FlowItem = self.registered_items[new_item["name"]].from_config(new_item["data"], parent=self)
            item.enabled = new_item.get("enabled", True)
            item.opened = new_item.get("open", False)
            self.add_item(item)

    def get(self, include_not_enabled=False) -> list:
        self.total.emit(len(self.items))
        self.n.emit(0)
        if include_not_enabled:
            return list(map(FlowItem.get, self.items))
        return [item.get() for item in self.items if item.enabled]


class MiniCheckList(QFrame):
    def __init__(self, items: Collection[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        grid = QGridLayout(self)
        grid.setVerticalSpacing(0)
        self.setLayout(grid)

        self.items: dict[str, QCheckBox] = {}
        for idx, item in enumerate(items):
            checkbox = QCheckBox(self)
            checkbox.setText(item)
            self.items[item] = checkbox
            grid.addWidget(checkbox, idx, 0)

    def get_config(self) -> dict[str, bool]:
        return {s: item.isChecked() for s, item in self.items.items()}

    def set_config(self, i: str, val: bool):
        self.items[i].setChecked(val)
