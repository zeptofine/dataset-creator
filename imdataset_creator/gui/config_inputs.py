from __future__ import annotations

import contextlib
import functools
from copy import deepcopy as objcopy

from PySide6.QtCore import QRect, QSize, Qt, Signal, Slot
from PySide6.QtGui import QAction, QDrag, QDragEnterEvent, QDragLeaveEvent, QDragMoveEvent, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QMenu,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ..configs.configtypes import ItemConfig, ItemData
from ..configs.keyworded import Keyworded, fancy_repr
from .frames import apply_tooltip
from .settings_inputs import BaseInput, ItemSettings, SettingsBox, SettingsItem

JSON_SERIALIZABLE = dict | list | tuple | str | int | float | bool | None


def copy_before_exec(f):
    @functools.wraps(f)
    def func(self, *args, **kwargs):
        return f(objcopy(self), *args, **kwargs)

    return func


class ProceduralConfigItem(QFrame):
    movable: bool = True

    move_down = Signal()
    move_up = Signal()
    position_changed = Signal()
    closed = Signal()
    duplicate = Signal()

    n_changed = Signal(int)
    total_changed = Signal(int)

    bound_item: type[Keyworded] | Keyworded

    reverted = Signal()

    def __init__(self, item: ItemDeclaration, parent: QWidget | None = None):
        super().__init__(parent)
        self.declaration = item
        self.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Raised)
        self.setLineWidth(2)

        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        (collapse_action := QAction("collapse", self)).triggered.connect(self.toggle_group)
        (duplicate_action := QAction("duplicate", self)).triggered.connect(self.duplicate.emit)
        (revert_action := QAction("revert to defaults", self)).triggered.connect(self.reverted.emit)
        self.addActions([collapse_action, duplicate_action, revert_action])

        # self._minimum_size = self.size()
        self.previous_position = None

        self._n = 0
        self._total = 0

        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(0)
        self._layout.setSizeConstraint(QVBoxLayout.SizeConstraint.SetMinimumSize)
        # setup top bar

        topbarwidget = QFrame(self)
        topbarwidget.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)
        self._top_section_layout = QGridLayout(topbarwidget)
        self._top_section_layout.setContentsMargins(6, 6, 6, 6)
        top_bar = self._top_bar()
        for idx, widget in enumerate(top_bar):
            self._top_section_layout.addWidget(widget, 0, idx)

        self._layout.addWidget(topbarwidget)
        self.settings_box: SettingsBox | None
        if self.declaration.settings is not None:
            self.settings_box = self.declaration.create_settings_widget(self)
            for row in self.settings_box.rows.values():
                self.reverted.connect(row.reset)

            self._layout.addWidget(self.settings_box)
        else:
            self.settings_box = None
        self.opened = True

    def _top_bar(self) -> list[QWidget]:
        widgets: list[QWidget] = []
        self.checkbox = QCheckBox()
        self.checkbox.setChecked(True)
        if self.declaration.title:
            self.checkbox.setText(self.declaration.title)
            if self.declaration.description:
                apply_tooltip(self.checkbox, self.declaration.description)

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

    def get(self):
        """produces something the item represents"""
        if self.settings_box is not None:
            return self.declaration.get(self.settings_box)
        return self.declaration.get()

    @Slot()
    def toggle_group(self):
        self.opened = not self.opened

    def mouseDoubleClickEvent(self, event: QMouseEvent) -> None:
        self.toggle_group()
        event.accept()

    @property
    def n(self) -> int:
        return self._n

    @n.setter
    def n(self, val):
        self._n = val
        self.n_changed.emit(self._n)

    @property
    def total(self):
        return self._total

    @total.setter
    def total(self, val):
        self._total = val
        self.total_changed.emit(self._total)

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
        if self.settings_box is not None:
            self.settings_box.setVisible(b)
        # if not b:
        # self._minimum_size = self.minimumSize()
        # self.setMinimumSize(0, 0)
        # else:
        # self.setMinimumSize(self._minimum_size)
        self.__opened = b

    def cfg_name(self):
        return self.declaration.bound_item.cfg_kwd()

    def get_cfg(self):
        if self.settings_box is None:
            return {}
        return self.settings_box.get_cfg()

    def from_cfg(self, dct):
        if self.settings_box is not None:
            self.settings_box.from_cfg(dct)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.cfg_name()})"

    def copy(self):
        cfg = self.get_cfg()
        new = self.__class__(self.declaration)
        new.from_cfg(cfg)
        return new


class ProceduralConfigList(QGroupBox):  # TODO: Better name lmao
    n = Signal(int)
    total = Signal(int)

    changed = Signal()

    def __init__(self, *items: ItemDeclaration, unique=False, parent=None):
        super().__init__(parent)
        self._layout = QGridLayout()
        self.all_are_unique = unique
        # self._layout.setContentsMargins(0, 0, 0, 0)
        self.bound_item = ProceduralConfigItem
        self.items: list[ProceduralConfigItem] = []
        self.registered_items: dict[str, ItemDeclaration] = {}
        self.created_items: set[ItemDeclaration] = set()

        self.setLayout(self._layout)
        self.scroll_area = QScrollArea(self)
        self.scroll_area.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
        self.scroll_widget = QWidget(self)

        self.name_text = QLabel(self)
        self.box = QVBoxLayout(self.scroll_widget)
        self.box.setContentsMargins(8, 8, 8, 8)
        self.box.setSpacing(5)
        self.scroll_widget.setLayout(self.box)

        self.scroll_widget.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.scroll_widget)
        self.add_box = QToolButton(self)
        self.add_box.setText("+")
        self.add_box.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.add_box.setDisabled(True)

        self.progressbar = QProgressBar(self)
        self.progressbar.setFormat("%p%  %v/%m")
        self.progressbar.hide()

        self.total.connect(self.progressbar.setMaximum)
        self.n.connect(self.progressbar.setValue)

        self.add_menu = QMenu(self)

        self.add_box.setMenu(self.add_menu)

        self._layout.addWidget(self.add_box, 0, 0)
        self._layout.addWidget(self.name_text, 0, 1)
        self._layout.addWidget(self.progressbar, 0, 2)
        self._layout.addWidget(self.scroll_area, 1, 0, 1, 3)

        self.register_item(*items)

    def label(self, s: str):
        self.name_text.setText(s)
        return self

    @Slot()
    def update_n(self):
        n = 0
        for item in self.items:
            n += item.n
        self.n.emit(n)

    @Slot()
    def update_total(self):
        total = 0
        for item in self.items:
            total += item.total

        if total > 0 and not self.progressbar.isVisible():
            self.progressbar.setVisible(True)
        elif total == 0 and self.progressbar.isVisible():
            self.progressbar.setVisible(False)
        self.total.emit(total)

    def update_menu(self):
        menu_items = {a.text(): a for a in self.add_menu.actions()}
        available_items = {
            item.title: item
            for item in self.registered_items.values()
            if not (item in self.created_items and self.all_are_unique)
        }

        for item in menu_items.keys() - available_items.keys():
            self.add_menu.removeAction(menu_items[item])

        for item in available_items.keys() - menu_items.keys():
            self.add_item_to_menu(available_items[item])

        if any(actions := self.add_menu.actions()):
            if len(actions) != 1:
                self.add_box.setMenu(self.add_menu)
            else:
                self.add_box.setMenu(None)  # type: ignore
                with contextlib.suppress(RuntimeError):
                    self.add_box.clicked.disconnect()
                self.add_box.clicked.connect(actions[0].trigger)

            self.add_box.setEnabled(True)
        elif self.add_box.isVisible():
            self.add_box.setEnabled(False)
        return

    def add_item_to_menu(self, item: ItemDeclaration):
        self.add_menu.addAction(item.title, lambda: self.initialize_item(item))

    def initialize_item(self, item: ItemDeclaration):
        self.add_item(self.bound_item(item, self))

    def register_item(self, *items: ItemDeclaration):
        for item in items:
            self._register_item(item)
        self.update_menu()

    def _register_item(self, item: ItemDeclaration):
        self.registered_items[item.bound_item.cfg_kwd()] = item
        if not self.add_box.isEnabled():
            self.add_box.setEnabled(True)

    def add_item(self, item: ProceduralConfigItem, idx=None):
        if idx is None:
            self.items.append(item)
            self.box.addWidget(item)
        else:
            self.items.insert(idx, item)
            self.box.insertWidget(idx, item)
        self.created_items.add(item.declaration)
        item.move_up.connect(lambda: self.move_item(item, -1))
        item.move_down.connect(lambda: self.move_item(item, 1))
        item.closed.connect(lambda: self.remove_item(item))
        item.duplicate.connect(lambda: self.duplicate_item(item))
        item.n_changed.connect(self.update_n)
        item.total_changed.connect(self.update_total)
        self.update_menu()

    def remove_item(self, item: ProceduralConfigItem):
        item.setGeometry(QRect(0, 0, 0, 0))
        self.box.removeWidget(item)
        item.hide()
        self.items.remove(item)
        self.created_items.remove(item.declaration)
        self.update_menu()

    def move_item(self, item: ProceduralConfigItem, direction: int):
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

    def duplicate_item(self, item: ProceduralConfigItem):
        """duplicates an item"""
        self.add_item(
            item,
            self.box.indexOf(item) + 1,
        )

    def empty(self):
        """Removes every item in the list"""
        for item in self.items.copy():
            self.remove_item(item)

    @Slot()
    def get_cfg(self) -> list[ItemConfig]:
        return [
            ItemConfig[ItemData](
                data=item.get_cfg(),
                enabled=item.enabled,
                name=item.cfg_name(),
                open=item.opened,
            )  # type: ignore
            for item in self.items
        ]

    def add_from_cfg(self, lst: list[ItemConfig]):
        for new_item in lst:
            item: ProceduralConfigItem = ProceduralConfigItem(self.registered_items[new_item["name"]], self)
            item.from_cfg(new_item["data"])
            item.enabled = new_item.get("enabled", True)
            item.opened = new_item.get("open", False)
            self.add_item(item)

    def get(self, include_not_enabled=False) -> list:
        if include_not_enabled:
            return list(map(ProceduralConfigItem.get, self.items))
        return [item.get() for item in self.items if item.enabled]


class ProceduralFlowListSettings(SettingsItem):  # TODO: Better name lmao
    def __init__(self, *items: ItemDeclaration, parent: QWidget | None = None):
        self.items = items
        self.parent = parent

    def create(self):
        self.widget: ProceduralConfigList = ProceduralConfigList(*self.items, parent=self.parent)
        return [self.widget]

    def from_cfg(self, val):
        self.widget.add_from_cfg(val)

    def get_cfg(self):
        return self.widget.get_cfg()

    def reset(self):
        self.widget.empty()


class ProceduralFlowListInput(BaseInput):
    def __init__(self, *items: ItemDeclaration, parent: QWidget | None = None):
        super().__init__()
        self.items = items
        self.parent: QWidget | None = parent

    def get_settings(self):
        return ProceduralFlowListSettings(*self.items, parent=self.parent)


@fancy_repr
class ItemDeclaration:
    def __init__(
        self,
        title: str,
        bound_item: type[Keyworded],
        desc: str | None = None,
        settings: ItemSettings | None = None,
    ):
        self.title: str = title
        self.description: str | None = desc
        self.bound_item = bound_item
        self.settings: ItemSettings | None = settings

    def create_settings_widget(self, parent=None):
        assert self.settings is not None
        return SettingsBox(self.settings, parent)

    def get(self, box: SettingsBox | None = None):
        if box is None:
            return self.bound_item()
        return self.bound_item.from_cfg(box)
