from __future__ import annotations

from abc import abstractmethod
from typing import TypedDict

from PySide6.QtCore import QRect, Qt, Signal, Slot
from PySide6.QtGui import QAction, QIcon, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
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


class ItemConfig(TypedDict):
    data: dict
    enabled: bool
    name: str
    open: bool


class FlowItem(QFrame):  # TODO: Better name lmao
    title: str = ""
    cfg_name: str
    desc: str = ""
    needs_settings: bool = False
    movable: bool = True

    move_down = Signal()
    move_up = Signal()
    position_changed = Signal()
    closed = Signal()
    duplicate = Signal()

    increment = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
        self.setLineWidth(2)
        self.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)

        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        (collapse_action := QAction("collapse", self)).triggered.connect(self.toggle_group)
        (duplicate_action := QAction("duplicate", self)).triggered.connect(self.duplicate.emit)
        revert_action = QAction("revert to defaults", self)
        revert_action.triggered.connect(self.reset_settings_group)
        self.addActions([collapse_action, duplicate_action, revert_action])

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
        self.descriptionwidget = QLabel(self.desc, self)
        self.descriptionwidget.hide()
        self.descriptionwidget.setWordWrap(True)

        self.groupgrid = QGridLayout()
        self.group.hide()
        self.group.setLayout(self.groupgrid)
        self._layout.addWidget(self.descriptionwidget, 1, 0, 1, len(top_bar))
        self._layout.addWidget(self.group, 2, 0, 1, len(top_bar))

    def _top_bar(self) -> list[QWidget]:
        widgets = []
        self.checkbox = QCheckBox()
        self.checkbox.setChecked(True)
        if self.title:
            self.checkbox.setText(self.title)
        widgets.append(self.checkbox)

        if self.movable:
            self.uparrow = QToolButton()
            self.downarrow = QToolButton()
            self.uparrow.setText("↑")
            self.downarrow.setText("↓")
            self.uparrow.clicked.connect(self.position_changed)
            self.uparrow.clicked.connect(self.move_up)
            self.downarrow.clicked.connect(self.position_changed)
            self.downarrow.clicked.connect(self.move_down)
            widgets.append(self.uparrow)
            widgets.append(self.downarrow)

        self.closebutton = QToolButton()
        self.closebutton.setText("X")
        self.closebutton.clicked.connect(self.closed)
        widgets.append(self.closebutton)

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
            self.descriptionwidget.setVisible(b)
        self.__opened = b

    # Saving and creating methods

    @abstractmethod
    def get_config(self) -> dict:
        return {}

    @classmethod
    @abstractmethod
    def from_config(cls, cfg: dict, parent=None):
        return cls(parent=parent)


class FlowList(QGroupBox):  # TODO: Better name lmao
    n = Signal(int)
    total = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._layout = QGridLayout()
        self.items: list[FlowItem] = []
        self.registered_items: dict[str, type[FlowItem]] = {}
        self.setLayout(self._layout)
        self.scrollarea = QScrollArea(self)
        self.scrollarea.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
        self.scrollwidget = QWidget(self)

        self.box = QVBoxLayout(self.scrollwidget)
        self.scrollwidget.setLayout(self.box)
        self.scrollwidget.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Maximum)
        self.scrollarea.setWidgetResizable(True)
        self.scrollarea.setWidget(self.scrollwidget)
        self.addbox = QToolButton(self)
        self.addbox.setText("+")
        self.addbox.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.addbox.hide()
        self.addbutton = QToolButton(self)
        self.addbutton.setText("+")
        self.addbutton.hide()
        self.progressbar = QProgressBar(self)
        self.progressbar.setFormat("%p%  %v/%m")

        self.total.connect(self.progressbar.setMaximum)
        self.n.connect(self.progressbar.setValue)

        self.addmenu = QMenu(self)
        self.addbox.setMenu(self.addmenu)

        self._layout.addWidget(self.addbox, 0, 0)
        self._layout.addWidget(self.addbutton, 0, 0)
        self._layout.addWidget(self.progressbar, 0, 1)
        self._layout.addWidget(self.scrollarea, 1, 0, 1, 2)

    def register_item(self, item: type[FlowItem]):
        self.addmenu.addAction(item.title, lambda: self.initialize_item(item))
        self.registered_items[item.title or item.cfg_name] = item
        if len(self.addmenu.actions()) == 1:
            self.addbutton.show()
            self.addbutton.clicked.connect(self.addmenu.actions()[0].trigger)
        elif not self.addbox.isVisible():
            self.addbox.show()
            self.addbutton.hide()

    def initialize_item(self, item: type[FlowItem]):
        self.add_item(item(self))

    def register_items(self, *items: type[FlowItem]):
        for item in items:
            self.register_item(item)

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
        newindex = min(max(index + direction, 0), self.box.count())

        self.box.removeWidget(item)
        self.box.insertWidget(newindex, item)
        self.items.insert(newindex, self.items.pop(index))

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
    def get_config(self) -> list:
        return [
            ItemConfig(
                data=item.get_config(),
                enabled=item.enabled,
                name=item.title or item.cfg_name,
                open=item.opened,
            )
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
