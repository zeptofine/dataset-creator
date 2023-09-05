from __future__ import annotations

from abc import abstractmethod
from typing import TypedDict

import PySide6.QtCore as QtCore
from PySide6.QtCore import QDate, QDateTime, QRect, Qt, QThread, QTime, Signal, Slot
from PySide6.QtGui import QIcon, QMouseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QSpinBox,
    QSplitter,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)


class ItemConfig(TypedDict):
    name: str
    data: dict


class FlowItem(QFrame):  # TODO: Better name lmao
    title: str = ""
    desc: str = ""
    needs_settings: bool = False

    collapsed = Signal(bool)

    def __init__(self, parent=None, size_policy=(QSizePolicy.Minimum, QSizePolicy.Maximum)):  # type: ignore
        super().__init__(parent)
        self.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)  # type: ignore
        self.setLineWidth(2)
        self.setSizePolicy(*size_policy)

        self.setup_widget()
        self.configure_settings_group()
        self.opened = True
        self.collapse(True)

    def setup_widget(self):
        self._layout = QGridLayout()
        self.setLayout(self._layout)

        self.checkbox = QCheckBox(self.title)
        self.checkbox.setChecked(True)
        self.closebutton = QToolButton()
        self.closebutton.setText("X")
        self.descriptionwidget = QLabel(self.desc)
        self.descriptionwidget.hide()
        self.descriptionwidget.setWordWrap(True)
        self.uparrow = QToolButton()
        self.downarrow = QToolButton()
        self.uparrow.setText("↑")
        self.downarrow.setText("↓")
        self.dropdown = QToolButton()
        self.dropdown.setText("⌤")
        self.dropdown.clicked.connect(self.toggle_group)
        self.group = QGroupBox()

        self.groupgrid = QGridLayout()
        self.group.hide()
        self.group.setLayout(self.groupgrid)
        self._layout.addWidget(self.checkbox, 0, 0)
        self._layout.addWidget(self.uparrow, 0, 1)
        self._layout.addWidget(self.downarrow, 0, 2)
        self._layout.addWidget(self.dropdown, 0, 3)
        self._layout.addWidget(self.closebutton, 0, 4)
        self._layout.addWidget(self.descriptionwidget, 1, 0, 1, 5)
        self._layout.addWidget(self.group, 2, 0, 1, 5)

    @abstractmethod
    def configure_settings_group(self):
        ...

    @abstractmethod
    def get_cfg(self) -> dict:
        return {}

    @classmethod
    @abstractmethod
    def from_cfg(cls, cfg, parent=None):
        return cls(parent=parent)

    @Slot()
    def toggle_group(self):
        self.opened = not self.opened
        self.collapse(self.opened)

    def collapse(self, b: bool):
        if self.needs_settings:
            self.group.setVisible(b)
        if self.desc:
            self.descriptionwidget.setVisible(b)
        self.collapsed.emit(b)

    def mouseDoubleClickEvent(self, event: QMouseEvent) -> None:
        self.toggle_group()
        event.accept()


class FlowList(QGroupBox):  # TODO: Better name lmao
    def __init__(self, parent=None):
        super().__init__(parent)
        self._layout = QGridLayout()
        self.items: dict[str, FlowItem] = {}
        self.registered_items: dict[str, type[FlowItem]] = {}
        self.setLayout(self._layout)
        self.scrollarea = QScrollArea(self)
        self.scrollarea.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)  # type: ignore
        self.scrollwidget = QWidget(self)

        self.box = QVBoxLayout(self.scrollwidget)
        self.scrollwidget.setLayout(self.box)
        self.scrollwidget.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Maximum)  # type: ignore

        self.scrollarea.setWidgetResizable(True)
        self.scrollarea.setWidget(self.scrollwidget)
        self.addbox = QToolButton(self)
        self.addbox.setText("+")
        self.addbox.setPopupMode(QToolButton.InstantPopup)  # type: ignore
        self.addbox.hide()
        self.addbutton = QToolButton(self)
        self.addbutton.setText("+")
        self.addbutton.hide()

        self.addmenu = QMenu(self)
        self.addbox.setMenu(self.addmenu)

        self._layout.addWidget(self.addbox, 0, 0)
        self._layout.addWidget(self.addbutton, 0, 0)
        self._layout.addWidget(self.scrollarea, 1, 0)

    def register_item(self, item: type[FlowItem]):
        self.addmenu.addAction(item.title, lambda: self.initialize_item(item))
        self.registered_items[item.title] = item
        if len(self.addmenu.actions()) == 1:
            self.addbutton.show()
            self.addbutton.clicked.connect(self.addmenu.actions()[0].trigger)
        if len(self.addmenu.actions()) > 1 and not self.addbox.isVisible():
            self.addbox.show()
            self.addbutton.hide()

    def initialize_item(self, item: type[FlowItem]):
        instance = item(self)
        self.add_item(instance)
        instance.closebutton.clicked.connect(lambda: self.remove_item(instance))
        instance.uparrow.clicked.connect(lambda: self.move_item(instance, -1))
        instance.downarrow.clicked.connect(lambda: self.move_item(instance, 1))

    def register_items(self, *items: type[FlowItem]):
        for item in items:
            self.register_item(item)

    def add_item(self, item: FlowItem):
        self.items[item.title] = item
        self.box.addWidget(item)

    def remove_item(self, item: FlowItem):
        item.setGeometry(QRect(0, 0, 0, 0))
        self.box.removeWidget(item)
        item.hide()
        self.items.pop(item.title)
        del item

    def move_item(self, item: FlowItem, direction: int):
        """
        moves items up and down the list via +n or -n

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
        newindex = index + direction
        if newindex < 0:
            return
        if newindex >= self.box.count():
            return
        self.box.removeWidget(item)
        self.box.insertWidget(newindex, item)

    @Slot()
    def get_cfg(self) -> list:
        return [{"name": title, "data": item.get_cfg()} for title, item in self.items.items()]

    def add_from_cfg(self, lst: list[ItemConfig]):
        for new_item in lst:
            self.add_item(self.registered_items[new_item["name"]].from_cfg(new_item["data"], parent=self))
