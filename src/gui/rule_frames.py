from __future__ import annotations

import os
import sys
import textwrap
from abc import abstractmethod
from pprint import pprint

from PySide6.QtCore import QDate, QDateTime, QRect, Qt, QThread, QTime, Signal, Slot
from PySide6.QtGui import QMouseEvent
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

from ..datarules import base_rules, data_rules, image_rules


class FlowItem(QFrame):  # TODO: Better name lmao
    title: str = ""
    desc: str = ""
    needs_settings: bool = False

    collapsed = Signal(bool)

    def __init__(self, parent: FlowList):
        super().__init__(parent)
        self.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)  # type: ignore
        self.setLineWidth(2)
        self._parent: FlowList = parent

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
        # self.uparrow.setPopupMode
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
    def get_cfg(self):
        ...

    @classmethod
    @abstractmethod
    def from_cfg(cls):
        ...

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
    def __init__(self):
        super().__init__()
        self._layout = QGridLayout()

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
        self.addmenu = QMenu(self)
        self.addbox.setMenu(self.addmenu)

        self._layout.addWidget(self.addbox, 0, 0)
        self._layout.addWidget(self.scrollarea, 1, 0)

    def register_item(self, item: type[FlowItem]):
        self.addmenu.addAction(item.title, lambda: self.initialize_item(item))

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
        self.box.addWidget(item)

    def remove_item(self, item: FlowItem):
        item.setGeometry(QRect(0, 0, 0, 0))
        self.box.removeWidget(item)
        item.hide()
        del item

    def move_item(self, item: FlowItem, direction: int):
        index = self.box.indexOf(item)
        if index == -1:
            return
        newindex = index + direction
        if newindex < 0:
            return
        if newindex >= self.box.count():
            return
        self.box.removeWidget(item)
        self.box.insertWidget(newindex, item)


class ProducerView(FlowItem):
    title = "Producer"

    bound_producer: type[base_rules.Producer]

    def setup_widget(self):
        super().setup_widget()
        if self.desc:
            self.desc += "\n"
        self.desc += f"Produces: {set(self.bound_producer.produces)}"
        self.descriptionwidget.setText(self.desc)
        # return super().setup_widget()

    @abstractmethod
    def get_producer(self):
        """Evaluates the settings and returns a Producer instance"""


class FileInfoProducerView(ProducerView):
    title = "File Info Producer"
    bound_producer = data_rules.FileInfoProducer


class ImShapeProducerView(ProducerView):
    title = "Image shape"
    bound_producer = image_rules.ImShapeProducer


class HashProducerView(ProducerView):
    title = "Hash Producer"
    desc = "gets a hash for the contents of an image"
    bound_producer = image_rules.HashProducer
    needs_settings = True

    def configure_settings_group(self):
        hash_type = QComboBox()
        hash_type.addItems([*image_rules.HASHERS])
        self.groupgrid.addWidget(QLabel("Hash type: "), 0, 0)
        self.groupgrid.addWidget(hash_type, 0, 1)


class RuleView(FlowItem):
    title = "Rule"

    bound_rule: type[base_rules.Rule]

    @abstractmethod
    def get_rule(self):
        """Evaluates the settings and returns a Rule instance"""


class StatRuleView(RuleView):
    title = "Time Range"
    desc = "only allow files created within a time frame."

    bound_rule = data_rules.StatRule
    needs_settings = True

    def configure_settings_group(self):
        self.after_widget = QDateTimeEdit(self)
        self.before_widget = QDateTimeEdit(self)
        self.after_widget.setCalendarPopup(True)
        self.before_widget.setCalendarPopup(True)
        self.after_widget.setDisplayFormat("dd/MM/yyyy h:mm AP")
        self.before_widget.setDisplayFormat("dd/MM/yyyy h:mm AP")
        self.after_widget.setDateTime(QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0)))
        self.before_widget.setDateTime(QDateTime.currentDateTime())
        format_label = QLabel("dd/MM/yyyy h:mm AP")
        format_label.setEnabled(False)
        self.groupgrid.addWidget(format_label, 0, 1)
        self.groupgrid.addWidget(QLabel("After: "), 1, 0)
        self.groupgrid.addWidget(QLabel("Before: "), 2, 0)
        self.groupgrid.addWidget(self.after_widget, 1, 1, 1, 2)
        self.groupgrid.addWidget(self.before_widget, 2, 1, 1, 2)


class BlacklistWhitelistView(RuleView):
    title = "Blacklist and Whitelist"
    desc = "Only allows paths that include strs in the whitelist and not in the blacklist"

    bound_rule = data_rules.BlacknWhitelistRule
    needs_settings = True

    def configure_settings_group(self):
        self.whitelist = QTextEdit(self)
        self.blacklist = QTextEdit(self)

        self.whitelist_exclusive = QCheckBox("exclusive", self)
        self.whitelist_exclusive.setToolTip("when enabled, only files that are valid to every single str is allowed")
        self.groupgrid.addWidget(QLabel("Whitelist: "), 0, 0)
        self.groupgrid.addWidget(QLabel("Blacklist: "), 2, 0)
        self.groupgrid.addWidget(self.whitelist_exclusive, 0, 1, 1, 1)
        self.groupgrid.addWidget(self.whitelist, 1, 0, 1, 2)
        self.groupgrid.addWidget(self.blacklist, 3, 0, 1, 2)


class TotalLimitRuleView(RuleView):
    title = "Total count"
    desc = "Limits the total number of files past this point"

    bound_rule = data_rules.TotalLimitRule
    needs_settings = True

    def configure_settings_group(self):
        self.limit_widget = QSpinBox(self)
        self.limit_widget.setRange(0, 1000000000)
        self.groupgrid.addWidget(QLabel("Limit: "), 0, 0)
        self.groupgrid.addWidget(self.limit_widget, 0, 1)


class ExistingRuleView(RuleView):
    title = "Existing"

    bound_rule = data_rules.ExistingRule
    needs_settings = True

    def configure_settings_group(self):
        self.exists_in = QComboBox(self)
        self.exists_in.addItems(["all", "any"])
        self.existing_list = QTextEdit(self)
        self.groupgrid.addWidget(QLabel("Exists in: "), 0, 0)
        self.groupgrid.addWidget(QLabel("of the folders"), 0, 2)
        self.groupgrid.addWidget(QLabel("Existing folders: "), 1, 0, 1, 3)
        self.groupgrid.addWidget(self.exists_in, 0, 1)
        self.groupgrid.addWidget(self.existing_list, 2, 0, 1, 3)


class ResRuleView(RuleView):
    title = "Resolution"

    bound_rule = image_rules.ResRule
    needs_settings = True

    def configure_settings_group(self):
        self.min = QSpinBox(self)
        self.max = QSpinBox(self)
        self.crop = QCheckBox(self)
        self.scale = QSpinBox(self)
        self.min.setMaximum(1_000_000_000)
        self.max.setMaximum(1_000_000_000)
        self.scale.setMaximum(128)  # I think this is valid
        self.max.setValue(2048)
        self.scale.setValue(4)
        self.crop.setChecked(True)
        self.groupgrid.addWidget(QLabel("Min / Max: "), 0, 0)
        self.groupgrid.addWidget(self.min, 1, 0)
        self.groupgrid.addWidget(self.max, 1, 1)
        self.groupgrid.addWidget(QLabel("Try to crop: "), 2, 0)
        self.groupgrid.addWidget(self.crop, 2, 1)
        self.groupgrid.addWidget(QLabel("Scale: "), 3, 0)
        self.groupgrid.addWidget(self.scale, 3, 1)


class ChannelRuleView(RuleView):
    title = "Channels"

    bound_rule = image_rules.ChannelRule
    needs_settings = True

    def configure_settings_group(self):
        self.min = QSpinBox(self)
        self.max = QSpinBox(self)
        self.max.setValue(3)
        self.groupgrid.addWidget(QLabel("Min / Max: "), 0, 0)
        self.groupgrid.addWidget(self.min, 0, 1)
        self.groupgrid.addWidget(self.max, 0, 2)


class HashRuleView(RuleView):
    title = "Hash"
    desc = "Uses imagehash functions to eliminate similar looking images"

    bound_rule = image_rules.HashRule
    needs_settings = True

    def configure_settings_group(self):
        self.ignore_all_btn = QCheckBox(self)
        self.resolver = QLineEdit(self)
        self.resolver.setText("mtime")
        self.ignore_all_btn.toggled.connect(self.toggle_resolver)
        self.groupgrid.addWidget(QLabel("Ignore all with conflicts: "), 0, 0)
        self.groupgrid.addWidget(self.ignore_all_btn, 0, 1)
        self.groupgrid.addWidget(QLabel("Conflict resolver column: "), 1, 0)
        self.groupgrid.addWidget(self.resolver, 2, 0, 1, 2)

    @Slot(bool)
    def toggle_resolver(self, x):
        self.resolver.setEnabled(not x)
