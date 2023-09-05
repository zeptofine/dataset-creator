from abc import abstractmethod

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
from .frames import FlowItem


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

    _datetime_format: str = "dd/MM/yyyy h:mm AP"

    def configure_settings_group(self):
        self.after_widget = QDateTimeEdit(self)
        self.before_widget = QDateTimeEdit(self)
        self.after_widget.setCalendarPopup(True)
        self.before_widget.setCalendarPopup(True)
        self.after_widget.setDisplayFormat(self._datetime_format)
        self.before_widget.setDisplayFormat(self._datetime_format)
        self.after_widget.setDateTime(QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0)))
        self.before_widget.setDateTime(QDateTime.currentDateTime())
        format_label = QLabel(self._datetime_format)
        format_label.setEnabled(False)
        self.groupgrid.addWidget(format_label, 0, 1)
        self.groupgrid.addWidget(QLabel("After: "), 1, 0)
        self.groupgrid.addWidget(QLabel("Before: "), 2, 0)
        self.groupgrid.addWidget(self.after_widget, 1, 1, 1, 2)
        self.groupgrid.addWidget(self.before_widget, 2, 1, 1, 2)

    def get_cfg(self):
        return {
            "after": self.after_widget.dateTime().toString(self._datetime_format),
            "before": self.before_widget.dateTime().toString(self._datetime_format),
        }

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.after_widget.setDateTime(QDateTime.fromString(cfg["after"], cls._datetime_format))
        self.before_widget.setDateTime(QDateTime.fromString(cfg["before"], cls._datetime_format))
        return self


class BlacklistWhitelistView(RuleView):
    title = "Blacklist and whitelist"
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

    def get_cfg(self):
        return {
            "whitelist": self.whitelist.toPlainText().splitlines(),
            "whitelist_exclusive": self.whitelist_exclusive.isChecked(),
            "blacklist": self.blacklist.toPlainText().splitlines(),
        }

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.whitelist.setText("\n".join(cfg["whitelist"]))
        self.whitelist_exclusive.setChecked(cfg["whitelist_exclusive"])
        self.blacklist.setText("\n".join(cfg["blacklist"]))
        return self


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

    def get_cfg(self):
        return {"limit": self.limit_widget.value()}

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.limit_widget.setValue(cfg["limit"])
        return self


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

    def get_cfg(self):
        return {
            "list": self.existing_list.toPlainText().splitlines(),
            "exists_in": self.exists_in.currentText(),
        }

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.existing_list.setText("\n".join(cfg["list"]))
        self.exists_in.setCurrentText(cfg["exists_in"])
        return self


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

    def get_cfg(self):
        return {
            "min": self.min.value(),
            "max": self.max.value(),
            "crop": self.crop.isChecked(),
            "scale": self.scale.value(),
        }

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.min.setValue(cfg["min"])
        self.max.setValue(cfg["max"])
        self.crop.setChecked(cfg["crop"])
        self.scale.setValue(cfg["scale"])
        return self


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

    def get_cfg(self):
        return {
            "min": self.min.value(),
            "max": self.max.value(),
        }


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

    def get_cfg(self):
        return {
            "resolver": self.resolver.text(),
            "ignore_all": self.ignore_all_btn.isChecked(),
        }

    @classmethod
    def from_cfg(cls, cfg, parent=None):
        self = cls(parent)
        self.ignore_all_btn.setChecked(cfg["ignore_all"])
        self.resolver.setText(cfg["resolver"])
        return self

    @Slot(bool)
    def toggle_resolver(self, x):
        self.resolver.setEnabled(not x)
