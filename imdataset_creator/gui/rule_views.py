from abc import abstractmethod

from PySide6.QtCore import QDate, QDateTime, QRect, Qt, QThread, QTime, Signal, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QSpinBox,
    QSplitter,
    QTextEdit,
    QToolButton,
    QWidget,
)

from ..configs import ItemData
from ..datarules import base_rules, data_rules, image_rules
from .frames import FlowItem


class RuleView(FlowItem):
    title = "Rule"

    @abstractmethod
    def get(self):
        """Evaluates the settings and returns a Rule instance"""
        super().get()
        return base_rules.Rule()


class StatRuleView(RuleView):
    title = "Time Range"
    desc = "only allow files created within a time frame."

    needs_settings = True
    bound_item = data_rules.StatRule
    _datetime_format: str = "dd/MM/yyyy h:mm AP"

    def configure_settings_group(self):
        self.after_widget = QDateTimeEdit(self)
        self.before_widget = QDateTimeEdit(self)
        self.after_widget.setCalendarPopup(True)
        self.before_widget.setCalendarPopup(True)
        self.after_widget.setDisplayFormat(self._datetime_format)
        self.before_widget.setDisplayFormat(self._datetime_format)
        format_label = QLabel(self._datetime_format, self)
        format_label.setEnabled(False)
        self.groupgrid.addWidget(format_label, 0, 1)
        self.groupgrid.addWidget(QLabel("After: ", self), 1, 0)
        self.groupgrid.addWidget(QLabel("Before: ", self), 2, 0)
        self.groupgrid.addWidget(self.after_widget, 1, 1, 1, 2)
        self.groupgrid.addWidget(self.before_widget, 2, 1, 1, 2)

    def get(self):
        super().get()
        return data_rules.StatRule(
            self.before_widget.dateTime().toString(self._datetime_format),
            self.after_widget.dateTime().toString(self._datetime_format),
        )

    def reset_settings_group(self):
        self.after_widget.setDateTime(QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0)))
        self.before_widget.setDateTime(QDateTime.currentDateTime())

    def get_config(self):
        return {
            "after": self.after_widget.dateTime().toString(self._datetime_format),
            "before": self.before_widget.dateTime().toString(self._datetime_format),
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.after_widget.setDateTime(QDateTime.fromString(cfg["after"], cls._datetime_format))
        self.before_widget.setDateTime(QDateTime.fromString(cfg["before"], cls._datetime_format))
        return self


class BlacklistWhitelistView(RuleView):
    title = "Blacklist and whitelist"
    desc = "Only allows paths that include strs in the whitelist and not in the blacklist"

    needs_settings = True
    bound_item = data_rules.BlacknWhitelistRule

    def configure_settings_group(self):
        self.whitelist = QTextEdit(self)
        self.blacklist = QTextEdit(self)

        self.groupgrid.addWidget(QLabel("Whitelist: ", self), 0, 0)
        self.groupgrid.addWidget(QLabel("Blacklist: ", self), 2, 0)
        self.groupgrid.addWidget(self.whitelist, 1, 0, 1, 2)
        self.groupgrid.addWidget(self.blacklist, 3, 0, 1, 2)

    def reset_settings_group(self):
        self.whitelist.clear()
        self.blacklist.clear()

    def get(self):
        super().get()
        return data_rules.BlacknWhitelistRule(
            self.whitelist.toPlainText().splitlines(),
            self.blacklist.toPlainText().splitlines(),
        )

    def get_config(self) -> data_rules.BlacknWhitelistData:
        return {
            "whitelist": self.whitelist.toPlainText().splitlines(),
            "blacklist": self.blacklist.toPlainText().splitlines(),
        }

    @classmethod
    def from_config(cls, cfg: data_rules.BlacknWhitelistData, parent=None):
        self = cls(parent)
        self.whitelist.setText("\n".join(cfg["whitelist"]))
        self.blacklist.setText("\n".join(cfg["blacklist"]))
        return self


class TotalLimitRuleView(RuleView):
    title = "Total count"
    desc = "Limits the total number of files past this point"

    needs_settings = True
    bound_item = data_rules.TotalLimitRule

    def configure_settings_group(self):
        self.limit_widget = QSpinBox(self)
        self.limit_widget.setRange(0, 1000000000)
        self.groupgrid.addWidget(QLabel("Limit: ", self), 0, 0)
        self.groupgrid.addWidget(self.limit_widget, 0, 1)

    def reset_settings_group(self):
        self.limit_widget.setValue(0)

    def get(self):
        super().get()
        return data_rules.TotalLimitRule(self.limit_widget.value())

    def get_config(self):
        return {"limit": self.limit_widget.value()}

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.limit_widget.setValue(cfg["limit"])
        return self


class ResRuleView(RuleView):
    title = "Resolution"

    needs_settings = True
    bound_item = image_rules.ResRule

    def configure_settings_group(self):
        self.min = QSpinBox(self)
        self.max = QSpinBox(self)
        self.crop = QCheckBox(self)
        self.scale = QSpinBox(self)
        self.min.valueChanged.connect(self.max.setMinimum)
        self.max.valueChanged.connect(self.min.setMaximum)
        self.min.setMaximum(1_000_000_000)
        self.max.setMaximum(1_000_000_000)
        self.scale.setRange(1, 128)  # I think this is valid
        self.groupgrid.addWidget(QLabel("Min / Max: ", self), 0, 0)
        self.groupgrid.addWidget(self.min, 1, 0)
        self.groupgrid.addWidget(self.max, 1, 1)
        self.groupgrid.addWidget(QLabel("Try to crop: ", self), 2, 0)
        self.groupgrid.addWidget(self.crop, 2, 1)
        self.groupgrid.addWidget(QLabel("Scale: ", self), 3, 0)
        self.groupgrid.addWidget(self.scale, 3, 1)

    def reset_settings_group(self):
        self.min.setValue(0)
        self.max.setValue(2048)
        self.scale.setValue(4)
        self.crop.setChecked(True)

    def get(self):
        super().get()
        return image_rules.ResRule(
            min_res=self.min.value(),
            max_res=self.max.value(),
            crop=self.crop.isChecked(),
            scale=self.scale.value(),
        )

    def get_config(self) -> image_rules.ResData:
        return {
            "min_res": self.min.value(),
            "max_res": self.max.value(),
            "crop": self.crop.isChecked(),
            "scale": self.scale.value(),
        }

    @classmethod
    def from_config(cls, cfg: image_rules.ResData, parent=None):
        self = cls(parent)
        self.min.setValue(cfg["min_res"])
        self.max.setValue(cfg["max_res"])
        self.crop.setChecked(cfg["crop"])
        self.scale.setValue(cfg["scale"])
        return self


class ChannelRuleView(RuleView):
    title = "Channels"

    needs_settings = True
    bound_item = image_rules.ChannelRule

    def configure_settings_group(self):
        self.min = QSpinBox(self)
        self.max = QSpinBox(self)
        self.min.setMinimum(1)
        self.min.valueChanged.connect(self.max.setMinimum)
        self.max.setMinimum(1)
        self.groupgrid.addWidget(QLabel("Min / Max: ", self), 0, 0)
        self.groupgrid.addWidget(self.min, 0, 1)
        self.groupgrid.addWidget(self.max, 0, 2)

    def reset_settings_group(self):
        self.min.setValue(1)
        self.max.setValue(3)

    def get(self):
        super().get()
        return image_rules.ChannelRule(min_channels=self.min.value(), max_channels=self.max.value())

    @classmethod
    def from_config(cls, cfg: ItemData, parent=None):
        self = cls(parent)
        self.min.setValue(cfg["min_channels"])
        self.max.setValue(cfg["max_channels"])
        return self

    def get_config(self):
        return {
            "min_channels": self.min.value(),
            "max_channels": self.max.value(),
        }


class HashRuleView(RuleView):
    title = "Hash"
    desc = "Uses imagehash functions to eliminate similar looking images"

    needs_settings = True
    bound_item = image_rules.HashRule

    def configure_settings_group(self):
        self.ignore_all_btn = QCheckBox(self)
        self.resolver = QLineEdit(self)
        self.resolver.setText("mtime")
        self.ignore_all_btn.toggled.connect(self.toggle_resolver)
        self.groupgrid.addWidget(QLabel("Ignore all with conflicts: ", self), 0, 0)
        self.groupgrid.addWidget(self.ignore_all_btn, 0, 1)
        self.groupgrid.addWidget(QLabel("Conflict resolver column: ", self), 1, 0)
        self.groupgrid.addWidget(self.resolver, 2, 0, 1, 2)

    def get(self):
        super().get()
        return image_rules.HashRule(
            resolver=self.resolver.text() if not self.ignore_all_btn.isChecked() else "ignore_all"
        )

    def get_config(self):
        return {
            "resolver": self.resolver.text(),
            "ignore_all": self.ignore_all_btn.isChecked(),
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.ignore_all_btn.setChecked(cfg["ignore_all"])
        self.resolver.setText(cfg["resolver"])
        return self

    @Slot(bool)
    def toggle_resolver(self, x):
        self.resolver.setEnabled(not x)