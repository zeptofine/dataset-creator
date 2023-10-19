from __future__ import annotations

from abc import abstractmethod

from PySide6.QtCore import QDate, QDateTime, QTime, Slot
from PySide6.QtWidgets import QCheckBox, QDateTimeEdit, QLabel, QLineEdit, QSpinBox, QTextEdit

from ..configs import ItemData
from ..datarules import base_rules, data_rules, image_rules
from .frames import BuilderDependencyList, FlowItem


class RuleView(FlowItem):
    title = "Rule"
    bound_item: type[base_rules.Rule]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__original_desc = self.desc

    def __init_subclass__(cls) -> None:
        cls.__wrap_get()

    def setup_widget(self, *args, **kwargs):
        super().setup_widget(*args, **kwargs)

    @abstractmethod
    def get(self) -> base_rules.Rule:
        """Evaluates the settings and returns a Rule instance"""
        super().get()
        return base_rules.Rule()

    @classmethod
    def __wrap_get(cls: type[RuleView]):
        original_get = cls.get
        original_get_config = cls.get_config

        def get_wrapper(self: RuleView):
            rule = original_get(self)
            if rule.requires:
                if isinstance(rule.requires, base_rules.DataColumn):
                    self.set_requires(str({rule.requires.name}))
                else:
                    self.set_requires(str(set({r.name for r in rule.requires})))
            return rule

        def get_config_wrapper(self: RuleView):
            self.get()
            return original_get_config(self)

        cls.get = get_wrapper
        cls.get_config = get_config_wrapper

    def set_requires(self, val):
        newdesc = self.__original_desc
        if val:
            newdesc = newdesc + ("\n" if newdesc else "") + f"requires: {val}"
            print("updated requires")
            self.desc = newdesc
            self.description_widget.setText(newdesc)
            if not self.description_widget.isVisible():
                self.description_widget.show()


class ItemsUnusedError(ValueError):
    def __init__(self):
        super().__init__("At least one item must be selected")


class StatRuleView(RuleView):
    title = "Time Range"
    desc = "only allow files created within a time frame."

    needs_settings = True
    bound_item = data_rules.StatRule
    _datetime_format: str = "dd/MM/yyyy h:mm AP"

    def configure_settings_group(self):
        self.use_after = QCheckBox(self)
        self.use_before = QCheckBox(self)
        self.after_widget = QDateTimeEdit(self)
        self.before_widget = QDateTimeEdit(self)
        self.after_widget.setCalendarPopup(True)
        self.before_widget.setCalendarPopup(True)
        self.after_widget.setDisplayFormat(self._datetime_format)
        self.before_widget.setDisplayFormat(self._datetime_format)
        self.use_after.stateChanged.connect(self.after_widget.setEnabled)
        self.use_before.stateChanged.connect(self.before_widget.setEnabled)
        format_label = QLabel(self._datetime_format, self)
        format_label.setEnabled(False)
        self.group_grid.addWidget(format_label, 0, 2, 1, 2)
        self.group_grid.addWidget(QLabel("After: ", self), 1, 0)
        self.group_grid.addWidget(QLabel("Before: ", self), 2, 0)
        self.group_grid.addWidget(self.use_after, 1, 1, 1, 1)
        self.group_grid.addWidget(self.use_before, 2, 1, 1, 1)
        self.group_grid.addWidget(self.after_widget, 1, 2, 1, 2)
        self.group_grid.addWidget(self.before_widget, 2, 2, 1, 2)

    def get(self):
        super().get()
        if not (self.use_before.isChecked() or self.use_after.isChecked()):
            raise ItemsUnusedError()
        return data_rules.StatRule(
            self.before_widget.dateTime().toString(self._datetime_format) if self.use_before.isChecked() else None,
            self.after_widget.dateTime().toString(self._datetime_format) if self.use_after.isChecked() else None,
        )

    def reset_settings_group(self):
        self.use_after.setChecked(False)
        self.use_before.setChecked(True)
        self.after_widget.setDateTime(QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0)))
        self.after_widget.setEnabled(False)
        self.before_widget.setDateTime(QDateTime.currentDateTime())
        self.before_widget.setEnabled(True)

    def get_config(self):
        if not (self.use_before.isChecked() or self.use_after.isChecked()):
            raise ItemsUnusedError()
        return {
            "after": self.after_widget.dateTime().toString(self._datetime_format)
            if self.use_after.isChecked()
            else None,
            "before": self.before_widget.dateTime().toString(self._datetime_format)
            if self.use_before.isChecked()
            else None,
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        if cfg["after"] is not None:
            self.use_after.setChecked(True)
            self.after_widget.setDateTime(QDateTime.fromString(cfg["after"], cls._datetime_format))
        if cfg["before"] is not None:
            self.use_before.setChecked(True)
            self.before_widget.setDateTime(QDateTime.fromString(cfg["before"], cls._datetime_format))
        return self


class BlacklistWhitelistView(RuleView):
    title = "Blacklist and whitelist"
    desc = "Only allows paths that include strs in the whitelist and not in the blacklist"

    needs_settings = True
    bound_item = data_rules.BlackWhitelistRule

    def configure_settings_group(self):
        self.whitelist = QTextEdit(self)
        self.blacklist = QTextEdit(self)

        self.group_grid.addWidget(QLabel("Whitelist: ", self), 0, 0)
        self.group_grid.addWidget(QLabel("Blacklist: ", self), 2, 0)
        self.group_grid.addWidget(self.whitelist, 1, 0, 1, 2)
        self.group_grid.addWidget(self.blacklist, 3, 0, 1, 2)

    def reset_settings_group(self):
        self.whitelist.clear()
        self.blacklist.clear()

    def get(self):
        super().get()
        return data_rules.BlackWhitelistRule(
            whitelist=self.whitelist.toPlainText().splitlines(),
            blacklist=self.blacklist.toPlainText().splitlines(),
        )

    def get_config(self) -> data_rules.BlackWhitelistData:
        return data_rules.BlackWhitelistData(
            {
                "whitelist": self.whitelist.toPlainText().splitlines(),
                "blacklist": self.blacklist.toPlainText().splitlines(),
            }
        )

    @classmethod
    def from_config(cls, cfg: data_rules.BlackWhitelistData, parent=None):
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
        self.group_grid.addWidget(QLabel("Limit: ", self), 0, 0)
        self.group_grid.addWidget(self.limit_widget, 0, 1)

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
        self.group_grid.addWidget(QLabel("Min / Max: ", self), 0, 0)
        self.group_grid.addWidget(self.min, 1, 0)
        self.group_grid.addWidget(self.max, 1, 1)
        self.group_grid.addWidget(QLabel("Try to crop: ", self), 2, 0)
        self.group_grid.addWidget(self.crop, 2, 1)
        self.group_grid.addWidget(QLabel("Scale: ", self), 3, 0)
        self.group_grid.addWidget(self.scale, 3, 1)

    def reset_settings_group(self):
        self.min.setValue(0)
        self.max.setValue(2048)
        self.scale.setValue(4)
        self.crop.setChecked(True)

    def get(self):
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
        self.group_grid.addWidget(QLabel("Min / Max: ", self), 0, 0)
        self.group_grid.addWidget(self.min, 0, 1)
        self.group_grid.addWidget(self.max, 0, 2)

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
        self.resolver = QLineEdit(self)
        self.resolver.setText("ignore_all")
        self.group_grid.addWidget(QLabel("Conflict resolver column: ", self), 0, 0)
        self.group_grid.addWidget(self.resolver, 1, 0, 1, 2)

    def get(self):
        super().get()
        return image_rules.HashRule(resolver=self.resolver.text())

    def get_config(self):
        return {
            "resolver": self.resolver.text(),
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.resolver.setText(cfg["resolver"])
        return self


class RuleList(BuilderDependencyList):
    items: list[RuleView]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_text("Rules")
        self.register_item(
            StatRuleView,
            BlacklistWhitelistView,
            TotalLimitRuleView,
            ResRuleView,
            ChannelRuleView,
            HashRuleView,
        )
