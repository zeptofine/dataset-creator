from __future__ import annotations

from PySide6.QtCore import QDate, QDateTime, QTime

from ..datarules import data_rules, image_rules
from .config_inputs import (
    ItemDeclaration,
    ItemSettings,
    ProceduralConfigList,
)
from .settings_inputs import (
    BoolInput,
    DateTimeInput,
    MultilineInput,
    NumberInput,
    RangeInput,
    TextInput,
)

# class RuleView(FlowItem):
#     title = "Rule"
#     bound_item: type[base_rules.Rule]
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.__original_desc = self.desc
#     def __init_subclass__(cls) -> None:
#         cls.__wrap_get()
#     def setup_widget(self, *args, **kwargs):
#         super().setup_widget(*args, **kwargs)
#     @abstractmethod
#     def get(self) -> base_rules.Rule:
#         """Evaluates the settings and returns a Rule instance"""
#         super().get()
#         return base_rules.Rule()
#     @classmethod
#     def __wrap_get(cls: type[Self]):
#         original_get = cls.get
#         original_get_config = cls.get_config
#         def get_wrapper(self: Self):
#             rule = original_get(self)
#             if rule.requires:
#                 if isinstance(rule.requires, base_rules.DataColumn):
#                     self.set_requires(str({rule.requires.name}))
#                 else:
#                     self.set_requires(str(set({r.name for r in rule.requires})))
#             return rule
#         def get_config_wrapper(self: Self):
#             self.get()
#             return original_get_config(self)
#         cls.get = get_wrapper
#         cls.get_config = get_config_wrapper
#     def set_requires(self, val):
#         newdesc = self.__original_desc
#         if val:
#             newdesc = newdesc + ("\n" if newdesc else "") + f"requires: {val}"
#             print("updated requires")
#             self.desc = newdesc
#             self.description_widget.setText(newdesc)
#             if not self.description_widget.isVisible():
#                 self.description_widget.show()


StatRuleView_ = ItemDeclaration(
    "Time Range",
    data_rules.StatRule,
    desc="Only allow files created within a time frame.",
    settings=ItemSettings(
        {
            "after": DateTimeInput(default=QDateTime(QDate(1970, 1, 1), QTime(0, 0, 0))).label("After:"),
            "before": DateTimeInput(default=QDateTime.currentDateTime()).label("Before:"),
        },
    ),
)

BlacklistWhitelistView_ = ItemDeclaration(
    "Blacklist and whitelist",
    data_rules.BlackWhitelistRule,
    desc="Only allows paths that include strs in the whitelist and not in the blacklist",
    settings=ItemSettings(
        {
            "whitelist": MultilineInput(is_list=True),
            "blacklist": MultilineInput(is_list=True),
        },
    ),
)


TotalLimitRuleView_ = ItemDeclaration(
    "Total count",
    data_rules.TotalLimitRule,
    desc="Limits the total number of files past this point",
    settings=ItemSettings({"limit": NumberInput((0, 1_000_000_000)).label("Limit:")}),
)

ResRuleView_ = ItemDeclaration(
    "Resolution",
    image_rules.ResRule,
    settings=ItemSettings(
        {
            ("min_res", "max_res"): RangeInput(default=(128, 2048), min_and_max_correlate=True).label("Min / Max:"),
            "crop": BoolInput(default=True).label("Try to crop:"),
            "scale": NumberInput((0, 128), default=4).label("Scale:"),
        }
    ),
)


ChannelRuleView_ = ItemDeclaration(
    "Channels",
    image_rules.ChannelRule,
    settings=ItemSettings(
        {("min_channels", "max_channels"): RangeInput(min_and_max_correlate=True).label("Min / Max:")}
    ),
)

HashRuleView_ = ItemDeclaration(
    "Hash",
    image_rules.HashRule,
    desc="Uses imagehash functions to eliminate similar looking images",
    settings=ItemSettings({"resolver": TextInput(default="ignore_all").label("Conflict resolver column:")}),
)


def rule_list(parent=None):
    return ProceduralConfigList(
        StatRuleView_,
        BlacklistWhitelistView_,
        TotalLimitRuleView_,
        ResRuleView_,
        ChannelRuleView_,
        HashRuleView_,
        parent=parent,
    ).label("Rules")
