from abc import abstractmethod
from typing import Callable

import numpy as np
from PySide6.QtWidgets import (
    QDoubleSpinBox,
    QLabel,
)

from ..datarules import base_rules, data_rules, image_rules
from ..image_filters import size_changers
from .frames import FlowItem, FlowList


class Filter(FlowItem):
    title = "Filter"

    bound_item: Callable

    @abstractmethod
    def __call__(self, img: np.ndarray):
        return img


class ResizeFilterView(Filter):
    title = "Resize"
    needs_settings = True

    bound_item = size_changers.Resize

    def configure_settings_group(self):
        self.scale = QDoubleSpinBox(self)
        self.scale.setSuffix("%")
        self.scale.setMinimum(1)
        self.scale.setMaximum(1_000)

        self.groupgrid.addWidget(QLabel("Scale:", self), 0, 0)
        self.groupgrid.addWidget(self.scale, 0, 1)

    def reset_settings_group(self):
        self.scale.setValue(100)

    def get_config(self):
        return {"scale": self.scale.value() / 100}

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"] * 100)
        return self
