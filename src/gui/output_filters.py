from abc import abstractmethod
from typing import Callable

import numpy as np
from PySide6.QtWidgets import (
    QDoubleSpinBox,
    QLabel,
    QSpinBox,
)

from ..datarules import base_rules, data_rules, image_rules
from ..datarules.base_rules import Filter
from ..image_filters import destroyers, resizer
from .frames import FlowItem, FlowList, MiniCheckList


class FilterView(FlowItem):
    title = "Filter"

    bound_item: type[Filter]

    def get(self):
        super().get()


class ResizeFilterView(FilterView):
    title = "Resize"
    needs_settings = True

    bound_item = resizer.Resize

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


class BlurFilterView(FilterView):
    title = "Blur"
    needs_settings = True

    bound_item = destroyers.Blur

    def configure_settings_group(self):
        self.algorithms = MiniCheckList(destroyers.AllBlurAlgos, self)
        self.scale = QDoubleSpinBox(self)
        self.scale.setSuffix("%")
        self.scale.setMinimum(1)
        self.scale.setMaximum(1_000)
        self.blur_range_x = QSpinBox(self)
        self.blur_range_x.setMinimum(0)
        self.blur_range_y = QSpinBox(self)
        self.blur_range_y.setMinimum(0)

        self.groupgrid.addWidget(self.algorithms, 0, 0, 1, 2)
        self.groupgrid.addWidget(QLabel("Scale:", self), 1, 0)
        self.groupgrid.addWidget(self.scale, 1, 1)
        self.groupgrid.addWidget(QLabel("Blur Range:", self), 2, 0)
        self.groupgrid.addWidget(self.blur_range_x, 2, 1)
        self.groupgrid.addWidget(self.blur_range_y, 3, 1)

    def reset_settings_group(self):
        self.scale.setValue(25)
        self.blur_range_x.setValue(1)
        self.blur_range_y.setValue(16)

    def get_config(self) -> destroyers.BlurData:
        return {
            "algorithms": [algo for algo, enabled in self.algorithms.get_config().items() if enabled],
            "blur_range": [self.blur_range_x.value(), self.blur_range_y.value()],
            "scale": self.scale.value() / 100,
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"] * 100)
        for item in cfg["algorithms"]:
            self.algorithms.set_config(item, True)
        r_x, r_y = cfg["blur_range"]
        self.blur_range_x.setValue(r_x)
        self.blur_range_y.setValue(r_y)

        return self


class NoiseFilterView(FilterView):
    title = "Noise"
    needs_settings = True

    bound_item = destroyers.Noise

    def configure_settings_group(self):
        self.algorithms = MiniCheckList(destroyers.AllNoiseAlgos, self)
        self.scale = QDoubleSpinBox(self)
        self.scale.setSuffix("%")
        self.scale.setMinimum(1)
        self.scale.setMaximum(1_000)
        self.intensity_range_x = QSpinBox(self)
        self.intensity_range_x.setMinimum(0)
        self.intensity_range_y = QSpinBox(self)
        self.intensity_range_y.setMinimum(0)

        self.groupgrid.addWidget(self.algorithms, 0, 0, 1, 2)
        self.groupgrid.addWidget(QLabel("Scale:", self), 1, 0)
        self.groupgrid.addWidget(self.scale, 1, 1)
        self.groupgrid.addWidget(QLabel("Intensity Range:", self), 2, 0)
        self.groupgrid.addWidget(self.intensity_range_x, 2, 1)
        self.groupgrid.addWidget(self.intensity_range_y, 3, 1)

    def reset_settings_group(self):
        self.scale.setValue(25)
        self.intensity_range_x.setValue(1)
        self.intensity_range_y.setValue(16)

    def get_config(self) -> destroyers.NoiseData:
        return {
            "algorithms": [algo for algo, enabled in self.algorithms.get_config().items() if enabled],
            "intensity_range": [self.intensity_range_x.value(), self.intensity_range_y.value()],
            "scale": self.scale.value() / 100,
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"] * 100)
        for item in cfg["algorithms"]:
            self.algorithms.set_config(item, True)
        r_x, r_y = cfg["intensity_range"]
        self.intensity_range_x.setValue(r_x)
        self.intensity_range_y.setValue(r_y)

        return self
