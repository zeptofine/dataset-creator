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
        algos = [algo for algo, enabled in self.algorithms.get_config().items() if enabled]
        if not algos:
            raise EmptyAlgorithmsError(self)
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


class CompressionFilterView(FilterView):
    title = "Compression"
    needs_settings = True

    bound_item = destroyers.Compression

    def configure_settings_group(self):
        self.algorithms = MiniCheckList(destroyers.AllCompressionAlgos, self)
        self.groupgrid.addWidget(self.algorithms, 0, 0, 1, 3)

        # jpeg quality
        self.j_range_min = QSpinBox(self)
        self.j_range_max = QSpinBox(self)
        self.j_range_max.setMaximum(100)
        self.j_range_min.valueChanged.connect(self.j_range_max.setMinimum)
        self.j_range_max.valueChanged.connect(self.j_range_min.setMaximum)
        self.groupgrid.addWidget(QLabel("JPEG quality range:", self), 1, 0)
        self.groupgrid.addWidget(self.j_range_min, 1, 1)
        self.groupgrid.addWidget(self.j_range_max, 1, 2)
        # webp quality
        self.w_range_min = QSpinBox(self)
        self.w_range_max = QSpinBox(self)
        self.w_range_max.setMaximum(100)
        self.w_range_min.valueChanged.connect(self.w_range_max.setMinimum)
        self.w_range_max.valueChanged.connect(self.w_range_min.setMaximum)
        self.groupgrid.addWidget(QLabel("WebP quality range:", self), 2, 0)
        self.groupgrid.addWidget(self.w_range_min, 2, 1)
        self.groupgrid.addWidget(self.w_range_max, 2, 2)
        # h264 crf
        self.h264_range_min = QSpinBox(self)
        self.h264_range_max = QSpinBox(self)
        self.h264_range_max.setMaximum(100)
        self.h264_range_min.valueChanged.connect(self.h264_range_max.setMinimum)
        self.h264_range_max.valueChanged.connect(self.h264_range_min.setMaximum)
        self.groupgrid.addWidget(QLabel("H264 CRF range:", self), 3, 0)
        self.groupgrid.addWidget(self.h264_range_min, 3, 1)
        self.groupgrid.addWidget(self.h264_range_max, 3, 2)
        # hevc crf
        self.hevc_range_min = QSpinBox(self)
        self.hevc_range_max = QSpinBox(self)
        self.hevc_range_min.setMaximum(100)
        self.hevc_range_min.valueChanged.connect(self.hevc_range_max.setMinimum)
        self.hevc_range_max.valueChanged.connect(self.hevc_range_min.setMaximum)
        self.groupgrid.addWidget(QLabel("HEVC CRF range:", self), 4, 0)
        self.groupgrid.addWidget(self.hevc_range_min, 4, 1)
        self.groupgrid.addWidget(self.hevc_range_max, 4, 2)
        # mpeg bitrate
        self.mpeg_bitrate = QSpinBox(self)
        self.mpeg_bitrate.setMaximum(1_000_000_000)  # idek what this is in gb
        self.groupgrid.addWidget(QLabel("MPEG bitrate:", self), 5, 0)
        self.groupgrid.addWidget(self.mpeg_bitrate, 5, 1, 1, 2)
        # mpeg2 bitrate
        self.mpeg2_bitrate = QSpinBox(self)
        self.mpeg2_bitrate.setMaximum(1_000_000_000)
        self.groupgrid.addWidget(QLabel("MPEG2 bitrate:", self), 6, 0)
        self.groupgrid.addWidget(self.mpeg2_bitrate, 6, 1, 1, 2)

    def reset_settings_group(self):
        self.j_range_min.setValue(0)
        self.j_range_max.setValue(100)
        self.w_range_min.setValue(1)
        self.w_range_max.setValue(100)
        self.h264_range_min.setValue(20)
        self.h264_range_max.setValue(28)
        self.hevc_range_min.setValue(25)
        self.hevc_range_max.setValue(33)

    def get_config(self) -> destroyers.CompressionData:
        algos = [algo for algo, enabled in self.algorithms.get_config().items() if enabled]
        if not algos:
            raise EmptyAlgorithmsError(self)
        return {
            "algorithms": [algo for algo, enabled in self.algorithms.get_config().items() if enabled],
            "jpeg_quality_range": [self.j_range_min.value(), self.j_range_max.value()],
            "webp_quality_range": [self.w_range_min.value(), self.w_range_max.value()],
            "h264_crf_range": [self.h264_range_min.value(), self.h264_range_max.value()],
            "hevc_crf_range": [self.hevc_range_min.value(), self.hevc_range_max.value()],
            "mpeg_bitrate": self.mpeg_bitrate.value(),
            "mpeg2_bitrate": self.mpeg2_bitrate.value(),
        }

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        for item in cfg["algorithms"]:
            self.algorithms.set_config(item, True)

        return self


class EmptyAlgorithmsError(Exception):
    """Raised when no algorithms are enabled"""

    def __init__(self, f: FilterView):
        super().__init__(f"No algorithms enabled in {f}")
