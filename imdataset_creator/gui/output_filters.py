from PySide6.QtCore import QSize
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFrame,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QSlider,
    QSpinBox,
    QToolButton,
    QWidget,
)

from ..datarules import Filter
from ..image_filters import destroyers, resizer
from .frames import FlowItem, FlowList, MiniCheckList, tooltip


class FilterView(FlowItem):
    title = "Filter"

    bound_item: type[Filter]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(QSize(self.size().width(), 200))

    def get(self):
        super().get()


TOOLTIPS = {
    "blur_range": "Range of values for blur kernel size or standard deviation (e.g., 1,10)",
    "blur_scale": "Adjusts the scaling of the blur range. For average and gaussian, this will add 1 when the new value is even",
    "noise_range": "Range of values for noise intensity (e.g., 0,50)",
    "scale_factor": "Adjusts the scaling of the noise range",
}


class ResizeFilterView(FilterView):
    title = "Resize"
    needs_settings = True

    bound_item = resizer.Resize

    def configure_settings_group(self):
        self.resize_mode = QComboBox(self)
        self.resize_mode.addItems(resizer.ResizeMode._member_names_)
        self.scale = QDoubleSpinBox(self)
        self.scale.setMinimum(1)
        self.scale.setMaximum(100_000)

        self.group_grid.addWidget(QLabel("Resize mode: ", self), 0, 0)
        self.group_grid.addWidget(self.resize_mode, 0, 1)
        self.group_grid.addWidget(QLabel("Scale:", self), 1, 0)
        self.group_grid.addWidget(self.scale, 1, 1)

    def reset_settings_group(self):
        self.scale.setValue(100)

    def get_config(self):
        return {"scale": self.scale.value() / 100}

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"] * 100)
        return self


class CropFilterView(FilterView):
    title = "Crop"
    desc = "Crop the image to the specified size. If the item is 0, it will not be considered"
    needs_settings = True

    bound_item = resizer.Crop

    def configure_settings_group(self) -> None:
        self.left_box = QSpinBox(self)
        self.top_box = QSpinBox(self)
        self.width_box = QSpinBox(self)
        self.height_box = QSpinBox(self)
        self.left_box.setMaximum(9_999_999)
        self.top_box.setMaximum(9_999_999)
        self.width_box.setMaximum(9_999_999)
        self.height_box.setMaximum(9_999_999)

        self.group_grid.addWidget(QLabel("Left:", self), 0, 0)
        self.group_grid.addWidget(self.left_box, 0, 1)
        self.group_grid.addWidget(QLabel("Top:", self), 1, 0)
        self.group_grid.addWidget(self.top_box, 1, 1)
        self.group_grid.addWidget(QLabel("Width:", self), 2, 0)
        self.group_grid.addWidget(self.width_box, 2, 1)
        self.group_grid.addWidget(QLabel("Height:", self), 3, 0)
        self.group_grid.addWidget(self.height_box, 3, 1)

    def get_config(self) -> resizer.CropData:
        return {
            "left": val if (val := self.left_box.value()) else None,
            "top": val if (val := self.top_box.value()) else None,
            "width": val if (val := self.width_box.value()) else None,
            "height": val if (val := self.height_box.value()) else None,
        }

    @classmethod
    def from_config(cls, cfg: resizer.CropData, parent=None):
        self = cls(parent)
        self.left_box.setValue(cfg["left"] or 0)
        self.top_box.setValue(cfg["top"] or 0)
        self.width_box.setValue(cfg["width"] or 0)
        self.height_box.setValue(cfg["height"] or 0)
        return self


class BlurFilterView(FilterView):
    title = "Blur"
    needs_settings = True

    bound_item = destroyers.Blur

    def configure_settings_group(self):
        self.algorithms = MiniCheckList(destroyers.BlurAlgorithm._member_names_, self)
        self.scale = QDoubleSpinBox(self)
        scale_label = QLabel("Scale:", self)
        tooltip(scale_label, TOOLTIPS["blur_scale"])

        self.scale.setMinimum(0)
        self.scale.setMaximum(100)
        self.scale.setSingleStep(0.1)

        blur_label = QLabel("Blur Range:", self)
        tooltip(blur_label, TOOLTIPS["blur_range"])
        self.blur_range_x = QSpinBox(self)
        self.blur_range_x.setMinimum(0)
        self.blur_range_y = QSpinBox(self)
        self.blur_range_y.setMinimum(0)

        self.group_grid.addWidget(self.algorithms, 0, 0, 1, 2)
        self.group_grid.addWidget(scale_label, 1, 0)
        self.group_grid.addWidget(self.scale, 1, 1)
        self.group_grid.addWidget(blur_label, 2, 0)
        self.group_grid.addWidget(self.blur_range_x, 2, 1)
        self.group_grid.addWidget(self.blur_range_y, 3, 1)

    def reset_settings_group(self):
        self.algorithms.disable_all()
        self.scale.setValue(0.25)
        self.blur_range_x.setValue(1)
        self.blur_range_y.setValue(16)

    def get_config(self) -> destroyers.BlurData:
        algos = [algo for algo, enabled in self.algorithms.get_config().items() if enabled]
        if not algos:
            raise EmptyAlgorithmsError(self)
        return destroyers.BlurData(
            {
                "algorithms": algos,
                "blur_range": [self.blur_range_x.value(), self.blur_range_y.value()],
                "scale": self.scale.value(),
            }
        )

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        self.scale.setValue(cfg["scale"])
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
        self.algorithms = MiniCheckList(destroyers.NoiseAlgorithm._member_names_, self)
        self.scale = QDoubleSpinBox(self)
        self.scale.setSuffix("%")
        self.scale.setMinimum(1)
        self.scale.setMaximum(1_000)
        intensity_label = QLabel("Intensity Range:", self)
        tooltip(intensity_label, TOOLTIPS["noise_range"])
        self.intensity_range_x = QSpinBox(self)
        self.intensity_range_x.setMinimum(0)
        self.intensity_range_y = QSpinBox(self)
        self.intensity_range_y.setMinimum(0)

        self.group_grid.addWidget(self.algorithms, 0, 0, 1, 2)
        self.group_grid.addWidget(QLabel("Scale:", self), 1, 0)
        self.group_grid.addWidget(self.scale, 1, 1)
        self.group_grid.addWidget(intensity_label, 2, 0)
        self.group_grid.addWidget(self.intensity_range_x, 2, 1)
        self.group_grid.addWidget(self.intensity_range_y, 3, 1)

    def reset_settings_group(self):
        self.scale.setValue(25)
        self.algorithms.disable_all()
        self.intensity_range_x.setValue(1)
        self.intensity_range_y.setValue(16)

    def get_config(self) -> destroyers.NoiseData:
        algos = [algo for algo, enabled in self.algorithms.get_config().items() if enabled]
        if not algos:
            raise EmptyAlgorithmsError(self)
        return destroyers.NoiseData(
            {
                "algorithms": algos,
                "intensity_range": [self.intensity_range_x.value(), self.intensity_range_y.value()],
                "scale": self.scale.value() / 100,
            }
        )

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
        self.algorithms = MiniCheckList(destroyers.CompressionAlgorithms._member_names_, self)
        self.group_grid.addWidget(self.algorithms, 0, 0, 1, 3)

        # jpeg quality
        self.j_range_min = QSpinBox(self)
        self.j_range_max = QSpinBox(self)
        self.j_range_max.setMaximum(100)
        self.j_range_min.valueChanged.connect(self.j_range_max.setMinimum)
        self.j_range_max.valueChanged.connect(self.j_range_min.setMaximum)
        self.group_grid.addWidget(QLabel("JPEG quality range:", self), 1, 0)
        self.group_grid.addWidget(self.j_range_min, 1, 1)
        self.group_grid.addWidget(self.j_range_max, 1, 2)
        # webp quality
        self.w_range_min = QSpinBox(self)
        self.w_range_max = QSpinBox(self)
        self.w_range_max.setMaximum(100)
        self.w_range_min.valueChanged.connect(self.w_range_max.setMinimum)
        self.w_range_max.valueChanged.connect(self.w_range_min.setMaximum)
        self.group_grid.addWidget(QLabel("WebP quality range:", self), 2, 0)
        self.group_grid.addWidget(self.w_range_min, 2, 1)
        self.group_grid.addWidget(self.w_range_max, 2, 2)
        # h264 crf
        self.h264_range_min = QSpinBox(self)
        self.h264_range_max = QSpinBox(self)
        self.h264_range_max.setMaximum(100)
        self.h264_range_min.valueChanged.connect(self.h264_range_max.setMinimum)
        self.h264_range_max.valueChanged.connect(self.h264_range_min.setMaximum)
        self.group_grid.addWidget(QLabel("H264 CRF range:", self), 3, 0)
        self.group_grid.addWidget(self.h264_range_min, 3, 1)
        self.group_grid.addWidget(self.h264_range_max, 3, 2)
        # hevc crf
        self.hevc_range_min = QSpinBox(self)
        self.hevc_range_max = QSpinBox(self)
        self.hevc_range_min.setMaximum(100)
        self.hevc_range_min.valueChanged.connect(self.hevc_range_max.setMinimum)
        self.hevc_range_max.valueChanged.connect(self.hevc_range_min.setMaximum)
        self.group_grid.addWidget(QLabel("HEVC CRF range:", self), 4, 0)
        self.group_grid.addWidget(self.hevc_range_min, 4, 1)
        self.group_grid.addWidget(self.hevc_range_max, 4, 2)
        # mpeg bitrate
        self.mpeg_bitrate = QSpinBox(self)
        self.mpeg_bitrate.setMaximum(1_000_000_000)  # idek what this is in gb
        self.group_grid.addWidget(QLabel("MPEG bitrate:", self), 5, 0)
        self.group_grid.addWidget(self.mpeg_bitrate, 5, 1, 1, 2)
        # mpeg2 bitrate
        self.mpeg2_bitrate = QSpinBox(self)
        self.mpeg2_bitrate.setMaximum(1_000_000_000)
        self.group_grid.addWidget(QLabel("MPEG2 bitrate:", self), 6, 0)
        self.group_grid.addWidget(self.mpeg2_bitrate, 6, 1, 1, 2)

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
        return destroyers.CompressionData(
            {
                "algorithms": [algo for algo, enabled in self.algorithms.get_config().items() if enabled],
                "jpeg_quality_range": [self.j_range_min.value(), self.j_range_max.value()],
                "webp_quality_range": [self.w_range_min.value(), self.w_range_max.value()],
                "h264_crf_range": [self.h264_range_min.value(), self.h264_range_max.value()],
                "hevc_crf_range": [self.hevc_range_min.value(), self.hevc_range_max.value()],
                "mpeg_bitrate": self.mpeg_bitrate.value(),
                "mpeg2_bitrate": self.mpeg2_bitrate.value(),
            }
        )

    @classmethod
    def from_config(cls, cfg, parent=None):
        self = cls(parent)
        for item in cfg["algorithms"]:
            self.algorithms.set_config(item, True)
        self.j_range_min.setValue(cfg["jpeg_quality_range"][0])
        self.j_range_max.setValue(cfg["jpeg_quality_range"][1])
        self.w_range_min.setValue(cfg["webp_quality_range"][0])
        self.w_range_max.setValue(cfg["webp_quality_range"][1])
        self.h264_range_min.setValue(cfg["h264_crf_range"][0])
        self.h264_range_max.setValue(cfg["h264_crf_range"][1])
        self.hevc_range_min.setValue(cfg["hevc_crf_range"][0])
        self.hevc_range_max.setValue(cfg["hevc_crf_range"][1])
        self.mpeg_bitrate.setValue(cfg["mpeg_bitrate"])
        self.mpeg2_bitrate.setValue(cfg["mpeg2_bitrate"])

        return self


class EmptyAlgorithmsError(Exception):
    """Raised when no algorithms are enabled"""

    def __init__(self, f: FilterView):
        super().__init__(f"No algorithms enabled in {f}")


class FilterList(FlowList):
    items: list[FilterView]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_text("Filters")
        self.register_item(
            ResizeFilterView,
            CropFilterView,
            BlurFilterView,
            NoiseFilterView,
            CompressionFilterView,
        )
