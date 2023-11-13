from __future__ import annotations

from PySide6.QtCore import Qt

from ..datarules import Output
from ..datarules.base_rules import Filter
from ..image_filters import destroyers, resizer
from .config_inputs import ItemDeclaration, ProceduralConfigList, ProceduralFlowListInput
from .settings_inputs import (
    BoolInput,
    DirectoryInput,
    DoubleInput,
    DropdownInput,
    EnumChecklistInput,
    ItemSettings,
    NumberInput,
    RangeInput,
    TextInput,
)

# class FilterView(FlowItem):
#     title = "Filter"
#     needs_settings = True

#     bound_item: type[Filter]

#     def __init__(self, parent=None):
#         super().__init__(parent)
#         self.setMinimumSize(QSize(self.size().width(), 200))

#     def get(self):
#         super().get()


TOOLTIPS = {
    "blur_range": "Range of values for blur kernel size or standard deviation (e.g., 1,10)",
    "blur_scale": "Adjusts the scaling of the blur range. For average and gaussian, this will add 1 when the new value is even",
    "noise_range": "Range of values for noise intensity (e.g., 0,50)",
    "scale_factor": "Adjusts the scaling of the noise range",
}


def mult_100(val):
    return val * 100


def div_100(val):
    return val / 100


FilterDeclaration = ItemDeclaration[Filter]


ResizeFilterView_ = FilterDeclaration(
    "Resize",
    resizer.Resize,
    settings=ItemSettings(
        {
            "mode": (
                DropdownInput(list(resizer.ResizeMode.__members__.values())).label("Resize mode: ")  # type: ignore
            ),
            "scale": (
                DoubleInput((1, 100_000), default=100)
                .label("Scale: ")
                .from_config_modification(mult_100)
                .to_config_modification(div_100)
            ),
        }
    ),
)

CropFilterView_ = FilterDeclaration(
    "Crop",
    resizer.Crop,
    desc="Crop the image to the specified size. If the item is 0, it will not be considered",
    settings=ItemSettings(
        {
            "left": NumberInput((0, 9_999_999)).label("Left:").set_optional(),
            "top": NumberInput((0, 9_999_999)).label("Top:").set_optional(),
            "width": NumberInput((0, 9_999_999)).label("Width:").set_optional(),
            "height": NumberInput((0, 9_999_999)).label("Height").set_optional(),
        }
    ),
)


BlurFilterView_ = FilterDeclaration(
    "Blur",
    destroyers.Blur,
    settings=ItemSettings(
        {
            "algorithms": EnumChecklistInput(destroyers.BlurAlgorithm),
            "scale": DoubleInput((0, 100), default=0.25, step=0.1).label("Scale:").tooltip(TOOLTIPS["blur_scale"]),
            "blur_range": (RangeInput(min_and_max_correlate=True).label("Blur Range:").tooltip(TOOLTIPS["blur_range"])),
        },
    ),
)

NoiseFilterView_ = FilterDeclaration(
    "Noise",
    destroyers.Noise,
    settings=ItemSettings(
        {
            "algorithms": EnumChecklistInput(destroyers.NoiseAlgorithm),
            "intensity_range": RangeInput().label("Intensity Range:").tooltip(TOOLTIPS["noise_range"]),
            "scale": (
                NumberInput((1, 1_000), default=25)
                .label("Scale:")
                .from_config_modification(mult_100)
                .to_config_modification(div_100)
            ),
        },
    ),
)


CompressionFilterView_ = FilterDeclaration(
    "Compression",
    destroyers.Compression,
    settings=ItemSettings(
        {
            "algorithms": EnumChecklistInput(destroyers.CompressionAlgorithms),
            "jpeg_quality_range": RangeInput(default=(0, 100)).label("JPEG quality:"),
            "webp_quality_range": RangeInput(default=(1, 100)).label("WebP quality:"),
            "h264_crf_range": RangeInput(default=(20, 28)).label("H.264 CRF"),
            "hevc_crf_range": RangeInput(default=(25, 33)).label("HEVC CRF"),
            "mpeg_bitrate": NumberInput((0, 1_000_000_000)).label("MPEG bitrate:"),
            "mpeg2_bitrate": NumberInput((0, 1_000_000_000)).label("MPEG2 bitrate:"),
        },
    ),
)

RandomFlipFilterView_ = FilterDeclaration(
    "Random Flip",
    bound_item=resizer.RandomFlip,
    settings=ItemSettings(
        {
            "flip_x_chance": (
                DoubleInput((0, 100), default=50, slider=Qt.Orientation.Horizontal)
                .label("horizontal flip chance:")
                .from_config_modification(mult_100)
                .to_config_modification(div_100)
            ),
            "flip_y_chance": (
                DoubleInput((0, 100), default=50, slider=Qt.Orientation.Horizontal)
                .label("vertical flip chance:")
                .from_config_modification(mult_100)
                .to_config_modification(div_100)
            ),
        },
    ),
)

RandomRotateFilterView_ = FilterDeclaration(
    "Random Rotate",
    bound_item=resizer.RandomRotate,
    settings=ItemSettings(
        {
            "rotate_direction": (EnumChecklistInput(resizer.RandomRotateDirections)),
            "rotate_chance": (
                DoubleInput((0, 100), default=50, slider=Qt.Orientation.Horizontal)
                .from_config_modification(mult_100)
                .to_config_modification(div_100)
                .label("Rotation chance:")
            ),
        },
    ),
)

OutputView_ = ItemDeclaration(
    "Output",
    Output,
    settings=ItemSettings(
        {
            "folder": DirectoryInput().label("Folder: "),
            "output_format": TextInput(default="{relative_path}/{file}.{ext}"),
            "overwrite": BoolInput(default=False).label("overwrite existing files"),
            "lst": ProceduralFlowListInput(
                ResizeFilterView_,
                CropFilterView_,
                BlurFilterView_,
                NoiseFilterView_,
                CompressionFilterView_,
                RandomFlipFilterView_,
                RandomRotateFilterView_,
            ),
        }
    ),
)


def output_list(parent=None):
    return ProceduralConfigList(OutputView_, parent=parent)
