from dataclasses import dataclass
from random import choice
from typing import Literal

import cv2
import numpy as np

from imdataset_creator.configs.configtypes import FilterData

from ..datarules.base_rules import Filter

np_gen = np.random.default_rng()

RESIZE_ALGOS = {
    "bicubic": cv2.INTER_CUBIC,
    "bilinear": cv2.INTER_LINEAR,
    "box": cv2.INTER_AREA,
    "nearest": cv2.INTER_NEAREST,
    "lanczos": cv2.INTER_LANCZOS4,
}
ResizeAlgorithms = Literal["bicubic", "bilinear", "box", "nearest", "lanczos", "down_up"]
DownUpScaleAlgorithms = list(RESIZE_ALGOS.keys())


@dataclass(frozen=True)
class Resize(Filter):
    algorithms: list[ResizeAlgorithms] | None = None
    down_up_range: tuple[float, float] = (0.5, 2)
    scale: float = 0.5

    def run(
        self,
        img: np.ndarray,
    ) -> np.ndarray:
        algorithms = self.algorithms
        original_algos = algorithms
        if algorithms is None:
            algorithms = ["down_up"]

        algorithm = choice(algorithms)

        h, w = img.shape[:2]
        new_h = int(h * self.scale)
        new_w = int(w * self.scale)
        if algorithm == "down_up":
            algo1: str
            algo2: str
            if original_algos is None:
                algo1 = choice(DownUpScaleAlgorithms)
                algo2 = choice(DownUpScaleAlgorithms)
            else:
                algo1 = original_algos[0]
                algo2 = original_algos[-1]

            scale_factor = np_gen.uniform(*self.down_up_range)
            return cv2.resize(
                cv2.resize(
                    img,
                    (int(w * scale_factor), int(h * scale_factor)),
                    interpolation=RESIZE_ALGOS[algo1],
                ),
                (new_w, new_h),
                interpolation=RESIZE_ALGOS[algo2],
            )

        return cv2.resize(
            img,
            (new_w, new_h),
            interpolation=RESIZE_ALGOS[algorithm],
        )


class CropData(FilterData):
    top: int | None
    left: int | None
    width: int | None
    height: int | None


@dataclass(frozen=True, repr=False)
class Crop(Filter):
    top: int | None
    left: int | None
    width: int | None
    height: int | None

    def run(self, img: np.ndarray) -> np.ndarray:
        newheight = None if self.height is None else (self.top or 0) + self.height
        newwidth = None if self.width is None else (self.left or 0) + self.width

        return img[self.top : newheight, self.left : newwidth]
