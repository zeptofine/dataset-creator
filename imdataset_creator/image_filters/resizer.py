from dataclasses import dataclass
from enum import Enum
from random import choice
from typing import Sequence

import cv2
import numpy as np

from ..configs.configtypes import FilterData, SpecialItemData
from ..datarules import Filter
from ..enum_helpers import listostr2listoenum

np_gen = np.random.default_rng()


class ResizeAlgos(Enum):
    BICUBIC = cv2.INTER_CUBIC
    BILINEAR = cv2.INTER_LINEAR
    BOX = cv2.INTER_AREA
    NEAREST = cv2.INTER_NEAREST
    LANCZOS = cv2.INTER_LANCZOS4
    DOWN_UP = False


DownUpAlgos = [e for e in ResizeAlgos if e != ResizeAlgos.DOWN_UP]


class ResizeMode(Enum):
    VALUE = 0
    MAX_RESOLUTION = 1
    MIN_RESOLUTION = 2


class ResizeData(SpecialItemData):
    mode: str
    algorithms: list[str]
    down_up_range: list[float]
    scale: float


@dataclass(frozen=True)
class Resize(Filter):
    mode: ResizeMode = ResizeMode.MIN_RESOLUTION
    algorithms: Sequence[ResizeAlgos] = (ResizeAlgos.BILINEAR,)
    down_up_range: tuple[float, float] = (0.5, 2)
    scale: float = 0.5

    def run(
        self,
        img: np.ndarray,
    ) -> np.ndarray:
        algorithms = self.algorithms
        original_algos = algorithms
        algorithm = choice(self.algorithms)

        h, w = img.shape[:2]
        scale = self.scale
        if self.mode == ResizeMode.MAX_RESOLUTION:
            scale = min(1, self.scale / max(h, w))
        elif self.mode == ResizeMode.MIN_RESOLUTION:
            scale = max(1, self.scale / min(h, w))

        new_h = int(h * scale)
        new_w = int(w * scale)

        if algorithm == "down_up":
            algo1: ResizeAlgos
            algo2: ResizeAlgos
            if original_algos is None:
                algo1 = choice(DownUpAlgos)
                algo2 = choice(DownUpAlgos)
            else:
                algo1 = original_algos[0]
                algo2 = original_algos[-1]

            scale_factor = np_gen.uniform(*self.down_up_range)
            return cv2.resize(
                cv2.resize(
                    img,
                    (int(w * scale_factor), int(h * scale_factor)),
                    interpolation=algo1.value,
                ),
                (new_w, new_h),
                interpolation=algo2.value,
            )

        return cv2.resize(
            img,
            (new_w, new_h),
            interpolation=algorithm.value,
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


class RandomFlipData(FilterData):
    flip_x_chance: float
    flip_y_chance: float


@dataclass(frozen=True, repr=False)
class RandomFlip(Filter):
    flip_x_chance: float
    flip_y_chance: float

    def run(self, img: np.ndarray) -> np.ndarray:
        if np_gen.uniform() < self.flip_x_chance:
            img = cv2.flip(img, 0)
        if np_gen.uniform() < self.flip_y_chance:
            img = cv2.flip(img, 1)
        return img


class RandomRotateDirections(Enum):
    ROTATE_180 = cv2.ROTATE_180
    ROTATE_90_CLOCKWISE = cv2.ROTATE_90_CLOCKWISE
    ROTATE_90_COUNTERCLOCKWISE = cv2.ROTATE_90_COUNTERCLOCKWISE


class RandomRotateData(FilterData):
    rotate_chance: float
    rotate_directions: list[str]


@dataclass(frozen=True, repr=False)
class RandomRotate(Filter):
    rotate_chance: float
    rotate_directions: list[RandomRotateDirections]

    def run(self, img: np.ndarray) -> np.ndarray:
        if np_gen.uniform() < self.rotate_chance:
            direction = choice(self.rotate_directions).value
            return cv2.rotate(img, direction)
        return img

    @classmethod
    def from_cfg(cls, cfg: RandomRotateData):
        return cls(
            rotate_chance=cfg["rotate_chance"],
            rotate_directions=[RandomRotateDirections[d] for d in cfg["rotate_directions"]],
        )
